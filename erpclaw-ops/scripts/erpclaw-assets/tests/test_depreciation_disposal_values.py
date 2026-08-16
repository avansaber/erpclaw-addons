"""Part A — VALUE tests for `post-depreciation`, `run-depreciation` and
`dispose-asset` (Wave G F21).

All three post `gl_entry` rows and rewrite an asset's carrying value, and all
three had the same single test: the generated contract assert that the action
dispatches. `test_assets.py` covers the impairment / revaluation / capitalize
family with real GL asserts; the depreciation and disposal family never got the
same treatment.

Register rows: `planning/wave_g/F21_TEST_DEPTH_REGISTER_2026-08-11.json`
(`post-depreciation`, `run-depreciation`, `dispose-asset`; all
`routability-only`, ledger reach `gl_entry`).

The disposal pins carried F21-FINDING-2: the shipped GL layout did not match the
action's own documented layout. **Fixed 2026-08-12 (M61)** — the proceeds and the
gain/loss plug now take explicit accounts, and these pins read the repaired
layout, per-leg. `planning/simlogs/m61_SIM_2026-08-12.md` has the before/after
journal entries.
"""
import json
import os
import sys
from decimal import Decimal

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from assets_helpers import (build_gl_env, call_action, is_error, is_ok,  # noqa: E402
                           load_db_query, ns, seed_asset, seed_asset_category)

M = load_db_query()

D = Decimal


def _msg(result: dict) -> str:
    return result.get("message", "") + result.get("error", "")


def _schedule(conn, asset_id):
    return conn.execute(
        "SELECT id, schedule_date, depreciation_amount, status, journal_entry_id "
        "FROM depreciation_schedule WHERE asset_id = ? ORDER BY schedule_date",
        (asset_id,)).fetchall()


def _gl(conn, voucher_type, voucher_id=None):
    if voucher_id:
        return conn.execute(
            "SELECT account_id, debit, credit, cost_center_id FROM gl_entry "
            "WHERE voucher_type = ? AND voucher_id = ? AND is_cancelled = 0 "
            "ORDER BY debit DESC", (voucher_type, voucher_id)).fetchall()
    return conn.execute(
        "SELECT account_id, debit, credit FROM gl_entry WHERE voucher_type = ? "
        "AND is_cancelled = 0 ORDER BY debit DESC", (voucher_type,)).fetchall()


def _asset(conn, asset_id):
    return conn.execute(
        "SELECT status, gross_value, current_book_value, accumulated_depreciation "
        "FROM asset WHERE id = ?", (asset_id,)).fetchone()


@pytest.fixture
def genv(conn):
    """A 5,000.00 in-use asset, straight line over 5 years, accounts wired."""
    env = build_gl_env(conn)
    gen = call_action(M.generate_depreciation_schedule, conn,
                      ns(asset_id=env["asset_id"]))
    assert is_ok(gen), gen
    env["schedule"] = _schedule(conn, env["asset_id"])
    assert env["schedule"], "the fixture needs at least one pending row"
    return env


# ── post-depreciation ────────────────────────────────────────────────────────

def test_posting_one_row_moves_exactly_that_amount(conn, genv):
    row = genv["schedule"][0]
    amount = D(row["depreciation_amount"])
    assert amount > 0

    res = call_action(M.post_depreciation, conn, ns(
        depreciation_schedule_id=row["id"], asset_id=None,
        posting_date=row["schedule_date"], cost_center_id=None))
    assert is_ok(res), res
    assert D(res["depreciation_amount"]) == amount
    assert D(res["new_accumulated_depreciation"]) == amount
    assert D(res["new_book_value"]) == D("5000.00") - amount

    # GL: DR depreciation expense, CR accumulated depreciation, same amount.
    gl = _gl(conn, "depreciation_entry", row["id"])
    assert len(gl) == 2
    assert gl[0]["account_id"] == genv["depreciation_account_id"]
    assert D(gl[0]["debit"]) == amount
    assert D(gl[0]["credit"]) == D("0")
    assert gl[1]["account_id"] == genv["accumulated_depreciation_account_id"]
    assert D(gl[1]["credit"]) == amount
    assert gl[0]["cost_center_id"] == genv["cost_center_id"]

    # The asset carries the same numbers the payload reported.
    a = _asset(conn, genv["asset_id"])
    assert D(a["accumulated_depreciation"]) == amount
    assert D(a["current_book_value"]) == D("5000.00") - amount
    assert D(a["current_book_value"]) + D(a["accumulated_depreciation"]) == \
        D(a["gross_value"]), "book value + accumulated must always equal gross"

    # The schedule row is consumed and points at its posting.
    after = _schedule(conn, genv["asset_id"])[0]
    assert after["status"] == "posted"
    assert after["journal_entry_id"] == row["id"]


def test_posting_the_same_row_twice_is_refused_and_posts_nothing_more(conn, genv):
    row = genv["schedule"][0]
    assert is_ok(call_action(M.post_depreciation, conn, ns(
        depreciation_schedule_id=row["id"], asset_id=None,
        posting_date=row["schedule_date"], cost_center_id=None)))
    gl_before = len(_gl(conn, "depreciation_entry"))

    again = call_action(M.post_depreciation, conn, ns(
        depreciation_schedule_id=row["id"], asset_id=None,
        posting_date=row["schedule_date"], cost_center_id=None))
    assert is_error(again)
    assert "already" in _msg(again)

    assert len(_gl(conn, "depreciation_entry")) == gl_before
    a = _asset(conn, genv["asset_id"])
    assert D(a["accumulated_depreciation"]) == D(row["depreciation_amount"])


def test_posting_without_wired_accounts_is_refused_before_any_gl(conn, genv):
    conn.execute("UPDATE asset_category SET accumulated_depreciation_account_id = NULL "
                 "WHERE id = ?", (genv["category_id"],))
    conn.commit()
    row = genv["schedule"][0]

    bad = call_action(M.post_depreciation, conn, ns(
        depreciation_schedule_id=row["id"], asset_id=None,
        posting_date=row["schedule_date"], cost_center_id=None))
    assert is_error(bad)
    assert "accumulated_depreciation_account_id" in _msg(bad)
    assert _gl(conn, "depreciation_entry") == []
    assert D(_asset(conn, genv["asset_id"])["current_book_value"]) == D("5000.00")


# ── run-depreciation ─────────────────────────────────────────────────────────

def test_batch_posts_every_due_row_and_stops_at_the_cutoff(conn, genv):
    sched = genv["schedule"]
    assert len(sched) >= 2, "this pin needs at least two scheduled periods"
    cutoff = sched[1]["schedule_date"]
    due = [r for r in sched if r["schedule_date"] <= cutoff]
    due_total = sum((D(r["depreciation_amount"]) for r in due), D("0"))

    res = call_action(M.run_depreciation, conn, ns(
        company_id=genv["company_id"], posting_date=cutoff, cost_center_id=None))
    assert is_ok(res), res
    assert res["entries_posted"] == len(due)
    assert sum((D(d["amount"]) for d in res["details"]), D("0")) == due_total

    # One balanced pair per posted row, and nothing else.
    gl = _gl(conn, "depreciation_entry")
    assert len(gl) == 2 * len(due)
    assert sum((D(g["debit"]) for g in gl), D("0")) == due_total
    assert sum((D(g["credit"]) for g in gl), D("0")) == due_total

    a = _asset(conn, genv["asset_id"])
    assert D(a["accumulated_depreciation"]) == due_total
    assert D(a["current_book_value"]) == D("5000.00") - due_total

    # Later periods are untouched.
    after = _schedule(conn, genv["asset_id"])
    assert [r["status"] for r in after[:len(due)]] == ["posted"] * len(due)
    assert all(r["status"] == "pending" for r in after[len(due):])


def test_a_second_batch_over_the_same_window_posts_nothing(conn, genv):
    cutoff = genv["schedule"][0]["schedule_date"]
    first = call_action(M.run_depreciation, conn, ns(
        company_id=genv["company_id"], posting_date=cutoff, cost_center_id=None))
    assert is_ok(first) and first["entries_posted"] >= 1
    gl_before = len(_gl(conn, "depreciation_entry"))
    accum_before = D(_asset(conn, genv["asset_id"])["accumulated_depreciation"])

    second = call_action(M.run_depreciation, conn, ns(
        company_id=genv["company_id"], posting_date=cutoff, cost_center_id=None))
    assert is_ok(second), second
    assert second["entries_posted"] == 0
    assert "No pending depreciation entries" in second["message"]
    assert len(_gl(conn, "depreciation_entry")) == gl_before
    assert D(_asset(conn, genv["asset_id"])["accumulated_depreciation"]) == accum_before


def test_batch_is_refused_for_an_unknown_company(conn, genv):
    bad = call_action(M.run_depreciation, conn, ns(
        company_id="no-such-company", posting_date="2026-12-31", cost_center_id=None))
    assert is_error(bad)
    assert _gl(conn, "depreciation_entry") == []


# ── dispose-asset ────────────────────────────────────────────────────────────
#
# M61 (2026-08-12) closed F21-FINDING-2: every proceeds and gain/loss leg used
# to post to the category's depreciation_account_id. The pins below now read the
# repaired layout — proceeds to --proceeds-account-id, the plug to
# --gain-loss-account-id, and the depreciation account never touched by a
# disposal at all. Plan home: planning/pending_items.md row M61.


def _by_account(gl):
    """{account_id: (total_debit, total_credit)} for one voucher's legs."""
    out = {}
    for g in gl:
        d, c = out.get(g["account_id"], (D("0"), D("0")))
        out[g["account_id"]] = (d + D(g["debit"]), c + D(g["credit"]))
    return out


def _root_types(conn, account_ids):
    return {r["id"]: r["root_type"] for r in conn.execute(
        "SELECT id, root_type FROM account WHERE id IN "
        "(%s)" % ",".join("?" * len(account_ids)), tuple(account_ids)).fetchall()}


def _depreciate_once(conn, genv):
    """Post the first scheduled period; returns (accumulated, book_value)."""
    row = genv["schedule"][0]
    assert is_ok(call_action(M.post_depreciation, conn, ns(
        depreciation_schedule_id=row["id"], asset_id=None,
        posting_date=row["schedule_date"], cost_center_id=None)))
    accum = D(row["depreciation_amount"])
    return accum, D("5000.00") - accum


def _dispose(conn, genv, **over):
    kwargs = dict(asset_id=genv["asset_id"], disposal_date="2026-09-30",
                  disposal_method="scrap", sale_amount=None,
                  buyer_details=None, cost_center_id=None,
                  proceeds_account_id=None,
                  gain_loss_account_id=genv["loss_account_id"])
    kwargs.update(over)
    return call_action(M.dispose_asset, conn, ns(**kwargs))


def _nothing_landed(conn, genv, book=D("5000.00")):
    """No disposal row, no disposal GL, asset untouched. `book` is a parameter so
    a refusal can be proven on a part-depreciated asset (M91) as well as on a
    pristine one — the carrying value must be whatever it was before the call."""
    assert conn.execute("SELECT COUNT(*) FROM asset_disposal").fetchone()[0] == 0
    assert _gl(conn, "asset_disposal") == []
    a = _asset(conn, genv["asset_id"])
    assert a["status"] == "in_use"
    assert D(a["current_book_value"]) == book


def _disposal_row(conn, disposal_id):
    """The persisted `asset_disposal` row. `get-asset` hands this straight back
    as `asset.disposal`, so every column here is user-visible."""
    return conn.execute(
        "SELECT asset_id, disposal_date, disposal_method, sale_amount, "
        "book_value_at_disposal, gain_or_loss, journal_entry_id, buyer_details "
        "FROM asset_disposal WHERE id = ?", (disposal_id,)).fetchone()


def _assert_disposal_persisted(conn, genv, res, *, sale, book, gain_or_loss,
                               method, asset_status, buyer=None,
                               date="2026-09-30"):
    """What was stored must be the money that actually moved, to the cent.

    M61 rider (2026-08-12): `sale_amount` was selected by the scrap pin and
    never asserted, so persisting the asset's gross value instead of the
    proceeds — a 6,000.00 sale recorded as 5,000.00 — left the whole suite
    green. Every money column of the row is now pinned on every disposal
    shape, against the payload as well as against the expected value.
    """
    row = _disposal_row(conn, res["disposal_id"])
    assert row is not None, "a disposal must be persisted, not just reported"
    assert row["asset_id"] == genv["asset_id"]
    assert row["disposal_date"] == date
    assert row["disposal_method"] == method
    assert D(row["sale_amount"]) == sale
    assert D(row["book_value_at_disposal"]) == book
    assert D(row["gain_or_loss"]) == gain_or_loss
    # …and the stored row must agree with the payload the caller was handed.
    assert D(row["sale_amount"]) == D(res["sale_amount"])
    assert D(row["book_value_at_disposal"]) == D(res["book_value_at_disposal"])
    assert D(row["gain_or_loss"]) == D(res["gain_or_loss"])
    assert row["buyer_details"] == buyer
    assert row["journal_entry_id"] == res["disposal_id"], (
        "a posted disposal must point at its own voucher")
    assert _asset(conn, genv["asset_id"])["status"] == asset_status
    return row


def test_scrapping_writes_the_loss_and_zeroes_the_carrying_value(conn, genv):
    accum, book = _depreciate_once(conn, genv)

    res = _dispose(conn, genv, disposal_method="scrap")
    assert is_ok(res), res
    assert D(res["sale_amount"]) == D("0")
    assert D(res["book_value_at_disposal"]) == book
    assert D(res["gain_or_loss"]) == -book
    assert res["new_status"] == "scrapped"

    _assert_disposal_persisted(conn, genv, res, sale=D("0"), book=book,
                               gain_or_loss=-book, method="scrap",
                               asset_status="scrapped")

    # DR accumulated depreciation + DR loss on disposal, CR the asset at gross.
    gl = _gl(conn, "asset_disposal", res["disposal_id"])
    assert len(gl) == 3
    by = _by_account(gl)
    assert by[genv["accumulated_depreciation_account_id"]] == (accum, D("0"))
    assert by[genv["asset_account_id"]] == (D("0"), D("5000.00"))
    assert by[genv["loss_account_id"]] == (book, D("0"))
    # The scrap loss is NOT depreciation expense (F21-FINDING-2's milder half).
    assert genv["depreciation_account_id"] not in by
    assert sum((D(g["debit"]) for g in gl), D("0")) == \
        sum((D(g["credit"]) for g in gl), D("0")) == D("5000.00")
    assert res["proceeds_account_id"] is None
    assert res["gain_loss_account_id"] == genv["loss_account_id"]

    a = _asset(conn, genv["asset_id"])
    assert a["status"] == "scrapped"
    assert D(a["current_book_value"]) == D("0")


def test_sale_at_a_gain_puts_the_money_in_the_bank_and_the_gain_in_income(conn, genv):
    """A 5,000.00 asset with 83.33 of depreciation sold for 6,000.00: the bank
    receives 6,000.00 and the books recognize a 1,083.33 gain. Before M61 both
    numbers landed in depreciation expense."""
    accum, book = _depreciate_once(conn, genv)
    gain = D("6000.00") - book

    res = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   buyer_details="Wayne Enterprises",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_ok(res), res
    assert D(res["gain_or_loss"]) == gain == D("1083.33")
    assert res["new_status"] == "sold"

    # The 6,000.00 the buyer paid is what gets stored, not the 5,000.00 the
    # asset cost.
    _assert_disposal_persisted(conn, genv, res, sale=D("6000.00"), book=book,
                               gain_or_loss=gain, method="sale",
                               asset_status="sold", buyer="Wayne Enterprises")

    gl = _gl(conn, "asset_disposal", res["disposal_id"])
    assert len(gl) == 4
    by = _by_account(gl)
    assert by[genv["accumulated_depreciation_account_id"]] == (accum, D("0"))
    assert by[genv["asset_account_id"]] == (D("0"), D("5000.00"))
    assert by[genv["bank_account_id"]] == (D("6000.00"), D("0"))
    assert by[genv["gain_account_id"]] == (D("0"), gain)
    assert genv["depreciation_account_id"] not in by, (
        "sale proceeds and the gain both used to be posted here")
    assert sum((D(g["debit"]) for g in gl), D("0")) == \
        sum((D(g["credit"]) for g in gl), D("0")) == D("6083.33")
    assert all(g["cost_center_id"] == genv["cost_center_id"] for g in gl)
    assert res["proceeds_account_id"] == genv["bank_account_id"]
    assert res["gain_loss_account_id"] == genv["gain_account_id"]
    assert D(_asset(conn, genv["asset_id"])["current_book_value"]) == D("0")


def test_sale_at_a_loss_puts_the_money_in_the_bank_and_the_loss_in_expense(conn, genv):
    """Same asset sold for 1,000.00: bank +1,000.00, loss 3,916.67."""
    accum, book = _depreciate_once(conn, genv)
    loss = book - D("1000.00")

    res = _dispose(conn, genv, disposal_method="sale", sale_amount="1000.00",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=genv["loss_account_id"])
    assert is_ok(res), res
    assert D(res["gain_or_loss"]) == -loss == D("-3916.67")
    _assert_disposal_persisted(conn, genv, res, sale=D("1000.00"), book=book,
                               gain_or_loss=-loss, method="sale",
                               asset_status="sold")

    gl = _gl(conn, "asset_disposal", res["disposal_id"])
    assert len(gl) == 4
    by = _by_account(gl)
    assert by[genv["accumulated_depreciation_account_id"]] == (accum, D("0"))
    assert by[genv["asset_account_id"]] == (D("0"), D("5000.00"))
    assert by[genv["bank_account_id"]] == (D("1000.00"), D("0"))
    assert by[genv["loss_account_id"]] == (loss, D("0"))
    assert genv["depreciation_account_id"] not in by
    assert sum((D(g["debit"]) for g in gl), D("0")) == \
        sum((D(g["credit"]) for g in gl), D("0")) == D("5000.00")


def test_sale_at_exactly_book_value_moves_no_profit_at_all(conn, genv):
    """gain_or_loss == 0: three legs, no P&L account touched, no gain/loss
    account required. Before M61 this posted the full carrying amount into
    depreciation expense."""
    accum, book = _depreciate_once(conn, genv)

    res = _dispose(conn, genv, disposal_method="sale", sale_amount=str(book),
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=None)
    assert is_ok(res), res
    assert D(res["gain_or_loss"]) == D("0")
    _assert_disposal_persisted(conn, genv, res, sale=book, book=book,
                               gain_or_loss=D("0"), method="sale",
                               asset_status="sold")

    gl = _gl(conn, "asset_disposal", res["disposal_id"])
    assert len(gl) == 3
    by = _by_account(gl)
    assert by[genv["accumulated_depreciation_account_id"]] == (accum, D("0"))
    assert by[genv["asset_account_id"]] == (D("0"), D("5000.00"))
    assert by[genv["bank_account_id"]] == (book, D("0"))
    roots = _root_types(conn, list(by))
    assert set(roots.values()) == {"asset"}, (
        "a disposal at book value has no profit effect; no income or expense "
        f"account may appear, got {roots}")
    assert res["gain_loss_account_id"] is None


def test_a_fully_depreciated_scrap_needs_neither_account(conn, genv):
    """Nothing left to lose and nothing received: DR accumulated / CR asset."""
    conn.execute("UPDATE asset SET accumulated_depreciation = '5000.00', "
                 "current_book_value = '0' WHERE id = ?", (genv["asset_id"],))
    conn.commit()

    res = _dispose(conn, genv, disposal_method="scrap", gain_loss_account_id=None)
    assert is_ok(res), res
    assert D(res["gain_or_loss"]) == D("0")
    _assert_disposal_persisted(conn, genv, res, sale=D("0"), book=D("0"),
                               gain_or_loss=D("0"), method="scrap",
                               asset_status="scrapped")

    gl = _gl(conn, "asset_disposal", res["disposal_id"])
    assert len(gl) == 2
    by = _by_account(gl)
    assert by[genv["accumulated_depreciation_account_id"]] == (D("5000.00"), D("0"))
    assert by[genv["asset_account_id"]] == (D("0"), D("5000.00"))
    assert res["proceeds_account_id"] is None and res["gain_loss_account_id"] is None


def _audit_rows(conn, action):
    return conn.execute(
        "SELECT skill, action, entity_type, entity_id, old_values, new_values, "
        "description FROM audit_log WHERE action = ? ORDER BY timestamp",
        (action,)).fetchall()


def test_a_disposal_is_recorded_in_the_audit_trail_with_both_accounts(conn, genv):
    """M61 rider (2026-08-12): M61 claimed both disposal accounts are answerable
    "from the payload and the audit trail". Only the payload half was pinned —
    deleting the whole `audit()` call, or blanking both account IDs out of
    `new_values`, left every assets test green, because no assets test read
    `audit_log` at all. This one does: a disposal that reached the bank and the
    gain account has to say so in the trail, not just in the reply."""
    _, book = _depreciate_once(conn, genv)

    res = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   buyer_details="Wayne Enterprises",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_ok(res), res

    rows = _audit_rows(conn, "dispose-asset")
    assert len(rows) == 1, "one disposal must leave exactly one audit row"
    row = rows[0]
    assert row["skill"] == "erpclaw-assets"
    assert row["entity_type"] == "asset"
    assert row["entity_id"] == genv["asset_id"]
    assert row["description"], "the trail entry must be readable, not blank"

    new = json.loads(row["new_values"])
    # The two accounts M61 added. Without them the trail cannot answer where the
    # money landed or where the gain was recognized.
    assert new["proceeds_account_id"] == genv["bank_account_id"]
    assert new["gain_loss_account_id"] == genv["gain_account_id"]
    assert new["proceeds_account_id"] == res["proceeds_account_id"]
    assert new["gain_loss_account_id"] == res["gain_loss_account_id"]
    # …and the money, agreeing with the payload to the cent.
    assert D(new["sale_amount"]) == D("6000.00") == D(res["sale_amount"])
    assert D(new["gain_or_loss"]) == D("1083.33") == D(res["gain_or_loss"])
    assert new["disposal_method"] == "sale"
    assert new["status"] == "sold" == res["new_status"]

    old = json.loads(row["old_values"])
    assert old["status"] == "in_use"
    assert D(old["current_book_value"]) == book


def test_disposing_twice_is_refused_and_posts_no_second_set_of_gl(conn, genv):
    first = _dispose(conn, genv, disposal_method="scrap")
    assert is_ok(first), first
    gl_before = len(_gl(conn, "asset_disposal"))

    again = _dispose(conn, genv, disposal_method="scrap",
                     disposal_date="2026-10-31")
    assert is_error(again)
    assert "already" in _msg(again)
    assert len(_gl(conn, "asset_disposal")) == gl_before
    assert conn.execute("SELECT COUNT(*) FROM asset_disposal").fetchone()[0] == 1


def test_an_unknown_disposal_method_is_refused(conn, genv):
    bad = _dispose(conn, genv, disposal_method="donate")
    assert is_error(bad)
    assert "Invalid disposal method" in _msg(bad)
    _nothing_landed(conn, genv)


# ── dispose-asset: the account gates (M61) ───────────────────────────────────

def test_a_sale_without_a_proceeds_account_is_refused_and_lands_nothing(conn, genv):
    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=None,
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_error(bad)
    assert "--proceeds-account-id is required" in _msg(bad)
    _nothing_landed(conn, genv)


def test_a_disposal_with_a_gain_or_loss_needs_the_pl_account(conn, genv):
    bad = _dispose(conn, genv, disposal_method="scrap", gain_loss_account_id=None)
    assert is_error(bad)
    assert "--gain-loss-account-id is required" in _msg(bad)
    assert "loss of 5000.00" in _msg(bad)
    _nothing_landed(conn, genv)


def test_proceeds_may_not_be_posted_to_a_pl_account(conn, genv):
    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=genv["depreciation_account_id"],
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_error(bad)
    assert "root_type 'expense'" in _msg(bad)
    _nothing_landed(conn, genv)


def test_the_gain_may_not_be_hidden_in_a_balance_sheet_account(conn, genv):
    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=genv["bank_account_id"])
    assert is_error(bad)
    assert "root_type 'asset'" in _msg(bad)
    assert "income or expense" in _msg(bad)
    _nothing_landed(conn, genv)


def test_proceeds_may_not_be_posted_to_the_assets_own_accounts(conn, genv):
    for account_id, label in ((genv["asset_account_id"], "asset account"),
                              (genv["accumulated_depreciation_account_id"],
                               "accumulated-depreciation account")):
        bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                       proceeds_account_id=account_id,
                       gain_loss_account_id=genv["gain_account_id"])
        assert is_error(bad), label
        assert "must differ from" in _msg(bad)
        _nothing_landed(conn, genv)


def test_a_receivable_proceeds_account_is_refused_with_a_steer(conn, genv):
    """A credit sale needs a customer party (GL step 5) and dispose-asset has no
    customer surface, so it is refused by name rather than by a raw step-5 error."""
    ar = conn.execute("SELECT id FROM account WHERE company_id = ? AND "
                      "account_type = 'receivable' LIMIT 1",
                      (genv["company_id"],)).fetchone()
    if ar is None:
        from assets_helpers import seed_account
        ar_id = seed_account(conn, genv["company_id"], "Debtors",
                             "receivable", "asset")
    else:
        ar_id = ar["id"]

    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=ar_id,
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_error(bad)
    assert "receivable account" in _msg(bad)
    _nothing_landed(conn, genv)


def test_a_group_account_is_refused_before_gl_validation_sees_it(conn, genv):
    conn.execute("UPDATE account SET is_group = 1 WHERE id = ?",
                 (genv["bank_account_id"],))
    conn.commit()
    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_error(bad)
    assert "group account" in _msg(bad)
    _nothing_landed(conn, genv)


def test_an_unknown_proceeds_account_is_refused(conn, genv):
    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id="no-such-account",
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_error(bad)
    assert "not found" in _msg(bad)
    _nothing_landed(conn, genv)


def test_a_negative_sale_amount_is_refused(conn, genv):
    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="-100.00",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_error(bad)
    assert "cannot be negative" in _msg(bad)
    _nothing_landed(conn, genv)


# ── dispose-asset: the account_type gates (M91, tightened by M94) ────────────
#
# M61 gated both legs on root_type alone. Its merge-QA pass proved that too
# loose in both directions, and Nik ruled TIGHT on 2026-08-12: the gates read
# account_type now. Plan home: planning/pending_items.md row M91; measurements in
# planning/simlogs/m91_SIM_2026-08-12.md.
#
# M91 could only allow ('revenue','expense') on the gain/loss leg because the
# registry carried no disposal class at all, which left its own residual: a gain
# credited to plain Sales Revenue was still accepted. M94 registered
# `disposal_gain_loss`, reclassified 4220 / 5340 in the shipped chart, and
# tightened this leg to it — on an install that knows the type. The tests below
# that name `revenue` or `expense` were re-pointed by M94; the ones that pin
# refusals still refuse, more of them than before.


def _acct(conn, genv, name, account_type, root_type):
    from assets_helpers import seed_account
    return seed_account(conn, genv["company_id"], name, account_type, root_type)


def test_the_gain_may_not_be_credited_to_cost_of_goods_sold(conn, genv):
    """M91 finding C, driven: a 1,083.33 gain credited to COGS used to be
    accepted. The pair balances and net income is right, so nothing that checks
    totals complains — but our own profitability report then reads cost of goods
    sold as -1,083.33 and the gain never reaches the revenue line."""
    _, book = _depreciate_once(conn, genv)
    cogs = _acct(conn, genv, "COGS - Materials", "cost_of_goods_sold", "expense")

    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=cogs)
    assert is_error(bad)
    assert "'cost_of_goods_sold' account" in _msg(bad)
    assert "gain/loss on disposal account" in _msg(bad)
    _nothing_landed(conn, genv, book)


def test_proceeds_may_not_be_posted_to_an_unrelated_fixed_asset_account(conn, genv):
    """M91 finding D, driven: 6,000.00 of proceeds posted to a vehicle account
    used to be accepted, because root_type='asset' is all the old gate read. The
    money never reaches cash, and `cash-flow` (which defines cash as
    account_type IN ('bank','cash')) reports 0.00 for a 6,000.00 sale."""
    _, book = _depreciate_once(conn, genv)
    vehicles = _acct(conn, genv, "Vehicles", "fixed_asset", "asset")

    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=vehicles,
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_error(bad)
    assert "'fixed_asset' account" in _msg(bad)
    assert "cash or bank account" in _msg(bad)
    _nothing_landed(conn, genv, book)


@pytest.mark.parametrize("account_type", [
    "stock", "accumulated_depreciation", "asset_received_not_billed",
    "capital_work_in_progress", "goodwill",
])
def test_proceeds_may_only_be_cash_or_bank(conn, genv, account_type):
    """Every other asset-side account_type in the registry is refused, not just
    the fixed-asset one the finding happened to use."""
    _, book = _depreciate_once(conn, genv)
    other = _acct(conn, genv, f"Some {account_type}", account_type, "asset")

    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=other,
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_error(bad), account_type
    assert f"'{account_type}' account" in _msg(bad)
    _nothing_landed(conn, genv, book)


@pytest.mark.parametrize("account_type", [
    "cost_of_goods_sold", "depreciation", "exchange_gain_loss", "rounding",
    "stock_adjustment",
])
def test_the_gain_or_loss_may_not_go_to_another_machinerys_pl_account(
        conn, genv, account_type):
    """Each of these is a P&L account belonging to a different mechanism —
    inventory costing, depreciation, ASC 830 FX, rounding, stock adjustment. A
    disposal gain or loss parked in one of them is money hidden inside another
    subsystem's line, and an FX revaluation run would sweep the third."""
    _, book = _depreciate_once(conn, genv)
    other = _acct(conn, genv, f"Some {account_type}", account_type, "expense")

    bad = _dispose(conn, genv, disposal_method="scrap",
                   gain_loss_account_id=other)
    assert is_error(bad), account_type
    assert f"'{account_type}' account" in _msg(bad)
    _nothing_landed(conn, genv, book)


def test_the_gain_or_loss_may_not_go_to_the_categorys_depreciation_account(
        conn, genv):
    """The M61 defect, refused by identity rather than by type. The fixture types
    its depreciation account plainly 'expense' (a legitimate chart does too), so
    the account_type allowlist alone would let M61 straight back in."""
    _, book = _depreciate_once(conn, genv)

    bad = _dispose(conn, genv, disposal_method="scrap",
                   gain_loss_account_id=genv["depreciation_account_id"])
    assert is_error(bad)
    assert "depreciation account" in _msg(bad)
    assert "not depreciation" in _msg(bad)
    _nothing_landed(conn, genv, book)


def test_an_account_with_no_account_type_is_refused_on_both_legs(conn, genv):
    """`account_type` is nullable and add-account leaves it unset if you do not
    pass one. Refusing is deliberate (M91): root_type alone is exactly the
    information this row proved insufficient. Pinned so the choice is visible and
    cannot drift silently — and the steer must now name BOTH remedies, because
    M94 made retyping an existing account possible (update-account
    --account-type), which is what M91's steer had to say was impossible."""
    _, book = _depreciate_once(conn, genv)
    untyped_asset = _acct(conn, genv, "Untyped Asset", None, "asset")
    untyped_income = _acct(conn, genv, "Untyped Income", None, "income")

    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=untyped_asset,
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_error(bad)
    assert "has no account_type set" in _msg(bad)
    assert "add-account --account-type bank" in bad.get("suggestion", "")
    assert "update-account --account-type bank" in bad.get("suggestion", "")
    _nothing_landed(conn, genv, book)

    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=untyped_income)
    assert is_error(bad)
    assert "has no account_type set" in _msg(bad)
    assert "add-account --account-type disposal_gain_loss" in bad.get("suggestion", "")
    assert "update-account --account-type disposal_gain_loss" in bad.get("suggestion", "")
    _nothing_landed(conn, genv, book)


def test_a_cash_account_takes_the_proceeds_and_the_money_is_exact(conn, genv):
    """The tight gate must not cost the legitimate cases anything. Petty cash is
    account_type='cash' (the shipped chart's 1111); every existing value test
    uses a 'bank' account, so this pins the other accepted type end to end."""
    accum, book = _depreciate_once(conn, genv)
    petty = _acct(conn, genv, "Petty Cash", "cash", "asset")
    gain = D("6000.00") - book

    res = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=petty,
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_ok(res), res
    assert D(res["gain_or_loss"]) == gain == D("1083.33")

    gl = _gl(conn, "asset_disposal", res["disposal_id"])
    assert len(gl) == 4
    by = _by_account(gl)
    assert by[genv["accumulated_depreciation_account_id"]] == (accum, D("0"))
    assert by[genv["asset_account_id"]] == (D("0"), D("5000.00"))
    assert by[petty] == (D("6000.00"), D("0"))
    assert by[genv["gain_account_id"]] == (D("0"), D("1083.33"))
    assert genv["depreciation_account_id"] not in by
    assert sum((D(g["debit"]) for g in gl), D("0")) == \
        sum((D(g["credit"]) for g in gl), D("0")) == D("6083.33")


def test_one_combined_disposal_account_may_carry_the_loss_too(conn, genv):
    """A single "Gain/(Loss) on Disposal" income account taking debits for losses
    is a legitimate chart, and the M61 SIM said so out loud. The account_type gate
    must not quietly outlaw it, and this is why M94 registered ONE type rather
    than an other_income/other_expense pair: a pair would force this shop to pick
    a side. The account is debited for a LOSS at the exact carrying amount."""
    accum, book = _depreciate_once(conn, genv)
    combined = _acct(conn, genv, "Gain/(Loss) on Disposal",
                     "disposal_gain_loss", "income")

    res = _dispose(conn, genv, disposal_method="scrap",
                   gain_loss_account_id=combined)
    assert is_ok(res), res
    assert D(res["gain_or_loss"]) == -book == D("-4916.67")

    gl = _gl(conn, "asset_disposal", res["disposal_id"])
    assert len(gl) == 3
    by = _by_account(gl)
    assert by[genv["accumulated_depreciation_account_id"]] == (accum, D("0"))
    assert by[genv["asset_account_id"]] == (D("0"), D("5000.00"))
    assert by[combined] == (D("4916.67"), D("0"))
    assert sum((D(g["debit"]) for g in gl), D("0")) == \
        sum((D(g["credit"]) for g in gl), D("0")) == D("5000.00")


# ── dispose-asset: the disposal gain/loss type (M94) ─────────────────────────
#
# M91's residual, closed. `disposal_gain_loss` is now a registered account type
# and the shipped chart's 4220 / 5340 carry it, so the gain/loss leg can be
# pinned to the disposal line instead of merely excluded from other machinery.
# Plan home: planning/pending_items.md row M94; SIM: planning/simlogs/m94_SIM_2026-08-12.md.


def _deregister_disposal_type(conn):
    """Make this install look like one that never ran foundation migration 035.

    Deleting the registry row is what an un-migrated install actually looks like
    (the row is only ever created by init_db's seed or by migration 035), so this
    reproduces the skew rather than simulating it.
    """
    conn.execute("DELETE FROM account_type_registry WHERE account_type = ?",
                 ("disposal_gain_loss",))
    conn.commit()


def test_the_gain_may_not_be_credited_to_plain_sales_revenue(conn, genv):
    """M94's headline, and exactly what M91 could not refuse. `4110 Sales Revenue`
    and `4220 Gain on Asset Disposal` both typed `revenue` in the shipped chart, so
    M91's allowlist accepted the sales line for a disposal gain: revenue overstated
    by the gain, the disposal invisible on its own line, every total still correct
    so nothing that checks balances complains."""
    _, book = _depreciate_once(conn, genv)
    sales = _acct(conn, genv, "Sales Revenue", "revenue", "income")

    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=sales)
    assert is_error(bad)
    assert "'revenue' account" in _msg(bad)
    assert "disposal_gain_loss" in _msg(bad)
    _nothing_landed(conn, genv, book)


def test_the_loss_may_not_be_debited_to_rent_expense(conn, genv):
    """The mirror. `5220 Rent Expense` and `5340 Loss on Asset Disposal` both typed
    `expense`, so M91's allowlist accepted rent for a disposal loss."""
    _, book = _depreciate_once(conn, genv)
    rent = _acct(conn, genv, "Rent Expense", "expense", "expense")

    bad = _dispose(conn, genv, disposal_method="scrap",
                   gain_loss_account_id=rent)
    assert is_error(bad)
    assert "'expense' account" in _msg(bad)
    assert "disposal_gain_loss" in _msg(bad)
    _nothing_landed(conn, genv, book)


def test_the_disposal_typed_account_takes_the_gain_and_the_money_is_exact(conn, genv):
    """The tight rule must cost the correct chart nothing. The fixture's gain
    account carries the type the shipped 4220 carries, and every leg is pinned to
    the cent so a future widening cannot pass by moving money instead of refusing."""
    accum, book = _depreciate_once(conn, genv)
    assert conn.execute("SELECT account_type FROM account WHERE id = ?",
                        (genv["gain_account_id"],)).fetchone()[0] == "disposal_gain_loss"

    res = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_ok(res), res
    assert D(res["gain_or_loss"]) == D("6000.00") - book == D("1083.33")

    gl = _gl(conn, "asset_disposal", res["disposal_id"])
    assert len(gl) == 4
    by = _by_account(gl)
    assert by[genv["accumulated_depreciation_account_id"]] == (accum, D("0"))
    assert by[genv["asset_account_id"]] == (D("0"), D("5000.00"))
    assert by[genv["bank_account_id"]] == (D("6000.00"), D("0"))
    assert by[genv["gain_account_id"]] == (D("0"), D("1083.33"))
    assert sum((D(g["debit"]) for g in gl), D("0")) == \
        sum((D(g["credit"]) for g in gl), D("0")) == D("6083.33")


def test_an_install_without_the_registered_type_keeps_the_pre_m94_rule(conn, genv):
    """The compatibility decision, driven rather than described.

    erpclaw-ops is an addon and module_manager runs foundation migrations only on
    the update-foundation path, so an install CAN hold this code with a chart whose
    4220 is still `revenue`. Shipping the tight allowlist unconditionally would
    refuse every disposal there. With the type absent from the registry the gate
    falls back to M91's allowlist and a `revenue`-typed gain account posts, exactly
    as it does on main today."""
    accum, book = _depreciate_once(conn, genv)
    _deregister_disposal_type(conn)
    legacy_gain = _acct(conn, genv, "Gain on Asset Disposal", "revenue", "income")

    res = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=legacy_gain)
    assert is_ok(res), res
    by = _by_account(_gl(conn, "asset_disposal", res["disposal_id"]))
    assert by[legacy_gain] == (D("0"), D("1083.33"))
    assert by[genv["bank_account_id"]] == (D("6000.00"), D("0"))
    assert by[genv["accumulated_depreciation_account_id"]] == (accum, D("0"))
    assert by[genv["asset_account_id"]] == (D("0"), D("5000.00"))


def test_the_pre_m94_fallback_still_refuses_everything_m91_refused(conn, genv):
    """The fallback is M91's rule, not "no rule". Cost of goods sold stays refused
    on an un-migrated install, so falling back cannot be used to smuggle finding C
    back in."""
    _, book = _depreciate_once(conn, genv)
    _deregister_disposal_type(conn)
    cogs = _acct(conn, genv, "COGS - Materials", "cost_of_goods_sold", "expense")

    bad = _dispose(conn, genv, disposal_method="scrap", gain_loss_account_id=cogs)
    assert is_error(bad)
    assert "'cost_of_goods_sold' account" in _msg(bad)
    _nothing_landed(conn, genv, book)


def test_the_fallback_refusal_tells_the_operator_the_foundation_is_behind(conn, genv):
    """An operator debugging a refused account on an un-migrated install has to be
    able to find out why the rule is the wider one. The steer says so; on a
    migrated install it must NOT, or every refusal would carry a false instruction."""
    _, book = _depreciate_once(conn, genv)
    _deregister_disposal_type(conn)
    cogs = _acct(conn, genv, "COGS - Materials", "cost_of_goods_sold", "expense")

    bad = _dispose(conn, genv, disposal_method="scrap", gain_loss_account_id=cogs)
    assert is_error(bad)
    assert "has not registered" in bad.get("suggestion", "")
    assert "update the erpclaw foundation" in bad.get("suggestion", "")

    # Same refusal on a registered install carries no such line.
    conn.execute("INSERT INTO account_type_registry "
                 "(account_type, skill_name, label, is_active) VALUES (?, ?, ?, 1)",
                 ("disposal_gain_loss", "erpclaw-assets", "Disposal Gain/Loss"))
    conn.commit()
    bad = _dispose(conn, genv, disposal_method="scrap", gain_loss_account_id=cogs)
    assert is_error(bad)
    assert "has not registered" not in bad.get("suggestion", "")
    _nothing_landed(conn, genv, book)


def test_a_deactivated_disposal_type_is_treated_as_absent(conn, genv):
    """`account_type_registry.is_active` is the switch `deactivate-account-type`
    flips, and `add-account` refuses an inactive type. The gate must read it the
    same way, or an install could be required to use a type it cannot create."""
    _, book = _depreciate_once(conn, genv)
    conn.execute("UPDATE account_type_registry SET is_active = 0 "
                 "WHERE account_type = 'disposal_gain_loss'")
    conn.commit()
    legacy_gain = _acct(conn, genv, "Gain on Asset Disposal", "revenue", "income")

    res = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=legacy_gain)
    assert is_ok(res), res


# ── the steer must work on the install it is handed to (M94 rider R3) ────────
#
# The refusal named `update-account --account-type ...` unconditionally. That
# action gained --account-type in the FOUNDATION, in the same M94 change that
# registers `disposal_gain_loss`; this module is an addon and can be updated
# without it. On a stale-foundation install the steer's own instruction returns
# {"status":"error","message":"No fields to update"}, or `ok` with the type
# unchanged when a --name rides along. The gate already asks the install's
# registry how strict to be; it now asks the same row which steer is true.


def test_the_steer_offers_retyping_only_where_the_install_can_retype(conn, genv):
    """The registered install: update-account IS there, so naming it is correct."""
    _, book = _depreciate_once(conn, genv)
    sales = _acct(conn, genv, "Sales Revenue", "revenue", "income")

    bad = _dispose(conn, genv, disposal_method="scrap", gain_loss_account_id=sales)
    assert is_error(bad)
    steer = bad.get("suggestion", "")
    assert "add-account --account-type disposal_gain_loss" in steer
    assert "update-account --account-type disposal_gain_loss" in steer
    assert "not available on this install" not in steer
    _nothing_landed(conn, genv, book)


def test_a_stale_foundation_is_never_told_to_run_update_account(conn, genv):
    """The population the fallback exists for. Their update-account drops
    --account-type, so the old steer sent them to an action that reports success
    and changes nothing. It must name what works here instead."""
    _, book = _depreciate_once(conn, genv)
    _deregister_disposal_type(conn)
    cogs = _acct(conn, genv, "COGS - Materials", "cost_of_goods_sold", "expense")

    bad = _dispose(conn, genv, disposal_method="scrap", gain_loss_account_id=cogs)
    assert is_error(bad)
    steer = bad.get("suggestion", "")
    assert "update-account --account-type" not in steer, steer
    assert "not available on this install" in steer
    assert "report success and change nothing" in steer
    # and it still names a remedy that DOES work there
    assert "add-account --account-type revenue" in steer
    assert "update the erpclaw foundation" in steer
    _nothing_landed(conn, genv, book)


def test_the_proceeds_leg_asks_the_same_question(conn, genv):
    """Both legs steer to retyping, so both must ask. A correct gain/loss steer
    beside a broken proceeds steer would be the same defect, half fixed."""
    _, book = _depreciate_once(conn, genv)
    _deregister_disposal_type(conn)
    untyped_asset = _acct(conn, genv, "Untyped Asset", None, "asset")

    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=untyped_asset,
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_error(bad)
    steer = bad.get("suggestion", "")
    assert "add-account --account-type bank" in steer
    assert "update-account --account-type" not in steer, steer
    assert "not available on this install" in steer
    _nothing_landed(conn, genv, book)


def test_a_deactivated_type_still_leaves_retyping_on_the_table(conn, genv):
    """The two questions are genuinely different and this is where they part. An
    operator who ran deactivate-account-type has a POST-M94 foundation: the wider
    allowlist is right, and update-account works, so the steer must still offer it
    — for `revenue`, the type this install accepts."""
    _, book = _depreciate_once(conn, genv)
    conn.execute("UPDATE account_type_registry SET is_active = 0 "
                 "WHERE account_type = 'disposal_gain_loss'")
    conn.commit()
    cogs = _acct(conn, genv, "COGS - Materials", "cost_of_goods_sold", "expense")

    bad = _dispose(conn, genv, disposal_method="scrap", gain_loss_account_id=cogs)
    assert is_error(bad)
    steer = bad.get("suggestion", "")
    assert "update-account --account-type revenue" in steer, steer
    assert "not available on this install" not in steer
    _nothing_landed(conn, genv, book)


def test_the_shipped_chart_types_both_disposal_accounts_for_this_gate(conn, genv):
    """The gate and the chart we ship must agree, or every install refuses its own
    designated disposal accounts on day one. Read from the chart asset itself, not
    from a copy of the expectation."""
    import json
    import os
    chart = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))),
        "erpclaw", "scripts", "erpclaw-gl", "assets", "charts", "us_gaap.json")
    rows = {r["account_number"]: r for r in json.load(open(chart))}
    assert rows["4220"]["name"] == "Gain on Asset Disposal"
    assert rows["4220"]["account_type"] == "disposal_gain_loss"
    assert rows["4220"]["root_type"] == "income"
    assert rows["5340"]["name"] == "Loss on Asset Disposal"
    assert rows["5340"]["account_type"] == "disposal_gain_loss"
    assert rows["5340"]["root_type"] == "expense"
    # And the type the chart uses is one this gate accepts, read off the module
    # under test rather than restated here.
    assert rows["4220"]["account_type"] in M.GAIN_LOSS_ACCOUNT_TYPES
    assert rows["5340"]["account_type"] in M.GAIN_LOSS_ACCOUNT_TYPES
    assert M.GAIN_LOSS_ACCOUNT_TYPES == (M.DISPOSAL_GAIN_LOSS_ACCOUNT_TYPE,)


def test_a_failed_gl_posting_leaves_no_disposal_row_behind(conn, genv):
    """The disposal row is written before the GL post. If the post is rejected,
    the whole call must roll back on THIS connection, not just on process exit."""
    conn.execute("UPDATE account SET is_group = 1 WHERE id = ?",
                 (genv["asset_account_id"],))
    conn.commit()

    bad = _dispose(conn, genv, disposal_method="sale", sale_amount="6000.00",
                   proceeds_account_id=genv["bank_account_id"],
                   gain_loss_account_id=genv["gain_account_id"])
    assert is_error(bad)
    assert "GL posting failed" in _msg(bad)
    assert "Step 2" in _msg(bad)
    _nothing_landed(conn, genv)
