"""Reserve idempotency and bulk atomicity for Shopify GL posting.

A payout's reserve hold posts once and its release posts once: the payout
carries each posting's journal entry id, and a repeat is refused naming that
journal entry. Every posting action writes its voucher and all of its ledger
rows in one transaction or writes nothing at all. Bulk posting counts an
object only when its posting succeeded, lists every failure with its reason,
and never commits any part of a failed posting.
"""
import sys
import uuid
from decimal import Decimal

from shopify_test_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_shopify_order, seed_shopify_refund, seed_shopify_payout,
    seed_shopify_dispute,
)
from erpclaw_lib.query import Q, P, Table, Field, insert_row

mod = load_db_query()
# The module whose globals post_reserve_gl resolves insert_gl_entries from.
GL = sys.modules[mod.shopify_post_reserve_gl.__module__]

ISSUED_2025 = "2025-06-30T12:00:00Z"
MISSING_COLUMNS_MESSAGE = (
    "Reserve posting needs the shopify payout reserve columns; "
    "run the module migrations (update the module) and retry"
)
_DROP_RESERVE_COLUMNS = (
    "ALTER TABLE shopify_payout DROP COLUMN reserve_hold_voucher_id",
    "ALTER TABLE shopify_payout DROP COLUMN reserve_release_voucher_id",
)


def _seed_fiscal_year(conn, company_id, year):
    fy_id = str(uuid.uuid4())
    sql, _ = insert_row("fiscal_year", {
        "id": P(), "name": P(), "start_date": P(),
        "end_date": P(), "is_closed": P(), "company_id": P(),
    })
    conn.execute(sql, (
        fy_id, f"FY-{year}-{fy_id[:6]}",
        f"{year}-01-01", f"{year}-12-31", 0, company_id,
    ))
    conn.commit()


def _payout(conn, env, reserved):
    payout_id = seed_shopify_payout(
        conn, env["shopify_account_id"], env["company_id"],
        gross="1000.00", fee="29.00", reserved_funds_gross=reserved,
    )
    t = Table("shopify_payout")
    conn.execute(
        Q.update(t).set(t.issued_at, P()).where(t.id == P()).get_sql(),
        (ISSUED_2025, payout_id),
    )
    conn.commit()
    return payout_id


def _counts(conn):
    out = []
    for name in ("journal_entry", "gl_entry", "audit_log"):
        t = Table(name)
        rows = conn.execute(Q.from_(t).select(t.star).get_sql()).fetchall()
        out.append(len(rows))
    return tuple(out)


def _reserve_row(conn, payout_id):
    t = Table("shopify_payout")
    sql = (Q.from_(t).select(
        t.reserve_hold_voucher_id, t.reserve_release_voucher_id,
        t.gl_status, t.gl_voucher_id,
    ).where(t.id == P()).get_sql())
    return conn.execute(sql, (payout_id,)).fetchone()


def _all_legs(conn):
    t = Table("gl_entry")
    sql = Q.from_(t).select(
        t.account_id, t.debit, t.credit,
        t.voucher_type, t.voucher_id, t.is_cancelled,
    ).get_sql()
    return [tuple(r) for r in conn.execute(sql).fetchall()]


def _legs_of(conn, voucher_id):
    t = Table("gl_entry")
    sql = (Q.from_(t).select(
        t.account_id, t.debit, t.credit, t.cost_center_id, t.posting_date,
    ).where(t.voucher_id == P()).get_sql())
    return conn.execute(sql, (voucher_id,)).fetchall()


def _post_reserve(conn, payout_id, reserve_type):
    return call_action(mod.shopify_post_reserve_gl, conn, ns(
        shopify_payout_id=payout_id, reserve_type=reserve_type))


def _call(fn, conn, args):
    """Drive an action; an escaping ValueError is recorded, not raised, so the
    row-count assertions are what report it."""
    try:
        return call_action(fn, conn, args)
    except ValueError as exc:
        return {"status": "raised", "message": str(exc)}


def _boom(*args, **kwargs):
    raise ValueError("planted GL failure")


def _reserve_debit_sum(conn, account_id):
    return sum(Decimal(debit) for acct, debit, _, _, _, _ in _all_legs(conn)
               if acct == account_id)


def test_second_hold_is_refused_and_posts_nothing(conn, env):
    acct = env["shopify_account"]
    _seed_fiscal_year(conn, env["company_id"], 2025)
    payout_id = _payout(conn, env, "250.00")
    before = _counts(conn)

    first = _post_reserve(conn, payout_id, "hold")
    assert is_ok(first), first
    je = first["journal_entry_id"]

    second = _post_reserve(conn, payout_id, "hold")
    assert is_error(second), {
        "second": second,
        "reserve_debit_sum": str(
            _reserve_debit_sum(conn, acct["reserve_account_id"])),
    }
    assert second["message"] == (
        f"Reserve hold for payout {payout_id} "
        f"is already posted as journal entry {je}")

    row = _reserve_row(conn, payout_id)
    assert row["reserve_hold_voucher_id"] == je
    assert row["reserve_release_voucher_id"] is None
    assert (row["gl_status"], row["gl_voucher_id"]) == ("pending", None)

    assert sorted(_all_legs(conn)) == sorted([
        (acct["reserve_account_id"], "250.00", "0.00",
         "journal_entry", je, 0),
        (acct["clearing_account_id"], "0.00", "250.00",
         "journal_entry", je, 0),
    ])
    assert _counts(conn) == (before[0] + 1, before[1] + 2, before[2] + 1)

    a = Table("audit_log")
    audits = conn.execute(
        Q.from_(a).select(a.action).where(a.action == P()).get_sql(),
        ("shopify-post-reserve-gl",)).fetchall()
    assert len(audits) == 1


def test_second_release_is_refused(conn, env):
    acct = env["shopify_account"]
    _seed_fiscal_year(conn, env["company_id"], 2025)
    payout_id = _payout(conn, env, "87.35")

    first = _post_reserve(conn, payout_id, "release")
    assert is_ok(first), first
    je = first["journal_entry_id"]

    second = _post_reserve(conn, payout_id, "release")
    assert is_error(second), second
    assert second["message"] == (
        f"Reserve release for payout {payout_id} "
        f"is already posted as journal entry {je}")

    legs = _legs_of(conn, je)
    assert sorted((r["debit"], r["credit"]) for r in legs) == sorted([
        ("87.35", "0.00"), ("0.00", "87.35"),
    ])
    by_account = {r["account_id"]: r for r in legs}
    assert (by_account[acct["clearing_account_id"]]["debit"],
            by_account[acct["clearing_account_id"]]["credit"]) == (
        "87.35", "0.00")
    assert (by_account[acct["reserve_account_id"]]["debit"],
            by_account[acct["reserve_account_id"]]["credit"]) == (
        "0.00", "87.35")


def test_hold_and_release_each_post_once(conn, env):
    acct = env["shopify_account"]
    _seed_fiscal_year(conn, env["company_id"], 2025)
    payout_id = _payout(conn, env, "400.10")

    hold = _post_reserve(conn, payout_id, "hold")
    assert is_ok(hold), hold
    release = _post_reserve(conn, payout_id, "release")
    assert is_ok(release), release
    assert hold["journal_entry_id"] != release["journal_entry_id"]

    third = _post_reserve(conn, payout_id, "hold")
    assert is_error(third), third
    assert third["message"] == (
        f"Reserve hold for payout {payout_id} is already posted "
        f"as journal entry {hold['journal_entry_id']}")

    row = _reserve_row(conn, payout_id)
    assert row["reserve_hold_voucher_id"] == hold["journal_entry_id"]
    assert row["reserve_release_voucher_id"] == release["journal_entry_id"]

    legs = _all_legs(conn)
    assert len(legs) == 4
    reserve_net = sum(Decimal(debit) - Decimal(credit)
                      for acct_id, debit, credit, _, _, _ in legs
                      if acct_id == acct["reserve_account_id"])
    clearing_net = sum(Decimal(debit) - Decimal(credit)
                       for acct_id, debit, credit, _, _, _ in legs
                       if acct_id == acct["clearing_account_id"])
    assert reserve_net == Decimal("0.00")
    assert clearing_net == Decimal("0.00")


def test_refused_hold_records_no_voucher(conn, env, monkeypatch):
    _seed_fiscal_year(conn, env["company_id"], 2025)
    payout_id = _payout(conn, env, "250.00")
    monkeypatch.setattr(GL, "insert_gl_entries", _boom)

    result = _post_reserve(conn, payout_id, "hold")

    assert is_error(result), result
    assert result["message"] == "GL posting failed: planted GL failure"
    conn.commit()
    assert _reserve_row(conn, payout_id)["reserve_hold_voucher_id"] is None

    monkeypatch.undo()
    retry = _post_reserve(conn, payout_id, "hold")
    assert is_ok(retry), retry
    assert _reserve_row(conn, payout_id)["reserve_hold_voucher_id"] == (
        retry["journal_entry_id"])


def test_each_posting_action_rolls_back_a_refused_posting(
        conn, env, monkeypatch):
    _seed_fiscal_year(conn, env["company_id"], 2025)
    order_id = seed_shopify_order(
        conn, env["shopify_account_id"], env["company_id"],
        shopify_order_id="ATOMIC-1",
        subtotal="100.00", shipping="0", tax="0", total="100.00")
    refund_id = seed_shopify_refund(
        conn, order_id, env["company_id"], refund_amount="20.00")
    payout_id = _payout(conn, env, "250.00")
    dispute_id = seed_shopify_dispute(
        conn, env["shopify_account_id"], env["company_id"],
        amount="50.00", status="needs_response")
    gift_id = seed_shopify_order(
        conn, env["shopify_account_id"], env["company_id"],
        shopify_order_id="ATOMIC-GC",
        subtotal="60.00", shipping="0", tax="0", total="60.00")
    cases = [
        ("shopify_order", order_id, mod.shopify_post_order_gl,
         ns(shopify_order_id=order_id)),
        ("shopify_refund", refund_id, mod.shopify_post_refund_gl,
         ns(shopify_refund_id=refund_id)),
        ("shopify_payout", payout_id, mod.shopify_post_payout_gl,
         ns(shopify_payout_id=payout_id)),
        ("shopify_dispute", dispute_id, mod.shopify_post_dispute_gl,
         ns(shopify_dispute_id=dispute_id)),
        ("shopify_order", gift_id, mod.shopify_post_gift_card_gl,
         ns(shopify_order_id=gift_id, gift_card_type="sold")),
    ]
    real_insert = GL.insert_gl_entries
    monkeypatch.setattr(GL, "insert_gl_entries", _boom)
    for table_name, object_id, fn, args in cases:
        before = _counts(conn)
        result = _call(fn, conn, args)
        assert is_error(result), (table_name, result)
        assert result["message"] == "GL posting failed: planted GL failure", (
            table_name, result)
        assert _counts(conn) == before, table_name
        conn.commit()
        assert _counts(conn) == before, table_name
        t = Table(table_name)
        row = conn.execute(
            Q.from_(t).select(t.gl_status).where(t.id == P()).get_sql(),
            (object_id,)).fetchone()
        assert row["gl_status"] == "pending", table_name

    monkeypatch.setattr(GL, "insert_gl_entries", real_insert)
    won_id = seed_shopify_dispute(
        conn, env["shopify_account_id"], env["company_id"],
        amount="50.00", status="needs_response")
    d = Table("shopify_dispute")
    conn.execute(
        Q.update(d).set(d.created_at, P()).where(d.id == P()).get_sql(),
        ("2025-04-10T10:00:00Z", won_id))
    conn.commit()
    first = call_action(mod.shopify_post_dispute_gl, conn, ns(
        shopify_dispute_id=won_id))
    assert is_ok(first), first
    conn.execute(
        Q.update(d).set(d.status, P()).where(d.id == P()).get_sql(),
        ("won", won_id))
    conn.commit()
    monkeypatch.setattr(GL, "reverse_gl_entries", _boom)
    before = _counts(conn)
    result = _call(mod.shopify_post_dispute_gl, conn, ns(
        shopify_dispute_id=won_id))
    assert is_error(result), result
    assert result["message"] == "GL reversal failed: planted GL failure", result
    assert _counts(conn) == before
    conn.commit()
    assert _counts(conn) == before


def test_bulk_failed_order_leaves_no_journal_and_is_not_counted(conn, env):
    _seed_fiscal_year(conn, env["company_id"], 2025)
    ok_id = seed_shopify_order(
        conn, env["shopify_account_id"], env["company_id"],
        shopify_order_id="BULK-OK",
        subtotal="100.00", shipping="0", tax="0", total="100.00")
    old_id = seed_shopify_order(
        conn, env["shopify_account_id"], env["company_id"],
        shopify_order_id="BULK-OLD",
        subtotal="100.00", shipping="0", tax="0", total="100.00")
    o = Table("shopify_order")
    conn.execute(
        Q.update(o).set(o.order_date, P()).where(o.id == P()).get_sql(),
        ("2025-03-15T10:00:00Z", ok_id))
    conn.execute(
        Q.update(o).set(o.order_date, P()).where(o.id == P()).get_sql(),
        ("2019-06-30T10:00:00Z", old_id))
    conn.commit()

    result = call_action(mod.shopify_bulk_post_gl, conn, ns(
        shopify_account_id=env["shopify_account_id"]))

    assert is_ok(result), result
    assert result["orders_posted"] == 1, result
    assert result["total_posted"] == 1, result

    j = Table("journal_entry")
    journals = conn.execute(Q.from_(j).select(j.id).get_sql()).fetchall()
    g = Table("gl_entry")
    gl_counts = {}
    for voucher in journals:
        rows = conn.execute(
            Q.from_(g).select(g.id).where(g.voucher_id == P()).get_sql(),
            (voucher["id"],)).fetchall()
        gl_counts[voucher["id"]] = len(rows)
    assert len(journals) == 1, {
        "journal_ids": [r["id"] for r in journals],
        "gl_counts": gl_counts,
    }
    assert all(n >= 1 for n in gl_counts.values()), gl_counts
    ok_row = conn.execute(
        Q.from_(o).select(o.gl_voucher_id, o.gl_status)
        .where(o.id == P()).get_sql(), (ok_id,)).fetchone()
    assert journals[0]["id"] == ok_row["gl_voucher_id"]
    assert result["failed_count"] == 1, result
    assert result["errors"] == [f"order:{old_id}"], result
    assert result["failures"] == [{
        "object": "order", "id": old_id,
        "message": ("GL posting failed: GL Validation Step 9 Failed: "
                    "No open fiscal year found for posting date 2019-06-30"),
    }], result
    legs = _legs_of(conn, ok_row["gl_voucher_id"])
    acct = env["shopify_account"]
    assert sorted(
        (r["account_id"], r["debit"], r["credit"],
         r["cost_center_id"], r["posting_date"]) for r in legs) == sorted([
            (acct["clearing_account_id"], "100.00", "0.00",
             None, "2025-03-15"),
            (acct["revenue_account_id"], "0.00", "100.00",
             env["cost_center_id"], "2025-03-15"),
        ])
    old_row = conn.execute(
        Q.from_(o).select(o.gl_status, o.gl_voucher_id)
        .where(o.id == P()).get_sql(), (old_id,)).fetchone()
    assert (old_row["gl_status"], old_row["gl_voucher_id"]) == (
        "pending", None)


def test_bulk_counts_a_refused_dispute_as_a_failure(conn, env):
    dispute_id = seed_shopify_dispute(
        conn, env["shopify_account_id"], env["company_id"],
        amount="50.00", status="won")

    result = call_action(mod.shopify_bulk_post_gl, conn, ns(
        shopify_account_id=env["shopify_account_id"]))

    assert is_ok(result), result
    assert result["disputes_posted"] == 0, result
    assert result["total_posted"] == 0, result
    assert result["failed_count"] == 1, result
    assert result["failures"] == [{
        "object": "dispute", "id": dispute_id,
        "message": "Cannot reverse dispute GL: no entries have been posted",
    }], result


def test_hold_lost_to_a_concurrent_call_writes_nothing(
        conn, env, monkeypatch):
    _seed_fiscal_year(conn, env["company_id"], 2025)
    payout_id = _payout(conn, env, "250.00")
    before = _counts(conn)
    real_insert = GL.insert_gl_entries

    def _winning(*args, **kwargs):
        ids = real_insert(*args, **kwargs)
        t = Table("shopify_payout")
        conn.execute(
            Q.update(t).set(Field("reserve_hold_voucher_id"), P())
            .where(t.id == P()).get_sql(),
            ("je-other", payout_id))
        return ids

    monkeypatch.setattr(GL, "insert_gl_entries", _winning)
    result = _post_reserve(conn, payout_id, "hold")

    assert is_error(result), result
    assert result["message"] == (
        f"Reserve hold for payout {payout_id} "
        "was posted by another call; nothing was written"), result
    conn.commit()
    assert _counts(conn) == before
    assert _reserve_row(conn, payout_id)["reserve_hold_voucher_id"] is None


def test_reserve_refuses_before_migration(conn, env):
    _seed_fiscal_year(conn, env["company_id"], 2025)
    payout_id = _payout(conn, env, "250.00")
    for statement in _DROP_RESERVE_COLUMNS:
        try:
            conn.execute(statement)
        except Exception:
            pass
        conn.commit()
    before = _counts(conn)

    result = _post_reserve(conn, payout_id, "hold")

    assert is_error(result), result
    assert result["message"] == MISSING_COLUMNS_MESSAGE, result
    assert _counts(conn) == before
