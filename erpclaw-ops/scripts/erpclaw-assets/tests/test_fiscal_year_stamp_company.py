"""Postings stamp the posting company's own fiscal year, not another company's.

Company B (decoy) is seeded FIRST with open year FY-DECOY 2025-07-01..2026-12-31,
then company A is built with its own year renamed to FY-A-2026. A company-blind
lookup returns the first row of any company (FY-DECOY); the company-scoped
lookup must return FY-A-2026 for A's postings.
"""
import os
import sys
import uuid
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from assets_helpers import (build_gl_env, call_action, is_error, is_ok,  # noqa: E402
                            load_db_query, ns, seed_company)

M = load_db_query()

D = Decimal

DECOY_YEAR = "FY-DECOY"
OWN_YEAR = "FY-A-2026"


def _u():
    return str(uuid.uuid4())


def _seed_decoy(conn):
    """Seed decoy company B and its open year FIRST (row order matters)."""
    bid = seed_company(conn, name="Decoy Co", abbr="DCY")
    conn.execute(
        "INSERT INTO fiscal_year (id, name, start_date, end_date, is_closed, company_id)"
        " VALUES (?, ?, ?, ?, 0, ?)",
        (_u(), DECOY_YEAR, "2025-07-01", "2026-12-31", bid))
    conn.commit()
    return bid


def _env_a(conn):
    """Build company A (own year renamed) with a generated schedule."""
    _seed_decoy(conn)
    env = build_gl_env(conn)
    conn.execute("UPDATE fiscal_year SET name = ? WHERE company_id = ?",
                 (OWN_YEAR, env["company_id"]))
    conn.commit()
    gen = call_action(M.generate_depreciation_schedule, conn,
                      ns(asset_id=env["asset_id"]))
    assert is_ok(gen), gen
    env["schedule"] = conn.execute(
        "SELECT id, schedule_date, depreciation_amount, status"
        " FROM depreciation_schedule WHERE asset_id = ? ORDER BY schedule_date",
        (env["asset_id"],)).fetchall()
    assert len(env["schedule"]) >= 2, "this pin needs at least two scheduled periods"
    return env


def _gl_years(conn, voucher_type, voucher_id):
    return conn.execute(
        "SELECT account_id, debit, credit, fiscal_year FROM gl_entry"
        " WHERE voucher_type = ? AND voucher_id = ? AND is_cancelled = 0",
        (voucher_type, voucher_id)).fetchall()


def test_post_depreciation_stamps_own_company_year(conn):
    env = _env_a(conn)
    row = env["schedule"][0]
    amount = D(row["depreciation_amount"])
    assert amount > 0

    res = call_action(M.post_depreciation, conn, ns(
        depreciation_schedule_id=row["id"], asset_id=None,
        posting_date=row["schedule_date"], cost_center_id=None))
    assert is_ok(res), res
    assert D(res["depreciation_amount"]) == amount

    legs = _gl_years(conn, "depreciation_entry", row["id"])
    assert len(legs) == 2
    assert all(g["fiscal_year"] == OWN_YEAR for g in legs), \
        [dict(g) for g in legs]
    assert sum((D(g["debit"]) for g in legs), D("0")) == amount
    assert sum((D(g["credit"]) for g in legs), D("0")) == amount


def test_dispose_asset_stamps_own_company_year(conn):
    env = _env_a(conn)
    res = call_action(M.dispose_asset, conn, ns(
        asset_id=env["asset_id"], disposal_date="2026-09-30",
        disposal_method="scrap", sale_amount=None, buyer_details=None,
        cost_center_id=None, proceeds_account_id=None,
        gain_loss_account_id=env["loss_account_id"]))
    assert is_ok(res), res
    assert D(res["book_value_at_disposal"]) == D("5000.00")
    assert D(res["gain_or_loss"]) == D("-5000.00")

    legs = _gl_years(conn, "asset_disposal", res["disposal_id"])
    assert len(legs) == 2
    assert all(g["fiscal_year"] == OWN_YEAR for g in legs), \
        [dict(g) for g in legs]
    assert sum((D(g["debit"]) for g in legs), D("0")) == D("5000.00")
    assert sum((D(g["credit"]) for g in legs), D("0")) == D("5000.00")


def test_run_depreciation_stamps_own_company_year(conn):
    env = _env_a(conn)
    sched = env["schedule"]
    cutoff = sched[1]["schedule_date"]
    due = [r for r in sched if r["schedule_date"] <= cutoff]

    res = call_action(M.run_depreciation, conn, ns(
        company_id=env["company_id"], posting_date=cutoff, cost_center_id=None))
    assert is_ok(res), res
    assert res["entries_posted"] == len(due)

    legs = conn.execute(
        "SELECT voucher_id, debit, credit, fiscal_year FROM gl_entry"
        " WHERE voucher_type = 'depreciation_entry' AND is_cancelled = 0").fetchall()
    assert len(legs) == 2 * len(due)
    assert {g["voucher_id"] for g in legs} == {r["id"] for r in due}
    assert all(g["fiscal_year"] == OWN_YEAR for g in legs), \
        [dict(g) for g in legs]


def test_post_depreciation_refused_without_own_company_year(conn):
    env = _env_a(conn)
    row = env["schedule"][0]
    conn.execute("DELETE FROM fiscal_year WHERE company_id = ?",
                 (env["company_id"],))
    conn.commit()

    bad = call_action(M.post_depreciation, conn, ns(
        depreciation_schedule_id=row["id"], asset_id=None,
        posting_date=row["schedule_date"], cost_center_id=None))
    assert is_error(bad), bad
    msg = bad.get("message", "") + bad.get("error", "")
    assert ("GL Validation Step 9 Failed: No open fiscal year found for posting date "
            + row["schedule_date"]) in msg, msg
    assert _gl_years(conn, "depreciation_entry", row["id"]) == []
