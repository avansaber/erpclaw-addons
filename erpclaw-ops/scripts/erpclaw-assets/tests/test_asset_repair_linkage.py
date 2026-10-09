"""A repair record agrees with its GL, book value and future depreciation."""
from decimal import Decimal

import pytest

from assets_helpers import load_db_query, call_action, ns, build_gl_env, seed_company, seed_account
from erpclaw_lib.query import Q, P, Table
from erpclaw_lib.gl_invariants import check_gl_invariants

M = load_db_query()


def rows(conn, name):
    return [dict(row) for row in conn.execute(Q.from_(Table(name)).select("*").get_sql()).fetchall()]


def state(conn):
    return {name: sorted(repr(row) for row in rows(conn, name)) for name in
        ("asset", "asset_maintenance", "depreciation_schedule", "gl_entry", "gl_chain_head", "audit_log", "naming_series")}


def schedule(conn, env, capex="1"):
    result = call_action(M.ACTIONS["schedule-maintenance"], conn, ns(
        company_id=env["company_id"], asset_id=env["asset_id"],
        maintenance_type="corrective", scheduled_date="2026-02-15", is_capex=capex))
    assert result["status"] == "ok", result
    return result["maintenance_id"]


@pytest.mark.parametrize("capex", ["1", "0"])
def test_recorded_cents_match_actual_posting_and_depreciation(conn, db_path, capex):
    env = build_gl_env(conn)
    cash = seed_account(conn, env["company_id"], "Repair Cash", account_type="bank", root_type="asset")
    expense = seed_account(conn, env["company_id"], "Repair Expense", "expense", "expense")
    generated = call_action(M.generate_depreciation_schedule, conn, ns(asset_id=env["asset_id"]))
    assert generated["status"] == "ok", generated
    first = sorted(rows(conn, "depreciation_schedule"), key=lambda row: row["schedule_date"])[0]
    posted = call_action(M.post_depreciation, conn, ns(
        depreciation_schedule_id=first["id"], posting_date=first["schedule_date"]))
    assert posted["status"] == "ok", posted
    posted_before = [row for row in rows(conn, "depreciation_schedule") if row["status"] == "posted"]
    asset_before = next(row for row in rows(conn, "asset") if row["id"] == env["asset_id"])
    pending_before = [row for row in rows(conn, "depreciation_schedule") if row["status"] == "pending"]
    mid = schedule(conn, env, capex)
    result = call_action(M.ACTIONS["complete-maintenance"], conn, ns(
        company_id=env["company_id"], maintenance_id=mid, cost="800.005",
        actual_date="2026-02-15", cash_account_id=cash, expense_account_id=expense))
    assert result["status"] == "ok", result
    assert result["cost"] == "800.01"
    maintenance = next(row for row in rows(conn, "asset_maintenance") if row["id"] == mid)
    assert maintenance["asset_id"] == env["asset_id"]
    assert maintenance["status"] == "completed" and maintenance["cost"] == "800.01"
    entries = [row for row in rows(conn, "gl_entry") if row["voucher_id"] == mid]
    assert len(entries) == 2
    accounts = {row["id"]: row["company_id"] for row in rows(conn, "account")}
    assert all(row["voucher_type"] == "asset_repair_capex" and accounts[row["account_id"]] == env["company_id"] for row in entries)
    assert sum((Decimal(row["debit"]) for row in entries), Decimal("0")) == Decimal("800.01")
    assert sum((Decimal(row["credit"]) for row in entries), Decimal("0")) == Decimal("800.01")
    asset_after = next(row for row in rows(conn, "asset") if row["id"] == env["asset_id"])
    change = Decimal("800.01") if capex == "1" else Decimal("0")
    assert Decimal(asset_after["current_book_value"]) == Decimal(asset_before["current_book_value"]) + change
    assert Decimal(asset_after["gross_value"]) == Decimal(asset_before["gross_value"]) + change
    assert [row for row in rows(conn, "depreciation_schedule") if row["status"] == "posted"] == posted_before
    pending_after = [row for row in rows(conn, "depreciation_schedule") if row["status"] == "pending"]
    if capex == "1":
        assert result["schedule_recompute"]["regenerated"] == len(pending_after) > 0
        assert sum((Decimal(row["depreciation_amount"]) for row in pending_after), Decimal("0")) == Decimal(asset_after["current_book_value"]) - Decimal(asset_after["salvage_value"])
    else:
        assert pending_after == pending_before
    before_repeat = state(conn)
    again = call_action(M.ACTIONS["complete-maintenance"], conn, ns(
        company_id=env["company_id"], maintenance_id=mid, cost="800.005",
        cash_account_id=cash, expense_account_id=expense))
    assert again["status"] == "error", again
    assert state(conn) == before_repeat
    invariants = check_gl_invariants(db_path)
    assert invariants["result"] == "pass", invariants
    assert invariants["verified"] > 0 and invariants["violations"] == [], invariants


@pytest.mark.parametrize("action", ["schedule-maintenance", "complete-maintenance"])
def test_other_company_cannot_schedule_or_complete_repair(conn, action):
    env = build_gl_env(conn)
    mid = schedule(conn, env)
    other = seed_company(conn, name="Other Asset Owner", abbr="OA")
    cash = seed_account(conn, env["company_id"], "Repair Cash", account_type="bank", root_type="asset")
    before = state(conn)
    result = call_action(M.ACTIONS[action], conn, ns(
        company_id=other, asset_id=env["asset_id"], maintenance_id=mid,
        maintenance_type="corrective", scheduled_date="2026-02-15",
        cost="800.00", cash_account_id=cash))
    assert result["status"] == "error", result
    assert state(conn) == before
    assert not conn.in_transaction


@pytest.mark.parametrize("cost", ["-0.004", "0.004"])
def test_capex_refuses_negative_or_zero_cents_without_writes(conn, cost):
    env = build_gl_env(conn)
    mid = schedule(conn, env)
    cash = seed_account(conn, env["company_id"], "Repair Cash", account_type="bank", root_type="asset")
    before = state(conn)
    result = call_action(M.complete_maintenance, conn, ns(
        company_id=env["company_id"], maintenance_id=mid, cost=cost, cash_account_id=cash))
    assert result["status"] == "error", result
    assert state(conn) == before
    assert not conn.in_transaction


def test_foreign_cash_account_refuses_without_repair_or_gl_effect(conn):
    env = build_gl_env(conn)
    mid = schedule(conn, env)
    other = seed_company(conn, name="Other Asset Owner", abbr="OA")
    cash = seed_account(conn, other, "Foreign Cash", account_type="bank", root_type="asset")
    before = state(conn)
    result = call_action(M.complete_maintenance, conn, ns(
        company_id=env["company_id"], maintenance_id=mid, cost="800.00", cash_account_id=cash))
    assert result["status"] == "error", result
    assert state(conn) == before
    assert not conn.in_transaction
