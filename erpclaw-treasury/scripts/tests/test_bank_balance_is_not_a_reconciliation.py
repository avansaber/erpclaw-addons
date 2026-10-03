"""A plain bank-balance update is not a reconciliation (m768).

Recording a balance must leave ``last_reconciled_date`` alone unless the
caller explicitly passes ``--reconciled-date`` for a statement-matched
reconciliation.
"""
import json
from datetime import date, timedelta

import pytest
from treasury_helpers import (
    call_action, ns, is_ok, is_error, load_db_query, seed_bank_account,
)

from erpclaw_lib.query import Field, P, Q, Table, fn


@pytest.fixture
def mod():
    return load_db_query()


def _bank_row(conn, acct_id):
    row = conn.execute(
        Q.from_(Table("bank_account_extended"))
        .select(Table("bank_account_extended").star)
        .where(Field("id") == P()).get_sql(),
        (acct_id,),
    ).fetchone()
    return dict(row)


def _cash_position_count(conn):
    row = conn.execute(
        Q.from_(Table("cash_position"))
        .select(fn.Count("*").as_("cnt")).get_sql(),
    ).fetchone()
    return row["cnt"]


def _audit_new_values(conn, entity_id, action):
    t = Table("audit_log")
    rows = conn.execute(
        Q.from_(t).select(t.new_values)
        .where(t.entity_id == P())
        .where(t.action == P()).get_sql(),
        (entity_id, action),
    ).fetchall()
    return [json.loads(r[0]) for r in rows if r[0]]


def test_balance_update_leaves_reconciled_date_unset(conn, env, mod):
    acct_id = seed_bank_account(conn, env["company_id"])
    assert _bank_row(conn, acct_id)["last_reconciled_date"] is None

    r = call_action(mod.ACTIONS["treasury-record-bank-balance"], conn, ns(
        account_id=acct_id,
        current_balance="52341.17",
        reconciled_date=None,
    ))
    assert is_ok(r)

    row = _bank_row(conn, acct_id)
    assert row["last_reconciled_date"] is None
    assert row["current_balance"] == "52341.17"
    assert r["last_reconciled_date"] is None

    rep = call_action(mod.ACTIONS["treasury-bank-summary-report"], conn, ns(
        company_id=env["company_id"],
    ))
    assert is_ok(rep)
    entry = [a for a in rep["accounts"] if a["id"] == acct_id][0]
    assert entry["last_reconciled_date"] is None


def test_balance_update_keeps_an_earlier_reconciled_date(conn, env, mod):
    acct_id = seed_bank_account(conn, env["company_id"])

    first = call_action(mod.ACTIONS["treasury-record-bank-balance"], conn, ns(
        account_id=acct_id,
        current_balance="50000.00",
        reconciled_date="2026-01-31",
    ))
    assert is_ok(first)
    assert _bank_row(conn, acct_id)["last_reconciled_date"] == "2026-01-31"

    second = call_action(mod.ACTIONS["treasury-record-bank-balance"], conn, ns(
        account_id=acct_id,
        current_balance="52341.17",
        reconciled_date=None,
    ))
    assert is_ok(second)
    assert _bank_row(conn, acct_id)["last_reconciled_date"] == "2026-01-31"
    assert second["last_reconciled_date"] == "2026-01-31"
    assert "reconciled_date" not in second


def test_reconciled_date_is_stored_when_stated(conn, env, mod):
    acct_id = seed_bank_account(conn, env["company_id"])

    r = call_action(mod.ACTIONS["treasury-record-bank-balance"], conn, ns(
        account_id=acct_id,
        current_balance="52341.17",
        reconciled_date="2026-01-31",
    ))
    assert is_ok(r)
    assert r["reconciled_date"] == "2026-01-31"
    assert r["last_reconciled_date"] == "2026-01-31"
    assert _bank_row(conn, acct_id)["last_reconciled_date"] == "2026-01-31"

    entries = _audit_new_values(conn, acct_id, "treasury-record-bank-balance")
    assert entries
    assert entries[-1].get("reconciled_date") == "2026-01-31"


def test_future_reconciled_date_refused(conn, env, mod):
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    acct_id = seed_bank_account(conn, env["company_id"])
    before = _bank_row(conn, acct_id)
    positions_before = _cash_position_count(conn)

    r = call_action(mod.ACTIONS["treasury-record-bank-balance"], conn, ns(
        account_id=acct_id,
        current_balance="52341.17",
        reconciled_date=tomorrow,
    ))
    assert is_error(r)
    assert r["message"] == "--reconciled-date cannot be in the future"

    after = _bank_row(conn, acct_id)
    assert after["current_balance"] == before["current_balance"]
    assert after["last_reconciled_date"] == before["last_reconciled_date"]
    assert _cash_position_count(conn) == positions_before


def test_invalid_reconciled_date_refused(conn, env, mod):
    acct_id = seed_bank_account(conn, env["company_id"])
    before = _bank_row(conn, acct_id)
    positions_before = _cash_position_count(conn)

    r = call_action(mod.ACTIONS["treasury-record-bank-balance"], conn, ns(
        account_id=acct_id,
        current_balance="52341.17",
        reconciled_date="2026-02-30",
    ))
    assert is_error(r)
    assert r["message"] == "Invalid --reconciled-date: 2026-02-30"

    after = _bank_row(conn, acct_id)
    assert after["current_balance"] == before["current_balance"]
    assert after["last_reconciled_date"] == before["last_reconciled_date"]
    assert _cash_position_count(conn) == positions_before
