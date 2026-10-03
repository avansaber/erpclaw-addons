"""m634 AI engine exact money arithmetic pin (Part A + Part B item 2).

The budget-overrun check in detect_anomalies subtracted two exact-decimal
text sums inside the SQL; on SQLite that arithmetic goes through a binary
float, so at magnitudes where the float can no longer hold cents the
recorded actual_spend flips (...06 where ...07 is exact). The check now
fetches each leg as text and subtracts in Python with Decimal.

Money is text: Decimal in Python, TEXT columns, exact string comparisons.
Never float. All seeds and direct reads are parameterised PyPika queries;
connections come from the conftest fixtures.
"""
import importlib.util
import json
import os
import sys
import uuid
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from ai_helpers import call_action, ns, is_ok

_SETUP_DIR = os.path.join(
    os.path.dirname(_TESTS_DIR), "..", "..", "..", "..",
    "erpclaw", "scripts", "erpclaw-setup")
_SETUP_DIR = os.path.abspath(_SETUP_DIR)
_IN_TREE_LIB = os.path.join(_SETUP_DIR, "lib")
if _IN_TREE_LIB not in sys.path:
    if importlib.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, _IN_TREE_LIB)

from erpclaw_lib.query import Q, P, Table, Field, insert_row


def _load():
    path = os.path.join(os.path.dirname(_TESTS_DIR), "db_query.py")
    spec = importlib.util.spec_from_file_location(
        "db_query_ai_engine_m634", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


MOD = _load()

BIG = "100000000000000.07"


def _u():
    return str(uuid.uuid4())


def _insert(conn, table, row):
    sql, cols = insert_row(table, {key: P() for key in row})
    conn.execute(sql, [row[c] for c in cols])


def _std_env(conn):
    cid = _u()
    _insert(conn, "company", {
        "id": cid, "name": "AI Exact Co %s" % cid[:6],
        "abbr": "AX%s" % cid[:4].upper(),
        "default_currency": "USD", "country": "United States",
        "fiscal_year_start_month": 1})
    expense = _u()
    _insert(conn, "account", {
        "id": expense, "name": "Operating Expenses",
        "account_number": "6000", "root_type": "expense",
        "account_type": "expense", "is_group": 0,
        "company_id": cid})
    cash = _u()
    _insert(conn, "account", {
        "id": cash, "name": "Cash",
        "account_number": "1000", "root_type": "asset",
        "account_type": "cash", "is_group": 0,
        "company_id": cid})
    fy_id = _u()
    _insert(conn, "fiscal_year", {
        "id": fy_id, "name": "FY-2026-%s" % cid[:6],
        "start_date": "2026-01-01", "end_date": "2026-12-31",
        "is_closed": 0, "company_id": cid})
    budget_id = _u()
    _insert(conn, "budget", {
        "id": budget_id, "fiscal_year_id": fy_id,
        "account_id": expense,
        "budget_amount": "100.00", "company_id": cid})
    conn.commit()
    return {"company_id": cid, "expense": expense,
            "cash": cash, "budget_id": budget_id}


def _gl(conn, account_id, posting_date, debit, credit):
    _insert(conn, "gl_entry", {
        "id": _u(), "posting_date": posting_date,
        "account_id": account_id, "debit": debit, "credit": credit,
        "voucher_type": "journal_entry", "voucher_id": _u(),
        "is_cancelled": 0})


def _anomalies_by_type(conn, anomaly_type):
    t = Table("anomaly")
    q = (Q.from_(t).select(t.star)
         .where(t.anomaly_type == P()))
    return [dict(r) for r in
            conn.execute(q.get_sql(), (anomaly_type,)).fetchall()]


class TestBudgetOverrunExact:
    def test_actual_spend_is_exact(self, conn):
        e = _std_env(conn)
        _gl(conn, e["expense"], "2026-04-01", BIG, "0")
        _gl(conn, e["cash"], "2026-04-01", "0", BIG)
        conn.commit()
        res = call_action(MOD.detect_anomalies, conn, ns(
            company_id=e["company_id"],
            from_date="2026-03-01", to_date="2026-04-30"))
        assert is_ok(res), res
        assert res["by_type"].get("budget_overrun", 0) >= 1
        rows = _anomalies_by_type(conn, "budget_overrun")
        assert len(rows) == 1
        actual = json.loads(rows[0]["actual"])
        assert actual["actual_spend"] == BIG
        baseline = json.loads(rows[0]["baseline"])
        assert baseline["budget_amount"] == "100.00"
