"""Exact decimals for the project hour reports (SQLite + PostgreSQL).

Quantities, hours and money are TEXT holding exact decimals. One billable
line of 0.70 hours at 1.15 must bill 0.81 (the binary float product is
0.80499... and rounds to 0.80), and an employee with 9.00 hours must rank
below one with 10.00 hours (as text "9.00" > "10.00").

Seed: employee A holds one billable line (0.70 h at 1.15) plus one
non-billable line (8.30 h at 1.00) for 9.00 h; employee B holds one
billable line (10.00 h at 2.00). Both timesheets are submitted.
"""
import importlib.util
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import urlparse

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest

from projects_helpers import (
    call_action, ns, is_ok, load_db_query, seed_employee,
)

MODULE_DIR = os.path.dirname(_TESTS_DIR)
SCRIPTS_DIR = os.path.dirname(MODULE_DIR)
ROOT_DIR = os.path.dirname(SCRIPTS_DIR)
ADDONS_DIR = os.path.dirname(ROOT_DIR)
SRC_DIR = os.path.dirname(ADDONS_DIR)
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
_IN_TREE_LIB = os.path.join(SETUP_DIR, "lib")
if _IN_TREE_LIB not in sys.path:
    import importlib as _il
    if _il.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, _IN_TREE_LIB)

from erpclaw_lib.db import get_connection
from erpclaw_lib.query import P, insert_row
from erpclaw_lib import seam as _seam

M = load_db_query()


def _today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _insert(conn, table, **data):
    sql, cols = insert_row(table, {k: P() for k in data})
    conn.execute(sql, [data[c] for c in cols])


def _seed_timesheet(conn, employee_id, project_id, company_id, lines, day):
    """Insert a submitted timesheet plus its detail lines; return its id."""
    ts_id = str(uuid.uuid4())
    total = sum((Decimal(l["hours"]) for l in lines), Decimal("0"))
    billable = sum(
        (Decimal(l["hours"]) for l in lines if l["billable"] == 1),
        Decimal("0"))
    _insert(
        conn, "timesheet", id=ts_id, naming_series="TS-%s" % ts_id[:6],
        employee_id=employee_id, start_date=day, end_date=day,
        total_hours=str(total), total_billable_hours=str(billable),
        total_billed_hours="0", total_cost="0",
        total_billable_amount="0", status="submitted",
        company_id=company_id)
    for line in lines:
        _insert(
            conn, "timesheet_detail", id=str(uuid.uuid4()),
            timesheet_id=ts_id, project_id=project_id, task_id=None,
            activity_type="consulting", hours=line["hours"],
            billing_rate=line["billing_rate"], billable=line["billable"],
            description=None, date=day)
    conn.commit()
    return ts_id


def seed_exact_data(conn, company_id, emp_a, emp_b, project_id, day=None):
    """Seed the two submitted timesheets described above."""
    day = day or _today()
    _seed_timesheet(conn, emp_a, project_id, company_id, [
        {"hours": "0.70", "billing_rate": "1.15", "billable": 1},
        {"hours": "8.30", "billing_rate": "1.00", "billable": 0},
    ], day)
    _seed_timesheet(conn, emp_b, project_id, company_id, [
        {"hours": "10.00", "billing_rate": "2.00", "billable": 1},
    ], day)


@pytest.fixture
def exact_env(conn, env):
    day = _today()
    emp_a = env["employee_id"]
    emp_b = seed_employee(conn, env["company_id"], name="Bob Exact")
    seed_exact_data(conn, env["company_id"], emp_a, emp_b,
                    env["project_id"], day)
    return {
        "company_id": env["company_id"],
        "project_id": env["project_id"],
        "emp_a": emp_a,
        "emp_b": emp_b,
    }


def check_get_project(result):
    assert is_ok(result), result
    assert result["project"]["timesheet_summary"] == {
        "total_hours": "19.00",
        "billable_hours": "10.70",
        "billable_amount": "20.81",
        "timesheet_count": 2,
    }


def check_profitability(result, emp_a, emp_b):
    assert is_ok(result), result
    employees = result["employees"]
    assert [e["employee_id"] for e in employees] == [emp_b, emp_a]
    by_id = {e["employee_id"]: e for e in employees}
    assert by_id[emp_a]["total_hours"] == "9.00"
    assert by_id[emp_a]["billable_hours"] == "0.70"
    assert by_id[emp_a]["total_cost"] == "9.11"
    assert by_id[emp_a]["billable_amount"] == "0.81"
    assert by_id[emp_b]["total_hours"] == "10.00"
    assert by_id[emp_b]["billable_hours"] == "10.00"
    assert by_id[emp_b]["total_cost"] == "20.00"
    assert by_id[emp_b]["billable_amount"] == "20.00"


def check_utilization(result, emp_a, emp_b):
    assert is_ok(result), result
    employees = result["employees"]
    assert [e["employee_id"] for e in employees] == [emp_b, emp_a]
    by_id = {e["employee_id"]: e for e in employees}
    assert by_id[emp_a]["billable_amount"] == "0.81"
    assert by_id[emp_a]["non_billable_hours"] == "8.30"
    assert by_id[emp_a]["utilization_percent"] == "7.78"
    assert by_id[emp_a]["project_count"] == 1
    assert by_id[emp_b]["billable_amount"] == "20.00"
    assert by_id[emp_b]["non_billable_hours"] == "0.00"
    assert by_id[emp_b]["project_count"] == 1


def check_status(result):
    assert is_ok(result), result
    assert result["hours_this_month"] == {
        "total": "19.00",
        "billable": "10.70",
    }


class TestTimesheetReportsExact:
    def test_get_project_summary_is_exact(self, conn, exact_env):
        check_get_project(call_action(
            M.get_project, conn, ns(project_id=exact_env["project_id"])))

    def test_profitability_is_exact_and_ranked(self, conn, exact_env):
        check_profitability(call_action(
            M.project_profitability, conn,
            ns(project_id=exact_env["project_id"])),
            exact_env["emp_a"], exact_env["emp_b"])

    def test_utilization_is_exact_and_ranked(self, conn, exact_env):
        check_utilization(call_action(
            M.resource_utilization, conn,
            ns(company_id=exact_env["company_id"])),
            exact_env["emp_a"], exact_env["emp_b"])

    def test_status_hours_this_month(self, conn, exact_env):
        check_status(call_action(
            M.status_action, conn,
            ns(company_id=exact_env["company_id"])))


@pytest.fixture(autouse=True)
def _dispose_seam_engines():
    yield
    _seam.dispose_engines()


@pytest.fixture
def pg_pair(monkeypatch):
    """Live PostgreSQL connection plus the exact-decimals env, reset per test."""
    pg_url = os.environ.get("ERPCLAW_PG_TEST_URL")
    if not pg_url:
        pytest.skip("ERPCLAW_PG_TEST_URL not set (live PostgreSQL required)")
    monkeypatch.setenv("ERPCLAW_DB_DIALECT", "postgresql")
    monkeypatch.setenv("ERPCLAW_DB_URL", pg_url)
    if "ERPCLAW_DB_PATH" in os.environ:
        monkeypatch.delenv("ERPCLAW_DB_PATH", raising=False)
    expected_db = urlparse(pg_url).path.strip("/")
    if not expected_db:
        raise RuntimeError("refusing to reset: ERPCLAW_PG_TEST_URL names no database")
    setup_conn = get_connection()
    try:
        resolved_db = setup_conn.execute("SELECT current_database()").fetchone()[0]
        if resolved_db != expected_db:
            raise RuntimeError(
                "refusing to reset: ERPCLAW_PG_TEST_URL names database %r "
                "but the connection resolved to %r" % (expected_db, resolved_db))
        setup_conn.execute("DROP SCHEMA public CASCADE")
        setup_conn.execute("CREATE SCHEMA public")
        setup_conn.commit()
    finally:
        setup_conn.close()
    init_schema_path = os.path.join(SETUP_DIR, "init_schema.py")
    spec = importlib.util.spec_from_file_location(
        "init_schema_pg_leg", init_schema_path)
    schema_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_mod)
    schema_mod.init_db(None)
    conn = get_connection()
    cid = str(uuid.uuid4())
    _insert(conn, "company", id=cid, name="Exact Co %s" % cid[:6],
            abbr="EX%s" % cid[:4], default_currency="USD",
            country="United States", fiscal_year_start_month=1)
    emp_a = str(uuid.uuid4())
    _insert(conn, "employee", id=emp_a, first_name="Alice",
            last_name="Exact", full_name="Alice Exact",
            date_of_joining="2025-01-15", status="active",
            company_id=cid)
    emp_b = str(uuid.uuid4())
    _insert(conn, "employee", id=emp_b, first_name="Bob",
            last_name="Exact", full_name="Bob Exact",
            date_of_joining="2025-01-15", status="active",
            company_id=cid)
    cust = str(uuid.uuid4())
    _insert(conn, "customer", id=cust, name="Acme %s" % cust[:6],
            company_id=cid)
    proj = str(uuid.uuid4())
    _insert(conn, "project", id=proj,
            project_name="Exact Project %s" % proj[:6], company_id=cid)
    conn.commit()
    seed_exact_data(conn, cid, emp_a, emp_b, proj)
    env = {"company_id": cid, "project_id": proj,
           "emp_a": emp_a, "emp_b": emp_b}
    try:
        yield conn, env
    finally:
        conn.close()


class TestTimesheetReportsExactPG:
    def test_get_project_summary_is_exact(self, pg_pair):
        conn, env = pg_pair
        check_get_project(call_action(
            M.get_project, conn, ns(project_id=env["project_id"])))

    def test_profitability_is_exact_and_ranked(self, pg_pair):
        conn, env = pg_pair
        check_profitability(call_action(
            M.project_profitability, conn,
            ns(project_id=env["project_id"])),
            env["emp_a"], env["emp_b"])

    def test_utilization_is_exact_and_ranked(self, pg_pair):
        conn, env = pg_pair
        check_utilization(call_action(
            M.resource_utilization, conn,
            ns(company_id=env["company_id"])),
            env["emp_a"], env["emp_b"])

    def test_status_hours_this_month(self, pg_pair):
        conn, env = pg_pair
        check_status(call_action(
            M.status_action, conn, ns(company_id=env["company_id"])))
