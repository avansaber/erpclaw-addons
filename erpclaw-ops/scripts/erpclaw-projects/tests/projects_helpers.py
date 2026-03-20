"""Shared helper functions for ERPClaw Projects L1 unit tests.

Provides:
  - DB bootstrap via init_schema.init_db()
  - load_db_query() for explicit module loading (avoids sys.path collisions)
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming_series, employees, customers, projects
  - build_env() for a complete projects test environment
"""
import argparse
import importlib.util
import io
import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_DIR = os.path.dirname(TESTS_DIR)               # erpclaw-projects/
SCRIPTS_DIR = os.path.dirname(MODULE_DIR)              # scripts/
ROOT_DIR = os.path.dirname(SCRIPTS_DIR)                # erpclaw-ops/
ADDONS_DIR = os.path.dirname(ROOT_DIR)                 # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)                  # src/

# Foundation schema init
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")

# Make erpclaw_lib importable
ERPCLAW_LIB = os.path.expanduser("~/.openclaw/erpclaw/lib")
if ERPCLAW_LIB not in sys.path:
    sys.path.insert(0, ERPCLAW_LIB)

from erpclaw_lib.db import setup_pragmas

# Make module dir importable
if MODULE_DIR not in sys.path:
    sys.path.insert(0, MODULE_DIR)


def load_db_query():
    """Load erpclaw-projects db_query.py explicitly."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_projects", db_query_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for action_name, fn in mod.ACTIONS.items():
        setattr(mod, action_name.replace("-", "_"), fn)
    return mod


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def init_all_tables(db_path: str):
    """Create foundation tables (project tables are in init_schema)."""
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    schema_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_mod)
    schema_mod.init_db(db_path)


class _ConnWrapper:
    """Thin wrapper so conn.company_id works (some actions set it)."""
    def __init__(self, real_conn):
        self._conn = real_conn
        self.company_id = None

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def execute(self, *a, **kw):
        return self._conn.execute(*a, **kw)

    def executemany(self, *a, **kw):
        return self._conn.executemany(*a, **kw)

    def executescript(self, *a, **kw):
        return self._conn.executescript(*a, **kw)

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return self._conn.close()

    @property
    def row_factory(self):
        return self._conn.row_factory

    @row_factory.setter
    def row_factory(self, value):
        self._conn.row_factory = value


class _DecimalSum:
    """Custom SQLite aggregate: SUM using Python Decimal for precision."""
    def __init__(self):
        self.total = Decimal("0")

    def step(self, value):
        if value is not None:
            self.total += Decimal(str(value))

    def finalize(self):
        return str(self.total)


def get_conn(db_path: str):
    """Return a wrapped sqlite3.Connection with FK enabled and Row factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    setup_pragmas(conn)
    conn.create_aggregate("decimal_sum", 1, _DecimalSum)
    return _ConnWrapper(conn)


# ---------------------------------------------------------------------------
# Action invocation helpers
# ---------------------------------------------------------------------------

def call_action(fn, conn, args) -> dict:
    """Invoke a domain function, capture stdout JSON, return parsed dict."""
    buf = io.StringIO()

    def _fake_exit(code=0):
        raise SystemExit(code)

    try:
        with patch("sys.stdout", buf), patch("sys.exit", side_effect=_fake_exit):
            fn(conn, args)
    except SystemExit:
        pass

    output = buf.getvalue().strip()
    if not output:
        return {"status": "error", "message": "no output captured"}
    return json.loads(output)


def ns(**kwargs) -> argparse.Namespace:
    """Build an argparse.Namespace from keyword args (mimics CLI flags)."""
    defaults = {
        "limit": "20",
        "offset": "0",
        "company_id": None,
        "search": None,
        # Project
        "project_id": None,
        "name": None,
        "description": None,
        "project_type": None,
        "billing_type": None,
        "estimated_cost": None,
        "actual_cost": None,
        "total_billed": None,
        "percent_complete": None,
        "customer_id": None,
        "cost_center_id": None,
        # Task
        "task_id": None,
        "assigned_to": None,
        "estimated_hours": None,
        "actual_hours": None,
        "depends_on": None,
        "parent_task_id": None,
        # Milestone
        "milestone_id": None,
        "target_date": None,
        "completion_date": None,
        # Timesheet
        "timesheet_id": None,
        "employee_id": None,
        "items": None,
        # Dates
        "start_date": None,
        "end_date": None,
        # Filters
        "status": None,
        "priority": None,
        "from_date": None,
        "to_date": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def is_error(result: dict) -> bool:
    return result.get("status") == "error"


def is_ok(result: dict) -> bool:
    return result.get("status") == "ok"


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def seed_company(conn, name="Project Test Co", abbr="PTC") -> str:
    """Insert a test company and return its ID."""
    cid = _uuid()
    conn.execute(
        """INSERT INTO company (id, name, abbr, default_currency, country,
           fiscal_year_start_month)
           VALUES (?, ?, ?, 'USD', 'United States', 1)""",
        (cid, f"{name} {cid[:6]}", f"{abbr}{cid[:4]}")
    )
    conn.commit()
    return cid


def seed_naming_series(conn, company_id: str):
    """Seed naming series for project entity types."""
    series = [
        ("project", "PROJ-", 0),
        ("task", "TASK-", 0),
        ("milestone", "MS-", 0),
        ("timesheet", "TS-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_employee(conn, company_id: str, name="John Doe") -> str:
    """Insert a test employee and return its ID."""
    eid = _uuid()
    first = name.split()[0]
    last = name.split()[-1]
    full = f"{first} {last}"
    conn.execute(
        """INSERT INTO employee (id, first_name, last_name, full_name,
           date_of_joining, status, company_id)
           VALUES (?, ?, ?, ?, '2025-01-15', 'active', ?)""",
        (eid, first, last, full, company_id)
    )
    conn.commit()
    return eid


def seed_customer(conn, company_id: str, name="Acme Corp") -> str:
    """Insert a test customer and return its ID."""
    cust_id = _uuid()
    conn.execute(
        """INSERT INTO customer (id, name, customer_type, status, company_id)
           VALUES (?, ?, 'company', 'active', ?)""",
        (cust_id, f"{name} {cust_id[:6]}", company_id)
    )
    conn.commit()
    return cust_id


def seed_project(conn, company_id: str, name="Test Project") -> str:
    """Insert a test project and return its ID."""
    from erpclaw_lib.naming import get_next_name
    pid = _uuid()
    naming = get_next_name(conn, "project", company_id=company_id)
    conn.execute(
        """INSERT INTO project (id, naming_series, project_name, project_type,
           status, priority, estimated_cost, actual_cost, billing_type,
           total_billed, profit_margin, percent_complete, company_id)
           VALUES (?, ?, ?, 'internal', 'open', 'medium', '0', '0',
                   'non_billable', '0', '0', '0', ?)""",
        (pid, naming, f"{name} {pid[:6]}", company_id)
    )
    conn.commit()
    return pid


def build_env(conn) -> dict:
    """Create a complete projects test environment.

    Returns dict with all IDs needed for project domain tests.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    emp_id = seed_employee(conn, cid)
    cust_id = seed_customer(conn, cid)
    proj_id = seed_project(conn, cid)

    return {
        "company_id": cid,
        "employee_id": emp_id,
        "customer_id": cust_id,
        "project_id": proj_id,
    }
