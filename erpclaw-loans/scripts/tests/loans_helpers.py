"""Shared helper functions for ERPClaw Loans unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + create_loans_tables()
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, customer, employee, supplier, account, naming series
  - load_db_query() for explicit module loading
"""
import argparse
import importlib.util
import io
import json
import os
import sqlite3
import sys
import uuid
from decimal import Decimal
from unittest.mock import patch

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_DIR = os.path.dirname(TESTS_DIR)          # erpclaw-loans/scripts/
ROOT_DIR = os.path.dirname(MODULE_DIR)            # erpclaw-loans/
ADDONS_DIR = os.path.dirname(ROOT_DIR)            # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)             # source/
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")
VERTICAL_INIT_PATH = os.path.join(ROOT_DIR, "init_db.py")

ERPCLAW_LIB = os.path.expanduser("~/.openclaw/erpclaw/lib")
if ERPCLAW_LIB not in sys.path:
    sys.path.insert(0, ERPCLAW_LIB)

from erpclaw_lib.db import setup_pragmas


def load_db_query():
    """Load erpclaw-loans db_query.py explicitly to avoid sys.path collisions."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_loans", db_query_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # Attach action functions as attributes (kebab -> underscore)
    for action_name, fn in mod.ACTIONS.items():
        setattr(mod, action_name.replace("-", "_"), fn)
    return mod


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def init_all_tables(db_path: str):
    """Create all ERPClaw core tables + loans vertical tables."""
    # 1. Foundation schema (company, account, naming_series, etc.)
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.init_db(db_path)

    # 2. Loans vertical schema
    spec2 = importlib.util.spec_from_file_location("loans_init", VERTICAL_INIT_PATH)
    m2 = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(m2)
    m2.create_loans_tables(db_path)


class _ConnWrapper:
    """Wraps sqlite3.Connection with company_id attribute for action functions."""
    def __init__(self, conn, company_id=None):
        self._conn = conn
        self.company_id = company_id

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def execute(self, *a, **kw):
        return self._conn.execute(*a, **kw)

    def executemany(self, *a, **kw):
        return self._conn.executemany(*a, **kw)

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return self._conn.close()


def get_conn(db_path: str) -> sqlite3.Connection:
    """Return a sqlite3.Connection with FK enabled and Row factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    setup_pragmas(conn)
    return conn


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
    return argparse.Namespace(**kwargs)


def is_error(result: dict) -> bool:
    """Check if a call_action result is an error response."""
    return result.get("status") == "error"


def is_ok(result: dict) -> bool:
    """Check if a call_action result is a success response."""
    return result.get("status") == "ok"


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _uuid() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def seed_company(conn, name="Test Lending Co", abbr="TLC") -> str:
    """Insert a test company via direct SQL and return its ID."""
    cid = _uuid()
    conn.execute(
        """INSERT INTO company (id, name, abbr, default_currency, country,
           fiscal_year_start_month)
           VALUES (?, ?, ?, 'USD', 'United States', 1)""",
        (cid, f"{name} {cid[:6]}", f"{abbr}{cid[:4]}")
    )
    conn.commit()
    return cid


def seed_customer(conn, company_id: str, name="Test Customer") -> str:
    """Insert a customer and return its ID."""
    cid = _uuid()
    conn.execute(
        """INSERT INTO customer (id, name, company_id, customer_type, status, credit_limit)
           VALUES (?, ?, ?, 'company', 'active', '0')""",
        (cid, name, company_id)
    )
    conn.commit()
    return cid


def seed_employee(conn, company_id: str, name="Test Employee") -> str:
    """Insert an employee and return its ID.

    Note: The employee table uses first_name/full_name, not name.
    The loans module _validate_applicant does SELECT id, name FROM employee
    which will fail at runtime for employee applicant type.
    We seed the employee correctly for schema integrity.
    """
    eid = _uuid()
    parts = name.split(" ", 1)
    first = parts[0]
    last = parts[1] if len(parts) > 1 else ""
    conn.execute(
        """INSERT INTO employee (id, first_name, last_name, full_name,
           company_id, status, date_of_joining)
           VALUES (?, ?, ?, ?, ?, 'active', '2025-01-01')""",
        (eid, first, last, name, company_id)
    )
    conn.commit()
    return eid


def seed_supplier(conn, company_id: str, name="Test Supplier") -> str:
    """Insert a supplier and return its ID."""
    sid = _uuid()
    conn.execute(
        """INSERT INTO supplier (id, name, company_id, supplier_type, status)
           VALUES (?, ?, ?, 'company', 'active')""",
        (sid, name, company_id)
    )
    conn.commit()
    return sid


def seed_account(conn, company_id: str, name="Test Account",
                 account_type="receivable", root_type="asset") -> str:
    """Insert an account and return its ID.

    account_type must be one of: bank, cash, receivable, payable, stock,
      fixed_asset, revenue, expense, equity, tax, etc.
    root_type must be one of: asset, liability, equity, income, expense.
    """
    aid = _uuid()
    conn.execute(
        """INSERT INTO account (id, name, company_id, account_type, root_type,
           is_group)
           VALUES (?, ?, ?, ?, ?, 0)""",
        (aid, name, company_id, account_type, root_type)
    )
    conn.commit()
    return aid


def seed_naming_series(conn, company_id: str):
    """Seed naming series for loan entity types."""
    series = [
        ("loan_application", "LAPP-", 0),
        ("loan", "LN-", 0),
        ("loan_repayment", "LRP-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def build_env(conn) -> dict:
    """Create a full loans test environment.

    Returns dict with company_id, customer_id, employee_id, supplier_id,
    three account IDs, and all naming series seeded.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    cust = seed_customer(conn, cid, "Acme Corp")
    emp = seed_employee(conn, cid, "John Doe")
    sup = seed_supplier(conn, cid, "Parts Inc")
    loan_acct = seed_account(conn, cid, "Loan Receivable", "receivable", "asset")
    interest_acct = seed_account(conn, cid, "Interest Income", "revenue", "income")
    disbursement_acct = seed_account(conn, cid, "Bank Account", "bank", "asset")
    bad_debt_acct = seed_account(conn, cid, "Bad Debt Expense", "expense", "expense")
    return {
        "company_id": cid,
        "customer_id": cust,
        "employee_id": emp,
        "supplier_id": sup,
        "loan_account_id": loan_acct,
        "interest_income_account_id": interest_acct,
        "disbursement_account_id": disbursement_acct,
        "bad_debt_account_id": bad_debt_acct,
    }
