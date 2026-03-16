"""Shared helper functions for ERPClaw Treasury unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + init_treasury_schema()
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming series, bank accounts
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
MODULE_DIR = os.path.dirname(TESTS_DIR)          # scripts/
ROOT_DIR = os.path.dirname(MODULE_DIR)            # erpclaw-treasury/
ADDONS_DIR = os.path.dirname(ROOT_DIR)            # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)             # src/
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")
VERTICAL_INIT_PATH = os.path.join(ROOT_DIR, "init_db.py")

ERPCLAW_LIB = os.path.expanduser("~/.openclaw/erpclaw/lib")
if ERPCLAW_LIB not in sys.path:
    sys.path.insert(0, ERPCLAW_LIB)


def load_db_query():
    """Load erpclaw-treasury db_query.py explicitly to avoid sys.path collisions."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_treasury", db_query_path)
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
    """Create all ERPClaw core tables + treasury vertical tables."""
    # 1. Foundation schema (company, account, naming_series, etc.)
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.init_db(db_path)

    # 2. Treasury vertical schema (7 tables)
    spec2 = importlib.util.spec_from_file_location("treasury_init", VERTICAL_INIT_PATH)
    m2 = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(m2)
    m2.init_treasury_schema(db_path)


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
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
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

def seed_company(conn, name="Test Treasury Co", abbr="TTC") -> str:
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


def seed_second_company(conn, name="Partner Corp", abbr="PC") -> str:
    """Insert a second company for inter-company transfer tests."""
    return seed_company(conn, name, abbr)


def seed_naming_series(conn, company_id: str):
    """Seed naming series for treasury entity types."""
    series = [
        ("bank_account_extended", "BACC-", 0),
        ("cash_position", "CPOS-", 0),
        ("cash_forecast", "CFST-", 0),
        ("investment", "INV-", 0),
        ("investment_transaction", "ITXN-", 0),
        ("inter_company_transfer", "ICT-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_bank_account(conn, company_id: str, bank_name="First National",
                      account_name="Operating", balance="50000") -> str:
    """Insert a bank account and return its ID."""
    mod = load_db_query()
    r = call_action(mod.ACTIONS["treasury-add-bank-account"], conn, ns(
        company_id=company_id,
        bank_name=bank_name,
        account_name=account_name,
        account_number="1234567890",
        routing_number="021000021",
        account_type="checking",
        currency="USD",
        current_balance=balance,
        gl_account_id=None,
        is_active=None,
        notes=None,
    ))
    assert is_ok(r), f"seed_bank_account failed: {r}"
    return r["account_id"]


def seed_investment(conn, company_id: str, name="6-Month CD",
                    principal="10000") -> str:
    """Insert an investment and return its ID."""
    mod = load_db_query()
    r = call_action(mod.ACTIONS["treasury-add-investment"], conn, ns(
        company_id=company_id,
        name=name,
        investment_type="cd",
        institution="First National Bank",
        account_number=None,
        principal=principal,
        current_value=principal,
        interest_rate="4.5",
        purchase_date="2026-01-15",
        maturity_date="2026-07-15",
        gl_account_id=None,
        notes=None,
    ))
    assert is_ok(r), f"seed_investment failed: {r}"
    return r["investment_id"]


def build_env(conn) -> dict:
    """Create a full treasury test environment.

    Returns dict with company_id, company_id_2, bank_account_id,
    investment_id, and all naming series seeded.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)

    # Second company for inter-company transfers
    cid2 = seed_second_company(conn)
    seed_naming_series(conn, cid2)

    bank_id = seed_bank_account(conn, cid)
    inv_id = seed_investment(conn, cid)

    return {
        "company_id": cid,
        "company_id_2": cid2,
        "bank_account_id": bank_id,
        "investment_id": inv_id,
    }
