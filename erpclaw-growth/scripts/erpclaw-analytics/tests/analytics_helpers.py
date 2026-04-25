"""Shared helper functions for ERPClaw Analytics unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + create_crmadv_tables()
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming series, GL data
  - build_env() for full test environment
  - load_db_query() for explicit module loading (avoids sys.path collisions)
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

# ──────────────────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────────────────

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_DIR = os.path.dirname(TESTS_DIR)                    # erpclaw-analytics/
SCRIPTS_DIR = os.path.dirname(MODULE_DIR)                  # scripts/
ROOT_DIR = os.path.dirname(SCRIPTS_DIR)                    # erpclaw-growth/
ADDONS_DIR = os.path.dirname(ROOT_DIR)                     # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)                      # source/

# Foundation schema init
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")

# Vertical schema init (parent growth init_db)
VERTICAL_INIT_PATH = os.path.join(ROOT_DIR, "init_db.py")

# Make erpclaw_lib importable
ERPCLAW_LIB = os.path.expanduser("~/.openclaw/erpclaw/lib")
if ERPCLAW_LIB not in sys.path:
    sys.path.insert(0, ERPCLAW_LIB)

from erpclaw_lib.db import setup_pragmas


def load_db_query():
    """Load erpclaw-analytics db_query.py explicitly to avoid sys.path collisions."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_analytics", db_query_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # Attach action functions as underscore-named attributes for convenience
    for action_name, fn in mod.ACTIONS.items():
        setattr(mod, action_name.replace("-", "_"), fn)
    return mod


# ──────────────────────────────────────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────────────────────────────────────

def init_all_tables(db_path: str):
    """Create all foundation + growth tables."""
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.init_db(db_path)

    spec2 = importlib.util.spec_from_file_location("growth_init", VERTICAL_INIT_PATH)
    m2 = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(m2)
    m2.create_crmadv_tables(db_path)


class _DecimalSum:
    """Custom SQLite aggregate: SUM using Python Decimal for precision."""
    def __init__(self):
        self.total = Decimal("0")
    def step(self, value):
        if value is not None:
            self.total += Decimal(str(value))
    def finalize(self):
        return str(self.total)


class _DecimalAbs:
    """Custom SQLite function: ABS for Decimal values."""
    pass


class _ConnWrapper:
    """Wrap a sqlite3.Connection so conn.company_id is accessible."""
    def __init__(self, conn):
        self._conn = conn
        self.company_id = None

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _decimal_abs(value):
    """SQLite function for decimal absolute value."""
    if value is None:
        return "0"
    return str(abs(Decimal(str(value))))


def get_conn(db_path: str):
    """Return a wrapped sqlite3.Connection with FK enabled, Row factory, and aggregates."""
    raw = sqlite3.connect(db_path)
    raw.row_factory = sqlite3.Row
    setup_pragmas(raw)
    raw.create_aggregate("decimal_sum", 1, _DecimalSum)
    raw.create_function("decimal_abs", 1, _decimal_abs)
    return _ConnWrapper(raw)


# ──────────────────────────────────────────────────────────────────────────────
# Action invocation helpers
# ──────────────────────────────────────────────────────────────────────────────

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
    return result.get("status") == "error"


def is_ok(result: dict) -> bool:
    return result.get("status") == "ok"


# ──────────────────────────────────────────────────────────────────────────────
# Utility
# ──────────────────────────────────────────────────────────────────────────────

def _uuid() -> str:
    return str(uuid.uuid4())


# ──────────────────────────────────────────────────────────────────────────────
# Seed helpers
# ──────────────────────────────────────────────────────────────────────────────

def seed_company(conn, name="Test Analytics Co", abbr="TAN") -> str:
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
    """Seed naming series for core entity types."""
    series = [
        ("account", "ACCT-", 0),
        ("fiscal_year", "FY-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_accounts(conn, company_id: str) -> dict:
    """Create a minimal chart of accounts. Returns dict of account IDs."""
    accounts = {}
    accts = [
        ("Cash", "1000", "asset", "cash", 0),
        ("Accounts Receivable", "1100", "asset", "receivable", 0),
        ("Revenue", "4000", "income", "revenue", 0),
        ("COGS", "5000", "expense", "cost_of_goods_sold", 0),
        ("Operating Expenses", "6000", "expense", "expense", 0),
        ("Accounts Payable", "2000", "liability", "payable", 0),
        ("Equity", "3000", "equity", "equity", 0),
    ]
    for name, acct_num, root_type, acct_type, is_group in accts:
        aid = _uuid()
        conn.execute(
            """INSERT INTO account (id, name, account_number, root_type,
               account_type, is_group, company_id)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (aid, name, acct_num, root_type, acct_type, is_group, company_id)
        )
        accounts[acct_type] = aid
    conn.commit()
    return accounts


def seed_gl_entries(conn, company_id: str, accounts: dict):
    """Seed a few GL entries for analytics to read."""
    # Revenue entry: debit cash, credit revenue
    for i in range(3):
        gl_id1 = _uuid()
        gl_id2 = _uuid()
        voucher_id = _uuid()
        amount = str(10000 + i * 5000)
        posting_date = f"2026-0{i+1}-15"
        conn.execute(
            """INSERT INTO gl_entry (id, posting_date, account_id, debit, credit,
               voucher_type, voucher_id, is_cancelled)
               VALUES (?, ?, ?, ?, '0', 'journal_entry', ?, 0)""",
            (gl_id1, posting_date, accounts["cash"], amount, voucher_id)
        )
        conn.execute(
            """INSERT INTO gl_entry (id, posting_date, account_id, debit, credit,
               voucher_type, voucher_id, is_cancelled)
               VALUES (?, ?, ?, '0', ?, 'journal_entry', ?, 0)""",
            (gl_id2, posting_date, accounts["revenue"], amount, voucher_id)
        )
    conn.commit()


def build_env(conn) -> dict:
    """Create a full analytics test environment with GL data."""
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    accounts = seed_accounts(conn, cid)
    seed_gl_entries(conn, cid, accounts)

    return {
        "company_id": cid,
        "accounts": accounts,
    }
