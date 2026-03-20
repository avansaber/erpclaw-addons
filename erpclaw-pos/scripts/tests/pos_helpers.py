"""Shared helper functions for ERPClaw POS unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + create_pos_tables()
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, items, naming series
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
ROOT_DIR = os.path.dirname(MODULE_DIR)            # erpclaw-pos/
ADDONS_DIR = os.path.dirname(ROOT_DIR)            # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)             # src/
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")
VERTICAL_INIT_PATH = os.path.join(ROOT_DIR, "init_db.py")

ERPCLAW_LIB = os.path.expanduser("~/.openclaw/erpclaw/lib")
if ERPCLAW_LIB not in sys.path:
    sys.path.insert(0, ERPCLAW_LIB)

from erpclaw_lib.db import setup_pragmas


def load_db_query():
    """Load erpclaw-pos db_query.py explicitly to avoid sys.path collisions."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_pos", db_query_path)
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
    """Create all ERPClaw core tables + POS vertical tables."""
    # 1. Foundation schema (company, account, naming_series, item, etc.)
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.init_db(db_path)

    # 2. POS vertical schema (5 tables)
    spec2 = importlib.util.spec_from_file_location("pos_init", VERTICAL_INIT_PATH)
    m2 = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(m2)
    m2.create_pos_tables(db_path)


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

def seed_company(conn, name="Test POS Co", abbr="TPC") -> str:
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


def seed_item(conn, name="Test Item", item_code="ITEM-001") -> str:
    """Insert a test item and return its ID."""
    iid = _uuid()
    conn.execute(
        """INSERT INTO item (id, item_name, item_code, stock_uom, is_stock_item, standard_rate)
           VALUES (?, ?, ?, 'Nos', 1, '10.00')""",
        (iid, name, f"{item_code}-{iid[:6]}")
    )
    conn.commit()
    return iid


def seed_naming_series(conn, company_id: str):
    """Seed naming series for POS entity types."""
    series = [
        ("pos_profile", "POS-", 0),
        ("pos_session", "POSS-", 0),
        ("pos_transaction", "PTXN-", 0),
        ("sales_invoice", "SINV-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_pos_profile(conn, company_id: str, name="Default POS") -> str:
    """Insert a POS profile and return its ID."""
    mod = load_db_query()
    r = call_action(mod.ACTIONS["pos-add-pos-profile"], conn, ns(
        company_id=company_id, name=name,
        warehouse_id=None, price_list_id=None,
        default_payment_method="cash",
        allow_discount="1", max_discount_pct="100",
        auto_print_receipt="0", is_active=None,
    ))
    assert is_ok(r), f"seed_pos_profile failed: {r}"
    return r["id"]


def seed_open_session(conn, profile_id: str, cashier="Test Cashier") -> str:
    """Open a POS session and return its ID."""
    mod = load_db_query()
    r = call_action(mod.ACTIONS["pos-open-session"], conn, ns(
        pos_profile_id=profile_id, cashier_name=cashier,
        opening_amount="100.00",
    ))
    assert is_ok(r), f"seed_open_session failed: {r}"
    return r["id"]


def build_env(conn) -> dict:
    """Create a full POS test environment.

    Returns dict with company_id, profile_id, session_id, item_id.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    item_id = seed_item(conn, "Widget A", "WDG-A")
    profile_id = seed_pos_profile(conn, cid)
    session_id = seed_open_session(conn, profile_id)
    return {
        "company_id": cid,
        "profile_id": profile_id,
        "session_id": session_id,
        "item_id": item_id,
    }
