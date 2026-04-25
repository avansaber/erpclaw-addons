"""Shared helper functions for ERPClaw Maintenance unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + init_maintenance_schema()
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming series
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
MODULE_DIR = os.path.dirname(TESTS_DIR)          # erpclaw-maintenance/scripts/
ROOT_DIR = os.path.dirname(MODULE_DIR)            # erpclaw-maintenance/
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
    """Load erpclaw-maintenance db_query.py explicitly to avoid sys.path collisions."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_maintenance", db_query_path)
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
    """Create all ERPClaw core tables + maintenance vertical tables."""
    # 1. Foundation schema (company, account, naming_series, etc.)
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.init_db(db_path)

    # 2. Maintenance vertical schema (11 tables)
    spec2 = importlib.util.spec_from_file_location("maintenance_init", VERTICAL_INIT_PATH)
    m2 = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(m2)
    m2.init_maintenance_schema(db_path)


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

def seed_company(conn, name="Test Maintenance Co", abbr="TMC") -> str:
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


def seed_naming_series(conn, company_id: str):
    """Seed naming series for maintenance entity types."""
    series = [
        ("equipment", "EQP-", 0),
        ("maintenance_plan", "MPL-", 0),
        ("maintenance_work_order", "MWO-", 0),
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
    """Create a full maintenance test environment.

    Returns dict with company_id and all naming series seeded.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    return {
        "company_id": cid,
    }
