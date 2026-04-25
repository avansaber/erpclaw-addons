"""Shared helper functions for ERPClaw Assets L1 unit tests.

Provides:
  - DB bootstrap via init_schema.init_db()
  - load_db_query() for explicit module loading (avoids sys.path collisions)
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming_series, accounts, asset categories
  - build_env() for a complete assets test environment
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
MODULE_DIR = os.path.dirname(TESTS_DIR)               # erpclaw-assets/
SCRIPTS_DIR = os.path.dirname(MODULE_DIR)              # scripts/
ROOT_DIR = os.path.dirname(SCRIPTS_DIR)                # erpclaw-ops/
ADDONS_DIR = os.path.dirname(ROOT_DIR)                 # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)                  # source/

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
    """Load erpclaw-assets db_query.py explicitly."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_assets", db_query_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for action_name, fn in mod.ACTIONS.items():
        setattr(mod, action_name.replace("-", "_"), fn)
    return mod


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def init_all_tables(db_path: str):
    """Create foundation tables (asset tables are in init_schema)."""
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
        # Asset category
        "name": None,
        "depreciation_method": None,
        "useful_life_years": None,
        "asset_account_id": None,
        "depreciation_account_id": None,
        "accumulated_depreciation_account_id": None,
        # Asset
        "asset_id": None,
        "asset_category_id": None,
        "item_id": None,
        "gross_value": None,
        "salvage_value": None,
        "purchase_date": None,
        "purchase_invoice_id": None,
        "depreciation_start_date": None,
        "location": None,
        "custodian_employee_id": None,
        "warranty_expiry_date": None,
        # Depreciation
        "depreciation_schedule_id": None,
        "posting_date": None,
        "cost_center_id": None,
        # Movement
        "movement_type": None,
        "movement_date": None,
        "from_location": None,
        "to_location": None,
        "from_employee_id": None,
        "to_employee_id": None,
        "reason": None,
        # Maintenance
        "maintenance_id": None,
        "maintenance_type": None,
        "scheduled_date": None,
        "actual_date": None,
        "cost": None,
        "performed_by": None,
        "description": None,
        "next_due_date": None,
        # Disposal
        "disposal_date": None,
        "disposal_method": None,
        "sale_amount": None,
        "buyer_details": None,
        # Reports
        "as_of_date": None,
        # Filters
        "status": None,
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

def seed_company(conn, name="Asset Test Co", abbr="ATC") -> str:
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
    """Seed naming series for asset entity types."""
    series = [
        ("asset", "AST-", 0),
        ("asset_category", "ACAT-", 0),
        ("asset_movement", "AMOV-", 0),
        ("asset_maintenance", "AMNT-", 0),
        ("asset_disposal", "ADSP-", 0),
        ("depreciation_schedule", "DEP-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_account(conn, company_id: str, name="Test Account",
                 account_type="asset", root_type="Asset") -> str:
    """Insert a test account and return its ID."""
    aid = _uuid()
    conn.execute(
        """INSERT INTO account (id, name, account_type, root_type,
           is_group, company_id)
           VALUES (?, ?, ?, ?, 0, ?)""",
        (aid, f"{name} {aid[:6]}", account_type, root_type, company_id)
    )
    conn.commit()
    return aid


def seed_asset_category(conn, company_id: str, name="Office Equipment") -> str:
    """Insert an asset category and return its ID."""
    cat_id = _uuid()
    conn.execute(
        """INSERT INTO asset_category (id, name, depreciation_method,
           useful_life_years, company_id)
           VALUES (?, ?, 'straight_line', 5, ?)""",
        (cat_id, f"{name} {cat_id[:6]}", company_id)
    )
    conn.commit()
    return cat_id


def seed_asset(conn, company_id: str, category_id: str,
               name="Laptop", gross_value="5000.00") -> str:
    """Insert a draft asset and return its ID."""
    from erpclaw_lib.naming import get_next_name
    aid = _uuid()
    naming = get_next_name(conn, "asset", company_id=company_id)
    conn.execute(
        """INSERT INTO asset (id, naming_series, asset_name, asset_category_id,
           gross_value, salvage_value, depreciation_method, useful_life_years,
           depreciation_start_date,
           current_book_value, accumulated_depreciation, status, company_id)
           VALUES (?, ?, ?, ?, ?, '0', 'straight_line', 5, '2026-01-01',
                   ?, '0', 'draft', ?)""",
        (aid, naming, f"{name} {aid[:6]}", category_id, gross_value,
         gross_value, company_id)
    )
    conn.commit()
    return aid


def build_env(conn) -> dict:
    """Create a complete assets test environment.

    Returns dict with all IDs needed for asset domain tests.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    cat_id = seed_asset_category(conn, cid)
    asset_id = seed_asset(conn, cid, cat_id)

    return {
        "company_id": cid,
        "category_id": cat_id,
        "asset_id": asset_id,
    }
