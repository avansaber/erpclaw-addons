"""Shared helper functions for ERPClaw Advanced Manufacturing L1 unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + init_advmfg_schema()
  - load_db_query() for explicit module loading (avoids sys.path collisions)
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming_series
  - build_env() for a complete advmfg test environment
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
MODULE_DIR = os.path.dirname(TESTS_DIR)               # erpclaw-advmfg/
SCRIPTS_DIR = os.path.dirname(MODULE_DIR)              # scripts/
ROOT_DIR = os.path.dirname(SCRIPTS_DIR)                # erpclaw-ops/
ADDONS_DIR = os.path.dirname(ROOT_DIR)                 # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)                  # source/

# Foundation schema init
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")

# Parent ops init_db (creates advmfg tables)
VERTICAL_INIT_PATH = os.path.join(ROOT_DIR, "init_db.py")

# Make erpclaw_lib importable
ERPCLAW_LIB = os.path.expanduser("~/.openclaw/erpclaw/lib")
if ERPCLAW_LIB not in sys.path:
    sys.path.insert(0, ERPCLAW_LIB)

from erpclaw_lib.db import setup_pragmas

# Make module dir importable (so shop_floor, tools, eco, recipes resolve)
if MODULE_DIR not in sys.path:
    sys.path.insert(0, MODULE_DIR)


def load_db_query():
    """Load erpclaw-advmfg db_query.py explicitly."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_advmfg", db_query_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for action_name, fn in mod.ACTIONS.items():
        setattr(mod, action_name.replace("-", "_"), fn)
    return mod


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def init_all_tables(db_path: str):
    """Create foundation tables + advmfg extension tables."""
    # Step 1: Foundation schema
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    schema_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_mod)
    schema_mod.init_db(db_path)

    # Step 2: Advmfg extension tables
    spec2 = importlib.util.spec_from_file_location("advmfg_init_db", VERTICAL_INIT_PATH)
    init_mod = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(init_mod)
    init_mod.init_advmfg_schema(db_path)


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
        "limit": 50,
        "offset": 0,
        "company_id": None,
        "search": None,
        "notes": None,
        # Shop Floor fields
        "entry_id": None,
        "equipment_id": None,
        "work_order_id": None,
        "operator": None,
        "entry_type": None,
        "start_time": None,
        "machine_status": None,
        "batch_number": None,
        "serial_number": None,
        "quantity_produced": None,
        "quantity_rejected": None,
        # Tool fields
        "tool_id": None,
        "name": None,
        "tool_type": None,
        "tool_code": None,
        "manufacturer": None,
        "model": None,
        "location": None,
        "purchase_date": None,
        "purchase_cost": None,
        "max_usage_count": None,
        "calibration_due": None,
        "condition": None,
        "tool_status": None,
        "usage_count": None,
        "usage_duration_minutes": None,
        "condition_after": None,
        # ECO fields
        "eco_id": None,
        "title": None,
        "eco_type": None,
        "description": None,
        "reason": None,
        "affected_items": None,
        "affected_boms": None,
        "impact_analysis": None,
        "requested_by": None,
        "approved_by": None,
        "priority": None,
        "implementation_date": None,
        "eco_status": None,
        # Recipe fields
        "recipe_id": None,
        "product_name": None,
        "recipe_type": None,
        "version": None,
        "batch_size": None,
        "batch_unit": None,
        "expected_yield": None,
        "instructions": None,
        "is_active": None,
        "ingredient_id": None,
        "ingredient_name": None,
        "item_id": None,
        "quantity": None,
        "unit": None,
        "sequence": None,
        "is_optional": None,
        # Shared
        "start_date": None,
        "end_date": None,
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

def seed_company(conn, name="AdvMfg Test Co", abbr="AMC") -> str:
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
    """Seed naming series for advmfg entity types."""
    series = [
        ("shop_floor_entry", "SFE-", 0),
        ("tool", "TOOL-", 0),
        ("engineering_change_order", "ECO-", 0),
        ("process_recipe", "RCPE-", 0),
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
    """Create a complete advmfg test environment.

    Returns dict with all IDs needed for advmfg domain tests.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)

    return {
        "company_id": cid,
    }
