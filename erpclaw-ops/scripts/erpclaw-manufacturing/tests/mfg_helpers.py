"""Shared helper functions for ERPClaw Manufacturing L1 unit tests.

Provides:
  - DB bootstrap via init_schema.init_db()
  - load_db_query() for explicit module loading (avoids sys.path collisions)
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming_series, items, warehouses
  - build_env() for a complete manufacturing test environment
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
MODULE_DIR = os.path.dirname(TESTS_DIR)               # erpclaw-manufacturing/
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
    """Load erpclaw-manufacturing db_query.py explicitly."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_manufacturing", db_query_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for action_name, fn in mod.ACTIONS.items():
        setattr(mod, action_name.replace("-", "_"), fn)
    return mod


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def init_all_tables(db_path: str):
    """Create foundation tables (manufacturing tables are in init_schema)."""
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
        "item_id": None,
        "bom_id": None,
        "work_order_id": None,
        "job_card_id": None,
        "production_plan_id": None,
        "name": None,
        "description": None,
        "quantity": None,
        "produced_qty": None,
        "for_quantity": None,
        "completed_qty": None,
        "items": None,
        "operations": None,
        "routing_id": None,
        "operation_id": None,
        "workstation_id": None,
        "hour_rate": None,
        "time_in_mins": None,
        "actual_time_in_mins": None,
        "workstation_type": None,
        "working_hours_per_day": None,
        "production_capacity": None,
        "holiday_list_id": None,
        "planned_start_date": None,
        "planned_end_date": None,
        "posting_date": None,
        "source_warehouse_id": None,
        "target_warehouse_id": None,
        "wip_warehouse_id": None,
        "sales_order_id": None,
        "supplier_id": None,
        "service_item_id": None,
        "supplier_warehouse_id": None,
        "planning_horizon_days": None,
        "is_active": None,
        "is_default": None,
        "is_primary": None,
        "uom": None,
        "bom_item_id": None,
        "substitute_item_id": None,
        "conversion_factor": None,
        "priority": None,
        "cost_allocation_pct": None,
        "procurement_type": None,
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

def seed_company(conn, name="MFG Test Co", abbr="MTC") -> str:
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
    """Seed naming series for manufacturing entity types."""
    series = [
        ("bom", "BOM-", 0),
        ("work_order", "WO-", 0),
        ("job_card", "JC-", 0),
        ("production_plan", "PP-", 0),
        ("operation", "OP-", 0),
        ("workstation", "WS-", 0),
        ("routing", "RT-", 0),
        ("subcontracting_order", "SCO-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_item(conn, company_id: str, name="Widget A",
              stock_uom="Each", standard_rate="100.00") -> str:
    """Insert a test item and return its ID."""
    iid = _uuid()
    code = f"ITEM-{iid[:8]}"
    conn.execute(
        """INSERT INTO item (id, item_name, item_code, stock_uom,
           standard_rate, is_stock_item)
           VALUES (?, ?, ?, ?, ?, 1)""",
        (iid, f"{name} {iid[:6]}", code, stock_uom, standard_rate)
    )
    conn.commit()
    return iid


def seed_warehouse(conn, company_id: str, name="Main Store") -> str:
    """Insert a test warehouse and return its ID."""
    wid = _uuid()
    conn.execute(
        """INSERT INTO warehouse (id, name, company_id) VALUES (?, ?, ?)""",
        (wid, f"{name} {wid[:6]}", company_id)
    )
    conn.commit()
    return wid


def seed_account(conn, company_id: str, name="Test Account",
                 account_type="expense", root_type="Expense") -> str:
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


def build_env(conn) -> dict:
    """Create a complete manufacturing test environment.

    Returns dict with all IDs needed for manufacturing domain tests.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    fg_item = seed_item(conn, cid, name="Finished Good", standard_rate="500.00")
    rm_item1 = seed_item(conn, cid, name="Raw Material 1", standard_rate="50.00")
    rm_item2 = seed_item(conn, cid, name="Raw Material 2", standard_rate="75.00")
    wh_source = seed_warehouse(conn, cid, name="Raw Material Store")
    wh_target = seed_warehouse(conn, cid, name="Finished Goods Store")
    wh_wip = seed_warehouse(conn, cid, name="WIP Store")

    return {
        "company_id": cid,
        "fg_item_id": fg_item,
        "rm_item1_id": rm_item1,
        "rm_item2_id": rm_item2,
        "wh_source_id": wh_source,
        "wh_target_id": wh_target,
        "wh_wip_id": wh_wip,
    }
