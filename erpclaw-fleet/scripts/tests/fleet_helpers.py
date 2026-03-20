"""Shared helper functions for ERPClaw Fleet L1 unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + create_fleet_tables()
  - load_db_query() for explicit module loading (avoids sys.path collisions)
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming_series
  - build_env() for a complete fleet test environment
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
MODULE_DIR = os.path.dirname(TESTS_DIR)               # scripts/
ROOT_DIR = os.path.dirname(MODULE_DIR)                 # erpclaw-fleet/
ADDONS_DIR = os.path.dirname(ROOT_DIR)                 # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)                  # src/

# Foundation schema init
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")

# Vertical schema init
VERTICAL_INIT_PATH = os.path.join(ROOT_DIR, "init_db.py")

# Make erpclaw_lib importable
ERPCLAW_LIB = os.path.expanduser("~/.openclaw/erpclaw/lib")
if ERPCLAW_LIB not in sys.path:
    sys.path.insert(0, ERPCLAW_LIB)

from erpclaw_lib.db import setup_pragmas

# Make scripts dir importable so domain modules resolve
if MODULE_DIR not in sys.path:
    sys.path.insert(0, MODULE_DIR)


def load_db_query():
    """Load erpclaw-fleet db_query.py explicitly to avoid sys.path collisions."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_fleet", db_query_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # Attach action functions as underscore-named attributes for convenience
    for action_name, fn in mod.ACTIONS.items():
        setattr(mod, action_name.replace("-", "_"), fn)
    return mod


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def init_all_tables(db_path: str):
    """Create foundation tables + fleet extension tables."""
    # Step 1: Foundation schema
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    schema_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_mod)
    schema_mod.init_db(db_path)

    # Step 2: Fleet extension tables
    spec2 = importlib.util.spec_from_file_location("fleet_init_db", VERTICAL_INIT_PATH)
    fleet_mod = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(fleet_mod)
    fleet_mod.create_fleet_tables(db_path)


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
        "limit": 20,
        "offset": 0,
        "company_id": None,
        "search": None,
        "notes": None,
        # Vehicle fields
        "vehicle_id": None,
        "make": None,
        "model": None,
        "year": None,
        "vin": None,
        "license_plate": None,
        "vehicle_type": None,
        "color": None,
        "purchase_date": None,
        "purchase_cost": None,
        "current_odometer": None,
        "fuel_type": None,
        "insurance_provider": None,
        "insurance_policy": None,
        "insurance_expiry": None,
        "vehicle_status": None,
        # Assignment fields
        "assignment_id": None,
        "driver_name": None,
        "driver_id": None,
        "start_date": None,
        "end_date": None,
        "assignment_status": None,
        # Fuel log fields
        "log_date": None,
        "gallons": None,
        "cost": None,
        "odometer_reading": None,
        "station": None,
        # Maintenance fields
        "maintenance_id": None,
        "maintenance_type": None,
        "scheduled_date": None,
        "completed_date": None,
        "vendor": None,
        "odometer_at_service": None,
        "maintenance_status": None,
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

def seed_company(conn, name="Fleet Test Co", abbr="FTC") -> str:
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
    """Seed naming series for fleet entity types."""
    series = [
        ("fleet_vehicle", "VEH-", 0),
        ("fleet_vehicle_maintenance", "FMNT-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_vehicle(conn, company_id: str, make="Toyota", model_name="Camry",
                 vehicle_type="sedan") -> str:
    """Insert a fleet vehicle and return its ID."""
    from erpclaw_lib.naming import get_next_name
    vid = _uuid()
    naming = get_next_name(conn, "fleet_vehicle", company_id=company_id)
    now = _now()
    conn.execute("""
        INSERT INTO fleet_vehicle (
            id, naming_series, make, model, year, vin, license_plate,
            vehicle_type, color, purchase_date, purchase_cost,
            current_odometer, fuel_type, vehicle_status,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        vid, naming, make, model_name, 2025, f"VIN-{vid[:8]}",
        f"PLT-{vid[:6]}", vehicle_type, "white",
        "2025-01-01", "25000.00", "0", "gasoline",
        "available", company_id, now, now,
    ))
    conn.commit()
    return vid


def build_env(conn) -> dict:
    """Create a complete fleet test environment.

    Returns dict with all IDs needed for fleet domain tests.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    vid = seed_vehicle(conn, cid)

    return {
        "company_id": cid,
        "vehicle_id": vid,
    }
