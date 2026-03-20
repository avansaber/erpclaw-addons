"""Shared helper functions for ERPClaw Support L1 unit tests.

Provides:
  - DB bootstrap via init_schema.init_db()
  - load_db_query() for explicit module loading (avoids sys.path collisions)
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming_series, customers, SLAs, issues
  - build_env() for a complete support test environment
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
MODULE_DIR = os.path.dirname(TESTS_DIR)               # erpclaw-support/
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
    """Load erpclaw-support db_query.py explicitly."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_support", db_query_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for action_name, fn in mod.ACTIONS.items():
        setattr(mod, action_name.replace("-", "_"), fn)
    return mod


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def init_all_tables(db_path: str):
    """Create foundation tables + maintenance_schedule/visit (from erpclaw-maintenance)."""
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    schema_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_mod)
    schema_mod.init_db(db_path)

    # Support module references maintenance_schedule / maintenance_visit
    # from the erpclaw-maintenance addon. Create them here for tests.
    conn = sqlite3.connect(db_path)
    setup_pragmas(conn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS maintenance_schedule (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            customer_id     TEXT,
            item_id         TEXT,
            serial_number_id TEXT,
            schedule_frequency TEXT NOT NULL DEFAULT 'quarterly'
                            CHECK(schedule_frequency IN ('monthly','quarterly','semi_annual','annual')),
            start_date      TEXT NOT NULL,
            end_date        TEXT NOT NULL,
            last_completed_date TEXT,
            next_due_date   TEXT,
            status          TEXT NOT NULL DEFAULT 'active'
                            CHECK(status IN ('active','expired','cancelled')),
            assigned_to     TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_sched_customer ON maintenance_schedule(customer_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_sched_status ON maintenance_schedule(status)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS maintenance_visit (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            maintenance_schedule_id TEXT NOT NULL REFERENCES maintenance_schedule(id) ON DELETE RESTRICT,
            customer_id     TEXT,
            visit_date      TEXT NOT NULL,
            completed_by    TEXT,
            observations    TEXT,
            work_done       TEXT,
            status          TEXT NOT NULL DEFAULT 'scheduled'
                            CHECK(status IN ('scheduled','completed','cancelled')),
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_visit_schedule ON maintenance_visit(maintenance_schedule_id)")
    conn.commit()
    conn.close()


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
        # Issue
        "issue_id": None,
        "subject": None,
        "description": None,
        "priority": None,
        "issue_type": None,
        "assigned_to": None,
        "resolution_notes": None,
        "reason": None,
        # SLA
        "sla_id": None,
        "name": None,
        "priorities": None,
        "working_hours": None,
        "is_default": None,
        # Comment
        "comment": None,
        "comment_by": None,
        "is_internal": None,
        # Customer / item / serial
        "customer_id": None,
        "item_id": None,
        "serial_number_id": None,
        # Warranty
        "warranty_claim_id": None,
        "warranty_expiry_date": None,
        "complaint_description": None,
        "resolution": None,
        "resolution_date": None,
        "cost": None,
        # Maintenance schedule
        "schedule_id": None,
        "schedule_frequency": None,
        "start_date": None,
        "end_date": None,
        "visit_date": None,
        "completed_by": None,
        "observations": None,
        "work_done": None,
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

def seed_company(conn, name="Support Test Co", abbr="STC") -> str:
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
    """Seed naming series for support entity types."""
    series = [
        ("issue", "ISS-", 0),
        ("warranty_claim", "WCL-", 0),
        ("maintenance_schedule", "MSC-", 0),
        ("maintenance_visit", "MVS-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_customer(conn, company_id: str, name="Support Client") -> str:
    """Insert a test customer and return its ID."""
    cust_id = _uuid()
    conn.execute(
        """INSERT INTO customer (id, name, customer_type, status, company_id)
           VALUES (?, ?, 'company', 'active', ?)""",
        (cust_id, f"{name} {cust_id[:6]}", company_id)
    )
    conn.commit()
    return cust_id


def seed_item(conn, company_id: str, name="Support Item") -> str:
    """Insert a test item and return its ID."""
    iid = _uuid()
    code = f"ITEM-{iid[:8]}"
    conn.execute(
        """INSERT INTO item (id, item_name, item_code, stock_uom, is_stock_item)
           VALUES (?, ?, ?, 'Each', 1)""",
        (iid, f"{name} {iid[:6]}", code)
    )
    conn.commit()
    return iid


def build_env(conn) -> dict:
    """Create a complete support test environment.

    Returns dict with all IDs needed for support domain tests.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    cust_id = seed_customer(conn, cid)
    item_id = seed_item(conn, cid)

    return {
        "company_id": cid,
        "customer_id": cust_id,
        "item_id": item_id,
    }
