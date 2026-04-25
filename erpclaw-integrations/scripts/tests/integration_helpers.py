"""Shared helper functions for ERPClaw Integrations L1 unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + create_integration_tables()
  - load_db_query() for explicit module loading (avoids sys.path collisions)
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming_series
  - build_env() for a complete integrations test environment
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
ROOT_DIR = os.path.dirname(MODULE_DIR)                 # erpclaw-integrations/
ADDONS_DIR = os.path.dirname(ROOT_DIR)                 # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)                  # source/

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
    """Load erpclaw-integrations db_query.py explicitly to avoid sys.path collisions."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_integrations", db_query_path)
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
    """Create foundation tables + integrations extension tables."""
    # Step 1: Foundation schema
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    schema_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_mod)
    schema_mod.init_db(db_path)

    # Step 2: Integrations extension tables
    spec2 = importlib.util.spec_from_file_location("integrations_init_db", VERTICAL_INIT_PATH)
    intg_mod = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(intg_mod)
    intg_mod.create_integration_tables(db_path)


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
        # Connector domain
        "connector_id": None,
        "name": None,
        "platform": None,
        "connector_type": None,
        "connector_status": None,
        "base_url": None,
        "config_json": None,
        # Credential domain
        "credential_id": None,
        "credential_type": None,
        "credential_key": None,
        "credential_value": None,
        "expires_at": None,
        # Webhook domain
        "webhook_id": None,
        "event_type": None,
        "webhook_url": None,
        "webhook_secret": None,
        # Sync domain
        "sync_id": None,
        "sync_type": None,
        "sync_status": None,
        "direction": None,
        "entity_type": None,
        "entity_id": None,
        "error_message": None,
        "error_id": None,
        "resolution_notes": None,
        "start_date": None,
        "end_date": None,
        # Schedule domain
        "schedule_id": None,
        "frequency": None,
        "next_run_at": None,
        "is_active": None,
        # Mapping domain
        "field_mapping_id": None,
        "source_field": None,
        "target_field": None,
        "transform_rule": None,
        "is_required": None,
        "default_value": None,
        # Entity map domain
        "entity_map_id": None,
        "local_id": None,
        "remote_id": None,
        # Transform rule domain
        "transform_rule_id": None,
        "rule_name": None,
        "rule_json": None,
        # Booking domain (connectors-v2)
        "property_id": None,
        "api_credentials_ref": None,
        "sync_reservations": None,
        "sync_rates": None,
        "sync_availability": None,
        "records_synced": None,
        "errors": None,
        # Delivery domain (connectors-v2)
        "store_id": None,
        "auto_accept": None,
        "sync_menu": None,
        "external_order_id": None,
        "order_data": None,
        "total_amount": None,
        "commission": None,
        "net_amount": None,
        "order_status": None,
        "order_id": None,
        # Real estate domain (connectors-v2)
        "agent_id": None,
        "sync_listings": None,
        "capture_leads": None,
        "lead_source": None,
        "contact_name": None,
        "contact_email": None,
        "contact_phone": None,
        "property_ref": None,
        "inquiry": None,
        # Financial domain (connectors-v2)
        "account_ref": None,
        "sync_enabled": None,
        "recipient": None,
        "message_body": None,
        "subject": None,
        # Productivity domain (connectors-v2)
        "workspace_id": None,
        "sync_calendar": None,
        "sync_contacts": None,
        "sync_files": None,
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

def seed_company(conn, name="Integration Test Co", abbr="ITC") -> str:
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
    """Seed naming series for integration entity types."""
    series = [
        ("integration_connector", "INT-", 0),
        ("integration_sync", "SYNC-", 0),
        ("connv2_booking_connector", "BKC-", 0),
        ("connv2_delivery_connector", "DLC-", 0),
        ("connv2_realestate_connector", "REC-", 0),
        ("connv2_financial_connector", "FNC-", 0),
        ("connv2_productivity_connector", "PDC-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_connector(conn, company_id: str, name="Test Shopify Connector",
                   platform="shopify") -> str:
    """Insert a core integration connector and return its ID."""
    from erpclaw_lib.naming import get_next_name
    cid = _uuid()
    naming = get_next_name(conn, "integration_connector", company_id=company_id)
    now = _now()
    conn.execute("""
        INSERT INTO integration_connector (
            id, naming_series, name, platform, connector_type, base_url,
            connector_status, config_json, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        cid, naming, name, platform, "bidirectional",
        "https://test.myshopify.com", "inactive", "{}",
        company_id, now, now,
    ))
    conn.commit()
    return cid


def build_env(conn) -> dict:
    """Create a complete integrations test environment.

    Returns dict with all IDs needed for integration domain tests.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    connector_id = seed_connector(conn, cid)

    return {
        "company_id": cid,
        "connector_id": connector_id,
    }
