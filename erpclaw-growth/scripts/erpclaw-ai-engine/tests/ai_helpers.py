"""Shared helper functions for ERPClaw AI Engine unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + create_crmadv_tables()
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming series, accounts, GL data
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
MODULE_DIR = os.path.dirname(TESTS_DIR)                    # erpclaw-ai-engine/
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
    """Load erpclaw-ai-engine db_query.py explicitly to avoid sys.path collisions."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_ai_engine", db_query_path)
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
    """Create all foundation + growth tables + missing AI engine tables."""
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.init_db(db_path)

    spec2 = importlib.util.spec_from_file_location("growth_init", VERTICAL_INIT_PATH)
    m2 = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(m2)
    m2.create_crmadv_tables(db_path)

    # cash_flow_forecast table referenced by AI engine but not yet in growth init_db
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cash_flow_forecast (
            id                  TEXT PRIMARY KEY,
            forecast_date       TEXT NOT NULL,
            generated_at        TEXT NOT NULL,
            horizon_days        INTEGER NOT NULL,
            starting_balance    TEXT NOT NULL DEFAULT '0',
            projected_inflows   TEXT,
            projected_outflows  TEXT,
            projected_balance   TEXT NOT NULL DEFAULT '0',
            confidence_interval TEXT,
            assumptions         TEXT,
            scenario            TEXT NOT NULL DEFAULT 'expected'
        )
    """)
    conn.commit()
    conn.close()


class _DecimalSum:
    """Custom SQLite aggregate: SUM using Python Decimal for precision."""
    def __init__(self):
        self.total = Decimal("0")
    def step(self, value):
        if value is not None:
            self.total += Decimal(str(value))
    def finalize(self):
        return str(self.total)


def _decimal_abs(value):
    """SQLite function for decimal absolute value."""
    if value is None:
        return "0"
    return str(abs(Decimal(str(value))))


class _ConnWrapper:
    """Wrap a sqlite3.Connection so conn.company_id is accessible."""
    def __init__(self, conn):
        self._conn = conn
        self.company_id = None

    def __getattr__(self, name):
        return getattr(self._conn, name)


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

def seed_company(conn, name="Test AI Co", abbr="TAI") -> str:
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
    """Seed GL entries for anomaly detection to scan."""
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


def seed_asset_category(conn, company_id: str, name="Equipment") -> str:
    """Insert an asset_category and return its ID."""
    acid = _uuid()
    conn.execute(
        """INSERT INTO asset_category (id, name, depreciation_method,
           useful_life_years, company_id)
           VALUES (?, ?, 'straight_line', 5, ?)""",
        (acid, f"{name} {acid[:6]}", company_id),
    )
    conn.commit()
    return acid


def seed_asset(conn, company_id: str, asset_category_id: str, *,
               gross_value: str, accumulated_depreciation: str,
               current_book_value: str, status: str = "in_use",
               name: str = "Server") -> str:
    """Insert an asset row with explicit book-value figures. Returns asset ID."""
    aid = _uuid()
    conn.execute(
        """INSERT INTO asset (id, asset_name, asset_category_id, gross_value,
           salvage_value, accumulated_depreciation, current_book_value,
           status, company_id)
           VALUES (?, ?, ?, ?, '0', ?, ?, ?, ?)""",
        (aid, f"{name} {aid[:6]}", asset_category_id, gross_value,
         accumulated_depreciation, current_book_value, status, company_id),
    )
    conn.commit()
    return aid


def seed_gl_with_dimensions(conn, company_id: str, account_id: str,
                            dimensions_list: list):
    """Seed one GL entry per entry in dimensions_list (a list of dicts) on the
    given account. Each dict is serialized into gl_entry.dimensions_json; pass
    ``{}`` for an untagged entry. Posting date is fixed inside 2026-Q1."""
    for i, dims in enumerate(dimensions_list):
        conn.execute(
            """INSERT INTO gl_entry (id, posting_date, account_id, debit, credit,
               voucher_type, voucher_id, is_cancelled, dimensions_json)
               VALUES (?, ?, ?, ?, '0', 'journal_entry', ?, 0, ?)""",
            (_uuid(), f"2026-02-{(i % 27) + 1:02d}", account_id,
             str(1000 + i), _uuid(), json.dumps(dims)),
        )
    conn.commit()


def seed_item(conn, *, name="Widget", is_stock_item=1) -> str:
    """Insert an item and return its ID."""
    iid = _uuid()
    conn.execute(
        """INSERT INTO item (id, item_code, item_name, is_stock_item, stock_uom)
           VALUES (?, ?, ?, ?, 'Nos')""",
        (iid, f"{name}-{iid[:8]}", f"{name} {iid[:6]}", is_stock_item),
    )
    conn.commit()
    return iid


def seed_warehouse(conn, company_id: str, name="Main WH") -> str:
    """Insert a warehouse and return its ID."""
    wid = _uuid()
    conn.execute(
        "INSERT INTO warehouse (id, name, warehouse_type, company_id) "
        "VALUES (?, ?, 'stores', ?)",
        (wid, f"{name} {wid[:6]}", company_id),
    )
    conn.commit()
    return wid


def seed_sle(conn, item_id: str, warehouse_id: str, *, actual_qty: str,
             posting_date: str = "2026-01-10") -> str:
    """Insert a single (non-cancelled) stock_ledger_entry contributing actual_qty
    to the item/warehouse on-hand balance. Returns the SLE ID."""
    sid = _uuid()
    conn.execute(
        """INSERT INTO stock_ledger_entry
           (id, posting_date, item_id, warehouse_id, actual_qty,
            qty_after_transaction, voucher_type, voucher_id, is_cancelled)
           VALUES (?, ?, ?, ?, ?, ?, 'stock_entry', ?, 0)""",
        (sid, posting_date, item_id, warehouse_id, actual_qty, actual_qty, _uuid()),
    )
    conn.commit()
    return sid


def seed_reservation(conn, company_id: str, item_id: str, warehouse_id: str, *,
                     reserved_qty: str, status: str = "active") -> str:
    """Insert a stock_reservation_entry (M5). Returns the reservation ID."""
    rid = _uuid()
    conn.execute(
        """INSERT INTO stock_reservation_entry
           (id, voucher_type, voucher_id, item_id, warehouse_id, reserved_qty,
            status, company_id)
           VALUES (?, 'manual', ?, ?, ?, ?, ?, ?)""",
        (rid, _uuid(), item_id, warehouse_id, reserved_qty, status, company_id),
    )
    conn.commit()
    return rid


def seed_subcontracting_order(conn, company_id: str, *, qty: str,
                              materials_transferred: str, received_qty: str,
                              status: str = "partially_received") -> str:
    """Insert a subcontracting_order (S5) with explicit transferred/received
    figures, seeding its NOT NULL supplier / service item / finished item / BOM
    prerequisites. Returns the subcontracting_order ID."""
    supplier_id = _uuid()
    conn.execute(
        "INSERT INTO supplier (id, name, company_id) VALUES (?, ?, ?)",
        (supplier_id, f"Subco {supplier_id[:6]}", company_id),
    )
    service_item_id = seed_item(conn, name="Assembly Service", is_stock_item=0)
    finished_item_id = seed_item(conn, name="Finished Good", is_stock_item=1)
    bom_id = _uuid()
    conn.execute(
        "INSERT INTO bom (id, item_id, quantity, company_id) VALUES (?, ?, '1', ?)",
        (bom_id, finished_item_id, company_id),
    )
    oid = _uuid()
    conn.execute(
        """INSERT INTO subcontracting_order
           (id, supplier_id, service_item_id, finished_item_id, bom_id, qty,
            status, materials_transferred, received_qty, company_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (oid, supplier_id, service_item_id, finished_item_id, bom_id, qty,
         status, materials_transferred, received_qty, company_id),
    )
    conn.commit()
    return oid


def build_env(conn) -> dict:
    """Create a full AI engine test environment with GL data."""
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    accounts = seed_accounts(conn, cid)
    seed_gl_entries(conn, cid, accounts)

    return {
        "company_id": cid,
        "accounts": accounts,
    }
