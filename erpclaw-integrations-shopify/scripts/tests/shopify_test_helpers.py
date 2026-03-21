"""Shared helper functions for ERPClaw Integrations Shopify L1 unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + create_shopify_tables()
    + create_integration_tables() (for integration_entity_map)
  - load_db_query() for explicit module loading
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, accounts, fiscal year, cost center,
    items, customers, orders, refunds, payouts, disputes
  - build_env() for a complete Shopify test environment
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
ROOT_DIR = os.path.dirname(MODULE_DIR)                 # erpclaw-integrations-shopify/
ADDONS_DIR = os.path.dirname(ROOT_DIR)                 # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)                  # src/

# Foundation schema init
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")

# Shopify schema init
SHOPIFY_INIT_PATH = os.path.join(ROOT_DIR, "init_db.py")

# Integration schema init (for integration_entity_map table)
INTEGRATIONS_DIR = os.path.join(ADDONS_DIR, "erpclaw-integrations")
INTEGRATIONS_INIT_PATH = os.path.join(INTEGRATIONS_DIR, "init_db.py")

# Make erpclaw_lib importable
ERPCLAW_LIB = os.path.expanduser("~/.openclaw/erpclaw/lib")
if ERPCLAW_LIB not in sys.path:
    sys.path.insert(0, ERPCLAW_LIB)

from erpclaw_lib.db import setup_pragmas

# Make scripts dir importable so domain modules resolve
if MODULE_DIR not in sys.path:
    sys.path.insert(0, MODULE_DIR)


def load_db_query():
    """Load erpclaw-integrations-shopify db_query.py explicitly."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location(
        "db_query_shopify", db_query_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # Attach action functions as underscore-named attributes
    for action_name, fn in mod.ACTIONS.items():
        setattr(mod, action_name.replace("-", "_"), fn)
    return mod


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def init_all_tables(db_path: str):
    """Create foundation tables + integration tables + shopify tables."""
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    schema_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_mod)
    schema_mod.init_db(db_path)

    if os.path.exists(INTEGRATIONS_INIT_PATH):
        spec2 = importlib.util.spec_from_file_location(
            "integrations_init_db", INTEGRATIONS_INIT_PATH)
        intg_mod = importlib.util.module_from_spec(spec2)
        spec2.loader.exec_module(intg_mod)
        intg_mod.create_integration_tables(db_path)

    spec3 = importlib.util.spec_from_file_location(
        "shopify_init_db", SHOPIFY_INIT_PATH)
    shopify_mod = importlib.util.module_from_spec(spec3)
    spec3.loader.exec_module(shopify_mod)
    shopify_mod.create_shopify_tables(db_path)


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
        "limit": 20, "offset": 0, "company_id": None, "status": None,
        "shopify_account_id": None, "shop_domain": None, "shop_name": None,
        "access_token": None, "api_version": None, "currency": None,
        "discount_method": None, "auto_post_gl": None, "track_cogs": None,
        "clearing_account_id": None, "revenue_account_id": None,
        "shipping_revenue_account_id": None, "tax_payable_account_id": None,
        "cogs_account_id": None, "inventory_account_id": None,
        "fee_account_id": None, "discount_account_id": None,
        "refund_account_id": None, "chargeback_account_id": None,
        "chargeback_fee_account_id": None,
        "gift_card_liability_account_id": None,
        "reserve_account_id": None, "bank_account_id": None,
        "sync_job_id": None, "sync_type": None, "sync_mode": None,
        "webhook_topic": None, "webhook_data": None,
        "shopify_product_id": None, "shopify_customer_id": None,
        "item_id": None, "customer_id": None,
        "gl_rule_id": None, "rule_name": None, "transaction_type": None,
        "debit_account_id": None, "credit_account_id": None, "priority": None,
        "shopify_order_id": None, "shopify_refund_id": None,
        "shopify_payout_id": None, "shopify_dispute_id": None,
        "gift_card_type": None, "reserve_type": None,
        "date_from": None, "date_to": None,
        "period_start": None, "period_end": None,
        "bank_reference": None, "reconciliation_id": None,
        # BROWSE domain
        "shopify_order_id_local": None, "shopify_refund_id_local": None,
        "shopify_dispute_id_local": None,
        "financial_status": None, "gl_status": None,
        "payout_status": None, "dispute_status": None,
        # REPORTS domain
        "period": None,
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


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def seed_company(conn, name="Shopify Test Co", abbr="STC") -> str:
    cid = _uuid()
    conn.execute(
        """INSERT INTO company (id, name, abbr, default_currency, country,
           fiscal_year_start_month)
           VALUES (?, ?, ?, 'USD', 'United States', 1)""",
        (cid, f"{name} {cid[:6]}", f"{abbr}{cid[:4]}")
    )
    conn.commit()
    return cid


def seed_fiscal_year(conn, company_id: str) -> str:
    fy_id = _uuid()
    year = datetime.now().year
    # Use UUID suffix to avoid UNIQUE constraint on name when called multiple times
    conn.execute(
        """INSERT INTO fiscal_year (id, name, start_date, end_date,
           is_closed, company_id)
           VALUES (?, ?, ?, ?, 0, ?)""",
        (fy_id, f"FY-{year}-{fy_id[:6]}", f"{year}-01-01", f"{year}-12-31",
         company_id)
    )
    conn.commit()
    return fy_id


def seed_cost_center(conn, company_id: str, name="Main") -> str:
    cc_id = _uuid()
    conn.execute(
        """INSERT INTO cost_center (id, name, company_id, is_group)
           VALUES (?, ?, ?, 0)""",
        (cc_id, f"{name} {cc_id[:6]}", company_id)
    )
    conn.commit()
    return cc_id


def seed_gl_account(conn, company_id: str, name: str, root_type: str,
                    account_type: str,
                    balance_direction: str = "debit_normal") -> str:
    acct_id = _uuid()
    conn.execute(
        """INSERT INTO account (id, name, root_type, account_type, currency,
           is_group, balance_direction, company_id, created_at, updated_at)
           VALUES (?, ?, ?, ?, 'USD', 0, ?, ?, ?, ?)""",
        (acct_id, f"{name} {acct_id[:6]}", root_type, account_type,
         balance_direction, company_id, _now(), _now())
    )
    conn.commit()
    return acct_id


def seed_item(conn, company_id: str, item_code: str,
              item_name: str = None, barcode: str = None,
              name: str = None) -> str:
    item_id = _uuid()
    display_name = item_name or name or item_code
    conn.execute(
        """INSERT INTO item (id, item_code, item_name, item_type,
           is_stock_item, created_at, updated_at)
           VALUES (?, ?, ?, 'stock', 1, ?, ?)""",
        (item_id, item_code, display_name, _now(), _now())
    )
    if barcode:
        conn.execute(
            "UPDATE item SET barcode = ? WHERE id = ?", (barcode, item_id))
    conn.commit()
    return item_id


def seed_customer(conn, company_id: str, name: str) -> str:
    cust_id = _uuid()
    conn.execute(
        """INSERT INTO customer (id, name, customer_type, territory,
           company_id, created_at, updated_at)
           VALUES (?, ?, 'individual', 'US', ?, ?, ?)""",
        (cust_id, name, company_id, _now(), _now())
    )
    conn.commit()
    return cust_id


def seed_shopify_account(conn, company_id: str, **kwargs) -> dict:
    """Create a shopify_account with all 14 GL accounts using add-account."""
    mod = load_db_query()
    result = call_action(mod.shopify_add_account, conn, ns(
        company_id=company_id,
        shop_domain=kwargs.get("shop_domain", "test-shop.myshopify.com"),
        access_token=kwargs.get("access_token", "shpat_test_token_123456789"),
        shop_name=kwargs.get("shop_name", "Test Shop"),
    ))
    assert is_ok(result), f"Failed to create shopify account: {result}"

    acct_row = conn.execute(
        "SELECT * FROM shopify_account WHERE id = ?",
        (result["id"],)
    ).fetchone()
    acct_dict = dict(acct_row)
    acct_dict["shopify_account_id"] = result["id"]
    return acct_dict


def seed_shopify_order(conn, shopify_account_id: str, company_id: str,
                       shopify_order_id: str = "1001",
                       subtotal: str = "100.00", shipping: str = "10.00",
                       tax: str = "8.00", discount: str = "0",
                       total: str = None,
                       gl_status: str = "pending") -> str:
    order_id = _uuid()
    now = _now()
    total_val = total or str(
        Decimal(subtotal) + Decimal(shipping) + Decimal(tax) - Decimal(discount))
    conn.execute(
        """INSERT INTO shopify_order (
            id, shopify_account_id, shopify_order_id, shopify_order_number,
            order_date, financial_status, fulfillment_status, currency,
            subtotal_amount, shipping_amount, tax_amount, discount_amount,
            total_amount, refunded_amount, gl_status, payment_gateway,
            is_gift_card_order, has_refunds, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (order_id, shopify_account_id, shopify_order_id, f"#{shopify_order_id}",
         _today(), "PAID", "UNFULFILLED", "USD",
         subtotal, shipping, tax, discount,
         total_val, "0", gl_status, "shopify_payments",
         0, 0, company_id, now, now)
    )
    conn.commit()
    return order_id


def seed_shopify_order_line_item(conn, order_id: str, company_id: str,
                                  sku: str = "SKU-001", quantity: int = 1,
                                  unit_price: str = "100.00",
                                  item_id: str = None,
                                  is_gift_card: int = 0) -> str:
    li_id = _uuid()
    total = str(Decimal(unit_price) * quantity)
    conn.execute(
        """INSERT INTO shopify_order_line_item (
            id, shopify_order_id_local, shopify_line_item_id, title, sku,
            quantity, unit_price, discount_amount, tax_amount, total_amount,
            item_id, is_gift_card, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (li_id, order_id, _uuid()[:12], f"Product {sku}", sku,
         quantity, unit_price, "0", "0", total,
         item_id, is_gift_card, company_id, _now())
    )
    conn.commit()
    return li_id


def seed_shopify_refund(conn, order_id: str, company_id: str,
                         refund_amount: str = "50.00",
                         tax_refund: str = "0",
                         shipping_refund: str = "0",
                         gl_status: str = "pending") -> str:
    refund_id = _uuid()
    conn.execute(
        """INSERT INTO shopify_refund (
            id, shopify_order_id_local, shopify_refund_id, refund_date,
            refund_amount, tax_refund_amount, shipping_refund_amount,
            refund_type, gl_status, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (refund_id, order_id, _uuid()[:12], _today(),
         refund_amount, tax_refund, shipping_refund,
         "partial", gl_status, company_id, _now())
    )
    conn.commit()
    return refund_id


def seed_shopify_payout(conn, shopify_account_id: str, company_id: str,
                         gross: str = "1000.00", fee: str = "29.00",
                         net: str = None, gl_status: str = "pending",
                         reserved_funds_gross: str = "0") -> str:
    payout_id = _uuid()
    net_val = net or str(Decimal(gross) - Decimal(fee))
    conn.execute(
        """INSERT INTO shopify_payout (
            id, shopify_account_id, shopify_payout_id, issued_at, status,
            gross_amount, fee_amount, net_amount,
            charges_gross, charges_fee, refunds_gross, refunds_fee,
            adjustments_gross, adjustments_fee,
            reserved_funds_gross, reserved_funds_fee,
            gl_status, reconciliation_status,
            company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (payout_id, shopify_account_id, _uuid()[:12], _today(), "paid",
         gross, fee, net_val,
         gross, fee, "0", "0", "0", "0",
         reserved_funds_gross, "0",
         gl_status, "unreconciled",
         company_id, _now())
    )
    conn.commit()
    return payout_id


def seed_shopify_dispute(conn, shopify_account_id: str, company_id: str,
                          amount: str = "50.00", fee_amount: str = "15.00",
                          status: str = "needs_response",
                          gl_status: str = "pending",
                          order_id: str = None) -> str:
    dispute_id = _uuid()
    conn.execute(
        """INSERT INTO shopify_dispute (
            id, shopify_account_id, shopify_dispute_id,
            shopify_order_id_local, dispute_type, status, amount,
            fee_amount, reason, evidence_due_by, gl_status,
            company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (dispute_id, shopify_account_id, _uuid()[:12],
         order_id, "chargeback", status, amount,
         fee_amount, "fraudulent", _today(), gl_status,
         company_id, _now())
    )
    conn.commit()
    return dispute_id


def seed_naming_series(conn, company_id: str):
    series = [
        ("shopify_account", "SHPFY-", 0),
        ("shopify_sync_job", "SHPSYNC-", 0),
        ("shopify_reconciliation_run", "SHPRECON-", 0),
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
    """Create a complete Shopify test environment."""
    cid = seed_company(conn)
    fy_id = seed_fiscal_year(conn, cid)
    cc_id = seed_cost_center(conn, cid)
    seed_naming_series(conn, cid)
    acct = seed_shopify_account(conn, cid)

    return {
        "company_id": cid,
        "fiscal_year_id": fy_id,
        "cost_center_id": cc_id,
        "shopify_account": acct,
        "shopify_account_id": acct["shopify_account_id"],
    }


# ---------------------------------------------------------------------------
# Aliases for backward compatibility with parallel sprint tests
# ---------------------------------------------------------------------------
build_shopify_env = build_env
seed_erpclaw_customer = seed_customer
seed_erpclaw_item = seed_item
