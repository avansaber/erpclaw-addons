"""Shared helper functions for erpclaw-integrations-stripe unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + create_stripe_tables()
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, account
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
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)                     # scripts/
MODULE_DIR = os.path.dirname(SCRIPTS_DIR)                     # erpclaw-integrations-stripe/
ADDONS_DIR = os.path.dirname(MODULE_DIR)                      # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)                         # src/

# Foundation schema init
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")

# Vertical schema init
STRIPE_INIT_PATH = os.path.join(MODULE_DIR, "init_db.py")

# Make erpclaw_lib importable
ERPCLAW_LIB = os.path.expanduser("~/.openclaw/erpclaw/lib")
if ERPCLAW_LIB not in sys.path:
    sys.path.insert(0, ERPCLAW_LIB)

# Make scripts dir importable for domain modules
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from erpclaw_lib.db import setup_pragmas


# ──────────────────────────────────────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────────────────────────────────────

def init_all_tables(db_path: str):
    """Create all foundation + stripe integration tables."""
    # Foundation tables (company, customer, employee, naming_series, audit_log, etc.)
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.init_db(db_path)

    # Stripe integration tables (17 tables)
    spec2 = importlib.util.spec_from_file_location("stripe_init", STRIPE_INIT_PATH)
    m2 = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(m2)
    m2.create_stripe_tables(db_path)


class _DecimalSum:
    """Custom SQLite aggregate: SUM using Python Decimal for precision."""
    def __init__(self):
        self.total = Decimal("0")
    def step(self, value):
        if value is not None:
            self.total += Decimal(str(value))
    def finalize(self):
        return str(self.total)


def get_conn(db_path: str) -> sqlite3.Connection:
    """Return a sqlite3.Connection with FK enabled and Row factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    setup_pragmas(conn)
    conn.create_aggregate("decimal_sum", 1, _DecimalSum)
    return conn


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
    """Check if a call_action result is an error response."""
    return result.get("status") == "error"


def is_ok(result: dict) -> bool:
    """Check if a call_action result is a success response."""
    return result.get("status") == "ok"


# ──────────────────────────────────────────────────────────────────────────────
# Utility
# ──────────────────────────────────────────────────────────────────────────────

def _uuid() -> str:
    return str(uuid.uuid4())


# ──────────────────────────────────────────────────────────────────────────────
# Seed helpers
# ──────────────────────────────────────────────────────────────────────────────

def seed_company(conn, name="Test Stripe Co", abbr="TSC") -> str:
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


def seed_gl_account(conn, company_id: str, name="Test Account",
                    root_type="asset", account_type="bank") -> str:
    """Insert a GL account and return its ID."""
    aid = _uuid()
    conn.execute(
        """INSERT INTO account (id, name, root_type, account_type,
           currency, is_group, balance_direction, company_id)
           VALUES (?, ?, ?, ?, 'USD', 0, 'debit_normal', ?)""",
        (aid, name, root_type, account_type, company_id)
    )
    conn.commit()
    return aid


def seed_stripe_account(conn, company_id: str, name="Test Stripe",
                        key="rk_test_fake_key_for_testing_1234567890") -> str:
    """Insert a stripe account via the add-account action pattern and return its ID.

    Creates the 5 auto-GL accounts and the stripe_account record.
    """
    from stripe_helpers import encrypt_key, now_iso

    acct_id = _uuid()
    now = now_iso()
    enc_key = encrypt_key(key)

    # Create 5 GL accounts
    gl_ids = {}
    gl_defs = [
        ("stripe_clearing_account_id", "Stripe Clearing", "asset", "bank", "debit_normal"),
        ("stripe_fees_account_id", "Stripe Processing Fees", "expense", "expense", "debit_normal"),
        ("stripe_payout_account_id", "Stripe Payout", "asset", "bank", "debit_normal"),
        ("dispute_expense_account_id", "Stripe Dispute Losses", "expense", "expense", "debit_normal"),
        ("unearned_revenue_account_id", "Stripe Unearned Revenue", "liability", "temporary", "credit_normal"),
    ]
    for mapping_field, suffix, root_type, acct_type, balance_dir in gl_defs:
        gl_id = _uuid()
        conn.execute(
            """INSERT INTO account (id, name, root_type, account_type, currency,
               is_group, balance_direction, company_id, created_at, updated_at)
               VALUES (?, ?, ?, ?, 'USD', 0, ?, ?, ?, ?)""",
            (gl_id, f"{name} - {suffix}", root_type, acct_type,
             balance_dir, company_id, now, now)
        )
        gl_ids[mapping_field] = gl_id

    conn.execute(
        """INSERT INTO stripe_account
            (id, company_id, account_name, restricted_key_enc,
             mode, is_connect_platform, default_currency,
             stripe_clearing_account_id, stripe_fees_account_id,
             stripe_payout_account_id, dispute_expense_account_id,
             unearned_revenue_account_id,
             status, created_at, updated_at)
           VALUES (?, ?, ?, ?, 'test', 0, 'USD', ?, ?, ?, ?, ?, 'active', ?, ?)""",
        (acct_id, company_id, name, enc_key,
         gl_ids["stripe_clearing_account_id"],
         gl_ids["stripe_fees_account_id"],
         gl_ids["stripe_payout_account_id"],
         gl_ids["dispute_expense_account_id"],
         gl_ids["unearned_revenue_account_id"],
         now, now)
    )
    conn.commit()
    return acct_id


def seed_erpclaw_customer(conn, company_id: str,
                          name: str = "Test Customer") -> str:
    """Insert an erpclaw customer and return its ID.

    The customer table does not have an email column, so matching
    is done by name.
    """
    cust_id = _uuid()
    now = "2026-01-01T00:00:00Z"
    conn.execute(
        """INSERT INTO customer (id, name, company_id, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?)""",
        (cust_id, name, company_id, now, now)
    )
    conn.commit()
    return cust_id


def seed_fiscal_year(conn, company_id: str, name="FY2026",
                     start="2026-01-01", end="2026-12-31") -> str:
    """Insert a fiscal year and return its ID."""
    fy_id = _uuid()
    now = "2026-01-01T00:00:00Z"
    conn.execute(
        """INSERT INTO fiscal_year (id, name, start_date, end_date, is_closed,
           company_id, created_at, updated_at)
           VALUES (?, ?, ?, ?, 0, ?, ?, ?)""",
        (fy_id, f"{name}_{fy_id[:6]}", start, end, company_id, now, now)
    )
    conn.commit()
    return fy_id


def seed_cost_center(conn, company_id: str, name="Main") -> str:
    """Insert a cost center and return its ID."""
    cc_id = _uuid()
    now = "2026-01-01T00:00:00Z"
    conn.execute(
        """INSERT INTO cost_center (id, name, company_id, is_group,
           created_at, updated_at)
           VALUES (?, ?, ?, 0, ?, ?)""",
        (cc_id, name, company_id, now, now)
    )
    conn.commit()
    return cc_id


def seed_charge(conn, stripe_account_id: str, company_id: str,
                stripe_id: str = "ch_test_001", amount: str = "100.00",
                status: str = "succeeded", customer_stripe_id: str = "",
                erpclaw_payment_entry_id=None) -> str:
    """Insert a stripe_charge record and return its internal ID."""
    cid = _uuid()
    now = "2026-03-15T12:00:00Z"
    conn.execute(
        """INSERT INTO stripe_charge
            (id, stripe_id, stripe_account_id, amount, currency,
             customer_stripe_id, status, amount_refunded, disputed,
             erpclaw_payment_entry_id,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, 'USD', ?, ?, '0', 0, ?, ?, ?, datetime('now'))""",
        (cid, stripe_id, stripe_account_id, amount, customer_stripe_id,
         status, erpclaw_payment_entry_id, company_id, now)
    )
    conn.commit()
    return cid


def seed_balance_transaction(conn, stripe_account_id: str, company_id: str,
                             stripe_id: str = "txn_test_001",
                             source_id: str = "ch_test_001",
                             amount: str = "100.00", fee: str = "3.20",
                             net: str = "96.80", bt_type: str = "charge",
                             payout_id: str = None, reconciled: int = 0) -> str:
    """Insert a stripe_balance_transaction and return its internal ID."""
    bid = _uuid()
    now = "2026-03-15T12:00:00Z"
    conn.execute(
        """INSERT INTO stripe_balance_transaction
            (id, stripe_id, stripe_account_id, type, source_id,
             amount, fee, net, currency, status,
             payout_id, reconciled, company_id,
             created_stripe, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'USD', 'available',
                   ?, ?, ?, ?, datetime('now'))""",
        (bid, stripe_id, stripe_account_id, bt_type, source_id,
         amount, fee, net, payout_id, reconciled, company_id, now)
    )
    conn.commit()
    return bid


def seed_refund(conn, stripe_account_id: str, company_id: str,
                stripe_id: str = "re_test_001", charge_stripe_id: str = "ch_test_001",
                amount: str = "50.00", status: str = "succeeded",
                erpclaw_payment_entry_id=None) -> str:
    """Insert a stripe_refund and return its internal ID."""
    rid = _uuid()
    now = "2026-03-15T12:00:00Z"
    conn.execute(
        """INSERT INTO stripe_refund
            (id, stripe_id, stripe_account_id, charge_stripe_id,
             amount, currency, status, erpclaw_payment_entry_id,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, ?, 'USD', ?, ?, ?, ?, datetime('now'))""",
        (rid, stripe_id, stripe_account_id, charge_stripe_id,
         amount, status, erpclaw_payment_entry_id, company_id, now)
    )
    conn.commit()
    return rid


def seed_dispute(conn, stripe_account_id: str, company_id: str,
                 stripe_id: str = "dp_test_001", charge_stripe_id: str = "ch_test_001",
                 amount: str = "100.00", status: str = "needs_response",
                 erpclaw_journal_entry_id=None) -> str:
    """Insert a stripe_dispute and return its internal ID."""
    did = _uuid()
    now = "2026-03-15T12:00:00Z"
    conn.execute(
        """INSERT INTO stripe_dispute
            (id, stripe_id, stripe_account_id, charge_stripe_id,
             amount, currency, status, erpclaw_journal_entry_id,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, ?, 'USD', ?, ?, ?, ?, datetime('now'))""",
        (did, stripe_id, stripe_account_id, charge_stripe_id,
         amount, status, erpclaw_journal_entry_id, company_id, now)
    )
    conn.commit()
    return did


def seed_payout(conn, stripe_account_id: str, company_id: str,
                stripe_id: str = "po_test_001", amount: str = "500.00",
                status: str = "paid", erpclaw_payment_entry_id=None) -> str:
    """Insert a stripe_payout and return its internal ID."""
    pid = _uuid()
    now = "2026-03-15T12:00:00Z"
    conn.execute(
        """INSERT INTO stripe_payout
            (id, stripe_id, stripe_account_id, amount, currency,
             status, reconciled, erpclaw_payment_entry_id,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, 'USD', ?, 0, ?, ?, ?, datetime('now'))""",
        (pid, stripe_id, stripe_account_id, amount, status,
         erpclaw_payment_entry_id, company_id, now)
    )
    conn.commit()
    return pid


def seed_application_fee(conn, stripe_account_id: str, company_id: str,
                         stripe_id: str = "fee_test_001", amount: str = "10.00",
                         erpclaw_journal_entry_id=None) -> str:
    """Insert a stripe_application_fee and return its internal ID."""
    fid = _uuid()
    now = "2026-03-15T12:00:00Z"
    conn.execute(
        """INSERT INTO stripe_application_fee
            (id, stripe_id, stripe_account_id, amount, currency,
             refunded_amount, erpclaw_journal_entry_id,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, 'USD', '0', ?, ?, ?, datetime('now'))""",
        (fid, stripe_id, stripe_account_id, amount,
         erpclaw_journal_entry_id, company_id, now)
    )
    conn.commit()
    return fid


def seed_subscription(conn, stripe_account_id: str, company_id: str,
                      stripe_id: str = "sub_test_001",
                      customer_stripe_id: str = "cus_test_001",
                      plan_amount: str = "49.99", plan_interval: str = "month",
                      status: str = "active") -> str:
    """Insert a stripe_subscription and return its internal ID."""
    sid = _uuid()
    now = "2026-03-15T12:00:00Z"
    conn.execute(
        """INSERT INTO stripe_subscription
            (id, stripe_id, stripe_account_id, customer_stripe_id,
             status, plan_interval, plan_amount, currency,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'USD', ?, ?, datetime('now'))""",
        (sid, stripe_id, stripe_account_id, customer_stripe_id,
         status, plan_interval, plan_amount, company_id, now)
    )
    conn.commit()
    return sid


def seed_customer_map(conn, stripe_account_id: str, company_id: str,
                      stripe_customer_id: str = "cus_test_001",
                      erpclaw_customer_id: str = None,
                      stripe_name: str = "Test Customer") -> str:
    """Insert a stripe_customer_map record and return its internal ID."""
    mid = _uuid()
    conn.execute(
        """INSERT INTO stripe_customer_map
            (id, stripe_account_id, stripe_customer_id,
             erpclaw_customer_id, stripe_name,
             match_method, match_confidence, company_id, created_at)
           VALUES (?, ?, ?, ?, ?, 'manual', '1.0', ?, datetime('now'))""",
        (mid, stripe_account_id, stripe_customer_id,
         erpclaw_customer_id, stripe_name, company_id)
    )
    conn.commit()
    return mid


def seed_transfer(conn, stripe_account_id: str, company_id: str,
                  stripe_id: str = "tr_test_001", amount: str = "200.00") -> str:
    """Insert a stripe_transfer and return its internal ID."""
    tid = _uuid()
    now = "2026-03-15T12:00:00Z"
    conn.execute(
        """INSERT INTO stripe_transfer
            (id, stripe_id, stripe_account_id, amount, currency,
             reversed, company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, 'USD', 0, ?, ?, datetime('now'))""",
        (tid, stripe_id, stripe_account_id, amount, company_id, now)
    )
    conn.commit()
    return tid


def seed_invoice(conn, stripe_account_id: str, company_id: str,
                 stripe_id: str = "in_test_001", amount_due: str = "100.00",
                 status: str = "paid") -> str:
    """Insert a stripe_invoice and return its internal ID."""
    iid = _uuid()
    now = "2026-03-15T12:00:00Z"
    conn.execute(
        """INSERT INTO stripe_invoice
            (id, stripe_id, stripe_account_id, amount_due, amount_paid,
             amount_remaining, currency, status,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, ?, '0', 'USD', ?, ?, ?, datetime('now'))""",
        (iid, stripe_id, stripe_account_id, amount_due, amount_due,
         status, company_id, now)
    )
    conn.commit()
    return iid


def build_stripe_env(conn) -> dict:
    """Create a complete stripe test environment.

    Returns dict with company_id, stripe_account_id, and company's company_id.
    """
    cid = seed_company(conn)
    acct_id = seed_stripe_account(conn, cid)
    return {
        "company_id": cid,
        "stripe_account_id": acct_id,
    }


def build_gl_ready_env(conn) -> dict:
    """Create a complete GL-ready test environment.

    Returns dict with company_id, stripe_account_id, fiscal_year_id,
    cost_center_id, and all GL account IDs needed for posting.
    """
    env = build_stripe_env(conn)
    cid = env["company_id"]

    # GL posting requires an open fiscal year
    fy_id = seed_fiscal_year(conn, cid)

    # Cost center for P&L accounts
    cc_id = seed_cost_center(conn, cid)

    env["fiscal_year_id"] = fy_id
    env["cost_center_id"] = cc_id

    # Read GL account IDs from stripe_account
    row = conn.execute(
        """SELECT stripe_clearing_account_id, stripe_fees_account_id,
                  stripe_payout_account_id, dispute_expense_account_id,
                  unearned_revenue_account_id
           FROM stripe_account WHERE id = ?""",
        (env["stripe_account_id"],)
    ).fetchone()
    env["clearing_id"] = row["stripe_clearing_account_id"]
    env["fees_id"] = row["stripe_fees_account_id"]
    env["payout_id"] = row["stripe_payout_account_id"]
    env["dispute_id"] = row["dispute_expense_account_id"]
    env["unearned_id"] = row["unearned_revenue_account_id"]

    return env
