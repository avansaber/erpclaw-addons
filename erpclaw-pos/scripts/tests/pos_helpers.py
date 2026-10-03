"""Shared helper functions for ERPClaw POS unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + create_pos_tables()
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, items, naming series
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
from decimal import Decimal, ROUND_HALF_UP
from unittest.mock import patch

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_DIR = os.path.dirname(TESTS_DIR)          # scripts/
ROOT_DIR = os.path.dirname(MODULE_DIR)            # erpclaw-pos/
ADDONS_DIR = os.path.dirname(ROOT_DIR)            # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)             # source/
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")
VERTICAL_INIT_PATH = os.path.join(ROOT_DIR, "init_db.py")

# M54: bind erpclaw_lib to the tree under test, never the deployed
# ~/.openclaw/erpclaw/lib symlink — the last install to run wins that symlink,
# so with several worktrees in flight it resolves to a tree nobody is testing
# (and DANGLES once that worktree is removed). The deployed install stays as
# the fallback for a published module repo, which ships no source/erpclaw/.
_IN_TREE_LIB = os.path.join(SETUP_DIR, "lib")
ERPCLAW_LIB = (_IN_TREE_LIB if os.path.isdir(os.path.join(_IN_TREE_LIB, "erpclaw_lib"))
               else os.path.join(os.path.expanduser(
                   os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
if ERPCLAW_LIB not in sys.path:
    if importlib.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, ERPCLAW_LIB)

from erpclaw_lib.db import setup_pragmas
from erpclaw_lib import cross_skill
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.query import Q, P, Table, Field, insert_row, dynamic_update, now


def load_db_query():
    """Load erpclaw-pos db_query.py explicitly to avoid sys.path collisions."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_pos", db_query_path)
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
    """Create all ERPClaw core tables + POS vertical tables."""
    # 1. Foundation schema (company, account, naming_series, item, etc.)
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.init_db(db_path)

    # 2. POS vertical schema (5 tables)
    spec2 = importlib.util.spec_from_file_location("pos_init", VERTICAL_INIT_PATH)
    m2 = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(m2)
    m2.create_pos_tables(db_path)


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

def seed_company(conn, name="Test POS Co", abbr="TPC") -> str:
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


def seed_item(conn, name="Test Item", item_code="ITEM-001", is_stock_item=1) -> str:
    """Insert a test item and return its ID."""
    iid = _uuid()
    conn.execute(
        """INSERT INTO item (id, item_name, item_code, stock_uom, is_stock_item, standard_rate)
           VALUES (?, ?, ?, 'Nos', ?, '10.00')""",
        (iid, name, f"{item_code}-{iid[:6]}", is_stock_item)
    )
    conn.commit()
    return iid


def seed_naming_series(conn, company_id: str):
    """Seed naming series for POS entity types."""
    series = [
        ("pos_profile", "POS-", 0),
        ("pos_session", "POSS-", 0),
        ("pos_transaction", "PTXN-", 0),
        ("sales_invoice", "SINV-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_customer(conn, company_id: str, name="Test Customer") -> str:
    """Insert an active customer and return its ID (mirrors selling_helpers)."""
    cid = _uuid()
    conn.execute(
        """INSERT INTO customer (id, name, company_id, customer_type, status, credit_limit)
           VALUES (?, ?, ?, 'company', 'active', '0')""",
        (cid, name, company_id)
    )
    conn.commit()
    return cid


def seed_account(conn, company_id: str, name="Test Account",
                 root_type="asset", account_type=None,
                 account_number=None) -> str:
    """Insert a GL account and return its ID (mirrors selling_helpers)."""
    aid = _uuid()
    direction = "debit_normal" if root_type in ("asset", "expense") else "credit_normal"
    conn.execute(
        """INSERT INTO account (id, name, account_number, root_type, account_type,
           balance_direction, company_id, depth)
           VALUES (?, ?, ?, ?, ?, ?, ?, 0)""",
        (aid, name, account_number or f"ACC-{aid[:6]}", root_type,
         account_type, direction, company_id)
    )
    conn.commit()
    return aid


def seed_fiscal_year(conn, company_id: str, name=None,
                     start="2026-01-01", end="2026-12-31") -> str:
    """Insert a fiscal year covering the test posting dates."""
    fid = _uuid()
    conn.execute(
        """INSERT INTO fiscal_year (id, name, start_date, end_date, company_id)
           VALUES (?, ?, ?, ?, ?)""",
        (fid, name or f"FY-{fid[:6]}", start, end, company_id)
    )
    conn.commit()
    return fid


def seed_cost_center(conn, company_id: str, name="Main CC") -> str:
    """Insert a cost center and return its ID."""
    ccid = _uuid()
    conn.execute(
        """INSERT INTO cost_center (id, name, company_id, is_group)
           VALUES (?, ?, ?, 0)""",
        (ccid, name, company_id)
    )
    conn.commit()
    return ccid


def seed_selling_accounts(conn, company_id: str) -> dict:
    """Seed the GL accounts submit_sales_invoice needs: receivable + revenue.

    Sets them as the company defaults, the same lookup the selling module
    uses first.
    """
    ar = seed_account(conn, company_id, "Accounts Receivable",
                      "asset", "receivable", "1100")
    revenue = seed_account(conn, company_id, "Sales Revenue",
                           "income", "revenue", "4000")
    conn.execute(
        """UPDATE company SET
           default_receivable_account_id = ?,
           default_income_account_id = ?
           WHERE id = ?""",
        (ar, revenue, company_id)
    )
    conn.commit()
    return {"ar": ar, "revenue": revenue}


def seed_till_accounts(conn, company_id: str) -> dict:
    """Seed Till Cash / Till Bank leaf accounts and set the company defaults.

    The defaults are written through the setup owning action
    (``update-company``), so callers need the ``selling_bridge`` fixture.
    """
    cash = seed_account(conn, company_id, "Till Cash", "asset", "cash")
    bank = seed_account(conn, company_id, "Till Bank", "asset", "bank")
    conn.commit()
    cross_skill.call_skill_action(
        "erpclaw", "update-company",
        {"--company-id": company_id,
         "--default-cash-account-id": cash,
         "--default-bank-account-id": bank},
    )
    conn.commit()
    return {"cash": cash, "bank": bank}


def seed_pos_profile(conn, company_id: str, name="Default POS") -> str:
    """Insert a POS profile and return its ID."""
    mod = load_db_query()
    r = call_action(mod.ACTIONS["pos-add-pos-profile"], conn, ns(
        company_id=company_id, name=name,
        warehouse_id=None, price_list_id=None,
        default_payment_method="cash",
        allow_discount="1", max_discount_pct="100",
        auto_print_receipt="0", is_active=None,
    ))
    assert is_ok(r), f"seed_pos_profile failed: {r}"
    return r["id"]


def seed_open_session(conn, profile_id: str, cashier="Test Cashier") -> str:
    """Open a POS session and return its ID."""
    mod = load_db_query()
    r = call_action(mod.ACTIONS["pos-open-session"], conn, ns(
        pos_profile_id=profile_id, cashier_name=cashier,
        opening_amount="100.00",
    ))
    assert is_ok(r), f"seed_open_session failed: {r}"
    return r["id"]


def build_env(conn) -> dict:
    """Create a full POS test environment.

    Returns dict with company_id, profile_id, session_id, item_id,
    customer_id. Also seeds the GL accounts, fiscal year and cost center
    the selling module needs to submit the linked sales invoice.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    seed_selling_accounts(conn, cid)
    seed_fiscal_year(conn, cid)
    seed_cost_center(conn, cid)
    customer_id = seed_customer(conn, cid)
    item_id = seed_item(conn, "Widget A", "WDG-A")
    profile_id = seed_pos_profile(conn, cid)
    session_id = seed_open_session(conn, profile_id)
    return {
        "company_id": cid,
        "profile_id": profile_id,
        "session_id": session_id,
        "item_id": item_id,
        "customer_id": customer_id,
    }


def seed_return_document(conn, original_id):
    """Old-style return document for read-path tests (PyPika, both backends).

    The pre-task ``return_transaction`` body copied exactly: flips the
    original to ``returned`` and writes a negated document (``-0.00`` rule
    kept) into the original's session, negating every line and every payment
    including change, with no credit note and no refund. Adds both link
    columns (``return_against_id`` / ``return_against_item_id``) so the new
    returnable accounting sees these lines as already returned. Returns the
    return document id.
    """
    def _dec(val):
        return Decimal("0") if val is None else Decimal(str(val))

    def _rnd(val):
        return val.quantize(Decimal("0.01"), ROUND_HALF_UP)

    t = Table("pos_transaction")
    orig = conn.execute(
        Q.from_(t).select(t.star).where(t.id == P()).get_sql(),
        (original_id,)).fetchone()
    assert orig is not None, f"seed_return_document: {original_id} not found"

    sql, params = dynamic_update(
        "pos_transaction",
        {"status": "returned", "updated_at": now()}, {"id": original_id})
    conn.execute(sql, params)

    rid = str(uuid.uuid4())
    naming = get_next_name(conn, "pos_transaction",
                           company_id=orig["company_id"])
    subtotal = str(_rnd(-_dec(orig["subtotal"])))
    discount_amount = str(_rnd(-_dec(orig["discount_amount"])))
    tax_amount = str(_rnd(-_dec(orig["tax_amount"])))
    grand_total = str(_rnd(-_dec(orig["grand_total"])))
    if _dec(orig["grand_total"]) == Decimal("0") and not _dec(
            grand_total).is_signed():
        grand_total = "-0.00"

    sql, _ = insert_row("pos_transaction", {
        "id": P(), "naming_series": P(), "pos_session_id": P(),
        "customer_id": P(), "customer_name": P(), "subtotal": P(),
        "discount_amount": P(), "discount_pct": P(), "tax_amount": P(),
        "grand_total": P(), "paid_amount": P(), "change_amount": P(),
        "status": P(), "company_id": P(), "return_against_id": P()})
    conn.execute(sql, (
        rid, naming, orig["pos_session_id"], orig["customer_id"],
        orig["customer_name"], subtotal, discount_amount,
        orig["discount_pct"], tax_amount, grand_total, grand_total, "0",
        "returned", orig["company_id"], original_id))

    ti = Table("pos_transaction_item")
    for line in conn.execute(
            Q.from_(ti).select(ti.star)
            .where(ti.pos_transaction_id == P()).get_sql(),
            (original_id,)).fetchall():
        sql, _ = insert_row("pos_transaction_item", {
            "id": P(), "pos_transaction_id": P(), "item_id": P(),
            "item_name": P(), "item_code": P(), "barcode": P(), "qty": P(),
            "rate": P(), "discount_pct": P(), "discount_amount": P(),
            "amount": P(), "uom": P(), "return_against_item_id": P()})
        conn.execute(sql, (
            str(uuid.uuid4()), rid, line["item_id"], line["item_name"],
            line["item_code"], line["barcode"],
            str(_rnd(-_dec(line["qty"]))), line["rate"],
            line["discount_pct"], str(_rnd(-_dec(line["discount_amount"]))),
            str(_rnd(-_dec(line["amount"]))), line["uom"], line["id"]))

    pp = Table("pos_payment")
    for pay in conn.execute(
            Q.from_(pp).select(pp.star)
            .where(pp.pos_transaction_id == P()).get_sql(),
            (original_id,)).fetchall():
        sql, _ = insert_row("pos_payment", {
            "id": P(), "pos_transaction_id": P(), "payment_method": P(),
            "amount": P(), "reference": P()})
        conn.execute(sql, (
            str(uuid.uuid4()), rid, pay["payment_method"],
            str(_rnd(-_dec(pay["amount"]))), f"Return of {original_id}"))
    conn.commit()
    return rid
