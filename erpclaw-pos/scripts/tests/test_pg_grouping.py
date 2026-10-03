"""PostgreSQL proof for the point-of-sale return rule and ordering.

This module proves on a live PostgreSQL server that the top items and the
session-summary top items are aggregated in Python with a deterministic
order, that the ``pos-status`` today figures count a returned sale, that
the daily report counts a returned sale and a zero-total return (the stored
``-0.00`` reading back as text), and that profile creation refuses a
duplicate id; the company, items and customer are seeded through their
owning actions.

The fixture is self-contained and touches only the expendable database the
test URL names: it requires ``ERPCLAW_PG_TEST_URL`` (skipping only when it
is unset), points the database settings at it for the test and restores
them afterwards, refuses unless the connection lands in the database the
URL names, resets the shared schema, provisions the foundation schema and
the point-of-sale tables through their installers with no path, and does
all database work through ``erpclaw_lib.db.get_connection()``.
"""
import importlib.util
import os
import sys
import urllib.parse
import uuid
from datetime import datetime

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest  # noqa: E402

from pos_helpers import (  # noqa: E402
    INIT_SCHEMA_PATH, SRC_DIR, VERTICAL_INIT_PATH, call_action, is_ok,
    load_db_query, ns, seed_return_document,
)
from erpclaw_lib.query import Q, P, Table  # noqa: E402

MOD = load_db_query()
A = MOD.ACTIONS

DAY = "2026-03-10"


def _pg_url():
    return os.environ.get("ERPCLAW_PG_TEST_URL")


needs_pg = pytest.mark.skipif(
    not _pg_url(),
    reason="ERPCLAW_PG_TEST_URL not set (live Postgres required)",
)


def _wanted_db(pg_url):
    return urllib.parse.urlparse(pg_url).path.rsplit("/", 1)[-1]


def _load_installer(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_domain(name, domain):
    path = os.path.join(SRC_DIR, "erpclaw", "scripts", domain, "db_query.py")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ok(action, conn, **kw):
    result = call_action(A[action], conn, ns(**kw))
    assert is_ok(result), f"{action} failed: {result}"
    return result


def _stamp(conn, row_id, ts):
    tab = Table("pos_transaction")
    conn.execute(
        Q.update(tab).set(tab.created_at, P()).where(
            tab.id == P()).get_sql(), (ts, row_id))
    conn.commit()


def _flip(conn, txn_id):
    tab = Table("pos_transaction")
    conn.execute(
        Q.update(tab).set(tab.status, P()).where(
            tab.id == P()).get_sql(), ("submitted", txn_id))
    conn.commit()


def _seed(conn):
    setup = _load_domain("_fnd_setup", "erpclaw-setup")
    inventory = _load_domain("_fnd_inventory", "erpclaw-inventory")
    selling = _load_domain("_fnd_selling", "erpclaw-selling")
    tag = uuid.uuid4().hex[:6]
    company_id = call_action(
        setup.ACTIONS["setup-company"], conn,
        ns(name=f"PG POS Co {tag}", abbr=f"PPC{tag[:4]}", currency="USD",
           country="United States", fiscal_year_start_month=1,
           industry=None, company_id=None, tax_id=None))["company_id"]
    widget_code = f"WDG-A-{tag}-W"
    widget_id = call_action(
        inventory.ACTIONS["add-item"], conn,
        ns(item_code=widget_code, item_name="Widget A", item_group=None,
           item_type="stock", stock_uom="Nos",
           valuation_method="moving_average", has_batch=None,
           has_serial=None, standard_rate="10.00", custom_fields=None,
           item_status=None))["item_id"]
    gadget_code = f"GDG-B-{tag}-G"
    gadget_id = call_action(
        inventory.ACTIONS["add-item"], conn,
        ns(item_code=gadget_code, item_name="Gadget B", item_group=None,
           item_type="stock", stock_uom="Nos",
           valuation_method="moving_average", has_batch=None,
           has_serial=None, standard_rate="10.00", custom_fields=None,
           item_status=None))["item_id"]
    customer_id = call_action(
        selling.ACTIONS["add-customer"], conn,
        ns(name="PG Customer", company_id=company_id,
           customer_type="company", customer_group=None,
           payment_terms_id=None, credit_limit="0", tax_id=None,
           exempt_from_sales_tax=None, primary_address=None,
           primary_contact=None, email=None, phone=None,
           default_price_list_id=None,
           custom_fields=None))["customer_id"]
    conn.commit()

    profile_id = _ok("pos-add-pos-profile", conn, company_id=company_id,
                     name="PG Counter", warehouse_id=None,
                     price_list_id=None, default_payment_method="cash",
                     allow_discount="1", max_discount_pct="100",
                     auto_print_receipt="0", is_active=None)["id"]
    session_id = _ok("pos-open-session", conn, pos_profile_id=profile_id,
                     cashier_name="PG Cashier",
                     opening_amount="100.00")["id"]

    first = _ok("pos-add-transaction", conn, pos_session_id=session_id,
                customer_id=customer_id, customer_name="PG Buyer")["id"]
    _stamp(conn, first, f"{DAY} 09:15:00")
    _ok("pos-add-transaction-item", conn, pos_transaction_id=first,
        item_id=widget_id, item_name=None, qty="3", rate="12.50", uom=None,
        barcode=None, discount_pct=None)
    _flip(conn, first)

    second = _ok("pos-add-transaction", conn, pos_session_id=session_id,
                 customer_id=customer_id, customer_name="PG Buyer")["id"]
    _stamp(conn, second, f"{DAY} 09:40:00")
    _ok("pos-add-transaction-item", conn, pos_transaction_id=second,
        item_id=gadget_id, item_name=None, qty="2", rate="5.00", uom=None,
        barcode=None, discount_pct=None)
    _flip(conn, second)

    conn.commit()
    return {"company_id": company_id, "session_id": session_id,
            "widget_id": widget_id, "gadget_id": gadget_id,
            "widget_code": widget_code, "gadget_code": gadget_code}


@pytest.fixture
def pg_conn(pg_book):
    from erpclaw_lib.db import get_connection
    conn = get_connection()
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


@pytest.fixture(scope="module")
def pg_book():
    pg_url = _pg_url()
    if not pg_url:
        pytest.skip("ERPCLAW_PG_TEST_URL not set (live Postgres required)")
    saved = {key: os.environ.get(key) for key in
             ("ERPCLAW_DB_DIALECT", "ERPCLAW_DB_URL", "ERPCLAW_DB_PATH")}
    os.environ["ERPCLAW_DB_DIALECT"] = "postgresql"
    os.environ["ERPCLAW_DB_URL"] = pg_url
    os.environ["ERPCLAW_DB_PATH"] = pg_url
    conn = None
    try:
        from erpclaw_lib.db import get_connection
        conn = get_connection()
        current = conn.execute("SELECT current_database()").fetchone()[0]
        if current != _wanted_db(pg_url):
            raise RuntimeError(
                "refusing to reset: the live connection is not in the "
                "database the test URL names")
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")
        conn.commit()
        _load_installer("init_schema_pg", INIT_SCHEMA_PATH).init_db()
        _load_installer("pos_init_pg", VERTICAL_INIT_PATH).create_pos_tables()
        yield _seed(conn)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@needs_pg
def test_pg_top_items_groups_by_item(pg_book, pg_conn):
    conn = pg_conn
    result = call_action(
        A["pos-top-items"], conn,
        ns(from_date=DAY, to_date=DAY, company_id=pg_book["company_id"],
           limit=None))
    assert is_ok(result), f"pos-top-items failed: {result}"
    assert [(item["item_name"], item["item_code"], item["total_qty"],
             item["total_revenue"], item["transaction_count"])
            for item in result["top_items"]] == [
        ("Widget A", pg_book["widget_code"], "3.00", "37.50", 1),
        ("Gadget B", pg_book["gadget_code"], "2.00", "10.00", 1),
    ]
    assert (result["from_date"], result["to_date"], result["count"]) == (
        DAY, DAY, 2)


@needs_pg
def test_pg_session_summary_top_items_groups_by_item(pg_book, pg_conn):
    conn = pg_conn
    result = call_action(A["pos-session-summary"], conn,
                         ns(pos_session_id=pg_book["session_id"]))
    assert is_ok(result), f"pos-session-summary failed: {result}"
    assert result["total_transactions"] == 2
    assert result["status_breakdown"] == {
        "submitted": {"count": 2, "total": "47.50"}}
    assert result["payment_breakdown"] == {}
    assert [(item["item_name"], item["item_code"], item["total_qty"],
             item["total_amount"]) for item in result["top_items"]] == [
        ("Widget A", pg_book["widget_code"], "3.00", "37.50"),
        ("Gadget B", pg_book["gadget_code"], "2.00", "10.00"),
    ]


@needs_pg
def test_pg_pos_status_counts_today(pg_book, pg_conn):
    conn = pg_conn
    profile_id = _ok("pos-add-pos-profile", conn,
                     company_id=pg_book["company_id"],
                     name="PG Status Counter", warehouse_id=None,
                     price_list_id=None, default_payment_method="cash",
                     allow_discount="1", max_discount_pct="100",
                     auto_print_receipt="0", is_active=None)["id"]
    session_id = _ok("pos-open-session", conn, pos_profile_id=profile_id,
                     cashier_name="PG Status Cashier",
                     opening_amount="0")["id"]
    today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    first = _ok("pos-add-transaction", conn, pos_session_id=session_id,
                customer_id=None, customer_name="PG Today")["id"]
    _stamp(conn, first, today)
    _ok("pos-add-transaction-item", conn, pos_transaction_id=first,
        item_id=pg_book["widget_id"], item_name=None, qty="1", rate="0.10",
        uom=None, barcode=None, discount_pct=None)
    _flip(conn, first)
    second = _ok("pos-add-transaction", conn, pos_session_id=session_id,
                 customer_id=None, customer_name="PG Today")["id"]
    _stamp(conn, second, today)
    _ok("pos-add-transaction-item", conn, pos_transaction_id=second,
        item_id=pg_book["gadget_id"], item_name=None, qty="1", rate="0.20",
        uom=None, barcode=None, discount_pct=None)
    _flip(conn, second)
    returned = seed_return_document(conn, second)
    _stamp(conn, returned, today)
    conn.commit()
    result = call_action(A["pos-status"], conn, ns())
    assert is_ok(result), f"pos-status failed: {result}"
    assert (result["today_transactions"], result["today_sales"]) == (
        3, "0.30")


@needs_pg
def test_pg_daily_report_return_accounting_with_zero_total_return(
        pg_book, pg_conn):
    conn = pg_conn
    profile_id = _ok("pos-add-pos-profile", conn,
                     company_id=pg_book["company_id"],
                     name="PG Daily Counter", warehouse_id=None,
                     price_list_id=None, default_payment_method="cash",
                     allow_discount="1", max_discount_pct="100",
                     auto_print_receipt="0", is_active=None)["id"]
    session_id = _ok("pos-open-session", conn, pos_profile_id=profile_id,
                     cashier_name="PG Daily Cashier",
                     opening_amount="100.00")["id"]
    sess_tab = Table("pos_session")
    conn.execute(
        Q.update(sess_tab).set(sess_tab.opened_at, P()).where(
            sess_tab.id == P()).get_sql(),
        ("2026-03-11 08:00:00", session_id))
    conn.commit()
    day = "2026-03-11"
    first = _ok("pos-add-transaction", conn, pos_session_id=session_id,
                customer_id=None, customer_name="PG Daily")["id"]
    _stamp(conn, first, f"{day} 09:15:00")
    _ok("pos-add-transaction-item", conn, pos_transaction_id=first,
        item_id=pg_book["widget_id"], item_name=None, qty="1", rate="0.10",
        uom=None, barcode=None, discount_pct=None)
    _ok("pos-add-payment", conn, pos_transaction_id=first,
        payment_method="cash", amount="0.10", reference=None)
    _flip(conn, first)
    second = _ok("pos-add-transaction", conn, pos_session_id=session_id,
                 customer_id=None, customer_name="PG Daily")["id"]
    _stamp(conn, second, f"{day} 09:40:00")
    _ok("pos-add-transaction-item", conn, pos_transaction_id=second,
        item_id=pg_book["gadget_id"], item_name=None, qty="1", rate="0.30",
        uom=None, barcode=None, discount_pct=None)
    _ok("pos-add-payment", conn, pos_transaction_id=second,
        payment_method="cash", amount="0.30", reference=None)
    _flip(conn, second)
    second_return = seed_return_document(conn, second)
    _stamp(conn, second_return, f"{day} 10:30:00")
    third = _ok("pos-add-transaction", conn, pos_session_id=session_id,
                customer_id=None, customer_name="PG Daily")["id"]
    _stamp(conn, third, f"{day} 11:00:00")
    _flip(conn, third)
    zero_return_id = seed_return_document(conn, third)
    zero_response = {"return_transaction_id": zero_return_id}
    _stamp(conn, zero_response["return_transaction_id"], f"{day} 11:30:00")
    conn.commit()

    result = call_action(A["pos-daily-report"], conn,
                         ns(date=day, company_id=pg_book["company_id"]))
    assert is_ok(result), f"pos-daily-report failed: {result}"
    assert (result["transaction_count"], result["total_sales"],
            result["total_discounts"], result["total_tax"],
            result["return_count"], result["total_returns"],
            result["net_sales"], result["sessions_count"]) == (
        3, "0.40", "0.00", "0.00", 2, "0.30", "0.10", 1)
    assert result["payment_methods"] == [
        {"method": "cash", "count": 2, "total": "0.40"}]

    _zero_tab = Table("pos_transaction")
    assert conn.execute(
        Q.from_(_zero_tab).select(_zero_tab.grand_total).where(
            _zero_tab.id == P()).get_sql(),
        (zero_return_id,)).fetchone()["grand_total"] == "-0.00"
    from erpclaw_lib.db import get_connection
    fresh = get_connection()
    try:
        txn_tab = Table("pos_transaction")
        stored = fresh.execute(
            Q.from_(txn_tab).select(txn_tab.grand_total).where(
                txn_tab.id == P()).get_sql(),
            (zero_response["return_transaction_id"],)).fetchone()
    finally:
        try:
            fresh.close()
        except Exception:
            pass
    assert stored["grand_total"] == "-0.00"


@needs_pg
def test_pg_top_items_quantity_tie_orders_by_name(pg_book, pg_conn):
    conn = pg_conn
    profile_id = _ok("pos-add-pos-profile", conn,
                     company_id=pg_book["company_id"],
                     name="PG Tie Counter", warehouse_id=None,
                     price_list_id=None, default_payment_method="cash",
                     allow_discount="1", max_discount_pct="100",
                     auto_print_receipt="0", is_active=None)["id"]
    session_id = _ok("pos-open-session", conn, pos_profile_id=profile_id,
                     cashier_name="PG Tie Cashier",
                     opening_amount="0")["id"]
    day = "2026-03-12"
    txn = _ok("pos-add-transaction", conn, pos_session_id=session_id,
              customer_id=None, customer_name="PG Tie")["id"]
    _stamp(conn, txn, f"{day} 09:00:00")
    _ok("pos-add-transaction-item", conn, pos_transaction_id=txn,
        item_id=pg_book["widget_id"], item_name=None, qty="2", rate="1.00",
        uom=None, barcode=None, discount_pct=None)
    _ok("pos-add-transaction-item", conn, pos_transaction_id=txn,
        item_id=pg_book["gadget_id"], item_name=None, qty="2", rate="3.00",
        uom=None, barcode=None, discount_pct=None)
    _flip(conn, txn)
    conn.commit()

    result = call_action(
        A["pos-top-items"], conn,
        ns(from_date=day, to_date=day, company_id=pg_book["company_id"],
           limit=None))
    assert is_ok(result), f"pos-top-items failed: {result}"
    assert [(item["item_name"], item["total_qty"], item["total_revenue"],
             item["transaction_count"])
            for item in result["top_items"]] == [
        ("Gadget B", "2.00", "6.00", 1),
        ("Widget A", "2.00", "2.00", 1),
    ]
    summary = call_action(A["pos-session-summary"], conn,
                          ns(pos_session_id=session_id))
    assert is_ok(summary), f"pos-session-summary failed: {summary}"
    assert [(item["item_name"], item["total_qty"], item["total_amount"])
            for item in summary["top_items"]] == [
        ("Gadget B", "2.00", "6.00"),
        ("Widget A", "2.00", "2.00"),
    ]


@needs_pg
def test_pg_profile_creation_refuses_duplicate_id(pg_book, pg_conn):
    import types
    conn = pg_conn
    action = A["pos-add-pos-profile"]
    globals_map = action.__globals__
    real_uuid = globals_map["uuid"]
    fixed_id = "11111111-2222-4333-8444-555555555555"

    class _FixedUUID:
        @staticmethod
        def uuid4():
            return fixed_id

    globals_map["uuid"] = types.SimpleNamespace(uuid4=_FixedUUID.uuid4)
    try:
        first = call_action(
            action, conn, ns(company_id=pg_book["company_id"],
                              name="PG Collision", warehouse_id=None,
                              price_list_id=None,
                              default_payment_method="cash",
                              allow_discount="1", max_discount_pct="100",
                              auto_print_receipt="0", is_active=None))
        assert is_ok(first), f"first insert failed: {first}"
        assert first["id"] == fixed_id
        second = call_action(
            action, conn, ns(company_id=pg_book["company_id"],
                              name="PG Collision", warehouse_id=None,
                              price_list_id=None,
                              default_payment_method="cash",
                              allow_discount="1", max_discount_pct="100",
                              auto_print_receipt="0", is_active=None))
        assert second.get("status") == "error"
        assert second.get("message") == (
            "Profile creation failed \u2014 check for duplicates "
            "or invalid references")
        try:
            conn.rollback()
        except Exception:
            pass
        rows = conn.execute(
            Q.from_(Table("pos_profile")).select(Table("pos_profile").id)
            .where(Table("pos_profile").id == P()).get_sql(),
            (fixed_id,)).fetchall()
        assert len(rows) == 1
    finally:
        globals_map["uuid"] = real_uuid
