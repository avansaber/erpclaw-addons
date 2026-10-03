"""PostgreSQL proof for the POS sale discount and settlement chain.

Mirrors the SQLite Sale A (``test_sale_discount_reaches_invoice_and_cash_settles``):
2 x Widget with a 1.50 transaction discount settled by cash 20.00 posts a
paid invoice carrying the discount as a negative line plus one submitted
receipt allocated to it. The fixture also pins ``ERPCLAW_DB_PATH`` to a
non-existent ``*.sqlite`` path so a child that received it would fail —
every child must run on the PostgreSQL URL instead.
"""
import importlib.util
import json
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
    load_db_query, ns,
)
from erpclaw_lib.query import Q, P, Table, Field  # noqa: E402

MOD = load_db_query()
A = MOD.ACTIONS

DAY = "2026-03-10"


class _Clock(datetime):
    """Stands in for ``datetime`` inside the transactions module."""
    frozen = datetime(2026, 3, 10, 12, 0, 0)

    @classmethod
    def now(cls, tz=None):
        return cls.frozen


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


def _insert_account(conn, company_id, name, number, root_type, account_type):
    aid = str(uuid.uuid4())
    direction = ("debit_normal" if root_type in ("asset", "expense")
                 else "credit_normal")
    t = Table("account")
    q = (Q.into(t)
         .columns("id", "name", "account_number", "root_type",
                  "account_type", "balance_direction", "company_id", "depth")
         .insert(P(), P(), P(), P(), P(), P(), P(), P()))
    conn.execute(q.get_sql(), (aid, name, number, root_type, account_type,
                               direction, company_id, 0))
    return aid


def _seed(conn):
    setup = _load_domain("_fnd_setup_pg", "erpclaw-setup")
    inventory = _load_domain("_fnd_inventory_pg", "erpclaw-inventory")
    selling = _load_domain("_fnd_selling_pg", "erpclaw-selling")
    tag = uuid.uuid4().hex[:6]
    company_id = call_action(
        setup.ACTIONS["setup-company"], conn,
        ns(name=f"PG POS Chain Co {tag}", abbr=f"PPC{tag[:4]}",
           currency="USD", country="United States",
           fiscal_year_start_month=1, industry=None, company_id=None,
           tax_id=None))["company_id"]
    widget_code = f"WDG-{tag}-W"
    widget_id = call_action(
        inventory.ACTIONS["add-item"], conn,
        ns(item_code=widget_code, item_name="Widget", item_group=None,
           item_type="service", stock_uom="Nos",
           valuation_method="moving_average", has_batch=None,
           has_serial=None, standard_rate="10.00", custom_fields=None,
           item_status=None))["item_id"]
    customer_id = call_action(
        selling.ACTIONS["add-customer"], conn,
        ns(name="PG Chain Customer", company_id=company_id,
           customer_type="company", customer_group=None,
           payment_terms_id=None, credit_limit="0", tax_id=None,
           exempt_from_sales_tax=None, primary_address=None,
           primary_contact=None, email=None, phone=None,
           default_price_list_id=None,
           custom_fields=None))["customer_id"]
    conn.commit()

    ar = _insert_account(conn, company_id, "Accounts Receivable", "1100",
                         "asset", "receivable")
    revenue = _insert_account(conn, company_id, "Sales Revenue", "4000",
                              "income", "revenue")
    cash = _insert_account(conn, company_id, "Till Cash", "1001", "asset",
                           "cash")
    bank = _insert_account(conn, company_id, "Till Bank", "1002", "asset",
                           "bank")
    conn.commit()
    updated = call_action(
        setup.ACTIONS["update-company"], conn,
        ns(company_id=company_id, name=None, abbr=None,
           default_currency=None, country=None, tax_id=None,
           default_receivable_account_id=ar,
           default_payable_account_id=None,
           default_income_account_id=revenue,
           default_expense_account_id=None, default_cost_center_id=None,
           default_warehouse_id=None, default_bank_account_id=bank,
           default_cash_account_id=cash, round_off_account_id=None,
           exchange_gain_loss_account_id=None, perpetual_inventory=None,
           enable_negative_stock=None, accounts_frozen_till_date=None,
           role_allowed_for_frozen_entries=None,
           fiscal_year_start_month=None))
    assert is_ok(updated), f"update-company failed: {updated}"

    profile_id = _ok("pos-add-pos-profile", conn, company_id=company_id,
                     name="PG Chain Counter", warehouse_id=None,
                     price_list_id=None, default_payment_method="cash",
                     allow_discount="1", max_discount_pct="100",
                     auto_print_receipt="0", is_active=None)["id"]
    session_id = _ok("pos-open-session", conn, pos_profile_id=profile_id,
                     cashier_name="PG Cashier",
                     opening_amount="100.00")["id"]
    conn.commit()
    return {"company_id": company_id, "session_id": session_id,
            "widget_id": widget_id, "widget_code": widget_code,
            "customer_id": customer_id, "ar": ar, "revenue": revenue,
            "cash": cash, "bank": bank}


@pytest.fixture(scope="module")
def pg_ledger(tmp_path_factory):
    pg_url = _pg_url()
    if not pg_url:
        pytest.skip("ERPCLAW_PG_TEST_URL not set (live Postgres required)")
    saved = {key: os.environ.get(key) for key in
             ("ERPCLAW_DB_DIALECT", "ERPCLAW_DB_URL", "ERPCLAW_DB_PATH",
              "OPENCLAW_SKILLS_DIR", "ERPCLAW_HOME")}
    os.environ["ERPCLAW_DB_DIALECT"] = "postgresql"
    os.environ["ERPCLAW_DB_URL"] = pg_url
    bogus = str(tmp_path_factory.mktemp("pg_bogus") / "child.sqlite")
    os.environ["ERPCLAW_DB_PATH"] = bogus
    base = tmp_path_factory.mktemp("pg_chain_skills")
    skills = base / "skills"
    (skills / "erpclaw").mkdir(parents=True)
    os.symlink(os.path.join(SRC_DIR, "erpclaw", "scripts"),
               str(skills / "erpclaw" / "scripts"))
    home = base / "erpclaw_home"
    home.mkdir()
    os.symlink(os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup",
                            "lib"),
               str(home / "lib"))
    os.environ["OPENCLAW_SKILLS_DIR"] = str(skills)
    os.environ["ERPCLAW_HOME"] = str(home)
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
        # Explicit URL: ERPCLAW_DB_PATH is a bogus *.sqlite path here, so
        # the installers cannot resolve the backend from the environment.
        _load_installer("init_schema_pg_chain",
                        INIT_SCHEMA_PATH).init_db(pg_url)
        _load_installer("pos_init_pg_chain",
                        VERTICAL_INIT_PATH).create_pos_tables(pg_url)
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


@pytest.fixture
def pg_conn(pg_ledger):
    from erpclaw_lib.db import get_connection
    conn = get_connection()
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


@needs_pg
def test_pg_sale_discount_and_settlement(pg_ledger, pg_conn, monkeypatch):
    conn = pg_conn
    monkeypatch.setitem(
        A["pos-submit-transaction"].__globals__, "datetime", _Clock)
    seed = pg_ledger
    txn = _ok("pos-add-transaction", conn,
              pos_session_id=seed["session_id"],
              customer_id=seed["customer_id"],
              customer_name="Walk-in")["id"]
    _ok("pos-add-transaction-item", conn, pos_transaction_id=txn,
        item_id=seed["widget_id"], item_name=None, qty="2", rate="10.00",
        uom=None, barcode=None, discount_pct=None)
    _ok("pos-apply-discount", conn, pos_transaction_id=txn,
        discount_pct=None, discount_amount="1.50")
    _ok("pos-add-payment", conn, pos_transaction_id=txn,
        payment_method="cash", amount="20.00", reference=None)
    conn.commit()

    r = _ok("pos-submit-transaction", conn, pos_transaction_id=txn)
    assert (r["transaction_status"], r["grand_total"],
            r["change_amount"]) == ("submitted", "18.50", "1.50")
    inv_id = r["sales_invoice_id"]

    si = Table("sales_invoice")
    inv = conn.execute(
        Q.from_(si).select(si.total_amount, si.grand_total,
                           si.outstanding_amount, si.status)
        .where(si.id == P()).get_sql(), (inv_id,)).fetchone()
    assert tuple(inv) == ("18.50", "18.50", "0", "paid")

    disc = conn.execute(
        Q.from_(Table("item")).select(Table("item").star)
        .where(Field("item_code") == P()).get_sql(),
        (f"POS-DISC-{seed['company_id']}",)).fetchone()
    assert disc is not None
    assert disc["is_stock_item"] == 0
    sii = Table("sales_invoice_item")
    lines = conn.execute(
        Q.from_(sii)
        .select(sii.item_id, sii.quantity, sii.rate, sii.net_amount)
        .where(sii.sales_invoice_id == P()).get_sql(),
        (inv_id,)).fetchall()
    assert sorted(tuple(x) for x in lines) == sorted([
        (seed["widget_id"], "2.00", "10.00", "20.00"),
        (disc["id"], "1.00", "-1.50", "-1.50"),
    ])

    pp = Table("pos_payment")
    pays = conn.execute(
        Q.from_(pp).select(pp.payment_entry_id)
        .where(pp.pos_transaction_id == P()).get_sql(),
        (txn,)).fetchall()
    assert len(pays) == 1 and pays[0]["payment_entry_id"]
    pe_id = pays[0]["payment_entry_id"]
    assert r["payment_entry_ids"] == [pe_id]
    pe = Table("payment_entry")
    entry = conn.execute(
        Q.from_(pe).select(pe.payment_type, pe.paid_amount,
                           pe.unallocated_amount, pe.status, pe.posting_date,
                           pe.paid_from_account, pe.paid_to_account)
        .where(pe.id == P()).get_sql(), (pe_id,)).fetchone()
    assert tuple(entry) == ("receive", "18.50", "0.00", "submitted",
                            DAY, seed["ar"], seed["cash"])
    pa = Table("payment_allocation")
    allocs = conn.execute(
        Q.from_(pa).select(pa.voucher_type, pa.voucher_id,
                           pa.allocated_amount)
        .where(pa.payment_entry_id == P()).get_sql(),
        (pe_id,)).fetchall()
    assert [tuple(x) for x in allocs] == [("sales_invoice", inv_id, "18.50")]

    from erpclaw_lib.gl_invariants import check_gl_invariants
    assert check_gl_invariants(_pg_url())["result"] == "pass"


def _reset_pg_ledger(conn, pg_url):
    """Fresh schema, installers and seed, independent of test order.

    The module-scoped ``pg_ledger`` fixture seeds once per module, so a second
    test in this file would otherwise book on top of the first test's sale.
    Resetting here keeps this test's per-account nets exact no matter which
    tests ran before it in the same database.
    """
    conn.execute("DROP SCHEMA public CASCADE")
    conn.execute("CREATE SCHEMA public")
    conn.commit()
    _load_installer("init_schema_pg_void", INIT_SCHEMA_PATH).init_db(pg_url)
    _load_installer("pos_init_pg_void",
                    VERTICAL_INIT_PATH).create_pos_tables(pg_url)
    return _seed(conn)


@needs_pg
def test_pg_void_reverses(pg_ledger, pg_conn, monkeypatch):
    """PostgreSQL proof for the m352b void chain.

    Sales A (2 x 10.00, discount 1.50, cash 20.00) and B (1 x 5.00, card 5.00)
    are submitted, then B is voided: its receipt and invoice end cancelled and
    only A's live legs remain in the per-account nets. The migration runs twice
    against the fixture database (the installer already declared everything, so
    both runs add nothing), and the GL invariants hold.
    """
    from decimal import Decimal as _Decimal
    from erpclaw_lib.gl_invariants import check_gl_invariants

    conn = pg_conn
    monkeypatch.setitem(
        A["pos-submit-transaction"].__globals__, "datetime", _Clock)
    seed = _reset_pg_ledger(conn, _pg_url())

    def _sale(qty, rate, method, amount, discount=None):
        txn = _ok("pos-add-transaction", conn,
                  pos_session_id=seed["session_id"],
                  customer_id=seed["customer_id"],
                  customer_name="Walk-in")["id"]
        _ok("pos-add-transaction-item", conn, pos_transaction_id=txn,
            item_id=seed["widget_id"], item_name=None, qty=qty, rate=rate,
            uom=None, barcode=None, discount_pct=None)
        if discount is not None:
            _ok("pos-apply-discount", conn, pos_transaction_id=txn,
                discount_pct=None, discount_amount=discount)
        _ok("pos-add-payment", conn, pos_transaction_id=txn,
            payment_method=method, amount=amount, reference=None)
        conn.commit()
        return txn

    def _pg_net(account_id):
        g = Table("gl_entry")
        rows = conn.execute(
            Q.from_(g).select(g.debit, g.credit, g.is_cancelled)
            .where(g.account_id == P()).get_sql(), (account_id,)).fetchall()
        total = sum((_Decimal(str(r[0])) - _Decimal(str(r[1]))
                     for r in rows if not r[2]), _Decimal("0"))
        return str(total.quantize(_Decimal("0.01")))

    a = _sale("2", "10.00", "cash", "20.00", discount="1.50")
    ra = _ok("pos-submit-transaction", conn, pos_transaction_id=a)
    b = _sale("1", "5.00", "card", "5.00")
    rb = _ok("pos-submit-transaction", conn, pos_transaction_id=b)
    assert rb["grand_total"] == "5.00"
    inv_b = rb["sales_invoice_id"]
    pe_b = rb["payment_entry_ids"][0]
    conn.commit()

    r = _ok("pos-void-transaction", conn, pos_transaction_id=b)
    assert r["transaction_status"] == "voided"
    assert r["cancelled_sales_invoice_id"] == inv_b
    assert r["cancelled_payment_entry_ids"] == [pe_b]

    pe = Table("payment_entry")
    entry = conn.execute(
        Q.from_(pe).select(pe.status)
        .where(pe.id == P()).get_sql(), (pe_b,)).fetchone()
    assert tuple(entry) == ("cancelled",)

    si = Table("sales_invoice")
    inv = conn.execute(
        Q.from_(si).select(si.status, si.outstanding_amount)
        .where(si.id == P()).get_sql(), (inv_b,)).fetchone()
    assert tuple(inv) == ("cancelled", "0")
    inv_a = conn.execute(
        Q.from_(si).select(si.status)
        .where(si.id == P()).get_sql(),
        (ra["sales_invoice_id"],)).fetchone()
    assert tuple(inv_a) == ("paid",)

    assert _pg_net(seed["ar"]) == "0.00"
    assert _pg_net(seed["revenue"]) == "-18.50"
    assert _pg_net(seed["cash"]) == "18.50"
    assert _pg_net(seed["bank"]) == "0.00"

    mig_path = os.path.join(SRC_DIR, "erpclaw-addons", "erpclaw-pos",
                            "migrations", "001_pos_return_links.py")
    mig_spec = importlib.util.spec_from_file_location(
        "pos_migration_001_pg", mig_path)
    mig = importlib.util.module_from_spec(mig_spec)
    mig_spec.loader.exec_module(mig)
    pg_url = _pg_url()
    assert mig.run_migration(pg_url)["added"] == []
    assert mig.run_migration(pg_url)["added"] == []

    assert check_gl_invariants(pg_url)["result"] == "pass"


@needs_pg
def test_pg_partial_return(pg_ledger, pg_conn, monkeypatch):
    """PostgreSQL proof for the m352c return chain (first return of Sale A).

    2 x Widget with a 1.50 transaction discount settled by cash 20.00, then
    a return of 1 x Widget: a -9.25 credit note against the sale's invoice
    plus a 9.25 customer refund allocated to it. Same figures as the SQLite
    test_partial_return_credit_note_and_refund; the GL invariants hold (this
    library holds no party-ledger check, so INV-22 does not apply).
    """
    from decimal import Decimal as _Decimal
    from erpclaw_lib.gl_invariants import check_gl_invariants

    conn = pg_conn
    monkeypatch.setitem(
        A["pos-submit-transaction"].__globals__, "datetime", _Clock)
    seed = _reset_pg_ledger(conn, _pg_url())

    txn = _ok("pos-add-transaction", conn,
              pos_session_id=seed["session_id"],
              customer_id=seed["customer_id"],
              customer_name="Walk-in")["id"]
    _ok("pos-add-transaction-item", conn, pos_transaction_id=txn,
        item_id=seed["widget_id"], item_name=None, qty="2", rate="10.00",
        uom=None, barcode=None, discount_pct=None)
    _ok("pos-apply-discount", conn, pos_transaction_id=txn,
        discount_pct=None, discount_amount="1.50")
    _ok("pos-add-payment", conn, pos_transaction_id=txn,
        payment_method="cash", amount="20.00", reference=None)
    conn.commit()

    r = _ok("pos-submit-transaction", conn, pos_transaction_id=txn)
    inv_id = r["sales_invoice_id"]

    ti = Table("pos_transaction_item")
    line = conn.execute(
        Q.from_(ti).select(ti.id)
        .where(ti.pos_transaction_id == P()).get_sql(),
        (txn,)).fetchone()[0]
    ret = _ok("pos-return-transaction", conn, pos_transaction_id=txn,
              items=json.dumps(
                  [{"pos_transaction_item_id": line, "qty": "1"}]))
    assert ret["original_transaction_id"] == txn
    assert ret["transaction_status"] == "submitted"
    assert ret["return_grand_total"] == "-9.25"
    cn = ret["credit_note_id"]
    pe_id = ret["refund_payment_entry_id"]

    si = Table("sales_invoice")
    inv = conn.execute(
        Q.from_(si)
        .select(si.is_return, si.return_against, si.total_amount,
                si.grand_total, si.outstanding_amount, si.status,
                si.posting_date)
        .where(si.id == P()).get_sql(), (cn,)).fetchone()
    assert tuple(inv) == (1, inv_id, "-9.25", "-9.25", "0", "paid", DAY)

    disc = conn.execute(
        Q.from_(Table("item")).select(Table("item").id)
        .where(Field("item_code") == P()).get_sql(),
        (f"POS-DISC-{seed['company_id']}",)).fetchone()[0]
    sii = Table("sales_invoice_item")
    lines = conn.execute(
        Q.from_(sii)
        .select(sii.item_id, sii.quantity, sii.rate, sii.net_amount)
        .where(sii.sales_invoice_id == P()).get_sql(),
        (cn,)).fetchall()
    assert sorted(tuple(x) for x in lines) == sorted([
        (seed["widget_id"], "-1.00", "10.00", "-10.00"),
        (disc, "-1.00", "-0.75", "0.75"),
    ])

    pt = Table("pos_transaction")
    doc = conn.execute(
        Q.from_(pt)
        .select(pt.status, pt.subtotal, pt.discount_amount, pt.grand_total,
                pt.paid_amount, pt.sales_invoice_id, pt.return_against_id,
                pt.pos_session_id)
        .where(pt.id == P()).get_sql(),
        (ret["return_transaction_id"],)).fetchone()
    assert tuple(doc) == ("returned", "-10.00", "-0.75", "-9.25", "-9.25",
                          cn, txn, seed["session_id"])

    pp = Table("pos_payment")
    pays = conn.execute(
        Q.from_(pp)
        .select(pp.payment_method, pp.amount, pp.payment_entry_id,
                pp.reference)
        .where(pp.pos_transaction_id == P()).get_sql(),
        (ret["return_transaction_id"],)).fetchall()
    assert [tuple(x) for x in pays] == [
        ("cash", "-9.25", pe_id, f"Return of {txn}")]

    pe = Table("payment_entry")
    entry = conn.execute(
        Q.from_(pe).select(pe.payment_type, pe.party_type, pe.paid_amount,
                           pe.unallocated_amount, pe.status,
                           pe.paid_from_account, pe.paid_to_account)
        .where(pe.id == P()).get_sql(), (pe_id,)).fetchone()
    assert tuple(entry) == ("pay", "customer", "9.25", "0.00", "submitted",
                            seed["cash"], seed["ar"])
    pa = Table("payment_allocation")
    allocs = conn.execute(
        Q.from_(pa).select(pa.voucher_type, pa.voucher_id,
                           pa.allocated_amount)
        .where(pa.payment_entry_id == P()).get_sql(),
        (pe_id,)).fetchall()
    assert [tuple(x) for x in allocs] == [("credit_note", cn, "9.25")]

    sale = conn.execute(
        Q.from_(pt).select(pt.status)
        .where(pt.id == P()).get_sql(), (txn,)).fetchone()
    assert tuple(sale) == ("submitted",)

    def _pg_net(account_id):
        g = Table("gl_entry")
        rows = conn.execute(
            Q.from_(g).select(g.debit, g.credit, g.is_cancelled)
            .where(g.account_id == P()).get_sql(), (account_id,)).fetchall()
        total = sum((_Decimal(str(r[0])) - _Decimal(str(r[1]))
                     for r in rows if not r[2]), _Decimal("0"))
        return str(total.quantize(_Decimal("0.01")))

    assert _pg_net(seed["ar"]) == "0.00"
    assert _pg_net(seed["revenue"]) == "-9.25"
    assert _pg_net(seed["cash"]) == "9.25"

    assert check_gl_invariants(_pg_url())["result"] == "pass"
