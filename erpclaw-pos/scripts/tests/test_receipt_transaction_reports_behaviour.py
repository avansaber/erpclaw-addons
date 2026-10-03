"""Behaviour of pos-generate-receipt, pos-get-transaction, pos-daily-report,
pos-hourly-sales, pos-top-items and pos-cashier-performance, read back against
a fully known day of POS trading.

One book is built once for the module (every submit runs the selling module in
a subprocess, which is slow) and every test only reads it:

Company A, session S1 (cashier "Test Cashier"), opened 2026-03-10 08:00:00
  T1 09:15 Widget A 3 @ 12.50 = 37.50; Gadget B 1 @ 80.00 less 10 % = 72.00;
     paid card 60.00 + cash 60.00 = 120.00; submitted, change 10.50
  T2 09:40 Cable C 10 @ 4.00 = 40.00; Widget A 6 @ 12.50 = 75.00;
     transaction discount 10 % = 11.50; grand 103.50; paid mobile 103.50
  T3 14:05 Gadget B 2 @ 80.00 = 160.00; paid cash 200.00; submitted, then
     returned at 14:30 (return document T3R)
  T4 11:30 Widget A 40 @ 12.50, left in draft
  T5 11:45 Cable C 30 @ 4.00, paid card 120.00, voided
Company A, session S2 (cashier "Dana"), opened 2026-03-10 13:00:00
  T6 14:20 Cable C 2 @ 5.00 = 10.00; paid cash 10.00
Company A, session S2b (cashier "Dana"), opened 2026-03-11 09:00:00
  T7 2026-03-11 10:00 Gadget B 1 @ 11.01 = 11.01; paid card 11.01
Company A, session S3 (cashier "Lee"), opened 2026-03-10 16:00:00, no sales
Company B, session S4 (cashier "Test Cashier"), opened 2026-03-10 08:30:00
  T8 09:20 Widget A 4 @ 5.00 = 20.00; paid cash 20.00

The rows' created_at / opened_at default to the database clock, so after each
document is written the test moves its timestamp to the fixed time above; the
invoice posting date comes from the submit handler's clock, which is frozen to
the same instant. Nothing depends on today's date or the machine timezone.
"""
import os
import sys
from datetime import datetime

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest  # noqa: E402
from pos_helpers import (  # noqa: E402
    SRC_DIR, build_env, call_action, get_conn, init_all_tables, is_error,
    is_ok, load_db_query, ns, seed_item, seed_open_session, seed_pos_profile,
    seed_till_accounts,
)
from erpclaw_lib.query import Q, P, Table, Field  # noqa: E402

MOD = load_db_query()
A = MOD.ACTIONS


class _Clock(datetime):
    """Stands in for ``datetime`` inside the transactions module."""
    frozen = datetime(2026, 3, 10, 12, 0, 0)

    @classmethod
    def now(cls, tz=None):
        return cls.frozen


def _ok(action, conn, **kw):
    r = call_action(A[action], conn, ns(**kw))
    assert is_ok(r), f"{action} failed: {r}"
    return r


def _set_ts(conn, table, column, row_id, ts):
    t = Table(table)
    conn.execute(Q.update(t).set(t[column], P()).where(t.id == P()).get_sql(),
                 (ts, row_id))
    conn.commit()


def _line(conn, txn, item, qty, rate, disc=None):
    _ok("pos-add-transaction-item", conn, pos_transaction_id=txn, item_id=item,
        item_name=None, qty=qty, rate=rate, uom=None, barcode=None,
        discount_pct=disc)


def _pay(conn, txn, method, amount):
    _ok("pos-add-payment", conn, pos_transaction_id=txn, payment_method=method,
        amount=amount, reference=None)


def _txn(conn, session, customer, ts):
    r = _ok("pos-add-transaction", conn, pos_session_id=session,
            customer_id=customer, customer_name="Walk-in Pat")
    _set_ts(conn, "pos_transaction", "created_at", r["id"], ts)
    return r["id"]


def _submit(conn, txn, ts):
    _Clock.frozen = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    return _ok("pos-submit-transaction", conn, pos_transaction_id=txn)


def _open(conn, profile, cashier, ts):
    sid = seed_open_session(conn, profile, cashier=cashier)
    _set_ts(conn, "pos_session", "opened_at", sid, ts)
    return sid


def _close(conn, sid, amount):
    _ok("pos-close-session", conn, id=sid, closing_amount=amount)


def _row(conn, table, row_id, *cols):
    t = Table(table)
    return conn.execute(Q.from_(t).select(*[t[c] for c in cols])
                        .where(t.id == P()).get_sql(), (row_id,)).fetchone()


@pytest.fixture(scope="module")
def book(tmp_path_factory):
    base = tmp_path_factory.mktemp("pos_book")
    path = str(base / "book.sqlite")
    init_all_tables(path)
    skills = base / "skills"
    (skills / "erpclaw").mkdir(parents=True)
    os.symlink(os.path.join(SRC_DIR, "erpclaw", "scripts"),
               str(skills / "erpclaw" / "scripts"))
    home = base / "erpclaw_home"
    home.mkdir()
    os.symlink(os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup", "lib"),
               str(home / "lib"))
    submit = A["pos-submit-transaction"]
    conn = get_conn(path)
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("ERPCLAW_DB_PATH", path)
        mp.setenv("OPENCLAW_SKILLS_DIR", str(skills))
        mp.setenv("ERPCLAW_HOME", str(home))
        mp.setitem(submit.__globals__, "datetime", _Clock)

        ea = build_env(conn)
        eb = build_env(conn)
        seed_till_accounts(conn, ea["company_id"])
        seed_till_accounts(conn, eb["company_id"])
        widget, cust = ea["item_id"], ea["customer_id"]
        gadget = seed_item(conn, "Gadget B", "GDG-B",
                                is_stock_item=0)
        cable = seed_item(conn, "Cable C", "CBL-C")
        s1 = ea["session_id"]
        _set_ts(conn, "pos_session", "opened_at", s1, "2026-03-10 08:00:00")
        s4 = eb["session_id"]
        _set_ts(conn, "pos_session", "opened_at", s4, "2026-03-10 08:30:00")
        p2 = seed_pos_profile(conn, ea["company_id"], name="Counter 2")

        t1 = _txn(conn, s1, cust, "2026-03-10 09:15:00")
        _line(conn, t1, widget, "3", "12.50")
        _line(conn, t1, gadget, "1", "80.00", "10")
        _pay(conn, t1, "card", "60.00")
        _pay(conn, t1, "cash", "60.00")
        _submit(conn, t1, "2026-03-10 09:15:00")

        t2 = _txn(conn, s1, cust, "2026-03-10 09:40:00")
        _line(conn, t2, cable, "10", "4.00")
        _line(conn, t2, widget, "6", "12.50")
        _ok("pos-apply-discount", conn, pos_transaction_id=t2,
            discount_pct="10", discount_amount=None)
        _pay(conn, t2, "mobile", "103.50")
        _submit(conn, t2, "2026-03-10 09:40:00")

        t3 = _txn(conn, s1, cust, "2026-03-10 14:05:00")
        _line(conn, t3, gadget, "2", "80.00")
        _pay(conn, t3, "cash", "200.00")
        _submit(conn, t3, "2026-03-10 14:05:00")
        t3r = _ok("pos-return-transaction", conn,
                  pos_transaction_id=t3)["return_transaction_id"]
        _set_ts(conn, "pos_transaction", "created_at", t3r, "2026-03-10 14:30:00")

        t4 = _txn(conn, s1, cust, "2026-03-10 11:30:00")
        _line(conn, t4, widget, "40", "12.50")

        t5 = _txn(conn, s1, cust, "2026-03-10 11:45:00")
        _line(conn, t5, cable, "30", "4.00")
        _pay(conn, t5, "card", "120.00")
        _ok("pos-void-transaction", conn, pos_transaction_id=t5)
        _close(conn, s1, "150.00")

        s2 = _open(conn, p2, "Dana", "2026-03-10 13:00:00")
        t6 = _txn(conn, s2, cust, "2026-03-10 14:20:00")
        _line(conn, t6, cable, "2", "5.00")
        _pay(conn, t6, "cash", "10.00")
        _submit(conn, t6, "2026-03-10 14:20:00")
        _close(conn, s2, "10.00")

        s2b = _open(conn, p2, "Dana", "2026-03-11 09:00:00")
        t7 = _txn(conn, s2b, cust, "2026-03-11 10:00:00")
        _line(conn, t7, gadget, "1", "11.01")
        _pay(conn, t7, "card", "11.01")
        _submit(conn, t7, "2026-03-11 10:00:00")

        p3 = seed_pos_profile(conn, ea["company_id"], name="Counter 3")
        s3 = _open(conn, p3, "Lee", "2026-03-10 16:00:00")

        t8 = _txn(conn, s4, eb["customer_id"], "2026-03-10 09:20:00")
        _line(conn, t8, eb["item_id"], "4", "5.00")
        _pay(conn, t8, "cash", "20.00")
        _submit(conn, t8, "2026-03-10 09:20:00")

        yield {
            "conn": conn, "a": ea["company_id"], "b": eb["company_id"],
            "customer": cust, "widget": widget, "gadget": gadget, "cable": cable,
            "widget_b": eb["item_id"], "s1": s1, "s2": s2, "s2b": s2b, "s3": s3,
            "s4": s4,
            "t1": t1, "t2": t2, "t3": t3, "t3r": t3r, "t4": t4, "t5": t5,
            "t6": t6, "t7": t7, "t8": t8,
        }
    conn.close()


def _code(conn, item_id):
    return _row(conn, "item", item_id, "item_code")["item_code"]


def _receipt_lines(r):
    return sorted((i["item_name"], i["qty"], i["rate"], i["discount_amount"],
                   i["amount"], i["uom"]) for i in r["items"])


# ---------------------------------------------------------------------------
# pos-get-transaction
# ---------------------------------------------------------------------------

def test_get_transaction_reads_totals_lines_payments_and_invoice_link(book):
    conn = book["conn"]
    r = call_action(A["pos-get-transaction"], conn,
                    ns(id=book["t1"], pos_transaction_id=None))
    assert is_ok(r)
    db = _row(conn, "pos_transaction", book["t1"], "naming_series",
              "sales_invoice_id", "status", "grand_total")
    assert (db["status"], db["grand_total"]) == ("submitted", "109.50")
    assert (r["transaction_status"], r["subtotal"], r["discount_pct"],
            r["discount_amount"], r["tax_amount"], r["grand_total"],
            r["paid_amount"], r["change_amount"]) == (
        "submitted", "109.50", "0", "0", "0", "109.50", "120.00", "10.50")
    assert r["receipt_number"] == db["naming_series"]
    assert r["sales_invoice_id"] == db["sales_invoice_id"]
    assert (r["company_id"], r["pos_session_id"], r["customer_id"],
            r["customer_name"]) == (book["a"], book["s1"], book["customer"],
                                    "Walk-in Pat")
    assert sorted((i["item_id"], i["item_code"], i["qty"], i["rate"],
                   i["discount_pct"], i["discount_amount"], i["amount"])
                  for i in r["items"]) == sorted([
        (book["widget"], _code(conn, book["widget"]), "3", "12.50", "0", "0.00", "37.50"),
        (book["gadget"], _code(conn, book["gadget"]), "1", "80.00", "10", "8.00", "72.00"),
    ])
    assert sorted((p["payment_method"], p["amount"]) for p in r["payments"]) == [
        ("card", "60.00"), ("cash", "60.00")]


def test_get_transaction_reads_transaction_discount_and_return_document(book):
    conn = book["conn"]
    r2 = call_action(A["pos-get-transaction"], conn,
                     ns(id=None, pos_transaction_id=book["t2"]))
    assert (r2["subtotal"], r2["discount_pct"], r2["discount_amount"],
            r2["grand_total"], r2["paid_amount"], r2["change_amount"]) == (
        "115.00", "10", "11.50", "103.50", "103.50", "0.00")

    orig = call_action(A["pos-get-transaction"], conn, ns(id=book["t3"], pos_transaction_id=None))
    assert (orig["transaction_status"], orig["grand_total"],
            orig["change_amount"]) == ("returned", "160.00", "40.00")

    ret = call_action(A["pos-get-transaction"], conn, ns(id=book["t3r"], pos_transaction_id=None))
    assert (ret["transaction_status"], ret["subtotal"], ret["discount_amount"],
            ret["tax_amount"], ret["grand_total"], ret["paid_amount"],
            ret["change_amount"], ret["receipt_number"]) == (
        "returned", "-160.00", "0.00", "0.00", "-160.00", "-160.00", "0", None)
    assert ret["sales_invoice_id"]
    t3_inv = _row(conn, "pos_transaction", book["t3"],
                  "sales_invoice_id")["sales_invoice_id"]
    cn = _row(conn, "sales_invoice", ret["sales_invoice_id"], "is_return",
              "return_against", "grand_total", "status")
    assert (cn["is_return"], cn["return_against"], cn["grand_total"],
            cn["status"]) == (1, t3_inv, "-160.00", "paid")
    assert [(i["item_id"], i["qty"], i["rate"], i["amount"]) for i in ret["items"]] == [
        (book["gadget"], "-2.00", "80.00", "-160.00")]
    assert [(p["payment_method"], p["amount"], p["reference"])
            for p in ret["payments"]] == [
        ("cash", "-160.00", f"Return of {book['t3']}")]


def test_get_transaction_refusals(book):
    conn = book["conn"]
    r = call_action(A["pos-get-transaction"], conn, ns(id=None, pos_transaction_id=None))
    assert is_error(r) and r["message"] == "--id is required"
    r = call_action(A["pos-get-transaction"], conn, ns(id="no-such-txn", pos_transaction_id=None))
    assert is_error(r) and r["message"] == "Transaction no-such-txn not found"


def test_submit_posts_a_balanced_invoice_for_the_line_discounted_sale(book):
    conn = book["conn"]
    inv_id = _row(conn, "pos_transaction", book["t1"], "sales_invoice_id")["sales_invoice_id"]
    inv = _row(conn, "sales_invoice", inv_id, "status", "posting_date",
               "grand_total", "customer_id", "company_id")
    assert (inv["status"], inv["posting_date"], inv["grand_total"],
            inv["customer_id"], inv["company_id"]) == (
        "paid", "2026-03-10", "109.50", book["customer"], book["a"])
    co = _row(conn, "company", book["a"], "default_receivable_account_id",
              "default_income_account_id")
    g = Table("gl_entry")
    legs = conn.execute(
        Q.from_(g).select(g.account_id, g.debit, g.credit, g.is_cancelled,
                          g.posting_date)
        .where(g.voucher_type == P()).where(g.voucher_id == P()).get_sql(),
        ("sales_invoice", inv_id)).fetchall()
    assert sorted(tuple(x) for x in legs) == sorted([
        (co["default_receivable_account_id"], "109.50", "0.00", 0, "2026-03-10"),
        (co["default_income_account_id"], "0.00", "109.50", 0, "2026-03-10"),
    ])


def test_submit_settles_every_sale_and_carries_the_discount(book):
    conn = book["conn"]
    t2_inv = _row(conn, "pos_transaction", book["t2"],
                  "sales_invoice_id")["sales_invoice_id"]
    disc_row = conn.execute(
        Q.from_(Table("item")).select(Table("item").id)
        .where(Table("item").item_code == P()).get_sql(),
        (f"POS-DISC-{book['a']}",)).fetchone()
    assert disc_row is not None
    disc_id = disc_row["id"]
    sii = Table("sales_invoice_item")
    lines = conn.execute(
        Q.from_(sii)
        .select(sii.item_id, sii.quantity, sii.rate, sii.net_amount)
        .where(sii.sales_invoice_id == P()).get_sql(),
        (t2_inv,)).fetchall()
    assert sorted(tuple(x) for x in lines) == sorted([
        (book["cable"], "10.00", "4.00", "40.00"),
        (book["widget"], "6.00", "12.50", "75.00"),
        (disc_id, "1.00", "-11.50", "-11.50"),
    ])
    t2_inv_row = _row(conn, "sales_invoice", t2_inv, "grand_total")
    assert t2_inv_row["grand_total"] == "103.50"

    for txn_key in ("t1", "t2", "t3", "t6", "t7", "t8"):
        inv_id = _row(conn, "pos_transaction", book[txn_key],
                      "sales_invoice_id")["sales_invoice_id"]
        assert inv_id, txn_key
        inv = _row(conn, "sales_invoice", inv_id, "status")
        assert inv["status"] == "paid", txn_key

    expected = {
        "t1": {"cash": "49.50", "card": "60.00"},
        "t2": {"mobile": "103.50"},
        "t3": {"cash": "160.00"},
        "t6": {"cash": "10.00"},
        "t7": {"card": "11.01"},
        "t8": {"cash": "20.00"},
    }
    pe = Table("payment_entry")
    pp = Table("pos_payment")
    for txn_key, methods in expected.items():
        rows = conn.execute(
            Q.from_(pp).select(pp.payment_method, pp.payment_entry_id)
            .where(pp.pos_transaction_id == P()).get_sql(),
            (book[txn_key],)).fetchall()
        assert {r["payment_method"] for r in rows} == set(methods), txn_key
        for r in rows:
            assert r["payment_entry_id"], (txn_key, r["payment_method"])
            entry = conn.execute(
                Q.from_(pe).select(pe.paid_amount, pe.status)
                .where(pe.id == P()).get_sql(),
                (r["payment_entry_id"],)).fetchone()
            assert (entry["status"], entry["paid_amount"]) == (
                "submitted", methods[r["payment_method"]]), txn_key


# ---------------------------------------------------------------------------
# pos-generate-receipt
# ---------------------------------------------------------------------------

def test_generate_receipt_for_a_split_payment_sale(book):
    conn = book["conn"]
    r = call_action(A["pos-generate-receipt"], conn, ns(pos_transaction_id=book["t1"]))
    assert is_ok(r)
    db = _row(conn, "pos_transaction", book["t1"], "naming_series")
    company = _row(conn, "company", book["a"], "name")
    assert (r["receipt_number"], r["naming_series"], r["id"], r["date"],
            r["company_name"], r["customer_name"]) == (
        db["naming_series"], db["naming_series"], book["t1"],
        "2026-03-10 09:15:00", company["name"], "Walk-in Pat")
    assert _receipt_lines(r) == [
        ("Gadget B", "1", "80.00", "8.00", "72.00", "Nos"),
        ("Widget A", "3", "12.50", "0.00", "37.50", "Nos"),
    ]
    assert (r["subtotal"], r["discount_pct"], r["discount_amount"], r["tax_amount"],
            r["grand_total"], r["paid_amount"], r["change_amount"], r["item_count"]) == (
        "109.50", "0", "0", "0", "109.50", "120.00", "10.50", 2)
    assert sorted((p["payment_method"], p["amount"]) for p in r["payments"]) == [
        ("card", "60.00"), ("cash", "60.00")]


def test_generate_receipt_for_discounted_returned_and_return_documents(book):
    conn = book["conn"]
    r2 = call_action(A["pos-generate-receipt"], conn, ns(pos_transaction_id=book["t2"]))
    assert _receipt_lines(r2) == [
        ("Cable C", "10", "4.00", "0.00", "40.00", "Nos"),
        ("Widget A", "6", "12.50", "0.00", "75.00", "Nos"),
    ]
    assert (r2["subtotal"], r2["discount_pct"], r2["discount_amount"],
            r2["grand_total"], r2["paid_amount"], r2["change_amount"]) == (
        "115.00", "10", "11.50", "103.50", "103.50", "0.00")
    assert [(p["payment_method"], p["amount"]) for p in r2["payments"]] == [
        ("mobile", "103.50")]

    r3 = call_action(A["pos-generate-receipt"], conn, ns(pos_transaction_id=book["t3"]))
    assert (r3["grand_total"], r3["paid_amount"], r3["change_amount"],
            r3["item_count"]) == ("160.00", "200.00", "40.00", 1)

    rr = call_action(A["pos-generate-receipt"], conn, ns(pos_transaction_id=book["t3r"]))
    assert (rr["receipt_number"], rr["date"], rr["grand_total"], rr["paid_amount"],
            rr["change_amount"]) == (None, "2026-03-10 14:30:00", "-160.00", "-160.00", "0")
    assert _receipt_lines(rr) == [("Gadget B", "-2.00", "80.00", "0.00", "-160.00", "Nos")]


def test_generate_receipt_refusals_write_nothing(book):
    conn = book["conn"]
    r = call_action(A["pos-generate-receipt"], conn, ns(pos_transaction_id=None))
    assert is_error(r) and r["message"] == "--pos-transaction-id is required"
    r = call_action(A["pos-generate-receipt"], conn, ns(pos_transaction_id="no-such-txn"))
    assert is_error(r) and r["message"] == "Transaction no-such-txn not found"
    r = call_action(A["pos-generate-receipt"], conn, ns(pos_transaction_id=book["t4"]))
    assert is_error(r)
    assert r["message"] == ("Receipt can only be generated for submitted/returned "
                            "transactions (current: draft)")
    r = call_action(A["pos-generate-receipt"], conn, ns(pos_transaction_id=book["t5"]))
    assert r["message"] == ("Receipt can only be generated for submitted/returned "
                            "transactions (current: voided)")
    for t, status, grand in ((book["t4"], "draft", "500.00"), (book["t5"], "voided", "120.00")):
        row = _row(conn, "pos_transaction", t, "status", "receipt_number",
                   "grand_total", "change_amount", "sales_invoice_id")
        assert tuple(row) == (status, None, grand, "0", None)


# ---------------------------------------------------------------------------
# pos-daily-report
# ---------------------------------------------------------------------------

def test_daily_report_counts_only_submitted_sales_of_the_company_and_day(book):
    conn = book["conn"]
    r = call_action(A["pos-daily-report"], conn, ns(date="2026-03-10", company_id=book["a"]))
    assert is_ok(r)
    assert (r["report_date"], r["transaction_count"], r["total_sales"],
            r["total_discounts"], r["total_tax"], r["sessions_count"]) == (
        "2026-03-10", 4, "383.00", "11.50", "0.00", 3)
    # A returned original stays a sale; only the return document is the
    # return. Ordered by the numeric total, so 270.00 ranks above 103.50
    # and 60.00.
    assert r["payment_methods"] == [
        {"method": "cash", "count": 3, "total": "270.00"},
        {"method": "mobile", "count": 1, "total": "103.50"},
        {"method": "card", "count": 1, "total": "60.00"},
    ]


def test_daily_report_other_company_next_day_all_companies_and_empty_day(book):
    conn = book["conn"]
    rb = call_action(A["pos-daily-report"], conn, ns(date="2026-03-10", company_id=book["b"]))
    assert (rb["transaction_count"], rb["total_sales"], rb["total_discounts"],
            rb["sessions_count"], rb["payment_methods"]) == (
        1, "20.00", "0.00", 1, [{"method": "cash", "count": 1, "total": "20.00"}])

    r11 = call_action(A["pos-daily-report"], conn, ns(date="2026-03-11", company_id=book["a"]))
    assert (r11["transaction_count"], r11["total_sales"], r11["sessions_count"],
            r11["payment_methods"]) == (
        1, "11.01", 1, [{"method": "card", "count": 1, "total": "11.01"}])

    both = call_action(A["pos-daily-report"], conn, ns(date="2026-03-10", company_id=None))
    assert (both["transaction_count"], both["total_sales"], both["sessions_count"]) == (
        5, "403.00", 4)
    assert both["payment_methods"][0] == {"method": "cash", "count": 4, "total": "290.00"}

    empty = call_action(A["pos-daily-report"], conn, ns(date="2026-03-12", company_id=book["a"]))
    assert (empty["transaction_count"], empty["total_sales"], empty["total_discounts"],
            empty["total_tax"], empty["payment_methods"], empty["sessions_count"]) == (
        0, "0.00", "0.00", "0.00", [], 0)


# ---------------------------------------------------------------------------
# pos-hourly-sales
# ---------------------------------------------------------------------------

def test_hourly_sales_buckets_submitted_sales_by_stored_hour(book):
    conn = book["conn"]
    r = call_action(A["pos-hourly-sales"], conn, ns(date="2026-03-10", company_id=book["a"]))
    assert is_ok(r)
    # Draft (11:30) and voided (11:45) contribute nothing; the returned
    # original (14:05) stays a sale and only the return document (14:30) is
    # excluded; company B's 09:20 sale is excluded.
    assert r["hourly_breakdown"] == [
        {"hour": "09", "hour_label": "09:00-09:59", "transaction_count": 2,
         "total_sales": "213.00"},
        {"hour": "14", "hour_label": "14:00-14:59", "transaction_count": 2,
         "total_sales": "170.00"},
    ]
    assert (r["report_date"], r["total_transactions"], r["total_sales"],
            r["peak_hour"], r["peak_hour_sales"]) == (
        "2026-03-10", 4, "383.00", "09:00-09:59", "213.00")


def test_hourly_sales_other_company_and_empty_day(book):
    conn = book["conn"]
    rb = call_action(A["pos-hourly-sales"], conn, ns(date="2026-03-10", company_id=book["b"]))
    assert rb["hourly_breakdown"] == [
        {"hour": "09", "hour_label": "09:00-09:59", "transaction_count": 1,
         "total_sales": "20.00"}]
    r11 = call_action(A["pos-hourly-sales"], conn, ns(date="2026-03-11", company_id=book["a"]))
    assert ([(h["hour"], h["total_sales"]) for h in r11["hourly_breakdown"]],
            r11["peak_hour_sales"]) == ([("10", "11.01")], "11.01")
    empty = call_action(A["pos-hourly-sales"], conn, ns(date="2026-03-12", company_id=book["a"]))
    assert (empty["hourly_breakdown"], empty["total_transactions"], empty["total_sales"],
            empty["peak_hour"], empty["peak_hour_sales"]) == ([], 0, "0.00", None, "0.00")


# ---------------------------------------------------------------------------
# pos-top-items
# ---------------------------------------------------------------------------

def test_top_items_ranks_by_numeric_quantity_with_revenue(book):
    conn = book["conn"]
    r = call_action(A["pos-top-items"], conn, ns(
        from_date="2026-03-10", to_date="2026-03-10", company_id=book["a"], limit=None))
    assert is_ok(r)
    # Quantity 12 ranks above 9: a text ordering would put "9" first. The draft
    # 40 Widget A and the voided 30 Cable C are excluded; the returned 2
    # Gadget B stay a sale and only the return document is excluded.
    assert [(i["item_id"], i["item_name"], i["total_qty"], i["total_revenue"],
             i["transaction_count"]) for i in r["top_items"]] == [
        (book["cable"], "Cable C", "12.00", "50.00", 2),
        (book["widget"], "Widget A", "9.00", "112.50", 2),
        (book["gadget"], "Gadget B", "3.00", "232.00", 2),
    ]
    assert (r["from_date"], r["to_date"], r["count"]) == ("2026-03-10", "2026-03-10", 3)
    assert r["top_items"][0]["item_code"] == _code(conn, book["cable"])


def test_top_items_date_range_limit_and_company_scope(book):
    conn = book["conn"]
    r = call_action(A["pos-top-items"], conn, ns(
        from_date="2026-03-10", to_date="2026-03-11", company_id=book["a"], limit=None))
    assert [(i["item_name"], i["total_qty"], i["total_revenue"], i["transaction_count"])
            for i in r["top_items"]] == [
        ("Cable C", "12.00", "50.00", 2), ("Widget A", "9.00", "112.50", 2),
        ("Gadget B", "4.00", "243.01", 3)]
    r = call_action(A["pos-top-items"], conn, ns(
        from_date="2026-03-10", to_date="2026-03-11", company_id=book["a"], limit=2))
    assert ([i["item_name"] for i in r["top_items"]], r["count"]) == (
        ["Cable C", "Widget A"], 2)
    rb = call_action(A["pos-top-items"], conn, ns(
        from_date="2026-03-10", to_date="2026-03-10", company_id=book["b"], limit=None))
    assert [(i["item_id"], i["total_qty"], i["total_revenue"]) for i in rb["top_items"]] == [
        (book["widget_b"], "4.00", "20.00")]
    none = call_action(A["pos-top-items"], conn, ns(
        from_date="2026-03-12", to_date="2026-03-31", company_id=book["a"], limit=None))
    assert (none["top_items"], none["count"]) == ([], 0)


# ---------------------------------------------------------------------------
# pos-cashier-performance
# ---------------------------------------------------------------------------

def test_cashier_performance_counts_submitted_sales_per_cashier(book):
    conn = book["conn"]
    r = call_action(A["pos-cashier-performance"], conn, ns(
        from_date="2026-03-10", to_date="2026-03-10", company_id=book["a"]))
    assert is_ok(r)
    # Test Cashier's returned sale stays a sale; draft and voided
    # transactions are excluded; company B's "Test Cashier" session is not
    # merged in.
    assert r["cashiers"] == [
        {"cashier_name": "Test Cashier", "session_count": 1, "transaction_count": 3,
         "total_sales": "373.00", "avg_transaction_value": "124.33"},
        {"cashier_name": "Dana", "session_count": 1, "transaction_count": 1,
         "total_sales": "10.00", "avg_transaction_value": "10.00"},
        {"cashier_name": "Lee", "session_count": 1, "transaction_count": 0,
         "total_sales": "0.00", "avg_transaction_value": "0.00"},
    ]
    assert (r["from_date"], r["to_date"], r["count"]) == ("2026-03-10", "2026-03-10", 3)
    s = _row(conn, "pos_session", book["s1"], "status", "cashier_name")
    assert tuple(s) == ("closed", "Test Cashier")


def test_cashier_performance_average_is_exact_half_up(book):
    conn = book["conn"]
    r = call_action(A["pos-cashier-performance"], conn, ns(
        from_date="2026-03-10", to_date="2026-03-11", company_id=book["a"]))
    # Dana: 10.00 + 11.01 = 21.01 over two sales; 10.505 rounds half up to 10.51.
    assert r["cashiers"][1] == {
        "cashier_name": "Dana", "session_count": 2, "transaction_count": 2,
        "total_sales": "21.01", "avg_transaction_value": "10.51"}
    rb = call_action(A["pos-cashier-performance"], conn, ns(
        from_date="2026-03-10", to_date="2026-03-10", company_id=book["b"]))
    assert rb["cashiers"] == [
        {"cashier_name": "Test Cashier", "session_count": 1, "transaction_count": 1,
         "total_sales": "20.00", "avg_transaction_value": "20.00"}]
    none = call_action(A["pos-cashier-performance"], conn, ns(
        from_date="2026-03-12", to_date="2026-03-31", company_id=book["a"]))
    assert (none["cashiers"], none["count"]) == ([], 0)
