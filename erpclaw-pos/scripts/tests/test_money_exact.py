"""Money exactness for erpclaw-pos aggregates.

Every money total in the POS read paths (reports, session live/close totals,
add-payment write-back, session summary, status) must be an exact decimal
string. Each test seeds amounts with cents that binary floating point cannot
represent exactly (0.10, 0.20, 1000.10, 2000.20, 500.05), runs one action, and
asserts every affected money figure as the exact string computed by hand.
"""
import os
import sys
from datetime import datetime

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from pos_helpers import (  # noqa: E402
    call_action, get_conn, is_ok, load_db_query, ns, seed_item,
    seed_open_session, seed_pos_profile, seed_return_document,
)
from erpclaw_lib.query import Q, P, Table, Field  # noqa: E402

MOD = load_db_query()
A = MOD.ACTIONS


def _ok(action, conn, **kw):
    r = call_action(A[action], conn, ns(**kw))
    assert is_ok(r), f"{action} failed: {r}"
    return r


def _set(conn, table, row_id, **cols):
    t = Table(table)
    q = Q.from_(t).select(t.id).where(t.id == P())
    assert conn.execute(q.get_sql(), (row_id,)).fetchone() is not None
    for column, value in cols.items():
        conn.execute(
            Q.update(t).set(t[column], P()).where(t.id == P()).get_sql(),
            (value, row_id))
    conn.commit()


def _fresh_row(db_path, table, row_id, *cols):
    t = Table(table)
    fresh = get_conn(db_path)
    try:
        return fresh.execute(
            Q.from_(t).select(*[t[c] for c in cols])
            .where(t.id == P()).get_sql(), (row_id,)).fetchone()
    finally:
        fresh.close()


def _txn(conn, mod_env, session, customer, ts):
    r = _ok("pos-add-transaction", conn,
            pos_session_id=session, customer_id=customer,
            customer_name="Exact Sam")
    _set(conn, "pos_transaction", r["id"], created_at=ts)
    return r["id"]


def _line(conn, txn, item, qty, rate):
    _ok("pos-add-transaction-item", conn, pos_transaction_id=txn,
        item_id=item, item_name=None, qty=qty, rate=rate, uom=None,
        barcode=None, discount_pct=None)


def _pay(conn, txn, method, amount):
    _ok("pos-add-payment", conn, pos_transaction_id=txn,
        payment_method=method, amount=amount, reference=None)


def _submit_flip(conn, txn):
    _set(conn, "pos_transaction", txn, status="submitted")


# ---------------------------------------------------------------------------
# pos-add-payment: total_paid is written back exactly
# ---------------------------------------------------------------------------

def test_add_payment_total_paid_is_exact(conn, db_path, env, mod):
    txn = _txn(conn, env, env["session_id"], env["customer_id"],
               "2026-03-10 09:15:00")
    _line(conn, txn, env["item_id"], "1", "0.30")
    r1 = _ok("pos-add-payment", conn, pos_transaction_id=txn,
             payment_method="cash", amount="0.10", reference=None)
    assert r1["total_paid"] == "0.10"
    r2 = _ok("pos-add-payment", conn, pos_transaction_id=txn,
             payment_method="cash", amount="0.20", reference=None)
    assert (r2["payment_amount"], r2["total_paid"]) == ("0.20", "0.30")
    row = _fresh_row(db_path, "pos_transaction", txn, "paid_amount")
    assert row["paid_amount"] == "0.30"


# ---------------------------------------------------------------------------
# pos-cash-reconciliation
# ---------------------------------------------------------------------------

def test_cash_reconciliation_sums_are_exact(conn, env, mod):
    sid = env["session_id"]
    _set(conn, "pos_session", sid, opened_at="2026-03-10 08:00:00")
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "1000.10")
    _pay(conn, t1, "cash", "1000.10")
    _set(conn, "pos_transaction", t1, change_amount="0.10")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:40:00")
    _line(conn, t2, env["item_id"], "1", "2000.20")
    _pay(conn, t2, "cash", "2000.20")
    _pay(conn, t2, "card", "0.30")
    _set(conn, "pos_transaction", t2, change_amount="0.20")
    _submit_flip(conn, t2)
    t3 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 10:05:00")
    _line(conn, t3, env["item_id"], "1", "0.10")
    _pay(conn, t3, "cash", "0.10")
    _submit_flip(conn, t3)
    seed_return_document(conn, t3)

    r = _ok("pos-cash-reconciliation", conn, pos_session_id=sid, id=None)
    assert (r["opening_amount"], r["cash_received"], r["cash_refunded"],
            r["change_given"], r["expected_cash"]) == (
        "100.00", "3000.40", "0.10", "0.30", "3100.00")
    assert r["non_cash_breakdown"] == {"card": "0.30"}
    assert (r["closing_amount"], r["cash_variance"]) == (None, None)


# ---------------------------------------------------------------------------
# pos-get-session / pos-close-session
# ---------------------------------------------------------------------------

def test_get_session_live_totals_are_exact(conn, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "1000.10")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:40:00")
    _line(conn, t2, env["item_id"], "1", "2000.20")
    _submit_flip(conn, t2)
    t3 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 10:05:00")
    _line(conn, t3, env["item_id"], "1", "0.10")
    _pay(conn, t3, "cash", "0.10")
    _submit_flip(conn, t3)
    seed_return_document(conn, t3)

    r = _ok("pos-get-session", conn, id=sid)
    assert (r["live_transaction_count"], r["live_total_sales"],
            r["live_total_returns"]) == (4, "3000.40", "0.10")


def test_close_session_totals_are_exact_and_stored_exact(
        conn, db_path, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "1000.10")
    _pay(conn, t1, "cash", "1000.20")
    _set(conn, "pos_transaction", t1, change_amount="0.10")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:40:00")
    _line(conn, t2, env["item_id"], "1", "2000.20")
    _pay(conn, t2, "cash", "2000.40")
    _set(conn, "pos_transaction", t2, change_amount="0.20")
    _submit_flip(conn, t2)

    r = _ok("pos-close-session", conn, id=sid, closing_amount="3100.30")
    assert (r["total_sales"], r["total_returns"], r["expected_amount"],
            r["difference"], r["closing_amount"]) == (
        "3000.30", "0.00", "3100.30", "0.00", "3100.30")
    row = _fresh_row(db_path, "pos_session", sid, "closing_amount",
                     "expected_amount", "difference", "total_sales",
                     "total_returns", "transaction_count")
    assert tuple(row) == ("3100.30", "3100.30", "0.00", "3000.30", "0.00", 2)


def test_close_session_return_accounting_is_stored_exact(
        conn, db_path, env, mod):
    sid = env["session_id"]
    _set(conn, "pos_session", sid, opened_at="2026-03-10 08:00:00")
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "1000.10")
    _pay(conn, t1, "cash", "1000.10")
    _set(conn, "pos_transaction", t1, change_amount="0.10")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:40:00")
    _line(conn, t2, env["item_id"], "1", "2000.20")
    _pay(conn, t2, "cash", "2000.20")
    _pay(conn, t2, "card", "0.30")
    _set(conn, "pos_transaction", t2, change_amount="0.20")
    _submit_flip(conn, t2)
    t3 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 10:05:00")
    _line(conn, t3, env["item_id"], "1", "0.10")
    _pay(conn, t3, "cash", "0.10")
    _submit_flip(conn, t3)
    seed_return_document(conn, t3)

    r = _ok("pos-close-session", conn, id=sid, closing_amount="3100.00")
    assert (r["total_sales"], r["total_returns"], r["expected_amount"],
            r["difference"], r["closing_amount"],
            r["transaction_count"]) == (
        "3000.40", "0.10", "3100.00", "0.00", "3100.00", 4)
    row = _fresh_row(db_path, "pos_session", sid, "closing_amount",
                     "expected_amount", "difference", "total_sales",
                     "total_returns", "transaction_count")
    assert tuple(row) == ("3100.00", "3100.00", "0.00", "3000.40",
                          "0.10", 4)


# ---------------------------------------------------------------------------
# pos-daily-report
# ---------------------------------------------------------------------------

def test_daily_report_totals_are_exact(conn, env, mod):
    day = "2026-03-10"
    _set(conn, "pos_session", env["session_id"],
         opened_at=f"{day} 08:00:00")
    t1 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:15:00")
    _line(conn, t1, env["item_id"], "1", "1000.10")
    _set(conn, "pos_transaction", t1, discount_amount="0.10",
         tax_amount="0.20")
    _pay(conn, t1, "cash", "1000.10")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:40:00")
    _line(conn, t2, env["item_id"], "1", "2000.20")
    _pay(conn, t2, "cash", "2000.20")
    _submit_flip(conn, t2)
    t3 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 10:05:00")
    _line(conn, t3, env["item_id"], "1", "0.30")
    _pay(conn, t3, "cash", "0.30")
    _submit_flip(conn, t3)
    t3r = seed_return_document(conn, t3)
    _set(conn, "pos_transaction", t3r, created_at=f"{day} 10:30:00")

    r = _ok("pos-daily-report", conn, date=day,
            company_id=env["company_id"])
    assert (r["transaction_count"], r["total_sales"], r["total_discounts"],
            r["total_tax"], r["return_count"], r["total_returns"],
            r["net_sales"], r["sessions_count"]) == (
        3, "3000.60", "0.10", "0.20", 1, "0.30", "3000.30", 1)
    assert r["payment_methods"] == [
        {"method": "cash", "count": 3, "total": "3000.60"}]


# ---------------------------------------------------------------------------
# pos-hourly-sales
# ---------------------------------------------------------------------------

def test_hourly_sales_totals_are_exact(conn, env, mod):
    day = "2026-03-10"
    t1 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:15:00")
    _line(conn, t1, env["item_id"], "1", "1000.10")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:40:00")
    _line(conn, t2, env["item_id"], "1", "2000.20")
    _submit_flip(conn, t2)
    t3 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 14:05:00")
    _line(conn, t3, env["item_id"], "1", "0.30")
    _submit_flip(conn, t3)

    r = _ok("pos-hourly-sales", conn, date=day,
            company_id=env["company_id"])
    assert r["hourly_breakdown"] == [
        {"hour": "09", "hour_label": "09:00-09:59", "transaction_count": 2,
         "total_sales": "3000.30"},
        {"hour": "14", "hour_label": "14:00-14:59", "transaction_count": 1,
         "total_sales": "0.30"},
    ]
    assert (r["total_transactions"], r["total_sales"], r["peak_hour"],
            r["peak_hour_sales"]) == (3, "3000.60", "09:00-09:59", "3000.30")


# ---------------------------------------------------------------------------
# pos-top-items
# ---------------------------------------------------------------------------

def test_top_items_revenue_is_exact(conn, env, mod):
    day = "2026-03-10"
    gadget = seed_item(conn, "Gadget B", "GDG-B")
    t1 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:15:00")
    _line(conn, t1, env["item_id"], "12", "0.10")
    _line(conn, t1, gadget, "9", "0.20")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:40:00")
    _line(conn, t2, env["item_id"], "3", "0.10")
    _submit_flip(conn, t2)

    r = _ok("pos-top-items", conn, from_date=day, to_date=day,
            company_id=env["company_id"], limit=None)
    assert [(i["item_name"], i["total_qty"], i["total_revenue"],
             i["transaction_count"]) for i in r["top_items"]] == [
        ("Widget A", "15.00", "1.50", 2),
        ("Gadget B", "9.00", "1.80", 1),
    ]


def test_top_items_quantity_tie_orders_by_name(conn, env, mod):
    day = "2026-03-10"
    gadget = seed_item(conn, "Gadget B", "GDG-B")
    t1 = _txn(conn, env, env["session_id"], env["customer_id"],
              "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "2", "0.10")
    _line(conn, t1, gadget, "2", "0.20")
    _submit_flip(conn, t1)

    r = _ok("pos-top-items", conn, from_date=day, to_date=day,
            company_id=env["company_id"], limit=None)
    assert [(i["item_name"], i["total_qty"], i["total_revenue"],
             i["transaction_count"]) for i in r["top_items"]] == [
        ("Gadget B", "2.00", "0.40", 1),
        ("Widget A", "2.00", "0.20", 1),
    ]


# ---------------------------------------------------------------------------
# pos-cashier-performance
# ---------------------------------------------------------------------------

def test_cashier_performance_totals_are_exact(conn, env, mod):
    day = "2026-03-10"
    _set(conn, "pos_session", env["session_id"],
         opened_at=f"{day} 08:00:00")
    t1 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:15:00")
    _line(conn, t1, env["item_id"], "1", "1000.10")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:40:00")
    _line(conn, t2, env["item_id"], "1", "2000.20")
    _submit_flip(conn, t2)
    p2 = seed_pos_profile(conn, env["company_id"], name="Counter 2")
    dana = seed_open_session(conn, p2, cashier="Dana")
    _set(conn, "pos_session", dana, opened_at=f"{day} 13:00:00")
    t3 = _txn(conn, env, dana, env["customer_id"], f"{day} 14:20:00")
    _line(conn, t3, env["item_id"], "1", "0.10")
    _submit_flip(conn, t3)

    r = _ok("pos-cashier-performance", conn, from_date=day, to_date=day,
            company_id=env["company_id"])
    assert r["cashiers"] == [
        {"cashier_name": "Test Cashier", "session_count": 1,
         "transaction_count": 2, "total_sales": "3000.30",
         "avg_transaction_value": "1500.15"},
        {"cashier_name": "Dana", "session_count": 1, "transaction_count": 1,
         "total_sales": "0.10", "avg_transaction_value": "0.10"},
    ]


# ---------------------------------------------------------------------------
# pos-session-summary
# ---------------------------------------------------------------------------

def test_session_summary_totals_are_exact(conn, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "2", "500.05")
    _pay(conn, t1, "cash", "1000.10")
    _submit_flip(conn, t1)
    gadget = seed_item(conn, "Gadget B", "GDG-B")
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:40:00")
    _line(conn, t2, gadget, "1", "2000.20")
    _pay(conn, t2, "card", "2000.20")
    _submit_flip(conn, t2)
    t3 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 11:30:00")
    _line(conn, t3, env["item_id"], "1", "50.00")
    t4 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 14:05:00")
    _line(conn, t4, env["item_id"], "1", "0.10")
    _pay(conn, t4, "cash", "0.10")
    _submit_flip(conn, t4)
    seed_return_document(conn, t4)

    r = _ok("pos-session-summary", conn, pos_session_id=sid)
    assert r["total_transactions"] == 5
    assert r["status_breakdown"] == {
        "submitted": {"count": 3, "total": "3000.40"},
        "draft": {"count": 1, "total": "50.00"},
        "returned": {"count": 1, "total": "0.10"},
    }
    assert r["payment_breakdown"] == {
        "cash": {"count": 3, "received": "1000.20", "refunded": "0.10",
                 "change_given": "0.00", "total": "1000.10"},
        "card": {"count": 1, "received": "2000.20", "refunded": "0.00",
                 "change_given": "0.00", "total": "2000.20"},
    }
    assert [(i["item_name"], i["total_qty"], i["total_amount"])
            for i in r["top_items"]] == [
        ("Widget A", "3.00", "1000.20"),
        ("Gadget B", "1.00", "2000.20"),
    ]


def test_session_summary_top_items_quantity_tie_orders_by_name(
        conn, env, mod):
    sid = env["session_id"]
    gadget = seed_item(conn, "Gadget B", "GDG-B")
    t1 = _txn(conn, env, sid, env["customer_id"],
              "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "2", "0.10")
    _line(conn, t1, gadget, "2", "0.20")
    _submit_flip(conn, t1)

    r = _ok("pos-session-summary", conn, pos_session_id=sid)
    assert [(i["item_name"], i["total_qty"], i["total_amount"])
            for i in r["top_items"]] == [
        ("Gadget B", "2.00", "0.40"),
        ("Widget A", "2.00", "0.20"),
    ]


# ---------------------------------------------------------------------------
# pos-status
# ---------------------------------------------------------------------------

def test_status_today_sales_is_exact(conn, env, mod):
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    t1 = _txn(conn, env, env["session_id"], env["customer_id"], now_ts)
    _line(conn, t1, env["item_id"], "1", "1000.10")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, env["session_id"], env["customer_id"], now_ts)
    _line(conn, t2, env["item_id"], "1", "2000.20")
    _submit_flip(conn, t2)

    r = _ok("pos-status", conn)
    assert (r["today_transactions"], r["today_sales"]) == (2, "3000.30")


def test_status_today_sales_counts_returned_original(conn, env, mod):
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sale_a = _txn(conn, env, env["session_id"], env["customer_id"], now_ts)
    _line(conn, sale_a, env["item_id"], "1", "0.10")
    _submit_flip(conn, sale_a)
    sale_b = _txn(conn, env, env["session_id"], env["customer_id"], now_ts)
    _line(conn, sale_b, env["item_id"], "1", "0.30")
    _submit_flip(conn, sale_b)
    ret = seed_return_document(conn, sale_b)
    _set(conn, "pos_transaction", ret, created_at=now_ts)

    r = _ok("pos-status", conn)
    assert (r["today_transactions"], r["today_sales"]) == (3, "0.40")


# ---------------------------------------------------------------------------
# Float-pair proofs: 90000000000000.11 + 90000000000000.22
#
# Each test below seeds the two amounts whose binary-float sum cannot give
# the exact figure in the money column one action totals. Near 1.8e14
# adjacent doubles are 1/32 apart: the float sum of 90000000000000.11 and
# 90000000000000.22 is 180000000000000.3125, which Python prints as
# 180000000000000.3, so a total taken through a binary float cannot produce
# "180000000000000.33". Each test asserts the exact Decimal figure computed
# by hand, "180000000000000.33" (or the exact derived figure).
# ---------------------------------------------------------------------------

def test_add_payment_float_pair_is_exact(conn, db_path, env, mod):
    txn = _txn(conn, env, env["session_id"], env["customer_id"],
               "2026-03-10 09:15:00")
    _line(conn, txn, env["item_id"], "1", "1.00")
    r1 = _ok("pos-add-payment", conn, pos_transaction_id=txn,
             payment_method="cash", amount="90000000000000.11",
             reference=None)
    assert r1["total_paid"] == "90000000000000.11"
    r2 = _ok("pos-add-payment", conn, pos_transaction_id=txn,
             payment_method="cash", amount="90000000000000.22",
             reference=None)
    assert (r2["payment_amount"], r2["total_paid"]) == (
        "90000000000000.22", "180000000000000.33")
    row = _fresh_row(db_path, "pos_transaction", txn, "paid_amount")
    assert row["paid_amount"] == "180000000000000.33"


def test_cash_reconciliation_float_pair_is_exact(conn, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "90000000000000.11")
    _pay(conn, t1, "cash", "90000000000000.11")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:40:00")
    _line(conn, t2, env["item_id"], "1", "90000000000000.22")
    _pay(conn, t2, "cash", "90000000000000.22")
    _submit_flip(conn, t2)

    r = _ok("pos-cash-reconciliation", conn, pos_session_id=sid, id=None)
    assert (r["opening_amount"], r["cash_received"], r["cash_refunded"],
            r["change_given"], r["expected_cash"]) == (
        "100.00", "180000000000000.33", "0.00", "0.00",
        "180000000000100.33")


def test_get_session_float_pair_is_exact(conn, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "90000000000000.11")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:40:00")
    _line(conn, t2, env["item_id"], "1", "90000000000000.22")
    _submit_flip(conn, t2)

    r = _ok("pos-get-session", conn, id=sid)
    assert (r["live_transaction_count"], r["live_total_sales"],
            r["live_total_returns"]) == (2, "180000000000000.33", "0.00")


def test_close_session_float_pair_is_exact_and_stored_exact(
        conn, db_path, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "90000000000000.11")
    _pay(conn, t1, "cash", "90000000000000.11")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:40:00")
    _line(conn, t2, env["item_id"], "1", "90000000000000.22")
    _pay(conn, t2, "cash", "90000000000000.22")
    _submit_flip(conn, t2)

    r = _ok("pos-close-session", conn, id=sid,
            closing_amount="180000000000100.33")
    assert (r["total_sales"], r["total_returns"], r["expected_amount"],
            r["difference"], r["closing_amount"]) == (
        "180000000000000.33", "0.00", "180000000000100.33", "0.00",
        "180000000000100.33")
    row = _fresh_row(db_path, "pos_session", sid, "closing_amount",
                     "expected_amount", "difference", "total_sales",
                     "total_returns", "transaction_count")
    assert tuple(row) == ("180000000000100.33", "180000000000100.33", "0.00",
                          "180000000000000.33", "0.00", 2)


def test_daily_report_float_pair_is_exact(conn, env, mod):
    day = "2026-03-10"
    _set(conn, "pos_session", env["session_id"],
         opened_at=f"{day} 08:00:00")
    t1 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:15:00")
    _line(conn, t1, env["item_id"], "1", "90000000000000.11")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:40:00")
    _line(conn, t2, env["item_id"], "1", "90000000000000.22")
    _submit_flip(conn, t2)

    r = _ok("pos-daily-report", conn, date=day,
            company_id=env["company_id"])
    assert (r["transaction_count"], r["total_sales"], r["total_discounts"],
            r["total_tax"], r["return_count"], r["total_returns"],
            r["net_sales"], r["sessions_count"]) == (
        2, "180000000000000.33", "0.00", "0.00", 0, "0.00",
        "180000000000000.33", 1)


def test_hourly_sales_float_pair_is_exact(conn, env, mod):
    day = "2026-03-10"
    t1 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:15:00")
    _line(conn, t1, env["item_id"], "1", "90000000000000.11")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:40:00")
    _line(conn, t2, env["item_id"], "1", "90000000000000.22")
    _submit_flip(conn, t2)

    r = _ok("pos-hourly-sales", conn, date=day,
            company_id=env["company_id"])
    assert r["hourly_breakdown"] == [
        {"hour": "09", "hour_label": "09:00-09:59", "transaction_count": 2,
         "total_sales": "180000000000000.33"},
    ]
    assert (r["total_transactions"], r["total_sales"], r["peak_hour"],
            r["peak_hour_sales"]) == (
        2, "180000000000000.33", "09:00-09:59", "180000000000000.33")


def test_top_items_float_pair_is_exact(conn, env, mod):
    day = "2026-03-10"
    t1 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:15:00")
    _line(conn, t1, env["item_id"], "1", "90000000000000.11")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:40:00")
    _line(conn, t2, env["item_id"], "1", "90000000000000.22")
    _submit_flip(conn, t2)

    r = _ok("pos-top-items", conn, from_date=day, to_date=day,
            company_id=env["company_id"], limit=None)
    assert [(i["item_name"], i["total_qty"], i["total_revenue"],
             i["transaction_count"]) for i in r["top_items"]] == [
        ("Widget A", "2.00", "180000000000000.33", 2),
    ]


def test_cashier_performance_float_pair_is_exact(conn, env, mod):
    day = "2026-03-10"
    _set(conn, "pos_session", env["session_id"],
         opened_at=f"{day} 08:00:00")
    t1 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:15:00")
    _line(conn, t1, env["item_id"], "1", "90000000000000.11")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:40:00")
    _line(conn, t2, env["item_id"], "1", "90000000000000.22")
    _submit_flip(conn, t2)

    r = _ok("pos-cashier-performance", conn, from_date=day, to_date=day,
            company_id=env["company_id"])
    assert r["cashiers"] == [
        {"cashier_name": "Test Cashier", "session_count": 1,
         "transaction_count": 2, "total_sales": "180000000000000.33",
         "avg_transaction_value": "90000000000000.17"},
    ]


def test_session_summary_float_pair_is_exact(conn, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "90000000000000.11")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:40:00")
    _line(conn, t2, env["item_id"], "1", "90000000000000.22")
    _submit_flip(conn, t2)

    r = _ok("pos-session-summary", conn, pos_session_id=sid)
    assert r["total_transactions"] == 2
    assert r["status_breakdown"] == {
        "submitted": {"count": 2, "total": "180000000000000.33"},
    }
    assert r["payment_breakdown"] == {}
    assert [(i["item_name"], i["total_qty"], i["total_amount"])
            for i in r["top_items"]] == [
        ("Widget A", "2.00", "180000000000000.33"),
    ]


def test_status_float_pair_is_exact(conn, env, mod):
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    t1 = _txn(conn, env, env["session_id"], env["customer_id"], now_ts)
    _line(conn, t1, env["item_id"], "1", "90000000000000.11")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, env["session_id"], env["customer_id"], now_ts)
    _line(conn, t2, env["item_id"], "1", "90000000000000.22")
    _submit_flip(conn, t2)

    r = _ok("pos-status", conn)
    assert (r["today_transactions"], r["today_sales"]) == (
        2, "180000000000000.33")


# ---------------------------------------------------------------------------
# Return accounting: a returned original stays a sale of its day; only the
# return document is the return, reported as a positive figure. Each test
# below repeats the seeds of its sibling above and asserts the corrected
# rule.
# ---------------------------------------------------------------------------

def test_get_session_return_accounting(conn, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "1000.10")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:40:00")
    _line(conn, t2, env["item_id"], "1", "2000.20")
    _submit_flip(conn, t2)
    t3 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 10:05:00")
    _line(conn, t3, env["item_id"], "1", "0.10")
    _pay(conn, t3, "cash", "0.10")
    _submit_flip(conn, t3)
    seed_return_document(conn, t3)

    r = _ok("pos-get-session", conn, id=sid)
    assert (r["live_transaction_count"], r["live_total_sales"],
            r["live_total_returns"]) == (4, "3000.40", "0.10")


def test_cash_reconciliation_return_accounting(conn, env, mod):
    sid = env["session_id"]
    _set(conn, "pos_session", sid, opened_at="2026-03-10 08:00:00")
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "1000.10")
    _pay(conn, t1, "cash", "1000.10")
    _set(conn, "pos_transaction", t1, change_amount="0.10")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:40:00")
    _line(conn, t2, env["item_id"], "1", "2000.20")
    _pay(conn, t2, "cash", "2000.20")
    _pay(conn, t2, "card", "0.30")
    _set(conn, "pos_transaction", t2, change_amount="0.20")
    _submit_flip(conn, t2)
    t3 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 10:05:00")
    _line(conn, t3, env["item_id"], "1", "0.10")
    _pay(conn, t3, "cash", "0.10")
    _submit_flip(conn, t3)
    seed_return_document(conn, t3)

    r = _ok("pos-cash-reconciliation", conn, pos_session_id=sid, id=None)
    assert (r["opening_amount"], r["cash_received"], r["cash_refunded"],
            r["change_given"], r["expected_cash"]) == (
        "100.00", "3000.40", "0.10", "0.30", "3100.00")


def test_daily_report_return_accounting(conn, env, mod):
    day = "2026-03-10"
    _set(conn, "pos_session", env["session_id"],
         opened_at=f"{day} 08:00:00")
    t1 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:15:00")
    _line(conn, t1, env["item_id"], "1", "1000.10")
    _set(conn, "pos_transaction", t1, discount_amount="0.10",
         tax_amount="0.20")
    _pay(conn, t1, "cash", "1000.10")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:40:00")
    _line(conn, t2, env["item_id"], "1", "2000.20")
    _pay(conn, t2, "cash", "2000.20")
    _submit_flip(conn, t2)
    t3 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 10:05:00")
    _line(conn, t3, env["item_id"], "1", "0.30")
    _pay(conn, t3, "cash", "0.30")
    _submit_flip(conn, t3)
    t3r = seed_return_document(conn, t3)
    _set(conn, "pos_transaction", t3r, created_at=f"{day} 10:30:00")

    r = _ok("pos-daily-report", conn, date=day,
            company_id=env["company_id"])
    assert (r["transaction_count"], r["total_sales"], r["total_discounts"],
            r["total_tax"], r["return_count"], r["total_returns"],
            r["net_sales"], r["sessions_count"]) == (
        3, "3000.60", "0.10", "0.20", 1, "0.30", "3000.30", 1)


def test_session_summary_return_accounting(conn, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "2", "500.05")
    _pay(conn, t1, "cash", "1000.10")
    _submit_flip(conn, t1)
    gadget = seed_item(conn, "Gadget B", "GDG-B")
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:40:00")
    _line(conn, t2, gadget, "1", "2000.20")
    _pay(conn, t2, "card", "2000.20")
    _submit_flip(conn, t2)
    t3 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 11:30:00")
    _line(conn, t3, env["item_id"], "1", "50.00")
    t4 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 14:05:00")
    _line(conn, t4, env["item_id"], "1", "0.10")
    _pay(conn, t4, "cash", "0.10")
    _submit_flip(conn, t4)
    seed_return_document(conn, t4)

    r = _ok("pos-session-summary", conn, pos_session_id=sid)
    assert r["status_breakdown"]["returned"] == {"count": 1, "total": "0.10"}


def test_daily_report_zero_total_return_accounting(conn, env, mod):
    day = "2026-03-10"
    _set(conn, "pos_session", env["session_id"],
         opened_at=f"{day} 08:00:00")
    t0 = _txn(conn, env, env["session_id"], env["customer_id"],
              f"{day} 09:15:00")
    _submit_flip(conn, t0)
    t0r = seed_return_document(conn, t0)
    _set(conn, "pos_transaction", t0r, created_at=f"{day} 10:30:00")

    r = _ok("pos-daily-report", conn, date=day,
            company_id=env["company_id"])
    assert (r["transaction_count"], r["total_sales"],
            r["return_count"], r["total_returns"]) == (
        1, "0.00", 1, "0.00")
