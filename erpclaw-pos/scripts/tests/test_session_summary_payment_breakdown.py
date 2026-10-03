"""Session summary payment breakdown: received, refunded, change, total.

Cash kept in the till is received minus refunded minus change given; the
summary's cash total must match what pos-close-session expects.
"""
import json
import os
import sys
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from pos_helpers import (  # noqa: E402
    call_action, get_conn, is_ok, load_db_query, ns, seed_return_document,
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


def _snapshot(conn):
    out = {}
    for table in ("pos_transaction", "pos_payment", "pos_session",
                  "gl_entry"):
        t = Table(table)
        rows = conn.execute(
            Q.from_(t).select(t.star).orderby(t.id).get_sql()).fetchall()
        out[table] = json.dumps(
            [{k: r[k] for k in r.keys()} for r in rows],
            sort_keys=True, default=str)
    return out


def test_sale_and_return_show_both_sides(conn, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "3", "18.00")
    _pay(conn, t1, "cash", "54.00")
    _submit_flip(conn, t1)
    seed_return_document(conn, t1)
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 10:05:00")
    _line(conn, t2, env["item_id"], "1", "20.00")
    _pay(conn, t2, "cash", "20.00")
    _submit_flip(conn, t2)
    _set(conn, "pos_transaction", t2, status="voided")

    r = _ok("pos-session-summary", conn, pos_session_id=sid)
    assert r["opening_amount"] == "100.00"
    assert r["payment_breakdown"] == {
        "cash": {"count": 2, "received": "54.00", "refunded": "54.00",
                 "change_given": "0.00", "total": "0.00"},
    }


def test_change_is_not_cash_kept(conn, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "18.50")
    _pay(conn, t1, "cash", "20.00")
    _set(conn, "pos_transaction", t1, change_amount="1.50")
    _submit_flip(conn, t1)

    r = _ok("pos-session-summary", conn, pos_session_id=sid)
    assert r["opening_amount"] == "100.00"
    assert r["payment_breakdown"] == {
        "cash": {"count": 1, "received": "20.00", "refunded": "0.00",
                 "change_given": "1.50", "total": "18.50"},
    }


def test_split_tender(conn, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "18.50")
    _pay(conn, t1, "card", "10.00")
    _pay(conn, t1, "cash", "8.50")
    _submit_flip(conn, t1)

    r = _ok("pos-session-summary", conn, pos_session_id=sid)
    assert r["opening_amount"] == "100.00"
    assert r["payment_breakdown"]["card"] == {
        "count": 1, "received": "10.00", "refunded": "0.00",
        "change_given": "0.00", "total": "10.00",
    }
    assert r["payment_breakdown"]["cash"] == {
        "count": 1, "received": "8.50", "refunded": "0.00",
        "change_given": "0.00", "total": "8.50",
    }


def test_cash_total_matches_close_session(conn, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "1", "18.50")
    _pay(conn, t1, "cash", "20.00")
    _set(conn, "pos_transaction", t1, change_amount="1.50")
    _submit_flip(conn, t1)
    t2 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 10:05:00")
    _line(conn, t2, env["item_id"], "3", "18.00")
    _pay(conn, t2, "cash", "54.00")
    _submit_flip(conn, t2)
    seed_return_document(conn, t2)

    r = _ok("pos-session-summary", conn, pos_session_id=sid)
    assert r["opening_amount"] == "100.00"
    cash_total = r["payment_breakdown"]["cash"]["total"]
    c = _ok("pos-close-session", conn, id=sid, closing_amount="0")
    till = (Decimal(r["opening_amount"]) + Decimal(cash_total)).quantize(
        Decimal("0.00"))
    assert str(till) == c["expected_amount"]


def test_summary_writes_nothing(conn, env, mod):
    sid = env["session_id"]
    t1 = _txn(conn, env, sid, env["customer_id"], "2026-03-10 09:15:00")
    _line(conn, t1, env["item_id"], "2", "500.05")
    _pay(conn, t1, "cash", "1000.10")
    _submit_flip(conn, t1)

    before = _snapshot(conn)
    _ok("pos-session-summary", conn, pos_session_id=sid)
    assert _snapshot(conn) == before
