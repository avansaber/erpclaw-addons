"""POS void refuses under a live credit note (m783d).

A completed POS sale whose sales invoice is named by a live credit note
raised in selling cannot be voided at the till: ``pos-void-transaction``
refuses before any child call (no receipt is touched). Once the note is
cancelled in selling, the void completes as today.

Money is text throughout: exact string comparisons, no float.
"""
import importlib.util
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from pos_helpers import (  # noqa: E402
    SRC_DIR,
    call_action,
    is_error,
    is_ok,
    load_db_query,
    ns,
    seed_item,
    seed_till_accounts,
)
from erpclaw_lib import cross_skill  # noqa: E402

MOD = load_db_query()
A = MOD.ACTIONS


def _load_selling():
    path = os.path.join(
        SRC_DIR, "erpclaw", "scripts", "erpclaw-selling", "db_query.py")
    spec = importlib.util.spec_from_file_location(
        "db_query_selling_pos_void", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SELL = _load_selling()


def _new_txn(conn, session_id, customer_id):
    r = call_action(A["pos-add-transaction"], conn, ns(
        pos_session_id=session_id, customer_id=customer_id,
        customer_name="Walk-in"))
    assert is_ok(r), f"pos-add-transaction failed: {r}"
    return r["id"]


def _add_line(conn, txn_id, item_id, qty, rate):
    r = call_action(A["pos-add-transaction-item"], conn, ns(
        pos_transaction_id=txn_id, item_id=item_id, item_name=None,
        qty=qty, rate=rate, uom=None, barcode=None, discount_pct=None))
    assert is_ok(r), f"pos-add-transaction-item failed: {r}"
    return r


def _pay(conn, txn_id, method, amount):
    r = call_action(A["pos-add-payment"], conn, ns(
        pos_transaction_id=txn_id, payment_method=method, amount=amount,
        reference=None))
    assert is_ok(r), f"pos-add-payment failed: {r}"
    return r


def _submit(conn, txn_id):
    return call_action(A["pos-submit-transaction"], conn, ns(
        pos_transaction_id=txn_id))


def _completed_sale(conn, env):
    """1 x Widget at 5.00, card 5.00, submitted: receipt paid."""
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn = _new_txn(conn, env["session_id"], env["customer_id"])
    _add_line(conn, txn, widget, "1", "5.00")
    _pay(conn, txn, "card", "5.00")
    rb = _submit(conn, txn)
    assert is_ok(rb), f"submit failed: {rb}"
    return txn, widget, rb


def _raise_note(conn, invoice_id, item_id):
    cn = call_action(SELL.create_credit_note, conn, ns(
        against_invoice_id=invoice_id,
        reason="Till sale returned in part",
        posting_date="2026-03-11",
        items=json.dumps(
            [{"item_id": item_id, "qty": "1", "rate": "5.00"}]),
    ))
    assert is_ok(cn), f"create_credit_note failed: {cn}"
    cn_id = cn["credit_note_id"]
    s = call_action(SELL.submit_sales_invoice, conn,
                    ns(sales_invoice_id=cn_id))
    assert is_ok(s), f"submit credit note failed: {s}"
    return cn_id


def _naming(conn, doc_id):
    row = conn.execute(
        "SELECT naming_series FROM sales_invoice WHERE id = ?",
        (doc_id,)).fetchone()
    return row["naming_series"] or doc_id


def _snapshot(conn):
    snap = {}
    for tbl in ("sales_invoice", "purchase_invoice", "payment_entry",
                "payment_allocation"):
        snap[tbl] = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM " + tbl + " ORDER BY id").fetchall()
        ]
    snap["counts"] = {}
    for tbl in ("gl_entry", "stock_ledger_entry", "payment_ledger_entry",
                "audit_log"):
        snap["counts"][tbl] = conn.execute(
            "SELECT COUNT(*) FROM " + tbl).fetchone()[0]
    return snap


def test_void_refuses_before_any_child_call(conn, env, mod, selling_bridge,
                                            monkeypatch):
    txn, widget, rb = _completed_sale(conn, env)
    inv = rb["sales_invoice_id"]
    pe = rb["payment_entry_ids"][0]
    cn_id = _raise_note(conn, inv, widget)
    naming = _naming(conn, cn_id)
    expected = (
        f"Cannot void: sales invoice {inv} has credit note {naming} "
        f"('submitted'); cancel the credit note first"
    )
    snap = _snapshot(conn)

    calls = []

    def _boom(skill, action, args=None, db_path=None, timeout=30):
        calls.append(action)
        raise AssertionError(
            f"child call must not happen under a live credit note: {action}")

    monkeypatch.setattr(cross_skill, "call_skill_action", _boom)
    r = call_action(mod.ACTIONS["pos-void-transaction"], conn, ns(
        pos_transaction_id=txn))
    assert is_error(r), f"expected refusal, got {r}"
    assert r["message"] == expected, r["message"]
    assert calls == [], calls
    assert conn.execute(
        "SELECT status FROM payment_entry WHERE id = ?",
        (pe,)).fetchone()["status"] == "submitted"
    assert _snapshot(conn) == snap
    assert conn.execute(
        "SELECT status FROM pos_transaction WHERE id = ?",
        (txn,)).fetchone()["status"] == "submitted"


def test_void_after_the_note_is_cancelled_succeeds(conn, env, mod,
                                                  selling_bridge):
    txn, widget, rb = _completed_sale(conn, env)
    inv = rb["sales_invoice_id"]
    pe = rb["payment_entry_ids"][0]
    cn_id = _raise_note(conn, inv, widget)
    c = call_action(SELL.cancel_sales_invoice, conn,
                    ns(sales_invoice_id=cn_id))
    assert is_ok(c), f"cancel credit note failed: {c}"
    r = call_action(mod.ACTIONS["pos-void-transaction"], conn, ns(
        pos_transaction_id=txn))
    assert is_ok(r), f"void failed: {r}"
    assert r["transaction_status"] == "voided"
    assert r["cancelled_sales_invoice_id"] == inv
    assert r["cancelled_payment_entry_ids"] == [pe]
    assert conn.execute(
        "SELECT status FROM payment_entry WHERE id = ?",
        (pe,)).fetchone()["status"] == "cancelled"
    assert conn.execute(
        "SELECT status FROM sales_invoice WHERE id = ?",
        (inv,)).fetchone()["status"] == "cancelled"
