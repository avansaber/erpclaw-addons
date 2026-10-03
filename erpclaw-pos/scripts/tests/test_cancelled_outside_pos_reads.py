"""POS readers skip sales cancelled outside POS.

A submitted sale whose linked sales invoice was cancelled with
``cancel-sales-invoice``, or whose receipt was cancelled with
``cancel-payment``, outside POS is a void still in progress: the summary and
cash reconciliation must not count it as a live sale, and must point at
``pos-void-transaction`` until the void is finished.
"""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest  # noqa: E402

from pos_helpers import (  # noqa: E402
    build_env, call_action, is_error, is_ok, load_db_query, ns,
    seed_item, seed_till_accounts,
)
from erpclaw_lib import cross_skill  # noqa: E402
from erpclaw_lib.query import Q, P, Table, Field  # noqa: E402

MOD = load_db_query()
A = MOD.ACTIONS

NEXT_STEP = "Run pos-void-transaction --id <id> for each transaction in voids_to_finish"


def _ok(action, conn, **kwargs):
    r = call_action(A[action], conn, ns(**kwargs))
    assert is_ok(r), f"{action} failed: {r}"
    return r


def _sale(conn, env, item_id, qty, rate, cash):
    txn = _ok(
        "pos-add-transaction", conn,
        pos_session_id=env["session_id"], customer_id=env["customer_id"],
        customer_name="Walk-in")["id"]
    _ok(
        "pos-add-transaction-item", conn, pos_transaction_id=txn,
        item_id=item_id, item_name=None, qty=qty, rate=rate, uom=None,
        barcode=None, discount_pct=None)
    _ok(
        "pos-add-payment", conn, pos_transaction_id=txn,
        payment_method="cash", amount=cash, reference=None)
    submitted = _ok("pos-submit-transaction", conn, pos_transaction_id=txn)
    assert submitted["transaction_status"] == "submitted"
    return txn, submitted


def _two_sales(conn, env, selling_bridge):
    """Live sale (2 x 10.00, cash exact) + doomed sale (3 x 7.00, cash 25.00)."""
    seed_till_accounts(conn, env["company_id"])
    live_item = seed_item(conn, "Live Widget", "LIVE")
    gone_item = seed_item(conn, "Gone Widget", "GONE")
    conn.commit()
    live_id, _ = _sale(conn, env, live_item, "2", "10.00", "20.00")
    gone_id, gone_result = _sale(conn, env, gone_item, "3", "7.00", "25.00")
    assert gone_result["change_amount"] == "4.00"
    return live_id, gone_id, gone_result


def _cancel_invoice(conn, invoice_id):
    conn.commit()
    r = cross_skill.call_skill_action(
        "erpclaw", "cancel-sales-invoice",
        {"--sales-invoice-id": invoice_id, "--user-confirmed": None})
    conn.commit()
    assert r.get("document_status") == "cancelled", f"cancel-sales-invoice failed: {r}"
    return r


def _cancel_payment(conn, payment_entry_id):
    conn.commit()
    r = cross_skill.call_skill_action(
        "erpclaw", "cancel-payment",
        {"--payment-entry-id": payment_entry_id, "--user-confirmed": None})
    conn.commit()
    assert r.get("document_status") == "cancelled", f"cancel-payment failed: {r}"
    return r


def _cancel_invoice_outside_pos(conn, submitted):
    """Cancel a submitted sale's invoice outside POS.

    A paid invoice refuses ``cancel-sales-invoice``, so the out-of-band
    sequence is the receipt first (which puts the invoice back to
    submitted) and then the invoice — the invoice still ends cancelled.
    """
    _cancel_payment(conn, submitted["payment_entry_ids"][0])
    return _cancel_invoice(conn, submitted["sales_invoice_id"])


def test_summary_moves_invoice_cancelled_sale_out(conn, env, mod,
                                                  selling_bridge):
    live_id, gone_id, gone_result = _two_sales(conn, env, selling_bridge)
    _cancel_invoice_outside_pos(conn, gone_result)

    r = _ok("pos-session-summary", conn, pos_session_id=env["session_id"])
    assert r["total_transactions"] == 2
    assert r["status_breakdown"]["submitted"] == {
        "count": 1, "total": "20.00"}
    assert r["status_breakdown"]["cancelled_outside_pos"] == {
        "count": 1, "total": "21.00"}
    assert r["payment_breakdown"] == {
        "cash": {"count": 1, "received": "20.00", "refunded": "0.00",
                 "change_given": "0.00", "total": "20.00"},
    }
    assert [t["item_name"] for t in r["top_items"]] == ["Live Widget"]
    assert r["top_items"][0]["total_qty"] == "2.00"
    assert r["top_items"][0]["total_amount"] == "20.00"
    assert r["voids_to_finish"] == [gone_id]
    assert r["next_step"] == NEXT_STEP


def test_summary_payment_cancelled_sale_out(conn, env, mod, selling_bridge):
    live_id, gone_id, gone_result = _two_sales(conn, env, selling_bridge)
    _cancel_payment(conn, gone_result["payment_entry_ids"][0])

    r = _ok("pos-session-summary", conn, pos_session_id=env["session_id"])
    assert r["total_transactions"] == 2
    assert r["status_breakdown"]["submitted"] == {
        "count": 1, "total": "20.00"}
    assert r["status_breakdown"]["cancelled_outside_pos"] == {
        "count": 1, "total": "21.00"}
    assert r["payment_breakdown"] == {
        "cash": {"count": 1, "received": "20.00", "refunded": "0.00",
                 "change_given": "0.00", "total": "20.00"},
    }
    assert [t["item_name"] for t in r["top_items"]] == ["Live Widget"]
    assert r["voids_to_finish"] == [gone_id]
    assert r["next_step"] == NEXT_STEP


def test_cash_reconciliation_excludes_cancelled_sale(conn, env, mod,
                                                     selling_bridge):
    live_id, gone_id, gone_result = _two_sales(conn, env, selling_bridge)
    _cancel_invoice_outside_pos(conn, gone_result)

    r = _ok("pos-cash-reconciliation", conn,
            pos_session_id=env["session_id"])
    assert r["cash_received"] == "20.00"
    assert r["cash_refunded"] == "0.00"
    assert r["change_given"] == "0.00"
    assert r["expected_cash"] == "120.00"
    assert r["non_cash_breakdown"] == {}
    assert r["voids_to_finish"] == [gone_id]
    assert r["next_step"] == NEXT_STEP


def test_finishing_the_void_clears_the_hint(conn, env, mod, selling_bridge):
    live_id, gone_id, gone_result = _two_sales(conn, env, selling_bridge)
    _cancel_invoice_outside_pos(conn, gone_result)

    v = _ok("pos-void-transaction", conn, pos_transaction_id=gone_id)
    assert v["transaction_status"] == "voided"

    r = _ok("pos-session-summary", conn, pos_session_id=env["session_id"])
    assert r["status_breakdown"]["submitted"] == {
        "count": 1, "total": "20.00"}
    assert r["status_breakdown"]["voided"] == {
        "count": 1, "total": "21.00"}
    assert "cancelled_outside_pos" not in r["status_breakdown"]
    assert "voids_to_finish" not in r
    assert "next_step" not in r
    assert r["payment_breakdown"] == {
        "cash": {"count": 1, "received": "20.00", "refunded": "0.00",
                 "change_given": "0.00", "total": "20.00"},
    }

    c = _ok("pos-cash-reconciliation", conn,
            pos_session_id=env["session_id"])
    assert c["cash_received"] == "20.00"
    assert c["change_given"] == "0.00"
    assert c["expected_cash"] == "120.00"
    assert "voids_to_finish" not in c
    assert "next_step" not in c


def test_clean_session_unchanged(conn, env, mod, selling_bridge):
    seed_till_accounts(conn, env["company_id"])
    conn.commit()
    live_id, _ = _sale(conn, env, env["item_id"], "2", "10.00", "20.00")

    r = _ok("pos-session-summary", conn, pos_session_id=env["session_id"])
    assert r["total_transactions"] == 1
    assert r["status_breakdown"] == {
        "submitted": {"count": 1, "total": "20.00"}}
    assert r["payment_breakdown"] == {
        "cash": {"count": 1, "received": "20.00", "refunded": "0.00",
                 "change_given": "0.00", "total": "20.00"},
    }
    assert "voids_to_finish" not in r
    assert "next_step" not in r

    c = _ok("pos-cash-reconciliation", conn,
            pos_session_id=env["session_id"])
    assert c["cash_received"] == "20.00"
    assert c["change_given"] == "0.00"
    assert c["expected_cash"] == "120.00"
    assert "voids_to_finish" not in c
    assert "next_step" not in c


def test_close_session_still_refuses(conn, env, mod, selling_bridge):
    live_id, gone_id, gone_result = _two_sales(conn, env, selling_bridge)
    _cancel_invoice_outside_pos(conn, gone_result)

    r = call_action(A["pos-close-session"], conn, ns(
        id=env["session_id"], closing_amount="120.00"))
    assert is_error(r), f"expected close refusal, got {r}"
    assert gone_id in r["message"]
    assert "finish it before closing" in r["message"]
