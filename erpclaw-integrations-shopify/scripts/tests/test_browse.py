"""Tests for erpclaw-integrations-shopify browse (read) actions.

Covers: list-orders, get-order, list-refunds, get-refund, list-payouts,
        get-payout, list-disputes, order-gl-detail.
"""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from shopify_test_helpers import (
    call_action, ns, is_ok, is_error,
    build_env,
    seed_shopify_order, seed_shopify_order_line_item,
    seed_shopify_refund, seed_shopify_payout,
    seed_shopify_dispute,
    _uuid,
)
from browse import ACTIONS


def _seed_payout_transaction(conn, payout_id, company_id,
                              txn_type="charge", gross="100.00",
                              fee="2.90", net="97.10"):
    """Create a payout transaction for testing."""
    tid = _uuid()
    conn.execute(
        """INSERT INTO shopify_payout_transaction
            (id, shopify_payout_id_local, shopify_balance_txn_id,
             transaction_type, gross_amount, fee_amount, net_amount,
             processed_at, company_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, '2026-03-14T12:00:00Z', ?)""",
        (tid, payout_id, _uuid()[:12],
         txn_type, gross, fee, net, company_id)
    )
    conn.commit()
    return tid


# ===========================================================================
# 1. test_list_orders
# ===========================================================================
class TestListOrders:

    def test_list_orders_returns_orders(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        seed_shopify_order(conn, acct_id, cid, shopify_order_id="6001")
        seed_shopify_order(conn, acct_id, cid, shopify_order_id="6002")

        result = call_action(ACTIONS["shopify-list-orders"], conn, ns(
            shopify_account_id=acct_id,
            date_from=None, date_to=None,
            financial_status=None, gl_status=None,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["count"] == 2
        assert len(result["orders"]) == 2


# ===========================================================================
# 2. test_get_order_with_line_items
# ===========================================================================
class TestGetOrder:

    def test_get_order_with_line_items(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        oid = seed_shopify_order(conn, acct_id, cid, shopify_order_id="7001",
                                  total="236.00", subtotal="200.00",
                                  tax="16.00", shipping="20.00")
        seed_shopify_order_line_item(conn, oid, cid, sku="SKU-A1",
                                      quantity=2, unit_price="60.00")
        seed_shopify_order_line_item(conn, oid, cid, sku="SKU-B2",
                                      quantity=1, unit_price="80.00")

        result = call_action(ACTIONS["shopify-get-order"], conn, ns(
            shopify_order_id_local=oid,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["id"] == oid
        assert len(result["line_items"]) == 2
        skus = {li["sku"] for li in result["line_items"]}
        assert "SKU-A1" in skus
        assert "SKU-B2" in skus


# ===========================================================================
# 3. test_list_payouts
# ===========================================================================
class TestListPayouts:

    def test_list_payouts_returns_payouts(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        seed_shopify_payout(conn, acct_id, cid)
        seed_shopify_payout(conn, acct_id, cid,
                             gross="500.00", fee="14.50")

        result = call_action(ACTIONS["shopify-list-payouts"], conn, ns(
            shopify_account_id=acct_id,
            payout_status=None, date_from=None, date_to=None,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["count"] == 2


# ===========================================================================
# 4. test_get_payout_with_transactions
# ===========================================================================
class TestGetPayout:

    def test_get_payout_with_transactions(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        pid = seed_shopify_payout(conn, acct_id, cid)
        _seed_payout_transaction(conn, pid, cid, txn_type="charge")
        _seed_payout_transaction(conn, pid, cid, txn_type="refund",
                                  gross="-50.00", fee="0", net="-50.00")

        result = call_action(ACTIONS["shopify-get-payout"], conn, ns(
            shopify_payout_id=pid,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["id"] == pid
        assert result["transaction_count"] == 2


# ===========================================================================
# 5. test_list_refunds
# ===========================================================================
class TestListRefunds:

    def test_list_refunds_returns_refunds(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        oid = seed_shopify_order(conn, acct_id, cid)
        seed_shopify_refund(conn, oid, cid, refund_amount="25.00")
        seed_shopify_refund(conn, oid, cid, refund_amount="15.00")

        result = call_action(ACTIONS["shopify-list-refunds"], conn, ns(
            shopify_account_id=acct_id,
            date_from=None, date_to=None, gl_status=None,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["count"] == 2


# ===========================================================================
# 6. test_get_refund
# ===========================================================================
class TestGetRefund:

    def test_get_refund_returns_detail(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        oid = seed_shopify_order(conn, acct_id, cid)
        rid = seed_shopify_refund(conn, oid, cid, refund_amount="50.00")

        result = call_action(ACTIONS["shopify-get-refund"], conn, ns(
            shopify_refund_id_local=rid,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["id"] == rid
        assert result["refund_amount"] == "50.00"


# ===========================================================================
# 7. test_list_disputes
# ===========================================================================
class TestListDisputes:

    def test_list_disputes_returns_disputes(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        seed_shopify_dispute(conn, acct_id, cid)
        seed_shopify_dispute(conn, acct_id, cid, amount="75.00")

        result = call_action(ACTIONS["shopify-list-disputes"], conn, ns(
            shopify_account_id=acct_id,
            dispute_status=None,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["count"] == 2


# ===========================================================================
# 8. test_order_gl_detail
# ===========================================================================
class TestOrderGlDetail:

    def test_order_gl_detail_no_gl(self, conn):
        """Order without GL posting should return empty gl_entries."""
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        oid = seed_shopify_order(conn, acct_id, cid)

        result = call_action(ACTIONS["shopify-order-gl-detail"], conn, ns(
            shopify_order_id_local=oid,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["gl_entries"] == []
        assert "No GL entries posted" in result["message"]
