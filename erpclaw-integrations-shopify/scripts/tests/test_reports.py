"""Tests for erpclaw-integrations-shopify reporting actions.

Covers: revenue-summary, fee-summary, refund-summary, payout-detail-report,
        product-revenue-report, customer-revenue-report, status.
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
    seed_customer, _uuid,
)
from reports import ACTIONS


def _seed_order_with_customer(conn, acct_id, company_id, customer_id,
                               shopify_order_id="8001",
                               total="118.00", subtotal="100.00",
                               tax="8.00", shipping="10.00"):
    """Seed an order linked to a customer."""
    from shopify_test_helpers import _now, _today
    oid = _uuid()
    now = _now()
    conn.execute(
        """INSERT INTO shopify_order
            (id, shopify_account_id, shopify_order_id, shopify_order_number,
             order_date, financial_status, fulfillment_status,
             currency, subtotal_amount, shipping_amount, tax_amount,
             discount_amount, total_amount, refunded_amount,
             gl_status, payment_gateway,
             is_gift_card_order, has_refunds,
             customer_id, company_id, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, 'PAID', 'UNFULFILLED',
                   'USD', ?, ?, ?,
                   '0', ?, '0',
                   'pending', 'shopify_payments',
                   0, 0,
                   ?, ?, ?, ?)""",
        (oid, acct_id, shopify_order_id, f"#{shopify_order_id}",
         _today(), subtotal, shipping, tax, total,
         customer_id, company_id, now, now)
    )
    conn.commit()
    return oid


# ===========================================================================
# 1. test_revenue_summary
# ===========================================================================
class TestRevenueSummary:

    def test_revenue_summary_aggregates(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        seed_shopify_order(conn, acct_id, cid, shopify_order_id="9001",
                           total="118.00", subtotal="100.00",
                           tax="8.00", shipping="10.00")
        seed_shopify_order(conn, acct_id, cid, shopify_order_id="9002",
                           total="59.00", subtotal="50.00",
                           tax="4.00", shipping="5.00")

        result = call_action(ACTIONS["shopify-revenue-summary"], conn, ns(
            shopify_account_id=acct_id,
            period=None, date_from=None, date_to=None,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["order_count"] == 2
        assert result["product_revenue"] == "150.00"
        assert result["shipping_revenue"] == "15.00"
        assert result["tax_collected"] == "12.00"
        assert result["gross_revenue"] == "177.00"


# ===========================================================================
# 2. test_fee_summary
# ===========================================================================
class TestFeeSummary:

    def test_fee_summary_calculates_rate(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        seed_shopify_payout(conn, acct_id, cid,
                             gross="1000.00", fee="29.00")

        result = call_action(ACTIONS["shopify-fee-summary"], conn, ns(
            shopify_account_id=acct_id,
            period=None, date_from=None, date_to=None,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["payout_count"] == 1
        assert result["total_fees"] == "29.00"
        assert result["effective_fee_rate"] == "2.90"


# ===========================================================================
# 3. test_refund_summary
# ===========================================================================
class TestRefundSummary:

    def test_refund_summary_counts(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        oid = seed_shopify_order(conn, acct_id, cid)
        seed_shopify_refund(conn, oid, cid, refund_amount="25.00",
                             tax_refund="2.00")
        seed_shopify_refund(conn, oid, cid, refund_amount="10.00",
                             tax_refund="1.00")

        result = call_action(ACTIONS["shopify-refund-summary"], conn, ns(
            shopify_account_id=acct_id,
            period=None, date_from=None, date_to=None,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["refund_count"] == 2
        assert result["total_refund_amount"] == "35.00"
        assert result["total_tax_refunded"] == "3.00"
        assert result["partial_refunds"] == 2


# ===========================================================================
# 4. test_payout_detail_report
# ===========================================================================
class TestPayoutDetailReport:

    def test_payout_detail_report_structure(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        seed_shopify_payout(conn, acct_id, cid)

        result = call_action(ACTIONS["shopify-payout-detail-report"], conn, ns(
            shopify_account_id=acct_id,
            date_from=None, date_to=None,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["payout_count"] == 1
        assert result["total_gross"] == "1000.00"
        assert result["total_fee"] == "29.00"
        assert result["total_net"] == "971.00"
        assert len(result["payouts"]) == 1


# ===========================================================================
# 5. test_product_revenue_report
# ===========================================================================
class TestProductRevenueReport:

    def test_product_revenue_by_sku(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        oid = seed_shopify_order(conn, acct_id, cid)
        seed_shopify_order_line_item(conn, oid, cid, sku="WIDGET-A",
                                      quantity=3, unit_price="25.00")
        seed_shopify_order_line_item(conn, oid, cid, sku="WIDGET-B",
                                      quantity=1, unit_price="100.00")

        result = call_action(ACTIONS["shopify-product-revenue-report"], conn, ns(
            shopify_account_id=acct_id,
            date_from=None, date_to=None,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["product_count"] == 2
        skus = [p["sku"] for p in result["products"]]
        assert "WIDGET-A" in skus
        assert "WIDGET-B" in skus


# ===========================================================================
# 6. test_customer_revenue_report
# ===========================================================================
class TestCustomerRevenueReport:

    def test_customer_revenue_report(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        cust_id = seed_customer(conn, cid, name="Alice Smith")
        _seed_order_with_customer(conn, acct_id, cid, cust_id,
                                   shopify_order_id="8001", total="200.00")
        _seed_order_with_customer(conn, acct_id, cid, cust_id,
                                   shopify_order_id="8002", total="150.00")

        result = call_action(ACTIONS["shopify-customer-revenue-report"], conn, ns(
            shopify_account_id=acct_id,
            date_from=None, date_to=None,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["customer_count"] == 1
        assert result["customers"][0]["order_count"] == 2
        assert result["customers"][0]["total_revenue"] == "350.00"


# ===========================================================================
# 7. test_status
# ===========================================================================
class TestStatus:

    def test_status_dashboard(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        seed_shopify_order(conn, acct_id, cid, shopify_order_id="9501")
        seed_shopify_payout(conn, acct_id, cid)

        result = call_action(ACTIONS["shopify-status"], conn, ns(
            shopify_account_id=acct_id,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["shopify_account_id"] == acct_id
        assert result["orders"]["total"] == 1
        assert result["payouts"]["total"] == 1
        assert "clearing_balance" in result
        assert "last_sync" in result
