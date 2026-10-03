"""Money-exactness tests for Shopify reporting actions.

Each test seeds TEXT money amounts using cents that binary floating point
cannot represent (0.10, 0.20) plus a half-cent boundary amount (2.675), runs
the action, and asserts every affected money figure as the exact hand-computed
string. The triple 0.10 + 0.20 + 2.675 is exactly 2.975, which rounds half-up
to "2.98"; the old SUM(CAST(... AS REAL)) float path accumulates
2.9749999999999996, which rounds to "2.97". A large-amount case seeds
1000000000000000.01 plus 1000000000000000.02 and expects 2000000000000000.03;
the old float path keeps only 2000000000000000.00. All seeds and reads here
go through PyPika with bound parameters.
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
    call_action, ns, is_ok,
    build_env,
    seed_shopify_order, seed_shopify_order_line_item,
    seed_shopify_refund, seed_shopify_payout, seed_shopify_dispute,
    seed_customer,
)
from erpclaw_lib.query import Q, P, Table, insert_row, update_row
from reports import ACTIONS

# Exact sum 0.10 + 0.20 + 2.675 = 2.975 -> "2.98" (half-up).
TRIPLE = ("0.10", "0.20", "2.675")


def _seed_triple_orders(conn, acct_id, cid):
    """Three orders whose every money column holds 0.10 / 0.20 / 2.675."""
    oids = []
    for i, val in enumerate(TRIPLE):
        oid = seed_shopify_order(
            conn, acct_id, cid, shopify_order_id=f"70{i}",
            subtotal=val, shipping=val, tax=val, discount=val, total=val)
        conn.execute(
            update_row("shopify_order", {"refunded_amount": P()}, {"id": P()}),
            (val, oid))
        oids.append(oid)
    conn.commit()
    return oids


class TestRevenueSummaryExact:

    def test_revenue_summary_exact(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        _seed_triple_orders(conn, acct_id, cid)

        result = call_action(ACTIONS["shopify-revenue-summary"], conn, ns(
            shopify_account_id=acct_id,
            period=None, date_from=None, date_to=None))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["order_count"] == 3
        assert result["product_revenue"] == "2.98"
        assert result["shipping_revenue"] == "2.98"
        assert result["tax_collected"] == "2.98"
        assert result["total_discounts"] == "2.98"
        assert result["gross_revenue"] == "2.98"
        assert result["total_refunded"] == "2.98"
        assert result["net_revenue"] == "0.00"


class TestRevenueSummaryLargeExact:

    def test_revenue_summary_large_amounts_exact(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        seed_shopify_order(
            conn, acct_id, cid, shopify_order_id="8001",
            subtotal="0", shipping="0", tax="0", discount="0",
            total="1000000000000000.01")
        seed_shopify_order(
            conn, acct_id, cid, shopify_order_id="8002",
            subtotal="0", shipping="0", tax="0", discount="0",
            total="1000000000000000.02")

        result = call_action(ACTIONS["shopify-revenue-summary"], conn, ns(
            shopify_account_id=acct_id,
            period=None, date_from=None, date_to=None))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["order_count"] == 2
        assert result["gross_revenue"] == "2000000000000000.03"


class TestFeeSummaryExact:

    def test_fee_summary_exact(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        for i, val in enumerate(TRIPLE):
            seed_shopify_payout(conn, acct_id, cid, gross=val, fee=val, net=val)

        result = call_action(ACTIONS["shopify-fee-summary"], conn, ns(
            shopify_account_id=acct_id,
            period=None, date_from=None, date_to=None))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["payout_count"] == 3
        assert result["total_gross"] == "2.98"
        assert result["total_fees"] == "2.98"
        assert result["total_net"] == "2.98"
        assert result["effective_fee_rate"] == "100.00"


class TestRefundSummaryExact:

    def test_refund_summary_exact(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        oid = seed_shopify_order(conn, acct_id, cid, shopify_order_id="7010")
        for val in TRIPLE:
            seed_shopify_refund(conn, oid, cid, refund_amount=val,
                                 tax_refund=val, shipping_refund=val)
        conn.execute(
            update_row(
                "shopify_refund", {"refund_type": P()},
                {"shopify_order_id_local": P(), "refund_amount": P()}),
            ("full", oid, "0.10"))
        conn.commit()

        result = call_action(ACTIONS["shopify-refund-summary"], conn, ns(
            shopify_account_id=acct_id,
            period=None, date_from=None, date_to=None))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["refund_count"] == 3
        assert result["total_refund_amount"] == "2.98"
        assert result["total_tax_refunded"] == "2.98"
        assert result["total_shipping_refunded"] == "2.98"
        assert result["full_refunds"] == 1
        assert result["partial_refunds"] == 2


class TestPayoutDetailReportExact:

    def test_payout_detail_report_exact(self, conn):
        from shopify_test_helpers import _uuid, _now
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        payout_id = seed_shopify_payout(
            conn, acct_id, cid, gross="3000.30", fee="0.30")
        for i, val in enumerate(TRIPLE):
            sql, _cols = insert_row("shopify_payout_transaction", {
                "id": P(), "shopify_payout_id_local": P(),
                "shopify_balance_txn_id": P(), "transaction_type": P(),
                "gross_amount": P(), "fee_amount": P(), "net_amount": P(),
                "company_id": P(), "created_at": P()})
            conn.execute(
                sql,
                (_uuid(), payout_id, f"txn-exact-{i}", "charge",
                 val, val, val, cid, _now()))
        conn.commit()

        result = call_action(ACTIONS["shopify-payout-detail-report"], conn, ns(
            shopify_account_id=acct_id, date_from=None, date_to=None))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["payout_count"] == 1
        assert result["total_gross"] == "3000.30"
        assert result["total_fee"] == "0.30"
        assert result["total_net"] == "3000.00"
        payout = result["payouts"][0]
        assert payout["gross"] == "3000.30"
        assert payout["fee"] == "0.30"
        assert payout["net"] == "3000.00"
        assert len(payout["transaction_breakdown"]) == 1
        breakdown = payout["transaction_breakdown"][0]
        assert breakdown["type"] == "charge"
        assert breakdown["count"] == 3
        assert breakdown["gross"] == "2.98"
        assert breakdown["fee"] == "2.98"
        assert breakdown["net"] == "2.98"


class TestProductRevenueReportExact:

    def test_product_revenue_report_exact(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        oid = seed_shopify_order(conn, acct_id, cid, shopify_order_id="7020")
        for val in TRIPLE:
            li_id = seed_shopify_order_line_item(
                conn, oid, cid, sku="EXACT-A", quantity=1,
                unit_price=val)
            conn.execute(
                update_row(
                    "shopify_order_line_item",
                    {"discount_amount": P(), "tax_amount": P()},
                    {"id": P()}),
                (val, val, li_id))
        seed_shopify_order_line_item(
            conn, oid, cid, sku="EXACT-BIG", quantity=1,
            unit_price="1000.10")
        conn.commit()

        result = call_action(ACTIONS["shopify-product-revenue-report"], conn, ns(
            shopify_account_id=acct_id, date_from=None, date_to=None))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["product_count"] == 2
        assert result["products"][0]["sku"] == "EXACT-BIG"
        assert result["products"][0]["total_revenue"] == "1000.10"
        assert result["products"][1]["sku"] == "EXACT-A"
        assert result["products"][1]["total_quantity"] == 3
        assert result["products"][1]["total_revenue"] == "2.98"
        assert result["products"][1]["total_discounts"] == "2.98"
        assert result["products"][1]["total_tax"] == "2.98"


class TestCustomerRevenueReportExact:

    def test_customer_revenue_report_exact(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        cust_small = seed_customer(conn, cid, name="Exact Small")
        cust_big = seed_customer(conn, cid, name="Exact Big")
        for i, val in enumerate(TRIPLE):
            oid = seed_shopify_order(
                conn, acct_id, cid, shopify_order_id=f"703{i}", total=val)
            conn.execute(
                update_row(
                    "shopify_order",
                    {"customer_id": P(), "refunded_amount": P()},
                    {"id": P()}),
                (cust_small, val, oid))
        big_oid = seed_shopify_order(
            conn, acct_id, cid, shopify_order_id="7039", total="2000.20")
        conn.execute(
            update_row("shopify_order", {"customer_id": P()}, {"id": P()}),
            (cust_big, big_oid))
        conn.commit()

        result = call_action(ACTIONS["shopify-customer-revenue-report"], conn, ns(
            shopify_account_id=acct_id, date_from=None, date_to=None))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["customer_count"] == 2
        assert result["customers"][0]["customer_name"] == "Exact Big"
        assert result["customers"][0]["total_revenue"] == "2000.20"
        assert result["customers"][0]["total_refunded"] == "0.00"
        assert result["customers"][0]["net_revenue"] == "2000.20"
        assert result["customers"][1]["customer_name"] == "Exact Small"
        assert result["customers"][1]["order_count"] == 3
        assert result["customers"][1]["total_revenue"] == "2.98"
        assert result["customers"][1]["total_refunded"] == "2.98"
        assert result["customers"][1]["net_revenue"] == "0.00"


class TestShopifyStatusExact:

    def test_shopify_status_exact(self, conn):
        env = build_env(conn)
        acct_id = env["shopify_account_id"]
        cid = env["company_id"]
        oids = _seed_triple_orders(conn, acct_id, cid)
        for oid, val in zip(oids, TRIPLE):
            seed_shopify_refund(conn, oid, cid, refund_amount=val)
        for val in TRIPLE:
            seed_shopify_payout(conn, acct_id, cid, gross=val, fee="0")
            seed_shopify_dispute(conn, acct_id, cid, amount=val)

        result = call_action(ACTIONS["shopify-status"], conn, ns(
            shopify_account_id=acct_id))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["orders"]["total"] == 3
        assert result["orders"]["total_revenue"] == "2.98"
        assert result["payouts"]["total"] == 3
        assert result["payouts"]["total_net_paid"] == "2.98"
        assert result["refunds"]["total"] == 3
        assert result["refunds"]["total_refunded"] == "2.98"
        assert result["disputes"]["total"] == 3
        assert result["disputes"]["total_disputed"] == "2.98"
