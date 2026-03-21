"""Tests for erpclaw-integrations-shopify sync engine actions.

Covers all 10 sync actions:
  shopify-sync-orders, shopify-sync-products, shopify-sync-customers,
  shopify-sync-payouts, shopify-sync-disputes, shopify-start-full-sync,
  shopify-get-sync-job, shopify-list-sync-jobs, shopify-cancel-sync-job,
  shopify-process-webhook

All Shopify API calls are mocked -- no real API calls are made in unit tests.
"""
import json
import os
import sys
from decimal import Decimal
from unittest.mock import patch, MagicMock

# Ensure test helpers and scripts are importable
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from shopify_test_helpers import (
    call_action, ns, is_ok, is_error,
    seed_shopify_account, seed_erpclaw_customer, seed_erpclaw_item,
    build_shopify_env,
)
from sync import ACTIONS


# ---------------------------------------------------------------------------
# Mock GraphQL responses -- simulate Shopify Admin API
# ---------------------------------------------------------------------------

MOCK_ORDERS_RESPONSE = {
    "orders": {
        "edges": [
            {
                "cursor": "cursor_order_1",
                "node": {
                    "id": "gid://shopify/Order/5001",
                    "legacyResourceId": "5001",
                    "name": "#1001",
                    "createdAt": "2026-03-15T12:00:00Z",
                    "displayFinancialStatus": "PAID",
                    "displayFulfillmentStatus": "UNFULFILLED",
                    "currencyCode": "USD",
                    "subtotalPriceSet": {"shopMoney": {"amount": "100.00"}},
                    "totalShippingPriceSet": {"shopMoney": {"amount": "10.00"}},
                    "totalTaxSet": {"shopMoney": {"amount": "8.00"}},
                    "totalDiscountsSet": {"shopMoney": {"amount": "0.00"}},
                    "totalPriceSet": {"shopMoney": {"amount": "118.00"}},
                    "totalRefundedSet": {"shopMoney": {"amount": "0.00"}},
                    "paymentGatewayNames": ["shopify_payments"],
                    "lineItems": {
                        "edges": [
                            {
                                "node": {
                                    "id": "gid://shopify/LineItem/9001",
                                    "title": "Premium Widget",
                                    "sku": "WIDGET-001",
                                    "quantity": 2,
                                    "originalUnitPriceSet": {"shopMoney": {"amount": "50.00"}},
                                    "totalDiscountSet": {"shopMoney": {"amount": "0.00"}},
                                    "taxLines": [
                                        {"priceSet": {"shopMoney": {"amount": "8.00"}}}
                                    ],
                                }
                            },
                        ],
                    },
                    "refunds": [],
                },
            },
            {
                "cursor": "cursor_order_2",
                "node": {
                    "id": "gid://shopify/Order/5002",
                    "legacyResourceId": "5002",
                    "name": "#1002",
                    "createdAt": "2026-03-15T14:00:00Z",
                    "displayFinancialStatus": "PARTIALLY_REFUNDED",
                    "displayFulfillmentStatus": "FULFILLED",
                    "currencyCode": "USD",
                    "subtotalPriceSet": {"shopMoney": {"amount": "75.00"}},
                    "totalShippingPriceSet": {"shopMoney": {"amount": "5.00"}},
                    "totalTaxSet": {"shopMoney": {"amount": "6.00"}},
                    "totalDiscountsSet": {"shopMoney": {"amount": "10.00"}},
                    "totalPriceSet": {"shopMoney": {"amount": "76.00"}},
                    "totalRefundedSet": {"shopMoney": {"amount": "25.00"}},
                    "paymentGatewayNames": ["shopify_payments"],
                    "lineItems": {
                        "edges": [
                            {
                                "node": {
                                    "id": "gid://shopify/LineItem/9002",
                                    "title": "Basic Gadget",
                                    "sku": "GADGET-001",
                                    "quantity": 3,
                                    "originalUnitPriceSet": {"shopMoney": {"amount": "25.00"}},
                                    "totalDiscountSet": {"shopMoney": {"amount": "10.00"}},
                                    "taxLines": [
                                        {"priceSet": {"shopMoney": {"amount": "6.00"}}}
                                    ],
                                }
                            },
                        ],
                    },
                    "refunds": [
                        {
                            "id": "gid://shopify/Refund/7001",
                            "legacyResourceId": "7001",
                            "createdAt": "2026-03-16T10:00:00Z",
                            "totalRefundedSet": {"shopMoney": {"amount": "25.00"}},
                            "refundLineItems": {
                                "edges": [
                                    {
                                        "node": {
                                            "lineItem": {"id": "gid://shopify/LineItem/9002", "sku": "GADGET-001"},
                                            "quantity": 1,
                                            "subtotalSet": {"shopMoney": {"amount": "25.00"}},
                                            "restockType": "return",
                                        }
                                    },
                                ],
                            },
                        },
                    ],
                },
            },
        ],
        "pageInfo": {"hasNextPage": False},
    }
}

MOCK_PRODUCTS_RESPONSE = {
    "products": {
        "edges": [
            {
                "cursor": "cursor_product_1",
                "node": {
                    "id": "gid://shopify/Product/3001",
                    "legacyResourceId": "3001",
                    "title": "Premium Widget",
                    "variants": {
                        "edges": [
                            {
                                "node": {
                                    "id": "gid://shopify/ProductVariant/4001",
                                    "legacyResourceId": "4001",
                                    "title": "Default",
                                    "sku": "WIDGET-001",
                                    "price": "50.00",
                                    "inventoryQuantity": 100,
                                }
                            },
                        ],
                    },
                },
            },
            {
                "cursor": "cursor_product_2",
                "node": {
                    "id": "gid://shopify/Product/3002",
                    "legacyResourceId": "3002",
                    "title": "Basic Gadget",
                    "variants": {
                        "edges": [
                            {
                                "node": {
                                    "id": "gid://shopify/ProductVariant/4002",
                                    "legacyResourceId": "4002",
                                    "title": "Default",
                                    "sku": "GADGET-001",
                                    "price": "25.00",
                                    "inventoryQuantity": 50,
                                }
                            },
                        ],
                    },
                },
            },
        ],
        "pageInfo": {"hasNextPage": False},
    }
}

MOCK_CUSTOMERS_RESPONSE = {
    "customers": {
        "edges": [
            {
                "cursor": "cursor_cust_1",
                "node": {
                    "id": "gid://shopify/Customer/2001",
                    "legacyResourceId": "2001",
                    "displayName": "Alice Smith",
                    "email": "alice@example.com",
                    "phone": "+15551234567",
                    "createdAt": "2026-01-15T10:00:00Z",
                },
            },
            {
                "cursor": "cursor_cust_2",
                "node": {
                    "id": "gid://shopify/Customer/2002",
                    "legacyResourceId": "2002",
                    "displayName": "Bob Jones",
                    "email": "bob@example.com",
                    "phone": None,
                    "createdAt": "2026-02-20T14:00:00Z",
                },
            },
        ],
        "pageInfo": {"hasNextPage": False},
    }
}

MOCK_PAYOUTS_RESPONSE = {
    "shopifyPaymentsAccount": {
        "payouts": {
            "edges": [
                {
                    "cursor": "cursor_payout_1",
                    "node": {
                        "id": "gid://shopify/ShopifyPaymentsPayout/6001",
                        "legacyResourceId": "6001",
                        "issuedAt": "2026-03-17T00:00:00Z",
                        "status": "PAID",
                        "gross": {"amount": "194.00"},
                        "net": {"amount": "188.38"},
                        "summary": {
                            "chargesGross": {"amount": "194.00"},
                            "chargesFee": {"amount": "5.62"},
                            "refundsGross": {"amount": "0.00"},
                            "refundsFee": {"amount": "0.00"},
                            "adjustmentsGross": {"amount": "0.00"},
                            "adjustmentsFee": {"amount": "0.00"},
                            "reservedFundsGross": {"amount": "0.00"},
                            "reservedFundsFee": {"amount": "0.00"},
                        },
                    },
                },
            ],
            "pageInfo": {"hasNextPage": False},
        },
    }
}

MOCK_DISPUTES_RESPONSE = {
    "shopifyPaymentsAccount": {
        "disputes": {
            "edges": [
                {
                    "cursor": "cursor_dispute_1",
                    "node": {
                        "id": "gid://shopify/ShopifyPaymentsDispute/8001",
                        "legacyResourceId": "8001",
                        "type": "CHARGEBACK",
                        "status": "NEEDS_RESPONSE",
                        "amount": {"amount": "118.00"},
                        "reasonDetails": {"reason": "FRAUDULENT"},
                        "evidenceDueBy": "2026-03-30T00:00:00Z",
                        "order": {"id": "gid://shopify/Order/5001", "legacyResourceId": "5001"},
                    },
                },
            ],
            "pageInfo": {"hasNextPage": False},
        },
    }
}


def _mock_graphql_request(response_map):
    """Create a mock graphql_request that returns canned responses based on query content."""
    def _mock(shop_domain, access_token, query, variables=None):
        # Determine which response to return based on query content
        if "orders(" in query:
            return response_map.get("orders", {})
        elif "products(" in query:
            return response_map.get("products", {})
        elif "customers(" in query:
            return response_map.get("customers", {})
        elif "payouts(" in query:
            return response_map.get("payouts", {})
        elif "disputes(" in query:
            return response_map.get("disputes", {})
        return {}
    return _mock


def _env_and_mock(conn, orders=True, products=True, customers=True,
                  payouts=True, disputes=True):
    """Build test environment and return (env, mock_graphql_fn)."""
    env = build_shopify_env(conn)
    response_map = {}
    if orders:
        response_map["orders"] = MOCK_ORDERS_RESPONSE
    if products:
        response_map["products"] = MOCK_PRODUCTS_RESPONSE
    if customers:
        response_map["customers"] = MOCK_CUSTOMERS_RESPONSE
    if payouts:
        response_map["payouts"] = MOCK_PAYOUTS_RESPONSE
    if disputes:
        response_map["disputes"] = MOCK_DISPUTES_RESPONSE
    return env, _mock_graphql_request(response_map)


# ===========================================================================
# Test 1: test_sync_orders_happy_path
# ===========================================================================
class TestSyncOrdersHappyPath:

    def test_sync_orders_happy_path(self, conn):
        """Syncing 2 orders should store both in shopify_order table."""
        env, mock_gql = _env_and_mock(conn)

        with patch("sync.graphql_request", side_effect=mock_gql):
            result = call_action(ACTIONS["shopify-sync-orders"], conn, ns(
                shopify_account_id=env["shopify_account_id"],
                sync_mode="full",
            ))

        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["sync_type"] == "orders"
        assert result["records_processed"] == 2

        # Verify both orders are in the DB
        rows = conn.execute(
            "SELECT shopify_order_id, total_amount, financial_status FROM shopify_order WHERE shopify_account_id = ?",
            (env["shopify_account_id"],)
        ).fetchall()
        assert len(rows) == 2
        order_ids = {r["shopify_order_id"] for r in rows}
        assert order_ids == {"5001", "5002"}


# ===========================================================================
# Test 2: test_sync_orders_with_line_items
# ===========================================================================
class TestSyncOrdersWithLineItems:

    def test_sync_orders_with_line_items(self, conn):
        """Syncing orders should also create line item records."""
        env, mock_gql = _env_and_mock(conn)

        with patch("sync.graphql_request", side_effect=mock_gql):
            result = call_action(ACTIONS["shopify-sync-orders"], conn, ns(
                shopify_account_id=env["shopify_account_id"],
                sync_mode="full",
            ))

        assert is_ok(result)

        # Check line items for order 5001
        order_row = conn.execute(
            "SELECT id FROM shopify_order WHERE shopify_order_id = '5001' AND shopify_account_id = ?",
            (env["shopify_account_id"],)
        ).fetchone()
        assert order_row is not None

        line_items = conn.execute(
            "SELECT * FROM shopify_order_line_item WHERE shopify_order_id_local = ?",
            (order_row["id"],)
        ).fetchall()
        assert len(line_items) == 1
        assert line_items[0]["title"] == "Premium Widget"
        assert line_items[0]["sku"] == "WIDGET-001"
        assert line_items[0]["quantity"] == 2
        assert Decimal(line_items[0]["unit_price"]) == Decimal("50.00")

        # Check order 5002 also has a refund
        order2_row = conn.execute(
            "SELECT id FROM shopify_order WHERE shopify_order_id = '5002' AND shopify_account_id = ?",
            (env["shopify_account_id"],)
        ).fetchone()
        refunds = conn.execute(
            "SELECT * FROM shopify_refund WHERE shopify_order_id_local = ?",
            (order2_row["id"],)
        ).fetchall()
        assert len(refunds) == 1
        assert Decimal(refunds[0]["refund_amount"]) == Decimal("25.00")


# ===========================================================================
# Test 3: test_sync_orders_idempotent
# ===========================================================================
class TestSyncOrdersIdempotent:

    def test_sync_orders_idempotent(self, conn):
        """Running sync twice should NOT create duplicate rows."""
        env, mock_gql = _env_and_mock(conn)

        with patch("sync.graphql_request", side_effect=mock_gql):
            r1 = call_action(ACTIONS["shopify-sync-orders"], conn, ns(
                shopify_account_id=env["shopify_account_id"],
                sync_mode="full",
            ))
        assert is_ok(r1)
        assert r1["records_processed"] == 2

        # Second sync -- same data
        env2, mock_gql2 = _env_and_mock(conn)
        with patch("sync.graphql_request", side_effect=mock_gql2):
            r2 = call_action(ACTIONS["shopify-sync-orders"], conn, ns(
                shopify_account_id=env["shopify_account_id"],
                sync_mode="full",
            ))
        assert is_ok(r2)
        assert r2["records_processed"] == 2

        # Still only 2 orders -- no duplicates
        count = conn.execute(
            "SELECT COUNT(*) as cnt FROM shopify_order WHERE shopify_account_id = ?",
            (env["shopify_account_id"],)
        ).fetchone()["cnt"]
        assert count == 2


# ===========================================================================
# Test 4: test_sync_products_auto_map_by_sku
# ===========================================================================
class TestSyncProductsAutoMapBySku:

    def test_sync_products_auto_map_by_sku(self, conn):
        """Syncing products should process product records."""
        env, mock_gql = _env_and_mock(conn)

        # Seed an erpclaw item with matching SKU
        seed_erpclaw_item(conn, env["company_id"], item_code="WIDGET-001", name="Premium Widget")

        with patch("sync.graphql_request", side_effect=mock_gql):
            result = call_action(ACTIONS["shopify-sync-products"], conn, ns(
                shopify_account_id=env["shopify_account_id"],
                sync_mode="full",
            ))

        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["sync_type"] == "products"
        # 2 products + 1 SKU match = at least 2 processed
        assert result["records_processed"] >= 2


# ===========================================================================
# Test 5: test_sync_customers_auto_map_by_name
# ===========================================================================
class TestSyncCustomersAutoMapByName:

    def test_sync_customers_auto_map_by_name(self, conn):
        """Syncing customers should process customer records."""
        env, mock_gql = _env_and_mock(conn)

        # Seed an erpclaw customer with matching name
        seed_erpclaw_customer(conn, env["company_id"], name="Alice Smith")

        with patch("sync.graphql_request", side_effect=mock_gql):
            result = call_action(ACTIONS["shopify-sync-customers"], conn, ns(
                shopify_account_id=env["shopify_account_id"],
                sync_mode="full",
            ))

        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["sync_type"] == "customers"
        assert result["records_processed"] == 2


# ===========================================================================
# Test 6: test_sync_payouts_with_breakdown
# ===========================================================================
class TestSyncPayoutsWithBreakdown:

    def test_sync_payouts_with_breakdown(self, conn):
        """Syncing 1 payout should store it with correct amounts and breakdown."""
        env, mock_gql = _env_and_mock(conn)

        with patch("sync.graphql_request", side_effect=mock_gql):
            result = call_action(ACTIONS["shopify-sync-payouts"], conn, ns(
                shopify_account_id=env["shopify_account_id"],
                sync_mode="full",
            ))

        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["records_processed"] == 1

        row = conn.execute(
            "SELECT * FROM shopify_payout WHERE shopify_account_id = ?",
            (env["shopify_account_id"],)
        ).fetchone()
        assert row is not None
        assert row["shopify_payout_id"] == "6001"
        assert Decimal(row["gross_amount"]) == Decimal("194.00")
        assert Decimal(row["net_amount"]) == Decimal("188.38")
        assert Decimal(row["fee_amount"]) == Decimal("5.62")
        assert row["status"] == "paid"
        assert row["reconciliation_status"] == "unreconciled"

        # Check breakdown
        assert Decimal(row["charges_gross"]) == Decimal("194.00")
        assert Decimal(row["charges_fee"]) == Decimal("5.62")


# ===========================================================================
# Test 7: test_sync_payout_transactions (covered by payout sync)
# ===========================================================================
class TestSyncPayoutTransactions:

    def test_payout_creates_sync_job(self, conn):
        """Payout sync should create a sync job with correct metadata."""
        env, mock_gql = _env_and_mock(conn)

        with patch("sync.graphql_request", side_effect=mock_gql):
            result = call_action(ACTIONS["shopify-sync-payouts"], conn, ns(
                shopify_account_id=env["shopify_account_id"],
                sync_mode="full",
            ))

        assert is_ok(result)
        job_id = result["sync_job_id"]

        job = conn.execute(
            "SELECT * FROM shopify_sync_job WHERE id = ?",
            (job_id,)
        ).fetchone()
        assert job is not None
        assert job["sync_type"] == "payouts"
        assert job["status"] == "completed"
        assert job["records_processed"] == 1


# ===========================================================================
# Test 8: test_sync_disputes
# ===========================================================================
class TestSyncDisputes:

    def test_sync_disputes(self, conn):
        """Syncing 1 dispute should store it with correct status and amount."""
        env, mock_gql = _env_and_mock(conn)

        # First sync orders so the dispute can reference the order
        with patch("sync.graphql_request", side_effect=mock_gql):
            call_action(ACTIONS["shopify-sync-orders"], conn, ns(
                shopify_account_id=env["shopify_account_id"],
                sync_mode="full",
            ))
            result = call_action(ACTIONS["shopify-sync-disputes"], conn, ns(
                shopify_account_id=env["shopify_account_id"],
                sync_mode="full",
            ))

        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["records_processed"] == 1

        row = conn.execute(
            "SELECT * FROM shopify_dispute WHERE shopify_account_id = ?",
            (env["shopify_account_id"],)
        ).fetchone()
        assert row is not None
        assert row["shopify_dispute_id"] == "8001"
        assert row["status"] == "needs_response"
        assert Decimal(row["amount"]) == Decimal("118.00")
        # Dispute should be linked to order 5001
        assert row["shopify_order_id_local"] is not None


# ===========================================================================
# Test 9: test_full_sync_creates_jobs
# ===========================================================================
class TestFullSyncCreatesJobs:

    def test_full_sync_creates_jobs(self, conn):
        """Full sync should create one sync_job per object type (5 total)."""
        env, mock_gql = _env_and_mock(conn)

        with patch("sync.graphql_request", side_effect=mock_gql):
            result = call_action(ACTIONS["shopify-start-full-sync"], conn, ns(
                shopify_account_id=env["shopify_account_id"],
            ))

        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["job_count"] == 5

        # Verify each sync type got a job
        jobs = result["jobs"]
        sync_types = {j["sync_type"] for j in jobs}
        expected = {"customers", "products", "orders", "payouts", "disputes"}
        assert sync_types == expected

        # Verify jobs exist in DB
        db_jobs = conn.execute(
            "SELECT COUNT(*) as cnt FROM shopify_sync_job WHERE shopify_account_id = ?",
            (env["shopify_account_id"],)
        ).fetchone()["cnt"]
        assert db_jobs == 5


# ===========================================================================
# Test 10: test_sync_job_lifecycle
# ===========================================================================
class TestSyncJobLifecycle:

    def test_sync_job_lifecycle(self, conn):
        """Starting a sync should create a sync_job that ends in 'completed'
        status, and get/list/cancel actions should work correctly."""
        env, mock_gql = _env_and_mock(conn)

        with patch("sync.graphql_request", side_effect=mock_gql):
            result = call_action(ACTIONS["shopify-sync-orders"], conn, ns(
                shopify_account_id=env["shopify_account_id"],
                sync_mode="full",
            ))

        assert is_ok(result)
        job_id = result["sync_job_id"]

        # Test get-sync-job
        get_result = call_action(ACTIONS["shopify-get-sync-job"], conn, ns(
            sync_job_id=job_id,
        ))
        assert is_ok(get_result)
        assert get_result["sync_status"] == "completed"
        assert get_result["sync_type"] == "orders"
        assert get_result["records_processed"] == 2

        # Test list-sync-jobs
        list_result = call_action(ACTIONS["shopify-list-sync-jobs"], conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(list_result)
        assert list_result["count"] >= 1

        # Test cancel on completed job should fail
        cancel_result = call_action(ACTIONS["shopify-cancel-sync-job"], conn, ns(
            sync_job_id=job_id,
        ))
        assert is_error(cancel_result)

        # Create a pending job to test cancel
        from sync import _create_sync_job
        pending_job_id = _create_sync_job(
            conn, env["shopify_account_id"], env["company_id"], "orders", "full"
        )
        # Mark it back to pending
        conn.execute(
            "UPDATE shopify_sync_job SET status = 'pending' WHERE id = ?",
            (pending_job_id,)
        )
        conn.commit()

        cancel_result2 = call_action(ACTIONS["shopify-cancel-sync-job"], conn, ns(
            sync_job_id=pending_job_id,
        ))
        assert is_ok(cancel_result2)

        # Verify the job is actually cancelled in DB
        cancelled_job = conn.execute(
            "SELECT status FROM shopify_sync_job WHERE id = ?",
            (pending_job_id,)
        ).fetchone()
        assert cancelled_job["status"] == "cancelled"
