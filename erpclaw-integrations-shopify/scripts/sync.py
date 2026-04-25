"""ERPClaw Integrations Shopify — sync engine actions.

10 actions for pulling data from the Shopify GraphQL Admin API into local
mirror tables, processing webhooks, and managing sync job lifecycle.

Imported by db_query.py (unified router).
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.response import ok, err, row_to_dict, rows_to_list
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import (
        Q, P, Table, Field, fn, Order,
        insert_row, update_row, dynamic_update,
    )
except ImportError:
    pass

# Add scripts directory to path for sibling imports
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from shopify_helpers import (
    SKILL, now_iso, shopify_amount_to_decimal,
    get_shopify_client, graphql_request, validate_shopify_account,
)

VALID_SYNC_TYPES = (
    "orders", "products", "customers", "payouts",
    "inventory", "disputes", "full",
)

# Order matters for full sync — customers first so matching can work,
# then transactional objects in dependency order.
FULL_SYNC_ORDER = (
    "customers", "products", "orders", "payouts", "disputes",
)


# ---------------------------------------------------------------------------
# Internal: sync job lifecycle
# ---------------------------------------------------------------------------

def _create_sync_job(conn, shopify_account_id, company_id, sync_type,
                     sync_mode="incremental"):
    """Create a shopify_sync_job record with status='running'."""
    job_id = str(uuid.uuid4())
    now = now_iso()
    sql, _ = insert_row("shopify_sync_job", {
        "id": P(), "shopify_account_id": P(), "sync_type": P(),
        "sync_mode": P(), "status": P(), "records_processed": P(),
        "records_created": P(), "records_updated": P(), "records_failed": P(),
        "started_at": P(), "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        job_id, shopify_account_id, sync_type,
        sync_mode, "running", 0,
        0, 0, 0,
        now, company_id, now,
    ))
    conn.commit()
    return job_id


def _complete_sync_job(conn, job_id, records_processed, records_created=0,
                       records_updated=0, records_failed=0):
    """Mark a sync job as completed with final counts."""
    now = now_iso()
    sql, params = dynamic_update("shopify_sync_job", {
        "status": "completed",
        "records_processed": records_processed,
        "records_created": records_created,
        "records_updated": records_updated,
        "records_failed": records_failed,
        "completed_at": now,
    }, {"id": job_id})
    conn.execute(sql, params)
    conn.commit()


def _fail_sync_job(conn, job_id, error_message, records_processed=0):
    """Mark a sync job as failed with error details."""
    now = now_iso()
    sql, params = dynamic_update("shopify_sync_job", {
        "status": "failed",
        "records_processed": records_processed,
        "error_message": str(error_message)[:2000],
        "completed_at": now,
    }, {"id": job_id})
    conn.execute(sql, params)
    conn.commit()


# ---------------------------------------------------------------------------
# Internal: object-type-specific sync handlers
# ---------------------------------------------------------------------------

# GraphQL queries for Shopify Admin API
_ORDERS_QUERY = """
query($cursor: String) {
  orders(first: 50, after: $cursor, sortKey: CREATED_AT) {
    edges {
      cursor
      node {
        id
        legacyResourceId
        name
        createdAt
        displayFinancialStatus
        displayFulfillmentStatus
        currencyCode
        subtotalPriceSet { shopMoney { amount } }
        totalShippingPriceSet { shopMoney { amount } }
        totalTaxSet { shopMoney { amount } }
        totalDiscountsSet { shopMoney { amount } }
        totalPriceSet { shopMoney { amount } }
        totalRefundedSet { shopMoney { amount } }
        paymentGatewayNames
        lineItems(first: 50) {
          edges {
            node {
              id
              title
              sku
              quantity
              originalUnitPriceSet { shopMoney { amount } }
              totalDiscountSet { shopMoney { amount } }
              taxLines { priceSet { shopMoney { amount } } }
            }
          }
        }
        refunds {
          id
          legacyResourceId
          createdAt
          totalRefundedSet { shopMoney { amount } }
          refundLineItems(first: 50) {
            edges {
              node {
                lineItem { id sku }
                quantity
                subtotalSet { shopMoney { amount } }
                restockType
              }
            }
          }
        }
      }
    }
    pageInfo { hasNextPage }
  }
}
"""

_PRODUCTS_QUERY = """
query($cursor: String) {
  products(first: 50, after: $cursor) {
    edges {
      cursor
      node {
        id
        legacyResourceId
        title
        variants(first: 50) {
          edges {
            node {
              id
              legacyResourceId
              title
              sku
              price
              inventoryQuantity
            }
          }
        }
      }
    }
    pageInfo { hasNextPage }
  }
}
"""

_CUSTOMERS_QUERY = """
query($cursor: String) {
  customers(first: 50, after: $cursor) {
    edges {
      cursor
      node {
        id
        legacyResourceId
        displayName
        email
        phone
        createdAt
      }
    }
    pageInfo { hasNextPage }
  }
}
"""

_PAYOUTS_QUERY = """
query($cursor: String) {
  shopifyPaymentsAccount {
    payouts(first: 50, after: $cursor) {
      edges {
        cursor
        node {
          id
          legacyResourceId
          issuedAt
          status
          net { amount }
          gross { amount }
          summary {
            chargesGross { amount }
            chargesFee { amount }
            refundsFee { amount }
            # NOTE: `refundsGross` was removed from ShopifyPaymentsPayoutSummary
            # in API 2026-04; the gross refund amount is no longer exposed
            # at the payout-summary level. The `refunds_gross` column on
            # `shopify_payout` stays at its '0' default until Shopify
            # surfaces an equivalent field again.
            adjustmentsGross { amount }
            adjustmentsFee { amount }
            reservedFundsGross { amount }
            reservedFundsFee { amount }
          }
        }
      }
      pageInfo { hasNextPage }
    }
  }
}
"""

_DISPUTES_QUERY = """
query($cursor: String) {
  shopifyPaymentsAccount {
    disputes(first: 50, after: $cursor) {
      edges {
        cursor
        node {
          id
          legacyResourceId
          type
          status
          amount { amount }
          reasonDetails { reason }
          evidenceDueBy
          order { id legacyResourceId }
        }
      }
      pageInfo { hasNextPage }
    }
  }
}
"""


def _safe_money(price_set):
    """Extract amount from a Shopify MoneyV2 / MoneyBag structure.

    Handles both MoneyBag (shopMoney.amount) and direct MoneyV2 (amount).
    Returns Decimal.
    """
    if price_set is None:
        return Decimal("0")
    # If it has shopMoney (MoneyBag), use that
    if isinstance(price_set, dict):
        shop_money = price_set.get("shopMoney")
        if shop_money:
            return shopify_amount_to_decimal(shop_money.get("amount"))
        # Direct MoneyV2
        return shopify_amount_to_decimal(price_set.get("amount"))
    return Decimal("0")


def _sync_orders(conn, client, acct_id, company_id, sync_job_id):
    """Sync orders from Shopify GraphQL API into shopify_order + line items + refunds."""
    cursor = None
    total = 0
    created = 0
    updated = 0

    while True:
        variables = {"cursor": cursor} if cursor else {}
        data = graphql_request(
            client["shop_domain"], client["access_token"],
            _ORDERS_QUERY, variables,
        )
        orders_data = data.get("orders", {})
        edges = orders_data.get("edges", [])

        for edge in edges:
            node = edge.get("node", {})
            cursor = edge.get("cursor")
            shopify_order_id = node.get("legacyResourceId", "")
            now = now_iso()

            # Check if order already exists
            existing = conn.execute(
                "SELECT id FROM shopify_order WHERE shopify_account_id = ? AND shopify_order_id = ?",
                (acct_id, shopify_order_id)
            ).fetchone()

            order_local_id = existing["id"] if existing else str(uuid.uuid4())

            subtotal = _safe_money(node.get("subtotalPriceSet"))
            shipping = _safe_money(node.get("totalShippingPriceSet"))
            tax = _safe_money(node.get("totalTaxSet"))
            discount = _safe_money(node.get("totalDiscountsSet"))
            total_price = _safe_money(node.get("totalPriceSet"))
            refunded = _safe_money(node.get("totalRefundedSet"))

            gateways = node.get("paymentGatewayNames", [])
            gateway_str = ",".join(gateways) if isinstance(gateways, list) else str(gateways or "")

            refunds = node.get("refunds", [])
            has_refunds = 1 if refunds else 0

            if existing:
                sql, params = dynamic_update("shopify_order", {
                    "shopify_order_number": node.get("name", ""),
                    "order_date": node.get("createdAt", ""),
                    "financial_status": node.get("displayFinancialStatus", ""),
                    "fulfillment_status": node.get("displayFulfillmentStatus", ""),
                    "subtotal_amount": str(subtotal),
                    "shipping_amount": str(shipping),
                    "tax_amount": str(tax),
                    "discount_amount": str(discount),
                    "total_amount": str(total_price),
                    "refunded_amount": str(refunded),
                    "payment_gateway": gateway_str,
                    "has_refunds": has_refunds,
                    "updated_at": now,
                }, {"id": order_local_id})
                conn.execute(sql, params)
                updated += 1
            else:
                sql, _ = insert_row("shopify_order", {
                    "id": P(), "shopify_account_id": P(), "shopify_order_id": P(),
                    "shopify_order_number": P(), "order_date": P(),
                    "financial_status": P(), "fulfillment_status": P(),
                    "currency": P(), "subtotal_amount": P(), "shipping_amount": P(),
                    "tax_amount": P(), "discount_amount": P(), "total_amount": P(),
                    "refunded_amount": P(), "gl_status": P(), "payment_gateway": P(),
                    "is_gift_card_order": P(), "has_refunds": P(),
                    "company_id": P(), "created_at": P(), "updated_at": P(),
                })
                conn.execute(sql, (
                    order_local_id, acct_id, shopify_order_id,
                    node.get("name", ""), node.get("createdAt", ""),
                    node.get("displayFinancialStatus", ""),
                    node.get("displayFulfillmentStatus", ""),
                    node.get("currencyCode", "USD"),
                    str(subtotal), str(shipping),
                    str(tax), str(discount), str(total_price),
                    str(refunded), "pending", gateway_str,
                    0, has_refunds,
                    company_id, now, now,
                ))
                created += 1

            # Sync line items
            line_items = node.get("lineItems", {}).get("edges", [])
            for li_edge in line_items:
                li_node = li_edge.get("node", {})
                li_gid = li_node.get("id", "")
                # Extract numeric part from GID for shopify_line_item_id
                li_id_parts = li_gid.split("/")
                shopify_li_id = li_id_parts[-1] if li_id_parts else li_gid

                unit_price = _safe_money(li_node.get("originalUnitPriceSet"))
                li_discount = _safe_money(li_node.get("totalDiscountSet"))
                qty = li_node.get("quantity", 1) or 1

                # Sum tax lines
                li_tax = Decimal("0")
                for tl in (li_node.get("taxLines") or []):
                    li_tax += _safe_money(tl.get("priceSet"))

                li_total = (unit_price * qty) - li_discount + li_tax

                # Check if line item exists
                existing_li = conn.execute(
                    "SELECT id FROM shopify_order_line_item WHERE shopify_order_id_local = ? AND shopify_line_item_id = ?",
                    (order_local_id, shopify_li_id)
                ).fetchone()

                if not existing_li:
                    li_local_id = str(uuid.uuid4())
                    # Try to match SKU to erpclaw item
                    sku = li_node.get("sku", "")
                    item_id = None
                    if sku:
                        item_row = conn.execute(
                            "SELECT id FROM item WHERE item_code = ?",
                            (sku,)
                        ).fetchone()
                        if item_row:
                            item_id = item_row["id"]

                    sql, _ = insert_row("shopify_order_line_item", {
                        "id": P(), "shopify_order_id_local": P(),
                        "shopify_line_item_id": P(), "title": P(), "sku": P(),
                        "quantity": P(), "unit_price": P(), "discount_amount": P(),
                        "tax_amount": P(), "total_amount": P(), "item_id": P(),
                        "is_gift_card": P(), "company_id": P(), "created_at": P(),
                    })
                    conn.execute(sql, (
                        li_local_id, order_local_id,
                        shopify_li_id, li_node.get("title", ""), sku,
                        qty, str(unit_price), str(li_discount),
                        str(li_tax), str(li_total), item_id,
                        0, company_id, now,
                    ))

            # Sync refunds
            for refund_node in refunds:
                shopify_refund_id = refund_node.get("legacyResourceId", "")
                existing_refund = conn.execute(
                    "SELECT id FROM shopify_refund WHERE shopify_order_id_local = ? AND shopify_refund_id = ?",
                    (order_local_id, shopify_refund_id)
                ).fetchone()

                if not existing_refund:
                    refund_local_id = str(uuid.uuid4())
                    refund_total = _safe_money(refund_node.get("totalRefundedSet"))

                    sql, _ = insert_row("shopify_refund", {
                        "id": P(), "shopify_order_id_local": P(),
                        "shopify_refund_id": P(), "refund_date": P(),
                        "refund_amount": P(), "refund_type": P(),
                        "gl_status": P(), "company_id": P(), "created_at": P(),
                    })
                    conn.execute(sql, (
                        refund_local_id, order_local_id,
                        shopify_refund_id, refund_node.get("createdAt", ""),
                        str(refund_total), "partial",
                        "pending", company_id, now,
                    ))

                    # Sync refund line items
                    refund_lis = refund_node.get("refundLineItems", {}).get("edges", [])
                    for rli_edge in refund_lis:
                        rli_node = rli_edge.get("node", {})
                        rli_li = rli_node.get("lineItem", {})
                        rli_li_id = rli_li.get("id", "").split("/")[-1] if rli_li.get("id") else ""

                        rli_local_id = str(uuid.uuid4())
                        sql, _ = insert_row("shopify_refund_line_item", {
                            "id": P(), "shopify_refund_id_local": P(),
                            "shopify_line_item_id": P(), "quantity": P(),
                            "subtotal_amount": P(), "restock_type": P(),
                            "company_id": P(), "created_at": P(),
                        })
                        conn.execute(sql, (
                            rli_local_id, refund_local_id,
                            rli_li_id, rli_node.get("quantity", 1),
                            str(_safe_money(rli_node.get("subtotalSet"))),
                            rli_node.get("restockType", "no_restock") or "no_restock",
                            company_id, now,
                        ))

            total += 1

        conn.commit()

        page_info = orders_data.get("pageInfo", {})
        if not page_info.get("hasNextPage", False) or not edges:
            break

    return total, created, updated


def _sync_products(conn, client, acct_id, company_id, sync_job_id):
    """Sync products from Shopify. Auto-map to erpclaw items by SKU match."""
    cursor = None
    total = 0

    while True:
        variables = {"cursor": cursor} if cursor else {}
        data = graphql_request(
            client["shop_domain"], client["access_token"],
            _PRODUCTS_QUERY, variables,
        )
        products_data = data.get("products", {})
        edges = products_data.get("edges", [])

        for edge in edges:
            node = edge.get("node", {})
            cursor = edge.get("cursor")

            # For each variant, try to match by SKU to erpclaw item
            variants = node.get("variants", {}).get("edges", [])
            for var_edge in variants:
                var_node = var_edge.get("node", {})
                sku = var_node.get("sku", "")
                if not sku:
                    continue

                # Check if item exists in erpclaw by SKU match
                item_row = conn.execute(
                    "SELECT id FROM item WHERE item_code = ?",
                    (sku,)
                ).fetchone()

                if item_row:
                    total += 1

            total += 1

        page_info = products_data.get("pageInfo", {})
        if not page_info.get("hasNextPage", False) or not edges:
            break

    return total


def _sync_customers(conn, client, acct_id, company_id, sync_job_id):
    """Sync customers from Shopify. Auto-match by name to erpclaw customer."""
    cursor = None
    total = 0
    cust_table = Table("customer")

    while True:
        variables = {"cursor": cursor} if cursor else {}
        data = graphql_request(
            client["shop_domain"], client["access_token"],
            _CUSTOMERS_QUERY, variables,
        )
        customers_data = data.get("customers", {})
        edges = customers_data.get("edges", [])

        for edge in edges:
            node = edge.get("node", {})
            cursor = edge.get("cursor")
            shopify_name = node.get("displayName", "")

            # Attempt to match to erpclaw customer by name
            if shopify_name:
                match_row = conn.execute(
                    Q.from_(cust_table).select(cust_table.id)
                    .where(cust_table.name == P())
                    .where(cust_table.company_id == P())
                    .get_sql(),
                    (shopify_name, company_id)
                ).fetchone()
                if match_row:
                    # Customer matched
                    pass

            total += 1

        conn.commit()

        page_info = customers_data.get("pageInfo", {})
        if not page_info.get("hasNextPage", False) or not edges:
            break

    return total


def _sync_payouts(conn, client, acct_id, company_id, sync_job_id):
    """Sync ShopifyPayments payouts with summary breakdown."""
    cursor = None
    total = 0
    now = now_iso()

    while True:
        variables = {"cursor": cursor} if cursor else {}
        data = graphql_request(
            client["shop_domain"], client["access_token"],
            _PAYOUTS_QUERY, variables,
        )
        # `shopifyPaymentsAccount` is null when the merchant hasn't enabled
        # Shopify Payments on this shop (typical for test/dev stores).
        # The default-arg form .get(k, {}) only kicks in when k is missing;
        # for an explicit null value Python returns None, so guard explicitly.
        payments_acct = data.get("shopifyPaymentsAccount") or {}
        payouts_data = payments_acct.get("payouts", {})
        edges = payouts_data.get("edges", [])

        for edge in edges:
            node = edge.get("node", {})
            cursor = edge.get("cursor")
            shopify_payout_id = node.get("legacyResourceId", "")

            # Check if payout already exists
            existing = conn.execute(
                "SELECT id FROM shopify_payout WHERE shopify_account_id = ? AND shopify_payout_id = ?",
                (acct_id, shopify_payout_id)
            ).fetchone()

            if existing:
                total += 1
                continue

            payout_local_id = str(uuid.uuid4())
            gross = _safe_money(node.get("gross"))
            net = _safe_money(node.get("net"))
            fee = gross - net

            summary = node.get("summary", {})

            sql, _ = insert_row("shopify_payout", {
                "id": P(), "shopify_account_id": P(), "shopify_payout_id": P(),
                "issued_at": P(), "status": P(),
                "gross_amount": P(), "fee_amount": P(), "net_amount": P(),
                "charges_gross": P(), "charges_fee": P(),
                "refunds_gross": P(), "refunds_fee": P(),
                "adjustments_gross": P(), "adjustments_fee": P(),
                "reserved_funds_gross": P(), "reserved_funds_fee": P(),
                "gl_status": P(), "reconciliation_status": P(),
                "company_id": P(), "created_at": P(),
            })
            conn.execute(sql, (
                payout_local_id, acct_id, shopify_payout_id,
                node.get("issuedAt", ""),
                (node.get("status", "SCHEDULED") or "SCHEDULED").lower(),
                str(gross), str(fee), str(net),
                str(_safe_money(summary.get("chargesGross"))),
                str(_safe_money(summary.get("chargesFee"))),
                # refundsGross removed in API 2026-04; column stays at 0.
                "0",
                str(_safe_money(summary.get("refundsFee"))),
                str(_safe_money(summary.get("adjustmentsGross"))),
                str(_safe_money(summary.get("adjustmentsFee"))),
                str(_safe_money(summary.get("reservedFundsGross"))),
                str(_safe_money(summary.get("reservedFundsFee"))),
                "pending", "unreconciled",
                company_id, now,
            ))
            total += 1

        conn.commit()

        page_info = payouts_data.get("pageInfo", {})
        if not page_info.get("hasNextPage", False) or not edges:
            break

    return total


def _sync_disputes(conn, client, acct_id, company_id, sync_job_id):
    """Sync Shopify disputes/chargebacks."""
    cursor = None
    total = 0
    now = now_iso()

    while True:
        variables = {"cursor": cursor} if cursor else {}
        data = graphql_request(
            client["shop_domain"], client["access_token"],
            _DISPUTES_QUERY, variables,
        )
        # `shopifyPaymentsAccount` is null when the merchant hasn't enabled
        # Shopify Payments on this shop (typical for test/dev stores).
        # The default-arg form .get(k, {}) only kicks in when k is missing;
        # for an explicit null value Python returns None, so guard explicitly.
        payments_acct = data.get("shopifyPaymentsAccount") or {}
        disputes_data = payments_acct.get("disputes", {})
        edges = disputes_data.get("edges", [])

        for edge in edges:
            node = edge.get("node", {})
            cursor = edge.get("cursor")
            shopify_dispute_id = node.get("legacyResourceId", "")

            # Check if dispute already exists
            existing = conn.execute(
                "SELECT id FROM shopify_dispute WHERE shopify_account_id = ? AND shopify_dispute_id = ?",
                (acct_id, shopify_dispute_id)
            ).fetchone()

            if existing:
                total += 1
                continue

            dispute_local_id = str(uuid.uuid4())

            # Try to find linked order
            order_node = node.get("order", {})
            order_local_id = None
            if order_node and order_node.get("legacyResourceId"):
                order_row = conn.execute(
                    "SELECT id FROM shopify_order WHERE shopify_account_id = ? AND shopify_order_id = ?",
                    (acct_id, order_node["legacyResourceId"])
                ).fetchone()
                if order_row:
                    order_local_id = order_row["id"]

            reason_details = node.get("reasonDetails", {})
            reason = reason_details.get("reason", "") if isinstance(reason_details, dict) else ""

            # Map Shopify status to our CHECK constraint values
            raw_status = (node.get("status", "NEEDS_RESPONSE") or "NEEDS_RESPONSE").lower()
            status_map = {
                "needs_response": "needs_response",
                "under_review": "under_review",
                "charge_refunded": "charge_refunded",
                "accepted": "accepted",
                "won": "won",
                "lost": "lost",
            }
            mapped_status = status_map.get(raw_status, "needs_response")

            sql, _ = insert_row("shopify_dispute", {
                "id": P(), "shopify_account_id": P(), "shopify_dispute_id": P(),
                "shopify_order_id_local": P(), "dispute_type": P(),
                "status": P(), "amount": P(), "reason": P(),
                "evidence_due_by": P(), "gl_status": P(),
                "company_id": P(), "created_at": P(),
            })
            conn.execute(sql, (
                dispute_local_id, acct_id, shopify_dispute_id,
                order_local_id, node.get("type", ""),
                mapped_status,
                str(_safe_money(node.get("amount"))),
                reason,
                node.get("evidenceDueBy", ""),
                "pending",
                company_id, now,
            ))
            total += 1

        conn.commit()

        page_info = disputes_data.get("pageInfo", {})
        if not page_info.get("hasNextPage", False) or not edges:
            break

    return total


# ---------------------------------------------------------------------------
# Handler dispatch table
# ---------------------------------------------------------------------------

_SYNC_HANDLERS = {
    "orders": _sync_orders,
    "products": _sync_products,
    "customers": _sync_customers,
    "payouts": _sync_payouts,
    "disputes": _sync_disputes,
}


# ---------------------------------------------------------------------------
# Internal: generic sync orchestrator
# ---------------------------------------------------------------------------

def _sync_object_type(conn, shopify_account_id, company_id, sync_type,
                      sync_mode="incremental"):
    """Generic paginated sync from Shopify GraphQL API.

    1. Create shopify_sync_job record (status='running')
    2. Get shopify client via get_shopify_client()
    3. Call the sync-type-specific handler
    4. Update sync_job with counts
    5. Set sync_job status='completed' or 'failed'

    Returns (sync_job_id, records_processed).
    """
    job_id = _create_sync_job(
        conn, shopify_account_id, company_id, sync_type, sync_mode,
    )

    client = get_shopify_client(conn, shopify_account_id)
    if not client:
        _fail_sync_job(conn, job_id, "Could not initialize Shopify client")
        return job_id, 0

    handler = _SYNC_HANDLERS.get(sync_type)
    if not handler:
        _fail_sync_job(conn, job_id, f"Unknown sync type: {sync_type}")
        return job_id, 0

    try:
        result = handler(conn, client, shopify_account_id, company_id, job_id)

        # Handle handlers that return tuple (total, created, updated) vs single int
        if isinstance(result, tuple):
            total, created_count, updated_count = result
            _complete_sync_job(conn, job_id, total, created_count, updated_count)
        else:
            _complete_sync_job(conn, job_id, result)
            total = result

        # Update last sync timestamp on the shopify_account
        sync_field_map = {
            "orders": "last_orders_sync_at",
            "products": "last_products_sync_at",
            "customers": "last_customers_sync_at",
            "payouts": "last_payouts_sync_at",
            "disputes": "last_disputes_sync_at",
        }
        sync_field = sync_field_map.get(sync_type)
        if sync_field:
            sql, params = dynamic_update("shopify_account", {
                sync_field: now_iso(),
                "updated_at": now_iso(),
            }, {"id": shopify_account_id})
            conn.execute(sql, params)
            conn.commit()

        return job_id, total
    except Exception as e:
        _fail_sync_job(conn, job_id, str(e))
        return job_id, 0


# ---------------------------------------------------------------------------
# Webhook event dispatch table — maps Shopify webhook topics to sync types
# ---------------------------------------------------------------------------

_WEBHOOK_TOPIC_MAP = {
    "orders/create": "orders",
    "orders/updated": "orders",
    "orders/paid": "orders",
    "orders/fulfilled": "orders",
    "orders/cancelled": "orders",
    "refunds/create": "orders",
    "products/create": "products",
    "products/update": "products",
    "products/delete": "products",
    "customers/create": "customers",
    "customers/update": "customers",
    "customers/delete": "customers",
    "disputes/create": "disputes",
    "disputes/update": "disputes",
    "payouts/create": "payouts",
    "payouts/update": "payouts",
}


# ===========================================================================
# PUBLIC ACTIONS
# ===========================================================================


# ---------------------------------------------------------------------------
# 1. shopify-sync-orders
# ---------------------------------------------------------------------------
def sync_orders(conn, args):
    """Sync orders (with line items and refunds) from Shopify."""
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)
    company_id = acct_row["company_id"]

    sync_mode = getattr(args, "sync_mode", None) or "incremental"

    job_id, count = _sync_object_type(
        conn, shopify_account_id, company_id, "orders", sync_mode,
    )

    audit(conn, SKILL, "shopify-sync-orders", "shopify_sync_job", job_id,
          new_values={"sync_type": "orders", "sync_mode": sync_mode})
    conn.commit()

    ok({
        "sync_job_id": job_id,
        "sync_type": "orders",
        "sync_mode": sync_mode,
        "records_processed": count,
    })


# ---------------------------------------------------------------------------
# 2. shopify-sync-products
# ---------------------------------------------------------------------------
def sync_products(conn, args):
    """Sync products and variants from Shopify. Auto-maps by SKU."""
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)
    company_id = acct_row["company_id"]

    sync_mode = getattr(args, "sync_mode", None) or "incremental"

    job_id, count = _sync_object_type(
        conn, shopify_account_id, company_id, "products", sync_mode,
    )

    audit(conn, SKILL, "shopify-sync-products", "shopify_sync_job", job_id,
          new_values={"sync_type": "products", "sync_mode": sync_mode})
    conn.commit()

    ok({
        "sync_job_id": job_id,
        "sync_type": "products",
        "sync_mode": sync_mode,
        "records_processed": count,
    })


# ---------------------------------------------------------------------------
# 3. shopify-sync-customers
# ---------------------------------------------------------------------------
def sync_customers(conn, args):
    """Sync customers from Shopify. Auto-matches by name to erpclaw customers."""
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)
    company_id = acct_row["company_id"]

    sync_mode = getattr(args, "sync_mode", None) or "incremental"

    job_id, count = _sync_object_type(
        conn, shopify_account_id, company_id, "customers", sync_mode,
    )

    audit(conn, SKILL, "shopify-sync-customers", "shopify_sync_job", job_id,
          new_values={"sync_type": "customers", "sync_mode": sync_mode})
    conn.commit()

    ok({
        "sync_job_id": job_id,
        "sync_type": "customers",
        "sync_mode": sync_mode,
        "records_processed": count,
    })


# ---------------------------------------------------------------------------
# 4. shopify-sync-payouts
# ---------------------------------------------------------------------------
def sync_payouts(conn, args):
    """Sync Shopify Payments payouts with summary breakdown."""
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)
    company_id = acct_row["company_id"]

    sync_mode = getattr(args, "sync_mode", None) or "incremental"

    job_id, count = _sync_object_type(
        conn, shopify_account_id, company_id, "payouts", sync_mode,
    )

    audit(conn, SKILL, "shopify-sync-payouts", "shopify_sync_job", job_id,
          new_values={"sync_type": "payouts", "sync_mode": sync_mode})
    conn.commit()

    ok({
        "sync_job_id": job_id,
        "sync_type": "payouts",
        "sync_mode": sync_mode,
        "records_processed": count,
    })


# ---------------------------------------------------------------------------
# 5. shopify-sync-disputes
# ---------------------------------------------------------------------------
def sync_disputes(conn, args):
    """Sync Shopify disputes/chargebacks."""
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)
    company_id = acct_row["company_id"]

    sync_mode = getattr(args, "sync_mode", None) or "incremental"

    job_id, count = _sync_object_type(
        conn, shopify_account_id, company_id, "disputes", sync_mode,
    )

    audit(conn, SKILL, "shopify-sync-disputes", "shopify_sync_job", job_id,
          new_values={"sync_type": "disputes", "sync_mode": sync_mode})
    conn.commit()

    ok({
        "sync_job_id": job_id,
        "sync_type": "disputes",
        "sync_mode": sync_mode,
        "records_processed": count,
    })


# ---------------------------------------------------------------------------
# 6. shopify-start-full-sync
# ---------------------------------------------------------------------------
def start_full_sync(conn, args):
    """Start a full sync for ALL Shopify object types.

    Syncs in dependency order: customers, products, orders, payouts, disputes.
    Creates one sync_job per object type.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)
    company_id = acct_row["company_id"]

    results = []
    total_records = 0
    for sync_type in FULL_SYNC_ORDER:
        job_id, count = _sync_object_type(
            conn, shopify_account_id, company_id, sync_type, "full",
        )
        results.append({
            "sync_type": sync_type,
            "sync_job_id": job_id,
            "records_processed": count,
        })
        total_records += count

    audit(conn, SKILL, "shopify-start-full-sync", "shopify_account", shopify_account_id,
          new_values={"total_records": total_records, "job_count": len(results)})
    conn.commit()

    ok({
        "shopify_account_id": shopify_account_id,
        "jobs": results,
        "total_records": total_records,
        "job_count": len(results),
    })


# ---------------------------------------------------------------------------
# 7. shopify-get-sync-job
# ---------------------------------------------------------------------------
def get_sync_job(conn, args):
    """Get details of a specific sync job."""
    sync_job_id = getattr(args, "sync_job_id", None)
    if not sync_job_id:
        err("--sync-job-id is required")

    t = Table("shopify_sync_job")
    row = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (sync_job_id,)
    ).fetchone()
    if not row:
        err(f"Sync job {sync_job_id} not found")

    data = row_to_dict(row)
    # Rename 'status' to 'sync_status' to avoid collision with ok() response status
    data["sync_status"] = data.pop("status", None)
    ok(data)


# ---------------------------------------------------------------------------
# 8. shopify-list-sync-jobs
# ---------------------------------------------------------------------------
def list_sync_jobs(conn, args):
    """List sync jobs for a Shopify account with optional filters."""
    shopify_account_id = getattr(args, "shopify_account_id", None)
    if not shopify_account_id:
        err("--shopify-account-id is required")

    t = Table("shopify_sync_job")
    q = Q.from_(t).select("*").where(
        t.shopify_account_id == P()
    ).orderby(t.created_at, order=Order.desc)

    params = [shopify_account_id]

    status = getattr(args, "status", None)
    if status:
        q = q.where(t.status == P())
        params.append(status)

    sync_type = getattr(args, "sync_type", None)
    if sync_type:
        q = q.where(t.sync_type == P())
        params.append(sync_type)

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.limit(limit).offset(offset)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({
        "sync_jobs": rows_to_list(rows),
        "count": len(rows),
    })


# ---------------------------------------------------------------------------
# 9. shopify-cancel-sync-job
# ---------------------------------------------------------------------------
def cancel_sync_job(conn, args):
    """Cancel a running or pending sync job."""
    sync_job_id = getattr(args, "sync_job_id", None)
    if not sync_job_id:
        err("--sync-job-id is required")

    t = Table("shopify_sync_job")
    row = conn.execute(
        Q.from_(t).select(t.id, t.status).where(t.id == P()).get_sql(),
        (sync_job_id,)
    ).fetchone()
    if not row:
        err(f"Sync job {sync_job_id} not found")

    if row["status"] in ("completed", "failed", "cancelled"):
        err(f"Cannot cancel sync job in '{row['status']}' state")

    now = now_iso()
    sql, params = dynamic_update("shopify_sync_job", {
        "status": "cancelled",
        "completed_at": now,
    }, {"id": sync_job_id})
    conn.execute(sql, params)

    audit(conn, SKILL, "shopify-cancel-sync-job", "shopify_sync_job", sync_job_id,
          new_values={"status": "cancelled"})
    conn.commit()

    ok({"sync_job_id": sync_job_id, "status": "cancelled"})


# ---------------------------------------------------------------------------
# 10. shopify-process-webhook
# ---------------------------------------------------------------------------
def process_webhook(conn, args):
    """Process an incoming Shopify webhook event.

    Dispatches to the appropriate sync handler based on the webhook topic.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)
    company_id = acct_row["company_id"]

    topic = getattr(args, "webhook_topic", None)
    if not topic:
        err("--webhook-topic is required (e.g., 'orders/create')")

    webhook_data_raw = getattr(args, "webhook_data", None)
    if not webhook_data_raw:
        err("--webhook-data is required (JSON string of the webhook payload)")

    try:
        payload = json.loads(webhook_data_raw) if isinstance(webhook_data_raw, str) else webhook_data_raw
    except json.JSONDecodeError:
        err("--webhook-data must be valid JSON")

    # Dispatch to appropriate handler
    sync_type = _WEBHOOK_TOPIC_MAP.get(topic)
    processed = False
    error_msg = None
    job_id = None
    count = 0

    if sync_type:
        try:
            job_id, count = _sync_object_type(
                conn, shopify_account_id, company_id,
                sync_type, "incremental",
            )
            processed = True
        except Exception as e:
            error_msg = str(e)
    else:
        # Unknown topic -- nothing to do
        processed = True

    audit(conn, SKILL, "shopify-process-webhook", "shopify_account", shopify_account_id,
          new_values={"topic": topic, "processed": processed})
    conn.commit()

    ok({
        "topic": topic,
        "processed": processed,
        "sync_type": sync_type,
        "sync_job_id": job_id,
        "records_processed": count,
        "error": error_msg,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "shopify-sync-orders": sync_orders,
    "shopify-sync-products": sync_products,
    "shopify-sync-customers": sync_customers,
    "shopify-sync-payouts": sync_payouts,
    "shopify-sync-disputes": sync_disputes,
    "shopify-start-full-sync": start_full_sync,
    "shopify-get-sync-job": get_sync_job,
    "shopify-list-sync-jobs": list_sync_jobs,
    "shopify-cancel-sync-job": cancel_sync_job,
    "shopify-process-webhook": process_webhook,
}
