"""ERPClaw Integrations Shopify -- reporting actions.

7 actions for Shopify analytics and health reporting.
All reads are local (no Shopify API calls).

Imported by db_query.py (unified router).
"""
import os
import sys
from decimal import Decimal, ROUND_HALF_UP

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.decimal_utils import to_decimal, round_currency, amounts_equal
    from erpclaw_lib.gl_posting import get_account_balance
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.query import (
        Q, P, Table, Field, fn, Order,
    )
except ImportError:
    pass

# Add scripts directory to path for sibling imports
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from shopify_helpers import (
    SKILL, validate_shopify_account, shopify_amount_to_decimal,
)


def _period_filter(q, date_field, args, params):
    """Apply optional --period (YYYY-MM) or --date-from / --date-to filters."""
    period = getattr(args, "period", None)
    if period:
        # YYYY-MM format => filter date_field starting with that prefix
        q = q.where(date_field >= P())
        params.append(f"{period}-01")
        q = q.where(date_field <= P())
        params.append(f"{period}-31")
        return q

    date_from = getattr(args, "date_from", None)
    if date_from:
        q = q.where(date_field >= P())
        params.append(date_from)

    date_to = getattr(args, "date_to", None)
    if date_to:
        q = q.where(date_field <= P())
        params.append(date_to)

    return q


# ---------------------------------------------------------------------------
# 1. shopify-revenue-summary
# ---------------------------------------------------------------------------
def revenue_summary(conn, args):
    """Revenue summary by period: product revenue, shipping, tax, discounts.

    Requires --shopify-account-id. Optional --period (YYYY-MM) or
    --date-from / --date-to.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)

    t = Table("shopify_order")
    q = Q.from_(t).select(
        fn.Count("*").as_("order_count"),
    ).where(t.shopify_account_id == P())
    params = [shopify_account_id]
    q = _period_filter(q, t.order_date, args, params)

    # We need raw SQL for SUMs of TEXT Decimal columns
    rows = conn.execute(
        f"""SELECT
                COUNT(*) AS order_count,
                COALESCE(SUM(CAST(subtotal_amount AS REAL)), 0) AS product_revenue,
                COALESCE(SUM(CAST(shipping_amount AS REAL)), 0) AS shipping_revenue,
                COALESCE(SUM(CAST(tax_amount AS REAL)), 0) AS tax_collected,
                COALESCE(SUM(CAST(discount_amount AS REAL)), 0) AS total_discounts,
                COALESCE(SUM(CAST(total_amount AS REAL)), 0) AS gross_revenue,
                COALESCE(SUM(CAST(refunded_amount AS REAL)), 0) AS total_refunded
            FROM shopify_order
            WHERE shopify_account_id = ?
              AND order_date >= ? AND order_date <= ?""",
        (shopify_account_id,
         getattr(args, "date_from", None) or (getattr(args, "period", None) or "2000-01") + "-01",
         getattr(args, "date_to", None) or (getattr(args, "period", None) or "2099-12") + "-31")
    ).fetchone()

    result = {
        "shopify_account_id": shopify_account_id,
        "order_count": rows["order_count"],
        "product_revenue": str(round_currency(to_decimal(str(rows["product_revenue"])))),
        "shipping_revenue": str(round_currency(to_decimal(str(rows["shipping_revenue"])))),
        "tax_collected": str(round_currency(to_decimal(str(rows["tax_collected"])))),
        "total_discounts": str(round_currency(to_decimal(str(rows["total_discounts"])))),
        "gross_revenue": str(round_currency(to_decimal(str(rows["gross_revenue"])))),
        "total_refunded": str(round_currency(to_decimal(str(rows["total_refunded"])))),
        "net_revenue": str(round_currency(
            to_decimal(str(rows["gross_revenue"])) -
            to_decimal(str(rows["total_refunded"]))
        )),
    }

    # Add period context
    period = getattr(args, "period", None)
    if period:
        result["period"] = period
    ok(result)


# ---------------------------------------------------------------------------
# 2. shopify-fee-summary
# ---------------------------------------------------------------------------
def fee_summary(conn, args):
    """Processing fee summary by period.

    Requires --shopify-account-id. Optional --period or --date-from/--date-to.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)

    row = conn.execute(
        """SELECT
                COUNT(*) AS payout_count,
                COALESCE(SUM(CAST(fee_amount AS REAL)), 0) AS total_fees,
                COALESCE(SUM(CAST(gross_amount AS REAL)), 0) AS total_gross,
                COALESCE(SUM(CAST(net_amount AS REAL)), 0) AS total_net
            FROM shopify_payout
            WHERE shopify_account_id = ?
              AND issued_at >= ? AND issued_at <= ?""",
        (shopify_account_id,
         getattr(args, "date_from", None) or (getattr(args, "period", None) or "2000-01") + "-01",
         getattr(args, "date_to", None) or (getattr(args, "period", None) or "2099-12") + "-31")
    ).fetchone()

    total_gross = to_decimal(str(row["total_gross"]))
    total_fees = to_decimal(str(row["total_fees"]))
    fee_rate = (total_fees / total_gross * Decimal("100")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    ) if total_gross > 0 else Decimal("0")

    ok({
        "shopify_account_id": shopify_account_id,
        "payout_count": row["payout_count"],
        "total_gross": str(round_currency(total_gross)),
        "total_fees": str(round_currency(total_fees)),
        "total_net": str(round_currency(to_decimal(str(row["total_net"])))),
        "effective_fee_rate": str(fee_rate),
    })


# ---------------------------------------------------------------------------
# 3. shopify-refund-summary
# ---------------------------------------------------------------------------
def refund_summary(conn, args):
    """Refund summary by period.

    Requires --shopify-account-id. Optional --period or --date-from/--date-to.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)

    row = conn.execute(
        """SELECT
                COUNT(*) AS refund_count,
                COALESCE(SUM(CAST(r.refund_amount AS REAL)), 0) AS total_refund_amount,
                COALESCE(SUM(CAST(r.tax_refund_amount AS REAL)), 0) AS total_tax_refunded,
                COALESCE(SUM(CAST(r.shipping_refund_amount AS REAL)), 0) AS total_shipping_refunded,
                SUM(CASE WHEN r.refund_type = 'full' THEN 1 ELSE 0 END) AS full_refunds,
                SUM(CASE WHEN r.refund_type = 'partial' THEN 1 ELSE 0 END) AS partial_refunds
            FROM shopify_refund r
            JOIN shopify_order o ON r.shopify_order_id_local = o.id
            WHERE o.shopify_account_id = ?
              AND r.refund_date >= ? AND r.refund_date <= ?""",
        (shopify_account_id,
         getattr(args, "date_from", None) or (getattr(args, "period", None) or "2000-01") + "-01",
         getattr(args, "date_to", None) or (getattr(args, "period", None) or "2099-12") + "-31")
    ).fetchone()

    ok({
        "shopify_account_id": shopify_account_id,
        "refund_count": row["refund_count"],
        "total_refund_amount": str(round_currency(to_decimal(str(row["total_refund_amount"])))),
        "total_tax_refunded": str(round_currency(to_decimal(str(row["total_tax_refunded"])))),
        "total_shipping_refunded": str(round_currency(to_decimal(str(row["total_shipping_refunded"])))),
        "full_refunds": row["full_refunds"] or 0,
        "partial_refunds": row["partial_refunds"] or 0,
    })


# ---------------------------------------------------------------------------
# 4. shopify-payout-detail-report
# ---------------------------------------------------------------------------
def payout_detail_report(conn, args):
    """Detailed payout breakdown for a date range.

    Shows each payout with gross/fee/net and transaction breakdown.
    Requires --shopify-account-id. Optional --date-from/--date-to.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)

    date_from = getattr(args, "date_from", None) or "2000-01-01"
    date_to = getattr(args, "date_to", None) or "2099-12-31"

    payouts = conn.execute(
        """SELECT id, shopify_payout_id, issued_at, status,
                  gross_amount, fee_amount, net_amount,
                  gl_status, reconciliation_status
           FROM shopify_payout
           WHERE shopify_account_id = ?
             AND issued_at >= ? AND issued_at <= ?
           ORDER BY issued_at DESC""",
        (shopify_account_id, date_from, date_to)
    ).fetchall()

    payout_details = []
    running_gross = Decimal("0")
    running_fee = Decimal("0")
    running_net = Decimal("0")

    for p in payouts:
        gross = to_decimal(str(p["gross_amount"]))
        fee = to_decimal(str(p["fee_amount"]))
        net = to_decimal(str(p["net_amount"]))
        running_gross += gross
        running_fee += fee
        running_net += net

        # Transaction breakdown for this payout
        txns = conn.execute(
            """SELECT transaction_type, COUNT(*) as cnt,
                      COALESCE(SUM(CAST(gross_amount AS REAL)), 0) as type_gross,
                      COALESCE(SUM(CAST(fee_amount AS REAL)), 0) as type_fee,
                      COALESCE(SUM(CAST(net_amount AS REAL)), 0) as type_net
               FROM shopify_payout_transaction
               WHERE shopify_payout_id_local = ?
               GROUP BY transaction_type""",
            (p["id"],)
        ).fetchall()

        breakdown = []
        for t in txns:
            breakdown.append({
                "type": t["transaction_type"],
                "count": t["cnt"],
                "gross": str(round_currency(to_decimal(str(t["type_gross"])))),
                "fee": str(round_currency(to_decimal(str(t["type_fee"])))),
                "net": str(round_currency(to_decimal(str(t["type_net"])))),
            })

        payout_details.append({
            "payout_id": p["id"],
            "shopify_payout_id": p["shopify_payout_id"],
            "issued_at": p["issued_at"],
            "status": p["status"],
            "gross": str(round_currency(gross)),
            "fee": str(round_currency(fee)),
            "net": str(round_currency(net)),
            "gl_status": p["gl_status"],
            "reconciliation_status": p["reconciliation_status"],
            "transaction_breakdown": breakdown,
        })

    ok({
        "shopify_account_id": shopify_account_id,
        "date_from": date_from,
        "date_to": date_to,
        "payout_count": len(payout_details),
        "total_gross": str(round_currency(running_gross)),
        "total_fee": str(round_currency(running_fee)),
        "total_net": str(round_currency(running_net)),
        "payouts": payout_details,
    })


# ---------------------------------------------------------------------------
# 5. shopify-product-revenue-report
# ---------------------------------------------------------------------------
def product_revenue_report(conn, args):
    """Revenue by product/SKU.

    Requires --shopify-account-id. Optional --date-from/--date-to.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)

    date_from = getattr(args, "date_from", None) or "2000-01-01"
    date_to = getattr(args, "date_to", None) or "2099-12-31"

    rows = conn.execute(
        """SELECT
                li.sku,
                li.title,
                SUM(li.quantity) AS total_quantity,
                COALESCE(SUM(CAST(li.total_amount AS REAL)), 0) AS total_revenue,
                COALESCE(SUM(CAST(li.discount_amount AS REAL)), 0) AS total_discounts,
                COALESCE(SUM(CAST(li.tax_amount AS REAL)), 0) AS total_tax,
                COUNT(DISTINCT li.shopify_order_id_local) AS order_count
            FROM shopify_order_line_item li
            JOIN shopify_order o ON li.shopify_order_id_local = o.id
            WHERE o.shopify_account_id = ?
              AND o.order_date >= ? AND o.order_date <= ?
            GROUP BY li.sku, li.title
            ORDER BY total_revenue DESC""",
        (shopify_account_id, date_from, date_to)
    ).fetchall()

    products = []
    for r in rows:
        products.append({
            "sku": r["sku"],
            "title": r["title"],
            "total_quantity": r["total_quantity"],
            "total_revenue": str(round_currency(to_decimal(str(r["total_revenue"])))),
            "total_discounts": str(round_currency(to_decimal(str(r["total_discounts"])))),
            "total_tax": str(round_currency(to_decimal(str(r["total_tax"])))),
            "order_count": r["order_count"],
        })

    ok({
        "shopify_account_id": shopify_account_id,
        "product_count": len(products),
        "products": products,
    })


# ---------------------------------------------------------------------------
# 6. shopify-customer-revenue-report
# ---------------------------------------------------------------------------
def customer_revenue_report(conn, args):
    """Revenue by customer.

    Requires --shopify-account-id. Optional --date-from/--date-to.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)

    date_from = getattr(args, "date_from", None) or "2000-01-01"
    date_to = getattr(args, "date_to", None) or "2099-12-31"

    rows = conn.execute(
        """SELECT
                o.customer_id,
                c.name AS customer_name,
                COUNT(*) AS order_count,
                COALESCE(SUM(CAST(o.total_amount AS REAL)), 0) AS total_revenue,
                COALESCE(SUM(CAST(o.refunded_amount AS REAL)), 0) AS total_refunded,
                MIN(o.order_date) AS first_order,
                MAX(o.order_date) AS last_order
            FROM shopify_order o
            LEFT JOIN customer c ON o.customer_id = c.id
            WHERE o.shopify_account_id = ?
              AND o.order_date >= ? AND o.order_date <= ?
              AND o.customer_id IS NOT NULL
            GROUP BY o.customer_id
            ORDER BY total_revenue DESC""",
        (shopify_account_id, date_from, date_to)
    ).fetchall()

    customers = []
    for r in rows:
        total = to_decimal(str(r["total_revenue"]))
        refunded = to_decimal(str(r["total_refunded"]))
        customers.append({
            "customer_id": r["customer_id"],
            "customer_name": r["customer_name"],
            "order_count": r["order_count"],
            "total_revenue": str(round_currency(total)),
            "total_refunded": str(round_currency(refunded)),
            "net_revenue": str(round_currency(total - refunded)),
            "first_order": r["first_order"],
            "last_order": r["last_order"],
        })

    ok({
        "shopify_account_id": shopify_account_id,
        "customer_count": len(customers),
        "customers": customers,
    })


# ---------------------------------------------------------------------------
# 7. shopify-status
# ---------------------------------------------------------------------------
def shopify_status(conn, args):
    """Overall Shopify integration health dashboard.

    Shows sync status, GL status, clearing balance, last sync times,
    and unreconciled count.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)

    # Order stats
    order_stats = conn.execute(
        """SELECT
                COUNT(*) AS total_orders,
                SUM(CASE WHEN gl_status = 'posted' THEN 1 ELSE 0 END) AS posted,
                SUM(CASE WHEN gl_status = 'pending' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN gl_status = 'failed' THEN 1 ELSE 0 END) AS failed,
                COALESCE(SUM(CAST(total_amount AS REAL)), 0) AS total_revenue
            FROM shopify_order
            WHERE shopify_account_id = ?""",
        (shopify_account_id,)
    ).fetchone()

    # Payout stats
    payout_stats = conn.execute(
        """SELECT
                COUNT(*) AS total_payouts,
                SUM(CASE WHEN reconciliation_status = 'unreconciled' THEN 1 ELSE 0 END) AS unreconciled,
                SUM(CASE WHEN reconciliation_status != 'unreconciled' THEN 1 ELSE 0 END) AS reconciled,
                COALESCE(SUM(CAST(net_amount AS REAL)), 0) AS total_net_paid
            FROM shopify_payout
            WHERE shopify_account_id = ?""",
        (shopify_account_id,)
    ).fetchone()

    # Refund stats
    refund_stats = conn.execute(
        """SELECT
                COUNT(*) AS total_refunds,
                COALESCE(SUM(CAST(r.refund_amount AS REAL)), 0) AS total_refunded
            FROM shopify_refund r
            JOIN shopify_order o ON r.shopify_order_id_local = o.id
            WHERE o.shopify_account_id = ?""",
        (shopify_account_id,)
    ).fetchone()

    # Dispute stats
    dispute_stats = conn.execute(
        """SELECT
                COUNT(*) AS total_disputes,
                SUM(CASE WHEN status = 'needs_response' THEN 1 ELSE 0 END) AS needs_response,
                COALESCE(SUM(CAST(amount AS REAL)), 0) AS total_disputed
            FROM shopify_dispute
            WHERE shopify_account_id = ?""",
        (shopify_account_id,)
    ).fetchone()

    # Clearing balance
    clearing_balance = Decimal("0")
    clearing_account_id = acct_row["clearing_account_id"]
    if clearing_account_id:
        try:
            bal = get_account_balance(conn, clearing_account_id)
            clearing_balance = to_decimal(bal["balance"])
        except Exception:
            pass

    # Last reconciliation
    last_recon = conn.execute(
        """SELECT id, run_date, status, discrepancy_amount
           FROM shopify_reconciliation_run
           WHERE shopify_account_id = ?
           ORDER BY created_at DESC LIMIT 1""",
        (shopify_account_id,)
    ).fetchone()

    ok({
        "shopify_account_id": shopify_account_id,
        "shop_name": acct_row["shop_name"],
        "status": acct_row["status"],
        "orders": {
            "total": order_stats["total_orders"],
            "gl_posted": order_stats["posted"] or 0,
            "gl_pending": order_stats["pending"] or 0,
            "gl_failed": order_stats["failed"] or 0,
            "total_revenue": str(round_currency(to_decimal(str(order_stats["total_revenue"])))),
        },
        "payouts": {
            "total": payout_stats["total_payouts"],
            "unreconciled": payout_stats["unreconciled"] or 0,
            "reconciled": payout_stats["reconciled"] or 0,
            "total_net_paid": str(round_currency(to_decimal(str(payout_stats["total_net_paid"])))),
        },
        "refunds": {
            "total": refund_stats["total_refunds"],
            "total_refunded": str(round_currency(to_decimal(str(refund_stats["total_refunded"])))),
        },
        "disputes": {
            "total": dispute_stats["total_disputes"],
            "needs_response": dispute_stats["needs_response"] or 0,
            "total_disputed": str(round_currency(to_decimal(str(dispute_stats["total_disputed"])))),
        },
        "clearing_balance": str(round_currency(clearing_balance)),
        "clearing_is_zero": amounts_equal(clearing_balance, Decimal("0")),
        "last_sync": {
            "orders": acct_row["last_orders_sync_at"],
            "products": acct_row["last_products_sync_at"],
            "customers": acct_row["last_customers_sync_at"],
            "payouts": acct_row["last_payouts_sync_at"],
            "disputes": acct_row["last_disputes_sync_at"],
        },
        "last_reconciliation": {
            "id": last_recon["id"] if last_recon else None,
            "date": last_recon["run_date"] if last_recon else None,
            "status": last_recon["status"] if last_recon else None,
            "discrepancy": last_recon["discrepancy_amount"] if last_recon else None,
        } if last_recon else None,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "shopify-revenue-summary": revenue_summary,
    "shopify-fee-summary": fee_summary,
    "shopify-refund-summary": refund_summary,
    "shopify-payout-detail-report": payout_detail_report,
    "shopify-product-revenue-report": product_revenue_report,
    "shopify-customer-revenue-report": customer_revenue_report,
    "shopify-status": shopify_status,
}
