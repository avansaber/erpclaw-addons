"""ERPClaw Integrations Shopify -- browse/read actions.

10 actions for browsing synced Shopify data from local mirror tables.
All reads are local (no Shopify API calls).

Imported by db_query.py (unified router).
"""
import os
import sys
from decimal import Decimal

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.response import ok, err, row_to_dict, rows_to_list
    from erpclaw_lib.query import (
        Q, P, Table, Field, fn, Order,
    )
except ImportError:
    pass

# Add scripts directory to path for sibling imports
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from shopify_helpers import SKILL, validate_shopify_account


# ---------------------------------------------------------------------------
# 1. shopify-list-orders
# ---------------------------------------------------------------------------
def list_orders(conn, args):
    """List synced Shopify orders with filters.

    Filters: --shopify-account-id (required), --date-from, --date-to,
    --financial-status, --gl-status.  Paginated via --limit and --offset.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    if not shopify_account_id:
        err("--shopify-account-id is required")

    t = Table("shopify_order")
    q = Q.from_(t).select(
        t.id, t.shopify_order_id, t.shopify_order_number,
        t.order_date, t.financial_status, t.fulfillment_status,
        t.currency, t.subtotal_amount, t.shipping_amount,
        t.tax_amount, t.discount_amount, t.total_amount,
        t.refunded_amount, t.gl_status, t.is_gift_card_order,
        t.has_refunds, t.payment_gateway,
    ).where(t.shopify_account_id == P())
    params = [shopify_account_id]

    date_from = getattr(args, "date_from", None)
    if date_from:
        q = q.where(t.order_date >= P())
        params.append(date_from)

    date_to = getattr(args, "date_to", None)
    if date_to:
        q = q.where(t.order_date <= P())
        params.append(date_to)

    financial_status = getattr(args, "financial_status", None)
    if financial_status:
        q = q.where(t.financial_status == P())
        params.append(financial_status)

    gl_status = getattr(args, "gl_status", None)
    if gl_status:
        q = q.where(t.gl_status == P())
        params.append(gl_status)

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.orderby(t.order_date, order=Order.desc).limit(P()).offset(P())
    params.extend([limit, offset])

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    orders = [row_to_dict(r) for r in rows]
    ok({"orders": orders, "count": len(orders), "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# 2. shopify-get-order
# ---------------------------------------------------------------------------
def get_order(conn, args):
    """Get a single Shopify order with line items, refunds, and GL entries."""
    order_id = getattr(args, "shopify_order_id_local", None)
    if not order_id:
        err("--shopify-order-id-local is required (internal UUID)")

    t = Table("shopify_order")
    row = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (order_id,)
    ).fetchone()
    if not row:
        err(f"Shopify order {order_id} not found")

    result = row_to_dict(row)

    # Line items
    li = Table("shopify_order_line_item")
    line_items = conn.execute(
        Q.from_(li).select("*")
        .where(li.shopify_order_id_local == P())
        .orderby(li.created_at)
        .get_sql(),
        (order_id,)
    ).fetchall()
    result["line_items"] = [row_to_dict(r) for r in line_items]

    # Refunds
    ref = Table("shopify_refund")
    refunds = conn.execute(
        Q.from_(ref).select("*")
        .where(ref.shopify_order_id_local == P())
        .orderby(ref.created_at)
        .get_sql(),
        (order_id,)
    ).fetchall()
    result["refunds"] = [row_to_dict(r) for r in refunds]

    # GL entries for this order
    gl = Table("gl_entry")
    gl_entries = conn.execute(
        Q.from_(gl).select(
            gl.id, gl.posting_date, gl.account_id,
            gl.debit_amount, gl.credit_amount, gl.voucher_id,
        )
        .where(gl.voucher_id == P())
        .orderby(gl.posting_date)
        .get_sql(),
        (row["gl_voucher_id"],)
    ).fetchall() if row["gl_voucher_id"] else []
    result["gl_entries"] = [row_to_dict(r) for r in gl_entries]

    ok(result)


# ---------------------------------------------------------------------------
# 3. shopify-list-refunds
# ---------------------------------------------------------------------------
def list_refunds(conn, args):
    """List synced Shopify refunds with filters.

    Filters: --shopify-account-id (required), --date-from, --date-to,
    --gl-status.  Paginated.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    if not shopify_account_id:
        err("--shopify-account-id is required")

    # Refunds are linked via shopify_order, so we join
    ref = Table("shopify_refund")
    o = Table("shopify_order")
    q = (Q.from_(ref)
         .join(o).on(ref.shopify_order_id_local == o.id)
         .select(
             ref.id, ref.shopify_refund_id, ref.refund_date,
             ref.refund_amount, ref.tax_refund_amount,
             ref.shipping_refund_amount, ref.refund_type,
             ref.gl_status, ref.gl_voucher_id, ref.credit_note_id,
             o.shopify_order_number, o.shopify_order_id,
         )
         .where(o.shopify_account_id == P()))
    params = [shopify_account_id]

    date_from = getattr(args, "date_from", None)
    if date_from:
        q = q.where(ref.refund_date >= P())
        params.append(date_from)

    date_to = getattr(args, "date_to", None)
    if date_to:
        q = q.where(ref.refund_date <= P())
        params.append(date_to)

    gl_status = getattr(args, "gl_status", None)
    if gl_status:
        q = q.where(ref.gl_status == P())
        params.append(gl_status)

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.orderby(ref.refund_date, order=Order.desc).limit(P()).offset(P())
    params.extend([limit, offset])

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    refunds = [row_to_dict(r) for r in rows]
    ok({"refunds": refunds, "count": len(refunds), "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# 4. shopify-get-refund
# ---------------------------------------------------------------------------
def get_refund(conn, args):
    """Get a single Shopify refund with line items."""
    refund_id = getattr(args, "shopify_refund_id_local", None)
    if not refund_id:
        err("--shopify-refund-id-local is required (internal UUID)")

    ref = Table("shopify_refund")
    row = conn.execute(
        Q.from_(ref).select("*").where(ref.id == P()).get_sql(),
        (refund_id,)
    ).fetchone()
    if not row:
        err(f"Shopify refund {refund_id} not found")

    result = row_to_dict(row)

    # Refund line items
    rli = Table("shopify_refund_line_item")
    line_items = conn.execute(
        Q.from_(rli).select("*")
        .where(rli.shopify_refund_id_local == P())
        .orderby(rli.created_at)
        .get_sql(),
        (refund_id,)
    ).fetchall()
    result["line_items"] = [row_to_dict(r) for r in line_items]

    ok(result)


# ---------------------------------------------------------------------------
# 5. shopify-list-payouts
# ---------------------------------------------------------------------------
def list_payouts(conn, args):
    """List synced Shopify payouts with filters.

    Filters: --shopify-account-id (required), --status, --date-from,
    --date-to.  Paginated.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    if not shopify_account_id:
        err("--shopify-account-id is required")

    t = Table("shopify_payout")
    q = Q.from_(t).select(
        t.id, t.shopify_payout_id, t.issued_at, t.status,
        t.gross_amount, t.fee_amount, t.net_amount,
        t.gl_status, t.reconciliation_status,
    ).where(t.shopify_account_id == P())
    params = [shopify_account_id]

    status = getattr(args, "payout_status", None)
    if status:
        q = q.where(t.status == P())
        params.append(status)

    date_from = getattr(args, "date_from", None)
    if date_from:
        q = q.where(t.issued_at >= P())
        params.append(date_from)

    date_to = getattr(args, "date_to", None)
    if date_to:
        q = q.where(t.issued_at <= P())
        params.append(date_to)

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.orderby(t.issued_at, order=Order.desc).limit(P()).offset(P())
    params.extend([limit, offset])

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    payouts = [row_to_dict(r) for r in rows]
    ok({"payouts": payouts, "count": len(payouts), "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# 6. shopify-get-payout
# ---------------------------------------------------------------------------
def get_payout(conn, args):
    """Get a single Shopify payout with all constituent transactions."""
    payout_id = getattr(args, "shopify_payout_id", None)
    if not payout_id:
        err("--shopify-payout-id is required (local UUID)")

    t = Table("shopify_payout")
    row = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (payout_id,)
    ).fetchone()
    if not row:
        err(f"Shopify payout {payout_id} not found")

    result = row_to_dict(row)

    # Payout transactions
    ptx = Table("shopify_payout_transaction")
    txns = conn.execute(
        Q.from_(ptx).select("*")
        .where(ptx.shopify_payout_id_local == P())
        .orderby(ptx.processed_at)
        .get_sql(),
        (payout_id,)
    ).fetchall()
    result["transactions"] = [row_to_dict(r) for r in txns]
    result["transaction_count"] = len(txns)

    ok(result)


# ---------------------------------------------------------------------------
# 7. shopify-list-payout-transactions
# ---------------------------------------------------------------------------
def list_payout_transactions(conn, args):
    """List transactions for a specific Shopify payout."""
    payout_id = getattr(args, "shopify_payout_id", None)
    if not payout_id:
        err("--shopify-payout-id is required (local UUID)")

    ptx = Table("shopify_payout_transaction")
    q = (Q.from_(ptx).select("*")
         .where(ptx.shopify_payout_id_local == P())
         .orderby(ptx.processed_at))
    rows = conn.execute(q.get_sql(), (payout_id,)).fetchall()
    txns = [row_to_dict(r) for r in rows]
    ok({"transactions": txns, "count": len(txns)})


# ---------------------------------------------------------------------------
# 8. shopify-list-disputes
# ---------------------------------------------------------------------------
def list_disputes(conn, args):
    """List synced Shopify disputes with filters.

    Filters: --shopify-account-id (required), --status.  Paginated.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    if not shopify_account_id:
        err("--shopify-account-id is required")

    t = Table("shopify_dispute")
    q = Q.from_(t).select(
        t.id, t.shopify_dispute_id, t.dispute_type, t.status,
        t.amount, t.fee_amount, t.reason, t.evidence_due_by,
        t.gl_status, t.shopify_order_id_local,
    ).where(t.shopify_account_id == P())
    params = [shopify_account_id]

    dispute_status = getattr(args, "dispute_status", None)
    if dispute_status:
        q = q.where(t.status == P())
        params.append(dispute_status)

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    params.extend([limit, offset])

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    disputes = [row_to_dict(r) for r in rows]
    ok({"disputes": disputes, "count": len(disputes), "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# 9. shopify-get-dispute
# ---------------------------------------------------------------------------
def get_dispute(conn, args):
    """Get a single Shopify dispute with full details."""
    dispute_id = getattr(args, "shopify_dispute_id_local", None)
    if not dispute_id:
        err("--shopify-dispute-id-local is required (internal UUID)")

    t = Table("shopify_dispute")
    row = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (dispute_id,)
    ).fetchone()
    if not row:
        err(f"Shopify dispute {dispute_id} not found")

    result = row_to_dict(row)

    # If linked to an order, fetch order details
    if row["shopify_order_id_local"]:
        o = Table("shopify_order")
        order_row = conn.execute(
            Q.from_(o).select(o.shopify_order_number, o.total_amount, o.order_date)
            .where(o.id == P()).get_sql(),
            (row["shopify_order_id_local"],)
        ).fetchone()
        if order_row:
            result["linked_order"] = row_to_dict(order_row)

    ok(result)


# ---------------------------------------------------------------------------
# 10. shopify-order-gl-detail
# ---------------------------------------------------------------------------
def order_gl_detail(conn, args):
    """Show GL entries posted for a specific Shopify order.

    Displays the journal entry and all debit/credit entries with account names.
    """
    order_id = getattr(args, "shopify_order_id_local", None)
    if not order_id:
        err("--shopify-order-id-local is required (internal UUID)")

    t = Table("shopify_order")
    row = conn.execute(
        Q.from_(t).select(
            t.id, t.shopify_order_number, t.total_amount,
            t.gl_status, t.gl_voucher_id,
        ).where(t.id == P()).get_sql(),
        (order_id,)
    ).fetchone()
    if not row:
        err(f"Shopify order {order_id} not found")

    result = row_to_dict(row)

    if not row["gl_voucher_id"]:
        result["gl_entries"] = []
        result["message"] = "No GL entries posted for this order"
        ok(result)
        return

    # Get GL entries with account names
    gl = Table("gl_entry")
    acct = Table("account")
    gl_rows = conn.execute(
        Q.from_(gl)
        .left_join(acct).on(gl.account_id == acct.id)
        .select(
            gl.id, gl.posting_date, gl.account_id,
            acct.name.as_("account_name"),
            gl.debit_amount, gl.credit_amount,
        )
        .where(gl.voucher_id == P())
        .orderby(gl.id)
        .get_sql(),
        (row["gl_voucher_id"],)
    ).fetchall()

    result["gl_entries"] = [row_to_dict(r) for r in gl_rows]
    result["entry_count"] = len(gl_rows)

    # Summarize totals
    total_debit = Decimal("0")
    total_credit = Decimal("0")
    for e in gl_rows:
        total_debit += Decimal(str(e["debit_amount"] or "0"))
        total_credit += Decimal(str(e["credit_amount"] or "0"))
    result["total_debit"] = str(total_debit)
    result["total_credit"] = str(total_credit)
    result["balanced"] = total_debit == total_credit

    ok(result)


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "shopify-list-orders": list_orders,
    "shopify-get-order": get_order,
    "shopify-list-refunds": list_refunds,
    "shopify-get-refund": get_refund,
    "shopify-list-payouts": list_payouts,
    "shopify-get-payout": get_payout,
    "shopify-list-payout-transactions": list_payout_transactions,
    "shopify-list-disputes": list_disputes,
    "shopify-get-dispute": get_dispute,
    "shopify-order-gl-detail": order_gl_detail,
}
