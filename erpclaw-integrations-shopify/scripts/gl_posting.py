"""ERPClaw Integrations Shopify -- GL posting actions.

8 actions for posting Shopify transactions to the General Ledger.
Each action follows the canonical GL posting pattern:
  1. Read Shopify object + resolved GL accounts from shopify_account
  2. Build GL entry list
  3. Create journal_entry (voucher document)
  4. Call erpclaw_lib.gl_posting.insert_gl_entries()
  5. Update Shopify object with gl_voucher_id
  6. Article 6: NEVER direct INSERT to gl_entry

Imported by db_query.py (unified router).
"""
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.gl_posting import insert_gl_entries, reverse_gl_entries
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
    SKILL, now_iso, shopify_amount_to_decimal, validate_shopify_account,
)


# ---------------------------------------------------------------------------
# Internal: journal entry creation
# ---------------------------------------------------------------------------

def _create_journal_entry(conn, company_id, posting_date, total_debit,
                          remark, entry_type="journal"):
    """Create a journal_entry voucher document and return its ID.

    The journal_entry is the voucher document that gl_entry rows reference.
    Status is set to 'submitted' since GL entries are posted atomically.
    """
    je_id = str(uuid.uuid4())
    now = now_iso()
    sql, _ = insert_row("journal_entry", {
        "id": P(), "posting_date": P(), "entry_type": P(),
        "total_debit": P(), "total_credit": P(),
        "currency": P(), "exchange_rate": P(),
        "remark": P(), "status": P(),
        "company_id": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        je_id, posting_date, entry_type,
        str(total_debit), str(total_debit),
        "USD", "1",
        remark, "submitted",
        company_id, now, now,
    ))
    return je_id


def _resolve_posting_date(order_date):
    """Extract a YYYY-MM-DD posting date from a Shopify ISO datetime string."""
    if not order_date:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # Handle both 'YYYY-MM-DDTHH:MM:SSZ' and 'YYYY-MM-DD'
    return order_date[:10]


def _get_cost_center(conn, company_id):
    """Get the default (first non-group) cost center for a company."""
    row = conn.execute(
        "SELECT id FROM cost_center WHERE company_id = ? AND is_group = 0 LIMIT 1",
        (company_id,)
    ).fetchone()
    return row["id"] if row else None


def _build_gl_entry(account_id, debit, credit, cost_center_id=None):
    """Build a single GL entry dict."""
    return {
        "account_id": account_id,
        "debit": str(round_currency(to_decimal(str(debit)))),
        "credit": str(round_currency(to_decimal(str(credit)))),
        "cost_center_id": cost_center_id,
    }


def _get_item_valuation(conn, item_id, quantity, company_id):
    """Get the valuation amount for COGS posting.

    Looks up the item's current valuation_rate from stock_ledger_entry
    (moving average). Falls back to item.last_purchase_rate if no SLE.
    """
    if not item_id:
        return Decimal("0")

    # Try latest stock_ledger_entry for valuation_rate
    sle = conn.execute(
        """SELECT valuation_rate FROM stock_ledger_entry
           WHERE item_id = ? AND is_cancelled = 0
           ORDER BY posting_date DESC, created_at DESC LIMIT 1""",
        (item_id,)
    ).fetchone()
    if sle and sle["valuation_rate"]:
        rate = to_decimal(str(sle["valuation_rate"]))
        return round_currency(rate * to_decimal(str(quantity)))

    # Fall back to item.last_purchase_rate
    item = conn.execute(
        "SELECT last_purchase_rate FROM item WHERE id = ?", (item_id,)
    ).fetchone()
    if item and item["last_purchase_rate"]:
        rate = to_decimal(str(item["last_purchase_rate"]))
        return round_currency(rate * to_decimal(str(quantity)))

    return Decimal("0")


# ---------------------------------------------------------------------------
# 1. shopify-post-order-gl
# ---------------------------------------------------------------------------
def post_order_gl(conn, args):
    """Post GL entries for a Shopify order.

    Revenue entry: DR Clearing, CR Revenue + Shipping Revenue + Tax Payable.
    If track_cogs enabled: DR COGS, CR Inventory.
    If discount_method='gross': separate DR Sales Discounts line.
    """
    shopify_order_id = getattr(args, "shopify_order_id", None)
    if not shopify_order_id:
        err("--shopify-order-id is required (local UUID)")

    t_order = Table("shopify_order")
    order = conn.execute(
        Q.from_(t_order).select("*").where(t_order.id == P()).get_sql(),
        (shopify_order_id,)
    ).fetchone()
    if not order:
        err(f"Shopify order {shopify_order_id} not found")

    if order["gl_status"] == "posted":
        err(f"Order {shopify_order_id} already has GL entries posted")

    acct_row = validate_shopify_account(conn, order["shopify_account_id"])
    company_id = acct_row["company_id"]
    cc_id = _get_cost_center(conn, company_id)
    posting_date = _resolve_posting_date(order["order_date"])

    # Amounts
    subtotal = shopify_amount_to_decimal(order["subtotal_amount"])
    shipping = shopify_amount_to_decimal(order["shipping_amount"])
    tax = shopify_amount_to_decimal(order["tax_amount"])
    discount = shopify_amount_to_decimal(order["discount_amount"])
    total = shopify_amount_to_decimal(order["total_amount"])

    discount_method = acct_row["discount_method"]

    # Build GL entries
    entries = []

    # DR Clearing for total amount
    entries.append(_build_gl_entry(acct_row["clearing_account_id"], total, 0))

    # CR Revenue
    if discount_method == "net":
        # Net method: revenue is subtotal minus discount
        revenue_credit = subtotal - discount
    else:
        # Gross method: revenue is full subtotal, discount is separate debit
        revenue_credit = subtotal

    if revenue_credit > Decimal("0"):
        entries.append(_build_gl_entry(
            acct_row["revenue_account_id"], 0, revenue_credit, cc_id))

    # DR Sales Discounts (only in gross method)
    if discount_method == "gross" and discount > Decimal("0"):
        entries.append(_build_gl_entry(
            acct_row["discount_account_id"], discount, 0, cc_id))

    # CR Shipping Revenue
    if shipping > Decimal("0"):
        entries.append(_build_gl_entry(
            acct_row["shipping_revenue_account_id"], 0, shipping, cc_id))

    # CR Tax Payable
    if tax > Decimal("0"):
        entries.append(_build_gl_entry(
            acct_row["tax_payable_account_id"], 0, tax))

    # Create journal entry voucher
    je_id = _create_journal_entry(
        conn, company_id, posting_date, total,
        f"Shopify order {order['shopify_order_id']} GL posting",
    )

    # Post GL entries via erpclaw_lib
    gl_ids = insert_gl_entries(
        conn, entries,
        voucher_type="journal_entry",
        voucher_id=je_id,
        posting_date=posting_date,
        company_id=company_id,
        remarks=f"Shopify order {order['shopify_order_id']}",
    )

    # COGS entries (separate entry_set)
    cogs_total = Decimal("0")
    if acct_row["track_cogs"]:
        cogs_entries = []
        # Get line items
        line_items = conn.execute(
            "SELECT * FROM shopify_order_line_item WHERE shopify_order_id_local = ?",
            (shopify_order_id,)
        ).fetchall()

        for li in line_items:
            if li["item_id"] and not li["is_gift_card"]:
                val = _get_item_valuation(
                    conn, li["item_id"], li["quantity"], company_id)
                if val > Decimal("0"):
                    cogs_total += val

        if cogs_total > Decimal("0"):
            cogs_entries.append(_build_gl_entry(
                acct_row["cogs_account_id"], cogs_total, 0, cc_id))
            cogs_entries.append(_build_gl_entry(
                acct_row["inventory_account_id"], 0, cogs_total))

            cogs_gl_ids = insert_gl_entries(
                conn, cogs_entries,
                voucher_type="journal_entry",
                voucher_id=je_id,
                posting_date=posting_date,
                company_id=company_id,
                remarks=f"Shopify order {order['shopify_order_id']} COGS",
                entry_set="cogs",
            )
            gl_ids.extend(cogs_gl_ids)

    # Update order with GL status
    sql, params = dynamic_update("shopify_order", {
        "gl_status": "posted",
        "gl_voucher_id": je_id,
        "updated_at": now_iso(),
    }, {"id": shopify_order_id})
    conn.execute(sql, params)

    audit(conn, SKILL, "shopify-post-order-gl", "shopify_order", shopify_order_id,
          new_values={"gl_voucher_id": je_id, "gl_status": "posted"})
    conn.commit()

    ok({
        "shopify_order_id": shopify_order_id,
        "journal_entry_id": je_id,
        "gl_entry_count": len(gl_ids),
        "gl_entry_ids": gl_ids,
        "total_amount": str(total),
        "cogs_amount": str(cogs_total),
        "discount_method": discount_method,
    })


# ---------------------------------------------------------------------------
# 2. shopify-post-refund-gl
# ---------------------------------------------------------------------------
def post_refund_gl(conn, args):
    """Post GL entries for a Shopify refund.

    Reversal: DR Revenue/Returns + Tax + Shipping, CR Clearing.
    If stock returned (restock_type='return'): DR Inventory, CR COGS.
    """
    shopify_refund_id = getattr(args, "shopify_refund_id", None)
    if not shopify_refund_id:
        err("--shopify-refund-id is required (local UUID)")

    refund = conn.execute(
        "SELECT * FROM shopify_refund WHERE id = ?",
        (shopify_refund_id,)
    ).fetchone()
    if not refund:
        err(f"Shopify refund {shopify_refund_id} not found")

    if refund["gl_status"] == "posted":
        err(f"Refund {shopify_refund_id} already has GL entries posted")

    # Get parent order and account
    order = conn.execute(
        "SELECT * FROM shopify_order WHERE id = ?",
        (refund["shopify_order_id_local"],)
    ).fetchone()
    if not order:
        err(f"Parent order not found for refund {shopify_refund_id}")

    acct_row = validate_shopify_account(conn, order["shopify_account_id"])
    company_id = acct_row["company_id"]
    cc_id = _get_cost_center(conn, company_id)
    posting_date = _resolve_posting_date(refund["refund_date"])

    refund_amount = shopify_amount_to_decimal(refund["refund_amount"])
    tax_refund = shopify_amount_to_decimal(
        refund["tax_refund_amount"] if refund["tax_refund_amount"] else "0")
    shipping_refund = shopify_amount_to_decimal(
        refund["shipping_refund_amount"] if refund["shipping_refund_amount"] else "0")

    # Revenue portion is refund minus tax and shipping
    revenue_refund = refund_amount - tax_refund - shipping_refund

    entries = []

    # DR Revenue (refund/returns)
    if revenue_refund > Decimal("0"):
        entries.append(_build_gl_entry(
            acct_row["refund_account_id"], revenue_refund, 0, cc_id))

    # DR Tax Payable (refund)
    if tax_refund > Decimal("0"):
        entries.append(_build_gl_entry(
            acct_row["tax_payable_account_id"], tax_refund, 0))

    # DR Shipping Revenue (refund)
    if shipping_refund > Decimal("0"):
        entries.append(_build_gl_entry(
            acct_row["shipping_revenue_account_id"], shipping_refund, 0, cc_id))

    # CR Clearing for total refund
    entries.append(_build_gl_entry(
        acct_row["clearing_account_id"], 0, refund_amount))

    # Create journal entry
    je_id = _create_journal_entry(
        conn, company_id, posting_date, refund_amount,
        f"Shopify refund {refund['shopify_refund_id']} GL posting",
    )

    gl_ids = insert_gl_entries(
        conn, entries,
        voucher_type="journal_entry",
        voucher_id=je_id,
        posting_date=posting_date,
        company_id=company_id,
        remarks=f"Shopify refund {refund['shopify_refund_id']}",
    )

    # COGS reversal for restocked items
    restock_total = Decimal("0")
    if acct_row["track_cogs"]:
        refund_line_items = conn.execute(
            "SELECT * FROM shopify_refund_line_item WHERE shopify_refund_id_local = ?",
            (shopify_refund_id,)
        ).fetchall()

        for rli in refund_line_items:
            if rli["restock_type"] in ("return", "legacy_restock"):
                item_id = rli["item_id"]
                if item_id:
                    val = _get_item_valuation(
                        conn, item_id, rli["quantity"], company_id)
                    restock_total += val

        if restock_total > Decimal("0"):
            cogs_entries = [
                _build_gl_entry(acct_row["inventory_account_id"],
                                restock_total, 0),
                _build_gl_entry(acct_row["cogs_account_id"],
                                0, restock_total, cc_id),
            ]
            cogs_gl_ids = insert_gl_entries(
                conn, cogs_entries,
                voucher_type="journal_entry",
                voucher_id=je_id,
                posting_date=posting_date,
                company_id=company_id,
                remarks=f"Shopify refund {refund['shopify_refund_id']} restock COGS",
                entry_set="cogs",
            )
            gl_ids.extend(cogs_gl_ids)

    # Update refund GL status
    sql, params = dynamic_update("shopify_refund", {
        "gl_status": "posted",
        "gl_voucher_id": je_id,
    }, {"id": shopify_refund_id})
    conn.execute(sql, params)

    audit(conn, SKILL, "shopify-post-refund-gl", "shopify_refund",
          shopify_refund_id,
          new_values={"gl_voucher_id": je_id, "gl_status": "posted"})
    conn.commit()

    ok({
        "shopify_refund_id": shopify_refund_id,
        "journal_entry_id": je_id,
        "gl_entry_count": len(gl_ids),
        "refund_amount": str(refund_amount),
        "restock_cogs": str(restock_total),
    })


# ---------------------------------------------------------------------------
# 3. shopify-post-payout-gl
# ---------------------------------------------------------------------------
def post_payout_gl(conn, args):
    """Post GL entries for a Shopify payout (bank settlement).

    Settlement: DR Bank, CR Clearing.
    Fees: DR Fees, CR Clearing (from payout summary breakdown).
    """
    shopify_payout_id = getattr(args, "shopify_payout_id", None)
    if not shopify_payout_id:
        err("--shopify-payout-id is required (local UUID)")

    payout = conn.execute(
        "SELECT * FROM shopify_payout WHERE id = ?",
        (shopify_payout_id,)
    ).fetchone()
    if not payout:
        err(f"Shopify payout {shopify_payout_id} not found")

    if payout["gl_status"] == "posted":
        err(f"Payout {shopify_payout_id} already has GL entries posted")

    acct_row = validate_shopify_account(conn, payout["shopify_account_id"])
    company_id = acct_row["company_id"]
    cc_id = _get_cost_center(conn, company_id)
    posting_date = _resolve_posting_date(payout["issued_at"])

    net_amount = shopify_amount_to_decimal(payout["net_amount"])
    fee_amount = shopify_amount_to_decimal(payout["fee_amount"])
    gross_amount = shopify_amount_to_decimal(payout["gross_amount"])

    entries = []

    # DR Bank for net amount
    if net_amount > Decimal("0"):
        entries.append(_build_gl_entry(
            acct_row["bank_account_id"], net_amount, 0))

    # DR Processing Fees
    if fee_amount > Decimal("0"):
        entries.append(_build_gl_entry(
            acct_row["fee_account_id"], fee_amount, 0, cc_id))

    # CR Clearing for gross amount
    entries.append(_build_gl_entry(
        acct_row["clearing_account_id"], 0, gross_amount))

    # Create journal entry
    je_id = _create_journal_entry(
        conn, company_id, posting_date, gross_amount,
        f"Shopify payout {payout['shopify_payout_id']} settlement",
    )

    gl_ids = insert_gl_entries(
        conn, entries,
        voucher_type="journal_entry",
        voucher_id=je_id,
        posting_date=posting_date,
        company_id=company_id,
        remarks=f"Shopify payout {payout['shopify_payout_id']}",
    )

    # Update payout GL status
    sql, params = dynamic_update("shopify_payout", {
        "gl_status": "posted",
        "gl_voucher_id": je_id,
    }, {"id": shopify_payout_id})
    conn.execute(sql, params)

    audit(conn, SKILL, "shopify-post-payout-gl", "shopify_payout",
          shopify_payout_id,
          new_values={"gl_voucher_id": je_id, "gl_status": "posted"})
    conn.commit()

    ok({
        "shopify_payout_id": shopify_payout_id,
        "journal_entry_id": je_id,
        "gl_entry_count": len(gl_ids),
        "net_amount": str(net_amount),
        "fee_amount": str(fee_amount),
        "gross_amount": str(gross_amount),
    })


# ---------------------------------------------------------------------------
# 4. shopify-post-dispute-gl
# ---------------------------------------------------------------------------
def post_dispute_gl(conn, args):
    """Post GL entries for a Shopify dispute (chargeback).

    Open/needs_response: DR Chargeback Loss + DR Fee ($15), CR Clearing.
    Won: reverse via reverse_gl_entries(). Lost: already posted.
    """
    shopify_dispute_id = getattr(args, "shopify_dispute_id", None)
    if not shopify_dispute_id:
        err("--shopify-dispute-id is required (local UUID)")

    dispute = conn.execute(
        "SELECT * FROM shopify_dispute WHERE id = ?",
        (shopify_dispute_id,)
    ).fetchone()
    if not dispute:
        err(f"Shopify dispute {shopify_dispute_id} not found")

    acct_row = validate_shopify_account(conn, dispute["shopify_account_id"])
    company_id = acct_row["company_id"]
    cc_id = _get_cost_center(conn, company_id)
    posting_date = _resolve_posting_date(dispute["created_at"])

    dispute_amount = shopify_amount_to_decimal(dispute["amount"])
    fee_amount = shopify_amount_to_decimal(
        dispute["fee_amount"] if dispute["fee_amount"] else "0")
    if fee_amount == Decimal("0"):
        fee_amount = Decimal("15.00")  # Standard Shopify chargeback fee

    status = dispute["status"]

    if status == "won":
        # Dispute won -- reverse previously posted GL entries
        if dispute["gl_status"] != "posted":
            err("Cannot reverse dispute GL: no entries have been posted")

        reversal_ids = reverse_gl_entries(
            conn,
            voucher_type="journal_entry",
            voucher_id=dispute["gl_voucher_id"],
            posting_date=posting_date,
        )

        sql, params = dynamic_update("shopify_dispute", {
            "gl_status": "posted",  # Keep posted (now reversed)
        }, {"id": shopify_dispute_id})
        conn.execute(sql, params)

        audit(conn, SKILL, "shopify-post-dispute-gl", "shopify_dispute",
              shopify_dispute_id,
              new_values={"action": "reversal", "reversal_count": len(reversal_ids)})
        conn.commit()

        ok({
            "shopify_dispute_id": shopify_dispute_id,
            "action": "reversed",
            "reversal_gl_entry_count": len(reversal_ids),
            "dispute_amount": str(dispute_amount),
        })
        return

    # Open/needs_response/under_review/accepted/lost -- post chargeback entries
    if dispute["gl_status"] == "posted":
        err(f"Dispute {shopify_dispute_id} already has GL entries posted")

    total_debit = dispute_amount + fee_amount
    entries = []

    # DR Chargeback Loss
    entries.append(_build_gl_entry(
        acct_row["chargeback_account_id"], dispute_amount, 0, cc_id))

    # DR Chargeback Fee
    if fee_amount > Decimal("0"):
        entries.append(_build_gl_entry(
            acct_row["chargeback_fee_account_id"], fee_amount, 0, cc_id))

    # CR Clearing
    entries.append(_build_gl_entry(
        acct_row["clearing_account_id"], 0, total_debit))

    je_id = _create_journal_entry(
        conn, company_id, posting_date, total_debit,
        f"Shopify dispute {dispute['shopify_dispute_id']} chargeback",
    )

    gl_ids = insert_gl_entries(
        conn, entries,
        voucher_type="journal_entry",
        voucher_id=je_id,
        posting_date=posting_date,
        company_id=company_id,
        remarks=f"Shopify dispute {dispute['shopify_dispute_id']}",
    )

    sql, params = dynamic_update("shopify_dispute", {
        "gl_status": "posted",
        "gl_voucher_id": je_id,
    }, {"id": shopify_dispute_id})
    conn.execute(sql, params)

    audit(conn, SKILL, "shopify-post-dispute-gl", "shopify_dispute",
          shopify_dispute_id,
          new_values={"gl_voucher_id": je_id, "gl_status": "posted"})
    conn.commit()

    ok({
        "shopify_dispute_id": shopify_dispute_id,
        "journal_entry_id": je_id,
        "gl_entry_count": len(gl_ids),
        "dispute_amount": str(dispute_amount),
        "fee_amount": str(fee_amount),
        "dispute_status": status,
    })


# ---------------------------------------------------------------------------
# 5. shopify-post-gift-card-gl
# ---------------------------------------------------------------------------
def post_gift_card_gl(conn, args):
    """Post GL entries for a gift card transaction.

    Sold: DR Clearing, CR Gift Card Liability (deferred revenue).
    Redeemed: DR Gift Card Liability, CR Revenue.
    """
    shopify_order_id = getattr(args, "shopify_order_id", None)
    if not shopify_order_id:
        err("--shopify-order-id is required (local UUID)")

    gift_card_type = getattr(args, "gift_card_type", None) or "sold"
    if gift_card_type not in ("sold", "redeemed"):
        err("--gift-card-type must be 'sold' or 'redeemed'")

    order = conn.execute(
        "SELECT * FROM shopify_order WHERE id = ?",
        (shopify_order_id,)
    ).fetchone()
    if not order:
        err(f"Shopify order {shopify_order_id} not found")

    acct_row = validate_shopify_account(conn, order["shopify_account_id"])
    company_id = acct_row["company_id"]
    cc_id = _get_cost_center(conn, company_id)
    posting_date = _resolve_posting_date(order["order_date"])

    # Gift card amount -- use total_amount for the order
    gc_amount = shopify_amount_to_decimal(order["total_amount"])

    entries = []

    if gift_card_type == "sold":
        # DR Clearing, CR Gift Card Liability
        entries.append(_build_gl_entry(
            acct_row["clearing_account_id"], gc_amount, 0))
        entries.append(_build_gl_entry(
            acct_row["gift_card_liability_account_id"], 0, gc_amount))
    else:
        # DR Gift Card Liability, CR Revenue
        entries.append(_build_gl_entry(
            acct_row["gift_card_liability_account_id"], gc_amount, 0))
        entries.append(_build_gl_entry(
            acct_row["revenue_account_id"], 0, gc_amount, cc_id))

    je_id = _create_journal_entry(
        conn, company_id, posting_date, gc_amount,
        f"Shopify gift card {gift_card_type} - order {order['shopify_order_id']}",
    )

    gl_ids = insert_gl_entries(
        conn, entries,
        voucher_type="journal_entry",
        voucher_id=je_id,
        posting_date=posting_date,
        company_id=company_id,
        remarks=f"Shopify gift card {gift_card_type} - {order['shopify_order_id']}",
    )

    # Update order GL status
    sql, params = dynamic_update("shopify_order", {
        "gl_status": "posted",
        "gl_voucher_id": je_id,
        "is_gift_card_order": 1,
        "updated_at": now_iso(),
    }, {"id": shopify_order_id})
    conn.execute(sql, params)

    audit(conn, SKILL, "shopify-post-gift-card-gl", "shopify_order",
          shopify_order_id,
          new_values={"gl_voucher_id": je_id, "gift_card_type": gift_card_type})
    conn.commit()

    ok({
        "shopify_order_id": shopify_order_id,
        "journal_entry_id": je_id,
        "gift_card_type": gift_card_type,
        "gl_entry_count": len(gl_ids),
        "amount": str(gc_amount),
    })


# ---------------------------------------------------------------------------
# 6. shopify-bulk-post-gl
# ---------------------------------------------------------------------------
def bulk_post_gl(conn, args):
    """Post GL for all unposted Shopify objects within a date range.

    Posts orders, refunds, payouts, and disputes that have gl_status='pending'.
    Sub-action stdout is suppressed to avoid multiple JSON outputs.
    """
    import io
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)

    date_from = getattr(args, "date_from", None)
    date_to = getattr(args, "date_to", None)

    results = {
        "orders_posted": 0,
        "refunds_posted": 0,
        "payouts_posted": 0,
        "disputes_posted": 0,
        "errors": [],
    }

    def _silent_call(fn, sub_args):
        """Call a sub-action, suppressing its stdout (ok()/err() calls)."""
        real_stdout = sys.stdout
        sys.stdout = io.StringIO()
        try:
            fn(conn, sub_args)
        except SystemExit:
            pass
        finally:
            sys.stdout = real_stdout

    # Post unposted orders
    order_q = "SELECT id FROM shopify_order WHERE shopify_account_id = ? AND gl_status = 'pending'"
    order_params = [shopify_account_id]
    if date_from:
        order_q += " AND order_date >= ?"
        order_params.append(date_from)
    if date_to:
        order_q += " AND order_date <= ?"
        order_params.append(date_to)

    orders = conn.execute(order_q, order_params).fetchall()
    for o in orders:
        try:
            _args = _make_args(shopify_order_id=o["id"])
            _silent_call(post_order_gl, _args)
            results["orders_posted"] += 1
        except (SystemExit, Exception):
            results["errors"].append(f"order:{o['id']}")

    # Post unposted refunds
    refund_q = """SELECT r.id FROM shopify_refund r
                  JOIN shopify_order o ON r.shopify_order_id_local = o.id
                  WHERE o.shopify_account_id = ? AND r.gl_status = 'pending'"""
    refund_params = [shopify_account_id]
    if date_from:
        refund_q += " AND r.refund_date >= ?"
        refund_params.append(date_from)
    if date_to:
        refund_q += " AND r.refund_date <= ?"
        refund_params.append(date_to)

    refunds = conn.execute(refund_q, refund_params).fetchall()
    for r in refunds:
        try:
            _args = _make_args(shopify_refund_id=r["id"])
            _silent_call(post_refund_gl, _args)
            results["refunds_posted"] += 1
        except (SystemExit, Exception):
            results["errors"].append(f"refund:{r['id']}")

    # Post unposted payouts
    payout_q = "SELECT id FROM shopify_payout WHERE shopify_account_id = ? AND gl_status = 'pending'"
    payout_params = [shopify_account_id]
    if date_from:
        payout_q += " AND issued_at >= ?"
        payout_params.append(date_from)
    if date_to:
        payout_q += " AND issued_at <= ?"
        payout_params.append(date_to)

    payouts = conn.execute(payout_q, payout_params).fetchall()
    for p in payouts:
        try:
            _args = _make_args(shopify_payout_id=p["id"])
            _silent_call(post_payout_gl, _args)
            results["payouts_posted"] += 1
        except (SystemExit, Exception):
            results["errors"].append(f"payout:{p['id']}")

    # Post unposted disputes
    dispute_q = "SELECT id FROM shopify_dispute WHERE shopify_account_id = ? AND gl_status = 'pending'"
    dispute_params = [shopify_account_id]

    disputes = conn.execute(dispute_q, dispute_params).fetchall()
    for d in disputes:
        try:
            _args = _make_args(shopify_dispute_id=d["id"])
            _silent_call(post_dispute_gl, _args)
            results["disputes_posted"] += 1
        except (SystemExit, Exception):
            results["errors"].append(f"dispute:{d['id']}")

    total_posted = (results["orders_posted"] + results["refunds_posted"] +
                    results["payouts_posted"] + results["disputes_posted"])

    audit(conn, SKILL, "shopify-bulk-post-gl", "shopify_account",
          shopify_account_id,
          new_values={"total_posted": total_posted})
    conn.commit()

    ok({
        "shopify_account_id": shopify_account_id,
        "total_posted": total_posted,
        **results,
    })


def _make_args(**kwargs):
    """Build a simple namespace for internal action calls."""
    import argparse
    return argparse.Namespace(**kwargs)


# ---------------------------------------------------------------------------
# 7. shopify-reverse-order-gl
# ---------------------------------------------------------------------------
def reverse_order_gl(conn, args):
    """Reverse GL entries for a Shopify order (correction).

    Calls reverse_gl_entries() to create mirror entries.
    """
    shopify_order_id = getattr(args, "shopify_order_id", None)
    if not shopify_order_id:
        err("--shopify-order-id is required (local UUID)")

    order = conn.execute(
        "SELECT * FROM shopify_order WHERE id = ?",
        (shopify_order_id,)
    ).fetchone()
    if not order:
        err(f"Shopify order {shopify_order_id} not found")

    if order["gl_status"] != "posted":
        err("Cannot reverse: order GL has not been posted")

    if not order["gl_voucher_id"]:
        err("Cannot reverse: no gl_voucher_id on order")

    posting_date = _resolve_posting_date(order["order_date"])

    reversal_ids = reverse_gl_entries(
        conn,
        voucher_type="journal_entry",
        voucher_id=order["gl_voucher_id"],
        posting_date=posting_date,
    )

    # Reset order GL status so it can be re-posted
    sql, params = dynamic_update("shopify_order", {
        "gl_status": "pending",
        "gl_voucher_id": None,
        "updated_at": now_iso(),
    }, {"id": shopify_order_id})
    conn.execute(sql, params)

    # Cancel the journal entry
    sql, params = dynamic_update("journal_entry", {
        "status": "cancelled",
        "updated_at": now_iso(),
    }, {"id": order["gl_voucher_id"]})
    conn.execute(sql, params)

    audit(conn, SKILL, "shopify-reverse-order-gl", "shopify_order",
          shopify_order_id,
          new_values={"action": "reversed", "reversal_count": len(reversal_ids)})
    conn.commit()

    ok({
        "shopify_order_id": shopify_order_id,
        "reversed_voucher_id": order["gl_voucher_id"],
        "reversal_gl_entry_count": len(reversal_ids),
    })


# ---------------------------------------------------------------------------
# 8. shopify-post-reserve-gl
# ---------------------------------------------------------------------------
def post_reserve_gl(conn, args):
    """Post GL entries for a Shopify payment reserve hold or release.

    Reserve hold: DR Reserve Receivable, CR Clearing.
    Reserve release: DR Clearing, CR Reserve Receivable.
    """
    shopify_payout_id = getattr(args, "shopify_payout_id", None)
    if not shopify_payout_id:
        err("--shopify-payout-id is required (local UUID)")

    reserve_type = getattr(args, "reserve_type", None) or "hold"
    if reserve_type not in ("hold", "release"):
        err("--reserve-type must be 'hold' or 'release'")

    payout = conn.execute(
        "SELECT * FROM shopify_payout WHERE id = ?",
        (shopify_payout_id,)
    ).fetchone()
    if not payout:
        err(f"Shopify payout {shopify_payout_id} not found")

    acct_row = validate_shopify_account(conn, payout["shopify_account_id"])
    company_id = acct_row["company_id"]
    posting_date = _resolve_posting_date(payout["issued_at"])

    reserve_amount = shopify_amount_to_decimal(
        payout["reserved_funds_gross"] if payout["reserved_funds_gross"] else "0")
    if reserve_amount <= Decimal("0"):
        err("No reserved funds to post")

    entries = []
    if reserve_type == "hold":
        # DR Reserve Receivable, CR Clearing
        entries.append(_build_gl_entry(
            acct_row["reserve_account_id"], reserve_amount, 0))
        entries.append(_build_gl_entry(
            acct_row["clearing_account_id"], 0, reserve_amount))
    else:
        # DR Clearing, CR Reserve Receivable
        entries.append(_build_gl_entry(
            acct_row["clearing_account_id"], reserve_amount, 0))
        entries.append(_build_gl_entry(
            acct_row["reserve_account_id"], 0, reserve_amount))

    je_id = _create_journal_entry(
        conn, company_id, posting_date, reserve_amount,
        f"Shopify reserve {reserve_type} - payout {payout['shopify_payout_id']}",
    )

    gl_ids = insert_gl_entries(
        conn, entries,
        voucher_type="journal_entry",
        voucher_id=je_id,
        posting_date=posting_date,
        company_id=company_id,
        remarks=f"Shopify reserve {reserve_type}",
    )

    audit(conn, SKILL, "shopify-post-reserve-gl", "shopify_payout",
          shopify_payout_id,
          new_values={"reserve_type": reserve_type, "amount": str(reserve_amount)})
    conn.commit()

    ok({
        "shopify_payout_id": shopify_payout_id,
        "journal_entry_id": je_id,
        "reserve_type": reserve_type,
        "gl_entry_count": len(gl_ids),
        "amount": str(reserve_amount),
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "shopify-post-order-gl": post_order_gl,
    "shopify-post-refund-gl": post_refund_gl,
    "shopify-post-payout-gl": post_payout_gl,
    "shopify-post-dispute-gl": post_dispute_gl,
    "shopify-post-gift-card-gl": post_gift_card_gl,
    "shopify-bulk-post-gl": bulk_post_gl,
    "shopify-reverse-order-gl": reverse_order_gl,
    "shopify-post-reserve-gl": post_reserve_gl,
}
