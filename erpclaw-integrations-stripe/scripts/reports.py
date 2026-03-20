"""ERPClaw Integrations Stripe — reporting actions.

7 actions for revenue, fee, reconciliation, payout detail, customer revenue,
MRR, and dispute reports.

Imported by db_query.py (unified router).
"""
import os
import sys
from decimal import Decimal

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
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

from stripe_helpers import validate_stripe_account


# ---------------------------------------------------------------------------
# 1. stripe-revenue-report
# ---------------------------------------------------------------------------
def revenue_report(conn, args):
    """Charges grouped by month, minus fees.

    Shows gross charges, total fees, and net revenue per month.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    # Use balance_transaction for accurate fee/net data
    rows = conn.execute(
        """SELECT
               substr(created_stripe, 1, 7) as month,
               COUNT(*) as charge_count,
               decimal_sum(amount) as gross,
               decimal_sum(fee) as total_fees,
               decimal_sum(net) as net_revenue
           FROM stripe_balance_transaction
           WHERE stripe_account_id = ? AND type = 'charge'
           GROUP BY substr(created_stripe, 1, 7)
           ORDER BY month DESC""",
        (stripe_account_id,)
    ).fetchall()

    months = []
    grand_gross = Decimal("0")
    grand_fees = Decimal("0")
    grand_net = Decimal("0")
    for r in rows:
        gross = to_decimal(str(r["gross"])) if r["gross"] else Decimal("0")
        fees = to_decimal(str(r["total_fees"])) if r["total_fees"] else Decimal("0")
        net = to_decimal(str(r["net_revenue"])) if r["net_revenue"] else Decimal("0")
        grand_gross += gross
        grand_fees += fees
        grand_net += net
        months.append({
            "month": r["month"],
            "charge_count": r["charge_count"],
            "gross": str(round_currency(gross)),
            "fees": str(round_currency(fees)),
            "net": str(round_currency(net)),
        })

    ok({
        "report": "revenue",
        "months": months,
        "totals": {
            "gross": str(round_currency(grand_gross)),
            "fees": str(round_currency(grand_fees)),
            "net": str(round_currency(grand_net)),
        },
        "month_count": len(months),
    })


# ---------------------------------------------------------------------------
# 2. stripe-fee-report
# ---------------------------------------------------------------------------
def fee_report(conn, args):
    """Fee breakdown by type from stripe_fee_detail and balance_transactions."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    # Try fee_detail table first (more granular)
    rows = conn.execute(
        """SELECT
               fd.fee_type,
               COUNT(*) as count,
               decimal_sum(fd.amount) as total
           FROM stripe_fee_detail fd
           JOIN stripe_balance_transaction bt ON fd.balance_transaction_id = bt.id
           WHERE bt.stripe_account_id = ?
           GROUP BY fd.fee_type
           ORDER BY total DESC""",
        (stripe_account_id,)
    ).fetchall()

    fee_types = []
    grand_total = Decimal("0")

    if rows:
        for r in rows:
            amt = to_decimal(str(r["total"])) if r["total"] else Decimal("0")
            grand_total += amt
            fee_types.append({
                "fee_type": r["fee_type"],
                "count": r["count"],
                "total": str(round_currency(amt)),
            })
    else:
        # Fallback: aggregate from balance_transaction.fee grouped by type
        fallback = conn.execute(
            """SELECT
                   type,
                   COUNT(*) as count,
                   decimal_sum(fee) as total_fee
               FROM stripe_balance_transaction
               WHERE stripe_account_id = ? AND fee != '0'
               GROUP BY type
               ORDER BY total_fee DESC""",
            (stripe_account_id,)
        ).fetchall()
        for r in fallback:
            amt = to_decimal(str(r["total_fee"])) if r["total_fee"] else Decimal("0")
            grand_total += amt
            fee_types.append({
                "fee_type": r["type"],
                "count": r["count"],
                "total": str(round_currency(amt)),
            })

    ok({
        "report": "fees",
        "fee_types": fee_types,
        "grand_total": str(round_currency(grand_total)),
    })


# ---------------------------------------------------------------------------
# 3. stripe-reconciliation-report
# ---------------------------------------------------------------------------
def reconciliation_report(conn, args):
    """Matched vs unmatched balance transaction counts and amounts."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    row = conn.execute(
        """SELECT
               COUNT(*) as total_transactions,
               SUM(CASE WHEN reconciled = 1 THEN 1 ELSE 0 END) as matched,
               SUM(CASE WHEN reconciled = 0 THEN 1 ELSE 0 END) as unmatched,
               decimal_sum(CASE WHEN reconciled = 1 THEN amount ELSE '0' END) as matched_amount,
               decimal_sum(CASE WHEN reconciled = 0 THEN amount ELSE '0' END) as unmatched_amount
           FROM stripe_balance_transaction
           WHERE stripe_account_id = ?""",
        (stripe_account_id,)
    ).fetchone()

    matched_amt = to_decimal(str(row["matched_amount"])) if row["matched_amount"] else Decimal("0")
    unmatched_amt = to_decimal(str(row["unmatched_amount"])) if row["unmatched_amount"] else Decimal("0")
    total = row["total_transactions"] or 0
    matched = row["matched"] or 0
    unmatched = row["unmatched"] or 0

    match_rate = (Decimal(str(matched)) / Decimal(str(total)) * 100) if total > 0 else Decimal("0")

    ok({
        "report": "reconciliation",
        "total_transactions": total,
        "matched": matched,
        "unmatched": unmatched,
        "matched_amount": str(round_currency(matched_amt)),
        "unmatched_amount": str(round_currency(unmatched_amt)),
        "match_rate_pct": str(round_currency(match_rate)),
    })


# ---------------------------------------------------------------------------
# 4. stripe-payout-detail-report
# ---------------------------------------------------------------------------
def payout_detail_report(conn, args):
    """Detailed breakdown of a specific payout with all constituent transactions."""
    payout_stripe_id = getattr(args, "payout_stripe_id", None)
    if not payout_stripe_id:
        err("--payout-stripe-id is required")

    t = Table("stripe_payout")
    payout = conn.execute(
        Q.from_(t).select("*").where(t.stripe_id == P()).get_sql(),
        (payout_stripe_id,)
    ).fetchone()
    if not payout:
        err(f"Payout {payout_stripe_id} not found")

    result = row_to_dict(payout)

    # Get all balance transactions in this payout
    bt = Table("stripe_balance_transaction")
    txns = conn.execute(
        Q.from_(bt).select("*").where(bt.payout_id == P()).get_sql(),
        (payout_stripe_id,)
    ).fetchall()

    txn_list = rows_to_list(txns)

    # Summarize by type
    type_summary = {}
    for txn in txn_list:
        txn_type = txn.get("type", "unknown")
        if txn_type not in type_summary:
            type_summary[txn_type] = {"count": 0, "amount": Decimal("0"), "fee": Decimal("0")}
        type_summary[txn_type]["count"] += 1
        type_summary[txn_type]["amount"] += to_decimal(txn.get("amount", "0"))
        type_summary[txn_type]["fee"] += to_decimal(txn.get("fee", "0"))

    summary = []
    for k, v in type_summary.items():
        summary.append({
            "type": k,
            "count": v["count"],
            "amount": str(round_currency(v["amount"])),
            "fee": str(round_currency(v["fee"])),
        })

    result["transactions"] = txn_list
    result["transaction_count"] = len(txn_list)
    result["type_summary"] = summary

    ok(result)


# ---------------------------------------------------------------------------
# 5. stripe-customer-revenue-report
# ---------------------------------------------------------------------------
def customer_revenue_report(conn, args):
    """Revenue breakdown by customer for a Stripe account."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    rows = conn.execute(
        """SELECT
               sc.customer_stripe_id,
               scm.stripe_name,
               scm.erpclaw_customer_id,
               COUNT(*) as charge_count,
               decimal_sum(sc.amount) as total_revenue
           FROM stripe_charge sc
           LEFT JOIN stripe_customer_map scm
               ON sc.customer_stripe_id = scm.stripe_customer_id
               AND sc.stripe_account_id = scm.stripe_account_id
           WHERE sc.stripe_account_id = ? AND sc.status = 'succeeded'
           GROUP BY sc.customer_stripe_id, scm.stripe_name, scm.erpclaw_customer_id
           ORDER BY total_revenue DESC""",
        (stripe_account_id,)
    ).fetchall()

    customers = []
    grand_total = Decimal("0")
    for r in rows:
        rev = to_decimal(str(r["total_revenue"])) if r["total_revenue"] else Decimal("0")
        grand_total += rev
        customers.append({
            "customer_stripe_id": r["customer_stripe_id"],
            "customer_name": r["stripe_name"],
            "erpclaw_customer_id": r["erpclaw_customer_id"],
            "charge_count": r["charge_count"],
            "total_revenue": str(round_currency(rev)),
        })

    ok({
        "report": "customer_revenue",
        "customers": customers,
        "customer_count": len(customers),
        "grand_total": str(round_currency(grand_total)),
    })


# ---------------------------------------------------------------------------
# 6. stripe-mrr-report
# ---------------------------------------------------------------------------
def mrr_report(conn, args):
    """Monthly Recurring Revenue from active subscriptions.

    Calculates MRR by normalizing all subscription plan amounts to monthly.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    rows = conn.execute(
        """SELECT plan_interval, plan_amount, status
           FROM stripe_subscription
           WHERE stripe_account_id = ? AND status IN ('active', 'trialing')""",
        (stripe_account_id,)
    ).fetchall()

    # Normalize to monthly
    interval_multipliers = {
        "day": Decimal("30"),        # daily -> monthly = x30
        "week": Decimal("4.333"),    # weekly -> monthly = x4.333
        "month": Decimal("1"),       # already monthly
        "year": Decimal("0.08333"),  # yearly -> monthly = /12
    }

    total_mrr = Decimal("0")
    active_count = 0
    trialing_count = 0
    by_interval = {}

    for r in rows:
        plan_amount = to_decimal(r["plan_amount"])
        interval = r["plan_interval"] or "month"
        multiplier = interval_multipliers.get(interval, Decimal("1"))
        monthly = round_currency(plan_amount * multiplier)
        total_mrr += monthly

        if r["status"] == "active":
            active_count += 1
        elif r["status"] == "trialing":
            trialing_count += 1

        if interval not in by_interval:
            by_interval[interval] = {"count": 0, "mrr": Decimal("0")}
        by_interval[interval]["count"] += 1
        by_interval[interval]["mrr"] += monthly

    interval_breakdown = []
    for k, v in by_interval.items():
        interval_breakdown.append({
            "interval": k,
            "subscription_count": v["count"],
            "mrr_contribution": str(round_currency(v["mrr"])),
        })

    ok({
        "report": "mrr",
        "total_mrr": str(round_currency(total_mrr)),
        "active_subscriptions": active_count,
        "trialing_subscriptions": trialing_count,
        "total_subscriptions": active_count + trialing_count,
        "interval_breakdown": interval_breakdown,
    })


# ---------------------------------------------------------------------------
# 7. stripe-dispute-report
# ---------------------------------------------------------------------------
def dispute_report(conn, args):
    """Disputes grouped by status with amounts."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    rows = conn.execute(
        """SELECT
               status,
               COUNT(*) as count,
               decimal_sum(amount) as total_amount
           FROM stripe_dispute
           WHERE stripe_account_id = ?
           GROUP BY status
           ORDER BY count DESC""",
        (stripe_account_id,)
    ).fetchall()

    statuses = []
    grand_total = Decimal("0")
    total_count = 0
    for r in rows:
        amt = to_decimal(str(r["total_amount"])) if r["total_amount"] else Decimal("0")
        grand_total += amt
        total_count += r["count"]
        statuses.append({
            "status": r["status"],
            "count": r["count"],
            "total_amount": str(round_currency(amt)),
        })

    ok({
        "report": "disputes",
        "statuses": statuses,
        "total_disputes": total_count,
        "total_amount": str(round_currency(grand_total)),
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "stripe-revenue-report": revenue_report,
    "stripe-fee-report": fee_report,
    "stripe-reconciliation-report": reconciliation_report,
    "stripe-payout-detail-report": payout_detail_report,
    "stripe-customer-revenue-report": customer_revenue_report,
    "stripe-mrr-report": mrr_report,
    "stripe-dispute-report": dispute_report,
}
