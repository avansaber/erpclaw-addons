"""ERPClaw Integrations Stripe — reporting actions.

7 actions for revenue, fee, reconciliation, payout detail, customer revenue,
MRR, and dispute reports.

Imported by db_query.py (unified router).
"""
import os
import sys
from decimal import Decimal, InvalidOperation

try:
    import importlib.util
    if importlib.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
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

    # Per-transaction grouping: a transaction with one or more detail rows
    # contributes those rows (source "fee_detail"); otherwise it contributes
    # its own fee grouped by type (source "balance_transaction", skipped when
    # numerically zero). Amounts are TEXT, so rows are read per-row and
    # grouped/summed in Python with Decimal.
    bt = Table("stripe_balance_transaction")
    fd = Table("stripe_fee_detail")
    transactions = conn.execute(
        Q.from_(bt).select(
            bt.id, bt.type, bt.fee
        ).where(
            bt.stripe_account_id == P()
        ).get_sql(),
        (stripe_account_id,)
    ).fetchall()
    details = conn.execute(
        Q.from_(fd).join(bt).on(
            fd.balance_transaction_id == bt.id
        ).select(
            fd.balance_transaction_id, fd.fee_type, fd.amount
        ).where(
            bt.stripe_account_id == P()
        ).get_sql(),
        (stripe_account_id,)
    ).fetchall()

    details_by_transaction = {}
    for d in details:
        details_by_transaction.setdefault(
            d["balance_transaction_id"], []).append(d)

    groups = {}
    for t in transactions:
        lines = details_by_transaction.get(t["id"])
        if lines:
            for d in lines:
                amt = to_decimal(str(d["amount"])) if d["amount"] is not None else Decimal("0")
                key = ("fee_detail", d["fee_type"])
                entry = groups.setdefault(key, {"count": 0, "total": Decimal("0")})
                entry["count"] += 1
                entry["total"] += amt
        else:
            amt = to_decimal(str(t["fee"])) if t["fee"] is not None else Decimal("0")
            if amt == 0:
                continue
            key = ("balance_transaction", t["type"])
            entry = groups.setdefault(key, {"count": 0, "total": Decimal("0")})
            entry["count"] += 1
            entry["total"] += amt

    fee_types = []
    grand_total = Decimal("0")
    for key, entry in sorted(
        groups.items(), key=lambda kv: (-kv[1]["total"], kv[0][1], kv[0][0])
    ):
        source, fee_type = key
        grand_total += entry["total"]
        fee_types.append({
            "fee_type": fee_type,
            "source": source,
            "count": entry["count"],
            "total": str(round_currency(entry["total"])),
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

    # Get all balance transactions in this payout, on the payout's own
    # Stripe account (as stripe-reconcile-payout counts them)
    bt = Table("stripe_balance_transaction")
    txns = conn.execute(
        Q.from_(bt).select("*").where(bt.payout_id == P())
        .where(bt.stripe_account_id == P()).get_sql(),
        (payout_stripe_id, result["stripe_account_id"])
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

    Active-only revenue: trialing subscriptions are an informational count,
    never revenue. Annual normalizes as amount / 12 (exact Decimal);
    monthly is amount as-is. Day (x30) and week (x4.333) multipliers are
    preserved pre-existing approximations, not exact calendar math.
    Contributions sum unrounded as Decimal, grouped by case-normalized
    currency and by currency/interval; displayed totals round only after
    grouping. mrr_by_currency is authoritative with currencies sorted and
    per-currency interval breakdowns. A single active currency keeps a
    string total_mrr and names currency; mixed active currencies return
    total_mrr null and currency null; no active rows give total_mrr "0.00",
    currency null and an empty currency list. Unknown, empty or missing
    active interval, blank active currency, and non-finite or negative
    active amounts refuse with an ordinary error; nothing is assumed.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    rows = conn.execute(
        """SELECT plan_interval, plan_amount, currency, status
           FROM stripe_subscription
           WHERE stripe_account_id = ? AND status IN ('active', 'trialing')""",
        (stripe_account_id,)
    ).fetchall()

    allowed_intervals = ("day", "week", "month", "year")

    active_count = 0
    trialing_count = 0
    per_currency = {}

    for r in rows:
        if r["status"] == "trialing":
            trialing_count += 1
            continue
        if r["status"] != "active":
            continue
        active_count += 1

        raw_interval = r["plan_interval"]
        interval = raw_interval.strip() if isinstance(raw_interval, str) else None
        if not interval or interval not in allowed_intervals:
            err(
                "Stripe MRR: unknown plan_interval %r for active subscription;"
                " expected one of day/week/month/year" % (raw_interval,)
            )

        raw_currency = r["currency"]
        if not isinstance(raw_currency, str) or not raw_currency.strip():
            err("Stripe MRR: blank currency for active subscription;"
                " cannot assume USD")
        currency = raw_currency.strip().upper()

        raw_amount = r["plan_amount"]
        if raw_amount is None or (isinstance(raw_amount, str)
                                  and not raw_amount.strip()):
            err("Stripe MRR: missing plan_amount for active subscription")
        try:
            amount = to_decimal(raw_amount)
        except (TypeError, ValueError, InvalidOperation):
            err("Stripe MRR: invalid plan_amount %r for active subscription"
                % (raw_amount,))
        if not amount.is_finite():
            err("Stripe MRR: non-finite plan_amount %r for active subscription"
                % (raw_amount,))
        if amount < Decimal("0"):
            err("Stripe MRR: negative plan_amount %r for active subscription"
                % (raw_amount,))

        if interval == "month":
            monthly = amount
        elif interval == "year":
            monthly = amount / Decimal("12")
        elif interval == "day":
            monthly = amount * Decimal("30")
        else:
            monthly = amount * Decimal("4.333")

        entry = per_currency.setdefault(
            currency, {"total": Decimal("0"), "count": 0, "by_interval": {}})
        entry["total"] += monthly
        entry["count"] += 1
        bucket = entry["by_interval"].setdefault(
            interval, {"count": 0, "total": Decimal("0")})
        bucket["count"] += 1
        bucket["total"] += monthly

    mrr_by_currency = []
    for currency in sorted(per_currency):
        entry = per_currency[currency]
        breakdown = []
        for interval in sorted(entry["by_interval"]):
            bucket = entry["by_interval"][interval]
            breakdown.append({
                "interval": interval,
                "subscription_count": bucket["count"],
                "mrr_contribution": str(round_currency(bucket["total"])),
            })
        rounded = str(round_currency(entry["total"]))
        mrr_by_currency.append({
            "currency": currency,
            "mrr": rounded,
            "total_mrr": rounded,
            "subscription_count": entry["count"],
            "interval_breakdown": breakdown,
        })

    if not mrr_by_currency:
        ok({
            "report": "mrr",
            "total_mrr": "0.00",
            "currency": None,
            "active_subscriptions": active_count,
            "trialing_subscriptions": trialing_count,
            "total_subscriptions": active_count + trialing_count,
            "mrr_by_currency": [],
            "interval_breakdown": [],
        })
    if len(mrr_by_currency) == 1:
        sole = mrr_by_currency[0]
        ok({
            "report": "mrr",
            "total_mrr": sole["mrr"],
            "currency": sole["currency"],
            "active_subscriptions": active_count,
            "trialing_subscriptions": trialing_count,
            "total_subscriptions": active_count + trialing_count,
            "mrr_by_currency": mrr_by_currency,
            "interval_breakdown": sole["interval_breakdown"],
        })
    ok({
        "report": "mrr",
        "total_mrr": None,
        "currency": None,
        "active_subscriptions": active_count,
        "trialing_subscriptions": trialing_count,
        "total_subscriptions": active_count + trialing_count,
        "mrr_by_currency": mrr_by_currency,
        "interval_breakdown": [],
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
