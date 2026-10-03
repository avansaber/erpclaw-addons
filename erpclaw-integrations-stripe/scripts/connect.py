"""ERPClaw Integrations Stripe — Connect platform actions.

6 actions for Stripe Connect: listing connected accounts, application fees,
transfers, and generating Connect-specific revenue/payout/fee reports.

Imported by db_query.py (unified router).
"""
import os
import sys
from decimal import Decimal

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
# 1. stripe-list-connected-accounts
# ---------------------------------------------------------------------------
def list_connected_accounts(conn, args):
    """List connected accounts from stripe_customer_map.

    Placeholder: reads stripe_customer_map entries to find connected
    Stripe accounts linked to this platform account.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_customer_map")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)
    params = [stripe_account_id]

    limit = getattr(args, "limit", 50) or 50
    q = q.limit(limit)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({"connected_accounts": rows_to_list(rows), "count": len(rows)})


# ---------------------------------------------------------------------------
# 2. stripe-list-application-fees
# ---------------------------------------------------------------------------
def list_application_fees(conn, args):
    """List Stripe Connect application fees."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_application_fee")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)
    params = [stripe_account_id]

    limit = getattr(args, "limit", 50) or 50
    q = q.limit(limit)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({"application_fees": rows_to_list(rows), "count": len(rows)})


# ---------------------------------------------------------------------------
# 3. stripe-list-transfers
# ---------------------------------------------------------------------------
def list_transfers(conn, args):
    """List Stripe Connect transfers between accounts."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_transfer")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)
    params = [stripe_account_id]

    limit = getattr(args, "limit", 50) or 50
    q = q.limit(limit)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({"transfers": rows_to_list(rows), "count": len(rows)})


# ---------------------------------------------------------------------------
# 4. stripe-list-credit-notes
# ---------------------------------------------------------------------------
def list_credit_notes(conn, args):
    """List Stripe credit notes synced for an account (ASC 606 adjustments)."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_credit_note")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)
    params = [stripe_account_id]

    limit = getattr(args, "limit", 50) or 50
    q = q.limit(limit)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({"credit_notes": rows_to_list(rows), "count": len(rows)})


# ---------------------------------------------------------------------------
# 5. stripe-connect-revenue-report
# ---------------------------------------------------------------------------
def connect_revenue_report(conn, args):
    """Generate Connect platform revenue report: SUM application fees by month.

    Groups application fees by month (from created_stripe) and sums amounts.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    rows = conn.execute(
        """SELECT
               substr(created_stripe, 1, 7) as month,
               COUNT(*) as fee_count,
               decimal_sum(amount) as total_amount
           FROM stripe_application_fee
           WHERE stripe_account_id = ?
           GROUP BY substr(created_stripe, 1, 7)
           ORDER BY month DESC""",
        (stripe_account_id,)
    ).fetchall()

    months = []
    grand_total = Decimal("0")
    for r in rows:
        amt = to_decimal(str(r["total_amount"])) if r["total_amount"] else Decimal("0")
        grand_total += amt
        months.append({
            "month": r["month"],
            "fee_count": r["fee_count"],
            "total_amount": str(round_currency(amt)),
        })

    ok({
        "report": "connect_revenue",
        "months": months,
        "grand_total": str(round_currency(grand_total)),
        "month_count": len(months),
    })


# ---------------------------------------------------------------------------
# 6. stripe-connect-payout-report
# ---------------------------------------------------------------------------
def connect_payout_report(conn, args):
    """Generate Connect platform payout report: SUM transfers by month.

    Fully reversed transfers are excluded from the paid-out totals and
    reported separately under reversed_count / reversed_amount.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_transfer")
    q = Q.from_(t).select(
        t.created_stripe, t.amount, t.reversed
    ).where(t.stripe_account_id == P())
    rows = conn.execute(q.get_sql(), (stripe_account_id,)).fetchall()

    buckets = {}
    for r in rows:
        created = r["created_stripe"]
        month = created[:7] if created else None
        bucket = buckets.setdefault(month, {
            "live_count": 0,
            "live_total": Decimal("0"),
            "reversed_count": 0,
            "reversed_total": Decimal("0"),
        })
        amt = to_decimal(str(r["amount"])) if r["amount"] else Decimal("0")
        if r["reversed"]:
            bucket["reversed_count"] += 1
            bucket["reversed_total"] += amt
        else:
            bucket["live_count"] += 1
            bucket["live_total"] += amt

    dated = sorted((m for m in buckets if m is not None), reverse=True)
    ordered = dated + ([None] if None in buckets else [])

    months = []
    grand_total = Decimal("0")
    grand_reversed_count = 0
    grand_reversed_total = Decimal("0")
    for month in ordered:
        bucket = buckets[month]
        grand_total += bucket["live_total"]
        grand_reversed_count += bucket["reversed_count"]
        grand_reversed_total += bucket["reversed_total"]
        months.append({
            "month": month,
            "transfer_count": bucket["live_count"],
            "total_amount": str(round_currency(bucket["live_total"])),
            "reversed_count": bucket["reversed_count"],
            "reversed_amount": str(round_currency(bucket["reversed_total"])),
        })

    ok({
        "report": "connect_payouts",
        "months": months,
        "grand_total": str(round_currency(grand_total)),
        "reversed_count": grand_reversed_count,
        "reversed_total": str(round_currency(grand_reversed_total)),
        "month_count": len(months),
    })


# ---------------------------------------------------------------------------
# 7. stripe-connect-fee-summary
# ---------------------------------------------------------------------------
def connect_fee_summary(conn, args):
    """Total platform fees earned as a Connect platform."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    row = conn.execute(
        """SELECT
               COUNT(*) as fee_count,
               decimal_sum(amount) as total_earned,
               decimal_sum(refunded_amount) as total_refunded
           FROM stripe_application_fee
           WHERE stripe_account_id = ?""",
        (stripe_account_id,)
    ).fetchone()

    total_earned = to_decimal(str(row["total_earned"])) if row["total_earned"] else Decimal("0")
    total_refunded = to_decimal(str(row["total_refunded"])) if row["total_refunded"] else Decimal("0")
    net = total_earned - total_refunded

    ok({
        "report": "connect_fee_summary",
        "fee_count": row["fee_count"],
        "total_earned": str(round_currency(total_earned)),
        "total_refunded": str(round_currency(total_refunded)),
        "net_earned": str(round_currency(net)),
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "stripe-list-connected-accounts": list_connected_accounts,
    "stripe-list-application-fees": list_application_fees,
    "stripe-list-transfers": list_transfers,
    "stripe-list-credit-notes": list_credit_notes,
    "stripe-connect-revenue-report": connect_revenue_report,
    "stripe-connect-payout-report": connect_payout_report,
    "stripe-connect-fee-summary": connect_fee_summary,
}
