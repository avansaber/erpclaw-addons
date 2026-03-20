"""ERPClaw Integrations Stripe — Connect platform actions.

6 actions for Stripe Connect: listing connected accounts, application fees,
transfers, and generating Connect-specific revenue/payout/fee reports.

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
# 4. stripe-connect-revenue-report
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
# 5. stripe-connect-payout-report
# ---------------------------------------------------------------------------
def connect_payout_report(conn, args):
    """Generate Connect platform payout report: SUM transfers by month."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    rows = conn.execute(
        """SELECT
               substr(created_stripe, 1, 7) as month,
               COUNT(*) as transfer_count,
               decimal_sum(amount) as total_amount
           FROM stripe_transfer
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
            "transfer_count": r["transfer_count"],
            "total_amount": str(round_currency(amt)),
        })

    ok({
        "report": "connect_payouts",
        "months": months,
        "grand_total": str(round_currency(grand_total)),
        "month_count": len(months),
    })


# ---------------------------------------------------------------------------
# 6. stripe-connect-fee-summary
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
    "stripe-connect-revenue-report": connect_revenue_report,
    "stripe-connect-payout-report": connect_payout_report,
    "stripe-connect-fee-summary": connect_fee_summary,
}
