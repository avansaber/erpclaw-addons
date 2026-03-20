"""ERPClaw Integrations Stripe — browse/read actions.

8 actions for listing and retrieving synced Stripe data from local tables.
All read-only — no writes, no GL posting.

Imported by db_query.py (unified router).
"""
import os
import sys

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

from stripe_helpers import validate_stripe_account


# ---------------------------------------------------------------------------
# 1. stripe-list-charges
# ---------------------------------------------------------------------------
def list_charges(conn, args):
    """List Stripe charges with optional filters.

    Filters: --status, --customer-stripe-id, --date-from, --date-to, --limit.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_charge")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)
    params = [stripe_account_id]

    status = getattr(args, "status", None)
    if status:
        q = q.where(t.status == P())
        params.append(status)

    customer_stripe_id = getattr(args, "customer_stripe_id", None)
    if customer_stripe_id:
        q = q.where(t.customer_stripe_id == P())
        params.append(customer_stripe_id)

    date_from = getattr(args, "date_from", None)
    if date_from:
        q = q.where(t.created_stripe >= P())
        params.append(date_from)

    date_to = getattr(args, "date_to", None)
    if date_to:
        q = q.where(t.created_stripe <= P())
        params.append(date_to)

    limit = getattr(args, "limit", 50) or 50
    q = q.limit(limit)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({"charges": rows_to_list(rows), "count": len(rows)})


# ---------------------------------------------------------------------------
# 2. stripe-get-charge
# ---------------------------------------------------------------------------
def get_charge(conn, args):
    """Get a single charge with related refunds, disputes, and balance transactions."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")

    charge_stripe_id = getattr(args, "charge_stripe_id", None)
    if not charge_stripe_id:
        err("--charge-stripe-id is required")

    t = Table("stripe_charge")
    charge = conn.execute(
        Q.from_(t).select("*").where(
            t.stripe_account_id == P()
        ).where(t.stripe_id == P()).get_sql(),
        (stripe_account_id, charge_stripe_id)
    ).fetchone()
    if not charge:
        err(f"Charge {charge_stripe_id} not found")

    result = row_to_dict(charge)

    # Related refunds
    rt = Table("stripe_refund")
    refunds = conn.execute(
        Q.from_(rt).select("*").where(
            rt.charge_stripe_id == P()
        ).where(rt.stripe_account_id == P()).get_sql(),
        (charge_stripe_id, stripe_account_id)
    ).fetchall()
    result["refunds"] = rows_to_list(refunds)

    # Related disputes
    dt = Table("stripe_dispute")
    disputes = conn.execute(
        Q.from_(dt).select("*").where(
            dt.charge_stripe_id == P()
        ).where(dt.stripe_account_id == P()).get_sql(),
        (charge_stripe_id, stripe_account_id)
    ).fetchall()
    result["disputes"] = rows_to_list(disputes)

    # Related balance transactions
    bt = Table("stripe_balance_transaction")
    balance_txns = conn.execute(
        Q.from_(bt).select("*").where(
            bt.source_id == P()
        ).where(bt.stripe_account_id == P()).get_sql(),
        (charge_stripe_id, stripe_account_id)
    ).fetchall()
    result["balance_transactions"] = rows_to_list(balance_txns)

    ok(result)


# ---------------------------------------------------------------------------
# 3. stripe-list-payouts
# ---------------------------------------------------------------------------
def list_payouts(conn, args):
    """List Stripe payouts with optional filters."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_payout")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)
    params = [stripe_account_id]

    status = getattr(args, "status", None)
    if status:
        q = q.where(t.status == P())
        params.append(status)

    limit = getattr(args, "limit", 50) or 50
    q = q.limit(limit)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({"payouts": rows_to_list(rows), "count": len(rows)})


# ---------------------------------------------------------------------------
# 4. stripe-get-payout
# ---------------------------------------------------------------------------
def get_payout(conn, args):
    """Get a single payout with constituent balance transactions and their charges."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")

    payout_stripe_id = getattr(args, "payout_stripe_id", None)
    if not payout_stripe_id:
        err("--payout-stripe-id is required")

    t = Table("stripe_payout")
    payout = conn.execute(
        Q.from_(t).select("*").where(
            t.stripe_account_id == P()
        ).where(t.stripe_id == P()).get_sql(),
        (stripe_account_id, payout_stripe_id)
    ).fetchone()
    if not payout:
        err(f"Payout {payout_stripe_id} not found")

    result = row_to_dict(payout)

    # Balance transactions for this payout
    bt = Table("stripe_balance_transaction")
    balance_txns = conn.execute(
        Q.from_(bt).select("*").where(
            bt.payout_id == P()
        ).where(bt.stripe_account_id == P()).get_sql(),
        (payout_stripe_id, stripe_account_id)
    ).fetchall()
    result["balance_transactions"] = rows_to_list(balance_txns)
    result["transaction_count"] = len(balance_txns)

    ok(result)


# ---------------------------------------------------------------------------
# 5. stripe-list-invoices
# ---------------------------------------------------------------------------
def list_invoices(conn, args):
    """List Stripe invoices with optional filters."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_invoice")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)
    params = [stripe_account_id]

    status = getattr(args, "status", None)
    if status:
        q = q.where(t.status == P())
        params.append(status)

    limit = getattr(args, "limit", 50) or 50
    q = q.limit(limit)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({"invoices": rows_to_list(rows), "count": len(rows)})


# ---------------------------------------------------------------------------
# 6. stripe-list-subscriptions
# ---------------------------------------------------------------------------
def list_subscriptions(conn, args):
    """List Stripe subscriptions with optional filters."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_subscription")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)
    params = [stripe_account_id]

    status = getattr(args, "status", None)
    if status:
        q = q.where(t.status == P())
        params.append(status)

    limit = getattr(args, "limit", 50) or 50
    q = q.limit(limit)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({"subscriptions": rows_to_list(rows), "count": len(rows)})


# ---------------------------------------------------------------------------
# 7. stripe-list-disputes
# ---------------------------------------------------------------------------
def list_disputes(conn, args):
    """List Stripe disputes with optional filters."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_dispute")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)
    params = [stripe_account_id]

    status = getattr(args, "status", None)
    if status:
        q = q.where(t.status == P())
        params.append(status)

    limit = getattr(args, "limit", 50) or 50
    q = q.limit(limit)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({"disputes": rows_to_list(rows), "count": len(rows)})


# ---------------------------------------------------------------------------
# 8. stripe-list-refunds
# ---------------------------------------------------------------------------
def list_refunds(conn, args):
    """List Stripe refunds with optional filters."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_refund")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)
    params = [stripe_account_id]

    limit = getattr(args, "limit", 50) or 50
    q = q.limit(limit)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({"refunds": rows_to_list(rows), "count": len(rows)})


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "stripe-list-charges": list_charges,
    "stripe-get-charge": get_charge,
    "stripe-list-payouts": list_payouts,
    "stripe-get-payout": get_payout,
    "stripe-list-invoices": list_invoices,
    "stripe-list-subscriptions": list_subscriptions,
    "stripe-list-disputes": list_disputes,
    "stripe-list-refunds": list_refunds,
}
