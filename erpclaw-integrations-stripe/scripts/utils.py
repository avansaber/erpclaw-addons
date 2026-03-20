"""ERPClaw Integrations Stripe — utility/dashboard actions.

3 actions for status dashboard, GL balance verification, and health check.

Imported by db_query.py (unified router).
"""
import os
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.gl_posting import get_account_balance
    from erpclaw_lib.query import (
        Q, P, Table, Field, fn, Order,
    )
except ImportError:
    pass

# Add scripts directory to path for sibling imports
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_helpers import (
    SKILL, now_iso, validate_stripe_account, decrypt_key, mask_key,
)


# ---------------------------------------------------------------------------
# 1. stripe-status
# ---------------------------------------------------------------------------
def stripe_status(conn, args):
    """Dashboard: account count, last sync, unreconciled count, GL balance.

    Provides a quick overview of the entire Stripe integration state.
    """
    # Count accounts
    acct_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM stripe_account"
    ).fetchone()["cnt"]

    # Get last sync
    last_sync = conn.execute(
        "SELECT last_sync_at FROM stripe_account ORDER BY last_sync_at DESC LIMIT 1"
    ).fetchone()
    last_sync_at = last_sync["last_sync_at"] if last_sync else None

    # Unreconciled balance transactions
    unreconciled = conn.execute(
        """SELECT COUNT(*) as cnt, decimal_sum(amount) as total
           FROM stripe_balance_transaction WHERE reconciled = 0"""
    ).fetchone()
    unreconciled_count = unreconciled["cnt"] or 0
    unreconciled_amount = to_decimal(str(unreconciled["total"])) if unreconciled["total"] else Decimal("0")

    # Pending charges (succeeded but no GL)
    pending_charges = conn.execute(
        """SELECT COUNT(*) as cnt FROM stripe_charge
           WHERE status = 'succeeded' AND erpclaw_payment_entry_id IS NULL"""
    ).fetchone()["cnt"]

    # Pending payouts
    pending_payouts = conn.execute(
        """SELECT COUNT(*) as cnt FROM stripe_payout
           WHERE status = 'paid' AND erpclaw_payment_entry_id IS NULL"""
    ).fetchone()["cnt"]

    # Open disputes
    open_disputes = conn.execute(
        """SELECT COUNT(*) as cnt FROM stripe_dispute
           WHERE status IN ('needs_response', 'under_review',
                           'warning_needs_response', 'warning_under_review')"""
    ).fetchone()["cnt"]

    # GL clearing balance (across all accounts)
    clearing_balance = Decimal("0")
    clearing_accounts = conn.execute(
        "SELECT stripe_clearing_account_id FROM stripe_account WHERE stripe_clearing_account_id IS NOT NULL"
    ).fetchall()
    for ca in clearing_accounts:
        bal = get_account_balance(conn, ca["stripe_clearing_account_id"])
        clearing_balance += to_decimal(bal["balance"])

    ok({
        "stripe_accounts": acct_count,
        "last_sync_at": last_sync_at,
        "unreconciled_transactions": unreconciled_count,
        "unreconciled_amount": str(round_currency(unreconciled_amount)),
        "pending_charges": pending_charges,
        "pending_payouts": pending_payouts,
        "open_disputes": open_disputes,
        "clearing_balance": str(round_currency(clearing_balance)),
        "health": "ok" if unreconciled_count == 0 and pending_charges == 0 else "needs_attention",
    })


# ---------------------------------------------------------------------------
# 2. stripe-verify-gl-balance
# ---------------------------------------------------------------------------
def verify_gl_balance(conn, args):
    """Check Stripe Clearing account balance.

    The clearing account should approach zero when all payouts are reconciled.
    A non-zero balance indicates unreconciled transactions.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if stripe_account_id:
        # Specific account
        acct = validate_stripe_account(conn, stripe_account_id)
        clearing_id = acct["stripe_clearing_account_id"]
        if not clearing_id:
            err("Stripe clearing account not configured")

        bal = get_account_balance(conn, clearing_id)
        balance = to_decimal(bal["balance"])

        ok({
            "stripe_account_id": stripe_account_id,
            "clearing_account_id": clearing_id,
            "debit_total": bal["debit_total"],
            "credit_total": bal["credit_total"],
            "balance": bal["balance"],
            "balanced": balance == Decimal("0"),
            "status": "balanced" if balance == Decimal("0") else "unbalanced",
        })
    else:
        # All accounts
        accounts = conn.execute(
            "SELECT id, account_name, stripe_clearing_account_id FROM stripe_account WHERE status = 'active'"
        ).fetchall()

        results = []
        all_balanced = True
        for a in accounts:
            clearing_id = a["stripe_clearing_account_id"]
            if not clearing_id:
                results.append({
                    "stripe_account_id": a["id"],
                    "account_name": a["account_name"],
                    "status": "no_clearing_account",
                })
                continue

            bal = get_account_balance(conn, clearing_id)
            balance = to_decimal(bal["balance"])
            if balance != Decimal("0"):
                all_balanced = False

            results.append({
                "stripe_account_id": a["id"],
                "account_name": a["account_name"],
                "clearing_account_id": clearing_id,
                "balance": bal["balance"],
                "balanced": balance == Decimal("0"),
            })

        ok({
            "accounts": results,
            "all_balanced": all_balanced,
            "status": "balanced" if all_balanced else "unbalanced",
        })


# ---------------------------------------------------------------------------
# 3. stripe-health-check
# ---------------------------------------------------------------------------
def health_check(conn, args):
    """Verify API connectivity and check for stale syncs.

    1. Checks if stripe package is importable
    2. For each active account, checks if last_sync > 24h ago
    3. Returns overall health status
    """
    issues = []
    warnings = []

    # 1. Check stripe package
    try:
        import stripe
        stripe_available = True
    except ImportError:
        stripe_available = False
        issues.append("stripe Python package not installed")

    # 2. Check accounts for stale syncs
    accounts = conn.execute(
        "SELECT id, account_name, last_sync_at, status, restricted_key_enc FROM stripe_account WHERE status = 'active'"
    ).fetchall()

    now = datetime.now(timezone.utc)
    stale_threshold = now - timedelta(hours=24)
    stale_threshold_str = stale_threshold.strftime("%Y-%m-%dT%H:%M:%SZ")

    account_health = []
    for a in accounts:
        acct_info = {
            "stripe_account_id": a["id"],
            "account_name": a["account_name"],
            "last_sync_at": a["last_sync_at"],
            "has_api_key": bool(a["restricted_key_enc"]),
        }

        if not a["last_sync_at"]:
            warnings.append(f"Account '{a['account_name']}' has never been synced")
            acct_info["sync_status"] = "never_synced"
        elif a["last_sync_at"] < stale_threshold_str:
            warnings.append(f"Account '{a['account_name']}' last synced {a['last_sync_at']} (>24h ago)")
            acct_info["sync_status"] = "stale"
        else:
            acct_info["sync_status"] = "ok"

        if not a["restricted_key_enc"]:
            issues.append(f"Account '{a['account_name']}' has no API key configured")

        account_health.append(acct_info)

    # 3. Overall status
    if issues:
        overall = "unhealthy"
    elif warnings:
        overall = "warnings"
    else:
        overall = "healthy"

    ok({
        "overall_health": overall,
        "stripe_package_available": stripe_available,
        "active_accounts": len(accounts),
        "accounts": account_health,
        "issues": issues,
        "warnings": warnings,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "stripe-status": stripe_status,
    "stripe-verify-gl-balance": verify_gl_balance,
    "stripe-health-check": health_check,
}
