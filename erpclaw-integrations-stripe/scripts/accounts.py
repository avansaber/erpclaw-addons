"""ERPClaw Integrations Stripe — account management actions.

6 actions for Stripe account CRUD, GL mapping, and connection testing.
Imported by db_query.py (unified router).
"""
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.response import ok, err, row_to_dict
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

from stripe_helpers import (
    SKILL, VALID_MODES, VALID_ACCOUNT_STATUSES,
    now_iso, mask_key, encrypt_key, decrypt_key,
    validate_company, validate_account_exists, validate_enum,
)

# GL account definitions for auto-creation when adding a Stripe account
_GL_ACCOUNT_DEFS = [
    {
        "suffix": "Stripe Clearing",
        "root_type": "asset",
        "account_type": "bank",
        "balance_direction": "debit_normal",
        "mapping_field": "stripe_clearing_account_id",
    },
    {
        "suffix": "Stripe Processing Fees",
        "root_type": "expense",
        "account_type": "expense",
        "balance_direction": "debit_normal",
        "mapping_field": "stripe_fees_account_id",
    },
    {
        "suffix": "Stripe Payout",
        "root_type": "asset",
        "account_type": "bank",
        "balance_direction": "debit_normal",
        "mapping_field": "stripe_payout_account_id",
    },
    {
        "suffix": "Stripe Dispute Losses",
        "root_type": "expense",
        "account_type": "expense",
        "balance_direction": "debit_normal",
        "mapping_field": "dispute_expense_account_id",
    },
    {
        "suffix": "Stripe Unearned Revenue",
        "root_type": "liability",
        "account_type": "temporary",
        "balance_direction": "credit_normal",
        "mapping_field": "unearned_revenue_account_id",
    },
]


# ---------------------------------------------------------------------------
# 1. stripe-add-account
# ---------------------------------------------------------------------------
def add_account(conn, args):
    """Create a new Stripe account configuration with encrypted API key.

    Auto-creates 5 GL accounts (clearing, fees, payout, disputes, unearned revenue)
    and sets up the default GL mapping on the stripe_account record.
    """
    company_id = getattr(args, "company_id", None)
    validate_company(conn, company_id)

    account_name = getattr(args, "account_name", None)
    if not account_name:
        err("--account-name is required")

    api_key = getattr(args, "api_key", None)
    if not api_key:
        err("--api-key is required (Stripe restricted key)")

    mode = getattr(args, "mode", "test")
    validate_enum(mode, VALID_MODES, "mode")

    # Encrypt the API key before storage
    encrypted_key = encrypt_key(api_key)

    # Optional fields
    webhook_secret = getattr(args, "webhook_secret", None)
    webhook_enc = encrypt_key(webhook_secret) if webhook_secret else None
    is_connect = 1 if getattr(args, "is_connect_platform", None) else 0

    now = now_iso()
    acct_id = str(uuid.uuid4())

    # -- Auto-create 5 GL accounts for this Stripe configuration --
    gl_mapping = {}
    acct_table = Table("account")

    for gl_def in _GL_ACCOUNT_DEFS:
        gl_id = str(uuid.uuid4())
        gl_name = f"{account_name} - {gl_def['suffix']}"

        sql, _ = insert_row("account", {
            "id": P(), "name": P(), "root_type": P(), "account_type": P(),
            "currency": P(), "is_group": P(), "balance_direction": P(),
            "company_id": P(), "created_at": P(), "updated_at": P(),
        })
        conn.execute(sql, (
            gl_id, gl_name, gl_def["root_type"], gl_def["account_type"],
            "USD", 0, gl_def["balance_direction"],
            company_id, now, now,
        ))
        gl_mapping[gl_def["mapping_field"]] = gl_id

    # -- Insert the stripe_account row --
    sql, _ = insert_row("stripe_account", {
        "id": P(), "company_id": P(), "account_name": P(),
        "restricted_key_enc": P(), "webhook_secret_enc": P(),
        "mode": P(), "is_connect_platform": P(), "default_currency": P(),
        "stripe_clearing_account_id": P(), "stripe_fees_account_id": P(),
        "stripe_payout_account_id": P(), "dispute_expense_account_id": P(),
        "unearned_revenue_account_id": P(),
        "status": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        acct_id, company_id, account_name,
        encrypted_key, webhook_enc,
        mode, is_connect, "USD",
        gl_mapping["stripe_clearing_account_id"],
        gl_mapping["stripe_fees_account_id"],
        gl_mapping["stripe_payout_account_id"],
        gl_mapping["dispute_expense_account_id"],
        gl_mapping["unearned_revenue_account_id"],
        "active", now, now,
    ))

    audit(conn, SKILL, "stripe-add-account", "stripe_account", acct_id,
          new_values={"account_name": account_name, "mode": mode})
    conn.commit()

    ok({
        "id": acct_id,
        "account_name": account_name,
        "mode": mode,
        "account_status": "active",
        "api_key": mask_key(api_key),
        "gl_accounts_created": len(gl_mapping),
        "gl_mapping": {k: v for k, v in gl_mapping.items()},
    })


# ---------------------------------------------------------------------------
# 2. stripe-update-account
# ---------------------------------------------------------------------------
def update_account(conn, args):
    """Update an existing Stripe account configuration.

    Supports updating account_name, api_key (re-encrypts), mode, and status.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")

    t = Table("stripe_account")
    existing = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (stripe_account_id,)
    ).fetchone()
    if not existing:
        err(f"Stripe account {stripe_account_id} not found")

    data = {}
    account_name = getattr(args, "account_name", None)
    if account_name is not None:
        data["account_name"] = account_name

    api_key = getattr(args, "api_key", None)
    if api_key is not None:
        data["restricted_key_enc"] = encrypt_key(api_key)

    mode = getattr(args, "mode", None)
    if mode is not None:
        validate_enum(mode, VALID_MODES, "mode")
        data["mode"] = mode

    status = getattr(args, "status", None)
    if status is not None:
        validate_enum(status, VALID_ACCOUNT_STATUSES, "status")
        data["status"] = status

    webhook_secret = getattr(args, "webhook_secret", None)
    if webhook_secret is not None:
        data["webhook_secret_enc"] = encrypt_key(webhook_secret)

    if not data:
        err("No fields to update. Provide at least one of: --account-name, --api-key, --mode, --status, --webhook-secret")

    data["updated_at"] = now_iso()

    sql, params = dynamic_update("stripe_account", data, {"id": stripe_account_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "stripe-update-account", "stripe_account", stripe_account_id,
          new_values={k: v for k, v in data.items() if k != "restricted_key_enc"})
    conn.commit()

    ok({
        "id": stripe_account_id,
        "updated_fields": [k for k in data if k != "updated_at"],
    })


# ---------------------------------------------------------------------------
# 3. stripe-get-account
# ---------------------------------------------------------------------------
def get_account(conn, args):
    """Get a Stripe account configuration with masked API key."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")

    t = Table("stripe_account")
    row = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (stripe_account_id,)
    ).fetchone()
    if not row:
        err(f"Stripe account {stripe_account_id} not found")

    result = row_to_dict(row)
    # Mask sensitive fields — never expose raw keys
    if result.get("restricted_key_enc"):
        decrypted = decrypt_key(result["restricted_key_enc"])
        result["api_key_masked"] = mask_key(decrypted)
    del result["restricted_key_enc"]
    if result.get("webhook_secret_enc"):
        result["webhook_secret_masked"] = "***"
    result.pop("webhook_secret_enc", None)

    ok(result)


# ---------------------------------------------------------------------------
# 4. stripe-list-accounts
# ---------------------------------------------------------------------------
def list_accounts(conn, args):
    """List all Stripe account configurations for a company."""
    company_id = getattr(args, "company_id", None)
    validate_company(conn, company_id)

    t = Table("stripe_account")
    q = Q.from_(t).select(
        t.id, t.account_name, t.stripe_account_id, t.mode,
        t.is_connect_platform, t.default_currency, t.status,
        t.last_sync_at, t.created_at,
    ).where(t.company_id == P()).orderby(t.created_at, order=Order.desc)

    rows = conn.execute(q.get_sql(), (company_id,)).fetchall()
    accounts = [row_to_dict(r) for r in rows]
    ok({"accounts": accounts, "count": len(accounts)})


# ---------------------------------------------------------------------------
# 5. stripe-configure-gl-mapping
# ---------------------------------------------------------------------------
def configure_gl_mapping(conn, args):
    """Update GL account mappings on a Stripe account.

    Validates each account_id exists in the account table before updating.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")

    t = Table("stripe_account")
    existing = conn.execute(
        Q.from_(t).select(t.id).where(t.id == P()).get_sql(),
        (stripe_account_id,)
    ).fetchone()
    if not existing:
        err(f"Stripe account {stripe_account_id} not found")

    # Collect all GL mapping updates
    mapping_fields = {
        "clearing_account_id": "stripe_clearing_account_id",
        "fees_account_id": "stripe_fees_account_id",
        "payout_account_id": "stripe_payout_account_id",
        "dispute_account_id": "dispute_expense_account_id",
        "unearned_revenue_account_id": "unearned_revenue_account_id",
        "platform_revenue_account_id": "platform_revenue_account_id",
    }

    data = {}
    for arg_name, col_name in mapping_fields.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            validate_account_exists(conn, val, label=f"GL account ({arg_name})")
            data[col_name] = val

    if not data:
        err("No GL mapping fields provided. Use --clearing-account-id, --fees-account-id, "
            "--payout-account-id, --dispute-account-id, --unearned-revenue-account-id, "
            "or --platform-revenue-account-id")

    data["updated_at"] = now_iso()

    sql, params = dynamic_update("stripe_account", data, {"id": stripe_account_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "stripe-configure-gl-mapping", "stripe_account", stripe_account_id,
          new_values=data)
    conn.commit()

    ok({
        "id": stripe_account_id,
        "updated_mappings": [k for k in data if k != "updated_at"],
    })


# ---------------------------------------------------------------------------
# 6. stripe-test-connection
# ---------------------------------------------------------------------------
def test_connection(conn, args):
    """Test Stripe API connectivity by calling stripe.Account.retrieve().

    Verifies the stored API key works and returns basic Stripe account info.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")

    t = Table("stripe_account")
    row = conn.execute(
        Q.from_(t).select(t.restricted_key_enc, t.mode, t.account_name).where(t.id == P()).get_sql(),
        (stripe_account_id,)
    ).fetchone()
    if not row:
        err(f"Stripe account {stripe_account_id} not found")

    try:
        import stripe
    except ImportError:
        err("stripe Python package not installed. Run: pip install stripe")

    api_key = decrypt_key(row["restricted_key_enc"])
    stripe.api_key = api_key

    try:
        # Use Balance.retrieve() — works with restricted keys (rk_test_/rk_live_)
        # from the ERPClaw Accounting Stripe App. Account.retrieve() requires
        # rak_account_read which restricted app keys typically lack.
        balance_info = stripe.Balance.retrieve()
        now = now_iso()

        sql, params = dynamic_update("stripe_account", {
            "updated_at": now,
        }, {"id": stripe_account_id})
        conn.execute(sql, params)
        conn.commit()

        available = balance_info.get("available", [])
        pending = balance_info.get("pending", [])

        ok({
            "connection": "success",
            "available_balance": [{"amount": b["amount"], "currency": b["currency"]} for b in available],
            "pending_balance": [{"amount": b["amount"], "currency": b["currency"]} for b in pending],
            "mode": row["mode"],
        })
    except stripe.error.AuthenticationError:
        # Update status to error
        sql, params = dynamic_update("stripe_account", {
            "status": "error",
            "updated_at": now_iso(),
        }, {"id": stripe_account_id})
        conn.execute(sql, params)
        conn.commit()
        err("Stripe authentication failed. Check your restricted API key (rk_test_ or rk_live_).")
    except stripe.error.APIConnectionError:
        err("Could not connect to Stripe API. Check network connectivity.")
    except Exception as e:
        err(f"Stripe connection test failed: {str(e)}")


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "stripe-add-account": add_account,
    "stripe-update-account": update_account,
    "stripe-get-account": get_account,
    "stripe-list-accounts": list_accounts,
    "stripe-configure-gl-mapping": configure_gl_mapping,
    "stripe-test-connection": test_connection,
}
