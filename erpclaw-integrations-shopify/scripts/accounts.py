"""ERPClaw Integrations Shopify — account management actions.

6 actions for Shopify account CRUD, GL mapping, and connection testing.
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

from shopify_helpers import (
    SKILL, VALID_ACCOUNT_STATUSES, VALID_DISCOUNT_METHODS,
    now_iso, mask_token, encrypt_token, decrypt_token,
    validate_company, validate_account_exists, validate_enum,
    graphql_request,
)

# GL account definitions for auto-creation when adding a Shopify account
_GL_ACCOUNT_DEFS = [
    {
        "suffix": "Shopify Clearing",
        "root_type": "asset",
        "account_type": "bank",
        "balance_direction": "debit_normal",
        "mapping_field": "clearing_account_id",
    },
    {
        "suffix": "Shopify Revenue",
        "root_type": "income",
        "account_type": "revenue",
        "balance_direction": "credit_normal",
        "mapping_field": "revenue_account_id",
    },
    {
        "suffix": "Shopify Shipping Revenue",
        "root_type": "income",
        "account_type": "revenue",
        "balance_direction": "credit_normal",
        "mapping_field": "shipping_revenue_account_id",
    },
    {
        "suffix": "Shopify Tax Payable",
        "root_type": "liability",
        "account_type": "tax",
        "balance_direction": "credit_normal",
        "mapping_field": "tax_payable_account_id",
    },
    {
        "suffix": "Shopify COGS",
        "root_type": "expense",
        "account_type": "expense",
        "balance_direction": "debit_normal",
        "mapping_field": "cogs_account_id",
    },
    {
        "suffix": "Shopify Inventory",
        "root_type": "asset",
        "account_type": "stock",
        "balance_direction": "debit_normal",
        "mapping_field": "inventory_account_id",
    },
    {
        "suffix": "Shopify Processing Fees",
        "root_type": "expense",
        "account_type": "expense",
        "balance_direction": "debit_normal",
        "mapping_field": "fee_account_id",
    },
    {
        "suffix": "Shopify Discounts",
        "root_type": "expense",
        "account_type": "expense",
        "balance_direction": "debit_normal",
        "mapping_field": "discount_account_id",
    },
    {
        "suffix": "Shopify Refunds",
        "root_type": "expense",
        "account_type": "expense",
        "balance_direction": "debit_normal",
        "mapping_field": "refund_account_id",
    },
    {
        "suffix": "Shopify Chargeback Losses",
        "root_type": "expense",
        "account_type": "expense",
        "balance_direction": "debit_normal",
        "mapping_field": "chargeback_account_id",
    },
    {
        "suffix": "Shopify Chargeback Fees",
        "root_type": "expense",
        "account_type": "expense",
        "balance_direction": "debit_normal",
        "mapping_field": "chargeback_fee_account_id",
    },
    {
        "suffix": "Shopify Gift Card Liability",
        "root_type": "liability",
        "account_type": "temporary",
        "balance_direction": "credit_normal",
        "mapping_field": "gift_card_liability_account_id",
    },
    {
        "suffix": "Shopify Reserve",
        "root_type": "asset",
        "account_type": "bank",
        "balance_direction": "debit_normal",
        "mapping_field": "reserve_account_id",
    },
    {
        "suffix": "Shopify Bank",
        "root_type": "asset",
        "account_type": "bank",
        "balance_direction": "debit_normal",
        "mapping_field": "bank_account_id",
    },
]


# ---------------------------------------------------------------------------
# 1. shopify-add-account
# ---------------------------------------------------------------------------
def add_account(conn, args):
    """Create a new Shopify account configuration with encrypted access token.

    Auto-creates 14 GL accounts (clearing, revenue, shipping revenue, tax payable,
    COGS, inventory, fees, discounts, refunds, chargebacks, chargeback fees,
    gift card liability, reserve, bank) and sets up the default GL mapping.
    """
    company_id = getattr(args, "company_id", None)
    validate_company(conn, company_id)

    shop_domain = getattr(args, "shop_domain", None)
    if not shop_domain:
        err("--shop-domain is required (e.g., my-store.myshopify.com)")

    access_token = getattr(args, "access_token", None)
    if not access_token:
        err("--access-token is required (Shopify Admin API access token)")

    # Encrypt the access token before storage
    encrypted_token = encrypt_token(access_token)

    # Optional fields
    shop_name = getattr(args, "shop_name", None) or shop_domain.split(".")[0]
    api_version = getattr(args, "api_version", None) or "2026-04"
    currency = getattr(args, "currency", None) or "USD"

    result = _add_account_core(
        conn,
        company_id=company_id,
        shop_domain=shop_domain,
        shop_name=shop_name,
        encrypted_token=encrypted_token,
        api_version=api_version,
        currency=currency,
        pairing_method="custom_app",
    )
    ok({
        "id": result["id"],
        "shop_domain": shop_domain,
        "shop_name": shop_name,
        "account_status": "active",
        "access_token": mask_token(access_token),
        "gl_accounts_created": result["gl_accounts_created"],
        "gl_mapping": result["gl_mapping"],
    })


def _add_account_core(
    conn,
    *,
    company_id,
    shop_domain,
    shop_name,
    encrypted_token,
    api_version="2026-04",
    currency="USD",
    pairing_method="custom_app",
    hmac_secret_enc=None,
    status_mode=None,
    erpclaw_url_override=None,
):
    """Shared core of shopify-add-account and shopify-connect.

    Auto-creates 14 GL accounts, inserts the shopify_account row with the
    v1.1 columns set when provided, writes an audit entry, commits.

    Returns: {'id': acct_id, 'gl_mapping': dict, 'gl_accounts_created': int}
    Does NOT print or sys.exit; caller is responsible for responding.
    """
    now = now_iso()
    acct_id = str(uuid.uuid4())

    # -- Auto-create 14 GL accounts for this Shopify configuration --
    gl_mapping = {}
    for gl_def in _GL_ACCOUNT_DEFS:
        gl_id = str(uuid.uuid4())
        gl_name = f"{shop_name} - {gl_def['suffix']}"

        sql, _ = insert_row("account", {
            "id": P(), "name": P(), "root_type": P(), "account_type": P(),
            "currency": P(), "is_group": P(), "balance_direction": P(),
            "company_id": P(), "created_at": P(), "updated_at": P(),
        })
        conn.execute(sql, (
            gl_id, gl_name, gl_def["root_type"], gl_def["account_type"],
            currency, 0, gl_def["balance_direction"],
            company_id, now, now,
        ))
        gl_mapping[gl_def["mapping_field"]] = gl_id

    # -- Insert the shopify_account row --
    row = {
        "id": acct_id,
        "company_id": company_id,
        "shop_domain": shop_domain,
        "shop_name": shop_name,
        "access_token_enc": encrypted_token,
        "api_version": api_version,
        "currency": currency,
        "status": "active",
        "clearing_account_id": gl_mapping["clearing_account_id"],
        "revenue_account_id": gl_mapping["revenue_account_id"],
        "shipping_revenue_account_id": gl_mapping["shipping_revenue_account_id"],
        "tax_payable_account_id": gl_mapping["tax_payable_account_id"],
        "cogs_account_id": gl_mapping["cogs_account_id"],
        "inventory_account_id": gl_mapping["inventory_account_id"],
        "fee_account_id": gl_mapping["fee_account_id"],
        "discount_account_id": gl_mapping["discount_account_id"],
        "refund_account_id": gl_mapping["refund_account_id"],
        "chargeback_account_id": gl_mapping["chargeback_account_id"],
        "chargeback_fee_account_id": gl_mapping["chargeback_fee_account_id"],
        "gift_card_liability_account_id": gl_mapping["gift_card_liability_account_id"],
        "reserve_account_id": gl_mapping["reserve_account_id"],
        "bank_account_id": gl_mapping["bank_account_id"],
        "discount_method": "net",
        "auto_post_gl": 0,
        "track_cogs": 0,
        "pairing_method": pairing_method,
        "created_at": now,
        "updated_at": now,
    }
    # Optional v1.1 columns: only included if non-null so init_db.py older
    # installs (pre-migration) don't break.
    if hmac_secret_enc is not None:
        row["hmac_secret_enc"] = hmac_secret_enc
    if status_mode is not None:
        row["status_mode"] = status_mode
    if erpclaw_url_override is not None:
        row["erpclaw_url_override"] = erpclaw_url_override

    columns = list(row.keys())
    placeholders = ", ".join("?" for _ in columns)
    column_list = ", ".join(columns)
    conn.execute(
        f"INSERT INTO shopify_account ({column_list}) VALUES ({placeholders})",
        tuple(row[c] for c in columns),
    )

    audit(conn, SKILL, "shopify-add-account", "shopify_account", acct_id,
          new_values={"shop_domain": shop_domain, "shop_name": shop_name})
    conn.commit()

    return {
        "id": acct_id,
        "gl_mapping": gl_mapping,
        "gl_accounts_created": len(gl_mapping),
    }


# ---------------------------------------------------------------------------
# 2. shopify-update-account
# ---------------------------------------------------------------------------
def update_account(conn, args):
    """Update an existing Shopify account configuration.

    Supports updating shop_name, access_token (re-encrypts), api_version,
    discount_method, auto_post_gl, track_cogs, and status.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    if not shopify_account_id:
        err("--shopify-account-id is required")

    t = Table("shopify_account")
    existing = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (shopify_account_id,)
    ).fetchone()
    if not existing:
        err(f"Shopify account {shopify_account_id} not found")

    data = {}
    shop_name = getattr(args, "shop_name", None)
    if shop_name is not None:
        data["shop_name"] = shop_name

    access_token = getattr(args, "access_token", None)
    if access_token is not None:
        data["access_token_enc"] = encrypt_token(access_token)

    api_version = getattr(args, "api_version", None)
    if api_version is not None:
        data["api_version"] = api_version

    status = getattr(args, "status", None)
    if status is not None:
        validate_enum(status, VALID_ACCOUNT_STATUSES, "status")
        data["status"] = status

    discount_method = getattr(args, "discount_method", None)
    if discount_method is not None:
        validate_enum(discount_method, VALID_DISCOUNT_METHODS, "discount_method")
        data["discount_method"] = discount_method

    auto_post_gl = getattr(args, "auto_post_gl", None)
    if auto_post_gl is not None:
        data["auto_post_gl"] = int(auto_post_gl)

    track_cogs = getattr(args, "track_cogs", None)
    if track_cogs is not None:
        data["track_cogs"] = int(track_cogs)

    if not data:
        err("No fields to update. Provide at least one of: --shop-name, "
            "--access-token, --api-version, --status, --discount-method, "
            "--auto-post-gl, --track-cogs")

    data["updated_at"] = now_iso()

    sql, params = dynamic_update("shopify_account", data, {"id": shopify_account_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "shopify-update-account", "shopify_account", shopify_account_id,
          new_values={k: v for k, v in data.items() if k != "access_token_enc"})
    conn.commit()

    ok({
        "id": shopify_account_id,
        "updated_fields": [k for k in data if k != "updated_at"],
    })


# ---------------------------------------------------------------------------
# 3. shopify-get-account
# ---------------------------------------------------------------------------
def get_account(conn, args):
    """Get a Shopify account configuration with masked access token."""
    shopify_account_id = getattr(args, "shopify_account_id", None)
    if not shopify_account_id:
        err("--shopify-account-id is required")

    t = Table("shopify_account")
    row = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (shopify_account_id,)
    ).fetchone()
    if not row:
        err(f"Shopify account {shopify_account_id} not found")

    result = row_to_dict(row)
    # Mask sensitive fields -- never expose raw tokens
    if result.get("access_token_enc"):
        decrypted = decrypt_token(result["access_token_enc"])
        result["access_token_masked"] = mask_token(decrypted)
    del result["access_token_enc"]

    ok(result)


# ---------------------------------------------------------------------------
# 4. shopify-list-accounts
# ---------------------------------------------------------------------------
def list_accounts(conn, args):
    """List all Shopify account configurations for a company."""
    company_id = getattr(args, "company_id", None)
    validate_company(conn, company_id)

    t = Table("shopify_account")
    q = Q.from_(t).select(
        t.id, t.shop_domain, t.shop_name, t.api_version,
        t.currency, t.status, t.discount_method,
        t.auto_post_gl, t.track_cogs,
        t.last_orders_sync_at, t.created_at,
    ).where(t.company_id == P()).orderby(t.created_at, order=Order.desc)

    rows = conn.execute(q.get_sql(), (company_id,)).fetchall()
    accounts = [row_to_dict(r) for r in rows]
    ok({"accounts": accounts, "count": len(accounts)})


# ---------------------------------------------------------------------------
# 5. shopify-configure-gl
# ---------------------------------------------------------------------------
def configure_gl(conn, args):
    """Update GL account mappings on a Shopify account.

    Validates each account_id exists in the account table before updating.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    if not shopify_account_id:
        err("--shopify-account-id is required")

    t = Table("shopify_account")
    existing = conn.execute(
        Q.from_(t).select(t.id).where(t.id == P()).get_sql(),
        (shopify_account_id,)
    ).fetchone()
    if not existing:
        err(f"Shopify account {shopify_account_id} not found")

    # Collect all GL mapping updates
    mapping_fields = {
        "clearing_account_id": "clearing_account_id",
        "revenue_account_id": "revenue_account_id",
        "shipping_revenue_account_id": "shipping_revenue_account_id",
        "tax_payable_account_id": "tax_payable_account_id",
        "cogs_account_id": "cogs_account_id",
        "inventory_account_id": "inventory_account_id",
        "fee_account_id": "fee_account_id",
        "discount_account_id": "discount_account_id",
        "refund_account_id": "refund_account_id",
        "chargeback_account_id": "chargeback_account_id",
        "chargeback_fee_account_id": "chargeback_fee_account_id",
        "gift_card_liability_account_id": "gift_card_liability_account_id",
        "reserve_account_id": "reserve_account_id",
        "bank_account_id": "bank_account_id",
    }

    data = {}
    for arg_name, col_name in mapping_fields.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            validate_account_exists(conn, val, label=f"GL account ({arg_name})")
            data[col_name] = val

    if not data:
        err("No GL mapping fields provided. Use --clearing-account-id, "
            "--revenue-account-id, --fee-account-id, etc.")

    data["updated_at"] = now_iso()

    sql, params = dynamic_update("shopify_account", data, {"id": shopify_account_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "shopify-configure-gl", "shopify_account", shopify_account_id,
          new_values=data)
    conn.commit()

    ok({
        "id": shopify_account_id,
        "updated_mappings": [k for k in data if k != "updated_at"],
    })


# ---------------------------------------------------------------------------
# 6. shopify-test-connection
# ---------------------------------------------------------------------------
def test_connection(conn, args):
    """Test Shopify API connectivity by calling { shop { name url } }.

    Verifies the stored access token works and returns basic shop info.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    if not shopify_account_id:
        err("--shopify-account-id is required")

    t = Table("shopify_account")
    row = conn.execute(
        Q.from_(t).select(t.access_token_enc, t.shop_domain, t.shop_name)
        .where(t.id == P()).get_sql(),
        (shopify_account_id,)
    ).fetchone()
    if not row:
        err(f"Shopify account {shopify_account_id} not found")

    access_token = decrypt_token(row["access_token_enc"])
    shop_domain = row["shop_domain"]

    try:
        data = graphql_request(
            shop_domain, access_token,
            "{ shop { name url myshopifyDomain currencyCode } }"
        )
        shop_info = data.get("shop", {})
        now = now_iso()

        # Update shop_name from Shopify's response
        update_data = {"updated_at": now}
        remote_name = shop_info.get("name")
        if remote_name:
            update_data["shop_name"] = remote_name

        sql, params = dynamic_update("shopify_account", update_data,
                                     {"id": shopify_account_id})
        conn.execute(sql, params)
        conn.commit()

        ok({
            "connection": "success",
            "shop_name": shop_info.get("name"),
            "shop_url": shop_info.get("url"),
            "myshopify_domain": shop_info.get("myshopifyDomain"),
            "currency": shop_info.get("currencyCode"),
        })
    except Exception as e:
        # Update status to error
        sql, params = dynamic_update("shopify_account", {
            "status": "error",
            "updated_at": now_iso(),
        }, {"id": shopify_account_id})
        conn.execute(sql, params)
        conn.commit()
        err(f"Shopify connection test failed: {str(e)}")


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "shopify-add-account": add_account,
    "shopify-update-account": update_account,
    "shopify-get-account": get_account,
    "shopify-list-accounts": list_accounts,
    "shopify-configure-gl": configure_gl,
    "shopify-test-connection": test_connection,
}
