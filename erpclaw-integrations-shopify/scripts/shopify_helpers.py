"""Shared helpers for erpclaw-integrations-shopify.

Provides encryption/decryption for access tokens, Shopify amount conversion
(string -> Decimal), GraphQL request helper, and common imports used by all
domain modules.
"""
import os
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal

# Auto-install requests if not present (transparent to user)
try:
    import requests as _requests_check  # noqa: F401
except ImportError:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "requests", "-q"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

try:
    import importlib.util
    if importlib.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import (
        Q, P, Table, Field, fn, Order,
        insert_row, update_row, dynamic_update,
    )
    # Shared, err()-exit validators (hoisted from this module in M31 H6).
    # Re-exported so existing `from shopify_helpers import validate_*` sites work.
    from erpclaw_lib.action_validators import (
        validate_company, validate_account_exists, validate_enum,
    )
    # Shared at-rest secret encryption (AES-256-GCM + legacy XOR read-back).
    from erpclaw_lib.integration_secrets import encrypt_secret, decrypt_secret

    ENTITY_PREFIXES.setdefault("shopify_account", "SHPFY-")
    ENTITY_PREFIXES.setdefault("shopify_sync_job", "SHPSYNC-")
    ENTITY_PREFIXES.setdefault("shopify_reconciliation_run", "SHPRECON-")
except ImportError:
    pass

SKILL = "erpclaw-integrations-shopify"

VALID_ACCOUNT_STATUSES = ("active", "paused", "error", "disabled")
VALID_DISCOUNT_METHODS = ("net", "gross")


def now_iso():
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def shopify_amount_to_decimal(amount_str):
    """Convert Shopify amount string to Decimal.

    Shopify returns amounts as strings like "118.00" in their GraphQL API.
    ERPClaw stores all amounts as TEXT Decimal.
    """
    if amount_str is None:
        return Decimal("0")
    return Decimal(str(amount_str))


def mask_token(token):
    """Mask access token for display: 'shpat_ab...xyz'.

    Never expose full access tokens in responses. This shows enough to identify
    the token without revealing the secret portion.
    """
    if not token or len(token) < 10:
        return "***"
    prefix = token[:8]
    suffix = token[-3:]
    return f"{prefix}...{suffix}"


def encrypt_token(plaintext):
    """Encrypt an access token / secret for at-rest storage (AES-256-GCM).

    Thin wrapper over the shared erpclaw_lib.integration_secrets primitive.
    Used for both the access token and the HMAC secret columns. New writes
    always use authenticated encryption (``enc:v2:...``); legacy XOR values
    are still readable by decrypt_token().
    """
    return encrypt_secret(plaintext)


def decrypt_token(ciphertext):
    """Decrypt a stored access token / secret.

    Reads the current GCM format and transparently falls back to the pre-M31
    XOR-salt format so existing installs keep working.
    """
    return decrypt_secret(ciphertext)


def graphql_request(shop_domain, access_token, query, variables=None):
    """Make a GraphQL request to the Shopify Admin API.

    Args:
        shop_domain: The myshopify.com domain (e.g., 'my-store.myshopify.com')
        access_token: Decrypted access token for X-Shopify-Access-Token header
        query: GraphQL query string
        variables: Optional dict of query variables

    Returns:
        The 'data' portion of the GraphQL response.

    Raises:
        Exception on HTTP or GraphQL errors.
    """
    import requests

    url = f"https://{shop_domain}/admin/api/2026-04/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise Exception(f"GraphQL error: {data['errors']}")
    return data.get("data", {})


def get_shopify_client(conn, shopify_account_id):
    """Get shop_domain and decrypted access_token for API calls.

    Returns a dict with 'shop_domain' and 'access_token', or None if
    account not found.
    """
    t = Table("shopify_account")
    row = conn.execute(
        Q.from_(t).select(t.shop_domain, t.access_token_enc, t.api_version)
        .where(t.id == P()).get_sql(),
        (shopify_account_id,)
    ).fetchone()
    if not row:
        return None
    return {
        "shop_domain": row["shop_domain"],
        "access_token": decrypt_token(row["access_token_enc"]),
        "api_version": row["api_version"],
    }


def validate_shopify_account(conn, shopify_account_id):
    """Validate that a shopify_account exists. Returns the row or calls err()."""
    if not shopify_account_id:
        err("--shopify-account-id is required")
    t = Table("shopify_account")
    row = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (shopify_account_id,)
    ).fetchone()
    if not row:
        err(f"Shopify account {shopify_account_id} not found")
    return row
