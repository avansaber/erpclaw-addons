"""Shared helpers for erpclaw-integrations-shopify.

Provides encryption/decryption for access tokens, Shopify amount conversion
(string -> Decimal), GraphQL request helper, and common imports used by all
domain modules.
"""
import base64
import hashlib
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
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import (
        Q, P, Table, Field, fn, Order,
        insert_row, update_row, dynamic_update,
    )

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
    """Encrypt access token for storage using XOR with machine-specific salt.

    Uses base64 encoding over XOR cipher with a salt derived from the
    user's home directory path. This provides basic obfuscation -- tokens
    are not stored in plaintext but this is not cryptographic-grade
    encryption. For production, use a proper secrets manager.
    """
    if not plaintext:
        return ""
    salt = hashlib.sha256(os.path.expanduser("~").encode()).digest()
    encrypted = bytes(b ^ salt[i % len(salt)] for i, b in enumerate(plaintext.encode()))
    return base64.b64encode(encrypted).decode()


def decrypt_token(ciphertext):
    """Decrypt access token from storage.

    Reverses the encrypt_token() operation.
    """
    if not ciphertext:
        return ""
    salt = hashlib.sha256(os.path.expanduser("~").encode()).digest()
    decoded = base64.b64decode(ciphertext.encode())
    decrypted = bytes(b ^ salt[i % len(salt)] for i, b in enumerate(decoded))
    return decrypted.decode()


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

    url = f"https://{shop_domain}/admin/api/2026-01/graphql.json"
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


def validate_company(conn, company_id):
    """Validate that a company exists. Calls err() and exits if not found."""
    if not company_id:
        err("--company-id is required")
    t = Table("company")
    row = conn.execute(
        Q.from_(t).select(t.id).where(t.id == P()).get_sql(),
        (company_id,)
    ).fetchone()
    if not row:
        err(f"Company {company_id} not found")


def validate_account_exists(conn, account_id, label="Account"):
    """Validate that a GL account exists. Calls err() and exits if not found."""
    if not account_id:
        return  # Optional field
    t = Table("account")
    row = conn.execute(
        Q.from_(t).select(t.id).where(t.id == P()).get_sql(),
        (account_id,)
    ).fetchone()
    if not row:
        err(f"{label} {account_id} not found in chart of accounts")


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


def validate_enum(value, valid_values, field_name):
    """Validate that a value is in the allowed set. Calls err() if invalid."""
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")
