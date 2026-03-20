"""Shared helpers for erpclaw-integrations-stripe.

Provides encryption/decryption for API keys, Stripe amount conversion
(cents <-> Decimal dollars), and common imports used by all domain modules.
"""
import base64
import hashlib
import os
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal

# Auto-install stripe SDK if not present (transparent to user)
try:
    import stripe as _stripe_check  # noqa: F401
except ImportError:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "stripe", "-q"],
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

    ENTITY_PREFIXES.setdefault("stripe_account", "STRP-")
    ENTITY_PREFIXES.setdefault("stripe_sync_job", "SYNC-")
    ENTITY_PREFIXES.setdefault("stripe_reconciliation_run", "RECON-")
except ImportError:
    pass

SKILL = "erpclaw-integrations-stripe"

VALID_MODES = ("test", "live")
VALID_ACCOUNT_STATUSES = ("active", "paused", "error", "disabled")


def now_iso():
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def cents_to_decimal(cents):
    """Convert Stripe cents (integer) to Decimal dollars.

    Stripe represents amounts in the smallest currency unit (cents for USD).
    ERPClaw stores all amounts as TEXT Decimal in dollars.
    """
    if cents is None:
        return Decimal("0")
    return Decimal(str(cents)) / Decimal("100")


def decimal_to_cents(amount):
    """Convert Decimal dollars to Stripe cents (integer).

    Used when creating Stripe API calls that expect cent amounts.
    """
    return int(round_currency(amount) * 100)


def mask_key(key):
    """Mask API key for display: 'rk_test_...abc'.

    Never expose full API keys in responses. This shows enough to identify
    the key without revealing the secret portion.
    """
    if not key or len(key) < 10:
        return "***"
    prefix = key[:8]
    suffix = key[-3:]
    return f"{prefix}...{suffix}"


def encrypt_key(plaintext):
    """Encrypt API key for storage using XOR with machine-specific salt.

    Uses base64 encoding over XOR cipher with a salt derived from the
    user's home directory path. This provides basic obfuscation — keys
    are not stored in plaintext but this is not cryptographic-grade
    encryption. For production, use a proper secrets manager.
    """
    if not plaintext:
        return ""
    salt = hashlib.sha256(os.path.expanduser("~").encode()).digest()
    encrypted = bytes(b ^ salt[i % len(salt)] for i, b in enumerate(plaintext.encode()))
    return base64.b64encode(encrypted).decode()


def decrypt_key(ciphertext):
    """Decrypt API key from storage.

    Reverses the encrypt_key() operation.
    """
    if not ciphertext:
        return ""
    salt = hashlib.sha256(os.path.expanduser("~").encode()).digest()
    decoded = base64.b64decode(ciphertext.encode())
    decrypted = bytes(b ^ salt[i % len(salt)] for i, b in enumerate(decoded))
    return decrypted.decode()


def get_stripe_client(conn, stripe_account_id):
    """Get a configured stripe module with the decrypted API key.

    Returns the stripe module with api_key set, or None if account not found.
    Requires the `stripe` package to be installed.
    """
    import stripe
    t = Table("stripe_account")
    row = conn.execute(
        Q.from_(t).select(t.restricted_key_enc, t.mode).where(t.id == P()).get_sql(),
        (stripe_account_id,)
    ).fetchone()
    if not row:
        return None
    stripe.api_key = decrypt_key(row["restricted_key_enc"])
    return stripe


def timestamp_to_iso(ts):
    """Convert Unix timestamp (from Stripe API) to ISO-8601 string."""
    if not ts:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def validate_stripe_account(conn, stripe_account_id):
    """Validate that a stripe_account exists. Returns the row or calls err()."""
    if not stripe_account_id:
        err("--stripe-account-id is required")
    t = Table("stripe_account")
    row = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (stripe_account_id,)
    ).fetchone()
    if not row:
        err(f"Stripe account {stripe_account_id} not found")
    return row


def validate_enum(value, valid_values, field_name):
    """Validate that a value is in the allowed set. Calls err() if invalid."""
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")
