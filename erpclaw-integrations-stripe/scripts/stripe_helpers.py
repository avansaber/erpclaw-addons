"""Shared helpers for erpclaw-integrations-stripe.

Provides encryption/decryption for API keys, Stripe amount conversion
(cents <-> Decimal dollars), and common imports used by all domain modules.
"""
import os
import subprocess
import sys
import uuid
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
    # Re-exported so existing `from stripe_helpers import validate_*` sites work.
    from erpclaw_lib.action_validators import (
        validate_company, validate_account_exists, validate_enum,
    )
    # Shared at-rest secret encryption (AES-256-GCM + legacy XOR read-back).
    from erpclaw_lib.integration_secrets import encrypt_secret, decrypt_secret

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
    """Encrypt an API key for at-rest storage (AES-256-GCM via the master key).

    Thin wrapper over the shared erpclaw_lib.integration_secrets primitive.
    New writes always use authenticated encryption (``enc:v2:...``); legacy
    XOR values are still readable by decrypt_key().
    """
    return encrypt_secret(plaintext)


def decrypt_key(ciphertext):
    """Decrypt a stored API key.

    Reads the current GCM format and transparently falls back to the pre-M31
    XOR-salt format so existing installs keep working.
    """
    return decrypt_secret(ciphertext)


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


# ---------------------------------------------------------------------------
# Internal GL-posting helpers — single-sourced for gl_posting.py + rev_rec.py
# (M31 H6; these were byte-identical copies in both modules).
# ---------------------------------------------------------------------------

def _resolve_cost_center_id(conn, company_id, explicit_cc_id=None):
    """Resolve cost_center_id: use explicit value if given, else company default.

    P&L accounts (expense, income) require a cost_center_id for GL validation
    Step 6. This function auto-resolves from the company table when no explicit
    value is provided, so users don't need to know cost center IDs.

    Returns the cost_center_id string, or calls err() if none can be resolved.
    """
    if explicit_cc_id:
        return explicit_cc_id

    t = Table("company")
    row = conn.execute(
        Q.from_(t).select(t.default_cost_center_id)
        .where(t.id == P()).get_sql(),
        (company_id,)
    ).fetchone()

    if row and row["default_cost_center_id"]:
        return row["default_cost_center_id"]

    err("No cost_center_id provided and company has no default_cost_center_id. "
        "Set a default cost center on the company or pass --cost-center-id.")


def _get_stripe_account_gl(conn, stripe_account_id):
    """Load the stripe_account row and return GL account mapping dict.

    Returns dict with keys: stripe_clearing_account_id, stripe_fees_account_id,
    stripe_payout_account_id, dispute_expense_account_id,
    unearned_revenue_account_id, platform_revenue_account_id, company_id.
    """
    t = Table("stripe_account")
    row = conn.execute(
        Q.from_(t).select(
            t.company_id,
            t.stripe_clearing_account_id,
            t.stripe_fees_account_id,
            t.stripe_payout_account_id,
            t.dispute_expense_account_id,
            t.unearned_revenue_account_id,
            t.platform_revenue_account_id,
        ).where(t.id == P()).get_sql(),
        (stripe_account_id,)
    ).fetchone()
    if not row:
        err(f"Stripe account {stripe_account_id} not found")
    return dict(row)


def _create_journal_entry(conn, company_id, posting_date, total_amount,
                          entry_type="journal", remark=None, currency="USD"):
    """Insert a journal_entry row and return its ID.

    The Stripe module creates journal entries directly as the voucher
    document for GL posting (disputes, connect fees).

    `currency` is the transaction currency (ISO 4217). exchange_rate is
    always "1": ERPClaw books in transaction currency, never converts.
    """
    je_id = str(uuid.uuid4())
    now = now_iso()

    sql, _ = insert_row("journal_entry", {
        "id": P(), "posting_date": P(), "entry_type": P(),
        "total_debit": P(), "total_credit": P(),
        "currency": P(), "exchange_rate": P(), "remark": P(),
        "status": P(), "company_id": P(),
        "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        je_id, posting_date, entry_type,
        str(round_currency(total_amount)), str(round_currency(total_amount)),
        (currency or "USD").upper(), "1", remark or "",
        "submitted", company_id,
        now, now,
    ))
    return je_id
