"""ERPClaw Integrations Stripe — GL posting actions.

6 actions for creating GL entries from synced Stripe data:
charges, refunds, disputes, payouts, Connect fees, and bulk posting.

Uses erpclaw_lib.gl_posting.insert_gl_entries() for ALL GL writes (Article 6).
Creates payment_entry / journal_entry rows as the voucher documents.

Imported by db_query.py (unified router).
"""
import io
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.gl_posting import insert_gl_entries, reverse_gl_entries
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
    SKILL, now_iso, validate_stripe_account,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _today():
    """Return today's date as YYYY-MM-DD string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


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


def _call_silently(fn, conn, args):
    """Call an action function, suppressing stdout (ok/err output).

    Used by bulk_post_gl to invoke individual posting actions without
    their stdout JSON clobbering the bulk response. The action's DB
    side-effects are preserved; only stdout is swallowed.

    Raises SystemExit on success (from ok()), which is re-raised.
    Raises any other exception on failure.
    """
    buf = io.StringIO()
    try:
        with patch("sys.stdout", buf):
            fn(conn, args)
    except SystemExit as e:
        # ok() calls sys.exit(0) — this is success
        if e.code == 0 or e.code is None:
            return
        raise


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


def _find_customer_for_charge(conn, stripe_account_id, customer_stripe_id):
    """Look up the erpclaw_customer_id from stripe_customer_map for a charge.

    Returns (erpclaw_customer_id, party_type) or (None, None) if not mapped.
    """
    if not customer_stripe_id:
        return None, None

    t = Table("stripe_customer_map")
    row = conn.execute(
        Q.from_(t).select(t.erpclaw_customer_id)
        .where(t.stripe_account_id == P())
        .where(t.stripe_customer_id == P())
        .get_sql(),
        (stripe_account_id, customer_stripe_id)
    ).fetchone()

    if row and row["erpclaw_customer_id"]:
        return row["erpclaw_customer_id"], "customer"
    return None, None


def _create_payment_entry(conn, company_id, payment_type, posting_date,
                          paid_from, paid_to, amount, party_type=None,
                          party_id=None, reference_number=None, is_return=0):
    """Insert a payment_entry row and return its ID.

    The Stripe module creates payment entries directly as the voucher
    document for GL posting.
    """
    pe_id = str(uuid.uuid4())
    now = now_iso()

    sql, _ = insert_row("payment_entry", {
        "id": P(), "payment_type": P(), "posting_date": P(),
        "party_type": P(), "party_id": P(),
        "paid_from_account": P(), "paid_to_account": P(),
        "paid_amount": P(), "received_amount": P(),
        "payment_currency": P(), "exchange_rate": P(),
        "reference_number": P(), "reference_date": P(),
        "status": P(), "unallocated_amount": P(),
        "company_id": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        pe_id, payment_type, posting_date,
        party_type, party_id,
        paid_from, paid_to,
        str(round_currency(amount)), str(round_currency(amount)),
        "USD", "1",
        reference_number, posting_date,
        "submitted", str(round_currency(amount)),
        company_id, now, now,
    ))
    return pe_id


def _create_journal_entry(conn, company_id, posting_date, total_amount,
                          entry_type="journal", remark=None):
    """Insert a journal_entry row and return its ID.

    The Stripe module creates journal entries directly as the voucher
    document for GL posting (disputes, connect fees).
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
        "USD", "1", remark or "",
        "submitted", company_id,
        now, now,
    ))
    return je_id


def _mark_balance_transactions_reconciled(conn, stripe_account_id,
                                          source_stripe_id, voucher_id):
    """Mark balance_transaction rows as reconciled for a given source.

    Sets reconciled=1, gl_voucher_id, reconciled_at on matching rows.
    """
    now = now_iso()
    conn.execute(
        """UPDATE stripe_balance_transaction
           SET reconciled = 1, gl_voucher_id = ?, gl_voucher_type = 'payment_entry',
               reconciled_at = ?
           WHERE stripe_account_id = ? AND source_id = ? AND reconciled = 0""",
        (voucher_id, now, stripe_account_id, source_stripe_id)
    )


# ---------------------------------------------------------------------------
# 1. stripe-post-charge-gl
# ---------------------------------------------------------------------------
def post_charge_gl(conn, args):
    """Post GL entries for a Stripe charge.

    Creates payment_entry (type='receive'):
      DR Stripe Clearing (net amount)
      DR Stripe Fees (fee amount)
      CR Revenue/AR or Unearned Revenue (gross amount)

    If customer is mapped: party_type='customer', party_id=erpclaw_customer_id,
    and CR goes to the clearing account (receivable with party).
    If no customer mapped: CR goes to Unearned Revenue.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    acct_row = validate_stripe_account(conn, stripe_account_id)

    charge_stripe_id = getattr(args, "charge_stripe_id", None)
    if not charge_stripe_id:
        err("--charge-stripe-id is required")

    # Find the charge
    t = Table("stripe_charge")
    charge = conn.execute(
        Q.from_(t).select("*").where(
            t.stripe_account_id == P()
        ).where(t.stripe_id == P()).get_sql(),
        (stripe_account_id, charge_stripe_id)
    ).fetchone()
    if not charge:
        err(f"Charge {charge_stripe_id} not found for account {stripe_account_id}")

    # Check if already posted
    if charge["erpclaw_payment_entry_id"]:
        err(f"Charge {charge_stripe_id} already posted with payment entry {charge['erpclaw_payment_entry_id']}")

    gl = _get_stripe_account_gl(conn, stripe_account_id)
    company_id = gl["company_id"]

    # Auto-resolve cost_center_id for P&L accounts (expense/income)
    explicit_cc = getattr(args, "cost_center_id", None)
    cost_center_id = _resolve_cost_center_id(conn, company_id, explicit_cc)

    # Get fee from balance_transaction
    bt_table = Table("stripe_balance_transaction")
    bt = conn.execute(
        Q.from_(bt_table).select(bt_table.amount, bt_table.fee, bt_table.net)
        .where(bt_table.stripe_account_id == P())
        .where(bt_table.source_id == P())
        .get_sql(),
        (stripe_account_id, charge_stripe_id)
    ).fetchone()

    gross = to_decimal(charge["amount"])
    if bt:
        fee = to_decimal(bt["fee"])
        net = to_decimal(bt["net"])
    else:
        fee = Decimal("0")
        net = gross

    posting_date = _today()

    # Find mapped customer
    customer_id, party_type = _find_customer_for_charge(
        conn, stripe_account_id, charge["customer_stripe_id"])

    # ASC 606 subscription-aware credit account selection:
    # If this charge is linked to a subscription that has an ASC 606
    # revenue contract, ALWAYS credit Unearned Revenue (deferred),
    # regardless of customer mapping. Revenue is recognized later
    # via stripe-recognize-subscription-revenue.
    is_asc606_sub = False
    if charge["invoice_stripe_id"]:
        inv_t = Table("stripe_invoice")
        inv_row = conn.execute(
            Q.from_(inv_t).select(inv_t.subscription_stripe_id)
            .where(inv_t.stripe_account_id == P())
            .where(inv_t.stripe_id == P())
            .get_sql(),
            (stripe_account_id, charge["invoice_stripe_id"])
        ).fetchone()
        if inv_row and inv_row["subscription_stripe_id"]:
            sub_t = Table("stripe_subscription")
            sub_row = conn.execute(
                Q.from_(sub_t).select(sub_t.erpclaw_revenue_contract_id)
                .where(sub_t.stripe_account_id == P())
                .where(sub_t.stripe_id == P())
                .get_sql(),
                (stripe_account_id, inv_row["subscription_stripe_id"])
            ).fetchone()
            if sub_row and sub_row["erpclaw_revenue_contract_id"]:
                is_asc606_sub = True

    # Determine credit account
    # Clearing account is bank type (no party needed).
    # Unearned Revenue is temporary/liability type (no party needed).
    if is_asc606_sub:
        # ASC 606: always defer subscription revenue
        credit_account = gl["unearned_revenue_account_id"]
    elif customer_id:
        credit_account = gl["stripe_clearing_account_id"]
    else:
        credit_account = gl["unearned_revenue_account_id"]

    # Build GL entries
    entries = []

    # DR Stripe Clearing (net amount) — bank/transit account
    if net > 0:
        entries.append({
            "account_id": gl["stripe_clearing_account_id"],
            "debit": str(round_currency(net)),
            "credit": "0",
        })

    # DR Stripe Fees (fee amount) — expense account, needs cost_center_id
    if fee > 0:
        entries.append({
            "account_id": gl["stripe_fees_account_id"],
            "debit": str(round_currency(fee)),
            "credit": "0",
            "cost_center_id": cost_center_id,
        })

    # CR Revenue/Unearned (gross amount)
    entries.append({
        "account_id": credit_account,
        "debit": "0",
        "credit": str(round_currency(gross)),
    })

    # Create payment_entry as the voucher
    pe_id = _create_payment_entry(
        conn, company_id, "receive", posting_date,
        paid_from=credit_account,
        paid_to=gl["stripe_clearing_account_id"],
        amount=gross,
        party_type=party_type,
        party_id=customer_id,
        reference_number=charge_stripe_id,
    )

    # Post GL entries via the sanctioned API
    gl_ids = insert_gl_entries(
        conn, entries,
        voucher_type="payment_entry",
        voucher_id=pe_id,
        posting_date=posting_date,
        company_id=company_id,
        remarks=f"Stripe charge {charge_stripe_id}",
    )

    # Update the charge with the payment entry ID
    sql, params = dynamic_update("stripe_charge", {
        "erpclaw_payment_entry_id": pe_id,
    }, {"id": charge["id"]})
    conn.execute(sql, params)

    # Mark balance transactions reconciled
    _mark_balance_transactions_reconciled(
        conn, stripe_account_id, charge_stripe_id, pe_id)

    audit(conn, SKILL, "stripe-post-charge-gl", "stripe_charge", charge["id"],
          new_values={"payment_entry_id": pe_id})
    conn.commit()

    ok({
        "charge_stripe_id": charge_stripe_id,
        "payment_entry_id": pe_id,
        "gl_entry_count": len(gl_ids),
        "gross": str(round_currency(gross)),
        "fee": str(round_currency(fee)),
        "net": str(round_currency(net)),
        "customer_mapped": customer_id is not None,
    })


# ---------------------------------------------------------------------------
# 2. stripe-post-refund-gl
# ---------------------------------------------------------------------------
def post_refund_gl(conn, args):
    """Post GL entries for a Stripe refund.

    Creates payment_entry (type='pay', is_return=1):
      DR Revenue/AR or Unearned Revenue (refund amount)
      CR Stripe Clearing (refund amount)
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    refund_stripe_id = getattr(args, "refund_stripe_id", None)
    if not refund_stripe_id:
        err("--refund-stripe-id is required")

    # Find the refund
    t = Table("stripe_refund")
    refund = conn.execute(
        Q.from_(t).select("*").where(
            t.stripe_account_id == P()
        ).where(t.stripe_id == P()).get_sql(),
        (stripe_account_id, refund_stripe_id)
    ).fetchone()
    if not refund:
        err(f"Refund {refund_stripe_id} not found for account {stripe_account_id}")

    if refund["erpclaw_payment_entry_id"]:
        err(f"Refund {refund_stripe_id} already posted with payment entry {refund['erpclaw_payment_entry_id']}")

    gl = _get_stripe_account_gl(conn, stripe_account_id)
    company_id = gl["company_id"]
    refund_amount = to_decimal(refund["amount"])
    posting_date = _today()

    # Look up original charge to find customer
    customer_id = None
    party_type = None
    if refund["charge_stripe_id"]:
        ct = Table("stripe_charge")
        charge = conn.execute(
            Q.from_(ct).select(ct.customer_stripe_id)
            .where(ct.stripe_account_id == P())
            .where(ct.stripe_id == P()).get_sql(),
            (stripe_account_id, refund["charge_stripe_id"])
        ).fetchone()
        if charge and charge["customer_stripe_id"]:
            customer_id, party_type = _find_customer_for_charge(
                conn, stripe_account_id, charge["customer_stripe_id"])

    # Determine debit account (reverse of charge CR)
    # Both clearing (bank) and unearned revenue (temporary) are non-AR/AP
    if customer_id:
        debit_account = gl["stripe_clearing_account_id"]
    else:
        debit_account = gl["unearned_revenue_account_id"]

    entries = [
        # DR Revenue/Unearned (refund amount)
        {
            "account_id": debit_account,
            "debit": str(round_currency(refund_amount)),
            "credit": "0",
        },
        # CR Stripe Clearing (refund amount)
        {
            "account_id": gl["stripe_clearing_account_id"],
            "debit": "0",
            "credit": str(round_currency(refund_amount)),
        },
    ]

    pe_id = _create_payment_entry(
        conn, company_id, "pay", posting_date,
        paid_from=gl["stripe_clearing_account_id"],
        paid_to=debit_account,
        amount=refund_amount,
        party_type=party_type,
        party_id=customer_id,
        reference_number=refund_stripe_id,
        is_return=1,
    )

    gl_ids = insert_gl_entries(
        conn, entries,
        voucher_type="payment_entry",
        voucher_id=pe_id,
        posting_date=posting_date,
        company_id=company_id,
        remarks=f"Stripe refund {refund_stripe_id}",
    )

    sql, params = dynamic_update("stripe_refund", {
        "erpclaw_payment_entry_id": pe_id,
    }, {"id": refund["id"]})
    conn.execute(sql, params)

    _mark_balance_transactions_reconciled(
        conn, stripe_account_id, refund_stripe_id, pe_id)

    audit(conn, SKILL, "stripe-post-refund-gl", "stripe_refund", refund["id"],
          new_values={"payment_entry_id": pe_id})
    conn.commit()

    ok({
        "refund_stripe_id": refund_stripe_id,
        "payment_entry_id": pe_id,
        "gl_entry_count": len(gl_ids),
        "refund_amount": str(round_currency(refund_amount)),
    })


# ---------------------------------------------------------------------------
# 3. stripe-post-dispute-gl
# ---------------------------------------------------------------------------
def post_dispute_gl(conn, args):
    """Post GL entries for a Stripe dispute (chargeback).

    For open disputes (needs_response, under_review, warning_*):
      Creates journal_entry:
        DR Dispute Receivable (dispute amount + $15 fee)
        CR Stripe Clearing (dispute amount + $15 fee)

    For won disputes: reverse the original journal entry.

    For lost disputes:
      Creates journal_entry:
        DR Dispute Losses (dispute amount)
        CR Dispute Receivable (dispute amount)
      (Original hold entries stay)
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    dispute_stripe_id = getattr(args, "dispute_stripe_id", None)
    if not dispute_stripe_id:
        err("--dispute-stripe-id is required")

    # Find the dispute
    t = Table("stripe_dispute")
    dispute = conn.execute(
        Q.from_(t).select("*").where(
            t.stripe_account_id == P()
        ).where(t.stripe_id == P()).get_sql(),
        (stripe_account_id, dispute_stripe_id)
    ).fetchone()
    if not dispute:
        err(f"Dispute {dispute_stripe_id} not found for account {stripe_account_id}")

    gl = _get_stripe_account_gl(conn, stripe_account_id)
    company_id = gl["company_id"]
    dispute_amount = to_decimal(dispute["amount"])
    posting_date = _today()
    dispute_fee = Decimal("15.00")  # Standard Stripe dispute fee

    # Auto-resolve cost_center_id for P&L accounts (expense)
    explicit_cc = getattr(args, "cost_center_id", None)
    cost_center_id = _resolve_cost_center_id(conn, company_id, explicit_cc)

    status = dispute["status"]

    if status == "won":
        # Reverse original dispute entries
        original_je_id = dispute["erpclaw_journal_entry_id"]
        if not original_je_id:
            err(f"Dispute {dispute_stripe_id} has no original journal entry to reverse")

        reversal_ids = reverse_gl_entries(
            conn,
            voucher_type="journal_entry",
            voucher_id=original_je_id,
            posting_date=posting_date,
        )

        # Update journal entry status
        sql, params = dynamic_update("journal_entry", {
            "status": "cancelled",
            "updated_at": now_iso(),
        }, {"id": original_je_id})
        conn.execute(sql, params)

        audit(conn, SKILL, "stripe-post-dispute-gl", "stripe_dispute", dispute["id"],
              new_values={"action": "won_reversal", "reversal_count": len(reversal_ids)})
        conn.commit()

        ok({
            "dispute_stripe_id": dispute_stripe_id,
            "action": "won_reversal",
            "original_journal_entry_id": original_je_id,
            "gl_reversal_count": len(reversal_ids),
        })

    elif status == "lost":
        # DR Dispute Losses, CR Dispute Receivable (clearing)
        total = dispute_amount

        entries = [
            # DR Dispute Losses (expense)
            {
                "account_id": gl["dispute_expense_account_id"],
                "debit": str(round_currency(total)),
                "credit": "0",
                "cost_center_id": cost_center_id,
            },
            # CR Stripe Clearing (release the hold)
            {
                "account_id": gl["stripe_clearing_account_id"],
                "debit": "0",
                "credit": str(round_currency(total)),
            },
        ]

        je_id = _create_journal_entry(
            conn, company_id, posting_date, total,
            remark=f"Stripe dispute lost {dispute_stripe_id}",
        )

        gl_ids = insert_gl_entries(
            conn, entries,
            voucher_type="journal_entry",
            voucher_id=je_id,
            posting_date=posting_date,
            company_id=company_id,
            remarks=f"Stripe dispute lost {dispute_stripe_id}",
        )

        # Update dispute resolution
        sql, params = dynamic_update("stripe_dispute", {
            "erpclaw_journal_entry_id": je_id,
            "resolution_amount": str(round_currency(total)),
        }, {"id": dispute["id"]})
        conn.execute(sql, params)

        audit(conn, SKILL, "stripe-post-dispute-gl", "stripe_dispute", dispute["id"],
              new_values={"action": "lost", "journal_entry_id": je_id})
        conn.commit()

        ok({
            "dispute_stripe_id": dispute_stripe_id,
            "action": "lost",
            "journal_entry_id": je_id,
            "gl_entry_count": len(gl_ids),
            "dispute_amount": str(round_currency(total)),
        })

    else:
        # Open dispute: hold funds
        if dispute["erpclaw_journal_entry_id"]:
            err(f"Dispute {dispute_stripe_id} already posted with journal entry {dispute['erpclaw_journal_entry_id']}")

        total = dispute_amount + dispute_fee

        entries = [
            # DR Dispute Receivable (via clearing — hold amount)
            {
                "account_id": gl["dispute_expense_account_id"],
                "debit": str(round_currency(total)),
                "credit": "0",
                "cost_center_id": cost_center_id,
            },
            # CR Stripe Clearing
            {
                "account_id": gl["stripe_clearing_account_id"],
                "debit": "0",
                "credit": str(round_currency(total)),
            },
        ]

        je_id = _create_journal_entry(
            conn, company_id, posting_date, total,
            remark=f"Stripe dispute {dispute_stripe_id} (amount + $15 fee)",
        )

        gl_ids = insert_gl_entries(
            conn, entries,
            voucher_type="journal_entry",
            voucher_id=je_id,
            posting_date=posting_date,
            company_id=company_id,
            remarks=f"Stripe dispute {dispute_stripe_id}",
        )

        sql, params = dynamic_update("stripe_dispute", {
            "erpclaw_journal_entry_id": je_id,
        }, {"id": dispute["id"]})
        conn.execute(sql, params)

        _mark_balance_transactions_reconciled(
            conn, stripe_account_id, dispute_stripe_id, je_id)

        audit(conn, SKILL, "stripe-post-dispute-gl", "stripe_dispute", dispute["id"],
              new_values={"action": "open_hold", "journal_entry_id": je_id})
        conn.commit()

        ok({
            "dispute_stripe_id": dispute_stripe_id,
            "action": "open_hold",
            "journal_entry_id": je_id,
            "gl_entry_count": len(gl_ids),
            "dispute_amount": str(round_currency(dispute_amount)),
            "dispute_fee": str(round_currency(dispute_fee)),
            "total_held": str(round_currency(total)),
        })


# ---------------------------------------------------------------------------
# 4. stripe-post-payout-gl
# ---------------------------------------------------------------------------
def post_payout_gl(conn, args):
    """Post GL entries for a Stripe payout (bank transfer).

    Creates payment_entry (type='internal_transfer'):
      DR Bank/Payout account (payout amount)
      CR Stripe Clearing (payout amount)
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    payout_stripe_id = getattr(args, "payout_stripe_id", None)
    if not payout_stripe_id:
        err("--payout-stripe-id is required")

    # Find the payout
    t = Table("stripe_payout")
    payout = conn.execute(
        Q.from_(t).select("*").where(
            t.stripe_account_id == P()
        ).where(t.stripe_id == P()).get_sql(),
        (stripe_account_id, payout_stripe_id)
    ).fetchone()
    if not payout:
        err(f"Payout {payout_stripe_id} not found for account {stripe_account_id}")

    if payout["erpclaw_payment_entry_id"]:
        err(f"Payout {payout_stripe_id} already posted with payment entry {payout['erpclaw_payment_entry_id']}")

    gl = _get_stripe_account_gl(conn, stripe_account_id)
    company_id = gl["company_id"]
    payout_amount = to_decimal(payout["amount"])
    posting_date = _today()

    entries = [
        # DR Bank/Payout account
        {
            "account_id": gl["stripe_payout_account_id"],
            "debit": str(round_currency(payout_amount)),
            "credit": "0",
        },
        # CR Stripe Clearing
        {
            "account_id": gl["stripe_clearing_account_id"],
            "debit": "0",
            "credit": str(round_currency(payout_amount)),
        },
    ]

    pe_id = _create_payment_entry(
        conn, company_id, "internal_transfer", posting_date,
        paid_from=gl["stripe_clearing_account_id"],
        paid_to=gl["stripe_payout_account_id"],
        amount=payout_amount,
        reference_number=payout_stripe_id,
    )

    gl_ids = insert_gl_entries(
        conn, entries,
        voucher_type="payment_entry",
        voucher_id=pe_id,
        posting_date=posting_date,
        company_id=company_id,
        remarks=f"Stripe payout {payout_stripe_id}",
    )

    sql, params = dynamic_update("stripe_payout", {
        "erpclaw_payment_entry_id": pe_id,
        "reconciled": 1,
    }, {"id": payout["id"]})
    conn.execute(sql, params)

    # Mark balance transactions for this payout
    now = now_iso()
    conn.execute(
        """UPDATE stripe_balance_transaction
           SET reconciled = 1, gl_voucher_id = ?, gl_voucher_type = 'payment_entry',
               reconciled_at = ?
           WHERE stripe_account_id = ? AND payout_id = ? AND reconciled = 0""",
        (pe_id, now, stripe_account_id, payout_stripe_id)
    )

    audit(conn, SKILL, "stripe-post-payout-gl", "stripe_payout", payout["id"],
          new_values={"payment_entry_id": pe_id})
    conn.commit()

    ok({
        "payout_stripe_id": payout_stripe_id,
        "payment_entry_id": pe_id,
        "gl_entry_count": len(gl_ids),
        "payout_amount": str(round_currency(payout_amount)),
    })


# ---------------------------------------------------------------------------
# 5. stripe-post-connect-fee-gl
# ---------------------------------------------------------------------------
def post_connect_fee_gl(conn, args):
    """Post GL entries for a Stripe Connect application fee.

    Creates journal_entry:
      DR Stripe Clearing (fee amount)
      CR Platform Revenue (fee amount)
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    app_fee_stripe_id = getattr(args, "app_fee_stripe_id", None)
    if not app_fee_stripe_id:
        err("--app-fee-stripe-id is required")

    # Find the application fee
    t = Table("stripe_application_fee")
    app_fee = conn.execute(
        Q.from_(t).select("*").where(
            t.stripe_account_id == P()
        ).where(t.stripe_id == P()).get_sql(),
        (stripe_account_id, app_fee_stripe_id)
    ).fetchone()
    if not app_fee:
        err(f"Application fee {app_fee_stripe_id} not found for account {stripe_account_id}")

    if app_fee["erpclaw_journal_entry_id"]:
        err(f"Application fee {app_fee_stripe_id} already posted with journal entry {app_fee['erpclaw_journal_entry_id']}")

    gl = _get_stripe_account_gl(conn, stripe_account_id)
    company_id = gl["company_id"]
    fee_amount = to_decimal(app_fee["amount"])
    posting_date = _today()

    # Auto-resolve cost_center_id for P&L accounts (income)
    explicit_cc = getattr(args, "cost_center_id", None)
    cost_center_id = _resolve_cost_center_id(conn, company_id, explicit_cc)

    # Platform revenue account is required for connect fees
    platform_rev = gl.get("platform_revenue_account_id")
    if not platform_rev:
        err("Platform revenue account not configured. Use stripe-configure-gl-mapping to set --platform-revenue-account-id")

    entries = [
        # DR Stripe Clearing
        {
            "account_id": gl["stripe_clearing_account_id"],
            "debit": str(round_currency(fee_amount)),
            "credit": "0",
        },
        # CR Platform Revenue
        {
            "account_id": platform_rev,
            "debit": "0",
            "credit": str(round_currency(fee_amount)),
            "cost_center_id": cost_center_id,
        },
    ]

    je_id = _create_journal_entry(
        conn, company_id, posting_date, fee_amount,
        remark=f"Stripe Connect fee {app_fee_stripe_id}",
    )

    gl_ids = insert_gl_entries(
        conn, entries,
        voucher_type="journal_entry",
        voucher_id=je_id,
        posting_date=posting_date,
        company_id=company_id,
        remarks=f"Stripe Connect fee {app_fee_stripe_id}",
    )

    sql, params = dynamic_update("stripe_application_fee", {
        "erpclaw_journal_entry_id": je_id,
    }, {"id": app_fee["id"]})
    conn.execute(sql, params)

    audit(conn, SKILL, "stripe-post-connect-fee-gl", "stripe_application_fee",
          app_fee["id"], new_values={"journal_entry_id": je_id})
    conn.commit()

    ok({
        "app_fee_stripe_id": app_fee_stripe_id,
        "journal_entry_id": je_id,
        "gl_entry_count": len(gl_ids),
        "fee_amount": str(round_currency(fee_amount)),
    })


# ---------------------------------------------------------------------------
# 6. stripe-bulk-post-gl
# ---------------------------------------------------------------------------
def bulk_post_gl(conn, args):
    """Bulk-post GL entries for all unposted Stripe objects.

    Finds all charges, refunds, disputes, payouts, and connect fees
    with gl_voucher_id IS NULL and posts them one by one.
    Optionally filtered by --date-from and --date-to.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    date_from = getattr(args, "date_from", None)
    date_to = getattr(args, "date_to", None)

    results = {
        "charges_posted": 0,
        "refunds_posted": 0,
        "disputes_posted": 0,
        "payouts_posted": 0,
        "connect_fees_posted": 0,
        "errors": [],
    }

    # 1. Unposted charges
    charge_q = """
        SELECT stripe_id FROM stripe_charge
        WHERE stripe_account_id = ? AND erpclaw_payment_entry_id IS NULL
          AND status = 'succeeded'
    """
    charge_params = [stripe_account_id]
    if date_from:
        charge_q += " AND created_stripe >= ?"
        charge_params.append(date_from)
    if date_to:
        charge_q += " AND created_stripe <= ?"
        charge_params.append(date_to)

    charges = conn.execute(charge_q, charge_params).fetchall()
    for ch in charges:
        try:
            mini_args = type("Args", (), {
                "stripe_account_id": stripe_account_id,
                "charge_stripe_id": ch["stripe_id"],
                "cost_center_id": getattr(args, "cost_center_id", None),
            })()
            _call_silently(post_charge_gl, conn, mini_args)
            results["charges_posted"] += 1
        except (SystemExit, Exception) as e:
            results["errors"].append(f"charge {ch['stripe_id']}: {e}")

    # 2. Unposted refunds
    refund_q = """
        SELECT stripe_id FROM stripe_refund
        WHERE stripe_account_id = ? AND erpclaw_payment_entry_id IS NULL
          AND status = 'succeeded'
    """
    refund_params = [stripe_account_id]
    if date_from:
        refund_q += " AND created_stripe >= ?"
        refund_params.append(date_from)
    if date_to:
        refund_q += " AND created_stripe <= ?"
        refund_params.append(date_to)

    refunds = conn.execute(refund_q, refund_params).fetchall()
    for rf in refunds:
        try:
            mini_args = type("Args", (), {
                "stripe_account_id": stripe_account_id,
                "refund_stripe_id": rf["stripe_id"],
                "cost_center_id": getattr(args, "cost_center_id", None),
            })()
            _call_silently(post_refund_gl, conn, mini_args)
            results["refunds_posted"] += 1
        except (SystemExit, Exception) as e:
            results["errors"].append(f"refund {rf['stripe_id']}: {e}")

    # 3. Unposted disputes (open status only)
    dispute_q = """
        SELECT stripe_id FROM stripe_dispute
        WHERE stripe_account_id = ? AND erpclaw_journal_entry_id IS NULL
          AND status IN ('needs_response', 'under_review',
                         'warning_needs_response', 'warning_under_review')
    """
    dispute_params = [stripe_account_id]
    if date_from:
        dispute_q += " AND created_stripe >= ?"
        dispute_params.append(date_from)
    if date_to:
        dispute_q += " AND created_stripe <= ?"
        dispute_params.append(date_to)

    disputes = conn.execute(dispute_q, dispute_params).fetchall()
    for dp in disputes:
        try:
            mini_args = type("Args", (), {
                "stripe_account_id": stripe_account_id,
                "dispute_stripe_id": dp["stripe_id"],
                "cost_center_id": getattr(args, "cost_center_id", None),
            })()
            _call_silently(post_dispute_gl, conn, mini_args)
            results["disputes_posted"] += 1
        except (SystemExit, Exception) as e:
            results["errors"].append(f"dispute {dp['stripe_id']}: {e}")

    # 4. Unposted payouts
    payout_q = """
        SELECT stripe_id FROM stripe_payout
        WHERE stripe_account_id = ? AND erpclaw_payment_entry_id IS NULL
          AND status = 'paid'
    """
    payout_params = [stripe_account_id]
    if date_from:
        payout_q += " AND created_stripe >= ?"
        payout_params.append(date_from)
    if date_to:
        payout_q += " AND created_stripe <= ?"
        payout_params.append(date_to)

    payouts = conn.execute(payout_q, payout_params).fetchall()
    for po in payouts:
        try:
            mini_args = type("Args", (), {
                "stripe_account_id": stripe_account_id,
                "payout_stripe_id": po["stripe_id"],
                "cost_center_id": getattr(args, "cost_center_id", None),
            })()
            _call_silently(post_payout_gl, conn, mini_args)
            results["payouts_posted"] += 1
        except (SystemExit, Exception) as e:
            results["errors"].append(f"payout {po['stripe_id']}: {e}")

    # 5. Unposted connect fees
    fee_q = """
        SELECT stripe_id FROM stripe_application_fee
        WHERE stripe_account_id = ? AND erpclaw_journal_entry_id IS NULL
    """
    fee_params = [stripe_account_id]
    if date_from:
        fee_q += " AND created_stripe >= ?"
        fee_params.append(date_from)
    if date_to:
        fee_q += " AND created_stripe <= ?"
        fee_params.append(date_to)

    fees = conn.execute(fee_q, fee_params).fetchall()
    for f in fees:
        try:
            mini_args = type("Args", (), {
                "stripe_account_id": stripe_account_id,
                "app_fee_stripe_id": f["stripe_id"],
                "cost_center_id": getattr(args, "cost_center_id", None),
            })()
            _call_silently(post_connect_fee_gl, conn, mini_args)
            results["connect_fees_posted"] += 1
        except (SystemExit, Exception) as e:
            results["errors"].append(f"connect_fee {f['stripe_id']}: {e}")

    total = (results["charges_posted"] + results["refunds_posted"] +
             results["disputes_posted"] + results["payouts_posted"] +
             results["connect_fees_posted"])

    audit(conn, SKILL, "stripe-bulk-post-gl", "stripe_account", stripe_account_id,
          new_values={"total_posted": total, "error_count": len(results["errors"])})
    conn.commit()

    results["total_posted"] = total
    ok(results)


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "stripe-post-charge-gl": post_charge_gl,
    "stripe-post-refund-gl": post_refund_gl,
    "stripe-post-dispute-gl": post_dispute_gl,
    "stripe-post-payout-gl": post_payout_gl,
    "stripe-post-connect-fee-gl": post_connect_fee_gl,
    "stripe-bulk-post-gl": bulk_post_gl,
}
