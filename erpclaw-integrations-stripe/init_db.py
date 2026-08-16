#!/usr/bin/env python3
"""ERPClaw Integrations — Stripe Deep Integration schema extension.

Adds 17 Stripe-specific tables to the shared database for full-cycle
payment reconciliation: charges, refunds, disputes, payouts, subscriptions,
invoices, Connect platform fees, and automated GL posting rules.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Every amount here — charge, refund, dispute, payout, fee,
subscription plan, reconciliation total — stays TEXT (Decimal strings) on every
backend, which is ADR-0034 dec. 1 and matters more in this module than in any
other addon: the invariant tier compares those strings exactly.
"""
import importlib.util
import os
import sys

# Bootstrap the shared lib only when it is not already reachable — an
# unconditional insert at position 0 overrides a caller that deliberately bound a
# different tree (ADR-0034 phase 2 step 2d).
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(
        os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))

from erpclaw_lib.seam import (  # noqa: E402
    CheckConstraint, Column, ForeignKey, Index, Integer, MetaData, Table, Text,
    UniqueConstraint, provision, reference_table, text,
)

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")
DISPLAY_NAME = "ERPClaw Integrations — Stripe"

REQUIRED_FOUNDATION = [
    "company", "account", "customer", "sales_invoice",
    "payment_entry", "gl_entry", "naming_series", "audit_log",
]

METADATA = MetaData()

# Foundation tables this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)
reference_table("account", METADATA)
reference_table("customer", METADATA)

# ---------------------------------------------------------------------------
# 1. stripe_account
# Central configuration for each Stripe account (test or live).
# Stores encrypted API key, webhook secret, and GL account mappings.
# ---------------------------------------------------------------------------
STRIPE_ACCOUNT = Table(
    "stripe_account", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("account_name", Text, nullable=False),
    Column("stripe_account_id", Text),
    Column("restricted_key_enc", Text, nullable=False),
    Column("webhook_secret_enc", Text),
    Column("mode", Text, nullable=False, server_default=text("'test'")),
    Column("is_connect_platform", Integer, nullable=False,
           server_default=text("0")),
    Column("default_currency", Text, nullable=False, server_default=text("'USD'")),
    Column("payout_schedule", Text),
    Column("stripe_clearing_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("stripe_fees_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("stripe_payout_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("dispute_expense_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("unearned_revenue_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("platform_revenue_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("last_sync_at", Text),
    Column("sync_from_date", Text),
    Column("status", Text, nullable=False, server_default=text("'active'")),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("mode IN ('test','live')", name="ck_stripe_account_mode"),
    CheckConstraint("is_connect_platform IN (0,1)",
                    name="ck_stripe_account_is_connect_platform"),
    CheckConstraint("status IN ('active','paused','error','disabled')",
                    name="ck_stripe_account_status"),
)

Index("idx_stripe_acct_company", STRIPE_ACCOUNT.c.company_id)
Index("idx_stripe_acct_status", STRIPE_ACCOUNT.c.status)
Index("idx_stripe_acct_mode", STRIPE_ACCOUNT.c.mode)

# ---------------------------------------------------------------------------
# 2. stripe_sync_job
# Tracks each sync operation (full, incremental, webhook, historical).
# ---------------------------------------------------------------------------
STRIPE_SYNC_JOB = Table(
    "stripe_sync_job", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("sync_type", Text, nullable=False),
    Column("object_type", Text, nullable=False),
    Column("status", Text, nullable=False, server_default=text("'pending'")),
    Column("records_fetched", Integer, nullable=False, server_default=text("0")),
    Column("records_processed", Integer, nullable=False, server_default=text("0")),
    Column("records_failed", Integer, nullable=False, server_default=text("0")),
    Column("cursor_position", Text),
    Column("sync_from", Text),
    Column("sync_to", Text),
    Column("error_message", Text),
    Column("started_at", Text),
    Column("completed_at", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "sync_type IN ('full','incremental','webhook','historical_import')",
        name="ck_stripe_sync_job_sync_type"),
    CheckConstraint(
        "object_type IN ('balance_transaction','charge','refund','dispute',"
        "'payout','customer','invoice','subscription','transfer',"
        "'credit_note','all')",
        name="ck_stripe_sync_job_object_type"),
    CheckConstraint(
        "status IN ('pending','running','completed','failed','cancelled')",
        name="ck_stripe_sync_job_status"),
)

Index("idx_stripe_sync_acct", STRIPE_SYNC_JOB.c.stripe_account_id)
Index("idx_stripe_sync_status", STRIPE_SYNC_JOB.c.status)
Index("idx_stripe_sync_company", STRIPE_SYNC_JOB.c.company_id)
Index("idx_stripe_sync_type", STRIPE_SYNC_JOB.c.sync_type)

# ---------------------------------------------------------------------------
# 3. stripe_balance_transaction
# Mirror of Stripe Balance Transaction objects. Core reconciliation entity.
# Amounts stored in DOLLARS (Decimal TEXT), not cents.
# ---------------------------------------------------------------------------
STRIPE_BALANCE_TRANSACTION = Table(
    "stripe_balance_transaction", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_id", Text, nullable=False, unique=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("type", Text),
    Column("reporting_category", Text),
    Column("source_id", Text),
    Column("source_type", Text),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("fee", Text, nullable=False, server_default=text("'0'")),
    Column("net", Text, nullable=False, server_default=text("'0'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("description", Text),
    Column("available_on", Text),
    Column("created_stripe", Text),
    Column("payout_id", Text),
    # Nullable status here, NOT NULL on most sibling tables — the original's
    # asymmetry, transcribed rather than tidied.
    Column("status", Text, server_default=text("'available'")),
    Column("reconciled", Integer, nullable=False, server_default=text("0")),
    Column("reconciled_at", Text),
    Column("gl_voucher_id", Text),
    Column("gl_voucher_type", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("status IN ('available','pending')",
                    name="ck_stripe_balance_transaction_status"),
    CheckConstraint("reconciled IN (0,1)",
                    name="ck_stripe_balance_transaction_reconciled"),
)

Index("idx_stripe_bt_acct", STRIPE_BALANCE_TRANSACTION.c.stripe_account_id)
Index("idx_stripe_bt_stripe", STRIPE_BALANCE_TRANSACTION.c.stripe_id)
Index("idx_stripe_bt_type", STRIPE_BALANCE_TRANSACTION.c.type)
Index("idx_stripe_bt_status", STRIPE_BALANCE_TRANSACTION.c.status)
Index("idx_stripe_bt_reconciled", STRIPE_BALANCE_TRANSACTION.c.reconciled)
Index("idx_stripe_bt_payout", STRIPE_BALANCE_TRANSACTION.c.payout_id)
Index("idx_stripe_bt_company", STRIPE_BALANCE_TRANSACTION.c.company_id)

# ---------------------------------------------------------------------------
# 4. stripe_charge
# Mirror of Stripe Charge objects, linked to erpclaw customer/invoice/payment.
# ---------------------------------------------------------------------------
STRIPE_CHARGE = Table(
    "stripe_charge", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_id", Text, nullable=False, unique=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("customer_stripe_id", Text),
    Column("description", Text),
    Column("payment_method_type", Text),
    Column("payment_intent_id", Text),
    Column("invoice_stripe_id", Text),
    Column("status", Text, nullable=False, server_default=text("'pending'")),
    Column("amount_refunded", Text, nullable=False, server_default=text("'0'")),
    Column("disputed", Integer, nullable=False, server_default=text("0")),
    Column("failure_code", Text),
    # The erpclaw_* links carry no foreign key here, unlike stripe_customer_map's
    # erpclaw_customer_id. Original asymmetry, preserved.
    Column("erpclaw_customer_id", Text),
    Column("erpclaw_invoice_id", Text),
    Column("erpclaw_payment_entry_id", Text),
    Column("metadata", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_stripe", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ('succeeded','pending','failed','refunded','disputed')",
        name="ck_stripe_charge_status"),
    CheckConstraint("disputed IN (0,1)", name="ck_stripe_charge_disputed"),
)

Index("idx_stripe_chg_acct", STRIPE_CHARGE.c.stripe_account_id)
Index("idx_stripe_chg_stripe", STRIPE_CHARGE.c.stripe_id)
Index("idx_stripe_chg_status", STRIPE_CHARGE.c.status)
Index("idx_stripe_chg_customer", STRIPE_CHARGE.c.customer_stripe_id)
Index("idx_stripe_chg_company", STRIPE_CHARGE.c.company_id)

# ---------------------------------------------------------------------------
# 5. stripe_refund
# Mirror of Stripe Refund objects.
# ---------------------------------------------------------------------------
STRIPE_REFUND = Table(
    "stripe_refund", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_id", Text, nullable=False, unique=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("charge_id", Text,
           ForeignKey("stripe_charge.id", ondelete="RESTRICT")),
    Column("charge_stripe_id", Text),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("reason", Text),
    Column("status", Text, nullable=False, server_default=text("'pending'")),
    Column("erpclaw_credit_note_id", Text),
    Column("erpclaw_payment_entry_id", Text),
    Column("metadata", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_stripe", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("status IN ('pending','succeeded','failed','canceled')",
                    name="ck_stripe_refund_status"),
)

Index("idx_stripe_ref_acct", STRIPE_REFUND.c.stripe_account_id)
Index("idx_stripe_ref_stripe", STRIPE_REFUND.c.stripe_id)
Index("idx_stripe_ref_charge", STRIPE_REFUND.c.charge_id)
Index("idx_stripe_ref_company", STRIPE_REFUND.c.company_id)

# ---------------------------------------------------------------------------
# 6. stripe_dispute
# Mirror of Stripe Dispute objects (chargebacks).
# ---------------------------------------------------------------------------
STRIPE_DISPUTE = Table(
    "stripe_dispute", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_id", Text, nullable=False, unique=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("charge_id", Text,
           ForeignKey("stripe_charge.id", ondelete="RESTRICT")),
    Column("charge_stripe_id", Text),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("reason", Text),
    Column("status", Text, nullable=False,
           server_default=text("'needs_response'")),
    Column("evidence_due_by", Text),
    Column("erpclaw_journal_entry_id", Text),
    Column("resolution_amount", Text),
    Column("metadata", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_stripe", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ('warning_needs_response','warning_under_review',"
        "'needs_response','under_review','won','lost')",
        name="ck_stripe_dispute_status"),
)

Index("idx_stripe_dsp_acct", STRIPE_DISPUTE.c.stripe_account_id)
Index("idx_stripe_dsp_stripe", STRIPE_DISPUTE.c.stripe_id)
Index("idx_stripe_dsp_charge", STRIPE_DISPUTE.c.charge_id)
Index("idx_stripe_dsp_status", STRIPE_DISPUTE.c.status)
Index("idx_stripe_dsp_company", STRIPE_DISPUTE.c.company_id)

# ---------------------------------------------------------------------------
# 7. stripe_payout
# Mirror of Stripe Payout objects (bank transfers from Stripe balance).
# ---------------------------------------------------------------------------
STRIPE_PAYOUT = Table(
    "stripe_payout", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_id", Text, nullable=False, unique=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("arrival_date", Text),
    Column("method", Text),
    Column("description", Text),
    Column("status", Text, nullable=False, server_default=text("'pending'")),
    Column("failure_code", Text),
    Column("destination_bank_last4", Text),
    Column("transaction_count", Integer, nullable=False, server_default=text("0")),
    Column("reconciled", Integer, nullable=False, server_default=text("0")),
    Column("erpclaw_payment_entry_id", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_stripe", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ('paid','pending','in_transit','canceled','failed')",
        name="ck_stripe_payout_status"),
    CheckConstraint("reconciled IN (0,1)", name="ck_stripe_payout_reconciled"),
)

Index("idx_stripe_pay_acct", STRIPE_PAYOUT.c.stripe_account_id)
Index("idx_stripe_pay_stripe", STRIPE_PAYOUT.c.stripe_id)
Index("idx_stripe_pay_status", STRIPE_PAYOUT.c.status)
Index("idx_stripe_pay_company", STRIPE_PAYOUT.c.company_id)

# ---------------------------------------------------------------------------
# 8. stripe_invoice
# Mirror of Stripe Invoice objects (for recurring billing / subscriptions).
# ---------------------------------------------------------------------------
STRIPE_INVOICE = Table(
    "stripe_invoice", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_id", Text, nullable=False, unique=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("customer_stripe_id", Text),
    Column("number", Text),
    Column("amount_due", Text, nullable=False, server_default=text("'0'")),
    Column("amount_paid", Text, nullable=False, server_default=text("'0'")),
    Column("amount_remaining", Text, nullable=False, server_default=text("'0'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    # Nullable, unlike the NOT NULL status on charge / refund / payout.
    Column("status", Text, server_default=text("'draft'")),
    Column("subscription_stripe_id", Text),
    Column("period_start", Text),
    Column("period_end", Text),
    Column("erpclaw_invoice_id", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_stripe", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ('draft','open','paid','void','uncollectible')",
        name="ck_stripe_invoice_status"),
)

Index("idx_stripe_inv_acct", STRIPE_INVOICE.c.stripe_account_id)
Index("idx_stripe_inv_stripe", STRIPE_INVOICE.c.stripe_id)
Index("idx_stripe_inv_status", STRIPE_INVOICE.c.status)
Index("idx_stripe_inv_customer", STRIPE_INVOICE.c.customer_stripe_id)
Index("idx_stripe_inv_company", STRIPE_INVOICE.c.company_id)

# ---------------------------------------------------------------------------
# 9. stripe_subscription
# Mirror of Stripe Subscription objects.
# ---------------------------------------------------------------------------
STRIPE_SUBSCRIPTION = Table(
    "stripe_subscription", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_id", Text, nullable=False, unique=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("customer_stripe_id", Text),
    Column("status", Text, nullable=False, server_default=text("'active'")),
    Column("current_period_start", Text),
    Column("current_period_end", Text),
    Column("cancel_at_period_end", Integer, nullable=False,
           server_default=text("0")),
    Column("canceled_at", Text),
    Column("plan_interval", Text),
    Column("plan_amount", Text, nullable=False, server_default=text("'0'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("erpclaw_revenue_contract_id", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_stripe", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ('active','past_due','canceled','unpaid','trialing',"
        "'incomplete')",
        name="ck_stripe_subscription_status"),
    CheckConstraint("cancel_at_period_end IN (0,1)",
                    name="ck_stripe_subscription_cancel_at_period_end"),
)

Index("idx_stripe_sub_acct", STRIPE_SUBSCRIPTION.c.stripe_account_id)
Index("idx_stripe_sub_stripe", STRIPE_SUBSCRIPTION.c.stripe_id)
Index("idx_stripe_sub_status", STRIPE_SUBSCRIPTION.c.status)
Index("idx_stripe_sub_customer", STRIPE_SUBSCRIPTION.c.customer_stripe_id)
Index("idx_stripe_sub_company", STRIPE_SUBSCRIPTION.c.company_id)

# ---------------------------------------------------------------------------
# 10. stripe_customer_map
# Maps Stripe customer IDs to erpclaw customer IDs.
# ---------------------------------------------------------------------------
STRIPE_CUSTOMER_MAP = Table(
    "stripe_customer_map", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("stripe_customer_id", Text, nullable=False),
    Column("erpclaw_customer_id", Text,
           ForeignKey("customer.id", ondelete="RESTRICT")),
    Column("stripe_email", Text),
    Column("stripe_name", Text),
    Column("match_method", Text, server_default=text("'manual'")),
    # A confidence score, but TEXT like every other decimal in the module.
    Column("match_confidence", Text, nullable=False, server_default=text("'1.0'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("match_method IN ('manual','email','name','metadata')",
                    name="ck_stripe_customer_map_match_method"),
    UniqueConstraint("stripe_account_id", "stripe_customer_id"),
)

Index("idx_stripe_cmap_acct", STRIPE_CUSTOMER_MAP.c.stripe_account_id)
Index("idx_stripe_cmap_erpclaw", STRIPE_CUSTOMER_MAP.c.erpclaw_customer_id)
Index("idx_stripe_cmap_company", STRIPE_CUSTOMER_MAP.c.company_id)

# ---------------------------------------------------------------------------
# 11. stripe_deep_webhook_event
# Incoming Stripe webhook events with idempotent processing.
# ---------------------------------------------------------------------------
STRIPE_DEEP_WEBHOOK_EVENT = Table(
    "stripe_deep_webhook_event", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("stripe_event_id", Text, nullable=False, unique=True),
    Column("event_type", Text, nullable=False),
    Column("api_version", Text),
    Column("object_id", Text),
    Column("object_type", Text),
    Column("payload", Text),
    Column("processed", Integer, nullable=False, server_default=text("0")),
    Column("process_attempts", Integer, nullable=False, server_default=text("0")),
    Column("max_attempts", Integer, nullable=False, server_default=text("3")),
    Column("processed_at", Text),
    Column("error_message", Text),
    Column("created_stripe", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("processed IN (0,1)",
                    name="ck_stripe_deep_webhook_event_processed"),
)

Index("idx_stripe_dwh_acct", STRIPE_DEEP_WEBHOOK_EVENT.c.stripe_account_id)
Index("idx_stripe_dwh_event", STRIPE_DEEP_WEBHOOK_EVENT.c.stripe_event_id)
Index("idx_stripe_dwh_type", STRIPE_DEEP_WEBHOOK_EVENT.c.event_type)
Index("idx_stripe_dwh_processed", STRIPE_DEEP_WEBHOOK_EVENT.c.processed)

# ---------------------------------------------------------------------------
# 12. stripe_credit_note
# Mirror of Stripe Credit Note objects.
# ---------------------------------------------------------------------------
STRIPE_CREDIT_NOTE = Table(
    "stripe_credit_note", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_id", Text, nullable=False, unique=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("invoice_stripe_id", Text),
    Column("customer_stripe_id", Text),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("reason", Text),
    # Defaulted but unconstrained: the only status column in the module with no
    # CHECK behind it. Transcribed as shipped.
    Column("status", Text, server_default=text("'issued'")),
    Column("erpclaw_credit_note_id", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_stripe", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_stripe_cn_acct", STRIPE_CREDIT_NOTE.c.stripe_account_id)
Index("idx_stripe_cn_stripe", STRIPE_CREDIT_NOTE.c.stripe_id)
Index("idx_stripe_cn_company", STRIPE_CREDIT_NOTE.c.company_id)

# ---------------------------------------------------------------------------
# 13. stripe_application_fee
# Connect platform application fees collected.
# ---------------------------------------------------------------------------
STRIPE_APPLICATION_FEE = Table(
    "stripe_application_fee", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_id", Text, nullable=False, unique=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("charge_stripe_id", Text),
    Column("account_stripe_id", Text),
    Column("refunded_amount", Text, nullable=False, server_default=text("'0'")),
    Column("erpclaw_journal_entry_id", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_stripe", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_stripe_af_acct", STRIPE_APPLICATION_FEE.c.stripe_account_id)
Index("idx_stripe_af_stripe", STRIPE_APPLICATION_FEE.c.stripe_id)
Index("idx_stripe_af_company", STRIPE_APPLICATION_FEE.c.company_id)

# ---------------------------------------------------------------------------
# 14. stripe_transfer
# Connect platform transfers between accounts.
# ---------------------------------------------------------------------------
STRIPE_TRANSFER = Table(
    "stripe_transfer", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_id", Text, nullable=False, unique=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("destination_account", Text),
    Column("description", Text),
    Column("reversed", Integer, nullable=False, server_default=text("0")),
    Column("erpclaw_journal_entry_id", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_stripe", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("reversed IN (0,1)", name="ck_stripe_transfer_reversed"),
)

Index("idx_stripe_xfr_acct", STRIPE_TRANSFER.c.stripe_account_id)
Index("idx_stripe_xfr_stripe", STRIPE_TRANSFER.c.stripe_id)
Index("idx_stripe_xfr_company", STRIPE_TRANSFER.c.company_id)

# ---------------------------------------------------------------------------
# 15. stripe_gl_rule
# Configurable rules for mapping Stripe transaction types to GL accounts.
# ---------------------------------------------------------------------------
STRIPE_GL_RULE = Table(
    "stripe_gl_rule", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("transaction_type", Text, nullable=False),
    Column("match_field", Text),
    Column("match_value", Text),
    Column("debit_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("credit_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("fee_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    # cost_center_id carries no foreign key, unlike the three account columns
    # above it. Original asymmetry, preserved.
    Column("cost_center_id", Text),
    Column("priority", Integer, nullable=False, server_default=text("0")),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "transaction_type IN ('charge','refund','dispute','payout',"
        "'connect_fee','other')",
        name="ck_stripe_gl_rule_transaction_type"),
    CheckConstraint("is_active IN (0,1)", name="ck_stripe_gl_rule_is_active"),
)

Index("idx_stripe_glr_acct", STRIPE_GL_RULE.c.stripe_account_id)
Index("idx_stripe_glr_type", STRIPE_GL_RULE.c.transaction_type)
Index("idx_stripe_glr_company", STRIPE_GL_RULE.c.company_id)

# ---------------------------------------------------------------------------
# 16. stripe_fee_detail
# Breakdown of fees per balance transaction (processing, Stripe fee, etc.).
# The only CASCADE in the module, and the only table with no company_id.
# ---------------------------------------------------------------------------
STRIPE_FEE_DETAIL = Table(
    "stripe_fee_detail", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("balance_transaction_id", Text,
           ForeignKey("stripe_balance_transaction.id", ondelete="CASCADE"),
           nullable=False),
    Column("fee_type", Text, nullable=False),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("description", Text),
    Column("application", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_stripe_fd_bt", STRIPE_FEE_DETAIL.c.balance_transaction_id)
Index("idx_stripe_fd_type", STRIPE_FEE_DETAIL.c.fee_type)

# ---------------------------------------------------------------------------
# 17. stripe_reconciliation_run
# Tracks each reconciliation run between Stripe and GL.
# ---------------------------------------------------------------------------
STRIPE_RECONCILIATION_RUN = Table(
    "stripe_reconciliation_run", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("stripe_account_id", Text,
           ForeignKey("stripe_account.id", ondelete="RESTRICT"), nullable=False),
    Column("run_date", Text, nullable=False),
    Column("period_start", Text, nullable=False),
    Column("period_end", Text, nullable=False),
    Column("transactions_processed", Integer, nullable=False,
           server_default=text("0")),
    Column("transactions_matched", Integer, nullable=False,
           server_default=text("0")),
    Column("transactions_unmatched", Integer, nullable=False,
           server_default=text("0")),
    Column("amount_reconciled", Text, nullable=False, server_default=text("'0'")),
    Column("amount_unreconciled", Text, nullable=False,
           server_default=text("'0'")),
    Column("status", Text, nullable=False, server_default=text("'running'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("status IN ('running','completed','failed')",
                    name="ck_stripe_reconciliation_run_status"),
)

Index("idx_stripe_rr_acct", STRIPE_RECONCILIATION_RUN.c.stripe_account_id)
Index("idx_stripe_rr_status", STRIPE_RECONCILIATION_RUN.c.status)
Index("idx_stripe_rr_company", STRIPE_RECONCILIATION_RUN.c.company_id)


def _require_foundation(db_path):
    """The pre-conversion installer's foundation probe, asked through the seam.

    The original read ``sqlite_master`` directly, so the guard that exists to
    produce a friendly error was itself SQLite-only — on PostgreSQL it would have
    raised before it could explain anything. ``seam.table_exists`` answers on both
    backends (ADR-0034 bulk-39).
    """
    from erpclaw_lib import seam

    missing = [t for t in REQUIRED_FOUNDATION if not seam.table_exists(t, db_path)]
    if missing:
        print(f"ERROR: Foundation tables missing: {', '.join(missing)}")
        print("Run erpclaw first: clawhub install erpclaw")
        sys.exit(1)


def create_stripe_tables(db_path=None):
    """Create Stripe tables and indexes on whichever backend is configured.

    Same contract as before the ADR-0034 conversion: idempotent, and the returned
    counts are what was ACTUALLY created rather than what was declared.
    """
    db_path = db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    _require_foundation(db_path)
    result = provision(METADATA, db_path)
    return {
        "database": db_path,
        "tables": result["tables"],
        "indexes": result["indexes"],
    }


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else None
    result = create_stripe_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
