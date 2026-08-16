#!/usr/bin/env python3
"""ERPClaw Integrations — Shopify Deep Integration schema extension.

Adds 11 Shopify-specific tables to the shared database for full-cycle
e-commerce order sync, payout reconciliation, dispute tracking, and
automated GL posting rules.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Every amount here — order subtotal, shipping, tax, discount,
refund, payout gross/fee/net, dispute, reconciliation balance — stays TEXT
(Decimal strings) on every backend, which is ADR-0034 dec. 1.

**The v1.1 pairing columns, and why they are declared here now.** Six columns on
`shopify_account` (`pairing_method`, `hmac_secret_enc`, `last_status_push_at`,
`disconnect_state`, `status_mode`, `erpclaw_url_override`) used to arrive through
an ``ALTER TABLE ADD COLUMN`` loop this installer ran after its own CREATEs —
the one module of the 40 that carried migration logic inside its installer
(step 2f drift audit, 2026-08-12). They are now DECLARED, alongside every other
column, so a fresh provision creates the table complete on either backend.

The ALTER path still has to exist, because an install that predates those
columns still needs them and ``provision()`` creates missing TABLES, not missing
COLUMNS. It moved to where it belongs: `migrations/001_shopify_account_v11_pairing_columns.py`,
applied by `module_manager._run_module_migrations` under this module's name. An
installer PROVISIONS, a migration ALTERS; a converted installer carries no
migration logic (founder ruling, 2026-08-12).
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
DISPLAY_NAME = "ERPClaw Integrations — Shopify"

REQUIRED_FOUNDATION = [
    "company", "account", "customer", "sales_invoice",
    "payment_entry", "gl_entry", "naming_series", "audit_log",
]

METADATA = MetaData()

# Foundation tables this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)
reference_table("account", METADATA)

# ==================================================================
# TABLE 1: shopify_account
# Central configuration for each Shopify shop connection.
# Stores encrypted access token, API version, GL account mappings,
# and sync preferences.
# ==================================================================
SHOPIFY_ACCOUNT = Table(
    "shopify_account", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("shop_domain", Text, nullable=False),
    Column("shop_name", Text),
    Column("access_token_enc", Text, nullable=False),
    Column("api_version", Text, nullable=False, server_default=text("'2026-04'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("status", Text, nullable=False, server_default=text("'active'")),
    Column("clearing_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("revenue_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("shipping_revenue_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("tax_payable_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("cogs_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("inventory_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("fee_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("discount_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("refund_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("chargeback_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("chargeback_fee_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("gift_card_liability_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("reserve_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("bank_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("discount_method", Text, nullable=False, server_default=text("'net'")),
    Column("auto_post_gl", Integer, nullable=False, server_default=text("0")),
    Column("track_cogs", Integer, nullable=False, server_default=text("0")),
    Column("default_warehouse_id", Text),
    Column("last_orders_sync_at", Text),
    Column("last_products_sync_at", Text),
    Column("last_customers_sync_at", Text),
    Column("last_payouts_sync_at", Text),
    Column("last_disputes_sync_at", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    # v1.1 App Store pairing flow. Declared last, which is where the ALTER loop
    # they replaced put them, and nullable with no default, which is all
    # `ALTER TABLE ... ADD COLUMN <name> TEXT` can produce.
    Column("pairing_method", Text),
    Column("hmac_secret_enc", Text),
    Column("last_status_push_at", Text),
    Column("disconnect_state", Text),
    Column("status_mode", Text),
    Column("erpclaw_url_override", Text),
    CheckConstraint("status IN ('active','paused','error','disabled')",
                    name="ck_shopify_account_status"),
    CheckConstraint("discount_method IN ('net','gross')",
                    name="ck_shopify_account_discount_method"),
    CheckConstraint("auto_post_gl IN (0,1)",
                    name="ck_shopify_account_auto_post_gl"),
    CheckConstraint("track_cogs IN (0,1)",
                    name="ck_shopify_account_track_cogs"),
)

Index("idx_shpfy_acct_company", SHOPIFY_ACCOUNT.c.company_id)
Index("idx_shpfy_acct_status", SHOPIFY_ACCOUNT.c.status)
Index("idx_shpfy_acct_domain", SHOPIFY_ACCOUNT.c.shop_domain)

# ==================================================================
# TABLE 2: shopify_order
# Mirror of Shopify Order objects. Linked to erpclaw sales_invoice,
# customer, and GL entries. Amounts stored as TEXT (Decimal).
# ==================================================================
SHOPIFY_ORDER = Table(
    "shopify_order", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("shopify_account_id", Text,
           ForeignKey("shopify_account.id", ondelete="RESTRICT"), nullable=False),
    Column("shopify_order_id", Text, nullable=False),
    Column("shopify_order_number", Text),
    Column("order_date", Text),
    Column("financial_status", Text),
    Column("fulfillment_status", Text),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("subtotal_amount", Text, nullable=False, server_default=text("'0'")),
    Column("shipping_amount", Text, nullable=False, server_default=text("'0'")),
    Column("tax_amount", Text, nullable=False, server_default=text("'0'")),
    Column("discount_amount", Text, nullable=False, server_default=text("'0'")),
    Column("total_amount", Text, nullable=False, server_default=text("'0'")),
    Column("refunded_amount", Text, nullable=False, server_default=text("'0'")),
    # No foreign key on purpose: the sync writes the Shopify side before the
    # erpclaw document exists. Preserved as shipped.
    Column("sales_invoice_id", Text),
    Column("customer_id", Text),
    Column("gl_status", Text, nullable=False, server_default=text("'pending'")),
    Column("gl_voucher_id", Text),
    Column("payment_gateway", Text),
    Column("is_gift_card_order", Integer, nullable=False, server_default=text("0")),
    Column("has_refunds", Integer, nullable=False, server_default=text("0")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("gl_status IN ('pending','posted','failed','skipped')",
                    name="ck_shopify_order_gl_status"),
    CheckConstraint("is_gift_card_order IN (0,1)",
                    name="ck_shopify_order_is_gift_card_order"),
    CheckConstraint("has_refunds IN (0,1)",
                    name="ck_shopify_order_has_refunds"),
    # The re-sync idempotency key: one row per Shopify order per shop.
    UniqueConstraint("shopify_account_id", "shopify_order_id"),
)

Index("idx_shpfy_ord_acct", SHOPIFY_ORDER.c.shopify_account_id)
Index("idx_shpfy_ord_sid", SHOPIFY_ORDER.c.shopify_order_id)
Index("idx_shpfy_ord_gl", SHOPIFY_ORDER.c.gl_status)
Index("idx_shpfy_ord_date", SHOPIFY_ORDER.c.order_date)
Index("idx_shpfy_ord_company", SHOPIFY_ORDER.c.company_id)

# ==================================================================
# TABLE 3: shopify_order_line_item
# Individual line items from Shopify orders, linked to erpclaw items
# by SKU match.
# ==================================================================
SHOPIFY_ORDER_LINE_ITEM = Table(
    "shopify_order_line_item", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("shopify_order_id_local", Text,
           ForeignKey("shopify_order.id", ondelete="CASCADE"), nullable=False),
    Column("shopify_line_item_id", Text, nullable=False),
    Column("title", Text),
    Column("sku", Text),
    Column("quantity", Integer, nullable=False, server_default=text("1")),
    Column("unit_price", Text, nullable=False, server_default=text("'0'")),
    Column("discount_amount", Text, nullable=False, server_default=text("'0'")),
    Column("tax_amount", Text, nullable=False, server_default=text("'0'")),
    Column("total_amount", Text, nullable=False, server_default=text("'0'")),
    Column("item_id", Text),
    Column("is_gift_card", Integer, nullable=False, server_default=text("0")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("is_gift_card IN (0,1)",
                    name="ck_shopify_order_line_item_is_gift_card"),
)

Index("idx_shpfy_oli_order", SHOPIFY_ORDER_LINE_ITEM.c.shopify_order_id_local)
Index("idx_shpfy_oli_sku", SHOPIFY_ORDER_LINE_ITEM.c.sku)
Index("idx_shpfy_oli_item", SHOPIFY_ORDER_LINE_ITEM.c.item_id)
Index("idx_shpfy_oli_company", SHOPIFY_ORDER_LINE_ITEM.c.company_id)

# ==================================================================
# TABLE 4: shopify_refund
# Mirror of Shopify Refund objects, linked to erpclaw credit notes.
# ==================================================================
SHOPIFY_REFUND = Table(
    "shopify_refund", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    # RESTRICT here, CASCADE on the line-item child two tables down: a refund is
    # a document, its lines are its children. Asymmetry preserved as shipped.
    Column("shopify_order_id_local", Text,
           ForeignKey("shopify_order.id", ondelete="RESTRICT"), nullable=False),
    Column("shopify_refund_id", Text, nullable=False),
    Column("refund_date", Text),
    Column("refund_amount", Text, nullable=False, server_default=text("'0'")),
    Column("tax_refund_amount", Text, nullable=False, server_default=text("'0'")),
    Column("shipping_refund_amount", Text, nullable=False,
           server_default=text("'0'")),
    Column("refund_type", Text, nullable=False, server_default=text("'partial'")),
    Column("gl_status", Text, nullable=False, server_default=text("'pending'")),
    Column("gl_voucher_id", Text),
    Column("credit_note_id", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("refund_type IN ('full','partial')",
                    name="ck_shopify_refund_refund_type"),
    CheckConstraint("gl_status IN ('pending','posted','failed','skipped')",
                    name="ck_shopify_refund_gl_status"),
    UniqueConstraint("shopify_order_id_local", "shopify_refund_id"),
)

Index("idx_shpfy_ref_order", SHOPIFY_REFUND.c.shopify_order_id_local)
Index("idx_shpfy_ref_sid", SHOPIFY_REFUND.c.shopify_refund_id)
Index("idx_shpfy_ref_gl", SHOPIFY_REFUND.c.gl_status)
Index("idx_shpfy_ref_company", SHOPIFY_REFUND.c.company_id)

# ==================================================================
# TABLE 5: shopify_refund_line_item
# Individual line items within a refund, tracking restocking.
# ==================================================================
SHOPIFY_REFUND_LINE_ITEM = Table(
    "shopify_refund_line_item", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("shopify_refund_id_local", Text,
           ForeignKey("shopify_refund.id", ondelete="CASCADE"), nullable=False),
    Column("shopify_line_item_id", Text, nullable=False),
    Column("quantity", Integer, nullable=False, server_default=text("1")),
    Column("subtotal_amount", Text, nullable=False, server_default=text("'0'")),
    # The one status column in this module without NOT NULL. Preserved.
    Column("restock_type", Text, server_default=text("'no_restock'")),
    Column("item_id", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "restock_type IN ('no_restock','cancel','return','legacy_restock')",
        name="ck_shopify_refund_line_item_restock_type"),
)

Index("idx_shpfy_rli_refund", SHOPIFY_REFUND_LINE_ITEM.c.shopify_refund_id_local)
Index("idx_shpfy_rli_item", SHOPIFY_REFUND_LINE_ITEM.c.item_id)
Index("idx_shpfy_rli_company", SHOPIFY_REFUND_LINE_ITEM.c.company_id)

# ==================================================================
# TABLE 6: shopify_payout
# Mirror of Shopify Payments payouts (bank transfers).
# ==================================================================
SHOPIFY_PAYOUT = Table(
    "shopify_payout", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("shopify_account_id", Text,
           ForeignKey("shopify_account.id", ondelete="RESTRICT"), nullable=False),
    Column("shopify_payout_id", Text, nullable=False),
    Column("issued_at", Text),
    Column("status", Text, nullable=False, server_default=text("'scheduled'")),
    Column("gross_amount", Text, nullable=False, server_default=text("'0'")),
    Column("fee_amount", Text, nullable=False, server_default=text("'0'")),
    Column("net_amount", Text, nullable=False, server_default=text("'0'")),
    Column("charges_gross", Text, nullable=False, server_default=text("'0'")),
    Column("charges_fee", Text, nullable=False, server_default=text("'0'")),
    Column("refunds_gross", Text, nullable=False, server_default=text("'0'")),
    Column("refunds_fee", Text, nullable=False, server_default=text("'0'")),
    Column("adjustments_gross", Text, nullable=False, server_default=text("'0'")),
    Column("adjustments_fee", Text, nullable=False, server_default=text("'0'")),
    Column("reserved_funds_gross", Text, nullable=False, server_default=text("'0'")),
    Column("reserved_funds_fee", Text, nullable=False, server_default=text("'0'")),
    Column("gl_status", Text, nullable=False, server_default=text("'pending'")),
    Column("gl_voucher_id", Text),
    Column("payment_entry_id", Text),
    Column("reconciliation_status", Text, nullable=False,
           server_default=text("'unreconciled'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ('scheduled','in_transit','paid','failed','cancelled')",
        name="ck_shopify_payout_status"),
    CheckConstraint("gl_status IN ('pending','posted','failed','skipped')",
                    name="ck_shopify_payout_gl_status"),
    # The wrapped IN list in the shipped DDL leaves a space either side of the
    # value list; the predicate is compared character for character.
    CheckConstraint(
        "reconciliation_status IN ( "
        "'unreconciled','auto_matched','manual_matched','discrepancy' )",
        name="ck_shopify_payout_reconciliation_status"),
    UniqueConstraint("shopify_account_id", "shopify_payout_id"),
)

Index("idx_shpfy_pay_acct", SHOPIFY_PAYOUT.c.shopify_account_id)
Index("idx_shpfy_pay_sid", SHOPIFY_PAYOUT.c.shopify_payout_id)
Index("idx_shpfy_pay_status", SHOPIFY_PAYOUT.c.status)
Index("idx_shpfy_pay_gl", SHOPIFY_PAYOUT.c.gl_status)
Index("idx_shpfy_pay_recon", SHOPIFY_PAYOUT.c.reconciliation_status)
Index("idx_shpfy_pay_company", SHOPIFY_PAYOUT.c.company_id)

# ==================================================================
# TABLE 7: shopify_payout_transaction
# Individual balance transactions within a payout.
# ==================================================================
SHOPIFY_PAYOUT_TRANSACTION = Table(
    "shopify_payout_transaction", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("shopify_payout_id_local", Text,
           ForeignKey("shopify_payout.id", ondelete="CASCADE"), nullable=False),
    Column("shopify_balance_txn_id", Text, nullable=False),
    Column("transaction_type", Text, nullable=False),
    Column("gross_amount", Text, nullable=False, server_default=text("'0'")),
    Column("fee_amount", Text, nullable=False, server_default=text("'0'")),
    Column("net_amount", Text, nullable=False, server_default=text("'0'")),
    Column("source_order_id", Text),
    Column("source_type", Text),
    Column("processed_at", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "transaction_type IN ( 'charge','refund','dispute','reserve', "
        "'adjustment','payout' )",
        name="ck_shopify_payout_transaction_transaction_type"),
)

Index("idx_shpfy_ptx_payout", SHOPIFY_PAYOUT_TRANSACTION.c.shopify_payout_id_local)
Index("idx_shpfy_ptx_type", SHOPIFY_PAYOUT_TRANSACTION.c.transaction_type)
Index("idx_shpfy_ptx_company", SHOPIFY_PAYOUT_TRANSACTION.c.company_id)

# ==================================================================
# TABLE 8: shopify_dispute
# Mirror of Shopify disputes (chargebacks).
# ==================================================================
SHOPIFY_DISPUTE = Table(
    "shopify_dispute", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("shopify_account_id", Text,
           ForeignKey("shopify_account.id", ondelete="RESTRICT"), nullable=False),
    Column("shopify_dispute_id", Text, nullable=False),
    # Nullable: a dispute can arrive before its order has been synced.
    Column("shopify_order_id_local", Text,
           ForeignKey("shopify_order.id", ondelete="RESTRICT")),
    Column("dispute_type", Text),
    Column("status", Text, nullable=False, server_default=text("'needs_response'")),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("fee_amount", Text, nullable=False, server_default=text("'0'")),
    Column("reason", Text),
    Column("evidence_due_by", Text),
    Column("gl_status", Text, nullable=False, server_default=text("'pending'")),
    Column("gl_voucher_id", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ( 'needs_response','under_review','charge_refunded', "
        "'accepted','won','lost' )",
        name="ck_shopify_dispute_status"),
    CheckConstraint("gl_status IN ('pending','posted','failed','skipped')",
                    name="ck_shopify_dispute_gl_status"),
    UniqueConstraint("shopify_account_id", "shopify_dispute_id"),
)

Index("idx_shpfy_dsp_acct", SHOPIFY_DISPUTE.c.shopify_account_id)
Index("idx_shpfy_dsp_sid", SHOPIFY_DISPUTE.c.shopify_dispute_id)
Index("idx_shpfy_dsp_order", SHOPIFY_DISPUTE.c.shopify_order_id_local)
Index("idx_shpfy_dsp_status", SHOPIFY_DISPUTE.c.status)
Index("idx_shpfy_dsp_company", SHOPIFY_DISPUTE.c.company_id)

# ==================================================================
# TABLE 9: shopify_gl_rule
# Configurable rules for mapping Shopify transaction types to GL accounts.
# ==================================================================
SHOPIFY_GL_RULE = Table(
    "shopify_gl_rule", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("shopify_account_id", Text,
           ForeignKey("shopify_account.id", ondelete="RESTRICT"), nullable=False),
    Column("rule_name", Text, nullable=False),
    Column("transaction_type", Text, nullable=False),
    Column("debit_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("credit_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("priority", Integer, nullable=False, server_default=text("0")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "transaction_type IN ( 'order','refund','payout','dispute', "
        "'gift_card_sale','gift_card_redeem','fee','reserve' )",
        name="ck_shopify_gl_rule_transaction_type"),
    CheckConstraint("is_active IN (0,1)", name="ck_shopify_gl_rule_is_active"),
    UniqueConstraint("shopify_account_id", "rule_name"),
)

Index("idx_shpfy_glr_acct", SHOPIFY_GL_RULE.c.shopify_account_id)
Index("idx_shpfy_glr_type", SHOPIFY_GL_RULE.c.transaction_type)
Index("idx_shpfy_glr_company", SHOPIFY_GL_RULE.c.company_id)

# ==================================================================
# TABLE 10: shopify_reconciliation_run
# Tracks each reconciliation run between Shopify payouts and GL.
# ==================================================================
SHOPIFY_RECONCILIATION_RUN = Table(
    "shopify_reconciliation_run", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("shopify_account_id", Text,
           ForeignKey("shopify_account.id", ondelete="RESTRICT"), nullable=False),
    Column("run_date", Text, nullable=False),
    Column("period_start", Text, nullable=False),
    Column("period_end", Text, nullable=False),
    Column("total_orders", Integer, nullable=False, server_default=text("0")),
    Column("total_payouts", Integer, nullable=False, server_default=text("0")),
    Column("orders_matched", Integer, nullable=False, server_default=text("0")),
    Column("orders_unmatched", Integer, nullable=False, server_default=text("0")),
    Column("payouts_matched", Integer, nullable=False, server_default=text("0")),
    Column("payouts_unmatched", Integer, nullable=False, server_default=text("0")),
    Column("expected_clearing_balance", Text, nullable=False,
           server_default=text("'0'")),
    Column("actual_clearing_balance", Text, nullable=False,
           server_default=text("'0'")),
    Column("discrepancy_amount", Text, nullable=False, server_default=text("'0'")),
    Column("status", Text, nullable=False, server_default=text("'running'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("status IN ('running','completed','discrepancy','failed')",
                    name="ck_shopify_reconciliation_run_status"),
)

Index("idx_shpfy_rr_acct", SHOPIFY_RECONCILIATION_RUN.c.shopify_account_id)
Index("idx_shpfy_rr_status", SHOPIFY_RECONCILIATION_RUN.c.status)
Index("idx_shpfy_rr_company", SHOPIFY_RECONCILIATION_RUN.c.company_id)

# ==================================================================
# TABLE 11: shopify_sync_job
# Tracks each sync operation (orders, products, customers, payouts, etc.).
# ==================================================================
SHOPIFY_SYNC_JOB = Table(
    "shopify_sync_job", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("shopify_account_id", Text,
           ForeignKey("shopify_account.id", ondelete="RESTRICT"), nullable=False),
    Column("sync_type", Text, nullable=False),
    Column("sync_mode", Text, nullable=False, server_default=text("'incremental'")),
    Column("status", Text, nullable=False, server_default=text("'pending'")),
    Column("records_processed", Integer, nullable=False, server_default=text("0")),
    Column("records_created", Integer, nullable=False, server_default=text("0")),
    Column("records_updated", Integer, nullable=False, server_default=text("0")),
    Column("records_failed", Integer, nullable=False, server_default=text("0")),
    Column("started_at", Text),
    Column("completed_at", Text),
    Column("error_message", Text),
    Column("cursor", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "sync_type IN ( 'orders','products','customers','payouts', "
        "'inventory','disputes','full' )",
        name="ck_shopify_sync_job_sync_type"),
    CheckConstraint("sync_mode IN ('full','incremental')",
                    name="ck_shopify_sync_job_sync_mode"),
    CheckConstraint(
        "status IN ('pending','running','completed','failed','cancelled')",
        name="ck_shopify_sync_job_status"),
)

Index("idx_shpfy_sync_acct", SHOPIFY_SYNC_JOB.c.shopify_account_id)
Index("idx_shpfy_sync_status", SHOPIFY_SYNC_JOB.c.status)
Index("idx_shpfy_sync_type", SHOPIFY_SYNC_JOB.c.sync_type)
Index("idx_shpfy_sync_company", SHOPIFY_SYNC_JOB.c.company_id)


def _require_foundation(db_path):
    """The pre-conversion installer's foundation probe, asked through the seam.

    The probed 8 tables and the wording are this module's own, unchanged; only
    the mechanism changed. The original read SQLite's own catalog table directly,
    so the guard that exists to produce a friendly error was itself SQLite-only —
    on PostgreSQL it would have raised before it could explain anything, and
    ``seam.table_exists`` answers on both backends (ADR-0034 bulk-39). This note
    names that catalog table in prose rather than as the identifier, because the
    seam-bypass ratchet counts string literals and an installers bucket that is
    only allowed to fall should not rise for a docstring.
    """
    from erpclaw_lib import seam

    missing = [t for t in REQUIRED_FOUNDATION if not seam.table_exists(t, db_path)]
    if missing:
        print(f"ERROR: Foundation tables missing: {', '.join(missing)}")
        print("Run erpclaw first: clawhub install erpclaw")
        sys.exit(1)


def create_shopify_tables(db_path=None):
    """Create Shopify tables and indexes on whichever backend is configured.

    Same contract as before the ADR-0034 conversion: idempotent, and the returned
    counts are what was ACTUALLY created rather than what was declared. It no
    longer reports `migrations_applied` — this installer runs no migration.
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
    result = create_shopify_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
