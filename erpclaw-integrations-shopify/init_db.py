#!/usr/bin/env python3
"""ERPClaw Integrations — Shopify Deep Integration schema extension.

Adds 11 Shopify-specific tables to the shared database for full-cycle
e-commerce order sync, payout reconciliation, dispute tracking, and
automated GL posting rules.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys

DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
DISPLAY_NAME = "ERPClaw Integrations — Shopify"

REQUIRED_FOUNDATION = [
    "company", "account", "customer", "sales_invoice",
    "payment_entry", "gl_entry", "naming_series", "audit_log",
]


def create_shopify_tables(db_path=None):
    db_path = db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    conn = sqlite3.connect(db_path)

    # Add erpclaw_lib to path for setup_pragmas
    lib_path = os.path.expanduser("~/.openclaw/erpclaw/lib")
    if lib_path not in sys.path:
        sys.path.insert(0, lib_path)
    from erpclaw_lib.db import setup_pragmas
    setup_pragmas(conn)

    # -- Verify ERPClaw foundation --
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    missing = [t for t in REQUIRED_FOUNDATION if t not in tables]
    if missing:
        print(f"ERROR: Foundation tables missing: {', '.join(missing)}")
        print("Run erpclaw first: clawhub install erpclaw")
        conn.close()
        sys.exit(1)

    tables_created = 0
    indexes_created = 0

    # ==================================================================
    # TABLE 1: shopify_account
    # Central configuration for each Shopify shop connection.
    # Stores encrypted access token, API version, GL account mappings,
    # and sync preferences.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_account (
            id                          TEXT PRIMARY KEY,
            naming_series               TEXT,
            company_id                  TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            shop_domain                 TEXT NOT NULL,
            shop_name                   TEXT,
            access_token_enc            TEXT NOT NULL,
            api_version                 TEXT NOT NULL DEFAULT '2026-01',
            currency                    TEXT NOT NULL DEFAULT 'USD',
            status                      TEXT NOT NULL DEFAULT 'active'
                                        CHECK(status IN ('active','paused','error','disabled')),
            clearing_account_id         TEXT REFERENCES account(id) ON DELETE RESTRICT,
            revenue_account_id          TEXT REFERENCES account(id) ON DELETE RESTRICT,
            shipping_revenue_account_id TEXT REFERENCES account(id) ON DELETE RESTRICT,
            tax_payable_account_id      TEXT REFERENCES account(id) ON DELETE RESTRICT,
            cogs_account_id             TEXT REFERENCES account(id) ON DELETE RESTRICT,
            inventory_account_id        TEXT REFERENCES account(id) ON DELETE RESTRICT,
            fee_account_id              TEXT REFERENCES account(id) ON DELETE RESTRICT,
            discount_account_id         TEXT REFERENCES account(id) ON DELETE RESTRICT,
            refund_account_id           TEXT REFERENCES account(id) ON DELETE RESTRICT,
            chargeback_account_id       TEXT REFERENCES account(id) ON DELETE RESTRICT,
            chargeback_fee_account_id   TEXT REFERENCES account(id) ON DELETE RESTRICT,
            gift_card_liability_account_id TEXT REFERENCES account(id) ON DELETE RESTRICT,
            reserve_account_id          TEXT REFERENCES account(id) ON DELETE RESTRICT,
            bank_account_id             TEXT REFERENCES account(id) ON DELETE RESTRICT,
            discount_method             TEXT NOT NULL DEFAULT 'net'
                                        CHECK(discount_method IN ('net','gross')),
            auto_post_gl                INTEGER NOT NULL DEFAULT 0 CHECK(auto_post_gl IN (0,1)),
            track_cogs                  INTEGER NOT NULL DEFAULT 0 CHECK(track_cogs IN (0,1)),
            default_warehouse_id        TEXT,
            last_orders_sync_at         TEXT,
            last_products_sync_at       TEXT,
            last_customers_sync_at      TEXT,
            last_payouts_sync_at        TEXT,
            last_disputes_sync_at       TEXT,
            created_at                  TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at                  TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_acct_company ON shopify_account(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_acct_status ON shopify_account(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_acct_domain ON shopify_account(shop_domain)")
    indexes_created += 3

    # ==================================================================
    # TABLE 2: shopify_order
    # Mirror of Shopify Order objects. Linked to erpclaw sales_invoice,
    # customer, and GL entries. Amounts stored as TEXT (Decimal).
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_order (
            id                      TEXT PRIMARY KEY,
            shopify_account_id      TEXT NOT NULL REFERENCES shopify_account(id) ON DELETE RESTRICT,
            shopify_order_id        TEXT NOT NULL,
            shopify_order_number    TEXT,
            order_date              TEXT,
            financial_status        TEXT,
            fulfillment_status      TEXT,
            currency                TEXT NOT NULL DEFAULT 'USD',
            subtotal_amount         TEXT NOT NULL DEFAULT '0',
            shipping_amount         TEXT NOT NULL DEFAULT '0',
            tax_amount              TEXT NOT NULL DEFAULT '0',
            discount_amount         TEXT NOT NULL DEFAULT '0',
            total_amount            TEXT NOT NULL DEFAULT '0',
            refunded_amount         TEXT NOT NULL DEFAULT '0',
            sales_invoice_id        TEXT,
            customer_id             TEXT,
            gl_status               TEXT NOT NULL DEFAULT 'pending'
                                    CHECK(gl_status IN ('pending','posted','failed','skipped')),
            gl_voucher_id           TEXT,
            payment_gateway         TEXT,
            is_gift_card_order      INTEGER NOT NULL DEFAULT 0 CHECK(is_gift_card_order IN (0,1)),
            has_refunds             INTEGER NOT NULL DEFAULT 0 CHECK(has_refunds IN (0,1)),
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at              TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(shopify_account_id, shopify_order_id)
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_ord_acct ON shopify_order(shopify_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_ord_sid ON shopify_order(shopify_order_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_ord_gl ON shopify_order(gl_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_ord_date ON shopify_order(order_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_ord_company ON shopify_order(company_id)")
    indexes_created += 5

    # ==================================================================
    # TABLE 3: shopify_order_line_item
    # Individual line items from Shopify orders, linked to erpclaw items
    # by SKU match.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_order_line_item (
            id                      TEXT PRIMARY KEY,
            shopify_order_id_local  TEXT NOT NULL REFERENCES shopify_order(id) ON DELETE CASCADE,
            shopify_line_item_id    TEXT NOT NULL,
            title                   TEXT,
            sku                     TEXT,
            quantity                INTEGER NOT NULL DEFAULT 1,
            unit_price              TEXT NOT NULL DEFAULT '0',
            discount_amount         TEXT NOT NULL DEFAULT '0',
            tax_amount              TEXT NOT NULL DEFAULT '0',
            total_amount            TEXT NOT NULL DEFAULT '0',
            item_id                 TEXT,
            is_gift_card            INTEGER NOT NULL DEFAULT 0 CHECK(is_gift_card IN (0,1)),
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_oli_order ON shopify_order_line_item(shopify_order_id_local)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_oli_sku ON shopify_order_line_item(sku)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_oli_item ON shopify_order_line_item(item_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_oli_company ON shopify_order_line_item(company_id)")
    indexes_created += 4

    # ==================================================================
    # TABLE 4: shopify_refund
    # Mirror of Shopify Refund objects, linked to erpclaw credit notes.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_refund (
            id                      TEXT PRIMARY KEY,
            shopify_order_id_local  TEXT NOT NULL REFERENCES shopify_order(id) ON DELETE RESTRICT,
            shopify_refund_id       TEXT NOT NULL,
            refund_date             TEXT,
            refund_amount           TEXT NOT NULL DEFAULT '0',
            tax_refund_amount       TEXT NOT NULL DEFAULT '0',
            shipping_refund_amount  TEXT NOT NULL DEFAULT '0',
            refund_type             TEXT NOT NULL DEFAULT 'partial'
                                    CHECK(refund_type IN ('full','partial')),
            gl_status               TEXT NOT NULL DEFAULT 'pending'
                                    CHECK(gl_status IN ('pending','posted','failed','skipped')),
            gl_voucher_id           TEXT,
            credit_note_id          TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(shopify_order_id_local, shopify_refund_id)
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_ref_order ON shopify_refund(shopify_order_id_local)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_ref_sid ON shopify_refund(shopify_refund_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_ref_gl ON shopify_refund(gl_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_ref_company ON shopify_refund(company_id)")
    indexes_created += 4

    # ==================================================================
    # TABLE 5: shopify_refund_line_item
    # Individual line items within a refund, tracking restocking.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_refund_line_item (
            id                      TEXT PRIMARY KEY,
            shopify_refund_id_local TEXT NOT NULL REFERENCES shopify_refund(id) ON DELETE CASCADE,
            shopify_line_item_id    TEXT NOT NULL,
            quantity                INTEGER NOT NULL DEFAULT 1,
            subtotal_amount         TEXT NOT NULL DEFAULT '0',
            restock_type            TEXT DEFAULT 'no_restock'
                                    CHECK(restock_type IN ('no_restock','cancel','return','legacy_restock')),
            item_id                 TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_rli_refund ON shopify_refund_line_item(shopify_refund_id_local)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_rli_item ON shopify_refund_line_item(item_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_rli_company ON shopify_refund_line_item(company_id)")
    indexes_created += 3

    # ==================================================================
    # TABLE 6: shopify_payout
    # Mirror of Shopify Payments payouts (bank transfers).
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_payout (
            id                      TEXT PRIMARY KEY,
            shopify_account_id      TEXT NOT NULL REFERENCES shopify_account(id) ON DELETE RESTRICT,
            shopify_payout_id       TEXT NOT NULL,
            issued_at               TEXT,
            status                  TEXT NOT NULL DEFAULT 'scheduled'
                                    CHECK(status IN ('scheduled','in_transit','paid','failed','cancelled')),
            gross_amount            TEXT NOT NULL DEFAULT '0',
            fee_amount              TEXT NOT NULL DEFAULT '0',
            net_amount              TEXT NOT NULL DEFAULT '0',
            charges_gross           TEXT NOT NULL DEFAULT '0',
            charges_fee             TEXT NOT NULL DEFAULT '0',
            refunds_gross           TEXT NOT NULL DEFAULT '0',
            refunds_fee             TEXT NOT NULL DEFAULT '0',
            adjustments_gross       TEXT NOT NULL DEFAULT '0',
            adjustments_fee         TEXT NOT NULL DEFAULT '0',
            reserved_funds_gross    TEXT NOT NULL DEFAULT '0',
            reserved_funds_fee      TEXT NOT NULL DEFAULT '0',
            gl_status               TEXT NOT NULL DEFAULT 'pending'
                                    CHECK(gl_status IN ('pending','posted','failed','skipped')),
            gl_voucher_id           TEXT,
            payment_entry_id        TEXT,
            reconciliation_status   TEXT NOT NULL DEFAULT 'unreconciled'
                                    CHECK(reconciliation_status IN (
                                        'unreconciled','auto_matched','manual_matched','discrepancy'
                                    )),
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(shopify_account_id, shopify_payout_id)
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_pay_acct ON shopify_payout(shopify_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_pay_sid ON shopify_payout(shopify_payout_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_pay_status ON shopify_payout(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_pay_gl ON shopify_payout(gl_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_pay_recon ON shopify_payout(reconciliation_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_pay_company ON shopify_payout(company_id)")
    indexes_created += 6

    # ==================================================================
    # TABLE 7: shopify_payout_transaction
    # Individual balance transactions within a payout.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_payout_transaction (
            id                      TEXT PRIMARY KEY,
            shopify_payout_id_local TEXT NOT NULL REFERENCES shopify_payout(id) ON DELETE CASCADE,
            shopify_balance_txn_id  TEXT NOT NULL,
            transaction_type        TEXT NOT NULL
                                    CHECK(transaction_type IN (
                                        'charge','refund','dispute','reserve',
                                        'adjustment','payout'
                                    )),
            gross_amount            TEXT NOT NULL DEFAULT '0',
            fee_amount              TEXT NOT NULL DEFAULT '0',
            net_amount              TEXT NOT NULL DEFAULT '0',
            source_order_id         TEXT,
            source_type             TEXT,
            processed_at            TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_ptx_payout ON shopify_payout_transaction(shopify_payout_id_local)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_ptx_type ON shopify_payout_transaction(transaction_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_ptx_company ON shopify_payout_transaction(company_id)")
    indexes_created += 3

    # ==================================================================
    # TABLE 8: shopify_dispute
    # Mirror of Shopify disputes (chargebacks).
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_dispute (
            id                      TEXT PRIMARY KEY,
            shopify_account_id      TEXT NOT NULL REFERENCES shopify_account(id) ON DELETE RESTRICT,
            shopify_dispute_id      TEXT NOT NULL,
            shopify_order_id_local  TEXT REFERENCES shopify_order(id) ON DELETE RESTRICT,
            dispute_type            TEXT,
            status                  TEXT NOT NULL DEFAULT 'needs_response'
                                    CHECK(status IN (
                                        'needs_response','under_review','charge_refunded',
                                        'accepted','won','lost'
                                    )),
            amount                  TEXT NOT NULL DEFAULT '0',
            fee_amount              TEXT NOT NULL DEFAULT '0',
            reason                  TEXT,
            evidence_due_by         TEXT,
            gl_status               TEXT NOT NULL DEFAULT 'pending'
                                    CHECK(gl_status IN ('pending','posted','failed','skipped')),
            gl_voucher_id           TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(shopify_account_id, shopify_dispute_id)
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_dsp_acct ON shopify_dispute(shopify_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_dsp_sid ON shopify_dispute(shopify_dispute_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_dsp_order ON shopify_dispute(shopify_order_id_local)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_dsp_status ON shopify_dispute(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_dsp_company ON shopify_dispute(company_id)")
    indexes_created += 5

    # ==================================================================
    # TABLE 9: shopify_gl_rule
    # Configurable rules for mapping Shopify transaction types to GL accounts.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_gl_rule (
            id                      TEXT PRIMARY KEY,
            shopify_account_id      TEXT NOT NULL REFERENCES shopify_account(id) ON DELETE RESTRICT,
            rule_name               TEXT NOT NULL,
            transaction_type        TEXT NOT NULL
                                    CHECK(transaction_type IN (
                                        'order','refund','payout','dispute',
                                        'gift_card_sale','gift_card_redeem','fee','reserve'
                                    )),
            debit_account_id        TEXT REFERENCES account(id) ON DELETE RESTRICT,
            credit_account_id       TEXT REFERENCES account(id) ON DELETE RESTRICT,
            is_active               INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            priority                INTEGER NOT NULL DEFAULT 0,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(shopify_account_id, rule_name)
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_glr_acct ON shopify_gl_rule(shopify_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_glr_type ON shopify_gl_rule(transaction_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_glr_company ON shopify_gl_rule(company_id)")
    indexes_created += 3

    # ==================================================================
    # TABLE 10: shopify_reconciliation_run
    # Tracks each reconciliation run between Shopify payouts and GL.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_reconciliation_run (
            id                          TEXT PRIMARY KEY,
            shopify_account_id          TEXT NOT NULL REFERENCES shopify_account(id) ON DELETE RESTRICT,
            run_date                    TEXT NOT NULL,
            period_start                TEXT NOT NULL,
            period_end                  TEXT NOT NULL,
            total_orders                INTEGER NOT NULL DEFAULT 0,
            total_payouts               INTEGER NOT NULL DEFAULT 0,
            orders_matched              INTEGER NOT NULL DEFAULT 0,
            orders_unmatched            INTEGER NOT NULL DEFAULT 0,
            payouts_matched             INTEGER NOT NULL DEFAULT 0,
            payouts_unmatched           INTEGER NOT NULL DEFAULT 0,
            expected_clearing_balance   TEXT NOT NULL DEFAULT '0',
            actual_clearing_balance     TEXT NOT NULL DEFAULT '0',
            discrepancy_amount          TEXT NOT NULL DEFAULT '0',
            status                      TEXT NOT NULL DEFAULT 'running'
                                        CHECK(status IN ('running','completed','discrepancy','failed')),
            company_id                  TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at                  TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_rr_acct ON shopify_reconciliation_run(shopify_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_rr_status ON shopify_reconciliation_run(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_rr_company ON shopify_reconciliation_run(company_id)")
    indexes_created += 3

    # ==================================================================
    # TABLE 11: shopify_sync_job
    # Tracks each sync operation (orders, products, customers, payouts, etc.).
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shopify_sync_job (
            id                  TEXT PRIMARY KEY,
            shopify_account_id  TEXT NOT NULL REFERENCES shopify_account(id) ON DELETE RESTRICT,
            sync_type           TEXT NOT NULL
                                CHECK(sync_type IN (
                                    'orders','products','customers','payouts',
                                    'inventory','disputes','full'
                                )),
            sync_mode           TEXT NOT NULL DEFAULT 'incremental'
                                CHECK(sync_mode IN ('full','incremental')),
            status              TEXT NOT NULL DEFAULT 'pending'
                                CHECK(status IN ('pending','running','completed','failed','cancelled')),
            records_processed   INTEGER NOT NULL DEFAULT 0,
            records_created     INTEGER NOT NULL DEFAULT 0,
            records_updated     INTEGER NOT NULL DEFAULT 0,
            records_failed      INTEGER NOT NULL DEFAULT 0,
            started_at          TEXT,
            completed_at        TEXT,
            error_message       TEXT,
            cursor              TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_sync_acct ON shopify_sync_job(shopify_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_sync_status ON shopify_sync_job(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_sync_type ON shopify_sync_job(sync_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shpfy_sync_company ON shopify_sync_job(company_id)")
    indexes_created += 4

    conn.commit()
    conn.close()

    return {
        "database": db_path,
        "tables": tables_created,
        "indexes": indexes_created,
    }


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else None
    result = create_shopify_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
