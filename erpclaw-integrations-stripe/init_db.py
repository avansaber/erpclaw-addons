#!/usr/bin/env python3
"""ERPClaw Integrations — Stripe Deep Integration schema extension.

Adds 17 Stripe-specific tables to the shared database for full-cycle
payment reconciliation: charges, refunds, disputes, payouts, subscriptions,
invoices, Connect platform fees, and automated GL posting rules.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys

DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
DISPLAY_NAME = "ERPClaw Integrations — Stripe"

REQUIRED_FOUNDATION = [
    "company", "account", "customer", "sales_invoice",
    "payment_entry", "gl_entry", "naming_series", "audit_log",
]


def create_stripe_tables(db_path=None):
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
    # TABLE 1: stripe_account
    # Central configuration for each Stripe account (test or live).
    # Stores encrypted API key, webhook secret, and GL account mappings.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_account (
            id                          TEXT PRIMARY KEY,
            naming_series               TEXT,
            company_id                  TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            account_name                TEXT NOT NULL,
            stripe_account_id           TEXT,
            restricted_key_enc          TEXT NOT NULL,
            webhook_secret_enc          TEXT,
            mode                        TEXT NOT NULL DEFAULT 'test'
                                        CHECK(mode IN ('test','live')),
            is_connect_platform         INTEGER NOT NULL DEFAULT 0 CHECK(is_connect_platform IN (0,1)),
            default_currency            TEXT NOT NULL DEFAULT 'USD',
            payout_schedule             TEXT,
            stripe_clearing_account_id  TEXT REFERENCES account(id) ON DELETE RESTRICT,
            stripe_fees_account_id      TEXT REFERENCES account(id) ON DELETE RESTRICT,
            stripe_payout_account_id    TEXT REFERENCES account(id) ON DELETE RESTRICT,
            dispute_expense_account_id  TEXT REFERENCES account(id) ON DELETE RESTRICT,
            unearned_revenue_account_id TEXT REFERENCES account(id) ON DELETE RESTRICT,
            platform_revenue_account_id TEXT REFERENCES account(id) ON DELETE RESTRICT,
            last_sync_at                TEXT,
            sync_from_date              TEXT,
            status                      TEXT NOT NULL DEFAULT 'active'
                                        CHECK(status IN ('active','paused','error','disabled')),
            created_at                  TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at                  TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_acct_company ON stripe_account(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_acct_status ON stripe_account(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_acct_mode ON stripe_account(mode)")
    indexes_created += 3

    # ==================================================================
    # TABLE 2: stripe_sync_job
    # Tracks each sync operation (full, incremental, webhook, historical).
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_sync_job (
            id                  TEXT PRIMARY KEY,
            stripe_account_id   TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            sync_type           TEXT NOT NULL
                                CHECK(sync_type IN ('full','incremental','webhook','historical_import')),
            object_type         TEXT NOT NULL
                                CHECK(object_type IN ('balance_transaction','charge','refund','dispute','payout','customer','invoice','subscription','all')),
            status              TEXT NOT NULL DEFAULT 'pending'
                                CHECK(status IN ('pending','running','completed','failed','cancelled')),
            records_fetched     INTEGER NOT NULL DEFAULT 0,
            records_processed   INTEGER NOT NULL DEFAULT 0,
            records_failed      INTEGER NOT NULL DEFAULT 0,
            cursor_position     TEXT,
            sync_from           TEXT,
            sync_to             TEXT,
            error_message       TEXT,
            started_at          TEXT,
            completed_at        TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_sync_acct ON stripe_sync_job(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_sync_status ON stripe_sync_job(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_sync_company ON stripe_sync_job(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_sync_type ON stripe_sync_job(sync_type)")
    indexes_created += 4

    # ==================================================================
    # TABLE 3: stripe_balance_transaction
    # Mirror of Stripe Balance Transaction objects. Core reconciliation entity.
    # Amounts stored in DOLLARS (Decimal TEXT), not cents.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_balance_transaction (
            id                  TEXT PRIMARY KEY,
            stripe_id           TEXT NOT NULL UNIQUE,
            stripe_account_id   TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            type                TEXT,
            reporting_category  TEXT,
            source_id           TEXT,
            source_type         TEXT,
            amount              TEXT NOT NULL DEFAULT '0',
            fee                 TEXT NOT NULL DEFAULT '0',
            net                 TEXT NOT NULL DEFAULT '0',
            currency            TEXT NOT NULL DEFAULT 'USD',
            description         TEXT,
            available_on        TEXT,
            created_stripe      TEXT,
            payout_id           TEXT,
            status              TEXT DEFAULT 'available'
                                CHECK(status IN ('available','pending')),
            reconciled          INTEGER NOT NULL DEFAULT 0 CHECK(reconciled IN (0,1)),
            reconciled_at       TEXT,
            gl_voucher_id       TEXT,
            gl_voucher_type     TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_bt_acct ON stripe_balance_transaction(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_bt_stripe ON stripe_balance_transaction(stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_bt_type ON stripe_balance_transaction(type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_bt_status ON stripe_balance_transaction(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_bt_reconciled ON stripe_balance_transaction(reconciled)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_bt_payout ON stripe_balance_transaction(payout_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_bt_company ON stripe_balance_transaction(company_id)")
    indexes_created += 7

    # ==================================================================
    # TABLE 4: stripe_charge
    # Mirror of Stripe Charge objects, linked to erpclaw customer/invoice/payment.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_charge (
            id                      TEXT PRIMARY KEY,
            stripe_id               TEXT NOT NULL UNIQUE,
            stripe_account_id       TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            amount                  TEXT NOT NULL DEFAULT '0',
            currency                TEXT NOT NULL DEFAULT 'USD',
            customer_stripe_id      TEXT,
            description             TEXT,
            payment_method_type     TEXT,
            payment_intent_id       TEXT,
            invoice_stripe_id       TEXT,
            status                  TEXT NOT NULL DEFAULT 'pending'
                                    CHECK(status IN ('succeeded','pending','failed','refunded','disputed')),
            amount_refunded         TEXT NOT NULL DEFAULT '0',
            disputed                INTEGER NOT NULL DEFAULT 0 CHECK(disputed IN (0,1)),
            failure_code            TEXT,
            erpclaw_customer_id     TEXT,
            erpclaw_invoice_id      TEXT,
            erpclaw_payment_entry_id TEXT,
            metadata                TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_stripe          TEXT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_chg_acct ON stripe_charge(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_chg_stripe ON stripe_charge(stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_chg_status ON stripe_charge(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_chg_customer ON stripe_charge(customer_stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_chg_company ON stripe_charge(company_id)")
    indexes_created += 5

    # ==================================================================
    # TABLE 5: stripe_refund
    # Mirror of Stripe Refund objects.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_refund (
            id                      TEXT PRIMARY KEY,
            stripe_id               TEXT NOT NULL UNIQUE,
            stripe_account_id       TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            charge_id               TEXT REFERENCES stripe_charge(id) ON DELETE RESTRICT,
            charge_stripe_id        TEXT,
            amount                  TEXT NOT NULL DEFAULT '0',
            currency                TEXT NOT NULL DEFAULT 'USD',
            reason                  TEXT,
            status                  TEXT NOT NULL DEFAULT 'pending'
                                    CHECK(status IN ('pending','succeeded','failed','canceled')),
            erpclaw_credit_note_id  TEXT,
            erpclaw_payment_entry_id TEXT,
            metadata                TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_stripe          TEXT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_ref_acct ON stripe_refund(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_ref_stripe ON stripe_refund(stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_ref_charge ON stripe_refund(charge_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_ref_company ON stripe_refund(company_id)")
    indexes_created += 4

    # ==================================================================
    # TABLE 6: stripe_dispute
    # Mirror of Stripe Dispute objects (chargebacks).
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_dispute (
            id                      TEXT PRIMARY KEY,
            stripe_id               TEXT NOT NULL UNIQUE,
            stripe_account_id       TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            charge_id               TEXT REFERENCES stripe_charge(id) ON DELETE RESTRICT,
            charge_stripe_id        TEXT,
            amount                  TEXT NOT NULL DEFAULT '0',
            currency                TEXT NOT NULL DEFAULT 'USD',
            reason                  TEXT,
            status                  TEXT NOT NULL DEFAULT 'needs_response'
                                    CHECK(status IN ('warning_needs_response','warning_under_review','needs_response','under_review','won','lost')),
            evidence_due_by         TEXT,
            erpclaw_journal_entry_id TEXT,
            resolution_amount       TEXT,
            metadata                TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_stripe          TEXT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_dsp_acct ON stripe_dispute(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_dsp_stripe ON stripe_dispute(stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_dsp_charge ON stripe_dispute(charge_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_dsp_status ON stripe_dispute(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_dsp_company ON stripe_dispute(company_id)")
    indexes_created += 5

    # ==================================================================
    # TABLE 7: stripe_payout
    # Mirror of Stripe Payout objects (bank transfers from Stripe balance).
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_payout (
            id                      TEXT PRIMARY KEY,
            stripe_id               TEXT NOT NULL UNIQUE,
            stripe_account_id       TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            amount                  TEXT NOT NULL DEFAULT '0',
            currency                TEXT NOT NULL DEFAULT 'USD',
            arrival_date            TEXT,
            method                  TEXT,
            description             TEXT,
            status                  TEXT NOT NULL DEFAULT 'pending'
                                    CHECK(status IN ('paid','pending','in_transit','canceled','failed')),
            failure_code            TEXT,
            destination_bank_last4  TEXT,
            transaction_count       INTEGER NOT NULL DEFAULT 0,
            reconciled              INTEGER NOT NULL DEFAULT 0 CHECK(reconciled IN (0,1)),
            erpclaw_payment_entry_id TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_stripe          TEXT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_pay_acct ON stripe_payout(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_pay_stripe ON stripe_payout(stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_pay_status ON stripe_payout(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_pay_company ON stripe_payout(company_id)")
    indexes_created += 4

    # ==================================================================
    # TABLE 8: stripe_invoice
    # Mirror of Stripe Invoice objects (for recurring billing / subscriptions).
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_invoice (
            id                      TEXT PRIMARY KEY,
            stripe_id               TEXT NOT NULL UNIQUE,
            stripe_account_id       TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            customer_stripe_id      TEXT,
            number                  TEXT,
            amount_due              TEXT NOT NULL DEFAULT '0',
            amount_paid             TEXT NOT NULL DEFAULT '0',
            amount_remaining        TEXT NOT NULL DEFAULT '0',
            currency                TEXT NOT NULL DEFAULT 'USD',
            status                  TEXT DEFAULT 'draft'
                                    CHECK(status IN ('draft','open','paid','void','uncollectible')),
            subscription_stripe_id  TEXT,
            period_start            TEXT,
            period_end              TEXT,
            erpclaw_invoice_id      TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_stripe          TEXT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_inv_acct ON stripe_invoice(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_inv_stripe ON stripe_invoice(stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_inv_status ON stripe_invoice(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_inv_customer ON stripe_invoice(customer_stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_inv_company ON stripe_invoice(company_id)")
    indexes_created += 5

    # ==================================================================
    # TABLE 9: stripe_subscription
    # Mirror of Stripe Subscription objects.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_subscription (
            id                          TEXT PRIMARY KEY,
            stripe_id                   TEXT NOT NULL UNIQUE,
            stripe_account_id           TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            customer_stripe_id          TEXT,
            status                      TEXT NOT NULL DEFAULT 'active'
                                        CHECK(status IN ('active','past_due','canceled','unpaid','trialing','incomplete')),
            current_period_start        TEXT,
            current_period_end          TEXT,
            cancel_at_period_end        INTEGER NOT NULL DEFAULT 0 CHECK(cancel_at_period_end IN (0,1)),
            canceled_at                 TEXT,
            plan_interval               TEXT,
            plan_amount                 TEXT NOT NULL DEFAULT '0',
            currency                    TEXT NOT NULL DEFAULT 'USD',
            erpclaw_revenue_contract_id TEXT,
            company_id                  TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_stripe              TEXT,
            created_at                  TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_sub_acct ON stripe_subscription(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_sub_stripe ON stripe_subscription(stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_sub_status ON stripe_subscription(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_sub_customer ON stripe_subscription(customer_stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_sub_company ON stripe_subscription(company_id)")
    indexes_created += 5

    # ==================================================================
    # TABLE 10: stripe_customer_map
    # Maps Stripe customer IDs to erpclaw customer IDs.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_customer_map (
            id                  TEXT PRIMARY KEY,
            stripe_account_id   TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            stripe_customer_id  TEXT NOT NULL,
            erpclaw_customer_id TEXT REFERENCES customer(id) ON DELETE RESTRICT,
            stripe_email        TEXT,
            stripe_name         TEXT,
            match_method        TEXT DEFAULT 'manual'
                                CHECK(match_method IN ('manual','email','name','metadata')),
            match_confidence    TEXT NOT NULL DEFAULT '1.0',
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(stripe_account_id, stripe_customer_id)
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_cmap_acct ON stripe_customer_map(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_cmap_erpclaw ON stripe_customer_map(erpclaw_customer_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_cmap_company ON stripe_customer_map(company_id)")
    indexes_created += 3

    # ==================================================================
    # TABLE 11: stripe_webhook_event
    # Incoming Stripe webhook events with idempotent processing.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_deep_webhook_event (
            id                  TEXT PRIMARY KEY,
            stripe_account_id   TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            stripe_event_id     TEXT NOT NULL UNIQUE,
            event_type          TEXT NOT NULL,
            api_version         TEXT,
            object_id           TEXT,
            object_type         TEXT,
            payload             TEXT,
            processed           INTEGER NOT NULL DEFAULT 0 CHECK(processed IN (0,1)),
            process_attempts    INTEGER NOT NULL DEFAULT 0,
            max_attempts        INTEGER NOT NULL DEFAULT 3,
            processed_at        TEXT,
            error_message       TEXT,
            created_stripe      TEXT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_dwh_acct ON stripe_deep_webhook_event(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_dwh_event ON stripe_deep_webhook_event(stripe_event_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_dwh_type ON stripe_deep_webhook_event(event_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_dwh_processed ON stripe_deep_webhook_event(processed)")
    indexes_created += 4

    # ==================================================================
    # TABLE 12: stripe_credit_note
    # Mirror of Stripe Credit Note objects.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_credit_note (
            id                      TEXT PRIMARY KEY,
            stripe_id               TEXT NOT NULL UNIQUE,
            stripe_account_id       TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            invoice_stripe_id       TEXT,
            customer_stripe_id      TEXT,
            amount                  TEXT NOT NULL DEFAULT '0',
            currency                TEXT NOT NULL DEFAULT 'USD',
            reason                  TEXT,
            status                  TEXT DEFAULT 'issued',
            erpclaw_credit_note_id  TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_stripe          TEXT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_cn_acct ON stripe_credit_note(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_cn_stripe ON stripe_credit_note(stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_cn_company ON stripe_credit_note(company_id)")
    indexes_created += 3

    # ==================================================================
    # TABLE 13: stripe_application_fee
    # Connect platform application fees collected.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_application_fee (
            id                      TEXT PRIMARY KEY,
            stripe_id               TEXT NOT NULL UNIQUE,
            stripe_account_id       TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            amount                  TEXT NOT NULL DEFAULT '0',
            currency                TEXT NOT NULL DEFAULT 'USD',
            charge_stripe_id        TEXT,
            account_stripe_id       TEXT,
            refunded_amount         TEXT NOT NULL DEFAULT '0',
            erpclaw_journal_entry_id TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_stripe          TEXT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_af_acct ON stripe_application_fee(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_af_stripe ON stripe_application_fee(stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_af_company ON stripe_application_fee(company_id)")
    indexes_created += 3

    # ==================================================================
    # TABLE 14: stripe_transfer
    # Connect platform transfers between accounts.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_transfer (
            id                      TEXT PRIMARY KEY,
            stripe_id               TEXT NOT NULL UNIQUE,
            stripe_account_id       TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            amount                  TEXT NOT NULL DEFAULT '0',
            currency                TEXT NOT NULL DEFAULT 'USD',
            destination_account     TEXT,
            description             TEXT,
            reversed                INTEGER NOT NULL DEFAULT 0 CHECK(reversed IN (0,1)),
            erpclaw_journal_entry_id TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_stripe          TEXT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_xfr_acct ON stripe_transfer(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_xfr_stripe ON stripe_transfer(stripe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_xfr_company ON stripe_transfer(company_id)")
    indexes_created += 3

    # ==================================================================
    # TABLE 15: stripe_gl_rule
    # Configurable rules for mapping Stripe transaction types to GL accounts.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_gl_rule (
            id                  TEXT PRIMARY KEY,
            stripe_account_id   TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            transaction_type    TEXT NOT NULL
                                CHECK(transaction_type IN ('charge','refund','dispute','payout','connect_fee','other')),
            match_field         TEXT,
            match_value         TEXT,
            debit_account_id    TEXT REFERENCES account(id) ON DELETE RESTRICT,
            credit_account_id   TEXT REFERENCES account(id) ON DELETE RESTRICT,
            fee_account_id      TEXT REFERENCES account(id) ON DELETE RESTRICT,
            cost_center_id      TEXT,
            priority            INTEGER NOT NULL DEFAULT 0,
            is_active           INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_glr_acct ON stripe_gl_rule(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_glr_type ON stripe_gl_rule(transaction_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_glr_company ON stripe_gl_rule(company_id)")
    indexes_created += 3

    # ==================================================================
    # TABLE 16: stripe_fee_detail
    # Breakdown of fees per balance transaction (processing, Stripe fee, etc.).
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_fee_detail (
            id                      TEXT PRIMARY KEY,
            balance_transaction_id  TEXT NOT NULL REFERENCES stripe_balance_transaction(id) ON DELETE CASCADE,
            fee_type                TEXT NOT NULL,
            amount                  TEXT NOT NULL DEFAULT '0',
            currency                TEXT NOT NULL DEFAULT 'USD',
            description             TEXT,
            application             TEXT,
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_fd_bt ON stripe_fee_detail(balance_transaction_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_fd_type ON stripe_fee_detail(fee_type)")
    indexes_created += 2

    # ==================================================================
    # TABLE 17: stripe_reconciliation_run
    # Tracks each reconciliation run between Stripe and GL.
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stripe_reconciliation_run (
            id                          TEXT PRIMARY KEY,
            stripe_account_id           TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
            run_date                    TEXT NOT NULL,
            period_start                TEXT NOT NULL,
            period_end                  TEXT NOT NULL,
            transactions_processed      INTEGER NOT NULL DEFAULT 0,
            transactions_matched        INTEGER NOT NULL DEFAULT 0,
            transactions_unmatched      INTEGER NOT NULL DEFAULT 0,
            amount_reconciled           TEXT NOT NULL DEFAULT '0',
            amount_unreconciled         TEXT NOT NULL DEFAULT '0',
            status                      TEXT NOT NULL DEFAULT 'running'
                                        CHECK(status IN ('running','completed','failed')),
            notes                       TEXT,
            company_id                  TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at                  TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_rr_acct ON stripe_reconciliation_run(stripe_account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_rr_status ON stripe_reconciliation_run(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stripe_rr_company ON stripe_reconciliation_run(company_id)")
    indexes_created += 3

    conn.commit()
    conn.close()

    return {
        "database": db_path,
        "tables": tables_created,
        "indexes": indexes_created,
    }


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else None
    result = create_stripe_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
