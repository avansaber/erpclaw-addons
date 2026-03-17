#!/usr/bin/env python3
"""ERPClaw POS schema extension — adds POS tables to the shared database.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys


DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")


def create_pos_tables(db_path):
    conn = sqlite3.connect(db_path)
    from erpclaw_lib.db import setup_pragmas
    setup_pragmas(conn)

    # Verify foundation exists
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    if "company" not in tables:
        print("ERROR: Foundation tables not found. Run erpclaw-setup first.")
        sys.exit(1)

    conn.executescript("""
        -- ==========================================================
        -- ERPClaw POS Domain Tables
        -- ==========================================================

        CREATE TABLE IF NOT EXISTS pos_profile (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            warehouse_id    TEXT,
            price_list_id   TEXT,
            default_payment_method TEXT NOT NULL DEFAULT 'cash'
                            CHECK(default_payment_method IN ('cash','card','mobile','split')),
            allow_discount  INTEGER NOT NULL DEFAULT 1 CHECK(allow_discount IN (0,1)),
            max_discount_pct TEXT NOT NULL DEFAULT '100',
            auto_print_receipt INTEGER NOT NULL DEFAULT 0 CHECK(auto_print_receipt IN (0,1)),
            is_active       INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_pos_profile_company ON pos_profile(company_id);

        CREATE TABLE IF NOT EXISTS pos_session (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            pos_profile_id  TEXT NOT NULL REFERENCES pos_profile(id) ON DELETE RESTRICT,
            cashier_name    TEXT,
            opening_amount  TEXT NOT NULL DEFAULT '0',
            closing_amount  TEXT,
            expected_amount TEXT,
            difference      TEXT,
            total_sales     TEXT NOT NULL DEFAULT '0',
            total_returns   TEXT NOT NULL DEFAULT '0',
            transaction_count INTEGER NOT NULL DEFAULT 0,
            opened_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            closed_at       TEXT,
            status          TEXT NOT NULL DEFAULT 'open'
                            CHECK(status IN ('open','closing','closed','reconciled')),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_pos_session_status ON pos_session(status);
        CREATE INDEX IF NOT EXISTS idx_pos_session_company ON pos_session(company_id);
        CREATE INDEX IF NOT EXISTS idx_pos_session_profile ON pos_session(pos_profile_id);

        CREATE TABLE IF NOT EXISTS pos_transaction (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            pos_session_id  TEXT NOT NULL REFERENCES pos_session(id) ON DELETE RESTRICT,
            customer_id     TEXT,
            customer_name   TEXT,
            subtotal        TEXT NOT NULL DEFAULT '0',
            discount_amount TEXT NOT NULL DEFAULT '0',
            discount_pct    TEXT NOT NULL DEFAULT '0',
            tax_amount      TEXT NOT NULL DEFAULT '0',
            grand_total     TEXT NOT NULL DEFAULT '0',
            paid_amount     TEXT NOT NULL DEFAULT '0',
            change_amount   TEXT NOT NULL DEFAULT '0',
            sales_invoice_id TEXT,
            receipt_number  TEXT,
            status          TEXT NOT NULL DEFAULT 'draft'
                            CHECK(status IN ('draft','held','submitted','voided','returned')),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_pos_txn_session ON pos_transaction(pos_session_id);
        CREATE INDEX IF NOT EXISTS idx_pos_txn_status ON pos_transaction(status);
        CREATE INDEX IF NOT EXISTS idx_pos_txn_company ON pos_transaction(company_id);

        CREATE TABLE IF NOT EXISTS pos_transaction_item (
            id              TEXT PRIMARY KEY,
            pos_transaction_id TEXT NOT NULL REFERENCES pos_transaction(id) ON DELETE CASCADE,
            item_id         TEXT NOT NULL,
            item_name       TEXT NOT NULL,
            item_code       TEXT,
            barcode         TEXT,
            qty             TEXT NOT NULL DEFAULT '1',
            rate            TEXT NOT NULL DEFAULT '0',
            discount_pct    TEXT NOT NULL DEFAULT '0',
            discount_amount TEXT NOT NULL DEFAULT '0',
            amount          TEXT NOT NULL DEFAULT '0',
            uom             TEXT NOT NULL DEFAULT 'Nos',
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_pos_txn_item_txn ON pos_transaction_item(pos_transaction_id);
        CREATE INDEX IF NOT EXISTS idx_pos_txn_item_item ON pos_transaction_item(item_id);

        CREATE TABLE IF NOT EXISTS pos_payment (
            id              TEXT PRIMARY KEY,
            pos_transaction_id TEXT NOT NULL REFERENCES pos_transaction(id) ON DELETE CASCADE,
            payment_method  TEXT NOT NULL DEFAULT 'cash'
                            CHECK(payment_method IN ('cash','card','mobile','check','gift_card','other')),
            amount          TEXT NOT NULL DEFAULT '0',
            reference       TEXT,
            payment_entry_id TEXT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_pos_payment_txn ON pos_payment(pos_transaction_id);
    """)

    conn.commit()
    conn.close()
    print(f"ERPClaw POS tables created in {db_path}")


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_PATH
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    create_pos_tables(db_path)
