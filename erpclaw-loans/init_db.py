#!/usr/bin/env python3
"""ERPClaw Loans schema extension — adds loan tables to the shared database.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys


DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")


def create_loans_tables(db_path):
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
        -- ERPClaw Loans Domain Tables
        -- ==========================================================

        CREATE TABLE IF NOT EXISTS loan_application (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            applicant_type  TEXT NOT NULL DEFAULT 'customer'
                            CHECK(applicant_type IN ('customer','employee','supplier')),
            applicant_id    TEXT NOT NULL,
            applicant_name  TEXT,
            loan_type       TEXT NOT NULL DEFAULT 'term_loan'
                            CHECK(loan_type IN ('term_loan','demand_loan','staff_loan','credit_line')),
            requested_amount TEXT NOT NULL DEFAULT '0',
            approved_amount TEXT NOT NULL DEFAULT '0',
            interest_rate   TEXT NOT NULL DEFAULT '0',
            repayment_method TEXT NOT NULL DEFAULT 'equal_installment'
                            CHECK(repayment_method IN ('equal_installment','equal_principal','bullet','custom')),
            repayment_periods INTEGER NOT NULL DEFAULT 12,
            application_date TEXT NOT NULL DEFAULT CURRENT_DATE,
            purpose         TEXT,
            collateral_description TEXT,
            collateral_value TEXT NOT NULL DEFAULT '0',
            status          TEXT NOT NULL DEFAULT 'draft'
                            CHECK(status IN ('draft','applied','approved','rejected','cancelled')),
            rejection_reason TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_loan_app_status ON loan_application(status);
        CREATE INDEX IF NOT EXISTS idx_loan_app_company ON loan_application(company_id);
        CREATE INDEX IF NOT EXISTS idx_loan_app_applicant ON loan_application(applicant_type, applicant_id);

        CREATE TABLE IF NOT EXISTS loan (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            loan_application_id TEXT REFERENCES loan_application(id) ON DELETE RESTRICT,
            applicant_type  TEXT NOT NULL DEFAULT 'customer'
                            CHECK(applicant_type IN ('customer','employee','supplier')),
            applicant_id    TEXT NOT NULL,
            applicant_name  TEXT,
            loan_type       TEXT NOT NULL DEFAULT 'term_loan'
                            CHECK(loan_type IN ('term_loan','demand_loan','staff_loan','credit_line')),
            loan_amount     TEXT NOT NULL DEFAULT '0',
            disbursed_amount TEXT NOT NULL DEFAULT '0',
            total_interest  TEXT NOT NULL DEFAULT '0',
            total_repaid    TEXT NOT NULL DEFAULT '0',
            outstanding_amount TEXT NOT NULL DEFAULT '0',
            interest_rate   TEXT NOT NULL DEFAULT '0',
            repayment_method TEXT NOT NULL DEFAULT 'equal_installment'
                            CHECK(repayment_method IN ('equal_installment','equal_principal','bullet','custom')),
            repayment_periods INTEGER NOT NULL DEFAULT 12,
            disbursement_date TEXT,
            maturity_date   TEXT,
            loan_account_id TEXT REFERENCES account(id) ON DELETE RESTRICT,
            interest_income_account_id TEXT REFERENCES account(id) ON DELETE RESTRICT,
            disbursement_account_id TEXT REFERENCES account(id) ON DELETE RESTRICT,
            status          TEXT NOT NULL DEFAULT 'draft'
                            CHECK(status IN ('draft','disbursed','partially_repaid','repaid','written_off','closed')),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_loan_status ON loan(status);
        CREATE INDEX IF NOT EXISTS idx_loan_company ON loan(company_id);
        CREATE INDEX IF NOT EXISTS idx_loan_applicant ON loan(applicant_type, applicant_id);
        CREATE INDEX IF NOT EXISTS idx_loan_app_ref ON loan(loan_application_id);

        CREATE TABLE IF NOT EXISTS loan_repayment_schedule (
            id              TEXT PRIMARY KEY,
            loan_id         TEXT NOT NULL REFERENCES loan(id) ON DELETE RESTRICT,
            installment_no  INTEGER NOT NULL,
            due_date        TEXT NOT NULL,
            principal_amount TEXT NOT NULL DEFAULT '0',
            interest_amount TEXT NOT NULL DEFAULT '0',
            total_amount    TEXT NOT NULL DEFAULT '0',
            paid_amount     TEXT NOT NULL DEFAULT '0',
            outstanding     TEXT NOT NULL DEFAULT '0',
            status          TEXT NOT NULL DEFAULT 'pending'
                            CHECK(status IN ('pending','partially_paid','paid','overdue','waived')),
            payment_date    TEXT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_lrs_loan ON loan_repayment_schedule(loan_id);
        CREATE INDEX IF NOT EXISTS idx_lrs_due_date ON loan_repayment_schedule(due_date);
        CREATE INDEX IF NOT EXISTS idx_lrs_status ON loan_repayment_schedule(status);

        CREATE TABLE IF NOT EXISTS loan_repayment (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            loan_id         TEXT NOT NULL REFERENCES loan(id) ON DELETE RESTRICT,
            repayment_date  TEXT NOT NULL DEFAULT CURRENT_DATE,
            principal_amount TEXT NOT NULL DEFAULT '0',
            interest_amount TEXT NOT NULL DEFAULT '0',
            penalty_amount  TEXT NOT NULL DEFAULT '0',
            total_amount    TEXT NOT NULL DEFAULT '0',
            payment_entry_id TEXT,
            payment_method  TEXT NOT NULL DEFAULT 'bank_transfer'
                            CHECK(payment_method IN ('cash','bank_transfer','check','auto_debit')),
            reference_number TEXT,
            remarks         TEXT,
            status          TEXT NOT NULL DEFAULT 'draft'
                            CHECK(status IN ('draft','submitted','cancelled')),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_lr_loan ON loan_repayment(loan_id);
        CREATE INDEX IF NOT EXISTS idx_lr_status ON loan_repayment(status);
        CREATE INDEX IF NOT EXISTS idx_lr_company ON loan_repayment(company_id);

        CREATE TABLE IF NOT EXISTS loan_write_off (
            id              TEXT PRIMARY KEY,
            loan_id         TEXT NOT NULL REFERENCES loan(id) ON DELETE RESTRICT,
            write_off_date  TEXT NOT NULL DEFAULT CURRENT_DATE,
            write_off_amount TEXT NOT NULL DEFAULT '0',
            outstanding_at_write_off TEXT NOT NULL DEFAULT '0',
            reason          TEXT,
            bad_debt_account_id TEXT REFERENCES account(id) ON DELETE RESTRICT,
            journal_entry_id TEXT,
            status          TEXT NOT NULL DEFAULT 'draft'
                            CHECK(status IN ('draft','submitted','cancelled')),
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_lwo_loan ON loan_write_off(loan_id);
        CREATE INDEX IF NOT EXISTS idx_lwo_company ON loan_write_off(company_id);
    """)

    conn.commit()
    conn.close()
    print(f"ERPClaw Loans tables created in {db_path}")


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_PATH
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    create_loans_tables(db_path)
