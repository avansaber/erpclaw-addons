"""ERPClaw Treasury -- schema initialization.

Creates 7 tables for bank accounts, cash management, investments,
inter-company transfers, and cash flow forecasting in the shared ERPClaw database.
Includes cash_flow_forecast (moved from core init_schema.py).
Requires company table to exist (erpclaw-setup).
"""
import os
import sqlite3
import sys

DB_PATH = os.environ.get(
    "ERPCLAW_DB_PATH",
    os.path.expanduser("~/.openclaw/erpclaw/data.sqlite"),
)


def init_treasury_schema(db_path: str = DB_PATH) -> dict:
    """Create treasury module tables and indexes."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")

    tables_created = 0
    indexes_created = 0

    # -------------------------------------------------------------------
    # 1. bank_account_extended -- bank accounts for treasury management
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bank_account_extended (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            bank_name           TEXT NOT NULL,
            account_name        TEXT NOT NULL,
            account_number      TEXT,
            routing_number      TEXT,
            account_type        TEXT NOT NULL DEFAULT 'checking'
                                CHECK(account_type IN ('checking','savings','money_market','cd','line_of_credit','other')),
            currency            TEXT NOT NULL DEFAULT 'USD',
            current_balance     TEXT NOT NULL DEFAULT '0',
            last_reconciled_date TEXT,
            gl_account_id       TEXT,
            is_active           INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            notes               TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bank_account_ext_company ON bank_account_extended(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bank_account_ext_type ON bank_account_extended(account_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bank_account_ext_active ON bank_account_extended(is_active)")
    indexes_created += 3

    # -------------------------------------------------------------------
    # 2. cash_position -- point-in-time cash snapshots
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cash_position (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            position_date       TEXT NOT NULL DEFAULT (date('now')),
            total_cash          TEXT NOT NULL DEFAULT '0',
            total_receivables   TEXT NOT NULL DEFAULT '0',
            total_payables      TEXT NOT NULL DEFAULT '0',
            net_position        TEXT NOT NULL DEFAULT '0',
            notes               TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cash_position_company ON cash_position(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cash_position_date ON cash_position(position_date)")
    indexes_created += 2

    # -------------------------------------------------------------------
    # 3. cash_forecast -- projected cash flow forecasts
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cash_forecast (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            forecast_name       TEXT NOT NULL,
            forecast_date       TEXT NOT NULL DEFAULT (date('now')),
            period_start        TEXT NOT NULL,
            period_end          TEXT NOT NULL,
            expected_inflows    TEXT NOT NULL DEFAULT '0',
            expected_outflows   TEXT NOT NULL DEFAULT '0',
            net_forecast        TEXT NOT NULL DEFAULT '0',
            assumptions         TEXT,
            forecast_type       TEXT NOT NULL DEFAULT 'short_term'
                                CHECK(forecast_type IN ('short_term','medium_term','long_term')),
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cash_forecast_company ON cash_forecast(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cash_forecast_type ON cash_forecast(forecast_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cash_forecast_date ON cash_forecast(forecast_date)")
    indexes_created += 3

    # -------------------------------------------------------------------
    # 4. investment -- investment instruments
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS investment (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            name                TEXT NOT NULL,
            investment_type     TEXT NOT NULL DEFAULT 'cd'
                                CHECK(investment_type IN ('cd','money_market','treasury_bill','bond','mutual_fund','other')),
            institution         TEXT,
            account_number      TEXT,
            principal           TEXT NOT NULL DEFAULT '0',
            current_value       TEXT NOT NULL DEFAULT '0',
            interest_rate       TEXT DEFAULT '0',
            purchase_date       TEXT,
            maturity_date       TEXT,
            gl_account_id       TEXT,
            status              TEXT NOT NULL DEFAULT 'active'
                                CHECK(status IN ('active','matured','redeemed','cancelled')),
            notes               TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_investment_company ON investment(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_investment_type ON investment(investment_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_investment_status ON investment(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_investment_maturity ON investment(maturity_date)")
    indexes_created += 4

    # -------------------------------------------------------------------
    # 5. investment_transaction -- transactions against investments
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS investment_transaction (
            id                  TEXT PRIMARY KEY,
            investment_id       TEXT NOT NULL REFERENCES investment(id) ON DELETE RESTRICT,
            transaction_type    TEXT NOT NULL DEFAULT 'purchase'
                                CHECK(transaction_type IN ('purchase','interest','dividend','redemption','fee','transfer')),
            transaction_date    TEXT NOT NULL DEFAULT (date('now')),
            amount              TEXT NOT NULL DEFAULT '0',
            reference           TEXT,
            notes               TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_inv_txn_investment ON investment_transaction(investment_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_inv_txn_company ON investment_transaction(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_inv_txn_date ON investment_transaction(transaction_date)")
    indexes_created += 3

    # -------------------------------------------------------------------
    # 6. inter_company_transfer -- fund transfers between companies
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS inter_company_transfer (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            from_company_id     TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            to_company_id       TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            amount              TEXT NOT NULL DEFAULT '0',
            transfer_date       TEXT NOT NULL DEFAULT (date('now')),
            reference           TEXT,
            reason              TEXT,
            status              TEXT NOT NULL DEFAULT 'draft'
                                CHECK(status IN ('draft','approved','completed','cancelled')),
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ict_company ON inter_company_transfer(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ict_from ON inter_company_transfer(from_company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ict_to ON inter_company_transfer(to_company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ict_status ON inter_company_transfer(status)")
    indexes_created += 4

    # -------------------------------------------------------------------
    # 7. cash_flow_forecast -- AI-generated cash flow projections
    #    (moved from core erpclaw-ai-engine tables in init_schema.py)
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cash_flow_forecast (
            id              TEXT PRIMARY KEY,
            forecast_date   TEXT NOT NULL,
            generated_at    TEXT DEFAULT (datetime('now')),
            horizon_days    INTEGER NOT NULL DEFAULT 30,
            starting_balance TEXT NOT NULL DEFAULT '0',
            projected_inflows TEXT,
            projected_outflows TEXT,
            projected_balance TEXT NOT NULL DEFAULT '0',
            confidence_interval TEXT,
            assumptions     TEXT,
            scenario        TEXT NOT NULL DEFAULT 'expected'
                            CHECK(scenario IN ('pessimistic','expected','optimistic')),
            expires_at      TEXT
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_date ON cash_flow_forecast(forecast_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_scenario ON cash_flow_forecast(scenario)")
    indexes_created += 2

    conn.commit()
    conn.close()

    return {
        "database": db_path,
        "tables": tables_created,
        "indexes": indexes_created,
    }


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    result = init_treasury_schema(path)
    print(f"ERPClaw Treasury schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
