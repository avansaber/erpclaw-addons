"""ERPClaw Planning -- schema initialization.

Creates 4 planning tables (planning_scenario, planning_scenario_line, forecast, forecast_line)
in the shared ERPClaw database.
"""
import os
import sqlite3
import sys

DB_PATH = os.environ.get("ERPCLAW_DB_PATH", os.path.expanduser("~/.openclaw/erpclaw/data.sqlite"))


def init_planning_schema(db_path: str = DB_PATH) -> dict:
    """Create planning tables and indexes."""
    conn = sqlite3.connect(db_path)
    from erpclaw_lib.db import setup_pragmas
    setup_pragmas(conn)

    tables_created = 0
    indexes_created = 0

    # -----------------------------------------------------------------------
    # 1. planning_scenario -- budget/scenario planning headers
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS planning_scenario (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            name                TEXT NOT NULL,
            scenario_type       TEXT NOT NULL DEFAULT 'base'
                                CHECK(scenario_type IN ('base','best_case','worst_case','what_if','budget','custom')),
            description         TEXT,
            assumptions         TEXT,
            base_scenario_id    TEXT REFERENCES planning_scenario(id),
            fiscal_year         TEXT,
            total_revenue       TEXT NOT NULL DEFAULT '0',
            total_expense       TEXT NOT NULL DEFAULT '0',
            net_income          TEXT NOT NULL DEFAULT '0',
            status              TEXT NOT NULL DEFAULT 'draft'
                                CHECK(status IN ('draft','active','approved','locked','archived')),
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_planning_scenario_company ON planning_scenario(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_planning_scenario_status ON planning_scenario(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_planning_scenario_type ON planning_scenario(scenario_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_planning_scenario_fiscal_year ON planning_scenario(fiscal_year)")
    indexes_created += 4

    # -----------------------------------------------------------------------
    # 2. planning_scenario_line -- individual budget/scenario line items
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS planning_scenario_line (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            scenario_id         TEXT NOT NULL REFERENCES planning_scenario(id) ON DELETE CASCADE,
            account_name        TEXT NOT NULL,
            account_type        TEXT NOT NULL DEFAULT 'expense'
                                CHECK(account_type IN ('revenue','expense','asset','liability')),
            period              TEXT NOT NULL,
            amount              TEXT NOT NULL DEFAULT '0',
            notes               TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_planning_scenario_line_scenario ON planning_scenario_line(scenario_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_planning_scenario_line_company ON planning_scenario_line(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_planning_scenario_line_period ON planning_scenario_line(period)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_planning_scenario_line_account ON planning_scenario_line(account_name)")
    indexes_created += 4

    # -----------------------------------------------------------------------
    # 3. forecast -- forecast headers
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS forecast (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            name                TEXT NOT NULL,
            forecast_type       TEXT NOT NULL DEFAULT 'rolling'
                                CHECK(forecast_type IN ('rolling','static','driver_based','custom')),
            period_type         TEXT NOT NULL DEFAULT 'monthly'
                                CHECK(period_type IN ('weekly','monthly','quarterly','annual')),
            start_period        TEXT NOT NULL,
            end_period          TEXT NOT NULL,
            description         TEXT,
            status              TEXT NOT NULL DEFAULT 'draft'
                                CHECK(status IN ('draft','active','locked','archived')),
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_company ON forecast(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_status ON forecast(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_type ON forecast(forecast_type)")
    indexes_created += 3

    # -----------------------------------------------------------------------
    # 4. forecast_line -- individual forecast line items
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS forecast_line (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            forecast_id         TEXT NOT NULL REFERENCES forecast(id) ON DELETE CASCADE,
            account_name        TEXT NOT NULL,
            account_type        TEXT NOT NULL DEFAULT 'revenue'
                                CHECK(account_type IN ('revenue','expense','asset','liability')),
            period              TEXT NOT NULL,
            forecast_amount     TEXT NOT NULL DEFAULT '0',
            actual_amount       TEXT DEFAULT '0',
            variance            TEXT DEFAULT '0',
            variance_pct        TEXT DEFAULT '0',
            notes               TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_line_forecast ON forecast_line(forecast_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_line_company ON forecast_line(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_line_period ON forecast_line(period)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_line_account ON forecast_line(account_name)")
    indexes_created += 4

    conn.commit()
    conn.close()

    return {
        "database": db_path,
        "tables": tables_created,
        "indexes": indexes_created,
    }


if __name__ == "__main__":
    result = init_planning_schema()
    print(f"ERPClaw Planning schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
