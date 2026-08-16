"""ERPClaw Treasury -- schema initialization.

Creates 7 tables for bank accounts, cash management, investments,
inter-company transfers, and cash flow forecasting in the shared ERPClaw database.
Includes cash_flow_forecast (moved from core init_schema.py).
Requires company table to exist (erpclaw-setup).

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Conversion rules are the pilot's (`erpclaw-esign`); every
balance, forecast and transfer amount stays TEXT, and ``Integer`` appears only
for the horizon-day count and the 0/1 active flag.
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
    provision, reference_table, text,
)

DB_PATH = os.environ.get(
    "ERPCLAW_DB_PATH",
    os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite"),
)

METADATA = MetaData()

# Foundation table this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)

# ---------------------------------------------------------------------------
# 1. bank_account_extended -- bank accounts for treasury management
# ---------------------------------------------------------------------------
BANK_ACCOUNT_EXTENDED = Table(
    "bank_account_extended", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("bank_name", Text, nullable=False),
    Column("account_name", Text, nullable=False),
    Column("account_number", Text),
    Column("routing_number", Text),
    Column("account_type", Text, nullable=False, server_default=text("'checking'")),
    Column("currency", Text, nullable=False, server_default=text("'USD'")),
    Column("current_balance", Text, nullable=False, server_default=text("'0'")),
    Column("last_reconciled_date", Text),
    # gl_account_id is a bare TEXT here — no foreign key to account — where the
    # sibling column on `investment` is spelled the same way. Both are carried
    # over as they shipped.
    Column("gl_account_id", Text),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "account_type IN ('checking','savings','money_market','cd',"
        "'line_of_credit','other')",
        name="ck_bank_account_extended_account_type"),
    CheckConstraint("is_active IN (0,1)",
                    name="ck_bank_account_extended_is_active"),
)

Index("idx_bank_account_ext_company", BANK_ACCOUNT_EXTENDED.c.company_id)
Index("idx_bank_account_ext_type", BANK_ACCOUNT_EXTENDED.c.account_type)
Index("idx_bank_account_ext_active", BANK_ACCOUNT_EXTENDED.c.is_active)

# ---------------------------------------------------------------------------
# 2. cash_position -- point-in-time cash snapshots
# ---------------------------------------------------------------------------
CASH_POSITION = Table(
    "cash_position", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("position_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("total_cash", Text, nullable=False, server_default=text("'0'")),
    Column("total_receivables", Text, nullable=False, server_default=text("'0'")),
    Column("total_payables", Text, nullable=False, server_default=text("'0'")),
    Column("net_position", Text, nullable=False, server_default=text("'0'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_cash_position_company", CASH_POSITION.c.company_id)
Index("idx_cash_position_date", CASH_POSITION.c.position_date)

# ---------------------------------------------------------------------------
# 3. cash_forecast -- projected cash flow forecasts
# ---------------------------------------------------------------------------
CASH_FORECAST = Table(
    "cash_forecast", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("forecast_name", Text, nullable=False),
    Column("forecast_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("period_start", Text, nullable=False),
    Column("period_end", Text, nullable=False),
    Column("expected_inflows", Text, nullable=False, server_default=text("'0'")),
    Column("expected_outflows", Text, nullable=False, server_default=text("'0'")),
    Column("net_forecast", Text, nullable=False, server_default=text("'0'")),
    Column("assumptions", Text),
    Column("forecast_type", Text, nullable=False,
           server_default=text("'short_term'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "forecast_type IN ('short_term','medium_term','long_term')",
        name="ck_cash_forecast_forecast_type"),
)

Index("idx_cash_forecast_company", CASH_FORECAST.c.company_id)
Index("idx_cash_forecast_type", CASH_FORECAST.c.forecast_type)
Index("idx_cash_forecast_date", CASH_FORECAST.c.forecast_date)

# ---------------------------------------------------------------------------
# 4. investment -- investment instruments
# ---------------------------------------------------------------------------
INVESTMENT = Table(
    "investment", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("investment_type", Text, nullable=False, server_default=text("'cd'")),
    Column("institution", Text),
    Column("account_number", Text),
    Column("principal", Text, nullable=False, server_default=text("'0'")),
    Column("current_value", Text, nullable=False, server_default=text("'0'")),
    # interest_rate is the one money-shaped column here without NOT NULL; the
    # default is still '0'. Carried over as it shipped.
    Column("interest_rate", Text, server_default=text("'0'")),
    Column("purchase_date", Text),
    Column("maturity_date", Text),
    Column("gl_account_id", Text),
    Column("status", Text, nullable=False, server_default=text("'active'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "investment_type IN ('cd','money_market','treasury_bill','bond',"
        "'mutual_fund','other')",
        name="ck_investment_investment_type"),
    CheckConstraint("status IN ('active','matured','redeemed','cancelled')",
                    name="ck_investment_status"),
)

Index("idx_investment_company", INVESTMENT.c.company_id)
Index("idx_investment_type", INVESTMENT.c.investment_type)
Index("idx_investment_status", INVESTMENT.c.status)
Index("idx_investment_maturity", INVESTMENT.c.maturity_date)

# ---------------------------------------------------------------------------
# 5. investment_transaction -- transactions against investments
# ---------------------------------------------------------------------------
INVESTMENT_TRANSACTION = Table(
    "investment_transaction", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("investment_id", Text,
           ForeignKey("investment.id", ondelete="RESTRICT"), nullable=False),
    Column("transaction_type", Text, nullable=False,
           server_default=text("'purchase'")),
    Column("transaction_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("reference", Text),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "transaction_type IN ('purchase','interest','dividend','redemption',"
        "'fee','transfer')",
        name="ck_investment_transaction_transaction_type"),
)

Index("idx_inv_txn_investment", INVESTMENT_TRANSACTION.c.investment_id)
Index("idx_inv_txn_company", INVESTMENT_TRANSACTION.c.company_id)
Index("idx_inv_txn_date", INVESTMENT_TRANSACTION.c.transaction_date)

# ---------------------------------------------------------------------------
# 6. inter_company_transfer -- fund transfers between companies
# ---------------------------------------------------------------------------
INTER_COMPANY_TRANSFER = Table(
    "inter_company_transfer", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("from_company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("to_company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("transfer_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("reference", Text),
    Column("reason", Text),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    # The owning company is a third, separate reference to `company` alongside
    # the two transfer endpoints.
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("status IN ('draft','approved','completed','cancelled')",
                    name="ck_inter_company_transfer_status"),
)

Index("idx_ict_company", INTER_COMPANY_TRANSFER.c.company_id)
Index("idx_ict_from", INTER_COMPANY_TRANSFER.c.from_company_id)
Index("idx_ict_to", INTER_COMPANY_TRANSFER.c.to_company_id)
Index("idx_ict_status", INTER_COMPANY_TRANSFER.c.status)

# ---------------------------------------------------------------------------
# 7. cash_flow_forecast -- AI-generated cash flow projections
#    (moved from core erpclaw-ai-engine tables in init_schema.py)
#    Alone among these tables it carries no company_id and no foreign key at
#    all, which is how it arrived from core.
# ---------------------------------------------------------------------------
CASH_FLOW_FORECAST = Table(
    "cash_flow_forecast", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("forecast_date", Text, nullable=False),
    Column("generated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("horizon_days", Integer, nullable=False, server_default=text("30")),
    Column("starting_balance", Text, nullable=False, server_default=text("'0'")),
    Column("projected_inflows", Text),
    Column("projected_outflows", Text),
    Column("projected_balance", Text, nullable=False, server_default=text("'0'")),
    Column("confidence_interval", Text),
    Column("assumptions", Text),
    Column("scenario", Text, nullable=False, server_default=text("'expected'")),
    Column("expires_at", Text),
    CheckConstraint("scenario IN ('pessimistic','expected','optimistic')",
                    name="ck_cash_flow_forecast_scenario"),
)

Index("idx_forecast_date", CASH_FLOW_FORECAST.c.forecast_date)
Index("idx_forecast_scenario", CASH_FLOW_FORECAST.c.scenario)


def init_treasury_schema(db_path: str = DB_PATH) -> dict:
    """Create treasury module tables and indexes on whichever backend is configured.

    Same contract as before the ADR-0034 conversion: idempotent (a re-run creates
    nothing), and the returned counts are what was ACTUALLY created rather than
    what was declared.
    """
    result = provision(METADATA, db_path)
    return {
        "database": db_path,
        "tables": result["tables"],
        "indexes": result["indexes"],
    }


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    result = init_treasury_schema(path)
    print(f"ERPClaw Treasury schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
