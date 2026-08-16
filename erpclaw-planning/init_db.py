"""ERPClaw Planning -- schema initialization.

Creates 4 planning tables (planning_scenario, planning_scenario_line, forecast, forecast_line)
in the shared ERPClaw database.

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Conversion rules are the pilot's (`erpclaw-esign`).
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
    CheckConstraint, Column, ForeignKey, Index, MetaData, Table, Text,
    provision, reference_table, text,
)

DB_PATH = os.environ.get("ERPCLAW_DB_PATH", os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite"))

METADATA = MetaData()

# Foundation table this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)

# ---------------------------------------------------------------------------
# 1. planning_scenario -- budget/scenario planning headers
# ---------------------------------------------------------------------------
SCENARIO = Table(
    "planning_scenario", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("scenario_type", Text, nullable=False, server_default=text("'base'")),
    Column("description", Text),
    Column("assumptions", Text),
    # Self-referential: a scenario may be derived from another scenario.
    Column("base_scenario_id", Text, ForeignKey("planning_scenario.id")),
    Column("fiscal_year", Text),
    Column("total_revenue", Text, nullable=False, server_default=text("'0'")),
    Column("total_expense", Text, nullable=False, server_default=text("'0'")),
    Column("net_income", Text, nullable=False, server_default=text("'0'")),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "scenario_type IN ('base','best_case','worst_case','what_if','budget','custom')",
        name="ck_planning_scenario_type"),
    CheckConstraint(
        "status IN ('draft','active','approved','locked','archived')",
        name="ck_planning_scenario_status"),
)

Index("idx_planning_scenario_company", SCENARIO.c.company_id)
Index("idx_planning_scenario_status", SCENARIO.c.status)
Index("idx_planning_scenario_type", SCENARIO.c.scenario_type)
Index("idx_planning_scenario_fiscal_year", SCENARIO.c.fiscal_year)

# ---------------------------------------------------------------------------
# 2. planning_scenario_line -- individual budget/scenario line items
# ---------------------------------------------------------------------------
SCENARIO_LINE = Table(
    "planning_scenario_line", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("scenario_id", Text,
           ForeignKey("planning_scenario.id", ondelete="CASCADE"), nullable=False),
    Column("account_name", Text, nullable=False),
    Column("account_type", Text, nullable=False, server_default=text("'expense'")),
    Column("period", Text, nullable=False),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "account_type IN ('revenue','expense','asset','liability')",
        name="ck_planning_scenario_line_account_type"),
)

Index("idx_planning_scenario_line_scenario", SCENARIO_LINE.c.scenario_id)
Index("idx_planning_scenario_line_company", SCENARIO_LINE.c.company_id)
Index("idx_planning_scenario_line_period", SCENARIO_LINE.c.period)
Index("idx_planning_scenario_line_account", SCENARIO_LINE.c.account_name)

# ---------------------------------------------------------------------------
# 3. forecast -- forecast headers
# ---------------------------------------------------------------------------
FORECAST = Table(
    "forecast", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("forecast_type", Text, nullable=False, server_default=text("'rolling'")),
    Column("period_type", Text, nullable=False, server_default=text("'monthly'")),
    Column("start_period", Text, nullable=False),
    Column("end_period", Text, nullable=False),
    Column("description", Text),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "forecast_type IN ('rolling','static','driver_based','custom')",
        name="ck_forecast_type"),
    CheckConstraint(
        "period_type IN ('weekly','monthly','quarterly','annual')",
        name="ck_forecast_period_type"),
    CheckConstraint(
        "status IN ('draft','active','locked','archived')",
        name="ck_forecast_status"),
)

Index("idx_forecast_company", FORECAST.c.company_id)
Index("idx_forecast_status", FORECAST.c.status)
Index("idx_forecast_type", FORECAST.c.forecast_type)

# ---------------------------------------------------------------------------
# 4. forecast_line -- individual forecast line items
# ---------------------------------------------------------------------------
FORECAST_LINE = Table(
    "forecast_line", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("forecast_id", Text,
           ForeignKey("forecast.id", ondelete="CASCADE"), nullable=False),
    Column("account_name", Text, nullable=False),
    Column("account_type", Text, nullable=False, server_default=text("'revenue'")),
    Column("period", Text, nullable=False),
    Column("forecast_amount", Text, nullable=False, server_default=text("'0'")),
    Column("actual_amount", Text, server_default=text("'0'")),
    Column("variance", Text, server_default=text("'0'")),
    Column("variance_pct", Text, server_default=text("'0'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "account_type IN ('revenue','expense','asset','liability')",
        name="ck_forecast_line_account_type"),
)

Index("idx_forecast_line_forecast", FORECAST_LINE.c.forecast_id)
Index("idx_forecast_line_company", FORECAST_LINE.c.company_id)
Index("idx_forecast_line_period", FORECAST_LINE.c.period)
Index("idx_forecast_line_account", FORECAST_LINE.c.account_name)


def init_planning_schema(db_path: str = DB_PATH) -> dict:
    """Create planning tables and indexes on whichever backend is configured.

    Same contract as before the ADR-0034 conversion: idempotent, and the returned
    counts are what was ACTUALLY created rather than what was declared.
    """
    result = provision(METADATA, db_path)
    return {
        "database": db_path,
        "tables": result["tables"],
        "indexes": result["indexes"],
    }


if __name__ == "__main__":
    result = init_planning_schema()
    print(f"ERPClaw Planning schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
