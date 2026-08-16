"""ERPClaw Ops (advanced manufacturing) -- schema initialization.

Creates 6 tables (shop_floor_entry, tool, tool_usage, engineering_change_order,
process_recipe, recipe_ingredient) in the shared ERPClaw database.

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
# 1. shop_floor_entry
# ---------------------------------------------------------------------------
SHOP_FLOOR_ENTRY = Table(
    "shop_floor_entry", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("equipment_id", Text),
    Column("work_order_id", Text),
    Column("operator", Text),
    Column("entry_type", Text, nullable=False, server_default=text("'production'")),
    Column("start_time", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("end_time", Text),
    Column("duration_minutes", Integer),
    Column("quantity_produced", Integer, server_default=text("0")),
    Column("quantity_rejected", Integer, server_default=text("0")),
    Column("batch_number", Text),
    Column("serial_number", Text),
    Column("machine_status", Text, server_default=text("'running'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "entry_type IN ('production','setup','downtime','quality_check',"
        "'changeover','other')",
        name="ck_shop_floor_entry_type"),
    CheckConstraint(
        "machine_status IN ('running','idle','setup','breakdown','maintenance','off')",
        name="ck_shop_floor_entry_machine_status"),
)

Index("idx_sfe_company", SHOP_FLOOR_ENTRY.c.company_id)
Index("idx_sfe_equipment", SHOP_FLOOR_ENTRY.c.equipment_id)
Index("idx_sfe_work_order", SHOP_FLOOR_ENTRY.c.work_order_id)
Index("idx_sfe_entry_type", SHOP_FLOOR_ENTRY.c.entry_type)
Index("idx_sfe_start_time", SHOP_FLOOR_ENTRY.c.start_time)

# ---------------------------------------------------------------------------
# 2. tool
# ---------------------------------------------------------------------------
TOOL = Table(
    "tool", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("tool_type", Text, nullable=False, server_default=text("'cutting'")),
    Column("tool_code", Text),
    Column("manufacturer", Text),
    Column("model", Text),
    Column("serial_number", Text),
    Column("location", Text),
    Column("purchase_date", Text),
    Column("purchase_cost", Text, server_default=text("'0'")),
    Column("max_usage_count", Integer),
    Column("current_usage_count", Integer, nullable=False, server_default=text("0")),
    Column("calibration_due", Text),
    Column("last_calibration", Text),
    Column("condition", Text, nullable=False, server_default=text("'good'")),
    Column("status", Text, nullable=False, server_default=text("'available'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "tool_type IN ('cutting','measuring','holding','forming','assembly',"
        "'inspection','other')",
        name="ck_tool_type"),
    CheckConstraint(
        "condition IN ('new','good','worn','needs_repair','scrapped')",
        name="ck_tool_condition"),
    CheckConstraint(
        "status IN ('available','in_use','maintenance','calibration','scrapped')",
        name="ck_tool_status"),
)

Index("idx_tool_company", TOOL.c.company_id)
Index("idx_tool_status", TOOL.c.status)
Index("idx_tool_condition", TOOL.c.condition)
Index("idx_tool_calibration_due", TOOL.c.calibration_due)

# ---------------------------------------------------------------------------
# 3. tool_usage
# ---------------------------------------------------------------------------
TOOL_USAGE = Table(
    "tool_usage", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("tool_id", Text,
           ForeignKey("tool.id", ondelete="RESTRICT"), nullable=False),
    Column("work_order_id", Text),
    Column("operator", Text),
    Column("usage_date", Text, nullable=False, server_default=text("CURRENT_DATE")),
    Column("usage_count", Integer, nullable=False, server_default=text("1")),
    Column("usage_duration_minutes", Integer),
    Column("condition_after", Text),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    # NULL is a member of this IN list in the shipped DDL. It is redundant --
    # `x IN (..., NULL)` never evaluates true for a NULL x -- but it is what
    # ships, and a conversion is not the place to correct someone's SQL.
    CheckConstraint(
        "condition_after IN ('good','worn','needs_repair','scrapped',NULL)",
        name="ck_tool_usage_condition_after"),
)

Index("idx_tool_usage_tool", TOOL_USAGE.c.tool_id)
Index("idx_tool_usage_company", TOOL_USAGE.c.company_id)
Index("idx_tool_usage_date", TOOL_USAGE.c.usage_date)

# ---------------------------------------------------------------------------
# 4. engineering_change_order
# ---------------------------------------------------------------------------
ENGINEERING_CHANGE_ORDER = Table(
    "engineering_change_order", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("title", Text, nullable=False),
    Column("eco_type", Text, nullable=False, server_default=text("'design'")),
    Column("description", Text),
    Column("reason", Text),
    Column("affected_items", Text),
    Column("affected_boms", Text),
    Column("impact_analysis", Text),
    Column("requested_by", Text),
    Column("approved_by", Text),
    Column("priority", Text, nullable=False, server_default=text("'medium'")),
    Column("implementation_date", Text),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "eco_type IN ('design','process','material','quality','cost_reduction','other')",
        name="ck_eco_type"),
    CheckConstraint("priority IN ('critical','high','medium','low')",
                    name="ck_eco_priority"),
    CheckConstraint(
        "status IN ('draft','review','approved','in_progress','implemented',"
        "'rejected','cancelled')",
        name="ck_eco_status"),
)

Index("idx_eco_company", ENGINEERING_CHANGE_ORDER.c.company_id)
Index("idx_eco_status", ENGINEERING_CHANGE_ORDER.c.status)
Index("idx_eco_priority", ENGINEERING_CHANGE_ORDER.c.priority)
Index("idx_eco_type", ENGINEERING_CHANGE_ORDER.c.eco_type)

# ---------------------------------------------------------------------------
# 5. process_recipe
# ---------------------------------------------------------------------------
PROCESS_RECIPE = Table(
    "process_recipe", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("product_name", Text, nullable=False),
    Column("recipe_type", Text, nullable=False, server_default=text("'standard'")),
    Column("version", Text, nullable=False, server_default=text("'1.0'")),
    Column("batch_size", Text, nullable=False, server_default=text("'1'")),
    Column("batch_unit", Text, server_default=text("'unit'")),
    Column("expected_yield", Text, server_default=text("'100'")),
    Column("description", Text),
    Column("instructions", Text),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "recipe_type IN ('standard','alternative','trial','obsolete')",
        name="ck_process_recipe_type"),
    CheckConstraint("is_active IN (0,1)", name="ck_process_recipe_is_active"),
)

Index("idx_recipe_company", PROCESS_RECIPE.c.company_id)
Index("idx_recipe_product", PROCESS_RECIPE.c.product_name)
Index("idx_recipe_type", PROCESS_RECIPE.c.recipe_type)
Index("idx_recipe_active", PROCESS_RECIPE.c.is_active)

# ---------------------------------------------------------------------------
# 6. recipe_ingredient
# ---------------------------------------------------------------------------
RECIPE_INGREDIENT = Table(
    "recipe_ingredient", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("recipe_id", Text,
           ForeignKey("process_recipe.id", ondelete="CASCADE"), nullable=False),
    Column("ingredient_name", Text, nullable=False),
    Column("item_id", Text),
    Column("quantity", Text, nullable=False, server_default=text("'0'")),
    Column("unit", Text, server_default=text("'unit'")),
    Column("sequence", Integer, nullable=False, server_default=text("0")),
    Column("is_optional", Integer, nullable=False, server_default=text("0")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("is_optional IN (0,1)",
                    name="ck_recipe_ingredient_is_optional"),
)

Index("idx_ingredient_recipe", RECIPE_INGREDIENT.c.recipe_id)
Index("idx_ingredient_company", RECIPE_INGREDIENT.c.company_id)


def init_advmfg_schema(db_path: str = DB_PATH) -> dict:
    """Create advanced-manufacturing tables on whichever backend is configured.

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
    # The positional db path the shipped installer accepted. The conversion
    # dropped it (Mac merge-QA, 2026-08-13), so `init_db.py /some/where.sqlite`
    # provisioned the default home and said nothing — a silently ignored
    # destination, which is worse than an error.
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    result = init_advmfg_schema(path)
    print(f"ERPClaw Ops schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
