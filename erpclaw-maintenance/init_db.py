"""ERPClaw Maintenance — schema initialization.

Creates 11 maintenance tables and indexes in the shared ERPClaw database.
Tables: equipment, equipment_reading, maintenance_plan, maintenance_plan_item,
maintenance_work_order, maintenance_work_order_item, maintenance_checklist,
maintenance_checklist_item, downtime_record,
maintenance_schedule, maintenance_visit (moved from core init_schema.py).

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Conversion rules are the pilot's (`erpclaw-esign`);
``equipment`` carries a self-referential foreign key, which the metadata
declaration resolves inside its own table.
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

DB_PATH = os.environ.get("ERPCLAW_DB_PATH", os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite"))

METADATA = MetaData()

# Foundation tables this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)

# ---------------------------------------------------------------------------
# 1. equipment — tracked equipment / assets
# ---------------------------------------------------------------------------
EQUIPMENT = Table(
    "equipment", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("equipment_type", Text, nullable=False, server_default=text("'machine'")),
    Column("model", Text),
    Column("manufacturer", Text),
    Column("serial_number", Text),
    Column("location", Text),
    Column("parent_equipment_id", Text, ForeignKey("equipment.id")),
    # asset_id and item_id carry no foreign key in the shipped DDL, unlike
    # company_id below. Preserved as declared.
    Column("asset_id", Text),
    Column("item_id", Text),
    Column("purchase_date", Text),
    Column("warranty_expiry", Text),
    Column("criticality", Text, nullable=False, server_default=text("'medium'")),
    Column("status", Text, nullable=False, server_default=text("'operational'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "equipment_type IN ('machine','vehicle','tool','instrument','fixture','other')",
        name="ck_equipment_equipment_type"),
    CheckConstraint("criticality IN ('critical','high','medium','low')",
                    name="ck_equipment_criticality"),
    CheckConstraint(
        "status IN ('operational','maintenance','breakdown','decommissioned')",
        name="ck_equipment_status"),
)

Index("idx_equipment_company", EQUIPMENT.c.company_id)
Index("idx_equipment_status", EQUIPMENT.c.status)
Index("idx_equipment_type", EQUIPMENT.c.equipment_type)
Index("idx_equipment_parent", EQUIPMENT.c.parent_equipment_id)
Index("idx_equipment_criticality", EQUIPMENT.c.criticality)

# ---------------------------------------------------------------------------
# 2. equipment_reading — meter / sensor readings
# ---------------------------------------------------------------------------
EQUIPMENT_READING = Table(
    "equipment_reading", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("equipment_id", Text,
           ForeignKey("equipment.id", ondelete="RESTRICT"), nullable=False),
    Column("reading_type", Text, nullable=False, server_default=text("'meter'")),
    Column("reading_value", Text, nullable=False),
    Column("reading_unit", Text),
    Column("reading_date", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("recorded_by", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "reading_type IN ('meter','temperature','pressure','vibration','other')",
        name="ck_equipment_reading_reading_type"),
)

Index("idx_equip_reading_equip", EQUIPMENT_READING.c.equipment_id)
Index("idx_equip_reading_company", EQUIPMENT_READING.c.company_id)
Index("idx_equip_reading_date", EQUIPMENT_READING.c.reading_date)

# ---------------------------------------------------------------------------
# 3. maintenance_plan — preventive / predictive schedules
# ---------------------------------------------------------------------------
MAINTENANCE_PLAN = Table(
    "maintenance_plan", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("equipment_id", Text,
           ForeignKey("equipment.id", ondelete="RESTRICT"), nullable=False),
    Column("plan_type", Text, nullable=False, server_default=text("'preventive'")),
    Column("frequency", Text, nullable=False, server_default=text("'monthly'")),
    Column("frequency_days", Integer),
    Column("last_performed", Text),
    Column("next_due", Text),
    Column("estimated_duration", Text),
    Column("estimated_cost", Text, server_default=text("'0'")),
    Column("assigned_to", Text),
    Column("instructions", Text),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "plan_type IN ('preventive','predictive','condition_based')",
        name="ck_maintenance_plan_plan_type"),
    CheckConstraint(
        "frequency IN ('daily','weekly','biweekly','monthly','quarterly',"
        "'semi_annual','annual')",
        name="ck_maintenance_plan_frequency"),
    CheckConstraint("is_active IN (0,1)", name="ck_maintenance_plan_is_active"),
)

Index("idx_maint_plan_equipment", MAINTENANCE_PLAN.c.equipment_id)
Index("idx_maint_plan_company", MAINTENANCE_PLAN.c.company_id)
Index("idx_maint_plan_next_due", MAINTENANCE_PLAN.c.next_due)
Index("idx_maint_plan_active", MAINTENANCE_PLAN.c.is_active)

# ---------------------------------------------------------------------------
# 4. maintenance_plan_item — spare parts for plans
# ---------------------------------------------------------------------------
MAINTENANCE_PLAN_ITEM = Table(
    "maintenance_plan_item", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("plan_id", Text,
           ForeignKey("maintenance_plan.id", ondelete="CASCADE"), nullable=False),
    Column("item_id", Text),
    Column("item_name", Text, nullable=False),
    Column("quantity", Text, nullable=False, server_default=text("'1'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_maint_plan_item_plan", MAINTENANCE_PLAN_ITEM.c.plan_id)
Index("idx_maint_plan_item_company", MAINTENANCE_PLAN_ITEM.c.company_id)

# ---------------------------------------------------------------------------
# 5. maintenance_work_order — corrective / preventive work orders
# ---------------------------------------------------------------------------
MAINTENANCE_WORK_ORDER = Table(
    "maintenance_work_order", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("equipment_id", Text,
           ForeignKey("equipment.id", ondelete="RESTRICT"), nullable=False),
    # plan_id carries no ON DELETE action, unlike equipment_id above.
    Column("plan_id", Text, ForeignKey("maintenance_plan.id")),
    Column("work_order_type", Text, nullable=False,
           server_default=text("'corrective'")),
    Column("priority", Text, nullable=False, server_default=text("'medium'")),
    Column("description", Text),
    Column("assigned_to", Text),
    Column("scheduled_date", Text),
    Column("started_at", Text),
    Column("completed_at", Text),
    Column("actual_duration", Text),
    Column("actual_cost", Text, server_default=text("'0'")),
    Column("failure_mode", Text),
    Column("root_cause", Text),
    Column("resolution", Text),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "work_order_type IN ('preventive','corrective','emergency','inspection')",
        name="ck_maintenance_work_order_work_order_type"),
    CheckConstraint("priority IN ('critical','high','medium','low')",
                    name="ck_maintenance_work_order_priority"),
    CheckConstraint(
        "status IN ('draft','scheduled','in_progress','completed','cancelled')",
        name="ck_maintenance_work_order_status"),
)

Index("idx_maint_wo_equipment", MAINTENANCE_WORK_ORDER.c.equipment_id)
Index("idx_maint_wo_plan", MAINTENANCE_WORK_ORDER.c.plan_id)
Index("idx_maint_wo_company", MAINTENANCE_WORK_ORDER.c.company_id)
Index("idx_maint_wo_status", MAINTENANCE_WORK_ORDER.c.status)
Index("idx_maint_wo_priority", MAINTENANCE_WORK_ORDER.c.priority)
Index("idx_maint_wo_scheduled", MAINTENANCE_WORK_ORDER.c.scheduled_date)

# ---------------------------------------------------------------------------
# 6. maintenance_work_order_item — parts used in work orders
# ---------------------------------------------------------------------------
MAINTENANCE_WORK_ORDER_ITEM = Table(
    "maintenance_work_order_item", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("work_order_id", Text,
           ForeignKey("maintenance_work_order.id", ondelete="CASCADE"),
           nullable=False),
    Column("item_id", Text),
    Column("item_name", Text, nullable=False),
    Column("quantity", Text, nullable=False, server_default=text("'1'")),
    Column("unit_cost", Text, server_default=text("'0'")),
    Column("total_cost", Text, server_default=text("'0'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_maint_wo_item_wo", MAINTENANCE_WORK_ORDER_ITEM.c.work_order_id)
Index("idx_maint_wo_item_company", MAINTENANCE_WORK_ORDER_ITEM.c.company_id)

# ---------------------------------------------------------------------------
# 7. maintenance_checklist — checklists attached to work orders
# ---------------------------------------------------------------------------
MAINTENANCE_CHECKLIST = Table(
    "maintenance_checklist", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("work_order_id", Text,
           ForeignKey("maintenance_work_order.id", ondelete="CASCADE"),
           nullable=False),
    Column("name", Text, nullable=False),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_maint_checklist_wo", MAINTENANCE_CHECKLIST.c.work_order_id)
Index("idx_maint_checklist_company", MAINTENANCE_CHECKLIST.c.company_id)

# ---------------------------------------------------------------------------
# 8. maintenance_checklist_item — individual checklist steps
#    (no company_id in the shipped DDL, unlike every table above)
# ---------------------------------------------------------------------------
MAINTENANCE_CHECKLIST_ITEM = Table(
    "maintenance_checklist_item", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("checklist_id", Text,
           ForeignKey("maintenance_checklist.id", ondelete="CASCADE"),
           nullable=False),
    Column("description", Text, nullable=False),
    Column("is_completed", Integer, nullable=False, server_default=text("0")),
    Column("completed_at", Text),
    Column("completed_by", Text),
    Column("notes", Text),
    Column("sort_order", Integer, nullable=False, server_default=text("0")),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("is_completed IN (0,1)",
                    name="ck_maintenance_checklist_item_is_completed"),
)

Index("idx_maint_cl_item_checklist", MAINTENANCE_CHECKLIST_ITEM.c.checklist_id)

# ---------------------------------------------------------------------------
# 9. downtime_record — equipment downtime tracking
# ---------------------------------------------------------------------------
DOWNTIME_RECORD = Table(
    "downtime_record", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("equipment_id", Text,
           ForeignKey("equipment.id", ondelete="RESTRICT"), nullable=False),
    # work_order_id carries no ON DELETE action, unlike equipment_id above.
    Column("work_order_id", Text, ForeignKey("maintenance_work_order.id")),
    Column("start_time", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("end_time", Text),
    Column("duration_hours", Text),
    Column("reason", Text, nullable=False, server_default=text("'breakdown'")),
    Column("description", Text),
    Column("impact", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "reason IN ('breakdown','maintenance','setup','changeover','other')",
        name="ck_downtime_record_reason"),
)

Index("idx_downtime_equipment", DOWNTIME_RECORD.c.equipment_id)
Index("idx_downtime_wo", DOWNTIME_RECORD.c.work_order_id)
Index("idx_downtime_company", DOWNTIME_RECORD.c.company_id)
Index("idx_downtime_reason", DOWNTIME_RECORD.c.reason)

# ---------------------------------------------------------------------------
# 10. maintenance_schedule — customer-facing service schedules
#     (moved from core erpclaw-support tables in init_schema.py; carries no
#     company_id and no customer/item foreign keys, unlike the tables above)
# ---------------------------------------------------------------------------
MAINTENANCE_SCHEDULE = Table(
    "maintenance_schedule", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("customer_id", Text),
    Column("item_id", Text),
    Column("serial_number_id", Text),
    Column("schedule_frequency", Text, nullable=False,
           server_default=text("'quarterly'")),
    Column("start_date", Text, nullable=False),
    Column("end_date", Text, nullable=False),
    Column("last_completed_date", Text),
    Column("next_due_date", Text),
    Column("status", Text, nullable=False, server_default=text("'active'")),
    Column("assigned_to", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "schedule_frequency IN ('monthly','quarterly','semi_annual','annual')",
        name="ck_maintenance_schedule_schedule_frequency"),
    CheckConstraint("status IN ('active','expired','cancelled')",
                    name="ck_maintenance_schedule_status"),
)

Index("idx_maint_sched_customer", MAINTENANCE_SCHEDULE.c.customer_id)
Index("idx_maint_sched_status", MAINTENANCE_SCHEDULE.c.status)

# ---------------------------------------------------------------------------
# 11. maintenance_visit — on-site service visits
#     (moved from core erpclaw-support tables in init_schema.py)
# ---------------------------------------------------------------------------
MAINTENANCE_VISIT = Table(
    "maintenance_visit", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("maintenance_schedule_id", Text,
           ForeignKey("maintenance_schedule.id", ondelete="RESTRICT"),
           nullable=False),
    Column("customer_id", Text),
    Column("visit_date", Text, nullable=False),
    Column("completed_by", Text),
    Column("observations", Text),
    Column("work_done", Text),
    Column("status", Text, nullable=False, server_default=text("'scheduled'")),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("status IN ('scheduled','completed','cancelled')",
                    name="ck_maintenance_visit_status"),
)

Index("idx_maint_visit_schedule", MAINTENANCE_VISIT.c.maintenance_schedule_id)


def init_maintenance_schema(db_path: str = DB_PATH) -> dict:
    """Create maintenance tables and indexes on whichever backend is configured.

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
    result = init_maintenance_schema()
    print(f"ERPClaw Maintenance schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
