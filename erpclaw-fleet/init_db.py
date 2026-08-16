#!/usr/bin/env python3
"""ERPClaw Fleet schema extension -- adds fleet management tables to the shared database.

4 tables: fleet_vehicle, fleet_vehicle_assignment, fleet_fuel_log, fleet_vehicle_maintenance.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]

ADR-0034 phase 2 bulk-39. The schema is declared as metadata and provisioned
through `erpclaw_lib.seam`, which emits dialect-correct DDL, instead of a
hand-written ``CREATE TABLE`` block opened with ``sqlite3.connect``. The old
shape could not run on PostgreSQL at all. Conversion rules are the pilot's
(`erpclaw-esign`): seam vocabulary only, money and IDs stay TEXT, and
``primary_key=True, nullable=True`` reproduces SQLite's ``id TEXT PRIMARY KEY``
without adding a NOT NULL that never shipped.
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

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")
DISPLAY_NAME = "ERPClaw Fleet"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log",
]

METADATA = MetaData()

# Foundation tables this module points at but does not own. Declared for foreign
# key resolution only and never created here — see `seam.reference_table`.
reference_table("company", METADATA)

# ---------------------------------------------------------------------------
# 1. fleet_vehicle
# ---------------------------------------------------------------------------
VEHICLE = Table(
    "fleet_vehicle", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("make", Text, nullable=False),
    Column("model", Text, nullable=False),
    Column("year", Integer),
    Column("vin", Text),
    Column("license_plate", Text),
    Column("vehicle_type", Text, server_default=text("'sedan'")),
    Column("color", Text),
    Column("purchase_date", Text),
    Column("purchase_cost", Text),
    Column("current_odometer", Text, server_default=text("'0'")),
    Column("fuel_type", Text, server_default=text("'gasoline'")),
    Column("insurance_provider", Text),
    Column("insurance_policy", Text),
    Column("insurance_expiry", Text),
    Column("vehicle_status", Text, server_default=text("'available'")),
    Column("notes", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "vehicle_type IN ('sedan','suv','truck','van','motorcycle','other')",
        name="ck_fleet_vehicle_type"),
    CheckConstraint(
        "fuel_type IN ('gasoline','diesel','electric','hybrid','other')",
        name="ck_fleet_vehicle_fuel_type"),
    CheckConstraint(
        "vehicle_status IN ('available','assigned','maintenance','retired')",
        name="ck_fleet_vehicle_status"),
)

Index("idx_fleet_vehicle_company", VEHICLE.c.company_id)
Index("idx_fleet_vehicle_status", VEHICLE.c.vehicle_status)
Index("idx_fleet_vehicle_type", VEHICLE.c.vehicle_type)
Index("idx_fleet_vehicle_vin", VEHICLE.c.vin)

# ---------------------------------------------------------------------------
# 2. fleet_vehicle_assignment
# ---------------------------------------------------------------------------
ASSIGNMENT = Table(
    "fleet_vehicle_assignment", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("vehicle_id", Text, ForeignKey("fleet_vehicle.id"), nullable=False),
    Column("driver_name", Text, nullable=False),
    Column("driver_id", Text),
    Column("start_date", Text, nullable=False),
    Column("end_date", Text),
    Column("assignment_status", Text, server_default=text("'active'")),
    Column("notes", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "assignment_status IN ('active','ended')",
        name="ck_fleet_assignment_status"),
)

Index("idx_fleet_assign_vehicle", ASSIGNMENT.c.vehicle_id)
Index("idx_fleet_assign_status", ASSIGNMENT.c.assignment_status)
Index("idx_fleet_assign_company", ASSIGNMENT.c.company_id)
Index("idx_fleet_assign_driver", ASSIGNMENT.c.driver_id)

# ---------------------------------------------------------------------------
# 3. fleet_fuel_log
# ---------------------------------------------------------------------------
FUEL_LOG = Table(
    "fleet_fuel_log", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("vehicle_id", Text, ForeignKey("fleet_vehicle.id"), nullable=False),
    Column("log_date", Text, nullable=False),
    Column("gallons", Text, nullable=False),
    Column("cost", Text, nullable=False),
    Column("odometer_reading", Text),
    Column("fuel_type", Text),
    Column("station", Text),
    Column("notes", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_fleet_fuel_vehicle", FUEL_LOG.c.vehicle_id)
Index("idx_fleet_fuel_date", FUEL_LOG.c.log_date)
Index("idx_fleet_fuel_company", FUEL_LOG.c.company_id)

# ---------------------------------------------------------------------------
# 4. fleet_vehicle_maintenance
# ---------------------------------------------------------------------------
MAINTENANCE = Table(
    "fleet_vehicle_maintenance", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("vehicle_id", Text, ForeignKey("fleet_vehicle.id"), nullable=False),
    Column("maintenance_type", Text, nullable=False),
    Column("scheduled_date", Text),
    Column("completed_date", Text),
    Column("cost", Text),
    Column("vendor", Text),
    Column("odometer_at_service", Text),
    Column("maintenance_status", Text, server_default=text("'scheduled'")),
    Column("notes", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "maintenance_type IN ('oil_change','tire_rotation','brake_service',"
        "'inspection','repair','scheduled','other')",
        name="ck_fleet_maintenance_type"),
    CheckConstraint(
        "maintenance_status IN ('scheduled','in_progress','completed','cancelled')",
        name="ck_fleet_maintenance_status"),
)

Index("idx_fleet_maint_vehicle", MAINTENANCE.c.vehicle_id)
Index("idx_fleet_maint_status", MAINTENANCE.c.maintenance_status)
Index("idx_fleet_maint_type", MAINTENANCE.c.maintenance_type)
Index("idx_fleet_maint_company", MAINTENANCE.c.company_id)



def _require_foundation(db_path):
    """The pre-conversion installer's foundation probe, asked through the seam.

    The original read ``sqlite_master`` directly, so the guard that exists to
    produce a friendly error was itself SQLite-only — on PostgreSQL it would have
    raised before it could explain anything. ``seam.table_exists`` answers on both
    backends (ADR-0034 bulk-39).
    """
    from erpclaw_lib import seam

    missing = [t for t in REQUIRED_FOUNDATION if not seam.table_exists(t, db_path)]
    if missing:
        print(f"ERROR: Foundation tables missing: {', '.join(missing)}")
        print("Run erpclaw-setup first: clawhub install erpclaw-setup")
        sys.exit(1)

def create_fleet_tables(db_path=None):
    """Create fleet tables and indexes on whichever backend is configured.

    Same contract as before the ADR-0034 conversion: idempotent, and the returned
    counts are what was ACTUALLY created rather than what was declared.
    """
    db_path = db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    _require_foundation(db_path)
    result = provision(METADATA, db_path)
    return {
        "database": db_path,
        "tables": result["tables"],
        "indexes": result["indexes"],
    }


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else None
    result = create_fleet_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
