#!/usr/bin/env python3
"""ERPClaw Fleet schema extension -- adds fleet management tables to the shared database.

4 tables: fleet_vehicle, fleet_vehicle_assignment, fleet_fuel_log, fleet_vehicle_maintenance.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys

DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
DISPLAY_NAME = "ERPClaw Fleet"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log",
]


def create_fleet_tables(db_path=None):
    db_path = db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    conn = sqlite3.connect(db_path)
    from erpclaw_lib.db import setup_pragmas
    setup_pragmas(conn)

    # -- Verify ERPClaw foundation --
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    missing = [t for t in REQUIRED_FOUNDATION if t not in tables]
    if missing:
        print(f"ERROR: Foundation tables missing: {', '.join(missing)}")
        print("Run erpclaw-setup first: clawhub install erpclaw-setup")
        conn.close()
        sys.exit(1)

    tables_created = 0
    indexes_created = 0

    # ==================================================================
    # 1. fleet_vehicle
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fleet_vehicle (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            make            TEXT NOT NULL,
            model           TEXT NOT NULL,
            year            INTEGER,
            vin             TEXT,
            license_plate   TEXT,
            vehicle_type    TEXT DEFAULT 'sedan'
                            CHECK(vehicle_type IN ('sedan','suv','truck','van','motorcycle','other')),
            color           TEXT,
            purchase_date   TEXT,
            purchase_cost   TEXT,
            current_odometer TEXT DEFAULT '0',
            fuel_type       TEXT DEFAULT 'gasoline'
                            CHECK(fuel_type IN ('gasoline','diesel','electric','hybrid','other')),
            insurance_provider TEXT,
            insurance_policy TEXT,
            insurance_expiry TEXT,
            vehicle_status  TEXT DEFAULT 'available'
                            CHECK(vehicle_status IN ('available','assigned','maintenance','retired')),
            notes           TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_vehicle_company ON fleet_vehicle(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_vehicle_status ON fleet_vehicle(vehicle_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_vehicle_type ON fleet_vehicle(vehicle_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_vehicle_vin ON fleet_vehicle(vin)")
    indexes_created += 4

    # ==================================================================
    # 2. fleet_vehicle_assignment
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fleet_vehicle_assignment (
            id              TEXT PRIMARY KEY,
            vehicle_id      TEXT NOT NULL REFERENCES fleet_vehicle(id),
            driver_name     TEXT NOT NULL,
            driver_id       TEXT,
            start_date      TEXT NOT NULL,
            end_date        TEXT,
            assignment_status TEXT DEFAULT 'active'
                            CHECK(assignment_status IN ('active','ended')),
            notes           TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_assign_vehicle ON fleet_vehicle_assignment(vehicle_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_assign_status ON fleet_vehicle_assignment(assignment_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_assign_company ON fleet_vehicle_assignment(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_assign_driver ON fleet_vehicle_assignment(driver_id)")
    indexes_created += 4

    # ==================================================================
    # 3. fleet_fuel_log
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fleet_fuel_log (
            id              TEXT PRIMARY KEY,
            vehicle_id      TEXT NOT NULL REFERENCES fleet_vehicle(id),
            log_date        TEXT NOT NULL,
            gallons         TEXT NOT NULL,
            cost            TEXT NOT NULL,
            odometer_reading TEXT,
            fuel_type       TEXT,
            station         TEXT,
            notes           TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_fuel_vehicle ON fleet_fuel_log(vehicle_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_fuel_date ON fleet_fuel_log(log_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_fuel_company ON fleet_fuel_log(company_id)")
    indexes_created += 3

    # ==================================================================
    # 4. fleet_vehicle_maintenance
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fleet_vehicle_maintenance (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            vehicle_id      TEXT NOT NULL REFERENCES fleet_vehicle(id),
            maintenance_type TEXT NOT NULL
                            CHECK(maintenance_type IN ('oil_change','tire_rotation','brake_service','inspection','repair','scheduled','other')),
            scheduled_date  TEXT,
            completed_date  TEXT,
            cost            TEXT,
            vendor          TEXT,
            odometer_at_service TEXT,
            maintenance_status TEXT DEFAULT 'scheduled'
                            CHECK(maintenance_status IN ('scheduled','in_progress','completed','cancelled')),
            notes           TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_maint_vehicle ON fleet_vehicle_maintenance(vehicle_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_maint_status ON fleet_vehicle_maintenance(maintenance_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_maint_type ON fleet_vehicle_maintenance(maintenance_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fleet_maint_company ON fleet_vehicle_maintenance(company_id)")
    indexes_created += 4

    conn.commit()
    conn.close()

    return {
        "database": db_path,
        "tables": tables_created,
        "indexes": indexes_created,
    }


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else None
    result = create_fleet_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
