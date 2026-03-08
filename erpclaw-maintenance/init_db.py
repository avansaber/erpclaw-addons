"""ERPClaw Maintenance — schema initialization.

Creates 9 maintenance tables and indexes in the shared ERPClaw database.
Tables: equipment, equipment_reading, maintenance_plan, maintenance_plan_item,
maintenance_work_order, maintenance_work_order_item, maintenance_checklist,
maintenance_checklist_item, downtime_record.
"""
import os
import sqlite3
import sys

DB_PATH = os.environ.get("ERPCLAW_DB_PATH", os.path.expanduser("~/.openclaw/erpclaw/data.sqlite"))


def init_maintenance_schema(db_path: str = DB_PATH) -> dict:
    """Create maintenance tables and indexes."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")

    tables_created = 0
    indexes_created = 0

    # -----------------------------------------------------------------------
    # 1. equipment — tracked equipment / assets
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS equipment (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            name                TEXT NOT NULL,
            equipment_type      TEXT NOT NULL DEFAULT 'machine'
                                CHECK(equipment_type IN ('machine','vehicle','tool','instrument','fixture','other')),
            model               TEXT,
            manufacturer        TEXT,
            serial_number       TEXT,
            location            TEXT,
            parent_equipment_id TEXT REFERENCES equipment(id),
            asset_id            TEXT,
            item_id             TEXT,
            purchase_date       TEXT,
            warranty_expiry     TEXT,
            criticality         TEXT NOT NULL DEFAULT 'medium'
                                CHECK(criticality IN ('critical','high','medium','low')),
            status              TEXT NOT NULL DEFAULT 'operational'
                                CHECK(status IN ('operational','maintenance','breakdown','decommissioned')),
            notes               TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_equipment_company ON equipment(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_equipment_status ON equipment(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_equipment_type ON equipment(equipment_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_equipment_parent ON equipment(parent_equipment_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_equipment_criticality ON equipment(criticality)")
    indexes_created += 5

    # -----------------------------------------------------------------------
    # 2. equipment_reading — meter / sensor readings
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS equipment_reading (
            id              TEXT PRIMARY KEY,
            equipment_id    TEXT NOT NULL REFERENCES equipment(id) ON DELETE RESTRICT,
            reading_type    TEXT NOT NULL DEFAULT 'meter'
                            CHECK(reading_type IN ('meter','temperature','pressure','vibration','other')),
            reading_value   TEXT NOT NULL,
            reading_unit    TEXT,
            reading_date    TEXT NOT NULL DEFAULT (datetime('now')),
            recorded_by     TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_equip_reading_equip ON equipment_reading(equipment_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_equip_reading_company ON equipment_reading(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_equip_reading_date ON equipment_reading(reading_date)")
    indexes_created += 3

    # -----------------------------------------------------------------------
    # 3. maintenance_plan — preventive / predictive schedules
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS maintenance_plan (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            name                TEXT NOT NULL,
            equipment_id        TEXT NOT NULL REFERENCES equipment(id) ON DELETE RESTRICT,
            plan_type           TEXT NOT NULL DEFAULT 'preventive'
                                CHECK(plan_type IN ('preventive','predictive','condition_based')),
            frequency           TEXT NOT NULL DEFAULT 'monthly'
                                CHECK(frequency IN ('daily','weekly','biweekly','monthly','quarterly','semi_annual','annual')),
            frequency_days      INTEGER,
            last_performed      TEXT,
            next_due            TEXT,
            estimated_duration  TEXT,
            estimated_cost      TEXT DEFAULT '0',
            assigned_to         TEXT,
            instructions        TEXT,
            is_active           INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_plan_equipment ON maintenance_plan(equipment_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_plan_company ON maintenance_plan(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_plan_next_due ON maintenance_plan(next_due)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_plan_active ON maintenance_plan(is_active)")
    indexes_created += 4

    # -----------------------------------------------------------------------
    # 4. maintenance_plan_item — spare parts for plans
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS maintenance_plan_item (
            id          TEXT PRIMARY KEY,
            plan_id     TEXT NOT NULL REFERENCES maintenance_plan(id) ON DELETE CASCADE,
            item_id     TEXT,
            item_name   TEXT NOT NULL,
            quantity    TEXT NOT NULL DEFAULT '1',
            notes       TEXT,
            company_id  TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at  TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_plan_item_plan ON maintenance_plan_item(plan_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_plan_item_company ON maintenance_plan_item(company_id)")
    indexes_created += 2

    # -----------------------------------------------------------------------
    # 5. maintenance_work_order — corrective / preventive work orders
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS maintenance_work_order (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            equipment_id        TEXT NOT NULL REFERENCES equipment(id) ON DELETE RESTRICT,
            plan_id             TEXT REFERENCES maintenance_plan(id),
            work_order_type     TEXT NOT NULL DEFAULT 'corrective'
                                CHECK(work_order_type IN ('preventive','corrective','emergency','inspection')),
            priority            TEXT NOT NULL DEFAULT 'medium'
                                CHECK(priority IN ('critical','high','medium','low')),
            description         TEXT,
            assigned_to         TEXT,
            scheduled_date      TEXT,
            started_at          TEXT,
            completed_at        TEXT,
            actual_duration     TEXT,
            actual_cost         TEXT DEFAULT '0',
            failure_mode        TEXT,
            root_cause          TEXT,
            resolution          TEXT,
            status              TEXT NOT NULL DEFAULT 'draft'
                                CHECK(status IN ('draft','scheduled','in_progress','completed','cancelled')),
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_wo_equipment ON maintenance_work_order(equipment_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_wo_plan ON maintenance_work_order(plan_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_wo_company ON maintenance_work_order(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_wo_status ON maintenance_work_order(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_wo_priority ON maintenance_work_order(priority)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_wo_scheduled ON maintenance_work_order(scheduled_date)")
    indexes_created += 6

    # -----------------------------------------------------------------------
    # 6. maintenance_work_order_item — parts used in work orders
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS maintenance_work_order_item (
            id              TEXT PRIMARY KEY,
            work_order_id   TEXT NOT NULL REFERENCES maintenance_work_order(id) ON DELETE CASCADE,
            item_id         TEXT,
            item_name       TEXT NOT NULL,
            quantity        TEXT NOT NULL DEFAULT '1',
            unit_cost       TEXT DEFAULT '0',
            total_cost      TEXT DEFAULT '0',
            notes           TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_wo_item_wo ON maintenance_work_order_item(work_order_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_wo_item_company ON maintenance_work_order_item(company_id)")
    indexes_created += 2

    # -----------------------------------------------------------------------
    # 7. maintenance_checklist — checklists attached to work orders
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS maintenance_checklist (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            work_order_id   TEXT NOT NULL REFERENCES maintenance_work_order(id) ON DELETE CASCADE,
            name            TEXT NOT NULL,
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_checklist_wo ON maintenance_checklist(work_order_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_checklist_company ON maintenance_checklist(company_id)")
    indexes_created += 2

    # -----------------------------------------------------------------------
    # 8. maintenance_checklist_item — individual checklist steps
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS maintenance_checklist_item (
            id              TEXT PRIMARY KEY,
            checklist_id    TEXT NOT NULL REFERENCES maintenance_checklist(id) ON DELETE CASCADE,
            description     TEXT NOT NULL,
            is_completed    INTEGER NOT NULL DEFAULT 0 CHECK(is_completed IN (0,1)),
            completed_at    TEXT,
            completed_by    TEXT,
            notes           TEXT,
            sort_order      INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_maint_cl_item_checklist ON maintenance_checklist_item(checklist_id)")
    indexes_created += 1

    # -----------------------------------------------------------------------
    # 9. downtime_record — equipment downtime tracking
    # -----------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS downtime_record (
            id              TEXT PRIMARY KEY,
            equipment_id    TEXT NOT NULL REFERENCES equipment(id) ON DELETE RESTRICT,
            work_order_id   TEXT REFERENCES maintenance_work_order(id),
            start_time      TEXT NOT NULL DEFAULT (datetime('now')),
            end_time        TEXT,
            duration_hours  TEXT,
            reason          TEXT NOT NULL DEFAULT 'breakdown'
                            CHECK(reason IN ('breakdown','maintenance','setup','changeover','other')),
            description     TEXT,
            impact          TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1

    conn.execute("CREATE INDEX IF NOT EXISTS idx_downtime_equipment ON downtime_record(equipment_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downtime_wo ON downtime_record(work_order_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downtime_company ON downtime_record(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downtime_reason ON downtime_record(reason)")
    indexes_created += 4

    conn.commit()
    conn.close()

    return {
        "database": db_path,
        "tables": tables_created,
        "indexes": indexes_created,
    }


if __name__ == "__main__":
    result = init_maintenance_schema()
    print(f"ERPClaw Maintenance schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
