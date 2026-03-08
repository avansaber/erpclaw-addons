"""ERPClaw Advanced Manufacturing -- schema initialization.

Creates 6 tables for shop floor, tools, ECOs, and process recipes
in the shared ERPClaw database.
Extends erpclaw-manufacturing. Requires company table to exist (erpclaw-setup).
"""
import os
import sqlite3
import sys

DB_PATH = os.environ.get(
    "ERPCLAW_DB_PATH",
    os.path.expanduser("~/.openclaw/erpclaw/data.sqlite"),
)


def init_advmfg_schema(db_path: str = DB_PATH) -> dict:
    """Create advanced manufacturing tables and indexes."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")

    tables_created = 0
    indexes_created = 0

    # -------------------------------------------------------------------
    # 1. shop_floor_entry -- production floor activity tracking
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shop_floor_entry (
            id                  TEXT PRIMARY KEY,
            equipment_id        TEXT,
            work_order_id       TEXT,
            operator            TEXT,
            entry_type          TEXT NOT NULL DEFAULT 'production'
                                CHECK(entry_type IN ('production','setup','downtime','quality_check','changeover','other')),
            start_time          TEXT NOT NULL DEFAULT (datetime('now')),
            end_time            TEXT,
            duration_minutes    INTEGER,
            quantity_produced   INTEGER DEFAULT 0,
            quantity_rejected   INTEGER DEFAULT 0,
            batch_number        TEXT,
            serial_number       TEXT,
            machine_status      TEXT DEFAULT 'running'
                                CHECK(machine_status IN ('running','idle','setup','breakdown','maintenance','off')),
            notes               TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sfe_company ON shop_floor_entry(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sfe_equipment ON shop_floor_entry(equipment_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sfe_work_order ON shop_floor_entry(work_order_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sfe_entry_type ON shop_floor_entry(entry_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sfe_start_time ON shop_floor_entry(start_time)")
    indexes_created += 5

    # -------------------------------------------------------------------
    # 2. tool -- tooling inventory and lifecycle
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tool (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            name                TEXT NOT NULL,
            tool_type           TEXT NOT NULL DEFAULT 'cutting'
                                CHECK(tool_type IN ('cutting','measuring','holding','forming','assembly','inspection','other')),
            tool_code           TEXT,
            manufacturer        TEXT,
            model               TEXT,
            serial_number       TEXT,
            location            TEXT,
            purchase_date       TEXT,
            purchase_cost       TEXT DEFAULT '0',
            max_usage_count     INTEGER,
            current_usage_count INTEGER NOT NULL DEFAULT 0,
            calibration_due     TEXT,
            last_calibration    TEXT,
            condition           TEXT NOT NULL DEFAULT 'good'
                                CHECK(condition IN ('new','good','worn','needs_repair','scrapped')),
            status              TEXT NOT NULL DEFAULT 'available'
                                CHECK(status IN ('available','in_use','maintenance','calibration','scrapped')),
            notes               TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_company ON tool(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_status ON tool(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_condition ON tool(condition)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_calibration_due ON tool(calibration_due)")
    indexes_created += 4

    # -------------------------------------------------------------------
    # 3. tool_usage -- usage records for tools
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tool_usage (
            id                      TEXT PRIMARY KEY,
            tool_id                 TEXT NOT NULL REFERENCES tool(id) ON DELETE RESTRICT,
            work_order_id           TEXT,
            operator                TEXT,
            usage_date              TEXT NOT NULL DEFAULT (date('now')),
            usage_count             INTEGER NOT NULL DEFAULT 1,
            usage_duration_minutes  INTEGER,
            condition_after         TEXT CHECK(condition_after IN ('good','worn','needs_repair','scrapped',NULL)),
            notes                   TEXT,
            company_id              TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at              TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_usage_tool ON tool_usage(tool_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_usage_company ON tool_usage(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tool_usage_date ON tool_usage(usage_date)")
    indexes_created += 3

    # -------------------------------------------------------------------
    # 4. engineering_change_order -- ECOs for design/process changes
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS engineering_change_order (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            title               TEXT NOT NULL,
            eco_type            TEXT NOT NULL DEFAULT 'design'
                                CHECK(eco_type IN ('design','process','material','quality','cost_reduction','other')),
            description         TEXT,
            reason              TEXT,
            affected_items      TEXT,
            affected_boms       TEXT,
            impact_analysis     TEXT,
            requested_by        TEXT,
            approved_by         TEXT,
            priority            TEXT NOT NULL DEFAULT 'medium'
                                CHECK(priority IN ('critical','high','medium','low')),
            implementation_date TEXT,
            status              TEXT NOT NULL DEFAULT 'draft'
                                CHECK(status IN ('draft','review','approved','in_progress','implemented','rejected','cancelled')),
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_eco_company ON engineering_change_order(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_eco_status ON engineering_change_order(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_eco_priority ON engineering_change_order(priority)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_eco_type ON engineering_change_order(eco_type)")
    indexes_created += 4

    # -------------------------------------------------------------------
    # 5. process_recipe -- manufacturing process recipes
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS process_recipe (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            name                TEXT NOT NULL,
            product_name        TEXT NOT NULL,
            recipe_type         TEXT NOT NULL DEFAULT 'standard'
                                CHECK(recipe_type IN ('standard','alternative','trial','obsolete')),
            version             TEXT NOT NULL DEFAULT '1.0',
            batch_size          TEXT NOT NULL DEFAULT '1',
            batch_unit          TEXT DEFAULT 'unit',
            expected_yield      TEXT DEFAULT '100',
            description         TEXT,
            instructions        TEXT,
            is_active           INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_recipe_company ON process_recipe(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_recipe_product ON process_recipe(product_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_recipe_type ON process_recipe(recipe_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_recipe_active ON process_recipe(is_active)")
    indexes_created += 4

    # -------------------------------------------------------------------
    # 6. recipe_ingredient -- child table for process_recipe
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS recipe_ingredient (
            id                  TEXT PRIMARY KEY,
            recipe_id           TEXT NOT NULL REFERENCES process_recipe(id) ON DELETE CASCADE,
            ingredient_name     TEXT NOT NULL,
            item_id             TEXT,
            quantity            TEXT NOT NULL DEFAULT '0',
            unit                TEXT DEFAULT 'unit',
            sequence            INTEGER NOT NULL DEFAULT 0,
            is_optional         INTEGER NOT NULL DEFAULT 0 CHECK(is_optional IN (0,1)),
            notes               TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ingredient_recipe ON recipe_ingredient(recipe_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ingredient_company ON recipe_ingredient(company_id)")
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
    result = init_advmfg_schema(path)
    print(f"ERPClaw Advanced Manufacturing schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
