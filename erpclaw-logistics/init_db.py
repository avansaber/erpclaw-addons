#!/usr/bin/env python3
"""ERPClaw Logistics schema extension -- adds logistics tables to the shared database.

10 tables: logistics_shipment, logistics_tracking_event, logistics_carrier,
logistics_carrier_rate, logistics_route, logistics_route_stop,
logistics_freight_charge, logistics_carrier_invoice.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys

DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
DISPLAY_NAME = "ERPClaw Logistics"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log", "supplier",
]


def create_logistics_tables(db_path=None):
    db_path = db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")

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
    # 1. logistics_carrier
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logistics_carrier (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            carrier_code    TEXT,
            supplier_id     TEXT REFERENCES supplier(id),
            contact_name    TEXT,
            contact_email   TEXT,
            contact_phone   TEXT,
            dot_number      TEXT,
            mc_number       TEXT,
            carrier_type    TEXT NOT NULL DEFAULT 'parcel'
                            CHECK(carrier_type IN ('ltl','ftl','parcel','freight_forwarder','courier')),
            insurance_expiry TEXT,
            carrier_status  TEXT NOT NULL DEFAULT 'active'
                            CHECK(carrier_status IN ('active','inactive','suspended')),
            on_time_pct     TEXT NOT NULL DEFAULT '100',
            total_shipments INTEGER NOT NULL DEFAULT 0,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_carrier_company ON logistics_carrier(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_carrier_status ON logistics_carrier(carrier_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_carrier_type ON logistics_carrier(carrier_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_carrier_supplier ON logistics_carrier(supplier_id)")
    indexes_created += 4

    # ==================================================================
    # 2. logistics_carrier_rate
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logistics_carrier_rate (
            id              TEXT PRIMARY KEY,
            carrier_id      TEXT NOT NULL REFERENCES logistics_carrier(id) ON DELETE CASCADE,
            origin_zone     TEXT,
            destination_zone TEXT,
            service_level   TEXT NOT NULL DEFAULT 'ground'
                            CHECK(service_level IN ('ground','express','overnight','freight','ltl')),
            weight_min      TEXT,
            weight_max      TEXT,
            rate_per_unit   TEXT,
            flat_rate       TEXT,
            effective_date  TEXT,
            expiry_date     TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_crate_carrier ON logistics_carrier_rate(carrier_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_crate_company ON logistics_carrier_rate(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_crate_service ON logistics_carrier_rate(service_level)")
    indexes_created += 3

    # ==================================================================
    # 3. logistics_shipment
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logistics_shipment (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            origin_address      TEXT,
            origin_city         TEXT,
            origin_state        TEXT,
            origin_zip          TEXT,
            destination_address TEXT,
            destination_city    TEXT,
            destination_state   TEXT,
            destination_zip     TEXT,
            carrier_id          TEXT REFERENCES logistics_carrier(id),
            service_level       TEXT NOT NULL DEFAULT 'ground'
                                CHECK(service_level IN ('ground','express','overnight','freight','ltl')),
            weight              TEXT,
            dimensions          TEXT,
            package_count       INTEGER NOT NULL DEFAULT 1,
            declared_value      TEXT,
            reference_number    TEXT,
            shipment_status     TEXT NOT NULL DEFAULT 'created'
                                CHECK(shipment_status IN ('created','picked_up','in_transit','out_for_delivery','delivered','exception','returned')),
            estimated_delivery  TEXT,
            actual_delivery     TEXT,
            shipping_cost       TEXT,
            tracking_number     TEXT,
            pod_signature       TEXT,
            pod_timestamp       TEXT,
            notes               TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id),
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_ship_company ON logistics_shipment(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_ship_status ON logistics_shipment(shipment_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_ship_carrier ON logistics_shipment(carrier_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_ship_tracking ON logistics_shipment(tracking_number)")
    indexes_created += 4

    # ==================================================================
    # 4. logistics_tracking_event
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logistics_tracking_event (
            id              TEXT PRIMARY KEY,
            shipment_id     TEXT NOT NULL REFERENCES logistics_shipment(id) ON DELETE CASCADE,
            event_timestamp TEXT NOT NULL,
            event_type      TEXT NOT NULL
                            CHECK(event_type IN ('created','picked_up','departed','arrived','out_for_delivery','delivered','exception','returned')),
            location        TEXT,
            description     TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_track_shipment ON logistics_tracking_event(shipment_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_track_type ON logistics_tracking_event(event_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_track_company ON logistics_tracking_event(company_id)")
    indexes_created += 3

    # ==================================================================
    # 5. logistics_route
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logistics_route (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            origin          TEXT,
            destination     TEXT,
            distance        TEXT,
            estimated_hours TEXT,
            route_status    TEXT NOT NULL DEFAULT 'active'
                            CHECK(route_status IN ('active','inactive')),
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_route_company ON logistics_route(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_route_status ON logistics_route(route_status)")
    indexes_created += 2

    # ==================================================================
    # 6. logistics_route_stop
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logistics_route_stop (
            id              TEXT PRIMARY KEY,
            route_id        TEXT NOT NULL REFERENCES logistics_route(id) ON DELETE CASCADE,
            stop_order      INTEGER NOT NULL DEFAULT 1,
            address         TEXT,
            city            TEXT,
            state           TEXT,
            zip_code        TEXT,
            estimated_arrival TEXT,
            stop_type       TEXT NOT NULL DEFAULT 'delivery'
                            CHECK(stop_type IN ('pickup','delivery','transfer')),
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_rstop_route ON logistics_route_stop(route_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_rstop_company ON logistics_route_stop(company_id)")
    indexes_created += 2

    # ==================================================================
    # 7. logistics_freight_charge
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logistics_freight_charge (
            id              TEXT PRIMARY KEY,
            shipment_id     TEXT NOT NULL REFERENCES logistics_shipment(id) ON DELETE CASCADE,
            charge_type     TEXT NOT NULL DEFAULT 'base'
                            CHECK(charge_type IN ('base','fuel_surcharge','accessorial','insurance','handling','customs')),
            description     TEXT,
            amount          TEXT NOT NULL DEFAULT '0',
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_fcharge_shipment ON logistics_freight_charge(shipment_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_fcharge_company ON logistics_freight_charge(company_id)")
    indexes_created += 2

    # ==================================================================
    # 8. logistics_carrier_invoice
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logistics_carrier_invoice (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            carrier_id      TEXT NOT NULL REFERENCES logistics_carrier(id),
            invoice_number  TEXT,
            invoice_date    TEXT,
            total_amount    TEXT NOT NULL DEFAULT '0',
            invoice_status  TEXT NOT NULL DEFAULT 'pending'
                            CHECK(invoice_status IN ('pending','verified','paid','disputed')),
            purchase_invoice_id TEXT,
            shipment_count  INTEGER NOT NULL DEFAULT 0,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_cinv_carrier ON logistics_carrier_invoice(carrier_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_cinv_company ON logistics_carrier_invoice(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_cinv_status ON logistics_carrier_invoice(invoice_status)")
    indexes_created += 3

    conn.commit()
    conn.close()

    return {
        "database": db_path,
        "tables": tables_created,
        "indexes": indexes_created,
    }


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else None
    result = create_logistics_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
