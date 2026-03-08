#!/usr/bin/env python3
"""ERPClaw Integrations schema extension -- adds integration tables to the shared database.

Operator-facing connectors for syncing data with external platforms.
17 tables: 9 core integration tables + 8 connectors-v2 tables
(booking, delivery, realestate, financial, productivity).

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys

DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
DISPLAY_NAME = "ERPClaw Integrations"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log",
]


def create_integration_tables(db_path=None):
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
    # TABLE 1: integration_connector
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS integration_connector (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            platform        TEXT NOT NULL
                            CHECK(platform IN ('shopify','woocommerce','amazon','quickbooks','stripe','square','xero','custom')),
            connector_type  TEXT NOT NULL DEFAULT 'bidirectional'
                            CHECK(connector_type IN ('inbound','outbound','bidirectional')),
            base_url        TEXT,
            connector_status TEXT NOT NULL DEFAULT 'inactive'
                            CHECK(connector_status IN ('active','inactive','error')),
            config_json     TEXT NOT NULL DEFAULT '{}',
            last_sync_at    TEXT,
            company_id      TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_connector_company ON integration_connector(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_connector_platform ON integration_connector(platform)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_connector_status ON integration_connector(connector_status)")
    indexes_created += 3

    # ==================================================================
    # TABLE 2: integration_credential
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS integration_credential (
            id              TEXT PRIMARY KEY,
            connector_id    TEXT NOT NULL REFERENCES integration_connector(id),
            credential_type TEXT NOT NULL
                            CHECK(credential_type IN ('api_key','oauth2','basic_auth','webhook_secret')),
            credential_key  TEXT NOT NULL,
            credential_value TEXT NOT NULL,
            expires_at      TEXT,
            company_id      TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_credential_connector ON integration_credential(connector_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_credential_company ON integration_credential(company_id)")
    indexes_created += 2

    # ==================================================================
    # TABLE 3: integration_webhook
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS integration_webhook (
            id              TEXT PRIMARY KEY,
            connector_id    TEXT NOT NULL REFERENCES integration_connector(id),
            event_type      TEXT NOT NULL,
            webhook_url     TEXT NOT NULL,
            webhook_secret  TEXT,
            is_active       INTEGER NOT NULL DEFAULT 1,
            company_id      TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_webhook_connector ON integration_webhook(connector_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_webhook_company ON integration_webhook(company_id)")
    indexes_created += 2

    # ==================================================================
    # TABLE 4: integration_sync
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS integration_sync (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            connector_id    TEXT NOT NULL REFERENCES integration_connector(id),
            sync_type       TEXT NOT NULL
                            CHECK(sync_type IN ('full','incremental','manual')),
            direction       TEXT NOT NULL
                            CHECK(direction IN ('inbound','outbound','bidirectional')),
            entity_type     TEXT,
            sync_status     TEXT NOT NULL DEFAULT 'pending'
                            CHECK(sync_status IN ('pending','running','completed','failed','cancelled')),
            records_processed INTEGER NOT NULL DEFAULT 0,
            records_failed  INTEGER NOT NULL DEFAULT 0,
            started_at      TEXT,
            completed_at    TEXT,
            error_message   TEXT,
            company_id      TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_sync_connector ON integration_sync(connector_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_sync_status ON integration_sync(sync_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_sync_company ON integration_sync(company_id)")
    indexes_created += 3

    # ==================================================================
    # TABLE 5: integration_sync_schedule
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS integration_sync_schedule (
            id              TEXT PRIMARY KEY,
            connector_id    TEXT NOT NULL REFERENCES integration_connector(id),
            entity_type     TEXT NOT NULL,
            frequency       TEXT NOT NULL
                            CHECK(frequency IN ('hourly','daily','weekly','monthly','manual')),
            sync_type       TEXT NOT NULL DEFAULT 'incremental',
            direction       TEXT NOT NULL DEFAULT 'bidirectional',
            is_active       INTEGER NOT NULL DEFAULT 1,
            last_run_at     TEXT,
            next_run_at     TEXT,
            company_id      TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_schedule_connector ON integration_sync_schedule(connector_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_schedule_company ON integration_sync_schedule(company_id)")
    indexes_created += 2

    # ==================================================================
    # TABLE 6: integration_field_mapping
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS integration_field_mapping (
            id              TEXT PRIMARY KEY,
            connector_id    TEXT NOT NULL REFERENCES integration_connector(id),
            entity_type     TEXT NOT NULL,
            source_field    TEXT NOT NULL,
            target_field    TEXT NOT NULL,
            transform_rule  TEXT,
            is_required     INTEGER NOT NULL DEFAULT 0,
            default_value   TEXT,
            company_id      TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_field_map_connector ON integration_field_mapping(connector_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_field_map_entity ON integration_field_mapping(entity_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_field_map_company ON integration_field_mapping(company_id)")
    indexes_created += 3

    # ==================================================================
    # TABLE 7: integration_entity_map
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS integration_entity_map (
            id              TEXT PRIMARY KEY,
            connector_id    TEXT NOT NULL REFERENCES integration_connector(id),
            entity_type     TEXT NOT NULL,
            local_id        TEXT NOT NULL,
            remote_id       TEXT NOT NULL,
            last_synced_at  TEXT,
            company_id      TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(connector_id, entity_type, local_id)
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_entity_map_connector ON integration_entity_map(connector_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_entity_map_entity ON integration_entity_map(entity_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_entity_map_local ON integration_entity_map(local_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_entity_map_remote ON integration_entity_map(remote_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_entity_map_company ON integration_entity_map(company_id)")
    indexes_created += 5

    # ==================================================================
    # TABLE 8: integration_transform_rule (supplementary)
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS integration_transform_rule (
            id              TEXT PRIMARY KEY,
            connector_id    TEXT NOT NULL REFERENCES integration_connector(id),
            entity_type     TEXT NOT NULL,
            rule_name       TEXT NOT NULL,
            rule_json       TEXT NOT NULL DEFAULT '{}',
            company_id      TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_transform_connector ON integration_transform_rule(connector_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_transform_company ON integration_transform_rule(company_id)")
    indexes_created += 2

    # ==================================================================
    # TABLE 9: integration_sync_error (child of sync)
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS integration_sync_error (
            id              TEXT PRIMARY KEY,
            sync_id         TEXT NOT NULL REFERENCES integration_sync(id),
            entity_type     TEXT,
            entity_id       TEXT,
            error_message   TEXT NOT NULL,
            is_resolved     INTEGER NOT NULL DEFAULT 0,
            resolution_notes TEXT,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            resolved_at     TEXT
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_sync_error_sync ON integration_sync_error(sync_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intg_sync_error_resolved ON integration_sync_error(is_resolved)")
    indexes_created += 2

    # ==================================================================
    # CONNECTORS V2 -- BOOKING DOMAIN
    # ==================================================================

    # TABLE 10: connv2_booking_connector
    conn.execute("""
        CREATE TABLE IF NOT EXISTS connv2_booking_connector (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            platform            TEXT NOT NULL
                                CHECK(platform IN ('booking_com','expedia','airbnb','vrbo')),
            property_id         TEXT,
            api_credentials_ref TEXT,
            sync_reservations   INTEGER NOT NULL DEFAULT 1,
            sync_rates          INTEGER NOT NULL DEFAULT 1,
            sync_availability   INTEGER NOT NULL DEFAULT 1,
            last_sync_at        TEXT,
            connector_status    TEXT NOT NULL DEFAULT 'inactive'
                                CHECK(connector_status IN ('active','inactive','error')),
            company_id          TEXT NOT NULL REFERENCES company(id),
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_bkc_company ON connv2_booking_connector(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_bkc_platform ON connv2_booking_connector(platform)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_bkc_status ON connv2_booking_connector(connector_status)")
    indexes_created += 3

    # TABLE 11: connv2_booking_sync_log
    conn.execute("""
        CREATE TABLE IF NOT EXISTS connv2_booking_sync_log (
            id              TEXT PRIMARY KEY,
            connector_id    TEXT NOT NULL REFERENCES connv2_booking_connector(id),
            sync_type       TEXT NOT NULL
                            CHECK(sync_type IN ('reservations','rates','availability')),
            direction       TEXT NOT NULL
                            CHECK(direction IN ('inbound','outbound')),
            records_synced  INTEGER NOT NULL DEFAULT 0,
            errors          INTEGER NOT NULL DEFAULT 0,
            sync_status     TEXT NOT NULL DEFAULT 'completed'
                            CHECK(sync_status IN ('pending','running','completed','failed')),
            started_at      TEXT,
            completed_at    TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_bsl_connector ON connv2_booking_sync_log(connector_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_bsl_company ON connv2_booking_sync_log(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_bsl_status ON connv2_booking_sync_log(sync_status)")
    indexes_created += 3

    # ==================================================================
    # CONNECTORS V2 -- DELIVERY DOMAIN
    # ==================================================================

    # TABLE 12: connv2_delivery_connector
    conn.execute("""
        CREATE TABLE IF NOT EXISTS connv2_delivery_connector (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            platform            TEXT NOT NULL
                                CHECK(platform IN ('doordash','ubereats','grubhub','postmates')),
            store_id            TEXT,
            api_credentials_ref TEXT,
            auto_accept         INTEGER NOT NULL DEFAULT 0,
            sync_menu           INTEGER NOT NULL DEFAULT 1,
            last_sync_at        TEXT,
            connector_status    TEXT NOT NULL DEFAULT 'inactive'
                                CHECK(connector_status IN ('active','inactive','error')),
            company_id          TEXT NOT NULL REFERENCES company(id),
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_dlc_company ON connv2_delivery_connector(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_dlc_platform ON connv2_delivery_connector(platform)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_dlc_status ON connv2_delivery_connector(connector_status)")
    indexes_created += 3

    # TABLE 13: connv2_delivery_order
    conn.execute("""
        CREATE TABLE IF NOT EXISTS connv2_delivery_order (
            id                  TEXT PRIMARY KEY,
            connector_id        TEXT NOT NULL REFERENCES connv2_delivery_connector(id),
            external_order_id   TEXT,
            order_data          TEXT,
            total_amount        TEXT,
            commission          TEXT,
            net_amount          TEXT,
            order_status        TEXT NOT NULL DEFAULT 'received'
                                CHECK(order_status IN ('received','confirmed','preparing','ready','picked_up','delivered','cancelled')),
            received_at         TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id),
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_dlo_connector ON connv2_delivery_order(connector_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_dlo_company ON connv2_delivery_order(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_dlo_status ON connv2_delivery_order(order_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_dlo_ext_id ON connv2_delivery_order(external_order_id)")
    indexes_created += 4

    # ==================================================================
    # CONNECTORS V2 -- REAL ESTATE DOMAIN
    # ==================================================================

    # TABLE 14: connv2_realestate_connector
    conn.execute("""
        CREATE TABLE IF NOT EXISTS connv2_realestate_connector (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            platform            TEXT NOT NULL
                                CHECK(platform IN ('zillow','realtor_com','mls','trulia')),
            agent_id            TEXT,
            api_credentials_ref TEXT,
            sync_listings       INTEGER NOT NULL DEFAULT 1,
            capture_leads       INTEGER NOT NULL DEFAULT 1,
            last_sync_at        TEXT,
            connector_status    TEXT NOT NULL DEFAULT 'inactive'
                                CHECK(connector_status IN ('active','inactive','error')),
            company_id          TEXT NOT NULL REFERENCES company(id),
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_rec_company ON connv2_realestate_connector(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_rec_platform ON connv2_realestate_connector(platform)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_rec_status ON connv2_realestate_connector(connector_status)")
    indexes_created += 3

    # TABLE 15: connv2_realestate_lead
    conn.execute("""
        CREATE TABLE IF NOT EXISTS connv2_realestate_lead (
            id              TEXT PRIMARY KEY,
            connector_id    TEXT NOT NULL REFERENCES connv2_realestate_connector(id),
            lead_source     TEXT,
            contact_name    TEXT,
            contact_email   TEXT,
            contact_phone   TEXT,
            property_ref    TEXT,
            inquiry         TEXT,
            lead_status     TEXT NOT NULL DEFAULT 'new'
                            CHECK(lead_status IN ('new','contacted','qualified','converted','lost')),
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_rel_connector ON connv2_realestate_lead(connector_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_rel_company ON connv2_realestate_lead(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_rel_status ON connv2_realestate_lead(lead_status)")
    indexes_created += 3

    # ==================================================================
    # CONNECTORS V2 -- FINANCIAL DOMAIN
    # ==================================================================

    # TABLE 16: connv2_financial_connector
    conn.execute("""
        CREATE TABLE IF NOT EXISTS connv2_financial_connector (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            platform            TEXT NOT NULL
                                CHECK(platform IN ('plaid','twilio','sendgrid','mailchimp')),
            account_ref         TEXT,
            api_credentials_ref TEXT,
            sync_enabled        INTEGER NOT NULL DEFAULT 1,
            last_sync_at        TEXT,
            connector_status    TEXT NOT NULL DEFAULT 'inactive'
                                CHECK(connector_status IN ('active','inactive','error')),
            company_id          TEXT NOT NULL REFERENCES company(id),
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_fnc_company ON connv2_financial_connector(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_fnc_platform ON connv2_financial_connector(platform)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_fnc_status ON connv2_financial_connector(connector_status)")
    indexes_created += 3

    # ==================================================================
    # CONNECTORS V2 -- PRODUCTIVITY DOMAIN
    # ==================================================================

    # TABLE 17: connv2_productivity_connector
    conn.execute("""
        CREATE TABLE IF NOT EXISTS connv2_productivity_connector (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            platform            TEXT NOT NULL
                                CHECK(platform IN ('google_workspace','microsoft_365','slack','zoom')),
            workspace_id        TEXT,
            api_credentials_ref TEXT,
            sync_calendar       INTEGER NOT NULL DEFAULT 1,
            sync_contacts       INTEGER NOT NULL DEFAULT 1,
            sync_files          INTEGER NOT NULL DEFAULT 0,
            last_sync_at        TEXT,
            connector_status    TEXT NOT NULL DEFAULT 'inactive'
                                CHECK(connector_status IN ('active','inactive','error')),
            company_id          TEXT NOT NULL REFERENCES company(id),
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_pdc_company ON connv2_productivity_connector(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_pdc_platform ON connv2_productivity_connector(platform)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cv2_pdc_status ON connv2_productivity_connector(connector_status)")
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
    result = create_integration_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
