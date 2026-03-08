#!/usr/bin/env python3
"""ERPClaw Alerts schema extension -- adds alert/notification tables to the shared database.

Configurable notification triggers: low stock, overdue invoices, expiring contracts, custom rules.
3 tables: alert_rule, alert_log, notification_channel.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys

DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
DISPLAY_NAME = "ERPClaw Alerts"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log",
]


def create_alerts_tables(db_path=None):
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
    # TABLE 1: alert_rule
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_rule (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            name                TEXT NOT NULL,
            description         TEXT,
            entity_type         TEXT NOT NULL,
            condition_json      TEXT NOT NULL DEFAULT '{}',
            severity            TEXT DEFAULT 'medium'
                                CHECK(severity IN ('low','medium','high','critical')),
            channel_ids         TEXT,
            cooldown_minutes    INTEGER DEFAULT 60,
            is_active           INTEGER DEFAULT 1,
            last_triggered_at   TEXT,
            trigger_count       INTEGER DEFAULT 0,
            company_id          TEXT NOT NULL,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_rule_company ON alert_rule(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_rule_entity ON alert_rule(entity_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_rule_severity ON alert_rule(severity)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_rule_active ON alert_rule(is_active)")
    indexes_created += 4

    # ==================================================================
    # TABLE 2: alert_log
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_log (
            id                  TEXT PRIMARY KEY,
            rule_id             TEXT REFERENCES alert_rule(id),
            rule_name           TEXT,
            entity_type         TEXT,
            entity_id           TEXT,
            severity            TEXT,
            message             TEXT NOT NULL,
            alert_status        TEXT DEFAULT 'triggered'
                                CHECK(alert_status IN ('triggered','acknowledged','resolved','expired')),
            acknowledged_by     TEXT,
            acknowledged_at     TEXT,
            resolved_at         TEXT,
            channel_results     TEXT,
            company_id          TEXT NOT NULL,
            created_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_log_rule ON alert_log(rule_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_log_company ON alert_log(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_log_severity ON alert_log(severity)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_log_status ON alert_log(alert_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_log_entity ON alert_log(entity_type, entity_id)")
    indexes_created += 5

    # ==================================================================
    # TABLE 3: notification_channel
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS notification_channel (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            name                TEXT NOT NULL,
            channel_type        TEXT NOT NULL
                                CHECK(channel_type IN ('email','webhook','telegram','sms')),
            config_json         TEXT NOT NULL DEFAULT '{}',
            is_active           INTEGER DEFAULT 1,
            company_id          TEXT NOT NULL,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_notif_channel_company ON notification_channel(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_notif_channel_type ON notification_channel(channel_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_notif_channel_active ON notification_channel(is_active)")
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
    result = create_alerts_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
