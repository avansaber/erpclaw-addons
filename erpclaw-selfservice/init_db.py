#!/usr/bin/env python3
"""ERPClaw Self-Service schema extension -- adds self-service permission tables.

5 tables across 4 domains: permissions, portal, sessions, reports.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys

DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
DISPLAY_NAME = "ERPClaw Self-Service"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log",
]


def create_selfservice_tables(db_path=None):
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
    # PERMISSIONS DOMAIN
    # ==================================================================

    # 1. selfservice_permission_profile -- permission profile templates
    conn.execute("""
        CREATE TABLE IF NOT EXISTS selfservice_permission_profile (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            description     TEXT,
            target_role     TEXT NOT NULL DEFAULT 'employee'
                            CHECK(target_role IN ('employee','client','tenant','patient','vendor','other')),
            allowed_actions TEXT DEFAULT '[]',
            denied_actions  TEXT DEFAULT '[]',
            record_scope    TEXT DEFAULT 'own'
                            CHECK(record_scope IN ('own','department','company')),
            field_visibility TEXT DEFAULT '{}',
            is_active       INTEGER NOT NULL DEFAULT 1,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_profile_company ON selfservice_permission_profile(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_profile_role ON selfservice_permission_profile(target_role)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_profile_active ON selfservice_permission_profile(is_active)")
    indexes_created += 3

    # 2. selfservice_profile_assignment -- per-user profile assignments
    conn.execute("""
        CREATE TABLE IF NOT EXISTS selfservice_profile_assignment (
            id              TEXT PRIMARY KEY,
            profile_id      TEXT NOT NULL REFERENCES selfservice_permission_profile(id),
            user_id         TEXT NOT NULL,
            user_email      TEXT,
            user_name       TEXT,
            assigned_by     TEXT,
            assignment_status TEXT NOT NULL DEFAULT 'active'
                            CHECK(assignment_status IN ('active','revoked')),
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_assign_profile ON selfservice_profile_assignment(profile_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_assign_user ON selfservice_profile_assignment(user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_assign_company ON selfservice_profile_assignment(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_assign_status ON selfservice_profile_assignment(assignment_status)")
    indexes_created += 4

    # ==================================================================
    # PORTAL DOMAIN
    # ==================================================================

    # 3. selfservice_portal_config
    conn.execute("""
        CREATE TABLE IF NOT EXISTS selfservice_portal_config (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            branding_json   TEXT DEFAULT '{}',
            welcome_message TEXT,
            enabled_modules TEXT DEFAULT '[]',
            enabled_actions TEXT DEFAULT '[]',
            require_mfa     INTEGER NOT NULL DEFAULT 0,
            session_timeout_minutes INTEGER NOT NULL DEFAULT 60,
            is_active       INTEGER NOT NULL DEFAULT 1,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_portal_company ON selfservice_portal_config(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_portal_active ON selfservice_portal_config(is_active)")
    indexes_created += 2

    # ==================================================================
    # SESSIONS DOMAIN
    # ==================================================================

    # 4. selfservice_session
    conn.execute("""
        CREATE TABLE IF NOT EXISTS selfservice_session (
            id              TEXT PRIMARY KEY,
            user_id         TEXT NOT NULL,
            profile_id      TEXT NOT NULL REFERENCES selfservice_permission_profile(id),
            portal_id       TEXT REFERENCES selfservice_portal_config(id),
            token           TEXT NOT NULL,
            ip_address      TEXT,
            user_agent      TEXT,
            session_status  TEXT NOT NULL DEFAULT 'active'
                            CHECK(session_status IN ('active','expired','ended')),
            expires_at      TEXT NOT NULL,
            last_activity_at TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_session_user ON selfservice_session(user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_session_token ON selfservice_session(token)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_session_company ON selfservice_session(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_session_status ON selfservice_session(session_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_session_profile ON selfservice_session(profile_id)")
    indexes_created += 5

    # ==================================================================
    # REPORTS / ACTIVITY DOMAIN
    # ==================================================================

    # 5. selfservice_activity_log
    conn.execute("""
        CREATE TABLE IF NOT EXISTS selfservice_activity_log (
            id              TEXT PRIMARY KEY,
            session_id      TEXT REFERENCES selfservice_session(id),
            user_id         TEXT NOT NULL,
            action          TEXT NOT NULL,
            entity_type     TEXT,
            entity_id       TEXT,
            result          TEXT NOT NULL DEFAULT 'allowed'
                            CHECK(result IN ('allowed','denied','error')),
            ip_address      TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_actlog_user ON selfservice_activity_log(user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_actlog_company ON selfservice_activity_log(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_actlog_session ON selfservice_activity_log(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_actlog_result ON selfservice_activity_log(result)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_actlog_action ON selfservice_activity_log(action)")
    indexes_created += 5

    conn.commit()
    conn.close()
    print(f"{DISPLAY_NAME}: {tables_created} tables, {indexes_created} indexes ensured.")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else None
    create_selfservice_tables(path)
