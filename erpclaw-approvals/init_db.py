#!/usr/bin/env python3
"""ERPClaw Approvals schema extension -- adds approval workflow tables to the shared database.

3 tables: approval_rule, approval_step, approval_request.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys

DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
DISPLAY_NAME = "ERPClaw Approvals"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log",
]


def create_approvals_tables(db_path=None):
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
    # 1. approval_rule
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS approval_rule (
            id              TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            entity_type     TEXT,
            conditions      TEXT,
            is_active       INTEGER NOT NULL DEFAULT 1,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_appr_rule_company ON approval_rule(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_appr_rule_entity ON approval_rule(entity_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_appr_rule_active ON approval_rule(is_active)")
    indexes_created += 3

    # ==================================================================
    # 2. approval_step
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS approval_step (
            id              TEXT PRIMARY KEY,
            rule_id         TEXT NOT NULL REFERENCES approval_rule(id) ON DELETE CASCADE,
            step_order      INTEGER NOT NULL DEFAULT 1,
            approver        TEXT NOT NULL,
            approval_type   TEXT NOT NULL DEFAULT 'sequential'
                            CHECK(approval_type IN ('sequential','parallel','conditional')),
            is_required     INTEGER NOT NULL DEFAULT 1,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_appr_step_rule ON approval_step(rule_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_appr_step_order ON approval_step(step_order)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_appr_step_company ON approval_step(company_id)")
    indexes_created += 3

    # ==================================================================
    # 3. approval_request
    # ==================================================================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS approval_request (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            rule_id         TEXT NOT NULL REFERENCES approval_rule(id),
            entity_type     TEXT,
            entity_id       TEXT,
            requested_by    TEXT,
            current_step    INTEGER NOT NULL DEFAULT 1,
            request_status  TEXT NOT NULL DEFAULT 'pending'
                            CHECK(request_status IN ('pending','in_progress','approved','rejected','cancelled')),
            notes           TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_appr_req_rule ON approval_request(rule_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_appr_req_entity ON approval_request(entity_type, entity_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_appr_req_status ON approval_request(request_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_appr_req_company ON approval_request(company_id)")
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
    result = create_approvals_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
