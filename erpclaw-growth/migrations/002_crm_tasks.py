"""erpclaw-growth migration 002: Tasks — first-class entity (Wave 1B F2).

Creates the two growth-owned task tables:
  - crm_task      — first-class task (status / priority / due_date lifecycle)
  - crm_task_link — many-to-many tie from a task to any CRM entity
                    (lead / opportunity / customer / crm_contact / crm_company)

Matches init_db.py create_crmadv_tables() exactly so fresh installs and existing
installs converge. Namespaced 'erpclaw-growth:002' in the shared
erpclaw_schema_migration ledger via the P1 module-migration runner.

crm_activity is NOT replaced — legacy activity_type='task' rows stay valid.
The crm_task_link CHECK already covers all 5 entity types because growth:001
(crm_contact / crm_company) ran first; no later CHECK widening is needed.
Idempotent (CREATE IF NOT EXISTS), dialect-aware.

money: no money columns in either table (TEXT/Decimal rule n/a here).
ADR: planning/decisions/ADR-0023-foundation-fk-columns-for-addon-owned-entities.md
"""
import argparse
import os
import sqlite3

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")

# SQLite DDL.
_DDL_SQLITE = [
    """CREATE TABLE IF NOT EXISTS crm_task (
        id                  TEXT PRIMARY KEY,
        subject             TEXT NOT NULL,
        description         TEXT,
        status              TEXT NOT NULL DEFAULT 'open'
                            CHECK(status IN ('open','in_progress','done','cancelled')),
        priority            TEXT NOT NULL DEFAULT 'medium'
                            CHECK(priority IN ('low','medium','high','urgent')),
        due_date            TEXT,
        assigned_to_user_id TEXT,
        created_by_user_id  TEXT,
        completed_at        TEXT,
        cancel_reason       TEXT,
        linked_count        INTEGER NOT NULL DEFAULT 0,
        company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
        created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_crm_task_company ON crm_task(company_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_task_status ON crm_task(status)",
    "CREATE INDEX IF NOT EXISTS idx_crm_task_assigned ON crm_task(assigned_to_user_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_task_due ON crm_task(due_date)",
    """CREATE TABLE IF NOT EXISTS crm_task_link (
        id                 TEXT PRIMARY KEY,
        crm_task_id        TEXT NOT NULL REFERENCES crm_task(id) ON DELETE CASCADE,
        linked_entity_type TEXT NOT NULL
                           CHECK(linked_entity_type IN ('lead','opportunity','customer','crm_contact','crm_company')),
        linked_entity_id   TEXT NOT NULL,
        company_id         TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
        created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_crm_task_link_task ON crm_task_link(crm_task_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_task_link_entity ON crm_task_link(linked_entity_type, linked_entity_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_task_link ON crm_task_link(crm_task_id, linked_entity_type, linked_entity_id)",
]

# Postgres DDL: same tables; identical CHECK + index forms are valid in PG.
_DDL_POSTGRES = [
    """CREATE TABLE IF NOT EXISTS crm_task (
        id                  TEXT PRIMARY KEY,
        subject             TEXT NOT NULL,
        description         TEXT,
        status              TEXT NOT NULL DEFAULT 'open'
                            CHECK(status IN ('open','in_progress','done','cancelled')),
        priority            TEXT NOT NULL DEFAULT 'medium'
                            CHECK(priority IN ('low','medium','high','urgent')),
        due_date            TEXT,
        assigned_to_user_id TEXT,
        created_by_user_id  TEXT,
        completed_at        TEXT,
        cancel_reason       TEXT,
        linked_count        INTEGER NOT NULL DEFAULT 0,
        company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
        created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_crm_task_company ON crm_task(company_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_task_status ON crm_task(status)",
    "CREATE INDEX IF NOT EXISTS idx_crm_task_assigned ON crm_task(assigned_to_user_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_task_due ON crm_task(due_date)",
    """CREATE TABLE IF NOT EXISTS crm_task_link (
        id                 TEXT PRIMARY KEY,
        crm_task_id        TEXT NOT NULL REFERENCES crm_task(id) ON DELETE CASCADE,
        linked_entity_type TEXT NOT NULL
                           CHECK(linked_entity_type IN ('lead','opportunity','customer','crm_contact','crm_company')),
        linked_entity_id   TEXT NOT NULL,
        company_id         TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
        created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_crm_task_link_task ON crm_task_link(crm_task_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_task_link_entity ON crm_task_link(linked_entity_type, linked_entity_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_task_link ON crm_task_link(crm_task_id, linked_entity_type, linked_entity_id)",
]


def _get_dialect():
    return os.environ.get("ERPCLAW_DB_DIALECT", "sqlite")


def _run_sqlite(path):
    conn = sqlite3.connect(path)
    try:
        from erpclaw_lib.db import setup_pragmas
        setup_pragmas(conn)
    except ImportError:
        conn.execute("PRAGMA busy_timeout=5000")
    for stmt in _DDL_SQLITE:
        conn.execute(stmt)
    conn.commit()
    conn.close()
    print("  crm_task / crm_task_link ensured.")


def _run_postgres(url):
    import psycopg2
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            for stmt in _DDL_POSTGRES:
                cur.execute(stmt)
        conn.commit()
        print("  Postgres: crm_task / crm_task_link ensured.")
    finally:
        conn.close()


def run_migration(db_path=None):
    if _get_dialect() == "postgresql":
        url = os.environ.get("ERPCLAW_DB_URL") or db_path
        if not url:
            print("Postgres dialect set but no connection URL (ERPCLAW_DB_URL). Nothing to migrate.")
            return
        _run_postgres(url)
        return
    path = db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    if not os.path.exists(path):
        print(f"Database not found at {path}. Nothing to migrate.")
        return
    _run_sqlite(path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="erpclaw-growth migration 002: tasks (first-class)")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("erpclaw-growth migration 002 complete.")
