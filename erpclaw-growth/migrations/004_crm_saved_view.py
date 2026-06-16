"""erpclaw-growth migration 004: Saved views (Wave 1B F4).

Creates the one growth-owned saved-view table:
  - crm_saved_view — a persisted, named view over a single CRM entity. Holds a
                     bounded filter-JSON (operator + column whitelist, validated
                     at SAVE-time, never interpolated into SQL) plus optional
                     sort / group-by / column-order JSON. company_id NOT NULL
                     (multi-company-safe; DECISION #2, Wave 1B plan). is_shared
                     0/1: a shared view is readable by every user in the company;
                     only the owner may update or delete it.

Matches init_db.py create_crmadv_tables() exactly so fresh installs and existing
installs converge. Namespaced 'erpclaw-growth:004' in the shared
erpclaw_schema_migration ledger via the P1 module-migration runner.

F4 touches NO foundation schema (Option A: a growth-side apply-saved-view that
calls foundation list-customers and post-filters in Python; the 4 native list-*
actions gain a --saved-view-id flag in-module). No migration ordering constraint.
Idempotent (CREATE IF NOT EXISTS), dialect-aware.

money: crm_saved_view holds no money columns (filter values are opaque TEXT/JSON).
ADR: planning/decisions/ADR-0023-foundation-fk-columns-for-addon-owned-entities.md
"""
import argparse
import os
import sqlite3

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")

# SQLite + Postgres share the same DDL here (no SQLite-only constructs; lower()-based
# partial-free unique index is valid on both backends).
_DDL = [
    """CREATE TABLE IF NOT EXISTS crm_saved_view (
        id                TEXT PRIMARY KEY,
        name              TEXT NOT NULL,
        entity_type       TEXT NOT NULL
                          CHECK(entity_type IN ('lead','opportunity','customer',
                                                'crm_contact','crm_company','crm_task')),
        owner_user_id     TEXT,
        is_shared         INTEGER NOT NULL DEFAULT 0 CHECK(is_shared IN (0,1)),
        filter_json       TEXT,
        sort_json         TEXT,
        group_by_json     TEXT,
        column_order_json TEXT,
        company_id        TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
        created_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_crm_saved_view_company ON crm_saved_view(company_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_saved_view_entity ON crm_saved_view(entity_type)",
    "CREATE INDEX IF NOT EXISTS idx_crm_saved_view_owner ON crm_saved_view(owner_user_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_saved_view_name "
    "ON crm_saved_view(company_id, owner_user_id, lower(name))",
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
    for stmt in _DDL:
        conn.execute(stmt)
    conn.commit()
    conn.close()
    print("  crm_saved_view ensured.")


def _run_postgres(url):
    import psycopg2
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            for stmt in _DDL:
                cur.execute(stmt)
        conn.commit()
        print("  Postgres: crm_saved_view ensured.")
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
    parser = argparse.ArgumentParser(description="erpclaw-growth migration 004: saved views")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("erpclaw-growth migration 004 complete.")
