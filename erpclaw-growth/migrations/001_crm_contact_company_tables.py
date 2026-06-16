"""erpclaw-growth migration 001: Contact + Company model (Wave 1B F1).

Creates the three growth-owned CRM-depth tables:
  - crm_company  — Org entity (domain UNIQUE where not NULL, case-insensitive)
  - crm_contact  — Person entity (email UNIQUE where not NULL, case-insensitive)
  - crm_contact_role — many-to-many person <-> company association

Matches init_db.py create_crmadv_tables() exactly so fresh installs and existing
installs converge. Namespaced 'erpclaw-growth:001' in the shared
erpclaw_schema_migration ledger via the P1 module-migration runner.

This migration MUST run before foundation migration 023 (023's FK targets —
crm_contact / crm_company — must exist first). Idempotent (CREATE IF NOT EXISTS),
dialect-aware.

money: crm_company.annual_revenue is TEXT (Python Decimal), never float.
ADR: planning/decisions/ADR-0023-foundation-fk-columns-for-addon-owned-entities.md
"""
import argparse
import os
import sqlite3

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")

# SQLite DDL (partial-unique-index form for the case-insensitive UNIQUE constraints).
_DDL_SQLITE = [
    """CREATE TABLE IF NOT EXISTS crm_company (
        id                 TEXT PRIMARY KEY,
        name               TEXT NOT NULL,
        domain             TEXT,
        industry           TEXT,
        employee_count     INTEGER,
        annual_revenue     TEXT,
        address_line1      TEXT,
        address_line2      TEXT,
        city               TEXT,
        state              TEXT,
        postal_code        TEXT,
        country            TEXT,
        linkedin_url       TEXT,
        lifecycle          TEXT NOT NULL DEFAULT 'prospect'
                           CHECK(lifecycle IN ('prospect','customer','partner','vendor','other')),
        linked_customer_id TEXT REFERENCES customer(id) ON DELETE SET NULL,
        assigned_to_user_id TEXT,
        notes              TEXT,
        company_id         TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
        created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_crm_company_company ON crm_company(company_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_company_lifecycle ON crm_company(lifecycle)",
    "CREATE INDEX IF NOT EXISTS idx_crm_company_linked_customer ON crm_company(linked_customer_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_company_domain ON crm_company(company_id, lower(domain)) WHERE domain IS NOT NULL",
    """CREATE TABLE IF NOT EXISTS crm_contact (
        id                 TEXT PRIMARY KEY,
        name               TEXT NOT NULL,
        email              TEXT,
        phone              TEXT,
        mobile             TEXT,
        job_title          TEXT,
        linkedin_url       TEXT,
        address_line1      TEXT,
        address_line2      TEXT,
        city               TEXT,
        state              TEXT,
        postal_code        TEXT,
        country            TEXT,
        lifecycle          TEXT NOT NULL DEFAULT 'lead'
                           CHECK(lifecycle IN ('lead','mql','sql','customer','other')),
        crm_company_id     TEXT REFERENCES crm_company(id) ON DELETE SET NULL,
        assigned_to_user_id TEXT,
        notes              TEXT,
        company_id         TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
        created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_crm_contact_company ON crm_contact(company_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_contact_crm_company ON crm_contact(crm_company_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_contact_lifecycle ON crm_contact(lifecycle)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_contact_email ON crm_contact(company_id, lower(email)) WHERE email IS NOT NULL",
    """CREATE TABLE IF NOT EXISTS crm_contact_role (
        id                 TEXT PRIMARY KEY,
        crm_contact_id     TEXT NOT NULL REFERENCES crm_contact(id) ON DELETE CASCADE,
        crm_company_id     TEXT NOT NULL REFERENCES crm_company(id) ON DELETE CASCADE,
        role_title         TEXT,
        is_primary         INTEGER NOT NULL DEFAULT 0 CHECK(is_primary IN (0,1)),
        started_at         TEXT,
        ended_at           TEXT,
        company_id         TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
        created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_crm_contact_role_contact ON crm_contact_role(crm_contact_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_contact_role_company ON crm_contact_role(crm_company_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_contact_role ON crm_contact_role(crm_contact_id, crm_company_id)",
]

# Postgres DDL: same tables; lower()-based partial unique indexes are valid in PG too.
_DDL_POSTGRES = [
    """CREATE TABLE IF NOT EXISTS crm_company (
        id                 TEXT PRIMARY KEY,
        name               TEXT NOT NULL,
        domain             TEXT,
        industry           TEXT,
        employee_count     INTEGER,
        annual_revenue     TEXT,
        address_line1      TEXT,
        address_line2      TEXT,
        city               TEXT,
        state              TEXT,
        postal_code        TEXT,
        country            TEXT,
        linkedin_url       TEXT,
        lifecycle          TEXT NOT NULL DEFAULT 'prospect'
                           CHECK(lifecycle IN ('prospect','customer','partner','vendor','other')),
        linked_customer_id TEXT REFERENCES customer(id) ON DELETE SET NULL,
        assigned_to_user_id TEXT,
        notes              TEXT,
        company_id         TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
        created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_crm_company_company ON crm_company(company_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_company_lifecycle ON crm_company(lifecycle)",
    "CREATE INDEX IF NOT EXISTS idx_crm_company_linked_customer ON crm_company(linked_customer_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_company_domain ON crm_company(company_id, lower(domain)) WHERE domain IS NOT NULL",
    """CREATE TABLE IF NOT EXISTS crm_contact (
        id                 TEXT PRIMARY KEY,
        name               TEXT NOT NULL,
        email              TEXT,
        phone              TEXT,
        mobile             TEXT,
        job_title          TEXT,
        linkedin_url       TEXT,
        address_line1      TEXT,
        address_line2      TEXT,
        city               TEXT,
        state              TEXT,
        postal_code        TEXT,
        country            TEXT,
        lifecycle          TEXT NOT NULL DEFAULT 'lead'
                           CHECK(lifecycle IN ('lead','mql','sql','customer','other')),
        crm_company_id     TEXT REFERENCES crm_company(id) ON DELETE SET NULL,
        assigned_to_user_id TEXT,
        notes              TEXT,
        company_id         TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
        created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_crm_contact_company ON crm_contact(company_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_contact_crm_company ON crm_contact(crm_company_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_contact_lifecycle ON crm_contact(lifecycle)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_contact_email ON crm_contact(company_id, lower(email)) WHERE email IS NOT NULL",
    """CREATE TABLE IF NOT EXISTS crm_contact_role (
        id                 TEXT PRIMARY KEY,
        crm_contact_id     TEXT NOT NULL REFERENCES crm_contact(id) ON DELETE CASCADE,
        crm_company_id     TEXT NOT NULL REFERENCES crm_company(id) ON DELETE CASCADE,
        role_title         TEXT,
        is_primary         INTEGER NOT NULL DEFAULT 0 CHECK(is_primary IN (0,1)),
        started_at         TEXT,
        ended_at           TEXT,
        company_id         TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
        created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_crm_contact_role_contact ON crm_contact_role(crm_contact_id)",
    "CREATE INDEX IF NOT EXISTS idx_crm_contact_role_company ON crm_contact_role(crm_company_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_contact_role ON crm_contact_role(crm_contact_id, crm_company_id)",
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
    print("  crm_contact / crm_company / crm_contact_role ensured.")


def _run_postgres(url):
    import psycopg2
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            for stmt in _DDL_POSTGRES:
                cur.execute(stmt)
        conn.commit()
        print("  Postgres: crm_contact / crm_company / crm_contact_role ensured.")
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
    parser = argparse.ArgumentParser(description="erpclaw-growth migration 001: contact + company model")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("erpclaw-growth migration 001 complete.")
