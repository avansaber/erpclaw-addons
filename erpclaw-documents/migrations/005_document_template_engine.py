"""erpclaw-documents migration 005: document template engine/format (Wave 0 S8).

Adds the columns that let a template opt into a real render engine without
breaking any existing template:

  - document_template.format  TEXT  enum text|markdown|html, DEFAULT 'text'
  - document_template.engine  TEXT  enum legacy_replace|jinja2, DEFAULT 'legacy_replace'
  - document.pdf_path         TEXT  nullable (reserved for the later PDF chunk)

Existing rows get the defaults, so `engine='legacy_replace'` keeps the naive
`str.replace` path verbatim — zero behavior change until a template opts into
jinja2. These are column adds (no new tables). Idempotent (column-presence
guard) + dialect-aware. Matches init_db.py exactly. Applied to existing installs
via the foundation module-migration runner (recorded as
`erpclaw-documents:005_document_template_engine`).
"""
import argparse
import os
import sqlite3

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")

# (table, column, column DDL) — column DDL must match init_db.py.
_COLUMNS = [
    ("document_template", "format",
     "TEXT NOT NULL DEFAULT 'text' CHECK(format IN ('text','markdown','html'))"),
    ("document_template", "engine",
     "TEXT NOT NULL DEFAULT 'legacy_replace' "
     "CHECK(engine IN ('legacy_replace','jinja2'))"),
    ("document", "pdf_path", "TEXT"),
]


def _get_dialect():
    return os.environ.get("ERPCLAW_DB_DIALECT", "sqlite")


def _sqlite_has_column(conn, table, column):
    return any(r[1] == column for r in conn.execute(f"PRAGMA table_info({table})"))


def _table_exists_sqlite(conn, table):
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _run_sqlite(path):
    conn = sqlite3.connect(path)
    try:
        from erpclaw_lib.db import setup_pragmas
        setup_pragmas(conn)
    except ImportError:
        conn.execute("PRAGMA busy_timeout=5000")
    for table, column, ddl in _COLUMNS:
        if _table_exists_sqlite(conn, table) and not _sqlite_has_column(conn, table, column):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
            print(f"  {table}.{column}: added.")
        else:
            print(f"  {table}.{column}: already present (or {table} absent).")
    conn.commit()
    conn.close()


def _run_postgres(url):
    import psycopg2
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            for table, column, ddl in _COLUMNS:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {ddl}")
        conn.commit()
        print("  Postgres: document_template.format/engine + document.pdf_path ensured.")
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
    parser = argparse.ArgumentParser(
        description="erpclaw-documents migration 005: document template engine/format")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("erpclaw-documents migration 005 complete.")
