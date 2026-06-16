"""erpclaw-growth migration 003: Pipeline stages (customizable) — Wave 1B F3.

Creates the two growth-owned pipeline tables + seeds the default pipeline:
  - crm_pipeline       — pipeline definition (catalog row, no company_id; shared
                         across the install like a chart-of-accounts template)
  - crm_pipeline_stage — ordered stage within a pipeline (stage_order UNIQUE per
                         pipeline; name UNIQUE case-insensitive per pipeline)

Then seeds the default "Standard Sales" 7-stage pipeline (the original hardcoded
opportunity stages) so existing opportunity rows have somewhere to point.

Matches init_db.py create_crmadv_tables() exactly so fresh installs and existing
installs converge. Namespaced 'erpclaw-growth:003' in the shared
erpclaw_schema_migration ledger via the P1 module-migration runner.

This migration MUST run before foundation migration 024 (024 backfills
opportunity.pipeline_stage_id by joining the seeded default pipeline; if 024 runs
first on an existing install it falls back to seeding the pipeline itself — Option A
self-contained — so ordering is forgiving either way).

money: crm_pipeline_stage.default_probability is TEXT (Python Decimal), never float.
ADR: planning/decisions/ADR-0023-foundation-fk-columns-for-addon-owned-entities.md
"""
import argparse
import os
import sqlite3
import uuid

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")

DEFAULT_PIPELINE_NAME = "Standard Sales"
# (stage_order, name, is_terminal_won, is_terminal_lost, default_probability)
DEFAULT_PIPELINE_STAGES = [
    (1, "new", 0, 0, "0"),
    (2, "contacted", 0, 0, "10"),
    (3, "qualified", 0, 0, "25"),
    (4, "proposal_sent", 0, 0, "50"),
    (5, "negotiation", 0, 0, "75"),
    (6, "won", 1, 0, "100"),
    (7, "lost", 0, 1, "0"),
]

_DDL = [
    """CREATE TABLE IF NOT EXISTS crm_pipeline (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL,
        description TEXT,
        is_default  INTEGER NOT NULL DEFAULT 0 CHECK(is_default IN (0,1)),
        is_active   INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
        created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_pipeline_name ON crm_pipeline(lower(name))",
    """CREATE TABLE IF NOT EXISTS crm_pipeline_stage (
        id                  TEXT PRIMARY KEY,
        crm_pipeline_id     TEXT NOT NULL REFERENCES crm_pipeline(id) ON DELETE CASCADE,
        stage_order         INTEGER NOT NULL,
        name                TEXT NOT NULL,
        is_terminal_won     INTEGER NOT NULL DEFAULT 0 CHECK(is_terminal_won IN (0,1)),
        is_terminal_lost    INTEGER NOT NULL DEFAULT 0 CHECK(is_terminal_lost IN (0,1)),
        default_probability TEXT NOT NULL DEFAULT '0',
        is_active           INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
        created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_crm_pipeline_stage_pipeline ON crm_pipeline_stage(crm_pipeline_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_pipeline_stage_order ON crm_pipeline_stage(crm_pipeline_id, stage_order)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_pipeline_stage_name ON crm_pipeline_stage(crm_pipeline_id, lower(name))",
]


def _get_dialect():
    return os.environ.get("ERPCLAW_DB_DIALECT", "sqlite")


def _seed_default_pipeline(execute, fetchone):
    """Seed the default 7-stage pipeline if no default exists. Idempotent.
    `execute`/`fetchone` are dialect-neutral callables (param style differs)."""
    row = fetchone("SELECT id FROM crm_pipeline WHERE is_default = 1 LIMIT 1", ())
    if row:
        return
    row = fetchone("SELECT id FROM crm_pipeline WHERE name = ? LIMIT 1",
                   (DEFAULT_PIPELINE_NAME,))
    if row:
        return
    pipeline_id = str(uuid.uuid4())
    execute("INSERT INTO crm_pipeline (id, name, description, is_default, is_active) "
            "VALUES (?, ?, ?, 1, 1)",
            (pipeline_id, DEFAULT_PIPELINE_NAME,
             "Default sales pipeline (the original 7 hardcoded opportunity stages)"))
    for order_no, name, won, lost, prob in DEFAULT_PIPELINE_STAGES:
        execute("INSERT INTO crm_pipeline_stage "
                "(id, crm_pipeline_id, stage_order, name, is_terminal_won, "
                " is_terminal_lost, default_probability, is_active) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
                (str(uuid.uuid4()), pipeline_id, order_no, name, won, lost, prob))


def _run_sqlite(path):
    conn = sqlite3.connect(path)
    try:
        from erpclaw_lib.db import setup_pragmas
        setup_pragmas(conn)
    except ImportError:
        conn.execute("PRAGMA busy_timeout=5000")
    for stmt in _DDL:
        conn.execute(stmt)
    _seed_default_pipeline(
        lambda sql, p: conn.execute(sql, p),
        lambda sql, p: conn.execute(sql, p).fetchone())
    conn.commit()
    conn.close()
    print("  crm_pipeline / crm_pipeline_stage ensured + default pipeline seeded.")


def _run_postgres(url):
    import psycopg2
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            for stmt in _DDL:
                cur.execute(stmt.replace("?", "%s") if "VALUES" in stmt else stmt)

            def _ex(sql, p):
                cur.execute(sql.replace("?", "%s"), p)

            def _fo(sql, p):
                cur.execute(sql.replace("?", "%s"), p)
                return cur.fetchone()
            _seed_default_pipeline(_ex, _fo)
        conn.commit()
        print("  Postgres: crm_pipeline / crm_pipeline_stage ensured + default pipeline seeded.")
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
    parser = argparse.ArgumentParser(description="erpclaw-growth migration 003: pipeline stages")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("erpclaw-growth migration 003 complete.")
