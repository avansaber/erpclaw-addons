"""erpclaw-alerts migration 001: email substrate (M8 phase A).

Creates the email_account / email_template / email_outbox / email_log tables for
the M8 email sender + queue. Matches init_db.py exactly. Applied to existing
alerts installs via the module migration runner (P1). email_log is append-only.
Idempotent (CREATE IF NOT EXISTS), dialect-aware.
"""
import argparse
import os
import sqlite3

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")

_DDL = [
    """CREATE TABLE IF NOT EXISTS email_account (
        id                   TEXT PRIMARY KEY,
        name                 TEXT NOT NULL,
        provider             TEXT NOT NULL DEFAULT 'smtp'
                             CHECK(provider IN ('smtp','ses','mailgun')),
        from_address         TEXT NOT NULL,
        reply_to_address     TEXT,
        is_default           INTEGER NOT NULL DEFAULT 0 CHECK(is_default IN (0,1)),
        is_active            INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
        config_json          TEXT NOT NULL DEFAULT '{}',
        last_health_check_at TEXT,
        last_health_status   TEXT,
        company_id           TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
        created_at           TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at           TEXT DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_email_account_company ON email_account(company_id)",
    "CREATE INDEX IF NOT EXISTS idx_email_account_default ON email_account(is_default)",
    """CREATE TABLE IF NOT EXISTS email_template (
        id                    TEXT PRIMARY KEY,
        name                  TEXT NOT NULL,
        subject               TEXT NOT NULL DEFAULT '',
        body_html             TEXT NOT NULL DEFAULT '',
        body_text             TEXT NOT NULL DEFAULT '',
        merge_field_list_json TEXT NOT NULL DEFAULT '[]',
        language              TEXT NOT NULL DEFAULT 'en',
        is_active             INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
        company_id            TEXT REFERENCES company(id) ON DELETE RESTRICT,
        created_at            TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at            TEXT DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_email_template_name ON email_template(name)",
    """CREATE TABLE IF NOT EXISTS email_outbox (
        id                  TEXT PRIMARY KEY,
        to_address          TEXT NOT NULL,
        from_account_id     TEXT REFERENCES email_account(id) ON DELETE RESTRICT,
        subject             TEXT NOT NULL DEFAULT '',
        body_html           TEXT NOT NULL DEFAULT '',
        body_text           TEXT NOT NULL DEFAULT '',
        template_id         TEXT REFERENCES email_template(id) ON DELETE SET NULL,
        merge_vars_json     TEXT NOT NULL DEFAULT '{}',
        status              TEXT NOT NULL DEFAULT 'queued'
                            CHECK(status IN ('queued','sending','sent','bounced','failed','retry')),
        attempt_count       INTEGER NOT NULL DEFAULT 0,
        next_attempt_at     TEXT,
        provider_message_id TEXT,
        sent_at             TEXT,
        error_message       TEXT,
        company_id          TEXT REFERENCES company(id) ON DELETE RESTRICT,
        created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at          TEXT DEFAULT CURRENT_TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_email_outbox_status ON email_outbox(status)",
    "CREATE INDEX IF NOT EXISTS idx_email_outbox_next_attempt ON email_outbox(next_attempt_at)",
    """CREATE TABLE IF NOT EXISTS email_log (
        id              TEXT PRIMARY KEY,
        email_outbox_id TEXT REFERENCES email_outbox(id) ON DELETE CASCADE,
        event_type      TEXT NOT NULL
                        CHECK(event_type IN ('queued','sending','sent','bounced','complaint',
                                             'delivered','opened','clicked','failed','retry')),
        event_at        TEXT DEFAULT CURRENT_TIMESTAMP,
        payload_json    TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_email_log_outbox ON email_log(email_outbox_id)",
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
    print("  email substrate ensured (email_account/template/outbox/log).")


def _run_postgres(url):
    import psycopg2
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            for stmt in _DDL:
                cur.execute(stmt)
        conn.commit()
        print("  Postgres: email substrate ensured.")
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
    parser = argparse.ArgumentParser(description="erpclaw-alerts migration 001: email substrate")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("erpclaw-alerts migration 001 complete.")
