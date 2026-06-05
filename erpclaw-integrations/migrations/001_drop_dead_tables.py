"""erpclaw-integrations migration 001: drop dead orphan tables (audit P2).

These tables were defined in this module's init_db but had ZERO code/doc
references (dead scaffolding — plaid_* for an unbuilt Plaid connector;
stripe_payment_intent/stripe_webhook_event superseded by the dedicated
erpclaw-integrations-stripe addon; s3_backup_record for an unbuilt S3 backup).
Removed from init_db for fresh installs; this drops them from existing DBs so
fresh == migrated. Children dropped before parents. Idempotent, dialect-aware.
stripe_config / s3_config are KEPT (referenced).
"""
import argparse
import os
import sqlite3

DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
_DROP_ORDER = [
    "plaid_transaction", "plaid_linked_account",
    "stripe_payment_intent", "stripe_webhook_event", "s3_backup_record",
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
    dropped = []
    for t in _DROP_ORDER:
        existed = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()
        conn.execute(f"DROP TABLE IF EXISTS {t}")
        if existed:
            dropped.append(t)
    conn.commit()
    conn.close()
    print(f"  dropped: {', '.join(dropped) if dropped else '(none — already absent)'}")


def _run_postgres(url):
    import psycopg2
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            for t in _DROP_ORDER:
                cur.execute(f"DROP TABLE IF EXISTS {t}")
        conn.commit()
        print("  Postgres: dead tables dropped (if present).")
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
    parser = argparse.ArgumentParser(description="erpclaw-integrations migration 001: drop dead tables")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("erpclaw-integrations migration 001 complete.")
