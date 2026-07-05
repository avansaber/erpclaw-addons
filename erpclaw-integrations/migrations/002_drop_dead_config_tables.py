"""erpclaw-integrations migration 002: drop the dead config tables (M31 H2 / audit B7).

plaid_config / stripe_config / s3_config were defined in this module's init_db but
the register (writers=[], readers=[]) proves ZERO writers and ZERO readers ever.
Migration 001 kept them on a "referenced" rationale; the M31 audit overturns that:
the sole reference was the erpclaw-meta ownership map (SKILL_TABLES) — a runtime-
computed doc-map, NOT a persistence path. All three also normalized plaintext
secrets (client_id/secret, publishable_key/secret_key, access_key_id/
secret_access_key), contradicting the typed-credential mechanism the live
connectors actually use (integration_credential + crypto.encrypt_field). Removed
from init_db for fresh installs; this drops them from existing DBs so fresh ==
migrated.

Idempotent (DROP IF EXISTS), dialect-aware. No inbound FKs (SIM-verified), so
drop order is immaterial. Forward-only: the tables are empty (no code ever wrote
them), so there is no data to preserve.
"""
import argparse
import os
import sqlite3

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")
_DROP_ORDER = [
    "plaid_config", "stripe_config", "s3_config",
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
        print("  Postgres: dead config tables dropped (if present).")
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
    parser = argparse.ArgumentParser(description="erpclaw-integrations migration 002: drop dead config tables")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("erpclaw-integrations migration 002 complete.")
