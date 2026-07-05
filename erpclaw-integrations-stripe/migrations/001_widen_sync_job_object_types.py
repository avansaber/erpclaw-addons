"""erpclaw-integrations-stripe migration 001: widen stripe_sync_job.object_type.

M33 / B8 (integrations deep-sync completion). The B8 completion registers two new
sync object types — `transfer` (Connect transfers → stripe_transfer) and
`credit_note` (→ stripe_credit_note) — in VALID_OBJECT_TYPES / FULL_SYNC_ORDER /
_SYNC_HANDLERS (sync.py). Each synced type creates a stripe_sync_job row whose
`object_type` is CHECK-constrained. The shipped CHECK enum did not include the two
new values, so a `transfer`/`credit_note` sync would fail with an IntegrityError on
job creation. init_db.py widens the CHECK for fresh installs; this widens it on
EXISTING (marketplace-live) installs so fresh == migrated.

Purely additive/permissive: the new enum is a strict superset of the old one, so
every existing stripe_sync_job row still satisfies it — no data can be invalidated.

- SQLite: rebuild stripe_sync_job with the widened CHECK (FK off,
  legacy_alter_table=ON, intersection-copy, indexes recreated, rows preserved,
  FK check clean). Idempotent — detects the already-widened CHECK and skips.
- PostgreSQL: DROP + re-ADD the object_type CHECK constraint with the wider enum.

Follows the foundation migration-007 rebuild idiom.
"""
import argparse
import os
import sqlite3

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")

# stripe_sync_job WITH the widened object_type CHECK. Whitespace-normalized-
# identical to init_db.py's CREATE TABLE (SQLite stores the DDL with IF NOT
# EXISTS stripped and whitespace normalized, so it is not byte-identical to the
# init_db source text — the column set + constraints match).
_SYNC_JOB_DDL_WIDENED = """
CREATE TABLE stripe_sync_job (
    id                  TEXT PRIMARY KEY,
    stripe_account_id   TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
    sync_type           TEXT NOT NULL
                        CHECK(sync_type IN ('full','incremental','webhook','historical_import')),
    object_type         TEXT NOT NULL
                        CHECK(object_type IN ('balance_transaction','charge','refund','dispute','payout','customer','invoice','subscription','transfer','credit_note','all')),
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','running','completed','failed','cancelled')),
    records_fetched     INTEGER NOT NULL DEFAULT 0,
    records_processed   INTEGER NOT NULL DEFAULT 0,
    records_failed      INTEGER NOT NULL DEFAULT 0,
    cursor_position     TEXT,
    sync_from           TEXT,
    sync_to             TEXT,
    error_message       TEXT,
    started_at          TEXT,
    completed_at        TEXT,
    company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
    created_at          TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

# New enum values whose presence in the live CHECK marks the migration as applied.
_MARKER = "'transfer'"


def _get_dialect():
    return os.environ.get("ERPCLAW_DB_DIALECT", "sqlite")


def _run_sqlite(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        from erpclaw_lib.db import setup_pragmas
        setup_pragmas(conn)
    except ImportError:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")

    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='stripe_sync_job'"
    ).fetchone()
    if not row:
        print("  stripe_sync_job absent; nothing to widen (module not initialized).")
        conn.close()
        return
    if _MARKER in (row[0] or ""):
        print("  stripe_sync_job.object_type CHECK already widened; nothing to do.")
        conn.close()
        return

    old_cols = [r[1] for r in conn.execute("PRAGMA table_info(stripe_sync_job)")]
    index_defs = [
        r[0] for r in conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' "
            "AND tbl_name='stripe_sync_job' AND sql IS NOT NULL"
        )
    ]
    before = conn.execute("SELECT COUNT(*) FROM stripe_sync_job").fetchone()[0]

    conn.execute("PRAGMA foreign_keys=OFF")
    # legacy_alter_table=ON so the RENAME does not rewrite any inbound FK references
    # to the temp name (defensive — stripe_sync_job has no FK children today).
    conn.execute("PRAGMA legacy_alter_table=ON")
    try:
        conn.execute("BEGIN")
        conn.execute("ALTER TABLE stripe_sync_job RENAME TO stripe_sync_job_b8_old")
        conn.execute(_SYNC_JOB_DDL_WIDENED)
        new_cols = [r[1] for r in conn.execute("PRAGMA table_info(stripe_sync_job)")]
        dropped = [c for c in old_cols if c not in new_cols]
        if dropped:
            conn.execute("ROLLBACK")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.close()
            raise RuntimeError(
                f"Migration 001 abort: stripe_sync_job has columns absent from the "
                f"target DDL that would be dropped: {dropped}. Update "
                f"_SYNC_JOB_DDL_WIDENED to match init_db.py."
            )
        common = ", ".join(c for c in new_cols if c in old_cols)
        conn.execute(
            f"INSERT INTO stripe_sync_job ({common}) "
            f"SELECT {common} FROM stripe_sync_job_b8_old"
        )
        conn.execute("DROP TABLE stripe_sync_job_b8_old")
        for ddl in index_defs:
            conn.execute(ddl)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute("PRAGMA legacy_alter_table=OFF")
        conn.execute("PRAGMA foreign_keys=ON")

    if conn.execute("SELECT COUNT(*) FROM stripe_sync_job").fetchone()[0] != before:
        conn.close()
        raise RuntimeError("Migration 001 row-count mismatch on stripe_sync_job")
    violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        conn.close()
        raise RuntimeError(
            f"Migration 001 left {len(violations)} FK violations: {violations[:5]}")
    dangling = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND sql LIKE '%_b8_old%'"
    ).fetchall()
    if dangling:
        conn.close()
        raise RuntimeError(
            f"Migration 001 left dangling refs to *_b8_old: {[r[0] for r in dangling]}")
    print(f"  stripe_sync_job.object_type CHECK widened (+transfer,+credit_note); "
          f"{before} rows preserved, FK check clean.")
    conn.close()


def _run_postgres(url):
    import psycopg2
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE stripe_sync_job "
                "DROP CONSTRAINT IF EXISTS stripe_sync_job_object_type_check")
            cur.execute(
                "ALTER TABLE stripe_sync_job ADD CONSTRAINT "
                "stripe_sync_job_object_type_check CHECK (object_type IN ("
                "'balance_transaction','charge','refund','dispute','payout',"
                "'customer','invoice','subscription','transfer','credit_note','all'))")
        conn.commit()
        print("  Postgres: stripe_sync_job.object_type CHECK widened "
              "(+transfer,+credit_note).")
    finally:
        conn.close()


def run_migration(db_path=None):
    if _get_dialect() == "postgresql":
        url = os.environ.get("ERPCLAW_DB_URL") or db_path
        if not url:
            print("Postgres dialect set but no connection URL (ERPCLAW_DB_URL). "
                  "Nothing to migrate.")
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
        description="Migration 001: widen stripe_sync_job.object_type CHECK")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("Migration 001 complete.")
