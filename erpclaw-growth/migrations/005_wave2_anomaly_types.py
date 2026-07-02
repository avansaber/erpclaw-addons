"""erpclaw-growth migration 005: extend anomaly.anomaly_type CHECK (Wave 2 AI1).

Adds two Wave 2 anomaly types to the growth-owned `anomaly` table's
anomaly_type CHECK so existing installs accept what the two new detectors emit:

  - reservation_over_available    — active stock reservations exceed on-hand qty
                                    (stock-out predicted; reads M5's
                                    stock_reservation_entry + the SLE balance).
  - subcontract_receipt_mismatch  — subcontract received qty diverges from
                                    materials transferred beyond tolerance
                                    (reads S5's subcontracting_order).

Mirrors init_db.create_crmadv_tables() exactly so fresh installs (which get the
full CHECK from init_db) and existing installs (which get it from this migration)
converge. Namespaced 'erpclaw-growth:005' in the shared erpclaw_schema_migration
ledger via the P1 module-migration runner (module_manager -> migration_runner).

Dialect-aware + idempotent, mirroring the M0 / Wave-1B CHECK-extension migrations
(foundation 024_displace_opportunity_stage_check.py is the structural precedent):

  - SQLite cannot ALTER a CHECK, so the table is rebuilt: rename -> recreate WITH
    the extended CHECK -> copy rows -> drop old -> reindex. `anomaly` has NO FK
    columns and NO inbound FK references (verified), so no FK-off rebuild dance is
    needed. Idempotency guard: if the new value already appears in the stored
    table SQL, the rebuild is skipped.
  - PostgreSQL: DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT with the full extended
    value list (the drop-first makes the re-add idempotent; every existing
    anomaly_type value is within the superset, so the re-validate passes).

All SQL is authored as FIXED string literals (no f-string / % / concatenation
value assembly) — the extended value list and column list are frozen, not user
input — so the static security scanner (constitution Article 10) stays clean.

money: `anomaly` holds no money columns (baseline/actual/deviation_pct are opaque
Decimal-as-text / JSON diagnostic strings, not postable amounts).

Usage:
    python3 005_wave2_anomaly_types.py [--db-path PATH]
"""
import argparse
import os
import sqlite3

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")

# One of the new values — used purely as the idempotency probe against the stored
# table SQL (SQLite) so a re-run skips the rebuild.
_NEW_VALUE_PROBE = "reservation_over_available"

# The `anomaly` table WITH the extended CHECK (18 existing + 2 Wave 2). Authored
# as a fixed literal so it matches init_db.create_crmadv_tables() exactly and
# carries no interpolation.
_ANOMALY_DDL_EXTENDED = """
CREATE TABLE anomaly (
    id              TEXT PRIMARY KEY,
    detected_at     TEXT DEFAULT CURRENT_TIMESTAMP,
    anomaly_type    TEXT NOT NULL CHECK(anomaly_type IN (
                        'price_spike','volume_change','duplicate_possible',
                        'margin_erosion','unusual_vendor','pattern_break',
                        'consumption_spike','late_pattern','round_number',
                        'ghost_employee','vendor_concentration',
                        'sequence_violation','benford_deviation','budget_overrun',
                        'inventory_shrinkage','payment_pattern_shift',
                        'asset_book_value_drift','dimension_tag_drift',
                        'reservation_over_available','subcontract_receipt_mismatch'
                    )),
    severity        TEXT NOT NULL DEFAULT 'info'
                    CHECK(severity IN ('info','warning','critical')),
    entity_type     TEXT,
    entity_id       TEXT,
    description     TEXT NOT NULL,
    evidence        TEXT,
    baseline        TEXT,
    actual          TEXT,
    deviation_pct   TEXT,
    status          TEXT NOT NULL DEFAULT 'new'
                    CHECK(status IN ('new','acknowledged','investigated','dismissed','resolved')),
    resolution_notes TEXT,
    assigned_to     TEXT,
    expires_at      TEXT
)
"""

# Fixed column list for the rebuild copy (matches _ANOMALY_DDL_EXTENDED order).
_ANOMALY_COPY_SQL = (
    "INSERT INTO anomaly ("
    "id, detected_at, anomaly_type, severity, entity_type, entity_id, "
    "description, evidence, baseline, actual, deviation_pct, status, "
    "resolution_notes, assigned_to, expires_at) "
    "SELECT "
    "id, detected_at, anomaly_type, severity, entity_type, entity_id, "
    "description, evidence, baseline, actual, deviation_pct, status, "
    "resolution_notes, assigned_to, expires_at "
    "FROM anomaly_w2ai1_old"
)

# PostgreSQL constraint re-definition, fixed literal (superset of existing values).
_PG_ADD_CONSTRAINT_SQL = (
    "ALTER TABLE anomaly ADD CONSTRAINT anomaly_anomaly_type_check "
    "CHECK (anomaly_type IN ("
    "'price_spike','volume_change','duplicate_possible',"
    "'margin_erosion','unusual_vendor','pattern_break',"
    "'consumption_spike','late_pattern','round_number',"
    "'ghost_employee','vendor_concentration',"
    "'sequence_violation','benford_deviation','budget_overrun',"
    "'inventory_shrinkage','payment_pattern_shift',"
    "'asset_book_value_drift','dimension_tag_drift',"
    "'reservation_over_available','subcontract_receipt_mismatch'))"
)

_ANOMALY_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_anomaly_status ON anomaly(status)",
    "CREATE INDEX IF NOT EXISTS idx_anomaly_type ON anomaly(anomaly_type)",
    "CREATE INDEX IF NOT EXISTS idx_anomaly_severity ON anomaly(severity)",
    "CREATE INDEX IF NOT EXISTS idx_anomaly_entity ON anomaly(entity_type, entity_id)",
]


def _get_dialect():
    return os.environ.get("ERPCLAW_DB_DIALECT", "sqlite")


def _sqlite_has_table(conn, table):
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _sqlite_check_already_extended(conn):
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='anomaly'"
    ).fetchone()
    return bool(row) and _NEW_VALUE_PROBE in (row[0] or "")


def _run_sqlite(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        from erpclaw_lib.db import setup_pragmas
        setup_pragmas(conn)
    except ImportError:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")

    if not _sqlite_has_table(conn, "anomaly"):
        print("  anomaly table absent (growth not installed). Nothing to migrate.")
        conn.close()
        return

    if _sqlite_check_already_extended(conn):
        print("  anomaly.anomaly_type CHECK already extended (idempotent no-op).")
        conn.close()
        return

    try:
        conn.execute("BEGIN")
        conn.execute("ALTER TABLE anomaly RENAME TO anomaly_w2ai1_old")
        conn.execute(_ANOMALY_DDL_EXTENDED)
        conn.execute(_ANOMALY_COPY_SQL)
        conn.execute("DROP TABLE anomaly_w2ai1_old")
        for idx in _ANOMALY_INDEXES:
            conn.execute(idx)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        conn.close()
        raise

    n = conn.execute("SELECT COUNT(*) FROM anomaly").fetchone()[0]
    dangling = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_w2ai1_old'"
    ).fetchall()
    if dangling:
        conn.close()
        raise RuntimeError(f"Migration 005 left dangling temp table: {[r[0] for r in dangling]}")
    print(f"  anomaly.anomaly_type CHECK extended (+2 Wave 2 types); {n} rows preserved.")
    conn.close()


def _run_postgres(url):
    import psycopg2
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.anomaly')")
            if cur.fetchone()[0] is None:
                print("  Postgres: anomaly table absent (growth not installed). Nothing to migrate.")
                return
            # Drop-first makes the re-add idempotent; the extended list is a
            # superset so re-validating existing rows always passes.
            cur.execute(
                "ALTER TABLE anomaly DROP CONSTRAINT IF EXISTS anomaly_anomaly_type_check")
            cur.execute(_PG_ADD_CONSTRAINT_SQL)
        conn.commit()
        print("  Postgres: anomaly.anomaly_type CHECK extended (+2 Wave 2 types).")
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
        description="erpclaw-growth migration 005: extend anomaly.anomaly_type CHECK (Wave 2 AI1)")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("erpclaw-growth migration 005 complete.")
