"""Regression guard for migrations/001_widen_sync_job_object_types.py (M33 B8).

The B8 completion registers two new stripe sync object types (`transfer`,
`credit_note`) that flow through the shared sync-job machinery, which writes a
`stripe_sync_job` row whose `object_type` is CHECK-constrained. The shipped
CHECK enum omitted the two new values, so `init_db.py` widened it for fresh
installs and this migration widens it on EXISTING (marketplace-live) installs.

This committed guard builds the OLD (pre-B8, 8-value CHECK) `stripe_sync_job`
schema, runs the migration, and asserts the rebuild is safe:
  * rows preserved verbatim,
  * the 4 shipped indexes recreated,
  * the CHECK now accepts `transfer`/`credit_note` and still rejects a bogus
    value (it widened, it did not drop),
  * a second run is an idempotent no-op.

Mirrors the M0 precedent of a committed guard per displacement/rebuild migration
(e.g. testing/unit/constitution/test_migration_fk_preservation.py for the
foundation account rebuild).
"""
import importlib.util
import os
import sqlite3
import uuid

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
_MODULE_DIR = os.path.dirname(_SCRIPTS_DIR)
_MIGRATION_PATH = os.path.join(
    _MODULE_DIR, "migrations", "001_widen_sync_job_object_types.py")

# The pre-B8 shape: stripe_sync_job with the ORIGINAL 8-value object_type CHECK
# (no 'transfer'/'credit_note') + the 4 shipped indexes + the FK parents.
_OLD_SCHEMA = """
CREATE TABLE company (id TEXT PRIMARY KEY, name TEXT, abbr TEXT);
CREATE TABLE stripe_account (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT
);
CREATE TABLE stripe_sync_job (
    id                  TEXT PRIMARY KEY,
    stripe_account_id   TEXT NOT NULL REFERENCES stripe_account(id) ON DELETE RESTRICT,
    sync_type           TEXT NOT NULL
                        CHECK(sync_type IN ('full','incremental','webhook','historical_import')),
    object_type         TEXT NOT NULL
                        CHECK(object_type IN ('balance_transaction','charge','refund','dispute','payout','customer','invoice','subscription','all')),
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
);
CREATE INDEX idx_stripe_sync_acct ON stripe_sync_job(stripe_account_id);
CREATE INDEX idx_stripe_sync_status ON stripe_sync_job(status);
CREATE INDEX idx_stripe_sync_company ON stripe_sync_job(company_id);
CREATE INDEX idx_stripe_sync_type ON stripe_sync_job(sync_type);
"""

_EXPECTED_INDEXES = {
    "idx_stripe_sync_acct", "idx_stripe_sync_status",
    "idx_stripe_sync_company", "idx_stripe_sync_type",
}


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "mig001_widen_sync_job", _MIGRATION_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _seed_old_db(path):
    """Build the pre-B8 DB and seed company/account + one sync_job row.

    Returns (company_id, stripe_account_id, sync_job_id). Closes its connection
    so the migration can take the write lock cleanly.
    """
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_OLD_SCHEMA)
    cid, aid, jid = (str(uuid.uuid4()) for _ in range(3))
    conn.execute("INSERT INTO company (id, name, abbr) VALUES (?, ?, ?)",
                 (cid, "Test Co", "TC"))
    conn.execute("INSERT INTO stripe_account (id, company_id) VALUES (?, ?)",
                 (aid, cid))
    conn.execute(
        "INSERT INTO stripe_sync_job "
        "(id, stripe_account_id, sync_type, object_type, status, company_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (jid, aid, "full", "charge", "completed", cid))
    conn.commit()
    conn.close()
    return cid, aid, jid


@pytest.fixture
def old_db(tmp_path):
    path = str(tmp_path / "old_schema.sqlite")
    cid, aid, jid = _seed_old_db(path)
    return {"path": path, "company_id": cid,
            "stripe_account_id": aid, "sync_job_id": jid}


def test_old_schema_rejects_new_object_types(old_db):
    """Sanity: the pre-B8 CHECK rejects 'transfer' — the migration has real work."""
    conn = sqlite3.connect(old_db["path"])
    conn.execute("PRAGMA foreign_keys=ON")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO stripe_sync_job "
            "(id, stripe_account_id, sync_type, object_type, status, company_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), old_db["stripe_account_id"], "full",
             "transfer", "completed", old_db["company_id"]))
    conn.close()


def test_migration_widens_check_preserves_rows_and_indexes(old_db):
    mig = _load_migration()
    mig.run_migration(old_db["path"])

    conn = sqlite3.connect(old_db["path"])
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    # Row preserved verbatim (same id + object_type).
    rows = conn.execute("SELECT * FROM stripe_sync_job").fetchall()
    assert len(rows) == 1
    assert rows[0]["id"] == old_db["sync_job_id"]
    assert rows[0]["object_type"] == "charge"

    # The 4 shipped indexes are present after the rebuild.
    idx = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' "
        "AND tbl_name='stripe_sync_job' AND name LIKE 'idx_%'")}
    assert _EXPECTED_INDEXES <= idx

    # CHECK now ACCEPTS both new object types.
    for obj_type in ("transfer", "credit_note"):
        conn.execute(
            "INSERT INTO stripe_sync_job "
            "(id, stripe_account_id, sync_type, object_type, status, company_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), old_db["stripe_account_id"], "full",
             obj_type, "completed", old_db["company_id"]))
    conn.commit()

    # CHECK still REJECTS a bogus value (widened, not dropped).
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO stripe_sync_job "
            "(id, stripe_account_id, sync_type, object_type, status, company_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), old_db["stripe_account_id"], "full",
             "definitely_not_a_valid_type", "completed", old_db["company_id"]))

    # No FK damage from the rebuild.
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    conn.close()


def test_migration_is_idempotent(old_db):
    mig = _load_migration()
    mig.run_migration(old_db["path"])
    # Second run must be a no-op — no error, no row loss, CHECK still widened.
    mig.run_migration(old_db["path"])

    conn = sqlite3.connect(old_db["path"])
    conn.execute("PRAGMA foreign_keys=ON")
    count = conn.execute("SELECT COUNT(*) FROM stripe_sync_job").fetchone()[0]
    assert count == 1
    # Still accepts a new type after the second (no-op) run.
    conn.execute(
        "INSERT INTO stripe_sync_job "
        "(id, stripe_account_id, sync_type, object_type, status, company_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (str(uuid.uuid4()), old_db["stripe_account_id"], "full",
         "credit_note", "completed", old_db["company_id"]))
    conn.commit()
    conn.close()
