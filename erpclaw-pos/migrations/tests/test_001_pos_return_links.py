"""Part A — erpclaw-pos migration 001: the return-link columns.

This migration is the only thing that carries an install PREDATING the
return-link release across to the current schema, and the ADR-0034 parity
oracle is structurally blind to it: that oracle compares two FRESH provisions,
and both of those already end with the two columns and the index present.
Fresh installs prove nothing about the upgrade, so the upgrade gets its own
tests.

The properties that matter:

  * an install missing both columns and the index acquires all three, and the
    columns it already had keep their identity AND their order;
  * a migrated database and a freshly provisioned one hold the same columns in
    the same order, and the same index — "fresh == migrated" is the whole
    point of moving the ALTER out of the installer rather than deleting it;
  * a second run adds nothing (idempotent), and so does a run against a fresh
    install;
  * an install with a row keeps that row byte-identical, with the new columns
    NULL. That is the migration's `MIGRATION_DATA_CLASS = "none"` declaration
    checked at runtime rather than taken on trust (M102: the gate catches an
    author who forgot, not one who was wrong);
  * a database without the tables is a clean skip, not a crash.

The pre-link fixture is built by provisioning the CONVERTED installer's
metadata and then dropping the index and the two columns, index first, so the
shape under test is derived from the shipped declaration rather than re-typed
beside it — a hand-copied fixture is a second source of truth that drifts.
The foundation is provisioned by its own installer for the same reason;
nothing here hand-writes DDL for a table another module owns. Everything
reaches the database through the seam (ADR-0034).
"""
import importlib.util
import os
import shutil
import sys

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_MIGRATIONS_DIR = os.path.dirname(_TESTS_DIR)
_MODULE_DIR = os.path.dirname(_MIGRATIONS_DIR)                        # .../erpclaw-pos/
_REPO = os.path.abspath(os.path.join(_MODULE_DIR, "..", "..", ".."))  # repo root
_LIB = os.path.join(_REPO, "source", "erpclaw", "scripts", "erpclaw-setup", "lib")
_SETUP_DIR = os.path.join(_REPO, "source", "erpclaw", "scripts", "erpclaw-setup")

if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, _LIB)

from erpclaw_lib import seam  # noqa: E402
from erpclaw_lib.db import get_connection, get_dialect  # noqa: E402

TABLE = "pos_transaction"
ITEM_TABLE = "pos_transaction_item"
INDEX_NAME = "idx_pos_txn_return_against"
LINK_COLUMNS = ["return_against_id", "return_against_item_id"]

# Fixed statements: no name is formatted into SQL, in the test either. Index
# first: the indexed column cannot be dropped while the index stands.
_DROP_LINK = (
    "DROP INDEX idx_pos_txn_return_against",
    "ALTER TABLE pos_transaction_item DROP COLUMN return_against_item_id",
    "ALTER TABLE pos_transaction DROP COLUMN return_against_id",
)
_INSERT_COMPANY = "INSERT INTO company (id, name, abbr) VALUES (?, ?, ?)"
_COMPANY_ROW = ("co-0001", "Fixture Retail Inc", "FRI")
_INSERT_PROFILE = "INSERT INTO pos_profile (id, name, company_id) VALUES (?, ?, ?)"
_PROFILE_ROW = ("prof-0001", "Fixture POS", "co-0001")
_INSERT_SESSION = "INSERT INTO pos_session (id, pos_profile_id, company_id) VALUES (?, ?, ?)"
_SESSION_ROW = ("sess-0001", "prof-0001", "co-0001")
_INSERT_TXN = (
    "INSERT INTO pos_transaction "
    "(id, pos_session_id, company_id, status, grand_total) "
    "VALUES (?, ?, ?, ?, ?)")
_TXN_ROW = ("txn-0001", "sess-0001", "co-0001", "submitted", "5.00")
_SELECT_TXN = (
    "SELECT id, pos_session_id, company_id, status, grand_total "
    "FROM pos_transaction")
_SELECT_LINK = "SELECT return_against_id FROM pos_transaction"


def _load(name, filename, directory):
    path = os.path.join(directory, filename)
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mig = _load("pos_migration_001", "001_pos_return_links.py", _MIGRATIONS_DIR)
installer = _load("pos_installer", "init_db.py", _MODULE_DIR)


@pytest.fixture(scope="module")
def _template(tmp_path_factory):
    """Foundation + current POS schema + an open session chain, built once.

    Built once and copied per test because the foundation installer takes ~2s
    and the tests below do not need eight of them. The foundation is here
    because `pos_transaction.company_id` and `pos_session_id` are real foreign
    keys and the seam turns foreign key enforcement ON — a fixture that dodges
    that is not testing the tables this module ships.
    """
    if get_dialect() != "sqlite":
        pytest.skip("fixture builds a SQLite database (module suites are "
                    "SQLite-pinned until ADR-0034 phase 5)")
    if not os.path.isfile(os.path.join(_SETUP_DIR, "init_schema.py")):
        pytest.skip("foundation installer not present (published module repo)")

    db = str(tmp_path_factory.mktemp("template") / "template.sqlite")
    _load("pos_test_init_schema", "init_schema.py", _SETUP_DIR).init_db(db)
    installer.provision(installer.METADATA, db)
    conn = get_connection(db)
    try:
        conn.execute(_INSERT_COMPANY, _COMPANY_ROW)
        conn.execute(_INSERT_PROFILE, _PROFILE_ROW)
        conn.execute(_INSERT_SESSION, _SESSION_ROW)
        conn.commit()
    finally:
        conn.close()
    return db


def _copy_template(_template, tmp_path, name):
    """A private copy of the template, sidecar journal files included."""
    target = str(tmp_path / name)
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(_template + suffix):
            shutil.copyfile(_template + suffix, target + suffix)
    return target


@pytest.fixture
def fresh_db(_template, tmp_path):
    """A database with the CURRENT POS schema."""
    return _copy_template(_template, tmp_path, "pos.sqlite")


@pytest.fixture
def pre_link_db(fresh_db):
    """`fresh_db` rewound to the shape that predates the return-link release."""
    conn = get_connection(fresh_db)
    try:
        for statement in _DROP_LINK:
            conn.execute(statement)
        conn.commit()
    except Exception as exc:  # noqa: BLE001 — a backend without DROP COLUMN
        pytest.skip(f"backend cannot rewind the fixture: {exc}")
    finally:
        conn.close()
    assert "return_against_id" not in seam.column_names(TABLE, fresh_db)
    assert "return_against_item_id" not in seam.column_names(
        ITEM_TABLE, fresh_db)
    assert INDEX_NAME not in seam.index_names(TABLE, fresh_db)
    return fresh_db


def test_an_install_missing_the_columns_and_index_acquires_them(pre_link_db):
    before_txn = seam.column_names(TABLE, pre_link_db)
    before_item = seam.column_names(ITEM_TABLE, pre_link_db)

    result = mig.run_migration(pre_link_db)

    assert sorted(result["added"]) == sorted(LINK_COLUMNS + [INDEX_NAME])
    assert result["already_present"] == []
    after_txn = seam.column_names(TABLE, pre_link_db)
    after_item = seam.column_names(ITEM_TABLE, pre_link_db)
    assert [c for c in LINK_COLUMNS if c not in after_txn + after_item] == []
    assert INDEX_NAME in seam.index_names(TABLE, pre_link_db)
    # The columns that were already there keep their identity and their order;
    # the new ones append. Nothing is reordered under an operator's data.
    assert after_txn[:len(before_txn)] == before_txn
    assert after_item[:len(before_item)] == before_item


def test_a_migrated_database_matches_a_fresh_one(pre_link_db, _template,
                                                 tmp_path):
    mig.run_migration(pre_link_db)

    other = _copy_template(_template, tmp_path, "fresh_again.sqlite")

    assert seam.column_names(TABLE, pre_link_db) == seam.column_names(
        TABLE, other)
    assert seam.column_names(ITEM_TABLE, pre_link_db) == seam.column_names(
        ITEM_TABLE, other)
    assert INDEX_NAME in seam.index_names(TABLE, pre_link_db)
    assert INDEX_NAME in seam.index_names(TABLE, other)


def test_a_second_run_adds_nothing(pre_link_db):
    first = mig.run_migration(pre_link_db)
    columns_after_first = (
        seam.column_names(TABLE, pre_link_db),
        seam.column_names(ITEM_TABLE, pre_link_db),
        seam.index_names(TABLE, pre_link_db),
    )

    second = mig.run_migration(pre_link_db)

    assert len(first["added"]) == 3
    assert second["added"] == []
    assert sorted(second["already_present"]) == sorted(
        LINK_COLUMNS + [INDEX_NAME])
    assert (seam.column_names(TABLE, pre_link_db),
            seam.column_names(ITEM_TABLE, pre_link_db),
            seam.index_names(TABLE, pre_link_db)) == columns_after_first


def test_a_fresh_install_is_a_no_op(fresh_db):
    """The converted installer declares everything, so there is nothing to add."""
    before = (seam.column_names(TABLE, fresh_db),
              seam.column_names(ITEM_TABLE, fresh_db))

    result = mig.run_migration(fresh_db)

    assert result["added"] == []
    assert sorted(result["already_present"]) == sorted(
        LINK_COLUMNS + [INDEX_NAME])
    assert (seam.column_names(TABLE, fresh_db),
            seam.column_names(ITEM_TABLE, fresh_db)) == before


def test_it_changes_no_row_it_finds(pre_link_db):
    """`MIGRATION_DATA_CLASS = "none"`, checked rather than trusted (M102)."""
    conn = get_connection(pre_link_db)
    try:
        conn.execute(_INSERT_TXN, _TXN_ROW)
        conn.commit()
    finally:
        conn.close()

    conn = get_connection(pre_link_db)
    try:
        before = conn.execute(_SELECT_TXN).fetchall()
    finally:
        conn.close()

    mig.run_migration(pre_link_db)

    conn = get_connection(pre_link_db)
    try:
        rows = conn.execute(_SELECT_TXN).fetchall()
        new_values = conn.execute(_SELECT_LINK).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1
    assert tuple(rows[0]) == _TXN_ROW
    assert tuple(rows[0]) == tuple(before[0])
    # The new column arrives empty: a column that held nothing before the run
    # is the "not data-changing" case of the definition.
    assert tuple(new_values[0]) == (None,)


def test_a_database_without_the_tables_is_a_clean_skip(tmp_path):
    db = str(tmp_path / "no_pos.sqlite")
    get_connection(db).close()          # the file exists; the tables do not

    result = mig.run_migration(db)

    assert result == {"added": [], "already_present": [], "reason": "table absent"}


def test_it_declares_that_it_changes_no_data():
    """The M102 declaration is part of the migration's contract, not decoration."""
    assert mig.MIGRATION_DATA_CLASS == "none"


def test_both_columns_and_the_index_are_declared_by_the_installer_too():
    """Fresh and migrated cannot diverge if both sources name the same three.

    The installer is the fresh path and this migration is the upgrade path. They
    are two files, so nothing but a test stops one of them growing a third
    column the other never hears about.
    """
    declared_txn = {c.name for c in installer.POS_TRANSACTION.columns}
    declared_item = {c.name for c in installer.POS_TRANSACTION_ITEM.columns}
    assert "return_against_id" in declared_txn
    assert "return_against_item_id" in declared_item
    assert [column for _, column, _ in mig.ADD_COLUMNS] == LINK_COLUMNS
    assert mig.INDEX_NAME == INDEX_NAME
