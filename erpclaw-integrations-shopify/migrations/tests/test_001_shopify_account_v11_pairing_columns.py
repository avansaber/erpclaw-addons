"""Part A — erpclaw-integrations-shopify migration 001: the v1.1 pairing columns.

This migration is the only thing that carries an install PREDATING the v1.1
release across to the current schema, and the ADR-0034 parity oracle is
structurally blind to it: that oracle compares two FRESH provisions, and both of
those already end with the six columns present. Fresh installs prove nothing
about the upgrade, so the upgrade gets its own tests.

The properties that matter:

  * an install missing all six acquires all six, and the columns it already had
    keep their identity AND their order;
  * a migrated database and a freshly provisioned one hold the same columns in
    the same order — "fresh == migrated" is the whole point of moving the ALTER
    out of the installer rather than deleting it;
  * a second run adds nothing (idempotent), and so does a run against a fresh
    install;
  * an install with a row keeps that row byte-identical, with the six new
    columns NULL. That is the migration's `MIGRATION_DATA_CLASS = "none"`
    declaration checked at runtime rather than taken on trust (M102: the gate
    catches an author who forgot, not one who was wrong);
  * a database without the table is a clean skip, not a crash.

The pre-v1.1 fixture is built by provisioning the CONVERTED installer's metadata
and then dropping the six columns, so the shape under test is derived from the
shipped declaration rather than re-typed beside it — a hand-copied 34-column
fixture is a second source of truth that drifts. The foundation is provisioned by
its own installer for the same reason; nothing here hand-writes DDL for a table
another module owns. Everything reaches the database through the seam (ADR-0034).
"""
import importlib.util
import os
import shutil
import sys

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_MIGRATIONS_DIR = os.path.dirname(_TESTS_DIR)
_MODULE_DIR = os.path.dirname(_MIGRATIONS_DIR)                        # ...-shopify/
_REPO = os.path.abspath(os.path.join(_MODULE_DIR, "..", "..", ".."))  # repo root
_LIB = os.path.join(_REPO, "source", "erpclaw", "scripts", "erpclaw-setup", "lib")
_SETUP_DIR = os.path.join(_REPO, "source", "erpclaw", "scripts", "erpclaw-setup")

if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, _LIB)

from erpclaw_lib import seam  # noqa: E402
from erpclaw_lib.db import get_connection, get_dialect  # noqa: E402

TABLE = "shopify_account"
V11_COLUMNS = ["pairing_method", "hmac_secret_enc", "last_status_push_at",
               "disconnect_state", "status_mode", "erpclaw_url_override"]

# Fixed statements: no name is formatted into SQL, in the test either.
_DROP_V11 = (
    "ALTER TABLE shopify_account DROP COLUMN pairing_method",
    "ALTER TABLE shopify_account DROP COLUMN hmac_secret_enc",
    "ALTER TABLE shopify_account DROP COLUMN last_status_push_at",
    "ALTER TABLE shopify_account DROP COLUMN disconnect_state",
    "ALTER TABLE shopify_account DROP COLUMN status_mode",
    "ALTER TABLE shopify_account DROP COLUMN erpclaw_url_override",
)
_INSERT_COMPANY = "INSERT INTO company (id, name, abbr) VALUES (?, ?, ?)"
_COMPANY_ROW = ("co-0001", "Fixture Retail Inc", "FRI")
_INSERT_ACCOUNT = (
    "INSERT INTO shopify_account "
    "(id, company_id, shop_domain, access_token_enc, api_version, currency, status) "
    "VALUES (?, ?, ?, ?, ?, ?, ?)")
_ACCOUNT_ROW = ("acct-0001", "co-0001", "demo.myshopify.com",
                "enc:FIXTURE", "2026-04", "USD", "active")
_SELECT_ACCOUNT = (
    "SELECT id, company_id, shop_domain, access_token_enc, api_version, "
    "currency, status FROM shopify_account")
_SELECT_V11 = (
    "SELECT pairing_method, hmac_secret_enc, last_status_push_at, "
    "disconnect_state, status_mode, erpclaw_url_override FROM shopify_account")


def _load(name, filename, directory):
    path = os.path.join(directory, filename)
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mig = _load("shopify_migration_001",
            "001_shopify_account_v11_pairing_columns.py", _MIGRATIONS_DIR)
installer = _load("shopify_installer", "init_db.py", _MODULE_DIR)


@pytest.fixture(scope="module")
def _template(tmp_path_factory):
    """Foundation + current shopify schema + one company, built once.

    Built once and copied per test because the foundation installer takes ~2s and
    eight tests do not need eight of them. The foundation is here because
    `shopify_account.company_id` is a real foreign key and the seam turns foreign
    key enforcement ON — a fixture that dodges that is not testing the table this
    module ships.
    """
    if get_dialect() != "sqlite":
        pytest.skip("fixture builds a SQLite database (module suites are "
                    "SQLite-pinned until ADR-0034 phase 5)")
    if not os.path.isfile(os.path.join(_SETUP_DIR, "init_schema.py")):
        pytest.skip("foundation installer not present (published module repo)")

    db = str(tmp_path_factory.mktemp("template") / "template.sqlite")
    _load("shopify_test_init_schema", "init_schema.py", _SETUP_DIR).init_db(db)
    installer.provision(installer.METADATA, db)
    conn = get_connection(db)
    try:
        conn.execute(_INSERT_COMPANY, _COMPANY_ROW)
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
    """A database with the CURRENT shopify schema."""
    return _copy_template(_template, tmp_path, "shopify.sqlite")


@pytest.fixture
def pre_v11_db(fresh_db):
    """`fresh_db` rewound to the shape that predates the v1.1 release."""
    conn = get_connection(fresh_db)
    try:
        for statement in _DROP_V11:
            conn.execute(statement)
        conn.commit()
    except Exception as exc:  # noqa: BLE001 — a backend without DROP COLUMN
        pytest.skip(f"backend cannot rewind the fixture: {exc}")
    finally:
        conn.close()
    assert not [c for c in V11_COLUMNS if c in seam.column_names(TABLE, fresh_db)]
    return fresh_db


def test_an_install_missing_the_six_acquires_the_six(pre_v11_db):
    before = seam.column_names(TABLE, pre_v11_db)

    result = mig.run_migration(pre_v11_db)

    assert sorted(result["added"]) == sorted(V11_COLUMNS)
    assert result["already_present"] == []
    after = seam.column_names(TABLE, pre_v11_db)
    assert [c for c in V11_COLUMNS if c not in after] == []
    # The columns that were already there keep their identity and their order;
    # the new ones append. Nothing is reordered under an operator's data.
    assert after[:len(before)] == before


def test_a_migrated_database_matches_a_fresh_one(pre_v11_db, _template, tmp_path):
    mig.run_migration(pre_v11_db)

    other = _copy_template(_template, tmp_path, "fresh_again.sqlite")

    assert seam.column_names(TABLE, pre_v11_db) == seam.column_names(TABLE, other)


def test_a_second_run_adds_nothing(pre_v11_db):
    first = mig.run_migration(pre_v11_db)
    columns_after_first = seam.column_names(TABLE, pre_v11_db)

    second = mig.run_migration(pre_v11_db)

    assert len(first["added"]) == 6
    assert second["added"] == []
    assert sorted(second["already_present"]) == sorted(V11_COLUMNS)
    assert seam.column_names(TABLE, pre_v11_db) == columns_after_first


def test_a_fresh_install_is_a_no_op(fresh_db):
    """The converted installer declares all six, so there is nothing to add."""
    before = seam.column_names(TABLE, fresh_db)

    result = mig.run_migration(fresh_db)

    assert result["added"] == []
    assert sorted(result["already_present"]) == sorted(V11_COLUMNS)
    assert seam.column_names(TABLE, fresh_db) == before


def test_it_changes_no_row_it_finds(pre_v11_db):
    """`MIGRATION_DATA_CLASS = "none"`, checked rather than trusted (M102)."""
    conn = get_connection(pre_v11_db)
    try:
        conn.execute(_INSERT_ACCOUNT, _ACCOUNT_ROW)
        conn.commit()
    finally:
        conn.close()

    mig.run_migration(pre_v11_db)

    conn = get_connection(pre_v11_db)
    try:
        rows = conn.execute(_SELECT_ACCOUNT).fetchall()
        new_values = conn.execute(_SELECT_V11).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1
    assert tuple(rows[0]) == _ACCOUNT_ROW
    # The six arrive empty: a column that held nothing before the run is the
    # "not data-changing" case of the definition.
    assert tuple(new_values[0]) == (None,) * 6


def test_a_database_without_the_table_is_a_clean_skip(tmp_path):
    db = str(tmp_path / "no_shopify.sqlite")
    get_connection(db).close()          # the file exists; the table does not

    result = mig.run_migration(db)

    assert result == {"added": [], "already_present": [], "reason": "table absent"}


def test_it_declares_that_it_changes_no_data():
    """The M102 declaration is part of the migration's contract, not decoration."""
    assert mig.MIGRATION_DATA_CLASS == "none"


def test_every_v11_column_is_declared_by_the_installer_too():
    """Fresh and migrated cannot diverge if both sources name the same six.

    The installer is the fresh path and this migration is the upgrade path. They
    are two files, so nothing but a test stops one of them growing a seventh
    column the other never hears about.
    """
    declared = {c.name for c in installer.SHOPIFY_ACCOUNT.columns}
    assert set(V11_COLUMNS) <= declared
    assert [column for column, _ in mig.ADD_COLUMNS] == V11_COLUMNS
