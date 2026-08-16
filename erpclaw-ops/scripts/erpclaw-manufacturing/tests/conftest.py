"""Shared pytest fixtures for ERPClaw Manufacturing unit tests."""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest
from mfg_helpers import init_all_tables, get_conn, build_env

# ── M97 canonical block: product SUBPROCESSES bind this checkout ─────────────
# mfg_helpers has already bound erpclaw_lib to the tree under test (M54);
# `_M97_CHILD_LIB` is that same directory, read off the imported package so it
# cannot drift from the real binding. The subcontracting lifecycle delegates
# add/submit-stock-entry, create-purchase-invoice and cancel-stock-entry to the
# foundation router as SUBPROCESSES (see subcontract_helpers), and the shipped
# bootstrap resolves erpclaw_lib from $ERPCLAW_HOME/lib FIRST (ADR-0017) --
# right on a user machine, wrong here, where that symlink points at whichever
# checkout last ran an install. The symlink into the temp home is seeded
# deliberately: under a BARE temp home the child dies with a structured
# "foundation not installed" error that most assertions accept.
# Full reasoning + the poison proof: testing/unit/L0/test_subprocess_home_pin.py
import erpclaw_lib

_M97_CHILD_LIB = os.path.dirname(os.path.dirname(
    os.path.abspath(erpclaw_lib.__file__)))


@pytest.fixture(scope="session", autouse=True)
def _isolated_erpclaw_home(tmp_path_factory):
    """Pin ERPCLAW_HOME at a throwaway install seeded with this tree's lib."""
    if not os.path.isdir(os.path.join(_M97_CHILD_LIB, "erpclaw_lib")):
        yield None          # published module repo: the deployed install is right
        return
    home = str(tmp_path_factory.mktemp("erpclaw_home"))
    os.symlink(_M97_CHILD_LIB, os.path.join(home, "lib"))
    _prev = os.environ.get("ERPCLAW_HOME")
    os.environ["ERPCLAW_HOME"] = home
    yield home
    if _prev is None:
        os.environ.pop("ERPCLAW_HOME", None)
    else:
        os.environ["ERPCLAW_HOME"] = _prev


@pytest.fixture
def db_path(tmp_path):
    """Per-test fresh SQLite database with foundation schema."""
    path = str(tmp_path / "test.sqlite")
    init_all_tables(path)
    os.environ["ERPCLAW_DB_PATH"] = path
    yield path
    os.environ.pop("ERPCLAW_DB_PATH", None)


@pytest.fixture
def conn(db_path):
    """Per-test database connection (auto-closes after test)."""
    connection = get_conn(db_path)
    yield connection
    connection.close()


@pytest.fixture
def fresh_db(conn):
    """Alias for conn -- enables invariant engine auto-hook from root conftest."""
    return conn


@pytest.fixture
def env(conn):
    """Full manufacturing environment: company, naming series, items, warehouses."""
    return build_env(conn)
