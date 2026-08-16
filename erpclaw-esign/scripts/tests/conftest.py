"""Shared pytest fixtures for ERPClaw E-Sign unit tests."""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

# Bind erpclaw_lib to THIS TREE's lib, not the deployed ~/.openclaw symlink.
# Same reasoning the L0 constitution conftest records: a branch that ADDS a lib
# module would otherwise import the OLD one — and pass, or in this module's case
# fail, for the wrong reason. ADR-0034 phase 2 converted this installer to
# `erpclaw_lib.seam`, which exists in the tree under test and may not exist in
# whatever is deployed on the machine running the suite.
_IN_TREE_LIB = os.path.abspath(os.path.join(
    _TESTS_DIR, "..", "..", "..", "..", "erpclaw", "scripts", "erpclaw-setup", "lib"))
if os.path.isdir(os.path.join(_IN_TREE_LIB, "erpclaw_lib")) and _IN_TREE_LIB not in sys.path:
    sys.path.insert(0, _IN_TREE_LIB)

import pytest
from esign_helpers import init_all_tables, get_conn, build_env


@pytest.fixture
def db_path(tmp_path):
    """Per-test fresh SQLite database with foundation + esign schema."""
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
    """Full esign environment: company, naming series."""
    return build_env(conn)
