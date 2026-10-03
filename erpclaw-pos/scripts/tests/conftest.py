"""Shared pytest fixtures for ERPClaw POS unit tests."""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest
from pos_helpers import init_all_tables, get_conn, build_env, load_db_query, SRC_DIR


@pytest.fixture
def db_path(tmp_path):
    """Per-test fresh SQLite database with full ERPClaw core + POS schema."""
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
    """Full POS environment: company, profile, session, item, naming series."""
    return build_env(conn)


@pytest.fixture
def mod():
    """Loaded db_query module with all ACTIONS."""
    return load_db_query()


@pytest.fixture
def selling_bridge(tmp_path, monkeypatch):
    """Route cross-skill 'erpclaw' calls at this checkout's foundation router.

    resolve_skill_script() checks $OPENCLAW_SKILLS_DIR/<skill>/scripts/db_query.py
    first; linking the whole scripts directory keeps the router's relative
    forward() to the selling domain working. Child processes resolve erpclaw_lib
    through $ERPCLAW_HOME/lib, pinned here to this tree's lib. The fixture's
    ERPCLAW_DB_PATH already points the subprocess at the test database.
    """
    skills = tmp_path / "skills"
    link = skills / "erpclaw" / "scripts"
    link.parent.mkdir(parents=True)
    os.symlink(os.path.join(SRC_DIR, "erpclaw", "scripts"), str(link))
    monkeypatch.setenv("OPENCLAW_SKILLS_DIR", str(skills))
    home = tmp_path / "erpclaw_home"
    home.mkdir()
    os.symlink(
        os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup", "lib"),
        str(home / "lib"),
    )
    monkeypatch.setenv("ERPCLAW_HOME", str(home))
    return str(skills)
