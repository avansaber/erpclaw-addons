"""Shared pytest fixtures for ERPClaw Integrations Shopify unit tests."""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest
from shopify_test_helpers import init_all_tables, get_conn, build_env, seed_company


@pytest.fixture(autouse=True)
def isolated_master_key(tmp_path, monkeypatch):
    """Per-test isolated master key for at-rest token encryption.

    M31 H6 replaced the addon's bespoke XOR token cipher with AES-256-GCM keyed
    by the foundation per-machine master key (erpclaw_lib.master_key). Redirect
    CONFIG_DIR / MASTER_KEY_PATH to a per-test tmp dir so encrypt_token /
    decrypt_token round-trip on an isolated key and never touch the real
    ~/.config/erpclaw/. Mirrors the Stripe suite's isolated_credentials fixture.
    """
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    import erpclaw_lib.master_key as mk_mod

    config_dir = tmp_path / ".config" / "erpclaw"
    config_dir.mkdir(parents=True)

    monkeypatch.setattr(mk_mod, "CONFIG_DIR", str(config_dir))
    monkeypatch.setattr(mk_mod, "MASTER_KEY_PATH", str(config_dir / "master.key"))
    yield


@pytest.fixture
def db_path(tmp_path):
    """Per-test fresh SQLite database with foundation + shopify schema."""
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
def company_id(conn):
    """Seed a test company and return its ID."""
    return seed_company(conn)


@pytest.fixture
def env(conn):
    """Full Shopify environment: company, fiscal year, cost center, shopify account."""
    return build_env(conn)
