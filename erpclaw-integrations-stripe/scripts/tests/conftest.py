"""Shared pytest fixtures for erpclaw-integrations-stripe unit tests."""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest
from stripe_test_helpers import init_all_tables, get_conn, seed_company


@pytest.fixture(autouse=True)
def isolated_credentials(tmp_path, monkeypatch):
    """Per-test isolated credentials store + seeded Stripe credential.

    v4.1.0+ moved the Stripe API key from the --api-key shell flag into a
    foundation-managed encrypted credentials store at
    ~/.config/erpclaw/credentials.json.enc. The accounts.py path now
    requires a Stripe credential to exist before any add-account /
    update-account / configure-gl-mapping action will succeed.

    This fixture redirects CONFIG_DIR / MASTER_KEY_PATH / CREDENTIALS_PATH
    to a per-test tmp dir, then seeds a fixture Stripe credential. No
    test touches the real ~/.config/erpclaw/.
    """
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    import erpclaw_lib.credentials as creds_mod
    import erpclaw_lib.master_key as mk_mod

    config_dir = tmp_path / ".config" / "erpclaw"
    config_dir.mkdir(parents=True)

    monkeypatch.setattr(mk_mod, "CONFIG_DIR", str(config_dir))
    monkeypatch.setattr(mk_mod, "MASTER_KEY_PATH", str(config_dir / "master.key"))
    monkeypatch.setattr(creds_mod, "CREDENTIALS_PATH", str(config_dir / "credentials.json.enc"))

    creds_mod.set_credential("stripe", "rk_FIXTURE_test_abc123xyz789def456")
    yield


@pytest.fixture
def db_path(tmp_path):
    """Per-test fresh SQLite database with foundation + stripe schema."""
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
def company_id(conn):
    """Seed a test company and return its ID."""
    return seed_company(conn)
