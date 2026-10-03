"""Live-PostgreSQL leg for the router required-table checks.

Seven module routers refuse to dispatch when their required tables are
absent. That guard must ask the connection's own dialect instead of reading
one backend's system tables directly: on PostgreSQL the direct read fails,
so every action died before dispatch even though the router selected the
right database.

This module provisions a live PostgreSQL schema and drives all seven
routers as subprocesses. It runs only when ERPCLAW_PG_TEST_URL names a
database; the SQLite-visible half of the contract lives in L0 and pins the
refusal messages byte for byte.
"""
import importlib.util
import json
import os
import subprocess
import sys
import uuid
from urllib.parse import urlparse

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

REPO_ROOT = os.path.abspath(os.path.join(_TESTS_DIR, "..", "..", "..", "..", ".."))
LIB = os.path.join(REPO_ROOT, "source", "erpclaw", "scripts", "erpclaw-setup", "lib")
SETUP_DIR = os.path.join(REPO_ROOT, "source", "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")

if LIB not in sys.path:
    import importlib as _il
    if _il.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, LIB)

from erpclaw_lib.db import get_connection
from erpclaw_lib import seam as _seam

ROUTER_CASES = (
    ("accounting-adv",
     "source/erpclaw/scripts/erpclaw-accounting-adv/db_query.py",
     "list-leases"),
    ("loans",
     "source/erpclaw-addons/erpclaw-loans/scripts/db_query.py",
     "loan-list-loans"),
    ("approvals",
     "source/erpclaw-addons/erpclaw-approvals/scripts/db_query.py",
     "approval-list-approval-rules"),
    ("fleet",
     "source/erpclaw-addons/erpclaw-fleet/scripts/db_query.py",
     "fleet-list-vehicles"),
    ("logistics",
     "source/erpclaw-addons/erpclaw-logistics/scripts/db_query.py",
     "logistics-list-carriers"),
    ("agriculture",
     "source/agricultureclaw/scripts/db_query.py",
     "agri-list-crop-types"),
    ("nonprofit",
     "source/nonprofitclaw/scripts/db_query.py",
     "nonprofit-list-donors"),
)

INSTALLERS = (
    ("source/erpclaw-addons/erpclaw-loans/init_db.py",
     "loans_init_m137c", "create_loans_tables"),
    ("source/erpclaw-addons/erpclaw-approvals/init_db.py",
     "approvals_init_m137c", "create_approvals_tables"),
    ("source/erpclaw-addons/erpclaw-fleet/init_db.py",
     "fleet_init_m137c", "create_fleet_tables"),
    ("source/erpclaw-addons/erpclaw-logistics/init_db.py",
     "logistics_init_m137c", "create_logistics_tables"),
    ("source/agricultureclaw/init_db.py",
     "agriculture_init_m137c", "create_agricultureclaw_tables"),
    ("source/nonprofitclaw/init_db.py",
     "nonprofit_init_m137c", "create_nonprofitclaw_tables"),
)


def seed_home(path):
    home = str(path / "home")
    os.makedirs(home, exist_ok=True)
    link = os.path.join(home, "lib")
    if not os.path.exists(link):
        os.symlink(LIB, link)
    return home


def pg_router_env(home, url):
    env = dict(os.environ)
    env["ERPCLAW_DB_DIALECT"] = "postgresql"
    env["ERPCLAW_DB_URL"] = url
    env.pop("ERPCLAW_DB_PATH", None)
    env["ERPCLAW_HOME"] = home
    env["PYTHONPATH"] = LIB + os.pathsep + env.get("PYTHONPATH", "")
    return env


def run_router(script, action, env, extra=()):
    return subprocess.run(
        [sys.executable, script, "--action", action, *extra],
        capture_output=True, text=True, timeout=120, env=env, cwd=REPO_ROOT,
    )


def _load_module(mod_name, path):
    spec = importlib.util.spec_from_file_location(mod_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_init_schema():
    return _load_module("init_schema_m137c_pg", INIT_SCHEMA_PATH)


def _seed_company(conn):
    cid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO company (id, name, abbr, default_currency, country,"
        " fiscal_year_start_month)"
        " VALUES (?, ?, ?, 'USD', 'United States', 1)",
        (cid, "PG Proof %s" % cid[:6], "PP%s" % cid[:4]),
    )
    conn.commit()
    return cid


def _reset_schema(url, full):
    """Drop and rebuild public, then provision foundation (plus modules).

    The module installers take no path and resolve it through
    ERPCLAW_DB_PATH, so that variable carries the test URL while they run
    and is restored afterwards; every router below still runs with no
    ERPCLAW_DB_PATH, proving URL-only resolution.
    """
    saved_path = os.environ.get("ERPCLAW_DB_PATH")
    if full:
        os.environ["ERPCLAW_DB_PATH"] = url
    try:
        return _reset_schema_inner(url, full)
    finally:
        if saved_path is None:
            os.environ.pop("ERPCLAW_DB_PATH", None)
        else:
            os.environ["ERPCLAW_DB_PATH"] = saved_path


def _reset_schema_inner(url, full):
    expected_db = urlparse(url).path.strip("/")
    if not expected_db:
        raise RuntimeError("refusing to reset: ERPCLAW_PG_TEST_URL names no database")
    setup_conn = get_connection()
    try:
        resolved_db = setup_conn.execute("SELECT current_database()").fetchone()[0]
        if resolved_db != expected_db:
            raise RuntimeError(
                "refusing to reset: ERPCLAW_PG_TEST_URL names database %r "
                "but the connection resolved to %r" % (expected_db, resolved_db))
        setup_conn.execute("DROP SCHEMA public CASCADE")
        setup_conn.execute("CREATE SCHEMA public")
        setup_conn.commit()
    finally:
        setup_conn.close()
    _load_init_schema().init_db(None)
    if full:
        for rel, mod_name, func_name in INSTALLERS:
            module = _load_module(mod_name, os.path.join(REPO_ROOT, rel))
            getattr(module, func_name)(None)
    conn = get_connection()
    try:
        company_id = _seed_company(conn)
    finally:
        conn.close()
    return company_id


@pytest.fixture(scope="module")
def pg_provisioned():
    """Live PostgreSQL with foundation, all six module schemas, one company."""
    pg_url = os.environ.get("ERPCLAW_PG_TEST_URL")
    if not pg_url:
        pytest.skip("ERPCLAW_PG_TEST_URL not set (live PostgreSQL required)")
    saved = {key: os.environ.get(key) for key in
             ("ERPCLAW_DB_DIALECT", "ERPCLAW_DB_URL", "ERPCLAW_DB_PATH")}
    os.environ["ERPCLAW_DB_DIALECT"] = "postgresql"
    os.environ["ERPCLAW_DB_URL"] = pg_url
    os.environ.pop("ERPCLAW_DB_PATH", None)
    try:
        company_id = _reset_schema(pg_url, full=True)
        yield {"url": pg_url, "company_id": company_id}
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        _seam.dispose_engines()


@pytest.fixture
def pg_foundation_only(pg_provisioned):
    """Fresh schema with foundation rows only; restores the full layout after."""
    url = pg_provisioned["url"]
    _reset_schema(url, full=False)
    yield pg_provisioned
    _reset_schema(url, full=True)


@pytest.mark.parametrize("name,rel,action",
                         [(c[0], c[1], c[2]) for c in ROUTER_CASES],
                         ids=[c[0] for c in ROUTER_CASES])
def test_postgresql_router_dispatches(tmp_path, pg_provisioned, name, rel, action):
    home = seed_home(tmp_path)
    env = pg_router_env(home, pg_provisioned["url"])
    proc = run_router(os.path.join(REPO_ROOT, rel), action, env,
                      extra=("--company-id", pg_provisioned["company_id"]))
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["status"] == "ok"


def test_postgresql_missing_module_tables_named(tmp_path, pg_foundation_only):
    """Foundation-only schema: the refusal names the module's own tables."""
    home = seed_home(tmp_path)
    env = pg_router_env(home, pg_foundation_only["url"])
    company_id = "m137c-" + uuid.uuid4().hex

    proc = run_router(
        os.path.join(REPO_ROOT,
                     "source/erpclaw-addons/erpclaw-approvals/scripts/db_query.py"),
        "approval-list-approval-rules", env,
        extra=("--company-id", company_id))
    assert proc.returncode == 1, proc.stderr
    assert json.loads(proc.stdout) == {
        "status": "error",
        "message": "Missing tables: approval_rule, approval_step, approval_request. "
                   "Run init_db.py first.",
        "suggestion": "python3 init_db.py",
    }

    proc = run_router(
        os.path.join(REPO_ROOT,
                     "source/erpclaw-addons/erpclaw-loans/scripts/db_query.py"),
        "loan-list-loans", env,
        extra=("--company-id", company_id))
    assert proc.returncode == 1, proc.stderr
    assert json.loads(proc.stdout) == {
        "status": "error",
        "error": "Required table 'loan' not found. Run erpclaw-loans init_db.py first.",
        "suggestion": "python3 init_db.py",
    }
