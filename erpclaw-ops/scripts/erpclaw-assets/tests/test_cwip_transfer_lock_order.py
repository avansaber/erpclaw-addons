"""Lock order + capitalize-once proofs for transfer-cwip-to-asset (m824).

Every ledger-posting action takes the company's ledger chain head before its
first write and decides on state re-read under the head. These tests pin that
`transfer-cwip-to-asset` does: the head is taken before the `asset` row moves,
a stale status seen under the head is refused before any write, and two
concurrent transfers capitalize exactly once.
"""
import importlib.util
import json
import os
import subprocess
import sys
import time
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest

from assets_helpers import (
    build_gl_env,
    call_action,
    get_conn,
    init_all_tables,
    is_error,
    is_ok,
    load_db_query,
    ns,
    seed_account,
    seed_asset_category,
    seed_company,
    seed_cost_center,
    seed_disposal_accounts,
    seed_fiscal_year,
    set_asset_status,
    seed_asset,
    wire_category_accounts,
)

M = load_db_query()

from erpclaw_lib.db import get_connection, get_dialect  # noqa: E402
from erpclaw_lib.query import Field, P, Q, Table  # noqa: E402
from erpclaw_lib.vendor.pypika.terms import ValueWrapper  # noqa: E402

_MODULE_DIR = os.path.dirname(_TESTS_DIR)
_SCRIPTS_DIR = os.path.dirname(_MODULE_DIR)
_ROOT_DIR = os.path.dirname(_SCRIPTS_DIR)
_ADDONS_DIR = os.path.dirname(_ROOT_DIR)
_SRC_DIR = os.path.dirname(_ADDONS_DIR)
_PAYMENTS_TESTS = os.path.join(
    _SRC_DIR, "erpclaw", "scripts", "erpclaw-payments", "tests")
_PROOFS_PATH = os.path.join(_PAYMENTS_TESTS, "test_chain_lock_proofs.py")
_PROXY_SRC_PATH = os.path.join(
    _PAYMENTS_TESTS, "test_payment_edit_and_allocation_compare_and_set.py")
_PAYMENTS_HELPERS_PATH = os.path.join(_PAYMENTS_TESTS, "payments_helpers.py")
_ASSETS_SCRIPT = os.path.join(_MODULE_DIR, "db_query.py")

if _PAYMENTS_TESTS not in sys.path:
    sys.path.insert(0, _PAYMENTS_TESTS)


def _load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


proofs = _load_module(_PROOFS_PATH, "payments_chain_lock_proofs")
_proxy_source = _load_module(
    _PROXY_SRC_PATH, "payments_recording_proxy_source")
_RecordingProxy = _proxy_source._RecordingProxy

TRANSFER_DATE = "2026-04-01"


# ---------------------------------------------------------------------------
# Book builders (same calls as _cwip_env / _new_cwip_asset / _accumulate in
# tests/test_assets.py, made inline).
# ---------------------------------------------------------------------------

def _cwip_env(conn):
    env = build_gl_env(conn)
    env["cwip_account_id"] = seed_account(
        conn, env["company_id"], "CWIP", "capital_work_in_progress", "asset")
    env["source_account_id"] = seed_account(
        conn, env["company_id"], "Cash", "asset", "asset")
    return env


def _new_cwip_asset(conn, env, project_id=None):
    r = call_action(M.add_cwip, conn, ns(
        company_id=env["company_id"], asset_category_id=env["category_id"],
        name="Plant Under Construction", project_id=project_id))
    assert is_ok(r), r
    return r["asset_id"]


def _accumulate(conn, env, asset_id, amount, vtype="purchase_invoice", vid=None):
    return call_action(M.accumulate_cwip_cost, conn, ns(
        asset_id=asset_id, source_voucher_type=vtype, source_voucher_id=vid,
        amount=amount, cwip_account_id=env["cwip_account_id"],
        source_account_id=env["source_account_id"], posting_date="2026-03-01"))


def _build_book(conn, amount="5000.00"):
    env = _cwip_env(conn)
    aid = _new_cwip_asset(conn, env)
    r = _accumulate(conn, env, aid, amount)
    assert is_ok(r), r
    conn.commit()
    return env, aid


def _cwip_env_pg(conn):
    """PostgreSQL book: build_gl_env minus assets_helpers.seed_naming_series.

    That helper's ``INSERT OR IGNORE`` is SQLite-only (psycopg2 raises
    ``SyntaxError: syntax error at or near "OR"``); erpclaw_lib's
    ``get_next_name`` self-seeds portably with ``ON CONFLICT``, so the
    pre-seed is skipped on PostgreSQL. Recorded in CHANGES.md.
    """
    cid = seed_company(conn)
    seed_fiscal_year(conn, cid)
    cc_id = seed_cost_center(conn, cid)
    cat_id = seed_asset_category(conn, cid)
    accts = wire_category_accounts(conn, cat_id, cid)
    env = {"company_id": cid, "category_id": cat_id, "cost_center_id": cc_id}
    env.update(accts)
    env.update(seed_disposal_accounts(conn, cid))
    env["cwip_account_id"] = seed_account(
        conn, cid, "CWIP", "capital_work_in_progress", "asset")
    env["source_account_id"] = seed_account(
        conn, cid, "Cash", "asset", "asset")
    return env


def _build_book_pg(conn, amount="5000.00"):
    env = _cwip_env_pg(conn)
    aid = _new_cwip_asset(conn, env)
    r = _accumulate(conn, env, aid, amount)
    assert is_ok(r), r
    conn.commit()
    return env, aid


@pytest.fixture
def db_path(tmp_path):
    """Per-test SQLite database; skips on the PostgreSQL lane.

    Shadows the directory conftest's fixture for this module only: on a
    PostgreSQL lane the shared ``init_schema.init_db`` routes a file path to
    the PostgreSQL provisioner, so SQLite-only tests must skip here in the
    fixture rather than in the test body.
    """
    if get_dialect() != "sqlite":
        pytest.skip("SQLite-only case")
    path = str(tmp_path / "test.sqlite")
    init_all_tables(path)
    os.environ["ERPCLAW_DB_PATH"] = path
    yield path
    os.environ.pop("ERPCLAW_DB_PATH", None)


@pytest.fixture
def pg_conn():
    """PostgreSQL connection on a freshly reset schema (payments conftest shape)."""
    proofs._pg_only()
    spec = importlib.util.spec_from_file_location(
        "payments_helpers_pg", _PAYMENTS_HELPERS_PATH)
    helpers_pg = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helpers_pg)
    old_url = os.environ.get("ERPCLAW_DB_URL")
    old_path = os.environ.get("ERPCLAW_DB_PATH")
    os.environ["ERPCLAW_DB_URL"] = os.environ.get("ERPCLAW_PG_TEST_URL")
    os.environ.pop("ERPCLAW_DB_PATH", None)
    try:
        helpers_pg.init_all_tables(None)
        conn = get_connection()
        yield conn
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001, S110 - best effort
            pass
        conn.close()
    finally:
        if old_url is None:
            os.environ.pop("ERPCLAW_DB_URL", None)
        else:
            os.environ["ERPCLAW_DB_URL"] = old_url
        if old_path is None:
            os.environ.pop("ERPCLAW_DB_PATH", None)
        else:
            os.environ["ERPCLAW_DB_PATH"] = old_path


def _snapshot(conn):
    out = {}
    for table in ("gl_entry", "asset", "asset_capitalization",
                  "cwip_cost_accumulation", "depreciation_schedule"):
        rows = conn.execute(
            "SELECT * FROM %s ORDER BY id" % table).fetchall()
        out[table] = [dict(r) for r in rows]
    return out


def _refusal_for(naming):
    return ("transfer-cwip-to-asset requires an under_construction asset; "
            "'%s' is 'in_use'." % naming)


def _flip_to_in_use_pypika(conn, asset_id):
    asset_t = Table("asset")
    conn.execute(
        (Q.update(asset_t)
         .set(Field("status"), ValueWrapper("in_use"))
         .where(asset_t.id == P())).get_sql(), (asset_id,))


# ---------------------------------------------------------------------------
# 1. Stale status under the head is refused before any write.
# ---------------------------------------------------------------------------

def test_stale_status_under_head_refused_before_any_write(
        conn, db_path, monkeypatch):
    if get_dialect() != "sqlite":
        pytest.skip("SQLite-only case")
    from erpclaw_lib.gl_posting import take_chain_heads as real_take
    env, aid = _build_book(conn)
    naming = conn.execute(
        "SELECT naming_series FROM asset WHERE id = ?",
        (aid,)).fetchone()["naming_series"]
    gl_before = _snapshot(conn)["gl_entry"]

    def _fake_take(conn_arg, company_ids):
        real_take(conn_arg, company_ids)
        _flip_to_in_use_pypika(conn_arg, aid)
        conn_arg.commit()
        real_take(conn_arg, company_ids)

    monkeypatch.setattr(M, "take_chain_heads", _fake_take, raising=False)
    real_cat = M._category_accounts
    calls = {"n": 0}

    def _counting(conn_arg, asset_dict_arg):
        calls["n"] += 1
        return real_cat(conn_arg, asset_dict_arg)

    monkeypatch.setattr(M, "_category_accounts", _counting)

    r = call_action(M.transfer_cwip_to_asset, conn, ns(
        asset_id=aid, depreciation_start_date=TRANSFER_DATE))
    assert is_error(r), r
    assert r["message"] == _refusal_for(naming)

    fresh = get_conn(db_path)
    try:
        row = fresh.execute(
            "SELECT status, current_book_value FROM asset WHERE id = ?",
            (aid,)).fetchone()
        assert row["status"] == "in_use"
        assert row["current_book_value"] == "5000.00"
        assert fresh.execute(
            "SELECT COUNT(*) c FROM asset_capitalization WHERE asset_id = ?",
            (aid,)).fetchone()["c"] == 0
        assert [dict(x) for x in fresh.execute(
            "SELECT * FROM gl_entry ORDER BY id").fetchall()] == gl_before
        assert fresh.execute(
            "SELECT COUNT(*) c FROM audit_log WHERE action = ? AND entity_id = ?",
            ("transfer-cwip-to-asset", aid)).fetchone()["c"] == 0
    finally:
        fresh.close()
    assert calls["n"] == 0


def test_stale_status_under_head_refused_before_any_write_pg(
        pg_conn, monkeypatch):
    proofs._pg_only()
    from erpclaw_lib.gl_posting import take_chain_heads as real_take
    env, aid = _build_book_pg(pg_conn)
    naming = pg_conn.execute(
        "SELECT naming_series FROM asset WHERE id = ?",
        (aid,)).fetchone()["naming_series"]
    gl_before = _snapshot(pg_conn)["gl_entry"]

    def _fake_take(conn_arg, company_ids):
        real_take(conn_arg, company_ids)
        _flip_to_in_use_pypika(conn_arg, aid)
        conn_arg.commit()
        real_take(conn_arg, company_ids)

    monkeypatch.setattr(M, "take_chain_heads", _fake_take, raising=False)
    real_cat = M._category_accounts
    calls = {"n": 0}

    def _counting(conn_arg, asset_dict_arg):
        calls["n"] += 1
        return real_cat(conn_arg, asset_dict_arg)

    monkeypatch.setattr(M, "_category_accounts", _counting)

    r = call_action(M.transfer_cwip_to_asset, pg_conn, ns(
        asset_id=aid, depreciation_start_date=TRANSFER_DATE))
    assert is_error(r), r
    assert r["message"] == _refusal_for(naming)

    fresh = get_connection()
    try:
        row = fresh.execute(
            "SELECT status, current_book_value FROM asset WHERE id = ?",
            (aid,)).fetchone()
        assert row["status"] == "in_use"
        assert row["current_book_value"] == "5000.00"
        assert fresh.execute(
            "SELECT COUNT(*) c FROM asset_capitalization WHERE asset_id = ?",
            (aid,)).fetchone()["c"] == 0
        assert [dict(x) for x in fresh.execute(
            "SELECT * FROM gl_entry ORDER BY id").fetchall()] == gl_before
        assert fresh.execute(
            "SELECT COUNT(*) c FROM audit_log WHERE action = ? AND entity_id = ?",
            ("transfer-cwip-to-asset", aid)).fetchone()["c"] == 0
    finally:
        fresh.close()
    assert calls["n"] == 0


# ---------------------------------------------------------------------------
# 2. Head before the asset row (SQLite).
# ---------------------------------------------------------------------------

def test_head_before_asset_row(conn):
    if get_dialect() != "sqlite":
        pytest.skip("SQLite-only case")
    env, aid = _build_book(conn)
    proxy = _RecordingProxy(conn)
    r = call_action(M.transfer_cwip_to_asset, proxy, ns(
        asset_id=aid, depreciation_start_date=TRANSFER_DATE))
    assert is_ok(r), r

    def _kind(sql):
        return sql.lstrip().split(None, 1)[0].upper()

    writes = [s for s in proxy.statements
              if _kind(s) in ("INSERT", "UPDATE", "DELETE")]
    assert writes, proxy.statements[:5]
    assert "gl_chain_head" in writes[0]
    assert _kind(writes[0]) == "INSERT"
    first_asset_update = next(
        i for i, s in enumerate(writes)
        if _kind(s) == "UPDATE" and "asset" in s)
    assert first_asset_update > 0


# ---------------------------------------------------------------------------
# 3. Happy path unchanged (SQLite).
# ---------------------------------------------------------------------------

def test_happy_path_unchanged(conn):
    if get_dialect() != "sqlite":
        pytest.skip("SQLite-only case")
    env, aid = _build_book(conn)
    r = call_action(M.transfer_cwip_to_asset, conn, ns(
        asset_id=aid, depreciation_start_date=TRANSFER_DATE))
    assert is_ok(r), r
    assert r["capitalized_amount"] == "5000.00"
    assert r["new_status"] == "in_use"
    assert conn.execute(
        "SELECT COUNT(*) c FROM asset_capitalization WHERE asset_id = ?",
        (aid,)).fetchone()["c"] == 1
    legs = conn.execute(
        "SELECT account_id, debit, credit FROM gl_entry "
        "WHERE voucher_type = 'asset_capitalization' AND voucher_id = ? "
        "AND is_cancelled = 0",
        (r["capitalization_id"],)).fetchall()
    assert len(legs) == 2
    by_acct = {row["account_id"]: dict(row) for row in legs}
    assert by_acct[env["asset_account_id"]]["debit"] == "5000.00"
    assert by_acct[env["asset_account_id"]]["credit"] == "0.00"
    assert by_acct[env["cwip_account_id"]]["debit"] == "0.00"
    assert by_acct[env["cwip_account_id"]]["credit"] == "5000.00"

    env2, aid2 = _build_book(conn)
    r2 = call_action(M.transfer_cwip_to_asset, conn, ns(
        asset_id=aid2, depreciation_start_date=TRANSFER_DATE,
        final_additional_cost="250.00",
        source_account_id=env2["source_account_id"]))
    assert is_ok(r2), r2
    assert r2["capitalized_amount"] == "5250.00"


# ---------------------------------------------------------------------------
# 4. Head first (PostgreSQL only).
# ---------------------------------------------------------------------------

def test_head_first_pg(pg_conn):
    proofs._pg_only()
    from erpclaw_lib.gl_posting import take_chain_heads as real_take
    env, aid = _build_book_pg(pg_conn)
    holder = get_connection()
    try:
        real_take(holder, [env["company_id"]])
        holder_pid = holder.execute(
            "SELECT pg_backend_pid() AS pid").fetchone()["pid"]
        penv = proofs._proc_env(ERPCLAW_PG_LOCK_TIMEOUT="45s")
        proc = subprocess.Popen(
            [sys.executable, _ASSETS_SCRIPT,
             "--action", "transfer-cwip-to-asset",
             "--asset-id", aid,
             "--depreciation-start-date", TRANSFER_DATE],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=penv)
        try:
            proc.wait(timeout=1.0)
            pytest.fail(
                "transfer-cwip-to-asset should block on the held head "
                "(rc=%s)" % (proc.returncode,))
        except subprocess.TimeoutExpired:
            assert proc.poll() is None, \
                "transfer-cwip-to-asset must still be alive"
        # Wait until the transfer is seen blocked on the head. Each
        # pg_stat_activity read uses a fresh short-lived connection: the
        # holder's open transaction would serve a stale snapshot taken
        # before the transfer started waiting. Only the transfer backend
        # blocked by the holder counts.
        deadline = time.monotonic() + 30
        while True:
            if proc.poll() is not None:
                out, err_text = proc.communicate(timeout=12)
                pytest.fail(
                    "transfer-cwip-to-asset exited early (rc=%s out=%s "
                    "err=%s)" % (proc.returncode, out, err_text))
            watcher = get_connection()
            try:
                waiting = watcher.execute(
                    "SELECT COUNT(*) c FROM pg_stat_activity "
                    "WHERE state = 'active' "
                    "AND wait_event_type = 'Lock' "
                    "AND query LIKE '%gl_chain_head%' "
                    "AND pid <> pg_backend_pid() "
                    "AND ? = ANY(pg_blocking_pids(pid))",
                    (holder_pid,),
                ).fetchone()["c"]
            finally:
                watcher.close()
            if int(waiting) > 0:
                break
            if time.monotonic() > deadline:
                pytest.fail(
                    "transfer-cwip-to-asset never blocked on the head")
            time.sleep(0.5)
        # The waiter is holding no asset row when the head comes first; a
        # flip-first order already holds it. Probe once more now, before
        # releasing the head.
        probe = get_connection()
        try:
            probe.execute("SET lock_timeout = '1s'")
            try:
                probe.execute(
                    "UPDATE asset SET status = status WHERE id = ?",
                    (aid,))
                probe.rollback()
            except Exception:
                probe.rollback()
                assert proc.poll() is None
                pytest.fail(
                    "transfer-cwip-to-asset holds the asset row "
                    "while waiting on the head")
        finally:
            probe.close()
        holder.rollback()
        out, err_text = proc.communicate(timeout=12)
        assert proc.returncode == 0, (out, err_text)
    finally:
        try:
            holder.rollback()
        except Exception:  # noqa: BLE001, S110 - best effort
            pass
        holder.close()


# ---------------------------------------------------------------------------
# 5. Capitalized once (PostgreSQL only).
# ---------------------------------------------------------------------------

def test_capitalized_once_pg(pg_conn):
    proofs._pg_only()
    for _ in range(5):
        env, aid = _build_book_pg(pg_conn)
        naming = pg_conn.execute(
            "SELECT naming_series FROM asset WHERE id = ?",
            (aid,)).fetchone()["naming_series"]
        expected = _refusal_for(naming)
        penv = proofs._proc_env()
        first = subprocess.Popen(
            [sys.executable, _ASSETS_SCRIPT,
             "--action", "transfer-cwip-to-asset",
             "--asset-id", aid,
             "--depreciation-start-date", TRANSFER_DATE],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=penv)
        second = subprocess.Popen(
            [sys.executable, _ASSETS_SCRIPT,
             "--action", "transfer-cwip-to-asset",
             "--asset-id", aid,
             "--depreciation-start-date", TRANSFER_DATE],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=penv)
        try:
            out_first, err_first = first.communicate(timeout=15)
            out_second, err_second = second.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            first.kill()
            second.kill()
            raise
        assert "deadlock" not in (out_first + err_first).lower()
        assert "deadlock" not in (out_second + err_second).lower()
        assert sorted(
            [first.returncode, second.returncode]) == [0, 1]
        loser_out = out_first if first.returncode == 1 else out_second
        assert json.loads(loser_out) == {"status": "error",
                                         "message": expected}
        assert pg_conn.execute(
            "SELECT COUNT(*) c FROM asset_capitalization WHERE asset_id = ?",
            (aid,)).fetchone()["c"] == 1
        cwip_rows = pg_conn.execute(
            "SELECT debit, credit FROM gl_entry WHERE account_id = ? "
            "AND is_cancelled = 0",
            (env["cwip_account_id"],)).fetchall()
        net = sum((Decimal(x["debit"]) - Decimal(x["credit"])
                   for x in cwip_rows), Decimal("0"))
        assert net == Decimal("0.00")
        proofs._assert_chain_intact(pg_conn, env["company_id"])
        proofs._assert_contiguous(pg_conn, env["company_id"])


# ---------------------------------------------------------------------------
# 6. Final compare-and-set (PostgreSQL only).
# ---------------------------------------------------------------------------

def test_final_compare_and_set_pg(pg_conn, monkeypatch):
    proofs._pg_only()
    env, aid = _build_book_pg(pg_conn)
    naming = pg_conn.execute(
        "SELECT naming_series FROM asset WHERE id = ?",
        (aid,)).fetchone()["naming_series"]
    before = _snapshot(pg_conn)
    real_cat = M._category_accounts

    def _sneaky(conn_arg, asset_dict_arg):
        result = real_cat(conn_arg, asset_dict_arg)
        other = get_connection()
        try:
            other.execute(
                "UPDATE asset SET status = ? WHERE id = ?",
                ("in_use", aid))
            other.commit()
        finally:
            other.close()
        return result

    monkeypatch.setattr(M, "_category_accounts", _sneaky)
    r = call_action(M.transfer_cwip_to_asset, pg_conn, ns(
        asset_id=aid, depreciation_start_date=TRANSFER_DATE))
    assert is_error(r), r
    assert r["message"] == _refusal_for(naming)

    fresh = get_connection()
    try:
        row = fresh.execute(
            "SELECT status, current_book_value FROM asset WHERE id = ?",
            (aid,)).fetchone()
        assert row["status"] == "in_use"
        assert row["current_book_value"] == "5000.00"
        assert fresh.execute(
            "SELECT COUNT(*) c FROM asset_capitalization WHERE asset_id = ?",
            (aid,)).fetchone()["c"] == 0
        after = _snapshot(fresh)
        assert after["gl_entry"] == before["gl_entry"]
        assert after["asset_capitalization"] == []
        assert after["cwip_cost_accumulation"] == before["cwip_cost_accumulation"]
        assert after["depreciation_schedule"] == before["depreciation_schedule"]
    finally:
        fresh.close()
