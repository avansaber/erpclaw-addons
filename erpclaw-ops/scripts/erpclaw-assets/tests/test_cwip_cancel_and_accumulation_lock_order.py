"""Lock order proofs for cancel-cwip and accumulate-cwip-cost (m826).

Every action that posts to the ledger and updates a document's state takes the
company's ledger chain head before its first write, then decides on state
re-read under the head. On the base, transfer-cwip-to-asset does (m824); these
tests pin that accumulate-cwip-cost and cancel-cwip do the same: a stale
status seen under the head is refused before any write, an accumulation adds
to the value read under the head, the cancel's final asset flip is a
compare-and-set, concurrent accumulations both count, and a cancel/transfer
race resolves exactly once.
"""
import importlib.util
import json
import os
import subprocess
import sys
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest

from assets_helpers import (
    call_action,
    get_conn,
    init_all_tables,
    is_error,
    is_ok,
    load_db_query,
    ns,
)

M = load_db_query()

from erpclaw_lib.db import get_connection, get_dialect  # noqa: E402
from erpclaw_lib.query import Field, P, Q, Table  # noqa: E402
from erpclaw_lib.vendor.pypika.terms import ValueWrapper  # noqa: E402


def _load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


xfer = _load_module(
    os.path.join(_TESTS_DIR, "test_cwip_transfer_lock_order.py"),
    "cwip_transfer_lock_order")

proofs = xfer.proofs
_RecordingProxy = xfer._RecordingProxy
_ASSETS_SCRIPT = xfer._ASSETS_SCRIPT

POSTING_DATE = "2026-04-01"
TRANSFER_DATE = "2026-04-01"


@pytest.fixture
def db_path(tmp_path):
    """Per-test SQLite database; skips on the PostgreSQL lane (m824 steps)."""
    if get_dialect() != "sqlite":
        pytest.skip("SQLite-only case")
    path = str(tmp_path / "test.sqlite")
    init_all_tables(path)
    os.environ["ERPCLAW_DB_PATH"] = path
    yield path
    os.environ.pop("ERPCLAW_DB_PATH", None)


@pytest.fixture
def pg_conn():
    """PostgreSQL connection on a freshly reset schema (m824 steps)."""
    proofs._pg_only()
    spec = importlib.util.spec_from_file_location(
        "payments_helpers_pg", xfer._PAYMENTS_HELPERS_PATH)
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


def _snapshot4(conn):
    out = {}
    for table in ("gl_entry", "asset", "cwip_cost_accumulation",
                  "asset_capitalization"):
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM %s" % table).fetchall()]
        out[table] = sorted(
            rows, key=lambda d: json.dumps(d, sort_keys=True, default=str))
    return out


def _accumulate_msg(naming, status):
    return ("accumulate-cwip-cost requires an under_construction asset; "
            "'%s' is '%s'. Start one with add-cwip." % (naming, status))


def _cancel_msg(naming, status):
    return ("cancel-cwip is only allowed before transfer; "
            "'%s' is '%s'." % (naming, status))


def _transfer_msg(naming, status):
    return ("transfer-cwip-to-asset requires an under_construction asset; "
            "'%s' is '%s'." % (naming, status))


def _naming(conn, aid):
    return conn.execute(
        "SELECT naming_series FROM asset WHERE id = ?",
        (aid,)).fetchone()["naming_series"]


def _accumulate_args(env, aid, amount):
    return ns(asset_id=aid, source_voucher_type="purchase_invoice",
              source_voucher_id=None, amount=amount,
              cwip_account_id=env["cwip_account_id"],
              source_account_id=env["source_account_id"],
              posting_date=POSTING_DATE)


def _cancel_args(aid):
    return ns(asset_id=aid, reason="test", posting_date=POSTING_DATE)


def _kind(sql):
    return sql.lstrip().split(None, 1)[0].upper()


def _is_asset_table_select(sql):
    return (_kind(sql) == "SELECT" and "asset" in sql
            and "cwip_cost_accumulation" not in sql
            and "asset_capitalization" not in sql
            and "asset_category" not in sql
            and "asset_id" not in sql)


def _accumulate_cmd(env, aid, amount):
    return [sys.executable, _ASSETS_SCRIPT,
            "--action", "accumulate-cwip-cost",
            "--asset-id", aid,
            "--source-voucher-type", "purchase_invoice",
            "--amount", amount,
            "--cwip-account-id", env["cwip_account_id"],
            "--source-account-id", env["source_account_id"],
            "--posting-date", POSTING_DATE]


def _cancel_cmd(aid):
    return [sys.executable, _ASSETS_SCRIPT,
            "--action", "cancel-cwip",
            "--asset-id", aid,
            "--reason", "test",
            "--posting-date", POSTING_DATE]


def _transfer_cmd(aid):
    return [sys.executable, _ASSETS_SCRIPT,
            "--action", "transfer-cwip-to-asset",
            "--asset-id", aid,
            "--depreciation-start-date", TRANSFER_DATE]


# ---------------------------------------------------------------------------
# 1. Accumulation refuses a stale status under the head.
# ---------------------------------------------------------------------------

def test_accumulation_refuses_stale_status_under_head(
        conn, db_path, monkeypatch):
    if get_dialect() != "sqlite":
        pytest.skip("SQLite-only case")
    from erpclaw_lib.gl_posting import take_chain_heads as real_take
    env, aid = xfer._build_book(conn)
    naming = _naming(conn, aid)
    gl_before = _snapshot4(conn)["gl_entry"]

    def _fake_take(conn_arg, company_ids):
        xfer._flip_to_in_use_pypika(conn_arg, aid)
        conn_arg.commit()
        return real_take(conn_arg, company_ids)

    monkeypatch.setattr(M, "take_chain_heads", _fake_take, raising=False)
    r = call_action(M.accumulate_cwip_cost, conn,
                    _accumulate_args(env, aid, "250.00"))
    assert is_error(r), r
    assert r["message"] == _accumulate_msg(naming, "in_use")

    fresh = get_conn(db_path)
    try:
        row = fresh.execute(
            "SELECT status, current_book_value FROM asset WHERE id = ?",
            (aid,)).fetchone()
        assert row["status"] == "in_use"
        assert row["current_book_value"] == "5000.00"
        assert fresh.execute(
            "SELECT COUNT(*) c FROM cwip_cost_accumulation WHERE asset_id = ?",
            (aid,)).fetchone()["c"] == 1
        assert _snapshot4(fresh)["gl_entry"] == gl_before
    finally:
        fresh.close()


def test_accumulation_refuses_stale_status_under_head_pg(
        pg_conn, monkeypatch):
    proofs._pg_only()
    from erpclaw_lib.gl_posting import take_chain_heads as real_take
    env, aid = xfer._build_book_pg(pg_conn)
    naming = _naming(pg_conn, aid)
    gl_before = _snapshot4(pg_conn)["gl_entry"]

    def _fake_take(conn_arg, company_ids):
        xfer._flip_to_in_use_pypika(conn_arg, aid)
        conn_arg.commit()
        return real_take(conn_arg, company_ids)

    monkeypatch.setattr(M, "take_chain_heads", _fake_take, raising=False)
    r = call_action(M.accumulate_cwip_cost, pg_conn,
                    _accumulate_args(env, aid, "250.00"))
    assert is_error(r), r
    assert r["message"] == _accumulate_msg(naming, "in_use")

    fresh = get_connection()
    try:
        row = fresh.execute(
            "SELECT status, current_book_value FROM asset WHERE id = ?",
            (aid,)).fetchone()
        assert row["status"] == "in_use"
        assert row["current_book_value"] == "5000.00"
        assert fresh.execute(
            "SELECT COUNT(*) c FROM cwip_cost_accumulation WHERE asset_id = ?",
            (aid,)).fetchone()["c"] == 1
        assert _snapshot4(fresh)["gl_entry"] == gl_before
    finally:
        fresh.close()


# ---------------------------------------------------------------------------
# 2. Accumulation adds to the value read under the head (SQLite).
# ---------------------------------------------------------------------------

def test_accumulation_adds_to_value_read_under_head(conn, monkeypatch):
    if get_dialect() != "sqlite":
        pytest.skip("SQLite-only case")
    from erpclaw_lib.gl_posting import take_chain_heads as real_take
    env, aid = xfer._build_book(conn)

    def _fake_take(conn_arg, company_ids):
        asset_t = Table("asset")
        conn_arg.execute(
            (Q.update(asset_t)
             .set(Field("gross_value"), ValueWrapper("6000.00"))
             .set(Field("current_book_value"), ValueWrapper("6000.00"))
             .where(asset_t.id == P())).get_sql(), (aid,))
        conn_arg.commit()
        return real_take(conn_arg, company_ids)

    monkeypatch.setattr(M, "take_chain_heads", _fake_take, raising=False)
    r = call_action(M.accumulate_cwip_cost, conn,
                    _accumulate_args(env, aid, "250.00"))
    assert is_ok(r), r
    assert r["accumulated_total"] == "6250.00"
    row = conn.execute(
        "SELECT gross_value, current_book_value FROM asset WHERE id = ?",
        (aid,)).fetchone()
    assert row["gross_value"] == "6250.00"
    assert row["current_book_value"] == "6250.00"


# ---------------------------------------------------------------------------
# 3. Cancel refuses a stale status under the head.
# ---------------------------------------------------------------------------

def test_cancel_refuses_stale_status_under_head(conn, db_path, monkeypatch):
    if get_dialect() != "sqlite":
        pytest.skip("SQLite-only case")
    from erpclaw_lib.gl_posting import take_chain_heads as real_take
    env, aid = xfer._build_book(conn)
    naming = _naming(conn, aid)

    calls = {"n": 0}
    real_reverse = M.reverse_gl_entries

    def _counting(conn_arg, *args, **kwargs):
        calls["n"] += 1
        return real_reverse(conn_arg, *args, **kwargs)

    monkeypatch.setattr(M, "reverse_gl_entries", _counting, raising=False)

    snap = {}

    def _fake_take(conn_arg, company_ids):
        xfer._flip_to_in_use_pypika(conn_arg, aid)
        conn_arg.commit()
        snap.update(_snapshot4(conn_arg))
        return real_take(conn_arg, company_ids)

    monkeypatch.setattr(M, "take_chain_heads", _fake_take, raising=False)
    r = call_action(M.cancel_cwip, conn, _cancel_args(aid))
    assert is_error(r), r
    assert r["message"] == _cancel_msg(naming, "in_use")
    assert calls["n"] == 0

    fresh = get_conn(db_path)
    try:
        assert _snapshot4(fresh) == snap
    finally:
        fresh.close()


def test_cancel_refuses_stale_status_under_head_pg(pg_conn, monkeypatch):
    proofs._pg_only()
    from erpclaw_lib.gl_posting import take_chain_heads as real_take
    env, aid = xfer._build_book_pg(pg_conn)
    naming = _naming(pg_conn, aid)

    calls = {"n": 0}
    real_reverse = M.reverse_gl_entries

    def _counting(conn_arg, *args, **kwargs):
        calls["n"] += 1
        return real_reverse(conn_arg, *args, **kwargs)

    monkeypatch.setattr(M, "reverse_gl_entries", _counting, raising=False)

    snap = {}

    def _fake_take(conn_arg, company_ids):
        xfer._flip_to_in_use_pypika(conn_arg, aid)
        conn_arg.commit()
        snap.update(_snapshot4(conn_arg))
        return real_take(conn_arg, company_ids)

    monkeypatch.setattr(M, "take_chain_heads", _fake_take, raising=False)
    r = call_action(M.cancel_cwip, pg_conn, _cancel_args(aid))
    assert is_error(r), r
    assert r["message"] == _cancel_msg(naming, "in_use")
    assert calls["n"] == 0

    fresh = get_connection()
    try:
        assert _snapshot4(fresh) == snap
    finally:
        fresh.close()


# ---------------------------------------------------------------------------
# 4. Cancel's final compare-and-set (PostgreSQL only).
# ---------------------------------------------------------------------------

def test_cancel_final_compare_and_set_pg(pg_conn, monkeypatch):
    proofs._pg_only()
    env, aid = xfer._build_book_pg(pg_conn)
    naming = _naming(pg_conn, aid)
    acc_id = pg_conn.execute(
        "SELECT id FROM cwip_cost_accumulation WHERE asset_id = ?",
        (aid,)).fetchone()["id"]
    real_reverse = M.reverse_gl_entries

    def _sneaky(conn_arg, *args, **kwargs):
        out = real_reverse(conn_arg, *args, **kwargs)
        other = get_connection()
        try:
            other.execute(
                "UPDATE asset SET status = ? WHERE id = ?",
                ("in_use", aid))
            other.commit()
        finally:
            other.close()
        return out

    monkeypatch.setattr(M, "reverse_gl_entries", _sneaky, raising=False)
    r = call_action(M.cancel_cwip, pg_conn, _cancel_args(aid))
    assert is_error(r), r
    assert r["message"] == _cancel_msg(naming, "in_use")

    fresh = get_connection()
    try:
        legs = fresh.execute(
            "SELECT is_cancelled FROM gl_entry "
            "WHERE voucher_type = ? AND voucher_id = ?",
            ("cwip_capitalization", acc_id)).fetchall()
        assert len(legs) == 2
        assert all(not row["is_cancelled"] for row in legs)
        assert fresh.execute(
            "SELECT status s FROM cwip_cost_accumulation WHERE id = ?",
            (acc_id,)).fetchone()["s"] == "submitted"
        row = fresh.execute(
            "SELECT status, current_book_value FROM asset WHERE id = ?",
            (aid,)).fetchone()
        assert row["status"] == "in_use"
        assert row["current_book_value"] == "5000.00"
    finally:
        fresh.close()


# ---------------------------------------------------------------------------
# 5. Head before the first write, and the decision under it (SQLite).
# ---------------------------------------------------------------------------

def test_accumulation_head_before_first_write(conn):
    if get_dialect() != "sqlite":
        pytest.skip("SQLite-only case")
    env, aid = xfer._build_book(conn)
    proxy = _RecordingProxy(conn)
    r = call_action(M.accumulate_cwip_cost, proxy,
                    _accumulate_args(env, aid, "250.00"))
    assert is_ok(r), r

    writes = [(i, s) for i, s in enumerate(proxy.statements)
              if _kind(s) in ("INSERT", "UPDATE", "DELETE")]
    assert writes, proxy.statements[:5]
    assert _kind(writes[0][1]) == "INSERT"
    assert "gl_chain_head" in writes[0][1]

    rereads = [i for i, s in enumerate(proxy.statements)
               if _is_asset_table_select(s) and i > writes[0][0]]
    assert rereads, [s for s in proxy.statements if "asset" in s][:8]
    first_reread = rereads[0]

    cwip_ins = next(
        i for i, s in enumerate(proxy.statements)
        if _kind(s) == "INSERT" and "cwip_cost_accumulation" in s)
    asset_upds = [i for i, s in enumerate(proxy.statements)
                  if _kind(s) == "UPDATE" and "asset" in s
                  and "gl_chain_head" not in s
                  and "cwip_cost_accumulation" not in s]
    assert asset_upds, proxy.statements
    assert first_reread < cwip_ins
    assert first_reread < asset_upds[0]


def test_cancel_head_before_first_write(conn):
    if get_dialect() != "sqlite":
        pytest.skip("SQLite-only case")
    env, aid = xfer._build_book(conn)
    proxy = _RecordingProxy(conn)
    r = call_action(M.cancel_cwip, proxy, _cancel_args(aid))
    assert is_ok(r), r

    writes = [(i, s) for i, s in enumerate(proxy.statements)
              if _kind(s) in ("INSERT", "UPDATE", "DELETE")]
    assert writes, proxy.statements[:5]
    assert _kind(writes[0][1]) == "INSERT"
    assert "gl_chain_head" in writes[0][1]

    rereads = [i for i, s in enumerate(proxy.statements)
               if _is_asset_table_select(s) and i > writes[0][0]]
    assert rereads, [s for s in proxy.statements if "asset" in s][:8]
    first_reread = rereads[0]

    gl_ins = next(
        i for i, s in enumerate(proxy.statements)
        if _kind(s) == "INSERT" and "gl_entry" in s)
    cwip_upds = [i for i, s in enumerate(proxy.statements)
                 if _kind(s) == "UPDATE" and "cwip_cost_accumulation" in s]
    assert cwip_upds, proxy.statements
    asset_upds = [i for i, s in enumerate(proxy.statements)
                  if _kind(s) == "UPDATE" and "asset" in s
                  and "gl_chain_head" not in s
                  and "cwip_cost_accumulation" not in s
                  and "gl_entry" not in s]
    assert asset_upds, proxy.statements
    assert first_reread < gl_ins
    assert first_reread < cwip_upds[0]
    assert first_reread < asset_upds[0]


# ---------------------------------------------------------------------------
# 6. Concurrent accumulations both count (PostgreSQL only).
# ---------------------------------------------------------------------------

def test_concurrent_accumulations_both_count_pg(pg_conn):
    proofs._pg_only()
    for _ in range(5):
        env, aid = xfer._build_book_pg(pg_conn)
        penv = proofs._proc_env()
        first = subprocess.Popen(
            _accumulate_cmd(env, aid, "100.00"),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=penv)
        second = subprocess.Popen(
            _accumulate_cmd(env, aid, "100.00"),
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
        assert first.returncode == 0, (out_first, err_first)
        assert second.returncode == 0, (out_second, err_second)
        row = pg_conn.execute(
            "SELECT gross_value, current_book_value FROM asset WHERE id = ?",
            (aid,)).fetchone()
        assert row["gross_value"] == "5200.00"
        assert row["current_book_value"] == "5200.00"
        assert pg_conn.execute(
            "SELECT COUNT(*) c FROM cwip_cost_accumulation WHERE asset_id = ?",
            (aid,)).fetchone()["c"] == 3
        proofs._assert_chain_intact(pg_conn, env["company_id"])
        proofs._assert_contiguous(pg_conn, env["company_id"])


# ---------------------------------------------------------------------------
# 7. Cancel and transfer race once (PostgreSQL only).
# ---------------------------------------------------------------------------

def test_cancel_and_transfer_race_once_pg(pg_conn):
    proofs._pg_only()
    for _ in range(5):
        env, aid = xfer._build_book_pg(pg_conn)
        naming = _naming(pg_conn, aid)
        penv = proofs._proc_env()
        cancel = subprocess.Popen(
            _cancel_cmd(aid),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=penv)
        transfer = subprocess.Popen(
            _transfer_cmd(aid),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=penv)
        try:
            cancel_out, cancel_err = cancel.communicate(timeout=15)
            transfer_out, transfer_err = transfer.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            cancel.kill()
            transfer.kill()
            raise
        assert cancel.returncode is not None
        assert transfer.returncode is not None
        assert "deadlock" not in (cancel_out + cancel_err).lower()
        assert "deadlock" not in (transfer_out + transfer_err).lower()
        assert sorted([cancel.returncode, transfer.returncode]) == [0, 1]
        fresh = get_connection()
        try:
            if transfer.returncode == 0:
                assert json.loads(cancel_out) == {
                    "status": "error",
                    "message": _cancel_msg(naming, "in_use")}
                row = fresh.execute(
                    "SELECT status FROM asset WHERE id = ?",
                    (aid,)).fetchone()
                assert row["status"] == "in_use"
                assert fresh.execute(
                    "SELECT COUNT(*) c FROM asset_capitalization "
                    "WHERE asset_id = ?",
                    (aid,)).fetchone()["c"] == 1
                assert fresh.execute(
                    "SELECT status s FROM cwip_cost_accumulation "
                    "WHERE asset_id = ?",
                    (aid,)).fetchone()["s"] == "submitted"
            else:
                assert json.loads(transfer_out) == {
                    "status": "error",
                    "message": _transfer_msg(naming, "cancelled")}
                row = fresh.execute(
                    "SELECT status, current_book_value FROM asset "
                    "WHERE id = ?",
                    (aid,)).fetchone()
                assert row["status"] == "cancelled"
                assert row["current_book_value"] == "0"
                assert fresh.execute(
                    "SELECT COUNT(*) c FROM asset_capitalization "
                    "WHERE asset_id = ?",
                    (aid,)).fetchone()["c"] == 0
                assert fresh.execute(
                    "SELECT status s FROM cwip_cost_accumulation "
                    "WHERE asset_id = ?",
                    (aid,)).fetchone()["s"] == "reversed"
        finally:
            fresh.close()
        proofs._assert_chain_intact(pg_conn, env["company_id"])
        proofs._assert_contiguous(pg_conn, env["company_id"])


# ---------------------------------------------------------------------------
# 8. Happy paths unchanged (SQLite).
# ---------------------------------------------------------------------------

def test_happy_path_accumulate(conn):
    if get_dialect() != "sqlite":
        pytest.skip("SQLite-only case")
    env, aid = xfer._build_book(conn)
    r = call_action(M.accumulate_cwip_cost, conn,
                    _accumulate_args(env, aid, "250.00"))
    assert is_ok(r), r
    assert r["accumulated_total"] == "5250.00"
    legs = conn.execute(
        "SELECT account_id, debit, credit FROM gl_entry "
        "WHERE voucher_type = 'cwip_capitalization' AND voucher_id = ? "
        "AND is_cancelled = 0",
        (r["accumulation_id"],)).fetchall()
    assert len(legs) == 2
    by_acct = {row["account_id"]: dict(row) for row in legs}
    assert by_acct[env["cwip_account_id"]]["debit"] == "250.00"
    assert by_acct[env["cwip_account_id"]]["credit"] == "0.00"
    assert by_acct[env["source_account_id"]]["debit"] == "0.00"
    assert by_acct[env["source_account_id"]]["credit"] == "250.00"


def test_happy_path_cancel(conn):
    if get_dialect() != "sqlite":
        pytest.skip("SQLite-only case")
    env, aid = xfer._build_book(conn)
    r = call_action(M.cancel_cwip, conn, _cancel_args(aid))
    assert is_ok(r), r
    assert r["accumulations_reversed"] == 1
    row = conn.execute(
        "SELECT status FROM asset WHERE id = ?",
        (aid,)).fetchone()
    assert row["status"] == "cancelled"
    legs = conn.execute(
        "SELECT debit, credit FROM gl_entry WHERE account_id = ?",
        (env["cwip_account_id"],)).fetchall()
    net = sum((Decimal(x["debit"]) - Decimal(x["credit"])
               for x in legs), Decimal("0"))
    assert net == Decimal("0.00")
