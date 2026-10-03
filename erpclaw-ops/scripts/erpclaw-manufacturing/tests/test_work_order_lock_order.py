"""Work-order transfer-materials takes the ledger chain head first (m831a).

Every action that posts to the ledger and changes a document's state must
take the company's ledger chain head before its first write, then decide on
state re-read under the head. These tests pin that order for
``transfer-materials``: a stale work order (or stale transferred quantities)
observed while waiting on the head is refused before any write, the head
write is the first write, the happy path is byte-identical, concurrent
transfers cannot jointly exceed the requirement, a mid-flight cancel flips
the header update to a refusal, and a refusal after the stock writes rolls
everything back.

SQLite legs run on the manufacturing ``conn`` fixture. PostgreSQL legs run
on the module-local ``pg_conn`` fixture, which mirrors the environment steps
of ``erpclaw-payments/tests/conftest.py`` ``db_path``. Money is exact text;
no float is used anywhere.
"""
import importlib.util
import json
import os
import subprocess
import sys
import time
import uuid
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest

from mfg_helpers import (call_action, get_conn, is_ok, load_db_query, ns,  # noqa: E402
                         seed_account, seed_company, seed_item,
                         seed_naming_series)

M = load_db_query()

from erpclaw_lib.db import get_connection  # noqa: E402
from erpclaw_lib.gl_posting import take_chain_heads as _real_take_heads  # noqa: E402

_SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(_TESTS_DIR)))))
_PAY_TESTS = os.path.join(
    _SRC_DIR, "erpclaw", "scripts", "erpclaw-payments", "tests")
_MFG_SCRIPT = os.path.join(os.path.dirname(_TESTS_DIR), "db_query.py")


def _load_pay_module(name, filename):
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(_PAY_TESTS, filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_proofs = _load_pay_module(
    "wo_lockorder_chain_proofs", "test_chain_lock_proofs.py")
_pg_only = _proofs._pg_only
_proc_env = _proofs._proc_env
_assert_chain_intact = _proofs._assert_chain_intact
_assert_contiguous = _proofs._assert_contiguous
_recmod = _load_pay_module(
    "wo_lockorder_recording",
    "test_payment_edit_and_allocation_compare_and_set.py")
_RecordingProxy = _recmod._RecordingProxy
_pg_helpers = _load_pay_module("payments_helpers_pg", "payments_helpers.py")

TRANSFER_DATE = "2026-06-05"

STATUS_MESSAGE = ("Cannot transfer materials for Work Order with status "
                  "'%s'. Must be 'not_started' or 'in_process'.")

# Pinned on the base by a sequential transfer of 4 after 10 are transferred
# (SQLite lane; pasted in CHANGES.md).
EXCEED_SQLITE_4_AFTER_10 = (
    "Transfer item 0: transferring 4 would exceed required qty. "
    "Required: 10.00, already transferred: 10")
# Pinned on the base by a sequential transfer of 6 after 6 are transferred
# (SQLite lane; pasted in CHANGES.md).
EXCEED_SQLITE_6_AFTER_6 = (
    "Transfer item 0: transferring 6 would exceed required qty. "
    "Required: 10.00, already transferred: 6")
# Pinned on the base by the same sequential calls on PostgreSQL.
EXCEED_PG_4_AFTER_10 = (
    "Transfer item 0: transferring 4 would exceed required qty. "
    "Required: 10.00, already transferred: 10.00")
EXCEED_PG_6_AFTER_6 = (
    "Transfer item 0: transferring 6 would exceed required qty. "
    "Required: 10.00, already transferred: 6.00")


@pytest.fixture
def pg_conn():
    """PostgreSQL connection on a freshly reset shared schema.

    Mirrors the environment steps of ``erpclaw-payments/tests/conftest.py``
    ``db_path``: ``ERPCLAW_DB_URL`` points at ``ERPCLAW_PG_TEST_URL``,
    ``ERPCLAW_DB_PATH`` is popped, the core schema (which already contains
    the manufacturing tables) is rebuilt, and both variables are restored
    afterwards. Skips outside the PostgreSQL lane.
    """
    _pg_only()
    test_url = os.environ.get("ERPCLAW_PG_TEST_URL")
    if not test_url:
        pytest.skip("needs ERPCLAW_PG_TEST_URL (private PostgreSQL cluster)")
    old_url = os.environ.get("ERPCLAW_DB_URL")
    old_path = os.environ.get("ERPCLAW_DB_PATH")
    os.environ["ERPCLAW_DB_URL"] = test_url
    os.environ.pop("ERPCLAW_DB_PATH", None)
    conn = None
    try:
        _pg_helpers.init_all_tables(None)
        conn = get_connection()
        yield conn
    finally:
        try:
            if conn is not None:
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


def _warehouse(conn, company_id, name, account_id):
    wid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO warehouse (id, name, company_id, account_id) "
        "VALUES (?, ?, ?, ?)",
        (wid, name, company_id, account_id))
    return wid


def _build_env(conn, with_naming_series):
    """One finished good, one raw material at 5.00, BOM ratio 1.

    ``seed_naming_series`` uses SQLite-only SQL, so PostgreSQL legs skip it:
    the actions under test do not need it (``get_next_name`` upserts its own
    row).
    """
    cid = seed_company(conn)
    if with_naming_series:
        seed_naming_series(conn, cid)
    conn.execute(
        "INSERT INTO fiscal_year (id, name, start_date, end_date, company_id) "
        "VALUES (?, ?, ?, ?, ?)",
        (str(uuid.uuid4()), "FY2026 " + cid[:8], "2026-01-01", "2026-12-31",
         cid))
    conn.execute(
        "INSERT INTO cost_center (id, name, company_id, is_group) "
        "VALUES (?, ?, ?, 0)",
        (str(uuid.uuid4()), "Main " + cid[:8], cid))
    env = {"company_id": cid}
    env["raw_acct"] = seed_account(conn, cid, "Raw Stock", "stock", "asset")
    env["wip_acct"] = seed_account(conn, cid, "WIP Stock", "stock", "asset")
    env["fg_acct"] = seed_account(conn, cid, "FG Stock", "stock", "asset")
    env["srnb_acct"] = seed_account(
        conn, cid, "Stock Received Not Billed",
        "stock_received_not_billed", "liability")
    env["cogs_acct"] = seed_account(
        conn, cid, "Cost of Goods Sold", "cost_of_goods_sold", "expense")
    env["raw_wh"] = _warehouse(conn, cid, "Raw Store", env["raw_acct"])
    env["wip_wh"] = _warehouse(conn, cid, "WIP Store", env["wip_acct"])
    env["fg_wh"] = _warehouse(conn, cid, "FG Store", env["fg_acct"])
    env["fg_item"] = seed_item(conn, cid, name="Finished Good",
                               standard_rate="0")
    env["rm"] = seed_item(conn, cid, name="Raw Material",
                          standard_rate="5.00")
    M.insert_sle_entries(
        conn,
        [{"item_id": env["rm"], "warehouse_id": env["raw_wh"],
          "actual_qty": "100", "incoming_rate": "5.00"}],
        voucher_type="stock_entry", voucher_id="opening-" + cid[:8],
        posting_date="2026-01-05", company_id=cid)
    conn.commit()
    r = call_action(M.add_bom, conn, ns(
        item_id=env["fg_item"], company_id=cid, quantity="1",
        items=json.dumps([{"item_id": env["rm"], "quantity": "1",
                           "rate": "5.00"}])))
    assert is_ok(r), r
    env["bom_id"] = r["bom_id"]
    return env


def _new_work_order(conn, env, qty="10"):
    r = call_action(M.add_work_order, conn, ns(
        bom_id=env["bom_id"], quantity=qty, company_id=env["company_id"],
        source_warehouse_id=env["raw_wh"],
        target_warehouse_id=env["fg_wh"],
        wip_warehouse_id=env["wip_wh"]))
    assert is_ok(r), r
    return r["work_order_id"]


def _start(conn, work_order_id):
    assert is_ok(call_action(M.start_work_order, conn,
                            ns(work_order_id=work_order_id)))


def _transfer(conn, env, work_order_id, qty):
    return call_action(M.transfer_materials, conn, ns(
        work_order_id=work_order_id, posting_date=TRANSFER_DATE,
        items=json.dumps([{"item_id": env["rm"], "qty": qty}])))


def _transfer_ok(conn, env, work_order_id, qty):
    r = _transfer(conn, env, work_order_id, qty)
    assert is_ok(r), r
    return r


def _table_count(conn, table):
    if table == "stock_ledger_entry":
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM stock_ledger_entry").fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) AS c FROM gl_entry").fetchone()
    return row["c"]


def _sle_for(conn, voucher_id):
    return conn.execute(
        "SELECT id FROM stock_ledger_entry "
        "WHERE voucher_type = 'work_order' AND voucher_id = ?",
        (voucher_id,)).fetchall()


def _gle_for(conn, voucher_id):
    return conn.execute(
        "SELECT id FROM gl_entry "
        "WHERE voucher_type = 'work_order' AND voucher_id = ?",
        (voucher_id,)).fetchall()


def _wrap_take_heads(monkeypatch, after_head):
    def sneaky(conn, company_ids):
        _real_take_heads(conn, company_ids)
        after_head(conn)
        conn.commit()
        _real_take_heads(conn, company_ids)
    monkeypatch.setattr(M, "take_chain_heads", sneaky, raising=False)


def _count_sle_calls(monkeypatch):
    calls = {"n": 0}
    real = M.insert_sle_entries

    def counting(conn, entries, *args, **kwargs):
        calls["n"] += 1
        return real(conn, entries, *args, **kwargs)

    monkeypatch.setattr(M, "insert_sle_entries", counting)
    return calls


def _run_1a(conn, fresh, monkeypatch, work_order_id, rm_id):
    before = (_table_count(conn, "stock_ledger_entry"),
              _table_count(conn, "gl_entry"))

    def complete_while_waiting(c):
        c.execute("UPDATE work_order SET status = 'completed' WHERE id = ?",
                  (work_order_id,))

    _wrap_take_heads(monkeypatch, complete_while_waiting)
    sle_calls = _count_sle_calls(monkeypatch)
    r = call_action(M.transfer_materials, conn, ns(
        work_order_id=work_order_id, posting_date=TRANSFER_DATE,
        items=json.dumps([{"item_id": rm_id, "qty": "4"}])))
    assert r == {"status": "error",
                 "message": STATUS_MESSAGE % "completed"}
    assert sle_calls["n"] == 0
    assert list(_sle_for(fresh, work_order_id)) == []
    assert list(_gle_for(fresh, work_order_id)) == []
    assert (_table_count(fresh, "stock_ledger_entry"),
            _table_count(fresh, "gl_entry")) == before


def test_1a_stale_completed_status_refused_before_any_write(
        conn, db_path, monkeypatch):
    env = _build_env(conn, True)
    wo_id = _new_work_order(conn, env)
    _start(conn, wo_id)
    fresh = get_conn(db_path)
    try:
        _run_1a(conn, fresh, monkeypatch, wo_id, env["rm"])
    finally:
        fresh.close()


def test_1a_stale_completed_status_refused_before_any_write_pg(
        pg_conn, monkeypatch):
    env = _build_env(pg_conn, False)
    wo_id = _new_work_order(pg_conn, env)
    _start(pg_conn, wo_id)
    fresh = get_connection()
    try:
        _run_1a(pg_conn, fresh, monkeypatch, wo_id, env["rm"])
    finally:
        fresh.close()


def _run_1b(conn, fresh, monkeypatch, work_order_id, rm_id, expected_message):
    _transfer_ok(conn, {"rm": rm_id}, work_order_id, "6")
    before = (_table_count(conn, "stock_ledger_entry"),
              _table_count(conn, "gl_entry"))

    def another_transfer_while_waiting(c):
        c.execute(
            "UPDATE work_order_item SET transferred_qty = CAST("
            "(CAST(transferred_qty AS NUMERIC) + CAST(? AS NUMERIC)) AS TEXT)"
            " WHERE work_order_id = ? AND item_id = ?",
            ("4", work_order_id, rm_id))

    _wrap_take_heads(monkeypatch, another_transfer_while_waiting)
    sle_calls = _count_sle_calls(monkeypatch)
    r = call_action(M.transfer_materials, conn, ns(
        work_order_id=work_order_id, posting_date=TRANSFER_DATE,
        items=json.dumps([{"item_id": rm_id, "qty": "4"}])))
    assert r == {"status": "error", "message": expected_message}
    assert sle_calls["n"] == 0
    second_voucher = work_order_id + ":transfer:2"
    assert list(_sle_for(fresh, second_voucher)) == []
    assert list(_gle_for(fresh, second_voucher)) == []
    assert (_table_count(fresh, "stock_ledger_entry"),
            _table_count(fresh, "gl_entry")) == before


def test_1b_stale_transferred_qty_refused_before_any_write(
        conn, db_path, monkeypatch):
    env = _build_env(conn, True)
    wo_id = _new_work_order(conn, env)
    _start(conn, wo_id)
    fresh = get_conn(db_path)
    try:
        _run_1b(conn, fresh, monkeypatch, wo_id, env["rm"],
                EXCEED_SQLITE_4_AFTER_10)
    finally:
        fresh.close()


def test_1b_stale_transferred_qty_refused_before_any_write_pg(
        pg_conn, monkeypatch):
    env = _build_env(pg_conn, False)
    wo_id = _new_work_order(pg_conn, env)
    _start(pg_conn, wo_id)
    fresh = get_connection()
    try:
        _run_1b(pg_conn, fresh, monkeypatch, wo_id, env["rm"],
                EXCEED_PG_4_AFTER_10)
    finally:
        fresh.close()


def test_2_head_is_the_first_write(conn):
    env = _build_env(conn, True)
    wo_id = _new_work_order(conn, env)
    _start(conn, wo_id)
    proxy = _RecordingProxy(conn)
    r = call_action(M.transfer_materials, proxy, ns(
        work_order_id=wo_id, posting_date=TRANSFER_DATE,
        items=json.dumps([{"item_id": env["rm"], "qty": "4"}])))
    assert is_ok(r), r
    writes = [s for s in proxy.statements
              if s.lstrip()[:6].upper() in ("INSERT", "UPDATE", "DELETE")]
    assert writes, "expected the transfer to write"
    head_positions = [i for i, s in enumerate(writes)
                      if "gl_chain_head" in s]
    assert head_positions and head_positions[0] == 0, writes[:5]
    assert writes[0].lstrip().upper().startswith("INSERT"), writes[0]
    for i, s in enumerate(writes):
        if ("stock_ledger_entry" in s or "stock_fifo_layer" in s
                or "work_order" in s):
            assert i > head_positions[0], s


def test_3_happy_path_unchanged(conn):
    env = _build_env(conn, True)
    wo_id = _new_work_order(conn, env)
    _start(conn, wo_id)
    r = _transfer(conn, env, wo_id, "10")
    assert is_ok(r), r
    assert r["items_transferred"] == 1
    assert r["sle_count"] == 2
    assert r["gl_count"] == 2
    sle = [dict(x) for x in conn.execute(
        "SELECT warehouse_id, actual_qty, incoming_rate, "
        "stock_value_difference, posting_date, is_cancelled "
        "FROM stock_ledger_entry "
        "WHERE voucher_type = 'work_order' AND voucher_id = ?",
        (wo_id,)).fetchall()]
    assert len(sle) == 2
    by_wh = {x["warehouse_id"]: x for x in sle}
    out = by_wh[env["raw_wh"]]
    assert out["actual_qty"] == "-10.00"
    assert out["incoming_rate"] == "0.00"
    assert out["stock_value_difference"] == "-50.00"
    assert out["posting_date"] == TRANSFER_DATE
    assert out["is_cancelled"] == 0
    inn = by_wh[env["wip_wh"]]
    assert inn["actual_qty"] == "10.00"
    assert inn["incoming_rate"] == "5.00"
    assert inn["stock_value_difference"] == "50.00"
    gl = [dict(x) for x in conn.execute(
        "SELECT account_id, debit, credit FROM gl_entry "
        "WHERE voucher_type = 'work_order' AND voucher_id = ?",
        (wo_id,)).fetchall()]
    legs = sorted((x["account_id"], x["debit"], x["credit"]) for x in gl)
    assert legs == sorted([(env["raw_acct"], "0.00", "50.00"),
                           (env["wip_acct"], "50.00", "0.00")])
    assert (sum(Decimal(x["debit"]) for x in gl)
            == sum(Decimal(x["credit"]) for x in gl))
    items = {x["item_id"]: dict(x) for x in conn.execute(
        "SELECT item_id, required_qty, transferred_qty FROM work_order_item "
        "WHERE work_order_id = ?", (wo_id,)).fetchall()}
    assert items[env["rm"]]["required_qty"] == "10.00"
    assert items[env["rm"]]["transferred_qty"] == "10"
    header = dict(conn.execute(
        "SELECT status, material_transferred_for_manufacturing FROM work_order "
        "WHERE id = ?", (wo_id,)).fetchone())
    assert header["status"] == "in_process"
    assert header["material_transferred_for_manufacturing"] == "10.00"


def _wait_for_blocked_backend(conn, proc, timeout_s=30.0):
    """Wait until the transfer subprocess is stuck on the held head.

    PostgreSQL-only. The holder sits idle in its transaction holding this
    company's chain head, and this connection is idle itself, so the
    transfer subprocess is the only backend here that can be waiting: its
    head-take upsert cannot proceed while the holder's row version is
    uncommitted. ``pg_locks`` reflects the lock table directly (unlike
    ``pg_stat_activity``, which goes through the stats collector), so the
    wait is visible no matter how slow interpreter startup is. On
    stock-first code the same wait appears after the FIFO row lock is
    taken, which is exactly what the FIFO probe then trips over.
    """
    my_db = conn.execute(
        "SELECT oid AS o FROM pg_database "
        "WHERE datname = current_database()").fetchone()["o"]
    deadline = time.monotonic() + timeout_s
    while True:
        if proc.poll() is not None:
            out, err = proc.communicate()
            pytest.fail(
                "the transfer exited while waiting for it to block "
                "(rc=%s out=%s err=%s)"
                % (proc.returncode, out, err))
        waiting = conn.execute(
            "SELECT COUNT(*) AS c FROM pg_locks "
            "WHERE NOT granted AND pid <> pg_backend_pid() "
            "AND (database = ? OR database IS NULL)",
            (my_db,)).fetchone()["c"]
        if waiting and int(waiting) >= 1:
            time.sleep(0.3)
            again = conn.execute(
                "SELECT COUNT(*) AS c FROM pg_locks "
                "WHERE NOT granted AND pid <> pg_backend_pid() "
                "AND (database = ? OR database IS NULL)",
                (my_db,)).fetchone()["c"]
            if again and int(again) >= 1:
                return
        if time.monotonic() > deadline:
            pytest.fail("the transfer never blocked on a lock")
        time.sleep(0.2)


def _transfer_subprocess(work_order_id, item_id, qty, timeout_s=None):
    penv = _proc_env()
    if timeout_s is not None:
        penv = _proc_env(ERPCLAW_PG_LOCK_TIMEOUT=timeout_s)
    return subprocess.Popen(
        [sys.executable, _MFG_SCRIPT,
         "--action", "transfer-materials",
         "--work-order-id", work_order_id,
         "--items", json.dumps([{"item_id": item_id, "qty": qty}]),
         "--posting-date", TRANSFER_DATE],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=penv)


def test_4_head_first_fifo_untouched_while_waiting(pg_conn):
    _pg_only()
    env = _build_env(pg_conn, False)
    wo_id = _new_work_order(pg_conn, env)
    _start(pg_conn, wo_id)
    pg_conn.execute("UPDATE item SET valuation_method = 'fifo' WHERE id = ?",
                    (env["rm"],))
    pg_conn.execute(
        "INSERT INTO stock_fifo_layer (id, item_id, warehouse_id, "
        "posting_date, qty, rate, remaining_qty, source_voucher_type, "
        "source_voucher_id, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (str(uuid.uuid4()), env["rm"], env["raw_wh"], "2026-01-05", "20",
         "5.00", "20", "stock_entry", "opening-" + env["company_id"][:8],
         "2026-01-05 00:00:00"))
    pg_conn.commit()
    holder = get_connection()
    try:
        _real_take_heads(holder, [env["company_id"]])
        proc = _transfer_subprocess(wo_id, env["rm"], "4", timeout_s="10s")
        try:
            try:
                proc.wait(timeout=1.0)
                pytest.fail(
                    "transfer-materials should block on the held head "
                    "(rc=%s)" % proc.returncode)
            except subprocess.TimeoutExpired:
                assert proc.poll() is None, \
                    "transfer-materials must still be alive"
            _wait_for_blocked_backend(pg_conn, proc)
            assert proc.poll() is None, \
                "transfer-materials must still be blocked"
            probe = get_connection()
            try:
                probe.execute("SET lock_timeout = '1s'")
                probe.execute(
                    "UPDATE stock_fifo_layer SET remaining_qty = "
                    "remaining_qty WHERE item_id = ? AND warehouse_id = ?",
                    (env["rm"], env["raw_wh"]))
                probe.rollback()
            finally:
                probe.close()
            holder.rollback()
            out, err = proc.communicate(timeout=12)
            assert proc.returncode == 0, (out, err)
            assert json.loads(out)["status"] == "ok", (out, err)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
    finally:
        try:
            holder.rollback()
        except Exception:  # noqa: BLE001, S110 - best effort
            pass
        holder.close()


def test_5_concurrent_transfers_keep_the_requirement(pg_conn):
    _pg_only()
    for _round in range(5):
        for _attempt in range(5):
            env = _build_env(pg_conn, False)
            wo_id = _new_work_order(pg_conn, env)
            _start(pg_conn, wo_id)
            first = _transfer_subprocess(wo_id, env["rm"], "6")
            started = time.monotonic()
            second = _transfer_subprocess(wo_id, env["rm"], "6")
            gap_ms = (time.monotonic() - started) * 1000
            if gap_ms < 50:
                break
            for _stray in (first, second):
                if _stray.poll() is None:
                    _stray.kill()
                _stray.wait()
        else:
            pytest.fail("could not start the two transfers within 50 ms")
        try:
            out1, err1 = first.communicate(timeout=15)
            out2, err2 = second.communicate(timeout=15)
        finally:
            for proc in (first, second):
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
        combined = out1 + err1 + out2 + err2
        assert "deadlock" not in combined, combined
        results = sorted(
            [(first.returncode, out1, err1),
             (second.returncode, out2, err2)])
        assert results[0][0] == 0, (out1, err1, out2, err2)
        assert results[1][0] == 1, (out1, err1, out2, err2)
        assert json.loads(results[0][1])["status"] == "ok", results[0][1]
        assert json.loads(results[1][1]) == {
            "status": "error", "message": EXCEED_PG_6_AFTER_6}, results[1]
        items = {x["item_id"]: dict(x) for x in pg_conn.execute(
            "SELECT item_id, required_qty, transferred_qty FROM work_order_item "
            "WHERE work_order_id = ?", (wo_id,)).fetchall()}
        assert Decimal(items[env["rm"]]["transferred_qty"]) == Decimal("6")
        header = dict(pg_conn.execute(
            "SELECT material_transferred_for_manufacturing FROM work_order "
            "WHERE id = ?", (wo_id,)).fetchone())
        assert (Decimal(header["material_transferred_for_manufacturing"])
                == Decimal("6"))
        assert len(_sle_for(pg_conn, wo_id)) == 2
        assert list(_sle_for(pg_conn, wo_id + ":transfer:2")) == []
        assert list(_gle_for(pg_conn, wo_id + ":transfer:2")) == []
        _assert_chain_intact(pg_conn, env["company_id"])
        _assert_contiguous(pg_conn, env["company_id"])


def test_6_cancelled_mid_flight_is_refused(pg_conn, monkeypatch):
    _pg_only()
    env = _build_env(pg_conn, False)
    wo_id = _new_work_order(pg_conn, env)
    _start(pg_conn, wo_id)
    real_sle = M.insert_sle_entries

    def cancel_after_stock(conn, entries, *args, **kwargs):
        out = real_sle(conn, entries, *args, **kwargs)
        other = get_connection()
        try:
            other.execute(
                "UPDATE work_order SET status = 'cancelled' WHERE id = ?",
                (wo_id,))
            other.commit()
        finally:
            other.close()
        return out

    monkeypatch.setattr(M, "insert_sle_entries", cancel_after_stock)
    r = _transfer(pg_conn, env, wo_id, "4")
    assert r == {"status": "error",
                 "message": STATUS_MESSAGE % "cancelled"}
    fresh = get_connection()
    try:
        assert list(_sle_for(fresh, wo_id)) == []
        assert list(_gle_for(fresh, wo_id)) == []
    finally:
        fresh.close()


def test_7_gl_refusal_rolls_back_the_stock_writes(conn, monkeypatch):
    env = _build_env(conn, True)
    wo_id = _new_work_order(conn, env)
    _start(conn, wo_id)

    def planted(conn, *args, **kwargs):
        raise ValueError("planted")

    monkeypatch.setattr(M, "insert_gl_entries", planted)
    r = _transfer(conn, env, wo_id, "4")
    assert r == {"status": "error", "message": "GL posting failed: planted"}
    assert list(_sle_for(conn, wo_id)) == []


# ---------------------------------------------------------------------------
# Completion legs (m831b): complete-work-order takes the chain head first.
#
# A work order for 10, started, with 10 transferred. Every completion below
# produces 4 unless stated otherwise. SQLite legs run on the manufacturing
# ``conn`` fixture; PostgreSQL legs run on the module-local ``pg_conn``
# fixture. Money is exact text; no float is used anywhere.
# ---------------------------------------------------------------------------

COMPLETION_DATE = "2026-06-15"

COMPLETE_STATUS_MESSAGE = ("Cannot complete Work Order with status "
                           "'%s'. Must be 'in_process'.")


def _complete(conn, work_order_id, qty, posting_date=COMPLETION_DATE):
    return call_action(M.complete_work_order, conn, ns(
        work_order_id=work_order_id, produced_qty=qty,
        posting_date=posting_date))


def _complete_ok(conn, work_order_id, qty, posting_date=COMPLETION_DATE):
    r = _complete(conn, work_order_id, qty, posting_date)
    assert is_ok(r), r
    return r


def _completion_env_started_with_stock(conn, with_naming_series):
    """One finished good, one raw material at 5.00, BOM ratio 1.

    Work order for 10, started, with the full 10 transferred to WIP.
    """
    env = _build_env(conn, with_naming_series)
    wo_id = _new_work_order(conn, env)
    _start(conn, wo_id)
    _transfer_ok(conn, env, wo_id, "10")
    return env, wo_id


def _run_c1(conn, fresh, monkeypatch, work_order_id):
    comp = work_order_id + ":completion"
    before = (_table_count(conn, "stock_ledger_entry"),
              _table_count(conn, "gl_entry"))

    def complete_while_waiting(c):
        c.execute("UPDATE work_order SET status = 'completed' WHERE id = ?",
                  (work_order_id,))

    _wrap_take_heads(monkeypatch, complete_while_waiting)
    sle_calls = _count_sle_calls(monkeypatch)
    r = _complete(conn, work_order_id, "4")
    assert r == {"status": "error",
                 "message": COMPLETE_STATUS_MESSAGE % "completed"}
    assert sle_calls["n"] == 0
    assert list(_sle_for(fresh, comp)) == []
    assert list(_gle_for(fresh, comp)) == []
    assert (_table_count(fresh, "stock_ledger_entry"),
            _table_count(fresh, "gl_entry")) == before


def test_c1_stale_completed_status_refused_before_any_write(
        conn, db_path, monkeypatch):
    env, wo_id = _completion_env_started_with_stock(conn, True)
    fresh = get_conn(db_path)
    try:
        _run_c1(conn, fresh, monkeypatch, wo_id)
    finally:
        fresh.close()


def test_c1_stale_completed_status_refused_before_any_write_pg(
        pg_conn, monkeypatch):
    env, wo_id = _completion_env_started_with_stock(pg_conn, False)
    fresh = get_connection()
    try:
        _run_c1(pg_conn, fresh, monkeypatch, wo_id)
    finally:
        fresh.close()


def test_c2_head_is_the_first_write(conn):
    env, wo_id = _completion_env_started_with_stock(conn, True)
    proxy = _RecordingProxy(conn)
    r = _complete(proxy, wo_id, "4")
    assert is_ok(r), r
    writes = [s for s in proxy.statements
              if s.lstrip()[:6].upper() in ("INSERT", "UPDATE", "DELETE")]
    assert writes, "expected the completion to write"
    head_positions = [i for i, s in enumerate(writes)
                      if "gl_chain_head" in s]
    assert head_positions and head_positions[0] == 0, writes[:5]
    assert writes[0].lstrip().upper().startswith("INSERT"), writes[0]
    for i, s in enumerate(writes):
        if ("stock_ledger_entry" in s or "stock_fifo_layer" in s
                or "work_order" in s):
            assert i > head_positions[0], s


def test_c3_happy_path_unchanged(conn):
    env, wo_id = _completion_env_started_with_stock(conn, True)
    r = _complete(conn, wo_id, "10")
    assert is_ok(r), r
    assert r["rm_cost"] == "50.00"
    assert r["operating_cost"] == "0.00"
    assert r["production_cost"] == "50.00"
    assert r["fg_rate"] == "5.00"
    assert r["produced_qty"] == "10.00"
    assert r["document_status"] == "completed"
    assert r["sle_count"] == 2
    assert r["gl_count"] == 2
    assert r["by_product_count"] == 0
    comp = wo_id + ":completion"
    sle = [dict(x) for x in conn.execute(
        "SELECT warehouse_id, actual_qty, incoming_rate, "
        "stock_value_difference, posting_date, is_cancelled "
        "FROM stock_ledger_entry "
        "WHERE voucher_type = 'work_order' AND voucher_id = ?",
        (comp,)).fetchall()]
    assert len(sle) == 2
    by_wh = {x["warehouse_id"]: x for x in sle}
    out = by_wh[env["wip_wh"]]
    assert out["actual_qty"] == "-10.00"
    assert out["incoming_rate"] == "0.00"
    assert out["stock_value_difference"] == "-50.00"
    assert out["posting_date"] == COMPLETION_DATE
    assert out["is_cancelled"] == 0
    inn = by_wh[env["fg_wh"]]
    assert inn["actual_qty"] == "10.00"
    assert inn["incoming_rate"] == "5.00"
    assert inn["stock_value_difference"] == "50.00"
    assert inn["posting_date"] == COMPLETION_DATE
    assert inn["is_cancelled"] == 0
    gl = [dict(x) for x in conn.execute(
        "SELECT account_id, debit, credit FROM gl_entry "
        "WHERE voucher_type = 'work_order' AND voucher_id = ?",
        (comp,)).fetchall()]
    legs = sorted((x["account_id"], x["debit"], x["credit"]) for x in gl)
    assert legs == sorted([(env["fg_acct"], "50.00", "0.00"),
                           (env["wip_acct"], "0.00", "50.00")])
    assert (sum(Decimal(x["debit"]) for x in gl)
            == sum(Decimal(x["credit"]) for x in gl))
    header = dict(conn.execute(
        "SELECT status, produced_qty FROM work_order WHERE id = ?",
        (wo_id,)).fetchone())
    assert header["status"] == "completed"
    assert Decimal(header["produced_qty"]) == Decimal("10")
    items = {x["item_id"]: dict(x) for x in conn.execute(
        "SELECT item_id, required_qty, transferred_qty, consumed_qty "
        "FROM work_order_item WHERE work_order_id = ?", (wo_id,)).fetchall()}
    assert Decimal(items[env["rm"]]["consumed_qty"]) == Decimal("10")


def _completion_subprocess(work_order_id, qty, timeout_s=None):
    penv = _proc_env()
    if timeout_s is not None:
        penv = _proc_env(ERPCLAW_PG_LOCK_TIMEOUT=timeout_s)
    return subprocess.Popen(
        [sys.executable, _MFG_SCRIPT,
         "--action", "complete-work-order",
         "--work-order-id", work_order_id,
         "--produced-qty", qty,
         "--posting-date", COMPLETION_DATE],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=penv)


def test_c4_concurrent_completions_serialise(pg_conn):
    _pg_only()
    for _round in range(5):
        for _attempt in range(5):
            env, wo_id = _completion_env_started_with_stock(pg_conn, False)
            first = _completion_subprocess(wo_id, "4")
            started = time.monotonic()
            second = _completion_subprocess(wo_id, "4")
            gap_ms = (time.monotonic() - started) * 1000
            if gap_ms < 50:
                break
            for _stray in (first, second):
                if _stray.poll() is None:
                    _stray.kill()
                _stray.wait()
        else:
            pytest.fail("could not start the two completions within 50 ms")
        try:
            out1, err1 = first.communicate(timeout=15)
            out2, err2 = second.communicate(timeout=15)
        finally:
            for proc in (first, second):
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
        combined = out1 + err1 + out2 + err2
        assert "deadlock" not in combined, combined
        assert first.returncode == 0, (out1, err1)
        assert second.returncode == 0, (out2, err2)
        assert json.loads(out1)["status"] == "ok", (out1, err1)
        assert json.loads(out2)["status"] == "ok", (out2, err2)
        header = dict(pg_conn.execute(
            "SELECT produced_qty FROM work_order WHERE id = ?",
            (wo_id,)).fetchone())
        assert Decimal(header["produced_qty"]) == Decimal("8")
        vouchers = sorted(r["voucher_id"] for r in pg_conn.execute(
            "SELECT DISTINCT voucher_id AS voucher_id "
            "FROM stock_ledger_entry "
            "WHERE voucher_type = 'work_order' AND voucher_id LIKE ?",
            (wo_id + ":completion%",)).fetchall())
        assert vouchers == [wo_id + ":completion",
                            wo_id + ":completion:2"], vouchers
        for voucher_id in vouchers:
            assert len(_sle_for(pg_conn, voucher_id)) > 0, voucher_id
        _assert_chain_intact(pg_conn, env["company_id"])
        _assert_contiguous(pg_conn, env["company_id"])


def test_c5_completed_mid_flight_is_refused(pg_conn, monkeypatch):
    _pg_only()
    env, wo_id = _completion_env_started_with_stock(pg_conn, False)
    comp = wo_id + ":completion"
    real_sle = M.insert_sle_entries

    def complete_after_stock(conn, entries, *args, **kwargs):
        out = real_sle(conn, entries, *args, **kwargs)
        other = get_connection()
        try:
            other.execute(
                "UPDATE work_order SET status = 'completed' WHERE id = ?",
                (wo_id,))
            other.commit()
        finally:
            other.close()
        return out

    monkeypatch.setattr(M, "insert_sle_entries", complete_after_stock)
    r = _complete(pg_conn, wo_id, "4")
    assert r == {"status": "error",
                 "message": COMPLETE_STATUS_MESSAGE % "completed"}
    fresh = get_connection()
    try:
        assert list(_sle_for(fresh, comp)) == []
        assert list(_gle_for(fresh, comp)) == []
    finally:
        fresh.close()


def test_c6_gl_refusal_rolls_back_the_stock_writes(conn, monkeypatch):
    env, wo_id = _completion_env_started_with_stock(conn, True)
    comp = wo_id + ":completion"
    before_produced = conn.execute(
        "SELECT produced_qty FROM work_order WHERE id = ?",
        (wo_id,)).fetchone()["produced_qty"]

    def planted(conn, *args, **kwargs):
        raise ValueError("planted")

    monkeypatch.setattr(M, "insert_gl_entries", planted)
    r = _complete(conn, wo_id, "4")
    assert r == {"status": "error", "message": "GL posting failed: planted"}
    assert list(_sle_for(conn, comp)) == []
    after_produced = conn.execute(
        "SELECT produced_qty FROM work_order WHERE id = ?",
        (wo_id,)).fetchone()["produced_qty"]
    assert after_produced == before_produced
