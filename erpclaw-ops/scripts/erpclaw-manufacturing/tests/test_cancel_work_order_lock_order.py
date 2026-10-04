"""Cancelling a work order takes the company's ledger chain head first (m847).

``cancel-work-order`` reverses every stock and ledger posting the order made
(each material transfer and each completion) and marks the order cancelled as
one decision under the company's ledger lock: the chain head is taken before
any write, the order is re-read under the head, reversals are decided by
active-row counts (never by swallowing an exception), and the final status
write is a compare-and-set. A cancel must never reverse the stock of a
posting while leaving its ledger rows standing.

Scenario for the SQLite legs (all dates fixed, moving-average valuation):
  - Raw Material: 100 opening at 5.00 in the raw store.
  - BOM: 1 finished good consumes 1 of RM.
  - Work order for 10, started; materials transferred on 2026-06-05
    (RM 10 = 50.00, voucher id = work order id).
  - Partial completion of 4 on 2026-06-15 (voucher id = "<wo id>:completion"):
    FG +4 at 5.00 = 20.00 into the FG store, RM -4 out of WIP.
  - Cancel on 2026-06-20.

Each warehouse is linked to its own stock account, so the stock legs can be
pinned by account. SQLite legs run on the manufacturing ``conn`` fixture.
PostgreSQL legs run on the module-local ``pg_conn`` fixture when
``ERPCLAW_DB_DIALECT=postgresql`` and the house expendable-target guard
accept the target. Money is exact text; no float is used anywhere.
"""
import importlib.util
import json
import os
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from decimal import Decimal
from urllib.parse import urlparse

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest

from mfg_helpers import (call_action, get_conn, is_ok, load_db_query, ns,  # noqa: E402
                         seed_account, seed_company, seed_item,
                         seed_naming_series)

M = load_db_query()

from erpclaw_lib.db import get_connection, get_dialect  # noqa: E402
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
    "cancel_lockorder_chain_proofs", "test_chain_lock_proofs.py")
_pg_only = _proofs._pg_only
_proc_env = _proofs._proc_env
_recmod = _load_pay_module(
    "cancel_lockorder_recording",
    "test_payment_edit_and_allocation_compare_and_set.py")
_RecordingProxy = _recmod._RecordingProxy
_pg_helpers = _load_pay_module("cancel_payments_helpers_pg", "payments_helpers.py")

TRANSFER_DATE = "2026-06-05"
COMPLETION_DATE = "2026-06-15"
CANCEL_DATE = "2026-06-20"

CANCEL_STATUS_MESSAGE = (
    "Cannot cancel Work Order with status '%s'. "
    "Completed and cancelled work orders cannot be cancelled.")
TRANSFER_STATUS_MESSAGE = (
    "Cannot transfer materials for Work Order with status 'cancelled'. "
    "Must be 'not_started' or 'in_process'.")
COMPLETE_STATUS_MESSAGE = (
    "Cannot complete Work Order with status 'cancelled'. "
    "Must be 'in_process'.")
HALF_MESSAGE = "Cannot cancel Work Order: planted. Nothing was changed."


@pytest.fixture
def pg_conn():
    """PostgreSQL connection on a freshly reset shared schema.

    Mirrors the environment steps of ``erpclaw-payments/tests/conftest.py``
    ``db_path``: ``ERPCLAW_DB_URL`` points at ``ERPCLAW_PG_TEST_URL``,
    ``ERPCLAW_DB_PATH`` is popped, the core schema (which already contains
    the manufacturing tables) is rebuilt through the house helper so the
    expendable-target guard checks the target, and both variables are
    restored afterwards. Skips outside the PostgreSQL lane.
    """
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


def _assert_pg_target(conn):
    """The PostgreSQL legs only run against the expendable test database."""
    assert get_dialect() == "postgresql"
    expected = urlparse(os.environ["ERPCLAW_PG_TEST_URL"]).path.strip("/")
    actual = conn.execute("SELECT current_database()").fetchone()[0]
    assert actual == expected, (actual, expected)


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


def _zero_env(conn, with_naming_series):
    """Same as the standard env but the order consumes a zero-rate material.

    The transfer and the completion post stock rows valued at zero, so both
    vouchers carry stock rows and no ledger rows.
    """
    env = _build_env(conn, with_naming_series)
    rm0 = seed_item(conn, env["company_id"], name="Zero RM",
                    standard_rate="0")
    M.insert_sle_entries(
        conn,
        [{"item_id": rm0, "warehouse_id": env["raw_wh"],
          "actual_qty": "20", "incoming_rate": "0"}],
        voucher_type="stock_entry",
        voucher_id="opening-zero-" + env["company_id"][:8],
        posting_date="2026-01-05", company_id=env["company_id"])
    conn.commit()
    r = call_action(M.add_bom, conn, ns(
        item_id=env["fg_item"], company_id=env["company_id"], quantity="1",
        items=json.dumps([{"item_id": rm0, "quantity": "1", "rate": "0"}])))
    assert is_ok(r), r
    env["rm0"] = rm0
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


def _transfer_ok(conn, env, work_order_id, qty):
    r = call_action(M.transfer_materials, conn, ns(
        work_order_id=work_order_id, posting_date=TRANSFER_DATE,
        items=json.dumps([{"item_id": env["rm"], "qty": qty}])))
    assert is_ok(r), r
    return r


def _complete_ok(conn, work_order_id, qty):
    r = call_action(M.complete_work_order, conn, ns(
        work_order_id=work_order_id, produced_qty=qty,
        posting_date=COMPLETION_DATE))
    assert is_ok(r), r
    return r


def _cancel(conn, work_order_id, posting_date=CANCEL_DATE):
    return call_action(M.cancel_work_order, conn, ns(
        work_order_id=work_order_id, posting_date=posting_date))


def _setup_transferred_and_partly_completed(conn, with_naming_series):
    """Standard scenario: transfer 10 at 5.00, partial completion of 4."""
    env = _build_env(conn, with_naming_series)
    wo_id = _new_work_order(conn, env)
    _start(conn, wo_id)
    _transfer_ok(conn, env, wo_id, "10")
    done = _complete_ok(conn, wo_id, "4")
    assert done["production_cost"] == "20.00", done
    assert done["fg_rate"] == "5.00", done
    return env, wo_id


def _gl_rows(conn, voucher_id):
    return [dict(r) for r in conn.execute(
        "SELECT id, account_id, debit, credit, posting_date, is_cancelled, "
        "remarks FROM gl_entry "
        "WHERE voucher_type = 'work_order' AND voucher_id = ?",
        (voucher_id,)).fetchall()]


def _sle_rows(conn, voucher_id):
    return [dict(r) for r in conn.execute(
        "SELECT id, item_id, warehouse_id, actual_qty, "
        "stock_value_difference, posting_date, is_cancelled "
        "FROM stock_ledger_entry "
        "WHERE voucher_type = 'work_order' AND voucher_id = ?",
        (voucher_id,)).fetchall()]


def _status(conn, work_order_id):
    return conn.execute("SELECT status FROM work_order WHERE id = ?",
                        (work_order_id,)).fetchone()["status"]


def _audit_count(conn, work_order_id):
    return conn.execute(
        "SELECT COUNT(*) FROM audit_log "
        "WHERE entity_type = 'work_order' AND entity_id = ?",
        (work_order_id,)).fetchone()[0]


def _snapshot_rows(conn, sql, params=()):
    return sorted(
        tuple((key, None if value is None else str(value))
              for key, value in sorted(dict(row).items()))
        for row in conn.execute(sql, params).fetchall())


def _full_snapshot(conn, work_order_id):
    return {
        "sle": _snapshot_rows(conn, "SELECT * FROM stock_ledger_entry"),
        "fifo": _snapshot_rows(conn, "SELECT * FROM stock_fifo_layer"),
        "gl": _snapshot_rows(conn, "SELECT * FROM gl_entry"),
        "wo": _snapshot_rows(
            conn, "SELECT * FROM work_order WHERE id = ?", (work_order_id,)),
        "jc": _snapshot_rows(
            conn, "SELECT * FROM job_card WHERE work_order_id = ?",
            (work_order_id,)),
    }


def _add_open_job_card(conn, work_order_id):
    r = call_action(M.add_operation, conn, ns(name="Assembly " + work_order_id[:8]))
    assert is_ok(r), r
    r = call_action(M.create_job_card, conn, ns(
        work_order_id=work_order_id, operation_id=r["operation_id"]))
    assert is_ok(r), r
    return r["job_card_id"]


def _family_voucher_ids(conn, work_order_id):
    prefix = work_order_id + ":"
    rows = conn.execute(
        "SELECT DISTINCT voucher_id FROM stock_ledger_entry "
        "WHERE voucher_type = 'work_order' AND voucher_id LIKE ?",
        (work_order_id + "%",)).fetchall()
    return sorted(r["voucher_id"] for r in rows
                  if r["voucher_id"] == work_order_id
                  or r["voucher_id"].startswith(prefix))


def _assert_family_nets_zero(conn, work_order_id):
    """Every family voucher nets to zero per account and per item/warehouse.

    An empty family nets to zero vacuously (a refused transfer posts
    nothing); callers assert the expected family contents separately.
    """
    vouchers = _family_voucher_ids(conn, work_order_id)
    debit, credit = defaultdict(Decimal), defaultdict(Decimal)
    for voucher_id in vouchers:
        for row in _gl_rows(conn, voucher_id):
            debit[row["account_id"]] += Decimal(str(row["debit"]))
            credit[row["account_id"]] += Decimal(str(row["credit"]))
    for account_id in set(debit) | set(credit):
        assert debit[account_id] == credit[account_id], account_id
    qty, value = defaultdict(Decimal), defaultdict(Decimal)
    for voucher_id in vouchers:
        for row in _sle_rows(conn, voucher_id):
            key = (row["item_id"], row["warehouse_id"])
            qty[key] += Decimal(str(row["actual_qty"]))
            value[key] += Decimal(str(row["stock_value_difference"]))
    for key in set(qty) | set(value):
        assert qty[key] == 0, key
        assert value[key] == 0, key


def test_1_head_before_any_write_and_reread_before_reversal(conn):
    env, wo_id = _setup_transferred_and_partly_completed(conn, True)
    proxy = _RecordingProxy(conn)
    r = _cancel(proxy, wo_id)
    assert is_ok(r), r
    writes = [s for s in proxy.statements
              if s.lstrip()[:6].upper() in ("INSERT", "UPDATE", "DELETE")]
    assert writes, "expected the cancel to write"
    head_positions = [i for i, s in enumerate(writes)
                      if "gl_chain_head" in s]
    assert head_positions and head_positions[0] == 0, writes[:5]
    assert writes[0].lstrip().upper().startswith("INSERT"), writes[0]
    for i, s in enumerate(writes):
        if ("stock_ledger_entry" in s or "stock_fifo_layer" in s
                or "gl_entry" in s or "work_order" in s):
            assert i > head_positions[0], s
    head_stmt = next(
        i for i, s in enumerate(proxy.statements) if s == writes[0])
    rereads = [
        i for i, s in enumerate(proxy.statements)
        if i > head_stmt and '"work_order"' in s
        and "work_order_item" not in s
        and s.lstrip().upper().startswith("SELECT")]
    assert rereads, "expected the work order re-read under the head"
    first_reversal = next(
        i for i, s in enumerate(proxy.statements)
        if i > head_stmt
        and s.lstrip()[:6].upper() in ("INSERT", "UPDATE", "DELETE")
        and ("stock_ledger_entry" in s or "stock_fifo_layer" in s
             or "gl_entry" in s))
    assert rereads[0] < first_reversal, (
        rereads[0], first_reversal,
        proxy.statements[first_reversal][:120])


def test_2_exact_reversal_amounts(conn):
    env, wo_id = _setup_transferred_and_partly_completed(conn, True)
    comp_id = wo_id + ":completion"
    assert _status(conn, wo_id) == "in_process"
    r = _cancel(conn, wo_id)
    assert is_ok(r), r
    assert _status(conn, wo_id) == "cancelled"

    transfer_gl = _gl_rows(conn, wo_id)
    assert len(transfer_gl) == 4
    assert {row["is_cancelled"] for row in transfer_gl} == {1}
    assert {row["posting_date"] for row in transfer_gl
            if row["remarks"].startswith("Reversal of ")} == {CANCEL_DATE}
    transfer_reversal_legs = sorted(
        (row["account_id"], row["debit"], row["credit"])
        for row in transfer_gl
        if row["remarks"].startswith("Reversal of "))
    assert transfer_reversal_legs == sorted([
        (env["raw_acct"], "50.00", "0.00"),
        (env["wip_acct"], "0.00", "50.00"),
    ])
    transfer_by_account = defaultdict(lambda: [Decimal("0"), Decimal("0")])
    for row in transfer_gl:
        transfer_by_account[row["account_id"]][0] += Decimal(row["debit"])
        transfer_by_account[row["account_id"]][1] += Decimal(row["credit"])
    assert dict(transfer_by_account) == {
        env["raw_acct"]: [Decimal("50.00"), Decimal("50.00")],
        env["wip_acct"]: [Decimal("50.00"), Decimal("50.00")],
    }

    transfer_sle = _sle_rows(conn, wo_id)
    assert len(transfer_sle) == 4
    assert {row["is_cancelled"] for row in transfer_sle} == {1}
    assert sorted(row["stock_value_difference"]
                  for row in transfer_sle) == \
        ["-50.00", "-50.00", "50.00", "50.00"]
    transfer_qty = defaultdict(Decimal)
    transfer_value = defaultdict(Decimal)
    for row in transfer_sle:
        key = (row["item_id"], row["warehouse_id"])
        transfer_qty[key] += Decimal(row["actual_qty"])
        transfer_value[key] += Decimal(row["stock_value_difference"])
    for key in transfer_qty:
        assert transfer_qty[key] == 0, key
        assert transfer_value[key] == Decimal("0.00"), key

    completion_gl = _gl_rows(conn, comp_id)
    assert len(completion_gl) == 4
    assert {row["is_cancelled"] for row in completion_gl} == {1}
    completion_reversal_legs = sorted(
        (row["account_id"], row["debit"], row["credit"])
        for row in completion_gl
        if row["remarks"].startswith("Reversal of "))
    assert completion_reversal_legs == sorted([
        (env["fg_acct"], "0.00", "20.00"),
        (env["wip_acct"], "20.00", "0.00"),
    ])

    completion_sle = _sle_rows(conn, comp_id)
    assert len(completion_sle) == 4
    assert {row["is_cancelled"] for row in completion_sle} == {1}
    assert sorted(row["stock_value_difference"]
                  for row in completion_sle) == \
        ["-20.00", "-20.00", "20.00", "20.00"]
    _assert_family_nets_zero(conn, wo_id)


def test_3_second_cancel_leaves_everything_unchanged(conn):
    env, wo_id = _setup_transferred_and_partly_completed(conn, True)
    assert is_ok(_cancel(conn, wo_id)), wo_id
    assert _status(conn, wo_id) == "cancelled"
    before = _full_snapshot(conn, wo_id)
    r = _cancel(conn, wo_id, posting_date="2026-06-25")
    assert r == {"status": "error",
                 "message": CANCEL_STATUS_MESSAGE % "cancelled"}
    assert _full_snapshot(conn, wo_id) == before
    assert _status(conn, wo_id) == "cancelled"


def _run_4_stale_status(conn, fresh, monkeypatch, work_order_id):
    comp_id = work_order_id + ":completion"
    before_sle = sorted(row["id"] for row in _sle_rows(conn, work_order_id))
    before_sle += sorted(row["id"] for row in _sle_rows(conn, comp_id))
    before_gl = sorted(row["id"] for row in _gl_rows(conn, work_order_id))
    before_gl += sorted(row["id"] for row in _gl_rows(conn, comp_id))
    before_audit = _audit_count(conn, work_order_id)

    real_head = M.take_chain_heads

    def sneaky(c, company_ids):
        real_head(c, company_ids)
        c.execute("UPDATE work_order SET status = 'cancelled' WHERE id = ?",
                  (work_order_id,))
        c.commit()
        real_head(c, company_ids)

    monkeypatch.setattr(M, "take_chain_heads", sneaky, raising=False)
    calls = {"n": 0}
    real_sle_reverse = M.reverse_sle_entries

    def counting(c, *args, **kwargs):
        calls["n"] += 1
        return real_sle_reverse(c, *args, **kwargs)

    monkeypatch.setattr(M, "reverse_sle_entries", counting)
    r = _cancel(conn, work_order_id)
    assert r == {"status": "error",
                 "message": CANCEL_STATUS_MESSAGE % "cancelled"}
    assert calls["n"] == 0
    after_sle = sorted(row["id"] for row in _sle_rows(fresh, work_order_id))
    after_sle += sorted(row["id"] for row in _sle_rows(fresh, comp_id))
    after_gl = sorted(row["id"] for row in _gl_rows(fresh, work_order_id))
    after_gl += sorted(row["id"] for row in _gl_rows(fresh, comp_id))
    assert after_sle == before_sle
    assert after_gl == before_gl
    assert _audit_count(fresh, work_order_id) == before_audit


def test_4_stale_status_refused_before_any_write(conn, db_path, monkeypatch):
    env, wo_id = _setup_transferred_and_partly_completed(conn, True)
    fresh = get_conn(db_path)
    try:
        _run_4_stale_status(conn, fresh, monkeypatch, wo_id)
    finally:
        fresh.close()


def test_4_stale_status_refused_before_any_write_pg(pg_conn, monkeypatch):
    _pg_only()
    _assert_pg_target(pg_conn)
    env, wo_id = _setup_transferred_and_partly_completed(pg_conn, False)
    fresh = get_connection()
    try:
        _run_4_stale_status(pg_conn, fresh, monkeypatch, wo_id)
    finally:
        fresh.close()


def test_5_ledger_refusal_rolls_back_the_stock_reversal(conn, monkeypatch):
    env, wo_id = _setup_transferred_and_partly_completed(conn, True)
    _add_open_job_card(conn, wo_id)
    before = _full_snapshot(conn, wo_id)
    real_reverse_gl = M.reverse_gl_entries
    state = {"n": 0}

    def planted(c, *args, **kwargs):
        state["n"] += 1
        if state["n"] == 2:
            raise ValueError("planted")
        return real_reverse_gl(c, *args, **kwargs)

    monkeypatch.setattr(M, "reverse_gl_entries", planted)
    r = _cancel(conn, wo_id)
    assert r == {"status": "error", "message": HALF_MESSAGE}
    assert state["n"] == 2
    assert _full_snapshot(conn, wo_id) == before
    assert _status(conn, wo_id) == "in_process"


def test_6_stock_only_voucher_cancels_cleanly(conn, monkeypatch):
    env = _zero_env(conn, True)
    wo_id = _new_work_order(conn, env)
    _start(conn, wo_id)
    r = call_action(M.transfer_materials, conn, ns(
        work_order_id=wo_id, posting_date=TRANSFER_DATE,
        items=json.dumps([{"item_id": env["rm0"], "qty": "4"}])))
    assert is_ok(r), r
    done = _complete_ok(conn, wo_id, "2")
    assert done["production_cost"] == "0.00", done
    assert done["gl_count"] == 0, done
    comp_id = wo_id + ":completion"
    assert _gl_rows(conn, wo_id) == []
    assert _gl_rows(conn, comp_id) == []
    calls = {"n": 0}
    real_reverse_gl = M.reverse_gl_entries

    def counting(c, *args, **kwargs):
        calls["n"] += 1
        return real_reverse_gl(c, *args, **kwargs)

    monkeypatch.setattr(M, "reverse_gl_entries", counting)
    r = _cancel(conn, wo_id)
    assert is_ok(r), r
    assert calls["n"] == 0
    assert _status(conn, wo_id) == "cancelled"
    for voucher_id in (wo_id, comp_id):
        rows = _sle_rows(conn, voucher_id)
        assert rows, voucher_id
        assert {row["is_cancelled"] for row in rows} == {1}
        assert _gl_rows(conn, voucher_id) == []
    _assert_family_nets_zero(conn, wo_id)


def test_7_final_status_race_rolls_everything_back(conn, monkeypatch):
    env, wo_id = _setup_transferred_and_partly_completed(conn, True)
    _add_open_job_card(conn, wo_id)
    before = _full_snapshot(conn, wo_id)
    real_reverse_gl = M.reverse_gl_entries

    def flip_after_real(c, *args, **kwargs):
        out = real_reverse_gl(c, *args, **kwargs)
        c.execute("UPDATE work_order SET status = 'completed' WHERE id = ?",
                  (wo_id,))
        return out

    monkeypatch.setattr(M, "reverse_gl_entries", flip_after_real)
    r = _cancel(conn, wo_id)
    assert r == {"status": "error",
                 "message": CANCEL_STATUS_MESSAGE % "completed"}
    assert _full_snapshot(conn, wo_id) == before
    assert _status(conn, wo_id) == "in_process"


def _pg_family_snapshot(conn, work_order_id):
    vouchers = _family_voucher_ids(conn, work_order_id)
    sle, gl, fifo = [], [], []
    for voucher_id in vouchers:
        sle += _snapshot_rows(
            conn,
            "SELECT * FROM stock_ledger_entry "
            "WHERE voucher_type = 'work_order' AND voucher_id = ?",
            (voucher_id,))
        gl += _snapshot_rows(
            conn,
            "SELECT * FROM gl_entry "
            "WHERE voucher_type = 'work_order' AND voucher_id = ?",
            (voucher_id,))
        fifo += _snapshot_rows(
            conn, "SELECT * FROM stock_fifo_layer WHERE source_voucher_id = ?",
            (voucher_id,))
    return {"sle": sorted(sle), "gl": sorted(gl), "fifo": sorted(fifo)}


def test_8_final_status_race_rolls_everything_back_pg(pg_conn, monkeypatch):
    _pg_only()
    _assert_pg_target(pg_conn)
    env, wo_id = _setup_transferred_and_partly_completed(pg_conn, False)
    before = _pg_family_snapshot(pg_conn, wo_id)
    real_reverse_gl = M.reverse_gl_entries

    def flip_on_other_conn(c, *args, **kwargs):
        out = real_reverse_gl(c, *args, **kwargs)
        other = get_connection()
        try:
            other.execute(
                "UPDATE work_order SET status = 'completed' WHERE id = ?",
                (wo_id,))
            other.commit()
        finally:
            other.close()
        return out

    monkeypatch.setattr(M, "reverse_gl_entries", flip_on_other_conn)
    r = _cancel(pg_conn, wo_id)
    assert r == {"status": "error",
                 "message": CANCEL_STATUS_MESSAGE % "completed"}
    fresh = get_connection()
    try:
        assert _pg_family_snapshot(fresh, wo_id) == before
        assert _status(fresh, wo_id) == "completed"
    finally:
        fresh.close()


def _wait_blocked_on_head(poll_conn, proc, appname, holder_pid,
                          timeout_s=30.0):
    """Poll until the named backend blocks on the holder's chain-head row.

    The poll connection is opened before the waiter starts and rolled back
    before every poll: a connection that sits idle in a transaction opened
    before the waiter connected keeps seeing an empty activity view, and a
    fresh connection per poll races the waiter's own connect-time setup.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        if proc.poll() is not None:
            out, err = proc.communicate()
            pytest.fail(
                "the cancel exited while waiting for it to block "
                "(rc=%s out=%s err=%s)"
                % (proc.returncode, out, err))
        poll_conn.rollback()
        rows = poll_conn.execute(
            "SELECT pg_blocking_pids(pid) AS blockers FROM pg_stat_activity "
            "WHERE application_name = ?",
            (appname,)).fetchall()
        for row in rows:
            blockers = dict(row)["blockers"] or []
            if int(holder_pid) in [int(pid) for pid in blockers]:
                return
        if time.monotonic() > deadline:
            pytest.fail("the cancel never blocked on the held head")
        time.sleep(0.2)


def _cancel_subprocess(work_order_id, posting_date=CANCEL_DATE, **env_over):
    penv = _proc_env(**env_over)
    return subprocess.Popen(
        [sys.executable, _MFG_SCRIPT,
         "--action", "cancel-work-order",
         "--work-order-id", work_order_id,
         "--posting-date", posting_date],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=penv)


def test_9_cancel_waits_on_head_without_touching_rows(pg_conn):
    _pg_only()
    _assert_pg_target(pg_conn)
    env, wo_id = _setup_transferred_and_partly_completed(pg_conn, False)
    holder = get_connection()
    try:
        _real_take_heads(holder, [env["company_id"]])
        holder_pid = holder.execute("SELECT pg_backend_pid()").fetchone()[0]
        appname = "m847-cancel-" + uuid.uuid4().hex[:8]
        poll_conn = get_connection()
        try:
            proc = _cancel_subprocess(
                wo_id, PGAPPNAME=appname, ERPCLAW_PG_LOCK_TIMEOUT="30s")
            try:
                _wait_blocked_on_head(poll_conn, proc, appname, holder_pid)
                assert proc.poll() is None, "the cancel must still be blocked"
                probe = get_connection()
                try:
                    probe.execute("SET lock_timeout = '1s'")
                    probe.execute(
                        "UPDATE stock_fifo_layer SET remaining_qty = "
                        "remaining_qty WHERE source_voucher_id = ?",
                        (wo_id,))
                    probe.execute(
                        "UPDATE stock_ledger_entry SET is_cancelled = "
                        "is_cancelled WHERE voucher_id = ?",
                        (wo_id,))
                    probe.rollback()
                finally:
                    probe.close()
                holder.rollback()
                out, err = proc.communicate(timeout=15)
                assert proc.returncode == 0, (out, err)
                assert json.loads(out)["status"] == "ok", (out, err)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
        finally:
            poll_conn.close()
    finally:
        try:
            holder.rollback()
        except Exception:  # noqa: BLE001, S110 - best effort release
            pass
        holder.close()


def _transfer_subprocess(work_order_id, item_id, qty):
    penv = _proc_env()
    return subprocess.Popen(
        [sys.executable, _MFG_SCRIPT,
         "--action", "transfer-materials",
         "--work-order-id", work_order_id,
         "--items", json.dumps([{"item_id": item_id, "qty": qty}]),
         "--posting-date", TRANSFER_DATE],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=penv)


def _completion_subprocess(work_order_id, qty):
    penv = _proc_env()
    return subprocess.Popen(
        [sys.executable, _MFG_SCRIPT,
         "--action", "complete-work-order",
         "--work-order-id", work_order_id,
         "--produced-qty", qty,
         "--posting-date", COMPLETION_DATE],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=penv)


def _launch_pair(first_fn, second_fn):
    """Start two subprocesses within 50 ms of each other."""
    for _ in range(5):
        first = first_fn()
        mark = time.monotonic()
        second = second_fn()
        if (time.monotonic() - mark) * 1000 < 50:
            return first, second
        for stray in (first, second):
            if stray.poll() is None:
                stray.kill()
            stray.wait()
    pytest.fail("could not start the two processes within 50 ms")


def _communicate(proc, label):
    try:
        return proc.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
        pytest.fail("the %s did not exit within 15 s (out=%s err=%s)"
                    % (label, out, err))


def test_10a_cancel_vs_transfer_race(pg_conn):
    _pg_only()
    _assert_pg_target(pg_conn)
    env = _build_env(pg_conn, False)
    for round_no in range(5):
        wo_id = _new_work_order(pg_conn, env)
        _start(pg_conn, wo_id)
        if round_no % 2 == 0:
            cancel_proc, transfer_proc = _launch_pair(
                lambda: _cancel_subprocess(wo_id),
                lambda: _transfer_subprocess(wo_id, env["rm"], "10"))
        else:
            transfer_proc, cancel_proc = _launch_pair(
                lambda: _transfer_subprocess(wo_id, env["rm"], "10"),
                lambda: _cancel_subprocess(wo_id))
        try:
            cout, cerr = _communicate(cancel_proc, "cancel")
            tout, terr = _communicate(transfer_proc, "transfer")
        finally:
            for proc in (cancel_proc, transfer_proc):
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
        combined = cout + cerr + tout + terr
        assert "deadlock" not in combined.lower(), combined
        assert cout.strip(), (cout, cerr, tout, terr)
        assert tout.strip(), (cout, cerr, tout, terr)
        cancel_doc, transfer_doc = json.loads(cout), json.loads(tout)
        if transfer_proc.returncode == 1:
            assert transfer_doc == {"status": "error",
                                    "message": TRANSFER_STATUS_MESSAGE}, \
                (tout, terr)
            assert cancel_proc.returncode == 0 \
                and cancel_doc["status"] == "ok", (cout, cerr)
        else:
            assert transfer_proc.returncode == 0 \
                and transfer_doc["status"] == "ok", (tout, terr)
            assert cancel_proc.returncode == 0 \
                and cancel_doc["status"] == "ok", (cout, cerr)
        fresh = get_connection()
        try:
            assert _status(fresh, wo_id) == "cancelled"
            if transfer_proc.returncode == 1:
                assert _family_voucher_ids(fresh, wo_id) == []
            else:
                assert _family_voucher_ids(fresh, wo_id) == [wo_id]
                _assert_family_nets_zero(fresh, wo_id)
        finally:
            fresh.close()


def test_10b_cancel_vs_completion_race(pg_conn):
    _pg_only()
    _assert_pg_target(pg_conn)
    env = _build_env(pg_conn, False)
    for round_no in range(5):
        wo_id = _new_work_order(pg_conn, env)
        _start(pg_conn, wo_id)
        _transfer_ok(pg_conn, env, wo_id, "10")
        if round_no % 2 == 0:
            cancel_proc, complete_proc = _launch_pair(
                lambda: _cancel_subprocess(wo_id),
                lambda: _completion_subprocess(wo_id, "10"))
        else:
            complete_proc, cancel_proc = _launch_pair(
                lambda: _completion_subprocess(wo_id, "10"),
                lambda: _cancel_subprocess(wo_id))
        try:
            cout, cerr = _communicate(cancel_proc, "cancel")
            mout, merr = _communicate(complete_proc, "completion")
        finally:
            for proc in (cancel_proc, complete_proc):
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
        combined = cout + cerr + mout + merr
        assert "deadlock" not in combined.lower(), combined
        assert cout.strip(), (cout, cerr, mout, merr)
        assert mout.strip(), (cout, cerr, mout, merr)
        cancel_doc, complete_doc = json.loads(cout), json.loads(mout)
        fresh = get_connection()
        try:
            if complete_proc.returncode == 1:
                assert complete_doc == {"status": "error",
                                        "message": COMPLETE_STATUS_MESSAGE}, \
                    (mout, merr)
                assert cancel_proc.returncode == 0 \
                    and cancel_doc["status"] == "ok", (cout, cerr)
                assert _status(fresh, wo_id) == "cancelled"
                _assert_family_nets_zero(fresh, wo_id)
            else:
                assert complete_proc.returncode == 0 \
                    and complete_doc["status"] == "ok", (mout, merr)
                assert _status(fresh, wo_id) == "completed"
                assert cancel_proc.returncode == 1, (cout, cerr)
                assert cancel_doc == {
                    "status": "error",
                    "message": CANCEL_STATUS_MESSAGE % "completed"}, \
                    (cout, cerr)
                for voucher_id in _family_voucher_ids(fresh, wo_id):
                    for row in _sle_rows(fresh, voucher_id):
                        assert row["is_cancelled"] == 0, voucher_id
                    for row in _gl_rows(fresh, voucher_id):
                        assert row["is_cancelled"] == 0, voucher_id
        finally:
            fresh.close()
