"""cancel-work-order: the ledger effects it reverses, the statuses it sets, and
the work orders it refuses.

Scenario (all dates fixed, moving-average valuation):
  - Raw Material 1: 100 opening at 5.00 in the raw store.
  - Raw Material 2: 50 opening at 8.00 in the raw store.
  - BOM: 1 finished good consumes 2 of RM1 and 1 of RM2.
  - Work order for 10, started; materials transferred on 2026-06-05
    (RM1 20 = 100.00, RM2 10 = 80.00, voucher id = work order id).
  - Partial completion of 4 on 2026-06-15 (voucher id = "<wo id>:completion"):
    FG +4 at 18.00 = 72.00 into the FG store, RM1 -8 and RM2 -4 out of WIP.
  - Cancel on 2026-06-20.

Each warehouse is linked to its own stock account, so the stock legs can be
pinned by account. The contra legs are compared leg for leg against what the
transfer and completion posted before the cancel.
"""
import json
import os
import sys
import uuid
from collections import defaultdict
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from mfg_helpers import (call_action, is_error, is_ok, load_db_query,  # noqa: E402
                         ns, seed_account, seed_company, seed_item,
                         seed_naming_series)

M = load_db_query()

TRANSFER_DATE = "2026-06-05"
COMPLETION_DATE = "2026-06-15"
CANCEL_DATE = "2026-06-20"


def _u():
    return str(uuid.uuid4())


def _warehouse(conn, company_id, name, account_id):
    wid = _u()
    conn.execute(
        "INSERT INTO warehouse (id, name, company_id, account_id) VALUES (?, ?, ?, ?)",
        (wid, name, company_id, account_id))
    return wid


def _ledger_env(conn):
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    conn.execute(
        "INSERT INTO fiscal_year (id, name, start_date, end_date, company_id) "
        "VALUES (?, ?, ?, ?, ?)", (_u(), "FY2026", "2026-01-01", "2026-12-31", cid))
    conn.execute(
        "INSERT INTO cost_center (id, name, company_id, is_group) VALUES (?, ?, ?, 0)",
        (_u(), "Main", cid))
    env = {"company_id": cid}
    env["raw_acct"] = seed_account(conn, cid, "Raw Stock", "stock", "asset")
    env["wip_acct"] = seed_account(conn, cid, "WIP Stock", "stock", "asset")
    env["fg_acct"] = seed_account(conn, cid, "FG Stock", "stock", "asset")
    env["srnb_acct"] = seed_account(conn, cid, "Stock Received Not Billed",
                                        "stock_received_not_billed", "liability")
    env["cogs_acct"] = seed_account(conn, cid, "Cost of Goods Sold",
                                    "cost_of_goods_sold", "expense")
    env["raw_wh"] = _warehouse(conn, cid, "Raw Store", env["raw_acct"])
    env["wip_wh"] = _warehouse(conn, cid, "WIP Store", env["wip_acct"])
    env["fg_wh"] = _warehouse(conn, cid, "FG Store", env["fg_acct"])
    env["fg_item"] = seed_item(conn, cid, name="Finished Good", standard_rate="0")
    env["rm1"] = seed_item(conn, cid, name="Raw Material 1", standard_rate="5.00")
    env["rm2"] = seed_item(conn, cid, name="Raw Material 2", standard_rate="8.00")
    M.insert_sle_entries(conn, [
        {"item_id": env["rm1"], "warehouse_id": env["raw_wh"], "actual_qty": "100",
         "incoming_rate": "5.00"},
        {"item_id": env["rm2"], "warehouse_id": env["raw_wh"], "actual_qty": "50",
         "incoming_rate": "8.00"},
    ], voucher_type="stock_entry", voucher_id="opening-" + cid[:8],
        posting_date="2026-01-05", company_id=cid)
    conn.commit()

    r = call_action(M.add_bom, conn, ns(
        item_id=env["fg_item"], company_id=cid, quantity="1",
        items=json.dumps([{"item_id": env["rm1"], "quantity": "2", "rate": "5.00"},
                          {"item_id": env["rm2"], "quantity": "1", "rate": "8.00"}])))
    assert is_ok(r), r
    env["bom_id"] = r["bom_id"]
    return env


def _work_order(conn, env, qty="10"):
    r = call_action(M.add_work_order, conn, ns(
        bom_id=env["bom_id"], quantity=qty, company_id=env["company_id"],
        source_warehouse_id=env["raw_wh"], target_warehouse_id=env["fg_wh"],
        wip_warehouse_id=env["wip_wh"]))
    assert is_ok(r), r
    return r["work_order_id"]


def _start_and_transfer(conn, env, wo_id):
    assert is_ok(call_action(M.start_work_order, conn, ns(work_order_id=wo_id)))
    r = call_action(M.transfer_materials, conn, ns(
        work_order_id=wo_id, posting_date=TRANSFER_DATE,
        items=json.dumps([{"item_id": env["rm1"], "qty": "20"},
                          {"item_id": env["rm2"], "qty": "10"}])))
    assert is_ok(r), r


def _complete(conn, wo_id, produced_qty):
    r = call_action(M.complete_work_order, conn, ns(
        work_order_id=wo_id, produced_qty=produced_qty, posting_date=COMPLETION_DATE))
    assert is_ok(r), r
    return r


def _gl_rows(conn, voucher_id):
    return [dict(r) for r in conn.execute(
        "SELECT id, account_id, debit, credit, posting_date, is_cancelled, remarks "
        "FROM gl_entry WHERE voucher_type = 'work_order' AND voucher_id = ?",
        (voucher_id,)).fetchall()]


def _sle_rows(conn, voucher_id):
    return [dict(r) for r in conn.execute(
        "SELECT id, item_id, warehouse_id, actual_qty, stock_value_difference, "
        "posting_date, is_cancelled FROM stock_ledger_entry "
        "WHERE voucher_type = 'work_order' AND voucher_id = ?",
        (voucher_id,)).fetchall()]


def _per_account(rows):
    """{account_id: (total debit, total credit)} as exact strings."""
    dr, cr = defaultdict(Decimal), defaultdict(Decimal)
    for r in rows:
        dr[r["account_id"]] += Decimal(r["debit"])
        cr[r["account_id"]] += Decimal(r["credit"])
    return {a: (str(dr[a]), str(cr[a])) for a in dr}


def _legs(rows):
    return sorted((r["account_id"], r["debit"], r["credit"]) for r in rows)


def _counts(conn):
    return (conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM stock_ledger_entry").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM gl_entry WHERE is_cancelled = 1").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM stock_ledger_entry "
                         "WHERE is_cancelled = 1").fetchone()[0])


def _active_qty(conn, item_id, warehouse_id):
    total = Decimal("0")
    for r in conn.execute(
            "SELECT actual_qty FROM stock_ledger_entry "
            "WHERE item_id = ? AND warehouse_id = ? AND is_cancelled = 0",
            (item_id, warehouse_id)).fetchall():
        total += Decimal(r["actual_qty"])
    return str(total)


def _status(conn, wo_id):
    return conn.execute("SELECT status FROM work_order WHERE id = ?",
                        (wo_id,)).fetchone()["status"]


def _assert_voucher_reversed(conn, voucher_id, before_gl, before_sle):
    """Every original leg is marked cancelled and has exactly one mirror dated
    the cancel date; each account nets to zero; SLE quantities and values are
    negated and marked cancelled."""
    before_ids = {r["id"] for r in before_gl}
    after = _gl_rows(conn, voucher_id)
    originals = [r for r in after if r["id"] in before_ids]
    reversals = [r for r in after if r["id"] not in before_ids]
    assert len(originals) == len(before_gl)
    assert len(reversals) == len(before_gl)
    assert {r["is_cancelled"] for r in originals} == {1}
    assert {r["is_cancelled"] for r in reversals} == {1}
    assert {r["posting_date"] for r in reversals} == {CANCEL_DATE}
    by_id = {r["id"]: r for r in before_gl}
    assert sorted((r["account_id"], r["debit"], r["credit"]) for r in originals) == \
        sorted((r["account_id"], r["debit"], r["credit"]) for r in before_gl)
    mirrored = sorted(
        (by_id[r["remarks"][len("Reversal of "):]]["account_id"],
         by_id[r["remarks"][len("Reversal of "):]]["credit"],
         by_id[r["remarks"][len("Reversal of "):]]["debit"]) for r in reversals)
    assert sorted((r["account_id"], r["debit"], r["credit"]) for r in reversals) == mirrored
    assert sorted(r["remarks"] for r in reversals) == sorted(
        "Reversal of " + i for i in before_ids)
    for acct, (dr, cr) in _per_account(after).items():
        assert Decimal(dr) == Decimal(cr), acct

    before_sle_ids = {r["id"] for r in before_sle}
    sle_after = _sle_rows(conn, voucher_id)
    sle_orig = [r for r in sle_after if r["id"] in before_sle_ids]
    sle_rev = [r for r in sle_after if r["id"] not in before_sle_ids]
    assert len(sle_orig) == len(before_sle) and len(sle_rev) == len(before_sle)
    assert {r["is_cancelled"] for r in sle_after} == {1}
    assert {r["posting_date"] for r in sle_rev} == {CANCEL_DATE}
    assert sorted((r["item_id"], r["warehouse_id"], str(-Decimal(r["actual_qty"])),
                   str(-Decimal(r["stock_value_difference"]))) for r in before_sle) == \
        sorted((r["item_id"], r["warehouse_id"], r["actual_qty"],
                r["stock_value_difference"]) for r in sle_rev)
    return reversals, sle_rev


def test_cancel_reverses_transfer_and_partial_completion(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start_and_transfer(conn, env, wo_id)
    done = _complete(conn, wo_id, "4")
    assert done["production_cost"] == "72.00" and done["fg_rate"] == "18.00"
    assert _status(conn, wo_id) == "in_process"
    comp_id = f"{wo_id}:completion"

    r = call_action(M.add_operation, conn, ns(name="Assembly " + wo_id[:8]))
    assert is_ok(r), r
    op_id = r["operation_id"]
    open_jc = call_action(M.create_job_card, conn, ns(
        work_order_id=wo_id, operation_id=op_id))["job_card_id"]
    done_jc = call_action(M.create_job_card, conn, ns(
        work_order_id=wo_id, operation_id=op_id))["job_card_id"]
    assert is_ok(call_action(M.complete_job_card, conn, ns(
        job_card_id=done_jc, actual_time_in_mins="30")))

    # What the transfer and completion posted, read back before the cancel.
    t_gl, t_sle = _gl_rows(conn, wo_id), _sle_rows(conn, wo_id)
    c_gl, c_sle = _gl_rows(conn, comp_id), _sle_rows(conn, comp_id)
    assert (len(t_gl), len(t_sle), len(c_gl), len(c_sle)) == (4, 4, 3, 3)
    raw, wip = env["raw_acct"], env["wip_acct"]
    assert _legs(t_gl) == sorted([(raw, "0.00", "100.00"), (raw, "0.00", "80.00"), (wip, "100.00", "0.00"), (wip, "80.00", "0.00")])
    assert all(r["account_id"] not in {env["srnb_acct"], env["cogs_acct"]} for r in t_gl)
    assert {r["is_cancelled"] for r in t_gl + t_sle + c_gl + c_sle} == {0}
    t_acct, c_acct = _per_account(t_gl), _per_account(c_gl)
    assert t_acct[env["raw_acct"]] == ("0.00", "180.00")
    assert t_acct[env["wip_acct"]] == ("180.00", "0.00")
    assert c_acct[env["wip_acct"]] == ("0.00", "72.00")
    assert c_acct[env["fg_acct"]] == ("72.00", "0.00")
    fg, wip = env["fg_acct"], env["wip_acct"]
    assert _legs(c_gl) == sorted(
        [(fg, "72.00", "0.00"), (wip, "0.00", "40.00"),
         (wip, "0.00", "32.00")])
    assert sum(Decimal(r["debit"]) for r in t_gl) == sum(Decimal(r["credit"]) for r in t_gl)
    assert sum(Decimal(r["debit"]) for r in c_gl) == sum(Decimal(r["credit"]) for r in c_gl)
    assert _active_qty(conn, env["rm1"], env["raw_wh"]) == "80.00"
    assert _active_qty(conn, env["fg_item"], env["fg_wh"]) == "4.00"

    r = call_action(M.cancel_work_order, conn, ns(
        work_order_id=wo_id, posting_date=CANCEL_DATE))
    assert is_ok(r), r
    assert r["work_order_id"] == wo_id

    t_rev, t_sle_rev = _assert_voucher_reversed(conn, wo_id, t_gl, t_sle)
    c_rev, c_sle_rev = _assert_voucher_reversed(conn, comp_id, c_gl, c_sle)

    # Reversal legs pinned by stock account.
    assert _per_account(t_rev)[env["raw_acct"]] == ("180.00", "0.00")
    assert _per_account(t_rev)[env["wip_acct"]] == ("0.00", "180.00")
    assert _per_account(c_rev)[env["wip_acct"]] == ("72.00", "0.00")
    assert _per_account(c_rev)[env["fg_acct"]] == ("0.00", "72.00")
    assert _legs(c_rev) == sorted(
        [(fg, "0.00", "72.00"), (wip, "40.00", "0.00"),
         (wip, "32.00", "0.00")])
    assert sorted((r["actual_qty"], r["stock_value_difference"]) for r in t_sle_rev) == \
        sorted([("-20.00", "-100.00"), ("-10.00", "-80.00"),
                ("20.00", "100.00"), ("10.00", "80.00")])
    assert sorted((r["actual_qty"], r["stock_value_difference"]) for r in c_sle_rev) == \
        sorted([("-4.00", "-72.00"), ("8.00", "40.00"), ("4.00", "32.00")])

    # Nothing from this work order is left active; stock is back to opening.
    assert conn.execute(
        "SELECT COUNT(*) FROM gl_entry WHERE voucher_type = 'work_order' "
        "AND voucher_id IN (?, ?) AND is_cancelled = 0", (wo_id, comp_id)).fetchone()[0] == 0
    assert _active_qty(conn, env["rm1"], env["raw_wh"]) == "100.00"
    assert _active_qty(conn, env["rm2"], env["raw_wh"]) == "50.00"
    assert _active_qty(conn, env["rm1"], env["wip_wh"]) == "0"
    assert _active_qty(conn, env["fg_item"], env["fg_wh"]) == "0"

    assert _status(conn, wo_id) == "cancelled"
    jc = {row["id"]: row["status"] for row in conn.execute(
        "SELECT id, status FROM job_card WHERE work_order_id = ?", (wo_id,)).fetchall()}
    assert jc == {open_jc: "cancelled", done_jc: "completed"}


def test_cancel_reverses_transfer_legs_exactly(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start_and_transfer(conn, env, wo_id)
    t_gl = _gl_rows(conn, wo_id)
    t_sle = _sle_rows(conn, wo_id)
    raw, wip = env["raw_acct"], env["wip_acct"]
    before_family = [dict(r) for r in conn.execute(
        "SELECT voucher_id, account_id FROM gl_entry WHERE voucher_type = 'work_order'").fetchall()]
    before_family = [r for r in before_family if r["voucher_id"] == wo_id or r["voucher_id"].startswith(wo_id + ":")]
    r = call_action(M.cancel_work_order, conn, ns(
        work_order_id=wo_id, posting_date=CANCEL_DATE))
    assert is_ok(r), r
    reversals, _ = _assert_voucher_reversed(conn, wo_id, t_gl, t_sle)
    assert _legs(reversals) == sorted([(raw, "100.00", "0.00"), (raw, "80.00", "0.00"), (wip, "0.00", "100.00"), (wip, "0.00", "80.00")])
    assert all(r["account_id"] not in {env["srnb_acct"], env["cogs_acct"]} for r in before_family)
    after_family = [dict(r) for r in conn.execute(
        "SELECT voucher_id, account_id FROM gl_entry WHERE voucher_type = 'work_order'").fetchall()]
    after_family = [r for r in after_family if r["voucher_id"] == wo_id or r["voucher_id"].startswith(wo_id + ":")]
    assert all(r["account_id"] not in {env["srnb_acct"], env["cogs_acct"]} for r in after_family)


def test_cancel_draft_work_order_writes_no_ledger_rows(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    before = _counts(conn)
    r = call_action(M.cancel_work_order, conn, ns(
        work_order_id=wo_id, posting_date=CANCEL_DATE))
    assert is_ok(r), r
    assert _status(conn, wo_id) == "cancelled"
    assert _counts(conn) == before
    assert _gl_rows(conn, wo_id) == [] and _sle_rows(conn, wo_id) == []


def test_cancel_refuses_completed_work_order(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start_and_transfer(conn, env, wo_id)
    _complete(conn, wo_id, "10")
    assert _status(conn, wo_id) == "completed"
    before = _counts(conn)
    r = call_action(M.cancel_work_order, conn, ns(
        work_order_id=wo_id, posting_date=CANCEL_DATE))
    assert is_error(r)
    assert r["message"] == (
        "Cannot cancel Work Order with status 'completed'. "
        "Completed and cancelled work orders cannot be cancelled.")
    assert _counts(conn) == before
    assert _status(conn, wo_id) == "completed"
    assert {row["is_cancelled"] for row in _gl_rows(conn, wo_id)} == {0}


def test_cancel_refuses_second_cancel(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start_and_transfer(conn, env, wo_id)
    assert is_ok(call_action(M.cancel_work_order, conn, ns(
        work_order_id=wo_id, posting_date=CANCEL_DATE)))
    before = _counts(conn)
    assert before[2] == 8 and before[3] == 8  # 4 transfer legs marked cancelled + 4 mirrors; 4 stock rows + 4 mirrors
    r = call_action(M.cancel_work_order, conn, ns(
        work_order_id=wo_id, posting_date="2026-06-25"))
    assert is_error(r)
    assert "status 'cancelled'" in r["message"]
    assert _counts(conn) == before
    assert _status(conn, wo_id) == "cancelled"


def test_cancel_refuses_missing_and_unknown_work_order(conn):
    _ledger_env(conn)
    before = _counts(conn)
    r = call_action(M.cancel_work_order, conn, ns(posting_date=CANCEL_DATE))
    assert is_error(r)
    assert r["message"] == "--work-order-id is required"
    missing = _u()
    r = call_action(M.cancel_work_order, conn, ns(
        work_order_id=missing, posting_date=CANCEL_DATE))
    assert is_error(r)
    assert r["message"] == f"Work Order {missing} not found"
    assert _counts(conn) == before


def test_cancel_reverses_completion_with_operating_cost(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start_and_transfer(conn, env, wo_id)

    r = call_action(M.add_workstation, conn, ns(
        name="WS " + wo_id[:8], hour_rate="60.00"))
    assert is_ok(r), r
    ws_id = r["workstation_id"]
    r = call_action(M.add_operation, conn, ns(name="Assembly " + wo_id[:8]))
    assert is_ok(r), r
    op_id = r["operation_id"]
    r = call_action(M.create_job_card, conn, ns(
        work_order_id=wo_id, operation_id=op_id, workstation_id=ws_id))
    assert is_ok(r), r
    jc_id = r["job_card_id"]
    assert is_ok(call_action(M.complete_job_card, conn, ns(
        job_card_id=jc_id, actual_time_in_mins="30")))

    done = _complete(conn, wo_id, "4")
    assert done["production_cost"] == "102.00"
    comp_id = f"{wo_id}:completion"
    c_gl, c_sle = _gl_rows(conn, comp_id), _sle_rows(conn, comp_id)
    fg, wip, srnb = env["fg_acct"], env["wip_acct"], env["srnb_acct"]
    assert _legs(c_gl) == sorted(
        [(fg, "102.00", "0.00"), (wip, "0.00", "40.00"),
         (wip, "0.00", "32.00"), (srnb, "0.00", "30.00")])

    r = call_action(M.cancel_work_order, conn, ns(
        work_order_id=wo_id, posting_date=CANCEL_DATE))
    assert is_ok(r), r

    c_rev, _ = _assert_voucher_reversed(conn, comp_id, c_gl, c_sle)
    assert _legs(c_rev) == sorted(
        [(fg, "0.00", "102.00"), (wip, "40.00", "0.00"),
         (wip, "32.00", "0.00"), (srnb, "30.00", "0.00")])
