"""Partial work-order completion consumes and costs only what it produced.

Scenario (all dates fixed, moving-average valuation):
  - Raw Material 1: 100 opening at 5.00 in the raw store.
  - Raw Material 2: 50 opening at 8.00 in the raw store.
  - BOM: 1 finished good consumes 2 of RM1 and 1 of RM2.
  - Work order for 10. Each completion issues from WIP only the raw
    material its output used (RM1 -8.00 and RM2 -4.00 for 4 units) at cost
    72.00 (rate 18.00); the final completion takes whatever remains so
    rounding cannot strand material in WIP; operating cost is absorbed once.

Concurrency: transfer and completion take the company's chain head before
reading the work order, so concurrent completions serialise and each derives its voucher number from the committed
ledger.
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
TRANSFER_DATE_2 = "2026-06-06"
COMPLETION_DATE = "2026-06-15"
COMPLETION_DATE_2 = "2026-06-16"
COMPLETION_DATE_3 = "2026-06-17"
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


def _start(conn, wo_id):
    assert is_ok(call_action(M.start_work_order, conn, ns(work_order_id=wo_id)))


def _transfer(conn, env, wo_id, rm1_qty, rm2_qty, posting_date=TRANSFER_DATE):
    r = call_action(M.transfer_materials, conn, ns(
        work_order_id=wo_id, posting_date=posting_date,
        items=json.dumps([{"item_id": env["rm1"], "qty": rm1_qty},
                          {"item_id": env["rm2"], "qty": rm2_qty}])))
    assert is_ok(r), r
    return r


def _complete(conn, wo_id, produced_qty, posting_date=COMPLETION_DATE):
    r = call_action(M.complete_work_order, conn, ns(
        work_order_id=wo_id, produced_qty=produced_qty, posting_date=posting_date))
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


def _gl_balanced(rows):
    return sum(Decimal(r["debit"]) for r in rows) == sum(Decimal(r["credit"]) for r in rows)


def _active_qty(conn, item_id, warehouse_id):
    total = Decimal("0")
    for r in conn.execute(
            "SELECT actual_qty FROM stock_ledger_entry "
            "WHERE item_id = ? AND warehouse_id = ? AND is_cancelled = 0",
            (item_id, warehouse_id)).fetchall():
        total += Decimal(r["actual_qty"])
    return str(total)


def _active_value(conn, item_id, warehouse_id):
    total = Decimal("0")
    for r in conn.execute(
            "SELECT stock_value_difference FROM stock_ledger_entry "
            "WHERE item_id = ? AND warehouse_id = ? AND is_cancelled = 0",
            (item_id, warehouse_id)).fetchall():
        total += Decimal(r["stock_value_difference"])
    return str(total)


def _status(conn, wo_id):
    return conn.execute("SELECT status FROM work_order WHERE id = ?",
                        (wo_id,)).fetchone()["status"]


def _wo_items(conn, wo_id):
    return {r["item_id"]: dict(r) for r in conn.execute(
        "SELECT item_id, required_qty, transferred_qty, consumed_qty "
        "FROM work_order_item WHERE work_order_id = ?", (wo_id,)).fetchall()}


def _wip_issue_qty(sle_rows, item_id, wip_wh):
    for r in sle_rows:
        if r["item_id"] == item_id and r["warehouse_id"] == wip_wh:
            return r["actual_qty"]
    return None


def _job_card_setup(conn, wo_id, hour_rate="60.00", minutes="30"):
    r = call_action(M.add_workstation, conn, ns(
        name="WS " + wo_id[:8], hour_rate=hour_rate))
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
        job_card_id=jc_id, actual_time_in_mins=minutes)))
    return ws_id, op_id, jc_id




def test_three_completions_after_one_full_transfer(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start(conn, wo_id)
    _transfer(conn, env, wo_id, "20", "10")

    comp1 = f"{wo_id}:completion"
    r1 = _complete(conn, wo_id, "4")
    assert r1["rm_cost"] == "72.00"
    assert r1["fg_rate"] == "18.00"
    assert r1["document_status"] == "in_process"
    assert r1["produced_qty"] == "4.00"
    sle1 = _sle_rows(conn, comp1)
    assert _wip_issue_qty(sle1, env["rm1"], env["wip_wh"]) == "-8.00"
    assert _wip_issue_qty(sle1, env["rm2"], env["wip_wh"]) == "-4.00"
    items = _wo_items(conn, wo_id)
    assert items[env["rm1"]]["consumed_qty"] == "8.00"
    assert items[env["rm2"]]["consumed_qty"] == "4.00"
    assert _active_qty(conn, env["rm1"], env["wip_wh"]) == "12.00"
    assert _active_qty(conn, env["rm2"], env["wip_wh"]) == "6.00"

    comp2 = f"{wo_id}:completion:2"
    r2 = _complete(conn, wo_id, "3", posting_date=COMPLETION_DATE_2)
    assert r2["rm_cost"] == "54.00"
    assert r2["fg_rate"] == "18.00"
    assert r2["document_status"] == "in_process"
    assert r2["produced_qty"] == "7.00"
    sle2 = _sle_rows(conn, comp2)
    assert _wip_issue_qty(sle2, env["rm1"], env["wip_wh"]) == "-6.00"
    assert _wip_issue_qty(sle2, env["rm2"], env["wip_wh"]) == "-3.00"

    comp3 = f"{wo_id}:completion:3"
    r3 = _complete(conn, wo_id, "3", posting_date=COMPLETION_DATE_3)
    assert r3["rm_cost"] == "54.00"
    assert r3["fg_rate"] == "18.00"
    assert r3["document_status"] == "completed"
    assert r3["produced_qty"] == "10.00"
    sle3 = _sle_rows(conn, comp3)
    assert _wip_issue_qty(sle3, env["rm1"], env["wip_wh"]) == "-6.00"
    assert _wip_issue_qty(sle3, env["rm2"], env["wip_wh"]) == "-3.00"

    assert _active_qty(conn, env["rm1"], env["wip_wh"]) == "0.00"
    assert _active_qty(conn, env["rm2"], env["wip_wh"]) == "0.00"
    items = _wo_items(conn, wo_id)
    # Stored text form comes from the transfer increment's cast (db_query.py transfer-materials), so it differs by backend.
    assert Decimal(items[env["rm1"]]["consumed_qty"]) == Decimal(items[env["rm1"]]["transferred_qty"]) == Decimal("20")
    assert Decimal(items[env["rm2"]]["consumed_qty"]) == Decimal(items[env["rm2"]]["transferred_qty"]) == Decimal("10")
    assert _active_qty(conn, env["fg_item"], env["fg_wh"]) == "10.00"
    assert _active_value(conn, env["fg_item"], env["fg_wh"]) == "180.00"

    gl1, gl2, gl3 = _gl_rows(conn, comp1), _gl_rows(conn, comp2), _gl_rows(conn, comp3)
    assert _gl_balanced(gl1) and _gl_balanced(gl2) and _gl_balanced(gl3)
    assert _per_account(gl1)[env["wip_acct"]] == ("0.00", "72.00")
    assert _per_account(gl1)[env["fg_acct"]] == ("72.00", "0.00")
    assert _per_account(gl2)[env["wip_acct"]] == ("0.00", "54.00")
    assert _per_account(gl2)[env["fg_acct"]] == ("54.00", "0.00")
    assert _per_account(gl3)[env["wip_acct"]] == ("0.00", "54.00")
    assert _per_account(gl3)[env["fg_acct"]] == ("54.00", "0.00")
    assert _status(conn, wo_id) == "completed"


def test_staggered_transfer(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start(conn, wo_id)
    _transfer(conn, env, wo_id, "10", "5")

    comp1 = f"{wo_id}:completion"
    r1 = _complete(conn, wo_id, "4")
    assert r1["rm_cost"] == "72.00"
    assert r1["fg_rate"] == "18.00"
    sle1 = _sle_rows(conn, comp1)
    assert _wip_issue_qty(sle1, env["rm1"], env["wip_wh"]) == "-8.00"
    assert _wip_issue_qty(sle1, env["rm2"], env["wip_wh"]) == "-4.00"

    r = call_action(M.transfer_materials, conn, ns(
        work_order_id=wo_id, posting_date=TRANSFER_DATE_2,
        items=json.dumps([{"item_id": env["rm1"], "qty": "10"},
                          {"item_id": env["rm2"], "qty": "5"}])))
    assert is_ok(r), r
    transfer2 = f"{wo_id}:transfer:2"
    assert _sle_rows(conn, transfer2) != []
    assert _sle_rows(conn, wo_id) != []
    t2_gl = _gl_rows(conn, transfer2)
    assert _per_account(t2_gl)[env["raw_acct"]] == ("0.00", "90.00")
    assert _per_account(t2_gl)[env["wip_acct"]] == ("90.00", "0.00")
    raw, wip = env["raw_acct"], env["wip_acct"]
    assert _legs(t2_gl) == sorted([(raw, "0.00", "50.00"), (raw, "0.00", "40.00"), (wip, "50.00", "0.00"), (wip, "40.00", "0.00")])
    assert len(t2_gl) == 4
    assert all(r["account_id"] not in {env["srnb_acct"], env["cogs_acct"]} for r in t2_gl)

    comp2 = f"{wo_id}:completion:2"
    r2 = _complete(conn, wo_id, "6", posting_date=COMPLETION_DATE_2)
    assert r2["rm_cost"] == "108.00"
    assert r2["fg_rate"] == "18.00"
    assert r2["document_status"] == "completed"
    assert r2["produced_qty"] == "10.00"
    sle2 = _sle_rows(conn, comp2)
    assert _wip_issue_qty(sle2, env["rm1"], env["wip_wh"]) == "-12.00"
    assert _wip_issue_qty(sle2, env["rm2"], env["wip_wh"]) == "-6.00"
    assert _active_qty(conn, env["rm1"], env["wip_wh"]) == "0.00"
    assert _active_qty(conn, env["rm2"], env["wip_wh"]) == "0.00"
    items = _wo_items(conn, wo_id)
    assert Decimal(items[env["rm1"]]["consumed_qty"]) == Decimal(items[env["rm1"]]["transferred_qty"]) == Decimal("20")
    assert Decimal(items[env["rm2"]]["consumed_qty"]) == Decimal(items[env["rm2"]]["transferred_qty"]) == Decimal("10")


def test_operating_cost_absorbed_once(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start(conn, wo_id)
    _job_card_setup(conn, wo_id, hour_rate="60.00", minutes="30")
    _transfer(conn, env, wo_id, "20", "10")

    r1 = _complete(conn, wo_id, "4")
    assert r1["operating_cost"] == "30.00"
    assert r1["production_cost"] == "102.00"

    r2 = _complete(conn, wo_id, "6", posting_date=COMPLETION_DATE_2)
    assert r2["operating_cost"] == "0.00"
    assert r2["production_cost"] == "108.00"
    assert r2["document_status"] == "completed"

    assert _active_value(conn, env["fg_item"], env["fg_wh"]) == "210.00"

    fg, wip, srnb = env["fg_acct"], env["wip_acct"], env["srnb_acct"]
    assert _legs(_gl_rows(conn, f"{wo_id}:completion")) == sorted(
        [(fg, "102.00", "0.00"), (wip, "0.00", "40.00"),
         (wip, "0.00", "32.00"), (srnb, "0.00", "30.00")])
    assert _legs(_gl_rows(conn, f"{wo_id}:completion:2")) == sorted(
        [(fg, "108.00", "0.00"), (wip, "0.00", "60.00"),
         (wip, "0.00", "48.00")])


def test_cancel_reverses_whole_family(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start(conn, wo_id)
    _transfer(conn, env, wo_id, "10", "5")
    _transfer(conn, env, wo_id, "10", "5", posting_date=TRANSFER_DATE_2)
    _complete(conn, wo_id, "4")
    _complete(conn, wo_id, "3", posting_date=COMPLETION_DATE_2)
    assert _status(conn, wo_id) == "in_process"

    family = [wo_id, f"{wo_id}:transfer:2", f"{wo_id}:completion",
              f"{wo_id}:completion:2"]
    for voucher_id in family:
        assert _sle_rows(conn, voucher_id) != [], voucher_id

    r = call_action(M.cancel_work_order, conn, ns(
        work_order_id=wo_id, posting_date=CANCEL_DATE))
    assert is_ok(r), r
    assert r["document_status"] == "cancelled"

    placeholders = ", ".join("?" for _ in family)
    gl_rows = [dict(x) for x in conn.execute(
        "SELECT id, account_id, debit, credit, is_cancelled FROM gl_entry "
        f"WHERE voucher_type = 'work_order' AND voucher_id IN ({placeholders})",
        family).fetchall()]
    sle_rows = [dict(x) for x in conn.execute(
        "SELECT id, is_cancelled FROM stock_ledger_entry "
        f"WHERE voucher_type = 'work_order' AND voucher_id IN ({placeholders})",
        family).fetchall()]
    assert gl_rows != [] and sle_rows != []
    assert {x["is_cancelled"] for x in gl_rows} == {1}
    assert {x["is_cancelled"] for x in sle_rows} == {1}
    totals = defaultdict(lambda: [Decimal("0"), Decimal("0")])
    for x in gl_rows:
        totals[x["account_id"]][0] += Decimal(x["debit"])
        totals[x["account_id"]][1] += Decimal(x["credit"])
    for acct, (dr, cr) in totals.items():
        assert dr == cr, acct

    assert _active_qty(conn, env["rm1"], env["raw_wh"]) == "100.00"
    assert _active_qty(conn, env["rm2"], env["raw_wh"]) == "50.00"
    assert _active_qty(conn, env["rm1"], env["wip_wh"]) == "0"
    assert _active_qty(conn, env["rm2"], env["wip_wh"]) == "0"
    assert _active_qty(conn, env["fg_item"], env["fg_wh"]) == "0"
    assert _status(conn, wo_id) == "cancelled"


def test_full_completion_posts_finished_goods_against_wip(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start(conn, wo_id)
    _transfer(conn, env, wo_id, "20", "10")

    r = _complete(conn, wo_id, "10")
    assert r["rm_cost"] == "180.00"
    assert r["operating_cost"] == "0.00"
    assert r["fg_rate"] == "18.00"
    assert r["document_status"] == "completed"
    assert r["produced_qty"] == "10.00"

    comp = f"{wo_id}:completion"
    sle = _sle_rows(conn, comp)
    assert len(sle) == 3
    by_item = {x["item_id"]: x for x in sle}
    assert by_item[env["fg_item"]]["actual_qty"] == "10.00"
    assert by_item[env["fg_item"]]["stock_value_difference"] == "180.00"
    assert by_item[env["rm1"]]["actual_qty"] == "-20.00"
    assert by_item[env["rm1"]]["stock_value_difference"] == "-100.00"
    assert by_item[env["rm2"]]["actual_qty"] == "-10.00"
    assert by_item[env["rm2"]]["stock_value_difference"] == "-80.00"

    gl = _gl_rows(conn, comp)
    assert _gl_balanced(gl)
    accts = _per_account(gl)
    fg, wip = env["fg_acct"], env["wip_acct"]
    assert r["gl_count"] == 3
    assert len(gl) == 3
    assert len(accts) == 2
    assert env["srnb_acct"] not in accts
    assert env["cogs_acct"] not in accts
    assert _legs(gl) == sorted(
        [(fg, "180.00", "0.00"), (wip, "0.00", "100.00"),
         (wip, "0.00", "80.00")])
    assert accts[env["fg_acct"]] == ("180.00", "0.00")
    assert accts[env["wip_acct"]] == ("0.00", "180.00")


def test_partial_completions_post_finished_goods_against_wip(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start(conn, wo_id)
    _transfer(conn, env, wo_id, "20", "10")
    fg, wip = env["fg_acct"], env["wip_acct"]

    _complete(conn, wo_id, "4")
    assert _legs(_gl_rows(conn, f"{wo_id}:completion")) == sorted(
        [(fg, "72.00", "0.00"), (wip, "0.00", "40.00"),
         (wip, "0.00", "32.00")])

    _complete(conn, wo_id, "6", posting_date=COMPLETION_DATE_2)
    assert _legs(_gl_rows(conn, f"{wo_id}:completion:2")) == sorted(
        [(fg, "108.00", "0.00"), (wip, "0.00", "60.00"),
         (wip, "0.00", "48.00")])

    for voucher_id in (f"{wo_id}:completion", f"{wo_id}:completion:2"):
        accts = {r["account_id"] for r in _gl_rows(conn, voucher_id)}
        assert env["srnb_acct"] not in accts
        assert env["cogs_acct"] not in accts


def test_under_allocated_outputs_expense_the_remainder(conn):
    env = _ledger_env(conn)
    by_product = seed_item(conn, env["company_id"], name="By Product",
                           standard_rate="1.00")
    r = call_action(M.add_bom_output, conn, ns(
        bom_id=env["bom_id"], item_id=env["fg_item"], quantity="1",
        is_primary="1", cost_allocation_pct="80"))
    assert is_ok(r), r
    r = call_action(M.add_bom_output, conn, ns(
        bom_id=env["bom_id"], item_id=by_product, quantity="1",
        is_primary="0", cost_allocation_pct="10"))
    assert is_ok(r), r

    wo_id = _work_order(conn, env)
    _start(conn, wo_id)
    _transfer(conn, env, wo_id, "20", "10")
    _complete(conn, wo_id, "10")

    comp = f"{wo_id}:completion"
    sle = _sle_rows(conn, comp)
    incoming = sorted(
        (x["item_id"], x["stock_value_difference"], x["warehouse_id"])
        for x in sle if Decimal(x["actual_qty"]) > 0)
    assert incoming == sorted(
        [(env["fg_item"], "144.00", env["fg_wh"]),
         (by_product, "18.00", env["fg_wh"])])

    fg, wip, cogs = env["fg_acct"], env["wip_acct"], env["cogs_acct"]
    gl = _gl_rows(conn, comp)
    assert _gl_balanced(gl)
    assert _legs(gl) == sorted(
        [(fg, "144.00", "0.00"), (fg, "18.00", "0.00"),
         (wip, "0.00", "100.00"), (wip, "0.00", "80.00"),
         (cogs, "18.00", "0.00")])


def test_rounding_and_operating_cost(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start(conn, wo_id)
    _job_card_setup(conn, wo_id, hour_rate="60.00", minutes="20")
    _transfer(conn, env, wo_id, "20", "10")

    comp1 = f"{wo_id}:completion"
    r1 = _complete(conn, wo_id, "3")
    assert r1["rm_cost"] == "54.00"
    assert r1["operating_cost"] == "20.00"
    assert r1["production_cost"] == "74.00"
    assert r1["fg_rate"] == "24.67"
    sle1 = _sle_rows(conn, comp1)
    fg_legs_1 = [x for x in sle1 if x["item_id"] == env["fg_item"]]
    assert len(fg_legs_1) == 1
    assert fg_legs_1[0]["stock_value_difference"] == "74.00"

    comp2 = f"{wo_id}:completion:2"
    r2 = _complete(conn, wo_id, "7", posting_date=COMPLETION_DATE_2)
    assert r2["rm_cost"] == "126.00"
    assert r2["operating_cost"] == "0.00"
    assert r2["production_cost"] == "126.00"
    assert r2["document_status"] == "completed"
    sle2 = _sle_rows(conn, comp2)
    fg_legs_2 = [x for x in sle2 if x["item_id"] == env["fg_item"]]
    assert len(fg_legs_2) == 1
    assert (Decimal(fg_legs_1[0]["stock_value_difference"])
            + Decimal(fg_legs_2[0]["stock_value_difference"])) == Decimal("200.00")


def test_by_product_partial_refused(conn):
    env = _ledger_env(conn)
    by_product = seed_item(conn, env["company_id"], name="By Product",
                           standard_rate="1.00")
    r = call_action(M.add_bom_output, conn, ns(
        bom_id=env["bom_id"], item_id=env["fg_item"], quantity="1",
        is_primary="1", cost_allocation_pct="80"))
    assert is_ok(r), r
    r = call_action(M.add_bom_output, conn, ns(
        bom_id=env["bom_id"], item_id=by_product, quantity="1",
        is_primary="0", cost_allocation_pct="20"))
    assert is_ok(r), r

    wo_id = _work_order(conn, env)
    _start(conn, wo_id)
    _transfer(conn, env, wo_id, "20", "10")

    before_gl = conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0]
    before_sle = conn.execute("SELECT COUNT(*) FROM stock_ledger_entry").fetchone()[0]
    before_produced = conn.execute(
        "SELECT produced_qty FROM work_order WHERE id = ?", (wo_id,)).fetchone()["produced_qty"]

    r = call_action(M.complete_work_order, conn, ns(
        work_order_id=wo_id, produced_qty="4", posting_date=COMPLETION_DATE))
    assert is_error(r)
    assert r["message"] == ("Partial completion is not supported for a BOM with "
                            "co-products or by-products; complete the remaining "
                            "quantity in one step.")
    assert conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0] == before_gl
    assert conn.execute("SELECT COUNT(*) FROM stock_ledger_entry").fetchone()[0] == before_sle
    assert conn.execute(
        "SELECT produced_qty FROM work_order WHERE id = ?", (wo_id,)).fetchone()["produced_qty"] == before_produced


def test_legacy_part_completed_work_order(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start(conn, wo_id)

    M.insert_sle_entries(conn, [
        {"item_id": env["rm1"], "warehouse_id": env["wip_wh"], "actual_qty": "20",
         "incoming_rate": "5.00"},
        {"item_id": env["rm2"], "warehouse_id": env["wip_wh"], "actual_qty": "10",
         "incoming_rate": "8.00"},
    ], voucher_type="stock_entry", voucher_id="seed-wip-" + wo_id[:8],
        posting_date="2026-06-01", company_id=env["company_id"])
    conn.commit()

    _transfer(conn, env, wo_id, "20", "10")

    comp1 = f"{wo_id}:completion"
    M.insert_sle_entries(conn, [
        {"item_id": env["fg_item"], "warehouse_id": env["fg_wh"], "actual_qty": "4",
         "incoming_rate": "45.00"},
        {"item_id": env["rm1"], "warehouse_id": env["wip_wh"], "actual_qty": "-20",
         "incoming_rate": "0"},
        {"item_id": env["rm2"], "warehouse_id": env["wip_wh"], "actual_qty": "-10",
         "incoming_rate": "0"},
    ], voucher_type="work_order", voucher_id=comp1,
        posting_date=COMPLETION_DATE, company_id=env["company_id"])
    conn.execute("UPDATE work_order SET produced_qty = '4.00' WHERE id = ?", (wo_id,))
    conn.execute(
        "UPDATE work_order_item SET consumed_qty = '8.00' "
        "WHERE work_order_id = ? AND item_id = ?", (wo_id, env["rm1"]))
    conn.execute(
        "UPDATE work_order_item SET consumed_qty = '4.00' "
        "WHERE work_order_id = ? AND item_id = ?", (wo_id, env["rm2"]))
    conn.commit()

    comp2 = f"{wo_id}:completion:2"
    r = _complete(conn, wo_id, "6", posting_date=COMPLETION_DATE_2)
    assert r["rm_cost"] == "0.00"
    assert r["document_status"] == "completed"
    assert r["produced_qty"] == "10.00"

    sle2 = _sle_rows(conn, comp2)
    assert sle2 != []
    assert all(x["warehouse_id"] != env["wip_wh"] for x in sle2)

    items = _wo_items(conn, wo_id)
    assert Decimal(items[env["rm1"]]["consumed_qty"]) == Decimal(items[env["rm1"]]["transferred_qty"]) == Decimal("20")
    assert Decimal(items[env["rm2"]]["consumed_qty"]) == Decimal(items[env["rm2"]]["transferred_qty"]) == Decimal("10")
    assert _active_qty(conn, env["rm1"], env["wip_wh"]) == "20.00"
    assert _active_qty(conn, env["rm2"], env["wip_wh"]) == "10.00"


def test_near_miss_voucher_ids_are_not_part_of_the_family(conn):
    env = _ledger_env(conn)
    wo_id = _work_order(conn, env)
    _start(conn, wo_id)
    _transfer(conn, env, wo_id, "20", "10")
    assert wo_id != wo_id.upper()
    planted = [wo_id.upper() + ":completion:2", wo_id + "X:completion:2"]
    for vid in planted:
        M.insert_sle_entries(conn, [
            {"item_id": env["rm1"], "warehouse_id": env["wip_wh"],
             "actual_qty": "-4", "incoming_rate": "0"},
            {"item_id": env["rm2"], "warehouse_id": env["wip_wh"],
             "actual_qty": "-2", "incoming_rate": "0"},
        ], voucher_type="work_order", voucher_id=vid,
            posting_date="2026-06-10", company_id=env["company_id"])
        conn.commit()
    r = _complete(conn, wo_id, "4")
    assert r["rm_cost"] == "72.00"
    assert r["fg_rate"] == "18.00"
    sle = _sle_rows(conn, f"{wo_id}:completion")
    assert _wip_issue_qty(sle, env["rm1"], env["wip_wh"]) == "-8.00"
    assert _wip_issue_qty(sle, env["rm2"], env["wip_wh"]) == "-4.00"
    assert _sle_rows(conn, f"{wo_id}:completion:2") == []
    sle_t = M.Table("stock_ledger_entry")
    for vid in planted:
        q = (M.Q.from_(sle_t).select(sle_t.is_cancelled)
             .where(sle_t.voucher_type == M.P())
             .where(sle_t.voucher_id == M.P()))
        rows = conn.execute(q.get_sql(), ("work_order", vid)).fetchall()
        assert rows != []
        assert all(x["is_cancelled"] == 0 for x in rows)
    rc = call_action(M.cancel_work_order, conn, ns(
        work_order_id=wo_id, posting_date=CANCEL_DATE))
    assert is_ok(rc), rc
    for vid in planted:
        q = (M.Q.from_(sle_t).select(sle_t.is_cancelled)
             .where(sle_t.voucher_type == M.P())
             .where(sle_t.voucher_id == M.P()))
        rows = conn.execute(q.get_sql(), ("work_order", vid)).fetchall()
        assert rows != []
        assert all(x["is_cancelled"] == 0 for x in rows)


def test_add_bom_refuses_duplicate_raw_material(conn):
    env = _ledger_env(conn)
    bom_t = M.Table("bom")
    bi_t = M.Table("bom_item")
    ns_t = M.Table("naming_series")
    bom_q = M.Q.from_(bom_t).select(M.fn.Count("*"))
    bi_q = M.Q.from_(bi_t).select(M.fn.Count("*"))
    ns_q = (M.Q.from_(ns_t).select(ns_t.current_value)
            .where(ns_t.entity_type == M.P())
            .where(ns_t.company_id == M.P()))
    bom_before = conn.execute(bom_q.get_sql(), ()).fetchone()[0]
    bi_before = conn.execute(bi_q.get_sql(), ()).fetchone()[0]
    ns_before = conn.execute(
        ns_q.get_sql(), ("bom", env["company_id"])).fetchone()["current_value"]
    r = call_action(M.add_bom, conn, ns(
        item_id=env["fg_item"], company_id=env["company_id"], quantity="1",
        items=json.dumps([{"item_id": env["rm1"], "quantity": "2"},
                          {"item_id": env["rm2"], "quantity": "1"},
                          {"item_id": env["rm1"], "quantity": "3"}])))
    assert is_error(r)
    assert r["message"] == (
        f"Item 2: item_id {env['rm1']} is already listed as item 0; "
        "list each raw material once")
    assert conn.execute(bom_q.get_sql(), ()).fetchone()[0] == bom_before
    assert conn.execute(bi_q.get_sql(), ()).fetchone()[0] == bi_before
    assert conn.execute(
        ns_q.get_sql(), ("bom", env["company_id"])).fetchone()["current_value"] == ns_before


def test_update_bom_refuses_duplicate_raw_material(conn):
    env = _ledger_env(conn)
    bi_t = M.Table("bom_item")
    bom_t = M.Table("bom")
    items_q = (M.Q.from_(bi_t).select(bi_t.id, bi_t.item_id, bi_t.quantity)
               .where(bi_t.bom_id == M.P()))
    rows_before = [dict(x) for x in conn.execute(
        items_q.get_sql(), (env["bom_id"],)).fetchall()]
    cost_q = (M.Q.from_(bom_t).select(bom_t.raw_material_cost)
              .where(bom_t.id == M.P()))
    cost_before = conn.execute(
        cost_q.get_sql(), (env["bom_id"],)).fetchone()["raw_material_cost"]
    r = call_action(M.update_bom, conn, ns(
        bom_id=env["bom_id"],
        items=json.dumps([{"item_id": env["rm2"], "quantity": "1"},
                          {"item_id": env["rm2"], "quantity": "4"}])))
    assert is_error(r)
    assert r["message"] == (
        f"Item 1: item_id {env['rm2']} is already listed as item 0; "
        "list each raw material once")
    rows_after = [dict(x) for x in conn.execute(
        items_q.get_sql(), (env["bom_id"],)).fetchall()]
    cost_after = conn.execute(
        cost_q.get_sql(), (env["bom_id"],)).fetchone()["raw_material_cost"]
    assert rows_after == rows_before
    assert cost_after == cost_before
