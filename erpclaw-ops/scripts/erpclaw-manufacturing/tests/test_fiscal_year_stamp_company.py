"""Postings stamp the posting company's own fiscal year, not another company's.

Company B (decoy) is seeded FIRST with open year FY-DECOY 2025-07-01..2026-12-31,
then company A is built with its own year renamed to FY-A-2026. A company-blind
lookup returns the first row of any company (FY-DECOY); the company-scoped
lookup must return FY-A-2026 for A's postings.
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

D = Decimal

DECOY_YEAR = "FY-DECOY"
OWN_YEAR = "FY-A-2026"
TRANSFER_DATE = "2026-06-05"
COMPLETION_DATE = "2026-06-15"


def _u():
    return str(uuid.uuid4())


def _seed_decoy(conn):
    """Seed decoy company B and its open year FIRST (row order matters)."""
    bid = seed_company(conn, name="Decoy Co", abbr="DCY")
    conn.execute(
        "INSERT INTO fiscal_year (id, name, start_date, end_date, is_closed, company_id)"
        " VALUES (?, ?, ?, ?, 0, ?)",
        (_u(), DECOY_YEAR, "2025-07-01", "2026-12-31", bid))
    conn.commit()
    return bid


def _warehouse(conn, company_id, name, account_id):
    wid = _u()
    conn.execute(
        "INSERT INTO warehouse (id, name, company_id, account_id) VALUES (?, ?, ?, ?)",
        (wid, name, company_id, account_id))
    return wid


def _ledger_env(conn, year_name="FY2026"):
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    conn.execute(
        "INSERT INTO fiscal_year (id, name, start_date, end_date, is_closed, company_id)"
        " VALUES (?, ?, ?, ?, 0, ?)",
        (_u(), year_name, "2026-01-01", "2026-12-31", cid))
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


def _env_a(conn):
    _seed_decoy(conn)
    env = _ledger_env(conn)
    conn.execute("UPDATE fiscal_year SET name = ? WHERE company_id = ?",
                 (OWN_YEAR, env["company_id"]))
    conn.commit()
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


def _transfer(conn, env, wo_id, rm1_qty="20", rm2_qty="10",
              posting_date=TRANSFER_DATE):
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
        "SELECT account_id, debit, credit, fiscal_year FROM gl_entry"
        " WHERE voucher_type = 'work_order' AND voucher_id = ? AND is_cancelled = 0",
        (voucher_id,)).fetchall()]


def _sle_rows(conn, voucher_id):
    return [dict(r) for r in conn.execute(
        "SELECT item_id, warehouse_id, actual_qty, fiscal_year FROM stock_ledger_entry"
        " WHERE voucher_type = 'work_order' AND voucher_id = ? AND is_cancelled = 0",
        (voucher_id,)).fetchall()]


def test_transfer_materials_stamps_own_company_year(conn):
    env = _env_a(conn)
    wo_id = _work_order(conn, env)
    _start(conn, wo_id)
    _transfer(conn, env, wo_id)

    sle = _sle_rows(conn, wo_id)
    assert len(sle) == 4
    assert sorted(s["actual_qty"] for s in sle) == ["-10.00", "-20.00", "10.00", "20.00"]
    assert all(s["fiscal_year"] == OWN_YEAR for s in sle), sle

    gl = _gl_rows(conn, wo_id)
    assert gl, "the transfer posts perpetual GL for its SLE rows"
    assert all(g["fiscal_year"] == OWN_YEAR for g in gl), gl
    assert sum((D(g["debit"]) for g in gl), D("0")) == D("180.00")
    assert sum((D(g["credit"]) for g in gl), D("0")) == D("180.00")


def test_complete_work_order_stamps_own_company_year(conn):
    env = _env_a(conn)
    wo_id = _work_order(conn, env)
    _start(conn, wo_id)
    _transfer(conn, env, wo_id)
    _complete(conn, wo_id, "10")

    voucher_id = wo_id + ":completion"
    sle = _sle_rows(conn, voucher_id)
    assert sle, "the completion writes stock ledger rows"
    assert all(s["fiscal_year"] == OWN_YEAR for s in sle), sle

    gl = _gl_rows(conn, voucher_id)
    assert gl, "the completion posts perpetual GL for its SLE rows"
    assert all(g["fiscal_year"] == OWN_YEAR for g in gl), gl
    assert sum((D(g["debit"]) for g in gl), D("0")) == \
        sum((D(g["credit"]) for g in gl), D("0"))


def test_transfer_materials_refused_without_own_company_year(conn):
    _seed_decoy(conn)
    env = _ledger_env(conn)
    conn.execute("DELETE FROM fiscal_year WHERE company_id = ?",
                 (env["company_id"],))
    conn.commit()
    wo_id = _work_order(conn, env)
    _start(conn, wo_id)

    bad = call_action(M.transfer_materials, conn, ns(
        work_order_id=wo_id, posting_date=TRANSFER_DATE,
        items=json.dumps([{"item_id": env["rm1"], "qty": "20"},
                          {"item_id": env["rm2"], "qty": "10"}])))
    assert is_error(bad), bad
    msg = bad.get("message", "") + bad.get("error", "")
    assert ("GL Validation Step 9 Failed: No open fiscal year found for posting date "
            + TRANSFER_DATE) in msg, msg
    assert _gl_rows(conn, wo_id) == []


def test_receive_subcontracted_items_stamps_own_company_year(conn, db_path, tmp_path):
    import subcontract_helpers as sc

    _seed_decoy(conn)
    sc.deploy_skills(tmp_path, db_path)
    ids = sc.seed_subcontract_env(conn, raw_rate="20.00", raw_per_fg="2",
                                  order_qty="100")
    conn.execute("UPDATE fiscal_year SET name = ? WHERE company_id = ?",
                 (OWN_YEAR, ids["company"]))
    conn.commit()
    mfg = M

    r = sc.call(mfg.add_subcontracting_order, conn, db_path,
                supplier_id=ids["supplier"], bom_id=ids["bom"], quantity="100",
                company_id=ids["company"], service_item_id=ids["service_item"],
                supplier_warehouse_id=ids["sub_wh"])
    assert r["status"] == "ok", r
    oid = r["subcontracting_order_id"]
    assert sc.call(mfg.submit_subcontracting_order, conn, db_path,
                   id=oid)["status"] == "ok"
    assert sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path,
                    order=oid, posting_date="2026-02-01")["status"] == "ok"

    rcv = sc.call(mfg.receive_subcontracted_items, conn, db_path, order=oid,
                   received_qty="60", subcontract_charge_rate="5.00",
                   posting_date="2026-02-05")
    assert rcv["status"] == "ok", rcv
    assert D(rcv["fg_total_cost"]) == D("2700.00")

    sle = [dict(x) for x in conn.execute(
        "SELECT voucher_id, actual_qty, fiscal_year FROM stock_ledger_entry"
        " WHERE voucher_type = 'purchase_receipt' AND voucher_id LIKE ?"
        " AND is_cancelled = 0", (oid + ":receive:%",)).fetchall()]
    assert len(sle) == 1
    assert D(sle[0]["actual_qty"]) == D("60.00")
    assert sle[0]["fiscal_year"] == OWN_YEAR, sle

    gl = [dict(x) for x in conn.execute(
        "SELECT account_id, debit, credit, fiscal_year FROM gl_entry"
        " WHERE voucher_type = 'purchase_receipt' AND voucher_id = ?"
        " AND is_cancelled = 0", (sle[0]["voucher_id"],)).fetchall()]
    assert gl, "the FG receipt posts perpetual GL for its SLE row"
    assert all(g["fiscal_year"] == OWN_YEAR for g in gl), gl
    assert sum((D(g["debit"]) for g in gl), D("0")) == D("2700.00")
    assert sum((D(g["credit"]) for g in gl), D("0")) == D("2700.00")
