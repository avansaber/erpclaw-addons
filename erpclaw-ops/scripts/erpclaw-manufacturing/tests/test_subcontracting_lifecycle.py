"""L1: Wave 2 S5 — subcontracting lifecycle (each transition happy + error).

draft -> submitted -> partially_received -> completed, plus the two cancel paths.
Real cross-skill subprocess calls (transfer/receive/cancel) run against a fresh
full-schema SQLite via a throwaway deployed-skills layout (subcontract_helpers).
Money is asserted as exact Decimal-as-text.

Covered:
  - submit happy + error (non-draft; missing service line on BOM)
  - transfer happy (full) + error (before submit; over-transfer)
  - receive partial 60 + 40 = 100 -> completed sets final_received_at + over-receive block
  - cancel-subcontracting-order happy (pre-transfer) + error (after transfer)
  - cancel-subcontract-transfer reverses the transfer SLE (cancel = reverse)
  - post-receipt cancel BLOCKED (both cancel paths)
"""
import json
import os
import subprocess
import sys
from decimal import Decimal

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import subcontract_helpers as sc  # noqa: E402
from mfg_helpers import init_all_tables, get_conn  # noqa: E402


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "subcontract.sqlite")
    init_all_tables(path)
    os.environ["ERPCLAW_DB_PATH"] = path
    yield path
    os.environ.pop("ERPCLAW_DB_PATH", None)
    os.environ.pop("OPENCLAW_SKILLS_DIR", None)


@pytest.fixture
def conn(db_path):
    c = get_conn(db_path)
    yield c
    c.close()


@pytest.fixture
def mfg():
    return sc.load_mfg()


@pytest.fixture
def env(conn, db_path, tmp_path, mfg):
    sc.deploy_skills(tmp_path, db_path)
    ids = sc.seed_subcontract_env(conn, raw_rate="20.00", raw_per_fg="2",
                                  order_qty="100")
    return ids


def _add(conn, db_path, mfg, env, qty="100"):
    r = sc.call(mfg.add_subcontracting_order, conn, db_path,
                supplier_id=env["supplier"], bom_id=env["bom"], quantity=qty,
                company_id=env["company"], service_item_id=env["service_item"],
                supplier_warehouse_id=env["sub_wh"])
    assert r["status"] == "ok", r
    return r["subcontracting_order_id"]


# --------------------------------------------------------------------------- #
# submit
# --------------------------------------------------------------------------- #

def test_submit_happy(conn, db_path, mfg, env):
    oid = _add(conn, db_path, mfg, env)
    r = sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    assert r["status"] == "ok", r
    assert conn.execute("SELECT status FROM subcontracting_order WHERE id=?",
                        (oid,)).fetchone()["status"] == "submitted"


def test_submit_already_submitted_error(conn, db_path, mfg, env):
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    r = sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    assert r["status"] == "error"
    assert "draft" in r["message"].lower()


def test_submit_rejects_bom_without_service_line(conn, db_path, mfg, env):
    """A service_item not listed on the BOM blocks submit."""
    # New service item that is NOT a BOM line.
    orphan_svc = sc._u()
    conn.execute(
        "INSERT INTO item (id, item_name, item_code, stock_uom, standard_rate, "
        "is_stock_item) VALUES (?,?,?,?,?,0)",
        (orphan_svc, "Orphan Svc", f"OSVC-{orphan_svc[:8]}", "Each", "0"))
    conn.commit()
    r = sc.call(mfg.add_subcontracting_order, conn, db_path,
                supplier_id=env["supplier"], bom_id=env["bom"], quantity="10",
                company_id=env["company"], service_item_id=orphan_svc,
                supplier_warehouse_id=env["sub_wh"])
    oid = r["subcontracting_order_id"]
    sr = sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    assert sr["status"] == "error"
    assert "service" in sr["message"].lower()


# --------------------------------------------------------------------------- #
# transfer
# --------------------------------------------------------------------------- #

def test_transfer_before_submit_error(conn, db_path, mfg, env):
    oid = _add(conn, db_path, mfg, env)
    r = sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path, order=oid)
    assert r["status"] == "error"
    assert "submitted" in r["message"].lower()


def test_transfer_full_happy(conn, db_path, mfg, env):
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    r = sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path, order=oid,
                posting_date="2026-02-01")
    assert r["status"] == "ok", r
    assert r["materials_transferred"] == "100.00"
    assert r["stock_entry_id"]
    # The transfer moved ONLY stock items (2 brackets/FG × 100 = 200), not the svc.
    se_items = conn.execute(
        "SELECT item_id, quantity FROM stock_entry_item WHERE stock_entry_id=?",
        (r["stock_entry_id"],)).fetchall()
    assert len(se_items) == 1
    assert se_items[0]["item_id"] == env["raw_item"]
    assert Decimal(se_items[0]["quantity"]) == Decimal("200.00")


def test_transfer_over_transfer_error(conn, db_path, mfg, env):
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path, order=oid)
    r = sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path, order=oid)
    assert r["status"] == "error"
    assert "fully transferred" in r["message"].lower()


# --------------------------------------------------------------------------- #
# receive — partial + completion + over-receive
# --------------------------------------------------------------------------- #

def test_partial_receipts_complete_and_set_timestamp(conn, db_path, mfg, env):
    """60 + 40 = 100 -> completed; final_received_at set only at completion."""
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path, order=oid,
            posting_date="2026-02-01")

    r1 = sc.call(mfg.receive_subcontracted_items, conn, db_path, order=oid,
                 received_qty="60", subcontract_charge_rate="5.00",
                 posting_date="2026-02-05")
    assert r1["status"] == "ok", r1
    o1 = conn.execute("SELECT status, received_qty, final_received_at "
                     "FROM subcontracting_order WHERE id=?", (oid,)).fetchone()
    assert o1["status"] == "partially_received"
    assert o1["received_qty"] == "60.00"
    assert o1["final_received_at"] is None      # not complete yet
    assert r1["fg_total_cost"] == "2700.00"     # 60×(40 raw + 5 charge)

    r2 = sc.call(mfg.receive_subcontracted_items, conn, db_path, order=oid,
                 received_qty="40", posting_date="2026-02-08")
    assert r2["status"] == "ok", r2
    o2 = conn.execute("SELECT status, received_qty, final_received_at "
                     "FROM subcontracting_order WHERE id=?", (oid,)).fetchone()
    assert o2["status"] == "completed"
    assert o2["received_qty"] == "100.00"
    assert o2["final_received_at"] is not None
    assert r2["fg_total_cost"] == "1800.00"     # 40×(40 raw + 5 charge); rate inherited

    # Two distinct FG SLE (one per receipt event), each balanced.
    fg = conn.execute(
        "SELECT COUNT(*) FROM stock_ledger_entry WHERE voucher_id LIKE ? "
        "AND is_cancelled=0", (oid + ":receive:%",)).fetchone()[0]
    assert fg == 2


def test_over_receive_blocked(conn, db_path, mfg, env):
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path, order=oid)
    r = sc.call(mfg.receive_subcontracted_items, conn, db_path, order=oid,
                received_qty="120", subcontract_charge_rate="5.00")
    assert r["status"] == "error"
    assert "exceed" in r["message"].lower()


def test_receive_before_transfer_blocked(conn, db_path, mfg, env):
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    r = sc.call(mfg.receive_subcontracted_items, conn, db_path, order=oid,
                received_qty="10", subcontract_charge_rate="5.00")
    assert r["status"] == "error"
    assert "transferred" in r["message"].lower()


# --------------------------------------------------------------------------- #
# cancel paths
# --------------------------------------------------------------------------- #

def test_cancel_order_pre_transfer_happy(conn, db_path, mfg, env):
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    r = sc.call(mfg.cancel_subcontracting_order, conn, db_path, id=oid,
                reason="customer cancelled")
    assert r["status"] == "ok", r
    assert conn.execute("SELECT status FROM subcontracting_order WHERE id=?",
                        (oid,)).fetchone()["status"] == "cancelled"


def test_cancel_order_after_transfer_blocked(conn, db_path, mfg, env):
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path, order=oid)
    r = sc.call(mfg.cancel_subcontracting_order, conn, db_path, id=oid,
                reason="late")
    assert r["status"] == "error"
    assert "transferred" in r["message"].lower()


def test_cancel_transfer_reverses_sle(conn, db_path, mfg, env):
    """cancel-subcontract-transfer reverses the transfer SLE (cancel = reverse:
    mirror entries, the original is not deleted)."""
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    t = sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path, order=oid)
    se_id = t["stock_entry_id"]

    sle_for_se = conn.execute(
        "SELECT COUNT(*) FROM stock_ledger_entry WHERE voucher_id=? AND voucher_type="
        "'stock_entry'", (se_id,)).fetchone()[0]
    assert sle_for_se > 0

    r = sc.call(mfg.cancel_subcontract_transfer, conn, db_path, stock_entry=se_id,
                order=oid, reason="wrong items shipped")
    assert r["status"] == "ok", r
    # cancel = reverse: original SLE stay, but cancelled (mirror inserted). The net
    # active qty for the original voucher nets to zero / rows flagged cancelled.
    orig = conn.execute(
        "SELECT is_cancelled FROM stock_ledger_entry WHERE voucher_id=?",
        (se_id,)).fetchall()
    assert any(row["is_cancelled"] == 1 for row in orig), "original SLE must be cancelled, not deleted"
    # materials_transferred walked back to 0; order back to 'submitted'.
    o = conn.execute("SELECT materials_transferred, status FROM subcontracting_order "
                    "WHERE id=?", (oid,)).fetchone()
    assert Decimal(o["materials_transferred"]) == Decimal("0")
    assert o["status"] == "submitted"


def test_cancel_transfer_after_receipt_blocked(conn, db_path, mfg, env):
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    t = sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path, order=oid)
    sc.call(mfg.receive_subcontracted_items, conn, db_path, order=oid,
            received_qty="50", subcontract_charge_rate="5.00")
    r = sc.call(mfg.cancel_subcontract_transfer, conn, db_path,
                stock_entry=t["stock_entry_id"], order=oid, reason="too late")
    assert r["status"] == "error"
    assert "received" in r["message"].lower()


def test_cancel_order_after_receipt_blocked(conn, db_path, mfg, env):
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path, order=oid)
    sc.call(mfg.receive_subcontracted_items, conn, db_path, order=oid,
            received_qty="50", subcontract_charge_rate="5.00")
    r = sc.call(mfg.cancel_subcontracting_order, conn, db_path, id=oid, reason="x")
    assert r["status"] == "error"


# --------------------------------------------------------------------------- #
# read side
# --------------------------------------------------------------------------- #

# --------------------------------------------------------------------------- #
# buying-delegated single-post (§Decision 6)
# --------------------------------------------------------------------------- #

def test_buying_delegation_single_post(conn, db_path, mfg, env):
    """create-purchase-receipt --subcontracting-order-id defers to the subcontract
    path and posts EXACTLY ONE FG SLE + one balanced GL pair (no double-post)."""
    oid = _add(conn, db_path, mfg, env, qty="50")
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    assert sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path,
                   order=oid, posting_date="2026-03-01")["status"] == "ok"

    gl_b, sle_b = sc.gl_count(conn), sc.active_sle_count(conn)
    cmd = ["python3", sc._FOUND_ROUTER, "--action", "create-purchase-receipt",
           "--subcontracting-order-id", oid, "--received-qty", "50",
           "--subcontract-charge-rate", "5.00", "--posting-date", "2026-03-05",
           "--db-path", db_path, "--user-confirmed"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    resp = json.loads(proc.stdout)
    assert resp["delegated_to"] == "receive-subcontracted-items"

    gl_a, sle_a = sc.gl_count(conn), sc.active_sle_count(conn)
    assert (sle_a - sle_b) == 1            # one FG SLE
    assert (gl_a - gl_b) == 2              # one balanced GL pair
    fg = conn.execute(
        "SELECT COUNT(*) FROM stock_ledger_entry WHERE voucher_id LIKE ? "
        "AND is_cancelled=0", (oid + ":receive:%",)).fetchone()[0]
    assert fg == 1


def test_buying_no_subcontracting_order_still_normal(conn, db_path, mfg, env):
    """create-purchase-receipt WITHOUT --subcontracting-order-id is unaffected
    (it still requires a PO and posts its own receipt — no delegation)."""
    cmd = ["python3", sc._FOUND_ROUTER, "--action", "create-purchase-receipt",
           "--db-path", db_path, "--user-confirmed"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    # No PO id and no subcontracting order -> the normal "purchase-order-id is
    # required" error path, NOT the delegation path.
    out = json.loads(proc.stdout)
    assert out["status"] == "error"
    assert "delegated_to" not in out


def test_get_and_list(conn, db_path, mfg, env):
    oid = _add(conn, db_path, mfg, env)
    g = sc.call(mfg.get_subcontracting_order, conn, db_path, id=oid)
    assert g["status"] == "ok"
    assert g["outstanding_transfer_qty"] == "100.00"
    assert g["outstanding_receive_qty"] == "100.00"
    lst = sc.call(mfg.list_subcontracting_orders, conn, db_path,
                  company_id=env["company"])
    assert lst["status"] == "ok"
    assert lst["count"] >= 1
