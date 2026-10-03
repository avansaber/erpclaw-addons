import json
import os
import sys
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from mfg_helpers import call_action, is_error, is_ok, ns, seed_item  # noqa: E402
from test_work_order_partial_completion import (  # noqa: E402
    M,
    COMPLETION_DATE,
    _active_qty,
    _complete,
    _ledger_env,
    _start,
    _transfer,
    _work_order,
)


def _snapshot(conn, wo_id):
    sle = conn.execute("SELECT COUNT(*) FROM stock_ledger_entry").fetchone()[0]
    gl = conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0]
    audit = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
    wo = conn.execute(
        "SELECT produced_qty, status FROM work_order WHERE id = ?", (wo_id,)).fetchone()
    consumed = {
        r["item_id"]: r["consumed_qty"]
        for r in conn.execute(
            "SELECT item_id, consumed_qty FROM work_order_item WHERE work_order_id = ?",
            (wo_id,)).fetchall()
    }
    return (sle, gl, audit, wo["produced_qty"], wo["status"], consumed)


def _assert_unchanged(conn, wo_id, before):
    after = _snapshot(conn, wo_id)
    assert after == before


def _expected_message(total, qty, pairs):
    short = "; ".join(
        f"{item_id} needs {need}, transferred {transferred}"
        for item_id, need, transferred in sorted(pairs))
    return (
        "Cannot complete Work Order: materials transferred to WIP do not cover "
        f"the quantity being completed ({total} of {qty}). "
        f"Short: {short}. Transfer the missing materials with transfer-materials first.")


def test_complete_with_nothing_transferred_refused(conn):
    env = _ledger_env(conn)
    wo = _work_order(conn, env, qty="4")
    _start(conn, wo)
    before = _snapshot(conn, wo)
    r = call_action(M.complete_work_order, conn, ns(
        work_order_id=wo, produced_qty="4", posting_date=COMPLETION_DATE))
    assert is_error(r)
    expected = _expected_message(
        "4.00", "4.00",
        [(env["rm1"], "8.00", "0.00"), (env["rm2"], "4.00", "0.00")])
    assert r["message"] == expected
    _assert_unchanged(conn, wo, before)


def test_partial_short_refused_then_covered_partial_ok(conn):
    env = _ledger_env(conn)
    wo = _work_order(conn, env, qty="10")
    _start(conn, wo)
    _transfer(conn, env, wo, "10", "5")
    before = _snapshot(conn, wo)
    r = call_action(M.complete_work_order, conn, ns(
        work_order_id=wo, produced_qty="6", posting_date=COMPLETION_DATE))
    assert is_error(r)
    expected = _expected_message(
        "6.00", "10.00",
        [(env["rm1"], "12.00", "10.00"), (env["rm2"], "6.00", "5.00")])
    assert r["message"] == expected
    _assert_unchanged(conn, wo, before)
    r2 = _complete(conn, wo, "5")
    assert r2["rm_cost"] == "90.00"
    assert r2["fg_rate"] == "18.00"
    assert r2["document_status"] == "in_process"


def test_final_short_refused(conn):
    env = _ledger_env(conn)
    wo = _work_order(conn, env, qty="10")
    _start(conn, wo)
    _transfer(conn, env, wo, "10", "5")
    before = _snapshot(conn, wo)
    r = call_action(M.complete_work_order, conn, ns(
        work_order_id=wo, produced_qty="10", posting_date=COMPLETION_DATE))
    assert is_error(r)
    expected = _expected_message(
        "10.00", "10.00",
        [(env["rm1"], "20.00", "10.00"), (env["rm2"], "10.00", "5.00")])
    assert r["message"] == expected
    _assert_unchanged(conn, wo, before)


def test_overproduction_needs_only_required(conn):
    env = _ledger_env(conn)
    wo = _work_order(conn, env, qty="10")
    _start(conn, wo)
    _transfer(conn, env, wo, "20", "10")
    r = _complete(conn, wo, "12")
    assert r["produced_qty"] == "12.00"
    assert r["rm_cost"] == "180.00"
    assert r["fg_rate"] == "15.00"


def test_zero_valued_component_transferred_posts_at_zero(conn):
    env = _ledger_env(conn)
    fg_item = seed_item(conn, env["company_id"], name="Zero FG", standard_rate="25.00")
    component = seed_item(conn, env["company_id"], name="Zero RM", standard_rate="0")
    M.insert_sle_entries(
        conn,
        [
            {
                "item_id": component,
                "warehouse_id": env["raw_wh"],
                "actual_qty": "10",
                "incoming_rate": "0",
            }
        ],
        voucher_type="stock_entry",
        voucher_id="opening-zero-" + env["company_id"][:8],
        posting_date="2026-01-05",
        company_id=env["company_id"],
    )
    conn.commit()
    r = call_action(M.add_bom, conn, ns(
        item_id=fg_item, company_id=env["company_id"], quantity="1",
        items=json.dumps([{"item_id": component, "quantity": "2", "rate": "0"}])))
    assert is_ok(r), r
    wo = _work_order(conn, {**env, "bom_id": r["bom_id"]}, qty="4")
    _start(conn, wo)
    r = call_action(M.transfer_materials, conn, ns(
        work_order_id=wo, posting_date="2026-06-05",
        items=json.dumps([{"item_id": component, "qty": "8"}])))
    assert is_ok(r), r
    r = _complete(conn, wo, "4")
    assert r["production_cost"] == "0.00"
    vid = f"{wo}:completion"
    sle_t = M.Table("stock_ledger_entry")
    q = (
        M.Q.from_(sle_t)
        .select(sle_t.incoming_rate, sle_t.stock_value_difference)
        .where(sle_t.voucher_type == M.P())
        .where(sle_t.voucher_id == M.P())
        .where(sle_t.warehouse_id == M.P())
    )
    rows = [dict(x) for x in conn.execute(q.get_sql(), ("work_order", vid, env["fg_wh"])).fetchall()]
    assert len(rows) == 1
    assert rows[0]["incoming_rate"] == "0.00"
    assert rows[0]["stock_value_difference"] == "0.00"
    assert r["gl_count"] == 0
    assert Decimal(_active_qty(conn, component, env["raw_wh"])) == Decimal("2.00")
