import json
import os
import sys
import uuid
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from mfg_helpers import call_action, is_ok, ns, seed_item  # noqa: E402
from test_work_order_partial_completion import (  # noqa: E402
    M,
    TRANSFER_DATE,
    _active_qty,
    _active_value,
    _complete,
    _gl_rows,
    _ledger_env,
    _sle_rows,
    _start,
    _work_order,
    CANCEL_DATE,
)


def _pin_rate(conn, fg_id):
    item_t = M.Table("item")
    q = M.Q.update(item_t).set(item_t.standard_rate, M.P()).where(item_t.id == M.P())
    conn.execute(q.get_sql(), ("25.00", fg_id))
    conn.commit()


def _detail_rows(conn, voucher_id, warehouse_id):
    sle_t = M.Table("stock_ledger_entry")
    q = (
        M.Q.from_(sle_t)
        .select(
            sle_t.actual_qty,
            sle_t.incoming_rate,
            sle_t.valuation_rate,
            sle_t.stock_value,
            sle_t.stock_value_difference,
            sle_t.qty_after_transaction,
        )
        .where(sle_t.voucher_type == M.P())
        .where(sle_t.voucher_id == M.P())
        .where(sle_t.warehouse_id == M.P())
    )
    return [dict(r) for r in conn.execute(q.get_sql(), ("work_order", voucher_id, warehouse_id)).fetchall()]


def _zero_env(conn):
    env = _ledger_env(conn)
    rm0 = seed_item(conn, env["company_id"], name="Zero RM", standard_rate="0")
    M.insert_sle_entries(
        conn,
        [
            {
                "item_id": rm0,
                "warehouse_id": env["raw_wh"],
                "actual_qty": "20",
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
        item_id=env["fg_item"], company_id=env["company_id"], quantity="1",
        items=json.dumps([{"item_id": rm0, "quantity": "2", "rate": "0"}])))
    assert is_ok(r), r
    env["rm0"] = rm0
    env["bom_id"] = r["bom_id"]
    return env


def _transfer_zero(conn, env, wo):
    r = call_action(M.transfer_materials, conn, ns(work_order_id=wo,
        posting_date=TRANSFER_DATE, items=json.dumps([{"item_id": env["rm0"], "qty": "8"}])))
    assert is_ok(r), r
    return r


def _cancel_state_rows(conn, voucher_id):
    sle_t = M.Table("stock_ledger_entry")
    q = (
        M.Q.from_(sle_t)
        .select(sle_t.actual_qty, sle_t.stock_value_difference, sle_t.is_cancelled)
        .where(sle_t.voucher_type == M.P())
        .where(sle_t.voucher_id == M.P())
    )
    return [dict(r) for r in conn.execute(q.get_sql(), ("work_order", voucher_id)).fetchall()]


def test_zero_cost_full_completion_posts_at_zero(conn):
    env = _zero_env(conn)
    _pin_rate(conn, env["fg_item"])
    wo = _work_order(conn, env, qty="4")
    _start(conn, wo)
    _transfer_zero(conn, env, wo)
    r = _complete(conn, wo, "4")
    assert r["rm_cost"] == "0.00"
    assert r["operating_cost"] == "0.00"
    assert r["production_cost"] == "0.00"
    assert r["fg_rate"] == "0.00"
    assert r["sle_count"] == 2
    assert r["document_status"] == "completed"
    assert r["status"] == "ok"
    vid = f"{wo}:completion"
    wip_rows = _detail_rows(conn, vid, env["wip_wh"])
    assert len(wip_rows) == 1
    assert wip_rows[0]["actual_qty"] == "-8.00"
    assert wip_rows[0]["stock_value_difference"] == "0.00"
    rows = _detail_rows(conn, vid, env["fg_wh"])
    assert len(rows) == 1
    row = rows[0]
    assert (
        row["actual_qty"],
        row["incoming_rate"],
        row["valuation_rate"],
        row["stock_value"],
        row["stock_value_difference"],
        row["qty_after_transaction"],
    ) == ("4.00", "0.00", "0.00", "0.00", "0.00", "4.00")
    assert r["gl_count"] == 0
    assert _gl_rows(conn, vid) == []
    assert _active_value(conn, env["fg_item"], env["fg_wh"]) == "0.00"
    assert _active_qty(conn, env["fg_item"], env["fg_wh"]) == "4.00"


def test_zero_cost_completion_dilutes_existing_stock(conn):
    env = _zero_env(conn)
    _pin_rate(conn, env["fg_item"])
    M.insert_sle_entries(
        conn,
        [
            {
                "item_id": env["fg_item"],
                "warehouse_id": env["fg_wh"],
                "actual_qty": "10",
                "incoming_rate": "25.00",
            }
        ],
        voucher_type="stock_entry",
        voucher_id="opening-fg-" + env["company_id"][:8],
        posting_date="2026-01-05",
        company_id=env["company_id"],
    )
    conn.commit()
    wo = _work_order(conn, env, qty="4")
    _start(conn, wo)
    _transfer_zero(conn, env, wo)
    r = _complete(conn, wo, "4")
    assert r["production_cost"] == "0.00"
    vid = f"{wo}:completion"
    wip_rows = _detail_rows(conn, vid, env["wip_wh"])
    assert len(wip_rows) == 1
    assert wip_rows[0]["actual_qty"] == "-8.00"
    assert wip_rows[0]["stock_value_difference"] == "0.00"
    rows = _detail_rows(conn, vid, env["fg_wh"])
    assert len(rows) == 1
    row = rows[0]
    assert row["qty_after_transaction"] == "14.00"
    assert row["valuation_rate"] == "17.86"
    assert row["stock_value"] == "250.00"
    assert row["stock_value_difference"] == "0.00"
    assert _active_value(conn, env["fg_item"], env["fg_wh"]) == "250.00"
    assert _gl_rows(conn, vid) == []


def test_zero_cost_completion_fifo_layer_at_zero(conn):
    env = _zero_env(conn)
    _pin_rate(conn, env["fg_item"])
    item_t = M.Table("item")
    q = M.Q.update(item_t).set(item_t.valuation_method, M.P()).where(item_t.id == M.P())
    conn.execute(q.get_sql(), ("fifo", env["fg_item"]))
    conn.commit()
    wo = _work_order(conn, env, qty="4")
    _start(conn, wo)
    _transfer_zero(conn, env, wo)
    r = _complete(conn, wo, "4")
    assert r["production_cost"] == "0.00"
    vid = f"{wo}:completion"
    fifo_t = M.Table("stock_fifo_layer")
    fq = (
        M.Q.from_(fifo_t)
        .select(fifo_t.qty, fifo_t.rate, fifo_t.remaining_qty)
        .where(fifo_t.source_voucher_id == M.P())
    )
    layers = [dict(x) for x in conn.execute(fq.get_sql(), (vid,)).fetchall()]
    assert len(layers) == 1
    assert layers[0]["qty"] == "4.00"
    assert layers[0]["rate"] == "0.00"
    assert layers[0]["remaining_qty"] == "4.00"
    wip_rows = _detail_rows(conn, vid, env["wip_wh"])
    assert len(wip_rows) == 1
    assert wip_rows[0]["actual_qty"] == "-8.00"
    assert wip_rows[0]["stock_value_difference"] == "0.00"
    rows = _detail_rows(conn, vid, env["fg_wh"])
    assert len(rows) == 1
    assert rows[0]["stock_value_difference"] == "0.00"
    assert _gl_rows(conn, vid) == []


def test_zero_cost_by_product_completion_posts_at_zero(conn):
    env = _zero_env(conn)
    _pin_rate(conn, env["fg_item"])
    by_prod = seed_item(conn, env["company_id"], name="By Product", standard_rate="20.00")
    r = call_action(
        M.add_bom_output,
        conn,
        ns(
            bom_id=env["bom_id"],
            item_id=env["fg_item"],
            quantity="1",
            is_primary="1",
            cost_allocation_pct="100",
        ),
    )
    assert is_ok(r), r
    r = call_action(
        M.add_bom_output,
        conn,
        ns(
            bom_id=env["bom_id"],
            item_id=by_prod,
            quantity="1",
            is_primary="0",
            cost_allocation_pct="0",
        ),
    )
    assert is_ok(r), r
    wo = _work_order(conn, env, qty="4")
    _start(conn, wo)
    _transfer_zero(conn, env, wo)
    r = _complete(conn, wo, "4")
    assert r["by_product_count"] == 1
    vid = f"{wo}:completion"
    wip_rows = _detail_rows(conn, vid, env["wip_wh"])
    assert len(wip_rows) == 1
    assert wip_rows[0]["actual_qty"] == "-8.00"
    assert wip_rows[0]["stock_value_difference"] == "0.00"
    sle_t = M.Table("stock_ledger_entry")
    q = (
        M.Q.from_(sle_t)
        .select(sle_t.actual_qty, sle_t.incoming_rate, sle_t.stock_value_difference)
        .where(sle_t.voucher_type == M.P())
        .where(sle_t.voucher_id == M.P())
        .where(sle_t.warehouse_id == M.P())
    )
    rows = [dict(x) for x in conn.execute(q.get_sql(), ("work_order", vid, env["fg_wh"])).fetchall()]
    assert len(rows) == 2
    for row in rows:
        assert row["actual_qty"] == "4.00"
        assert row["incoming_rate"] == "0.00"
        assert row["stock_value_difference"] == "0.00"
    assert r["gl_count"] == 0


def test_zero_cost_partial_completion_cancel_reverses(conn):
    env = _zero_env(conn)
    _pin_rate(conn, env["fg_item"])
    wo = _work_order(conn, env, qty="10")
    _start(conn, wo)
    _transfer_zero(conn, env, wo)
    r = _complete(conn, wo, "4")
    assert r["document_status"] == "in_process"
    vid = f"{wo}:completion"
    wip_rows = _detail_rows(conn, vid, env["wip_wh"])
    assert len(wip_rows) == 1
    assert wip_rows[0]["actual_qty"] == "-8.00"
    assert wip_rows[0]["stock_value_difference"] == "0.00"
    rc = call_action(M.cancel_work_order, conn, ns(work_order_id=wo, posting_date=CANCEL_DATE))
    assert is_ok(rc), rc
    rows = _cancel_state_rows(conn, vid)
    assert len(rows) == 4
    assert all(x["is_cancelled"] == 1 for x in rows)
    qtys = sorted(x["actual_qty"] for x in rows)
    assert "-4.00" in qtys
    for x in rows:
        assert Decimal(x["stock_value_difference"]) == 0
    assert _active_qty(conn, env["fg_item"], env["fg_wh"]) == "0"
    assert _gl_rows(conn, vid) == []


def test_costed_completion_keeps_its_cost(conn):
    env = _ledger_env(conn)
    _pin_rate(conn, env["fg_item"])
    wo = _work_order(conn, env, qty="10")
    _start(conn, wo)
    r = call_action(
        M.transfer_materials,
        conn,
        ns(
            work_order_id=wo,
            posting_date="2026-06-05",
            items=json.dumps(
                [
                    {"item_id": env["rm1"], "qty": "20"},
                    {"item_id": env["rm2"], "qty": "10"},
                ]
            ),
        ),
    )
    assert is_ok(r), r
    r = _complete(conn, wo, "4")
    assert r["production_cost"] == "72.00"
    vid = f"{wo}:completion"
    sle_t = M.Table("stock_ledger_entry")
    q = (
        M.Q.from_(sle_t)
        .select(sle_t.actual_qty, sle_t.incoming_rate, sle_t.stock_value_difference)
        .where(sle_t.voucher_type == M.P())
        .where(sle_t.voucher_id == M.P())
        .where(sle_t.warehouse_id == M.P())
    )
    rows = [dict(x) for x in conn.execute(q.get_sql(), ("work_order", vid, env["fg_wh"])).fetchall()]
    assert len(rows) == 1
    assert rows[0]["incoming_rate"] == "18.00"
    assert rows[0]["stock_value_difference"] == "72.00"


def test_library_zero_rate_default_still_substitutes(conn):
    env = _ledger_env(conn)
    _pin_rate(conn, env["fg_item"])
    vid = "lib-default-" + uuid.uuid4().hex[:8]
    M.insert_sle_entries(
        conn,
        [
            {
                "item_id": env["fg_item"],
                "warehouse_id": env["fg_wh"],
                "actual_qty": "4",
                "incoming_rate": "0",
            }
        ],
        voucher_type="stock_entry",
        voucher_id=vid,
        posting_date="2026-01-05",
        company_id=env["company_id"],
    )
    conn.commit()
    sle_t = M.Table("stock_ledger_entry")
    q = (
        M.Q.from_(sle_t)
        .select(sle_t.incoming_rate, sle_t.stock_value_difference)
        .where(sle_t.voucher_type == M.P())
        .where(sle_t.voucher_id == M.P())
    )
    rows = [dict(x) for x in conn.execute(q.get_sql(), ("stock_entry", vid)).fetchall()]
    assert len(rows) == 1
    assert rows[0]["incoming_rate"] == "25.00"
    assert rows[0]["stock_value_difference"] == "100.00"


def test_library_exact_rate_rules(conn):
    env = _ledger_env(conn)
    _pin_rate(conn, env["fg_item"])
    sle_t = M.Table("stock_ledger_entry")
    vid_a = "lib-exact-a-" + uuid.uuid4().hex[:8]
    M.insert_sle_entries(
        conn,
        [
            {
                "item_id": env["fg_item"],
                "warehouse_id": env["fg_wh"],
                "actual_qty": "4",
                "incoming_rate": "0",
                "exact_incoming_rate": True,
            }
        ],
        voucher_type="stock_entry",
        voucher_id=vid_a,
        posting_date="2026-01-05",
        company_id=env["company_id"],
    )
    conn.commit()
    qa = (
        M.Q.from_(sle_t)
        .select(sle_t.incoming_rate, sle_t.stock_value_difference)
        .where(sle_t.voucher_type == M.P())
        .where(sle_t.voucher_id == M.P())
    )
    rows_a = [dict(x) for x in conn.execute(qa.get_sql(), ("stock_entry", vid_a)).fetchall()]
    assert len(rows_a) == 1
    assert rows_a[0]["incoming_rate"] == "0.00"
    assert rows_a[0]["stock_value_difference"] == "0.00"
    vid_b = "lib-exact-b-" + uuid.uuid4().hex[:8]
    try:
        M.insert_sle_entries(
            conn,
            [
                {
                    "item_id": env["fg_item"],
                    "warehouse_id": env["fg_wh"],
                    "actual_qty": "4",
                    "incoming_rate": "-1",
                    "exact_incoming_rate": True,
                }
            ],
            voucher_type="stock_entry",
            voucher_id=vid_b,
            posting_date="2026-01-05",
            company_id=env["company_id"],
        )
    except ValueError as e:
        assert str(e) == (
            f"Cannot value incoming stock for item {env['fg_item']}: "
            "an exact incoming rate cannot be negative (-1)."
        )
    else:
        raise AssertionError("expected ValueError for negative exact rate")
    qb = (
        M.Q.from_(sle_t)
        .select(sle_t.id)
        .where(sle_t.voucher_type == M.P())
        .where(sle_t.voucher_id == M.P())
    )
    assert conn.execute(qb.get_sql(), ("stock_entry", vid_b)).fetchall() == []
    conn.rollback()
    vid_c = "lib-exact-c-" + uuid.uuid4().hex[:8]
    try:
        M.insert_sle_entries(
            conn,
            [
                {
                    "item_id": env["fg_item"],
                    "warehouse_id": env["fg_wh"],
                    "actual_qty": "4",
                    "incoming_rate": "0",
                    "exact_incoming_rate": True,
                    "require_rate": True,
                }
            ],
            voucher_type="stock_entry",
            voucher_id=vid_c,
            posting_date="2026-01-05",
            company_id=env["company_id"],
        )
    except ValueError as e:
        assert str(e).startswith(
            f"Cannot value incoming stock for item {env['fg_item']}: no rate was provided"
        )
    else:
        raise AssertionError("expected ValueError for exact plus require at zero")
    qc = (
        M.Q.from_(sle_t)
        .select(sle_t.id)
        .where(sle_t.voucher_type == M.P())
        .where(sle_t.voucher_id == M.P())
    )
    assert conn.execute(qc.get_sql(), ("stock_entry", vid_c)).fetchall() == []
    conn.rollback()
