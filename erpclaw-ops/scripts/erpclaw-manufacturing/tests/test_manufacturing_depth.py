"""Behavioural depth tests for six manufacturing actions.

Each action below previously had only shape (response keys) or routability
coverage: the old tests never read the database back, so a perfectly shaped
response with no write behind it still passed. Every happy-path test here
reads the stored rows back and pins exact values; every refusal test proves
the error message is truthful and the database is byte-identical afterwards.

Ledger note per action (so a later reader does not add assertions that
cannot hold):
  - update-bom: master data only, writes no ledger legs. The zero-count
    checks below pin that; no debit/credit assertions apply.
  - create-job-card / complete-job-card: job_card rows only, no ledger legs.
  - cancel-work-order: DOES reach the ledger. Full reversal-leg assertions
    live in test_cancel_work_order_ledger.py; this file asserts the stored
    status transitions and that a cancel with nothing posted writes no legs.
  - generate-work-orders: work_order + work_order_item rows only, no legs.
  - get-production-plan: read-only. Asserts the response matches the stored
    rows and that the call itself writes nothing (not even an audit row).

Money and quantities are text: exact string comparisons on stored values,
Decimal only to re-add legs for the balance check. Never float.
"""
import json
import os
import sys
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from mfg_helpers import (  # noqa: E402
    _uuid, call_action, is_error, is_ok, load_db_query, ns,
)

M = load_db_query()

_SNAPSHOT_TABLES = (
    "bom",
    "bom_item",
    "bom_operation",
    "work_order",
    "work_order_item",
    "job_card",
    "production_plan",
    "production_plan_item",
    "production_plan_material",
    "gl_entry",
    "stock_ledger_entry",
    "naming_series",
    "audit_log",
)


def _snapshot(conn):
    """Full content of every manufacturing/ledger table, normalised to text."""
    snap = {}
    for table in _SNAPSHOT_TABLES:
        rows = [dict(r) for r in conn.execute("SELECT * FROM " + table).fetchall()]
        snap[table] = sorted(
            tuple(sorted((key, str(value)) for key, value in row.items()))
            for row in rows
        )
    return snap


def _row(conn, table, row_id):
    row = conn.execute("SELECT * FROM " + table + " WHERE id = ?", (row_id,)).fetchone()
    assert row is not None, f"expected a row in {table} with id {row_id}"
    return dict(row)


def _rows(conn, table, where, params):
    return [
        dict(r)
        for r in conn.execute("SELECT * FROM " + table + " WHERE " + where, params).fetchall()
    ]


def _count(conn, table):
    return conn.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]


def _make_bom(conn, env, lines, quantity="1"):
    items_json = json.dumps(lines)
    r = call_action(M.add_bom, conn, ns(
        item_id=env["fg_item_id"], items=items_json,
        company_id=env["company_id"], quantity=quantity,
    ))
    assert is_ok(r), r
    return r["bom_id"]


def _make_work_order(conn, env, bom_id, qty="10"):
    r = call_action(M.add_work_order, conn, ns(
        bom_id=bom_id, quantity=qty, company_id=env["company_id"],
    ))
    assert is_ok(r), r
    return r["work_order_id"]


def _start(conn, wo_id):
    r = call_action(M.start_work_order, conn, ns(work_order_id=wo_id))
    assert is_ok(r), r


def _make_operation(conn, name):
    r = call_action(M.add_operation, conn, ns(name=name))
    assert is_ok(r), r
    return r["operation_id"]


def _make_plan(conn, env, bom_id, planned_qty="100"):
    items_json = json.dumps([{
        "item_id": env["fg_item_id"], "bom_id": bom_id, "planned_qty": planned_qty,
    }])
    r = call_action(M.create_production_plan, conn, ns(
        company_id=env["company_id"], items=items_json,
    ))
    assert is_ok(r), r
    return r["production_plan_id"]


# ===================================================================
# update-bom (stored row; no ledger legs by design)
# ===================================================================

class TestUpdateBomDepth:
    def test_update_bom_quantity_rewrites_stored_row_and_leaves_lines_untouched(
        self, conn, env,
    ):
        bom_id = _make_bom(conn, env, [
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
        ])
        decoy_id = _make_bom(conn, env, [
            {"item_id": env["rm_item2_id"], "quantity": "1", "rate": "75.00"},
        ])
        before = _row(conn, "bom", bom_id)
        assert before["quantity"] == "1.00"
        assert before["raw_material_cost"] == "100.00"
        assert before["total_cost"] == "100.00"
        lines_before = _rows(conn, "bom_item", "bom_id = ?", (bom_id,))
        assert len(lines_before) == 1
        decoy_before = _row(conn, "bom", decoy_id)

        r = call_action(M.update_bom, conn, ns(bom_id=bom_id, quantity="5"))
        assert is_ok(r), r
        assert "quantity" in r["updated_fields"]
        assert r["raw_material_cost"] == "100.00"
        assert r["total_cost"] == "100.00"

        after = _row(conn, "bom", bom_id)
        assert after["quantity"] == "5.00"  # from "1.00" to "5.00"
        assert after["raw_material_cost"] == "100.00"
        assert after["operating_cost"] == "0.00"
        assert after["total_cost"] == "100.00"
        # The component lines are untouched by a header-only update.
        assert _rows(conn, "bom_item", "bom_id = ?", (bom_id,)) == lines_before
        # A BOM that was not named is byte-identical.
        assert _row(conn, "bom", decoy_id) == decoy_before
        # Master-data edit: no ledger legs may appear.
        assert _count(conn, "gl_entry") == 0
        assert _count(conn, "stock_ledger_entry") == 0

    def test_update_bom_items_replace_lines_and_recalculate_costs(self, conn, env):
        bom_id = _make_bom(conn, env, [
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
            {"item_id": env["rm_item2_id"], "quantity": "1", "rate": "75.00"},
        ])
        assert _row(conn, "bom", bom_id)["raw_material_cost"] == "175.00"

        r = call_action(M.update_bom, conn, ns(
            bom_id=bom_id,
            items=json.dumps([
                {"item_id": env["rm_item2_id"], "quantity": "3", "rate": "75.00"},
            ]),
        ))
        assert is_ok(r), r
        assert "items" in r["updated_fields"]
        assert r["raw_material_cost"] == "225.00"
        assert r["total_cost"] == "225.00"

        lines = _rows(conn, "bom_item", "bom_id = ?", (bom_id,))
        assert len(lines) == 1
        assert lines[0]["item_id"] == env["rm_item2_id"]
        assert lines[0]["quantity"] == "3.00"
        assert lines[0]["rate"] == "75.00"
        assert lines[0]["amount"] == "225.00"
        assert Decimal(lines[0]["quantity"]) * Decimal(lines[0]["rate"]) == Decimal("225.00")
        after = _row(conn, "bom", bom_id)
        assert after["raw_material_cost"] == "225.00"  # from "175.00"
        assert after["total_cost"] == "225.00"
        assert _count(conn, "gl_entry") == 0
        assert _count(conn, "stock_ledger_entry") == 0

    def test_update_bom_refuses_zero_quantity_without_writing(self, conn, env):
        bom_id = _make_bom(conn, env, [
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
        ])
        before = _snapshot(conn)

        r = call_action(M.update_bom, conn, ns(bom_id=bom_id, quantity="0"))
        assert is_error(r)
        assert r["message"] == "--quantity must be greater than 0"
        assert _snapshot(conn) == before
        assert _row(conn, "bom", bom_id)["quantity"] == "1.00"


# ===================================================================
# create-job-card (stored row; no ledger legs by design)
# ===================================================================

class TestCreateJobCardDepth:
    def test_create_job_card_inserts_open_card_with_wo_quantity(self, conn, env):
        bom_id = _make_bom(conn, env, [
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
        ])
        wo_id = _make_work_order(conn, env, bom_id, qty="10")
        _start(conn, wo_id)
        wo_before = _row(conn, "work_order", wo_id)
        assert wo_before["status"] == "in_process"
        op_id = _make_operation(conn, "Drilling")

        r = call_action(M.create_job_card, conn, ns(
            work_order_id=wo_id, operation_id=op_id,
        ))
        assert is_ok(r), r
        assert r["for_quantity"] == "10.00"

        card = _row(conn, "job_card", r["job_card_id"])
        assert card["work_order_id"] == wo_id
        assert card["operation_id"] == op_id
        assert card["status"] == "open"
        assert card["for_quantity"] == "10.00"
        assert card["completed_qty"] == "0"
        assert card["total_time_in_minutes"] == "0"
        assert card["naming_series"].startswith("JC-")
        # The work order itself is not moved by creating a card.
        assert _row(conn, "work_order", wo_id)["status"] == "in_process"
        # Card creation posts no ledger legs.
        assert _count(conn, "gl_entry") == 0
        assert _count(conn, "stock_ledger_entry") == 0

    def test_create_job_card_refuses_missing_operation_without_writing(
        self, conn, env,
    ):
        bom_id = _make_bom(conn, env, [
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
        ])
        wo_id = _make_work_order(conn, env, bom_id, qty="10")
        _start(conn, wo_id)
        before = _snapshot(conn)

        r = call_action(M.create_job_card, conn, ns(work_order_id=wo_id))
        assert is_error(r)
        assert r["message"] == "--operation-id is required"
        assert _snapshot(conn) == before
        assert _rows(conn, "job_card", "work_order_id = ?", (wo_id,)) == []


# ===================================================================
# complete-job-card (stored row; no ledger legs by design)
# ===================================================================

class TestCompleteJobCardDepth:
    def test_complete_job_card_moves_open_to_completed_with_time_and_qty(
        self, conn, env,
    ):
        bom_id = _make_bom(conn, env, [
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
        ])
        wo_id = _make_work_order(conn, env, bom_id, qty="10")
        _start(conn, wo_id)
        op_id = _make_operation(conn, "Assembly")
        first = call_action(M.create_job_card, conn, ns(
            work_order_id=wo_id, operation_id=op_id,
        ))["job_card_id"]
        second = call_action(M.create_job_card, conn, ns(
            work_order_id=wo_id, operation_id=op_id,
        ))["job_card_id"]
        assert _row(conn, "job_card", first)["status"] == "open"

        r = call_action(M.complete_job_card, conn, ns(
            job_card_id=first, actual_time_in_mins="45", completed_qty="10",
        ))
        assert is_ok(r), r
        assert r["completed_qty"] == "10.00"
        assert r["total_time_in_minutes"] == "45.00"

        done = _row(conn, "job_card", first)
        assert done["status"] == "completed"  # from "open"
        assert done["total_time_in_minutes"] == "45.00"
        assert done["completed_qty"] == "10.00"  # from "0"
        assert done["time_completed"] not in (None, "")
        # The sibling card and the work order are not moved.
        assert _row(conn, "job_card", second)["status"] == "open"
        assert _row(conn, "job_card", second)["completed_qty"] == "0"
        assert _row(conn, "work_order", wo_id)["status"] == "in_process"
        # Time booking posts no ledger legs.
        assert _count(conn, "gl_entry") == 0
        assert _count(conn, "stock_ledger_entry") == 0

    def test_complete_job_card_refuses_completed_card_without_writing(
        self, conn, env,
    ):
        bom_id = _make_bom(conn, env, [
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
        ])
        wo_id = _make_work_order(conn, env, bom_id, qty="10")
        _start(conn, wo_id)
        op_id = _make_operation(conn, "Polishing")
        card_id = call_action(M.create_job_card, conn, ns(
            work_order_id=wo_id, operation_id=op_id,
        ))["job_card_id"]
        assert is_ok(call_action(M.complete_job_card, conn, ns(
            job_card_id=card_id, actual_time_in_mins="30",
        )))
        before = _snapshot(conn)

        r = call_action(M.complete_job_card, conn, ns(
            job_card_id=card_id, actual_time_in_mins="5",
        ))
        assert is_error(r)
        assert r["message"] == (
            "Cannot complete Job Card with status 'completed'. "
            "Must be 'open' or 'in_process'."
        )
        assert _snapshot(conn) == before
        assert _row(conn, "job_card", card_id)["total_time_in_minutes"] == "30.00"


# ===================================================================
# cancel-work-order (stored rows; ledger reversal covered in
# test_cancel_work_order_ledger.py — here the no-posting cancel writes no
# legs, and both sides of any legs it did write would have to net to zero)
# ===================================================================

class TestCancelWorkOrderDepth:
    def test_cancel_moves_work_order_and_open_cards_to_cancelled(self, conn, env):
        bom_id = _make_bom(conn, env, [
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
        ])
        wo_id = _make_work_order(conn, env, bom_id, qty="10")
        _start(conn, wo_id)
        op_id = _make_operation(conn, "Packing")
        open_card = call_action(M.create_job_card, conn, ns(
            work_order_id=wo_id, operation_id=op_id,
        ))["job_card_id"]
        done_card = call_action(M.create_job_card, conn, ns(
            work_order_id=wo_id, operation_id=op_id,
        ))["job_card_id"]
        assert is_ok(call_action(M.complete_job_card, conn, ns(
            job_card_id=done_card, actual_time_in_mins="20", completed_qty="10",
        )))
        assert _row(conn, "work_order", wo_id)["status"] == "in_process"

        r = call_action(M.cancel_work_order, conn, ns(work_order_id=wo_id))
        assert is_ok(r), r
        assert r["document_status"] == "cancelled"

        assert _row(conn, "work_order", wo_id)["status"] == "cancelled"
        assert _row(conn, "job_card", open_card)["status"] == "cancelled"
        # A finished card is history, not collateral: it stays completed.
        assert _row(conn, "job_card", done_card)["status"] == "completed"
        # Nothing was ever posted for this order, so the cancel posts no
        # legs either; both ledgers stay empty and therefore balance.
        assert _count(conn, "gl_entry") == 0
        assert _count(conn, "stock_ledger_entry") == 0

    def test_cancel_work_order_refuses_unknown_order_without_writing(
        self, conn, env,
    ):
        bom_id = _make_bom(conn, env, [
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
        ])
        _make_work_order(conn, env, bom_id, qty="10")
        before = _snapshot(conn)
        missing = _uuid()

        r = call_action(M.cancel_work_order, conn, ns(work_order_id=missing))
        assert is_error(r)
        assert r["message"] == f"Work Order {missing} not found"
        assert _snapshot(conn) == before


# ===================================================================
# generate-work-orders (stored rows; no ledger legs by design)
# ===================================================================

class TestGenerateWorkOrdersDepth:
    def test_generate_creates_scaled_work_order_and_links_plan_item(
        self, conn, env,
    ):
        bom_id = _make_bom(conn, env, [
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
            {"item_id": env["rm_item2_id"], "quantity": "1", "rate": "75.00"},
        ])
        plan_id = _make_plan(conn, env, bom_id, planned_qty="100")

        r = call_action(M.generate_work_orders, conn, ns(production_plan_id=plan_id))
        assert is_ok(r), r
        assert r["work_orders_created"] == 1
        assert len(r["work_order_ids"]) == 1
        wo_id = r["work_order_ids"][0]

        wo = _row(conn, "work_order", wo_id)
        assert wo["item_id"] == env["fg_item_id"]
        assert wo["bom_id"] == bom_id
        assert wo["qty"] == "100.00"
        assert wo["produced_qty"] == "0"
        assert wo["status"] == "draft"
        assert wo["production_plan_id"] == plan_id
        assert Decimal(wo["qty"]) == Decimal("100.00")

        lines = _rows(conn, "work_order_item", "work_order_id = ?", (wo_id,))
        assert len(lines) == 2
        by_item = {line["item_id"]: line for line in lines}
        # BOM base quantity is 1: 2 x 100 and 1 x 100, exact text.
        assert by_item[env["rm_item1_id"]]["required_qty"] == "200.00"
        assert by_item[env["rm_item2_id"]]["required_qty"] == "100.00"
        for line in lines:
            assert line["transferred_qty"] == "0"
            assert line["consumed_qty"] == "0"

        plan_item = _rows(
            conn, "production_plan_item", "production_plan_id = ?", (plan_id,))
        assert len(plan_item) == 1
        assert plan_item[0]["work_order_id"] == wo_id
        assert plan_item[0]["planned_qty"] == "100.00"
        assert _count(conn, "gl_entry") == 0
        assert _count(conn, "stock_ledger_entry") == 0

        # Generating twice must not duplicate: the plan item is linked now.
        again = call_action(
            M.generate_work_orders, conn, ns(production_plan_id=plan_id))
        assert is_ok(again), again
        assert again["work_orders_created"] == 0
        assert _count(conn, "work_order") == 1

    def test_generate_work_orders_refuses_unknown_plan_without_writing(
        self, conn, env,
    ):
        before = _snapshot(conn)
        missing = _uuid()

        r = call_action(M.generate_work_orders, conn, ns(production_plan_id=missing))
        assert is_error(r)
        assert r["message"] == f"Production Plan {missing} not found"
        assert _snapshot(conn) == before
        assert _count(conn, "work_order") == 0


# ===================================================================
# get-production-plan (read-only: response must match the stored rows,
# and the call itself writes nothing)
# ===================================================================

class TestGetProductionPlanDepth:
    def test_get_production_plan_returns_stored_rows_and_writes_nothing(
        self, conn, env,
    ):
        bom_id = _make_bom(conn, env, [
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
            {"item_id": env["rm_item2_id"], "quantity": "1", "rate": "75.00"},
        ])
        plan_id = _make_plan(conn, env, bom_id, planned_qty="100")
        assert is_ok(call_action(M.run_mrp, conn, ns(production_plan_id=plan_id)))
        before = _snapshot(conn)

        r = call_action(M.get_production_plan, conn, ns(production_plan_id=plan_id))
        assert is_ok(r), r
        # The plan row's own status survives the envelope mapping untouched.
        assert r["document_status"] == "submitted"
        assert r["company_id"] == env["company_id"]

        stored_items = _rows(
            conn, "production_plan_item", "production_plan_id = ?", (plan_id,))
        assert len(r["items"]) == len(stored_items) == 1
        assert r["items"][0]["item_id"] == stored_items[0]["item_id"] == env["fg_item_id"]
        assert r["items"][0]["bom_id"] == stored_items[0]["bom_id"] == bom_id
        assert r["items"][0]["planned_qty"] == stored_items[0]["planned_qty"] == "100.00"
        assert r["items"][0]["produced_qty"] == "0"
        assert r["items"][0]["item_code"] not in (None, "")

        stored_materials = _rows(
            conn, "production_plan_material", "production_plan_id = ?", (plan_id,))
        assert len(r["materials"]) == len(stored_materials) == 2
        resp_mats = {m["item_id"]: m for m in r["materials"]}
        # BOM base quantity 1, plan 100: 2 x 100 and 1 x 100, no stock on hand.
        assert resp_mats[env["rm_item1_id"]]["required_qty"] == "200.00"
        assert resp_mats[env["rm_item1_id"]]["shortfall_qty"] == "200.00"
        assert resp_mats[env["rm_item2_id"]]["required_qty"] == "100.00"
        assert resp_mats[env["rm_item2_id"]]["shortfall_qty"] == "100.00"
        for stored in stored_materials:
            echoed = resp_mats[stored["item_id"]]
            assert echoed["required_qty"] == stored["required_qty"]
            assert echoed["available_qty"] == stored["available_qty"]
            assert echoed["on_order_qty"] == stored["on_order_qty"]
            assert echoed["shortfall_qty"] == stored["shortfall_qty"]
            assert Decimal(echoed["shortfall_qty"]) == (
                Decimal(echoed["required_qty"])
                - Decimal(echoed["available_qty"])
                - Decimal(echoed["on_order_qty"])
            )
        assert r["total_shortfall_items"] == 2

        # A read returns rows; it stores none — not even an audit row.
        assert _snapshot(conn) == before

    def test_get_production_plan_refuses_unknown_plan_without_writing(
        self, conn, env,
    ):
        bom_id = _make_bom(conn, env, [
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
        ])
        _make_plan(conn, env, bom_id, planned_qty="100")
        before = _snapshot(conn)
        missing = _uuid()

        r = call_action(M.get_production_plan, conn, ns(production_plan_id=missing))
        assert is_error(r)
        assert r["message"] == f"Production Plan {missing} not found"
        assert _snapshot(conn) == before
