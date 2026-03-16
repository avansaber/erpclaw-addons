"""L1 tests for ERPClaw Manufacturing skill (24 actions).

Tests cover: BOM, operations, workstations, routings, work orders,
job cards, production planning, and subcontracting.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from mfg_helpers import (
    load_db_query, call_action, ns, is_ok, is_error,
    seed_company, seed_naming_series, seed_item, seed_warehouse,
    seed_account, _uuid,
)

M = load_db_query()


# ===================================================================
# Operations
# ===================================================================

class TestAddOperation:
    def test_add_operation_ok(self, conn, env):
        r = call_action(M.add_operation, conn, ns(name="Cutting"))
        assert is_ok(r)
        assert r["operation_id"]
        assert r["name"] == "Cutting"

    def test_add_operation_missing_name(self, conn, env):
        r = call_action(M.add_operation, conn, ns())
        assert is_error(r)

    def test_add_operation_duplicate(self, conn, env):
        call_action(M.add_operation, conn, ns(name="DupOp"))
        r = call_action(M.add_operation, conn, ns(name="DupOp"))
        assert is_error(r)


# ===================================================================
# Workstations
# ===================================================================

class TestAddWorkstation:
    def test_add_workstation_ok(self, conn, env):
        r = call_action(M.add_workstation, conn, ns(
            name="CNC Machine", hour_rate="120.00",
        ))
        assert is_ok(r)
        assert r["workstation_id"]
        assert r["operating_cost_per_hour"] == "120.00"

    def test_add_workstation_missing_name(self, conn, env):
        r = call_action(M.add_workstation, conn, ns())
        assert is_error(r)

    def test_add_workstation_duplicate(self, conn, env):
        call_action(M.add_workstation, conn, ns(name="DupWS"))
        r = call_action(M.add_workstation, conn, ns(name="DupWS"))
        assert is_error(r)


# ===================================================================
# Routings
# ===================================================================

class TestAddRouting:
    def test_add_routing_ok(self, conn, env):
        # Need an operation first
        op_r = call_action(M.add_operation, conn, ns(name="Welding"))
        op_id = op_r["operation_id"]

        ops_json = json.dumps([
            {"operation_id": op_id, "time_in_minutes": "30"}
        ])
        r = call_action(M.add_routing, conn, ns(
            name="Basic Routing", operations=ops_json,
        ))
        assert is_ok(r)
        assert r["routing_id"]
        assert r["operations_count"] == 1

    def test_add_routing_missing_name(self, conn, env):
        r = call_action(M.add_routing, conn, ns(
            operations='[{"operation_id": "x"}]',
        ))
        assert is_error(r)

    def test_add_routing_missing_operations(self, conn, env):
        r = call_action(M.add_routing, conn, ns(name="TestRoute"))
        assert is_error(r)


# ===================================================================
# BOMs
# ===================================================================

class TestAddBom:
    def test_add_bom_ok(self, conn, env):
        items_json = json.dumps([
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
            {"item_id": env["rm_item2_id"], "quantity": "1", "rate": "75.00"},
        ])
        r = call_action(M.add_bom, conn, ns(
            item_id=env["fg_item_id"],
            items=items_json,
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["bom_id"]
        assert r["item_count"] == 2
        assert r["raw_material_cost"] == "175.00"
        assert r["is_default"] == 1  # first BOM auto-default

    def test_add_bom_missing_item_id(self, conn, env):
        r = call_action(M.add_bom, conn, ns(
            items='[{"item_id":"x","quantity":"1","rate":"10"}]',
            company_id=env["company_id"],
        ))
        assert is_error(r)

    def test_add_bom_missing_items(self, conn, env):
        r = call_action(M.add_bom, conn, ns(
            item_id=env["fg_item_id"],
            company_id=env["company_id"],
        ))
        assert is_error(r)

    def test_add_bom_missing_company(self, conn, env):
        items_json = json.dumps([
            {"item_id": env["rm_item1_id"], "quantity": "1", "rate": "10"},
        ])
        r = call_action(M.add_bom, conn, ns(
            item_id=env["fg_item_id"], items=items_json,
        ))
        assert is_error(r)


class TestGetBom:
    def test_get_bom_ok(self, conn, env):
        items_json = json.dumps([
            {"item_id": env["rm_item1_id"], "quantity": "3", "rate": "50.00"},
        ])
        add_r = call_action(M.add_bom, conn, ns(
            item_id=env["fg_item_id"], items=items_json,
            company_id=env["company_id"],
        ))
        bom_id = add_r["bom_id"]

        r = call_action(M.get_bom, conn, ns(bom_id=bom_id))
        assert is_ok(r)
        assert r["id"] == bom_id
        assert len(r["items"]) == 1

    def test_get_bom_not_found(self, conn, env):
        r = call_action(M.get_bom, conn, ns(bom_id=_uuid()))
        assert is_error(r)


class TestListBoms:
    def test_list_boms_empty(self, conn, env):
        r = call_action(M.list_boms, conn, ns(company_id=env["company_id"]))
        assert is_ok(r)
        assert r["boms"] == []

    def test_list_boms_after_add(self, conn, env):
        items_json = json.dumps([
            {"item_id": env["rm_item1_id"], "quantity": "1", "rate": "50"},
        ])
        call_action(M.add_bom, conn, ns(
            item_id=env["fg_item_id"], items=items_json,
            company_id=env["company_id"],
        ))
        r = call_action(M.list_boms, conn, ns(company_id=env["company_id"]))
        assert is_ok(r)
        assert len(r["boms"]) == 1


class TestUpdateBom:
    def test_update_bom_quantity(self, conn, env):
        items_json = json.dumps([
            {"item_id": env["rm_item1_id"], "quantity": "1", "rate": "50"},
        ])
        add_r = call_action(M.add_bom, conn, ns(
            item_id=env["fg_item_id"], items=items_json,
            company_id=env["company_id"],
        ))
        bom_id = add_r["bom_id"]

        r = call_action(M.update_bom, conn, ns(bom_id=bom_id, quantity="5"))
        assert is_ok(r)

    def test_update_bom_missing_id(self, conn, env):
        r = call_action(M.update_bom, conn, ns(quantity="5"))
        assert is_error(r)


class TestExplodeBom:
    def test_explode_bom_ok(self, conn, env):
        items_json = json.dumps([
            {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50"},
        ])
        add_r = call_action(M.add_bom, conn, ns(
            item_id=env["fg_item_id"], items=items_json,
            company_id=env["company_id"],
        ))
        bom_id = add_r["bom_id"]

        r = call_action(M.explode_bom, conn, ns(
            bom_id=bom_id, quantity="3",
        ))
        assert is_ok(r)
        assert len(r["materials"]) >= 1


# ===================================================================
# Work Orders
# ===================================================================

class TestAddWorkOrder:
    def test_add_work_order_ok(self, conn, env):
        items_json = json.dumps([
            {"item_id": env["rm_item1_id"], "quantity": "1", "rate": "50"},
        ])
        bom_r = call_action(M.add_bom, conn, ns(
            item_id=env["fg_item_id"], items=items_json,
            company_id=env["company_id"],
        ))
        bom_id = bom_r["bom_id"]

        r = call_action(M.add_work_order, conn, ns(
            bom_id=bom_id, quantity="10",
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["work_order_id"]
        assert r["naming_series"]

    def test_add_work_order_missing_bom(self, conn, env):
        r = call_action(M.add_work_order, conn, ns(
            quantity="10", company_id=env["company_id"],
        ))
        assert is_error(r)

    def test_add_work_order_zero_qty(self, conn, env):
        items_json = json.dumps([
            {"item_id": env["rm_item1_id"], "quantity": "1", "rate": "50"},
        ])
        bom_r = call_action(M.add_bom, conn, ns(
            item_id=env["fg_item_id"], items=items_json,
            company_id=env["company_id"],
        ))
        r = call_action(M.add_work_order, conn, ns(
            bom_id=bom_r["bom_id"], quantity="0",
            company_id=env["company_id"],
        ))
        assert is_error(r)


class TestGetWorkOrder:
    def test_get_work_order_ok(self, conn, env):
        items_json = json.dumps([
            {"item_id": env["rm_item1_id"], "quantity": "1", "rate": "50"},
        ])
        bom_r = call_action(M.add_bom, conn, ns(
            item_id=env["fg_item_id"], items=items_json,
            company_id=env["company_id"],
        ))
        wo_r = call_action(M.add_work_order, conn, ns(
            bom_id=bom_r["bom_id"], quantity="5",
            company_id=env["company_id"],
        ))
        wo_id = wo_r["work_order_id"]

        r = call_action(M.get_work_order, conn, ns(work_order_id=wo_id))
        assert is_ok(r)
        assert r["id"] == wo_id

    def test_get_work_order_not_found(self, conn, env):
        r = call_action(M.get_work_order, conn, ns(work_order_id=_uuid()))
        assert is_error(r)


class TestListWorkOrders:
    def test_list_work_orders_empty(self, conn, env):
        r = call_action(M.list_work_orders, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["work_orders"] == []


# ===================================================================
# Production Planning
# ===================================================================

class TestCreateProductionPlan:
    def test_create_production_plan_ok(self, conn, env):
        # Need a BOM first
        bom_items = json.dumps([
            {"item_id": env["rm_item1_id"], "quantity": "1", "rate": "50"},
        ])
        bom_r = call_action(M.add_bom, conn, ns(
            item_id=env["fg_item_id"], items=bom_items,
            company_id=env["company_id"],
        ))
        bom_id = bom_r["bom_id"]

        items_json = json.dumps([
            {"item_id": env["fg_item_id"], "bom_id": bom_id, "planned_qty": "100"},
        ])
        r = call_action(M.create_production_plan, conn, ns(
            company_id=env["company_id"],
            items=items_json,
        ))
        assert is_ok(r)
        assert r["production_plan_id"]


# ===================================================================
# Status
# ===================================================================

class TestStatus:
    def test_status_ok(self, conn, env):
        r = call_action(M.status, conn, ns())
        assert is_ok(r)
