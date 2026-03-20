"""L1 tests for ERPClaw Manufacturing skill (29 actions).

Tests cover: BOM, operations, workstations, routings, work orders,
job cards, production planning, subcontracting, material substitution,
co-products/by-products, and make-vs-buy decisions.
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


# ===================================================================
# Feature #7: Material Substitution
# ===================================================================

def _create_bom_with_items(conn, env):
    """Helper: create a BOM and return (bom_id, bom_item_ids dict)."""
    items_json = json.dumps([
        {"item_id": env["rm_item1_id"], "quantity": "2", "rate": "50.00"},
        {"item_id": env["rm_item2_id"], "quantity": "1", "rate": "75.00"},
    ])
    r = call_action(M.add_bom, conn, ns(
        item_id=env["fg_item_id"],
        items=items_json,
        company_id=env["company_id"],
    ))
    bom_id = r["bom_id"]

    # Get actual bom_item IDs from DB
    rows = conn.execute(
        "SELECT id, item_id FROM bom_item WHERE bom_id = ?", (bom_id,)
    ).fetchall()
    bom_item_ids = {row["item_id"]: row["id"] for row in rows}
    return bom_id, bom_item_ids


class TestAddBomSubstitute:
    def test_add_substitute_ok(self, conn, env):
        bom_id, bom_item_ids = _create_bom_with_items(conn, env)
        bi_id = bom_item_ids[env["rm_item1_id"]]
        # Use rm_item2 as substitute for rm_item1
        r = call_action(M.add_bom_substitute, conn, ns(
            bom_item_id=bi_id,
            substitute_item_id=env["rm_item2_id"],
            conversion_factor="1.5",
            priority="1",
        ))
        assert is_ok(r)
        assert r["substitute_id"]
        assert r["conversion_factor"] == "1.50"
        assert r["priority"] == 1

    def test_add_substitute_missing_bom_item(self, conn, env):
        r = call_action(M.add_bom_substitute, conn, ns(
            substitute_item_id=env["rm_item2_id"],
        ))
        assert is_error(r)

    def test_add_substitute_missing_sub_item(self, conn, env):
        bom_id, bom_item_ids = _create_bom_with_items(conn, env)
        bi_id = bom_item_ids[env["rm_item1_id"]]
        r = call_action(M.add_bom_substitute, conn, ns(
            bom_item_id=bi_id,
        ))
        assert is_error(r)

    def test_add_substitute_same_as_primary(self, conn, env):
        bom_id, bom_item_ids = _create_bom_with_items(conn, env)
        bi_id = bom_item_ids[env["rm_item1_id"]]
        r = call_action(M.add_bom_substitute, conn, ns(
            bom_item_id=bi_id,
            substitute_item_id=env["rm_item1_id"],
        ))
        assert is_error(r)

    def test_add_substitute_duplicate(self, conn, env):
        bom_id, bom_item_ids = _create_bom_with_items(conn, env)
        bi_id = bom_item_ids[env["rm_item1_id"]]
        call_action(M.add_bom_substitute, conn, ns(
            bom_item_id=bi_id,
            substitute_item_id=env["rm_item2_id"],
        ))
        r = call_action(M.add_bom_substitute, conn, ns(
            bom_item_id=bi_id,
            substitute_item_id=env["rm_item2_id"],
        ))
        assert is_error(r)

    def test_add_substitute_invalid_bom_item_id(self, conn, env):
        r = call_action(M.add_bom_substitute, conn, ns(
            bom_item_id=_uuid(),
            substitute_item_id=env["rm_item2_id"],
        ))
        assert is_error(r)

    def test_add_substitute_zero_conversion_factor(self, conn, env):
        bom_id, bom_item_ids = _create_bom_with_items(conn, env)
        bi_id = bom_item_ids[env["rm_item1_id"]]
        r = call_action(M.add_bom_substitute, conn, ns(
            bom_item_id=bi_id,
            substitute_item_id=env["rm_item2_id"],
            conversion_factor="0",
        ))
        assert is_error(r)


class TestListBomSubstitutes:
    def test_list_substitutes_empty(self, conn, env):
        bom_id, bom_item_ids = _create_bom_with_items(conn, env)
        bi_id = bom_item_ids[env["rm_item1_id"]]
        r = call_action(M.list_bom_substitutes, conn, ns(bom_item_id=bi_id))
        assert is_ok(r)
        assert r["substitutes"] == []
        assert r["count"] == 0

    def test_list_substitutes_after_add(self, conn, env):
        bom_id, bom_item_ids = _create_bom_with_items(conn, env)
        bi_id = bom_item_ids[env["rm_item1_id"]]
        call_action(M.add_bom_substitute, conn, ns(
            bom_item_id=bi_id,
            substitute_item_id=env["rm_item2_id"],
            conversion_factor="2.0",
            priority="1",
        ))
        r = call_action(M.list_bom_substitutes, conn, ns(bom_item_id=bi_id))
        assert is_ok(r)
        assert r["count"] == 1
        assert r["substitutes"][0]["substitute_item_id"] == env["rm_item2_id"]

    def test_list_substitutes_missing_bom_item_id(self, conn, env):
        r = call_action(M.list_bom_substitutes, conn, ns())
        assert is_error(r)


# ===================================================================
# Feature #8: Co-Products / By-Products
# ===================================================================

class TestAddBomOutput:
    def test_add_bom_output_primary(self, conn, env):
        bom_id, _ = _create_bom_with_items(conn, env)
        r = call_action(M.add_bom_output, conn, ns(
            bom_id=bom_id,
            item_id=env["fg_item_id"],
            quantity="1",
            is_primary="1",
            cost_allocation_pct="80",
        ))
        assert is_ok(r)
        assert r["output_id"]
        assert r["is_primary"] == 1
        assert r["cost_allocation_pct"] == "80.00"

    def test_add_bom_output_byproduct(self, conn, env):
        bom_id, _ = _create_bom_with_items(conn, env)
        # Create a by-product item
        bp_item = seed_item(conn, env["company_id"], name="By-Product X", standard_rate="20.00")
        r = call_action(M.add_bom_output, conn, ns(
            bom_id=bom_id,
            item_id=bp_item,
            quantity="5",
            is_primary="0",
            cost_allocation_pct="20",
        ))
        assert is_ok(r)
        assert r["is_primary"] == 0
        assert r["qty"] == "5.00"

    def test_add_bom_output_missing_bom_id(self, conn, env):
        r = call_action(M.add_bom_output, conn, ns(
            item_id=env["fg_item_id"],
            quantity="1",
        ))
        assert is_error(r)

    def test_add_bom_output_missing_quantity(self, conn, env):
        bom_id, _ = _create_bom_with_items(conn, env)
        r = call_action(M.add_bom_output, conn, ns(
            bom_id=bom_id,
            item_id=env["fg_item_id"],
        ))
        assert is_error(r)

    def test_add_bom_output_duplicate_item(self, conn, env):
        bom_id, _ = _create_bom_with_items(conn, env)
        call_action(M.add_bom_output, conn, ns(
            bom_id=bom_id,
            item_id=env["fg_item_id"],
            quantity="1",
            is_primary="1",
            cost_allocation_pct="100",
        ))
        r = call_action(M.add_bom_output, conn, ns(
            bom_id=bom_id,
            item_id=env["fg_item_id"],
            quantity="1",
        ))
        assert is_error(r)

    def test_add_bom_output_invalid_cost_pct(self, conn, env):
        bom_id, _ = _create_bom_with_items(conn, env)
        r = call_action(M.add_bom_output, conn, ns(
            bom_id=bom_id,
            item_id=env["fg_item_id"],
            quantity="1",
            cost_allocation_pct="150",
        ))
        assert is_error(r)


class TestListBomOutputs:
    def test_list_bom_outputs_empty(self, conn, env):
        bom_id, _ = _create_bom_with_items(conn, env)
        r = call_action(M.list_bom_outputs, conn, ns(bom_id=bom_id))
        assert is_ok(r)
        assert r["outputs"] == []
        assert r["count"] == 0

    def test_list_bom_outputs_with_items(self, conn, env):
        bom_id, _ = _create_bom_with_items(conn, env)
        bp_item = seed_item(conn, env["company_id"], name="By-Product Y", standard_rate="15.00")
        call_action(M.add_bom_output, conn, ns(
            bom_id=bom_id, item_id=env["fg_item_id"],
            quantity="1", is_primary="1", cost_allocation_pct="70",
        ))
        call_action(M.add_bom_output, conn, ns(
            bom_id=bom_id, item_id=bp_item,
            quantity="3", is_primary="0", cost_allocation_pct="30",
        ))
        r = call_action(M.list_bom_outputs, conn, ns(bom_id=bom_id))
        assert is_ok(r)
        assert r["count"] == 2
        # Primary should be listed first (ORDER BY is_primary DESC)
        assert r["outputs"][0]["is_primary"] == 1

    def test_list_bom_outputs_missing_bom_id(self, conn, env):
        r = call_action(M.list_bom_outputs, conn, ns())
        assert is_error(r)


# ===================================================================
# Feature #9: Make vs Buy Decision
# ===================================================================

class TestUpdateItemProcurementType:
    def test_update_to_manufacture(self, conn, env):
        r = call_action(M.update_item_procurement_type, conn, ns(
            item_id=env["rm_item1_id"],
            procurement_type="manufacture",
        ))
        assert is_ok(r)
        assert r["default_procurement_type"] == "manufacture"

    def test_update_to_both(self, conn, env):
        r = call_action(M.update_item_procurement_type, conn, ns(
            item_id=env["rm_item1_id"],
            procurement_type="both",
        ))
        assert is_ok(r)
        assert r["default_procurement_type"] == "both"

    def test_update_to_purchase(self, conn, env):
        # First set to manufacture, then back to purchase
        call_action(M.update_item_procurement_type, conn, ns(
            item_id=env["rm_item1_id"],
            procurement_type="manufacture",
        ))
        r = call_action(M.update_item_procurement_type, conn, ns(
            item_id=env["rm_item1_id"],
            procurement_type="purchase",
        ))
        assert is_ok(r)
        assert r["default_procurement_type"] == "purchase"

    def test_update_invalid_type(self, conn, env):
        r = call_action(M.update_item_procurement_type, conn, ns(
            item_id=env["rm_item1_id"],
            procurement_type="outsource",
        ))
        assert is_error(r)

    def test_update_missing_item_id(self, conn, env):
        r = call_action(M.update_item_procurement_type, conn, ns(
            procurement_type="manufacture",
        ))
        assert is_error(r)

    def test_update_missing_procurement_type(self, conn, env):
        r = call_action(M.update_item_procurement_type, conn, ns(
            item_id=env["rm_item1_id"],
        ))
        assert is_error(r)

    def test_update_nonexistent_item(self, conn, env):
        r = call_action(M.update_item_procurement_type, conn, ns(
            item_id=_uuid(),
            procurement_type="manufacture",
        ))
        assert is_error(r)


class TestMrpMakeVsBuy:
    """Test that run-mrp and generate-purchase-requests respect procurement type."""

    def test_mrp_manufacture_type_excluded_from_purchase_requests(self, conn, env):
        """An item with procurement_type=manufacture should not appear in purchase requests."""
        # Set rm_item1 to manufacture
        call_action(M.update_item_procurement_type, conn, ns(
            item_id=env["rm_item1_id"],
            procurement_type="manufacture",
        ))
        # Create BOM and production plan
        bom_id, _ = _create_bom_with_items(conn, env)
        items_json = json.dumps([
            {"item_id": env["fg_item_id"], "bom_id": bom_id, "planned_qty": "10"},
        ])
        pp_r = call_action(M.create_production_plan, conn, ns(
            company_id=env["company_id"],
            items=items_json,
        ))
        pp_id = pp_r["production_plan_id"]
        # Run MRP
        mrp_r = call_action(M.run_mrp, conn, ns(production_plan_id=pp_id))
        assert is_ok(mrp_r)
        # Generate purchase requests — rm_item1 should be excluded
        pr_r = call_action(M.generate_purchase_requests, conn, ns(
            production_plan_id=pp_id,
        ))
        assert is_ok(pr_r)
        purchase_item_ids = [p["item_id"] for p in pr_r["purchase_requests"]]
        assert env["rm_item1_id"] not in purchase_item_ids
        # But rm_item1 should appear in manufacture_items
        mfg_item_ids = [m["item_id"] for m in pr_r["manufacture_items"]]
        assert env["rm_item1_id"] in mfg_item_ids

    def test_mrp_both_type_appears_in_both_lists(self, conn, env):
        """An item with procurement_type=both should appear in both lists."""
        call_action(M.update_item_procurement_type, conn, ns(
            item_id=env["rm_item1_id"],
            procurement_type="both",
        ))
        bom_id, _ = _create_bom_with_items(conn, env)
        items_json = json.dumps([
            {"item_id": env["fg_item_id"], "bom_id": bom_id, "planned_qty": "10"},
        ])
        pp_r = call_action(M.create_production_plan, conn, ns(
            company_id=env["company_id"],
            items=items_json,
        ))
        pp_id = pp_r["production_plan_id"]
        call_action(M.run_mrp, conn, ns(production_plan_id=pp_id))
        pr_r = call_action(M.generate_purchase_requests, conn, ns(
            production_plan_id=pp_id,
        ))
        assert is_ok(pr_r)
        purchase_item_ids = [p["item_id"] for p in pr_r["purchase_requests"]]
        mfg_item_ids = [m["item_id"] for m in pr_r["manufacture_items"]]
        assert env["rm_item1_id"] in purchase_item_ids
        assert env["rm_item1_id"] in mfg_item_ids

    def test_mrp_default_purchase_type_in_purchase_requests(self, conn, env):
        """Default procurement_type=purchase items should appear in purchase requests."""
        bom_id, _ = _create_bom_with_items(conn, env)
        items_json = json.dumps([
            {"item_id": env["fg_item_id"], "bom_id": bom_id, "planned_qty": "10"},
        ])
        pp_r = call_action(M.create_production_plan, conn, ns(
            company_id=env["company_id"],
            items=items_json,
        ))
        pp_id = pp_r["production_plan_id"]
        call_action(M.run_mrp, conn, ns(production_plan_id=pp_id))
        pr_r = call_action(M.generate_purchase_requests, conn, ns(
            production_plan_id=pp_id,
        ))
        assert is_ok(pr_r)
        # Both items default to 'purchase', so both should be in purchase_requests
        purchase_item_ids = [p["item_id"] for p in pr_r["purchase_requests"]]
        assert env["rm_item1_id"] in purchase_item_ids
        assert env["rm_item2_id"] in purchase_item_ids
