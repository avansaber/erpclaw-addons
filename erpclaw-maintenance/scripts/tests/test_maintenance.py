"""L1 pytest tests for erpclaw-maintenance (39 actions across 5 domain modules).

Tests cover:
  equipment.py (10): add/update/get/list equipment, add-equipment-child,
    list-equipment-tree, add/list equipment-reading, link-equipment-asset, import-equipment
  plans.py (6): add/update/get/list maintenance-plan, add/list plan-item
  work_orders.py (12): add/update/get/list maintenance-work-order, add/list wo-item,
    start/complete/cancel work-order, generate-preventive-work-orders,
    add/list downtime-record
  checklists.py (4): add-checklist, get-checklist, add-checklist-item, complete-checklist-item
  reports.py (7): equipment-status-report, maintenance-cost-report,
    pm-compliance-report, downtime-report, spare-parts-usage,
    equipment-history, status
"""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from maintenance_helpers import call_action, ns, is_ok, is_error, _uuid


# ===========================================================================
# Equipment helpers
# ===========================================================================

def _add_equipment(conn, env, mod, name="CNC Mill 1", eq_type="machine",
                   criticality="medium"):
    """Add equipment and return result."""
    return call_action(mod.maintenance_add_equipment, conn, ns(
        name=name,
        company_id=env["company_id"],
        equipment_type=eq_type,
        model="Model X",
        manufacturer="Acme Mfg",
        serial_number="SN-001",
        location="Factory Floor A",
        parent_equipment_id=None,
        asset_id=None,
        item_id=None,
        purchase_date="2024-01-15",
        warranty_expiry="2027-01-15",
        criticality=criticality,
        equipment_status="operational",
        notes=None,
    ))


def _add_plan(conn, env, mod, eq_id, plan_name="Monthly Lubrication",
              frequency="monthly", next_due="2025-07-01"):
    """Add maintenance plan and return result."""
    return call_action(mod.maintenance_add_maintenance_plan, conn, ns(
        plan_name=plan_name,
        equipment_id=eq_id,
        company_id=env["company_id"],
        plan_type="preventive",
        frequency=frequency,
        frequency_days=None,
        last_performed=None,
        next_due=next_due,
        estimated_duration="2h",
        estimated_cost="150.00",
        assigned_to="Technician A",
        instructions="Lubricate all moving parts",
        is_active=None,
        item_id=None,
    ))


def _add_work_order(conn, env, mod, eq_id, plan_id=None, wo_type="corrective"):
    """Add maintenance work order and return result."""
    return call_action(mod.maintenance_add_maintenance_work_order, conn, ns(
        equipment_id=eq_id,
        company_id=env["company_id"],
        plan_id=plan_id,
        work_order_type=wo_type,
        priority="medium",
        description="Replace worn bearing",
        assigned_to="Tech B",
        scheduled_date="2025-07-01",
        failure_mode=None,
        wo_status=None,
    ))


# ===========================================================================
# Equipment Actions
# ===========================================================================


class TestAddEquipment:
    def test_add_equipment_ok(self, conn, env, mod):
        r = _add_equipment(conn, env, mod)
        assert is_ok(r), r
        assert r["name"] == "CNC Mill 1"
        assert r["equipment_status"] == "operational"
        assert r["naming_series"].startswith("EQP-")

    def test_add_equipment_missing_name(self, conn, env, mod):
        r = call_action(mod.maintenance_add_equipment, conn, ns(
            name=None,
            company_id=env["company_id"],
            equipment_type=None,
            model=None, manufacturer=None, serial_number=None,
            location=None, parent_equipment_id=None, asset_id=None,
            item_id=None, purchase_date=None, warranty_expiry=None,
            criticality=None, equipment_status=None, notes=None,
        ))
        assert is_error(r)

    def test_add_equipment_invalid_type(self, conn, env, mod):
        r = call_action(mod.maintenance_add_equipment, conn, ns(
            name="Bad Type",
            company_id=env["company_id"],
            equipment_type="spaceship",
            model=None, manufacturer=None, serial_number=None,
            location=None, parent_equipment_id=None, asset_id=None,
            item_id=None, purchase_date=None, warranty_expiry=None,
            criticality=None, equipment_status=None, notes=None,
        ))
        assert is_error(r)


class TestUpdateEquipment:
    def test_update_equipment_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        r2 = call_action(mod.maintenance_update_equipment, conn, ns(
            equipment_id=eq_id,
            name=None,
            equipment_type=None,
            model=None,
            manufacturer=None,
            serial_number=None,
            location="Factory Floor B",
            criticality=None,
            notes=None,
            purchase_date=None,
            warranty_expiry=None,
            equipment_status=None,
        ))
        assert is_ok(r2), r2
        assert "location" in r2["updated_fields"]

    def test_update_equipment_no_fields(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        r2 = call_action(mod.maintenance_update_equipment, conn, ns(
            equipment_id=eq_id,
            name=None, equipment_type=None, model=None,
            manufacturer=None, serial_number=None, location=None,
            criticality=None, notes=None, purchase_date=None,
            warranty_expiry=None, equipment_status=None,
        ))
        assert is_error(r2)


class TestGetEquipment:
    def test_get_equipment_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        r2 = call_action(mod.maintenance_get_equipment, conn, ns(
            equipment_id=eq_id,
        ))
        assert is_ok(r2), r2
        assert r2["id"] == eq_id

    def test_get_equipment_not_found(self, conn, env, mod):
        r = call_action(mod.maintenance_get_equipment, conn, ns(
            equipment_id=_uuid(),
        ))
        assert is_error(r)


class TestListEquipment:
    def test_list_equipment(self, conn, env, mod):
        _add_equipment(conn, env, mod, "Machine A")
        _add_equipment(conn, env, mod, "Machine B", eq_type="vehicle")

        r = call_action(mod.maintenance_list_equipment, conn, ns(
            company_id=env["company_id"],
            equipment_type=None,
            equipment_status=None,
            criticality=None,
            search=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 2


class TestAddEquipmentChild:
    def test_add_equipment_child_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod, "Parent Machine")
        parent_id = r1["id"]

        r2 = call_action(mod.maintenance_add_equipment_child, conn, ns(
            parent_equipment_id=parent_id,
            name="Sub-Assembly A",
            company_id=env["company_id"],
            equipment_type="machine",
            model=None,
            manufacturer=None,
            serial_number=None,
            location=None,
            criticality=None,
            notes=None,
        ))
        assert is_ok(r2), r2
        assert r2["parent_equipment_id"] == parent_id


class TestListEquipmentTree:
    def test_list_equipment_tree(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod, "Root Machine")
        root_id = r1["id"]

        call_action(mod.maintenance_add_equipment_child, conn, ns(
            parent_equipment_id=root_id,
            name="Child 1",
            company_id=env["company_id"],
            equipment_type=None, model=None, manufacturer=None,
            serial_number=None, location=None, criticality=None, notes=None,
        ))

        r = call_action(mod.maintenance_list_equipment_tree, conn, ns(
            equipment_id=root_id,
            company_id=None,
        ))
        assert is_ok(r), r
        assert r["tree"]["id"] == root_id
        assert len(r["tree"]["children"]) == 1


class TestAddEquipmentReading:
    def test_add_reading_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        r2 = call_action(mod.maintenance_add_equipment_reading, conn, ns(
            equipment_id=eq_id,
            reading_value="1500",
            company_id=env["company_id"],
            reading_type="meter",
            reading_unit="hours",
            reading_date=None,
            recorded_by="Operator A",
        ))
        assert is_ok(r2), r2
        assert r2["reading_value"] == "1500"


class TestListEquipmentReadings:
    def test_list_readings(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        for val in ["1000", "1500", "2000"]:
            call_action(mod.maintenance_add_equipment_reading, conn, ns(
                equipment_id=eq_id,
                reading_value=val,
                company_id=env["company_id"],
                reading_type="meter",
                reading_unit="hours",
                reading_date=None,
                recorded_by=None,
            ))

        r = call_action(mod.maintenance_list_equipment_readings, conn, ns(
            equipment_id=eq_id,
            reading_type=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 3


class TestLinkEquipmentAsset:
    def test_link_asset_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        fake_asset_id = _uuid()

        r2 = call_action(mod.maintenance_link_equipment_asset, conn, ns(
            equipment_id=eq_id,
            asset_id=fake_asset_id,
        ))
        assert is_ok(r2), r2
        assert r2["asset_id"] == fake_asset_id


class TestImportEquipment:
    def test_import_stub(self, conn, env, mod):
        r = call_action(mod.maintenance_import_equipment, conn, ns())
        assert is_ok(r), r
        assert r["imported"] == 0


# ===========================================================================
# Maintenance Plan Actions
# ===========================================================================


class TestAddMaintenancePlan:
    def test_add_plan_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        r = _add_plan(conn, env, mod, eq_id)
        assert is_ok(r), r
        assert r["plan_type"] == "preventive"
        assert r["frequency"] == "monthly"
        assert r["frequency_days"] == 30

    def test_add_plan_missing_equipment(self, conn, env, mod):
        r = call_action(mod.maintenance_add_maintenance_plan, conn, ns(
            plan_name="Test Plan",
            equipment_id=None,
            company_id=env["company_id"],
            plan_type=None, frequency=None, frequency_days=None,
            last_performed=None, next_due=None, estimated_duration=None,
            estimated_cost=None, assigned_to=None, instructions=None,
            is_active=None, item_id=None,
        ))
        assert is_error(r)


class TestUpdateMaintenancePlan:
    def test_update_plan_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_plan(conn, env, mod, eq_id)
        plan_id = r2["id"]

        r = call_action(mod.maintenance_update_maintenance_plan, conn, ns(
            plan_id=plan_id,
            plan_name=None,
            plan_type=None,
            frequency="quarterly",
            next_due=None,
            estimated_duration=None,
            estimated_cost=None,
            assigned_to=None,
            instructions=None,
            last_performed=None,
            is_active=None,
            frequency_days=None,
        ))
        assert is_ok(r), r
        assert "frequency" in r["updated_fields"]


class TestGetMaintenancePlan:
    def test_get_plan_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_plan(conn, env, mod, eq_id)
        plan_id = r2["id"]

        r = call_action(mod.maintenance_get_maintenance_plan, conn, ns(
            plan_id=plan_id,
        ))
        assert is_ok(r), r
        assert r["id"] == plan_id
        assert "items" in r


class TestListMaintenancePlans:
    def test_list_plans(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        _add_plan(conn, env, mod, eq_id, "Plan A")
        _add_plan(conn, env, mod, eq_id, "Plan B", frequency="quarterly")

        r = call_action(mod.maintenance_list_maintenance_plans, conn, ns(
            company_id=env["company_id"],
            equipment_id=None,
            plan_type=None,
            is_active=None,
            search=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 2


class TestAddPlanItem:
    def test_add_plan_item_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_plan(conn, env, mod, eq_id)
        plan_id = r2["id"]

        r = call_action(mod.maintenance_add_plan_item, conn, ns(
            plan_id=plan_id,
            item_name="Lubricant Oil 10W-40",
            company_id=env["company_id"],
            item_id=None,
            quantity="2",
            notes=None,
        ))
        assert is_ok(r), r
        assert r["item_name"] == "Lubricant Oil 10W-40"


class TestListPlanItems:
    def test_list_plan_items(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_plan(conn, env, mod, eq_id)
        plan_id = r2["id"]

        call_action(mod.maintenance_add_plan_item, conn, ns(
            plan_id=plan_id,
            item_name="Oil Filter",
            company_id=env["company_id"],
            item_id=None, quantity="1", notes=None,
        ))

        r = call_action(mod.maintenance_list_plan_items, conn, ns(
            plan_id=plan_id,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 1


# ===========================================================================
# Work Order Actions
# ===========================================================================


class TestAddMaintenanceWorkOrder:
    def test_add_work_order_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        r = _add_work_order(conn, env, mod, eq_id)
        assert is_ok(r), r
        assert r["work_order_type"] == "corrective"
        assert r["wo_status"] == "draft"

    def test_add_work_order_missing_equipment(self, conn, env, mod):
        r = call_action(mod.maintenance_add_maintenance_work_order, conn, ns(
            equipment_id=None,
            company_id=env["company_id"],
            plan_id=None, work_order_type=None, priority=None,
            description=None, assigned_to=None, scheduled_date=None,
            failure_mode=None, wo_status=None,
        ))
        assert is_error(r)


class TestUpdateMaintenanceWorkOrder:
    def test_update_work_order_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]

        r = call_action(mod.maintenance_update_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
            work_order_type=None,
            priority="high",
            description=None,
            assigned_to=None,
            scheduled_date=None,
            failure_mode=None,
            root_cause=None,
            resolution=None,
            actual_duration=None,
            actual_cost=None,
            wo_status=None,
        ))
        assert is_ok(r), r
        assert "priority" in r["updated_fields"]


class TestGetMaintenanceWorkOrder:
    def test_get_work_order_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]

        r = call_action(mod.maintenance_get_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
        ))
        assert is_ok(r), r
        assert r["id"] == wo_id
        assert "items" in r
        assert "checklists" in r


class TestListMaintenanceWorkOrders:
    def test_list_work_orders(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        _add_work_order(conn, env, mod, eq_id)
        _add_work_order(conn, env, mod, eq_id, wo_type="emergency")

        r = call_action(mod.maintenance_list_maintenance_work_orders, conn, ns(
            company_id=env["company_id"],
            equipment_id=None,
            wo_status=None,
            work_order_type=None,
            priority=None,
            plan_id=None,
            search=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 2


class TestAddWoItem:
    def test_add_wo_item_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]

        r = call_action(mod.maintenance_add_wo_item, conn, ns(
            work_order_id=wo_id,
            item_name="Bearing 6205",
            company_id=env["company_id"],
            item_id=None,
            quantity="2",
            unit_cost="25.00",
            notes=None,
        ))
        assert is_ok(r), r
        assert r["total_cost"] == "50.00"


class TestListWoItems:
    def test_list_wo_items(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]

        call_action(mod.maintenance_add_wo_item, conn, ns(
            work_order_id=wo_id,
            item_name="Bearing 6205",
            company_id=env["company_id"],
            item_id=None, quantity="2", unit_cost="25.00", notes=None,
        ))

        r = call_action(mod.maintenance_list_wo_items, conn, ns(
            work_order_id=wo_id,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 1


class TestStartWorkOrder:
    def test_start_work_order_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]

        r = call_action(mod.maintenance_start_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
        ))
        assert is_ok(r), r
        assert r["wo_status"] == "in_progress"

    def test_start_completed_fails(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]

        # Start then complete
        call_action(mod.maintenance_start_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
        ))
        call_action(mod.maintenance_complete_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
            actual_duration=None,
            actual_cost=None,
            resolution=None,
            root_cause=None,
        ))

        # Try to start again
        r = call_action(mod.maintenance_start_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
        ))
        assert is_error(r)


class TestCompleteWorkOrder:
    def test_complete_work_order_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]

        # Start first
        call_action(mod.maintenance_start_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
        ))

        r = call_action(mod.maintenance_complete_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
            actual_duration="2h",
            actual_cost="150.00",
            resolution="Replaced bearing",
            root_cause="Wear and tear",
        ))
        assert is_ok(r), r
        assert r["wo_status"] == "completed"
        assert r["actual_cost"] == "150.00"

    def test_complete_updates_plan(self, conn, env, mod):
        """Completing a WO linked to a plan should update plan.last_performed."""
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        pr = _add_plan(conn, env, mod, eq_id, next_due="2025-01-01")
        plan_id = pr["id"]

        wr = _add_work_order(conn, env, mod, eq_id, plan_id=plan_id,
                             wo_type="preventive")
        wo_id = wr["id"]

        call_action(mod.maintenance_start_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
        ))
        r = call_action(mod.maintenance_complete_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
            actual_duration=None,
            actual_cost=None,
            resolution=None,
            root_cause=None,
        ))
        assert is_ok(r), r
        assert r["plan_updated"] is True


class TestCancelWorkOrder:
    def test_cancel_work_order_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]

        r = call_action(mod.maintenance_cancel_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
        ))
        assert is_ok(r), r
        assert r["wo_status"] == "cancelled"

    def test_cancel_completed_fails(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]

        # Start then complete
        call_action(mod.maintenance_start_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
        ))
        call_action(mod.maintenance_complete_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
            actual_duration=None, actual_cost=None,
            resolution=None, root_cause=None,
        ))

        r = call_action(mod.maintenance_cancel_maintenance_work_order, conn, ns(
            work_order_id=wo_id,
        ))
        assert is_error(r)


class TestGeneratePreventiveWorkOrders:
    def test_generate_preventive_wos(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        # Create a plan with next_due in the past
        _add_plan(conn, env, mod, eq_id, "Overdue Plan", next_due="2025-01-01")

        r = call_action(mod.maintenance_generate_preventive_work_orders, conn, ns(
            company_id=env["company_id"],
            as_of_date="2025-07-01",
        ))
        assert is_ok(r), r
        assert r["generated"] >= 1

    def test_generate_no_overdue(self, conn, env, mod):
        """No plans overdue = 0 generated."""
        r = call_action(mod.maintenance_generate_preventive_work_orders, conn, ns(
            company_id=env["company_id"],
            as_of_date="2025-01-01",
        ))
        assert is_ok(r), r
        assert r["generated"] == 0


# ===========================================================================
# Downtime Actions
# ===========================================================================


class TestAddDowntimeRecord:
    def test_add_downtime_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        r = call_action(mod.maintenance_add_downtime_record, conn, ns(
            equipment_id=eq_id,
            company_id=env["company_id"],
            reason="breakdown",
            start_time="2025-06-15T08:00:00Z",
            end_time="2025-06-15T12:00:00Z",
            duration_hours="4",
            description="Motor failure",
            impact="Production line stopped",
            work_order_id=None,
        ))
        assert is_ok(r), r
        assert r["reason"] == "breakdown"
        assert r["duration_hours"] == "4"


class TestListDowntimeRecords:
    def test_list_downtime(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]

        for reason in ["breakdown", "maintenance"]:
            call_action(mod.maintenance_add_downtime_record, conn, ns(
                equipment_id=eq_id,
                company_id=env["company_id"],
                reason=reason,
                start_time=None, end_time=None, duration_hours="2",
                description=None, impact=None, work_order_id=None,
            ))

        r = call_action(mod.maintenance_list_downtime_records, conn, ns(
            equipment_id=eq_id,
            company_id=None,
            work_order_id=None,
            reason=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 2


# ===========================================================================
# Checklist Actions
# ===========================================================================


class TestAddChecklist:
    def test_add_checklist_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]

        r = call_action(mod.maintenance_add_checklist, conn, ns(
            work_order_id=wo_id,
            checklist_name="Pre-Start Checklist",
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert r["name"] == "Pre-Start Checklist"


class TestGetChecklist:
    def test_get_checklist_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]
        r3 = call_action(mod.maintenance_add_checklist, conn, ns(
            work_order_id=wo_id,
            checklist_name="Safety Checklist",
            company_id=env["company_id"],
        ))
        cl_id = r3["id"]

        r = call_action(mod.maintenance_get_checklist, conn, ns(
            checklist_id=cl_id,
        ))
        assert is_ok(r), r
        assert r["id"] == cl_id
        assert "items" in r


class TestAddChecklistItem:
    def test_add_checklist_item_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]
        r3 = call_action(mod.maintenance_add_checklist, conn, ns(
            work_order_id=wo_id,
            checklist_name="Test CL",
            company_id=env["company_id"],
        ))
        cl_id = r3["id"]

        r = call_action(mod.maintenance_add_checklist_item, conn, ns(
            checklist_id=cl_id,
            description="Check oil level",
            sort_order=1,
            notes=None,
        ))
        assert is_ok(r), r
        assert r["is_completed"] == 0


class TestCompleteChecklistItem:
    def test_complete_checklist_item_ok(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]
        r3 = call_action(mod.maintenance_add_checklist, conn, ns(
            work_order_id=wo_id,
            checklist_name="Test CL",
            company_id=env["company_id"],
        ))
        cl_id = r3["id"]
        r4 = call_action(mod.maintenance_add_checklist_item, conn, ns(
            checklist_id=cl_id,
            description="Verify pressure",
            sort_order=1,
            notes=None,
        ))
        item_id = r4["id"]

        r = call_action(mod.maintenance_complete_checklist_item, conn, ns(
            checklist_item_id=item_id,
            completed_by="Tech A",
            notes="Pressure is 45 PSI, within range",
        ))
        assert is_ok(r), r
        assert r["is_completed"] == 1

    def test_complete_already_completed_fails(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        r2 = _add_work_order(conn, env, mod, eq_id)
        wo_id = r2["id"]
        r3 = call_action(mod.maintenance_add_checklist, conn, ns(
            work_order_id=wo_id,
            checklist_name="Test CL",
            company_id=env["company_id"],
        ))
        cl_id = r3["id"]
        r4 = call_action(mod.maintenance_add_checklist_item, conn, ns(
            checklist_id=cl_id,
            description="Check valve",
            sort_order=1,
            notes=None,
        ))
        item_id = r4["id"]

        # Complete once
        call_action(mod.maintenance_complete_checklist_item, conn, ns(
            checklist_item_id=item_id,
            completed_by=None, notes=None,
        ))

        # Try again
        r = call_action(mod.maintenance_complete_checklist_item, conn, ns(
            checklist_item_id=item_id,
            completed_by=None, notes=None,
        ))
        assert is_error(r)


# ===========================================================================
# Reports
# ===========================================================================


class TestEquipmentStatusReport:
    def test_equipment_status_report(self, conn, env, mod):
        _add_equipment(conn, env, mod, "Machine A")
        _add_equipment(conn, env, mod, "Machine B")

        r = call_action(mod.maintenance_equipment_status_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert r["total_equipment"] == 2


class TestMaintenanceCostReport:
    def test_cost_report(self, conn, env, mod):
        r = call_action(mod.maintenance_cost_report, conn, ns(
            company_id=env["company_id"],
            equipment_id=None,
            from_date=None,
            to_date=None,
        ))
        assert is_ok(r), r
        assert "grand_total" in r


class TestPMComplianceReport:
    def test_pm_compliance_report(self, conn, env, mod):
        r = call_action(mod.maintenance_pm_compliance_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert "compliance_pct" in r


class TestDowntimeReport:
    def test_downtime_report(self, conn, env, mod):
        r = call_action(mod.maintenance_downtime_report, conn, ns(
            company_id=env["company_id"],
            from_date=None,
            to_date=None,
        ))
        assert is_ok(r), r
        assert "grand_total_hours" in r


class TestSparePartsUsage:
    def test_spare_parts_usage(self, conn, env, mod):
        r = call_action(mod.maintenance_spare_parts_usage, conn, ns(
            company_id=env["company_id"],
            limit=20,
        ))
        assert is_ok(r), r
        assert "items" in r


class TestEquipmentHistory:
    def test_equipment_history(self, conn, env, mod):
        r1 = _add_equipment(conn, env, mod)
        eq_id = r1["id"]
        _add_work_order(conn, env, mod, eq_id)

        r = call_action(mod.maintenance_equipment_history, conn, ns(
            equipment_id=eq_id,
            limit=50,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] >= 1


class TestModuleStatus:
    def test_status(self, conn, env, mod):
        r = call_action(mod.status, conn, ns())
        assert is_ok(r), r
        assert r["skill"] == "erpclaw-maintenance"
        assert r["actions_available"] == 39
