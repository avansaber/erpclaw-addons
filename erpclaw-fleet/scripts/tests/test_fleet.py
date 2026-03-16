"""L1 tests for ERPClaw Fleet module (15 actions, 4 tables).

Actions tested:
  Vehicles:       fleet-add-vehicle, fleet-update-vehicle, fleet-get-vehicle, fleet-list-vehicles
  Assignments:    fleet-add-vehicle-assignment, fleet-end-vehicle-assignment, fleet-list-vehicle-assignments
  Fuel logs:      fleet-add-fuel-log, fleet-list-fuel-logs
  Maintenance:    fleet-add-vehicle-maintenance, fleet-complete-vehicle-maintenance, fleet-list-vehicle-maintenance
  Reports:        fleet-vehicle-cost-report, fleet-vehicle-utilization-report
  Status:         status
"""
import pytest
from fleet_helpers import call_action, ns, is_error, is_ok, load_db_query, seed_vehicle

mod = load_db_query()


# =============================================================================
# Vehicles
# =============================================================================

class TestAddVehicle:
    def test_basic_create(self, conn, env):
        result = call_action(mod.fleet_add_vehicle, conn, ns(
            company_id=env["company_id"],
            make="Honda",
            model="Civic",
            year="2025",
            vehicle_type="sedan",
            fuel_type="gasoline",
        ))
        assert is_ok(result), result
        assert result["make"] == "Honda"
        assert result["model"] == "Civic"
        assert result["vehicle_status"] == "available"
        assert "id" in result
        assert "naming_series" in result

    def test_create_truck(self, conn, env):
        result = call_action(mod.fleet_add_vehicle, conn, ns(
            company_id=env["company_id"],
            make="Ford",
            model="F-150",
            vehicle_type="truck",
            fuel_type="diesel",
        ))
        assert is_ok(result), result
        assert result["vehicle_type"] == "truck"

    def test_missing_make_fails(self, conn, env):
        result = call_action(mod.fleet_add_vehicle, conn, ns(
            company_id=env["company_id"],
            make=None,
            model="Civic",
        ))
        assert is_error(result)

    def test_missing_model_fails(self, conn, env):
        result = call_action(mod.fleet_add_vehicle, conn, ns(
            company_id=env["company_id"],
            make="Honda",
            model=None,
        ))
        assert is_error(result)

    def test_missing_company_fails(self, conn, env):
        result = call_action(mod.fleet_add_vehicle, conn, ns(
            company_id=None,
            make="Honda",
            model="Civic",
        ))
        assert is_error(result)

    def test_invalid_vehicle_type_fails(self, conn, env):
        result = call_action(mod.fleet_add_vehicle, conn, ns(
            company_id=env["company_id"],
            make="Honda",
            model="Civic",
            vehicle_type="spaceship",
        ))
        assert is_error(result)

    def test_invalid_fuel_type_fails(self, conn, env):
        result = call_action(mod.fleet_add_vehicle, conn, ns(
            company_id=env["company_id"],
            make="Honda",
            model="Civic",
            fuel_type="nuclear",
        ))
        assert is_error(result)


class TestUpdateVehicle:
    def test_update_color(self, conn, env):
        result = call_action(mod.fleet_update_vehicle, conn, ns(
            vehicle_id=env["vehicle_id"],
            color="red",
        ))
        assert is_ok(result), result
        assert "color" in result["updated_fields"]

    def test_update_status(self, conn, env):
        result = call_action(mod.fleet_update_vehicle, conn, ns(
            vehicle_id=env["vehicle_id"],
            vehicle_status="maintenance",
        ))
        assert is_ok(result), result
        assert "vehicle_status" in result["updated_fields"]

    def test_no_fields_fails(self, conn, env):
        result = call_action(mod.fleet_update_vehicle, conn, ns(
            vehicle_id=env["vehicle_id"],
        ))
        assert is_error(result)

    def test_missing_id_fails(self, conn, env):
        result = call_action(mod.fleet_update_vehicle, conn, ns(
            vehicle_id=None,
            color="red",
        ))
        assert is_error(result)

    def test_invalid_status_fails(self, conn, env):
        result = call_action(mod.fleet_update_vehicle, conn, ns(
            vehicle_id=env["vehicle_id"],
            vehicle_status="flying",
        ))
        assert is_error(result)


class TestGetVehicle:
    def test_get_existing(self, conn, env):
        result = call_action(mod.fleet_get_vehicle, conn, ns(
            vehicle_id=env["vehicle_id"],
        ))
        assert is_ok(result), result
        assert result["make"] == "Toyota"
        assert "active_assignments" in result
        assert "fuel_log_count" in result
        assert "maintenance_count" in result

    def test_get_missing_fails(self, conn, env):
        result = call_action(mod.fleet_get_vehicle, conn, ns(
            vehicle_id="nonexistent-id",
        ))
        assert is_error(result)

    def test_get_no_id_fails(self, conn, env):
        result = call_action(mod.fleet_get_vehicle, conn, ns(
            vehicle_id=None,
        ))
        assert is_error(result)


class TestListVehicles:
    def test_list_by_company(self, conn, env):
        result = call_action(mod.fleet_list_vehicles, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1
        assert len(result["rows"]) >= 1

    def test_list_filter_by_status(self, conn, env):
        result = call_action(mod.fleet_list_vehicles, conn, ns(
            company_id=env["company_id"],
            vehicle_status="available",
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_list_empty(self, conn, env):
        result = call_action(mod.fleet_list_vehicles, conn, ns(
            vehicle_status="retired",
        ))
        assert is_ok(result), result
        assert result["total_count"] == 0


# =============================================================================
# Assignments
# =============================================================================

class TestAddVehicleAssignment:
    def test_basic_assignment(self, conn, env):
        result = call_action(mod.fleet_add_vehicle_assignment, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            driver_name="John Smith",
            start_date="2026-03-01",
        ))
        assert is_ok(result), result
        assert result["driver_name"] == "John Smith"
        assert result["assignment_status"] == "active"
        assert "id" in result

        # Verify vehicle status changed to assigned
        veh = call_action(mod.fleet_get_vehicle, conn, ns(
            vehicle_id=env["vehicle_id"],
        ))
        assert veh["vehicle_status"] == "assigned"

    def test_missing_driver_fails(self, conn, env):
        result = call_action(mod.fleet_add_vehicle_assignment, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            driver_name=None,
            start_date="2026-03-01",
        ))
        assert is_error(result)

    def test_missing_start_date_fails(self, conn, env):
        result = call_action(mod.fleet_add_vehicle_assignment, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            driver_name="Jane Doe",
            start_date=None,
        ))
        assert is_error(result)


class TestEndVehicleAssignment:
    def _assign(self, conn, env):
        result = call_action(mod.fleet_add_vehicle_assignment, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            driver_name="John Smith",
            start_date="2026-03-01",
        ))
        return result["id"]

    def test_end_assignment(self, conn, env):
        assign_id = self._assign(conn, env)
        result = call_action(mod.fleet_end_vehicle_assignment, conn, ns(
            assignment_id=assign_id,
            end_date="2026-03-15",
        ))
        assert is_ok(result), result
        assert result["assignment_status"] == "ended"
        assert result["end_date"] == "2026-03-15"

        # Vehicle should return to available
        veh = call_action(mod.fleet_get_vehicle, conn, ns(
            vehicle_id=env["vehicle_id"],
        ))
        assert veh["vehicle_status"] == "available"

    def test_end_already_ended_fails(self, conn, env):
        assign_id = self._assign(conn, env)
        call_action(mod.fleet_end_vehicle_assignment, conn, ns(
            assignment_id=assign_id,
        ))
        result = call_action(mod.fleet_end_vehicle_assignment, conn, ns(
            assignment_id=assign_id,
        ))
        assert is_error(result)

    def test_end_nonexistent_fails(self, conn, env):
        result = call_action(mod.fleet_end_vehicle_assignment, conn, ns(
            assignment_id="nonexistent-id",
        ))
        assert is_error(result)


class TestListVehicleAssignments:
    def test_list_empty(self, conn, env):
        result = call_action(mod.fleet_list_vehicle_assignments, conn, ns(
            vehicle_id=env["vehicle_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] == 0

    def test_list_after_assignment(self, conn, env):
        call_action(mod.fleet_add_vehicle_assignment, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            driver_name="Driver A",
            start_date="2026-03-01",
        ))
        result = call_action(mod.fleet_list_vehicle_assignments, conn, ns(
            vehicle_id=env["vehicle_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] == 1


# =============================================================================
# Fuel Logs
# =============================================================================

class TestAddFuelLog:
    def test_basic_fuel_log(self, conn, env):
        result = call_action(mod.fleet_add_fuel_log, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            log_date="2026-03-10",
            gallons="12.5",
            cost="45.00",
            odometer_reading="5000",
            station="Shell Station",
        ))
        assert is_ok(result), result
        assert result["gallons"] == "12.5"
        assert result["cost"] == "45.00"
        assert "id" in result

    def test_fuel_log_updates_odometer(self, conn, env):
        call_action(mod.fleet_add_fuel_log, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            log_date="2026-03-10",
            gallons="10",
            cost="35.00",
            odometer_reading="10000",
        ))
        veh = call_action(mod.fleet_get_vehicle, conn, ns(
            vehicle_id=env["vehicle_id"],
        ))
        assert veh["current_odometer"] == "10000"

    def test_missing_gallons_fails(self, conn, env):
        result = call_action(mod.fleet_add_fuel_log, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            log_date="2026-03-10",
            gallons=None,
            cost="45.00",
        ))
        assert is_error(result)

    def test_missing_cost_fails(self, conn, env):
        result = call_action(mod.fleet_add_fuel_log, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            log_date="2026-03-10",
            gallons="10",
            cost=None,
        ))
        assert is_error(result)


class TestListFuelLogs:
    def test_list_empty(self, conn, env):
        result = call_action(mod.fleet_list_fuel_logs, conn, ns(
            vehicle_id=env["vehicle_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] == 0

    def test_list_with_date_filter(self, conn, env):
        call_action(mod.fleet_add_fuel_log, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            log_date="2026-03-10",
            gallons="10",
            cost="35.00",
        ))
        result = call_action(mod.fleet_list_fuel_logs, conn, ns(
            vehicle_id=env["vehicle_id"],
            start_date="2026-03-01",
            end_date="2026-03-31",
        ))
        assert is_ok(result), result
        assert result["total_count"] == 1


# =============================================================================
# Maintenance
# =============================================================================

class TestAddVehicleMaintenance:
    def test_basic_maintenance(self, conn, env):
        result = call_action(mod.fleet_add_vehicle_maintenance, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            maintenance_type="oil_change",
            scheduled_date="2026-04-01",
            cost="75.00",
            vendor="Quick Lube",
        ))
        assert is_ok(result), result
        assert result["maintenance_type"] == "oil_change"
        assert result["maintenance_status"] == "scheduled"
        assert result["cost"] == "75.00"
        assert "naming_series" in result

    def test_missing_maintenance_type_fails(self, conn, env):
        result = call_action(mod.fleet_add_vehicle_maintenance, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            maintenance_type=None,
        ))
        assert is_error(result)

    def test_invalid_maintenance_type_fails(self, conn, env):
        result = call_action(mod.fleet_add_vehicle_maintenance, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            maintenance_type="warp_drive_alignment",
        ))
        assert is_error(result)


class TestCompleteVehicleMaintenance:
    def _create_maint(self, conn, env):
        result = call_action(mod.fleet_add_vehicle_maintenance, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            maintenance_type="inspection",
            scheduled_date="2026-04-01",
        ))
        return result["id"]

    def test_complete(self, conn, env):
        maint_id = self._create_maint(conn, env)
        result = call_action(mod.fleet_complete_vehicle_maintenance, conn, ns(
            maintenance_id=maint_id,
            completed_date="2026-04-05",
            cost="120.00",
            vendor="City Auto",
        ))
        assert is_ok(result), result
        assert result["maintenance_status"] == "completed"
        assert result["completed_date"] == "2026-04-05"

    def test_complete_already_completed_fails(self, conn, env):
        maint_id = self._create_maint(conn, env)
        call_action(mod.fleet_complete_vehicle_maintenance, conn, ns(
            maintenance_id=maint_id,
        ))
        result = call_action(mod.fleet_complete_vehicle_maintenance, conn, ns(
            maintenance_id=maint_id,
        ))
        assert is_error(result)

    def test_complete_nonexistent_fails(self, conn, env):
        result = call_action(mod.fleet_complete_vehicle_maintenance, conn, ns(
            maintenance_id="nonexistent-id",
        ))
        assert is_error(result)


class TestListVehicleMaintenance:
    def test_list_empty(self, conn, env):
        result = call_action(mod.fleet_list_vehicle_maintenance, conn, ns(
            vehicle_id=env["vehicle_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] == 0

    def test_list_by_type(self, conn, env):
        call_action(mod.fleet_add_vehicle_maintenance, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            maintenance_type="tire_rotation",
        ))
        result = call_action(mod.fleet_list_vehicle_maintenance, conn, ns(
            vehicle_id=env["vehicle_id"],
            maintenance_type="tire_rotation",
        ))
        assert is_ok(result), result
        assert result["total_count"] == 1


# =============================================================================
# Reports
# =============================================================================

class TestVehicleCostReport:
    def test_basic_report(self, conn, env):
        result = call_action(mod.fleet_vehicle_cost_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["report"] == "vehicle-cost"
        assert result["total_vehicles"] >= 1
        assert len(result["vehicles"]) >= 1

    def test_report_with_costs(self, conn, env):
        # Add fuel and maintenance costs
        call_action(mod.fleet_add_fuel_log, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            log_date="2026-03-10",
            gallons="10",
            cost="35.00",
        ))
        call_action(mod.fleet_add_vehicle_maintenance, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            maintenance_type="oil_change",
            cost="50.00",
        ))
        # Complete the maintenance to set the cost
        result = call_action(mod.fleet_vehicle_cost_report, conn, ns(
            company_id=env["company_id"],
            vehicle_id=env["vehicle_id"],
        ))
        assert is_ok(result), result
        assert len(result["vehicles"]) == 1
        veh = result["vehicles"][0]
        assert veh["total_fuel_cost"] == "35.00"
        assert veh["total_maintenance_cost"] == "50.00"
        assert veh["total_cost"] == "85.00"

    def test_report_missing_company_fails(self, conn, env):
        result = call_action(mod.fleet_vehicle_cost_report, conn, ns(
            company_id=None,
        ))
        assert is_error(result)


class TestVehicleUtilizationReport:
    def test_basic_utilization(self, conn, env):
        result = call_action(mod.fleet_vehicle_utilization_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["report"] == "vehicle-utilization"
        assert result["total_vehicles"] >= 1
        assert "utilization_rate_pct" in result

    def test_utilization_with_assignment(self, conn, env):
        call_action(mod.fleet_add_vehicle_assignment, conn, ns(
            vehicle_id=env["vehicle_id"],
            company_id=env["company_id"],
            driver_name="Test Driver",
            start_date="2026-03-01",
        ))
        result = call_action(mod.fleet_vehicle_utilization_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        # 1 vehicle assigned out of 1 total = 100%
        assert result["utilization_rate_pct"] == "100.00"
        assert result["active_assignments"] == 1


# =============================================================================
# Status
# =============================================================================

class TestStatus:
    def test_status(self, conn, env):
        result = call_action(mod.status, conn, ns())
        assert is_ok(result), result
        assert result["skill"] == "erpclaw-fleet"
        assert result["total_tables"] == 4
        assert "record_counts" in result
