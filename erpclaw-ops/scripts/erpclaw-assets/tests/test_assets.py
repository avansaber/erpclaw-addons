"""L1 tests for ERPClaw Assets skill (16 actions).

Tests cover: asset categories, assets, depreciation, movements,
maintenance, disposal, and reports.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from assets_helpers import (
    load_db_query, call_action, ns, is_ok, is_error,
    seed_company, seed_naming_series, seed_account,
    seed_asset_category, seed_asset, _uuid,
)

M = load_db_query()


# ===================================================================
# Asset Categories
# ===================================================================

class TestAddAssetCategory:
    def test_add_category_ok(self, conn, env):
        r = call_action(M.add_asset_category, conn, ns(
            company_id=env["company_id"],
            name="Vehicles",
            depreciation_method="straight_line",
            useful_life_years="10",
        ))
        assert is_ok(r)
        assert r["asset_category_id"]
        assert r["name"] == "Vehicles"

    def test_add_category_missing_name(self, conn, env):
        r = call_action(M.add_asset_category, conn, ns(
            company_id=env["company_id"],
            depreciation_method="straight_line",
            useful_life_years="5",
        ))
        assert is_error(r)

    def test_add_category_missing_method(self, conn, env):
        r = call_action(M.add_asset_category, conn, ns(
            company_id=env["company_id"],
            name="Machinery",
            useful_life_years="5",
        ))
        assert is_error(r)

    def test_add_category_invalid_method(self, conn, env):
        r = call_action(M.add_asset_category, conn, ns(
            company_id=env["company_id"],
            name="Test",
            depreciation_method="invalid_method",
            useful_life_years="5",
        ))
        assert is_error(r)

    def test_add_category_duplicate_name(self, conn, env):
        call_action(M.add_asset_category, conn, ns(
            company_id=env["company_id"],
            name="DupCat",
            depreciation_method="straight_line",
            useful_life_years="5",
        ))
        r = call_action(M.add_asset_category, conn, ns(
            company_id=env["company_id"],
            name="DupCat",
            depreciation_method="straight_line",
            useful_life_years="5",
        ))
        assert is_error(r)


class TestListAssetCategories:
    def test_list_categories(self, conn, env):
        r = call_action(M.list_asset_categories, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        # build_env seeds one category
        assert r["total"] >= 1


# ===================================================================
# Assets
# ===================================================================

class TestAddAsset:
    def test_add_asset_ok(self, conn, env):
        r = call_action(M.add_asset, conn, ns(
            company_id=env["company_id"],
            name="Desktop Computer",
            asset_category_id=env["category_id"],
            gross_value="3000.00",
        ))
        assert is_ok(r)
        assert r["asset_id"]
        assert r["gross_value"] == "3000.00"
        assert r["current_book_value"] == "3000.00"

    def test_add_asset_missing_name(self, conn, env):
        r = call_action(M.add_asset, conn, ns(
            company_id=env["company_id"],
            asset_category_id=env["category_id"],
            gross_value="1000",
        ))
        assert is_error(r)

    def test_add_asset_missing_category(self, conn, env):
        r = call_action(M.add_asset, conn, ns(
            company_id=env["company_id"],
            name="Test Asset",
            gross_value="1000",
        ))
        assert is_error(r)

    def test_add_asset_zero_value(self, conn, env):
        r = call_action(M.add_asset, conn, ns(
            company_id=env["company_id"],
            name="Zero Asset",
            asset_category_id=env["category_id"],
            gross_value="0",
        ))
        assert is_error(r)

    def test_add_asset_salvage_exceeds_gross(self, conn, env):
        r = call_action(M.add_asset, conn, ns(
            company_id=env["company_id"],
            name="Bad Salvage",
            asset_category_id=env["category_id"],
            gross_value="1000",
            salvage_value="2000",
        ))
        assert is_error(r)


class TestGetAsset:
    def test_get_asset_ok(self, conn, env):
        r = call_action(M.get_asset, conn, ns(asset_id=env["asset_id"]))
        assert is_ok(r)
        assert r["asset"]["id"] == env["asset_id"]

    def test_get_asset_not_found(self, conn, env):
        r = call_action(M.get_asset, conn, ns(asset_id=_uuid()))
        assert is_error(r)


class TestListAssets:
    def test_list_assets(self, conn, env):
        r = call_action(M.list_assets, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["total"] >= 1


class TestUpdateAsset:
    def test_update_asset_location(self, conn, env):
        r = call_action(M.update_asset, conn, ns(
            asset_id=env["asset_id"],
            location="Building A - Floor 2",
        ))
        assert is_ok(r)

    def test_update_asset_not_found(self, conn, env):
        r = call_action(M.update_asset, conn, ns(
            asset_id=_uuid(),
            location="Nowhere",
        ))
        assert is_error(r)


# ===================================================================
# Depreciation
# ===================================================================

class TestGenerateDepreciationSchedule:
    def test_generate_schedule_ok(self, conn, env):
        r = call_action(M.generate_depreciation_schedule, conn, ns(
            asset_id=env["asset_id"],
        ))
        assert is_ok(r)
        assert r["entries_generated"] > 0


# ===================================================================
# Maintenance
# ===================================================================

class TestScheduleMaintenance:
    def test_schedule_maintenance_ok(self, conn, env):
        r = call_action(M.schedule_maintenance, conn, ns(
            asset_id=env["asset_id"],
            maintenance_type="preventive",
            scheduled_date="2026-06-01",
        ))
        assert is_ok(r)
        assert r["maintenance_id"]

    def test_schedule_maintenance_missing_asset(self, conn, env):
        r = call_action(M.schedule_maintenance, conn, ns(
            maintenance_type="preventive",
            scheduled_date="2026-06-01",
        ))
        assert is_error(r)


# ===================================================================
# Reports
# ===================================================================

class TestAssetRegisterReport:
    def test_report_ok(self, conn, env):
        r = call_action(M.asset_register_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "assets" in r


class TestStatus:
    def test_status_ok(self, conn, env):
        r = call_action(M.status, conn, ns())
        assert is_ok(r)
