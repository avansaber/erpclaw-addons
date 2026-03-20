"""L1 tests for ERPClaw Integrations -- sync domain (14 actions).

Actions tested:
  Syncs:      integration-start-sync, integration-get-sync, integration-list-syncs,
              integration-cancel-sync, integration-retry-sync
  Schedules:  integration-add-sync-schedule, integration-update-sync-schedule,
              integration-list-sync-schedules, integration-delete-sync-schedule
  Errors:     integration-add-sync-error, integration-list-sync-errors,
              integration-resolve-sync-error
  Reports:    integration-sync-summary-report, integration-get-sync-log
  Mappings:   integration-add-field-mapping, integration-update-field-mapping,
              integration-get-field-mapping, integration-list-field-mappings,
              integration-delete-field-mapping
  Entity maps: integration-add-entity-map, integration-get-entity-map,
               integration-list-entity-maps, integration-delete-entity-map
  Transforms: integration-add-transform-rule, integration-list-transform-rules,
              integration-delete-transform-rule
  Status:     status
"""
import pytest
from integration_helpers import call_action, ns, is_error, is_ok, load_db_query

mod = load_db_query()


# =============================================================================
# Syncs
# =============================================================================

class TestStartSync:
    def test_start_sync(self, conn, env):
        result = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        assert is_ok(result), result
        assert result["sync_status"] == "running"
        assert result["sync_type"] == "full"
        assert result["direction"] == "inbound"
        assert "id" in result
        assert "naming_series" in result

    def test_start_sync_incremental(self, conn, env):
        result = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="incremental",
            direction="outbound",
            entity_type="products",
        ))
        assert is_ok(result), result
        assert result["sync_type"] == "incremental"
        assert result["direction"] == "outbound"

    def test_missing_sync_type_fails(self, conn, env):
        result = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            direction="inbound",
        ))
        assert is_error(result)

    def test_missing_direction_fails(self, conn, env):
        result = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
        ))
        assert is_error(result)

    def test_invalid_sync_type_fails(self, conn, env):
        result = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="invalid",
            direction="inbound",
        ))
        assert is_error(result)


class TestGetSync:
    def test_get_sync(self, conn, env):
        start = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        result = call_action(mod.integration_get_sync, conn, ns(
            sync_id=start["id"],
        ))
        assert is_ok(result), result
        assert result["id"] == start["id"]
        assert "error_count" in result

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.integration_get_sync, conn, ns(
            sync_id="nonexistent-id",
        ))
        assert is_error(result)


class TestListSyncs:
    def test_list_by_connector(self, conn, env):
        call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        result = call_action(mod.integration_list_syncs, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_list_by_company(self, conn, env):
        call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        result = call_action(mod.integration_list_syncs, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1


class TestCancelSync:
    def test_cancel_running_sync(self, conn, env):
        start = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        result = call_action(mod.integration_cancel_sync, conn, ns(
            sync_id=start["id"],
        ))
        assert is_ok(result), result
        assert result["sync_status"] == "cancelled"

    def test_cancel_already_cancelled_fails(self, conn, env):
        start = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        call_action(mod.integration_cancel_sync, conn, ns(
            sync_id=start["id"],
        ))
        result = call_action(mod.integration_cancel_sync, conn, ns(
            sync_id=start["id"],
        ))
        assert is_error(result)


class TestRetrySync:
    def test_retry_cancelled_sync(self, conn, env):
        start = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        call_action(mod.integration_cancel_sync, conn, ns(
            sync_id=start["id"],
        ))
        result = call_action(mod.integration_retry_sync, conn, ns(
            sync_id=start["id"],
        ))
        assert is_ok(result), result
        assert result["sync_status"] == "running"
        assert result["original_sync_id"] == start["id"]
        assert result["id"] != start["id"]  # new sync

    def test_retry_running_sync_fails(self, conn, env):
        start = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        result = call_action(mod.integration_retry_sync, conn, ns(
            sync_id=start["id"],
        ))
        assert is_error(result)


# =============================================================================
# Sync Schedules
# =============================================================================

class TestSyncSchedules:
    def test_add_schedule(self, conn, env):
        result = call_action(mod.integration_add_sync_schedule, conn, ns(
            connector_id=env["connector_id"],
            entity_type="products",
            frequency="daily",
        ))
        assert is_ok(result), result
        assert result["entity_type"] == "products"
        assert result["frequency"] == "daily"
        assert result["is_active"] == 1

    def test_add_schedule_missing_entity_type_fails(self, conn, env):
        result = call_action(mod.integration_add_sync_schedule, conn, ns(
            connector_id=env["connector_id"],
            frequency="daily",
        ))
        assert is_error(result)

    def test_add_schedule_missing_frequency_fails(self, conn, env):
        result = call_action(mod.integration_add_sync_schedule, conn, ns(
            connector_id=env["connector_id"],
            entity_type="products",
        ))
        assert is_error(result)

    def test_update_schedule(self, conn, env):
        add = call_action(mod.integration_add_sync_schedule, conn, ns(
            connector_id=env["connector_id"],
            entity_type="products",
            frequency="daily",
        ))
        result = call_action(mod.integration_update_sync_schedule, conn, ns(
            schedule_id=add["id"],
            frequency="hourly",
        ))
        assert is_ok(result), result
        assert "frequency" in result["updated_fields"]

    def test_list_schedules(self, conn, env):
        call_action(mod.integration_add_sync_schedule, conn, ns(
            connector_id=env["connector_id"],
            entity_type="orders",
            frequency="hourly",
        ))
        result = call_action(mod.integration_list_sync_schedules, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_delete_schedule(self, conn, env):
        add = call_action(mod.integration_add_sync_schedule, conn, ns(
            connector_id=env["connector_id"],
            entity_type="inventory",
            frequency="weekly",
        ))
        result = call_action(mod.integration_delete_sync_schedule, conn, ns(
            schedule_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["deleted"] is True


# =============================================================================
# Sync Errors
# =============================================================================

class TestSyncErrors:
    def test_add_sync_error(self, conn, env):
        start = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        result = call_action(mod.integration_add_sync_error, conn, ns(
            sync_id=start["id"],
            error_message="Product SKU-123 failed validation",
            entity_type="product",
            entity_id="SKU-123",
        ))
        assert is_ok(result), result
        assert result["error_message"] == "Product SKU-123 failed validation"

    def test_add_error_increments_failed_count(self, conn, env):
        start = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        call_action(mod.integration_add_sync_error, conn, ns(
            sync_id=start["id"],
            error_message="Error 1",
        ))
        call_action(mod.integration_add_sync_error, conn, ns(
            sync_id=start["id"],
            error_message="Error 2",
        ))
        get_result = call_action(mod.integration_get_sync, conn, ns(
            sync_id=start["id"],
        ))
        assert is_ok(get_result), get_result
        assert get_result["records_failed"] == 2

    def test_list_sync_errors(self, conn, env):
        start = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        call_action(mod.integration_add_sync_error, conn, ns(
            sync_id=start["id"],
            error_message="Test error",
        ))
        result = call_action(mod.integration_list_sync_errors, conn, ns(
            sync_id=start["id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_resolve_sync_error(self, conn, env):
        start = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        err_result = call_action(mod.integration_add_sync_error, conn, ns(
            sync_id=start["id"],
            error_message="Fixable error",
        ))
        result = call_action(mod.integration_resolve_sync_error, conn, ns(
            error_id=err_result["id"],
            resolution_notes="Fixed by adjusting mapping",
        ))
        assert is_ok(result), result
        assert result["is_resolved"] == 1

    def test_resolve_already_resolved_fails(self, conn, env):
        start = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        err_result = call_action(mod.integration_add_sync_error, conn, ns(
            sync_id=start["id"],
            error_message="Fixable error",
        ))
        call_action(mod.integration_resolve_sync_error, conn, ns(
            error_id=err_result["id"],
        ))
        result = call_action(mod.integration_resolve_sync_error, conn, ns(
            error_id=err_result["id"],
        ))
        assert is_error(result)


# =============================================================================
# Sync Reports
# =============================================================================

class TestSyncSummaryReport:
    def test_summary_report(self, conn, env):
        call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        result = call_action(mod.integration_sync_summary_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_syncs"] >= 1
        assert "running" in result
        assert "success_rate" in result


class TestGetSyncLog:
    def test_get_sync_log(self, conn, env):
        start = call_action(mod.integration_start_sync, conn, ns(
            connector_id=env["connector_id"],
            sync_type="full",
            direction="inbound",
        ))
        call_action(mod.integration_add_sync_error, conn, ns(
            sync_id=start["id"],
            error_message="Log error test",
        ))
        result = call_action(mod.integration_get_sync_log, conn, ns(
            sync_id=start["id"],
        ))
        assert is_ok(result), result
        assert "sync" in result
        assert "errors" in result
        assert result["error_count"] >= 1


# =============================================================================
# Mappings
# =============================================================================

class TestFieldMappings:
    def test_add_field_mapping(self, conn, env):
        result = call_action(mod.integration_add_field_mapping, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            source_field="title",
            target_field="item_name",
        ))
        assert is_ok(result), result
        assert result["source_field"] == "title"
        assert result["target_field"] == "item_name"

    def test_add_mapping_missing_source_fails(self, conn, env):
        result = call_action(mod.integration_add_field_mapping, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            target_field="item_name",
        ))
        assert is_error(result)

    def test_update_field_mapping(self, conn, env):
        add = call_action(mod.integration_add_field_mapping, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            source_field="title",
            target_field="item_name",
        ))
        result = call_action(mod.integration_update_field_mapping, conn, ns(
            field_mapping_id=add["id"],
            target_field="item_description",
        ))
        assert is_ok(result), result
        assert "target_field" in result["updated_fields"]

    def test_get_field_mapping(self, conn, env):
        add = call_action(mod.integration_add_field_mapping, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            source_field="title",
            target_field="item_name",
        ))
        result = call_action(mod.integration_get_field_mapping, conn, ns(
            field_mapping_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["id"] == add["id"]

    def test_list_field_mappings(self, conn, env):
        call_action(mod.integration_add_field_mapping, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            source_field="title",
            target_field="item_name",
        ))
        result = call_action(mod.integration_list_field_mappings, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_delete_field_mapping(self, conn, env):
        add = call_action(mod.integration_add_field_mapping, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            source_field="title",
            target_field="item_name",
        ))
        result = call_action(mod.integration_delete_field_mapping, conn, ns(
            field_mapping_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["deleted"] is True


# =============================================================================
# Entity Maps
# =============================================================================

class TestEntityMaps:
    def test_add_entity_map(self, conn, env):
        result = call_action(mod.integration_add_entity_map, conn, ns(
            connector_id=env["connector_id"],
            entity_type="customer",
            local_id="cust-001",
            remote_id="shopify-cust-123",
        ))
        assert is_ok(result), result
        assert result["local_id"] == "cust-001"
        assert result["remote_id"] == "shopify-cust-123"

    def test_add_duplicate_entity_map_fails(self, conn, env):
        call_action(mod.integration_add_entity_map, conn, ns(
            connector_id=env["connector_id"],
            entity_type="customer",
            local_id="cust-001",
            remote_id="shopify-cust-123",
        ))
        result = call_action(mod.integration_add_entity_map, conn, ns(
            connector_id=env["connector_id"],
            entity_type="customer",
            local_id="cust-001",
            remote_id="shopify-cust-456",
        ))
        assert is_error(result)

    def test_get_entity_map(self, conn, env):
        add = call_action(mod.integration_add_entity_map, conn, ns(
            connector_id=env["connector_id"],
            entity_type="customer",
            local_id="cust-002",
            remote_id="shopify-cust-789",
        ))
        result = call_action(mod.integration_get_entity_map, conn, ns(
            entity_map_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["id"] == add["id"]

    def test_list_entity_maps(self, conn, env):
        call_action(mod.integration_add_entity_map, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            local_id="prod-001",
            remote_id="shopify-prod-123",
        ))
        result = call_action(mod.integration_list_entity_maps, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_delete_entity_map(self, conn, env):
        add = call_action(mod.integration_add_entity_map, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            local_id="prod-del",
            remote_id="shopify-prod-del",
        ))
        result = call_action(mod.integration_delete_entity_map, conn, ns(
            entity_map_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["deleted"] is True


# =============================================================================
# Transform Rules
# =============================================================================

class TestTransformRules:
    def test_add_transform_rule(self, conn, env):
        result = call_action(mod.integration_add_transform_rule, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            rule_name="price_conversion",
            rule_json='{"operation": "multiply", "factor": 100}',
        ))
        assert is_ok(result), result
        assert result["rule_name"] == "price_conversion"

    def test_add_transform_missing_rule_json_fails(self, conn, env):
        result = call_action(mod.integration_add_transform_rule, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            rule_name="bad_rule",
        ))
        assert is_error(result)

    def test_add_transform_invalid_json_fails(self, conn, env):
        result = call_action(mod.integration_add_transform_rule, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            rule_name="bad_rule",
            rule_json="not json{{{",
        ))
        assert is_error(result)

    def test_list_transform_rules(self, conn, env):
        call_action(mod.integration_add_transform_rule, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            rule_name="test_rule",
            rule_json='{"op": "noop"}',
        ))
        result = call_action(mod.integration_list_transform_rules, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_delete_transform_rule(self, conn, env):
        add = call_action(mod.integration_add_transform_rule, conn, ns(
            connector_id=env["connector_id"],
            entity_type="product",
            rule_name="del_rule",
            rule_json='{"op": "del"}',
        ))
        result = call_action(mod.integration_delete_transform_rule, conn, ns(
            transform_rule_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["deleted"] is True


# =============================================================================
# Status
# =============================================================================

class TestStatus:
    def test_status(self, conn, env):
        result = call_action(mod.status, conn, ns())
        assert is_ok(result), result
        assert result["skill"] == "erpclaw-integrations"
        assert result["tables"] == 17
        assert result["actions"] > 0
