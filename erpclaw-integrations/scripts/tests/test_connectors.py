"""L1 tests for ERPClaw Integrations -- connectors domain (14 actions).

Actions tested:
  Connectors:     integration-add-connector, integration-update-connector,
                  integration-get-connector, integration-list-connectors,
                  integration-activate-connector, integration-deactivate-connector,
                  integration-test-connector
  Credentials:    integration-add-connector-credential, integration-list-connector-credentials,
                  integration-delete-connector-credential
  Webhooks:       integration-add-webhook, integration-list-webhooks, integration-delete-webhook
  Reports:        integration-connector-health-report
"""
import pytest
from integration_helpers import call_action, ns, is_error, is_ok, load_db_query, seed_connector

mod = load_db_query()


# =============================================================================
# Connectors
# =============================================================================

class TestAddConnector:
    def test_basic_create(self, conn, env):
        result = call_action(mod.integration_add_connector, conn, ns(
            company_id=env["company_id"],
            name="My Shopify Store",
            platform="shopify",
        ))
        assert is_ok(result), result
        assert result["name"] == "My Shopify Store"
        assert result["platform"] == "shopify"
        assert result["connector_status"] == "inactive"
        assert "id" in result
        assert "naming_series" in result

    def test_create_with_all_fields(self, conn, env):
        result = call_action(mod.integration_add_connector, conn, ns(
            company_id=env["company_id"],
            name="WooCommerce Site",
            platform="woocommerce",
            connector_type="inbound",
            base_url="https://mystore.com",
            config_json='{"api_version": "v3"}',
        ))
        assert is_ok(result), result
        assert result["platform"] == "woocommerce"

    def test_missing_name_fails(self, conn, env):
        result = call_action(mod.integration_add_connector, conn, ns(
            company_id=env["company_id"],
            platform="shopify",
        ))
        assert is_error(result)

    def test_missing_platform_fails(self, conn, env):
        result = call_action(mod.integration_add_connector, conn, ns(
            company_id=env["company_id"],
            name="Test",
        ))
        assert is_error(result)

    def test_invalid_platform_fails(self, conn, env):
        result = call_action(mod.integration_add_connector, conn, ns(
            company_id=env["company_id"],
            name="Test",
            platform="invalid_platform",
        ))
        assert is_error(result)

    def test_missing_company_fails(self, conn, env):
        result = call_action(mod.integration_add_connector, conn, ns(
            name="Test",
            platform="shopify",
        ))
        assert is_error(result)

    def test_invalid_config_json_fails(self, conn, env):
        result = call_action(mod.integration_add_connector, conn, ns(
            company_id=env["company_id"],
            name="Test",
            platform="shopify",
            config_json="not valid json{{{",
        ))
        assert is_error(result)


class TestUpdateConnector:
    def test_update_name(self, conn, env):
        result = call_action(mod.integration_update_connector, conn, ns(
            connector_id=env["connector_id"],
            name="Updated Name",
        ))
        assert is_ok(result), result
        assert "name" in result["updated_fields"]

    def test_update_platform(self, conn, env):
        result = call_action(mod.integration_update_connector, conn, ns(
            connector_id=env["connector_id"],
            platform="woocommerce",
        ))
        assert is_ok(result), result
        assert "platform" in result["updated_fields"]

    def test_no_fields_fails(self, conn, env):
        result = call_action(mod.integration_update_connector, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_error(result)

    def test_missing_connector_fails(self, conn, env):
        result = call_action(mod.integration_update_connector, conn, ns(
            connector_id="nonexistent-id",
            name="Updated",
        ))
        assert is_error(result)


class TestGetConnector:
    def test_get_existing(self, conn, env):
        result = call_action(mod.integration_get_connector, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_ok(result), result
        assert result["id"] == env["connector_id"]
        assert "credential_count" in result
        assert "webhook_count" in result
        assert "mapping_count" in result

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.integration_get_connector, conn, ns(
            connector_id="nonexistent-id",
        ))
        assert is_error(result)


class TestListConnectors:
    def test_list_all(self, conn, env):
        result = call_action(mod.integration_list_connectors, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1
        assert len(result["connectors"]) >= 1

    def test_list_by_platform(self, conn, env):
        result = call_action(mod.integration_list_connectors, conn, ns(
            company_id=env["company_id"],
            platform="shopify",
        ))
        assert is_ok(result), result
        for c in result["connectors"]:
            assert c["platform"] == "shopify"

    def test_list_empty_platform(self, conn, env):
        result = call_action(mod.integration_list_connectors, conn, ns(
            company_id=env["company_id"],
            platform="xero",
        ))
        assert is_ok(result), result
        assert result["total_count"] == 0


class TestActivateDeactivateConnector:
    def test_activate(self, conn, env):
        result = call_action(mod.integration_activate_connector, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_ok(result), result
        assert result["connector_status"] == "active"

    def test_activate_already_active_fails(self, conn, env):
        call_action(mod.integration_activate_connector, conn, ns(
            connector_id=env["connector_id"],
        ))
        result = call_action(mod.integration_activate_connector, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_error(result)

    def test_deactivate(self, conn, env):
        # First activate
        call_action(mod.integration_activate_connector, conn, ns(
            connector_id=env["connector_id"],
        ))
        # Then deactivate
        result = call_action(mod.integration_deactivate_connector, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_ok(result), result
        assert result["connector_status"] == "inactive"

    def test_deactivate_already_inactive_fails(self, conn, env):
        result = call_action(mod.integration_deactivate_connector, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_error(result)


class TestTestConnector:
    def test_check_no_credentials(self, conn, env):
        result = call_action(mod.integration_test_connector, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_ok(result), result
        assert result["test_passed"] is False
        assert "No credentials configured" in result["issues"]

    def test_check_with_credentials(self, conn, env):
        # Add a credential first
        call_action(mod.integration_add_connector_credential, conn, ns(
            connector_id=env["connector_id"],
            credential_type="api_key",
            credential_key="X-Api-Key",
            credential_value="test-key-123",
        ))
        result = call_action(mod.integration_test_connector, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_ok(result), result
        assert result["test_passed"] is True
        assert result["credential_count"] == 1


# =============================================================================
# Credentials
# =============================================================================

class TestConnectorCredentials:
    def test_add_credential(self, conn, env):
        result = call_action(mod.integration_add_connector_credential, conn, ns(
            connector_id=env["connector_id"],
            credential_type="api_key",
            credential_key="Authorization",
            credential_value="Bearer token123",
        ))
        assert is_ok(result), result
        assert result["credential_type"] == "api_key"
        assert result["credential_key"] == "Authorization"

    def test_add_credential_missing_type_fails(self, conn, env):
        result = call_action(mod.integration_add_connector_credential, conn, ns(
            connector_id=env["connector_id"],
            credential_key="Authorization",
            credential_value="Bearer token123",
        ))
        assert is_error(result)

    def test_add_credential_invalid_type_fails(self, conn, env):
        result = call_action(mod.integration_add_connector_credential, conn, ns(
            connector_id=env["connector_id"],
            credential_type="invalid_type",
            credential_key="Authorization",
            credential_value="Bearer token123",
        ))
        assert is_error(result)

    def test_list_credentials(self, conn, env):
        call_action(mod.integration_add_connector_credential, conn, ns(
            connector_id=env["connector_id"],
            credential_type="api_key",
            credential_key="Key1",
            credential_value="Val1",
        ))
        result = call_action(mod.integration_list_connector_credentials, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_delete_credential(self, conn, env):
        add_result = call_action(mod.integration_add_connector_credential, conn, ns(
            connector_id=env["connector_id"],
            credential_type="api_key",
            credential_key="Key1",
            credential_value="Val1",
        ))
        cred_id = add_result["id"]
        result = call_action(mod.integration_delete_connector_credential, conn, ns(
            credential_id=cred_id,
        ))
        assert is_ok(result), result
        assert result["deleted"] is True


# =============================================================================
# Webhooks
# =============================================================================

class TestWebhooks:
    def test_add_webhook(self, conn, env):
        result = call_action(mod.integration_add_webhook, conn, ns(
            connector_id=env["connector_id"],
            event_type="order.created",
            webhook_url="https://example.com/webhooks/orders",
        ))
        assert is_ok(result), result
        assert result["event_type"] == "order.created"
        assert result["webhook_url"] == "https://example.com/webhooks/orders"

    def test_add_webhook_missing_event_type_fails(self, conn, env):
        result = call_action(mod.integration_add_webhook, conn, ns(
            connector_id=env["connector_id"],
            webhook_url="https://example.com/webhooks",
        ))
        assert is_error(result)

    def test_add_webhook_missing_url_fails(self, conn, env):
        result = call_action(mod.integration_add_webhook, conn, ns(
            connector_id=env["connector_id"],
            event_type="order.created",
        ))
        assert is_error(result)

    def test_list_webhooks(self, conn, env):
        call_action(mod.integration_add_webhook, conn, ns(
            connector_id=env["connector_id"],
            event_type="order.created",
            webhook_url="https://example.com/wh1",
        ))
        result = call_action(mod.integration_list_webhooks, conn, ns(
            connector_id=env["connector_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_delete_webhook(self, conn, env):
        add_result = call_action(mod.integration_add_webhook, conn, ns(
            connector_id=env["connector_id"],
            event_type="order.updated",
            webhook_url="https://example.com/wh-del",
        ))
        wh_id = add_result["id"]
        result = call_action(mod.integration_delete_webhook, conn, ns(
            webhook_id=wh_id,
        ))
        assert is_ok(result), result
        assert result["deleted"] is True


# =============================================================================
# Health report
# =============================================================================

class TestConnectorHealthReport:
    def test_report(self, conn, env):
        result = call_action(mod.integration_connector_health_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1
        assert "connectors" in result
        c = result["connectors"][0]
        assert "connector_id" in c
        assert "name" in c
        assert "platform" in c
        assert "has_credentials" in c
