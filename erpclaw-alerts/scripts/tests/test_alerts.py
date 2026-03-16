"""Tests for ERPClaw Alerts domain.

Actions tested (14 total):
  - alert-add-alert-rule
  - alert-update-alert-rule
  - alert-get-alert-rule
  - alert-list-alert-rules
  - alert-activate-alert-rule
  - alert-deactivate-alert-rule
  - alert-add-notification-channel
  - alert-list-notification-channels
  - alert-delete-notification-channel
  - alert-trigger-alert
  - alert-list-alert-logs
  - alert-acknowledge-alert
  - alert-summary-report
  - status
"""
import json
import pytest
from alerts_helpers import call_action, ns, is_error, is_ok, load_db_query

mod = load_db_query()


# ─────────────────────────────────────────────────────────────────────────────
# Alert Rules
# ─────────────────────────────────────────────────────────────────────────────

class TestAddAlertRule:
    def test_create_basic(self, conn, env):
        result = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name="Low Stock Alert",
            entity_type="item",
            condition_json='{"field":"qty","op":"<","value":10}',
            severity="high",
            description="Alert when stock is low",
            channel_ids=None,
            cooldown_minutes=30,
            is_active=None,
        ))
        assert is_ok(result), result
        assert result["name"] == "Low Stock Alert"
        assert result["entity_type"] == "item"
        assert result["severity"] == "high"
        assert result["is_active"] == 1
        assert "id" in result
        assert "naming_series" in result

    def test_create_with_defaults(self, conn, env):
        result = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name="Default Rule",
            entity_type="invoice",
            condition_json=None,
            severity=None,
            description=None,
            channel_ids=None,
            cooldown_minutes=None,
            is_active=None,
        ))
        assert is_ok(result), result
        assert result["severity"] == "medium"
        assert result["is_active"] == 1

    def test_missing_name_fails(self, conn, env):
        result = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name=None,
            entity_type="item",
            condition_json=None,
            severity=None,
            description=None,
            channel_ids=None,
            cooldown_minutes=None,
            is_active=None,
        ))
        assert is_error(result), result

    def test_missing_entity_type_fails(self, conn, env):
        result = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name="Test Rule",
            entity_type=None,
            condition_json=None,
            severity=None,
            description=None,
            channel_ids=None,
            cooldown_minutes=None,
            is_active=None,
        ))
        assert is_error(result), result

    def test_invalid_severity_fails(self, conn, env):
        result = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name="Bad Severity",
            entity_type="item",
            condition_json=None,
            severity="extreme",
            description=None,
            channel_ids=None,
            cooldown_minutes=None,
            is_active=None,
        ))
        assert is_error(result), result

    def test_missing_company_fails(self, conn, env):
        result = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=None,
            name="No Company",
            entity_type="item",
            condition_json=None,
            severity=None,
            description=None,
            channel_ids=None,
            cooldown_minutes=None,
            is_active=None,
        ))
        assert is_error(result), result


class TestUpdateAlertRule:
    def _create_rule(self, conn, env):
        result = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name="Update Me",
            entity_type="invoice",
            condition_json='{}',
            severity="low",
            description="Initial",
            channel_ids=None,
            cooldown_minutes=60,
            is_active=None,
        ))
        assert is_ok(result), result
        return result["id"]

    def test_update_name(self, conn, env):
        rule_id = self._create_rule(conn, env)
        result = call_action(mod.alert_update_alert_rule, conn, ns(
            rule_id=rule_id,
            name="Updated Name",
            description=None,
            entity_type=None,
            condition_json=None,
            severity=None,
            channel_ids=None,
            cooldown_minutes=None,
        ))
        assert is_ok(result), result
        assert "name" in result["updated_fields"]

    def test_update_no_fields_fails(self, conn, env):
        rule_id = self._create_rule(conn, env)
        result = call_action(mod.alert_update_alert_rule, conn, ns(
            rule_id=rule_id,
            name=None,
            description=None,
            entity_type=None,
            condition_json=None,
            severity=None,
            channel_ids=None,
            cooldown_minutes=None,
        ))
        assert is_error(result), result


class TestGetAlertRule:
    def test_get_existing(self, conn, env):
        create = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name="Fetchable",
            entity_type="contract",
            condition_json='{"days_left":"<7"}',
            severity="critical",
            description="Expiring contracts",
            channel_ids=None,
            cooldown_minutes=120,
            is_active=None,
        ))
        assert is_ok(create), create

        result = call_action(mod.alert_get_alert_rule, conn, ns(
            rule_id=create["id"],
        ))
        assert is_ok(result), result
        assert result["name"] == "Fetchable"
        assert result["severity"] == "critical"
        assert result["log_count"] == 0

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.alert_get_alert_rule, conn, ns(
            rule_id="nonexistent-id",
        ))
        assert is_error(result), result


class TestListAlertRules:
    def test_list_empty(self, conn, env):
        result = call_action(mod.alert_list_alert_rules, conn, ns(
            company_id=env["company_id"],
            entity_type=None,
            severity=None,
            is_active=None,
            search=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(result), result
        assert result["total_count"] == 0
        assert result["rules"] == []

    def test_list_filters_by_company(self, conn, env):
        call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name="Company Rule",
            entity_type="item",
            condition_json=None,
            severity=None,
            description=None,
            channel_ids=None,
            cooldown_minutes=None,
            is_active=None,
        ))
        result = call_action(mod.alert_list_alert_rules, conn, ns(
            company_id=env["company_id"],
            entity_type=None,
            severity=None,
            is_active=None,
            search=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(result), result
        assert result["total_count"] == 1


class TestActivateDeactivateRule:
    def _create_rule(self, conn, env):
        result = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name="Toggle Rule",
            entity_type="item",
            condition_json=None,
            severity=None,
            description=None,
            channel_ids=None,
            cooldown_minutes=None,
            is_active=None,
        ))
        assert is_ok(result), result
        return result["id"]

    def test_deactivate(self, conn, env):
        rule_id = self._create_rule(conn, env)
        result = call_action(mod.alert_deactivate_alert_rule, conn, ns(
            rule_id=rule_id,
        ))
        assert is_ok(result), result
        assert result["is_active"] == 0

    def test_activate(self, conn, env):
        rule_id = self._create_rule(conn, env)
        # Deactivate first
        call_action(mod.alert_deactivate_alert_rule, conn, ns(rule_id=rule_id))
        # Then activate
        result = call_action(mod.alert_activate_alert_rule, conn, ns(
            rule_id=rule_id,
        ))
        assert is_ok(result), result
        assert result["is_active"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# Notification Channels
# ─────────────────────────────────────────────────────────────────────────────

class TestAddNotificationChannel:
    def test_create_email_channel(self, conn, env):
        result = call_action(mod.alert_add_notification_channel, conn, ns(
            company_id=env["company_id"],
            name="Email Channel",
            channel_type="email",
            config_json='{"to":"admin@example.com"}',
            is_active=None,
        ))
        assert is_ok(result), result
        assert result["name"] == "Email Channel"
        assert result["channel_type"] == "email"
        assert result["is_active"] == 1

    def test_create_webhook_channel(self, conn, env):
        result = call_action(mod.alert_add_notification_channel, conn, ns(
            company_id=env["company_id"],
            name="Webhook Channel",
            channel_type="webhook",
            config_json='{"url":"https://hook.example.com/alert"}',
            is_active=None,
        ))
        assert is_ok(result), result
        assert result["channel_type"] == "webhook"

    def test_invalid_channel_type_fails(self, conn, env):
        result = call_action(mod.alert_add_notification_channel, conn, ns(
            company_id=env["company_id"],
            name="Bad Type",
            channel_type="carrier_pigeon",
            config_json=None,
            is_active=None,
        ))
        assert is_error(result), result

    def test_missing_name_fails(self, conn, env):
        result = call_action(mod.alert_add_notification_channel, conn, ns(
            company_id=env["company_id"],
            name=None,
            channel_type="email",
            config_json=None,
            is_active=None,
        ))
        assert is_error(result), result


class TestListNotificationChannels:
    def test_list_channels(self, conn, env):
        call_action(mod.alert_add_notification_channel, conn, ns(
            company_id=env["company_id"],
            name="Ch1",
            channel_type="email",
            config_json=None,
            is_active=None,
        ))
        result = call_action(mod.alert_list_notification_channels, conn, ns(
            company_id=env["company_id"],
            channel_type=None,
            is_active=None,
            search=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(result), result
        assert result["total_count"] == 1


class TestDeleteNotificationChannel:
    def test_delete_channel(self, conn, env):
        ch = call_action(mod.alert_add_notification_channel, conn, ns(
            company_id=env["company_id"],
            name="Deletable",
            channel_type="sms",
            config_json=None,
            is_active=None,
        ))
        assert is_ok(ch), ch
        result = call_action(mod.alert_delete_notification_channel, conn, ns(
            channel_id=ch["id"],
        ))
        assert is_ok(result), result
        assert result["deleted"] is True

    def test_delete_nonexistent_fails(self, conn, env):
        result = call_action(mod.alert_delete_notification_channel, conn, ns(
            channel_id="nonexistent",
        ))
        assert is_error(result), result


# ─────────────────────────────────────────────────────────────────────────────
# Alert Triggering and Logs
# ─────────────────────────────────────────────────────────────────────────────

class TestTriggerAlert:
    def _create_rule(self, conn, env):
        result = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name="Low Stock Rule",
            entity_type="item",
            condition_json='{"field":"qty","op":"<","value":5}',
            severity="high",
            description=None,
            channel_ids=None,
            cooldown_minutes=None,
            is_active=None,
        ))
        assert is_ok(result), result
        return result["id"]

    def test_trigger_basic(self, conn, env):
        rule_id = self._create_rule(conn, env)
        result = call_action(mod.alert_trigger_alert, conn, ns(
            rule_id=rule_id,
            message="Item WIDGET-001 is below threshold (qty=3)",
            entity_id="item-001",
            channel_results=None,
        ))
        assert is_ok(result), result
        assert result["rule_id"] == rule_id
        assert result["severity"] == "high"
        assert result["alert_status"] == "triggered"
        assert result["entity_id"] == "item-001"

    def test_trigger_missing_message_fails(self, conn, env):
        rule_id = self._create_rule(conn, env)
        result = call_action(mod.alert_trigger_alert, conn, ns(
            rule_id=rule_id,
            message=None,
            entity_id=None,
            channel_results=None,
        ))
        assert is_error(result), result


class TestListAlertLogs:
    def test_list_logs_after_trigger(self, conn, env):
        # Create rule and trigger it
        rule = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name="Trigger Test",
            entity_type="invoice",
            condition_json=None,
            severity="medium",
            description=None,
            channel_ids=None,
            cooldown_minutes=None,
            is_active=None,
        ))
        assert is_ok(rule), rule

        call_action(mod.alert_trigger_alert, conn, ns(
            rule_id=rule["id"],
            message="Invoice overdue",
            entity_id="inv-123",
            channel_results=None,
        ))

        result = call_action(mod.alert_list_alert_logs, conn, ns(
            company_id=env["company_id"],
            rule_id=None,
            severity=None,
            alert_status=None,
            entity_type=None,
            search=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(result), result
        assert result["total_count"] == 1
        assert result["logs"][0]["message"] == "Invoice overdue"


class TestAcknowledgeAlert:
    def _trigger_and_get_log_id(self, conn, env):
        rule = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name="Ack Test Rule",
            entity_type="item",
            condition_json=None,
            severity="critical",
            description=None,
            channel_ids=None,
            cooldown_minutes=None,
            is_active=None,
        ))
        assert is_ok(rule), rule

        triggered = call_action(mod.alert_trigger_alert, conn, ns(
            rule_id=rule["id"],
            message="Critical issue detected",
            entity_id=None,
            channel_results=None,
        ))
        assert is_ok(triggered), triggered
        return triggered["id"]

    def test_acknowledge(self, conn, env):
        log_id = self._trigger_and_get_log_id(conn, env)
        result = call_action(mod.alert_acknowledge_alert, conn, ns(
            alert_log_id=log_id,
            acknowledged_by="admin@example.com",
        ))
        assert is_ok(result), result
        assert result["alert_status"] == "acknowledged"
        assert result["acknowledged_by"] == "admin@example.com"

    def test_acknowledge_already_acknowledged_fails(self, conn, env):
        log_id = self._trigger_and_get_log_id(conn, env)
        # First acknowledge
        call_action(mod.alert_acknowledge_alert, conn, ns(
            alert_log_id=log_id,
            acknowledged_by="admin@example.com",
        ))
        # Try again
        result = call_action(mod.alert_acknowledge_alert, conn, ns(
            alert_log_id=log_id,
            acknowledged_by="other@example.com",
        ))
        assert is_error(result), result

    def test_acknowledge_missing_by_fails(self, conn, env):
        log_id = self._trigger_and_get_log_id(conn, env)
        result = call_action(mod.alert_acknowledge_alert, conn, ns(
            alert_log_id=log_id,
            acknowledged_by=None,
        ))
        assert is_error(result), result


# ─────────────────────────────────────────────────────────────────────────────
# Summary Report
# ─────────────────────────────────────────────────────────────────────────────

class TestAlertSummaryReport:
    def test_empty_report(self, conn, env):
        result = call_action(mod.alert_summary_report, conn, ns(
            company_id=env["company_id"],
            start_date=None,
            end_date=None,
        ))
        assert is_ok(result), result
        assert result["total_alerts"] == 0
        assert result["by_severity"] == {}
        assert result["by_status"] == {}

    def test_report_with_data(self, conn, env):
        rule = call_action(mod.alert_add_alert_rule, conn, ns(
            company_id=env["company_id"],
            name="Report Rule",
            entity_type="item",
            condition_json=None,
            severity="high",
            description=None,
            channel_ids=None,
            cooldown_minutes=None,
            is_active=None,
        ))
        assert is_ok(rule), rule

        call_action(mod.alert_trigger_alert, conn, ns(
            rule_id=rule["id"],
            message="First alert",
            entity_id=None,
            channel_results=None,
        ))
        call_action(mod.alert_trigger_alert, conn, ns(
            rule_id=rule["id"],
            message="Second alert",
            entity_id=None,
            channel_results=None,
        ))

        result = call_action(mod.alert_summary_report, conn, ns(
            company_id=env["company_id"],
            start_date=None,
            end_date=None,
        ))
        assert is_ok(result), result
        assert result["total_alerts"] == 2
        assert result["by_severity"]["high"] == 2
        assert result["by_status"]["triggered"] == 2


# ─────────────────────────────────────────────────────────────────────────────
# Status
# ─────────────────────────────────────────────────────────────────────────────

class TestStatus:
    def test_status(self, conn, env):
        result = call_action(mod.status, conn, ns())
        assert is_ok(result), result
        assert result["skill"] == "erpclaw-alerts"
        assert result["total_tables"] == 3
