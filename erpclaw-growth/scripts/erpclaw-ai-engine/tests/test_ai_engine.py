"""L1 pytest tests for erpclaw-ai-engine (22 actions).

Covers: anomaly detection/management, scenarios, business rules,
categorization, relationship scoring, conversation context,
pending decisions, audit conversations, status.
"""
import json
import os
import pytest
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from ai_helpers import call_action, ns, is_ok, is_error, load_db_query, _uuid

MOD = load_db_query()


# ===========================================================================
# Anomaly Detection & Management
# ===========================================================================

class TestDetectAnomalies:
    def test_detect_anomalies_no_data(self, conn, env):
        """With minimal GL data, detect-anomalies should run without error."""
        r = call_action(MOD.detect_anomalies, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        assert is_ok(r)
        assert "anomalies_detected" in r
        assert isinstance(r["anomaly_ids"], list)

    def test_detect_anomalies_missing_company(self, conn, env):
        r = call_action(MOD.detect_anomalies, conn, ns(
            company_id=None,
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        assert is_error(r)


class TestListAnomalies:
    def test_list_anomalies_empty(self, conn, env):
        r = call_action(MOD.list_anomalies, conn, ns(
            company_id=env["company_id"],
            severity=None, status=None,
            limit="20", offset="0",
        ))
        assert is_ok(r)
        assert "anomalies" in r
        assert r["total_count"] >= 0

    def test_list_anomalies_with_filter(self, conn, env):
        # Run detection first to create anomalies
        call_action(MOD.detect_anomalies, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))

        r = call_action(MOD.list_anomalies, conn, ns(
            company_id=env["company_id"],
            severity="info", status=None,
            limit="20", offset="0",
        ))
        assert is_ok(r)


class TestAcknowledgeAnomaly:
    def test_acknowledge_anomaly(self, conn, env):
        # Create an anomaly manually
        anomaly_id = _uuid()
        conn.execute(
            """INSERT INTO anomaly (id, anomaly_type, severity, entity_type,
               entity_id, description, status)
               VALUES (?, 'price_spike', 'warning', 'gl_entry', ?, 'Test anomaly', 'new')""",
            (anomaly_id, _uuid())
        )
        conn.commit()

        r = call_action(MOD.acknowledge_anomaly, conn, ns(anomaly_id=anomaly_id))
        assert is_ok(r)
        assert r["anomaly"]["status"] == "acknowledged"

    def test_acknowledge_missing_id(self, conn, env):
        r = call_action(MOD.acknowledge_anomaly, conn, ns(anomaly_id=None))
        assert is_error(r)

    def test_acknowledge_not_new(self, conn, env):
        anomaly_id = _uuid()
        conn.execute(
            """INSERT INTO anomaly (id, anomaly_type, severity, entity_type,
               entity_id, description, status)
               VALUES (?, 'price_spike', 'warning', 'gl_entry', ?, 'Test', 'acknowledged')""",
            (anomaly_id, _uuid())
        )
        conn.commit()

        r = call_action(MOD.acknowledge_anomaly, conn, ns(anomaly_id=anomaly_id))
        assert is_error(r)


class TestDismissAnomaly:
    def test_dismiss_anomaly(self, conn, env):
        anomaly_id = _uuid()
        conn.execute(
            """INSERT INTO anomaly (id, anomaly_type, severity, entity_type,
               entity_id, description, status)
               VALUES (?, 'round_number', 'info', 'gl_entry', ?, 'Test', 'new')""",
            (anomaly_id, _uuid())
        )
        conn.commit()

        r = call_action(MOD.dismiss_anomaly, conn, ns(
            anomaly_id=anomaly_id, reason="False positive",
        ))
        assert is_ok(r)
        assert r["anomaly"]["status"] == "dismissed"


# ===========================================================================
# Scenarios
# ===========================================================================

class TestCreateScenario:
    def test_create_scenario(self, conn, env):
        r = call_action(MOD.create_scenario, conn, ns(
            name="What if we raise prices 10%?",
            company_id=env["company_id"],
            scenario_type="price_change",
            assumptions='{"price_change_pct": 10}',
        ))
        assert is_ok(r)
        assert r["scenario"]["question"] == "What if we raise prices 10%?"
        assert r["scenario"]["scenario_type"] == "price_change"

    def test_create_scenario_missing_name(self, conn, env):
        r = call_action(MOD.create_scenario, conn, ns(
            name=None, company_id=env["company_id"],
            scenario_type="price_change", assumptions=None,
        ))
        assert is_error(r)


class TestListScenarios:
    def test_list_scenarios(self, conn, env):
        call_action(MOD.create_scenario, conn, ns(
            name="Test scenario", company_id=env["company_id"],
            scenario_type="price_change", assumptions=None,
        ))
        r = call_action(MOD.list_scenarios, conn, ns(
            company_id=env["company_id"],
            limit="20", offset="0",
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


# ===========================================================================
# Business Rules
# ===========================================================================

class TestAddBusinessRule:
    def test_add_rule(self, conn, env):
        r = call_action(MOD.add_business_rule, conn, ns(
            rule_text="Block purchases over $50,000 without approval",
            severity="block", name="High-value purchase guard",
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["business_rule"]["rule_text"] == "Block purchases over $50,000 without approval"
        assert r["business_rule"]["action"] == "block"

    def test_add_rule_missing_text(self, conn, env):
        r = call_action(MOD.add_business_rule, conn, ns(
            rule_text=None, severity="warn", name=None,
            company_id=env["company_id"],
        ))
        assert is_error(r)


class TestListBusinessRules:
    def test_list_rules(self, conn, env):
        call_action(MOD.add_business_rule, conn, ns(
            rule_text="Test rule", severity="warn", name=None,
            company_id=env["company_id"],
        ))
        r = call_action(MOD.list_business_rules, conn, ns(
            company_id=env["company_id"], is_active="1",
            limit="20", offset="0",
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestEvaluateBusinessRules:
    def test_evaluate_with_match(self, conn, env):
        call_action(MOD.add_business_rule, conn, ns(
            rule_text="Block all", severity="block", name=None,
            company_id=env["company_id"],
        ))
        r = call_action(MOD.evaluate_business_rules, conn, ns(
            action_type="purchase", action_data='{"amount": "60000"}',
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["triggered"] is True
        assert r["recommended_action"] == "block"

    def test_evaluate_no_match(self, conn, env):
        # No rules in empty DB
        r = call_action(MOD.evaluate_business_rules, conn, ns(
            action_type="purchase", action_data='{"amount": "100"}',
            company_id=None,
        ))
        assert is_ok(r)
        assert r["triggered"] is False


# ===========================================================================
# Categorization Rules
# ===========================================================================

class TestAddCategorizationRule:
    def test_add_categorization_rule(self, conn, env):
        acct_id = env["accounts"]["expense"]
        r = call_action(MOD.add_categorization_rule, conn, ns(
            pattern="AMAZON", account_id=acct_id,
            source="bank_feed", cost_center_id=None,
        ))
        assert is_ok(r)
        assert r["categorization_rule"]["pattern"] == "AMAZON"

    def test_add_categorization_rule_missing_pattern(self, conn, env):
        r = call_action(MOD.add_categorization_rule, conn, ns(
            pattern=None, account_id=None, source=None,
            cost_center_id=None,
        ))
        assert is_error(r)


class TestCategorizeTransaction:
    def test_categorize_match(self, conn, env):
        acct_id = env["accounts"]["expense"]
        call_action(MOD.add_categorization_rule, conn, ns(
            pattern="OFFICE DEPOT", account_id=acct_id,
            source="bank_feed", cost_center_id=None,
        ))

        r = call_action(MOD.categorize_transaction, conn, ns(
            description="OFFICE DEPOT PURCHASE #12345",
        ))
        assert is_ok(r)
        assert r["match"] is True
        assert r["account_id"] == acct_id

    def test_categorize_no_match(self, conn, env):
        r = call_action(MOD.categorize_transaction, conn, ns(
            description="UNKNOWN VENDOR XYZ",
        ))
        assert is_ok(r)
        assert r["match"] is False


# ===========================================================================
# Conversation Context
# ===========================================================================

class TestSaveConversationContext:
    def test_save_context(self, conn, env):
        context_data = json.dumps({
            "context_type": "active_workflow",
            "summary": "Working on Q1 close",
            "user_id": "test-user",
            "related_entities": {"company_id": env["company_id"]},
            "state": {"step": 1},
            "priority": 1,
        })
        r = call_action(MOD.save_conversation_context, conn, ns(
            context_data=context_data,
        ))
        assert is_ok(r)
        assert r["context"]["context_type"] == "active_workflow"

    def test_save_context_missing_data(self, conn, env):
        r = call_action(MOD.save_conversation_context, conn, ns(
            context_data=None,
        ))
        assert is_error(r)


class TestGetConversationContext:
    def test_get_context_by_id(self, conn, env):
        context_data = json.dumps({
            "context_type": "pending_decision",
            "summary": "Test context",
        })
        save_r = call_action(MOD.save_conversation_context, conn, ns(
            context_data=context_data,
        ))
        ctx_id = save_r["context"]["id"]

        r = call_action(MOD.get_conversation_context, conn, ns(
            context_id=ctx_id,
        ))
        assert is_ok(r)
        assert r["context"]["id"] == ctx_id

    def test_get_latest_context(self, conn, env):
        context_data = json.dumps({
            "context_type": "active_workflow",
            "summary": "Latest context",
        })
        call_action(MOD.save_conversation_context, conn, ns(
            context_data=context_data,
        ))

        r = call_action(MOD.get_conversation_context, conn, ns(
            context_id=None,
        ))
        assert is_ok(r)
        assert r["context"] is not None


# ===========================================================================
# Pending Decisions
# ===========================================================================

class TestAddPendingDecision:
    def test_add_decision(self, conn, env):
        r = call_action(MOD.add_pending_decision, conn, ns(
            description="Approve budget increase?",
            options='["Approve", "Deny", "Defer"]',
            to_date="2026-04-01",
            decision_type="budget",
            context_id=None,
        ))
        assert is_ok(r)
        assert r["pending_decision"]["question"] == "Approve budget increase?"
        assert r["pending_decision"]["status"] == "pending"


# ===========================================================================
# Audit Conversation
# ===========================================================================

class TestLogAuditConversation:
    def test_log_audit(self, conn, env):
        r = call_action(MOD.log_audit_conversation, conn, ns(
            action_name="detect-anomalies",
            result="Found 3 anomalies",
            details='{"anomalies_detected": 3}',
        ))
        assert is_ok(r)
        assert r["audit_entry"]["voucher_type"] == "detect-anomalies"


# ===========================================================================
# Discover Correlations
# ===========================================================================

class TestDiscoverCorrelations:
    def test_discover_correlations(self, conn, env):
        r = call_action(MOD.discover_correlations, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
            min_strength=None,
        ))
        assert is_ok(r)
        assert "correlations_discovered" in r
        assert "correlation_ids" in r


class TestListCorrelations:
    def test_list_correlations(self, conn, env):
        r = call_action(MOD.list_correlations, conn, ns(
            company_id=None, min_strength=None,
            limit="20", offset="0",
        ))
        assert is_ok(r)
        assert "correlations" in r


# ===========================================================================
# Cash Flow Forecasting
# ===========================================================================

class TestForecastCashFlow:
    def test_forecast_cash_flow(self, conn, env):
        r = call_action(MOD.forecast_cash_flow, conn, ns(
            company_id=env["company_id"],
            horizon_days="30",
            from_date=None, to_date=None,
        ))
        assert is_ok(r)
        assert "starting_balance" in r or "scenarios" in r or "forecast_ids" in r


class TestGetForecast:
    def test_get_forecast_latest(self, conn, env):
        # First generate a forecast
        call_action(MOD.forecast_cash_flow, conn, ns(
            company_id=env["company_id"],
            horizon_days="30",
            from_date=None, to_date=None,
        ))
        r = call_action(MOD.get_forecast, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)


# ===========================================================================
# Status
# ===========================================================================

class TestStatusAction:
    def test_status(self, conn, env):
        r = call_action(MOD.status, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "ai_engine" in r or "status" in r or "tables" in r
