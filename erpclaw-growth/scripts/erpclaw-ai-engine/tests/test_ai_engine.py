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

from ai_helpers import (
    call_action, ns, is_ok, is_error, load_db_query, _uuid,
    seed_asset_category, seed_asset, seed_gl_with_dimensions,
    seed_item, seed_warehouse, seed_sle, seed_reservation,
    seed_subcontracting_order,
    seed_customer, seed_rate_plan, seed_meter, seed_meter_reading,
    seed_prepaid_balance, seed_billing_period,
)

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


# ===========================================================================
# AI1 — asset_book_value_drift + dimension_tag_drift  (Wave 1)
# ===========================================================================

class TestAI1AssetBookValueDrift:
    def _detect(self, conn, company_id):
        return call_action(MOD.detect_anomalies, conn, ns(
            company_id=company_id,
            from_date="2026-01-01", to_date="2026-03-31",
        ))

    def test_book_value_spike_raises(self, conn, env):
        """An asset whose book value far exceeds gross - accumulated raises
        asset_book_value_drift."""
        cat = seed_asset_category(conn, env["company_id"])
        # gross 10000, accumulated 4000 => expected book 6000; actual 25000 spike
        seed_asset(conn, env["company_id"], cat,
                   gross_value="10000", accumulated_depreciation="4000",
                   current_book_value="25000")
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("asset_book_value_drift", 0) >= 1

        rows = call_action(MOD.list_anomalies, conn, ns(
            company_id=env["company_id"], severity=None,
            status=None, limit="50", offset="0",
        ))
        types = {a["anomaly_type"] for a in rows["anomalies"]}
        assert "asset_book_value_drift" in types

    def test_clean_asset_no_drift(self, conn, env):
        """NEGATIVE CONTROL: book value == gross - accumulated must NOT fire."""
        cat = seed_asset_category(conn, env["company_id"])
        seed_asset(conn, env["company_id"], cat,
                   gross_value="10000", accumulated_depreciation="4000",
                   current_book_value="6000")
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("asset_book_value_drift", 0) == 0


class TestAI1DimensionTagDrift:
    def _detect(self, conn, company_id):
        return call_action(MOD.detect_anomalies, conn, ns(
            company_id=company_id,
            from_date="2026-01-01", to_date="2026-03-31",
        ))

    def test_inconsistent_tagging_raises(self, conn, env):
        """An account_type batch where some entries carry a dimension key and
        others omit it raises dimension_tag_drift."""
        acct = env["accounts"]["expense"]
        seed_gl_with_dimensions(conn, env["company_id"], acct, [
            {"department": _uuid()},
            {"department": _uuid()},
            {"department": _uuid()},
            {},   # untagged — the drift
            {},
        ])
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("dimension_tag_drift", 0) >= 1

        rows = call_action(MOD.list_anomalies, conn, ns(
            company_id=env["company_id"], severity=None,
            status=None, limit="50", offset="0",
        ))
        types = {a["anomaly_type"] for a in rows["anomalies"]}
        assert "dimension_tag_drift" in types

    def test_consistent_tagging_no_drift(self, conn, env):
        """NEGATIVE CONTROL: a batch all consistently tagged must NOT fire."""
        acct = env["accounts"]["expense"]
        dept = _uuid()
        seed_gl_with_dimensions(conn, env["company_id"], acct, [
            {"department": dept},
            {"department": dept},
            {"department": dept},
            {"department": dept},
        ])
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("dimension_tag_drift", 0) == 0

    def test_all_untagged_no_drift(self, conn, env):
        """NEGATIVE CONTROL: a batch with no dimensions at all must NOT fire
        (the default seed_gl_entries data is untagged)."""
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("dimension_tag_drift", 0) == 0


# ===========================================================================
# AI1 — reservation_over_available + subcontract_receipt_mismatch  (Wave 2)
# ===========================================================================

def _find_anomaly(conn, company_id, anomaly_type):
    """Return the first list-anomalies row of a given type, or None."""
    rows = call_action(MOD.list_anomalies, conn, ns(
        company_id=company_id, severity=None, status=None,
        limit="50", offset="0",
    ))
    for a in rows["anomalies"]:
        if a["anomaly_type"] == anomaly_type:
            return a
    return None


class TestAI1ReservationOverAvailable:
    def _detect(self, conn, company_id):
        return call_action(MOD.detect_anomalies, conn, ns(
            company_id=company_id,
            from_date="2026-01-01", to_date="2026-03-31",
        ))

    def test_reservations_exceed_on_hand_raises(self, conn, env):
        """Active reservations (100) above on-hand stock (40) for an
        item/warehouse raise reservation_over_available with exact figures."""
        item_id = seed_item(conn)
        wh_id = seed_warehouse(conn, env["company_id"])
        seed_sle(conn, item_id, wh_id, actual_qty="40")          # on hand 40
        seed_reservation(conn, env["company_id"], item_id, wh_id,
                         reserved_qty="100")                      # reserved 100
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("reservation_over_available", 0) == 1
        assert r["by_severity"].get("warning", 0) >= 1

        a = _find_anomaly(conn, env["company_id"], "reservation_over_available")
        assert a is not None
        assert a["severity"] == "warning"          # on-hand > 0
        assert a["entity_type"] == "item"
        assert a["entity_id"] == f"{item_id}:{wh_id}"
        assert json.loads(a["baseline"]) == {"on_hand_qty": "40.00"}
        assert json.loads(a["actual"]) == {"reserved_qty": "100.00"}
        assert a["deviation_pct"] == "60.00"        # 60 short / 100 reserved
        assert json.loads(a["evidence"])["shortfall"] == "60.00"

    def test_zero_on_hand_is_critical(self, conn, env):
        """Reservations against an item with NO stock at all => critical."""
        item_id = seed_item(conn)
        wh_id = seed_warehouse(conn, env["company_id"])
        seed_reservation(conn, env["company_id"], item_id, wh_id,
                         reserved_qty="25")
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        a = _find_anomaly(conn, env["company_id"], "reservation_over_available")
        assert a is not None
        assert a["severity"] == "critical"

    def test_sufficient_stock_no_anomaly(self, conn, env):
        """NEGATIVE CONTROL: on-hand (100) >= reserved (30) must NOT fire."""
        item_id = seed_item(conn)
        wh_id = seed_warehouse(conn, env["company_id"])
        seed_sle(conn, item_id, wh_id, actual_qty="100")
        seed_reservation(conn, env["company_id"], item_id, wh_id,
                         reserved_qty="30")
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("reservation_over_available", 0) == 0

    def test_released_reservation_ignored(self, conn, env):
        """NEGATIVE CONTROL: a non-active (released) reservation is not counted."""
        item_id = seed_item(conn)
        wh_id = seed_warehouse(conn, env["company_id"])
        seed_sle(conn, item_id, wh_id, actual_qty="0")
        seed_reservation(conn, env["company_id"], item_id, wh_id,
                         reserved_qty="80", status="released")
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("reservation_over_available", 0) == 0


class TestAI1SubcontractReceiptMismatch:
    def _detect(self, conn, company_id):
        return call_action(MOD.detect_anomalies, conn, ns(
            company_id=company_id,
            from_date="2026-01-01", to_date="2026-03-31",
        ))

    def test_over_receipt_raises_critical(self, conn, env):
        """Received FG (100) far exceeds materials transferred (10) => a critical
        subcontract_receipt_mismatch with exact figures."""
        oid = seed_subcontracting_order(
            conn, env["company_id"], qty="100",
            materials_transferred="10", received_qty="100",
            status="partially_received")
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("subcontract_receipt_mismatch", 0) == 1
        assert r["by_severity"].get("critical", 0) >= 1

        a = _find_anomaly(conn, env["company_id"], "subcontract_receipt_mismatch")
        assert a is not None
        assert a["severity"] == "critical"
        assert a["entity_type"] == "subcontracting_order"
        assert a["entity_id"] == oid
        assert json.loads(a["baseline"]) == {"materials_transferred": "10.00"}
        assert json.loads(a["actual"]) == {"received_qty": "100.00"}
        assert a["deviation_pct"] == "900.00"       # (100-10)/10 * 100
        assert json.loads(a["evidence"])["direction"] == "over_receipt"

    def test_completed_under_receipt_warns(self, conn, env):
        """A completed order where transferred (100) materially exceeds received
        (90) => a warning (yield loss). 10 short / 100 = 10% > 5% tolerance."""
        seed_subcontracting_order(
            conn, env["company_id"], qty="90",
            materials_transferred="100", received_qty="90",
            status="completed")
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("subcontract_receipt_mismatch", 0) == 1

        a = _find_anomaly(conn, env["company_id"], "subcontract_receipt_mismatch")
        assert a is not None
        assert a["severity"] == "warning"
        assert a["deviation_pct"] == "10.00"
        assert json.loads(a["evidence"])["direction"] == "under_receipt"

    def test_within_tolerance_no_anomaly(self, conn, env):
        """NEGATIVE CONTROL: received (98) vs transferred (100) = 2% divergence,
        within the 5% tolerance, must NOT fire."""
        seed_subcontracting_order(
            conn, env["company_id"], qty="98",
            materials_transferred="100", received_qty="98",
            status="completed")
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("subcontract_receipt_mismatch", 0) == 0

    def test_partial_under_receipt_not_flagged(self, conn, env):
        """NEGATIVE CONTROL: an in-progress (partially_received) order with
        received (40) below transferred (100) is legitimate mid-flow and must NOT
        fire (under-receipt is only checked on completed orders)."""
        seed_subcontracting_order(
            conn, env["company_id"], qty="100",
            materials_transferred="100", received_qty="40",
            status="partially_received")
        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("subcontract_receipt_mismatch", 0) == 0


# ===========================================================================
# AI1 — consumption_spike + rate_plan_mismatch  (Wave F usage anomalies)
# ===========================================================================

class TestAI1UsageAnomaly:
    """Wave F AI1 detector (S1.5): usage spike > Nx baseline + rate-plan
    mismatch, per OVERVIEW's AI1 table Wave-F row."""

    def _detect(self, conn, company_id):
        return call_action(MOD.detect_anomalies, conn, ns(
            company_id=company_id,
            from_date="2026-03-01", to_date="2026-03-31",
        ))

    def _seed_baseline(self, conn, meter_id, values=("10", "10", "10")):
        """Three pre-window readings (the minimum baseline)."""
        for i, v in enumerate(values):
            seed_meter_reading(conn, meter_id, f"2026-01-{10 + i:02d}", v)

    def test_consumption_spike_raises_critical(self, conn, env):
        """Window avg (100) is 10x the baseline avg (10) => critical
        consumption_spike with exact figures."""
        cust = seed_customer(conn, env["company_id"])
        meter_id = seed_meter(conn, cust)
        self._seed_baseline(conn, meter_id)                 # baseline avg 10
        seed_meter_reading(conn, meter_id, "2026-03-10", "100")  # window avg 100

        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("consumption_spike", 0) == 1
        assert r["by_severity"].get("critical", 0) >= 1

        a = _find_anomaly(conn, env["company_id"], "consumption_spike")
        assert a is not None
        assert a["severity"] == "critical"                  # 10x > 2*3x
        assert a["entity_type"] == "meter"
        assert a["entity_id"] == meter_id
        assert json.loads(a["baseline"]) == {"baseline_avg_consumption": "10.00"}
        assert json.loads(a["actual"]) == {"window_avg_consumption": "100.00"}
        assert a["deviation_pct"] == "900.00"               # (100-10)/10 * 100

    def test_moderate_spike_is_warning(self, conn, env):
        """Window avg (40) is 4x baseline (10): above the 3x threshold but
        under the 6x critical line => warning."""
        cust = seed_customer(conn, env["company_id"])
        meter_id = seed_meter(conn, cust)
        self._seed_baseline(conn, meter_id)
        seed_meter_reading(conn, meter_id, "2026-03-10", "40")

        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        a = _find_anomaly(conn, env["company_id"], "consumption_spike")
        assert a is not None
        assert a["severity"] == "warning"
        assert a["deviation_pct"] == "300.00"

    def test_normal_usage_no_spike(self, conn, env):
        """NEGATIVE CONTROL: window avg (12) vs baseline (10) must NOT fire."""
        cust = seed_customer(conn, env["company_id"])
        meter_id = seed_meter(conn, cust)
        self._seed_baseline(conn, meter_id)
        seed_meter_reading(conn, meter_id, "2026-03-10", "12")

        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("consumption_spike", 0) == 0

    def test_insufficient_baseline_no_spike(self, conn, env):
        """NEGATIVE CONTROL: only 2 pre-window readings (< the 3-reading
        minimum) => no baseline, no fire, however large the window reading."""
        cust = seed_customer(conn, env["company_id"])
        meter_id = seed_meter(conn, cust)
        seed_meter_reading(conn, meter_id, "2026-01-10", "10")
        seed_meter_reading(conn, meter_id, "2026-01-11", "10")
        seed_meter_reading(conn, meter_id, "2026-03-10", "500")

        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("consumption_spike", 0) == 0

    def test_prepaid_overage_raises_critical_mismatch(self, conn, env):
        """A meter on a prepaid_credit plan whose balance carries accrued
        overage => critical rate_plan_mismatch."""
        cust = seed_customer(conn, env["company_id"])
        plan = seed_rate_plan(conn, plan_type="prepaid_credit")
        seed_meter(conn, cust, rate_plan_id=plan)
        bal = seed_prepaid_balance(conn, cust, plan, remaining="0",
                                   overage="25.50", status="exhausted")

        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 1

        a = _find_anomaly(conn, env["company_id"], "rate_plan_mismatch")
        assert a is not None
        assert a["severity"] == "critical"
        assert a["entity_type"] == "prepaid_credit_balance"
        assert a["entity_id"] == bal
        assert json.loads(a["actual"]) == {"overage_amount": "25.50"}

    def test_prepaid_exhausted_no_overage_is_warning(self, conn, env):
        """Exhausted balance with zero overage => warning (cap hit, not yet
        exceeded)."""
        cust = seed_customer(conn, env["company_id"])
        plan = seed_rate_plan(conn, plan_type="prepaid_credit")
        seed_meter(conn, cust, rate_plan_id=plan)
        seed_prepaid_balance(conn, cust, plan, remaining="0",
                             overage="0", status="exhausted")

        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        a = _find_anomaly(conn, env["company_id"], "rate_plan_mismatch")
        assert a is not None
        assert a["severity"] == "warning"

    def test_prepaid_active_balance_no_mismatch(self, conn, env):
        """NEGATIVE CONTROL: an active prepaid balance with credit left and no
        overage must NOT fire."""
        cust = seed_customer(conn, env["company_id"])
        plan = seed_rate_plan(conn, plan_type="prepaid_credit")
        seed_meter(conn, cust, rate_plan_id=plan)
        seed_prepaid_balance(conn, cust, plan, remaining="60",
                             overage="0", status="active")

        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 0

    def test_tier_ceiling_exceeded_raises_mismatch(self, conn, env):
        """A single reading (600) beyond a fully-closed tiered plan's
        per-billing-period top ceiling (500) => warning rate_plan_mismatch
        with exact figures (single-reading lower bound: no billing periods
        exist, and one reading always bills into exactly one period)."""
        cust = seed_customer(conn, env["company_id"])
        plan = seed_rate_plan(conn, plan_type="tiered", tiers=[
            ("0", "100", "0.10"), ("100", "500", "0.08")])
        meter_id = seed_meter(conn, cust, rate_plan_id=plan)
        rid = seed_meter_reading(conn, meter_id, "2026-03-10", "600")

        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 1

        a = _find_anomaly(conn, env["company_id"], "rate_plan_mismatch")
        assert a is not None
        assert a["severity"] == "warning"
        assert a["entity_type"] == "meter_reading"
        assert a["entity_id"] == rid
        assert json.loads(a["baseline"]) == {"plan_ceiling": "500.00"}
        assert json.loads(a["actual"]) == {"reading_consumption": "600.00"}
        assert a["deviation_pct"] == "20.00"                # (600-500)/500 * 100

    def test_open_ended_plan_never_fires(self, conn, env):
        """NEGATIVE CONTROL: a plan whose top tier is open-ended (NULL
        tier_end) defines no ceiling and must NOT fire, whatever the usage."""
        cust = seed_customer(conn, env["company_id"])
        plan = seed_rate_plan(conn, plan_type="tiered", tiers=[
            ("0", "100", "0.10"), ("100", None, "0.08")])
        meter_id = seed_meter(conn, cust, rate_plan_id=plan)
        seed_meter_reading(conn, meter_id, "2026-03-10", "9999")

        r = self._detect(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 0

    def test_idempotent_rerun_no_duplicates(self, conn, env):
        """A second sweep must not duplicate open usage anomalies (the
        _insert_anomaly new/acknowledged dedup)."""
        cust = seed_customer(conn, env["company_id"])
        meter_id = seed_meter(conn, cust)
        self._seed_baseline(conn, meter_id)
        seed_meter_reading(conn, meter_id, "2026-03-10", "100")

        r1 = self._detect(conn, env["company_id"])
        assert is_ok(r1)
        assert r1["by_type"].get("consumption_spike", 0) == 1
        r2 = self._detect(conn, env["company_id"])
        assert is_ok(r2)
        assert r2["by_type"].get("consumption_spike", 0) == 0
        n = conn.execute(
            "SELECT COUNT(*) FROM anomaly WHERE anomaly_type = 'consumption_spike'"
        ).fetchone()[0]
        assert n == 1


class TestAI1UsageAnomalyQaBounceRegressions:
    """Pins for the 2026-07-25 QA bounce of S1.5 — one regression per
    executed defect (2 HIGH, 1 MEDIUM, 1 LOW)."""

    def _detect_default(self, conn, company_id):
        """The default (no-date-flags) sweep — exactly what QA broke."""
        return call_action(MOD.detect_anomalies, conn, ns(
            company_id=company_id, from_date=None, to_date=None,
        ))

    def _detect_window(self, conn, company_id, from_date, to_date):
        return call_action(MOD.detect_anomalies, conn, ns(
            company_id=company_id, from_date=from_date, to_date=to_date,
        ))

    # --- HIGH 1: consumption_spike must fire on the default invocation ---

    def test_default_sweep_consumption_spike_fires(self, conn, env):
        """QA reproduction: 5 baseline readings of 10 (Jan) + one 500 (Mar),
        NO date flags. The old code put every reading in the window (empty
        baseline) and could never emit; the recency split must fire."""
        cust = seed_customer(conn, env["company_id"])
        meter_id = seed_meter(conn, cust)
        for i in range(5):
            seed_meter_reading(conn, meter_id, f"2026-01-{10 + i:02d}", "10")
        seed_meter_reading(conn, meter_id, "2026-03-10", "500")

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("consumption_spike", 0) == 1

        a = _find_anomaly(conn, env["company_id"], "consumption_spike")
        assert a is not None
        # window = last 3 readings (10,10,500) avg 173.33 vs baseline 10.00
        assert a["severity"] == "critical"       # 520*3 > 30*3*6
        assert json.loads(a["baseline"]) == {"baseline_avg_consumption": "10.00"}
        assert json.loads(a["actual"]) == {"window_avg_consumption": "173.33"}
        assert json.loads(a["evidence"])["window_mode"] == "recent_readings"

    def test_default_sweep_steady_usage_stays_silent(self, conn, env):
        """NEGATIVE CONTROL (QA reproduction a): 12 months of steady 100/month
        on a plan with a 500/period ceiling must fire NEITHER anomaly on the
        default sweep — the old code falsely accused this customer with a
        1200-vs-500 whole-window comparison."""
        cust = seed_customer(conn, env["company_id"])
        plan = seed_rate_plan(conn, plan_type="tiered", tiers=[
            ("0", "100", "0.10"), ("100", "500", "0.08")])
        meter_id = seed_meter(conn, cust, rate_plan_id=plan)
        for month_date in ("2025-08-01", "2025-09-01", "2025-10-01",
                           "2025-11-01", "2025-12-01", "2026-01-01",
                           "2026-02-01", "2026-03-01", "2026-04-01",
                           "2026-05-01", "2026-06-01", "2026-07-01"):
            seed_meter_reading(conn, meter_id, month_date, "100")

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("consumption_spike", 0) == 0
        assert r["by_type"].get("rate_plan_mismatch", 0) == 0

    # --- HIGH 2: per-billing-period ceiling accounting ---

    def test_explicit_quarter_window_compliant_customer_not_accused(
            self, conn, env):
        """QA reproduction b: ordinary quarterly window over monthly usage of
        200 against a 500/period ceiling — fully compliant, must NOT fire."""
        cust = seed_customer(conn, env["company_id"])
        plan = seed_rate_plan(conn, plan_type="tiered", tiers=[
            ("0", "100", "0.10"), ("100", "500", "0.08")])
        meter_id = seed_meter(conn, cust, rate_plan_id=plan)
        for month_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
            seed_meter_reading(conn, meter_id, month_date, "200")

        r = self._detect_window(conn, env["company_id"],
                                "2026-01-01", "2026-03-31")
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 0

    def test_tier_ceiling_fires_per_billing_period(self, conn, env):
        """TRUE POSITIVE with billing periods: an open January period whose
        readings sum to 600 (> the 500 ceiling) fires exactly once, keyed to
        that period; a rated February period whose AUTHORITATIVE stored
        total_consumption is 400 stays silent even though its raw readings
        sum higher (run-billing's figure wins for rated periods)."""
        cust = seed_customer(conn, env["company_id"])
        plan = seed_rate_plan(conn, plan_type="tiered", tiers=[
            ("0", "100", "0.10"), ("100", "500", "0.08")])
        meter_id = seed_meter(conn, cust, rate_plan_id=plan)
        jan = seed_billing_period(conn, cust, meter_id, plan,
                                  "2026-01-01", "2026-01-31", status="open")
        seed_billing_period(conn, cust, meter_id, plan,
                            "2026-02-01", "2026-02-28", status="rated",
                            total_consumption="400")
        seed_meter_reading(conn, meter_id, "2026-01-10", "300")
        seed_meter_reading(conn, meter_id, "2026-01-20", "300")
        seed_meter_reading(conn, meter_id, "2026-02-10", "300")
        seed_meter_reading(conn, meter_id, "2026-02-20", "300")

        r = self._detect_window(conn, env["company_id"],
                                "2026-01-01", "2026-02-28")
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 1

        a = _find_anomaly(conn, env["company_id"], "rate_plan_mismatch")
        assert a is not None
        assert a["severity"] == "warning"
        assert a["entity_type"] == "billing_period"
        assert a["entity_id"] == jan
        assert json.loads(a["baseline"]) == {"plan_ceiling": "500.00"}
        assert json.loads(a["actual"]) == {"period_consumption": "600.00"}
        assert a["deviation_pct"] == "20.00"

    # --- MEDIUM: datetime-carrying reading_date must not be dropped ---

    def test_datetime_reading_date_counts_in_window(self, conn, env):
        """QA reproduction: a reading stored as '2026-03-31 08:00:00' (legal
        through add-meter-reading) must land in the March window instead of
        silently vanishing from both buckets."""
        cust = seed_customer(conn, env["company_id"])
        meter_id = seed_meter(conn, cust)
        for i in range(3):
            seed_meter_reading(conn, meter_id, f"2026-01-{10 + i:02d}", "10")
        seed_meter_reading(conn, meter_id, "2026-03-31 08:00:00", "500")

        r = self._detect_window(conn, env["company_id"],
                                "2026-03-01", "2026-03-31")
        assert is_ok(r)
        assert r["by_type"].get("consumption_spike", 0) == 1
        a = _find_anomaly(conn, env["company_id"], "consumption_spike")
        assert json.loads(a["actual"]) == {"window_avg_consumption": "500.00"}

    # --- LOW: exact-Nx boundary is strict ---

    def test_exact_multiplier_boundary_does_not_fire(self, conn, env):
        """QA reproduction: baseline (1,2,4) has a non-terminating avg (7/3);
        a window reading of exactly 7 (= 3x) fired through divide-then-
        multiply rounding. Cross-multiplied comparison keeps the strict >
        exact: exactly-3x stays silent, 7.01 (just above) fires."""
        cust = seed_customer(conn, env["company_id"])
        meter_exact = seed_meter(conn, cust)
        meter_above = seed_meter(conn, cust)
        for meter_id in (meter_exact, meter_above):
            for day, val in (("2026-01-10", "1"), ("2026-01-11", "2"),
                             ("2026-01-12", "4")):
                seed_meter_reading(conn, meter_id, day, val)
        seed_meter_reading(conn, meter_exact, "2026-03-10", "7")
        seed_meter_reading(conn, meter_above, "2026-03-10", "7.01")

        r = self._detect_window(conn, env["company_id"],
                                "2026-03-01", "2026-03-31")
        assert is_ok(r)
        assert r["by_type"].get("consumption_spike", 0) == 1
        fired = [row[0] for row in conn.execute(
            "SELECT entity_id FROM anomaly "
            "WHERE anomaly_type = 'consumption_spike'").fetchall()]
        assert fired == [meter_above]
        a = _find_anomaly(conn, env["company_id"], "consumption_spike")
        assert a["severity"] == "warning"      # 3.004x is above 3x, below 6x
        assert a["deviation_pct"] == "200.43"  # (7.01-7/3)/(7/3)*100, exact


class TestAI1UsageAnomalyQaBounce2Regressions:
    """Pins for the 2026-07-25 QA bounce #2 of S1.5 — two MEDIUM
    'falsely accuses a compliant customer' defects in the rate_plan_mismatch
    volume-ceiling heuristic (DEFECT-A: a meter plan change re-judged
    already-rated history under the NEW plan; DEFECT-B: max(tier_end) was
    applied to plan types whose tiers are per-band caps, not cumulative
    volume bands)."""

    def _detect_default(self, conn, company_id):
        return call_action(MOD.detect_anomalies, conn, ns(
            company_id=company_id, from_date=None, to_date=None,
        ))

    # --- DEFECT-A: each billing period is judged against ITS OWN plan ---

    def test_plan_downgrade_does_not_accuse_rated_history(self, conn, env):
        """QA reproduction (qa_p1_e2e.py + qa_probe2.py P1 seeded variant):
        periods rated/paid at 1000 under the open-ended 'Unlimited' plan
        (billing_period.rate_plan_id == Unlimited), then the meter downgrades
        to 'Small' (500/period ceiling) via one shipped action. The history
        WAS priced and rated under Unlimited — zero mismatches."""
        cust = seed_customer(conn, env["company_id"])
        unlimited = seed_rate_plan(conn, plan_type="tiered", name="Unlimited",
                                   tiers=[("0", None, "0.10")])
        small = seed_rate_plan(conn, plan_type="tiered", name="Small", tiers=[
            ("0", "100", "0.10"), ("100", "500", "0.08")])
        meter_id = seed_meter(conn, cust, rate_plan_id=unlimited)
        for start, end, status in (
                ("2026-01-01", "2026-01-31", "rated"),
                ("2026-02-01", "2026-02-28", "paid"),
                ("2026-03-01", "2026-03-31", "paid")):
            seed_billing_period(conn, cust, meter_id, unlimited, start, end,
                                status=status, total_consumption="1000")
            seed_meter_reading(conn, meter_id, end, "1000")
        # The shipped-action downgrade: update-meter --rate-plan-id Small.
        conn.execute("UPDATE meter SET rate_plan_id = ? WHERE id = ?",
                     (small, meter_id))
        conn.commit()

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 0
        n = conn.execute("SELECT COUNT(*) FROM anomaly "
                         "WHERE anomaly_type = 'rate_plan_mismatch'"
                         ).fetchone()[0]
        assert n == 0

    def test_rated_history_still_fires_under_its_own_plan(self, conn, env):
        """Complement (guards against over-fix): a period RATED under 'Small'
        (500 ceiling) at 600 keeps firing after the meter upgrades to the
        open-ended 'Unlimited' — the period's own plan governs in BOTH
        directions; an upgrade must not silence a genuine historical
        violation."""
        cust = seed_customer(conn, env["company_id"])
        small = seed_rate_plan(conn, plan_type="tiered", name="Small", tiers=[
            ("0", "100", "0.10"), ("100", "500", "0.08")])
        unlimited = seed_rate_plan(conn, plan_type="tiered", name="Unlimited",
                                   tiers=[("0", None, "0.10")])
        meter_id = seed_meter(conn, cust, rate_plan_id=small)
        # usage_charge = what run-billing stores when it rates 600 under
        # Small (100@0.10 + 400@0.08, top 100 unpriced) — a genuinely-rated
        # row always carries the charge its own plan computed, and the
        # round-3 attribution guard verifies exactly that agreement.
        jan = seed_billing_period(conn, cust, meter_id, small,
                                  "2026-01-01", "2026-01-31", status="rated",
                                  total_consumption="600",
                                  usage_charge="42.00")
        conn.execute("UPDATE meter SET rate_plan_id = ? WHERE id = ?",
                     (unlimited, meter_id))
        conn.commit()

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 1
        a = _find_anomaly(conn, env["company_id"], "rate_plan_mismatch")
        assert a is not None
        assert a["severity"] == "warning"
        assert a["entity_type"] == "billing_period"
        assert a["entity_id"] == jan
        assert json.loads(a["baseline"]) == {"plan_ceiling": "500.00"}
        assert json.loads(a["actual"]) == {"period_consumption": "600.00"}
        assert a["deviation_pct"] == "20.00"
        # Evidence names the PERIOD's plan, not the meter's current one.
        assert json.loads(a["evidence"])["rate_plan_id"] == small

    # --- DEFECT-B: max(tier_end) only for cumulative volume-band plans ---

    def test_tou_closed_bands_not_a_volume_ceiling(self, conn, env):
        """QA reproduction (qa_p2_e2e.py): a time_of_use plan with two closed
        0-1000 bands (peak @0.20, off_peak @0.10); an open January period
        whose readings sum to 1200 (600 peak + 600 off-peak). Each band is
        within its own 1000 cap — max(tier_end) is NOT a volume ceiling for
        TOU; must NOT fire."""
        cust = seed_customer(conn, env["company_id"])
        tou = seed_rate_plan(conn, plan_type="time_of_use", name="TOU")
        for i, (rate, tou_period) in enumerate(
                (("0.20", "peak"), ("0.10", "off_peak"))):
            conn.execute(
                """INSERT INTO rate_tier (id, rate_plan_id, tier_start,
                   tier_end, rate, time_of_use_period, sort_order)
                   VALUES (?, ?, '0', '1000', ?, ?, ?)""",
                (_uuid(), tou, rate, tou_period, i))
        meter_id = seed_meter(conn, cust, rate_plan_id=tou)
        seed_billing_period(conn, cust, meter_id, tou,
                            "2026-01-01", "2026-01-31", status="open")
        seed_meter_reading(conn, meter_id, "2026-01-15", "600")  # peak band
        seed_meter_reading(conn, meter_id, "2026-01-31", "600")  # off-peak
        conn.commit()

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 0

    def test_demand_hybrid_no_ceiling_volume_discount_still_fires(
            self, conn, env):
        """DEFECT-B on the no-period single-reading path: demand and hybrid
        plans with fully-closed tiers never define a volume ceiling, however
        large the reading; volume_discount (IN the cumulative volume-band
        set _calculate_charge walks) with the same tier shape still fires."""
        cust = seed_customer(conn, env["company_id"])
        vd_reading = None
        for ptype in ("demand", "hybrid", "volume_discount"):
            plan = seed_rate_plan(conn, plan_type=ptype, name=ptype,
                                  tiers=[("0", "500", "0.10")])
            meter_id = seed_meter(conn, cust, rate_plan_id=plan)
            rid = seed_meter_reading(conn, meter_id, "2026-03-10", "900")
            if ptype == "volume_discount":
                vd_reading = rid

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 1
        rows = [dict(x) for x in conn.execute(
            "SELECT entity_type, entity_id FROM anomaly "
            "WHERE anomaly_type = 'rate_plan_mismatch'").fetchall()]
        assert rows == [{"entity_type": "meter_reading",
                         "entity_id": vd_reading}]


class TestAI1UsageAnomalyQaRound3Regressions:
    """Pins for the 2026-07-26 QA round 3 (directed final round, pm-scoped):
    DEFECT-C — a mid-cycle plan UPGRADE leaves a terminal billing_period
    whose rate_plan_id names a plan that never priced it (run-billing's
    open-period UPDATE re-rates under the meter's current plan without
    rewriting the period's plan); the detector must not accuse a correctly
    priced bill it cannot attribute. DEFECT-D — a garbage rate_tier.tier_end
    (accepted unvalidated by add-rate-plan) aborted the ENTIRE
    detect-anomalies action company-wide."""

    def _detect_default(self, conn, company_id):
        return call_action(MOD.detect_anomalies, conn, ns(
            company_id=company_id, from_date=None, to_date=None,
        ))

    # --- DEFECT-C: unattributable terminal period is never accused ---

    def test_mid_cycle_upgrade_correctly_priced_bill_not_accused(
            self, conn, env):
        """QA reproduction (qa3_probeA.py end-state, all shipped actions):
        January period opened under 'Small' (500 ceiling), customer upgrades
        to open-ended 'Unlimited' mid-cycle, run-billing rates 1000 kWh at
        $100.00 (= 1000 x 0.10 under Unlimited) but the row still records
        Small. Recomputing Small's tiers gives $42.00 != $100.00 — the
        stored plan did not price this period, so the sweep stays SILENT."""
        cust = seed_customer(conn, env["company_id"])
        small = seed_rate_plan(conn, plan_type="tiered", name="Small", tiers=[
            ("0", "100", "0.10"), ("100", "500", "0.08")])
        unlimited = seed_rate_plan(conn, plan_type="tiered", name="Unlimited",
                                   tiers=[("0", None, "0.10")])
        # Meter already upgraded (update-meter --rate-plan-id Unlimited).
        meter_id = seed_meter(conn, cust, rate_plan_id=unlimited)
        # The lying terminal row exactly as run-billing leaves it: plan =
        # Small (never rewritten), charge = what Unlimited actually billed.
        seed_billing_period(conn, cust, meter_id, small,
                            "2026-01-01", "2026-01-31", status="rated",
                            total_consumption="1000", usage_charge="100.00")
        seed_meter_reading(conn, meter_id, "2026-01-05", "0")
        seed_meter_reading(conn, meter_id, "2026-01-31", "1000")

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 0
        assert r["by_type"].get("consumption_spike", 0) == 0
        n = conn.execute("SELECT COUNT(*) FROM anomaly "
                         "WHERE anomaly_type = 'rate_plan_mismatch'"
                         ).fetchone()[0]
        assert n == 0

    def test_truthful_small_rated_overage_still_fires(self, conn, env):
        """Complement (the post-lane-A composition leg): the SAME 1000-kWh
        overage genuinely rated under Small stores usage_charge $42.00
        (100@0.10 + 400@0.08, 500 kWh unpriced) — recomputation AGREES, so
        the accusation fires normally. The attribution guard silences only
        rows the stored plan did not price."""
        cust = seed_customer(conn, env["company_id"])
        small = seed_rate_plan(conn, plan_type="tiered", name="Small", tiers=[
            ("0", "100", "0.10"), ("100", "500", "0.08")])
        meter_id = seed_meter(conn, cust, rate_plan_id=small)
        jan = seed_billing_period(conn, cust, meter_id, small,
                                  "2026-01-01", "2026-01-31", status="rated",
                                  total_consumption="1000",
                                  usage_charge="42.00")

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 1
        a = _find_anomaly(conn, env["company_id"], "rate_plan_mismatch")
        assert a is not None
        assert a["entity_type"] == "billing_period"
        assert a["entity_id"] == jan
        assert json.loads(a["baseline"]) == {"plan_ceiling": "500.00"}
        assert json.loads(a["actual"]) == {"period_consumption": "1000.00"}
        assert a["deviation_pct"] == "100.00"

    # --- DEFECT-D: one bad tier row must never abort the company sweep ---

    @pytest.mark.parametrize("bad_tier_end", [
        "", "  ", "NaN", "sNaN", "abc", "123456789012345678901234567",
    ], ids=["blank", "whitespace", "nan", "snan", "alpha", "27-digit"])
    def test_garbage_tier_end_never_aborts_sweep(self, conn, env,
                                                 bad_tier_end):
        """QA reproduction (qa3_edge.py battery + oversized value):
        add-rate-plan accepts the junk tier_end; detect-anomalies must
        complete (no company-wide abort), stay silent on the un-judgeable
        plan, and every OTHER finding must survive (a second meter's genuine
        consumption spike still comes through)."""
        cust = seed_customer(conn, env["company_id"])
        plan = seed_rate_plan(conn, plan_type="tiered", name="Junk",
                              tiers=[("0", bad_tier_end, "0.10")])
        meter_bad = seed_meter(conn, cust, rate_plan_id=plan)
        seed_meter_reading(conn, meter_bad, "2026-03-10", "900")
        # Second meter: a genuine spike that must SURVIVE the junk plan
        # (5 steady readings so the recency split leaves a full baseline:
        # window = last 3 = (10, 10, 500), baseline = (10, 10, 10)).
        meter_ok = seed_meter(conn, cust)
        for i in range(5):
            seed_meter_reading(conn, meter_ok, f"2026-01-{10 + i:02d}", "10")
        seed_meter_reading(conn, meter_ok, "2026-03-10", "500")

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 0
        assert r["by_type"].get("consumption_spike", 0) == 1

    @pytest.mark.parametrize("benign_tier_end", [" 500 ", "5e2"],
                             ids=["padded", "scientific"])
    def test_parseable_tier_end_variants_still_fire(self, conn, env,
                                                    benign_tier_end):
        """Guard against over-containment (QA's benign rows): a padded or
        scientific-notation tier_end still resolves to the 500 ceiling and
        the 900 reading still fires."""
        cust = seed_customer(conn, env["company_id"])
        plan = seed_rate_plan(conn, plan_type="tiered", name="Padded",
                              tiers=[("0", benign_tier_end, "0.10")])
        meter_id = seed_meter(conn, cust, rate_plan_id=plan)
        rid = seed_meter_reading(conn, meter_id, "2026-03-10", "900")

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 1
        a = _find_anomaly(conn, env["company_id"], "rate_plan_mismatch")
        assert a["entity_type"] == "meter_reading"
        assert a["entity_id"] == rid
        assert json.loads(a["baseline"]) == {"plan_ceiling": "500.00"}


class TestAI1UsageAnomalyRound4PmInline:
    """Pins for the 2026-07-27 pm-inline round (main session):
    DEFECT-E — an oversized-but-finite figure (>= 1E26 survives is_finite,
    overflows the 28-digit context in emission arithmetic) reachable through
    shipped add-meter-reading aborted the ENTIRE company sweep in all four
    emission branches. DEFECT-F — the round-3 exact charge-agreement guard
    silenced TRUE accusations forever after a routine update-rate-plan
    re-price; replaced by the impossible-charge discriminator (silence only
    when stored usage_charge exceeds the stored plan's maximum billable at
    its own ceiling)."""

    def _detect_default(self, conn, company_id):
        return call_action(MOD.detect_anomalies, conn, ns(
            company_id=company_id, from_date=None, to_date=None,
        ))

    # --- DEFECT-E: oversized figures skip the row, never the sweep ---

    def test_oversized_reading_never_aborts_sweep(self, conn, env):
        """One meter with a 1E26 reading; ANOTHER customer's genuine 10x
        spike must still be reported (the QA blast-radius scenario)."""
        runaway = seed_customer(conn, env["company_id"], name="Runaway Co")
        plan = seed_rate_plan(conn, plan_type="tiered", name="Small", tiers=[
            ("0", "600", "0.07")])
        bad_meter = seed_meter(conn, runaway, rate_plan_id=plan)
        seed_meter_reading(conn, bad_meter, "2026-06-01", "0")
        seed_meter_reading(
            conn, bad_meter, "2026-06-15",
            "100000000000000000000000000")   # 1E26, shipped-action reachable

        normal = seed_customer(conn, env["company_id"], name="Normal Cafe")
        ok_meter = seed_meter(conn, normal, rate_plan_id=None)
        for day in range(1, 6):
            seed_meter_reading(conn, ok_meter, f"2026-05-0{day}", "10")
        seed_meter_reading(conn, ok_meter, "2026-06-20", "500")

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("consumption_spike", 0) == 1
        a = _find_anomaly(conn, env["company_id"], "consumption_spike")
        assert a is not None and a["entity_id"] == ok_meter

    def test_oversized_stored_period_total_skips_period_only(
            self, conn, env):
        """A terminal period carrying a garbage 1E26 total is skipped; a
        sane over-ceiling period on the SAME plan still fires."""
        cust = seed_customer(conn, env["company_id"])
        plan = seed_rate_plan(conn, plan_type="tiered", name="Small", tiers=[
            ("0", "500", "0.08")])
        meter_id = seed_meter(conn, cust, rate_plan_id=plan)
        seed_billing_period(conn, cust, meter_id, plan,
                            "2026-01-01", "2026-01-31", status="rated",
                            total_consumption="1" + "0" * 26,
                            usage_charge="40.00")
        feb = seed_billing_period(conn, cust, meter_id, plan,
                                  "2026-02-01", "2026-02-28", status="rated",
                                  total_consumption="1000",
                                  usage_charge="40.00")

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 1
        a = _find_anomaly(conn, env["company_id"], "rate_plan_mismatch")
        assert a is not None and a["entity_id"] == feb

    # --- DEFECT-F: a re-priced plan's true accusations keep firing ---

    def test_reprice_keeps_true_accusation_firing(self, conn, env):
        """The QA round-4 scenario: 1000 kWh genuinely rated under Small
        (600 @ 0.07 -> stored charge 42.00), then the utility raises the
        rate to 0.08 (update-rate-plan deletes+re-inserts tiers). Stored
        42.00 <= new maximum 48.00 -> explainable under Small -> the
        over-ceiling accusation MUST still fire."""
        cust = seed_customer(conn, env["company_id"])
        plan = seed_rate_plan(conn, plan_type="tiered", name="Small", tiers=[
            ("0", "600", "0.07")])
        meter_id = seed_meter(conn, cust, rate_plan_id=plan)
        jan = seed_billing_period(conn, cust, meter_id, plan,
                                  "2026-01-01", "2026-01-31", status="rated",
                                  total_consumption="1000",
                                  usage_charge="42.00")
        # update-rate-plan semantics: tiers deleted + re-inserted at 0.08.
        conn.execute("UPDATE rate_tier SET rate = '0.08' "
                     "WHERE rate_plan_id = ?", (plan,))
        conn.commit()

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 1
        a = _find_anomaly(conn, env["company_id"], "rate_plan_mismatch")
        assert a is not None and a["entity_id"] == jan

    def test_impossible_charge_still_silent(self, conn, env):
        """The DEFECT-C leg under the new discriminator: stored charge
        100.00 EXCEEDS Small's maximum billable 42.00 (100@0.10 + 400@0.08)
        -> impossible under the stored plan -> silent."""
        cust = seed_customer(conn, env["company_id"])
        small = seed_rate_plan(conn, plan_type="tiered", name="Small", tiers=[
            ("0", "100", "0.10"), ("100", "500", "0.08")])
        meter_id = seed_meter(conn, cust, rate_plan_id=small)
        seed_billing_period(conn, cust, meter_id, small,
                            "2026-01-01", "2026-01-31", status="rated",
                            total_consumption="1000", usage_charge="100.00")

        r = self._detect_default(conn, env["company_id"])
        assert is_ok(r)
        assert r["by_type"].get("rate_plan_mismatch", 0) == 0


class TestMigration006WavefUsageAnomalyType:
    """L2 rehearsal pinned: migration 006 rebuilds the anomaly CHECK on a DB
    that still carries the Wave-2 (20-value) enum."""

    def _load_migration(self, number_prefix):
        import importlib.util
        mig_dir = os.path.join(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
            "migrations")
        fname = [f for f in os.listdir(mig_dir)
                 if f.startswith(number_prefix)][0]
        spec = importlib.util.spec_from_file_location(
            f"growth_mig_{number_prefix}", os.path.join(mig_dir, fname))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_migration_extends_check_preserves_rows_and_is_idempotent(
            self, tmp_path):
        import sqlite3
        db = str(tmp_path / "mig006.sqlite")
        m005 = self._load_migration("005")
        m006 = self._load_migration("006")

        # Old-shape DB: the 20-value CHECK from migration 005's frozen DDL.
        conn = sqlite3.connect(db)
        conn.execute(m005._ANOMALY_DDL_EXTENDED)
        conn.execute(
            "INSERT INTO anomaly (id, anomaly_type, severity, entity_type, "
            "entity_id, description, status) VALUES ('a1', 'price_spike', "
            "'warning', 'gl_entry', 'e1', 'pre-existing row', 'new')")
        conn.commit()
        # Old CHECK must REJECT the Wave F value (proves the rehearsal is real).
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO anomaly (id, anomaly_type, severity, description) "
                "VALUES ('bad', 'rate_plan_mismatch', 'info', 'x')")
        conn.close()

        m006.run_migration(db)

        conn = sqlite3.connect(db)
        # Pre-existing row preserved; new value now accepted; no temp table.
        assert conn.execute(
            "SELECT COUNT(*) FROM anomaly").fetchone()[0] == 1
        conn.execute(
            "INSERT INTO anomaly (id, anomaly_type, severity, description) "
            "VALUES ('a2', 'rate_plan_mismatch', 'warning', 'wave f row')")
        conn.commit()
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
            "AND name LIKE '%_wfai1_old'").fetchone()[0] == 0
        conn.close()

        # Idempotent re-run keeps both rows.
        m006.run_migration(db)
        conn = sqlite3.connect(db)
        assert conn.execute(
            "SELECT COUNT(*) FROM anomaly").fetchone()[0] == 2
        conn.close()
