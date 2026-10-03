"""L1 tests for ERPClaw Compliance module (38 actions, 8 tables, 4 domains).

Actions tested:
  Audit:      compliance-add-audit-plan, compliance-update-audit-plan, compliance-get-audit-plan,
              compliance-list-audit-plans, compliance-start-audit, compliance-complete-audit,
              compliance-add-audit-finding, compliance-list-audit-findings
  Risk:       compliance-add-risk, compliance-update-risk, compliance-get-risk, compliance-list-risks,
              compliance-add-risk-assessment, compliance-list-risk-assessments,
              compliance-risk-matrix-report, compliance-close-risk
  Controls:   compliance-add-control-test, compliance-update-control-test, compliance-get-control-test,
              compliance-list-control-tests, compliance-execute-control-test
  Calendar:   compliance-add-calendar-item, compliance-update-calendar-item, compliance-get-calendar-item,
              compliance-list-calendar-items, compliance-complete-calendar-item,
              compliance-overdue-items-report, compliance-dashboard
  Policy:     compliance-add-policy, compliance-update-policy, compliance-get-policy,
              compliance-list-policies, compliance-publish-policy, compliance-retire-policy,
              compliance-add-policy-acknowledgment, compliance-list-policy-acknowledgments,
              compliance-policy-compliance-report
  Status:     status
"""
import pytest
from compliance_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_company, seed_naming_series, seed_audit_plan, seed_risk, seed_policy,
    seed_employee,
)

mod = load_db_query()

from collections import Counter
from datetime import date

from erpclaw_lib.query import Field, P, Q, Table


# =============================================================================
# Audit Domain
# =============================================================================

class TestAddAuditPlan:
    def test_basic_create(self, conn, env):
        result = call_action(mod.compliance_add_audit_plan, conn, ns(
            company_id=env["company_id"],
            name="Q2 Internal Audit",
            audit_type="internal",
            scope="Financial controls",
            lead_auditor="Jane Auditor",
            planned_start="2026-04-01",
            planned_end="2026-04-30",
        ))
        assert is_ok(result), result
        assert result["name"] == "Q2 Internal Audit"
        assert result["plan_status"] == "draft"
        assert "id" in result
        assert "naming_series" in result

    def test_external_audit(self, conn, env):
        result = call_action(mod.compliance_add_audit_plan, conn, ns(
            company_id=env["company_id"],
            name="SOX External Audit",
            audit_type="external",
        ))
        assert is_ok(result), result

    def test_missing_name_fails(self, conn, env):
        result = call_action(mod.compliance_add_audit_plan, conn, ns(
            company_id=env["company_id"],
            name=None,
        ))
        assert is_error(result)

    def test_missing_company_fails(self, conn, env):
        result = call_action(mod.compliance_add_audit_plan, conn, ns(
            company_id=None,
            name="Orphan Audit",
        ))
        assert is_error(result)

    def test_invalid_type_fails(self, conn, env):
        result = call_action(mod.compliance_add_audit_plan, conn, ns(
            company_id=env["company_id"],
            name="Bad Type",
            audit_type="surprise",
        ))
        assert is_error(result)


class TestUpdateAuditPlan:
    def test_update_scope(self, conn, env):
        result = call_action(mod.compliance_update_audit_plan, conn, ns(
            audit_plan_id=env["audit_plan_id"],
            scope="Updated scope",
        ))
        assert is_ok(result), result
        assert "scope" in result["updated_fields"]

    def test_no_fields_fails(self, conn, env):
        result = call_action(mod.compliance_update_audit_plan, conn, ns(
            audit_plan_id=env["audit_plan_id"],
        ))
        assert is_error(result)

    def test_nonexistent_fails(self, conn, env):
        result = call_action(mod.compliance_update_audit_plan, conn, ns(
            audit_plan_id="nonexistent-id",
            name="X",
        ))
        assert is_error(result)


class TestGetAuditPlan:
    def test_get_existing(self, conn, env):
        result = call_action(mod.compliance_get_audit_plan, conn, ns(
            audit_plan_id=env["audit_plan_id"],
        ))
        assert is_ok(result), result
        assert result["name"] == "Test Audit"
        assert "finding_count" in result
        assert "open_findings" in result
        assert "critical_findings" in result

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.compliance_get_audit_plan, conn, ns(
            audit_plan_id="nonexistent-id",
        ))
        assert is_error(result)


class TestListAuditPlans:
    def test_list_by_company(self, conn, env):
        result = call_action(mod.compliance_list_audit_plans, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_list_by_status(self, conn, env):
        result = call_action(mod.compliance_list_audit_plans, conn, ns(
            company_id=env["company_id"],
            status="draft",
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1


class TestStartAudit:
    def test_start_from_draft(self, conn, env):
        result = call_action(mod.compliance_start_audit, conn, ns(
            audit_plan_id=env["audit_plan_id"],
        ))
        assert is_ok(result), result
        assert result["plan_status"] == "in_progress"
        assert "actual_start" in result

    def test_start_completed_fails(self, conn, env):
        # Start it first
        call_action(mod.compliance_start_audit, conn, ns(
            audit_plan_id=env["audit_plan_id"],
        ))
        # Complete it
        call_action(mod.compliance_complete_audit, conn, ns(
            audit_plan_id=env["audit_plan_id"],
        ))
        # Try to start again
        result = call_action(mod.compliance_start_audit, conn, ns(
            audit_plan_id=env["audit_plan_id"],
        ))
        assert is_error(result)


class TestCompleteAudit:
    def test_complete_in_progress(self, conn, env):
        call_action(mod.compliance_start_audit, conn, ns(
            audit_plan_id=env["audit_plan_id"],
        ))
        result = call_action(mod.compliance_complete_audit, conn, ns(
            audit_plan_id=env["audit_plan_id"],
        ))
        assert is_ok(result), result
        assert result["plan_status"] == "completed"
        assert "actual_end" in result

    def test_complete_draft_fails(self, conn, env):
        result = call_action(mod.compliance_complete_audit, conn, ns(
            audit_plan_id=env["audit_plan_id"],
        ))
        assert is_error(result)


class TestAddAuditFinding:
    def test_basic_finding(self, conn, env):
        result = call_action(mod.compliance_add_audit_finding, conn, ns(
            audit_plan_id=env["audit_plan_id"],
            company_id=env["company_id"],
            title="Lack of segregation of duties",
            finding_type="major",
            area="Accounts Payable",
            recommendation="Implement dual approval",
        ))
        assert is_ok(result), result
        assert result["title"] == "Lack of segregation of duties"
        assert result["finding_type"] == "major"
        assert result["finding_status"] == "open"

    def test_critical_finding(self, conn, env):
        result = call_action(mod.compliance_add_audit_finding, conn, ns(
            audit_plan_id=env["audit_plan_id"],
            company_id=env["company_id"],
            title="No backup procedures",
            finding_type="critical",
        ))
        assert is_ok(result), result
        assert result["finding_type"] == "critical"

    def test_missing_title_fails(self, conn, env):
        result = call_action(mod.compliance_add_audit_finding, conn, ns(
            audit_plan_id=env["audit_plan_id"],
            company_id=env["company_id"],
            title=None,
        ))
        assert is_error(result)


class TestListAuditFindings:
    def test_list_by_plan(self, conn, env):
        call_action(mod.compliance_add_audit_finding, conn, ns(
            audit_plan_id=env["audit_plan_id"],
            company_id=env["company_id"],
            title="Finding A",
        ))
        result = call_action(mod.compliance_list_audit_findings, conn, ns(
            audit_plan_id=env["audit_plan_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_list_by_type(self, conn, env):
        call_action(mod.compliance_add_audit_finding, conn, ns(
            audit_plan_id=env["audit_plan_id"],
            company_id=env["company_id"],
            title="Critical Finding",
            finding_type="critical",
        ))
        result = call_action(mod.compliance_list_audit_findings, conn, ns(
            finding_type="critical",
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1


# =============================================================================
# Risk Domain
# =============================================================================

class TestAddRisk:
    def test_basic_create(self, conn, env):
        result = call_action(mod.compliance_add_risk, conn, ns(
            company_id=env["company_id"],
            name="Data Breach Risk",
            category="technology",
            likelihood=4,
            impact=5,
            owner="CISO",
            mitigation_plan="Implement MFA",
        ))
        assert is_ok(result), result
        assert result["name"] == "Data Breach Risk"
        assert result["risk_score"] == 20
        assert result["risk_level"] == "critical"
        assert result["risk_status"] == "identified"

    def test_low_risk(self, conn, env):
        result = call_action(mod.compliance_add_risk, conn, ns(
            company_id=env["company_id"],
            name="Minor Process Risk",
            likelihood=1,
            impact=2,
        ))
        assert is_ok(result), result
        assert result["risk_score"] == 2
        assert result["risk_level"] == "low"

    def test_missing_name_fails(self, conn, env):
        result = call_action(mod.compliance_add_risk, conn, ns(
            company_id=env["company_id"],
            name=None,
        ))
        assert is_error(result)

    def test_invalid_category_fails(self, conn, env):
        result = call_action(mod.compliance_add_risk, conn, ns(
            company_id=env["company_id"],
            name="Bad Category Risk",
            category="cosmic",
        ))
        assert is_error(result)


class TestUpdateRisk:
    def test_update_likelihood(self, conn, env):
        result = call_action(mod.compliance_update_risk, conn, ns(
            risk_id=env["risk_id"],
            likelihood=5,
        ))
        assert is_ok(result), result
        assert "likelihood" in result["updated_fields"]
        assert "risk_score" in result["updated_fields"]
        assert "risk_level" in result["updated_fields"]

    def test_update_status(self, conn, env):
        result = call_action(mod.compliance_update_risk, conn, ns(
            risk_id=env["risk_id"],
            status="mitigating",
        ))
        assert is_ok(result), result
        assert "status" in result["updated_fields"]

    def test_update_residual_scores(self, conn, env):
        result = call_action(mod.compliance_update_risk, conn, ns(
            risk_id=env["risk_id"],
            residual_likelihood=2,
            residual_impact=2,
        ))
        assert is_ok(result), result
        assert "residual_likelihood" in result["updated_fields"]
        assert "residual_impact" in result["updated_fields"]
        assert "residual_score" in result["updated_fields"]

    def test_no_fields_fails(self, conn, env):
        result = call_action(mod.compliance_update_risk, conn, ns(
            risk_id=env["risk_id"],
        ))
        assert is_error(result)


class TestGetRisk:
    def test_get_existing(self, conn, env):
        result = call_action(mod.compliance_get_risk, conn, ns(
            risk_id=env["risk_id"],
        ))
        assert is_ok(result), result
        assert result["name"] == "Test Risk"
        assert "assessment_count" in result

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.compliance_get_risk, conn, ns(
            risk_id="nonexistent-id",
        ))
        assert is_error(result)


class TestListRisks:
    def test_list_by_company(self, conn, env):
        result = call_action(mod.compliance_list_risks, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_list_by_level(self, conn, env):
        result = call_action(mod.compliance_list_risks, conn, ns(
            company_id=env["company_id"],
            risk_level="medium",
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1


class TestAddRiskAssessment:
    def test_basic_assessment(self, conn, env):
        result = call_action(mod.compliance_add_risk_assessment, conn, ns(
            risk_id=env["risk_id"],
            company_id=env["company_id"],
            likelihood=4,
            impact=4,
            assessor="Risk Analyst",
            notes="Quarterly review",
        ))
        assert is_ok(result), result
        assert result["score"] == 16
        assert result["risk_level"] == "critical"

    def test_low_assessment(self, conn, env):
        result = call_action(mod.compliance_add_risk_assessment, conn, ns(
            risk_id=env["risk_id"],
            company_id=env["company_id"],
            likelihood=1,
            impact=1,
        ))
        assert is_ok(result), result
        assert result["score"] == 1
        assert result["risk_level"] == "low"

    def test_missing_risk_id_fails(self, conn, env):
        result = call_action(mod.compliance_add_risk_assessment, conn, ns(
            risk_id=None,
            company_id=env["company_id"],
        ))
        assert is_error(result)


class TestListRiskAssessments:
    def test_list_by_risk(self, conn, env):
        call_action(mod.compliance_add_risk_assessment, conn, ns(
            risk_id=env["risk_id"],
            company_id=env["company_id"],
            likelihood=3,
            impact=3,
        ))
        result = call_action(mod.compliance_list_risk_assessments, conn, ns(
            risk_id=env["risk_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1


class TestRiskMatrixReport:
    def test_basic_matrix(self, conn, env):
        result = call_action(mod.compliance_risk_matrix_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "matrix" in result
        assert "summary" in result
        assert result["total_active_risks"] >= 1
        # Matrix should have 25 cells (5x5)
        assert len(result["matrix"]) == 25

    def test_missing_company_fails(self, conn, env):
        result = call_action(mod.compliance_risk_matrix_report, conn, ns(
            company_id=None,
        ))
        assert is_error(result)


class TestCloseRisk:
    def test_close_risk(self, conn, env):
        result = call_action(mod.compliance_close_risk, conn, ns(
            risk_id=env["risk_id"],
        ))
        assert is_ok(result), result
        assert result["risk_status"] == "closed"

    def test_close_already_closed_fails(self, conn, env):
        call_action(mod.compliance_close_risk, conn, ns(
            risk_id=env["risk_id"],
        ))
        result = call_action(mod.compliance_close_risk, conn, ns(
            risk_id=env["risk_id"],
        ))
        assert is_error(result)


# =============================================================================
# Controls Domain
# =============================================================================

class TestAddControlTest:
    def test_basic_create(self, conn, env):
        result = call_action(mod.compliance_add_control_test, conn, ns(
            company_id=env["company_id"],
            control_name="Access Review",
            control_description="Review user access quarterly",
            control_type="detective",
            frequency="quarterly",
            tester="IT Security",
        ))
        assert is_ok(result), result
        assert result["control_name"] == "Access Review"
        assert result["test_result_status"] == "not_tested"
        assert "naming_series" in result

    def test_missing_name_fails(self, conn, env):
        result = call_action(mod.compliance_add_control_test, conn, ns(
            company_id=env["company_id"],
            control_name=None,
        ))
        assert is_error(result)

    def test_invalid_type_fails(self, conn, env):
        result = call_action(mod.compliance_add_control_test, conn, ns(
            company_id=env["company_id"],
            control_name="Bad Type",
            control_type="magical",
        ))
        assert is_error(result)


class TestUpdateControlTest:
    def _create(self, conn, env):
        result = call_action(mod.compliance_add_control_test, conn, ns(
            company_id=env["company_id"],
            control_name="Test Control",
        ))
        return result["id"]

    def test_update_description(self, conn, env):
        test_id = self._create(conn, env)
        result = call_action(mod.compliance_update_control_test, conn, ns(
            control_test_id=test_id,
            control_description="Updated description",
        ))
        assert is_ok(result), result
        assert "control_description" in result["updated_fields"]

    def test_update_frequency(self, conn, env):
        test_id = self._create(conn, env)
        result = call_action(mod.compliance_update_control_test, conn, ns(
            control_test_id=test_id,
            frequency="monthly",
        ))
        assert is_ok(result), result
        assert "frequency" in result["updated_fields"]


class TestGetControlTest:
    def test_get_existing(self, conn, env):
        r = call_action(mod.compliance_add_control_test, conn, ns(
            company_id=env["company_id"],
            control_name="Get Me",
        ))
        result = call_action(mod.compliance_get_control_test, conn, ns(
            control_test_id=r["id"],
        ))
        assert is_ok(result), result
        assert result["control_name"] == "Get Me"

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.compliance_get_control_test, conn, ns(
            control_test_id="nonexistent-id",
        ))
        assert is_error(result)


class TestListControlTests:
    def test_list_by_company(self, conn, env):
        call_action(mod.compliance_add_control_test, conn, ns(
            company_id=env["company_id"],
            control_name="Listable Control",
        ))
        result = call_action(mod.compliance_list_control_tests, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1


class TestExecuteControlTest:
    def _create(self, conn, env):
        result = call_action(mod.compliance_add_control_test, conn, ns(
            company_id=env["company_id"],
            control_name="Execute Me",
        ))
        return result["id"]

    def test_execute_effective(self, conn, env):
        test_id = self._create(conn, env)
        result = call_action(mod.compliance_execute_control_test, conn, ns(
            control_test_id=test_id,
            test_result="effective",
            tester="QA Lead",
            evidence="Reviewed logs, all clean",
        ))
        assert is_ok(result), result
        assert result["test_result_status"] == "effective"

    def test_execute_ineffective_with_deficiency(self, conn, env):
        test_id = self._create(conn, env)
        result = call_action(mod.compliance_execute_control_test, conn, ns(
            control_test_id=test_id,
            test_result="ineffective",
            deficiency_type="material_weakness",
            notes="Remediation plan: implement by Q3",
        ))
        assert is_ok(result), result
        assert result["test_result_status"] == "ineffective"

    def test_missing_result_fails(self, conn, env):
        test_id = self._create(conn, env)
        result = call_action(mod.compliance_execute_control_test, conn, ns(
            control_test_id=test_id,
            test_result=None,
        ))
        assert is_error(result)

    def test_invalid_result_fails(self, conn, env):
        test_id = self._create(conn, env)
        result = call_action(mod.compliance_execute_control_test, conn, ns(
            control_test_id=test_id,
            test_result="perfect",
        ))
        assert is_error(result)


# =============================================================================
# Calendar Domain
# =============================================================================

class TestAddCalendarItem:
    def test_basic_create(self, conn, env):
        result = call_action(mod.compliance_add_calendar_item, conn, ns(
            company_id=env["company_id"],
            title="Annual Tax Filing",
            compliance_type="filing",
            due_date="2026-04-15",
            responsible="CFO",
            recurrence="annual",
        ))
        assert is_ok(result), result
        assert result["title"] == "Annual Tax Filing"
        assert result["calendar_status"] == "upcoming"
        assert result["due_date"] == "2026-04-15"

    def test_missing_title_fails(self, conn, env):
        result = call_action(mod.compliance_add_calendar_item, conn, ns(
            company_id=env["company_id"],
            title=None,
            due_date="2026-04-15",
        ))
        assert is_error(result)

    def test_missing_due_date_fails(self, conn, env):
        result = call_action(mod.compliance_add_calendar_item, conn, ns(
            company_id=env["company_id"],
            title="No Date",
            due_date=None,
        ))
        assert is_error(result)


class TestUpdateCalendarItem:
    def _create(self, conn, env):
        result = call_action(mod.compliance_add_calendar_item, conn, ns(
            company_id=env["company_id"],
            title="Updatable Item",
            due_date="2026-06-01",
        ))
        return result["id"]

    def test_update_due_date(self, conn, env):
        item_id = self._create(conn, env)
        result = call_action(mod.compliance_update_calendar_item, conn, ns(
            calendar_item_id=item_id,
            due_date="2026-07-01",
        ))
        assert is_ok(result), result
        assert "due_date" in result["updated_fields"]

    def test_no_fields_fails(self, conn, env):
        item_id = self._create(conn, env)
        result = call_action(mod.compliance_update_calendar_item, conn, ns(
            calendar_item_id=item_id,
        ))
        assert is_error(result)


class TestGetCalendarItem:
    def test_get_existing(self, conn, env):
        r = call_action(mod.compliance_add_calendar_item, conn, ns(
            company_id=env["company_id"],
            title="Get Me Item",
            due_date="2026-05-01",
        ))
        result = call_action(mod.compliance_get_calendar_item, conn, ns(
            calendar_item_id=r["id"],
        ))
        assert is_ok(result), result
        assert result["title"] == "Get Me Item"

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.compliance_get_calendar_item, conn, ns(
            calendar_item_id="nonexistent-id",
        ))
        assert is_error(result)


class TestListCalendarItems:
    def test_list_by_company(self, conn, env):
        call_action(mod.compliance_add_calendar_item, conn, ns(
            company_id=env["company_id"],
            title="Listable Item",
            due_date="2026-08-01",
        ))
        result = call_action(mod.compliance_list_calendar_items, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1


class TestCompleteCalendarItem:
    def _create(self, conn, env):
        result = call_action(mod.compliance_add_calendar_item, conn, ns(
            company_id=env["company_id"],
            title="Complete Me",
            due_date="2026-04-01",
        ))
        return result["id"]

    def test_complete(self, conn, env):
        item_id = self._create(conn, env)
        result = call_action(mod.compliance_complete_calendar_item, conn, ns(
            calendar_item_id=item_id,
        ))
        assert is_ok(result), result
        assert result["calendar_status"] == "completed"
        assert "completed_date" in result

    def test_complete_already_completed_fails(self, conn, env):
        item_id = self._create(conn, env)
        call_action(mod.compliance_complete_calendar_item, conn, ns(
            calendar_item_id=item_id,
        ))
        result = call_action(mod.compliance_complete_calendar_item, conn, ns(
            calendar_item_id=item_id,
        ))
        assert is_error(result)


class TestOverdueItemsReport:
    def test_basic_report(self, conn, env):
        result = call_action(mod.compliance_overdue_items_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "overdue_calendar_items" in result
        assert "overdue_findings" in result
        assert "total_overdue" in result

    def test_report_with_overdue_item(self, conn, env):
        # Create an item with past due date
        call_action(mod.compliance_add_calendar_item, conn, ns(
            company_id=env["company_id"],
            title="Past Due Item",
            due_date="2020-01-01",
        ))
        result = call_action(mod.compliance_overdue_items_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["overdue_calendar_count"] >= 1


# ---------------------------------------------------------------------------
# compliance-dashboard depth helpers.
# ---------------------------------------------------------------------------
# NOTE (ledger): compliance-dashboard never reaches the ledger. It runs SELECTs
# only -- no INSERT/UPDATE, no audit() call, no commit -- so no two-leg/balance
# assertion can hold for it. The depth signal here is (a) exact agreement
# between the response buckets and independently-read stored rows, and
# (b) proof that the call wrote nothing.
# NOTE (money): the compliance tables hold no monetary columns (only counts,
# statuses and dates-as-TEXT), so no Decimal comparison applies anywhere below;
# every assertion below compares exact ints/strings, never float.
_DASHBOARD_TABLES = (
    "audit_plan",
    "audit_finding",
    "risk_register",
    "risk_assessment",
    "control_test",
    "compliance_calendar",
    "policy",
    "policy_acknowledgment",
)


def _seam_rows(conn, table_name, company_id):
    """Company-scoped rows read back through the seam (PyPika-built query)."""
    table = Table(table_name)
    sql = Q.from_(table).select(table.star).where(
        Field("company_id") == P()).get_sql()
    return [dict(row) for row in conn.execute(sql, (company_id,)).fetchall()]


def _seam_snapshot(conn):
    """Whole-table snapshots (id-ordered) for before/after identity checks."""
    snapshot = {}
    for name in _DASHBOARD_TABLES + ("audit_log",):
        table = Table(name)
        sql = Q.from_(table).select(table.star).orderby(table.id).get_sql()
        snapshot[name] = [tuple(row) for row in conn.execute(sql).fetchall()]
    return snapshot


class TestComplianceDashboard:
    def test_basic_dashboard(self, conn, env):
        cid = env["company_id"]
        before = _seam_snapshot(conn)
        result = call_action(mod.compliance_dashboard, conn, ns(
            company_id=cid,
        ))
        assert is_ok(result), result
        assert "audit_plans" in result
        assert "risks_by_level" in result
        assert "control_tests" in result
        assert "calendar_items" in result
        assert "policies" in result
        assert "overdue_items" in result
        assert "open_findings" in result
        # Behavioural depth: the fresh env seeds exactly one draft audit plan,
        # one identified medium risk and one draft policy, and nothing else, so
        # the buckets below pin exact stored-row state, not just key shape.
        assert result["company_id"] == cid
        assert result["report_date"] == date.today().isoformat()
        assert result["audit_plans"] == {"draft": 1}
        assert result["risks_by_level"] == {"medium": 1}
        assert result["policies"] == {"draft": 1}
        assert result["control_tests"] == {}
        assert result["calendar_items"] == {}
        assert result["overdue_items"] == 0
        assert result["open_findings"] == 0
        # Each bucket matches an independent seam read of the same rows.
        plans = _seam_rows(conn, "audit_plan", cid)
        assert result["audit_plans"] == dict(Counter(r["status"] for r in plans))
        risks = [r for r in _seam_rows(conn, "risk_register", cid)
                 if r["status"] != "closed"]
        assert result["risks_by_level"] == dict(
            Counter(r["risk_level"] for r in risks))
        assert result["control_tests"] == {}
        assert result["calendar_items"] == {}
        # Read-only proof: the dashboard changed no stored row, not even audit_log.
        assert _seam_snapshot(conn) == before

    def test_dashboard_reflects_seeded_state(self, conn, env):
        cid = env["company_id"]
        # Build a known cross-domain state through the module's own actions.
        second = call_action(mod.compliance_add_audit_plan, conn, ns(
            company_id=cid,
            name="SOX Walkthrough",
            audit_type="external",
        ))
        assert is_ok(second), second
        started = call_action(mod.compliance_start_audit, conn, ns(
            audit_plan_id=second["id"],
        ))
        assert is_ok(started), started

        high = call_action(mod.compliance_add_risk, conn, ns(
            company_id=cid,
            name="Vendor failure",
            category="operational",
            likelihood=4,
            impact=3,
        ))
        assert is_ok(high), high
        doomed = call_action(mod.compliance_add_risk, conn, ns(
            company_id=cid,
            name="Retired worry",
            category="financial",
            likelihood=5,
            impact=5,
        ))
        assert is_ok(doomed), doomed
        closed = call_action(mod.compliance_close_risk, conn, ns(
            risk_id=doomed["id"],
        ))
        assert is_ok(closed), closed

        ctrl_a = call_action(mod.compliance_add_control_test, conn, ns(
            company_id=cid,
            control_name="Access review",
            control_type="detective",
        ))
        assert is_ok(ctrl_a), ctrl_a
        ctrl_b = call_action(mod.compliance_add_control_test, conn, ns(
            company_id=cid,
            control_name="Backup check",
            control_type="preventive",
        ))
        assert is_ok(ctrl_b), ctrl_b
        executed = call_action(mod.compliance_execute_control_test, conn, ns(
            control_test_id=ctrl_a["id"],
            test_result="effective",
        ))
        assert is_ok(executed), executed

        past = call_action(mod.compliance_add_calendar_item, conn, ns(
            company_id=cid,
            title="Overdue filing",
            due_date="2020-01-01",
        ))
        assert is_ok(past), past
        future = call_action(mod.compliance_add_calendar_item, conn, ns(
            company_id=cid,
            title="Next filing",
            due_date="2030-01-01",
        ))
        assert is_ok(future), future
        done_item = call_action(mod.compliance_add_calendar_item, conn, ns(
            company_id=cid,
            title="Filed already",
            due_date="2020-02-01",
        ))
        assert is_ok(done_item), done_item
        completed = call_action(mod.compliance_complete_calendar_item, conn, ns(
            calendar_item_id=done_item["id"],
        ))
        assert is_ok(completed), completed

        finding_open = call_action(mod.compliance_add_audit_finding, conn, ns(
            audit_plan_id=env["audit_plan_id"],
            company_id=cid,
            title="Missing sign-off",
            finding_type="major",
            remediation_due="2030-06-01",
        ))
        assert is_ok(finding_open), finding_open
        finding_late = call_action(mod.compliance_add_audit_finding, conn, ns(
            audit_plan_id=env["audit_plan_id"],
            company_id=cid,
            title="Late remediation",
            finding_type="minor",
            remediation_due="2020-03-01",
        ))
        assert is_ok(finding_late), finding_late

        extra_policy = call_action(mod.compliance_add_policy, conn, ns(
            company_id=cid,
            title="Travel Policy",
        ))
        assert is_ok(extra_policy), extra_policy
        published = call_action(mod.compliance_publish_policy, conn, ns(
            policy_id=extra_policy["id"],
        ))
        assert is_ok(published), published

        # A second company whose rows must NOT leak into this dashboard.
        other_cid = seed_company(conn)
        seed_naming_series(conn, other_cid)
        seed_audit_plan(conn, other_cid, name="Other Co Audit")
        call_action(mod.compliance_add_calendar_item, conn, ns(
            company_id=other_cid,
            title="Other Co Item",
            due_date="2020-01-01",
        ))

        before = _seam_snapshot(conn)
        result = call_action(mod.compliance_dashboard, conn, ns(
            company_id=cid,
        ))
        assert is_ok(result), result

        # Exact pinned values for the seeded edges: the closed critical risk is
        # excluded, and overdue_items counts calendar rows only, so the past-due
        # finding leaves it at 1.
        assert result["audit_plans"] == {"draft": 1, "in_progress": 1}
        assert result["risks_by_level"] == {"medium": 1, "high": 1}
        assert result["control_tests"] == {"not_tested": 1, "effective": 1}
        assert result["calendar_items"] == {"upcoming": 2, "completed": 1}
        assert result["policies"] == {"draft": 1, "published": 1}
        assert result["overdue_items"] == 1
        assert result["open_findings"] == 2

        # Full agreement with independent seam reads of the same stored rows.
        plans = _seam_rows(conn, "audit_plan", cid)
        assert result["audit_plans"] == dict(Counter(r["status"] for r in plans))
        risks = [r for r in _seam_rows(conn, "risk_register", cid)
                 if r["status"] != "closed"]
        assert result["risks_by_level"] == dict(
            Counter(r["risk_level"] for r in risks))
        ctrls = _seam_rows(conn, "control_test", cid)
        assert result["control_tests"] == dict(
            Counter(r["test_result"] for r in ctrls))
        cal = _seam_rows(conn, "compliance_calendar", cid)
        assert result["calendar_items"] == dict(Counter(r["status"] for r in cal))
        today = date.today().isoformat()
        assert result["overdue_items"] == sum(
            1 for r in cal
            if r["status"] not in ("completed", "waived")
            and (r["due_date"] or "") < today)
        findings = _seam_rows(conn, "audit_finding", cid)
        assert result["open_findings"] == sum(
            1 for r in findings
            if r["remediation_status"] in ("open", "in_progress", "overdue"))
        assert result["report_date"] == today
        assert result["company_id"] == cid

        # Read-only proof: the dashboard changed no stored row, not even audit_log.
        assert _seam_snapshot(conn) == before

    def test_dashboard_refusal_keeps_db_identical(self, conn, env):
        cid = env["company_id"]
        before = _seam_snapshot(conn)
        refused = call_action(mod.compliance_dashboard, conn, ns(
            company_id=None,
        ))
        assert is_error(refused), refused
        assert "--company-id" in refused.get("message", "")
        ghost = "00000000-0000-0000-0000-000000000000"
        missing = call_action(mod.compliance_dashboard, conn, ns(
            company_id=ghost,
        ))
        assert is_error(missing), missing
        assert "not found" in missing.get("message", "")
        assert ghost in missing.get("message", "")
        # Neither refusal half-wrote: every table is byte-identical.
        assert _seam_snapshot(conn) == before
        # The database still answers afterwards.
        result = call_action(mod.compliance_dashboard, conn, ns(
            company_id=cid,
        ))
        assert is_ok(result), result
        assert result["company_id"] == cid


# =============================================================================
# Policy Domain
# =============================================================================

class TestAddPolicy:
    def test_basic_create(self, conn, env):
        result = call_action(mod.compliance_add_policy, conn, ns(
            company_id=env["company_id"],
            title="Information Security Policy",
            policy_type="it",
            content="All systems must be secured.",
            owner="CISO",
        ))
        assert is_ok(result), result
        assert result["title"] == "Information Security Policy"
        assert result["policy_status"] == "draft"
        assert "naming_series" in result

    def test_with_acknowledgment(self, conn, env):
        result = call_action(mod.compliance_add_policy, conn, ns(
            company_id=env["company_id"],
            title="Code of Conduct",
            requires_acknowledgment="1",
        ))
        assert is_ok(result), result

    def test_missing_title_fails(self, conn, env):
        result = call_action(mod.compliance_add_policy, conn, ns(
            company_id=env["company_id"],
            title=None,
        ))
        assert is_error(result)

    def test_invalid_type_fails(self, conn, env):
        result = call_action(mod.compliance_add_policy, conn, ns(
            company_id=env["company_id"],
            title="Bad Type",
            policy_type="cosmic",
        ))
        assert is_error(result)


class TestUpdatePolicy:
    def test_update_content(self, conn, env):
        result = call_action(mod.compliance_update_policy, conn, ns(
            policy_id=env["policy_id"],
            content="Updated policy content.",
        ))
        assert is_ok(result), result
        assert "content" in result["updated_fields"]

    def test_no_fields_fails(self, conn, env):
        result = call_action(mod.compliance_update_policy, conn, ns(
            policy_id=env["policy_id"],
        ))
        assert is_error(result)


class TestGetPolicy:
    def test_get_existing(self, conn, env):
        result = call_action(mod.compliance_get_policy, conn, ns(
            policy_id=env["policy_id"],
        ))
        assert is_ok(result), result
        assert result["title"] == "Test Policy"
        assert "acknowledgment_count" in result

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.compliance_get_policy, conn, ns(
            policy_id="nonexistent-id",
        ))
        assert is_error(result)


class TestListPolicies:
    def test_list_by_company(self, conn, env):
        result = call_action(mod.compliance_list_policies, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_list_by_type(self, conn, env):
        result = call_action(mod.compliance_list_policies, conn, ns(
            company_id=env["company_id"],
            policy_type="general",
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1


class TestPublishPolicy:
    def test_publish(self, conn, env):
        result = call_action(mod.compliance_publish_policy, conn, ns(
            policy_id=env["policy_id"],
            effective_date="2026-04-01",
        ))
        assert is_ok(result), result
        assert result["policy_status"] == "published"
        assert result["effective_date"] == "2026-04-01"

    def test_publish_already_published_fails(self, conn, env):
        call_action(mod.compliance_publish_policy, conn, ns(
            policy_id=env["policy_id"],
        ))
        result = call_action(mod.compliance_publish_policy, conn, ns(
            policy_id=env["policy_id"],
        ))
        assert is_error(result)


class TestRetirePolicy:
    def test_retire(self, conn, env):
        result = call_action(mod.compliance_retire_policy, conn, ns(
            policy_id=env["policy_id"],
        ))
        assert is_ok(result), result
        assert result["policy_status"] == "retired"

    def test_retire_already_retired_fails(self, conn, env):
        call_action(mod.compliance_retire_policy, conn, ns(
            policy_id=env["policy_id"],
        ))
        result = call_action(mod.compliance_retire_policy, conn, ns(
            policy_id=env["policy_id"],
        ))
        assert is_error(result)

    def test_publish_retired_fails(self, conn, env):
        call_action(mod.compliance_retire_policy, conn, ns(
            policy_id=env["policy_id"],
        ))
        result = call_action(mod.compliance_publish_policy, conn, ns(
            policy_id=env["policy_id"],
        ))
        assert is_error(result)


class TestAddPolicyAcknowledgment:
    def test_basic_ack(self, conn, env):
        result = call_action(mod.compliance_add_policy_acknowledgment, conn, ns(
            policy_id=env["policy_id"],
            company_id=env["company_id"],
            employee_name="Jane Auditor",
            employee_id=env["employee_id"],
            ip_address="192.168.1.1",
        ))
        assert is_ok(result), result
        assert result["employee_name"] == "Jane Auditor"
        assert result["policy_id"] == env["policy_id"]

    def test_missing_employee_name_fails(self, conn, env):
        result = call_action(mod.compliance_add_policy_acknowledgment, conn, ns(
            policy_id=env["policy_id"],
            company_id=env["company_id"],
            employee_name=None,
        ))
        assert is_error(result)


class TestListPolicyAcknowledgments:
    def test_list_by_policy(self, conn, env):
        call_action(mod.compliance_add_policy_acknowledgment, conn, ns(
            policy_id=env["policy_id"],
            company_id=env["company_id"],
            employee_name="Emp A",
        ))
        result = call_action(mod.compliance_list_policy_acknowledgments, conn, ns(
            policy_id=env["policy_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1


class TestPolicyComplianceReport:
    def test_basic_report(self, conn, env):
        # Publish with ack requirement
        pol = call_action(mod.compliance_add_policy, conn, ns(
            company_id=env["company_id"],
            title="Ack Policy",
            requires_acknowledgment="1",
        ))
        call_action(mod.compliance_publish_policy, conn, ns(
            policy_id=pol["id"],
        ))
        result = call_action(mod.compliance_policy_compliance_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "policies" in result
        assert result["total_policies_requiring_ack"] >= 1
        assert "total_employees" in result


# =============================================================================
# Status
# =============================================================================

class TestStatus:
    def test_status(self, conn, env):
        result = call_action(mod.status, conn, ns())
        assert is_ok(result), result
        assert result["skill"] == "erpclaw-compliance"
        assert result["actions_available"] == 38
        assert "audit" in result["domains"]
        assert "risk" in result["domains"]
        assert "controls" in result["domains"]
        assert "policy" in result["domains"]
