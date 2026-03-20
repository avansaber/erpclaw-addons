"""L1 tests for ERPClaw Quality skill (14 actions).

Tests cover: inspection templates, quality inspections, non-conformances,
quality goals, and the quality dashboard.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from quality_helpers import (
    load_db_query, call_action, ns, is_ok, is_error, _uuid,
)

M = load_db_query()


# ===================================================================
# Inspection Templates
# ===================================================================

class TestAddInspectionTemplate:
    def test_add_template_ok(self, conn, env):
        r = call_action(M.add_inspection_template, conn, ns(
            name="Incoming Raw Material",
            inspection_type="incoming",
        ))
        assert is_ok(r)
        assert r["template"]["id"]
        assert r["template"]["name"] == "Incoming Raw Material"

    def test_add_template_with_parameters(self, conn, env):
        params_json = json.dumps([
            {"parameter_name": "Thickness", "parameter_type": "numeric",
             "min_value": "0.5", "max_value": "1.5", "uom": "mm"},
            {"parameter_name": "Color Check", "parameter_type": "non_numeric",
             "acceptance_value": "pass"},
        ])
        r = call_action(M.add_inspection_template, conn, ns(
            name="Dimensional Check",
            inspection_type="in_process",
            parameters=params_json,
        ))
        assert is_ok(r)
        assert len(r["template"]["parameters"]) == 2

    def test_add_template_missing_name(self, conn, env):
        r = call_action(M.add_inspection_template, conn, ns(
            inspection_type="incoming",
        ))
        assert is_error(r)

    def test_add_template_missing_type(self, conn, env):
        r = call_action(M.add_inspection_template, conn, ns(
            name="No Type",
        ))
        assert is_error(r)

    def test_add_template_invalid_type(self, conn, env):
        r = call_action(M.add_inspection_template, conn, ns(
            name="Bad Type",
            inspection_type="invalid",
        ))
        assert is_error(r)


class TestGetInspectionTemplate:
    def test_get_template_ok(self, conn, env):
        add_r = call_action(M.add_inspection_template, conn, ns(
            name="Get Template Test",
            inspection_type="outgoing",
        ))
        tid = add_r["template"]["id"]

        r = call_action(M.get_inspection_template, conn, ns(template_id=tid))
        assert is_ok(r)
        assert r["template"]["id"] == tid

    def test_get_template_not_found(self, conn, env):
        r = call_action(M.get_inspection_template, conn, ns(template_id=_uuid()))
        assert is_error(r)


class TestListInspectionTemplates:
    def test_list_templates_empty(self, conn, env):
        r = call_action(M.list_inspection_templates, conn, ns())
        assert is_ok(r)
        assert r["templates"] == []


# ===================================================================
# Quality Inspections
# ===================================================================

class TestAddQualityInspection:
    def test_add_inspection_ok(self, conn, env):
        r = call_action(M.add_quality_inspection, conn, ns(
            item_id=env["item_id"],
            inspection_type="incoming",
            inspection_date="2026-03-10",
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["inspection"]["id"]
        assert r["inspection"]["status"] == "accepted"

    def test_add_inspection_with_template(self, conn, env):
        params_json = json.dumps([
            {"parameter_name": "Weight", "parameter_type": "numeric",
             "min_value": "90", "max_value": "110"},
        ])
        tmpl_r = call_action(M.add_inspection_template, conn, ns(
            name="Weight Check",
            inspection_type="incoming",
            parameters=params_json,
        ))
        tmpl_id = tmpl_r["template"]["id"]

        r = call_action(M.add_quality_inspection, conn, ns(
            item_id=env["item_id"],
            inspection_type="incoming",
            inspection_date="2026-03-10",
            template_id=tmpl_id,
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert len(r["inspection"]["readings"]) == 1

    def test_add_inspection_missing_item(self, conn, env):
        r = call_action(M.add_quality_inspection, conn, ns(
            inspection_type="incoming",
            inspection_date="2026-03-10",
            company_id=env["company_id"],
        ))
        assert is_error(r)

    def test_add_inspection_invalid_type(self, conn, env):
        r = call_action(M.add_quality_inspection, conn, ns(
            item_id=env["item_id"],
            inspection_type="invalid",
            inspection_date="2026-03-10",
            company_id=env["company_id"],
        ))
        assert is_error(r)


class TestListQualityInspections:
    def test_list_inspections(self, conn, env):
        call_action(M.add_quality_inspection, conn, ns(
            item_id=env["item_id"],
            inspection_type="incoming",
            inspection_date="2026-03-10",
            company_id=env["company_id"],
        ))
        r = call_action(M.list_quality_inspections, conn, ns())
        assert is_ok(r)
        assert r["total"] >= 1


# ===================================================================
# Non-Conformances
# ===================================================================

class TestAddNonConformance:
    def test_add_nc_ok(self, conn, env):
        r = call_action(M.add_non_conformance, conn, ns(
            description="Surface defect found",
            severity="major",
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["non_conformance"]["id"]
        assert r["non_conformance"]["status"] == "open"

    def test_add_nc_missing_description(self, conn, env):
        r = call_action(M.add_non_conformance, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_error(r)


class TestUpdateNonConformance:
    def test_update_nc_ok(self, conn, env):
        add_r = call_action(M.add_non_conformance, conn, ns(
            description="Dimension out of spec",
            company_id=env["company_id"],
        ))
        nc_id = add_r["non_conformance"]["id"]

        r = call_action(M.update_non_conformance, conn, ns(
            non_conformance_id=nc_id,
            root_cause="Worn tooling",
            corrective_action="Replace tool",
        ))
        assert is_ok(r)

    def test_update_nc_not_found(self, conn, env):
        r = call_action(M.update_non_conformance, conn, ns(
            non_conformance_id=_uuid(),
            root_cause="Test",
        ))
        assert is_error(r)


class TestListNonConformances:
    def test_list_ncs_empty(self, conn, env):
        r = call_action(M.list_non_conformances, conn, ns())
        assert is_ok(r)
        assert r["total"] == 0


# ===================================================================
# Quality Goals
# ===================================================================

class TestAddQualityGoal:
    def test_add_goal_ok(self, conn, env):
        r = call_action(M.add_quality_goal, conn, ns(
            name="Reduce defect rate",
            target_value="2",
        ))
        assert is_ok(r)

    def test_add_goal_missing_name(self, conn, env):
        r = call_action(M.add_quality_goal, conn, ns(
            target_value="5",
        ))
        assert is_error(r)

    def test_add_goal_missing_target(self, conn, env):
        r = call_action(M.add_quality_goal, conn, ns(
            name="No Target Goal",
        ))
        assert is_error(r)


# ===================================================================
# Dashboard
# ===================================================================

class TestQualityDashboard:
    def test_dashboard_ok(self, conn, env):
        r = call_action(M.quality_dashboard, conn, ns())
        assert is_ok(r)


# ===================================================================
# Status
# ===================================================================

class TestStatus:
    def test_status_ok(self, conn, env):
        r = call_action(M.status, conn, ns())
        assert is_ok(r)
