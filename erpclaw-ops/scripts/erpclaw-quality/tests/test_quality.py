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

from decimal import Decimal, ROUND_HALF_UP

from quality_helpers import (
    load_db_query, call_action, ns, is_ok, is_error, _uuid,
)
from erpclaw_lib.query import Field, P, Q, Table, fn

M = load_db_query()

_SNAPSHOT_TABLES = (
    "quality_inspection",
    "quality_inspection_reading",
    "quality_inspection_template",
    "quality_inspection_parameter",
    "non_conformance",
    "quality_goal",
    "naming_series",
    "audit_log",
    "gl_entry",
    "stock_ledger_entry",
)


def _snapshot(conn):
    snap = {}
    for name in _SNAPSHOT_TABLES:
        tbl = Table(name)
        rows = conn.execute(Q.from_(tbl).select(tbl.star).get_sql()).fetchall()
        snap[name] = sorted(
            tuple(sorted((key, str(value)) for key, value in dict(row).items()))
            for row in rows
        )
    return snap


def _row(conn, table, row_id):
    tbl = Table(table)
    q = Q.from_(tbl).select(tbl.star).where(Field("id") == P())
    row = conn.execute(q.get_sql(), (row_id,)).fetchone()
    assert row is not None, "expected a row in %s with id %s" % (table, row_id)
    return dict(row)


def _count(conn, table):
    tbl = Table(table)
    q = Q.from_(tbl).select(fn.Count("*").as_("cnt"))
    return conn.execute(q.get_sql()).fetchone()["cnt"]


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
            severity="major",
            company_id=env["company_id"],
        ))
        assert is_ok(add_r), add_r
        nc_id = add_r["non_conformance"]["id"]

        before = _row(conn, "non_conformance", nc_id)
        assert before["description"] == "Dimension out of spec"
        assert before["severity"] == "major"
        assert before["status"] == "open"
        assert before["root_cause"] is None
        naming = before["naming_series"]

        decoy_r = call_action(M.add_non_conformance, conn, ns(
            description="Untouched decoy",
            company_id=env["company_id"],
        ))
        assert is_ok(decoy_r), decoy_r
        decoy_before = _row(conn, "non_conformance",
                            decoy_r["non_conformance"]["id"])

        r = call_action(M.update_non_conformance, conn, ns(
            non_conformance_id=nc_id,
            root_cause="Worn tooling",
            corrective_action="Replace tool",
            status="investigating",
        ))
        assert is_ok(r), r

        # No ledger legs by design: a non-conformance update stores quality
        # fields only, so the books must stay empty rather than balance.
        assert _count(conn, "gl_entry") == 0
        assert _count(conn, "stock_ledger_entry") == 0

        after = _row(conn, "non_conformance", nc_id)
        assert after["root_cause"] == "Worn tooling"
        assert after["corrective_action"] == "Replace tool"
        assert after["status"] == "investigating"
        # The envelope mirrors the store.
        assert r["non_conformance"]["root_cause"] == after["root_cause"]
        assert r["non_conformance"]["status"] == after["status"]
        # Columns the call did not name keep their exact values.
        assert after["description"] == "Dimension out of spec"
        assert after["severity"] == "major"
        assert after["naming_series"] == naming
        assert after["preventive_action"] is None
        assert after["updated_at"] is not None
        # The decoy row is byte-identical.
        assert _row(conn, "non_conformance",
                    decoy_r["non_conformance"]["id"]) == decoy_before

    def test_update_nc_not_found(self, conn, env):
        missing = _uuid()
        before = _snapshot(conn)
        r = call_action(M.update_non_conformance, conn, ns(
            non_conformance_id=missing,
            root_cause="Test",
        ))
        assert is_error(r)
        assert r["message"] == "Non-conformance %s not found" % missing
        assert _snapshot(conn) == before


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
        single = json.dumps([{"parameter_name": "Weight",
                              "parameter_type": "numeric",
                              "min_value": "0", "max_value": "10"}])
        t1 = call_action(M.add_inspection_template, conn, ns(
            name="Weight check",
            inspection_type="incoming",
            parameters=single,
        ))
        assert is_ok(t1), t1
        weight_id = t1["template"]["parameters"][0]["id"]

        pair = json.dumps([
            {"parameter_name": "Length", "parameter_type": "numeric",
             "min_value": "0", "max_value": "10"},
            {"parameter_name": "Finish", "parameter_type": "non_numeric",
             "acceptance_value": "smooth"},
        ])
        t2 = call_action(M.add_inspection_template, conn, ns(
            name="Length and finish",
            inspection_type="incoming",
            parameters=pair,
        ))
        assert is_ok(t2), t2
        length_id = t2["template"]["parameters"][0]["id"]
        finish_id = t2["template"]["parameters"][1]["id"]

        def add_inspection(template_id):
            added = call_action(M.add_quality_inspection, conn, ns(
                item_id=env["item_id"],
                inspection_type="incoming",
                inspection_date="2026-03-10",
                template_id=template_id,
                company_id=env["company_id"],
            ))
            assert is_ok(added), added
            return added["inspection"]["id"]

        accepted_id = add_inspection(t1["template"]["id"])
        rejected_id = add_inspection(t1["template"]["id"])
        mixed_id = add_inspection(t2["template"]["id"])

        def record(inspection_id, values):
            recorded = call_action(M.record_inspection_readings, conn, ns(
                quality_inspection_id=inspection_id,
                readings=json.dumps(values),
            ))
            assert is_ok(recorded), recorded

        record(accepted_id, [{"parameter_id": weight_id,
                              "reading_value": "5"}])
        record(rejected_id, [{"parameter_id": weight_id,
                              "reading_value": "99"}])
        record(mixed_id, [{"parameter_id": length_id,
                           "reading_value": "5"},
                          {"parameter_id": finish_id,
                           "reading_value": "rough"}])

        for inspection_id in (accepted_id, rejected_id, mixed_id):
            evaluated = call_action(M.evaluate_inspection, conn, ns(
                quality_inspection_id=inspection_id,
            ))
            assert is_ok(evaluated), evaluated

        n1 = call_action(M.add_non_conformance, conn, ns(
            description="Scratch",
            severity="major",
            company_id=env["company_id"],
        ))
        n2 = call_action(M.add_non_conformance, conn, ns(
            description="Dent",
            severity="minor",
            company_id=env["company_id"],
        ))
        n3 = call_action(M.add_non_conformance, conn, ns(
            description="Crack",
            severity="critical",
            company_id=env["company_id"],
        ))
        assert is_ok(n1), n1
        assert is_ok(n2), n2
        assert is_ok(n3), n3
        closed = call_action(M.update_non_conformance, conn, ns(
            non_conformance_id=n3["non_conformance"]["id"],
            status="resolved",
            resolution_date="2026-03-11",
        ))
        assert is_ok(closed), closed

        g1 = call_action(M.add_quality_goal, conn, ns(
            name="Cut scrap",
            target_value="2",
        ))
        g2 = call_action(M.add_quality_goal, conn, ns(
            name="On-time checks",
            target_value="99",
        ))
        assert is_ok(g1), g1
        assert is_ok(g2), g2
        risk = call_action(M.update_quality_goal, conn, ns(
            quality_goal_id=g2["quality_goal"]["id"],
            status="at_risk",
        ))
        assert is_ok(risk), risk

        before = _snapshot(conn)
        r = call_action(M.quality_dashboard, conn, ns())
        assert is_ok(r), r
        dash = r["dashboard"]

        assert dash["inspections"]["total"] == 3
        assert dash["inspections"]["by_status"] == {
            "accepted": 1, "rejected": 1, "partially_accepted": 1}
        expected_rate = str((Decimal(1) / Decimal(3) * Decimal(100)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP))
        assert expected_rate == "33.33"
        assert dash["inspections"]["pass_rate_pct"] == expected_rate

        assert dash["non_conformances"]["total_open"] == 2
        assert dash["non_conformances"]["by_severity"] == {
            "major": 1, "minor": 1}

        assert dash["quality_goals"]["total"] == 2
        assert dash["quality_goals"]["by_status"] == {
            "on_track": 1, "at_risk": 1}

        # Read-only: the aggregate stores nothing, not even an audit row.
        assert _snapshot(conn) == before


# ===================================================================
# Status
# ===================================================================

class TestStatus:
    def test_status_ok(self, conn, env):
        r = call_action(M.status, conn, ns())
        assert is_ok(r)
