"""Audit rows name the record (m708): every compliance audit row carries
(skill, action, entity_type, entity_id) = ("erpclaw-compliance", <action>,
<table>, <record id>) so a record's history is found by its id."""
import json

from compliance_helpers import call_action, is_ok, load_db_query, ns

from erpclaw_lib.query import P, Q, Table

mod = load_db_query()

COMPLIANCE_TABLES = ("policy", "policy_acknowledgment", "risk_register",
                     "risk_assessment", "control_test", "compliance_calendar",
                     "audit_plan", "audit_finding")


def _audit_rows_by_entity(conn, entity_id):
    t = Table("audit_log")
    q = Q.from_(t).select(
        t.skill, t.action, t.entity_type, t.entity_id, t.new_values
    ).where(t.entity_id == P())
    return conn.execute(q.get_sql(), (entity_id,)).fetchall()


def _assert_single(conn, entity_id, skill, action, entity_type):
    rows = _audit_rows_by_entity(conn, entity_id)
    matches = [r for r in rows
               if r[0] == skill and r[1] == action and r[2] == entity_type]
    assert len(matches) == 1, (
        f"expected exactly one audit_log row {skill, action, entity_type} "
        f"for entity {entity_id}, found {len(matches)} of {len(rows)}")
    return matches[0]


def _drive_policy(conn, env):
    cid = env["company_id"]
    pol = call_action(mod.compliance_add_policy, conn, ns(
        company_id=cid,
        title="Audit Trail Policy",
    ))
    assert is_ok(pol), pol
    pid = pol["id"]
    upd = call_action(mod.compliance_update_policy, conn, ns(
        policy_id=pid,
        title="Audit Trail Policy v2",
        content="Updated content.",
    ))
    assert is_ok(upd), upd
    pub = call_action(mod.compliance_publish_policy, conn, ns(
        policy_id=pid,
    ))
    assert is_ok(pub), pub
    ack = call_action(mod.compliance_add_policy_acknowledgment, conn, ns(
        policy_id=pid,
        company_id=cid,
        employee_name="Audit Employee",
    ))
    assert is_ok(ack), ack
    ret = call_action(mod.compliance_retire_policy, conn, ns(
        policy_id=pid,
    ))
    assert is_ok(ret), ret
    return {"policy_id": pid, "ack_id": ack["id"]}


def _drive_risk(conn, env):
    cid = env["company_id"]
    risk = call_action(mod.compliance_add_risk, conn, ns(
        company_id=cid,
        name="Audit Trail Risk",
    ))
    assert is_ok(risk), risk
    rid = risk["id"]
    upd = call_action(mod.compliance_update_risk, conn, ns(
        risk_id=rid,
        owner="Audit Owner",
    ))
    assert is_ok(upd), upd
    assess = call_action(mod.compliance_add_risk_assessment, conn, ns(
        risk_id=rid,
        company_id=cid,
        likelihood=4,
        impact=4,
        assessor="Auditor",
    ))
    assert is_ok(assess), assess
    close = call_action(mod.compliance_close_risk, conn, ns(
        risk_id=rid,
    ))
    assert is_ok(close), close
    return {"risk_id": rid, "assessment_id": assess["id"]}


def _drive_controls(conn, env):
    cid = env["company_id"]
    ctrl = call_action(mod.compliance_add_control_test, conn, ns(
        company_id=cid,
        control_name="Audit Trail Control",
    ))
    assert is_ok(ctrl), ctrl
    tid = ctrl["id"]
    upd = call_action(mod.compliance_update_control_test, conn, ns(
        control_test_id=tid,
        control_description="Updated description",
    ))
    assert is_ok(upd), upd
    exe = call_action(mod.compliance_execute_control_test, conn, ns(
        control_test_id=tid,
        test_result="effective",
    ))
    assert is_ok(exe), exe
    item = call_action(mod.compliance_add_calendar_item, conn, ns(
        company_id=cid,
        title="Audit Trail Item",
        due_date="2026-06-01",
    ))
    assert is_ok(item), item
    iid = item["id"]
    upd2 = call_action(mod.compliance_update_calendar_item, conn, ns(
        calendar_item_id=iid,
        title="Audit Trail Item v2",
    ))
    assert is_ok(upd2), upd2
    done = call_action(mod.compliance_complete_calendar_item, conn, ns(
        calendar_item_id=iid,
    ))
    assert is_ok(done), done
    return {"control_test_id": tid, "calendar_item_id": iid}


def _drive_audit(conn, env):
    cid = env["company_id"]
    plan = call_action(mod.compliance_add_audit_plan, conn, ns(
        company_id=cid,
        name="Audit Trail Plan",
    ))
    assert is_ok(plan), plan
    pid = plan["id"]
    upd = call_action(mod.compliance_update_audit_plan, conn, ns(
        audit_plan_id=pid,
        scope="Updated scope",
    ))
    assert is_ok(upd), upd
    start = call_action(mod.compliance_start_audit, conn, ns(
        audit_plan_id=pid,
    ))
    assert is_ok(start), start
    done = call_action(mod.compliance_complete_audit, conn, ns(
        audit_plan_id=pid,
    ))
    assert is_ok(done), done
    finding = call_action(mod.compliance_add_audit_finding, conn, ns(
        audit_plan_id=pid,
        company_id=cid,
        title="Audit Trail Finding",
        finding_type="major",
    ))
    assert is_ok(finding), finding
    return {"audit_plan_id": pid, "finding_id": finding["id"]}


def test_policy_audit_rows_name_the_record(conn, env):
    ids = _drive_policy(conn, env)
    _assert_single(conn, ids["policy_id"], "erpclaw-compliance",
                   "compliance-add-policy", "policy")
    upd_row = _assert_single(conn, ids["policy_id"], "erpclaw-compliance",
                             "compliance-update-policy", "policy")
    _assert_single(conn, ids["policy_id"], "erpclaw-compliance",
                   "compliance-publish-policy", "policy")
    _assert_single(conn, ids["policy_id"], "erpclaw-compliance",
                   "compliance-retire-policy", "policy")
    _assert_single(conn, ids["ack_id"], "erpclaw-compliance",
                   "compliance-add-policy-acknowledgment",
                   "policy_acknowledgment")
    assert json.loads(upd_row[4]) == {"updated_fields": ["title", "content"]}


def test_risk_audit_rows_name_the_record(conn, env):
    ids = _drive_risk(conn, env)
    _assert_single(conn, ids["risk_id"], "erpclaw-compliance",
                   "compliance-add-risk", "risk_register")
    _assert_single(conn, ids["risk_id"], "erpclaw-compliance",
                   "compliance-update-risk", "risk_register")
    _assert_single(conn, ids["assessment_id"], "erpclaw-compliance",
                   "compliance-add-risk-assessment", "risk_assessment")
    _assert_single(conn, ids["risk_id"], "erpclaw-compliance",
                   "compliance-close-risk", "risk_register")


def test_controls_audit_rows_name_the_record(conn, env):
    ids = _drive_controls(conn, env)
    _assert_single(conn, ids["control_test_id"], "erpclaw-compliance",
                   "compliance-add-control-test", "control_test")
    _assert_single(conn, ids["control_test_id"], "erpclaw-compliance",
                   "compliance-update-control-test", "control_test")
    _assert_single(conn, ids["control_test_id"], "erpclaw-compliance",
                   "compliance-execute-control-test", "control_test")
    _assert_single(conn, ids["calendar_item_id"], "erpclaw-compliance",
                   "compliance-add-calendar-item", "compliance_calendar")
    _assert_single(conn, ids["calendar_item_id"], "erpclaw-compliance",
                   "compliance-update-calendar-item", "compliance_calendar")
    _assert_single(conn, ids["calendar_item_id"], "erpclaw-compliance",
                   "compliance-complete-calendar-item", "compliance_calendar")


def test_audit_audit_rows_name_the_record(conn, env):
    ids = _drive_audit(conn, env)
    _assert_single(conn, ids["audit_plan_id"], "erpclaw-compliance",
                   "compliance-add-audit-plan", "audit_plan")
    _assert_single(conn, ids["audit_plan_id"], "erpclaw-compliance",
                   "compliance-update-audit-plan", "audit_plan")
    _assert_single(conn, ids["audit_plan_id"], "erpclaw-compliance",
                   "compliance-start-audit", "audit_plan")
    _assert_single(conn, ids["audit_plan_id"], "erpclaw-compliance",
                   "compliance-complete-audit", "audit_plan")
    _assert_single(conn, ids["finding_id"], "erpclaw-compliance",
                   "compliance-add-audit-finding", "audit_finding")


def test_no_audit_row_is_keyed_by_company(conn, env):
    _drive_policy(conn, env)
    _drive_risk(conn, env)
    _drive_controls(conn, env)
    _drive_audit(conn, env)
    cid = env["company_id"]
    t = Table("audit_log")
    q = Q.from_(t).select(t.skill, t.action, t.entity_type, t.entity_id)
    rows = conn.execute(q.get_sql()).fetchall()
    mine = [r for r in rows if r[0] == "erpclaw-compliance"]
    assert mine, "expected audit rows written by erpclaw-compliance"
    assert all(r[3] != cid for r in mine), "audit row keyed by company id"
    assert all(r[0] not in COMPLIANCE_TABLES for r in rows), \
        "audit row carries a table name as skill"
    assert all(r[1].startswith("compliance-") for r in mine), \
        "compliance audit row action missing compliance- prefix"
