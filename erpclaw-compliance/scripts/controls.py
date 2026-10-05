"""ERPClaw Compliance -- controls domain module

Actions for control tests and compliance calendar (2 tables, 12 actions).
Imported by db_query.py (unified router).
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone, date

try:
    import importlib.util
    if importlib.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Case, Field, LiteralValue, Order, P, Q, Table, dynamic_update, fn, insert_row, now as sql_now, update_row
except ImportError:
    pass

SKILL = "erpclaw-compliance"

# Register naming prefixes
ENTITY_PREFIXES.setdefault("control_test", "CTRL-")
ENTITY_PREFIXES.setdefault("compliance_calendar", "CCAL-")

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
_today_iso = lambda: date.today().isoformat()

VALID_CONTROL_TYPES = ("preventive", "detective", "corrective", "compensating")
VALID_FREQUENCIES = ("continuous", "daily", "weekly", "monthly", "quarterly", "semi_annual", "annual")
VALID_TEST_RESULTS = ("not_tested", "effective", "ineffective", "partially_effective", "not_applicable")
VALID_DEFICIENCY_TYPES = ("significant", "material_weakness", "control_deficiency")
VALID_COMPLIANCE_TYPES = ("filing", "certification", "renewal", "inspection", "report", "training", "other")
VALID_RECURRENCES = ("none", "monthly", "quarterly", "semi_annual", "annual")
VALID_CALENDAR_STATUSES = ("upcoming", "in_progress", "completed", "overdue", "waived")
VALID_FRAMEWORKS = ("hipaa", "ferpa", "general")
ATTESTATION_DISCLAIMER = (
    "This is a records-readiness report based on recorded control tests, "
    "policies, acknowledgments, and evidence references. It is not legal "
    "certification of HIPAA, FERPA, or any compliance posture, "
    "nor an external auditor opinion."
)


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _validate_enum(value, valid_values, field_name):
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")


# ===========================================================================
# CONTROL TEST ACTIONS
# ===========================================================================

# ---------------------------------------------------------------------------
# 1. add-control-test
# ---------------------------------------------------------------------------
def add_control_test(conn, args):
    _validate_company(conn, args.company_id)

    control_name = getattr(args, "control_name", None)
    if not control_name:
        err("--control-name is required")

    control_type = getattr(args, "control_type", None) or "preventive"
    _validate_enum(control_type, VALID_CONTROL_TYPES, "control-type")

    frequency = getattr(args, "frequency", None) or "quarterly"
    _validate_enum(frequency, VALID_FREQUENCIES, "frequency")

    test_id = str(uuid.uuid4())
    naming = get_next_name(conn, "control_test", company_id=args.company_id)
    now = _now_iso()
    sql, _ = insert_row("control_test", {
        "id": P(), "naming_series": P(), "control_name": P(),
        "control_description": P(), "control_type": P(), "frequency": P(),
        "test_date": P(), "tester": P(), "test_procedure": P(),
        "test_result": P(), "evidence": P(), "deficiency_type": P(),
        "remediation_plan": P(), "next_test_date": P(),
        "company_id": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        test_id, naming, control_name,
        getattr(args, "control_description", None),
        control_type, frequency,
        getattr(args, "test_date", None) or _today_iso(),
        getattr(args, "tester", None),
        getattr(args, "test_procedure", None),
        "not_tested",
        getattr(args, "evidence", None),
        None,  # deficiency_type
        None,  # remediation_plan
        getattr(args, "next_test_date", None),
        args.company_id, now, now,
    ))
    audit(conn, SKILL, "compliance-add-control-test", "control_test", test_id)
    conn.commit()
    ok({
        "id": test_id, "naming_series": naming,
        "control_name": control_name, "test_result_status": "not_tested",
    })


# ---------------------------------------------------------------------------
# 2. update-control-test
# ---------------------------------------------------------------------------
def update_control_test(conn, args):
    test_id = getattr(args, "control_test_id", None)
    if not test_id:
        err("--control-test-id is required")
    if not conn.execute(Q.from_(Table("control_test")).select(Field('id')).where(Field("id") == P()).get_sql(), (test_id,)).fetchone():
        err(f"Control test {test_id} not found")

    data, changed = {}, []
    for arg_name, col_name in {
        "control_name": "control_name",
        "control_description": "control_description",
        "test_procedure": "test_procedure",
        "tester": "tester",
        "evidence": "evidence",
        "next_test_date": "next_test_date",
        "notes": "notes",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            data[col_name] = val
            changed.append(col_name)

    control_type = getattr(args, "control_type", None)
    if control_type is not None:
        _validate_enum(control_type, VALID_CONTROL_TYPES, "control-type")
        data["control_type"] = control_type
        changed.append("control_type")

    frequency = getattr(args, "frequency", None)
    if frequency is not None:
        _validate_enum(frequency, VALID_FREQUENCIES, "frequency")
        data["frequency"] = frequency
        changed.append("frequency")

    if not changed:
        err("No fields to update")

    data["updated_at"] = sql_now()
    sql, params = dynamic_update("control_test", data, {"id": test_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "compliance-update-control-test", "control_test", test_id, new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": test_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# 3. get-control-test
# ---------------------------------------------------------------------------
def get_control_test(conn, args):
    test_id = getattr(args, "control_test_id", None)
    if not test_id:
        err("--control-test-id is required")
    row = conn.execute(Q.from_(Table("control_test")).select(Table("control_test").star).where(Field("id") == P()).get_sql(), (test_id,)).fetchone()
    if not row:
        err(f"Control test {test_id} not found")
    ok(row_to_dict(row))


# ---------------------------------------------------------------------------
# 4. list-control-tests
# ---------------------------------------------------------------------------
def list_control_tests(conn, args):
    t = Table("control_test")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star))
    params = []

    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "control_type", None):
        q = q.where(t.control_type == P())
        q_cnt = q_cnt.where(t.control_type == P())
        params.append(args.control_type)
    if getattr(args, "test_result", None):
        q = q.where(t.test_result == P())
        q_cnt = q_cnt.where(t.test_result == P())
        params.append(args.test_result)
    if getattr(args, "frequency", None):
        q = q.where(t.frequency == P())
        q_cnt = q_cnt.where(t.frequency == P())
        params.append(args.frequency)
    if getattr(args, "search", None):
        like = LiteralValue("?")
        crit = (t.control_name.like(like)) | (t.control_description.like(like))
        q = q.where(crit)
        q_cnt = q_cnt.where(crit)
        params.extend([f"%{args.search}%", f"%{args.search}%"])

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]
    params.extend([args.limit, args.offset])
    q = q.orderby(t.test_date, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ---------------------------------------------------------------------------
# 5. execute-control-test
# ---------------------------------------------------------------------------
def execute_control_test(conn, args):
    test_id = getattr(args, "control_test_id", None)
    if not test_id:
        err("--control-test-id is required")
    if not conn.execute(Q.from_(Table("control_test")).select(Field('id')).where(Field("id") == P()).get_sql(), (test_id,)).fetchone():
        err(f"Control test {test_id} not found")

    test_result = getattr(args, "test_result", None)
    if not test_result:
        err("--test-result is required")
    _validate_enum(test_result, VALID_TEST_RESULTS, "test-result")

    now = _now_iso()
    upd_data = {
        "test_result": test_result,
        "test_date": _today_iso(),
        "updated_at": now,
    }

    tester = getattr(args, "tester", None)
    if tester:
        upd_data["tester"] = tester

    evidence = getattr(args, "evidence", None)
    if evidence:
        upd_data["evidence"] = evidence

    # Set deficiency_type if result is ineffective
    deficiency_type = getattr(args, "deficiency_type", None)
    if deficiency_type:
        _validate_enum(deficiency_type, VALID_DEFICIENCY_TYPES, "deficiency-type")
        upd_data["deficiency_type"] = deficiency_type

    notes = getattr(args, "notes", None)
    if notes:
        upd_data["remediation_plan"] = notes

    sql, params = dynamic_update("control_test", upd_data, {"id": test_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "compliance-execute-control-test", "control_test", test_id, new_values={"test_result": test_result})
    conn.commit()
    ok({"id": test_id, "test_result_status": test_result, "test_date": _today_iso()})


# ===========================================================================
# COMPLIANCE CALENDAR ACTIONS
# ===========================================================================

# ---------------------------------------------------------------------------
# 6. add-calendar-item
# ---------------------------------------------------------------------------
def add_calendar_item(conn, args):
    _validate_company(conn, args.company_id)

    title = getattr(args, "title", None)
    if not title:
        err("--title is required")

    compliance_type = getattr(args, "compliance_type", None) or "filing"
    _validate_enum(compliance_type, VALID_COMPLIANCE_TYPES, "compliance-type")

    due_date = getattr(args, "due_date", None)
    if not due_date:
        err("--due-date is required")

    recurrence = getattr(args, "recurrence", None)
    if recurrence:
        _validate_enum(recurrence, VALID_RECURRENCES, "recurrence")

    item_id = str(uuid.uuid4())
    naming = get_next_name(conn, "compliance_calendar", company_id=args.company_id)
    now = _now_iso()
    sql, _ = insert_row("compliance_calendar", {
        "id": P(), "title": P(), "compliance_type": P(), "due_date": P(),
        "reminder_days": P(), "responsible": P(), "description": P(),
        "recurrence": P(), "status": P(), "notes": P(),
        "company_id": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        item_id, title, compliance_type, due_date,
        int(getattr(args, "reminder_days", None) or 30),
        getattr(args, "responsible", None),
        getattr(args, "description", None),
        recurrence,
        "upcoming",
        getattr(args, "notes", None),
        args.company_id, now, now,
    ))
    audit(conn, SKILL, "compliance-add-calendar-item", "compliance_calendar", item_id)
    conn.commit()
    ok({"id": item_id, "title": title, "due_date": due_date, "calendar_status": "upcoming"})


# ---------------------------------------------------------------------------
# 7. update-calendar-item
# ---------------------------------------------------------------------------
def update_calendar_item(conn, args):
    item_id = getattr(args, "calendar_item_id", None)
    if not item_id:
        err("--calendar-item-id is required")
    if not conn.execute(Q.from_(Table("compliance_calendar")).select(Field('id')).where(Field("id") == P()).get_sql(), (item_id,)).fetchone():
        err(f"Calendar item {item_id} not found")

    data, changed = {}, []
    for arg_name, col_name in {
        "title": "title",
        "due_date": "due_date",
        "responsible": "responsible",
        "description": "description",
        "notes": "notes",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            data[col_name] = val
            changed.append(col_name)

    compliance_type = getattr(args, "compliance_type", None)
    if compliance_type is not None:
        _validate_enum(compliance_type, VALID_COMPLIANCE_TYPES, "compliance-type")
        data["compliance_type"] = compliance_type
        changed.append("compliance_type")

    recurrence = getattr(args, "recurrence", None)
    if recurrence is not None:
        _validate_enum(recurrence, VALID_RECURRENCES, "recurrence")
        data["recurrence"] = recurrence
        changed.append("recurrence")

    reminder_days = getattr(args, "reminder_days", None)
    if reminder_days is not None:
        data["reminder_days"] = int(reminder_days)
        changed.append("reminder_days")

    if not changed:
        err("No fields to update")

    data["updated_at"] = sql_now()
    sql, params = dynamic_update("compliance_calendar", data, {"id": item_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "compliance-update-calendar-item", "compliance_calendar", item_id, new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": item_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# 8. get-calendar-item
# ---------------------------------------------------------------------------
def get_calendar_item(conn, args):
    item_id = getattr(args, "calendar_item_id", None)
    if not item_id:
        err("--calendar-item-id is required")
    row = conn.execute(Q.from_(Table("compliance_calendar")).select(Table("compliance_calendar").star).where(Field("id") == P()).get_sql(), (item_id,)).fetchone()
    if not row:
        err(f"Calendar item {item_id} not found")
    ok(row_to_dict(row))


# ---------------------------------------------------------------------------
# 9. list-calendar-items
# ---------------------------------------------------------------------------
def list_calendar_items(conn, args):
    t = Table("compliance_calendar")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star))
    params = []

    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "compliance_type", None):
        q = q.where(t.compliance_type == P())
        q_cnt = q_cnt.where(t.compliance_type == P())
        params.append(args.compliance_type)
    if getattr(args, "status", None):
        q = q.where(t.status == P())
        q_cnt = q_cnt.where(t.status == P())
        params.append(args.status)
    if getattr(args, "search", None):
        like = LiteralValue("?")
        crit = (t.title.like(like)) | (t.description.like(like))
        q = q.where(crit)
        q_cnt = q_cnt.where(crit)
        params.extend([f"%{args.search}%", f"%{args.search}%"])

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]
    params.extend([args.limit, args.offset])
    q = q.orderby(t.due_date).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ---------------------------------------------------------------------------
# 10. complete-calendar-item
# ---------------------------------------------------------------------------
def complete_calendar_item(conn, args):
    item_id = getattr(args, "calendar_item_id", None)
    if not item_id:
        err("--calendar-item-id is required")
    row = conn.execute(Q.from_(Table("compliance_calendar")).select(Field('status')).where(Field("id") == P()).get_sql(), (item_id,)).fetchone()
    if not row:
        err(f"Calendar item {item_id} not found")
    if row[0] == "completed":
        err("Calendar item is already completed")

    now = _now_iso()
    sql = update_row("compliance_calendar",
                     data={"status": P(), "completed_date": P(), "updated_at": P()},
                     where={"id": P()})
    conn.execute(sql, ("completed", _today_iso(), now, item_id))
    audit(conn, SKILL, "compliance-complete-calendar-item", "compliance_calendar", item_id)
    conn.commit()
    ok({"id": item_id, "calendar_status": "completed", "completed_date": _today_iso()})


# ---------------------------------------------------------------------------
# 11. overdue-items-report
# ---------------------------------------------------------------------------
def overdue_items_report(conn, args):
    _validate_company(conn, args.company_id)

    today = _today_iso()

    # Find overdue calendar items
    overdue_calendar = conn.execute("""
        SELECT * FROM compliance_calendar
        WHERE company_id = ? AND status NOT IN ('completed', 'waived') AND due_date < ?
        ORDER BY due_date ASC
    """, (args.company_id, today)).fetchall()

    # Find overdue audit findings
    overdue_findings = conn.execute("""
        SELECT * FROM audit_finding
        WHERE company_id = ? AND remediation_status NOT IN ('remediated', 'verified', 'accepted')
          AND remediation_due IS NOT NULL AND remediation_due < ?
        ORDER BY remediation_due ASC
    """, (args.company_id, today)).fetchall()

    ok({
        "company_id": args.company_id,
        "report_date": today,
        "overdue_calendar_items": [row_to_dict(r) for r in overdue_calendar],
        "overdue_calendar_count": len(overdue_calendar),
        "overdue_findings": [row_to_dict(r) for r in overdue_findings],
        "overdue_findings_count": len(overdue_findings),
        "total_overdue": len(overdue_calendar) + len(overdue_findings),
    })


# ---------------------------------------------------------------------------
# 12. compliance-dashboard
# ---------------------------------------------------------------------------
def compliance_dashboard(conn, args):
    _validate_company(conn, args.company_id)

    today = _today_iso()

    # Audit plan summary
    audit_plans = conn.execute("""
        SELECT status, COUNT(*) FROM audit_plan
        WHERE company_id = ? GROUP BY status
    """, (args.company_id,)).fetchall()

    # Risk summary
    risks = conn.execute("""
        SELECT risk_level, COUNT(*) FROM risk_register
        WHERE company_id = ? AND status != 'closed' GROUP BY risk_level
    """, (args.company_id,)).fetchall()

    # Control test summary
    controls = conn.execute("""
        SELECT test_result, COUNT(*) FROM control_test
        WHERE company_id = ? GROUP BY test_result
    """, (args.company_id,)).fetchall()

    # Calendar summary
    calendar = conn.execute("""
        SELECT status, COUNT(*) FROM compliance_calendar
        WHERE company_id = ? GROUP BY status
    """, (args.company_id,)).fetchall()

    # Overdue count
    overdue_count = conn.execute("""
        SELECT COUNT(*) FROM compliance_calendar
        WHERE company_id = ? AND status NOT IN ('completed', 'waived') AND due_date < ?
    """, (args.company_id, today)).fetchone()[0]

    # Open findings count
    open_findings = conn.execute("""
        SELECT COUNT(*) FROM audit_finding
        WHERE company_id = ? AND remediation_status IN ('open', 'in_progress', 'overdue')
    """, (args.company_id,)).fetchone()[0]

    # Policy summary
    policies = conn.execute("""
        SELECT status, COUNT(*) FROM policy
        WHERE company_id = ? GROUP BY status
    """, (args.company_id,)).fetchall()

    ok({
        "company_id": args.company_id,
        "report_date": today,
        "audit_plans": {r[0]: r[1] for r in audit_plans},
        "risks_by_level": {r[0]: r[1] for r in risks},
        "control_tests": {r[0]: r[1] for r in controls},
        "calendar_items": {r[0]: r[1] for r in calendar},
        "policies": {r[0]: r[1] for r in policies},
        "overdue_items": overdue_count,
        "open_findings": open_findings,
    })


# ---------------------------------------------------------------------------
# 13. attestation-report (read-only)
# ---------------------------------------------------------------------------
def compliance_attestation_report(conn, args):
    company_id = getattr(args, "company_id", None)
    framework = getattr(args, "framework", None)
    as_of_raw = getattr(args, "as_of_date", None)
    _validate_company(conn, company_id)
    if not framework:
        err("--framework is required")
    if framework not in VALID_FRAMEWORKS:
        err("Invalid framework: %s. Must be one of: %s" % (framework, ", ".join(VALID_FRAMEWORKS)))
    if as_of_raw is None or (isinstance(as_of_raw, str) and as_of_raw.strip() == ""):
        as_of_date = _today_iso()
    else:
        try:
            as_of_date = date.fromisoformat(str(as_of_raw).strip()).isoformat()
        except ValueError:
            try:
                as_of_date = datetime.fromisoformat(str(as_of_raw).strip()).date().isoformat()
            except ValueError:
                err("Invalid --as-of-date: %s. Use YYYY-MM-DD" % (as_of_raw,))
    ct = Table("control_test")
    q = Q.from_(ct).select(ct.id, ct.control_name, ct.test_result, ct.test_date, ct.tester, ct.evidence).where(ct.company_id == P()).orderby(ct.control_name).orderby(ct.id)
    control_rows = conn.execute(q.get_sql(), (company_id,)).fetchall()
    pol = Table("policy")
    qp = Q.from_(pol).select(pol.id, pol.title, pol.requires_acknowledgment).where(pol.company_id == P()).where(pol.status == P()).orderby(pol.title).orderby(pol.id)
    policy_rows = conn.execute(qp.get_sql(), (company_id, "published")).fetchall()
    emp = Table("employee")
    qe = Q.from_(emp).select(emp.id, emp.full_name).where(emp.company_id == P()).orderby(emp.id)
    employee_rows = conn.execute(qe.get_sql(), (company_id,)).fetchall()
    ack = Table("policy_acknowledgment")
    qa = Q.from_(ack).select(ack.policy_id, ack.employee_id, ack.employee_name).where(ack.company_id == P())
    ack_rows = conn.execute(qa.get_sql(), (company_id,)).fetchall()
    controls_by_result = {}
    for r in control_rows:
        key = r[2] if r[2] else "not_tested"
        controls_by_result[key] = controls_by_result.get(key, 0) + 1
    def _missing_evidence(value):
        return value is None or (isinstance(value, str) and value.strip() == "")
    controls_missing_evidence = 0
    for r in control_rows:
        if _missing_evidence(r[5]):
            controls_missing_evidence += 1
    tested_rows = [r for r in control_rows if (r[2] if r[2] else "not_tested") != "not_tested"]
    tested_missing = 0
    for r in tested_rows:
        if _missing_evidence(r[5]):
            tested_missing += 1
    has_ineffective = False
    for r in tested_rows:
        if r[2] == "ineffective":
            has_ineffective = True
            break
    required_policy_ids = []
    for r in policy_rows:
        try:
            flag = int(r[2] or 0)
        except (TypeError, ValueError):
            flag = 1 if str(r[2]) == "1" else 0
        if flag == 1:
            required_policy_ids.append(r[0])
    required_set = set(required_policy_ids)
    employees = [(r[0], r[1] if r[1] else "") for r in employee_rows]
    filtered_acks = [r for r in ack_rows if r[0] in required_set]
    acked_id_set = set()
    acked_name_set = set()
    for r in filtered_acks:
        pid = r[0]
        eid = r[1]
        ename = (r[2] or "").strip() if r[2] else ""
        if eid:
            acked_id_set.add((pid, eid))
        if ename:
            acked_name_set.add((pid, ename))
    missing_pairs = []
    for pid in required_policy_ids:
        for eid, full in employees:
            if (pid, eid) in acked_id_set:
                continue
            if (pid, (full or "").strip()) in acked_name_set:
                continue
            missing_pairs.append({"policy_id": pid, "employee_id": eid, "employee_name": full})
    acknowledgment_gaps = len(missing_pairs)
    if required_policy_ids and employees:
        employees_required = len(employees)
    elif required_policy_ids:
        employees_required = 0
    else:
        employees_required = 0
    required_acknowledgments = len(required_policy_ids) * len(employees)
    acknowledgments_recorded = len(filtered_acks)
    gap_codes = []
    if not tested_rows:
        gap_codes.append("no_tested_controls")
    if tested_missing > 0:
        gap_codes.append("missing_control_evidence")
    if has_ineffective:
        gap_codes.append("ineffective_control_present")
    if acknowledgment_gaps > 0:
        gap_codes.append("missing_policy_acknowledgment")
    if tested_rows and not gap_codes:
        readiness = "evidence_complete"
    else:
        readiness = "gaps_present"
        if not gap_codes:
            gap_codes.append("no_tested_controls")
    evidence_rows = []
    for r in control_rows:
        evidence_rows.append({
            "control_test_id": r[0],
            "id": r[0],
            "control_name": r[1],
            "test_result": r[2],
            "result": r[2],
            "test_date": r[3],
            "tester": r[4],
            "evidence_reference": r[5],
            "evidence": r[5],
        })
    ok({
        "company_id": company_id,
        "framework": framework,
        "as_of_date": as_of_date,
        "report_date": as_of_date,
        "readiness": readiness,
        "readiness_status": readiness,
        "gap_codes": gap_codes,
        "gaps": gap_codes,
        "controls_by_result": dict(controls_by_result),
        "controls_by_test_result": dict(controls_by_result),
        "control_tests_by_result": dict(controls_by_result),
        "total_controls": len(control_rows),
        "tested_controls": len(tested_rows),
        "controls_tested": len(tested_rows),
        "controls_missing_evidence": controls_missing_evidence,
        "missing_evidence_count": controls_missing_evidence,
        "active_policies": len(policy_rows),
        "active_policy_count": len(policy_rows),
        "policies_requiring_acknowledgment": len(required_policy_ids),
        "total_employees": len(employees),
        "employees_required_to_acknowledge": employees_required,
        "employees_required": employees_required,
        "required_acknowledgments": required_acknowledgments,
        "acknowledgments_recorded": acknowledgments_recorded,
        "acknowledgment_count": acknowledgments_recorded,
        "acknowledgment_gaps": acknowledgment_gaps,
        "missing_acknowledgment_count": acknowledgment_gaps,
        "evidence": evidence_rows,
        "evidence_rows": evidence_rows,
        "disclaimer": ATTESTATION_DISCLAIMER,
    })


# ---------------------------------------------------------------------------
# Action Router
# ---------------------------------------------------------------------------
ACTIONS = {
    "compliance-add-control-test": add_control_test,
    "compliance-update-control-test": update_control_test,
    "compliance-get-control-test": get_control_test,
    "compliance-list-control-tests": list_control_tests,
    "compliance-execute-control-test": execute_control_test,
    "compliance-add-calendar-item": add_calendar_item,
    "compliance-update-calendar-item": update_calendar_item,
    "compliance-get-calendar-item": get_calendar_item,
    "compliance-list-calendar-items": list_calendar_items,
    "compliance-complete-calendar-item": complete_calendar_item,
    "compliance-overdue-items-report": overdue_items_report,
    "compliance-dashboard": compliance_dashboard,
    "compliance-attestation-report": compliance_attestation_report,
}
