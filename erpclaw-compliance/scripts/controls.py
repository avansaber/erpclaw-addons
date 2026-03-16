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
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, LiteralValue, Case, insert_row, update_row, dynamic_update
except ImportError:
    pass

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
    audit(conn, "control_test", test_id, "compliance-add-control-test", args.company_id)
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

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("control_test", data, {"id": test_id})
    conn.execute(sql, params)
    audit(conn, "control_test", test_id, "compliance-update-control-test", None, {"updated_fields": changed})
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
    audit(conn, "control_test", test_id, "compliance-execute-control-test", None, {"test_result": test_result})
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
    audit(conn, "compliance_calendar", item_id, "compliance-add-calendar-item", args.company_id)
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

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("compliance_calendar", data, {"id": item_id})
    conn.execute(sql, params)
    audit(conn, "compliance_calendar", item_id, "compliance-update-calendar-item", None, {"updated_fields": changed})
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
    audit(conn, "compliance_calendar", item_id, "compliance-complete-calendar-item", None)
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
}
