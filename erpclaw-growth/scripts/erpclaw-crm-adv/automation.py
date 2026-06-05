"""erpclaw-crm-adv -- automation domain module

Actions for marketing automation workflows, lead scoring, nurture sequences, and drip sequences (6 tables, 17 actions).
Imported by db_query.py (unified router).
"""
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row

    ENTITY_PREFIXES.setdefault("crmadv_automation_workflow", "AWFL-")
    ENTITY_PREFIXES.setdefault("crmadv_nurture_sequence", "ANUR-")
except ImportError:
    pass

SKILL = "erpclaw-crm-adv"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_WORKFLOW_STATUSES = ("active", "inactive", "paused")
VALID_SEQUENCE_STATUSES = ("draft", "active", "paused", "completed")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _parse_json_fields(d, *fields):
    """Parse JSON string fields into Python objects for get/list."""
    for f in fields:
        if f in d and isinstance(d[f], str):
            try:
                d[f] = json.loads(d[f])
            except (json.JSONDecodeError, TypeError):
                pass
    return d


# ===========================================================================
# 1. add-automation-workflow
# ===========================================================================
def add_automation_workflow(conn, args):
    _validate_company(conn, args.company_id)
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    wf_id = str(uuid.uuid4())
    now = _now_iso()
    conn.company_id = args.company_id
    naming = get_next_name(conn, "crmadv_automation_workflow")

    conditions_json = getattr(args, "conditions_json", None) or "{}"
    actions_json = getattr(args, "actions_json", None) or "[]"

    # Validate JSON
    try:
        json.loads(conditions_json)
    except (json.JSONDecodeError, TypeError):
        err("Invalid JSON in --conditions-json")
    try:
        json.loads(actions_json)
    except (json.JSONDecodeError, TypeError):
        err("Invalid JSON in --actions-json")

    conn.execute("""
        INSERT INTO crmadv_automation_workflow (
            id, naming_series, name, trigger_event, conditions_json, actions_json,
            workflow_status, execution_count, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        wf_id, naming, name,
        getattr(args, "trigger_event", None),
        conditions_json, actions_json,
        "inactive", 0,
        args.company_id, now, now,
    ))
    audit(conn, SKILL, "add-automation-workflow", "crmadv_automation_workflow", wf_id,
          new_values={"name": name})
    conn.commit()
    ok({"id": wf_id, "naming_series": naming, "name": name, "workflow_status": "inactive"})


# ===========================================================================
# 2. update-automation-workflow
# ===========================================================================
def update_automation_workflow(conn, args):
    wf_id = getattr(args, "workflow_id", None)
    if not wf_id:
        err("--workflow-id is required")
    if not conn.execute(Q.from_(Table("crmadv_automation_workflow")).select(Field('id')).where(Field("id") == P()).get_sql(), (wf_id,)).fetchone():
        err(f"Automation workflow {wf_id} not found")

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "name": "name", "trigger_event": "trigger_event",
        "conditions_json": "conditions_json", "actions_json": "actions_json",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            # Validate JSON fields
            if arg_name in ("conditions_json", "actions_json"):
                try:
                    json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    err(f"Invalid JSON in --{arg_name.replace('_', '-')}")
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    if not updates:
        err("No fields to update")

    updates.append("updated_at = ?")
    params.append(_now_iso())
    params.append(wf_id)
    conn.execute(f"UPDATE crmadv_automation_workflow SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, SKILL, "update-automation-workflow", "crmadv_automation_workflow", wf_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": wf_id, "updated_fields": changed})


# ===========================================================================
# 3. list-automation-workflows
# ===========================================================================
def list_automation_workflows(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "workflow_status_filter", None):
        where.append("workflow_status = ?")
        params.append(args.workflow_status_filter)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM crmadv_automation_workflow WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM crmadv_automation_workflow WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    result_rows = []
    for r in rows:
        d = row_to_dict(r)
        _parse_json_fields(d, "conditions_json", "actions_json")
        result_rows.append(d)
    ok({
        "rows": result_rows,
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 4. activate-workflow
# ===========================================================================
def activate_workflow(conn, args):
    wf_id = getattr(args, "workflow_id", None)
    if not wf_id:
        err("--workflow-id is required")
    row = conn.execute(Q.from_(Table("crmadv_automation_workflow")).select(Table("crmadv_automation_workflow").star).where(Field("id") == P()).get_sql(), (wf_id,)).fetchone()
    if not row:
        err(f"Automation workflow {wf_id} not found")

    d = row_to_dict(row)
    if d["workflow_status"] == "active":
        err("Workflow is already active")

    now = _now_iso()
    conn.execute("""
        UPDATE crmadv_automation_workflow
        SET workflow_status = 'active', updated_at = ?
        WHERE id = ?
    """, (now, wf_id))
    audit(conn, SKILL, "activate-workflow", "crmadv_automation_workflow", wf_id,
          new_values={"workflow_status": "active"})
    conn.commit()
    ok({"id": wf_id, "workflow_status": "active"})


# ===========================================================================
# 5. deactivate-workflow
# ===========================================================================
def deactivate_workflow(conn, args):
    wf_id = getattr(args, "workflow_id", None)
    if not wf_id:
        err("--workflow-id is required")
    row = conn.execute(Q.from_(Table("crmadv_automation_workflow")).select(Table("crmadv_automation_workflow").star).where(Field("id") == P()).get_sql(), (wf_id,)).fetchone()
    if not row:
        err(f"Automation workflow {wf_id} not found")

    d = row_to_dict(row)
    if d["workflow_status"] == "inactive":
        err("Workflow is already inactive")

    now = _now_iso()
    conn.execute("""
        UPDATE crmadv_automation_workflow
        SET workflow_status = 'inactive', updated_at = ?
        WHERE id = ?
    """, (now, wf_id))
    audit(conn, SKILL, "deactivate-workflow", "crmadv_automation_workflow", wf_id,
          new_values={"workflow_status": "inactive"})
    conn.commit()
    ok({"id": wf_id, "workflow_status": "inactive"})


# ===========================================================================
# 6. add-lead-score-rule
# ===========================================================================
def add_lead_score_rule(conn, args):
    _validate_company(conn, args.company_id)
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    criteria_json = getattr(args, "criteria_json", None)
    if not criteria_json:
        err("--criteria-json is required")
    try:
        json.loads(criteria_json)
    except (json.JSONDecodeError, TypeError):
        err("Invalid JSON in --criteria-json")

    points_raw = getattr(args, "points", None)
    if points_raw is None:
        err("--points is required")
    try:
        points = int(points_raw)
    except (ValueError, TypeError):
        err("--points must be an integer")

    rule_id = str(uuid.uuid4())
    now = _now_iso()

    conn.execute("""
        INSERT INTO crmadv_lead_score_rule (
            id, name, criteria_json, points, is_active,
            company_id, created_at
        ) VALUES (?,?,?,?,?,?,?)
    """, (
        rule_id, name, criteria_json, points,
        1, args.company_id, now,
    ))
    audit(conn, SKILL, "add-lead-score-rule", "crmadv_lead_score_rule", rule_id,
          new_values={"name": name, "points": points})
    conn.commit()
    ok({"id": rule_id, "name": name, "points": points})


# ===========================================================================
# 7. list-lead-score-rules
# ===========================================================================
def list_lead_score_rules(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM crmadv_lead_score_rule WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM crmadv_lead_score_rule WHERE {where_sql} ORDER BY points DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    result_rows = []
    for r in rows:
        d = row_to_dict(r)
        _parse_json_fields(d, "criteria_json")
        result_rows.append(d)
    ok({
        "rows": result_rows,
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 8. add-nurture-sequence
# ===========================================================================
def add_nurture_sequence(conn, args):
    _validate_company(conn, args.company_id)
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    ns_id = str(uuid.uuid4())
    now = _now_iso()
    conn.company_id = args.company_id
    naming = get_next_name(conn, "crmadv_nurture_sequence")

    steps_json = getattr(args, "steps_json", None) or "[]"
    try:
        steps = json.loads(steps_json)
    except (json.JSONDecodeError, TypeError):
        err("Invalid JSON in --steps-json")
        steps = []  # unreachable but satisfies linter

    total_steps = len(steps) if isinstance(steps, list) else 0

    conn.execute("""
        INSERT INTO crmadv_nurture_sequence (
            id, naming_series, name, description, steps_json, total_steps,
            sequence_status, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        ns_id, naming, name,
        getattr(args, "description", None),
        steps_json, total_steps,
        "draft", args.company_id, now, now,
    ))
    audit(conn, SKILL, "add-nurture-sequence", "crmadv_nurture_sequence", ns_id,
          new_values={"name": name, "total_steps": total_steps})
    conn.commit()
    ok({"id": ns_id, "naming_series": naming, "name": name,
        "total_steps": total_steps, "sequence_status": "draft"})


# ===========================================================================
# 9. list-nurture-sequences
# ===========================================================================
def list_nurture_sequences(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM crmadv_nurture_sequence WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM crmadv_nurture_sequence WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    result_rows = []
    for r in rows:
        d = row_to_dict(r)
        _parse_json_fields(d, "steps_json")
        result_rows.append(d)
    ok({
        "rows": result_rows,
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 10. automation-performance-report
# ===========================================================================
def automation_performance_report(conn, args):
    _validate_company(conn, args.company_id)

    total_workflows = conn.execute(
        "SELECT COUNT(*) FROM crmadv_automation_workflow WHERE company_id = ?",
        (args.company_id,)
    ).fetchone()[0]
    active_workflows = conn.execute(
        "SELECT COUNT(*) FROM crmadv_automation_workflow WHERE company_id = ? AND workflow_status = 'active'",
        (args.company_id,)
    ).fetchone()[0]
    total_executions = conn.execute(
        "SELECT COALESCE(SUM(execution_count), 0) FROM crmadv_automation_workflow WHERE company_id = ?",
        (args.company_id,)
    ).fetchone()[0]

    total_rules = conn.execute(
        "SELECT COUNT(*) FROM crmadv_lead_score_rule WHERE company_id = ? AND is_active = 1",
        (args.company_id,)
    ).fetchone()[0]

    total_sequences = conn.execute(
        "SELECT COUNT(*) FROM crmadv_nurture_sequence WHERE company_id = ?",
        (args.company_id,)
    ).fetchone()[0]
    active_sequences = conn.execute(
        "SELECT COUNT(*) FROM crmadv_nurture_sequence WHERE company_id = ? AND sequence_status = 'active'",
        (args.company_id,)
    ).fetchone()[0]

    ok({
        "total_workflows": total_workflows,
        "active_workflows": active_workflows,
        "total_executions": total_executions,
        "total_lead_score_rules": total_rules,
        "total_nurture_sequences": total_sequences,
        "active_nurture_sequences": active_sequences,
    })


# ===========================================================================
# 11. add-drip-sequence (M8 phase B -- standalone create)
# ===========================================================================
def add_drip_sequence(conn, args):
    _validate_company(conn, args.company_id)
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    ds_id = str(uuid.uuid4())
    now = _now_iso()

    conn.execute("""
        INSERT INTO crmadv_drip_sequence (
            id, company_id, name, description, is_active, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?)
    """, (
        ds_id, args.company_id, name,
        getattr(args, "description", None),
        1, now, now,
    ))
    audit(conn, SKILL, "add-drip-sequence", "crmadv_drip_sequence", ds_id,
          new_values={"name": name})
    conn.commit()
    ok({"id": ds_id, "name": name, "is_active": 1})


# ===========================================================================
# 12. list-drip-sequences (M8 phase B -- read, owning-module)
# ===========================================================================
def list_drip_sequences(conn, args):
    _validate_company(conn, args.company_id)

    where, params = ["company_id = ?"], [args.company_id]
    if getattr(args, "is_active", None) is not None:
        where.append("is_active = ?")
        params.append(args.is_active)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM crmadv_drip_sequence WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM crmadv_drip_sequence WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    result_rows = [row_to_dict(r) for r in rows]
    ok({
        "rows": result_rows,
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 13. add-drip-step (M8 phase B -- standalone create within a drip sequence)
# ===========================================================================
def add_drip_step(conn, args):
    sequence_id = getattr(args, "sequence_id", None)
    if not sequence_id:
        err("--sequence-id is required")
    if not conn.execute(
        Q.from_(Table("crmadv_drip_sequence")).select(Field('id')).where(Field("id") == P()).get_sql(),
        (sequence_id,),
    ).fetchone():
        err(f"Drip sequence {sequence_id} not found")

    step_order_raw = getattr(args, "step_order", None)
    if step_order_raw is None:
        err("--step-order is required")
    try:
        step_order = int(step_order_raw)
    except (ValueError, TypeError):
        err("--step-order must be an integer")

    delay_hours_raw = getattr(args, "delay_hours", None)
    if delay_hours_raw is None:
        err("--delay-hours is required")
    try:
        delay_hours = int(delay_hours_raw)
    except (ValueError, TypeError):
        err("--delay-hours must be an integer")

    step_id = str(uuid.uuid4())
    now = _now_iso()

    conn.execute("""
        INSERT INTO crmadv_drip_sequence_step (
            id, sequence_id, step_order, delay_hours, email_template_id,
            is_active, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?)
    """, (
        step_id, sequence_id, step_order, delay_hours,
        getattr(args, "email_template_id", None),
        1, now, now,
    ))
    audit(conn, SKILL, "add-drip-step", "crmadv_drip_sequence_step", step_id,
          new_values={"sequence_id": sequence_id, "step_order": step_order})
    conn.commit()
    ok({"id": step_id, "sequence_id": sequence_id, "step_order": step_order,
        "delay_hours": delay_hours, "is_active": 1})


# ===========================================================================
# 14. list-drip-steps (M8 phase B -- read, owning-module)
# ===========================================================================
def list_drip_steps(conn, args):
    sequence_id = getattr(args, "sequence_id", None)
    if not sequence_id:
        err("--sequence-id is required")
    if not conn.execute(
        Q.from_(Table("crmadv_drip_sequence")).select(Field('id')).where(Field("id") == P()).get_sql(),
        (sequence_id,),
    ).fetchone():
        err(f"Drip sequence {sequence_id} not found")

    params = [sequence_id]
    total = conn.execute(
        "SELECT COUNT(*) FROM crmadv_drip_sequence_step WHERE sequence_id = ?", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        "SELECT * FROM crmadv_drip_sequence_step WHERE sequence_id = ? ORDER BY step_order LIMIT ? OFFSET ?",
        params
    ).fetchall()
    result_rows = [row_to_dict(r) for r in rows]
    ok({
        "rows": result_rows,
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 15. enroll-contact (M8 phase B -- enroll a contact into a drip sequence)
# ===========================================================================
def enroll_contact(conn, args):
    sequence_id = getattr(args, "sequence_id", None)
    if not sequence_id:
        err("--sequence-id is required")
    contact_id = getattr(args, "contact_id", None)
    if not contact_id:
        err("--contact-id is required")

    # The sequence must exist AND be active to accept enrollments.
    seq = conn.execute(
        Q.from_(Table("crmadv_drip_sequence")).select(Field("id"), Field("is_active"))
        .where(Field("id") == P()).get_sql(),
        (sequence_id,),
    ).fetchone()
    if not seq:
        err(f"Drip sequence {sequence_id} not found")
    if row_to_dict(seq)["is_active"] != 1:
        err(f"Drip sequence {sequence_id} is not active")

    # next_send_at is driven by the first step (lowest step_order). With no
    # steps the enrollment has nothing to send, so next_send_at stays NULL.
    first_step = conn.execute(
        "SELECT delay_hours FROM crmadv_drip_sequence_step "
        "WHERE sequence_id = ? ORDER BY step_order LIMIT 1",
        (sequence_id,),
    ).fetchone()

    # enrolled_at and next_send_at share one base instant so next_send_at is
    # exactly enrolled_at + the first step's delay_hours.
    now_dt = datetime.now(timezone.utc).replace(microsecond=0)
    now = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if first_step is not None:
        base = now_dt + timedelta(hours=int(first_step["delay_hours"]))
        next_send_at = base.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        next_send_at = None

    enr_id = str(uuid.uuid4())
    conn.execute("""
        INSERT INTO crmadv_drip_enrollment (
            id, sequence_id, contact_id, current_step, status,
            next_send_at, enrolled_at, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        enr_id, sequence_id, contact_id, 0, "active",
        next_send_at, now, now, now,
    ))
    audit(conn, SKILL, "enroll-contact", "crmadv_drip_enrollment", enr_id,
          new_values={"sequence_id": sequence_id, "contact_id": contact_id})
    conn.commit()
    # NB: the response envelope owns the top-level "status" key, so the
    # enrollment's own status is surfaced as "enrollment_status".
    ok({"id": enr_id, "sequence_id": sequence_id, "contact_id": contact_id,
        "current_step": 0, "enrollment_status": "active", "next_send_at": next_send_at})


# ===========================================================================
# 16. list-enrollments (M8 phase B -- read, owning-module)
# ===========================================================================
def list_enrollments(conn, args):
    sequence_id = getattr(args, "sequence_id", None)
    if not sequence_id:
        err("--sequence-id is required")
    if not conn.execute(
        Q.from_(Table("crmadv_drip_sequence")).select(Field('id')).where(Field("id") == P()).get_sql(),
        (sequence_id,),
    ).fetchone():
        err(f"Drip sequence {sequence_id} not found")

    # Build the optional status filter from literal fragments only (no user
    # data is ever interpolated into SQL; values stay bound parameters).
    params = [sequence_id]
    status_clause = ""
    if getattr(args, "status", None):
        status_clause = " AND status = ?"
        params.append(args.status)

    total = conn.execute(
        "SELECT COUNT(*) FROM crmadv_drip_enrollment WHERE sequence_id = ?" + status_clause,
        params,
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        "SELECT * FROM crmadv_drip_enrollment WHERE sequence_id = ?" + status_clause
        + " ORDER BY enrolled_at DESC LIMIT ? OFFSET ?",
        params,
    ).fetchall()
    result_rows = [row_to_dict(r) for r in rows]
    ok({
        "rows": result_rows,
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 17. cancel-enrollment (M8 phase B -- guarded UPDATE)
# ===========================================================================
def cancel_enrollment(conn, args):
    enr_id = getattr(args, "enrollment_id", None)
    if not enr_id:
        err("--enrollment-id is required")
    row = conn.execute(
        Q.from_(Table("crmadv_drip_enrollment")).select(Table("crmadv_drip_enrollment").star)
        .where(Field("id") == P()).get_sql(),
        (enr_id,),
    ).fetchone()
    if not row:
        err(f"Drip enrollment {enr_id} not found")

    d = row_to_dict(row)
    if d["status"] == "cancelled":
        err("Enrollment is already cancelled")

    now = _now_iso()
    conn.execute("""
        UPDATE crmadv_drip_enrollment
        SET status = 'cancelled', next_send_at = NULL, updated_at = ?
        WHERE id = ?
    """, (now, enr_id))
    audit(conn, SKILL, "cancel-enrollment", "crmadv_drip_enrollment", enr_id,
          new_values={"status": "cancelled"})
    conn.commit()
    # "enrollment_status" -- the envelope owns the top-level "status" key.
    ok({"id": enr_id, "enrollment_status": "cancelled"})


# ===========================================================================
# 18. process-drip-sends (M8 phase B -- cron worker, completes M8-B)
# ===========================================================================
def _resolve_recipient_email(conn, contact_id):
    """READ-only lookup of a CRM contact's email address.

    The CRM's contactable entity that carries an email address is the
    foundation `lead` table; enrollment.contact_id is its id. This is a
    cross-module READ (any module may read any table) -- we never write it.
    Returns the email string, or None when the contact / email is missing.
    """
    row = conn.execute(
        "SELECT email FROM lead WHERE id = ?", (contact_id,)
    ).fetchone()
    if row and row["email"]:
        return row["email"]
    return None


def _dispatch_email(conn, to_address, template_id, company_id, db_path):
    """Send one drip step's email via the erpclaw-alerts `send-email` ACTION.

    Cross-module reach is by INVOKING the action (subprocess to alerts'
    db_query.py) -- we never write erpclaw-alerts' email_outbox/email_log
    tables directly (owning-module-writes rule). Returns (ok: bool, info: str).
    This is the single seam tests patch (no real subprocess in CI).
    """
    from erpclaw_lib.dependencies import check_subprocess_target, resolve_skill_script
    dep_err = check_subprocess_target(conn, "erpclaw-alerts", "email_outbox")
    if dep_err:
        return False, dep_err["error"]
    script = resolve_skill_script("erpclaw-alerts")
    cmd = [
        sys.executable, script,
        "--action", "send-email",
        "--to", to_address,
        "--template-id", template_id,
    ]
    if company_id:
        cmd.extend(["--company-id", company_id])
    if db_path:
        cmd.extend(["--db-path", db_path])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        return False, "send-email timed out (30s)"
    if result.returncode != 0:
        return False, (result.stdout.strip() or result.stderr.strip() or "send-email failed")
    try:
        resp = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError):
        return False, f"invalid response from erpclaw-alerts: {result.stdout[:200]}"
    if resp.get("status") != "ok":
        return False, resp.get("message") or resp.get("error") or "send-email failed"
    return True, resp.get("email_outbox_id") or "queued"


def _process_one_enrollment(conn, enr, now, now_dt, db_path):
    """Advance a single due enrollment by one step inside one transaction.

    Returns a per-enrollment detail dict. Raises on unexpected DB errors so the
    caller can roll back and record the failure.
    """
    enr_id = enr["id"]
    steps = [row_to_dict(s) for s in conn.execute(
        "SELECT id, step_order, delay_hours, email_template_id "
        "FROM crmadv_drip_sequence_step WHERE sequence_id = ? ORDER BY step_order",
        (enr["sequence_id"],)
    ).fetchall()]
    n_steps = len(steps)
    cur = int(enr["current_step"])

    # Nothing left to send: the enrollment has caught up to (or past) the last
    # step. Mark it completed and stop scheduling.
    if cur >= n_steps:
        conn.execute(
            "UPDATE crmadv_drip_enrollment SET status = 'completed', "
            "next_send_at = NULL, updated_at = ? WHERE id = ?",
            (now, enr_id))
        audit(conn, SKILL, "process-drip-sends", "crmadv_drip_enrollment", enr_id,
              new_values={"status": "completed", "reason": "no steps remaining"})
        conn.commit()
        return {"enrollment_id": enr_id, "outcome": "completed", "advanced": False,
                "note": "no steps remaining"}

    step = steps[cur]
    template_id = step.get("email_template_id")
    recipient = None

    if template_id:
        recipient = _resolve_recipient_email(conn, enr["contact_id"])
        if not recipient:
            # No deliverable address: skip WITHOUT advancing so a later run can
            # retry once the contact has an email.
            return {"enrollment_id": enr_id, "outcome": "skipped", "advanced": False,
                    "note": f"no email for contact {enr['contact_id']}"}
        sent_ok, info = _dispatch_email(
            conn, recipient, template_id, enr.get("seq_company_id"), db_path)
        if not sent_ok:
            # Send failed: do not advance; surface the provider error.
            return {"enrollment_id": enr_id, "outcome": "skipped", "advanced": False,
                    "note": f"send failed: {info}"}
    # else: step carries no template -> no-op send, but still advance.

    new_step = cur + 1
    if new_step < n_steps:
        next_delay = int(steps[new_step]["delay_hours"])
        next_send_at = (now_dt + timedelta(hours=next_delay)).strftime("%Y-%m-%dT%H:%M:%SZ")
        new_status = "active"
    else:
        next_send_at = None
        new_status = "completed"

    conn.execute(
        "UPDATE crmadv_drip_enrollment SET current_step = ?, status = ?, "
        "next_send_at = ?, updated_at = ? WHERE id = ?",
        (new_step, new_status, next_send_at, now, enr_id))
    audit(conn, SKILL, "process-drip-sends", "crmadv_drip_enrollment", enr_id,
          new_values={"current_step": new_step, "status": new_status,
                      "next_send_at": next_send_at})
    conn.commit()

    return {"enrollment_id": enr_id, "outcome": "sent", "advanced": True,
            "completed": new_status == "completed", "current_step": new_step,
            "next_send_at": next_send_at, "recipient": recipient,
            "email_template_id": template_id,
            "note": None if template_id else "no-op (step has no email_template_id)"}


def process_drip_sends(conn, args):
    """Cron worker: find due active enrollments and send the next step's email.

    Due = status='active' AND next_send_at IS NOT NULL AND next_send_at <= now.
    Each enrollment is advanced in its own transaction. Cross-module reach is via
    the erpclaw-alerts send-email ACTION only; the CRM contact is READ-only; only
    crmadv_* tables are written here. --now overrides the clock for deterministic
    tests.
    """
    now_arg = getattr(args, "now", None)
    if now_arg:
        try:
            now_dt = datetime.strptime(now_arg, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            err("--now must be ISO format YYYY-MM-DDTHH:MM:SSZ")
            return  # unreachable; err() exits
    else:
        now_dt = datetime.now(timezone.utc).replace(microsecond=0)
    now = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    limit_raw = getattr(args, "limit", None)
    try:
        limit = int(limit_raw) if limit_raw is not None else 100
    except (ValueError, TypeError):
        err("--limit must be an integer")
        return
    if limit <= 0:
        limit = 100

    where = ["e.status = 'active'", "e.next_send_at IS NOT NULL", "e.next_send_at <= ?"]
    params = [now]
    if getattr(args, "company_id", None):
        where.append("s.company_id = ?")
        params.append(args.company_id)
    where_sql = " AND ".join(where)
    params.append(limit)

    rows = conn.execute(
        "SELECT e.*, s.company_id AS seq_company_id "
        "FROM crmadv_drip_enrollment e "
        "JOIN crmadv_drip_sequence s ON s.id = e.sequence_id "
        f"WHERE {where_sql} ORDER BY e.next_send_at LIMIT ?",
        params
    ).fetchall()
    enrollments = [row_to_dict(r) for r in rows]

    db_path = getattr(args, "db_path", None)
    processed = sent = completed = skipped = 0
    details = []

    for enr in enrollments:
        processed += 1
        try:
            detail = _process_one_enrollment(conn, enr, now, now_dt, db_path)
        except Exception as e:  # noqa: BLE001 -- isolate one bad enrollment
            conn.rollback()
            skipped += 1
            details.append({"enrollment_id": enr["id"], "outcome": "error",
                            "advanced": False, "note": str(e)})
            continue

        outcome = detail["outcome"]
        if outcome == "sent":
            sent += 1
            if detail.get("completed"):
                completed += 1
        elif outcome == "completed":
            completed += 1
        elif outcome == "skipped":
            skipped += 1
        details.append(detail)

    ok({"processed": processed, "sent": sent, "completed": completed,
        "skipped": skipped, "details": details})


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "add-automation-workflow": add_automation_workflow,
    "update-automation-workflow": update_automation_workflow,
    "list-automation-workflows": list_automation_workflows,
    "activate-workflow": activate_workflow,
    "deactivate-workflow": deactivate_workflow,
    "add-lead-score-rule": add_lead_score_rule,
    "list-lead-score-rules": list_lead_score_rules,
    "add-nurture-sequence": add_nurture_sequence,
    "list-nurture-sequences": list_nurture_sequences,
    "automation-performance-report": automation_performance_report,
    "add-drip-sequence": add_drip_sequence,
    "list-drip-sequences": list_drip_sequences,
    "add-drip-step": add_drip_step,
    "list-drip-steps": list_drip_steps,
    "enroll-contact": enroll_contact,
    "list-enrollments": list_enrollments,
    "cancel-enrollment": cancel_enrollment,
    "process-drip-sends": process_drip_sends,
}
