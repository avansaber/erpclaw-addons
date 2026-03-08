"""erpclaw-crm-adv -- automation domain module

Actions for marketing automation workflows, lead scoring, and nurture sequences (3 tables, 10 actions).
Imported by db_query.py (unified router).
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit

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
    if not conn.execute("SELECT id FROM company WHERE id = ?", (company_id,)).fetchone():
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
    if not conn.execute("SELECT id FROM crmadv_automation_workflow WHERE id = ?", (wf_id,)).fetchone():
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
    row = conn.execute("SELECT * FROM crmadv_automation_workflow WHERE id = ?", (wf_id,)).fetchone()
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
    row = conn.execute("SELECT * FROM crmadv_automation_workflow WHERE id = ?", (wf_id,)).fetchone()
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
}
