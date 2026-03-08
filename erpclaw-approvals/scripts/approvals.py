"""ERPClaw Approvals -- approvals domain module

Actions for approval rules, steps, and requests (3 tables, 13 actions).
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

    ENTITY_PREFIXES.setdefault("approval_rule", "ARULE-")
    ENTITY_PREFIXES.setdefault("approval_request", "APR-")
except ImportError:
    pass

SKILL = "erpclaw-approvals"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------------------------------------------------------------------
# Validation constants
# ---------------------------------------------------------------------------
VALID_APPROVAL_TYPES = ("sequential", "parallel", "conditional")
VALID_REQUEST_STATUSES = ("pending", "in_progress", "approved", "rejected", "cancelled")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute("SELECT id FROM company WHERE id = ?", (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _validate_enum(value, valid_values, field_name):
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")


# ===========================================================================
# 1. add-approval-rule
# ===========================================================================
def add_approval_rule(conn, args):
    _validate_company(conn, args.company_id)
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    rule_id = str(uuid.uuid4())
    now = _now_iso()

    entity_type = getattr(args, "entity_type", None)
    conditions = getattr(args, "conditions", None)

    conn.execute("""
        INSERT INTO approval_rule (
            id, name, entity_type, conditions, is_active, company_id,
            created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?)
    """, (
        rule_id, name, entity_type, conditions,
        1, args.company_id, now, now,
    ))
    audit(conn, SKILL, "approval-add-approval-rule", "approval_rule", rule_id,
          new_values={"name": name, "entity_type": entity_type})
    conn.commit()
    ok({"id": rule_id, "name": name, "is_active": 1})


# ===========================================================================
# 2. update-approval-rule
# ===========================================================================
def update_approval_rule(conn, args):
    rule_id = getattr(args, "id", None)
    if not rule_id:
        err("--id is required")
    if not conn.execute("SELECT id FROM approval_rule WHERE id = ?", (rule_id,)).fetchone():
        err(f"Approval rule {rule_id} not found")

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "name": "name", "entity_type": "entity_type",
        "conditions": "conditions",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        updates.append("is_active = ?")
        params.append(int(is_active))
        changed.append("is_active")

    if not updates:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(rule_id)
    conn.execute(f"UPDATE approval_rule SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, SKILL, "approval-update-approval-rule", "approval_rule", rule_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": rule_id, "updated_fields": changed})


# ===========================================================================
# 3. get-approval-rule
# ===========================================================================
def get_approval_rule(conn, args):
    rule_id = getattr(args, "id", None)
    if not rule_id:
        err("--id is required")
    row = conn.execute("SELECT * FROM approval_rule WHERE id = ?", (rule_id,)).fetchone()
    if not row:
        err(f"Approval rule {rule_id} not found")
    data = row_to_dict(row)

    # Include steps
    steps = conn.execute(
        "SELECT * FROM approval_step WHERE rule_id = ? ORDER BY step_order",
        (rule_id,)
    ).fetchall()
    data["steps"] = [row_to_dict(s) for s in steps]
    data["step_count"] = len(steps)
    ok(data)


# ===========================================================================
# 4. list-approval-rules
# ===========================================================================
def list_approval_rules(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "entity_type", None):
        where.append("entity_type = ?")
        params.append(args.entity_type)
    if getattr(args, "search", None):
        where.append("(name LIKE ?)")
        params.append(f"%{args.search}%")

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        where.append("is_active = ?")
        params.append(int(is_active))

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM approval_rule WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM approval_rule WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 5. add-approval-step
# ===========================================================================
def add_approval_step(conn, args):
    rule_id = getattr(args, "rule_id", None)
    if not rule_id:
        err("--rule-id is required")
    if not conn.execute("SELECT id FROM approval_rule WHERE id = ?", (rule_id,)).fetchone():
        err(f"Approval rule {rule_id} not found")

    approver = getattr(args, "approver", None)
    if not approver:
        err("--approver is required")

    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")
    _validate_company(conn, company_id)

    approval_type = getattr(args, "approval_type", None) or "sequential"
    _validate_enum(approval_type, VALID_APPROVAL_TYPES, "approval-type")

    step_order = int(getattr(args, "step_order", None) or 1)
    is_required = int(getattr(args, "is_required", None) or 1)

    step_id = str(uuid.uuid4())
    now = _now_iso()

    conn.execute("""
        INSERT INTO approval_step (
            id, rule_id, step_order, approver, approval_type, is_required,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        step_id, rule_id, step_order, approver, approval_type, is_required,
        company_id, now, now,
    ))
    audit(conn, SKILL, "approval-add-approval-step", "approval_step", step_id,
          new_values={"rule_id": rule_id, "approver": approver, "step_order": step_order})
    conn.commit()
    ok({
        "id": step_id, "rule_id": rule_id, "step_order": step_order,
        "approver": approver, "approval_type": approval_type,
    })


# ===========================================================================
# 6. list-approval-steps
# ===========================================================================
def list_approval_steps(conn, args):
    rule_id = getattr(args, "rule_id", None)
    where, params = ["1=1"], []
    if rule_id:
        where.append("rule_id = ?")
        params.append(rule_id)
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM approval_step WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM approval_step WHERE {where_sql} ORDER BY step_order ASC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 7. submit-for-approval
# ===========================================================================
def submit_for_approval(conn, args):
    rule_id = getattr(args, "rule_id", None)
    if not rule_id:
        err("--rule-id is required")
    rule = conn.execute("SELECT * FROM approval_rule WHERE id = ?", (rule_id,)).fetchone()
    if not rule:
        err(f"Approval rule {rule_id} not found")

    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")
    _validate_company(conn, company_id)

    # Check rule is active
    rule_data = row_to_dict(rule)
    if not rule_data.get("is_active"):
        err("Approval rule is not active")

    # Check at least one step exists
    step_count = conn.execute(
        "SELECT COUNT(*) FROM approval_step WHERE rule_id = ?", (rule_id,)
    ).fetchone()[0]
    if step_count == 0:
        err("Approval rule has no steps defined")

    entity_type = getattr(args, "entity_type", None)
    entity_id = getattr(args, "entity_id", None)
    requested_by = getattr(args, "requested_by", None)

    req_id = str(uuid.uuid4())
    conn.company_id = company_id
    naming = get_next_name(conn, "approval_request", company_id=company_id)
    now = _now_iso()

    conn.execute("""
        INSERT INTO approval_request (
            id, naming_series, rule_id, entity_type, entity_id,
            requested_by, current_step, request_status, notes, company_id,
            created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        req_id, naming, rule_id, entity_type, entity_id,
        requested_by, 1, "pending",
        getattr(args, "notes", None),
        company_id, now, now,
    ))
    audit(conn, SKILL, "approval-submit-for-approval", "approval_request", req_id,
          new_values={"rule_id": rule_id, "entity_type": entity_type, "entity_id": entity_id})
    conn.commit()
    ok({
        "id": req_id, "naming_series": naming, "rule_id": rule_id,
        "request_status": "pending", "current_step": 1,
    })


# ===========================================================================
# 8. approve-request
# ===========================================================================
def approve_request(conn, args):
    req_id = getattr(args, "id", None)
    if not req_id:
        err("--id is required")
    row = conn.execute("SELECT * FROM approval_request WHERE id = ?", (req_id,)).fetchone()
    if not row:
        err(f"Approval request {req_id} not found")

    data = row_to_dict(row)
    if data["request_status"] not in ("pending", "in_progress"):
        err(f"Cannot approve request in status '{data['request_status']}'. Must be pending or in_progress.")

    current_step = data["current_step"]
    rule_id = data["rule_id"]

    # Get total steps for this rule
    max_step = conn.execute(
        "SELECT MAX(step_order) FROM approval_step WHERE rule_id = ?",
        (rule_id,)
    ).fetchone()[0] or 1

    notes = getattr(args, "notes", None)

    if current_step >= max_step:
        # Final step -- mark as approved
        conn.execute(
            "UPDATE approval_request SET request_status = 'approved', notes = COALESCE(?, notes), "
            "updated_at = datetime('now') WHERE id = ?",
            (notes, req_id)
        )
        new_status = "approved"
        new_step = current_step
    else:
        # Advance to next step
        new_step = current_step + 1
        conn.execute(
            "UPDATE approval_request SET current_step = ?, request_status = 'in_progress', "
            "notes = COALESCE(?, notes), updated_at = datetime('now') WHERE id = ?",
            (new_step, notes, req_id)
        )
        new_status = "in_progress"

    audit(conn, SKILL, "approval-approve-request", "approval_request", req_id,
          new_values={"request_status": new_status, "current_step": new_step})
    conn.commit()
    ok({"id": req_id, "request_status": new_status, "current_step": new_step})


# ===========================================================================
# 9. reject-request
# ===========================================================================
def reject_request(conn, args):
    req_id = getattr(args, "id", None)
    if not req_id:
        err("--id is required")
    row = conn.execute("SELECT * FROM approval_request WHERE id = ?", (req_id,)).fetchone()
    if not row:
        err(f"Approval request {req_id} not found")

    data = row_to_dict(row)
    if data["request_status"] not in ("pending", "in_progress"):
        err(f"Cannot reject request in status '{data['request_status']}'. Must be pending or in_progress.")

    notes = getattr(args, "notes", None)
    conn.execute(
        "UPDATE approval_request SET request_status = 'rejected', notes = ?, "
        "updated_at = datetime('now') WHERE id = ?",
        (notes, req_id)
    )
    audit(conn, SKILL, "approval-reject-request", "approval_request", req_id,
          new_values={"request_status": "rejected", "notes": notes})
    conn.commit()
    ok({"id": req_id, "request_status": "rejected"})


# ===========================================================================
# 10. cancel-request
# ===========================================================================
def cancel_request(conn, args):
    req_id = getattr(args, "id", None)
    if not req_id:
        err("--id is required")
    row = conn.execute("SELECT * FROM approval_request WHERE id = ?", (req_id,)).fetchone()
    if not row:
        err(f"Approval request {req_id} not found")

    data = row_to_dict(row)
    if data["request_status"] in ("approved", "rejected", "cancelled"):
        err(f"Cannot cancel request in status '{data['request_status']}'.")

    conn.execute(
        "UPDATE approval_request SET request_status = 'cancelled', updated_at = datetime('now') WHERE id = ?",
        (req_id,)
    )
    audit(conn, SKILL, "approval-cancel-request", "approval_request", req_id,
          new_values={"request_status": "cancelled"})
    conn.commit()
    ok({"id": req_id, "request_status": "cancelled"})


# ===========================================================================
# 11. list-approval-requests
# ===========================================================================
def list_approval_requests(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "status", None):
        where.append("request_status = ?")
        params.append(args.status)
    if getattr(args, "entity_type", None):
        where.append("entity_type = ?")
        params.append(args.entity_type)
    if getattr(args, "rule_id", None):
        where.append("rule_id = ?")
        params.append(args.rule_id)
    if getattr(args, "search", None):
        where.append("(notes LIKE ? OR requested_by LIKE ?)")
        params.extend([f"%{args.search}%", f"%{args.search}%"])

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM approval_request WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM approval_request WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 12. get-approval-request
# ===========================================================================
def get_approval_request(conn, args):
    req_id = getattr(args, "id", None)
    if not req_id:
        err("--id is required")
    row = conn.execute("SELECT * FROM approval_request WHERE id = ?", (req_id,)).fetchone()
    if not row:
        err(f"Approval request {req_id} not found")
    data = row_to_dict(row)

    # Include rule info
    rule = conn.execute(
        "SELECT name, entity_type FROM approval_rule WHERE id = ?",
        (data["rule_id"],)
    ).fetchone()
    if rule:
        data["rule_name"] = rule[0]

    # Include steps
    steps = conn.execute(
        "SELECT * FROM approval_step WHERE rule_id = ? ORDER BY step_order",
        (data["rule_id"],)
    ).fetchall()
    data["steps"] = [row_to_dict(s) for s in steps]
    data["total_steps"] = len(steps)
    ok(data)


# ===========================================================================
# 13. status
# ===========================================================================
def status_action(conn, args):
    counts = {}
    for tbl in ("approval_rule", "approval_step", "approval_request"):
        counts[tbl] = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    ok({
        "skill": "erpclaw-approvals",
        "version": "1.0.0",
        "total_tables": 3,
        "record_counts": counts,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "approval-add-approval-rule": add_approval_rule,
    "approval-update-approval-rule": update_approval_rule,
    "approval-get-approval-rule": get_approval_rule,
    "approval-list-approval-rules": list_approval_rules,
    "approval-add-approval-step": add_approval_step,
    "approval-list-approval-steps": list_approval_steps,
    "approval-submit-for-approval": submit_for_approval,
    "approval-approve-request": approve_request,
    "approval-reject-request": reject_request,
    "approval-cancel-request": cancel_request,
    "approval-list-approval-requests": list_approval_requests,
    "approval-get-approval-request": get_approval_request,
    "status": status_action,
}
