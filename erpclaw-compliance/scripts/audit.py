"""ERPClaw Compliance -- audit domain module

Actions for audit plans and findings (2 tables, 8 actions).
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
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row
except ImportError:
    pass

# Register naming prefixes
ENTITY_PREFIXES.setdefault("audit_plan", "AUD-")

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_AUDIT_TYPES = ("internal", "external", "regulatory", "special")
VALID_AUDIT_STATUSES = ("draft", "scheduled", "in_progress", "completed", "cancelled")
VALID_FINDING_TYPES = ("critical", "major", "minor", "observation", "improvement")
VALID_REMEDIATION_STATUSES = ("open", "in_progress", "remediated", "verified", "overdue", "accepted")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _validate_enum(value, valid_values, field_name):
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")


# ---------------------------------------------------------------------------
# 1. add-audit-plan
# ---------------------------------------------------------------------------
def add_audit_plan(conn, args):
    _validate_company(conn, args.company_id)

    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    audit_type = getattr(args, "audit_type", None) or "internal"
    _validate_enum(audit_type, VALID_AUDIT_TYPES, "audit-type")

    plan_id = str(uuid.uuid4())
    naming = get_next_name(conn, "audit_plan", company_id=args.company_id)
    now = _now_iso()
    conn.execute("""
        INSERT INTO audit_plan (
            id, naming_series, name, audit_type, scope, lead_auditor,
            planned_start, planned_end, status, notes,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        plan_id, naming, name, audit_type,
        getattr(args, "scope", None),
        getattr(args, "lead_auditor", None),
        getattr(args, "planned_start", None),
        getattr(args, "planned_end", None),
        "draft",
        getattr(args, "notes", None),
        args.company_id, now, now,
    ))
    audit(conn, "audit_plan", plan_id, "compliance-add-audit-plan", args.company_id)
    conn.commit()
    ok({"id": plan_id, "naming_series": naming, "name": name, "plan_status": "draft"})


# ---------------------------------------------------------------------------
# 2. update-audit-plan
# ---------------------------------------------------------------------------
def update_audit_plan(conn, args):
    plan_id = getattr(args, "audit_plan_id", None)
    if not plan_id:
        err("--audit-plan-id is required")
    if not conn.execute(Q.from_(Table("audit_plan")).select(Field('id')).where(Field("id") == P()).get_sql(), (plan_id,)).fetchone():
        err(f"Audit plan {plan_id} not found")

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "name": "name",
        "scope": "scope",
        "lead_auditor": "lead_auditor",
        "planned_start": "planned_start",
        "planned_end": "planned_end",
        "notes": "notes",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    audit_type = getattr(args, "audit_type", None)
    if audit_type is not None:
        _validate_enum(audit_type, VALID_AUDIT_TYPES, "audit-type")
        updates.append("audit_type = ?")
        params.append(audit_type)
        changed.append("audit_type")

    if not updates:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(plan_id)
    conn.execute(f"UPDATE audit_plan SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, "audit_plan", plan_id, "compliance-update-audit-plan", None, {"updated_fields": changed})
    conn.commit()
    ok({"id": plan_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# 3. get-audit-plan
# ---------------------------------------------------------------------------
def get_audit_plan(conn, args):
    plan_id = getattr(args, "audit_plan_id", None)
    if not plan_id:
        err("--audit-plan-id is required")
    row = conn.execute(Q.from_(Table("audit_plan")).select(Table("audit_plan").star).where(Field("id") == P()).get_sql(), (plan_id,)).fetchone()
    if not row:
        err(f"Audit plan {plan_id} not found")
    data = row_to_dict(row)

    # Enrich: finding counts
    finding_counts = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN remediation_status = 'open' THEN 1 ELSE 0 END) as open_count,
            SUM(CASE WHEN finding_type = 'critical' THEN 1 ELSE 0 END) as critical_count
        FROM audit_finding WHERE audit_plan_id = ?
    """, (plan_id,)).fetchone()
    data["finding_count"] = finding_counts[0]
    data["open_findings"] = finding_counts[1]
    data["critical_findings"] = finding_counts[2]
    ok(data)


# ---------------------------------------------------------------------------
# 4. list-audit-plans
# ---------------------------------------------------------------------------
def list_audit_plans(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "status", None):
        where.append("status = ?")
        params.append(args.status)
    if getattr(args, "search", None):
        where.append("(name LIKE ? OR scope LIKE ?)")
        params.extend([f"%{args.search}%", f"%{args.search}%"])

    where_sql = " AND ".join(where)
    total = conn.execute(f"SELECT COUNT(*) FROM audit_plan WHERE {where_sql}", params).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM audit_plan WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ---------------------------------------------------------------------------
# 5. start-audit
# ---------------------------------------------------------------------------
def start_audit(conn, args):
    plan_id = getattr(args, "audit_plan_id", None)
    if not plan_id:
        err("--audit-plan-id is required")
    row = conn.execute(Q.from_(Table("audit_plan")).select(Field('status')).where(Field("id") == P()).get_sql(), (plan_id,)).fetchone()
    if not row:
        err(f"Audit plan {plan_id} not found")
    current_status = row[0]
    if current_status not in ("draft", "scheduled"):
        err(f"Cannot start audit in status '{current_status}'. Must be draft or scheduled.")

    now = _now_iso()
    conn.execute(
        "UPDATE audit_plan SET status = 'in_progress', actual_start = ?, updated_at = ? WHERE id = ?",
        (now, now, plan_id)
    )
    audit(conn, "audit_plan", plan_id, "compliance-start-audit", None)
    conn.commit()
    ok({"id": plan_id, "plan_status": "in_progress", "actual_start": now})


# ---------------------------------------------------------------------------
# 6. complete-audit
# ---------------------------------------------------------------------------
def complete_audit(conn, args):
    plan_id = getattr(args, "audit_plan_id", None)
    if not plan_id:
        err("--audit-plan-id is required")
    row = conn.execute(Q.from_(Table("audit_plan")).select(Field('status')).where(Field("id") == P()).get_sql(), (plan_id,)).fetchone()
    if not row:
        err(f"Audit plan {plan_id} not found")
    current_status = row[0]
    if current_status != "in_progress":
        err(f"Cannot complete audit in status '{current_status}'. Must be in_progress.")

    now = _now_iso()
    conn.execute(
        "UPDATE audit_plan SET status = 'completed', actual_end = ?, updated_at = ? WHERE id = ?",
        (now, now, plan_id)
    )
    audit(conn, "audit_plan", plan_id, "compliance-complete-audit", None)
    conn.commit()
    ok({"id": plan_id, "plan_status": "completed", "actual_end": now})


# ---------------------------------------------------------------------------
# 7. add-audit-finding
# ---------------------------------------------------------------------------
def add_audit_finding(conn, args):
    audit_plan_id = getattr(args, "audit_plan_id", None)
    if not audit_plan_id:
        err("--audit-plan-id is required")
    if not conn.execute(Q.from_(Table("audit_plan")).select(Field('id')).where(Field("id") == P()).get_sql(), (audit_plan_id,)).fetchone():
        err(f"Audit plan {audit_plan_id} not found")

    _validate_company(conn, args.company_id)

    title = getattr(args, "title", None)
    if not title:
        err("--title is required")

    finding_type = getattr(args, "finding_type", None) or "observation"
    _validate_enum(finding_type, VALID_FINDING_TYPES, "finding-type")

    finding_id = str(uuid.uuid4())
    now = _now_iso()
    conn.execute("""
        INSERT INTO audit_finding (
            id, audit_plan_id, finding_type, title, description,
            area, root_cause, recommendation, management_response,
            remediation_due, remediation_status, assigned_to,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        finding_id, audit_plan_id, finding_type, title,
        getattr(args, "description", None),
        getattr(args, "area", None),
        getattr(args, "root_cause", None),
        getattr(args, "recommendation", None),
        None,  # management_response
        getattr(args, "remediation_due", None),
        "open",
        getattr(args, "assigned_to", None),
        args.company_id, now, now,
    ))
    audit(conn, "audit_finding", finding_id, "compliance-add-audit-finding", args.company_id)
    conn.commit()
    ok({"id": finding_id, "title": title, "finding_type": finding_type, "finding_status": "open"})


# ---------------------------------------------------------------------------
# 8. list-audit-findings
# ---------------------------------------------------------------------------
def list_audit_findings(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "audit_plan_id", None):
        where.append("audit_plan_id = ?")
        params.append(args.audit_plan_id)
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "finding_type", None):
        where.append("finding_type = ?")
        params.append(args.finding_type)
    if getattr(args, "remediation_status", None):
        where.append("remediation_status = ?")
        params.append(args.remediation_status)

    where_sql = " AND ".join(where)
    total = conn.execute(f"SELECT COUNT(*) FROM audit_finding WHERE {where_sql}", params).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM audit_finding WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ---------------------------------------------------------------------------
# Action Router
# ---------------------------------------------------------------------------
ACTIONS = {
    "compliance-add-audit-plan": add_audit_plan,
    "compliance-update-audit-plan": update_audit_plan,
    "compliance-get-audit-plan": get_audit_plan,
    "compliance-list-audit-plans": list_audit_plans,
    "compliance-start-audit": start_audit,
    "compliance-complete-audit": complete_audit,
    "compliance-add-audit-finding": add_audit_finding,
    "compliance-list-audit-findings": list_audit_findings,
}
