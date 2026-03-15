"""ERPClaw Compliance -- policy domain module

Actions for policies and acknowledgments (2 tables, 10 actions).
Imported by db_query.py (unified router).
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone, date

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection, DEFAULT_DB_PATH
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row
except ImportError:
    pass

# Register naming prefixes
ENTITY_PREFIXES.setdefault("policy", "POL-")

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
_today_iso = lambda: date.today().isoformat()

VALID_POLICY_TYPES = ("general", "hr", "financial", "it", "safety", "compliance", "operational", "other")
VALID_POLICY_STATUSES = ("draft", "review", "approved", "published", "retired")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _validate_enum(value, valid_values, field_name):
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")


# ---------------------------------------------------------------------------
# 1. add-policy
# ---------------------------------------------------------------------------
def add_policy(conn, args):
    _validate_company(conn, args.company_id)

    title = getattr(args, "title", None)
    if not title:
        err("--title is required")

    policy_type = getattr(args, "policy_type", None) or "general"
    _validate_enum(policy_type, VALID_POLICY_TYPES, "policy-type")

    policy_id = str(uuid.uuid4())
    naming = get_next_name(conn, "policy", company_id=args.company_id)
    now = _now_iso()

    requires_ack_raw = getattr(args, "requires_acknowledgment", None)
    requires_ack = 1 if requires_ack_raw == "1" or requires_ack_raw == 1 else 0

    conn.execute("""
        INSERT INTO policy (
            id, naming_series, title, policy_type, version,
            content, effective_date, review_date, owner,
            status, requires_acknowledgment,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        policy_id, naming, title, policy_type,
        getattr(args, "version", None) or "1.0",
        getattr(args, "content", None),
        getattr(args, "effective_date", None),
        getattr(args, "review_date", None),
        getattr(args, "owner", None),
        "draft",
        requires_ack,
        args.company_id, now, now,
    ))
    audit(conn, "policy", policy_id, "compliance-add-policy", args.company_id)
    conn.commit()
    ok({"id": policy_id, "naming_series": naming, "title": title, "policy_status": "draft"})


# ---------------------------------------------------------------------------
# 2. update-policy
# ---------------------------------------------------------------------------
def update_policy(conn, args):
    policy_id = getattr(args, "policy_id", None)
    if not policy_id:
        err("--policy-id is required")
    if not conn.execute(Q.from_(Table("policy")).select(Field('id')).where(Field("id") == P()).get_sql(), (policy_id,)).fetchone():
        err(f"Policy {policy_id} not found")

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "title": "title",
        "version": "version",
        "content": "content",
        "review_date": "review_date",
        "owner": "owner",
        "notes": "notes",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    policy_type = getattr(args, "policy_type", None)
    if policy_type is not None:
        _validate_enum(policy_type, VALID_POLICY_TYPES, "policy-type")
        updates.append("policy_type = ?")
        params.append(policy_type)
        changed.append("policy_type")

    requires_ack_raw = getattr(args, "requires_acknowledgment", None)
    if requires_ack_raw is not None:
        requires_ack = 1 if requires_ack_raw == "1" or requires_ack_raw == 1 else 0
        updates.append("requires_acknowledgment = ?")
        params.append(requires_ack)
        changed.append("requires_acknowledgment")

    if not updates:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(policy_id)
    conn.execute(f"UPDATE policy SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, "policy", policy_id, "compliance-update-policy", None, {"updated_fields": changed})
    conn.commit()
    ok({"id": policy_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# 3. get-policy
# ---------------------------------------------------------------------------
def get_policy(conn, args):
    policy_id = getattr(args, "policy_id", None)
    if not policy_id:
        err("--policy-id is required")
    row = conn.execute(Q.from_(Table("policy")).select(Table("policy").star).where(Field("id") == P()).get_sql(), (policy_id,)).fetchone()
    if not row:
        err(f"Policy {policy_id} not found")
    data = row_to_dict(row)

    # Enrich: acknowledgment count
    ack_count = conn.execute(
        "SELECT COUNT(*) FROM policy_acknowledgment WHERE policy_id = ?", (policy_id,)
    ).fetchone()[0]
    data["acknowledgment_count"] = ack_count
    ok(data)


# ---------------------------------------------------------------------------
# 4. list-policies
# ---------------------------------------------------------------------------
def list_policies(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "policy_type", None):
        where.append("policy_type = ?")
        params.append(args.policy_type)
    if getattr(args, "status", None):
        where.append("status = ?")
        params.append(args.status)
    if getattr(args, "search", None):
        where.append("(title LIKE ? OR content LIKE ?)")
        params.extend([f"%{args.search}%", f"%{args.search}%"])

    where_sql = " AND ".join(where)
    total = conn.execute(f"SELECT COUNT(*) FROM policy WHERE {where_sql}", params).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM policy WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ---------------------------------------------------------------------------
# 5. publish-policy
# ---------------------------------------------------------------------------
def publish_policy(conn, args):
    policy_id = getattr(args, "policy_id", None)
    if not policy_id:
        err("--policy-id is required")
    row = conn.execute(Q.from_(Table("policy")).select(Field('status')).where(Field("id") == P()).get_sql(), (policy_id,)).fetchone()
    if not row:
        err(f"Policy {policy_id} not found")
    if row[0] == "published":
        err("Policy is already published")
    if row[0] == "retired":
        err("Cannot publish a retired policy")

    effective_date = getattr(args, "effective_date", None) or _today_iso()
    now = _now_iso()
    conn.execute(
        "UPDATE policy SET status = 'published', effective_date = ?, updated_at = ? WHERE id = ?",
        (effective_date, now, policy_id)
    )
    audit(conn, "policy", policy_id, "compliance-publish-policy", None)
    conn.commit()
    ok({"id": policy_id, "policy_status": "published", "effective_date": effective_date})


# ---------------------------------------------------------------------------
# 6. retire-policy
# ---------------------------------------------------------------------------
def retire_policy(conn, args):
    policy_id = getattr(args, "policy_id", None)
    if not policy_id:
        err("--policy-id is required")
    row = conn.execute(Q.from_(Table("policy")).select(Field('status')).where(Field("id") == P()).get_sql(), (policy_id,)).fetchone()
    if not row:
        err(f"Policy {policy_id} not found")
    if row[0] == "retired":
        err("Policy is already retired")

    now = _now_iso()
    conn.execute(
        "UPDATE policy SET status = 'retired', updated_at = ? WHERE id = ?",
        (now, policy_id)
    )
    audit(conn, "policy", policy_id, "compliance-retire-policy", None)
    conn.commit()
    ok({"id": policy_id, "policy_status": "retired"})


# ---------------------------------------------------------------------------
# 7. add-policy-acknowledgment
# ---------------------------------------------------------------------------
def add_policy_acknowledgment(conn, args):
    policy_id = getattr(args, "policy_id", None)
    if not policy_id:
        err("--policy-id is required")
    if not conn.execute(Q.from_(Table("policy")).select(Field('id')).where(Field("id") == P()).get_sql(), (policy_id,)).fetchone():
        err(f"Policy {policy_id} not found")

    _validate_company(conn, args.company_id)

    employee_name = getattr(args, "employee_name", None)
    if not employee_name:
        err("--employee-name is required")

    ack_id = str(uuid.uuid4())
    now = _now_iso()
    conn.execute("""
        INSERT INTO policy_acknowledgment (
            id, policy_id, employee_name, employee_id,
            acknowledged_date, ip_address, notes,
            company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        ack_id, policy_id, employee_name,
        getattr(args, "employee_id", None),
        _today_iso(),
        getattr(args, "ip_address", None),
        getattr(args, "notes", None),
        args.company_id, now,
    ))
    audit(conn, "policy_acknowledgment", ack_id, "compliance-add-policy-acknowledgment", args.company_id)
    conn.commit()
    ok({"id": ack_id, "policy_id": policy_id, "employee_name": employee_name})


# ---------------------------------------------------------------------------
# 8. list-policy-acknowledgments
# ---------------------------------------------------------------------------
def list_policy_acknowledgments(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "policy_id", None):
        where.append("policy_id = ?")
        params.append(args.policy_id)
    if getattr(args, "employee_id", None):
        where.append("employee_id = ?")
        params.append(args.employee_id)

    where_sql = " AND ".join(where)
    total = conn.execute(f"SELECT COUNT(*) FROM policy_acknowledgment WHERE {where_sql}", params).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM policy_acknowledgment WHERE {where_sql} ORDER BY acknowledged_date DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ---------------------------------------------------------------------------
# 9. policy-compliance-report
# ---------------------------------------------------------------------------
def policy_compliance_report(conn, args):
    _validate_company(conn, args.company_id)

    # Get all published policies that require acknowledgment
    policies = conn.execute("""
        SELECT p.id, p.title, p.policy_type,
               COUNT(pa.id) as ack_count
        FROM policy p
        LEFT JOIN policy_acknowledgment pa ON pa.policy_id = p.id
        WHERE p.company_id = ? AND p.status = 'published' AND p.requires_acknowledgment = 1
        GROUP BY p.id, p.title, p.policy_type
        ORDER BY p.title
    """, (args.company_id,)).fetchall()

    # Get total employee count for percentage
    emp_count = conn.execute(
        "SELECT COUNT(*) FROM employee WHERE company_id = ?",
        (args.company_id,)
    ).fetchone()
    total_employees = emp_count[0] if emp_count else 0

    report = []
    for p in policies:
        ack_count = p[3]
        pct = round((ack_count / total_employees * 100), 1) if total_employees > 0 else 0
        report.append({
            "policy_id": p[0],
            "title": p[1],
            "policy_type": p[2],
            "acknowledgment_count": ack_count,
            "total_employees": total_employees,
            "compliance_pct": pct,
        })

    ok({
        "company_id": args.company_id,
        "report_date": _today_iso(),
        "policies": report,
        "total_policies_requiring_ack": len(report),
        "total_employees": total_employees,
    })


# ---------------------------------------------------------------------------
# 10. status
# ---------------------------------------------------------------------------
def skill_status(conn, args):
    ok({
        "skill": "erpclaw-compliance",
        "version": "1.0.0",
        "actions_available": 38,
        "domains": ["audit", "risk", "controls", "policy"],
        "database": DEFAULT_DB_PATH,
    })


# ---------------------------------------------------------------------------
# Action Router
# ---------------------------------------------------------------------------
ACTIONS = {
    "compliance-add-policy": add_policy,
    "compliance-update-policy": update_policy,
    "compliance-get-policy": get_policy,
    "compliance-list-policies": list_policies,
    "compliance-publish-policy": publish_policy,
    "compliance-retire-policy": retire_policy,
    "compliance-add-policy-acknowledgment": add_policy_acknowledgment,
    "compliance-list-policy-acknowledgments": list_policy_acknowledgments,
    "compliance-policy-compliance-report": policy_compliance_report,
    "status": skill_status,
}
