"""ERPClaw Compliance -- risk domain module

Actions for risk register and risk assessments (2 tables, 8 actions).
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
ENTITY_PREFIXES.setdefault("risk_register", "RISK-")

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_RISK_CATEGORIES = ("operational", "financial", "compliance", "strategic", "reputational", "technology", "other")
VALID_RISK_STATUSES = ("identified", "assessed", "mitigating", "monitoring", "closed", "accepted")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _validate_enum(value, valid_values, field_name):
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")


def _calc_risk_level(score):
    """Return risk level from score (likelihood * impact)."""
    if score <= 4:
        return "low"
    elif score <= 9:
        return "medium"
    elif score <= 15:
        return "high"
    else:
        return "critical"


# ---------------------------------------------------------------------------
# 1. add-risk
# ---------------------------------------------------------------------------
def add_risk(conn, args):
    _validate_company(conn, args.company_id)

    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    category = getattr(args, "category", None) or "operational"
    _validate_enum(category, VALID_RISK_CATEGORIES, "category")

    raw_likelihood = getattr(args, "likelihood", None)
    likelihood = int(raw_likelihood) if raw_likelihood is not None else 3
    raw_impact = getattr(args, "impact", None)
    impact = int(raw_impact) if raw_impact is not None else 3
    if not (1 <= likelihood <= 5):
        err("--likelihood must be between 1 and 5")
    if not (1 <= impact <= 5):
        err("--impact must be between 1 and 5")

    risk_score = likelihood * impact
    risk_level = _calc_risk_level(risk_score)

    risk_id = str(uuid.uuid4())
    naming = get_next_name(conn, "risk_register", company_id=args.company_id)
    now = _now_iso()
    conn.execute("""
        INSERT INTO risk_register (
            id, naming_series, name, category, description,
            likelihood, impact, risk_score, risk_level,
            owner, mitigation_plan,
            residual_likelihood, residual_impact, residual_score,
            status, review_date,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        risk_id, naming, name, category,
        getattr(args, "description", None),
        likelihood, impact, risk_score, risk_level,
        getattr(args, "owner", None),
        getattr(args, "mitigation_plan", None),
        None, None, None,
        "identified",
        getattr(args, "review_date", None),
        args.company_id, now, now,
    ))
    audit(conn, "risk_register", risk_id, "compliance-add-risk", args.company_id)
    conn.commit()
    ok({
        "id": risk_id, "naming_series": naming, "name": name,
        "risk_score": risk_score, "risk_level": risk_level, "risk_status": "identified",
    })


# ---------------------------------------------------------------------------
# 2. update-risk
# ---------------------------------------------------------------------------
def update_risk(conn, args):
    risk_id = getattr(args, "risk_id", None)
    if not risk_id:
        err("--risk-id is required")
    row = conn.execute(Q.from_(Table("risk_register")).select(Field('likelihood'), Field('impact')).where(Field("id") == P()).get_sql(), (risk_id,)).fetchone()
    if not row:
        err(f"Risk {risk_id} not found")

    current_likelihood = row[0]
    current_impact = row[1]

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "name": "name",
        "description": "description",
        "owner": "owner",
        "mitigation_plan": "mitigation_plan",
        "review_date": "review_date",
        "notes": "notes",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    category = getattr(args, "category", None)
    if category is not None:
        _validate_enum(category, VALID_RISK_CATEGORIES, "category")
        updates.append("category = ?")
        params.append(category)
        changed.append("category")

    status = getattr(args, "status", None)
    if status is not None:
        _validate_enum(status, VALID_RISK_STATUSES, "status")
        updates.append("status = ?")
        params.append(status)
        changed.append("status")

    # Handle likelihood/impact recalculation
    new_likelihood = getattr(args, "likelihood", None)
    new_impact = getattr(args, "impact", None)
    if new_likelihood is not None:
        new_likelihood = int(new_likelihood)
        if not (1 <= new_likelihood <= 5):
            err("--likelihood must be between 1 and 5")
        updates.append("likelihood = ?")
        params.append(new_likelihood)
        changed.append("likelihood")
        current_likelihood = new_likelihood
    if new_impact is not None:
        new_impact = int(new_impact)
        if not (1 <= new_impact <= 5):
            err("--impact must be between 1 and 5")
        updates.append("impact = ?")
        params.append(new_impact)
        changed.append("impact")
        current_impact = new_impact

    if new_likelihood is not None or new_impact is not None:
        risk_score = current_likelihood * current_impact
        risk_level = _calc_risk_level(risk_score)
        updates.append("risk_score = ?")
        params.append(risk_score)
        updates.append("risk_level = ?")
        params.append(risk_level)
        changed.extend(["risk_score", "risk_level"])

    # Residual scores
    res_likelihood = getattr(args, "residual_likelihood", None)
    res_impact = getattr(args, "residual_impact", None)
    if res_likelihood is not None:
        res_likelihood = int(res_likelihood)
        if not (1 <= res_likelihood <= 5):
            err("--residual-likelihood must be between 1 and 5")
        updates.append("residual_likelihood = ?")
        params.append(res_likelihood)
        changed.append("residual_likelihood")
    if res_impact is not None:
        res_impact = int(res_impact)
        if not (1 <= res_impact <= 5):
            err("--residual-impact must be between 1 and 5")
        updates.append("residual_impact = ?")
        params.append(res_impact)
        changed.append("residual_impact")

    if res_likelihood is not None or res_impact is not None:
        # Need current residual values for the one that wasn't updated
        cur = conn.execute(Q.from_(Table("risk_register")).select(Field('residual_likelihood'), Field('residual_impact')).where(Field("id") == P()).get_sql(), (risk_id,)).fetchone()
        rl = res_likelihood if res_likelihood is not None else (cur[0] if cur[0] is not None else current_likelihood)
        ri = res_impact if res_impact is not None else (cur[1] if cur[1] is not None else current_impact)
        residual_score = rl * ri
        updates.append("residual_score = ?")
        params.append(residual_score)
        changed.append("residual_score")

    if not updates:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(risk_id)
    conn.execute(f"UPDATE risk_register SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, "risk_register", risk_id, "compliance-update-risk", None, {"updated_fields": changed})
    conn.commit()
    ok({"id": risk_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# 3. get-risk
# ---------------------------------------------------------------------------
def get_risk(conn, args):
    risk_id = getattr(args, "risk_id", None)
    if not risk_id:
        err("--risk-id is required")
    row = conn.execute(Q.from_(Table("risk_register")).select(Table("risk_register").star).where(Field("id") == P()).get_sql(), (risk_id,)).fetchone()
    if not row:
        err(f"Risk {risk_id} not found")
    data = row_to_dict(row)

    # Enrich: assessment count
    assess_count = conn.execute(
        "SELECT COUNT(*) FROM risk_assessment WHERE risk_id = ?", (risk_id,)
    ).fetchone()[0]
    data["assessment_count"] = assess_count
    ok(data)


# ---------------------------------------------------------------------------
# 4. list-risks
# ---------------------------------------------------------------------------
def list_risks(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "category", None):
        where.append("category = ?")
        params.append(args.category)
    if getattr(args, "status", None):
        where.append("status = ?")
        params.append(args.status)
    if getattr(args, "risk_level", None):
        where.append("risk_level = ?")
        params.append(args.risk_level)
    if getattr(args, "search", None):
        where.append("(name LIKE ? OR description LIKE ?)")
        params.extend([f"%{args.search}%", f"%{args.search}%"])

    where_sql = " AND ".join(where)
    total = conn.execute(f"SELECT COUNT(*) FROM risk_register WHERE {where_sql}", params).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM risk_register WHERE {where_sql} ORDER BY risk_score DESC, created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ---------------------------------------------------------------------------
# 5. add-risk-assessment
# ---------------------------------------------------------------------------
def add_risk_assessment(conn, args):
    risk_id = getattr(args, "risk_id", None)
    if not risk_id:
        err("--risk-id is required")
    if not conn.execute(Q.from_(Table("risk_register")).select(Field('id')).where(Field("id") == P()).get_sql(), (risk_id,)).fetchone():
        err(f"Risk {risk_id} not found")

    _validate_company(conn, args.company_id)

    raw_likelihood = getattr(args, "likelihood", None)
    likelihood = int(raw_likelihood) if raw_likelihood is not None else 3
    raw_impact = getattr(args, "impact", None)
    impact = int(raw_impact) if raw_impact is not None else 3
    if not (1 <= likelihood <= 5):
        err("--likelihood must be between 1 and 5")
    if not (1 <= impact <= 5):
        err("--impact must be between 1 and 5")

    score = likelihood * impact

    assess_id = str(uuid.uuid4())
    now = _now_iso()
    conn.execute("""
        INSERT INTO risk_assessment (
            id, risk_id, assessment_date, assessor,
            likelihood, impact, score, notes,
            company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        assess_id, risk_id, now[:10],
        getattr(args, "assessor", None),
        likelihood, impact, score,
        getattr(args, "notes", None),
        args.company_id, now,
    ))
    audit(conn, "risk_assessment", assess_id, "compliance-add-risk-assessment", args.company_id)
    conn.commit()
    ok({"id": assess_id, "risk_id": risk_id, "score": score, "risk_level": _calc_risk_level(score)})


# ---------------------------------------------------------------------------
# 6. list-risk-assessments
# ---------------------------------------------------------------------------
def list_risk_assessments(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "risk_id", None):
        where.append("risk_id = ?")
        params.append(args.risk_id)

    where_sql = " AND ".join(where)
    total = conn.execute(f"SELECT COUNT(*) FROM risk_assessment WHERE {where_sql}", params).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM risk_assessment WHERE {where_sql} ORDER BY assessment_date DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ---------------------------------------------------------------------------
# 7. risk-matrix-report
# ---------------------------------------------------------------------------
def risk_matrix_report(conn, args):
    _validate_company(conn, args.company_id)

    rows = conn.execute("""
        SELECT likelihood, impact, COUNT(*) as count
        FROM risk_register
        WHERE company_id = ? AND status != 'closed'
        GROUP BY likelihood, impact
        ORDER BY likelihood, impact
    """, (args.company_id,)).fetchall()

    # Build 5x5 matrix
    matrix = {}
    for l in range(1, 6):
        for i in range(1, 6):
            matrix[f"{l}x{i}"] = 0
    for r in rows:
        matrix[f"{r[0]}x{r[1]}"] = r[2]

    # Summary counts by level
    level_counts = conn.execute("""
        SELECT risk_level, COUNT(*) as count
        FROM risk_register
        WHERE company_id = ? AND status != 'closed'
        GROUP BY risk_level
    """, (args.company_id,)).fetchall()
    summary = {r[0]: r[1] for r in level_counts}

    total = conn.execute(
        "SELECT COUNT(*) FROM risk_register WHERE company_id = ? AND status != 'closed'",
        (args.company_id,)
    ).fetchone()[0]

    ok({
        "company_id": args.company_id,
        "matrix": matrix,
        "summary": summary,
        "total_active_risks": total,
    })


# ---------------------------------------------------------------------------
# 8. close-risk
# ---------------------------------------------------------------------------
def close_risk(conn, args):
    risk_id = getattr(args, "risk_id", None)
    if not risk_id:
        err("--risk-id is required")
    row = conn.execute(Q.from_(Table("risk_register")).select(Field('status')).where(Field("id") == P()).get_sql(), (risk_id,)).fetchone()
    if not row:
        err(f"Risk {risk_id} not found")
    if row[0] == "closed":
        err("Risk is already closed")

    conn.execute(
        "UPDATE risk_register SET status = 'closed', updated_at = datetime('now') WHERE id = ?",
        (risk_id,)
    )
    audit(conn, "risk_register", risk_id, "compliance-close-risk", None)
    conn.commit()
    ok({"id": risk_id, "risk_status": "closed"})


# ---------------------------------------------------------------------------
# Action Router
# ---------------------------------------------------------------------------
ACTIONS = {
    "compliance-add-risk": add_risk,
    "compliance-update-risk": update_risk,
    "compliance-get-risk": get_risk,
    "compliance-list-risks": list_risks,
    "compliance-add-risk-assessment": add_risk_assessment,
    "compliance-list-risk-assessments": list_risk_assessments,
    "compliance-risk-matrix-report": risk_matrix_report,
    "compliance-close-risk": close_risk,
}
