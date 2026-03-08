"""erpclaw-crm-adv -- territories domain module

Actions for sales territory management, assignments, and quotas (3 tables, 10 actions).
Imported by db_query.py (unified router).
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.decimal_utils import to_decimal, round_currency

    ENTITY_PREFIXES.setdefault("crmadv_territory", "TERR-")
except ImportError:
    pass

SKILL = "erpclaw-crm-adv"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_TERRITORY_TYPES = ("geographic", "industry", "named_account", "product")
VALID_TERRITORY_STATUSES = ("active", "inactive")
VALID_ASSIGNMENT_STATUSES = ("active", "ended")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute("SELECT id FROM company WHERE id = ?", (company_id,)).fetchone():
        err(f"Company {company_id} not found")


# ===========================================================================
# 1. add-territory
# ===========================================================================
def add_territory(conn, args):
    _validate_company(conn, args.company_id)
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    territory_type = getattr(args, "territory_type", None) or "geographic"
    if territory_type not in VALID_TERRITORY_TYPES:
        err(f"Invalid territory_type: {territory_type}. Must be one of: {', '.join(VALID_TERRITORY_TYPES)}")

    parent_id = getattr(args, "parent_territory_id", None)
    if parent_id:
        if not conn.execute("SELECT id FROM crmadv_territory WHERE id = ?", (parent_id,)).fetchone():
            err(f"Parent territory {parent_id} not found")

    ter_id = str(uuid.uuid4())
    now = _now_iso()
    conn.company_id = args.company_id
    naming = get_next_name(conn, "crmadv_territory")

    conn.execute("""
        INSERT INTO crmadv_territory (
            id, naming_series, name, region, parent_territory_id,
            territory_type, territory_status, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        ter_id, naming, name,
        getattr(args, "region", None),
        parent_id,
        territory_type, "active",
        args.company_id, now, now,
    ))
    audit(conn, SKILL, "add-territory", "crmadv_territory", ter_id,
          new_values={"name": name, "territory_type": territory_type})
    conn.commit()
    ok({"id": ter_id, "naming_series": naming, "name": name, "territory_type": territory_type, "territory_status": "active"})


# ===========================================================================
# 2. update-territory
# ===========================================================================
def update_territory(conn, args):
    ter_id = getattr(args, "territory_id", None)
    if not ter_id:
        err("--territory-id is required")
    if not conn.execute("SELECT id FROM crmadv_territory WHERE id = ?", (ter_id,)).fetchone():
        err(f"Territory {ter_id} not found")

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "name": "name", "region": "region",
        "territory_type": "territory_type",
        "parent_territory_id": "parent_territory_id",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    if not updates:
        err("No fields to update")

    updates.append("updated_at = ?")
    params.append(_now_iso())
    params.append(ter_id)
    conn.execute(f"UPDATE crmadv_territory SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, SKILL, "update-territory", "crmadv_territory", ter_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": ter_id, "updated_fields": changed})


# ===========================================================================
# 3. get-territory
# ===========================================================================
def get_territory(conn, args):
    ter_id = getattr(args, "territory_id", None)
    if not ter_id:
        err("--territory-id is required")
    row = conn.execute("SELECT * FROM crmadv_territory WHERE id = ?", (ter_id,)).fetchone()
    if not row:
        err(f"Territory {ter_id} not found")
    ok(row_to_dict(row))


# ===========================================================================
# 4. list-territories
# ===========================================================================
def list_territories(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "territory_type", None):
        where.append("territory_type = ?")
        params.append(args.territory_type)
    if getattr(args, "search", None):
        where.append("(name LIKE ? OR region LIKE ?)")
        params.extend([f"%{args.search}%"] * 2)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM crmadv_territory WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM crmadv_territory WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 5. add-territory-assignment
# ===========================================================================
def add_territory_assignment(conn, args):
    territory_id = getattr(args, "territory_id", None)
    if not territory_id:
        err("--territory-id is required")
    if not conn.execute("SELECT id FROM crmadv_territory WHERE id = ?", (territory_id,)).fetchone():
        err(f"Territory {territory_id} not found")

    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")

    salesperson = getattr(args, "salesperson", None)
    if not salesperson:
        err("--salesperson is required")

    ta_id = str(uuid.uuid4())
    now = _now_iso()

    conn.execute("""
        INSERT INTO crmadv_territory_assignment (
            id, territory_id, salesperson, start_date, end_date,
            assignment_status, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?)
    """, (
        ta_id, territory_id, salesperson,
        getattr(args, "start_date", None),
        getattr(args, "end_date", None),
        "active", company_id, now,
    ))
    audit(conn, SKILL, "add-territory-assignment", "crmadv_territory_assignment", ta_id,
          new_values={"territory_id": territory_id, "salesperson": salesperson})
    conn.commit()
    ok({"id": ta_id, "territory_id": territory_id, "salesperson": salesperson, "assignment_status": "active"})


# ===========================================================================
# 6. list-territory-assignments
# ===========================================================================
def list_territory_assignments(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "territory_id", None):
        where.append("territory_id = ?")
        params.append(args.territory_id)
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM crmadv_territory_assignment WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM crmadv_territory_assignment WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 7. set-territory-quota
# ===========================================================================
def set_territory_quota(conn, args):
    territory_id = getattr(args, "territory_id", None)
    if not territory_id:
        err("--territory-id is required")
    if not conn.execute("SELECT id FROM crmadv_territory WHERE id = ?", (territory_id,)).fetchone():
        err(f"Territory {territory_id} not found")

    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")

    period = getattr(args, "period", None)
    if not period:
        err("--period is required")

    quota_amount = getattr(args, "quota_amount", None)
    if not quota_amount:
        err("--quota-amount is required")

    # Check if quota already exists for this territory+period
    existing = conn.execute(
        "SELECT id FROM crmadv_territory_quota WHERE territory_id = ? AND period = ? AND company_id = ?",
        (territory_id, period, company_id)
    ).fetchone()

    now = _now_iso()
    action_taken = None
    if existing:
        q_id = existing[0]
        action_taken = "updated"
        conn.execute("""
            UPDATE crmadv_territory_quota
            SET quota_amount = ?, updated_at = ?
            WHERE id = ?
        """, (quota_amount, now, q_id))
        audit(conn, SKILL, "set-territory-quota", "crmadv_territory_quota", q_id,
              new_values={"quota_amount": quota_amount, "period": period, "action": "updated"})
    else:
        q_id = str(uuid.uuid4())
        action_taken = "created"
        conn.execute("""
            INSERT INTO crmadv_territory_quota (
                id, territory_id, period, quota_amount, actual_amount,
                attainment_pct, company_id, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            q_id, territory_id, period, quota_amount,
            "0", "0", company_id, now, now,
        ))
        audit(conn, SKILL, "set-territory-quota", "crmadv_territory_quota", q_id,
              new_values={"quota_amount": quota_amount, "period": period, "action": "created"})

    conn.commit()
    ok({"id": q_id, "territory_id": territory_id, "period": period,
        "quota_amount": quota_amount, "action": action_taken})


# ===========================================================================
# 8. list-territory-quotas
# ===========================================================================
def list_territory_quotas(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "territory_id", None):
        where.append("territory_id = ?")
        params.append(args.territory_id)
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM crmadv_territory_quota WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM crmadv_territory_quota WHERE {where_sql} ORDER BY period DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 9. territory-performance-report
# ===========================================================================
def territory_performance_report(conn, args):
    _validate_company(conn, args.company_id)

    total = conn.execute(
        "SELECT COUNT(*) FROM crmadv_territory WHERE company_id = ? AND territory_status = 'active'",
        (args.company_id,)
    ).fetchone()[0]

    rows = conn.execute("""
        SELECT t.id, t.naming_series, t.name, t.region, t.territory_type,
               COALESCE(q.quota_amount, '0') as quota_amount,
               COALESCE(q.actual_amount, '0') as actual_amount,
               COALESCE(q.attainment_pct, '0') as attainment_pct,
               q.period,
               (SELECT COUNT(*) FROM crmadv_territory_assignment ta
                WHERE ta.territory_id = t.id AND ta.assignment_status = 'active') as active_reps
        FROM crmadv_territory t
        LEFT JOIN crmadv_territory_quota q ON t.id = q.territory_id
        WHERE t.company_id = ? AND t.territory_status = 'active'
        ORDER BY t.name
        LIMIT ? OFFSET ?
    """, (args.company_id, args.limit, args.offset)).fetchall()

    ok({
        "rows": [row_to_dict(r) for r in rows],
        "count": len(rows),
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 10. territory-comparison-report
# ===========================================================================
def territory_comparison_report(conn, args):
    _validate_company(conn, args.company_id)

    rows = conn.execute("""
        SELECT t.id, t.name, t.region, t.territory_type,
               COUNT(DISTINCT ta.id) as total_reps,
               COUNT(DISTINCT tq.id) as quota_periods,
               COALESCE(SUM(CAST(tq.actual_amount AS REAL)), 0) as total_actual,
               COALESCE(SUM(CAST(tq.quota_amount AS REAL)), 0) as total_quota
        FROM crmadv_territory t
        LEFT JOIN crmadv_territory_assignment ta ON t.id = ta.territory_id AND ta.assignment_status = 'active'
        LEFT JOIN crmadv_territory_quota tq ON t.id = tq.territory_id
        WHERE t.company_id = ? AND t.territory_status = 'active'
        GROUP BY t.id
        ORDER BY total_actual DESC
        LIMIT ? OFFSET ?
    """, (args.company_id, args.limit, args.offset)).fetchall()

    results = []
    for r in rows:
        d = row_to_dict(r)
        total_quota = float(d.get("total_quota", 0) or 0)
        total_actual = float(d.get("total_actual", 0) or 0)
        attainment = round(total_actual / total_quota * 100, 1) if total_quota > 0 else 0.0
        d["overall_attainment_pct"] = attainment
        d["attainment_pct"] = attainment
        d["total_actual"] = str(round_currency(to_decimal(str(total_actual))))
        d["total_quota"] = str(round_currency(to_decimal(str(total_quota))))
        results.append(d)

    total = conn.execute(
        "SELECT COUNT(*) FROM crmadv_territory WHERE company_id = ? AND territory_status = 'active'",
        (args.company_id,)
    ).fetchone()[0]

    ok({
        "rows": results,
        "count": len(results),
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "add-territory": add_territory,
    "update-territory": update_territory,
    "get-territory": get_territory,
    "list-territories": list_territories,
    "add-territory-assignment": add_territory_assignment,
    "list-territory-assignments": list_territory_assignments,
    "set-territory-quota": set_territory_quota,
    "list-territory-quotas": list_territory_quotas,
    "territory-performance-report": territory_performance_report,
    "territory-comparison-report": territory_comparison_report,
}
