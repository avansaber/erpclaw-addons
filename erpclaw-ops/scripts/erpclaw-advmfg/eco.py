"""ERPClaw Advanced Manufacturing -- ECO domain module.

Engineering Change Orders: create, review, approve, implement, reject.
8 actions exported via ACTIONS dict.
"""
import os
import sys
import uuid

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit

SKILL = "erpclaw-advmfg"

VALID_ECO_TYPES = (
    "design", "process", "material", "quality", "cost_reduction", "other",
)
VALID_ECO_STATUSES = (
    "draft", "review", "approved", "in_progress", "implemented", "rejected", "cancelled",
)
VALID_PRIORITIES = ("critical", "high", "medium", "low")


# ---------------------------------------------------------------------------
# add-eco
# ---------------------------------------------------------------------------
def add_eco(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "title", None):
        err("--title is required")

    if not conn.execute(
        "SELECT id FROM company WHERE id = ?", (args.company_id,)
    ).fetchone():
        err(f"Company {args.company_id} not found")

    eco_type = getattr(args, "eco_type", None) or "design"
    if eco_type not in VALID_ECO_TYPES:
        err(f"Invalid eco-type: {eco_type}")

    priority = getattr(args, "priority", None) or "medium"
    if priority not in VALID_PRIORITIES:
        err(f"Invalid priority: {priority}")

    eco_id = str(uuid.uuid4())
    ns = get_next_name(conn, "engineering_change_order", company_id=args.company_id)

    conn.execute(
        """INSERT INTO engineering_change_order
           (id, naming_series, title, eco_type, description, reason,
            affected_items, affected_boms, impact_analysis,
            requested_by, priority, status, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            eco_id, ns, args.title, eco_type,
            getattr(args, "description", None),
            getattr(args, "reason", None),
            getattr(args, "affected_items", None),
            getattr(args, "affected_boms", None),
            getattr(args, "impact_analysis", None),
            getattr(args, "requested_by", None),
            priority, "draft", args.company_id,
        ),
    )
    audit(conn, SKILL, "add-eco", "engineering_change_order", eco_id,
          new_values={"title": args.title, "naming_series": ns})
    conn.commit()
    ok({"eco_id": eco_id, "naming_series": ns, "eco_status": "draft"})


# ---------------------------------------------------------------------------
# update-eco
# ---------------------------------------------------------------------------
def update_eco(conn, args):
    eco_id = getattr(args, "eco_id", None)
    if not eco_id:
        err("--eco-id is required")
    row = conn.execute(
        "SELECT * FROM engineering_change_order WHERE id = ?", (eco_id,)
    ).fetchone()
    if not row:
        err(f"ECO {eco_id} not found")
    if row["status"] in ("implemented", "rejected", "cancelled"):
        err(f"Cannot update ECO in status '{row['status']}'")

    updates, params, changed = [], [], []

    for field, attr in [
        ("title", "title"),
        ("description", "description"),
        ("reason", "reason"),
        ("affected_items", "affected_items"),
        ("affected_boms", "affected_boms"),
        ("impact_analysis", "impact_analysis"),
        ("requested_by", "requested_by"),
        ("implementation_date", "implementation_date"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            updates.append(f"{field} = ?")
            params.append(val)
            changed.append(field)

    et = getattr(args, "eco_type", None)
    if et is not None:
        if et not in VALID_ECO_TYPES:
            err(f"Invalid eco-type: {et}")
        updates.append("eco_type = ?")
        params.append(et)
        changed.append("eco_type")

    pr = getattr(args, "priority", None)
    if pr is not None:
        if pr not in VALID_PRIORITIES:
            err(f"Invalid priority: {pr}")
        updates.append("priority = ?")
        params.append(pr)
        changed.append("priority")

    if not changed:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(eco_id)
    conn.execute(
        f"UPDATE engineering_change_order SET {', '.join(updates)} WHERE id = ?", params
    )
    audit(conn, SKILL, "update-eco", "engineering_change_order", eco_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"eco_id": eco_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# get-eco
# ---------------------------------------------------------------------------
def get_eco(conn, args):
    eco_id = getattr(args, "eco_id", None)
    if not eco_id:
        err("--eco-id is required")
    row = conn.execute(
        "SELECT * FROM engineering_change_order WHERE id = ?", (eco_id,)
    ).fetchone()
    if not row:
        err(f"ECO {eco_id} not found")

    data = row_to_dict(row)
    data["eco_status"] = data.pop("status", "draft")
    ok(data)


# ---------------------------------------------------------------------------
# list-ecos
# ---------------------------------------------------------------------------
def list_ecos(conn, args):
    conditions, params = [], []
    company_id = getattr(args, "company_id", None)
    if company_id:
        conditions.append("company_id = ?")
        params.append(company_id)
    eco_type = getattr(args, "eco_type", None)
    if eco_type:
        conditions.append("eco_type = ?")
        params.append(eco_type)
    eco_status = getattr(args, "eco_status", None)
    if eco_status:
        conditions.append("status = ?")
        params.append(eco_status)
    priority = getattr(args, "priority", None)
    if priority:
        conditions.append("priority = ?")
        params.append(priority)
    search = getattr(args, "search", None)
    if search:
        conditions.append("(title LIKE ? OR description LIKE ? OR reason LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    total = conn.execute(
        f"SELECT COUNT(*) as cnt FROM engineering_change_order {where}", params
    ).fetchone()["cnt"]

    rows = conn.execute(
        f"SELECT * FROM engineering_change_order {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    ecos = []
    for r in rows:
        d = row_to_dict(r)
        d["eco_status"] = d.pop("status", "draft")
        ecos.append(d)

    ok({"ecos": ecos, "total_count": total, "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# submit-eco-for-review
# ---------------------------------------------------------------------------
def submit_eco_for_review(conn, args):
    eco_id = getattr(args, "eco_id", None)
    if not eco_id:
        err("--eco-id is required")
    row = conn.execute(
        "SELECT status FROM engineering_change_order WHERE id = ?", (eco_id,)
    ).fetchone()
    if not row:
        err(f"ECO {eco_id} not found")
    if row["status"] != "draft":
        err(f"Cannot submit ECO for review in status '{row['status']}'. Must be draft")

    conn.execute(
        "UPDATE engineering_change_order SET status = 'review', updated_at = datetime('now') WHERE id = ?",
        (eco_id,),
    )
    audit(conn, SKILL, "submit-eco-for-review", "engineering_change_order", eco_id,
          new_values={"eco_status": "review"})
    conn.commit()
    ok({"eco_id": eco_id, "eco_status": "review"})


# ---------------------------------------------------------------------------
# approve-eco
# ---------------------------------------------------------------------------
def approve_eco(conn, args):
    eco_id = getattr(args, "eco_id", None)
    if not eco_id:
        err("--eco-id is required")
    row = conn.execute(
        "SELECT status FROM engineering_change_order WHERE id = ?", (eco_id,)
    ).fetchone()
    if not row:
        err(f"ECO {eco_id} not found")
    if row["status"] != "review":
        err(f"Cannot approve ECO in status '{row['status']}'. Must be in review")

    approved_by = getattr(args, "approved_by", None)
    update_sql = "UPDATE engineering_change_order SET status = 'approved', updated_at = datetime('now')"
    params = []
    if approved_by:
        update_sql += ", approved_by = ?"
        params.append(approved_by)
    update_sql += " WHERE id = ?"
    params.append(eco_id)

    conn.execute(update_sql, params)
    audit(conn, SKILL, "approve-eco", "engineering_change_order", eco_id,
          new_values={"eco_status": "approved", "approved_by": approved_by})
    conn.commit()
    ok({"eco_id": eco_id, "eco_status": "approved", "approved_by": approved_by})


# ---------------------------------------------------------------------------
# implement-eco
# ---------------------------------------------------------------------------
def implement_eco(conn, args):
    eco_id = getattr(args, "eco_id", None)
    if not eco_id:
        err("--eco-id is required")
    row = conn.execute(
        "SELECT status FROM engineering_change_order WHERE id = ?", (eco_id,)
    ).fetchone()
    if not row:
        err(f"ECO {eco_id} not found")
    if row["status"] not in ("approved", "in_progress"):
        err(f"Cannot implement ECO in status '{row['status']}'. Must be approved or in_progress")

    conn.execute(
        "UPDATE engineering_change_order SET status = 'implemented', updated_at = datetime('now') WHERE id = ?",
        (eco_id,),
    )
    audit(conn, SKILL, "implement-eco", "engineering_change_order", eco_id,
          new_values={"eco_status": "implemented"})
    conn.commit()
    ok({"eco_id": eco_id, "eco_status": "implemented"})


# ---------------------------------------------------------------------------
# reject-eco
# ---------------------------------------------------------------------------
def reject_eco(conn, args):
    eco_id = getattr(args, "eco_id", None)
    if not eco_id:
        err("--eco-id is required")
    row = conn.execute(
        "SELECT status FROM engineering_change_order WHERE id = ?", (eco_id,)
    ).fetchone()
    if not row:
        err(f"ECO {eco_id} not found")
    if row["status"] not in ("draft", "review"):
        err(f"Cannot reject ECO in status '{row['status']}'. Must be draft or review")

    notes = getattr(args, "notes", None)
    update_sql = "UPDATE engineering_change_order SET status = 'rejected', updated_at = datetime('now')"
    params = []
    if notes:
        # Store rejection reason in description if no dedicated field
        update_sql += ", reason = COALESCE(reason, '') || ' [Rejected: ' || ? || ']'"
        params.append(notes)
    update_sql += " WHERE id = ?"
    params.append(eco_id)

    conn.execute(update_sql, params)
    audit(conn, SKILL, "reject-eco", "engineering_change_order", eco_id,
          new_values={"eco_status": "rejected"})
    conn.commit()
    ok({"eco_id": eco_id, "eco_status": "rejected"})


# ---------------------------------------------------------------------------
# ACTIONS registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "add-eco": add_eco,
    "update-eco": update_eco,
    "get-eco": get_eco,
    "list-ecos": list_ecos,
    "submit-eco-for-review": submit_eco_for_review,
    "approve-eco": approve_eco,
    "implement-eco": implement_eco,
    "reject-eco": reject_eco,
}
