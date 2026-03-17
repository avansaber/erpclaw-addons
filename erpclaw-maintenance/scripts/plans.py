"""ERPClaw Maintenance — Maintenance Plans domain module.

6 actions: add-maintenance-plan, update-maintenance-plan,
get-maintenance-plan, list-maintenance-plans, add-plan-item, list-plan-items.
"""
import os
import sys
import uuid
import sqlite3
from datetime import datetime, timezone

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row, dynamic_update

SKILL = "erpclaw-maintenance"

VALID_PLAN_TYPES = ("preventive", "predictive", "condition_based")
VALID_FREQUENCIES = ("daily", "weekly", "biweekly", "monthly", "quarterly", "semi_annual", "annual")

FREQUENCY_DAYS_MAP = {
    "daily": 1,
    "weekly": 7,
    "biweekly": 14,
    "monthly": 30,
    "quarterly": 90,
    "semi_annual": 182,
    "annual": 365,
}


def add_maintenance_plan(conn, args):
    """Add a new maintenance plan."""
    name = getattr(args, "plan_name", None)
    equipment_id = getattr(args, "equipment_id", None)
    company_id = getattr(args, "company_id", None)

    if not name or not equipment_id or not company_id:
        err("--plan-name, --equipment-id, and --company-id are required")

    # Validate equipment exists
    eq = conn.execute(Q.from_(Table("equipment")).select(Field('id')).where(Field("id") == P()).get_sql(), (equipment_id,)).fetchone()
    if not eq:
        err(f"Equipment {equipment_id} not found")

    plan_type = getattr(args, "plan_type", None) or "preventive"
    if plan_type not in VALID_PLAN_TYPES:
        err(f"Invalid plan_type: {plan_type}. Must be one of: {', '.join(VALID_PLAN_TYPES)}")

    frequency = getattr(args, "frequency", None) or "monthly"
    if frequency not in VALID_FREQUENCIES:
        err(f"Invalid frequency: {frequency}. Must be one of: {', '.join(VALID_FREQUENCIES)}")

    frequency_days = getattr(args, "frequency_days", None)
    if frequency_days is not None:
        try:
            frequency_days = int(frequency_days)
        except (ValueError, TypeError):
            err("--frequency-days must be an integer")
    else:
        frequency_days = FREQUENCY_DAYS_MAP.get(frequency)

    plan_id = str(uuid.uuid4())
    naming = get_next_name(conn, "maintenance_plan", company_id=company_id)
    now = datetime.now(timezone.utc).isoformat()

    estimated_cost = getattr(args, "estimated_cost", None) or "0"
    next_due = getattr(args, "next_due", None)
    is_active_val = getattr(args, "is_active", None)
    is_active = 1 if is_active_val is None else int(is_active_val)

    sql, _ = insert_row("maintenance_plan", {"id": P(), "naming_series": P(), "name": P(), "equipment_id": P(), "plan_type": P(), "frequency": P(), "frequency_days": P(), "last_performed": P(), "next_due": P(), "estimated_duration": P(), "estimated_cost": P(), "assigned_to": P(), "instructions": P(), "is_active": P(), "company_id": P(), "created_at": P(), "updated_at": P()})
    conn.execute(sql,
        (plan_id, naming, name, equipment_id, plan_type,
         frequency, frequency_days,
         getattr(args, "last_performed", None),
         next_due,
         getattr(args, "estimated_duration", None),
         estimated_cost,
         getattr(args, "assigned_to", None),
         getattr(args, "instructions", None),
         is_active, company_id, now, now),
    )
    conn.commit()

    audit(conn, SKILL, "maintenance-add-maintenance-plan", "maintenance_plan", plan_id,
          new_values={"name": name, "equipment_id": equipment_id},
          description=f"Added maintenance plan: {name}")
    conn.commit()

    ok({
        "id": plan_id,
        "naming_series": naming,
        "name": name,
        "equipment_id": equipment_id,
        "plan_type": plan_type,
        "frequency": frequency,
        "frequency_days": frequency_days,
        "next_due": next_due,
        "estimated_cost": estimated_cost,
    })


def update_maintenance_plan(conn, args):
    """Update an existing maintenance plan."""
    plan_id = getattr(args, "plan_id", None)
    if not plan_id:
        err("--plan-id is required")

    row = conn.execute(Q.from_(Table("maintenance_plan")).select(Field('id')).where(Field("id") == P()).get_sql(), (plan_id,)).fetchone()
    if not row:
        err(f"Maintenance plan {plan_id} not found")

    data = {}
    updated_fields = []
    now = datetime.now(timezone.utc).isoformat()

    for field, attr in [
        ("name", "plan_name"), ("plan_type", "plan_type"),
        ("frequency", "frequency"), ("next_due", "next_due"),
        ("estimated_duration", "estimated_duration"),
        ("estimated_cost", "estimated_cost"),
        ("assigned_to", "assigned_to"), ("instructions", "instructions"),
        ("last_performed", "last_performed"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            if field == "plan_type" and val not in VALID_PLAN_TYPES:
                err(f"Invalid plan_type: {val}")
            if field == "frequency" and val not in VALID_FREQUENCIES:
                err(f"Invalid frequency: {val}")
            data[field] = val
            updated_fields.append(field)

    is_active_val = getattr(args, "is_active", None)
    if is_active_val is not None:
        data["is_active"] = int(is_active_val)
        updated_fields.append("is_active")

    frequency_days = getattr(args, "frequency_days", None)
    if frequency_days is not None:
        data["frequency_days"] = int(frequency_days)
        updated_fields.append("frequency_days")

    if not updated_fields:
        err("No fields to update")

    data["updated_at"] = now
    sql, params = dynamic_update("maintenance_plan", data, {"id": plan_id})
    conn.execute(sql, params)
    conn.commit()

    audit(conn, SKILL, "maintenance-update-maintenance-plan", "maintenance_plan", plan_id,
          new_values={f: "updated" for f in updated_fields},
          description=f"Updated plan {plan_id}: {', '.join(updated_fields)}")
    conn.commit()

    ok({"id": plan_id, "updated_fields": updated_fields})


def get_maintenance_plan(conn, args):
    """Get a single maintenance plan by ID."""
    plan_id = getattr(args, "plan_id", None)
    if not plan_id:
        err("--plan-id is required")

    conn.row_factory = sqlite3.Row
    row = conn.execute(Q.from_(Table("maintenance_plan")).select(Table("maintenance_plan").star).where(Field("id") == P()).get_sql(), (plan_id,)).fetchone()
    if not row:
        err(f"Maintenance plan {plan_id} not found")

    data = dict(row)
    # Get plan items
    items = conn.execute(Q.from_(Table("maintenance_plan_item")).select(Table("maintenance_plan_item").star).where(Field("plan_id") == P()).orderby(Field("created_at")).get_sql(), (plan_id,)).fetchall()
    data["items"] = [dict(i) for i in items]
    data["item_count"] = len(items)
    ok(data)


def list_maintenance_plans(conn, args):
    """List maintenance plans with optional filters."""
    company_id = getattr(args, "company_id", None)
    equipment_id = getattr(args, "equipment_id", None)
    plan_type = getattr(args, "plan_type", None)
    is_active_val = getattr(args, "is_active", None)
    search = getattr(args, "search", None)
    limit = getattr(args, "limit", None) or 50
    offset = getattr(args, "offset", None) or 0

    where = []
    params = []

    if company_id:
        where.append("mp.company_id = ?")
        params.append(company_id)
    if equipment_id:
        where.append("mp.equipment_id = ?")
        params.append(equipment_id)
    if plan_type:
        where.append("mp.plan_type = ?")
        params.append(plan_type)
    if is_active_val is not None:
        where.append("mp.is_active = ?")
        params.append(int(is_active_val))
    if search:
        where.append("(LOWER(mp.name) LIKE LOWER(?) OR LOWER(e.name) LIKE LOWER(?))")
        params.extend([f"%{search}%"] * 2)

    where_sql = " AND ".join(where) if where else "1=1"

    conn.row_factory = sqlite3.Row
    count = conn.execute(
        f"""SELECT COUNT(*) FROM maintenance_plan mp
            LEFT JOIN equipment e ON mp.equipment_id = e.id
            WHERE {where_sql}""",
        params,
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT mp.*, e.name as equipment_name FROM maintenance_plan mp
            LEFT JOIN equipment e ON mp.equipment_id = e.id
            WHERE {where_sql} ORDER BY mp.created_at DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    ok({
        "items": [dict(r) for r in rows],
        "total_count": count,
        "limit": limit,
        "offset": offset,
    })


def add_plan_item(conn, args):
    """Add a spare part / item to a maintenance plan."""
    plan_id = getattr(args, "plan_id", None)
    item_name = getattr(args, "item_name", None)
    company_id = getattr(args, "company_id", None)

    if not plan_id or not item_name or not company_id:
        err("--plan-id, --item-name, and --company-id are required")

    plan = conn.execute(Q.from_(Table("maintenance_plan")).select(Field('id')).where(Field("id") == P()).get_sql(), (plan_id,)).fetchone()
    if not plan:
        err(f"Maintenance plan {plan_id} not found")

    item_id_val = str(uuid.uuid4())
    quantity = getattr(args, "quantity", None) or "1"
    now = datetime.now(timezone.utc).isoformat()

    sql, _ = insert_row("maintenance_plan_item", {"id": P(), "plan_id": P(), "item_id": P(), "item_name": P(), "quantity": P(), "notes": P(), "company_id": P(), "created_at": P()})
    conn.execute(sql,
        (item_id_val, plan_id,
         getattr(args, "item_id", None),
         item_name, quantity,
         getattr(args, "notes", None),
         company_id, now),
    )
    conn.commit()

    ok({
        "id": item_id_val,
        "plan_id": plan_id,
        "item_name": item_name,
        "quantity": quantity,
    })


def list_plan_items(conn, args):
    """List items in a maintenance plan."""
    plan_id = getattr(args, "plan_id", None)
    if not plan_id:
        err("--plan-id is required")

    conn.row_factory = sqlite3.Row
    rows = conn.execute(Q.from_(Table("maintenance_plan_item")).select(Table("maintenance_plan_item").star).where(Field("plan_id") == P()).orderby(Field("created_at")).get_sql(), (plan_id,)).fetchall()

    ok({
        "items": [dict(r) for r in rows],
        "total_count": len(rows),
        "plan_id": plan_id,
    })


ACTIONS = {
    "maintenance-add-maintenance-plan": add_maintenance_plan,
    "maintenance-update-maintenance-plan": update_maintenance_plan,
    "maintenance-get-maintenance-plan": get_maintenance_plan,
    "maintenance-list-maintenance-plans": list_maintenance_plans,
    "maintenance-add-plan-item": add_plan_item,
    "maintenance-list-plan-items": list_plan_items,
}
