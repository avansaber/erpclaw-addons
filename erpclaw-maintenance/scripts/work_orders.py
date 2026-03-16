"""ERPClaw Maintenance — Work Orders domain module.

12 actions: add-maintenance-work-order, update-maintenance-work-order,
get-maintenance-work-order, list-maintenance-work-orders, add-wo-item,
list-wo-items, start-maintenance-work-order, complete-maintenance-work-order,
cancel-maintenance-work-order, generate-preventive-work-orders,
add-downtime-record, list-downtime-records.
"""
import os
import sys
import uuid
import sqlite3
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err
from erpclaw_lib.audit import audit
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, LiteralValue, insert_row, update_row, dynamic_update

SKILL = "erpclaw-maintenance"

VALID_WO_TYPES = ("preventive", "corrective", "emergency", "inspection")
VALID_PRIORITIES = ("critical", "high", "medium", "low")
VALID_WO_STATUSES = ("draft", "scheduled", "in_progress", "completed", "cancelled")
VALID_DOWNTIME_REASONS = ("breakdown", "maintenance", "setup", "changeover", "other")

FREQUENCY_DAYS_MAP = {
    "daily": 1,
    "weekly": 7,
    "biweekly": 14,
    "monthly": 30,
    "quarterly": 90,
    "semi_annual": 182,
    "annual": 365,
}


def add_maintenance_work_order(conn, args):
    """Add a new maintenance work order."""
    equipment_id = getattr(args, "equipment_id", None)
    company_id = getattr(args, "company_id", None)

    if not equipment_id or not company_id:
        err("--equipment-id and --company-id are required")

    eq = conn.execute(Q.from_(Table("equipment")).select(Field('id')).where(Field("id") == P()).get_sql(), (equipment_id,)).fetchone()
    if not eq:
        err(f"Equipment {equipment_id} not found")

    wo_type = getattr(args, "work_order_type", None) or "corrective"
    if wo_type not in VALID_WO_TYPES:
        err(f"Invalid work_order_type: {wo_type}. Must be one of: {', '.join(VALID_WO_TYPES)}")

    priority = getattr(args, "priority", None) or "medium"
    if priority not in VALID_PRIORITIES:
        err(f"Invalid priority: {priority}. Must be one of: {', '.join(VALID_PRIORITIES)}")

    wo_id = str(uuid.uuid4())
    naming = get_next_name(conn, "maintenance_work_order", company_id=company_id)
    now = datetime.now(timezone.utc).isoformat()

    plan_id = getattr(args, "plan_id", None)
    if plan_id:
        plan = conn.execute(Q.from_(Table("maintenance_plan")).select(Field('id')).where(Field("id") == P()).get_sql(), (plan_id,)).fetchone()
        if not plan:
            err(f"Maintenance plan {plan_id} not found")

    wo_status = getattr(args, "wo_status", None) or "draft"
    if wo_status not in VALID_WO_STATUSES:
        err(f"Invalid status: {wo_status}")

    sql, _ = insert_row("maintenance_work_order", {"id": P(), "naming_series": P(), "equipment_id": P(), "plan_id": P(), "work_order_type": P(), "priority": P(), "description": P(), "assigned_to": P(), "scheduled_date": P(), "started_at": P(), "completed_at": P(), "actual_duration": P(), "actual_cost": P(), "failure_mode": P(), "root_cause": P(), "resolution": P(), "status": P(), "company_id": P(), "created_at": P(), "updated_at": P()})
    conn.execute(sql,
        (wo_id, naming, equipment_id, plan_id,
         wo_type, priority,
         getattr(args, "description", None),
         getattr(args, "assigned_to", None),
         getattr(args, "scheduled_date", None),
         None, None, None, "0",
         getattr(args, "failure_mode", None),
         None, None,
         wo_status, company_id, now, now),
    )
    conn.commit()

    audit(conn, SKILL, "maintenance-add-maintenance-work-order", "maintenance_work_order", wo_id,
          new_values={"equipment_id": equipment_id, "type": wo_type},
          description=f"Added work order {naming} for equipment {equipment_id}")
    conn.commit()

    ok({
        "id": wo_id,
        "naming_series": naming,
        "equipment_id": equipment_id,
        "work_order_type": wo_type,
        "priority": priority,
        "wo_status": wo_status,
    })


def update_maintenance_work_order(conn, args):
    """Update a maintenance work order."""
    wo_id = getattr(args, "work_order_id", None)
    if not wo_id:
        err("--work-order-id is required")

    row = conn.execute(Q.from_(Table("maintenance_work_order")).select(Table("maintenance_work_order").star).where(Field("id") == P()).get_sql(), (wo_id,)).fetchone()
    if not row:
        err(f"Work order {wo_id} not found")

    data = {}
    updated_fields = []
    now = datetime.now(timezone.utc).isoformat()

    for field, attr in [
        ("work_order_type", "work_order_type"), ("priority", "priority"),
        ("description", "description"), ("assigned_to", "assigned_to"),
        ("scheduled_date", "scheduled_date"), ("failure_mode", "failure_mode"),
        ("root_cause", "root_cause"), ("resolution", "resolution"),
        ("actual_duration", "actual_duration"), ("actual_cost", "actual_cost"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            if field == "work_order_type" and val not in VALID_WO_TYPES:
                err(f"Invalid work_order_type: {val}")
            if field == "priority" and val not in VALID_PRIORITIES:
                err(f"Invalid priority: {val}")
            data[field] = val
            updated_fields.append(field)

    wo_status = getattr(args, "wo_status", None)
    if wo_status is not None:
        if wo_status not in VALID_WO_STATUSES:
            err(f"Invalid status: {wo_status}")
        data["status"] = wo_status
        updated_fields.append("status")

    if not updated_fields:
        err("No fields to update")

    data["updated_at"] = now
    sql, params = dynamic_update("maintenance_work_order", data, {"id": wo_id})
    conn.execute(sql, params)
    conn.commit()

    ok({"id": wo_id, "updated_fields": updated_fields})


def get_maintenance_work_order(conn, args):
    """Get a single work order by ID."""
    wo_id = getattr(args, "work_order_id", None)
    if not wo_id:
        err("--work-order-id is required")

    conn.row_factory = sqlite3.Row
    row = conn.execute(Q.from_(Table("maintenance_work_order")).select(Table("maintenance_work_order").star).where(Field("id") == P()).get_sql(), (wo_id,)).fetchone()
    if not row:
        err(f"Work order {wo_id} not found")

    data = dict(row)
    data["wo_status"] = data.pop("status", None)

    # Get items
    items = conn.execute(Q.from_(Table("maintenance_work_order_item")).select(Table("maintenance_work_order_item").star).where(Field("work_order_id") == P()).orderby(Field("created_at")).get_sql(), (wo_id,)).fetchall()
    data["items"] = [dict(i) for i in items]
    data["item_count"] = len(items)

    # Get checklists
    checklists = conn.execute(Q.from_(Table("maintenance_checklist")).select(Table("maintenance_checklist").star).where(Field("work_order_id") == P()).orderby(Field("created_at")).get_sql(), (wo_id,)).fetchall()
    data["checklists"] = [dict(c) for c in checklists]

    ok(data)


def list_maintenance_work_orders(conn, args):
    """List work orders with optional filters."""
    company_id = getattr(args, "company_id", None)
    equipment_id = getattr(args, "equipment_id", None)
    wo_status = getattr(args, "wo_status", None)
    wo_type = getattr(args, "work_order_type", None)
    priority = getattr(args, "priority", None)
    plan_id = getattr(args, "plan_id", None)
    search = getattr(args, "search", None)
    limit = getattr(args, "limit", None) or 50
    offset = getattr(args, "offset", None) or 0

    where = []
    params = []

    if company_id:
        where.append("wo.company_id = ?")
        params.append(company_id)
    if equipment_id:
        where.append("wo.equipment_id = ?")
        params.append(equipment_id)
    if wo_status:
        where.append("wo.status = ?")
        params.append(wo_status)
    if wo_type:
        where.append("wo.work_order_type = ?")
        params.append(wo_type)
    if priority:
        where.append("wo.priority = ?")
        params.append(priority)
    if plan_id:
        where.append("wo.plan_id = ?")
        params.append(plan_id)
    if search:
        where.append("(wo.description LIKE ? OR e.name LIKE ?)")
        params.extend([f"%{search}%"] * 2)

    where_sql = " AND ".join(where) if where else "1=1"

    conn.row_factory = sqlite3.Row
    count = conn.execute(
        f"""SELECT COUNT(*) FROM maintenance_work_order wo
            LEFT JOIN equipment e ON wo.equipment_id = e.id
            WHERE {where_sql}""",
        params,
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT wo.*, e.name as equipment_name FROM maintenance_work_order wo
            LEFT JOIN equipment e ON wo.equipment_id = e.id
            WHERE {where_sql} ORDER BY wo.created_at DESC LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    items = []
    for r in rows:
        d = dict(r)
        d["wo_status"] = d.pop("status", None)
        items.append(d)

    ok({
        "items": items,
        "total_count": count,
        "limit": limit,
        "offset": offset,
    })


def add_wo_item(conn, args):
    """Add a spare part / item to a work order."""
    work_order_id = getattr(args, "work_order_id", None)
    item_name = getattr(args, "item_name", None)
    company_id = getattr(args, "company_id", None)

    if not work_order_id or not item_name or not company_id:
        err("--work-order-id, --item-name, and --company-id are required")

    wo = conn.execute(Q.from_(Table("maintenance_work_order")).select(Field('id')).where(Field("id") == P()).get_sql(), (work_order_id,)).fetchone()
    if not wo:
        err(f"Work order {work_order_id} not found")

    item_row_id = str(uuid.uuid4())
    quantity = getattr(args, "quantity", None) or "1"
    unit_cost = getattr(args, "unit_cost", None) or "0"

    # Calculate total cost
    try:
        qty_dec = Decimal(quantity)
        uc_dec = Decimal(unit_cost)
        total_cost = str((qty_dec * uc_dec).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    except Exception:
        total_cost = "0"

    now = datetime.now(timezone.utc).isoformat()

    sql, _ = insert_row("maintenance_work_order_item", {"id": P(), "work_order_id": P(), "item_id": P(), "item_name": P(), "quantity": P(), "unit_cost": P(), "total_cost": P(), "notes": P(), "company_id": P(), "created_at": P()})
    conn.execute(sql,
        (item_row_id, work_order_id,
         getattr(args, "item_id", None),
         item_name, quantity, unit_cost, total_cost,
         getattr(args, "notes", None),
         company_id, now),
    )
    conn.commit()

    ok({
        "id": item_row_id,
        "work_order_id": work_order_id,
        "item_name": item_name,
        "quantity": quantity,
        "unit_cost": unit_cost,
        "total_cost": total_cost,
    })


def list_wo_items(conn, args):
    """List items in a work order."""
    work_order_id = getattr(args, "work_order_id", None)
    if not work_order_id:
        err("--work-order-id is required")

    conn.row_factory = sqlite3.Row
    rows = conn.execute(Q.from_(Table("maintenance_work_order_item")).select(Table("maintenance_work_order_item").star).where(Field("work_order_id") == P()).orderby(Field("created_at")).get_sql(), (work_order_id,)).fetchall()

    ok({
        "items": [dict(r) for r in rows],
        "total_count": len(rows),
        "work_order_id": work_order_id,
    })


def start_maintenance_work_order(conn, args):
    """Start a work order (draft/scheduled -> in_progress)."""
    wo_id = getattr(args, "work_order_id", None)
    if not wo_id:
        err("--work-order-id is required")

    conn.row_factory = sqlite3.Row
    row = conn.execute(Q.from_(Table("maintenance_work_order")).select(Table("maintenance_work_order").star).where(Field("id") == P()).get_sql(), (wo_id,)).fetchone()
    if not row:
        err(f"Work order {wo_id} not found")

    current_status = row["status"]
    if current_status not in ("draft", "scheduled"):
        err(f"Cannot start work order in '{current_status}' status. Must be 'draft' or 'scheduled'.")

    now = datetime.now(timezone.utc).isoformat()

    sql = update_row("maintenance_work_order",
                     data={"status": P(), "started_at": P(), "updated_at": P()},
                     where={"id": P()})
    conn.execute(sql, ("in_progress", now, now, wo_id))

    # Update equipment status to 'maintenance'
    sql = update_row("equipment",
                     data={"status": P(), "updated_at": P()},
                     where={"id": P()})
    conn.execute(sql, ("maintenance", now, row["equipment_id"]))
    conn.commit()

    audit(conn, SKILL, "maintenance-start-maintenance-work-order", "maintenance_work_order", wo_id,
          old_values={"status": current_status},
          new_values={"status": "in_progress"},
          description=f"Started work order {wo_id}")
    conn.commit()

    ok({
        "id": wo_id,
        "wo_status": "in_progress",
        "started_at": now,
        "equipment_id": row["equipment_id"],
    })


def complete_maintenance_work_order(conn, args):
    """Complete a work order (in_progress -> completed).

    Updates equipment status to operational, and if linked to a plan,
    updates plan.last_performed and calculates next_due.
    """
    wo_id = getattr(args, "work_order_id", None)
    if not wo_id:
        err("--work-order-id is required")

    conn.row_factory = sqlite3.Row
    row = conn.execute(Q.from_(Table("maintenance_work_order")).select(Table("maintenance_work_order").star).where(Field("id") == P()).get_sql(), (wo_id,)).fetchone()
    if not row:
        err(f"Work order {wo_id} not found")

    current_status = row["status"]
    if current_status != "in_progress":
        err(f"Cannot complete work order in '{current_status}' status. Must be 'in_progress'.")

    now = datetime.now(timezone.utc).isoformat()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Calculate actual duration if started_at is set
    actual_duration = getattr(args, "actual_duration", None)
    actual_cost = getattr(args, "actual_cost", None) or row["actual_cost"] or "0"
    resolution = getattr(args, "resolution", None)
    root_cause = getattr(args, "root_cause", None)

    # Calculate total cost from WO items
    woi = Table("maintenance_work_order_item")
    items_cost_row = conn.execute(
        Q.from_(woi).select(
            fn.Coalesce(fn.Sum(LiteralValue("CAST(\"total_cost\" AS REAL)")), 0)
        ).where(woi.work_order_id == P()).get_sql(),
        (wo_id,),
    ).fetchone()
    items_cost = str(Decimal(str(items_cost_row[0])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    if actual_cost == "0" and items_cost != "0.00":
        actual_cost = items_cost

    # PyPika: skipped — too complex (COALESCE with column fallback in SET)
    update_params = [now, actual_duration, actual_cost, resolution, root_cause, now, wo_id]
    conn.execute(
        """UPDATE "maintenance_work_order" SET "status" = 'completed', "completed_at" = ?,
           "actual_duration" = COALESCE(?, "actual_duration"),
           "actual_cost" = ?, "resolution" = COALESCE(?, "resolution"),
           "root_cause" = COALESCE(?, "root_cause"),
           "updated_at" = ? WHERE "id" = ?""",
        update_params,
    )

    # Update equipment status back to operational
    sql = update_row("equipment",
                     data={"status": P(), "updated_at": P()},
                     where={"id": P()})
    conn.execute(sql, ("operational", now, row["equipment_id"]))

    # Update plan if linked
    plan_updated = False
    if row["plan_id"]:
        plan = conn.execute(Q.from_(Table("maintenance_plan")).select(Table("maintenance_plan").star).where(Field("id") == P()).get_sql(), (row["plan_id"],)).fetchone()
        if plan:
            freq_days = plan["frequency_days"]
            if not freq_days:
                freq_days = FREQUENCY_DAYS_MAP.get(plan["frequency"], 30)
            next_due_date = datetime.now(timezone.utc) + timedelta(days=freq_days)
            next_due_str = next_due_date.strftime("%Y-%m-%d")

            plan_upd = update_row("maintenance_plan",
                                 data={"last_performed": P(), "next_due": P(), "updated_at": P()},
                                 where={"id": P()})
            conn.execute(plan_upd, (today_str, next_due_str, now, row["plan_id"]))
            plan_updated = True

    conn.commit()

    audit(conn, SKILL, "maintenance-complete-maintenance-work-order", "maintenance_work_order", wo_id,
          old_values={"status": current_status},
          new_values={"status": "completed", "actual_cost": actual_cost},
          description=f"Completed work order {wo_id}")
    conn.commit()

    result = {
        "id": wo_id,
        "wo_status": "completed",
        "completed_at": now,
        "actual_cost": actual_cost,
        "equipment_id": row["equipment_id"],
        "plan_updated": plan_updated,
    }
    if plan_updated:
        result["plan_id"] = row["plan_id"]
    ok(result)


def cancel_maintenance_work_order(conn, args):
    """Cancel a work order (any non-completed status -> cancelled)."""
    wo_id = getattr(args, "work_order_id", None)
    if not wo_id:
        err("--work-order-id is required")

    row = conn.execute(Q.from_(Table("maintenance_work_order")).select(Field('status'), Field('equipment_id')).where(Field("id") == P()).get_sql(), (wo_id,)).fetchone()
    if not row:
        err(f"Work order {wo_id} not found")

    current_status = row[0]
    if current_status in ("completed", "cancelled"):
        err(f"Cannot cancel work order in '{current_status}' status")

    now = datetime.now(timezone.utc).isoformat()
    sql = update_row("maintenance_work_order",
                     data={"status": P(), "updated_at": P()},
                     where={"id": P()})
    conn.execute(sql, ("cancelled", now, wo_id))

    # If equipment was in maintenance, restore to operational
    if current_status == "in_progress":
        sql = update_row("equipment",
                         data={"status": P(), "updated_at": P()},
                         where={"id": P()})
        conn.execute(sql, ("operational", now, row[1]))

    conn.commit()

    audit(conn, SKILL, "maintenance-cancel-maintenance-work-order", "maintenance_work_order", wo_id,
          old_values={"status": current_status},
          new_values={"status": "cancelled"},
          description=f"Cancelled work order {wo_id}")
    conn.commit()

    ok({"id": wo_id, "wo_status": "cancelled"})


def generate_preventive_work_orders(conn, args):
    """Scan due maintenance plans and create work orders for each."""
    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    as_of = getattr(args, "as_of_date", None) or today_str

    conn.row_factory = sqlite3.Row

    # Find plans that are due (next_due <= as_of) and active
    mp = Table("maintenance_plan")
    plans = conn.execute(
        Q.from_(mp).select(mp.star)
        .where(mp.company_id == P())
        .where(mp.is_active == 1)
        .where(mp.next_due.isnotnull())
        .where(mp.next_due <= P()).get_sql(),
        (company_id, as_of),
    ).fetchall()

    created = []
    mwo = Table("maintenance_work_order")
    for plan in plans:
        # Check if there's already an open WO for this plan
        existing = conn.execute(
            Q.from_(mwo).select(mwo.id)
            .where(mwo.plan_id == P())
            .where(mwo.status != "completed")
            .where(mwo.status != "cancelled").get_sql(),
            (plan["id"],),
        ).fetchone()
        if existing:
            continue

        wo_id = str(uuid.uuid4())
        naming = get_next_name(conn, "maintenance_work_order", company_id=company_id)
        now = datetime.now(timezone.utc).isoformat()

        sql, _ = insert_row("maintenance_work_order", {"id": P(), "naming_series": P(), "equipment_id": P(), "plan_id": P(), "work_order_type": P(), "priority": P(), "description": P(), "assigned_to": P(), "scheduled_date": P(), "status": P(), "company_id": P(), "created_at": P(), "updated_at": P()})
        conn.execute(sql,
            (wo_id, naming, plan["equipment_id"], plan["id"],
             "preventive", "medium",
             f"Preventive maintenance per plan: {plan['name']}",
             plan["assigned_to"], plan["next_due"],
             "scheduled", company_id, now, now),
        )
        created.append({
            "id": wo_id,
            "naming_series": naming,
            "equipment_id": plan["equipment_id"],
            "plan_id": plan["id"],
            "plan_name": plan["name"],
            "scheduled_date": plan["next_due"],
        })

    conn.commit()

    ok({
        "generated": len(created),
        "work_orders": created,
        "as_of_date": as_of,
    })


def add_downtime_record(conn, args):
    """Add a downtime record for equipment."""
    equipment_id = getattr(args, "equipment_id", None)
    company_id = getattr(args, "company_id", None)

    if not equipment_id or not company_id:
        err("--equipment-id and --company-id are required")

    eq = conn.execute(Q.from_(Table("equipment")).select(Field('id')).where(Field("id") == P()).get_sql(), (equipment_id,)).fetchone()
    if not eq:
        err(f"Equipment {equipment_id} not found")

    reason = getattr(args, "reason", None) or "breakdown"
    if reason not in VALID_DOWNTIME_REASONS:
        err(f"Invalid reason: {reason}. Must be one of: {', '.join(VALID_DOWNTIME_REASONS)}")

    dt_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    start_time = getattr(args, "start_time", None) or now
    end_time = getattr(args, "end_time", None)
    duration_hours = getattr(args, "duration_hours", None)

    sql, _ = insert_row("downtime_record", {"id": P(), "equipment_id": P(), "work_order_id": P(), "start_time": P(), "end_time": P(), "duration_hours": P(), "reason": P(), "description": P(), "impact": P(), "company_id": P(), "created_at": P(), "updated_at": P()})
    conn.execute(sql,
        (dt_id, equipment_id,
         getattr(args, "work_order_id", None),
         start_time, end_time, duration_hours, reason,
         getattr(args, "description", None),
         getattr(args, "impact", None),
         company_id, now, now),
    )
    conn.commit()

    ok({
        "id": dt_id,
        "equipment_id": equipment_id,
        "reason": reason,
        "start_time": start_time,
        "end_time": end_time,
        "duration_hours": duration_hours,
    })


def list_downtime_records(conn, args):
    """List downtime records with optional filters."""
    equipment_id = getattr(args, "equipment_id", None)
    company_id = getattr(args, "company_id", None)
    work_order_id = getattr(args, "work_order_id", None)
    reason = getattr(args, "reason", None)
    limit = getattr(args, "limit", None) or 50
    offset = getattr(args, "offset", None) or 0

    dt = Table("downtime_record")
    q = Q.from_(dt).select(dt.star)
    q_cnt = Q.from_(dt).select(fn.Count(dt.star))
    params = []

    if equipment_id:
        q = q.where(dt.equipment_id == P())
        q_cnt = q_cnt.where(dt.equipment_id == P())
        params.append(equipment_id)
    if company_id:
        q = q.where(dt.company_id == P())
        q_cnt = q_cnt.where(dt.company_id == P())
        params.append(company_id)
    if work_order_id:
        q = q.where(dt.work_order_id == P())
        q_cnt = q_cnt.where(dt.work_order_id == P())
        params.append(work_order_id)
    if reason:
        q = q.where(dt.reason == P())
        q_cnt = q_cnt.where(dt.reason == P())
        params.append(reason)

    conn.row_factory = sqlite3.Row
    count = conn.execute(q_cnt.get_sql(), params).fetchone()[0]

    q = q.orderby(dt.start_time, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()

    ok({
        "items": [dict(r) for r in rows],
        "total_count": count,
        "limit": limit,
        "offset": offset,
    })


ACTIONS = {
    "maintenance-add-maintenance-work-order": add_maintenance_work_order,
    "maintenance-update-maintenance-work-order": update_maintenance_work_order,
    "maintenance-get-maintenance-work-order": get_maintenance_work_order,
    "maintenance-list-maintenance-work-orders": list_maintenance_work_orders,
    "maintenance-add-wo-item": add_wo_item,
    "maintenance-list-wo-items": list_wo_items,
    "maintenance-start-maintenance-work-order": start_maintenance_work_order,
    "maintenance-complete-maintenance-work-order": complete_maintenance_work_order,
    "maintenance-cancel-maintenance-work-order": cancel_maintenance_work_order,
    "maintenance-generate-preventive-work-orders": generate_preventive_work_orders,
    "maintenance-add-downtime-record": add_downtime_record,
    "maintenance-list-downtime-records": list_downtime_records,
}
