"""ERPClaw Maintenance — Reports domain module.

7 actions: equipment-status-report, maintenance-cost-report,
pm-compliance-report, downtime-report, spare-parts-usage,
equipment-history, status.
"""
import os
import sys
import sqlite3
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.response import ok, err

SKILL = "erpclaw-maintenance"


def equipment_status_report(conn, args):
    """Equipment counts grouped by status."""
    company_id = getattr(args, "company_id", None)

    where = ""
    params = []
    if company_id:
        where = "WHERE company_id = ?"
        params = [company_id]

    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"""SELECT status, COUNT(*) as count FROM equipment {where}
            GROUP BY status ORDER BY count DESC""",
        params,
    ).fetchall()

    total = sum(r["count"] for r in rows)
    breakdown = {r["status"]: r["count"] for r in rows}

    ok({
        "total_equipment": total,
        "by_status": breakdown,
        "company_id": company_id,
    })


def maintenance_cost_report(conn, args):
    """Maintenance costs grouped by equipment, with optional date range."""
    company_id = getattr(args, "company_id", None)
    from_date = getattr(args, "from_date", None)
    to_date = getattr(args, "to_date", None)
    equipment_id = getattr(args, "equipment_id", None)

    where = ["wo.status = 'completed'"]
    params = []

    if company_id:
        where.append("wo.company_id = ?")
        params.append(company_id)
    if equipment_id:
        where.append("wo.equipment_id = ?")
        params.append(equipment_id)
    if from_date:
        where.append("wo.completed_at >= ?")
        params.append(from_date)
    if to_date:
        where.append("wo.completed_at <= ?")
        params.append(to_date)

    where_sql = " AND ".join(where)

    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"""SELECT e.name as equipment_name, wo.equipment_id,
                   COUNT(wo.id) as work_order_count,
                   SUM(CAST(COALESCE(wo.actual_cost, '0') AS REAL)) as total_cost
            FROM maintenance_work_order wo
            LEFT JOIN equipment e ON wo.equipment_id = e.id
            WHERE {where_sql}
            GROUP BY wo.equipment_id
            ORDER BY total_cost DESC""",
        params,
    ).fetchall()

    items = []
    grand_total = Decimal("0")
    for r in rows:
        cost = Decimal(str(r["total_cost"])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        grand_total += cost
        items.append({
            "equipment_id": r["equipment_id"],
            "equipment_name": r["equipment_name"],
            "work_order_count": r["work_order_count"],
            "total_cost": str(cost),
        })

    ok({
        "items": items,
        "grand_total": str(grand_total),
        "from_date": from_date,
        "to_date": to_date,
    })


def pm_compliance_report(conn, args):
    """Preventive maintenance compliance — on-time vs overdue PMs."""
    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")

    conn.row_factory = sqlite3.Row

    # Active plans
    plans = conn.execute(
        """SELECT mp.*, e.name as equipment_name FROM maintenance_plan mp
           LEFT JOIN equipment e ON mp.equipment_id = e.id
           WHERE mp.company_id = ? AND mp.is_active = 1""",
        (company_id,),
    ).fetchall()

    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    on_time = 0
    overdue = 0
    no_schedule = 0
    overdue_plans = []

    for p in plans:
        if not p["next_due"]:
            no_schedule += 1
            continue
        if p["next_due"] <= today:
            # Check if there's a completed WO after last_performed
            if p["last_performed"]:
                overdue += 1
                overdue_plans.append({
                    "plan_id": p["id"],
                    "plan_name": p["name"],
                    "equipment_name": p["equipment_name"],
                    "next_due": p["next_due"],
                    "days_overdue": (datetime.strptime(today, "%Y-%m-%d") -
                                     datetime.strptime(p["next_due"], "%Y-%m-%d")).days,
                })
            else:
                # Never performed = overdue
                overdue += 1
                overdue_plans.append({
                    "plan_id": p["id"],
                    "plan_name": p["name"],
                    "equipment_name": p["equipment_name"],
                    "next_due": p["next_due"],
                    "days_overdue": (datetime.strptime(today, "%Y-%m-%d") -
                                     datetime.strptime(p["next_due"], "%Y-%m-%d")).days,
                })
        else:
            on_time += 1

    total = on_time + overdue + no_schedule
    compliance_pct = round((on_time / total * 100), 1) if total > 0 else 0.0

    ok({
        "total_plans": total,
        "on_time": on_time,
        "overdue": overdue,
        "no_schedule": no_schedule,
        "compliance_pct": compliance_pct,
        "overdue_plans": overdue_plans,
    })


def downtime_report(conn, args):
    """Total downtime hours grouped by equipment."""
    company_id = getattr(args, "company_id", None)
    from_date = getattr(args, "from_date", None)
    to_date = getattr(args, "to_date", None)

    where = []
    params = []

    if company_id:
        where.append("dr.company_id = ?")
        params.append(company_id)
    if from_date:
        where.append("dr.start_time >= ?")
        params.append(from_date)
    if to_date:
        where.append("dr.start_time <= ?")
        params.append(to_date)

    where_sql = " AND ".join(where) if where else "1=1"

    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"""SELECT e.name as equipment_name, dr.equipment_id,
                   COUNT(dr.id) as incident_count,
                   SUM(CAST(COALESCE(dr.duration_hours, '0') AS REAL)) as total_hours
            FROM downtime_record dr
            LEFT JOIN equipment e ON dr.equipment_id = e.id
            WHERE {where_sql}
            GROUP BY dr.equipment_id
            ORDER BY total_hours DESC""",
        params,
    ).fetchall()

    items = []
    grand_total = Decimal("0")
    for r in rows:
        hours = Decimal(str(r["total_hours"])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        grand_total += hours
        items.append({
            "equipment_id": r["equipment_id"],
            "equipment_name": r["equipment_name"],
            "incident_count": r["incident_count"],
            "total_hours": str(hours),
        })

    ok({
        "items": items,
        "grand_total_hours": str(grand_total),
        "from_date": from_date,
        "to_date": to_date,
    })


def spare_parts_usage(conn, args):
    """Most used spare parts across work orders."""
    company_id = getattr(args, "company_id", None)
    limit = getattr(args, "limit", None) or 20

    where = ""
    params = []
    if company_id:
        where = "WHERE woi.company_id = ?"
        params = [company_id]

    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"""SELECT woi.item_name,
                   SUM(CAST(woi.quantity AS REAL)) as total_quantity,
                   SUM(CAST(woi.total_cost AS REAL)) as total_cost,
                   COUNT(DISTINCT woi.work_order_id) as used_in_orders
            FROM maintenance_work_order_item woi
            {where}
            GROUP BY woi.item_name
            ORDER BY total_quantity DESC
            LIMIT ?""",
        params + [limit],
    ).fetchall()

    items = []
    for r in rows:
        items.append({
            "item_name": r["item_name"],
            "total_quantity": str(Decimal(str(r["total_quantity"])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
            "total_cost": str(Decimal(str(r["total_cost"])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
            "used_in_orders": r["used_in_orders"],
        })

    ok({"items": items, "total_parts": len(items)})


def equipment_history(conn, args):
    """All work orders for a specific equipment."""
    equipment_id = getattr(args, "equipment_id", None)
    if not equipment_id:
        err("--equipment-id is required")

    conn.row_factory = sqlite3.Row
    eq = conn.execute("SELECT * FROM equipment WHERE id = ?", (equipment_id,)).fetchone()
    if not eq:
        err(f"Equipment {equipment_id} not found")

    limit = getattr(args, "limit", None) or 50
    offset = getattr(args, "offset", None) or 0

    rows = conn.execute(
        """SELECT * FROM maintenance_work_order WHERE equipment_id = ?
           ORDER BY created_at DESC LIMIT ? OFFSET ?""",
        (equipment_id, limit, offset),
    ).fetchall()

    count = conn.execute(
        "SELECT COUNT(*) FROM maintenance_work_order WHERE equipment_id = ?",
        (equipment_id,),
    ).fetchone()[0]

    wo_items = []
    for r in rows:
        d = dict(r)
        d["wo_status"] = d.pop("status", None)
        wo_items.append(d)

    ok({
        "equipment_id": equipment_id,
        "equipment_name": eq["name"],
        "work_orders": wo_items,
        "total_count": count,
        "limit": limit,
        "offset": offset,
    })


def module_status(conn, args):
    """Module status check."""
    ok({
        "skill": "erpclaw-maintenance",
        "version": "1.0.0",
        "actions_available": 39,
        "tables": [
            "equipment", "equipment_reading",
            "maintenance_plan", "maintenance_plan_item",
            "maintenance_work_order", "maintenance_work_order_item",
            "maintenance_checklist", "maintenance_checklist_item",
            "downtime_record",
        ],
    })


ACTIONS = {
    "maintenance-equipment-status-report": equipment_status_report,
    "maintenance-cost-report": maintenance_cost_report,
    "maintenance-pm-compliance-report": pm_compliance_report,
    "maintenance-downtime-report": downtime_report,
    "maintenance-spare-parts-usage": spare_parts_usage,
    "maintenance-equipment-history": equipment_history,
    "status": module_status,
}
