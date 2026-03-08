"""ERPClaw Maintenance — Checklists domain module.

4 actions: add-checklist, get-checklist, add-checklist-item, complete-checklist-item.
"""
import os
import sys
import uuid
import sqlite3
from datetime import datetime, timezone

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.response import ok, err
from erpclaw_lib.audit import audit

SKILL = "erpclaw-maintenance"


def add_checklist(conn, args):
    """Add a checklist to a work order."""
    work_order_id = getattr(args, "work_order_id", None)
    name = getattr(args, "checklist_name", None)
    company_id = getattr(args, "company_id", None)

    if not work_order_id or not name or not company_id:
        err("--work-order-id, --checklist-name, and --company-id are required")

    wo = conn.execute("SELECT id FROM maintenance_work_order WHERE id = ?", (work_order_id,)).fetchone()
    if not wo:
        err(f"Work order {work_order_id} not found")

    cl_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """INSERT INTO maintenance_checklist (id, work_order_id, name, company_id, created_at)
           VALUES (?,?,?,?,?)""",
        (cl_id, work_order_id, name, company_id, now),
    )
    conn.commit()

    ok({
        "id": cl_id,
        "work_order_id": work_order_id,
        "name": name,
    })


def get_checklist(conn, args):
    """Get a checklist with all its items."""
    checklist_id = getattr(args, "checklist_id", None)
    if not checklist_id:
        err("--checklist-id is required")

    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM maintenance_checklist WHERE id = ?", (checklist_id,)).fetchone()
    if not row:
        err(f"Checklist {checklist_id} not found")

    data = dict(row)

    items = conn.execute(
        "SELECT * FROM maintenance_checklist_item WHERE checklist_id = ? ORDER BY sort_order, created_at",
        (checklist_id,),
    ).fetchall()
    data["items"] = [dict(i) for i in items]
    data["item_count"] = len(items)
    data["completed_count"] = sum(1 for i in items if i["is_completed"])

    ok(data)


def add_checklist_item(conn, args):
    """Add an item to a checklist."""
    checklist_id = getattr(args, "checklist_id", None)
    description = getattr(args, "description", None)

    if not checklist_id or not description:
        err("--checklist-id and --description are required")

    cl = conn.execute("SELECT id FROM maintenance_checklist WHERE id = ?", (checklist_id,)).fetchone()
    if not cl:
        err(f"Checklist {checklist_id} not found")

    item_id = str(uuid.uuid4())
    sort_order = getattr(args, "sort_order", None) or 0
    try:
        sort_order = int(sort_order)
    except (ValueError, TypeError):
        sort_order = 0

    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """INSERT INTO maintenance_checklist_item (id, checklist_id, description,
           is_completed, sort_order, notes, created_at)
           VALUES (?,?,?,0,?,?,?)""",
        (item_id, checklist_id, description, sort_order,
         getattr(args, "notes", None), now),
    )
    conn.commit()

    ok({
        "id": item_id,
        "checklist_id": checklist_id,
        "description": description,
        "is_completed": 0,
        "sort_order": sort_order,
    })


def complete_checklist_item(conn, args):
    """Mark a checklist item as completed."""
    item_id = getattr(args, "checklist_item_id", None)
    if not item_id:
        err("--checklist-item-id is required")

    row = conn.execute(
        "SELECT id, is_completed, checklist_id FROM maintenance_checklist_item WHERE id = ?",
        (item_id,),
    ).fetchone()
    if not row:
        err(f"Checklist item {item_id} not found")

    if row[1] == 1:
        err(f"Checklist item {item_id} is already completed")

    now = datetime.now(timezone.utc).isoformat()
    completed_by = getattr(args, "completed_by", None)

    conn.execute(
        """UPDATE maintenance_checklist_item
           SET is_completed = 1, completed_at = ?, completed_by = ?, notes = COALESCE(?, notes)
           WHERE id = ?""",
        (now, completed_by, getattr(args, "notes", None), item_id),
    )
    conn.commit()

    ok({
        "id": item_id,
        "checklist_id": row[2],
        "is_completed": 1,
        "completed_at": now,
        "completed_by": completed_by,
    })


ACTIONS = {
    "maintenance-add-checklist": add_checklist,
    "maintenance-get-checklist": get_checklist,
    "maintenance-add-checklist-item": add_checklist_item,
    "maintenance-complete-checklist-item": complete_checklist_item,
}
