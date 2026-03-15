"""ERPClaw Advanced Manufacturing -- Tools domain module.

Tool inventory, usage tracking, calibration, utilization reports.
8 actions exported via ACTIONS dict.
"""
import os
import sys
import uuid
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row

SKILL = "erpclaw-advmfg"

VALID_TOOL_TYPES = (
    "cutting", "measuring", "holding", "forming", "assembly", "inspection", "other",
)
VALID_CONDITIONS = ("new", "good", "worn", "needs_repair", "scrapped")
VALID_TOOL_STATUSES = ("available", "in_use", "maintenance", "calibration", "scrapped")


# ---------------------------------------------------------------------------
# add-tool
# ---------------------------------------------------------------------------
def add_tool(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "name", None):
        err("--name is required")

    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    tool_type = getattr(args, "tool_type", None) or "cutting"
    if tool_type not in VALID_TOOL_TYPES:
        err(f"Invalid tool-type: {tool_type}")

    tool_id = str(uuid.uuid4())
    ns = get_next_name(conn, "tool", company_id=args.company_id)

    max_usage = getattr(args, "max_usage_count", None)
    if max_usage is not None:
        max_usage = int(max_usage)

    conn.execute(
        """INSERT INTO tool
           (id, naming_series, name, tool_type, tool_code, manufacturer,
            model, serial_number, location, purchase_date, purchase_cost,
            max_usage_count, current_usage_count, calibration_due,
            condition, status, notes, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?,?,?)""",
        (
            tool_id, ns, args.name, tool_type,
            getattr(args, "tool_code", None),
            getattr(args, "manufacturer", None),
            getattr(args, "model", None),
            getattr(args, "serial_number", None),
            getattr(args, "location", None),
            getattr(args, "purchase_date", None),
            getattr(args, "purchase_cost", None) or "0",
            max_usage,
            getattr(args, "calibration_due", None),
            "good", "available",
            getattr(args, "notes", None),
            args.company_id,
        ),
    )
    audit(conn, SKILL, "add-tool", "tool", tool_id,
          new_values={"name": args.name, "naming_series": ns})
    conn.commit()
    ok({"tool_id": tool_id, "naming_series": ns, "tool_status": "available",
        "condition_value": "good"})


# ---------------------------------------------------------------------------
# update-tool
# ---------------------------------------------------------------------------
def update_tool(conn, args):
    tool_id = getattr(args, "tool_id", None)
    if not tool_id:
        err("--tool-id is required")
    row = conn.execute(Q.from_(Table("tool")).select(Table("tool").star).where(Field("id") == P()).get_sql(), (tool_id,)).fetchone()
    if not row:
        err(f"Tool {tool_id} not found")

    updates, params, changed = [], [], []

    for field, attr in [
        ("name", "name"),
        ("tool_code", "tool_code"),
        ("manufacturer", "manufacturer"),
        ("model", "model"),
        ("serial_number", "serial_number"),
        ("location", "location"),
        ("purchase_cost", "purchase_cost"),
        ("calibration_due", "calibration_due"),
        ("notes", "notes"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            updates.append(f"{field} = ?")
            params.append(val)
            changed.append(field)

    tt = getattr(args, "tool_type", None)
    if tt is not None:
        if tt not in VALID_TOOL_TYPES:
            err(f"Invalid tool-type: {tt}")
        updates.append("tool_type = ?")
        params.append(tt)
        changed.append("tool_type")

    cond = getattr(args, "condition", None)
    if cond is not None:
        if cond not in VALID_CONDITIONS:
            err(f"Invalid condition: {cond}")
        updates.append("condition = ?")
        params.append(cond)
        changed.append("condition")

    ts = getattr(args, "tool_status", None)
    if ts is not None:
        if ts not in VALID_TOOL_STATUSES:
            err(f"Invalid tool-status: {ts}")
        updates.append("status = ?")
        params.append(ts)
        changed.append("status")

    mu = getattr(args, "max_usage_count", None)
    if mu is not None:
        updates.append("max_usage_count = ?")
        params.append(int(mu))
        changed.append("max_usage_count")

    if not changed:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(tool_id)
    conn.execute(
        f"UPDATE tool SET {', '.join(updates)} WHERE id = ?", params
    )
    audit(conn, SKILL, "update-tool", "tool", tool_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"tool_id": tool_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# get-tool
# ---------------------------------------------------------------------------
def get_tool(conn, args):
    tool_id = getattr(args, "tool_id", None)
    if not tool_id:
        err("--tool-id is required")
    row = conn.execute(Q.from_(Table("tool")).select(Table("tool").star).where(Field("id") == P()).get_sql(), (tool_id,)).fetchone()
    if not row:
        err(f"Tool {tool_id} not found")

    data = row_to_dict(row)
    data["tool_status"] = data.pop("status", "available")

    # Get usage count
    usage_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM tool_usage WHERE tool_id = ?",
        (tool_id,),
    ).fetchone()
    data["usage_records"] = usage_count["cnt"] if usage_count else 0
    ok(data)


# ---------------------------------------------------------------------------
# list-tools
# ---------------------------------------------------------------------------
def list_tools(conn, args):
    conditions, params = [], []
    company_id = getattr(args, "company_id", None)
    if company_id:
        conditions.append("company_id = ?")
        params.append(company_id)
    tool_type = getattr(args, "tool_type", None)
    if tool_type:
        conditions.append("tool_type = ?")
        params.append(tool_type)
    cond = getattr(args, "condition", None)
    if cond:
        conditions.append("condition = ?")
        params.append(cond)
    ts = getattr(args, "tool_status", None)
    if ts:
        conditions.append("status = ?")
        params.append(ts)
    search = getattr(args, "search", None)
    if search:
        conditions.append("(name LIKE ? OR tool_code LIKE ? OR manufacturer LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    total = conn.execute(
        f"SELECT COUNT(*) as cnt FROM tool {where}", params
    ).fetchone()["cnt"]

    rows = conn.execute(
        f"SELECT * FROM tool {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    tools_list = []
    for r in rows:
        d = row_to_dict(r)
        d["tool_status"] = d.pop("status", "available")
        tools_list.append(d)

    ok({"tools": tools_list, "total_count": total, "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# add-tool-usage
# ---------------------------------------------------------------------------
def add_tool_usage(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "tool_id", None):
        err("--tool-id is required")

    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    tool = conn.execute(Q.from_(Table("tool")).select(Table("tool").star).where(Field("id") == P()).get_sql(), (args.tool_id,)).fetchone()
    if not tool:
        err(f"Tool {args.tool_id} not found")
    if tool["status"] == "scrapped":
        err("Cannot log usage for a scrapped tool")

    usage_count = int(getattr(args, "usage_count", None) or 1)
    usage_id = str(uuid.uuid4())

    duration = getattr(args, "usage_duration_minutes", None)
    if duration is not None:
        duration = int(duration)

    condition_after = getattr(args, "condition_after", None)
    if condition_after is not None and condition_after not in VALID_CONDITIONS:
        err(f"Invalid condition-after: {condition_after}")

    sql, _ = insert_row("tool_usage", {"id": P(), "tool_id": P(), "work_order_id": P(), "operator": P(), "usage_count": P(), "usage_duration_minutes": P(), "condition_after": P(), "notes": P(), "company_id": P()})
    conn.execute(sql,
        (
            usage_id, args.tool_id,
            getattr(args, "work_order_id", None),
            getattr(args, "operator", None),
            usage_count, duration, condition_after,
            getattr(args, "notes", None),
            args.company_id,
        ),
    )

    # Increment current_usage_count on tool
    new_count = tool["current_usage_count"] + usage_count
    update_sql = "UPDATE tool SET current_usage_count = ?, updated_at = datetime('now')"
    update_params = [new_count]

    # Update condition if specified
    if condition_after:
        update_sql += ", condition = ?"
        update_params.append(condition_after)
        # Auto-scrap if condition is scrapped
        if condition_after == "scrapped":
            update_sql += ", status = 'scrapped'"

    update_sql += " WHERE id = ?"
    update_params.append(args.tool_id)
    conn.execute(update_sql, update_params)

    audit(conn, SKILL, "add-tool-usage", "tool_usage", usage_id,
          new_values={"tool_id": args.tool_id, "usage_count": usage_count})
    conn.commit()
    ok({
        "usage_id": usage_id,
        "tool_id": args.tool_id,
        "new_usage_count": new_count,
        "condition_after": condition_after or tool["condition"],
    })


# ---------------------------------------------------------------------------
# list-tool-usage
# ---------------------------------------------------------------------------
def list_tool_usage(conn, args):
    conditions, params = [], []
    tool_id = getattr(args, "tool_id", None)
    if tool_id:
        conditions.append("tool_id = ?")
        params.append(tool_id)
    company_id = getattr(args, "company_id", None)
    if company_id:
        conditions.append("company_id = ?")
        params.append(company_id)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    total = conn.execute(
        f"SELECT COUNT(*) as cnt FROM tool_usage {where}", params
    ).fetchone()["cnt"]

    rows = conn.execute(
        f"SELECT * FROM tool_usage {where} ORDER BY usage_date DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    usages = [row_to_dict(r) for r in rows]
    ok({"usages": usages, "total_count": total, "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# calibration-due-report
# ---------------------------------------------------------------------------
def calibration_due_report(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    rows = conn.execute(
        """SELECT * FROM tool
           WHERE company_id = ?
             AND calibration_due IS NOT NULL
             AND status NOT IN ('scrapped')
           ORDER BY calibration_due ASC""",
        (args.company_id,),
    ).fetchall()

    tools_list = []
    overdue = 0
    from datetime import date as dt_date
    today = dt_date.today().isoformat()
    for r in rows:
        d = row_to_dict(r)
        d["tool_status"] = d.pop("status", "available")
        d["is_overdue"] = d.get("calibration_due", "") < today if d.get("calibration_due") else False
        if d["is_overdue"]:
            overdue += 1
        tools_list.append(d)

    ok({
        "tools": tools_list,
        "total_count": len(tools_list),
        "overdue_count": overdue,
    })


# ---------------------------------------------------------------------------
# tool-utilization-report
# ---------------------------------------------------------------------------
def tool_utilization_report(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    rows = conn.execute(
        """SELECT t.id, t.name, t.tool_type, t.current_usage_count,
                  t.max_usage_count, t.condition, t.status,
                  COUNT(tu.id) as usage_records,
                  COALESCE(SUM(tu.usage_count), 0) as total_usages,
                  COALESCE(SUM(tu.usage_duration_minutes), 0) as total_duration
           FROM tool t
           LEFT JOIN tool_usage tu ON tu.tool_id = t.id
           WHERE t.company_id = ?
           GROUP BY t.id
           ORDER BY total_usages DESC""",
        (args.company_id,),
    ).fetchall()

    tools_list = []
    for r in rows:
        d = row_to_dict(r)
        d["tool_status"] = d.pop("status", "available")
        # Utilization % = current_usage_count / max_usage_count * 100
        if d.get("max_usage_count") and d["max_usage_count"] > 0:
            util = Decimal(str(d["current_usage_count"])) / Decimal(str(d["max_usage_count"])) * Decimal("100")
            d["utilization_pct"] = str(util.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        else:
            d["utilization_pct"] = "N/A"
        tools_list.append(d)

    ok({"tools": tools_list, "total_count": len(tools_list)})


# ---------------------------------------------------------------------------
# ACTIONS registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "add-tool": add_tool,
    "update-tool": update_tool,
    "get-tool": get_tool,
    "list-tools": list_tools,
    "add-tool-usage": add_tool_usage,
    "list-tool-usage": list_tool_usage,
    "calibration-due-report": calibration_due_report,
    "tool-utilization-report": tool_utilization_report,
}
