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
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, LiteralValue, insert_row, update_row, dynamic_update

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

    sql, _ = insert_row("tool", {
        "id": P(), "naming_series": P(), "name": P(), "tool_type": P(),
        "tool_code": P(), "manufacturer": P(), "model": P(),
        "serial_number": P(), "location": P(), "purchase_date": P(),
        "purchase_cost": P(), "max_usage_count": P(), "current_usage_count": P(),
        "calibration_due": P(), "condition": P(), "status": P(),
        "notes": P(), "company_id": P(),
    })
    conn.execute(sql,
        (
            tool_id, ns, args.name, tool_type,
            getattr(args, "tool_code", None),
            getattr(args, "manufacturer", None),
            getattr(args, "model", None),
            getattr(args, "serial_number", None),
            getattr(args, "location", None),
            getattr(args, "purchase_date", None),
            getattr(args, "purchase_cost", None) or "0",
            max_usage, 0,
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

    data, changed = {}, []

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
            data[field] = val
            changed.append(field)

    tt = getattr(args, "tool_type", None)
    if tt is not None:
        if tt not in VALID_TOOL_TYPES:
            err(f"Invalid tool-type: {tt}")
        data["tool_type"] = tt
        changed.append("tool_type")

    cond = getattr(args, "condition", None)
    if cond is not None:
        if cond not in VALID_CONDITIONS:
            err(f"Invalid condition: {cond}")
        data["condition"] = cond
        changed.append("condition")

    ts = getattr(args, "tool_status", None)
    if ts is not None:
        if ts not in VALID_TOOL_STATUSES:
            err(f"Invalid tool-status: {ts}")
        data["status"] = ts
        changed.append("status")

    mu = getattr(args, "max_usage_count", None)
    if mu is not None:
        data["max_usage_count"] = int(mu)
        changed.append("max_usage_count")

    if not changed:
        err("No fields to update")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("tool", data, {"id": tool_id})
    conn.execute(sql, params)
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
    tu = Table("tool_usage")
    usage_count = conn.execute(
        Q.from_(tu).select(fn.Count(tu.star).as_("cnt")).where(tu.tool_id == P()).get_sql(),
        (tool_id,),
    ).fetchone()
    data["usage_records"] = usage_count["cnt"] if usage_count else 0
    ok(data)


# ---------------------------------------------------------------------------
# list-tools
# ---------------------------------------------------------------------------
def list_tools(conn, args):
    t = Table("tool")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star).as_("cnt"))
    params = []

    company_id = getattr(args, "company_id", None)
    if company_id:
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(company_id)
    tool_type = getattr(args, "tool_type", None)
    if tool_type:
        q = q.where(t.tool_type == P())
        q_cnt = q_cnt.where(t.tool_type == P())
        params.append(tool_type)
    cond = getattr(args, "condition", None)
    if cond:
        q = q.where(t.condition == P())
        q_cnt = q_cnt.where(t.condition == P())
        params.append(cond)
    ts = getattr(args, "tool_status", None)
    if ts:
        q = q.where(t.status == P())
        q_cnt = q_cnt.where(t.status == P())
        params.append(ts)
    search = getattr(args, "search", None)
    if search:
        like = LiteralValue("?")
        crit = (t.name.like(like)) | (t.tool_code.like(like)) | (t.manufacturer.like(like))
        q = q.where(crit)
        q_cnt = q_cnt.where(crit)
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    total = conn.execute(q_cnt.get_sql(), params).fetchone()["cnt"]

    q = q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()

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
    upd_data = {
        "current_usage_count": new_count,
        "updated_at": LiteralValue("datetime('now')"),
    }

    # Update condition if specified
    if condition_after:
        upd_data["condition"] = condition_after
        # Auto-scrap if condition is scrapped
        if condition_after == "scrapped":
            upd_data["status"] = "scrapped"

    upd_sql, upd_params = dynamic_update("tool", upd_data, {"id": args.tool_id})
    conn.execute(upd_sql, upd_params)

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
    tu = Table("tool_usage")
    q = Q.from_(tu).select(tu.star)
    q_cnt = Q.from_(tu).select(fn.Count(tu.star).as_("cnt"))
    params = []

    tool_id = getattr(args, "tool_id", None)
    if tool_id:
        q = q.where(tu.tool_id == P())
        q_cnt = q_cnt.where(tu.tool_id == P())
        params.append(tool_id)
    company_id = getattr(args, "company_id", None)
    if company_id:
        q = q.where(tu.company_id == P())
        q_cnt = q_cnt.where(tu.company_id == P())
        params.append(company_id)

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    total = conn.execute(q_cnt.get_sql(), params).fetchone()["cnt"]

    q = q.orderby(tu.usage_date, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()

    usages = [row_to_dict(r) for r in rows]
    ok({"usages": usages, "total_count": total, "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# calibration-due-report
# ---------------------------------------------------------------------------
def calibration_due_report(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    t = Table("tool")
    q = (Q.from_(t).select(t.star)
         .where(t.company_id == P())
         .where(t.calibration_due.isnotnull())
         .where(t.status != "scrapped")
         .orderby(t.calibration_due))
    rows = conn.execute(q.get_sql(), (args.company_id,)).fetchall()

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

    t = Table("tool")
    tu = Table("tool_usage")
    q = (Q.from_(t)
         .left_join(tu).on(tu.tool_id == t.id)
         .select(
             t.id, t.name, t.tool_type, t.current_usage_count,
             t.max_usage_count, t.condition, t.status,
             fn.Count(tu.id).as_("usage_records"),
             fn.Coalesce(fn.Sum(tu.usage_count), 0).as_("total_usages"),
             fn.Coalesce(fn.Sum(tu.usage_duration_minutes), 0).as_("total_duration"),
         )
         .where(t.company_id == P())
         .groupby(t.id)
         .orderby(Field("total_usages"), order=Order.desc))
    rows = conn.execute(q.get_sql(), (args.company_id,)).fetchall()

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
