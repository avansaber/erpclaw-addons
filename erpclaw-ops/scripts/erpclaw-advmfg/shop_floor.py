"""ERPClaw Advanced Manufacturing -- Shop Floor domain module.

Shop floor entry tracking, dashboards, production logs, OEE reporting.
8 actions exported via ACTIONS dict.
"""
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, LiteralValue, insert_row, update_row, dynamic_update

SKILL = "erpclaw-advmfg"

VALID_ENTRY_TYPES = (
    "production", "setup", "downtime", "quality_check", "changeover", "other",
)
VALID_MACHINE_STATUSES = (
    "running", "idle", "setup", "breakdown", "maintenance", "off",
)


# ---------------------------------------------------------------------------
# add-shop-floor-entry
# ---------------------------------------------------------------------------
def add_shop_floor_entry(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    entry_type = getattr(args, "entry_type", None) or "production"
    if entry_type not in VALID_ENTRY_TYPES:
        err(f"Invalid entry-type: {entry_type}")

    machine_status = getattr(args, "machine_status", None) or "running"
    if machine_status not in VALID_MACHINE_STATUSES:
        err(f"Invalid machine-status: {machine_status}")

    entry_id = str(uuid.uuid4())
    ns = get_next_name(conn, "shop_floor_entry", company_id=args.company_id)
    start_time = getattr(args, "start_time", None) or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    sql, _ = insert_row("shop_floor_entry", {"id": P(), "equipment_id": P(), "work_order_id": P(), "operator": P(), "entry_type": P(), "start_time": P(), "machine_status": P(), "batch_number": P(), "serial_number": P(), "notes": P(), "company_id": P()})
    conn.execute(sql,
        (
            entry_id,
            getattr(args, "equipment_id", None),
            getattr(args, "work_order_id", None),
            getattr(args, "operator", None),
            entry_type,
            start_time,
            machine_status,
            getattr(args, "batch_number", None),
            getattr(args, "serial_number", None),
            getattr(args, "notes", None),
            args.company_id,
        ),
    )
    audit(conn, SKILL, "add-shop-floor-entry", "shop_floor_entry", entry_id,
          new_values={"naming_series": ns, "entry_type": entry_type})
    conn.commit()
    ok({"entry_id": entry_id, "naming_series": ns, "entry_type": entry_type,
        "machine_status_value": machine_status})


# ---------------------------------------------------------------------------
# update-shop-floor-entry
# ---------------------------------------------------------------------------
def update_shop_floor_entry(conn, args):
    entry_id = getattr(args, "entry_id", None)
    if not entry_id:
        err("--entry-id is required")
    row = conn.execute(Q.from_(Table("shop_floor_entry")).select(Table("shop_floor_entry").star).where(Field("id") == P()).get_sql(), (entry_id,)).fetchone()
    if not row:
        err(f"Shop floor entry {entry_id} not found")

    data, changed = {}, []

    for field, attr in [
        ("equipment_id", "equipment_id"),
        ("work_order_id", "work_order_id"),
        ("operator", "operator"),
        ("batch_number", "batch_number"),
        ("serial_number", "serial_number"),
        ("notes", "notes"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            data[field] = val
            changed.append(field)

    et = getattr(args, "entry_type", None)
    if et is not None:
        if et not in VALID_ENTRY_TYPES:
            err(f"Invalid entry-type: {et}")
        data["entry_type"] = et
        changed.append("entry_type")

    ms = getattr(args, "machine_status", None)
    if ms is not None:
        if ms not in VALID_MACHINE_STATUSES:
            err(f"Invalid machine-status: {ms}")
        data["machine_status"] = ms
        changed.append("machine_status")

    qp = getattr(args, "quantity_produced", None)
    if qp is not None:
        data["quantity_produced"] = int(qp)
        changed.append("quantity_produced")

    qr = getattr(args, "quantity_rejected", None)
    if qr is not None:
        data["quantity_rejected"] = int(qr)
        changed.append("quantity_rejected")

    if not changed:
        err("No fields to update")

    sql, params = dynamic_update("shop_floor_entry", data, {"id": entry_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "update-shop-floor-entry", "shop_floor_entry", entry_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"entry_id": entry_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# get-shop-floor-entry
# ---------------------------------------------------------------------------
def get_shop_floor_entry(conn, args):
    entry_id = getattr(args, "entry_id", None)
    if not entry_id:
        err("--entry-id is required")
    row = conn.execute(Q.from_(Table("shop_floor_entry")).select(Table("shop_floor_entry").star).where(Field("id") == P()).get_sql(), (entry_id,)).fetchone()
    if not row:
        err(f"Shop floor entry {entry_id} not found")

    data = row_to_dict(row)
    data["machine_status_value"] = data.pop("machine_status", "running")
    ok(data)


# ---------------------------------------------------------------------------
# list-shop-floor-entries
# ---------------------------------------------------------------------------
def list_shop_floor_entries(conn, args):
    t = Table("shop_floor_entry")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star).as_("cnt"))
    params = []

    company_id = getattr(args, "company_id", None)
    if company_id:
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(company_id)
    equipment_id = getattr(args, "equipment_id", None)
    if equipment_id:
        q = q.where(t.equipment_id == P())
        q_cnt = q_cnt.where(t.equipment_id == P())
        params.append(equipment_id)
    entry_type = getattr(args, "entry_type", None)
    if entry_type:
        q = q.where(t.entry_type == P())
        q_cnt = q_cnt.where(t.entry_type == P())
        params.append(entry_type)
    search = getattr(args, "search", None)
    if search:
        like = LiteralValue("?")
        q = q.where((t.operator.like(like)) | (t.batch_number.like(like)) | (t.notes.like(like)))
        q_cnt = q_cnt.where((t.operator.like(like)) | (t.batch_number.like(like)) | (t.notes.like(like)))
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    total = conn.execute(q_cnt.get_sql(), params).fetchone()["cnt"]

    q = q.orderby(t.start_time, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()

    entries = []
    for r in rows:
        d = row_to_dict(r)
        d["machine_status_value"] = d.pop("machine_status", "running")
        entries.append(d)

    ok({"entries": entries, "total_count": total, "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# complete-shop-floor-entry
# ---------------------------------------------------------------------------
def complete_shop_floor_entry(conn, args):
    entry_id = getattr(args, "entry_id", None)
    if not entry_id:
        err("--entry-id is required")

    row = conn.execute(Q.from_(Table("shop_floor_entry")).select(Table("shop_floor_entry").star).where(Field("id") == P()).get_sql(), (entry_id,)).fetchone()
    if not row:
        err(f"Shop floor entry {entry_id} not found")
    if row["end_time"] is not None:
        err(f"Shop floor entry {entry_id} is already completed")

    end_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Calculate duration
    try:
        start_dt = datetime.strptime(row["start_time"], "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        duration_minutes = int((end_dt - start_dt).total_seconds() / 60)
    except (ValueError, TypeError):
        duration_minutes = 0

    data = {
        "end_time": end_time,
        "duration_minutes": duration_minutes,
        "machine_status": "idle",
    }

    qp = getattr(args, "quantity_produced", None)
    if qp is not None:
        data["quantity_produced"] = int(qp)

    qr = getattr(args, "quantity_rejected", None)
    if qr is not None:
        data["quantity_rejected"] = int(qr)

    notes = getattr(args, "notes", None)
    if notes is not None:
        data["notes"] = notes

    sql, params = dynamic_update("shop_floor_entry", data, {"id": entry_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "complete-shop-floor-entry", "shop_floor_entry", entry_id,
          new_values={"end_time": end_time, "duration_minutes": duration_minutes})
    conn.commit()
    ok({
        "entry_id": entry_id,
        "end_time": end_time,
        "duration_minutes": duration_minutes,
        "machine_status_value": "idle",
    })


# ---------------------------------------------------------------------------
# shop-floor-dashboard
# ---------------------------------------------------------------------------
def shop_floor_dashboard(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    company_id = args.company_id
    t = Table("shop_floor_entry")

    # Total entries
    total = conn.execute(
        Q.from_(t).select(fn.Count(t.star).as_("cnt")).where(t.company_id == P()).get_sql(),
        (company_id,),
    ).fetchone()["cnt"]

    # By entry type
    by_type = conn.execute(
        Q.from_(t).select(t.entry_type, fn.Count(t.star).as_("cnt"))
        .where(t.company_id == P())
        .groupby(t.entry_type).orderby(Field("cnt"), order=Order.desc).get_sql(),
        (company_id,),
    ).fetchall()
    type_breakdown = {r["entry_type"]: r["cnt"] for r in by_type}

    # Total production
    prod_stats = conn.execute(
        Q.from_(t).select(
            fn.Coalesce(fn.Sum(t.quantity_produced), 0).as_("total_produced"),
            fn.Coalesce(fn.Sum(t.quantity_rejected), 0).as_("total_rejected"),
        ).where(t.company_id == P()).get_sql(),
        (company_id,),
    ).fetchone()

    total_produced = prod_stats["total_produced"]
    total_rejected = prod_stats["total_rejected"]
    rejection_rate = "0"
    if total_produced > 0:
        rate = Decimal(str(total_rejected)) / Decimal(str(total_produced + total_rejected)) * Decimal("100")
        rejection_rate = str(rate.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    # Active (no end_time)
    active = conn.execute(
        Q.from_(t).select(fn.Count(t.star).as_("cnt"))
        .where(t.company_id == P()).where(t.end_time.isnull()).get_sql(),
        (company_id,),
    ).fetchone()["cnt"]

    ok({
        "company_id": company_id,
        "total_entries": total,
        "active_entries": active,
        "by_entry_type": type_breakdown,
        "total_produced": total_produced,
        "total_rejected": total_rejected,
        "rejection_rate_pct": rejection_rate,
    })


# ---------------------------------------------------------------------------
# production-log-report
# ---------------------------------------------------------------------------
def production_log_report(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    t = Table("shop_floor_entry")
    q = Q.from_(t).select(t.star).where(t.company_id == P())
    params = [args.company_id]

    start_date = getattr(args, "start_date", None)
    if start_date:
        q = q.where(t.start_time >= P())
        params.append(start_date)
    end_date = getattr(args, "end_date", None)
    if end_date:
        q = q.where(t.start_time <= P())
        params.append(end_date + " 23:59:59")
    equipment_id = getattr(args, "equipment_id", None)
    if equipment_id:
        q = q.where(t.equipment_id == P())
        params.append(equipment_id)
    operator = getattr(args, "operator", None)
    if operator:
        q = q.where(t.operator == P())
        params.append(operator)

    q = q.orderby(t.start_time, order=Order.desc)
    rows = conn.execute(q.get_sql(), params).fetchall()

    entries = []
    total_produced = 0
    total_rejected = 0
    total_duration = 0
    for r in rows:
        d = row_to_dict(r)
        d["machine_status_value"] = d.pop("machine_status", "running")
        entries.append(d)
        total_produced += d.get("quantity_produced") or 0
        total_rejected += d.get("quantity_rejected") or 0
        total_duration += d.get("duration_minutes") or 0

    ok({
        "entries": entries,
        "total_count": len(entries),
        "total_produced": total_produced,
        "total_rejected": total_rejected,
        "total_duration_minutes": total_duration,
    })


# ---------------------------------------------------------------------------
# oee-report (Overall Equipment Effectiveness)
# ---------------------------------------------------------------------------
def oee_report(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "equipment_id", None):
        err("--equipment-id is required")

    t = Table("shop_floor_entry")
    q = Q.from_(t).select(t.star).where(t.company_id == P()).where(t.equipment_id == P())
    params = [args.company_id, args.equipment_id]

    start_date = getattr(args, "start_date", None)
    if start_date:
        q = q.where(t.start_time >= P())
        params.append(start_date)
    end_date = getattr(args, "end_date", None)
    if end_date:
        q = q.where(t.start_time <= P())
        params.append(end_date + " 23:59:59")

    rows = conn.execute(q.get_sql(), params).fetchall()

    if not rows:
        ok({
            "equipment_id": args.equipment_id,
            "availability": "0",
            "performance": "0",
            "quality": "0",
            "oee": "0",
            "message": "No entries found for this equipment",
        })
        return

    total_time = Decimal("0")
    production_time = Decimal("0")
    downtime = Decimal("0")
    total_produced = Decimal("0")
    total_good = Decimal("0")

    for r in rows:
        dur = Decimal(str(r["duration_minutes"] or 0))
        total_time += dur
        if r["entry_type"] == "production":
            production_time += dur
            produced = Decimal(str(r["quantity_produced"] or 0))
            rejected = Decimal(str(r["quantity_rejected"] or 0))
            total_produced += produced
            total_good += (produced - rejected)
        elif r["entry_type"] in ("downtime", "breakdown"):
            downtime += dur

    # Availability = (Total Time - Downtime) / Total Time
    if total_time > 0:
        availability = ((total_time - downtime) / total_time * Decimal("100")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
    else:
        availability = Decimal("0")

    # Performance = Production Time / (Total Time - Downtime)
    available_time = total_time - downtime
    if available_time > 0:
        performance = (production_time / available_time * Decimal("100")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
    else:
        performance = Decimal("0")

    # Quality = Good Units / Total Produced
    if total_produced > 0:
        quality = (total_good / total_produced * Decimal("100")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
    else:
        quality = Decimal("0")

    # OEE = Availability * Performance * Quality / 10000
    oee = (availability * performance * quality / Decimal("10000")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    ok({
        "equipment_id": args.equipment_id,
        "total_time_minutes": str(total_time),
        "production_time_minutes": str(production_time),
        "downtime_minutes": str(downtime),
        "total_produced": str(total_produced),
        "total_good": str(total_good),
        "availability": str(availability),
        "performance": str(performance),
        "quality": str(quality),
        "oee": str(oee),
    })


# ---------------------------------------------------------------------------
# ACTIONS registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "add-shop-floor-entry": add_shop_floor_entry,
    "update-shop-floor-entry": update_shop_floor_entry,
    "get-shop-floor-entry": get_shop_floor_entry,
    "list-shop-floor-entries": list_shop_floor_entries,
    "complete-shop-floor-entry": complete_shop_floor_entry,
    "shop-floor-dashboard": shop_floor_dashboard,
    "production-log-report": production_log_report,
    "oee-report": oee_report,
}
