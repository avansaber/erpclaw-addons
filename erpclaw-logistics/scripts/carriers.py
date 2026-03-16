"""ERPClaw Logistics -- carriers domain module

Actions for carrier management, rates, performance, and cost comparison.
Imported by db_query.py (unified router).
"""
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row, dynamic_update
    from erpclaw_lib.vendor.pypika.terms import LiteralValue

    ENTITY_PREFIXES.setdefault("logistics_carrier", "CAR-")
except ImportError:
    pass

SKILL = "erpclaw-logistics"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_CARRIER_TYPES = ("ltl", "ftl", "parcel", "freight_forwarder", "courier")
VALID_CARRIER_STATUSES = ("active", "inactive", "suspended")
VALID_SERVICE_LEVELS = ("ground", "express", "overnight", "freight", "ltl")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _validate_enum(value, valid_values, field_name):
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")


def _get_carrier(conn, carrier_id):
    if not carrier_id:
        err("--id is required")
    row = conn.execute(Q.from_(Table("logistics_carrier")).select(Table("logistics_carrier").star).where(Field("id") == P()).get_sql(), (carrier_id,)).fetchone()
    if not row:
        err(f"Carrier {carrier_id} not found")
    return row


# ===========================================================================
# 1. add-carrier
# ===========================================================================
def add_carrier(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    carrier_type = getattr(args, "carrier_type", None) or "parcel"
    _validate_enum(carrier_type, VALID_CARRIER_TYPES, "carrier-type")

    supplier_id = getattr(args, "supplier_id", None)
    if supplier_id:
        if not conn.execute(Q.from_(Table("supplier")).select(Field('id')).where(Field("id") == P()).get_sql(), (supplier_id,)).fetchone():
            err(f"Supplier {supplier_id} not found")

    carrier_id = str(uuid.uuid4())
    conn.company_id = company_id
    naming = get_next_name(conn, "logistics_carrier", company_id=company_id)
    now = _now_iso()

    sql, _ = insert_row("logistics_carrier", {
        "id": P(), "naming_series": P(), "name": P(), "carrier_code": P(),
        "supplier_id": P(), "contact_name": P(), "contact_email": P(),
        "contact_phone": P(), "dot_number": P(), "mc_number": P(),
        "carrier_type": P(), "insurance_expiry": P(), "carrier_status": P(),
        "on_time_pct": P(), "total_shipments": P(), "company_id": P(),
        "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        carrier_id, naming, name,
        getattr(args, "carrier_code", None),
        supplier_id,
        getattr(args, "contact_name", None),
        getattr(args, "contact_email", None),
        getattr(args, "contact_phone", None),
        getattr(args, "dot_number", None),
        getattr(args, "mc_number", None),
        carrier_type,
        getattr(args, "insurance_expiry", None),
        "active", "100", 0,
        company_id, now, now,
    ))
    audit(conn, SKILL, "logistics-add-carrier", "logistics_carrier", carrier_id,
          new_values={"name": name, "carrier_type": carrier_type, "supplier_id": supplier_id})
    conn.commit()
    ok({
        "id": carrier_id, "naming_series": naming, "name": name,
        "carrier_type": carrier_type, "carrier_status": "active",
        "supplier_id": supplier_id,
    })


# ===========================================================================
# 2. update-carrier
# ===========================================================================
def update_carrier(conn, args):
    carrier_id = getattr(args, "id", None)
    _get_carrier(conn, carrier_id)

    data, changed = {}, []

    # Handle supplier_id with FK validation
    supplier_id = getattr(args, "supplier_id", None)
    if supplier_id is not None:
        if supplier_id and not conn.execute(Q.from_(Table("supplier")).select(Field('id')).where(Field("id") == P()).get_sql(), (supplier_id,)).fetchone():
            err(f"Supplier {supplier_id} not found")
        data["supplier_id"] = supplier_id if supplier_id else None
        changed.append("supplier_id")

    for arg_name, col_name in {
        "name": "name", "carrier_code": "carrier_code",
        "contact_name": "contact_name", "contact_email": "contact_email",
        "contact_phone": "contact_phone", "dot_number": "dot_number",
        "mc_number": "mc_number", "insurance_expiry": "insurance_expiry",
        "on_time_pct": "on_time_pct",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            data[col_name] = val
            changed.append(col_name)

    carrier_type = getattr(args, "carrier_type", None)
    if carrier_type:
        _validate_enum(carrier_type, VALID_CARRIER_TYPES, "carrier-type")
        data["carrier_type"] = carrier_type
        changed.append("carrier_type")

    carrier_status = getattr(args, "carrier_status", None)
    if carrier_status:
        _validate_enum(carrier_status, VALID_CARRIER_STATUSES, "carrier-status")
        data["carrier_status"] = carrier_status
        changed.append("carrier_status")

    if not data:
        err("No fields to update")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("logistics_carrier", data, {"id": carrier_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "logistics-update-carrier", "logistics_carrier", carrier_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": carrier_id, "updated_fields": changed})


# ===========================================================================
# 3. get-carrier
# ===========================================================================
def get_carrier(conn, args):
    carrier_id = getattr(args, "id", None)
    row = _get_carrier(conn, carrier_id)
    data = row_to_dict(row)

    # Include rates
    rates = conn.execute(Q.from_(Table("logistics_carrier_rate")).select(Table("logistics_carrier_rate").star).where(Field("carrier_id") == P()).orderby(Field("service_level")).orderby(Field("created_at")).get_sql(), (carrier_id,)).fetchall()
    data["rates"] = [row_to_dict(r) for r in rates]
    data["rate_count"] = len(rates)

    # Recent shipment stats
    t_ship = Table("logistics_shipment")
    shipments = conn.execute(
        Q.from_(t_ship).select(t_ship.shipment_status, fn.Count(t_ship.star).as_("cnt"))
        .where(t_ship.carrier_id == P()).groupby(t_ship.shipment_status).get_sql(),
        (carrier_id,)
    ).fetchall()
    data["shipment_stats"] = {r[0]: r[1] for r in shipments}

    ok(data)


# ===========================================================================
# 4. list-carriers
# ===========================================================================
def list_carriers(conn, args):
    t = Table("logistics_carrier")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star).as_("cnt"))
    params = []

    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "carrier_status", None):
        q = q.where(t.carrier_status == P())
        q_cnt = q_cnt.where(t.carrier_status == P())
        params.append(args.carrier_status)
    if getattr(args, "carrier_type", None):
        q = q.where(t.carrier_type == P())
        q_cnt = q_cnt.where(t.carrier_type == P())
        params.append(args.carrier_type)
    if getattr(args, "search", None):
        search_crit = (t.name.like(P()) | t.carrier_code.like(P()))
        q = q.where(search_crit)
        q_cnt = q_cnt.where(search_crit)
        params.extend([f"%{args.search}%"] * 2)

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]
    q = q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [args.limit, args.offset]).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 5. add-carrier-rate
# ===========================================================================
def add_carrier_rate(conn, args):
    carrier_id = getattr(args, "carrier_id", None)
    if not carrier_id:
        err("--carrier-id is required")
    if not conn.execute(Q.from_(Table("logistics_carrier")).select(Field('id')).where(Field("id") == P()).get_sql(), (carrier_id,)).fetchone():
        err(f"Carrier {carrier_id} not found")

    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    service_level = getattr(args, "service_level", None) or "ground"
    _validate_enum(service_level, VALID_SERVICE_LEVELS, "service-level")

    rate_id = str(uuid.uuid4())

    sql, _ = insert_row("logistics_carrier_rate", {
        "id": P(), "carrier_id": P(), "origin_zone": P(), "destination_zone": P(),
        "service_level": P(), "weight_min": P(), "weight_max": P(),
        "rate_per_unit": P(), "flat_rate": P(), "effective_date": P(),
        "expiry_date": P(), "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        rate_id, carrier_id,
        getattr(args, "origin_zone", None),
        getattr(args, "destination_zone", None),
        service_level,
        getattr(args, "weight_min", None),
        getattr(args, "weight_max", None),
        getattr(args, "rate_per_unit", None),
        getattr(args, "flat_rate", None),
        getattr(args, "effective_date", None),
        getattr(args, "expiry_date", None),
        company_id, _now_iso(),
    ))
    audit(conn, SKILL, "logistics-add-carrier-rate", "logistics_carrier_rate", rate_id,
          new_values={"carrier_id": carrier_id, "service_level": service_level})
    conn.commit()
    ok({
        "id": rate_id, "carrier_id": carrier_id,
        "service_level": service_level,
    })


# ===========================================================================
# 6. list-carrier-rates
# ===========================================================================
def list_carrier_rates(conn, args):
    t = Table("logistics_carrier_rate")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star).as_("cnt"))
    params = []

    carrier_id = getattr(args, "carrier_id", None)
    if carrier_id:
        q = q.where(t.carrier_id == P())
        q_cnt = q_cnt.where(t.carrier_id == P())
        params.append(carrier_id)
    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "service_level", None):
        q = q.where(t.service_level == P())
        q_cnt = q_cnt.where(t.service_level == P())
        params.append(args.service_level)

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]
    q = q.orderby(t.service_level).orderby(t.created_at).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [args.limit, args.offset]).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 7. carrier-performance-report
# ===========================================================================
def carrier_performance_report(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    t_car = Table("logistics_carrier")
    carriers = conn.execute(
        Q.from_(t_car).select(t_car.id, t_car.name, t_car.carrier_type,
                              t_car.on_time_pct, t_car.total_shipments, t_car.carrier_status)
        .where(t_car.company_id == P())
        .orderby(t_car.total_shipments, order=Order.desc).get_sql(),
        (company_id,)
    ).fetchall()

    t_ship = Table("logistics_shipment")
    report = []
    for c in carriers:
        carrier_data = row_to_dict(c)
        # Count actual shipments by status
        stats = conn.execute(
            Q.from_(t_ship).select(t_ship.shipment_status, fn.Count(t_ship.star))
            .where(t_ship.carrier_id == P()).groupby(t_ship.shipment_status).get_sql(),
            (c["id"],)
        ).fetchall()
        carrier_data["shipment_breakdown"] = {s[0]: s[1] for s in stats}
        delivered = carrier_data["shipment_breakdown"].get("delivered", 0)
        exceptions = carrier_data["shipment_breakdown"].get("exception", 0)
        carrier_data["exception_rate"] = (
            str(round(exceptions / (delivered + exceptions) * 100, 1))
            if (delivered + exceptions) > 0 else "0"
        )
        report.append(carrier_data)

    ok({
        "report": "carrier-performance",
        "company_id": company_id,
        "total_carriers": len(report),
        "carriers": report,
    })


# ===========================================================================
# 8. carrier-cost-comparison
# ===========================================================================
def carrier_cost_comparison(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    service_level = getattr(args, "service_level", None)

    t_car = Table("logistics_carrier")
    carriers = conn.execute(
        Q.from_(t_car).select(t_car.id, t_car.name, t_car.carrier_type)
        .where(t_car.company_id == P()).where(t_car.carrier_status == "active")
        .orderby(t_car.name).get_sql(),
        (company_id,)
    ).fetchall()

    t_rate = Table("logistics_carrier_rate")
    t_ship = Table("logistics_shipment")
    comparison = []
    for c in carriers:
        c_data = row_to_dict(c)
        q_rate = (Q.from_(t_rate)
                  .select(t_rate.service_level, t_rate.rate_per_unit, t_rate.flat_rate)
                  .where(t_rate.carrier_id == P()))
        rate_params = [c["id"]]
        if service_level:
            q_rate = q_rate.where(t_rate.service_level == P())
            rate_params.append(service_level)
        q_rate = q_rate.orderby(t_rate.service_level)
        rates = conn.execute(q_rate.get_sql(), rate_params).fetchall()
        c_data["rates"] = [row_to_dict(r) for r in rates]

        # Average shipping cost from actual shipments
        q_ship = (Q.from_(t_ship).select(t_ship.shipping_cost)
                  .where(t_ship.carrier_id == P()).where(t_ship.shipping_cost.isnotnull()))
        ship_params = [c["id"]]
        if service_level:
            q_ship = q_ship.where(t_ship.service_level == P())
            ship_params.append(service_level)
        ship_rows = conn.execute(q_ship.get_sql(), ship_params).fetchall()
        if ship_rows:
            costs = [Decimal(r[0]) for r in ship_rows]
            c_data["avg_actual_cost"] = str(sum(costs) / len(costs))
            c_data["shipment_count"] = len(costs)
        else:
            c_data["avg_actual_cost"] = "N/A"
            c_data["shipment_count"] = 0

        comparison.append(c_data)

    ok({
        "report": "logistics-carrier-cost-comparison",
        "company_id": company_id,
        "service_level_filter": service_level,
        "carriers": comparison,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "logistics-add-carrier": add_carrier,
    "logistics-update-carrier": update_carrier,
    "logistics-get-carrier": get_carrier,
    "logistics-list-carriers": list_carriers,
    "logistics-add-carrier-rate": add_carrier_rate,
    "logistics-list-carrier-rates": list_carrier_rates,
    "logistics-carrier-performance-report": carrier_performance_report,
    "logistics-carrier-cost-comparison": carrier_cost_comparison,
}
