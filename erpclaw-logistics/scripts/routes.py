"""ERPClaw Logistics -- routes domain module

Actions for route management, stops, and optimization.
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

    ENTITY_PREFIXES.setdefault("logistics_route", "RTE-")
except ImportError:
    pass

SKILL = "erpclaw-logistics"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_ROUTE_STATUSES = ("active", "inactive")
VALID_STOP_TYPES = ("pickup", "delivery", "transfer")


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


def _get_route(conn, route_id):
    if not route_id:
        err("--id is required")
    row = conn.execute(Q.from_(Table("logistics_route")).select(Table("logistics_route").star).where(Field("id") == P()).get_sql(), (route_id,)).fetchone()
    if not row:
        err(f"Route {route_id} not found")
    return row


# ===========================================================================
# 1. add-route
# ===========================================================================
def add_route(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    route_id = str(uuid.uuid4())
    conn.company_id = company_id
    naming = get_next_name(conn, "logistics_route", company_id=company_id)
    now = _now_iso()

    sql, _ = insert_row("logistics_route", {
        "id": P(), "naming_series": P(), "name": P(), "origin": P(),
        "destination": P(), "distance": P(), "estimated_hours": P(),
        "route_status": P(), "company_id": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        route_id, naming, name,
        getattr(args, "origin", None),
        getattr(args, "destination", None),
        getattr(args, "distance", None),
        getattr(args, "estimated_hours", None),
        "active",
        company_id, now, now,
    ))
    audit(conn, SKILL, "logistics-add-route", "logistics_route", route_id,
          new_values={"name": name})
    conn.commit()
    ok({
        "id": route_id, "naming_series": naming, "name": name,
        "route_status": "active",
    })


# ===========================================================================
# 2. update-route
# ===========================================================================
def update_route(conn, args):
    route_id = getattr(args, "id", None)
    _get_route(conn, route_id)

    data, changed = {}, []
    for arg_name, col_name in {
        "name": "name", "origin": "origin", "destination": "destination",
        "distance": "distance", "estimated_hours": "estimated_hours",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            data[col_name] = val
            changed.append(col_name)

    route_status = getattr(args, "route_status", None)
    if route_status:
        _validate_enum(route_status, VALID_ROUTE_STATUSES, "route-status")
        data["route_status"] = route_status
        changed.append("route_status")

    if not data:
        err("No fields to update")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("logistics_route", data, {"id": route_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "logistics-update-route", "logistics_route", route_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": route_id, "updated_fields": changed})


# ===========================================================================
# 3. list-routes
# ===========================================================================
def list_routes(conn, args):
    t = Table("logistics_route")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star).as_("cnt"))
    params = []

    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "route_status", None):
        q = q.where(t.route_status == P())
        q_cnt = q_cnt.where(t.route_status == P())
        params.append(args.route_status)
    if getattr(args, "search", None):
        search_crit = (t.name.like(P()) | t.origin.like(P()) | t.destination.like(P()))
        q = q.where(search_crit)
        q_cnt = q_cnt.where(search_crit)
        params.extend([f"%{args.search}%"] * 3)

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]
    q = q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [args.limit, args.offset]).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 4. add-route-stop
# ===========================================================================
def add_route_stop(conn, args):
    route_id = getattr(args, "route_id", None)
    if not route_id:
        err("--route-id is required")
    if not conn.execute(Q.from_(Table("logistics_route")).select(Field('id')).where(Field("id") == P()).get_sql(), (route_id,)).fetchone():
        err(f"Route {route_id} not found")

    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    stop_type = getattr(args, "stop_type", None) or "delivery"
    _validate_enum(stop_type, VALID_STOP_TYPES, "stop-type")

    stop_id = str(uuid.uuid4())
    stop_order = int(getattr(args, "stop_order", None) or 1)

    sql, _ = insert_row("logistics_route_stop", {
        "id": P(), "route_id": P(), "stop_order": P(), "address": P(),
        "city": P(), "state": P(), "zip_code": P(), "estimated_arrival": P(),
        "stop_type": P(), "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        stop_id, route_id, stop_order,
        getattr(args, "address", None),
        getattr(args, "city", None),
        getattr(args, "state", None),
        getattr(args, "zip_code", None),
        getattr(args, "estimated_arrival", None),
        stop_type,
        company_id, _now_iso(),
    ))
    audit(conn, SKILL, "logistics-add-route-stop", "logistics_route_stop", stop_id,
          new_values={"route_id": route_id, "stop_order": stop_order, "stop_type": stop_type})
    conn.commit()
    ok({
        "id": stop_id, "route_id": route_id,
        "stop_order": stop_order, "stop_type": stop_type,
    })


# ===========================================================================
# 5. list-route-stops
# ===========================================================================
def list_route_stops(conn, args):
    route_id = getattr(args, "route_id", None)
    t = Table("logistics_route_stop")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star).as_("cnt"))
    params = []

    if route_id:
        q = q.where(t.route_id == P())
        q_cnt = q_cnt.where(t.route_id == P())
        params.append(route_id)
    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(args.company_id)

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]
    q = q.orderby(t.stop_order).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [args.limit, args.offset]).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 6. optimize-route-report
# ===========================================================================
def optimize_route_report(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    t_route = Table("logistics_route")
    routes = conn.execute(
        Q.from_(t_route).select(t_route.star)
        .where(t_route.company_id == P()).where(t_route.route_status == "active")
        .orderby(t_route.name).get_sql(),
        (company_id,)
    ).fetchall()

    t_stop = Table("logistics_route_stop")
    t_ship = Table("logistics_shipment")
    report = []
    for r in routes:
        r_data = row_to_dict(r)
        stops = conn.execute(
            Q.from_(t_stop).select(t_stop.star)
            .where(t_stop.route_id == P()).orderby(t_stop.stop_order).get_sql(),
            (r["id"],)
        ).fetchall()
        r_data["stops"] = [row_to_dict(s) for s in stops]
        r_data["stop_count"] = len(stops)

        # Count shipments using this route's origin/destination
        shipment_count = conn.execute(
            Q.from_(t_ship).select(fn.Count(t_ship.star))
            .where(t_ship.company_id == P())
            .where(t_ship.origin_city == P())
            .where(t_ship.destination_city == P()).get_sql(),
            (company_id, r_data.get("origin", ""), r_data.get("destination", ""))
        ).fetchone()[0]
        r_data["matching_shipments"] = shipment_count

        report.append(r_data)

    ok({
        "report": "optimize-route",
        "company_id": company_id,
        "active_routes": len(report),
        "routes": report,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "logistics-add-route": add_route,
    "logistics-update-route": update_route,
    "logistics-list-routes": list_routes,
    "logistics-add-route-stop": add_route_stop,
    "logistics-list-route-stops": list_route_stops,
    "logistics-optimize-route-report": optimize_route_report,
}
