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
    if not conn.execute("SELECT id FROM company WHERE id = ?", (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _validate_enum(value, valid_values, field_name):
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")


def _get_route(conn, route_id):
    if not route_id:
        err("--id is required")
    row = conn.execute("SELECT * FROM logistics_route WHERE id = ?", (route_id,)).fetchone()
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

    conn.execute("""
        INSERT INTO logistics_route (
            id, naming_series, name, origin, destination, distance,
            estimated_hours, route_status, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
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

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "name": "name", "origin": "origin", "destination": "destination",
        "distance": "distance", "estimated_hours": "estimated_hours",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    route_status = getattr(args, "route_status", None)
    if route_status:
        _validate_enum(route_status, VALID_ROUTE_STATUSES, "route-status")
        updates.append("route_status = ?")
        params.append(route_status)
        changed.append("route_status")

    if not updates:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(route_id)
    conn.execute(f"UPDATE logistics_route SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, SKILL, "logistics-update-route", "logistics_route", route_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": route_id, "updated_fields": changed})


# ===========================================================================
# 3. list-routes
# ===========================================================================
def list_routes(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "route_status", None):
        where.append("route_status = ?")
        params.append(args.route_status)
    if getattr(args, "search", None):
        where.append("(name LIKE ? OR origin LIKE ? OR destination LIKE ?)")
        params.extend([f"%{args.search}%"] * 3)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM logistics_route WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM logistics_route WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
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
    if not conn.execute("SELECT id FROM logistics_route WHERE id = ?", (route_id,)).fetchone():
        err(f"Route {route_id} not found")

    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    stop_type = getattr(args, "stop_type", None) or "delivery"
    _validate_enum(stop_type, VALID_STOP_TYPES, "stop-type")

    stop_id = str(uuid.uuid4())
    stop_order = int(getattr(args, "stop_order", None) or 1)

    conn.execute("""
        INSERT INTO logistics_route_stop (
            id, route_id, stop_order, address, city, state, zip_code,
            estimated_arrival, stop_type, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
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
    where, params = ["1=1"], []
    if route_id:
        where.append("route_id = ?")
        params.append(route_id)
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM logistics_route_stop WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM logistics_route_stop WHERE {where_sql} ORDER BY stop_order ASC LIMIT ? OFFSET ?",
        params
    ).fetchall()
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

    routes = conn.execute(
        "SELECT * FROM logistics_route WHERE company_id = ? AND route_status = 'active' ORDER BY name",
        (company_id,)
    ).fetchall()

    report = []
    for r in routes:
        r_data = row_to_dict(r)
        stops = conn.execute(
            "SELECT * FROM logistics_route_stop WHERE route_id = ? ORDER BY stop_order",
            (r["id"],)
        ).fetchall()
        r_data["stops"] = [row_to_dict(s) for s in stops]
        r_data["stop_count"] = len(stops)

        # Count shipments using this route's origin/destination
        shipment_count = conn.execute(
            "SELECT COUNT(*) FROM logistics_shipment "
            "WHERE company_id = ? AND origin_city = ? AND destination_city = ?",
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
