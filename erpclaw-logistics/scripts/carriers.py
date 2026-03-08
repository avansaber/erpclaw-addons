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
    if not conn.execute("SELECT id FROM company WHERE id = ?", (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _validate_enum(value, valid_values, field_name):
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")


def _get_carrier(conn, carrier_id):
    if not carrier_id:
        err("--id is required")
    row = conn.execute("SELECT * FROM logistics_carrier WHERE id = ?", (carrier_id,)).fetchone()
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

    carrier_id = str(uuid.uuid4())
    conn.company_id = company_id
    naming = get_next_name(conn, "logistics_carrier", company_id=company_id)
    now = _now_iso()

    conn.execute("""
        INSERT INTO logistics_carrier (
            id, naming_series, name, carrier_code, contact_name, contact_email,
            contact_phone, dot_number, mc_number, carrier_type,
            insurance_expiry, carrier_status, on_time_pct, total_shipments,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        carrier_id, naming, name,
        getattr(args, "carrier_code", None),
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
          new_values={"name": name, "carrier_type": carrier_type})
    conn.commit()
    ok({
        "id": carrier_id, "naming_series": naming, "name": name,
        "carrier_type": carrier_type, "carrier_status": "active",
    })


# ===========================================================================
# 2. update-carrier
# ===========================================================================
def update_carrier(conn, args):
    carrier_id = getattr(args, "id", None)
    _get_carrier(conn, carrier_id)

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "name": "name", "carrier_code": "carrier_code",
        "contact_name": "contact_name", "contact_email": "contact_email",
        "contact_phone": "contact_phone", "dot_number": "dot_number",
        "mc_number": "mc_number", "insurance_expiry": "insurance_expiry",
        "on_time_pct": "on_time_pct",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    carrier_type = getattr(args, "carrier_type", None)
    if carrier_type:
        _validate_enum(carrier_type, VALID_CARRIER_TYPES, "carrier-type")
        updates.append("carrier_type = ?")
        params.append(carrier_type)
        changed.append("carrier_type")

    carrier_status = getattr(args, "carrier_status", None)
    if carrier_status:
        _validate_enum(carrier_status, VALID_CARRIER_STATUSES, "carrier-status")
        updates.append("carrier_status = ?")
        params.append(carrier_status)
        changed.append("carrier_status")

    if not updates:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(carrier_id)
    conn.execute(f"UPDATE logistics_carrier SET {', '.join(updates)} WHERE id = ?", params)
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
    rates = conn.execute(
        "SELECT * FROM logistics_carrier_rate WHERE carrier_id = ? ORDER BY service_level, created_at",
        (carrier_id,)
    ).fetchall()
    data["rates"] = [row_to_dict(r) for r in rates]
    data["rate_count"] = len(rates)

    # Recent shipment stats
    shipments = conn.execute(
        "SELECT shipment_status, COUNT(*) as cnt FROM logistics_shipment "
        "WHERE carrier_id = ? GROUP BY shipment_status", (carrier_id,)
    ).fetchall()
    data["shipment_stats"] = {r[0]: r[1] for r in shipments}

    ok(data)


# ===========================================================================
# 4. list-carriers
# ===========================================================================
def list_carriers(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "carrier_status", None):
        where.append("carrier_status = ?")
        params.append(args.carrier_status)
    if getattr(args, "carrier_type", None):
        where.append("carrier_type = ?")
        params.append(args.carrier_type)
    if getattr(args, "search", None):
        where.append("(name LIKE ? OR carrier_code LIKE ?)")
        params.extend([f"%{args.search}%"] * 2)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM logistics_carrier WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM logistics_carrier WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
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
    if not conn.execute("SELECT id FROM logistics_carrier WHERE id = ?", (carrier_id,)).fetchone():
        err(f"Carrier {carrier_id} not found")

    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    service_level = getattr(args, "service_level", None) or "ground"
    _validate_enum(service_level, VALID_SERVICE_LEVELS, "service-level")

    rate_id = str(uuid.uuid4())

    conn.execute("""
        INSERT INTO logistics_carrier_rate (
            id, carrier_id, origin_zone, destination_zone, service_level,
            weight_min, weight_max, rate_per_unit, flat_rate,
            effective_date, expiry_date, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
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
    where, params = ["1=1"], []
    carrier_id = getattr(args, "carrier_id", None)
    if carrier_id:
        where.append("carrier_id = ?")
        params.append(carrier_id)
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "service_level", None):
        where.append("service_level = ?")
        params.append(args.service_level)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM logistics_carrier_rate WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM logistics_carrier_rate WHERE {where_sql} ORDER BY service_level, created_at LIMIT ? OFFSET ?",
        params
    ).fetchall()
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

    carriers = conn.execute(
        "SELECT id, name, carrier_type, on_time_pct, total_shipments, carrier_status "
        "FROM logistics_carrier WHERE company_id = ? ORDER BY total_shipments DESC",
        (company_id,)
    ).fetchall()

    report = []
    for c in carriers:
        carrier_data = row_to_dict(c)
        # Count actual shipments by status
        stats = conn.execute(
            "SELECT shipment_status, COUNT(*) FROM logistics_shipment "
            "WHERE carrier_id = ? GROUP BY shipment_status", (c["id"],)
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

    carriers = conn.execute(
        "SELECT id, name, carrier_type FROM logistics_carrier "
        "WHERE company_id = ? AND carrier_status = 'active' ORDER BY name",
        (company_id,)
    ).fetchall()

    comparison = []
    for c in carriers:
        c_data = row_to_dict(c)
        rate_where = "carrier_id = ?"
        rate_params = [c["id"]]
        if service_level:
            rate_where += " AND service_level = ?"
            rate_params.append(service_level)

        rates = conn.execute(
            f"SELECT service_level, rate_per_unit, flat_rate FROM logistics_carrier_rate "
            f"WHERE {rate_where} ORDER BY service_level",
            rate_params
        ).fetchall()
        c_data["rates"] = [row_to_dict(r) for r in rates]

        # Average shipping cost from actual shipments
        ship_where = "carrier_id = ? AND shipping_cost IS NOT NULL"
        ship_params = [c["id"]]
        if service_level:
            ship_where += " AND service_level = ?"
            ship_params.append(service_level)

        ship_rows = conn.execute(
            f"SELECT shipping_cost FROM logistics_shipment WHERE {ship_where}",
            ship_params
        ).fetchall()
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
