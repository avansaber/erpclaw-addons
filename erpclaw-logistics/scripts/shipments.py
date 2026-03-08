"""ERPClaw Logistics -- shipments domain module

Actions for shipments, tracking events, and proof of delivery.
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

    ENTITY_PREFIXES.setdefault("logistics_shipment", "SHIP-")
except ImportError:
    pass

SKILL = "erpclaw-logistics"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_SERVICE_LEVELS = ("ground", "express", "overnight", "freight", "ltl")
VALID_SHIPMENT_STATUSES = ("created", "picked_up", "in_transit", "out_for_delivery",
                           "delivered", "exception", "returned")
VALID_EVENT_TYPES = ("created", "picked_up", "departed", "arrived",
                     "out_for_delivery", "delivered", "exception", "returned")


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


def _get_shipment(conn, shipment_id):
    if not shipment_id:
        err("--id is required")
    row = conn.execute("SELECT * FROM logistics_shipment WHERE id = ?", (shipment_id,)).fetchone()
    if not row:
        err(f"Shipment {shipment_id} not found")
    return row


# ===========================================================================
# 1. add-shipment
# ===========================================================================
def add_shipment(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    service_level = getattr(args, "service_level", None) or "ground"
    _validate_enum(service_level, VALID_SERVICE_LEVELS, "service-level")

    # Validate carrier if provided
    carrier_id = getattr(args, "carrier_id", None)
    if carrier_id:
        if not conn.execute("SELECT id FROM logistics_carrier WHERE id = ?", (carrier_id,)).fetchone():
            err(f"Carrier {carrier_id} not found")

    shipment_id = str(uuid.uuid4())
    conn.company_id = company_id
    naming = get_next_name(conn, "logistics_shipment", company_id=company_id)
    now = _now_iso()

    conn.execute("""
        INSERT INTO logistics_shipment (
            id, naming_series, origin_address, origin_city, origin_state, origin_zip,
            destination_address, destination_city, destination_state, destination_zip,
            carrier_id, service_level, weight, dimensions, package_count,
            declared_value, reference_number, shipment_status,
            estimated_delivery, shipping_cost, tracking_number, notes,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        shipment_id, naming,
        getattr(args, "origin_address", None),
        getattr(args, "origin_city", None),
        getattr(args, "origin_state", None),
        getattr(args, "origin_zip", None),
        getattr(args, "destination_address", None),
        getattr(args, "destination_city", None),
        getattr(args, "destination_state", None),
        getattr(args, "destination_zip", None),
        carrier_id, service_level,
        getattr(args, "weight", None),
        getattr(args, "dimensions", None),
        int(getattr(args, "package_count", None) or 1),
        getattr(args, "declared_value", None),
        getattr(args, "reference_number", None),
        "created",
        getattr(args, "estimated_delivery", None),
        getattr(args, "shipping_cost", None),
        getattr(args, "tracking_number", None),
        getattr(args, "notes", None),
        company_id, now, now,
    ))
    audit(conn, SKILL, "logistics-add-shipment", "logistics_shipment", shipment_id,
          new_values={"naming_series": naming, "service_level": service_level})
    conn.commit()
    ok({
        "id": shipment_id, "naming_series": naming,
        "shipment_status": "created", "service_level": service_level,
    })


# ===========================================================================
# 2. update-shipment
# ===========================================================================
def update_shipment(conn, args):
    shipment_id = getattr(args, "id", None)
    _get_shipment(conn, shipment_id)

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "origin_address": "origin_address", "origin_city": "origin_city",
        "origin_state": "origin_state", "origin_zip": "origin_zip",
        "destination_address": "destination_address", "destination_city": "destination_city",
        "destination_state": "destination_state", "destination_zip": "destination_zip",
        "weight": "weight", "dimensions": "dimensions",
        "declared_value": "declared_value", "reference_number": "reference_number",
        "estimated_delivery": "estimated_delivery", "shipping_cost": "shipping_cost",
        "tracking_number": "tracking_number", "notes": "notes",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    service_level = getattr(args, "service_level", None)
    if service_level:
        _validate_enum(service_level, VALID_SERVICE_LEVELS, "service-level")
        updates.append("service_level = ?")
        params.append(service_level)
        changed.append("service_level")

    carrier_id = getattr(args, "carrier_id", None)
    if carrier_id:
        if not conn.execute("SELECT id FROM logistics_carrier WHERE id = ?", (carrier_id,)).fetchone():
            err(f"Carrier {carrier_id} not found")
        updates.append("carrier_id = ?")
        params.append(carrier_id)
        changed.append("carrier_id")

    package_count = getattr(args, "package_count", None)
    if package_count is not None:
        updates.append("package_count = ?")
        params.append(int(package_count))
        changed.append("package_count")

    if not updates:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(shipment_id)
    conn.execute(f"UPDATE logistics_shipment SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, SKILL, "logistics-update-shipment", "logistics_shipment", shipment_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": shipment_id, "updated_fields": changed})


# ===========================================================================
# 3. get-shipment
# ===========================================================================
def get_shipment(conn, args):
    shipment_id = getattr(args, "id", None)
    row = _get_shipment(conn, shipment_id)
    data = row_to_dict(row)

    # Include tracking events
    events = conn.execute(
        "SELECT * FROM logistics_tracking_event WHERE shipment_id = ? ORDER BY event_timestamp DESC",
        (shipment_id,)
    ).fetchall()
    data["tracking_events"] = [row_to_dict(e) for e in events]
    data["event_count"] = len(events)

    # Include freight charges
    charges = conn.execute(
        "SELECT * FROM logistics_freight_charge WHERE shipment_id = ? ORDER BY created_at",
        (shipment_id,)
    ).fetchall()
    data["freight_charges"] = [row_to_dict(c) for c in charges]
    total_freight = sum(Decimal(c["amount"] or "0") for c in data["freight_charges"])
    data["total_freight"] = str(total_freight)

    # Carrier name if available
    if data.get("carrier_id"):
        carrier = conn.execute(
            "SELECT name FROM logistics_carrier WHERE id = ?", (data["carrier_id"],)
        ).fetchone()
        if carrier:
            data["carrier_name"] = carrier[0]

    ok(data)


# ===========================================================================
# 4. list-shipments
# ===========================================================================
def list_shipments(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "shipment_status", None):
        where.append("shipment_status = ?")
        params.append(args.shipment_status)
    if getattr(args, "carrier_id", None):
        where.append("carrier_id = ?")
        params.append(args.carrier_id)
    if getattr(args, "service_level", None):
        where.append("service_level = ?")
        params.append(args.service_level)
    if getattr(args, "search", None):
        where.append("(tracking_number LIKE ? OR reference_number LIKE ? OR notes LIKE ?)")
        params.extend([f"%{args.search}%"] * 3)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM logistics_shipment WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM logistics_shipment WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 5. update-shipment-status
# ===========================================================================
def update_shipment_status(conn, args):
    shipment_id = getattr(args, "id", None)
    row = _get_shipment(conn, shipment_id)
    data = row_to_dict(row)

    new_status = getattr(args, "shipment_status", None)
    if not new_status:
        err("--shipment-status is required")
    _validate_enum(new_status, VALID_SHIPMENT_STATUSES, "shipment-status")

    old_status = data["shipment_status"]

    updates = ["shipment_status = ?", "updated_at = datetime('now')"]
    params = [new_status]

    # Auto-set actual_delivery when delivered
    if new_status == "delivered" and not data.get("actual_delivery"):
        updates.append("actual_delivery = ?")
        params.append(_now_iso())

    params.append(shipment_id)
    conn.execute(
        f"UPDATE logistics_shipment SET {', '.join(updates)} WHERE id = ?", params
    )

    # Update carrier total_shipments on first delivery
    if new_status == "delivered" and old_status != "delivered" and data.get("carrier_id"):
        conn.execute(
            "UPDATE logistics_carrier SET total_shipments = total_shipments + 1, "
            "updated_at = datetime('now') WHERE id = ?",
            (data["carrier_id"],)
        )

    audit(conn, SKILL, "logistics-update-shipment-status", "logistics_shipment", shipment_id,
          old_values={"shipment_status": old_status},
          new_values={"shipment_status": new_status})
    conn.commit()
    ok({"id": shipment_id, "shipment_status": new_status, "old_status": old_status})


# ===========================================================================
# 6. add-tracking-event
# ===========================================================================
def add_tracking_event(conn, args):
    shipment_id = getattr(args, "shipment_id", None)
    if not shipment_id:
        err("--shipment-id is required")
    if not conn.execute("SELECT id FROM logistics_shipment WHERE id = ?", (shipment_id,)).fetchone():
        err(f"Shipment {shipment_id} not found")

    event_type = getattr(args, "event_type", None)
    if not event_type:
        err("--event-type is required")
    _validate_enum(event_type, VALID_EVENT_TYPES, "event-type")

    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    event_id = str(uuid.uuid4())
    event_timestamp = getattr(args, "event_timestamp", None) or _now_iso()

    conn.execute("""
        INSERT INTO logistics_tracking_event (
            id, shipment_id, event_timestamp, event_type, location, description,
            company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?)
    """, (
        event_id, shipment_id, event_timestamp, event_type,
        getattr(args, "location", None),
        getattr(args, "description", None),
        company_id, _now_iso(),
    ))
    audit(conn, SKILL, "logistics-add-tracking-event", "logistics_tracking_event", event_id,
          new_values={"shipment_id": shipment_id, "event_type": event_type})
    conn.commit()
    ok({
        "id": event_id, "shipment_id": shipment_id,
        "event_type": event_type, "event_timestamp": event_timestamp,
    })


# ===========================================================================
# 7. list-tracking-events
# ===========================================================================
def list_tracking_events(conn, args):
    where, params = ["1=1"], []
    shipment_id = getattr(args, "shipment_id", None)
    if shipment_id:
        where.append("shipment_id = ?")
        params.append(shipment_id)
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "event_type", None):
        where.append("event_type = ?")
        params.append(args.event_type)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM logistics_tracking_event WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM logistics_tracking_event WHERE {where_sql} ORDER BY event_timestamp DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 8. add-proof-of-delivery
# ===========================================================================
def add_proof_of_delivery(conn, args):
    shipment_id = getattr(args, "id", None)
    row = _get_shipment(conn, shipment_id)
    data = row_to_dict(row)

    pod_signature = getattr(args, "pod_signature", None)
    if not pod_signature:
        err("--pod-signature is required")

    pod_timestamp = getattr(args, "pod_timestamp", None) or _now_iso()

    conn.execute(
        "UPDATE logistics_shipment SET pod_signature = ?, pod_timestamp = ?, "
        "shipment_status = 'delivered', actual_delivery = COALESCE(actual_delivery, ?), "
        "updated_at = datetime('now') WHERE id = ?",
        (pod_signature, pod_timestamp, pod_timestamp, shipment_id)
    )

    # Update carrier total_shipments if not already delivered
    if data["shipment_status"] != "delivered" and data.get("carrier_id"):
        conn.execute(
            "UPDATE logistics_carrier SET total_shipments = total_shipments + 1, "
            "updated_at = datetime('now') WHERE id = ?",
            (data["carrier_id"],)
        )

    audit(conn, SKILL, "logistics-add-proof-of-delivery", "logistics_shipment", shipment_id,
          new_values={"pod_signature": pod_signature, "pod_timestamp": pod_timestamp})
    conn.commit()
    ok({
        "id": shipment_id, "shipment_status": "delivered",
        "pod_signature": pod_signature, "pod_timestamp": pod_timestamp,
    })


# ===========================================================================
# 9. generate-bill-of-lading
# ===========================================================================
def generate_bill_of_lading(conn, args):
    shipment_id = getattr(args, "id", None)
    row = _get_shipment(conn, shipment_id)
    data = row_to_dict(row)

    carrier_name = None
    if data.get("carrier_id"):
        carrier = conn.execute(
            "SELECT name, carrier_code, dot_number, mc_number FROM logistics_carrier WHERE id = ?",
            (data["carrier_id"],)
        ).fetchone()
        if carrier:
            carrier_name = carrier[0]
            data["carrier_name"] = carrier[0]
            data["carrier_code"] = carrier[1]
            data["carrier_dot"] = carrier[2]
            data["carrier_mc"] = carrier[3]

    # Include freight charges
    charges = conn.execute(
        "SELECT charge_type, description, amount FROM logistics_freight_charge WHERE shipment_id = ?",
        (shipment_id,)
    ).fetchall()
    data["freight_charges"] = [row_to_dict(c) for c in charges]
    total_charges = sum(Decimal(c["amount"] or "0") for c in data["freight_charges"])
    data["total_charges"] = str(total_charges)

    data["document_type"] = "Bill of Lading"
    data["generated_at"] = _now_iso()

    audit(conn, SKILL, "logistics-generate-bill-of-lading", "logistics_shipment", shipment_id)
    ok(data)


# ===========================================================================
# 10. shipment-summary-report
# ===========================================================================
def shipment_summary_report(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    total = conn.execute(
        "SELECT COUNT(*) FROM logistics_shipment WHERE company_id = ?", (company_id,)
    ).fetchone()[0]

    by_status = {}
    rows = conn.execute(
        "SELECT shipment_status, COUNT(*) as cnt FROM logistics_shipment "
        "WHERE company_id = ? GROUP BY shipment_status", (company_id,)
    ).fetchall()
    for r in rows:
        by_status[r[0]] = r[1]

    by_service = {}
    rows2 = conn.execute(
        "SELECT service_level, COUNT(*) as cnt FROM logistics_shipment "
        "WHERE company_id = ? GROUP BY service_level", (company_id,)
    ).fetchall()
    for r in rows2:
        by_service[r[0]] = r[1]

    delivered = by_status.get("delivered", 0)
    on_time = 0
    if delivered > 0:
        on_time = conn.execute(
            "SELECT COUNT(*) FROM logistics_shipment WHERE company_id = ? "
            "AND shipment_status = 'delivered' AND estimated_delivery IS NOT NULL "
            "AND actual_delivery <= estimated_delivery",
            (company_id,)
        ).fetchone()[0]

    ok({
        "report": "shipment-summary",
        "company_id": company_id,
        "total_shipments": total,
        "by_status": by_status,
        "by_service_level": by_service,
        "delivered": delivered,
        "on_time_deliveries": on_time,
        "on_time_pct": str(round(on_time / delivered * 100, 1)) if delivered > 0 else "N/A",
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "logistics-add-shipment": add_shipment,
    "logistics-update-shipment": update_shipment,
    "logistics-get-shipment": get_shipment,
    "logistics-list-shipments": list_shipments,
    "logistics-update-shipment-status": update_shipment_status,
    "logistics-add-tracking-event": add_tracking_event,
    "logistics-list-tracking-events": list_tracking_events,
    "logistics-add-proof-of-delivery": add_proof_of_delivery,
    "logistics-generate-bill-of-lading": generate_bill_of_lading,
    "logistics-shipment-summary-report": shipment_summary_report,
}
