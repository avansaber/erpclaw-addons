"""ERPClaw Logistics -- reports domain module

Cross-domain report actions and skill status.
Imported by db_query.py (unified router).
"""
import os
import sys
from decimal import Decimal

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.response import ok, err
except ImportError:
    pass

SKILL = "erpclaw-logistics"

LOGISTICS_TABLES = [
    "logistics_shipment", "logistics_tracking_event",
    "logistics_carrier", "logistics_carrier_rate",
    "logistics_route", "logistics_route_stop",
    "logistics_freight_charge", "logistics_carrier_invoice",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute("SELECT id FROM company WHERE id = ?", (company_id,)).fetchone():
        err(f"Company {company_id} not found")


# ===========================================================================
# 1. on-time-delivery-report
# ===========================================================================
def on_time_delivery_report(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    total_delivered = conn.execute(
        "SELECT COUNT(*) FROM logistics_shipment WHERE company_id = ? "
        "AND shipment_status = 'delivered'", (company_id,)
    ).fetchone()[0]

    on_time = 0
    late = 0
    no_estimate = 0

    if total_delivered > 0:
        on_time = conn.execute(
            "SELECT COUNT(*) FROM logistics_shipment WHERE company_id = ? "
            "AND shipment_status = 'delivered' AND estimated_delivery IS NOT NULL "
            "AND actual_delivery <= estimated_delivery", (company_id,)
        ).fetchone()[0]

        late = conn.execute(
            "SELECT COUNT(*) FROM logistics_shipment WHERE company_id = ? "
            "AND shipment_status = 'delivered' AND estimated_delivery IS NOT NULL "
            "AND actual_delivery > estimated_delivery", (company_id,)
        ).fetchone()[0]

        no_estimate = total_delivered - on_time - late

    # By carrier
    by_carrier = []
    carriers = conn.execute(
        "SELECT c.id, c.name, COUNT(s.id) as total, "
        "SUM(CASE WHEN s.estimated_delivery IS NOT NULL AND s.actual_delivery <= s.estimated_delivery THEN 1 ELSE 0 END) as on_time_count "
        "FROM logistics_carrier c "
        "JOIN logistics_shipment s ON s.carrier_id = c.id "
        "WHERE s.company_id = ? AND s.shipment_status = 'delivered' "
        "GROUP BY c.id, c.name ORDER BY total DESC",
        (company_id,)
    ).fetchall()
    for c in carriers:
        carrier_total = c[2]
        carrier_on_time = c[3]
        by_carrier.append({
            "carrier_id": c[0],
            "carrier_name": c[1],
            "delivered": carrier_total,
            "on_time": carrier_on_time,
            "on_time_pct": str(round(carrier_on_time / carrier_total * 100, 1)) if carrier_total > 0 else "0",
        })

    ok({
        "report": "on-time-delivery",
        "company_id": company_id,
        "total_delivered": total_delivered,
        "on_time": on_time,
        "late": late,
        "no_estimate": no_estimate,
        "on_time_pct": str(round(on_time / total_delivered * 100, 1)) if total_delivered > 0 else "N/A",
        "by_carrier": by_carrier,
    })


# ===========================================================================
# 2. delivery-exception-report
# ===========================================================================
def delivery_exception_report(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    # Shipments with exception status
    exceptions = conn.execute(
        "SELECT * FROM logistics_shipment WHERE company_id = ? "
        "AND shipment_status = 'exception' ORDER BY updated_at DESC",
        (company_id,)
    ).fetchall()

    # Exception tracking events
    exception_events = conn.execute(
        "SELECT te.*, s.tracking_number, s.reference_number "
        "FROM logistics_tracking_event te "
        "JOIN logistics_shipment s ON s.id = te.shipment_id "
        "WHERE te.company_id = ? AND te.event_type = 'exception' "
        "ORDER BY te.event_timestamp DESC",
        (company_id,)
    ).fetchall()

    # Returned shipments
    returned = conn.execute(
        "SELECT COUNT(*) FROM logistics_shipment WHERE company_id = ? "
        "AND shipment_status = 'returned'", (company_id,)
    ).fetchone()[0]

    ok({
        "report": "delivery-exception",
        "company_id": company_id,
        "total_exceptions": len(exceptions),
        "total_returned": returned,
        "exception_shipments": [
            {
                "id": e["id"], "tracking_number": e["tracking_number"],
                "reference_number": e["reference_number"],
                "destination_city": e["destination_city"],
                "destination_state": e["destination_state"],
                "carrier_id": e["carrier_id"],
            } for e in exceptions
        ],
        "exception_events": [
            {
                "shipment_id": ev["shipment_id"],
                "event_timestamp": ev["event_timestamp"],
                "location": ev["location"],
                "description": ev["description"],
            } for ev in exception_events
        ],
    })


# ===========================================================================
# 3. status
# ===========================================================================
def status_action(conn, args):
    counts = {}
    for tbl in LOGISTICS_TABLES:
        counts[tbl] = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    ok({
        "skill": "erpclaw-logistics",
        "version": "1.0.0",
        "total_tables": len(LOGISTICS_TABLES),
        "record_counts": counts,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "logistics-on-time-delivery-report": on_time_delivery_report,
    "logistics-delivery-exception-report": delivery_exception_report,
    "status": status_action,
}
