"""ERPClaw Connectors V2 -- reports domain module

Cross-domain reports and status action (4 reports + status).
Imported by db_query.py (unified router).
"""
import os
import sys

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row
except ImportError:
    pass

SKILL = "erpclaw-connectors-v2"

ALL_TABLES = [
    "connv2_booking_connector", "connv2_booking_sync_log",
    "connv2_delivery_connector", "connv2_delivery_order",
    "connv2_realestate_connector", "connv2_realestate_lead",
    "connv2_financial_connector",
    "connv2_productivity_connector",
]

CONNECTOR_TABLES = [
    ("connv2_booking_connector", "booking"),
    ("connv2_delivery_connector", "delivery"),
    ("connv2_realestate_connector", "realestate"),
    ("connv2_financial_connector", "financial"),
    ("connv2_productivity_connector", "productivity"),
]


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field("id")).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


# ===========================================================================
# 1. connector-usage-report
# ===========================================================================
def connector_usage_report(conn, args):
    _validate_company(conn, args.company_id)
    results = []
    for table, domain in CONNECTOR_TABLES:
        row = conn.execute(f"""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN connector_status = 'active' THEN 1 ELSE 0 END) as active,
                   SUM(CASE WHEN connector_status = 'inactive' THEN 1 ELSE 0 END) as inactive,
                   SUM(CASE WHEN connector_status = 'error' THEN 1 ELSE 0 END) as in_error
            FROM {table}
            WHERE company_id = ?
        """, (args.company_id,)).fetchone()
        results.append({
            "domain": domain,
            "total_connectors": row["total"],
            "active": row["active"],
            "inactive": row["inactive"],
            "in_error": row["in_error"],
        })
    ok({"rows": results, "count": len(results)})


# ===========================================================================
# 2. sync-volume-report
# ===========================================================================
def sync_volume_report(conn, args):
    _validate_company(conn, args.company_id)

    # Booking sync volumes
    booking_syncs = conn.execute("""
        SELECT COUNT(*) as total_syncs,
               COALESCE(SUM(records_synced), 0) as total_records,
               COALESCE(SUM(errors), 0) as total_errors
        FROM connv2_booking_sync_log
        WHERE company_id = ?
    """, (args.company_id,)).fetchone()

    # Delivery order volumes
    delivery_orders = conn.execute("""
        SELECT COUNT(*) as total_orders
        FROM connv2_delivery_order
        WHERE company_id = ?
    """, (args.company_id,)).fetchone()

    # Real estate lead volumes
    re_leads = conn.execute("""
        SELECT COUNT(*) as total_leads
        FROM connv2_realestate_lead
        WHERE company_id = ?
    """, (args.company_id,)).fetchone()

    ok({
        "booking_syncs": row_to_dict(booking_syncs),
        "delivery_orders": row_to_dict(delivery_orders),
        "realestate_leads": row_to_dict(re_leads),
    })


# ===========================================================================
# 3. error-rate-report
# ===========================================================================
def error_rate_report(conn, args):
    _validate_company(conn, args.company_id)
    results = []
    for table, domain in CONNECTOR_TABLES:
        row = conn.execute(f"""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN connector_status = 'error' THEN 1 ELSE 0 END) as error_count
            FROM {table}
            WHERE company_id = ?
        """, (args.company_id,)).fetchone()
        total = row["total"]
        error_count = row["error_count"]
        error_rate = round(error_count / total * 100, 1) if total > 0 else 0.0
        results.append({
            "domain": domain,
            "total_connectors": total,
            "error_count": error_count,
            "error_rate_pct": error_rate,
        })
    ok({"rows": results, "count": len(results)})


# ===========================================================================
# 4. status
# ===========================================================================
def status_action(conn, args):
    counts = {}
    for tbl in ALL_TABLES:
        try:
            counts[tbl] = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        except Exception:
            counts[tbl] = -1
    ok({
        "skill": "erpclaw-connectors-v2",
        "version": "1.0.0",
        "total_tables": len(ALL_TABLES),
        "record_counts": counts,
        "domains": ["booking", "delivery", "realestate", "financial", "productivity", "reports"],
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "integration-connector-usage-report": connector_usage_report,
    "integration-sync-volume-report": sync_volume_report,
    "integration-error-rate-report": error_rate_report,
    "status": status_action,
}
