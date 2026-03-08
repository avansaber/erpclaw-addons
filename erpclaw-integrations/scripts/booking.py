"""ERPClaw Connectors V2 -- booking domain module

Actions for booking platform connectors (2 tables, 8 actions).
Supports Booking.com, Expedia, Airbnb, VRBO integrations.
Imported by db_query.py (unified router).
"""
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.decimal_utils import to_decimal, round_currency

    ENTITY_PREFIXES.setdefault("connv2_booking_connector", "BKC-")
except ImportError:
    pass

SKILL = "erpclaw-connectors-v2"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_PLATFORMS = ("booking_com", "expedia", "airbnb", "vrbo")
VALID_SYNC_TYPES = ("reservations", "rates", "availability")
VALID_DIRECTIONS = ("inbound", "outbound")
VALID_SYNC_STATUSES = ("pending", "running", "completed", "failed")
VALID_CONNECTOR_STATUSES = ("active", "inactive", "error")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute("SELECT id FROM company WHERE id = ?", (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _get_connector(conn, connector_id):
    if not connector_id:
        err("--connector-id is required")
    row = conn.execute(
        "SELECT * FROM connv2_booking_connector WHERE id = ?", (connector_id,)
    ).fetchone()
    if not row:
        err(f"Booking connector {connector_id} not found")
    return row


# ===========================================================================
# 1. add-booking-connector
# ===========================================================================
def add_booking_connector(conn, args):
    _validate_company(conn, args.company_id)
    platform = getattr(args, "platform", None)
    if not platform:
        err("--platform is required")
    if platform not in VALID_PLATFORMS:
        err(f"Invalid platform: {platform}. Must be one of: {', '.join(VALID_PLATFORMS)}")

    conn_id = str(uuid.uuid4())
    now = _now_iso()
    conn.company_id = args.company_id
    naming = get_next_name(conn, "connv2_booking_connector")

    conn.execute("""
        INSERT INTO connv2_booking_connector (
            id, naming_series, platform, property_id, api_credentials_ref,
            sync_reservations, sync_rates, sync_availability,
            connector_status, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        conn_id, naming, platform,
        getattr(args, "property_id", None),
        getattr(args, "api_credentials_ref", None),
        1 if getattr(args, "sync_reservations", None) != "0" else 0,
        1 if getattr(args, "sync_rates", None) != "0" else 0,
        1 if getattr(args, "sync_availability", None) != "0" else 0,
        "inactive",
        args.company_id, now, now,
    ))
    audit(conn, SKILL, "integration-add-booking-connector", "connv2_booking_connector", conn_id,
          new_values={"platform": platform})
    conn.commit()
    ok({"id": conn_id, "naming_series": naming, "platform": platform,
        "connector_status": "inactive"})


# ===========================================================================
# 2. configure-booking-sync
# ===========================================================================
def configure_booking_sync(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "sync_reservations": "sync_reservations",
        "sync_rates": "sync_rates",
        "sync_availability": "sync_availability",
        "connector_status": "connector_status",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            if arg_name == "connector_status":
                if val not in VALID_CONNECTOR_STATUSES:
                    err(f"Invalid connector_status: {val}")
                updates.append(f"{col_name} = ?")
                params.append(val)
            else:
                updates.append(f"{col_name} = ?")
                params.append(int(val))
            changed.append(col_name)

    if not updates:
        err("No fields to update. Provide --sync-reservations, --sync-rates, --sync-availability, or --connector-status")

    updates.append("updated_at = ?")
    params.append(_now_iso())
    params.append(connector_id)
    conn.execute(
        f"UPDATE connv2_booking_connector SET {', '.join(updates)} WHERE id = ?", params
    )
    audit(conn, SKILL, "integration-configure-booking-sync", "connv2_booking_connector", connector_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": connector_id, "updated_fields": changed})


# ===========================================================================
# 3. sync-reservations
# ===========================================================================
def sync_reservations(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    log_id = str(uuid.uuid4())
    now = _now_iso()
    records = int(getattr(args, "records_synced", None) or 0)
    errors = int(getattr(args, "errors", None) or 0)
    sync_status = "completed" if errors == 0 else "failed"

    company_id = row["company_id"]
    conn.execute("""
        INSERT INTO connv2_booking_sync_log (
            id, connector_id, sync_type, direction, records_synced, errors,
            sync_status, started_at, completed_at, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        log_id, connector_id, "reservations", "inbound",
        records, errors, sync_status, now, now,
        company_id, now,
    ))
    conn.execute(
        "UPDATE connv2_booking_connector SET last_sync_at = ?, updated_at = ? WHERE id = ?",
        (now, now, connector_id)
    )
    audit(conn, SKILL, "integration-sync-reservations", "connv2_booking_sync_log", log_id,
          new_values={"records": records, "errors": errors})
    conn.commit()
    ok({"sync_log_id": log_id, "connector_id": connector_id,
        "records_synced": records, "sync_status": sync_status})


# ===========================================================================
# 4. push-rates
# ===========================================================================
def push_rates(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    log_id = str(uuid.uuid4())
    now = _now_iso()
    records = int(getattr(args, "records_synced", None) or 0)
    errors = int(getattr(args, "errors", None) or 0)
    sync_status = "completed" if errors == 0 else "failed"

    company_id = row["company_id"]
    conn.execute("""
        INSERT INTO connv2_booking_sync_log (
            id, connector_id, sync_type, direction, records_synced, errors,
            sync_status, started_at, completed_at, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        log_id, connector_id, "rates", "outbound",
        records, errors, sync_status, now, now,
        company_id, now,
    ))
    conn.execute(
        "UPDATE connv2_booking_connector SET last_sync_at = ?, updated_at = ? WHERE id = ?",
        (now, now, connector_id)
    )
    audit(conn, SKILL, "integration-push-rates", "connv2_booking_sync_log", log_id,
          new_values={"records": records})
    conn.commit()
    ok({"sync_log_id": log_id, "connector_id": connector_id,
        "records_synced": records, "sync_status": sync_status})


# ===========================================================================
# 5. push-availability
# ===========================================================================
def push_availability(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    log_id = str(uuid.uuid4())
    now = _now_iso()
    records = int(getattr(args, "records_synced", None) or 0)
    errors = int(getattr(args, "errors", None) or 0)
    sync_status = "completed" if errors == 0 else "failed"

    company_id = row["company_id"]
    conn.execute("""
        INSERT INTO connv2_booking_sync_log (
            id, connector_id, sync_type, direction, records_synced, errors,
            sync_status, started_at, completed_at, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        log_id, connector_id, "availability", "outbound",
        records, errors, sync_status, now, now,
        company_id, now,
    ))
    conn.execute(
        "UPDATE connv2_booking_connector SET last_sync_at = ?, updated_at = ? WHERE id = ?",
        (now, now, connector_id)
    )
    audit(conn, SKILL, "integration-push-availability", "connv2_booking_sync_log", log_id,
          new_values={"records": records})
    conn.commit()
    ok({"sync_log_id": log_id, "connector_id": connector_id,
        "records_synced": records, "sync_status": sync_status})


# ===========================================================================
# 6. list-booking-syncs
# ===========================================================================
def list_booking_syncs(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("l.company_id = ?")
        params.append(args.company_id)
    if getattr(args, "connector_id", None):
        where.append("l.connector_id = ?")
        params.append(args.connector_id)
    if getattr(args, "sync_type", None):
        where.append("l.sync_type = ?")
        params.append(args.sync_type)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM connv2_booking_sync_log l WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"""SELECT l.*, c.platform FROM connv2_booking_sync_log l
            LEFT JOIN connv2_booking_connector c ON l.connector_id = c.id
            WHERE {where_sql} ORDER BY l.created_at DESC LIMIT ? OFFSET ?""",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 7. booking-revenue-report
# ===========================================================================
def booking_revenue_report(conn, args):
    _validate_company(conn, args.company_id)
    rows = conn.execute("""
        SELECT c.platform, c.property_id,
               COUNT(l.id) as total_syncs,
               SUM(l.records_synced) as total_records,
               SUM(l.errors) as total_errors
        FROM connv2_booking_connector c
        LEFT JOIN connv2_booking_sync_log l ON c.id = l.connector_id
        WHERE c.company_id = ?
        GROUP BY c.id, c.platform, c.property_id
        ORDER BY total_syncs DESC
    """, (args.company_id,)).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "count": len(rows),
    })


# ===========================================================================
# 8. booking-channel-report
# ===========================================================================
def booking_channel_report(conn, args):
    _validate_company(conn, args.company_id)
    rows = conn.execute("""
        SELECT c.platform,
               COUNT(DISTINCT c.id) as connector_count,
               SUM(CASE WHEN c.connector_status = 'active' THEN 1 ELSE 0 END) as active_count,
               COUNT(l.id) as total_sync_logs,
               SUM(CASE WHEN l.sync_status = 'failed' THEN 1 ELSE 0 END) as failed_syncs
        FROM connv2_booking_connector c
        LEFT JOIN connv2_booking_sync_log l ON c.id = l.connector_id
        WHERE c.company_id = ?
        GROUP BY c.platform
        ORDER BY connector_count DESC
    """, (args.company_id,)).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "count": len(rows),
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "integration-add-booking-connector": add_booking_connector,
    "integration-configure-booking-sync": configure_booking_sync,
    "integration-sync-reservations": sync_reservations,
    "integration-push-rates": push_rates,
    "integration-push-availability": push_availability,
    "integration-list-booking-syncs": list_booking_syncs,
    "integration-booking-revenue-report": booking_revenue_report,
    "integration-booking-channel-report": booking_channel_report,
}
