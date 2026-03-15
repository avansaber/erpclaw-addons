"""ERPClaw Connectors V2 -- productivity domain module

Actions for productivity platform connectors (1 table, 6 actions).
Supports Google Workspace, Microsoft 365, Slack, Zoom integrations.
Imported by db_query.py (unified router).
"""
import os
import sys
import uuid
from datetime import datetime, timezone

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row

    ENTITY_PREFIXES.setdefault("connv2_productivity_connector", "PDC-")
except ImportError:
    pass

SKILL = "erpclaw-connectors-v2"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_PLATFORMS = ("google_workspace", "microsoft_365", "slack", "zoom")
VALID_CONNECTOR_STATUSES = ("active", "inactive", "error")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field("id")).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _get_connector(conn, connector_id):
    if not connector_id:
        err("--connector-id is required")
    row = conn.execute(Q.from_(Table("connv2_productivity_connector")).select(Table("connv2_productivity_connector").star).where(Field("id") == P()).get_sql(), (connector_id,)).fetchone()
    if not row:
        err(f"Productivity connector {connector_id} not found")
    return row


# ===========================================================================
# 1. add-productivity-connector
# ===========================================================================
def add_productivity_connector(conn, args):
    _validate_company(conn, args.company_id)
    platform = getattr(args, "platform", None)
    if not platform:
        err("--platform is required")
    if platform not in VALID_PLATFORMS:
        err(f"Invalid platform: {platform}. Must be one of: {', '.join(VALID_PLATFORMS)}")

    conn_id = str(uuid.uuid4())
    now = _now_iso()
    conn.company_id = args.company_id
    naming = get_next_name(conn, "connv2_productivity_connector")

    sql, _ = insert_row("connv2_productivity_connector", {"id": P(), "naming_series": P(), "platform": P(), "workspace_id": P(), "api_credentials_ref": P(), "sync_calendar": P(), "sync_contacts": P(), "sync_files": P(), "connector_status": P(), "company_id": P(), "created_at": P(), "updated_at": P()})
    conn.execute(sql, (
        conn_id, naming, platform,
        getattr(args, "workspace_id", None),
        getattr(args, "api_credentials_ref", None),
        1 if getattr(args, "sync_calendar", None) != "0" else 0,
        1 if getattr(args, "sync_contacts", None) != "0" else 0,
        1 if getattr(args, "sync_files", None) == "1" else 0,
        "inactive",
        args.company_id, now, now,
    ))
    audit(conn, SKILL, "integration-add-productivity-connector", "connv2_productivity_connector", conn_id,
          new_values={"platform": platform})
    conn.commit()
    ok({"id": conn_id, "naming_series": naming, "platform": platform,
        "connector_status": "inactive"})


# ===========================================================================
# 2. sync-calendar
# ===========================================================================
def sync_calendar(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    now = _now_iso()
    conn.execute(
        "UPDATE connv2_productivity_connector SET last_sync_at = ?, updated_at = ? WHERE id = ?",
        (now, now, connector_id)
    )
    audit(conn, SKILL, "integration-sync-calendar", "connv2_productivity_connector", connector_id,
          new_values={"sync_type": "calendar"})
    conn.commit()
    ok({"connector_id": connector_id, "sync_type": "calendar", "synced_at": now})


# ===========================================================================
# 3. sync-contacts
# ===========================================================================
def sync_contacts(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    now = _now_iso()
    conn.execute(
        "UPDATE connv2_productivity_connector SET last_sync_at = ?, updated_at = ? WHERE id = ?",
        (now, now, connector_id)
    )
    audit(conn, SKILL, "integration-sync-contacts", "connv2_productivity_connector", connector_id,
          new_values={"sync_type": "contacts"})
    conn.commit()
    ok({"connector_id": connector_id, "sync_type": "contacts", "synced_at": now})


# ===========================================================================
# 4. sync-files
# ===========================================================================
def sync_files(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    now = _now_iso()
    conn.execute(
        "UPDATE connv2_productivity_connector SET last_sync_at = ?, updated_at = ? WHERE id = ?",
        (now, now, connector_id)
    )
    audit(conn, SKILL, "integration-sync-files", "connv2_productivity_connector", connector_id,
          new_values={"sync_type": "files"})
    conn.commit()
    ok({"connector_id": connector_id, "sync_type": "files", "synced_at": now})


# ===========================================================================
# 5. list-productivity-syncs
# ===========================================================================
def list_productivity_syncs(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "platform", None):
        where.append("platform = ?")
        params.append(args.platform)
    if getattr(args, "connector_status", None):
        where.append("connector_status = ?")
        params.append(args.connector_status)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM connv2_productivity_connector WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"""SELECT * FROM connv2_productivity_connector
            WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?""",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 6. sync-status-report
# ===========================================================================
def sync_status_report(conn, args):
    _validate_company(conn, args.company_id)
    rows = conn.execute("""
        SELECT platform,
               COUNT(*) as connector_count,
               SUM(CASE WHEN connector_status = 'active' THEN 1 ELSE 0 END) as active_count,
               SUM(CASE WHEN connector_status = 'error' THEN 1 ELSE 0 END) as error_count,
               SUM(sync_calendar) as calendars_synced,
               SUM(sync_contacts) as contacts_synced,
               SUM(sync_files) as files_synced
        FROM connv2_productivity_connector
        WHERE company_id = ?
        GROUP BY platform
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
    "integration-add-productivity-connector": add_productivity_connector,
    "integration-sync-calendar": sync_calendar,
    "integration-sync-contacts": sync_contacts,
    "integration-sync-files": sync_files,
    "integration-list-productivity-syncs": list_productivity_syncs,
    "integration-sync-status-report": sync_status_report,
}
