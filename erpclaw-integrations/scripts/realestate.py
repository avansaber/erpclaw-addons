"""ERPClaw Connectors V2 -- realestate domain module

Actions for real estate platform connectors (2 tables, 6 actions).
Supports Zillow, Realtor.com, MLS, Trulia integrations.
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

    ENTITY_PREFIXES.setdefault("connv2_realestate_connector", "REC-")
except ImportError:
    pass

SKILL = "erpclaw-connectors-v2"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_PLATFORMS = ("zillow", "realtor_com", "mls", "trulia")
VALID_LEAD_STATUSES = ("new", "contacted", "qualified", "converted", "lost")
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
        "SELECT * FROM connv2_realestate_connector WHERE id = ?", (connector_id,)
    ).fetchone()
    if not row:
        err(f"Real estate connector {connector_id} not found")
    return row


# ===========================================================================
# 1. add-realestate-connector
# ===========================================================================
def add_realestate_connector(conn, args):
    _validate_company(conn, args.company_id)
    platform = getattr(args, "platform", None)
    if not platform:
        err("--platform is required")
    if platform not in VALID_PLATFORMS:
        err(f"Invalid platform: {platform}. Must be one of: {', '.join(VALID_PLATFORMS)}")

    conn_id = str(uuid.uuid4())
    now = _now_iso()
    conn.company_id = args.company_id
    naming = get_next_name(conn, "connv2_realestate_connector")

    conn.execute("""
        INSERT INTO connv2_realestate_connector (
            id, naming_series, platform, agent_id, api_credentials_ref,
            sync_listings, capture_leads, connector_status,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        conn_id, naming, platform,
        getattr(args, "agent_id", None),
        getattr(args, "api_credentials_ref", None),
        1 if getattr(args, "sync_listings", None) != "0" else 0,
        1 if getattr(args, "capture_leads", None) != "0" else 0,
        "inactive",
        args.company_id, now, now,
    ))
    audit(conn, SKILL, "integration-add-realestate-connector", "connv2_realestate_connector", conn_id,
          new_values={"platform": platform})
    conn.commit()
    ok({"id": conn_id, "naming_series": naming, "platform": platform,
        "connector_status": "inactive"})


# ===========================================================================
# 2. sync-listings
# ===========================================================================
def sync_listings(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    now = _now_iso()
    conn.execute(
        "UPDATE connv2_realestate_connector SET last_sync_at = ?, updated_at = ? WHERE id = ?",
        (now, now, connector_id)
    )
    audit(conn, SKILL, "integration-sync-listings", "connv2_realestate_connector", connector_id,
          new_values={"sync_type": "listings"})
    conn.commit()
    ok({"connector_id": connector_id, "sync_type": "listings", "synced_at": now})


# ===========================================================================
# 3. capture-leads
# ===========================================================================
def capture_leads(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    lead_id = str(uuid.uuid4())
    now = _now_iso()
    company_id = row["company_id"]

    contact_name = getattr(args, "contact_name", None)
    if not contact_name:
        err("--contact-name is required")

    conn.execute("""
        INSERT INTO connv2_realestate_lead (
            id, connector_id, lead_source, contact_name, contact_email,
            contact_phone, property_ref, inquiry, lead_status,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        lead_id, connector_id,
        getattr(args, "lead_source", None),
        contact_name,
        getattr(args, "contact_email", None),
        getattr(args, "contact_phone", None),
        getattr(args, "property_ref", None),
        getattr(args, "inquiry", None),
        "new",
        company_id, now, now,
    ))
    audit(conn, SKILL, "integration-capture-leads", "connv2_realestate_lead", lead_id,
          new_values={"contact_name": contact_name})
    conn.commit()
    ok({"id": lead_id, "connector_id": connector_id,
        "contact_name": contact_name, "lead_status": "new"})


# ===========================================================================
# 4. list-realestate-syncs
# ===========================================================================
def list_realestate_syncs(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("c.company_id = ?")
        params.append(args.company_id)
    if getattr(args, "connector_id", None):
        where.append("c.id = ?")
        params.append(args.connector_id)
    if getattr(args, "platform", None):
        where.append("c.platform = ?")
        params.append(args.platform)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM connv2_realestate_connector c WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"""SELECT c.* FROM connv2_realestate_connector c
            WHERE {where_sql} ORDER BY c.created_at DESC LIMIT ? OFFSET ?""",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 5. listing-performance-report
# ===========================================================================
def listing_performance_report(conn, args):
    _validate_company(conn, args.company_id)
    rows = conn.execute("""
        SELECT c.platform, c.agent_id,
               c.connector_status,
               COUNT(l.id) as total_leads,
               SUM(CASE WHEN l.lead_status = 'converted' THEN 1 ELSE 0 END) as converted_leads
        FROM connv2_realestate_connector c
        LEFT JOIN connv2_realestate_lead l ON c.id = l.connector_id
        WHERE c.company_id = ?
        GROUP BY c.id, c.platform, c.agent_id, c.connector_status
        ORDER BY total_leads DESC
    """, (args.company_id,)).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "count": len(rows),
    })


# ===========================================================================
# 6. lead-source-report
# ===========================================================================
def lead_source_report(conn, args):
    _validate_company(conn, args.company_id)
    rows = conn.execute("""
        SELECT c.platform,
               l.lead_status,
               COUNT(l.id) as lead_count
        FROM connv2_realestate_lead l
        JOIN connv2_realestate_connector c ON l.connector_id = c.id
        WHERE l.company_id = ?
        GROUP BY c.platform, l.lead_status
        ORDER BY c.platform, lead_count DESC
    """, (args.company_id,)).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "count": len(rows),
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "integration-add-realestate-connector": add_realestate_connector,
    "integration-sync-listings": sync_listings,
    "integration-capture-leads": capture_leads,
    "integration-list-realestate-syncs": list_realestate_syncs,
    "integration-listing-performance-report": listing_performance_report,
    "integration-lead-source-report": lead_source_report,
}
