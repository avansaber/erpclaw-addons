"""ERPClaw Self-Service -- reports domain module

Activity logging, usage analytics, and status reporting.
6 actions: usage-report, portal-analytics-report, permission-audit-report,
           active-sessions-report, log-activity, status.
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.response import ok, err, row_to_dict
except ImportError:
    pass

SKILL = "erpclaw-selfservice"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_ACTIVITY_RESULTS = ("allowed", "denied", "error")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute("SELECT id FROM company WHERE id = ?", (company_id,)).fetchone():
        err(f"Company {company_id} not found")


# ===========================================================================
# 1. log-activity
# ===========================================================================
def log_activity(conn, args):
    _validate_company(conn, args.company_id)
    user_id = getattr(args, "user_id", None)
    if not user_id:
        err("--user-id is required")
    action_name = getattr(args, "action_name", None)
    if not action_name:
        err("--action-name is required")

    result_val = getattr(args, "result", None) or "allowed"
    if result_val not in VALID_ACTIVITY_RESULTS:
        err(f"Invalid result: {result_val}. Must be one of: {', '.join(VALID_ACTIVITY_RESULTS)}")

    session_id = getattr(args, "session_id", None)
    if session_id:
        if not conn.execute("SELECT id FROM selfservice_session WHERE id = ?", (session_id,)).fetchone():
            err(f"Session {session_id} not found")

    log_id = str(uuid.uuid4())
    now = _now_iso()

    conn.execute("""
        INSERT INTO selfservice_activity_log (
            id, session_id, user_id, action, entity_type, entity_id,
            result, ip_address, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        log_id, session_id, user_id, action_name,
        getattr(args, "entity_type", None),
        getattr(args, "entity_id", None),
        result_val,
        getattr(args, "ip_address", None),
        args.company_id, now,
    ))
    conn.commit()
    ok({"id": log_id, "user_id": user_id, "action": action_name, "result": result_val})


# ===========================================================================
# 2. usage-report
# ===========================================================================
def usage_report(conn, args):
    _validate_company(conn, args.company_id)

    rows = conn.execute("""
        SELECT action, result, COUNT(*) as count
        FROM selfservice_activity_log
        WHERE company_id = ?
        GROUP BY action, result
        ORDER BY count DESC
        LIMIT ? OFFSET ?
    """, (args.company_id, args.limit, args.offset)).fetchall()

    total_activities = conn.execute(
        "SELECT COUNT(*) FROM selfservice_activity_log WHERE company_id = ?",
        (args.company_id,)
    ).fetchone()[0]

    total_users = conn.execute(
        "SELECT COUNT(DISTINCT user_id) FROM selfservice_activity_log WHERE company_id = ?",
        (args.company_id,)
    ).fetchone()[0]

    ok({
        "company_id": args.company_id,
        "total_activities": total_activities,
        "unique_users": total_users,
        "action_breakdown": [row_to_dict(r) for r in rows],
    })


# ===========================================================================
# 3. portal-analytics-report
# ===========================================================================
def portal_analytics_report(conn, args):
    _validate_company(conn, args.company_id)

    portals = conn.execute(
        "SELECT id, name, is_active, session_timeout_minutes FROM selfservice_portal_config WHERE company_id = ?",
        (args.company_id,)
    ).fetchall()

    portal_stats = []
    for p in portals:
        pd = row_to_dict(p)
        session_count = conn.execute(
            "SELECT COUNT(*) FROM selfservice_session WHERE portal_id = ? AND company_id = ?",
            (pd["id"], args.company_id)
        ).fetchone()[0]
        active_sessions = conn.execute(
            "SELECT COUNT(*) FROM selfservice_session WHERE portal_id = ? AND session_status = 'active' AND company_id = ?",
            (pd["id"], args.company_id)
        ).fetchone()[0]
        pd["total_sessions"] = session_count
        pd["active_sessions"] = active_sessions
        portal_stats.append(pd)

    ok({
        "company_id": args.company_id,
        "total_portals": len(portals),
        "portals": portal_stats,
    })


# ===========================================================================
# 4. permission-audit-report
# ===========================================================================
def permission_audit_report(conn, args):
    _validate_company(conn, args.company_id)

    rows = conn.execute("""
        SELECT a.user_id, a.action, a.result, a.entity_type, a.entity_id, a.created_at,
               s.token, s.ip_address as session_ip
        FROM selfservice_activity_log a
        LEFT JOIN selfservice_session s ON a.session_id = s.id
        WHERE a.company_id = ?
        ORDER BY a.created_at DESC
        LIMIT ? OFFSET ?
    """, (args.company_id, args.limit, args.offset)).fetchall()

    denied_count = conn.execute(
        "SELECT COUNT(*) FROM selfservice_activity_log WHERE company_id = ? AND result = 'denied'",
        (args.company_id,)
    ).fetchone()[0]

    error_count = conn.execute(
        "SELECT COUNT(*) FROM selfservice_activity_log WHERE company_id = ? AND result = 'error'",
        (args.company_id,)
    ).fetchone()[0]

    ok({
        "company_id": args.company_id,
        "denied_count": denied_count,
        "error_count": error_count,
        "audit_trail": [row_to_dict(r) for r in rows],
    })


# ===========================================================================
# 5. active-sessions-report
# ===========================================================================
def active_sessions_report(conn, args):
    _validate_company(conn, args.company_id)

    total_active = conn.execute(
        "SELECT COUNT(*) FROM selfservice_session WHERE company_id = ? AND session_status = 'active'",
        (args.company_id,)
    ).fetchone()[0]

    total_expired = conn.execute(
        "SELECT COUNT(*) FROM selfservice_session WHERE company_id = ? AND session_status = 'expired'",
        (args.company_id,)
    ).fetchone()[0]

    total_ended = conn.execute(
        "SELECT COUNT(*) FROM selfservice_session WHERE company_id = ? AND session_status = 'ended'",
        (args.company_id,)
    ).fetchone()[0]

    by_profile = conn.execute("""
        SELECT p.name as profile_name, COUNT(*) as active_count
        FROM selfservice_session s
        JOIN selfservice_permission_profile p ON s.profile_id = p.id
        WHERE s.company_id = ? AND s.session_status = 'active'
        GROUP BY p.name
        ORDER BY active_count DESC
    """, (args.company_id,)).fetchall()

    ok({
        "company_id": args.company_id,
        "active": total_active,
        "expired": total_expired,
        "ended": total_ended,
        "by_profile": [row_to_dict(r) for r in by_profile],
    })


# ===========================================================================
# 6. status
# ===========================================================================
def status_action(conn, args):
    tables = {
        "selfservice_permission_profile": 0,
        "selfservice_profile_assignment": 0,
        "selfservice_portal_config": 0,
        "selfservice_session": 0,
        "selfservice_activity_log": 0,
    }
    for tbl in tables:
        try:
            tables[tbl] = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        except Exception:
            tables[tbl] = -1

    ok({
        "skill": SKILL,
        "version": "1.0.0",
        "tables": tables,
        "healthy": all(v >= 0 for v in tables.values()),
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "selfservice-log-activity": log_activity,
    "selfservice-usage-report": usage_report,
    "selfservice-portal-analytics-report": portal_analytics_report,
    "selfservice-permission-audit-report": permission_audit_report,
    "selfservice-active-sessions-report": active_sessions_report,
    "status": status_action,
}
