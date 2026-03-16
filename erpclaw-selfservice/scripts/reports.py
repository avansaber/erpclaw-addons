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
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row
except ImportError:
    pass

SKILL = "erpclaw-selfservice"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_ACTIVITY_RESULTS = ("allowed", "denied", "error")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    t_co = Table("company")
    q_co = Q.from_(t_co).select(t_co.id).where(t_co.id == P())
    if not conn.execute(q_co.get_sql(), (company_id,)).fetchone():
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
        t_ss = Table("selfservice_session")
        q_ss = Q.from_(t_ss).select(t_ss.id).where(t_ss.id == P())
        if not conn.execute(q_ss.get_sql(), (session_id,)).fetchone():
            err(f"Session {session_id} not found")

    log_id = str(uuid.uuid4())
    now = _now_iso()

    sql, _ = insert_row("selfservice_activity_log", {
        "id": P(), "session_id": P(), "user_id": P(), "action": P(),
        "entity_type": P(), "entity_id": P(), "result": P(),
        "ip_address": P(), "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
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

    t = Table("selfservice_activity_log")
    q_breakdown = (Q.from_(t)
                   .select(t.action, t.result, fn.Count("*").as_("count"))
                   .where(t.company_id == P())
                   .groupby(t.action, t.result)
                   .orderby(Field("count"), order=Order.desc)
                   .limit(P()).offset(P()))
    rows = conn.execute(q_breakdown.get_sql(), (args.company_id, args.limit, args.offset)).fetchall()

    q_total = Q.from_(t).select(fn.Count("*")).where(t.company_id == P())
    total_activities = conn.execute(q_total.get_sql(), (args.company_id,)).fetchone()[0]

    from erpclaw_lib.vendor.pypika.terms import LiteralValue
    q_users = Q.from_(t).select(LiteralValue('COUNT(DISTINCT "user_id")')).where(t.company_id == P())
    total_users = conn.execute(q_users.get_sql(), (args.company_id,)).fetchone()[0]

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

    t_pc = Table("selfservice_portal_config")
    q_portals = (Q.from_(t_pc)
                 .select(t_pc.id, t_pc.name, t_pc.is_active, t_pc.session_timeout_minutes)
                 .where(t_pc.company_id == P()))
    portals = conn.execute(q_portals.get_sql(), (args.company_id,)).fetchall()

    t_ss = Table("selfservice_session")
    portal_stats = []
    for p in portals:
        pd = row_to_dict(p)
        q_sc = (Q.from_(t_ss).select(fn.Count("*"))
                .where(t_ss.portal_id == P()).where(t_ss.company_id == P()))
        session_count = conn.execute(q_sc.get_sql(), (pd["id"], args.company_id)).fetchone()[0]

        q_ac = (Q.from_(t_ss).select(fn.Count("*"))
                .where(t_ss.portal_id == P()).where(t_ss.session_status == "active")
                .where(t_ss.company_id == P()))
        active_sessions = conn.execute(q_ac.get_sql(), (pd["id"], args.company_id)).fetchone()[0]
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

    t_a = Table("selfservice_activity_log")
    t_s = Table("selfservice_session")
    q_trail = (Q.from_(t_a)
               .left_join(t_s).on(t_a.session_id == t_s.id)
               .select(t_a.user_id, t_a.action, t_a.result, t_a.entity_type,
                       t_a.entity_id, t_a.created_at,
                       t_s.token, t_s.ip_address.as_("session_ip"))
               .where(t_a.company_id == P())
               .orderby(t_a.created_at, order=Order.desc)
               .limit(P()).offset(P()))
    rows = conn.execute(q_trail.get_sql(), (args.company_id, args.limit, args.offset)).fetchall()

    q_denied = (Q.from_(t_a).select(fn.Count("*"))
                .where(t_a.company_id == P()).where(t_a.result == "denied"))
    denied_count = conn.execute(q_denied.get_sql(), (args.company_id,)).fetchone()[0]

    q_error = (Q.from_(t_a).select(fn.Count("*"))
               .where(t_a.company_id == P()).where(t_a.result == "error"))
    error_count = conn.execute(q_error.get_sql(), (args.company_id,)).fetchone()[0]

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

    t_ss = Table("selfservice_session")

    q_active = (Q.from_(t_ss).select(fn.Count("*"))
                .where(t_ss.company_id == P()).where(t_ss.session_status == "active"))
    total_active = conn.execute(q_active.get_sql(), (args.company_id,)).fetchone()[0]

    q_expired = (Q.from_(t_ss).select(fn.Count("*"))
                 .where(t_ss.company_id == P()).where(t_ss.session_status == "expired"))
    total_expired = conn.execute(q_expired.get_sql(), (args.company_id,)).fetchone()[0]

    q_ended = (Q.from_(t_ss).select(fn.Count("*"))
               .where(t_ss.company_id == P()).where(t_ss.session_status == "ended"))
    total_ended = conn.execute(q_ended.get_sql(), (args.company_id,)).fetchone()[0]

    t_p = Table("selfservice_permission_profile")
    q_bp = (Q.from_(t_ss)
            .join(t_p).on(t_ss.profile_id == t_p.id)
            .select(t_p.name.as_("profile_name"), fn.Count("*").as_("active_count"))
            .where(t_ss.company_id == P()).where(t_ss.session_status == "active")
            .groupby(t_p.name)
            .orderby(Field("active_count"), order=Order.desc))
    by_profile = conn.execute(q_bp.get_sql(), (args.company_id,)).fetchall()

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
            t = Table(tbl)
            q = Q.from_(t).select(fn.Count("*"))
            tables[tbl] = conn.execute(q.get_sql()).fetchone()[0]
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
