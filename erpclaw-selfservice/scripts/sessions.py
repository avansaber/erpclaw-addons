"""ERPClaw Self-Service -- sessions domain module

Manages self-service sessions with token-based auth, expiration, and activity tracking.
5 actions: create-session, list-sessions, get-session, expire-session, list-active-sessions.
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row
except ImportError:
    pass

SKILL = "erpclaw-selfservice"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    t_co = Table("company")
    q_co = Q.from_(t_co).select(t_co.id).where(t_co.id == P())
    if not conn.execute(q_co.get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _parse_json_fields(d, fields):
    """Parse JSON text fields in a row dict into Python objects."""
    for f in fields:
        if f in d and d[f] is not None:
            try:
                d[f] = json.loads(d[f])
            except (json.JSONDecodeError, TypeError):
                pass
    return d


# ===========================================================================
# 1. create-session
# ===========================================================================
def create_session(conn, args):
    _validate_company(conn, args.company_id)
    user_id = getattr(args, "user_id", None)
    if not user_id:
        err("--user-id is required")

    profile_id = getattr(args, "profile_id", None)
    if not profile_id:
        err("--profile-id is required")
    t_prof = Table("selfservice_permission_profile")
    q_prof = Q.from_(t_prof).select(t_prof.id).where(t_prof.id == P())
    if not conn.execute(q_prof.get_sql(), (profile_id,)).fetchone():
        err(f"Profile {profile_id} not found")

    token = getattr(args, "token", None)
    if not token:
        err("--token is required")

    expires_at = getattr(args, "expires_at", None)
    if not expires_at:
        err("--expires-at is required")

    portal_id = getattr(args, "portal_id", None)
    if portal_id:
        t_pc = Table("selfservice_portal_config")
        q_pc = Q.from_(t_pc).select(t_pc.id).where(t_pc.id == P())
        if not conn.execute(q_pc.get_sql(), (portal_id,)).fetchone():
            err(f"Portal config {portal_id} not found")

    session_id = str(uuid.uuid4())
    now = _now_iso()

    sql, _ = insert_row("selfservice_session", {
        "id": P(), "user_id": P(), "profile_id": P(), "portal_id": P(),
        "token": P(), "ip_address": P(), "user_agent": P(),
        "session_status": P(), "expires_at": P(), "last_activity_at": P(),
        "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        session_id, user_id, profile_id, portal_id, token,
        getattr(args, "ip_address", None),
        getattr(args, "user_agent", None),
        "active", expires_at, now, args.company_id, now,
    ))
    audit(conn, SKILL, "selfservice-create-session", "selfservice_session", session_id,
          new_values={"user_id": user_id, "profile_id": profile_id})
    conn.commit()
    ok({"id": session_id, "user_id": user_id, "profile_id": profile_id,
        "token": token, "session_status": "active", "expires_at": expires_at})


# ===========================================================================
# 2. list-sessions
# ===========================================================================
def list_sessions(conn, args):
    t = Table("selfservice_session")
    q_count = Q.from_(t).select(fn.Count("*"))
    q_rows = Q.from_(t).select(t.star)
    params = []

    if getattr(args, "company_id", None):
        q_count = q_count.where(t.company_id == P())
        q_rows = q_rows.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "user_id", None):
        q_count = q_count.where(t.user_id == P())
        q_rows = q_rows.where(t.user_id == P())
        params.append(args.user_id)
    if getattr(args, "profile_id", None):
        q_count = q_count.where(t.profile_id == P())
        q_rows = q_rows.where(t.profile_id == P())
        params.append(args.profile_id)

    total = conn.execute(q_count.get_sql(), params).fetchone()[0]
    page_params = list(params) + [args.limit, args.offset]
    q_rows = q_rows.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q_rows.get_sql(), page_params).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 3. get-session
# ===========================================================================
def get_session(conn, args):
    session_id = getattr(args, "session_id", None)
    token = getattr(args, "token", None)
    if not session_id and not token:
        err("--session-id or --token is required")

    if session_id:
        t = Table("selfservice_session")
        q = Q.from_(t).select(t.star).where(t.id == P())
        row = conn.execute(q.get_sql(), (session_id,)).fetchone()
    else:
        t_s = Table("selfservice_session")
        t_p = Table("selfservice_permission_profile")
        q = (Q.from_(t_s)
             .left_join(t_p).on(t_s.profile_id == t_p.id)
             .select(t_s.star, t_p.allowed_actions, t_p.denied_actions,
                     t_p.record_scope, t_p.field_visibility, t_p.target_role)
             .where(t_s.token == P()))
        row = conn.execute(q.get_sql(), (token,)).fetchone()

    if not row:
        err("Session not found")

    d = row_to_dict(row)

    # If fetched by token, check validity
    if token:
        if d["session_status"] != "active":
            err(f"Session is {d['session_status']}")

        now = _now_iso()
        if d["expires_at"] and d["expires_at"] < now:
            sql = update_row("selfservice_session", data={"session_status": P()}, where={"id": P()})
            conn.execute(sql, ("expired", d["id"]))
            conn.commit()
            err("Session has expired")

        # Update last activity
        sql = update_row("selfservice_session", data={"last_activity_at": P()}, where={"id": P()})
        conn.execute(sql, (now, d["id"]))
        conn.commit()

        _parse_json_fields(d, ["allowed_actions", "denied_actions", "field_visibility"])
        ok({
            "valid": True,
            "session_id": d["id"],
            "user_id": d["user_id"],
            "profile_id": d["profile_id"],
            "target_role": d.get("target_role"),
            "allowed_actions": d.get("allowed_actions", []),
            "denied_actions": d.get("denied_actions", []),
            "record_scope": d.get("record_scope"),
            "field_visibility": d.get("field_visibility", {}),
            "expires_at": d["expires_at"],
        })
    else:
        ok(d)


# ===========================================================================
# 4. expire-session
# ===========================================================================
def expire_session(conn, args):
    session_id = getattr(args, "session_id", None)
    if not session_id:
        err("--session-id is required")
    t = Table("selfservice_session")
    q = Q.from_(t).select(t.id, t.session_status).where(t.id == P())
    row = conn.execute(q.get_sql(), (session_id,)).fetchone()
    if not row:
        err(f"Session {session_id} not found")
    if row["session_status"] == "ended":
        err(f"Session {session_id} is already ended")
    if row["session_status"] == "expired":
        err(f"Session {session_id} is already expired")

    sql = update_row("selfservice_session", data={"session_status": P()}, where={"id": P()})
    conn.execute(sql, ("expired", session_id))
    audit(conn, SKILL, "selfservice-expire-session", "selfservice_session", session_id,
          new_values={"session_status": "expired"})
    conn.commit()
    ok({"id": session_id, "session_status": "expired"})


# ===========================================================================
# 5. list-active-sessions
# ===========================================================================
def list_active_sessions(conn, args):
    t = Table("selfservice_session")
    q_count = Q.from_(t).select(fn.Count("*")).where(t.session_status == "active")
    q_rows = Q.from_(t).select(t.star).where(t.session_status == "active")
    params = []

    if getattr(args, "company_id", None):
        q_count = q_count.where(t.company_id == P())
        q_rows = q_rows.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "user_id", None):
        q_count = q_count.where(t.user_id == P())
        q_rows = q_rows.where(t.user_id == P())
        params.append(args.user_id)

    total = conn.execute(q_count.get_sql(), params).fetchone()[0]
    page_params = list(params) + [args.limit, args.offset]
    q_rows = q_rows.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q_rows.get_sql(), page_params).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "selfservice-create-session": create_session,
    "selfservice-list-sessions": list_sessions,
    "selfservice-get-session": get_session,
    "selfservice-expire-session": expire_session,
    "selfservice-list-active-sessions": list_active_sessions,
}
