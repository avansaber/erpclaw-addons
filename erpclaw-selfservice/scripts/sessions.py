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
except ImportError:
    pass

SKILL = "erpclaw-selfservice"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute("SELECT id FROM company WHERE id = ?", (company_id,)).fetchone():
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
    if not conn.execute("SELECT id FROM selfservice_permission_profile WHERE id = ?", (profile_id,)).fetchone():
        err(f"Profile {profile_id} not found")

    token = getattr(args, "token", None)
    if not token:
        err("--token is required")

    expires_at = getattr(args, "expires_at", None)
    if not expires_at:
        err("--expires-at is required")

    portal_id = getattr(args, "portal_id", None)
    if portal_id:
        if not conn.execute("SELECT id FROM selfservice_portal_config WHERE id = ?", (portal_id,)).fetchone():
            err(f"Portal config {portal_id} not found")

    session_id = str(uuid.uuid4())
    now = _now_iso()

    conn.execute("""
        INSERT INTO selfservice_session (
            id, user_id, profile_id, portal_id, token, ip_address, user_agent,
            session_status, expires_at, last_activity_at, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
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
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "user_id", None):
        where.append("user_id = ?")
        params.append(args.user_id)
    if getattr(args, "profile_id", None):
        where.append("profile_id = ?")
        params.append(args.profile_id)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM selfservice_session WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM selfservice_session WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
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
        row = conn.execute("SELECT * FROM selfservice_session WHERE id = ?", (session_id,)).fetchone()
    else:
        row = conn.execute("""
            SELECT s.*, p.allowed_actions, p.denied_actions, p.record_scope,
                   p.field_visibility, p.target_role
            FROM selfservice_session s
            LEFT JOIN selfservice_permission_profile p ON s.profile_id = p.id
            WHERE s.token = ?
        """, (token,)).fetchone()

    if not row:
        err("Session not found")

    d = row_to_dict(row)

    # If fetched by token, check validity
    if token:
        if d["session_status"] != "active":
            err(f"Session is {d['session_status']}")

        now = _now_iso()
        if d["expires_at"] and d["expires_at"] < now:
            conn.execute("UPDATE selfservice_session SET session_status = 'expired' WHERE id = ?", (d["id"],))
            conn.commit()
            err("Session has expired")

        # Update last activity
        conn.execute("UPDATE selfservice_session SET last_activity_at = ? WHERE id = ?", (now, d["id"]))
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
    row = conn.execute("SELECT id, session_status FROM selfservice_session WHERE id = ?",
                       (session_id,)).fetchone()
    if not row:
        err(f"Session {session_id} not found")
    if row["session_status"] == "ended":
        err(f"Session {session_id} is already ended")
    if row["session_status"] == "expired":
        err(f"Session {session_id} is already expired")

    conn.execute("UPDATE selfservice_session SET session_status = 'expired' WHERE id = ?", (session_id,))
    audit(conn, SKILL, "selfservice-expire-session", "selfservice_session", session_id,
          new_values={"session_status": "expired"})
    conn.commit()
    ok({"id": session_id, "session_status": "expired"})


# ===========================================================================
# 5. list-active-sessions
# ===========================================================================
def list_active_sessions(conn, args):
    where, params = ["session_status = 'active'"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "user_id", None):
        where.append("user_id = ?")
        params.append(args.user_id)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM selfservice_session WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM selfservice_session WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
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
