"""ERPClaw Self-Service -- permissions domain module

Manages permission profiles and per-user permission assignments.
8 actions: add-profile, list-profiles, get-profile, update-profile,
           add-permission, list-permissions, remove-permission, validate-permission.
"""
import json
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
except ImportError:
    pass

ENTITY_PREFIXES.setdefault("selfservice_permission_profile", "SSPROF-")

SKILL = "erpclaw-selfservice"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_TARGET_ROLES = ("employee", "client", "tenant", "patient", "vendor", "other")
VALID_RECORD_SCOPES = ("own", "department", "company")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    t_co = Table("company")
    q_co = Q.from_(t_co).select(t_co.id).where(t_co.id == P())
    if not conn.execute(q_co.get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _validate_json(value, field_name):
    """Validate that a value is valid JSON. Returns parsed object or None."""
    if value is None:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        err(f"Invalid JSON for {field_name}: {value}")


def _parse_json_fields(d, fields):
    """Parse JSON text fields in a row dict into Python objects."""
    for f in fields:
        if f in d and d[f] is not None:
            try:
                d[f] = json.loads(d[f])
            except (json.JSONDecodeError, TypeError):
                pass
    return d


JSON_PROFILE_FIELDS = ["allowed_actions", "denied_actions", "field_visibility"]


# ===========================================================================
# 1. add-profile
# ===========================================================================
def add_profile(conn, args):
    _validate_company(conn, args.company_id)
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    target_role = getattr(args, "target_role", None)
    if not target_role:
        err("--target-role is required")
    if target_role not in VALID_TARGET_ROLES:
        err(f"Invalid target_role: {target_role}. Must be one of: {', '.join(VALID_TARGET_ROLES)}")

    record_scope = getattr(args, "record_scope", None) or "own"
    if record_scope not in VALID_RECORD_SCOPES:
        err(f"Invalid record_scope: {record_scope}. Must be one of: {', '.join(VALID_RECORD_SCOPES)}")

    allowed_actions = getattr(args, "allowed_actions", None) or "[]"
    _validate_json(allowed_actions, "allowed_actions")

    denied_actions = getattr(args, "denied_actions", None) or "[]"
    _validate_json(denied_actions, "denied_actions")

    field_visibility = getattr(args, "field_visibility", None) or "{}"
    _validate_json(field_visibility, "field_visibility")

    profile_id = str(uuid.uuid4())
    now = _now_iso()
    conn.company_id = args.company_id
    naming = get_next_name(conn, "selfservice_permission_profile")

    conn.execute("""
        INSERT INTO selfservice_permission_profile (
            id, naming_series, name, description, target_role,
            allowed_actions, denied_actions, record_scope, field_visibility,
            is_active, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        profile_id, naming, name,
        getattr(args, "description", None),
        target_role, allowed_actions, denied_actions, record_scope,
        field_visibility, 1, args.company_id, now, now,
    ))
    audit(conn, SKILL, "selfservice-add-profile", "selfservice_permission_profile", profile_id,
          new_values={"name": name, "target_role": target_role})
    conn.commit()
    ok({"id": profile_id, "naming_series": naming, "name": name, "target_role": target_role})


# ===========================================================================
# 2. list-profiles
# ===========================================================================
def list_profiles(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "target_role", None):
        where.append("target_role = ?")
        params.append(args.target_role)
    if getattr(args, "search", None):
        where.append("(name LIKE ? OR description LIKE ?)")
        params.extend([f"%{args.search}%"] * 2)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM selfservice_permission_profile WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM selfservice_permission_profile WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    items = [_parse_json_fields(row_to_dict(r), JSON_PROFILE_FIELDS) for r in rows]
    ok({
        "rows": items,
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 3. get-profile
# ===========================================================================
def get_profile(conn, args):
    profile_id = getattr(args, "profile_id", None)
    if not profile_id:
        err("--profile-id is required")
    t = Table("selfservice_permission_profile")
    q = Q.from_(t).select(t.star).where(t.id == P())
    row = conn.execute(q.get_sql(), (profile_id,)).fetchone()
    if not row:
        err(f"Profile {profile_id} not found")
    d = row_to_dict(row)
    _parse_json_fields(d, JSON_PROFILE_FIELDS)
    ok(d)


# ===========================================================================
# 4. update-profile
# ===========================================================================
def update_profile(conn, args):
    profile_id = getattr(args, "profile_id", None)
    if not profile_id:
        err("--profile-id is required")
    t_prof = Table("selfservice_permission_profile")
    q_prof = Q.from_(t_prof).select(t_prof.id).where(t_prof.id == P())
    if not conn.execute(q_prof.get_sql(), (profile_id,)).fetchone():
        err(f"Profile {profile_id} not found")

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "name": "name", "description": "description", "target_role": "target_role",
        "allowed_actions": "allowed_actions", "denied_actions": "denied_actions",
        "record_scope": "record_scope", "field_visibility": "field_visibility",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            if arg_name == "target_role" and val not in VALID_TARGET_ROLES:
                err(f"Invalid target_role: {val}. Must be one of: {', '.join(VALID_TARGET_ROLES)}")
            if arg_name == "record_scope" and val not in VALID_RECORD_SCOPES:
                err(f"Invalid record_scope: {val}. Must be one of: {', '.join(VALID_RECORD_SCOPES)}")
            if arg_name in ("allowed_actions", "denied_actions", "field_visibility"):
                _validate_json(val, arg_name)
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    if not updates:
        err("No fields to update")

    updates.append("updated_at = ?")
    params.append(_now_iso())
    params.append(profile_id)
    conn.execute(f"UPDATE selfservice_permission_profile SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, SKILL, "selfservice-update-profile", "selfservice_permission_profile", profile_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": profile_id, "updated_fields": changed})


# ===========================================================================
# 5. add-permission
# ===========================================================================
def add_permission(conn, args):
    _validate_company(conn, args.company_id)
    profile_id = getattr(args, "profile_id", None)
    if not profile_id:
        err("--profile-id is required")
    t_prof = Table("selfservice_permission_profile")
    q_prof = Q.from_(t_prof).select(t_prof.id).where(t_prof.id == P())
    if not conn.execute(q_prof.get_sql(), (profile_id,)).fetchone():
        err(f"Profile {profile_id} not found")

    user_id = getattr(args, "user_id", None)
    if not user_id:
        err("--user-id is required")

    # Check for duplicate active assignment
    t_pa = Table("selfservice_profile_assignment")
    q_dup = (Q.from_(t_pa).select(t_pa.id)
             .where(t_pa.profile_id == P())
             .where(t_pa.user_id == P())
             .where(t_pa.assignment_status == "active"))
    existing = conn.execute(q_dup.get_sql(), (profile_id, user_id)).fetchone()
    if existing:
        err(f"User {user_id} already has an active permission for profile {profile_id}")

    perm_id = str(uuid.uuid4())
    now = _now_iso()

    conn.execute("""
        INSERT INTO selfservice_profile_assignment (
            id, profile_id, user_id, user_email, user_name, assigned_by,
            assignment_status, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        perm_id, profile_id, user_id,
        getattr(args, "user_email", None),
        getattr(args, "user_name", None),
        getattr(args, "assigned_by", None),
        "active", args.company_id, now,
    ))
    audit(conn, SKILL, "selfservice-add-permission", "selfservice_profile_assignment", perm_id,
          new_values={"profile_id": profile_id, "user_id": user_id})
    conn.commit()
    ok({"id": perm_id, "profile_id": profile_id, "user_id": user_id, "permission_status": "active"})


# ===========================================================================
# 6. list-permissions
# ===========================================================================
def list_permissions(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "profile_id", None):
        where.append("profile_id = ?")
        params.append(args.profile_id)
    if getattr(args, "user_id", None):
        where.append("user_id = ?")
        params.append(args.user_id)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM selfservice_profile_assignment WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM selfservice_profile_assignment WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 7. remove-permission
# ===========================================================================
def remove_permission(conn, args):
    permission_id = getattr(args, "permission_id", None)
    if not permission_id:
        err("--permission-id is required")
    t = Table("selfservice_profile_assignment")
    q = Q.from_(t).select(t.id, t.assignment_status).where(t.id == P())
    row = conn.execute(q.get_sql(), (permission_id,)).fetchone()
    if not row:
        err(f"Permission {permission_id} not found")
    if row["assignment_status"] == "revoked":
        err(f"Permission {permission_id} is already revoked")

    sql = update_row("selfservice_profile_assignment",
                     data={"assignment_status": P()},
                     where={"id": P()})
    conn.execute(sql, ("revoked", permission_id))
    audit(conn, SKILL, "selfservice-remove-permission", "selfservice_profile_assignment", permission_id,
          new_values={"assignment_status": "revoked"})
    conn.commit()
    ok({"id": permission_id, "permission_status": "revoked"})


# ===========================================================================
# 8. validate-permission
# ===========================================================================
def validate_permission(conn, args):
    user_id = getattr(args, "user_id", None)
    if not user_id:
        err("--user-id is required")
    action_name = getattr(args, "action_name", None)
    if not action_name:
        err("--action-name is required")

    # Find all active profiles assigned to this user
    rows = conn.execute("""
        SELECT p.allowed_actions, p.denied_actions, p.record_scope, p.name as profile_name
        FROM selfservice_profile_assignment a
        JOIN selfservice_permission_profile p ON a.profile_id = p.id
        WHERE a.user_id = ? AND a.assignment_status = 'active' AND p.is_active = 1
    """, (user_id,)).fetchall()

    if not rows:
        ok({"user_id": user_id, "action": action_name, "permitted": False,
            "reason": "No active permission assignments found"})
        return

    for row in rows:
        d = row_to_dict(row)
        denied = []
        allowed = []
        try:
            denied = json.loads(d["denied_actions"]) if d["denied_actions"] else []
        except (json.JSONDecodeError, TypeError):
            denied = []
        try:
            allowed = json.loads(d["allowed_actions"]) if d["allowed_actions"] else []
        except (json.JSONDecodeError, TypeError):
            allowed = []

        # Deny takes precedence
        if action_name in denied:
            ok({"user_id": user_id, "action": action_name, "permitted": False,
                "reason": f"Action denied by profile: {d['profile_name']}",
                "record_scope": d["record_scope"]})
            return

        if action_name in allowed:
            ok({"user_id": user_id, "action": action_name, "permitted": True,
                "profile_name": d["profile_name"],
                "record_scope": d["record_scope"]})
            return

    # Not found in any profile
    ok({"user_id": user_id, "action": action_name, "permitted": False,
        "reason": "Action not in any assigned profile's allowed list"})


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "selfservice-add-profile": add_profile,
    "selfservice-list-profiles": list_profiles,
    "selfservice-get-profile": get_profile,
    "selfservice-update-profile": update_profile,
    "selfservice-add-permission": add_permission,
    "selfservice-list-permissions": list_permissions,
    "selfservice-remove-permission": remove_permission,
    "selfservice-validate-permission": validate_permission,
}
