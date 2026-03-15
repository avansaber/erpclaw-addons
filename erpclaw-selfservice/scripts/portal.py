"""ERPClaw Self-Service -- portal domain module

Manages portal configuration: branding, enabled modules/actions, MFA, timeouts.
6 actions: add-portal-config, list-portal-configs, get-portal-config,
           update-portal-config, activate-portal, deactivate-portal.
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

ENTITY_PREFIXES.setdefault("selfservice_portal_config", "SSPORT-")

SKILL = "erpclaw-selfservice"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

JSON_PORTAL_FIELDS = ["branding_json", "enabled_modules", "enabled_actions"]


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


# ===========================================================================
# 1. add-portal-config
# ===========================================================================
def add_portal_config(conn, args):
    _validate_company(conn, args.company_id)
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    branding_json = getattr(args, "branding_json", None) or "{}"
    _validate_json(branding_json, "branding_json")

    enabled_modules = getattr(args, "enabled_modules", None) or "[]"
    _validate_json(enabled_modules, "enabled_modules")

    enabled_actions = getattr(args, "enabled_actions", None) or "[]"
    _validate_json(enabled_actions, "enabled_actions")

    require_mfa = getattr(args, "require_mfa", None)
    require_mfa_val = int(require_mfa) if require_mfa is not None else 0

    session_timeout = getattr(args, "session_timeout_minutes", None)
    session_timeout_val = int(session_timeout) if session_timeout is not None else 60

    portal_id = str(uuid.uuid4())
    now = _now_iso()
    conn.company_id = args.company_id
    naming = get_next_name(conn, "selfservice_portal_config")

    sql, _ = insert_row("selfservice_portal_config", {
        "id": P(), "naming_series": P(), "name": P(), "branding_json": P(),
        "welcome_message": P(), "enabled_modules": P(), "enabled_actions": P(),
        "require_mfa": P(), "session_timeout_minutes": P(),
        "is_active": P(), "company_id": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        portal_id, naming, name, branding_json,
        getattr(args, "welcome_message", None),
        enabled_modules, enabled_actions,
        require_mfa_val, session_timeout_val,
        1, args.company_id, now, now,
    ))
    audit(conn, SKILL, "selfservice-add-portal-config", "selfservice_portal_config", portal_id,
          new_values={"name": name})
    conn.commit()
    ok({"id": portal_id, "naming_series": naming, "name": name})


# ===========================================================================
# 2. list-portal-configs
# ===========================================================================
def list_portal_configs(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "search", None):
        where.append("(name LIKE ? OR welcome_message LIKE ?)")
        params.extend([f"%{args.search}%"] * 2)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM selfservice_portal_config WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM selfservice_portal_config WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    items = [_parse_json_fields(row_to_dict(r), JSON_PORTAL_FIELDS) for r in rows]
    ok({
        "rows": items,
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 3. get-portal-config
# ===========================================================================
def get_portal_config(conn, args):
    portal_id = getattr(args, "portal_id", None)
    if not portal_id:
        err("--portal-id is required")
    t = Table("selfservice_portal_config")
    q = Q.from_(t).select(t.star).where(t.id == P())
    row = conn.execute(q.get_sql(), (portal_id,)).fetchone()
    if not row:
        err(f"Portal config {portal_id} not found")
    d = row_to_dict(row)
    _parse_json_fields(d, JSON_PORTAL_FIELDS)
    ok(d)


# ===========================================================================
# 4. update-portal-config
# ===========================================================================
def update_portal_config(conn, args):
    portal_id = getattr(args, "portal_id", None)
    if not portal_id:
        err("--portal-id is required")
    t = Table("selfservice_portal_config")
    q = Q.from_(t).select(t.id).where(t.id == P())
    if not conn.execute(q.get_sql(), (portal_id,)).fetchone():
        err(f"Portal config {portal_id} not found")

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "name": "name", "branding_json": "branding_json",
        "welcome_message": "welcome_message",
        "enabled_modules": "enabled_modules", "enabled_actions": "enabled_actions",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            if arg_name in ("branding_json", "enabled_modules", "enabled_actions"):
                _validate_json(val, arg_name)
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    require_mfa = getattr(args, "require_mfa", None)
    if require_mfa is not None:
        updates.append("require_mfa = ?")
        params.append(int(require_mfa))
        changed.append("require_mfa")

    session_timeout = getattr(args, "session_timeout_minutes", None)
    if session_timeout is not None:
        updates.append("session_timeout_minutes = ?")
        params.append(int(session_timeout))
        changed.append("session_timeout_minutes")

    if not updates:
        err("No fields to update")

    updates.append("updated_at = ?")
    params.append(_now_iso())
    params.append(portal_id)
    conn.execute(f"UPDATE selfservice_portal_config SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, SKILL, "selfservice-update-portal-config", "selfservice_portal_config", portal_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": portal_id, "updated_fields": changed})


# ===========================================================================
# 5. activate-portal
# ===========================================================================
def activate_portal(conn, args):
    portal_id = getattr(args, "portal_id", None)
    if not portal_id:
        err("--portal-id is required")
    t = Table("selfservice_portal_config")
    q = Q.from_(t).select(t.id, t.is_active).where(t.id == P())
    row = conn.execute(q.get_sql(), (portal_id,)).fetchone()
    if not row:
        err(f"Portal config {portal_id} not found")
    if row["is_active"] == 1:
        err(f"Portal {portal_id} is already active")

    conn.execute("UPDATE selfservice_portal_config SET is_active = 1, updated_at = ? WHERE id = ?",
                 (_now_iso(), portal_id))
    audit(conn, SKILL, "selfservice-activate-portal", "selfservice_portal_config", portal_id,
          new_values={"is_active": 1})
    conn.commit()
    ok({"id": portal_id, "portal_status": "active"})


# ===========================================================================
# 6. deactivate-portal
# ===========================================================================
def deactivate_portal(conn, args):
    portal_id = getattr(args, "portal_id", None)
    if not portal_id:
        err("--portal-id is required")
    t = Table("selfservice_portal_config")
    q = Q.from_(t).select(t.id, t.is_active).where(t.id == P())
    row = conn.execute(q.get_sql(), (portal_id,)).fetchone()
    if not row:
        err(f"Portal config {portal_id} not found")
    if row["is_active"] == 0:
        err(f"Portal {portal_id} is already inactive")

    conn.execute("UPDATE selfservice_portal_config SET is_active = 0, updated_at = ? WHERE id = ?",
                 (_now_iso(), portal_id))
    audit(conn, SKILL, "selfservice-deactivate-portal", "selfservice_portal_config", portal_id,
          new_values={"is_active": 0})
    conn.commit()
    ok({"id": portal_id, "portal_status": "inactive"})


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "selfservice-add-portal-config": add_portal_config,
    "selfservice-list-portal-configs": list_portal_configs,
    "selfservice-get-portal-config": get_portal_config,
    "selfservice-update-portal-config": update_portal_config,
    "selfservice-activate-portal": activate_portal,
    "selfservice-deactivate-portal": deactivate_portal,
}
