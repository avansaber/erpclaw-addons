"""ERPClaw Integrations -- connectors domain module

Actions for managing connector configs, credentials, and webhooks.
14 actions across 3 sub-entities.
Imported by db_query.py (unified router).
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

    ENTITY_PREFIXES.setdefault("integration_connector", "INT-")
except ImportError:
    pass

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------------------------------------------------------------------
# Validation constants
# ---------------------------------------------------------------------------
VALID_PLATFORMS = ("shopify", "woocommerce", "amazon", "quickbooks", "stripe", "square", "xero", "custom")
VALID_CONNECTOR_TYPES = ("inbound", "outbound", "bidirectional")
VALID_CONNECTOR_STATUSES = ("active", "inactive", "error")
VALID_CREDENTIAL_TYPES = ("api_key", "oauth2", "basic_auth", "webhook_secret")

SKILL = "erpclaw-integrations"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute("SELECT id FROM company WHERE id = ?", (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _validate_enum(value, valid_values, field_name):
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")


def _get_connector(conn, connector_id):
    if not connector_id:
        err("--connector-id is required")
    row = conn.execute("SELECT * FROM integration_connector WHERE id = ?", (connector_id,)).fetchone()
    if not row:
        err(f"Connector {connector_id} not found")
    return row


# ===========================================================================
# 1. add-connector
# ===========================================================================
def add_connector(conn, args):
    _validate_company(conn, args.company_id)
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")
    platform = getattr(args, "platform", None)
    if not platform:
        err("--platform is required")
    _validate_enum(platform, VALID_PLATFORMS, "platform")

    connector_type = getattr(args, "connector_type", None) or "bidirectional"
    _validate_enum(connector_type, VALID_CONNECTOR_TYPES, "connector-type")

    config_json = getattr(args, "config_json", None) or "{}"
    # Validate JSON
    try:
        json.loads(config_json)
    except (json.JSONDecodeError, TypeError):
        err("--config-json must be valid JSON")

    cid = str(uuid.uuid4())
    naming = get_next_name(conn, "integration_connector", company_id=args.company_id)
    now = _now_iso()

    conn.execute("""
        INSERT INTO integration_connector (
            id, naming_series, name, platform, connector_type, base_url,
            connector_status, config_json, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        cid, naming, name, platform, connector_type,
        getattr(args, "base_url", None),
        "inactive", config_json, args.company_id, now, now,
    ))
    audit(conn, SKILL, "integration-add-connector", "integration_connector", cid,
          new_values={"name": name, "platform": platform})
    conn.commit()
    ok({"id": cid, "naming_series": naming, "name": name,
        "platform": platform, "connector_status": "inactive"})


# ===========================================================================
# 2. update-connector
# ===========================================================================
def update_connector(conn, args):
    cid = getattr(args, "connector_id", None)
    row = _get_connector(conn, cid)

    updates, params, changed = [], [], []

    for col, arg_name, validator in [
        ("name", "name", None),
        ("platform", "platform", VALID_PLATFORMS),
        ("connector_type", "connector_type", VALID_CONNECTOR_TYPES),
        ("base_url", "base_url", None),
        ("config_json", "config_json", None),
    ]:
        val = getattr(args, arg_name, None)
        if val is not None:
            if validator:
                _validate_enum(val, validator, arg_name.replace("_", "-"))
            if col == "config_json":
                try:
                    json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    err("--config-json must be valid JSON")
            updates.append(f"{col} = ?")
            params.append(val)
            changed.append(col)

    if not updates:
        err("No fields to update. Provide at least one field flag.")

    updates.append("updated_at = ?")
    params.append(_now_iso())
    params.append(cid)

    conn.execute(
        f"UPDATE integration_connector SET {', '.join(updates)} WHERE id = ?",
        params,
    )
    audit(conn, SKILL, "integration-update-connector", "integration_connector", cid,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": cid, "updated_fields": changed})


# ===========================================================================
# 3. get-connector
# ===========================================================================
def get_connector(conn, args):
    cid = getattr(args, "connector_id", None)
    row = _get_connector(conn, cid)
    data = row_to_dict(row)

    # Count child entities
    cred_count = conn.execute(
        "SELECT COUNT(*) FROM integration_credential WHERE connector_id = ?", (cid,)
    ).fetchone()[0]
    webhook_count = conn.execute(
        "SELECT COUNT(*) FROM integration_webhook WHERE connector_id = ?", (cid,)
    ).fetchone()[0]
    mapping_count = conn.execute(
        "SELECT COUNT(*) FROM integration_field_mapping WHERE connector_id = ?", (cid,)
    ).fetchone()[0]

    data["credential_count"] = cred_count
    data["webhook_count"] = webhook_count
    data["mapping_count"] = mapping_count
    ok(data)


# ===========================================================================
# 4. list-connectors
# ===========================================================================
def list_connectors(conn, args):
    where, params = [], []
    company_id = getattr(args, "company_id", None)
    if company_id:
        where.append("company_id = ?")
        params.append(company_id)
    platform = getattr(args, "platform", None)
    if platform:
        where.append("platform = ?")
        params.append(platform)
    connector_status = getattr(args, "connector_status", None)
    if connector_status:
        where.append("connector_status = ?")
        params.append(connector_status)
    search = getattr(args, "search", None)
    if search:
        where.append("(name LIKE ? OR platform LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    clause = (" WHERE " + " AND ".join(where)) if where else ""

    total = conn.execute(f"SELECT COUNT(*) FROM integration_connector{clause}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM integration_connector{clause} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [args.limit, args.offset],
    ).fetchall()

    ok({"connectors": [row_to_dict(r) for r in rows], "total_count": total})


# ===========================================================================
# 5. activate-connector
# ===========================================================================
def activate_connector(conn, args):
    cid = getattr(args, "connector_id", None)
    row = _get_connector(conn, cid)
    current = row_to_dict(row)
    if current["connector_status"] == "active":
        err("Connector is already active")

    conn.execute(
        "UPDATE integration_connector SET connector_status = 'active', updated_at = ? WHERE id = ?",
        (_now_iso(), cid),
    )
    audit(conn, SKILL, "integration-activate-connector", "integration_connector", cid)
    conn.commit()
    ok({"id": cid, "connector_status": "active"})


# ===========================================================================
# 6. deactivate-connector
# ===========================================================================
def deactivate_connector(conn, args):
    cid = getattr(args, "connector_id", None)
    row = _get_connector(conn, cid)
    current = row_to_dict(row)
    if current["connector_status"] == "inactive":
        err("Connector is already inactive")

    conn.execute(
        "UPDATE integration_connector SET connector_status = 'inactive', updated_at = ? WHERE id = ?",
        (_now_iso(), cid),
    )
    audit(conn, SKILL, "integration-deactivate-connector", "integration_connector", cid)
    conn.commit()
    ok({"id": cid, "connector_status": "inactive"})


# ===========================================================================
# 7. test-connector
# ===========================================================================
def check_connector(conn, args):
    """Validate connector config is complete (has credentials, has URL if needed).
    Does NOT make actual network calls."""
    cid = getattr(args, "connector_id", None)
    row = _get_connector(conn, cid)
    data = row_to_dict(row)

    issues = []
    # Check credentials exist
    cred_count = conn.execute(
        "SELECT COUNT(*) FROM integration_credential WHERE connector_id = ?", (cid,)
    ).fetchone()[0]
    if cred_count == 0:
        issues.append("No credentials configured")

    # Check base_url for platforms that need it
    needs_url = ("shopify", "woocommerce", "amazon", "quickbooks", "xero")
    if data["platform"] in needs_url and not data.get("base_url"):
        issues.append(f"Platform '{data['platform']}' requires a base_url")

    # Check config_json is valid
    try:
        json.loads(data.get("config_json", "{}"))
    except (json.JSONDecodeError, TypeError):
        issues.append("config_json is not valid JSON")

    test_passed = len(issues) == 0
    audit(conn, SKILL, "integration-test-connector", "integration_connector", cid,
          new_values={"test_passed": test_passed, "issues": issues})
    conn.commit()
    ok({"id": cid, "test_passed": test_passed, "issues": issues,
        "credential_count": cred_count})


# ===========================================================================
# 8. add-connector-credential
# ===========================================================================
def add_connector_credential(conn, args):
    cid = getattr(args, "connector_id", None)
    row = _get_connector(conn, cid)
    company_id = row_to_dict(row)["company_id"]

    credential_type = getattr(args, "credential_type", None)
    if not credential_type:
        err("--credential-type is required")
    _validate_enum(credential_type, VALID_CREDENTIAL_TYPES, "credential-type")

    credential_key = getattr(args, "credential_key", None)
    if not credential_key:
        err("--credential-key is required")
    credential_value = getattr(args, "credential_value", None)
    if not credential_value:
        err("--credential-value is required")

    cred_id = str(uuid.uuid4())
    conn.execute("""
        INSERT INTO integration_credential (
            id, connector_id, credential_type, credential_key, credential_value,
            expires_at, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?)
    """, (
        cred_id, cid, credential_type, credential_key, credential_value,
        getattr(args, "expires_at", None), company_id, _now_iso(),
    ))
    audit(conn, SKILL, "integration-add-connector-credential", "integration_credential", cred_id)
    conn.commit()
    ok({"id": cred_id, "connector_id": cid, "credential_type": credential_type,
        "credential_key": credential_key})


# ===========================================================================
# 9. list-connector-credentials
# ===========================================================================
def list_connector_credentials(conn, args):
    cid = getattr(args, "connector_id", None)
    if not cid:
        err("--connector-id is required")

    total = conn.execute(
        "SELECT COUNT(*) FROM integration_credential WHERE connector_id = ?", (cid,)
    ).fetchone()[0]
    rows = conn.execute(
        "SELECT id, connector_id, credential_type, credential_key, expires_at, company_id, created_at "
        "FROM integration_credential WHERE connector_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (cid, args.limit, args.offset),
    ).fetchall()

    # Mask credential values in list output
    ok({"credentials": [row_to_dict(r) for r in rows], "total_count": total})


# ===========================================================================
# 10. delete-connector-credential
# ===========================================================================
def delete_connector_credential(conn, args):
    cred_id = getattr(args, "credential_id", None)
    if not cred_id:
        err("--credential-id is required")
    row = conn.execute("SELECT * FROM integration_credential WHERE id = ?", (cred_id,)).fetchone()
    if not row:
        err(f"Credential {cred_id} not found")

    conn.execute("DELETE FROM integration_credential WHERE id = ?", (cred_id,))
    audit(conn, SKILL, "integration-delete-connector-credential", "integration_credential", cred_id)
    conn.commit()
    ok({"id": cred_id, "deleted": True})


# ===========================================================================
# 11. add-webhook
# ===========================================================================
def add_webhook(conn, args):
    cid = getattr(args, "connector_id", None)
    row = _get_connector(conn, cid)
    company_id = row_to_dict(row)["company_id"]

    event_type = getattr(args, "event_type", None)
    if not event_type:
        err("--event-type is required")
    webhook_url = getattr(args, "webhook_url", None)
    if not webhook_url:
        err("--webhook-url is required")

    wh_id = str(uuid.uuid4())
    conn.execute("""
        INSERT INTO integration_webhook (
            id, connector_id, event_type, webhook_url, webhook_secret,
            is_active, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?)
    """, (
        wh_id, cid, event_type, webhook_url,
        getattr(args, "webhook_secret", None),
        1, company_id, _now_iso(),
    ))
    audit(conn, SKILL, "integration-add-webhook", "integration_webhook", wh_id)
    conn.commit()
    ok({"id": wh_id, "connector_id": cid, "event_type": event_type,
        "webhook_url": webhook_url})


# ===========================================================================
# 12. list-webhooks
# ===========================================================================
def list_webhooks(conn, args):
    cid = getattr(args, "connector_id", None)
    if not cid:
        err("--connector-id is required")

    total = conn.execute(
        "SELECT COUNT(*) FROM integration_webhook WHERE connector_id = ?", (cid,)
    ).fetchone()[0]
    rows = conn.execute(
        "SELECT * FROM integration_webhook WHERE connector_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (cid, args.limit, args.offset),
    ).fetchall()

    ok({"webhooks": [row_to_dict(r) for r in rows], "total_count": total})


# ===========================================================================
# 13. delete-webhook
# ===========================================================================
def delete_webhook(conn, args):
    wh_id = getattr(args, "webhook_id", None)
    if not wh_id:
        err("--webhook-id is required")
    row = conn.execute("SELECT * FROM integration_webhook WHERE id = ?", (wh_id,)).fetchone()
    if not row:
        err(f"Webhook {wh_id} not found")

    conn.execute("DELETE FROM integration_webhook WHERE id = ?", (wh_id,))
    audit(conn, SKILL, "integration-delete-webhook", "integration_webhook", wh_id)
    conn.commit()
    ok({"id": wh_id, "deleted": True})


# ===========================================================================
# 14. connector-health-report
# ===========================================================================
def connector_health_report(conn, args):
    company_id = getattr(args, "company_id", None)
    where = ""
    params = []
    if company_id:
        where = " WHERE company_id = ?"
        params = [company_id]

    connectors = conn.execute(
        f"SELECT * FROM integration_connector{where} ORDER BY created_at DESC",
        params,
    ).fetchall()

    report = []
    for c in connectors:
        cd = row_to_dict(c)
        cid = cd["id"]
        cred_count = conn.execute(
            "SELECT COUNT(*) FROM integration_credential WHERE connector_id = ?", (cid,)
        ).fetchone()[0]
        last_sync = conn.execute(
            "SELECT * FROM integration_sync WHERE connector_id = ? ORDER BY created_at DESC LIMIT 1",
            (cid,),
        ).fetchone()
        failed_syncs = conn.execute(
            "SELECT COUNT(*) FROM integration_sync WHERE connector_id = ? AND sync_status = 'failed'",
            (cid,),
        ).fetchone()[0]

        report.append({
            "connector_id": cid,
            "name": cd["name"],
            "platform": cd["platform"],
            "connector_status": cd["connector_status"],
            "credential_count": cred_count,
            "has_credentials": cred_count > 0,
            "last_sync_status": row_to_dict(last_sync).get("sync_status") if last_sync else None,
            "failed_sync_count": failed_syncs,
        })

    ok({"connectors": report, "total_count": len(report)})


# ===========================================================================
# Action registry
# ===========================================================================
ACTIONS = {
    "integration-add-connector": add_connector,
    "integration-update-connector": update_connector,
    "integration-get-connector": get_connector,
    "integration-list-connectors": list_connectors,
    "integration-activate-connector": activate_connector,
    "integration-deactivate-connector": deactivate_connector,
    "integration-test-connector": check_connector,
    "integration-add-connector-credential": add_connector_credential,
    "integration-list-connector-credentials": list_connector_credentials,
    "integration-delete-connector-credential": delete_connector_credential,
    "integration-add-webhook": add_webhook,
    "integration-list-webhooks": list_webhooks,
    "integration-delete-webhook": delete_webhook,
    "integration-connector-health-report": connector_health_report,
}
