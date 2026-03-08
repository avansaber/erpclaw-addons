"""ERPClaw Alerts -- alerts domain module

Actions for alert rules, notification channels, and alert logs (3 tables, 14 actions).
Imported by db_query.py (unified router).
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection, DEFAULT_DB_PATH
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit

    ENTITY_PREFIXES.setdefault("alert_rule", "ALRT-")
    ENTITY_PREFIXES.setdefault("notification_channel", "NCHP-")
except ImportError:
    DEFAULT_DB_PATH = "~/.openclaw/erpclaw/data.sqlite"
    pass

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------------------------------------------------------------------
# Validation constants
# ---------------------------------------------------------------------------
VALID_SEVERITIES = ("low", "medium", "high", "critical")
VALID_CHANNEL_TYPES = ("email", "webhook", "telegram", "sms")
VALID_ALERT_STATUSES = ("triggered", "acknowledged", "resolved", "expired")


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


def _validate_json(value, field_name):
    """Validate that a string is valid JSON. Returns parsed object or calls err()."""
    if not value:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        err(f"Invalid JSON for {field_name}: {value}")


def _parse_json_field(value):
    """Safely parse a JSON string field, returning the parsed value or the original."""
    if value is None:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def _get_rule(conn, rule_id):
    """Fetch an alert rule by ID, or err() if not found."""
    if not rule_id:
        err("--rule-id is required")
    row = conn.execute("SELECT * FROM alert_rule WHERE id = ?", (rule_id,)).fetchone()
    if not row:
        err(f"Alert rule {rule_id} not found")
    return row


# ===========================================================================
# 1. add-alert-rule
# ===========================================================================
def add_alert_rule(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    entity_type = getattr(args, "entity_type", None)
    if not entity_type:
        err("--entity-type is required")

    condition_json = getattr(args, "condition_json", None) or "{}"
    _validate_json(condition_json, "condition-json")

    severity = getattr(args, "severity", None) or "medium"
    _validate_enum(severity, VALID_SEVERITIES, "severity")

    channel_ids = getattr(args, "channel_ids", None)
    if channel_ids:
        _validate_json(channel_ids, "channel-ids")

    cooldown_minutes = getattr(args, "cooldown_minutes", None)
    if cooldown_minutes is None:
        cooldown_minutes = 60

    is_active_raw = getattr(args, "is_active", None)
    is_active = int(is_active_raw) if is_active_raw is not None else 1

    rule_id = str(uuid.uuid4())
    naming = get_next_name(conn, "alert_rule", company_id=company_id)
    now = _now_iso()

    conn.execute("""
        INSERT INTO alert_rule (
            id, naming_series, name, description, entity_type, condition_json,
            severity, channel_ids, cooldown_minutes, is_active, trigger_count,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,0,?,?,?)
    """, (
        rule_id, naming, name,
        getattr(args, "description", None),
        entity_type, condition_json, severity, channel_ids,
        cooldown_minutes, is_active,
        company_id, now, now,
    ))
    audit(conn, "alert_rule", rule_id, "alert-add-alert-rule", company_id)
    conn.commit()

    ok({
        "id": rule_id,
        "naming_series": naming,
        "name": name,
        "entity_type": entity_type,
        "severity": severity,
        "is_active": is_active,
    })


# ===========================================================================
# 2. update-alert-rule
# ===========================================================================
def update_alert_rule(conn, args):
    rule_id = getattr(args, "rule_id", None)
    row = _get_rule(conn, rule_id)

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "name": "name",
        "description": "description",
        "entity_type": "entity_type",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    condition_json = getattr(args, "condition_json", None)
    if condition_json is not None:
        _validate_json(condition_json, "condition-json")
        updates.append("condition_json = ?")
        params.append(condition_json)
        changed.append("condition_json")

    severity = getattr(args, "severity", None)
    if severity is not None:
        _validate_enum(severity, VALID_SEVERITIES, "severity")
        updates.append("severity = ?")
        params.append(severity)
        changed.append("severity")

    channel_ids = getattr(args, "channel_ids", None)
    if channel_ids is not None:
        _validate_json(channel_ids, "channel-ids")
        updates.append("channel_ids = ?")
        params.append(channel_ids)
        changed.append("channel_ids")

    cooldown_minutes = getattr(args, "cooldown_minutes", None)
    if cooldown_minutes is not None:
        updates.append("cooldown_minutes = ?")
        params.append(cooldown_minutes)
        changed.append("cooldown_minutes")

    if not updates:
        err("No fields to update")

    now = _now_iso()
    updates.append("updated_at = ?")
    params.append(now)
    params.append(rule_id)

    conn.execute(
        f"UPDATE alert_rule SET {', '.join(updates)} WHERE id = ?",
        params,
    )
    company_id = row["company_id"] if hasattr(row, "keys") else row[12]
    audit(conn, "alert_rule", rule_id, "alert-update-alert-rule", company_id)
    conn.commit()

    ok({"id": rule_id, "updated_fields": changed})


# ===========================================================================
# 3. get-alert-rule
# ===========================================================================
def get_alert_rule(conn, args):
    rule_id = getattr(args, "rule_id", None)
    row = _get_rule(conn, rule_id)
    d = row_to_dict(row)

    # Parse JSON fields
    d["condition_json"] = _parse_json_field(d.get("condition_json"))
    d["channel_ids"] = _parse_json_field(d.get("channel_ids"))

    # Count associated logs
    log_count = conn.execute(
        "SELECT COUNT(*) FROM alert_log WHERE rule_id = ?", (rule_id,)
    ).fetchone()[0]
    d["log_count"] = log_count

    ok(d)


# ===========================================================================
# 4. list-alert-rules
# ===========================================================================
def list_alert_rules(conn, args):
    where, params = ["1=1"], []

    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "entity_type", None):
        where.append("entity_type = ?")
        params.append(args.entity_type)
    if getattr(args, "severity", None):
        where.append("severity = ?")
        params.append(args.severity)

    is_active_raw = getattr(args, "is_active", None)
    if is_active_raw is not None:
        where.append("is_active = ?")
        params.append(int(is_active_raw))

    if getattr(args, "search", None):
        where.append("(name LIKE ? OR description LIKE ?)")
        params.extend([f"%{args.search}%", f"%{args.search}%"])

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM alert_rule WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"SELECT * FROM alert_rule WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [args.limit, args.offset],
    ).fetchall()

    results = []
    for row in rows:
        d = row_to_dict(row)
        d["condition_json"] = _parse_json_field(d.get("condition_json"))
        d["channel_ids"] = _parse_json_field(d.get("channel_ids"))
        results.append(d)

    ok({"rules": results, "total_count": total})


# ===========================================================================
# 5. activate-alert-rule
# ===========================================================================
def activate_alert_rule(conn, args):
    rule_id = getattr(args, "rule_id", None)
    row = _get_rule(conn, rule_id)

    now = _now_iso()
    conn.execute(
        "UPDATE alert_rule SET is_active = 1, updated_at = ? WHERE id = ?",
        (now, rule_id),
    )
    company_id = row["company_id"] if hasattr(row, "keys") else row[12]
    audit(conn, "alert_rule", rule_id, "alert-activate-alert-rule", company_id)
    conn.commit()

    ok({"id": rule_id, "is_active": 1})


# ===========================================================================
# 6. deactivate-alert-rule
# ===========================================================================
def deactivate_alert_rule(conn, args):
    rule_id = getattr(args, "rule_id", None)
    row = _get_rule(conn, rule_id)

    now = _now_iso()
    conn.execute(
        "UPDATE alert_rule SET is_active = 0, updated_at = ? WHERE id = ?",
        (now, rule_id),
    )
    company_id = row["company_id"] if hasattr(row, "keys") else row[12]
    audit(conn, "alert_rule", rule_id, "alert-deactivate-alert-rule", company_id)
    conn.commit()

    ok({"id": rule_id, "is_active": 0})


# ===========================================================================
# 7. add-notification-channel
# ===========================================================================
def add_notification_channel(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    channel_type = getattr(args, "channel_type", None)
    if not channel_type:
        err("--channel-type is required")
    _validate_enum(channel_type, VALID_CHANNEL_TYPES, "channel-type")

    config_json = getattr(args, "config_json", None) or "{}"
    _validate_json(config_json, "config-json")

    is_active_raw = getattr(args, "is_active", None)
    is_active = int(is_active_raw) if is_active_raw is not None else 1

    ch_id = str(uuid.uuid4())
    naming = get_next_name(conn, "notification_channel", company_id=company_id)
    now = _now_iso()

    conn.execute("""
        INSERT INTO notification_channel (
            id, naming_series, name, channel_type, config_json,
            is_active, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        ch_id, naming, name, channel_type, config_json,
        is_active, company_id, now, now,
    ))
    audit(conn, "notification_channel", ch_id, "alert-add-notification-channel", company_id)
    conn.commit()

    ok({
        "id": ch_id,
        "naming_series": naming,
        "name": name,
        "channel_type": channel_type,
        "is_active": is_active,
    })


# ===========================================================================
# 8. list-notification-channels
# ===========================================================================
def list_notification_channels(conn, args):
    where, params = ["1=1"], []

    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "channel_type", None):
        where.append("channel_type = ?")
        params.append(args.channel_type)

    is_active_raw = getattr(args, "is_active", None)
    if is_active_raw is not None:
        where.append("is_active = ?")
        params.append(int(is_active_raw))

    if getattr(args, "search", None):
        where.append("name LIKE ?")
        params.append(f"%{args.search}%")

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM notification_channel WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"SELECT * FROM notification_channel WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [args.limit, args.offset],
    ).fetchall()

    results = []
    for row in rows:
        d = row_to_dict(row)
        d["config_json"] = _parse_json_field(d.get("config_json"))
        results.append(d)

    ok({"channels": results, "total_count": total})


# ===========================================================================
# 9. delete-notification-channel
# ===========================================================================
def delete_notification_channel(conn, args):
    channel_id = getattr(args, "channel_id", None)
    if not channel_id:
        err("--channel-id is required")

    row = conn.execute(
        "SELECT * FROM notification_channel WHERE id = ?", (channel_id,)
    ).fetchone()
    if not row:
        err(f"Notification channel {channel_id} not found")

    company_id = row["company_id"] if hasattr(row, "keys") else row[6]

    conn.execute("DELETE FROM notification_channel WHERE id = ?", (channel_id,))
    audit(conn, "notification_channel", channel_id, "alert-delete-notification-channel", company_id)
    conn.commit()

    ok({"id": channel_id, "deleted": True})


# ===========================================================================
# 10. trigger-alert
# ===========================================================================
def trigger_alert(conn, args):
    rule_id = getattr(args, "rule_id", None)
    row = _get_rule(conn, rule_id)

    message = getattr(args, "message", None)
    if not message:
        err("--message is required")

    # Extract rule details
    if hasattr(row, "keys"):
        rule_name = row["name"]
        entity_type = row["entity_type"]
        severity = row["severity"]
        company_id = row["company_id"]
    else:
        rule_name = row[2]
        entity_type = row[4]
        severity = row[6]
        company_id = row[12]

    entity_id = getattr(args, "entity_id", None)
    channel_results = getattr(args, "channel_results", None)
    if channel_results:
        _validate_json(channel_results, "channel-results")

    log_id = str(uuid.uuid4())
    now = _now_iso()

    # Insert alert log entry
    conn.execute("""
        INSERT INTO alert_log (
            id, rule_id, rule_name, entity_type, entity_id, severity,
            message, alert_status, channel_results, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        log_id, rule_id, rule_name, entity_type, entity_id, severity,
        message, "triggered", channel_results, company_id, now,
    ))

    # Update rule: last_triggered_at and trigger_count
    conn.execute("""
        UPDATE alert_rule SET
            last_triggered_at = ?,
            trigger_count = trigger_count + 1,
            updated_at = ?
        WHERE id = ?
    """, (now, now, rule_id))

    audit(conn, "alert_log", log_id, "alert-trigger-alert", company_id)
    conn.commit()

    ok({
        "id": log_id,
        "rule_id": rule_id,
        "rule_name": rule_name,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "severity": severity,
        "alert_status": "triggered",
        "message": message,
    })


# ===========================================================================
# 11. list-alert-logs
# ===========================================================================
def list_alert_logs(conn, args):
    where, params = ["1=1"], []

    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "rule_id", None):
        where.append("rule_id = ?")
        params.append(args.rule_id)
    if getattr(args, "severity", None):
        where.append("severity = ?")
        params.append(args.severity)
    if getattr(args, "alert_status", None):
        where.append("alert_status = ?")
        params.append(args.alert_status)
    if getattr(args, "entity_type", None):
        where.append("entity_type = ?")
        params.append(args.entity_type)
    if getattr(args, "search", None):
        where.append("(message LIKE ? OR rule_name LIKE ?)")
        params.extend([f"%{args.search}%", f"%{args.search}%"])

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM alert_log WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"SELECT * FROM alert_log WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [args.limit, args.offset],
    ).fetchall()

    results = []
    for row in rows:
        d = row_to_dict(row)
        d["channel_results"] = _parse_json_field(d.get("channel_results"))
        results.append(d)

    ok({"logs": results, "total_count": total})


# ===========================================================================
# 12. acknowledge-alert
# ===========================================================================
def acknowledge_alert(conn, args):
    alert_log_id = getattr(args, "alert_log_id", None)
    if not alert_log_id:
        err("--alert-log-id is required")

    row = conn.execute(
        "SELECT * FROM alert_log WHERE id = ?", (alert_log_id,)
    ).fetchone()
    if not row:
        err(f"Alert log {alert_log_id} not found")

    acknowledged_by = getattr(args, "acknowledged_by", None)
    if not acknowledged_by:
        err("--acknowledged-by is required")

    current_status = row["alert_status"] if hasattr(row, "keys") else row[7]
    if current_status == "acknowledged":
        err(f"Alert {alert_log_id} is already acknowledged")

    company_id = row["company_id"] if hasattr(row, "keys") else row[12]
    now = _now_iso()

    conn.execute("""
        UPDATE alert_log SET
            alert_status = 'acknowledged',
            acknowledged_by = ?,
            acknowledged_at = ?
        WHERE id = ?
    """, (acknowledged_by, now, alert_log_id))

    audit(conn, "alert_log", alert_log_id, "alert-acknowledge-alert", company_id)
    conn.commit()

    ok({
        "id": alert_log_id,
        "alert_status": "acknowledged",
        "acknowledged_by": acknowledged_by,
        "acknowledged_at": now,
    })


# ===========================================================================
# 13. alert-summary-report
# ===========================================================================
def alert_summary_report(conn, args):
    where, params = ["1=1"], []

    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "start_date", None):
        where.append("created_at >= ?")
        params.append(args.start_date)
    if getattr(args, "end_date", None):
        where.append("created_at <= ?")
        params.append(args.end_date)

    where_sql = " AND ".join(where)

    # Total alerts
    total = conn.execute(
        f"SELECT COUNT(*) FROM alert_log WHERE {where_sql}", params
    ).fetchone()[0]

    # By severity
    severity_rows = conn.execute(f"""
        SELECT severity, COUNT(*) AS cnt
        FROM alert_log WHERE {where_sql}
        GROUP BY severity ORDER BY cnt DESC
    """, params).fetchall()
    by_severity = {}
    for r in severity_rows:
        by_severity[r[0] or "unknown"] = r[1]

    # By status
    status_rows = conn.execute(f"""
        SELECT alert_status, COUNT(*) AS cnt
        FROM alert_log WHERE {where_sql}
        GROUP BY alert_status ORDER BY cnt DESC
    """, params).fetchall()
    by_status = {}
    for r in status_rows:
        by_status[r[0] or "unknown"] = r[1]

    # By entity_type
    entity_rows = conn.execute(f"""
        SELECT entity_type, COUNT(*) AS cnt
        FROM alert_log WHERE {where_sql}
        GROUP BY entity_type ORDER BY cnt DESC
    """, params).fetchall()
    by_entity_type = {}
    for r in entity_rows:
        by_entity_type[r[0] or "unknown"] = r[1]

    # Top rules
    top_rules = conn.execute(f"""
        SELECT rule_name, rule_id, COUNT(*) AS cnt
        FROM alert_log WHERE {where_sql}
        GROUP BY rule_id, rule_name ORDER BY cnt DESC
        LIMIT 10
    """, params).fetchall()
    top_rules_list = []
    for r in top_rules:
        top_rules_list.append({
            "rule_name": r[0],
            "rule_id": r[1],
            "alert_count": r[2],
        })

    ok({
        "report": "alert-summary",
        "total_alerts": total,
        "by_severity": by_severity,
        "by_status": by_status,
        "by_entity_type": by_entity_type,
        "top_rules": top_rules_list,
    })


# ===========================================================================
# 14. status
# ===========================================================================
def status_action(conn, args):
    table_counts = {}
    for table in ["alert_rule", "alert_log", "notification_channel"]:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            table_counts[table] = count
        except Exception:
            table_counts[table] = "missing"

    ok({
        "skill": "erpclaw-alerts",
        "version": "1.0.0",
        "tables": table_counts,
        "total_tables": len(table_counts),
        "database": DEFAULT_DB_PATH,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "alert-add-alert-rule": add_alert_rule,
    "alert-update-alert-rule": update_alert_rule,
    "alert-get-alert-rule": get_alert_rule,
    "alert-list-alert-rules": list_alert_rules,
    "alert-activate-alert-rule": activate_alert_rule,
    "alert-deactivate-alert-rule": deactivate_alert_rule,
    "alert-add-notification-channel": add_notification_channel,
    "alert-list-notification-channels": list_notification_channels,
    "alert-delete-notification-channel": delete_notification_channel,
    "alert-trigger-alert": trigger_alert,
    "alert-list-alert-logs": list_alert_logs,
    "alert-acknowledge-alert": acknowledge_alert,
    "alert-summary-report": alert_summary_report,
    "status": status_action,
}
