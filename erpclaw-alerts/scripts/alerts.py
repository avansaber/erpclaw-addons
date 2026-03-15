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
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row

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
    t = Table("company")
    q = Q.from_(t).select(t.id).where(t.id == P())
    if not conn.execute(q.get_sql(), (company_id,)).fetchone():
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
    t = Table("alert_rule")
    q = Q.from_(t).select(t.star).where(t.id == P())
    row = conn.execute(q.get_sql(), (rule_id,)).fetchone()
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

    sql, _ = insert_row("alert_rule", {
        "id": P(), "naming_series": P(), "name": P(), "description": P(),
        "entity_type": P(), "condition_json": P(),
        "severity": P(), "channel_ids": P(), "cooldown_minutes": P(),
        "is_active": P(), "trigger_count": P(),
        "company_id": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        rule_id, naming, name,
        getattr(args, "description", None),
        entity_type, condition_json, severity, channel_ids,
        cooldown_minutes, is_active, 0,
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
    t = Table("alert_log")
    q = Q.from_(t).select(fn.Count("*")).where(t.rule_id == P())
    log_count = conn.execute(q.get_sql(), (rule_id,)).fetchone()[0]
    d["log_count"] = log_count

    ok(d)


# ===========================================================================
# 4. list-alert-rules
# ===========================================================================
def list_alert_rules(conn, args):
    t = Table("alert_rule")
    q_count = Q.from_(t).select(fn.Count("*"))
    q_rows = Q.from_(t).select(t.star)
    params = []

    if getattr(args, "company_id", None):
        q_count = q_count.where(t.company_id == P())
        q_rows = q_rows.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "entity_type", None):
        q_count = q_count.where(t.entity_type == P())
        q_rows = q_rows.where(t.entity_type == P())
        params.append(args.entity_type)
    if getattr(args, "severity", None):
        q_count = q_count.where(t.severity == P())
        q_rows = q_rows.where(t.severity == P())
        params.append(args.severity)

    is_active_raw = getattr(args, "is_active", None)
    if is_active_raw is not None:
        q_count = q_count.where(t.is_active == P())
        q_rows = q_rows.where(t.is_active == P())
        params.append(int(is_active_raw))

    if getattr(args, "search", None):
        s = f"%{args.search}%"
        search_crit = (t.name.like(P())) | (t.description.like(P()))
        q_count = q_count.where(search_crit)
        q_rows = q_rows.where(search_crit)
        params.extend([s, s])

    total = conn.execute(q_count.get_sql(), params).fetchone()[0]

    q_rows = q_rows.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q_rows.get_sql(), params + [args.limit, args.offset]).fetchall()

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
    sql = update_row("alert_rule",
                     data={"is_active": P(), "updated_at": P()},
                     where={"id": P()})
    conn.execute(sql, (1, now, rule_id))
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
    sql = update_row("alert_rule",
                     data={"is_active": P(), "updated_at": P()},
                     where={"id": P()})
    conn.execute(sql, (0, now, rule_id))
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

    sql, _ = insert_row("notification_channel", {
        "id": P(), "naming_series": P(), "name": P(), "channel_type": P(),
        "config_json": P(), "is_active": P(), "company_id": P(),
        "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
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
    t = Table("notification_channel")
    q_count = Q.from_(t).select(fn.Count("*"))
    q_rows = Q.from_(t).select(t.star)
    params = []

    if getattr(args, "company_id", None):
        q_count = q_count.where(t.company_id == P())
        q_rows = q_rows.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "channel_type", None):
        q_count = q_count.where(t.channel_type == P())
        q_rows = q_rows.where(t.channel_type == P())
        params.append(args.channel_type)

    is_active_raw = getattr(args, "is_active", None)
    if is_active_raw is not None:
        q_count = q_count.where(t.is_active == P())
        q_rows = q_rows.where(t.is_active == P())
        params.append(int(is_active_raw))

    if getattr(args, "search", None):
        q_count = q_count.where(t.name.like(P()))
        q_rows = q_rows.where(t.name.like(P()))
        params.append(f"%{args.search}%")

    total = conn.execute(q_count.get_sql(), params).fetchone()[0]

    q_rows = q_rows.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q_rows.get_sql(), params + [args.limit, args.offset]).fetchall()

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

    t = Table("notification_channel")
    q = Q.from_(t).select(t.star).where(t.id == P())
    row = conn.execute(q.get_sql(), (channel_id,)).fetchone()
    if not row:
        err(f"Notification channel {channel_id} not found")

    company_id = row["company_id"] if hasattr(row, "keys") else row[6]

    q_del = Q.from_(t).delete().where(t.id == P())
    conn.execute(q_del.get_sql(), (channel_id,))
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
    sql, _ = insert_row("alert_log", {
        "id": P(), "rule_id": P(), "rule_name": P(), "entity_type": P(),
        "entity_id": P(), "severity": P(),
        "message": P(), "alert_status": P(), "channel_results": P(),
        "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        log_id, rule_id, rule_name, entity_type, entity_id, severity,
        message, "triggered", channel_results, company_id, now,
    ))

    # Update rule: last_triggered_at and trigger_count
    conn.execute(
        "UPDATE \"alert_rule\" SET \"last_triggered_at\"=?,\"trigger_count\"=\"trigger_count\"+1,\"updated_at\"=? WHERE \"id\"=?",
        (now, now, rule_id),
    )

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
    t = Table("alert_log")
    q_count = Q.from_(t).select(fn.Count("*"))
    q_rows = Q.from_(t).select(t.star)
    params = []

    if getattr(args, "company_id", None):
        q_count = q_count.where(t.company_id == P())
        q_rows = q_rows.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "rule_id", None):
        q_count = q_count.where(t.rule_id == P())
        q_rows = q_rows.where(t.rule_id == P())
        params.append(args.rule_id)
    if getattr(args, "severity", None):
        q_count = q_count.where(t.severity == P())
        q_rows = q_rows.where(t.severity == P())
        params.append(args.severity)
    if getattr(args, "alert_status", None):
        q_count = q_count.where(t.alert_status == P())
        q_rows = q_rows.where(t.alert_status == P())
        params.append(args.alert_status)
    if getattr(args, "entity_type", None):
        q_count = q_count.where(t.entity_type == P())
        q_rows = q_rows.where(t.entity_type == P())
        params.append(args.entity_type)
    if getattr(args, "search", None):
        s = f"%{args.search}%"
        search_crit = (t.message.like(P())) | (t.rule_name.like(P()))
        q_count = q_count.where(search_crit)
        q_rows = q_rows.where(search_crit)
        params.extend([s, s])

    total = conn.execute(q_count.get_sql(), params).fetchone()[0]

    q_rows = q_rows.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q_rows.get_sql(), params + [args.limit, args.offset]).fetchall()

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

    t = Table("alert_log")
    q = Q.from_(t).select(t.star).where(t.id == P())
    row = conn.execute(q.get_sql(), (alert_log_id,)).fetchone()
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

    sql = update_row("alert_log",
                     data={"alert_status": P(), "acknowledged_by": P(), "acknowledged_at": P()},
                     where={"id": P()})
    conn.execute(sql, ("acknowledged", acknowledged_by, now, alert_log_id))

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
    t = Table("alert_log")
    params = []
    base_crit = None

    if getattr(args, "company_id", None):
        crit = t.company_id == P()
        base_crit = crit if base_crit is None else base_crit & crit
        params.append(args.company_id)
    if getattr(args, "start_date", None):
        crit = t.created_at >= P()
        base_crit = crit if base_crit is None else base_crit & crit
        params.append(args.start_date)
    if getattr(args, "end_date", None):
        crit = t.created_at <= P()
        base_crit = crit if base_crit is None else base_crit & crit
        params.append(args.end_date)

    # Total alerts
    q_total = Q.from_(t).select(fn.Count("*"))
    if base_crit is not None:
        q_total = q_total.where(base_crit)
    total = conn.execute(q_total.get_sql(), params).fetchone()[0]

    # By severity
    q_sev = Q.from_(t).select(t.severity, fn.Count("*").as_("cnt")).groupby(t.severity).orderby(Field("cnt"), order=Order.desc)
    if base_crit is not None:
        q_sev = q_sev.where(base_crit)
    severity_rows = conn.execute(q_sev.get_sql(), params).fetchall()
    by_severity = {}
    for r in severity_rows:
        by_severity[r[0] or "unknown"] = r[1]

    # By status
    q_st = Q.from_(t).select(t.alert_status, fn.Count("*").as_("cnt")).groupby(t.alert_status).orderby(Field("cnt"), order=Order.desc)
    if base_crit is not None:
        q_st = q_st.where(base_crit)
    status_rows = conn.execute(q_st.get_sql(), params).fetchall()
    by_status = {}
    for r in status_rows:
        by_status[r[0] or "unknown"] = r[1]

    # By entity_type
    q_et = Q.from_(t).select(t.entity_type, fn.Count("*").as_("cnt")).groupby(t.entity_type).orderby(Field("cnt"), order=Order.desc)
    if base_crit is not None:
        q_et = q_et.where(base_crit)
    entity_rows = conn.execute(q_et.get_sql(), params).fetchall()
    by_entity_type = {}
    for r in entity_rows:
        by_entity_type[r[0] or "unknown"] = r[1]

    # Top rules
    q_top = (Q.from_(t)
             .select(t.rule_name, t.rule_id, fn.Count("*").as_("cnt"))
             .groupby(t.rule_id, t.rule_name)
             .orderby(Field("cnt"), order=Order.desc)
             .limit(10))
    if base_crit is not None:
        q_top = q_top.where(base_crit)
    top_rules = conn.execute(q_top.get_sql(), params).fetchall()
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
            t = Table(table)
            q = Q.from_(t).select(fn.Count("*"))
            count = conn.execute(q.get_sql()).fetchone()[0]
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
