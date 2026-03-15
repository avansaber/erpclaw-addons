"""ERPClaw Integrations -- sync domain module

Actions for managing sync operations, schedules, errors, and logs.
14 actions.
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
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row

    ENTITY_PREFIXES.setdefault("integration_sync", "SYNC-")
except ImportError:
    pass

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------------------------------------------------------------------
# Validation constants
# ---------------------------------------------------------------------------
VALID_SYNC_TYPES = ("full", "incremental", "manual")
VALID_DIRECTIONS = ("inbound", "outbound", "bidirectional")
VALID_SYNC_STATUSES = ("pending", "running", "completed", "failed", "cancelled")
VALID_FREQUENCIES = ("hourly", "daily", "weekly", "monthly", "manual")

SKILL = "erpclaw-integrations"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_connector(conn, connector_id):
    if not connector_id:
        err("--connector-id is required")
    row = conn.execute(Q.from_(Table("integration_connector")).select(Table("integration_connector").star).where(Field("id") == P()).get_sql(), (connector_id,)).fetchone()
    if not row:
        err(f"Connector {connector_id} not found")
    return row


def _validate_enum(value, valid_values, field_name):
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")


# ===========================================================================
# 1. start-sync
# ===========================================================================
def start_sync(conn, args):
    cid = getattr(args, "connector_id", None)
    connector = _get_connector(conn, cid)
    company_id = row_to_dict(connector)["company_id"]

    sync_type = getattr(args, "sync_type", None)
    if not sync_type:
        err("--sync-type is required")
    _validate_enum(sync_type, VALID_SYNC_TYPES, "sync-type")

    direction = getattr(args, "direction", None)
    if not direction:
        err("--direction is required")
    _validate_enum(direction, VALID_DIRECTIONS, "direction")

    entity_type = getattr(args, "entity_type", None)

    sid = str(uuid.uuid4())
    naming = get_next_name(conn, "integration_sync", company_id=company_id)
    now = _now_iso()

    sql, _ = insert_row("integration_sync", {"id": P(), "naming_series": P(), "connector_id": P(), "sync_type": P(), "direction": P(), "entity_type": P(), "sync_status": P(), "records_processed": P(), "records_failed": P(), "started_at": P(), "company_id": P(), "created_at": P()})
    conn.execute(sql, (
        sid, naming, cid, sync_type, direction, entity_type,
        "running", 0, 0, now, company_id, now,
    ))

    # Update connector last_sync_at
    conn.execute(
        "UPDATE integration_connector SET last_sync_at = ?, updated_at = ? WHERE id = ?",
        (now, now, cid),
    )

    audit(conn, SKILL, "integration-start-sync", "integration_sync", sid,
          new_values={"sync_type": sync_type, "direction": direction})
    conn.commit()
    ok({"id": sid, "naming_series": naming, "connector_id": cid,
        "sync_type": sync_type, "direction": direction, "sync_status": "running"})


# ===========================================================================
# 2. get-sync
# ===========================================================================
def get_sync(conn, args):
    sid = getattr(args, "sync_id", None)
    if not sid:
        err("--sync-id is required")
    row = conn.execute(Q.from_(Table("integration_sync")).select(Table("integration_sync").star).where(Field("id") == P()).get_sql(), (sid,)).fetchone()
    if not row:
        err(f"Sync {sid} not found")

    data = row_to_dict(row)
    # Include error count
    error_count = conn.execute(Q.from_(Table("integration_sync_error")).select(fn.Count("*")).where(Field("sync_id") == P()).get_sql(), (sid,)).fetchone()[0]
    data["error_count"] = error_count
    ok(data)


# ===========================================================================
# 3. list-syncs
# ===========================================================================
def list_syncs(conn, args):
    where, params = [], []
    cid = getattr(args, "connector_id", None)
    if cid:
        where.append("connector_id = ?")
        params.append(cid)
    sync_status = getattr(args, "sync_status", None)
    if sync_status:
        where.append("sync_status = ?")
        params.append(sync_status)
    company_id = getattr(args, "company_id", None)
    if company_id:
        where.append("company_id = ?")
        params.append(company_id)

    clause = (" WHERE " + " AND ".join(where)) if where else ""
    total = conn.execute(f"SELECT COUNT(*) FROM integration_sync{clause}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM integration_sync{clause} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [args.limit, args.offset],
    ).fetchall()

    ok({"syncs": [row_to_dict(r) for r in rows], "total_count": total})


# ===========================================================================
# 4. cancel-sync
# ===========================================================================
def cancel_sync(conn, args):
    sid = getattr(args, "sync_id", None)
    if not sid:
        err("--sync-id is required")
    row = conn.execute(Q.from_(Table("integration_sync")).select(Table("integration_sync").star).where(Field("id") == P()).get_sql(), (sid,)).fetchone()
    if not row:
        err(f"Sync {sid} not found")

    data = row_to_dict(row)
    if data["sync_status"] in ("completed", "failed", "cancelled"):
        err(f"Cannot cancel sync with status '{data['sync_status']}'")

    now = _now_iso()
    conn.execute(
        "UPDATE integration_sync SET sync_status = 'cancelled', completed_at = ? WHERE id = ?",
        (now, sid),
    )
    audit(conn, SKILL, "integration-cancel-sync", "integration_sync", sid)
    conn.commit()
    ok({"id": sid, "sync_status": "cancelled"})


# ===========================================================================
# 5. add-sync-schedule
# ===========================================================================
def add_sync_schedule(conn, args):
    cid = getattr(args, "connector_id", None)
    connector = _get_connector(conn, cid)
    company_id = row_to_dict(connector)["company_id"]

    entity_type = getattr(args, "entity_type", None)
    if not entity_type:
        err("--entity-type is required")

    frequency = getattr(args, "frequency", None)
    if not frequency:
        err("--frequency is required")
    _validate_enum(frequency, VALID_FREQUENCIES, "frequency")

    sync_type = getattr(args, "sync_type", None) or "incremental"
    direction = getattr(args, "direction", None) or "bidirectional"

    sched_id = str(uuid.uuid4())
    now = _now_iso()

    sql, _ = insert_row("integration_sync_schedule", {"id": P(), "connector_id": P(), "entity_type": P(), "frequency": P(), "sync_type": P(), "direction": P(), "is_active": P(), "next_run_at": P(), "company_id": P(), "created_at": P()})
    conn.execute(sql, (
        sched_id, cid, entity_type, frequency, sync_type, direction,
        1, getattr(args, "next_run_at", None), company_id, now,
    ))
    audit(conn, SKILL, "integration-add-sync-schedule", "integration_sync_schedule", sched_id)
    conn.commit()
    ok({"id": sched_id, "connector_id": cid, "entity_type": entity_type,
        "frequency": frequency, "is_active": 1})


# ===========================================================================
# 6. update-sync-schedule
# ===========================================================================
def update_sync_schedule(conn, args):
    sched_id = getattr(args, "schedule_id", None)
    if not sched_id:
        err("--schedule-id is required")
    row = conn.execute(Q.from_(Table("integration_sync_schedule")).select(Table("integration_sync_schedule").star).where(Field("id") == P()).get_sql(), (sched_id,)).fetchone()
    if not row:
        err(f"Schedule {sched_id} not found")

    updates, params, changed = [], [], []

    for col, arg_name, validator in [
        ("entity_type", "entity_type", None),
        ("frequency", "frequency", VALID_FREQUENCIES),
        ("sync_type", "sync_type", VALID_SYNC_TYPES),
        ("direction", "direction", VALID_DIRECTIONS),
        ("next_run_at", "next_run_at", None),
    ]:
        val = getattr(args, arg_name, None)
        if val is not None:
            if validator:
                _validate_enum(val, validator, arg_name.replace("_", "-"))
            updates.append(f"{col} = ?")
            params.append(val)
            changed.append(col)

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        updates.append("is_active = ?")
        params.append(int(is_active))
        changed.append("is_active")

    if not updates:
        err("No fields to update. Provide at least one field flag.")

    params.append(sched_id)
    conn.execute(
        f"UPDATE integration_sync_schedule SET {', '.join(updates)} WHERE id = ?",
        params,
    )
    audit(conn, SKILL, "integration-update-sync-schedule", "integration_sync_schedule", sched_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": sched_id, "updated_fields": changed})


# ===========================================================================
# 7. list-sync-schedules
# ===========================================================================
def list_sync_schedules(conn, args):
    where, params = [], []
    cid = getattr(args, "connector_id", None)
    if cid:
        where.append("connector_id = ?")
        params.append(cid)
    company_id = getattr(args, "company_id", None)
    if company_id:
        where.append("company_id = ?")
        params.append(company_id)

    clause = (" WHERE " + " AND ".join(where)) if where else ""
    total = conn.execute(f"SELECT COUNT(*) FROM integration_sync_schedule{clause}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM integration_sync_schedule{clause} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [args.limit, args.offset],
    ).fetchall()

    ok({"schedules": [row_to_dict(r) for r in rows], "total_count": total})


# ===========================================================================
# 8. delete-sync-schedule
# ===========================================================================
def delete_sync_schedule(conn, args):
    sched_id = getattr(args, "schedule_id", None)
    if not sched_id:
        err("--schedule-id is required")
    row = conn.execute(Q.from_(Table("integration_sync_schedule")).select(Table("integration_sync_schedule").star).where(Field("id") == P()).get_sql(), (sched_id,)).fetchone()
    if not row:
        err(f"Schedule {sched_id} not found")

    conn.execute("DELETE FROM integration_sync_schedule WHERE id = ?", (sched_id,))
    audit(conn, SKILL, "integration-delete-sync-schedule", "integration_sync_schedule", sched_id)
    conn.commit()
    ok({"id": sched_id, "deleted": True})


# ===========================================================================
# 9. add-sync-error
# ===========================================================================
def add_sync_error(conn, args):
    sid = getattr(args, "sync_id", None)
    if not sid:
        err("--sync-id is required")
    row = conn.execute(Q.from_(Table("integration_sync")).select(Table("integration_sync").star).where(Field("id") == P()).get_sql(), (sid,)).fetchone()
    if not row:
        err(f"Sync {sid} not found")

    error_message = getattr(args, "error_message", None)
    if not error_message:
        err("--error-message is required")

    err_id = str(uuid.uuid4())
    now = _now_iso()

    sql, _ = insert_row("integration_sync_error", {"id": P(), "sync_id": P(), "entity_type": P(), "entity_id": P(), "error_message": P(), "is_resolved": P(), "created_at": P()})
    conn.execute(sql, (
        err_id, sid,
        getattr(args, "entity_type", None),
        getattr(args, "entity_id", None),
        error_message, 0, now,
    ))

    # Increment records_failed on the sync
    conn.execute(
        "UPDATE integration_sync SET records_failed = records_failed + 1 WHERE id = ?",
        (sid,),
    )

    audit(conn, SKILL, "integration-add-sync-error", "integration_sync_error", err_id)
    conn.commit()
    ok({"id": err_id, "sync_id": sid, "error_message": error_message})


# ===========================================================================
# 10. list-sync-errors
# ===========================================================================
def list_sync_errors(conn, args):
    where, params = [], []
    sid = getattr(args, "sync_id", None)
    if sid:
        where.append("sync_id = ?")
        params.append(sid)
    is_resolved = getattr(args, "is_resolved", None)
    if is_resolved is not None:
        where.append("is_resolved = ?")
        params.append(int(is_resolved))

    clause = (" WHERE " + " AND ".join(where)) if where else ""
    total = conn.execute(f"SELECT COUNT(*) FROM integration_sync_error{clause}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM integration_sync_error{clause} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [args.limit, args.offset],
    ).fetchall()

    ok({"errors": [row_to_dict(r) for r in rows], "total_count": total})


# ===========================================================================
# 11. resolve-sync-error
# ===========================================================================
def resolve_sync_error(conn, args):
    err_id = getattr(args, "error_id", None)
    if not err_id:
        err("--error-id is required")
    row = conn.execute(Q.from_(Table("integration_sync_error")).select(Table("integration_sync_error").star).where(Field("id") == P()).get_sql(), (err_id,)).fetchone()
    if not row:
        err(f"Sync error {err_id} not found")

    data = row_to_dict(row)
    if data["is_resolved"] == 1:
        err("Sync error is already resolved")

    now = _now_iso()
    resolution_notes = getattr(args, "resolution_notes", None) or ""
    conn.execute(
        "UPDATE integration_sync_error SET is_resolved = 1, resolution_notes = ?, resolved_at = ? WHERE id = ?",
        (resolution_notes, now, err_id),
    )
    audit(conn, SKILL, "integration-resolve-sync-error", "integration_sync_error", err_id)
    conn.commit()
    ok({"id": err_id, "is_resolved": 1, "resolved_at": now})


# ===========================================================================
# 12. retry-sync
# ===========================================================================
def retry_sync(conn, args):
    sid = getattr(args, "sync_id", None)
    if not sid:
        err("--sync-id is required")
    row = conn.execute(Q.from_(Table("integration_sync")).select(Table("integration_sync").star).where(Field("id") == P()).get_sql(), (sid,)).fetchone()
    if not row:
        err(f"Sync {sid} not found")

    data = row_to_dict(row)
    if data["sync_status"] not in ("failed", "cancelled"):
        err(f"Can only retry syncs with status 'failed' or 'cancelled', got '{data['sync_status']}'")

    # Create a new sync based on the original
    new_sid = str(uuid.uuid4())
    naming = get_next_name(conn, "integration_sync", company_id=data["company_id"])
    now = _now_iso()

    sql, _ = insert_row("integration_sync", {"id": P(), "naming_series": P(), "connector_id": P(), "sync_type": P(), "direction": P(), "entity_type": P(), "sync_status": P(), "records_processed": P(), "records_failed": P(), "started_at": P(), "company_id": P(), "created_at": P()})
    conn.execute(sql, (
        new_sid, naming, data["connector_id"], data["sync_type"], data["direction"],
        data["entity_type"], "running", 0, 0, now, data["company_id"], now,
    ))

    audit(conn, SKILL, "integration-retry-sync", "integration_sync", new_sid,
          new_values={"original_sync_id": sid})
    conn.commit()
    ok({"id": new_sid, "naming_series": naming, "original_sync_id": sid,
        "sync_status": "running"})


# ===========================================================================
# 13. sync-summary-report
# ===========================================================================
def sync_summary_report(conn, args):
    where, params = [], []
    company_id = getattr(args, "company_id", None)
    if company_id:
        where.append("s.company_id = ?")
        params.append(company_id)
    cid = getattr(args, "connector_id", None)
    if cid:
        where.append("s.connector_id = ?")
        params.append(cid)
    start_date = getattr(args, "start_date", None)
    if start_date:
        where.append("s.created_at >= ?")
        params.append(start_date)
    end_date = getattr(args, "end_date", None)
    if end_date:
        where.append("s.created_at <= ?")
        params.append(end_date)

    clause = (" WHERE " + " AND ".join(where)) if where else ""

    total_syncs = conn.execute(
        f"SELECT COUNT(*) FROM integration_sync s{clause}", params
    ).fetchone()[0]
    completed = conn.execute(
        f"SELECT COUNT(*) FROM integration_sync s{clause}{' AND ' if where else ' WHERE '}s.sync_status = 'completed'",
        params,
    ).fetchone()[0]
    failed = conn.execute(
        f"SELECT COUNT(*) FROM integration_sync s{clause}{' AND ' if where else ' WHERE '}s.sync_status = 'failed'",
        params,
    ).fetchone()[0]
    running = conn.execute(
        f"SELECT COUNT(*) FROM integration_sync s{clause}{' AND ' if where else ' WHERE '}s.sync_status = 'running'",
        params,
    ).fetchone()[0]

    total_records = conn.execute(
        f"SELECT COALESCE(SUM(s.records_processed), 0) FROM integration_sync s{clause}",
        params,
    ).fetchone()[0]
    total_errors = conn.execute(
        f"SELECT COALESCE(SUM(s.records_failed), 0) FROM integration_sync s{clause}",
        params,
    ).fetchone()[0]

    ok({
        "total_syncs": total_syncs,
        "completed": completed,
        "failed": failed,
        "running": running,
        "total_records_processed": total_records,
        "total_records_failed": total_errors,
        "success_rate": f"{(completed / total_syncs * 100):.1f}%" if total_syncs > 0 else "N/A",
    })


# ===========================================================================
# 14. get-sync-log
# ===========================================================================
def get_sync_log(conn, args):
    sid = getattr(args, "sync_id", None)
    if not sid:
        err("--sync-id is required")
    row = conn.execute(Q.from_(Table("integration_sync")).select(Table("integration_sync").star).where(Field("id") == P()).get_sql(), (sid,)).fetchone()
    if not row:
        err(f"Sync {sid} not found")

    sync_data = row_to_dict(row)
    errors = conn.execute(
        "SELECT * FROM integration_sync_error WHERE sync_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (sid, args.limit, args.offset),
    ).fetchall()

    ok({
        "sync": sync_data,
        "errors": [row_to_dict(e) for e in errors],
        "error_count": len(errors),
    })


# ===========================================================================
# Action registry
# ===========================================================================
ACTIONS = {
    "integration-start-sync": start_sync,
    "integration-get-sync": get_sync,
    "integration-list-syncs": list_syncs,
    "integration-cancel-sync": cancel_sync,
    "integration-add-sync-schedule": add_sync_schedule,
    "integration-update-sync-schedule": update_sync_schedule,
    "integration-list-sync-schedules": list_sync_schedules,
    "integration-delete-sync-schedule": delete_sync_schedule,
    "integration-add-sync-error": add_sync_error,
    "integration-list-sync-errors": list_sync_errors,
    "integration-resolve-sync-error": resolve_sync_error,
    "integration-retry-sync": retry_sync,
    "integration-sync-summary-report": sync_summary_report,
    "integration-get-sync-log": get_sync_log,
}
