"""ERPClaw Integrations -- mappings domain module

Actions for managing field mappings, entity maps, and transform rules.
12 actions.
Imported by db_query.py (unified router).
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

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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


# ===========================================================================
# 1. add-field-mapping
# ===========================================================================
def add_field_mapping(conn, args):
    cid = getattr(args, "connector_id", None)
    connector = _get_connector(conn, cid)
    company_id = row_to_dict(connector)["company_id"]

    entity_type = getattr(args, "entity_type", None)
    if not entity_type:
        err("--entity-type is required")
    source_field = getattr(args, "source_field", None)
    if not source_field:
        err("--source-field is required")
    target_field = getattr(args, "target_field", None)
    if not target_field:
        err("--target-field is required")

    transform_rule = getattr(args, "transform_rule", None)
    if transform_rule:
        try:
            json.loads(transform_rule)
        except (json.JSONDecodeError, TypeError):
            err("--transform-rule must be valid JSON")

    fm_id = str(uuid.uuid4())
    now = _now_iso()

    is_required = getattr(args, "is_required", None)
    is_required_val = int(is_required) if is_required is not None else 0

    sql, _ = insert_row("integration_field_mapping", {"id": P(), "connector_id": P(), "entity_type": P(), "source_field": P(), "target_field": P(), "transform_rule": P(), "is_required": P(), "default_value": P(), "company_id": P(), "created_at": P()})
    conn.execute(sql, (
        fm_id, cid, entity_type, source_field, target_field,
        transform_rule, is_required_val,
        getattr(args, "default_value", None),
        company_id, now,
    ))
    audit(conn, SKILL, "integration-add-field-mapping", "integration_field_mapping", fm_id,
          new_values={"source_field": source_field, "target_field": target_field})
    conn.commit()
    ok({"id": fm_id, "connector_id": cid, "entity_type": entity_type,
        "source_field": source_field, "target_field": target_field})


# ===========================================================================
# 2. update-field-mapping
# ===========================================================================
def update_field_mapping(conn, args):
    fm_id = getattr(args, "field_mapping_id", None)
    if not fm_id:
        err("--field-mapping-id is required")
    row = conn.execute(Q.from_(Table("integration_field_mapping")).select(Table("integration_field_mapping").star).where(Field("id") == P()).get_sql(), (fm_id,)).fetchone()
    if not row:
        err(f"Field mapping {fm_id} not found")

    updates, params, changed = [], [], []

    for col, arg_name in [
        ("source_field", "source_field"),
        ("target_field", "target_field"),
        ("transform_rule", "transform_rule"),
        ("default_value", "default_value"),
    ]:
        val = getattr(args, arg_name, None)
        if val is not None:
            if col == "transform_rule":
                try:
                    json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    err("--transform-rule must be valid JSON")
            updates.append(f"{col} = ?")
            params.append(val)
            changed.append(col)

    is_required = getattr(args, "is_required", None)
    if is_required is not None:
        updates.append("is_required = ?")
        params.append(int(is_required))
        changed.append("is_required")

    if not updates:
        err("No fields to update. Provide at least one field flag.")

    # PyPika: skipped — dynamic UPDATE with variable columns
    params.append(fm_id)
    conn.execute(
        f"UPDATE integration_field_mapping SET {', '.join(updates)} WHERE id = ?",
        params,
    )
    audit(conn, SKILL, "integration-update-field-mapping", "integration_field_mapping", fm_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": fm_id, "updated_fields": changed})


# ===========================================================================
# 3. get-field-mapping
# ===========================================================================
def get_field_mapping(conn, args):
    fm_id = getattr(args, "field_mapping_id", None)
    if not fm_id:
        err("--field-mapping-id is required")
    row = conn.execute(Q.from_(Table("integration_field_mapping")).select(Table("integration_field_mapping").star).where(Field("id") == P()).get_sql(), (fm_id,)).fetchone()
    if not row:
        err(f"Field mapping {fm_id} not found")
    ok(row_to_dict(row))


# ===========================================================================
# 4. list-field-mappings
# ===========================================================================
def list_field_mappings(conn, args):
    # PyPika: skipped — dynamic WHERE with optional filters
    where, params = [], []
    cid = getattr(args, "connector_id", None)
    if cid:
        where.append("connector_id = ?")
        params.append(cid)
    entity_type = getattr(args, "entity_type", None)
    if entity_type:
        where.append("entity_type = ?")
        params.append(entity_type)

    clause = (" WHERE " + " AND ".join(where)) if where else ""
    total = conn.execute(f"SELECT COUNT(*) FROM integration_field_mapping{clause}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM integration_field_mapping{clause} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [args.limit, args.offset],
    ).fetchall()

    ok({"field_mappings": [row_to_dict(r) for r in rows], "total_count": total})


# ===========================================================================
# 5. delete-field-mapping
# ===========================================================================
def delete_field_mapping(conn, args):
    fm_id = getattr(args, "field_mapping_id", None)
    if not fm_id:
        err("--field-mapping-id is required")
    row = conn.execute(Q.from_(Table("integration_field_mapping")).select(Table("integration_field_mapping").star).where(Field("id") == P()).get_sql(), (fm_id,)).fetchone()
    if not row:
        err(f"Field mapping {fm_id} not found")

    _tfm = Table("integration_field_mapping")
    conn.execute(Q.from_(_tfm).delete().where(_tfm.id == P()).get_sql(), (fm_id,))
    audit(conn, SKILL, "integration-delete-field-mapping", "integration_field_mapping", fm_id)
    conn.commit()
    ok({"id": fm_id, "deleted": True})


# ===========================================================================
# 6. add-entity-map
# ===========================================================================
def add_entity_map(conn, args):
    cid = getattr(args, "connector_id", None)
    connector = _get_connector(conn, cid)
    company_id = row_to_dict(connector)["company_id"]

    entity_type = getattr(args, "entity_type", None)
    if not entity_type:
        err("--entity-type is required")
    local_id = getattr(args, "local_id", None)
    if not local_id:
        err("--local-id is required")
    remote_id = getattr(args, "remote_id", None)
    if not remote_id:
        err("--remote-id is required")

    # Check for duplicate
    _tem = Table("integration_entity_map")
    existing = conn.execute(
        Q.from_(_tem).select(_tem.id)
        .where(_tem.connector_id == P())
        .where(_tem.entity_type == P())
        .where(_tem.local_id == P()).get_sql(),
        (cid, entity_type, local_id),
    ).fetchone()
    if existing:
        err(f"Entity map already exists for connector={cid}, entity_type={entity_type}, local_id={local_id}")

    em_id = str(uuid.uuid4())
    now = _now_iso()

    sql, _ = insert_row("integration_entity_map", {"id": P(), "connector_id": P(), "entity_type": P(), "local_id": P(), "remote_id": P(), "last_synced_at": P(), "company_id": P(), "created_at": P()})
    conn.execute(sql, (
        em_id, cid, entity_type, local_id, remote_id,
        now, company_id, now,
    ))
    audit(conn, SKILL, "integration-add-entity-map", "integration_entity_map", em_id,
          new_values={"local_id": local_id, "remote_id": remote_id})
    conn.commit()
    ok({"id": em_id, "connector_id": cid, "entity_type": entity_type,
        "local_id": local_id, "remote_id": remote_id})


# ===========================================================================
# 7. get-entity-map
# ===========================================================================
def get_entity_map(conn, args):
    em_id = getattr(args, "entity_map_id", None)
    if not em_id:
        err("--entity-map-id is required")
    row = conn.execute(Q.from_(Table("integration_entity_map")).select(Table("integration_entity_map").star).where(Field("id") == P()).get_sql(), (em_id,)).fetchone()
    if not row:
        err(f"Entity map {em_id} not found")
    ok(row_to_dict(row))


# ===========================================================================
# 8. list-entity-maps
# ===========================================================================
def list_entity_maps(conn, args):
    # PyPika: skipped — dynamic WHERE with optional filters
    where, params = [], []
    cid = getattr(args, "connector_id", None)
    if cid:
        where.append("connector_id = ?")
        params.append(cid)
    entity_type = getattr(args, "entity_type", None)
    if entity_type:
        where.append("entity_type = ?")
        params.append(entity_type)
    local_id = getattr(args, "local_id", None)
    if local_id:
        where.append("local_id = ?")
        params.append(local_id)
    remote_id = getattr(args, "remote_id", None)
    if remote_id:
        where.append("remote_id = ?")
        params.append(remote_id)

    clause = (" WHERE " + " AND ".join(where)) if where else ""
    total = conn.execute(f"SELECT COUNT(*) FROM integration_entity_map{clause}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM integration_entity_map{clause} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [args.limit, args.offset],
    ).fetchall()

    ok({"entity_maps": [row_to_dict(r) for r in rows], "total_count": total})


# ===========================================================================
# 9. delete-entity-map
# ===========================================================================
def delete_entity_map(conn, args):
    em_id = getattr(args, "entity_map_id", None)
    if not em_id:
        err("--entity-map-id is required")
    row = conn.execute(Q.from_(Table("integration_entity_map")).select(Table("integration_entity_map").star).where(Field("id") == P()).get_sql(), (em_id,)).fetchone()
    if not row:
        err(f"Entity map {em_id} not found")

    _tem = Table("integration_entity_map")
    conn.execute(Q.from_(_tem).delete().where(_tem.id == P()).get_sql(), (em_id,))
    audit(conn, SKILL, "integration-delete-entity-map", "integration_entity_map", em_id)
    conn.commit()
    ok({"id": em_id, "deleted": True})


# ===========================================================================
# 10. add-transform-rule
# ===========================================================================
def add_transform_rule(conn, args):
    cid = getattr(args, "connector_id", None)
    connector = _get_connector(conn, cid)
    company_id = row_to_dict(connector)["company_id"]

    entity_type = getattr(args, "entity_type", None)
    if not entity_type:
        err("--entity-type is required")
    rule_name = getattr(args, "rule_name", None)
    if not rule_name:
        err("--rule-name is required")
    rule_json = getattr(args, "rule_json", None)
    if not rule_json:
        err("--rule-json is required")
    try:
        json.loads(rule_json)
    except (json.JSONDecodeError, TypeError):
        err("--rule-json must be valid JSON")

    tr_id = str(uuid.uuid4())
    now = _now_iso()

    sql, _ = insert_row("integration_transform_rule", {"id": P(), "connector_id": P(), "entity_type": P(), "rule_name": P(), "rule_json": P(), "company_id": P(), "created_at": P()})
    conn.execute(sql, (
        tr_id, cid, entity_type, rule_name, rule_json, company_id, now,
    ))
    audit(conn, SKILL, "integration-add-transform-rule", "integration_transform_rule", tr_id,
          new_values={"rule_name": rule_name})
    conn.commit()
    ok({"id": tr_id, "connector_id": cid, "entity_type": entity_type,
        "rule_name": rule_name})


# ===========================================================================
# 11. list-transform-rules
# ===========================================================================
def list_transform_rules(conn, args):
    # PyPika: skipped — dynamic WHERE with optional filters
    where, params = [], []
    cid = getattr(args, "connector_id", None)
    if cid:
        where.append("connector_id = ?")
        params.append(cid)
    entity_type = getattr(args, "entity_type", None)
    if entity_type:
        where.append("entity_type = ?")
        params.append(entity_type)

    clause = (" WHERE " + " AND ".join(where)) if where else ""
    total = conn.execute(f"SELECT COUNT(*) FROM integration_transform_rule{clause}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM integration_transform_rule{clause} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [args.limit, args.offset],
    ).fetchall()

    ok({"transform_rules": [row_to_dict(r) for r in rows], "total_count": total})


# ===========================================================================
# 12. delete-transform-rule
# ===========================================================================
def delete_transform_rule(conn, args):
    tr_id = getattr(args, "transform_rule_id", None)
    if not tr_id:
        err("--transform-rule-id is required")
    row = conn.execute(Q.from_(Table("integration_transform_rule")).select(Table("integration_transform_rule").star).where(Field("id") == P()).get_sql(), (tr_id,)).fetchone()
    if not row:
        err(f"Transform rule {tr_id} not found")

    _ttr = Table("integration_transform_rule")
    conn.execute(Q.from_(_ttr).delete().where(_ttr.id == P()).get_sql(), (tr_id,))
    audit(conn, SKILL, "integration-delete-transform-rule", "integration_transform_rule", tr_id)
    conn.commit()
    ok({"id": tr_id, "deleted": True})


# ===========================================================================
# Action registry
# ===========================================================================
ACTIONS = {
    "integration-add-field-mapping": add_field_mapping,
    "integration-update-field-mapping": update_field_mapping,
    "integration-get-field-mapping": get_field_mapping,
    "integration-list-field-mappings": list_field_mappings,
    "integration-delete-field-mapping": delete_field_mapping,
    "integration-add-entity-map": add_entity_map,
    "integration-get-entity-map": get_entity_map,
    "integration-list-entity-maps": list_entity_maps,
    "integration-delete-entity-map": delete_entity_map,
    "integration-add-transform-rule": add_transform_rule,
    "integration-list-transform-rules": list_transform_rules,
    "integration-delete-transform-rule": delete_transform_rule,
}
