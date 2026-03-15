"""ERPClaw Maintenance — Equipment domain module.

10 actions: add-equipment, update-equipment, get-equipment, list-equipment,
add-equipment-child, list-equipment-tree, add-equipment-reading,
list-equipment-readings, link-equipment-asset, import-equipment.
"""
import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row

SKILL = "erpclaw-maintenance"

VALID_EQUIPMENT_TYPES = ("machine", "vehicle", "tool", "instrument", "fixture", "other")
VALID_CRITICALITY = ("critical", "high", "medium", "low")
VALID_EQUIPMENT_STATUS = ("operational", "maintenance", "breakdown", "decommissioned")
VALID_READING_TYPES = ("meter", "temperature", "pressure", "vibration", "other")


def add_equipment(conn, args):
    """Add a new equipment record."""
    name = getattr(args, "name", None)
    company_id = getattr(args, "company_id", None)
    if not name or not company_id:
        err("--name and --company-id are required")

    eq_type = getattr(args, "equipment_type", None) or "machine"
    if eq_type not in VALID_EQUIPMENT_TYPES:
        err(f"Invalid equipment_type: {eq_type}. Must be one of: {', '.join(VALID_EQUIPMENT_TYPES)}")

    criticality = getattr(args, "criticality", None) or "medium"
    if criticality not in VALID_CRITICALITY:
        err(f"Invalid criticality: {criticality}. Must be one of: {', '.join(VALID_CRITICALITY)}")

    eq_status = getattr(args, "equipment_status", None) or "operational"
    if eq_status not in VALID_EQUIPMENT_STATUS:
        err(f"Invalid status: {eq_status}. Must be one of: {', '.join(VALID_EQUIPMENT_STATUS)}")

    eq_id = str(uuid.uuid4())
    naming = get_next_name(conn, "equipment", company_id=company_id)
    now = datetime.now(timezone.utc).isoformat()

    sql, _ = insert_row("equipment", {"id": P(), "naming_series": P(), "name": P(), "equipment_type": P(), "model": P(), "manufacturer": P(), "serial_number": P(), "location": P(), "parent_equipment_id": P(), "asset_id": P(), "item_id": P(), "purchase_date": P(), "warranty_expiry": P(), "criticality": P(), "status": P(), "notes": P(), "company_id": P(), "created_at": P(), "updated_at": P()})
    conn.execute(sql,
        (eq_id, naming, name, eq_type,
         getattr(args, "model", None),
         getattr(args, "manufacturer", None),
         getattr(args, "serial_number", None),
         getattr(args, "location", None),
         getattr(args, "parent_equipment_id", None),
         getattr(args, "asset_id", None),
         getattr(args, "item_id", None),
         getattr(args, "purchase_date", None),
         getattr(args, "warranty_expiry", None),
         criticality, eq_status,
         getattr(args, "notes", None),
         company_id, now, now),
    )
    conn.commit()

    audit(conn, SKILL, "maintenance-add-equipment", "equipment", eq_id,
          new_values={"name": name}, description=f"Added equipment: {name}")
    conn.commit()

    ok({
        "id": eq_id,
        "naming_series": naming,
        "name": name,
        "equipment_type": eq_type,
        "equipment_status": eq_status,
        "criticality": criticality,
        "company_id": company_id,
    })


def update_equipment(conn, args):
    """Update an existing equipment record."""
    eq_id = getattr(args, "equipment_id", None)
    if not eq_id:
        err("--equipment-id is required")

    row = conn.execute(Q.from_(Table("equipment")).select(Table("equipment").star).where(Field("id") == P()).get_sql(), (eq_id,)).fetchone()
    if not row:
        err(f"Equipment {eq_id} not found")

    updates = []
    params = []
    updated_fields = []
    now = datetime.now(timezone.utc).isoformat()

    for field, attr in [
        ("name", "name"), ("equipment_type", "equipment_type"),
        ("model", "model"), ("manufacturer", "manufacturer"),
        ("serial_number", "serial_number"), ("location", "location"),
        ("criticality", "criticality"), ("notes", "notes"),
        ("purchase_date", "purchase_date"), ("warranty_expiry", "warranty_expiry"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            if field == "equipment_type" and val not in VALID_EQUIPMENT_TYPES:
                err(f"Invalid equipment_type: {val}")
            if field == "criticality" and val not in VALID_CRITICALITY:
                err(f"Invalid criticality: {val}")
            updates.append(f"{field} = ?")
            params.append(val)
            updated_fields.append(field)

    # Handle equipment_status separately to avoid ok() collision
    eq_status = getattr(args, "equipment_status", None)
    if eq_status is not None:
        if eq_status not in VALID_EQUIPMENT_STATUS:
            err(f"Invalid status: {eq_status}")
        updates.append("status = ?")
        params.append(eq_status)
        updated_fields.append("status")

    if not updates:
        err("No fields to update")

    updates.append("updated_at = ?")
    params.append(now)
    params.append(eq_id)

    conn.execute(f"UPDATE equipment SET {', '.join(updates)} WHERE id = ?", params)
    conn.commit()

    audit(conn, SKILL, "maintenance-update-equipment", "equipment", eq_id,
          new_values={f: "updated" for f in updated_fields},
          description=f"Updated equipment {eq_id}: {', '.join(updated_fields)}")
    conn.commit()

    ok({"id": eq_id, "updated_fields": updated_fields})


def get_equipment(conn, args):
    """Get a single equipment record by ID."""
    eq_id = getattr(args, "equipment_id", None)
    if not eq_id:
        err("--equipment-id is required")

    conn.row_factory = _row_factory(conn)
    row = conn.execute(Q.from_(Table("equipment")).select(Table("equipment").star).where(Field("id") == P()).get_sql(), (eq_id,)).fetchone()
    if not row:
        err(f"Equipment {eq_id} not found")

    data = dict(row)
    # Rename status to equipment_status to avoid ok() collision
    data["equipment_status"] = data.pop("status", None)
    ok(data)


def list_equipment(conn, args):
    """List equipment with optional filters."""
    company_id = getattr(args, "company_id", None)
    eq_type = getattr(args, "equipment_type", None)
    eq_status = getattr(args, "equipment_status", None)
    criticality = getattr(args, "criticality", None)
    search = getattr(args, "search", None)
    limit = getattr(args, "limit", None) or 50
    offset = getattr(args, "offset", None) or 0

    where = []
    params = []

    if company_id:
        where.append("company_id = ?")
        params.append(company_id)
    if eq_type:
        where.append("equipment_type = ?")
        params.append(eq_type)
    if eq_status:
        where.append("status = ?")
        params.append(eq_status)
    if criticality:
        where.append("criticality = ?")
        params.append(criticality)
    if search:
        where.append("(name LIKE ? OR model LIKE ? OR serial_number LIKE ?)")
        params.extend([f"%{search}%"] * 3)

    where_sql = " AND ".join(where) if where else "1=1"

    conn.row_factory = _row_factory(conn)
    count = conn.execute(f"SELECT COUNT(*) FROM equipment WHERE {where_sql}", params).fetchone()[0]

    rows = conn.execute(
        f"SELECT * FROM equipment WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    items = []
    for r in rows:
        d = dict(r)
        d["equipment_status"] = d.pop("status", None)
        items.append(d)

    ok({"items": items, "total_count": count, "limit": limit, "offset": offset})


def add_equipment_child(conn, args):
    """Add a child equipment (set parent_equipment_id)."""
    parent_id = getattr(args, "parent_equipment_id", None)
    name = getattr(args, "name", None)
    company_id = getattr(args, "company_id", None)

    if not parent_id or not name or not company_id:
        err("--parent-equipment-id, --name, and --company-id are required")

    parent = conn.execute(Q.from_(Table("equipment")).select(Field('id')).where(Field("id") == P()).get_sql(), (parent_id,)).fetchone()
    if not parent:
        err(f"Parent equipment {parent_id} not found")

    eq_type = getattr(args, "equipment_type", None) or "machine"
    criticality = getattr(args, "criticality", None) or "medium"

    eq_id = str(uuid.uuid4())
    naming = get_next_name(conn, "equipment", company_id=company_id)
    now = datetime.now(timezone.utc).isoformat()

    sql, _ = insert_row("equipment", {"id": P(), "naming_series": P(), "name": P(), "equipment_type": P(), "model": P(), "manufacturer": P(), "serial_number": P(), "location": P(), "parent_equipment_id": P(), "criticality": P(), "status": P(), "notes": P(), "company_id": P(), "created_at": P(), "updated_at": P()})
    conn.execute(sql,
        (eq_id, naming, name, eq_type,
         getattr(args, "model", None),
         getattr(args, "manufacturer", None),
         getattr(args, "serial_number", None),
         getattr(args, "location", None),
         parent_id, criticality, "operational",
         getattr(args, "notes", None),
         company_id, now, now),
    )
    conn.commit()

    audit(conn, SKILL, "maintenance-add-equipment-child", "equipment", eq_id,
          new_values={"name": name, "parent_equipment_id": parent_id},
          description=f"Added child equipment: {name} under {parent_id}")
    conn.commit()

    ok({
        "id": eq_id,
        "naming_series": naming,
        "name": name,
        "parent_equipment_id": parent_id,
        "equipment_type": eq_type,
    })


def list_equipment_tree(conn, args):
    """List equipment tree (recursive children)."""
    root_id = getattr(args, "equipment_id", None)
    company_id = getattr(args, "company_id", None)

    if not root_id and not company_id:
        err("--equipment-id or --company-id is required")

    conn.row_factory = _row_factory(conn)

    def _get_children(parent_id):
        rows = conn.execute(Q.from_(Table("equipment")).select(Table("equipment").star).where(Field("parent_equipment_id") == P()).orderby(Field("name")).get_sql(), (parent_id,)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["equipment_status"] = d.pop("status", None)
            d["children"] = _get_children(d["id"])
            result.append(d)
        return result

    if root_id:
        root = conn.execute(Q.from_(Table("equipment")).select(Table("equipment").star).where(Field("id") == P()).get_sql(), (root_id,)).fetchone()
        if not root:
            err(f"Equipment {root_id} not found")
        root_data = dict(root)
        root_data["equipment_status"] = root_data.pop("status", None)
        root_data["children"] = _get_children(root_id)
        ok({"tree": root_data})
    else:
        # Get all root nodes (no parent) for company
        roots = conn.execute(
            "SELECT * FROM equipment WHERE company_id = ? AND parent_equipment_id IS NULL ORDER BY name",
            (company_id,),
        ).fetchall()
        tree = []
        for r in roots:
            d = dict(r)
            d["equipment_status"] = d.pop("status", None)
            d["children"] = _get_children(d["id"])
            tree.append(d)
        ok({"tree": tree, "total_roots": len(tree)})


def add_equipment_reading(conn, args):
    """Add a meter/sensor reading for equipment."""
    equipment_id = getattr(args, "equipment_id", None)
    reading_value = getattr(args, "reading_value", None)
    company_id = getattr(args, "company_id", None)

    if not equipment_id or not reading_value or not company_id:
        err("--equipment-id, --reading-value, and --company-id are required")

    eq = conn.execute(Q.from_(Table("equipment")).select(Field('id')).where(Field("id") == P()).get_sql(), (equipment_id,)).fetchone()
    if not eq:
        err(f"Equipment {equipment_id} not found")

    reading_type = getattr(args, "reading_type", None) or "meter"
    if reading_type not in VALID_READING_TYPES:
        err(f"Invalid reading_type: {reading_type}. Must be one of: {', '.join(VALID_READING_TYPES)}")

    reading_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    reading_date = getattr(args, "reading_date", None) or now

    sql, _ = insert_row("equipment_reading", {"id": P(), "equipment_id": P(), "reading_type": P(), "reading_value": P(), "reading_unit": P(), "reading_date": P(), "recorded_by": P(), "company_id": P(), "created_at": P()})
    conn.execute(sql,
        (reading_id, equipment_id, reading_type, reading_value,
         getattr(args, "reading_unit", None),
         reading_date,
         getattr(args, "recorded_by", None),
         company_id, now),
    )
    conn.commit()

    ok({
        "id": reading_id,
        "equipment_id": equipment_id,
        "reading_type": reading_type,
        "reading_value": reading_value,
        "reading_date": reading_date,
    })


def list_equipment_readings(conn, args):
    """List readings for an equipment."""
    equipment_id = getattr(args, "equipment_id", None)
    if not equipment_id:
        err("--equipment-id is required")

    limit = getattr(args, "limit", None) or 50
    offset = getattr(args, "offset", None) or 0
    reading_type = getattr(args, "reading_type", None)

    where = ["equipment_id = ?"]
    params = [equipment_id]

    if reading_type:
        where.append("reading_type = ?")
        params.append(reading_type)

    where_sql = " AND ".join(where)

    conn.row_factory = _row_factory(conn)
    count = conn.execute(
        f"SELECT COUNT(*) FROM equipment_reading WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"SELECT * FROM equipment_reading WHERE {where_sql} ORDER BY reading_date DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    ok({
        "items": [dict(r) for r in rows],
        "total_count": count,
        "limit": limit,
        "offset": offset,
    })


def link_equipment_asset(conn, args):
    """Link equipment to an asset from erpclaw-assets."""
    eq_id = getattr(args, "equipment_id", None)
    asset_id = getattr(args, "asset_id", None)

    if not eq_id or not asset_id:
        err("--equipment-id and --asset-id are required")

    row = conn.execute(Q.from_(Table("equipment")).select(Field('id')).where(Field("id") == P()).get_sql(), (eq_id,)).fetchone()
    if not row:
        err(f"Equipment {eq_id} not found")

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE equipment SET asset_id = ?, updated_at = ? WHERE id = ?",
        (asset_id, now, eq_id),
    )
    conn.commit()

    audit(conn, SKILL, "maintenance-link-equipment-asset", "equipment", eq_id,
          new_values={"asset_id": asset_id},
          description=f"Linked equipment {eq_id} to asset {asset_id}")
    conn.commit()

    ok({"id": eq_id, "asset_id": asset_id})


def import_equipment(conn, args):
    """Import equipment (stub)."""
    ok({"imported": 0, "message": "Import not yet implemented. Use add-equipment for individual records."})


def _row_factory(conn):
    """Return sqlite3.Row factory if not already set."""
    import sqlite3
    return sqlite3.Row


ACTIONS = {
    "maintenance-add-equipment": add_equipment,
    "maintenance-update-equipment": update_equipment,
    "maintenance-get-equipment": get_equipment,
    "maintenance-list-equipment": list_equipment,
    "maintenance-add-equipment-child": add_equipment_child,
    "maintenance-list-equipment-tree": list_equipment_tree,
    "maintenance-add-equipment-reading": add_equipment_reading,
    "maintenance-list-equipment-readings": list_equipment_readings,
    "maintenance-link-equipment-asset": link_equipment_asset,
    "maintenance-import-equipment": import_equipment,
}
