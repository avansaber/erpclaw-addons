"""ERPClaw Fleet -- fleet domain module

Actions for vehicles, assignments, fuel logs, maintenance, and reports (4 tables, 15 actions).
Imported by db_query.py (unified router).
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit

    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row
    ENTITY_PREFIXES.setdefault("fleet_vehicle", "VEH-")
    ENTITY_PREFIXES.setdefault("fleet_vehicle_maintenance", "FMNT-")
except ImportError:
    pass

SKILL = "erpclaw-fleet"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------------------------------------------------------------------
# Validation constants
# ---------------------------------------------------------------------------
VALID_VEHICLE_TYPES = ("sedan", "suv", "truck", "van", "motorcycle", "other")
VALID_VEHICLE_STATUSES = ("available", "assigned", "maintenance", "retired")
VALID_FUEL_TYPES = ("gasoline", "diesel", "electric", "hybrid", "other")
VALID_ASSIGNMENT_STATUSES = ("active", "ended")
VALID_MAINTENANCE_TYPES = ("oil_change", "tire_rotation", "brake_service",
                           "inspection", "repair", "scheduled", "other")
VALID_MAINTENANCE_STATUSES = ("scheduled", "in_progress", "completed", "cancelled")


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


def _validate_vehicle(conn, vehicle_id):
    if not vehicle_id:
        err("--vehicle-id is required")
    t = Table("fleet_vehicle")
    q = Q.from_(t).select(t.id).where(t.id == P())
    if not conn.execute(q.get_sql(), (vehicle_id,)).fetchone():
        err(f"Vehicle {vehicle_id} not found")


# ===========================================================================
# 1. add-vehicle
# ===========================================================================
def add_vehicle(conn, args):
    _validate_company(conn, args.company_id)
    make = getattr(args, "make", None)
    if not make:
        err("--make is required")
    model = getattr(args, "model", None)
    if not model:
        err("--model is required")

    vehicle_type = getattr(args, "vehicle_type", None) or "sedan"
    _validate_enum(vehicle_type, VALID_VEHICLE_TYPES, "vehicle-type")

    fuel_type = getattr(args, "fuel_type", None) or "gasoline"
    _validate_enum(fuel_type, VALID_FUEL_TYPES, "fuel-type")

    veh_id = str(uuid.uuid4())
    naming = get_next_name(conn, "fleet_vehicle", company_id=args.company_id)
    now = _now_iso()

    year_val = getattr(args, "year", None)
    if year_val is not None:
        year_val = int(year_val)

    sql, _ = insert_row("fleet_vehicle", {
        "id": P(), "naming_series": P(), "make": P(), "model": P(), "year": P(),
        "vin": P(), "license_plate": P(), "vehicle_type": P(), "color": P(),
        "purchase_date": P(), "purchase_cost": P(), "current_odometer": P(),
        "fuel_type": P(), "insurance_provider": P(), "insurance_policy": P(),
        "insurance_expiry": P(), "vehicle_status": P(), "notes": P(),
        "company_id": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        veh_id, naming, make, model, year_val,
        getattr(args, "vin", None),
        getattr(args, "license_plate", None),
        vehicle_type,
        getattr(args, "color", None),
        getattr(args, "purchase_date", None),
        getattr(args, "purchase_cost", None),
        str(to_decimal(getattr(args, "current_odometer", None) or "0")),
        fuel_type,
        getattr(args, "insurance_provider", None),
        getattr(args, "insurance_policy", None),
        getattr(args, "insurance_expiry", None),
        "available", getattr(args, "notes", None),
        args.company_id, now, now,
    ))
    audit(conn, SKILL, "fleet-add-vehicle", "fleet_vehicle", veh_id,
          new_values={"make": make, "model": model})
    conn.commit()
    ok({
        "id": veh_id, "naming_series": naming, "make": make, "model": model,
        "vehicle_type": vehicle_type, "vehicle_status": "available",
    })


# ===========================================================================
# 2. update-vehicle
# ===========================================================================
def update_vehicle(conn, args):
    veh_id = getattr(args, "vehicle_id", None)
    if not veh_id:
        err("--vehicle-id is required")
    _validate_vehicle(conn, veh_id)

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "make": "make", "model": "model", "vin": "vin",
        "license_plate": "license_plate", "color": "color",
        "purchase_cost": "purchase_cost",
        "insurance_provider": "insurance_provider",
        "insurance_policy": "insurance_policy",
        "insurance_expiry": "insurance_expiry",
        "notes": "notes",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    year_val = getattr(args, "year", None)
    if year_val is not None:
        updates.append("year = ?")
        params.append(int(year_val))
        changed.append("year")

    vehicle_type = getattr(args, "vehicle_type", None)
    if vehicle_type is not None:
        _validate_enum(vehicle_type, VALID_VEHICLE_TYPES, "vehicle-type")
        updates.append("vehicle_type = ?")
        params.append(vehicle_type)
        changed.append("vehicle_type")

    fuel_type = getattr(args, "fuel_type", None)
    if fuel_type is not None:
        _validate_enum(fuel_type, VALID_FUEL_TYPES, "fuel-type")
        updates.append("fuel_type = ?")
        params.append(fuel_type)
        changed.append("fuel_type")

    vehicle_status = getattr(args, "vehicle_status", None)
    if vehicle_status is not None:
        _validate_enum(vehicle_status, VALID_VEHICLE_STATUSES, "vehicle-status")
        updates.append("vehicle_status = ?")
        params.append(vehicle_status)
        changed.append("vehicle_status")

    current_odometer = getattr(args, "current_odometer", None)
    if current_odometer is not None:
        updates.append("current_odometer = ?")
        params.append(str(to_decimal(current_odometer)))
        changed.append("current_odometer")

    if not updates:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(veh_id)
    conn.execute(f"UPDATE fleet_vehicle SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, SKILL, "fleet-update-vehicle", "fleet_vehicle", veh_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": veh_id, "updated_fields": changed})


# ===========================================================================
# 3. get-vehicle
# ===========================================================================
def get_vehicle(conn, args):
    veh_id = getattr(args, "vehicle_id", None)
    if not veh_id:
        err("--vehicle-id is required")
    t = Table("fleet_vehicle")
    q = Q.from_(t).select(t.star).where(t.id == P())
    row = conn.execute(q.get_sql(), (veh_id,)).fetchone()
    if not row:
        err(f"Vehicle {veh_id} not found")
    data = row_to_dict(row)

    # Active assignments
    t_assign = Table("fleet_vehicle_assignment")
    q_assign = Q.from_(t_assign).select(t_assign.star).where(t_assign.vehicle_id == P()).where(t_assign.assignment_status == "active")
    assignments = conn.execute(q_assign.get_sql(), (veh_id,)).fetchall()
    data["active_assignments"] = [row_to_dict(a) for a in assignments]

    # Fuel log count
    t_fuel = Table("fleet_fuel_log")
    q_fuel = Q.from_(t_fuel).select(fn.Count("*")).where(t_fuel.vehicle_id == P())
    fuel_count = conn.execute(q_fuel.get_sql(), (veh_id,)).fetchone()[0]
    data["fuel_log_count"] = fuel_count

    # Maintenance count
    t_maint = Table("fleet_vehicle_maintenance")
    q_maint = Q.from_(t_maint).select(fn.Count("*")).where(t_maint.vehicle_id == P())
    maint_count = conn.execute(q_maint.get_sql(), (veh_id,)).fetchone()[0]
    data["maintenance_count"] = maint_count
    ok(data)


# ===========================================================================
# 4. list-vehicles
# ===========================================================================
def list_vehicles(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "vehicle_status", None):
        where.append("vehicle_status = ?")
        params.append(args.vehicle_status)
    if getattr(args, "vehicle_type", None):
        where.append("vehicle_type = ?")
        params.append(args.vehicle_type)
    if getattr(args, "search", None):
        where.append("(make LIKE ? OR model LIKE ? OR license_plate LIKE ? OR vin LIKE ?)")
        params.extend([f"%{args.search}%"] * 4)

    where_sql = " AND ".join(where)
    total = conn.execute(f"SELECT COUNT(*) FROM fleet_vehicle WHERE {where_sql}", params).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM fleet_vehicle WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 5. add-vehicle-assignment
# ===========================================================================
def add_vehicle_assignment(conn, args):
    vehicle_id = getattr(args, "vehicle_id", None)
    _validate_vehicle(conn, vehicle_id)

    driver_name = getattr(args, "driver_name", None)
    if not driver_name:
        err("--driver-name is required")

    start_date = getattr(args, "start_date", None)
    if not start_date:
        err("--start-date is required")

    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")
    _validate_company(conn, company_id)

    assign_id = str(uuid.uuid4())
    now = _now_iso()

    sql, _ = insert_row("fleet_vehicle_assignment", {
        "id": P(), "vehicle_id": P(), "driver_name": P(), "driver_id": P(),
        "start_date": P(), "end_date": P(), "assignment_status": P(),
        "notes": P(), "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        assign_id, vehicle_id, driver_name,
        getattr(args, "driver_id", None),
        start_date,
        getattr(args, "end_date", None),
        "active",
        getattr(args, "notes", None),
        company_id, now,
    ))

    # Update vehicle status to assigned
    conn.execute(
        "UPDATE \"fleet_vehicle\" SET \"vehicle_status\"=?,\"updated_at\"=datetime('now') WHERE \"id\"=?",
        ("assigned", vehicle_id),
    )

    audit(conn, SKILL, "fleet-add-vehicle-assignment", "fleet_vehicle_assignment", assign_id,
          new_values={"vehicle_id": vehicle_id, "driver_name": driver_name})
    conn.commit()
    ok({
        "id": assign_id, "vehicle_id": vehicle_id, "driver_name": driver_name,
        "assignment_status": "active",
    })


# ===========================================================================
# 6. end-vehicle-assignment
# ===========================================================================
def end_vehicle_assignment(conn, args):
    assign_id = getattr(args, "assignment_id", None)
    if not assign_id:
        err("--assignment-id is required")
    t_assign = Table("fleet_vehicle_assignment")
    q = Q.from_(t_assign).select(t_assign.star).where(t_assign.id == P())
    row = conn.execute(q.get_sql(), (assign_id,)).fetchone()
    if not row:
        err(f"Assignment {assign_id} not found")

    data = row_to_dict(row)
    if data["assignment_status"] != "active":
        err(f"Cannot end assignment in status '{data['assignment_status']}'. Must be active.")

    end_date = getattr(args, "end_date", None) or _now_iso()[:10]
    sql = update_row("fleet_vehicle_assignment",
                     data={"assignment_status": P(), "end_date": P()},
                     where={"id": P()})
    conn.execute(sql, ("ended", end_date, assign_id))

    # Check if vehicle has other active assignments
    vehicle_id = data["vehicle_id"]
    t_va = Table("fleet_vehicle_assignment")
    q_active = (Q.from_(t_va).select(fn.Count("*"))
                .where(t_va.vehicle_id == P())
                .where(t_va.assignment_status == "active")
                .where(t_va.id != P()))
    active_count = conn.execute(q_active.get_sql(), (vehicle_id, assign_id)).fetchone()[0]
    if active_count == 0:
        conn.execute(
            "UPDATE \"fleet_vehicle\" SET \"vehicle_status\"=?,\"updated_at\"=datetime('now') WHERE \"id\"=?",
            ("available", vehicle_id),
        )

    audit(conn, SKILL, "fleet-end-vehicle-assignment", "fleet_vehicle_assignment", assign_id,
          new_values={"assignment_status": "ended", "end_date": end_date})
    conn.commit()
    ok({"id": assign_id, "assignment_status": "ended", "end_date": end_date})


# ===========================================================================
# 7. list-vehicle-assignments
# ===========================================================================
def list_vehicle_assignments(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "vehicle_id", None):
        where.append("vehicle_id = ?")
        params.append(args.vehicle_id)
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "assignment_status", None):
        where.append("assignment_status = ?")
        params.append(args.assignment_status)
    if getattr(args, "driver_id", None):
        where.append("driver_id = ?")
        params.append(args.driver_id)
    if getattr(args, "search", None):
        where.append("driver_name LIKE ?")
        params.append(f"%{args.search}%")

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM fleet_vehicle_assignment WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM fleet_vehicle_assignment WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 8. add-fuel-log
# ===========================================================================
def add_fuel_log(conn, args):
    vehicle_id = getattr(args, "vehicle_id", None)
    _validate_vehicle(conn, vehicle_id)

    log_date = getattr(args, "log_date", None)
    if not log_date:
        err("--log-date is required")

    gallons = getattr(args, "gallons", None)
    if not gallons:
        err("--gallons is required")

    cost = getattr(args, "cost", None)
    if not cost:
        err("--cost is required")

    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")
    _validate_company(conn, company_id)

    odometer_reading = getattr(args, "odometer_reading", None)

    log_id = str(uuid.uuid4())
    now = _now_iso()

    sql, _ = insert_row("fleet_fuel_log", {
        "id": P(), "vehicle_id": P(), "log_date": P(), "gallons": P(),
        "cost": P(), "odometer_reading": P(), "fuel_type": P(),
        "station": P(), "notes": P(), "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        log_id, vehicle_id, log_date,
        str(to_decimal(gallons)),
        str(round_currency(to_decimal(cost))),
        str(to_decimal(odometer_reading)) if odometer_reading else None,
        getattr(args, "fuel_type", None),
        getattr(args, "station", None),
        getattr(args, "notes", None),
        company_id, now,
    ))

    # Update vehicle odometer if provided
    if odometer_reading:
        conn.execute(
            "UPDATE \"fleet_vehicle\" SET \"current_odometer\"=?,\"updated_at\"=datetime('now') WHERE \"id\"=?",
            (str(to_decimal(odometer_reading)), vehicle_id),
        )

    audit(conn, SKILL, "fleet-add-fuel-log", "fleet_fuel_log", log_id,
          new_values={"vehicle_id": vehicle_id, "gallons": gallons, "cost": cost})
    conn.commit()
    ok({
        "id": log_id, "vehicle_id": vehicle_id, "log_date": log_date,
        "gallons": str(to_decimal(gallons)),
        "cost": str(round_currency(to_decimal(cost))),
    })


# ===========================================================================
# 9. list-fuel-logs
# ===========================================================================
def list_fuel_logs(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "vehicle_id", None):
        where.append("vehicle_id = ?")
        params.append(args.vehicle_id)
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "start_date", None):
        where.append("log_date >= ?")
        params.append(args.start_date)
    if getattr(args, "end_date", None):
        where.append("log_date <= ?")
        params.append(args.end_date)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM fleet_fuel_log WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM fleet_fuel_log WHERE {where_sql} ORDER BY log_date DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 10. add-vehicle-maintenance
# ===========================================================================
def add_vehicle_maintenance(conn, args):
    vehicle_id = getattr(args, "vehicle_id", None)
    _validate_vehicle(conn, vehicle_id)

    maintenance_type = getattr(args, "maintenance_type", None)
    if not maintenance_type:
        err("--maintenance-type is required")
    _validate_enum(maintenance_type, VALID_MAINTENANCE_TYPES, "maintenance-type")

    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")
    _validate_company(conn, company_id)

    cost = getattr(args, "cost", None)
    maint_id = str(uuid.uuid4())
    naming = get_next_name(conn, "fleet_vehicle_maintenance", company_id=company_id)
    now = _now_iso()

    sql, _ = insert_row("fleet_vehicle_maintenance", {
        "id": P(), "naming_series": P(), "vehicle_id": P(), "maintenance_type": P(),
        "scheduled_date": P(), "completed_date": P(), "cost": P(), "vendor": P(),
        "odometer_at_service": P(), "maintenance_status": P(), "notes": P(),
        "company_id": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        maint_id, naming, vehicle_id, maintenance_type,
        getattr(args, "scheduled_date", None),
        getattr(args, "completed_date", None),
        str(round_currency(to_decimal(cost))) if cost else None,
        getattr(args, "vendor", None),
        getattr(args, "odometer_at_service", None),
        "scheduled",
        getattr(args, "notes", None),
        company_id, now, now,
    ))
    audit(conn, SKILL, "fleet-add-vehicle-maintenance", "fleet_vehicle_maintenance", maint_id,
          new_values={"vehicle_id": vehicle_id, "maintenance_type": maintenance_type})
    conn.commit()
    ok({
        "id": maint_id, "naming_series": naming, "vehicle_id": vehicle_id,
        "maintenance_type": maintenance_type, "maintenance_status": "scheduled",
        "cost": str(round_currency(to_decimal(cost))) if cost else None,
    })


# ===========================================================================
# 11. complete-vehicle-maintenance
# ===========================================================================
def complete_vehicle_maintenance(conn, args):
    maint_id = getattr(args, "maintenance_id", None)
    if not maint_id:
        err("--maintenance-id is required")
    t_m = Table("fleet_vehicle_maintenance")
    q = Q.from_(t_m).select(t_m.star).where(t_m.id == P())
    row = conn.execute(q.get_sql(), (maint_id,)).fetchone()
    if not row:
        err(f"Maintenance record {maint_id} not found")

    data = row_to_dict(row)
    if data["maintenance_status"] in ("completed", "cancelled"):
        err(f"Cannot complete maintenance in status '{data['maintenance_status']}'.")

    completed_date = getattr(args, "completed_date", None) or _now_iso()[:10]
    updates = ["maintenance_status = 'completed'", "completed_date = ?", "updated_at = datetime('now')"]
    params = [completed_date]

    cost = getattr(args, "cost", None)
    if cost is not None:
        updates.append("cost = ?")
        params.append(str(round_currency(to_decimal(cost))))

    vendor = getattr(args, "vendor", None)
    if vendor is not None:
        updates.append("vendor = ?")
        params.append(vendor)

    odometer = getattr(args, "odometer_at_service", None)
    if odometer is not None:
        updates.append("odometer_at_service = ?")
        params.append(odometer)

    notes = getattr(args, "notes", None)
    if notes is not None:
        updates.append("notes = ?")
        params.append(notes)

    params.append(maint_id)
    conn.execute(
        f"UPDATE fleet_vehicle_maintenance SET {', '.join(updates)} WHERE id = ?",
        params
    )
    audit(conn, SKILL, "fleet-complete-vehicle-maintenance", "fleet_vehicle_maintenance", maint_id,
          new_values={"maintenance_status": "completed"})
    conn.commit()
    ok({"id": maint_id, "maintenance_status": "completed", "completed_date": completed_date})


# ===========================================================================
# 12. list-vehicle-maintenance
# ===========================================================================
def list_vehicle_maintenance(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "vehicle_id", None):
        where.append("vehicle_id = ?")
        params.append(args.vehicle_id)
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "maintenance_status", None):
        where.append("maintenance_status = ?")
        params.append(args.maintenance_status)
    if getattr(args, "maintenance_type", None):
        where.append("maintenance_type = ?")
        params.append(args.maintenance_type)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM fleet_vehicle_maintenance WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM fleet_vehicle_maintenance WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 13. vehicle-cost-report
# ===========================================================================
def vehicle_cost_report(conn, args):
    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")
    _validate_company(conn, company_id)

    where_clause = "v.company_id = ?"
    params = [company_id]

    vehicle_id = getattr(args, "vehicle_id", None)
    if vehicle_id:
        where_clause += " AND v.id = ?"
        params.append(vehicle_id)

    rows = conn.execute(f"""
        SELECT v.id, v.make, v.model, v.year, v.license_plate,
               COALESCE(SUM(CAST(f.cost AS REAL)), 0) AS total_fuel_cost,
               (SELECT COALESCE(SUM(CAST(m.cost AS REAL)), 0)
                FROM fleet_vehicle_maintenance m WHERE m.vehicle_id = v.id) AS total_maint_cost
        FROM fleet_vehicle v
        LEFT JOIN fleet_fuel_log f ON f.vehicle_id = v.id
        WHERE {where_clause}
        GROUP BY v.id
        ORDER BY v.make, v.model
    """, params).fetchall()

    vehicles = []
    for r in rows:
        fuel_cost = Decimal(str(r[5])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        maint_cost = Decimal(str(r[6])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        total = (fuel_cost + maint_cost).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        vehicles.append({
            "vehicle_id": r[0], "make": r[1], "model": r[2],
            "year": r[3], "license_plate": r[4],
            "total_fuel_cost": str(fuel_cost),
            "total_maintenance_cost": str(maint_cost),
            "total_cost": str(total),
        })

    ok({
        "report": "vehicle-cost",
        "total_vehicles": len(vehicles),
        "vehicles": vehicles,
    })


# ===========================================================================
# 14. vehicle-utilization-report
# ===========================================================================
def vehicle_utilization_report(conn, args):
    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")
    _validate_company(conn, company_id)

    params = [company_id]

    # Vehicle counts by status
    status_rows = conn.execute("""
        SELECT vehicle_status, COUNT(*) FROM fleet_vehicle
        WHERE company_id = ? GROUP BY vehicle_status
    """, params).fetchall()

    status_counts = {}
    total = 0
    for r in status_rows:
        status_counts[r[0]] = r[1]
        total += r[1]

    # Active assignment count
    active_assignments = conn.execute("""
        SELECT COUNT(*) FROM fleet_vehicle_assignment
        WHERE assignment_status = 'active' AND company_id = ?
    """, params).fetchone()[0]

    utilization_rate = "0.00"
    if total > 0:
        assigned = status_counts.get("assigned", 0)
        utilization_rate = str(
            (Decimal(str(assigned)) / Decimal(str(total)) * Decimal("100")).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        )

    ok({
        "report": "vehicle-utilization",
        "total_vehicles": total,
        "status_breakdown": status_counts,
        "active_assignments": active_assignments,
        "utilization_rate_pct": utilization_rate,
    })


# ===========================================================================
# 15. status
# ===========================================================================
def status_action(conn, args):
    counts = {}
    for tbl in ("fleet_vehicle", "fleet_vehicle_assignment", "fleet_fuel_log", "fleet_vehicle_maintenance"):
        t = Table(tbl)
        q = Q.from_(t).select(fn.Count("*"))
        counts[tbl] = conn.execute(q.get_sql()).fetchone()[0]
    ok({
        "skill": "erpclaw-fleet",
        "version": "1.0.0",
        "total_tables": 4,
        "record_counts": counts,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "fleet-add-vehicle": add_vehicle,
    "fleet-update-vehicle": update_vehicle,
    "fleet-get-vehicle": get_vehicle,
    "fleet-list-vehicles": list_vehicles,
    "fleet-add-vehicle-assignment": add_vehicle_assignment,
    "fleet-end-vehicle-assignment": end_vehicle_assignment,
    "fleet-list-vehicle-assignments": list_vehicle_assignments,
    "fleet-add-fuel-log": add_fuel_log,
    "fleet-list-fuel-logs": list_fuel_logs,
    "fleet-add-vehicle-maintenance": add_vehicle_maintenance,
    "fleet-complete-vehicle-maintenance": complete_vehicle_maintenance,
    "fleet-list-vehicle-maintenance": list_vehicle_maintenance,
    "fleet-vehicle-cost-report": vehicle_cost_report,
    "fleet-vehicle-utilization-report": vehicle_utilization_report,
    "status": status_action,
}
