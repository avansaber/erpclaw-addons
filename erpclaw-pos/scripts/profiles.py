#!/usr/bin/env python3
"""erpclaw-pos profiles domain module.

POS profile management — register types, warehouse/price list config,
discount rules. Imported by the unified erpclaw-pos db_query.py router.
"""
import os
import sqlite3
import sys
import uuid
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit

SKILL = "erpclaw-pos"

VALID_PAYMENT_METHODS = ("cash", "card", "mobile", "split")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _dec(val):
    if val is None:
        return Decimal("0")
    return Decimal(str(val))


def _round(val):
    return val.quantize(Decimal("0.01"), ROUND_HALF_UP)


# ---------------------------------------------------------------------------
# add-pos-profile
# ---------------------------------------------------------------------------
def add_pos_profile(conn, args):
    name = getattr(args, "name", None)
    company_id = getattr(args, "company_id", None)

    if not name:
        err("--name is required")
    if not company_id:
        err("--company-id is required")

    # Validate company exists
    if not conn.execute("SELECT id FROM company WHERE id = ?",
                        (company_id,)).fetchone():
        err(f"Company {company_id} not found")

    warehouse_id = getattr(args, "warehouse_id", None)
    price_list_id = getattr(args, "price_list_id", None)

    default_pm = getattr(args, "default_payment_method", None) or "cash"
    if default_pm not in VALID_PAYMENT_METHODS:
        err(f"--default-payment-method must be one of: {', '.join(VALID_PAYMENT_METHODS)}")

    allow_discount_raw = getattr(args, "allow_discount", None)
    allow_discount = 1
    if allow_discount_raw is not None:
        allow_discount = 1 if str(allow_discount_raw).lower() in ("1", "true", "yes") else 0

    max_discount_pct = str(_round(_dec(
        getattr(args, "max_discount_pct", None) or "100"
    )))

    auto_print_raw = getattr(args, "auto_print_receipt", None)
    auto_print = 0
    if auto_print_raw is not None:
        auto_print = 1 if str(auto_print_raw).lower() in ("1", "true", "yes") else 0

    profile_id = str(uuid.uuid4())
    naming = get_next_name(conn, "pos_profile", company_id=company_id)

    try:
        conn.execute(
            """INSERT INTO pos_profile
               (id, naming_series, name, warehouse_id, price_list_id,
                default_payment_method, allow_discount, max_discount_pct,
                auto_print_receipt, is_active, company_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (profile_id, naming, name, warehouse_id, price_list_id,
             default_pm, allow_discount, max_discount_pct,
             auto_print, 1, company_id))
    except sqlite3.IntegrityError as e:
        sys.stderr.write(f"[{SKILL}] {e}\n")
        err("Profile creation failed — check for duplicates or invalid references")

    audit(conn, SKILL, "pos-add-pos-profile", "pos_profile", profile_id,
          new_values={"name": name, "naming_series": naming})
    conn.commit()
    ok({"id": profile_id, "naming_series": naming, "name": name})


# ---------------------------------------------------------------------------
# update-pos-profile
# ---------------------------------------------------------------------------
def update_pos_profile(conn, args):
    pid = getattr(args, "id", None)
    if not pid:
        err("--id is required")

    row = conn.execute("SELECT * FROM pos_profile WHERE id = ?", (pid,)).fetchone()
    if not row:
        err(f"POS profile {pid} not found")

    updates, params, changed = [], [], []

    if getattr(args, "name", None) is not None:
        updates.append("name = ?"); params.append(args.name); changed.append("name")
    if getattr(args, "warehouse_id", None) is not None:
        updates.append("warehouse_id = ?"); params.append(args.warehouse_id); changed.append("warehouse_id")
    if getattr(args, "price_list_id", None) is not None:
        updates.append("price_list_id = ?"); params.append(args.price_list_id); changed.append("price_list_id")
    if getattr(args, "default_payment_method", None) is not None:
        if args.default_payment_method not in VALID_PAYMENT_METHODS:
            err(f"--default-payment-method must be one of: {', '.join(VALID_PAYMENT_METHODS)}")
        updates.append("default_payment_method = ?"); params.append(args.default_payment_method)
        changed.append("default_payment_method")
    if getattr(args, "allow_discount", None) is not None:
        val = 1 if str(args.allow_discount).lower() in ("1", "true", "yes") else 0
        updates.append("allow_discount = ?"); params.append(val); changed.append("allow_discount")
    if getattr(args, "max_discount_pct", None) is not None:
        updates.append("max_discount_pct = ?")
        params.append(str(_round(_dec(args.max_discount_pct))))
        changed.append("max_discount_pct")
    if getattr(args, "auto_print_receipt", None) is not None:
        val = 1 if str(args.auto_print_receipt).lower() in ("1", "true", "yes") else 0
        updates.append("auto_print_receipt = ?"); params.append(val); changed.append("auto_print_receipt")
    if getattr(args, "is_active", None) is not None:
        val = 1 if str(args.is_active).lower() in ("1", "true", "yes") else 0
        updates.append("is_active = ?"); params.append(val); changed.append("is_active")

    if not changed:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(pid)
    conn.execute(f"UPDATE pos_profile SET {', '.join(updates)} WHERE id = ?", params)

    audit(conn, SKILL, "pos-update-pos-profile", "pos_profile", pid,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": pid, "updated_fields": changed})


# ---------------------------------------------------------------------------
# get-pos-profile
# ---------------------------------------------------------------------------
def get_pos_profile(conn, args):
    pid = getattr(args, "id", None)
    if not pid:
        err("--id is required")

    row = conn.execute("SELECT * FROM pos_profile WHERE id = ?", (pid,)).fetchone()
    if not row:
        err(f"POS profile {pid} not found")

    data = row_to_dict(row)

    # Count open sessions
    session_count = conn.execute(
        "SELECT COUNT(*) FROM pos_session WHERE pos_profile_id = ? AND status = 'open'",
        (pid,)).fetchone()[0]
    data["open_sessions"] = session_count

    ok(data)


# ---------------------------------------------------------------------------
# list-pos-profiles
# ---------------------------------------------------------------------------
def list_pos_profiles(conn, args):
    params = []
    where = ["1=1"]

    company_id = getattr(args, "company_id", None)
    is_active = getattr(args, "is_active", None)
    search = getattr(args, "search", None)

    if company_id:
        where.append("p.company_id = ?"); params.append(company_id)
    if is_active is not None:
        val = 1 if str(is_active).lower() in ("1", "true", "yes") else 0
        where.append("p.is_active = ?"); params.append(val)
    if search:
        where.append("p.name LIKE ?"); params.append(f"%{search}%")

    where_clause = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM pos_profile p WHERE {where_clause}",
        params).fetchone()[0]

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    rows = conn.execute(
        f"""SELECT p.* FROM pos_profile p
            WHERE {where_clause}
            ORDER BY p.name LIMIT ? OFFSET ?""",
        params + [limit, offset]).fetchall()

    ok({"profiles": [row_to_dict(r) for r in rows],
        "total": total, "limit": limit, "offset": offset,
        "has_more": offset + limit < total})


# ---------------------------------------------------------------------------
# Action Router
# ---------------------------------------------------------------------------
ACTIONS = {
    "pos-add-pos-profile": add_pos_profile,
    "pos-update-pos-profile": update_pos_profile,
    "pos-get-pos-profile": get_pos_profile,
    "pos-list-pos-profiles": list_pos_profiles,
}
