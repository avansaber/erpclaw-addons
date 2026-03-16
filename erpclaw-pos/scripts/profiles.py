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
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row, dynamic_update
from erpclaw_lib.vendor.pypika.terms import LiteralValue

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
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
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
        sql, _ = insert_row("pos_profile", {"id": P(), "naming_series": P(), "name": P(), "warehouse_id": P(), "price_list_id": P(), "default_payment_method": P(), "allow_discount": P(), "max_discount_pct": P(), "auto_print_receipt": P(), "is_active": P(), "company_id": P()})
        conn.execute(sql,
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

    row = conn.execute(Q.from_(Table("pos_profile")).select(Table("pos_profile").star).where(Field("id") == P()).get_sql(), (pid,)).fetchone()
    if not row:
        err(f"POS profile {pid} not found")

    data, changed = {}, []

    if getattr(args, "name", None) is not None:
        data["name"] = args.name; changed.append("name")
    if getattr(args, "warehouse_id", None) is not None:
        data["warehouse_id"] = args.warehouse_id; changed.append("warehouse_id")
    if getattr(args, "price_list_id", None) is not None:
        data["price_list_id"] = args.price_list_id; changed.append("price_list_id")
    if getattr(args, "default_payment_method", None) is not None:
        if args.default_payment_method not in VALID_PAYMENT_METHODS:
            err(f"--default-payment-method must be one of: {', '.join(VALID_PAYMENT_METHODS)}")
        data["default_payment_method"] = args.default_payment_method
        changed.append("default_payment_method")
    if getattr(args, "allow_discount", None) is not None:
        val = 1 if str(args.allow_discount).lower() in ("1", "true", "yes") else 0
        data["allow_discount"] = val; changed.append("allow_discount")
    if getattr(args, "max_discount_pct", None) is not None:
        data["max_discount_pct"] = str(_round(_dec(args.max_discount_pct)))
        changed.append("max_discount_pct")
    if getattr(args, "auto_print_receipt", None) is not None:
        val = 1 if str(args.auto_print_receipt).lower() in ("1", "true", "yes") else 0
        data["auto_print_receipt"] = val; changed.append("auto_print_receipt")
    if getattr(args, "is_active", None) is not None:
        val = 1 if str(args.is_active).lower() in ("1", "true", "yes") else 0
        data["is_active"] = val; changed.append("is_active")

    if not changed:
        err("No fields to update")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("pos_profile", data, {"id": pid})
    conn.execute(sql, params)

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

    row = conn.execute(Q.from_(Table("pos_profile")).select(Table("pos_profile").star).where(Field("id") == P()).get_sql(), (pid,)).fetchone()
    if not row:
        err(f"POS profile {pid} not found")

    data = row_to_dict(row)

    # Count open sessions
    t_sess = Table("pos_session")
    session_count = conn.execute(
        Q.from_(t_sess).select(fn.Count(t_sess.star))
        .where(t_sess.pos_profile_id == P()).where(t_sess.status == "open").get_sql(),
        (pid,)).fetchone()[0]
    data["open_sessions"] = session_count

    ok(data)


# ---------------------------------------------------------------------------
# list-pos-profiles
# ---------------------------------------------------------------------------
def list_pos_profiles(conn, args):
    t = Table("pos_profile")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star))
    params = []

    company_id = getattr(args, "company_id", None)
    is_active = getattr(args, "is_active", None)
    search = getattr(args, "search", None)

    if company_id:
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(company_id)
    if is_active is not None:
        val = 1 if str(is_active).lower() in ("1", "true", "yes") else 0
        q = q.where(t.is_active == P())
        q_cnt = q_cnt.where(t.is_active == P())
        params.append(val)
    if search:
        q = q.where(t.name.like(P()))
        q_cnt = q_cnt.where(t.name.like(P()))
        params.append(f"%{search}%")

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    q = q.orderby(t.name).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()

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
