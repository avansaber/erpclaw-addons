#!/usr/bin/env python3
"""erpclaw-pos sessions domain module.

POS session lifecycle — open, close, track cash float and totals.
Imported by the unified erpclaw-pos db_query.py router.
"""
import os
import sys
import uuid
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

import importlib.util
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit
from erpclaw_lib.db import integrity_error_types
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row, dynamic_update, now
try:
    from transactions import is_cancelled_outside_pos
except ImportError:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from transactions import is_cancelled_outside_pos

SKILL = "erpclaw-pos"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _dec(val):
    if val is None:
        return Decimal("0")
    return Decimal(str(val))


def _round(val):
    return val.quantize(Decimal("0.01"), ROUND_HALF_UP)


def _exact_total(rows, column):
    """Sum a fetched money column exactly with Decimal, never float."""
    return sum((_dec(r[column]) for r in rows), Decimal("0"))


def _is_return_document(status, grand_total) -> bool:
    """A return document is a returned row whose stored total is signed.

    The original sale keeps status returned with its unsigned total and
    counts as a sale; only the negated return document (including -0.00)
    counts as the return, reported as a positive figure."""
    return status == "returned" and _dec(grand_total).is_signed()


# ---------------------------------------------------------------------------
# open-session
# ---------------------------------------------------------------------------
def open_session(conn, args):
    profile_id = getattr(args, "pos_profile_id", None)
    cashier_name = getattr(args, "cashier_name", None)
    opening_amount = getattr(args, "opening_amount", None) or "0"

    if not profile_id:
        err("--pos-profile-id is required")
    if not cashier_name:
        err("--cashier-name is required")

    # Validate profile exists and is active
    profile = conn.execute(Q.from_(Table("pos_profile")).select(Field('id'), Field('company_id'), Field('is_active')).where(Field("id") == P()).get_sql(), (profile_id,)).fetchone()
    if not profile:
        err(f"POS profile {profile_id} not found")
    if not profile["is_active"]:
        err(f"POS profile {profile_id} is not active")

    company_id = profile["company_id"]

    # Only one open session per profile
    t_sess = Table("pos_session")
    existing = conn.execute(
        Q.from_(t_sess).select(t_sess.id)
        .where(t_sess.pos_profile_id == P()).where(t_sess.status == "open").get_sql(),
        (profile_id,)).fetchone()
    if existing:
        err(f"Profile {profile_id} already has an open session: {existing['id']}")

    opening = str(_round(_dec(opening_amount)))
    session_id = str(uuid.uuid4())
    naming = get_next_name(conn, "pos_session", company_id=company_id)

    try:
        sql, _ = insert_row("pos_session", {"id": P(), "naming_series": P(), "pos_profile_id": P(), "cashier_name": P(), "opening_amount": P(), "total_sales": P(), "total_returns": P(), "transaction_count": P(), "status": P(), "company_id": P()})
        conn.execute(sql,
            (session_id, naming, profile_id, cashier_name, opening,
             "0", "0", 0, "open", company_id))
    except integrity_error_types() as e:
        sys.stderr.write(f"[{SKILL}] {e}\n")
        err("Session creation failed")

    audit(conn, SKILL, "pos-open-session", "pos_session", session_id,
          new_values={"cashier_name": cashier_name, "naming_series": naming,
                      "opening_amount": opening})
    conn.commit()
    ok({"id": session_id, "naming_series": naming,
        "cashier_name": cashier_name, "opening_amount": opening,
        "session_status": "open"})


# ---------------------------------------------------------------------------
# get-session
# ---------------------------------------------------------------------------
def get_session(conn, args):
    sid = getattr(args, "id", None)
    if not sid:
        err("--id is required")

    row = conn.execute(Q.from_(Table("pos_session")).select(Table("pos_session").star).where(Field("id") == P()).get_sql(), (sid,)).fetchone()
    if not row:
        err(f"Session {sid} not found")

    data = row_to_dict(row)

    # Compute live totals from transactions. Money is exact text: fetch the
    # rows and total them in Python with Decimal rather than summing a
    # numeric cast in SQL.
    t = Table("pos_transaction")
    q = (Q.from_(t).select(t.status, t.grand_total)
         .where(t.pos_session_id == P()))
    lines = conn.execute(q.get_sql(), (sid,)).fetchall()

    data["live_transaction_count"] = len(lines)
    data["live_total_sales"] = str(_round(sum(
        (_dec(r["grand_total"]) for r in lines
         if not _is_return_document(r["status"], r["grand_total"])
         and r["status"] in ("submitted", "returned")),
        Decimal("0"))))
    data["live_total_returns"] = str(_round(abs(sum(
        (_dec(r["grand_total"]) for r in lines
         if _is_return_document(r["status"], r["grand_total"])),
        Decimal("0")))))

    # Rename status to session_status to avoid ok() overwrite
    data["session_status"] = data.pop("status", None)

    ok(data)


# ---------------------------------------------------------------------------
# list-sessions
# ---------------------------------------------------------------------------
def list_sessions(conn, args):
    s = Table("pos_session")
    p = Table("pos_profile")
    q = Q.from_(s).left_join(p).on(s.pos_profile_id == p.id).select(s.star, p.name.as_("profile_name"))
    q_cnt = Q.from_(s).select(fn.Count(s.star))
    params = []

    profile_id = getattr(args, "pos_profile_id", None)
    status = getattr(args, "status", None)
    company_id = getattr(args, "company_id", None)

    if profile_id:
        q = q.where(s.pos_profile_id == P())
        q_cnt = q_cnt.where(s.pos_profile_id == P())
        params.append(profile_id)
    if status:
        q = q.where(s.status == P())
        q_cnt = q_cnt.where(s.status == P())
        params.append(status)
    if company_id:
        q = q.where(s.company_id == P())
        q_cnt = q_cnt.where(s.company_id == P())
        params.append(company_id)

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    q = q.orderby(s.opened_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()

    sessions = []
    for r in rows:
        d = row_to_dict(r)
        d["session_status"] = d.pop("status", None)
        sessions.append(d)

    ok({"sessions": sessions, "total": total,
        "limit": limit, "offset": offset,
        "has_more": offset + limit < total})


# ---------------------------------------------------------------------------
# close-session
# ---------------------------------------------------------------------------
def close_session(conn, args):
    sid = getattr(args, "id", None)
    closing_amount = getattr(args, "closing_amount", None)

    if not sid:
        err("--id is required")
    if closing_amount is None:
        err("--closing-amount is required")

    row = conn.execute(Q.from_(Table("pos_session")).select(Table("pos_session").star).where(Field("id") == P()).get_sql(), (sid,)).fetchone()
    if not row:
        err(f"Session {sid} not found")
    if row["status"] != "open":
        err(f"Session {sid} is not open (current: {row['status']})")

    in_flight = conn.execute(Q.from_(Table("pos_transaction")).select(Field("id"), Field("status"), Field("sales_invoice_id"), Field("return_against_id")).where(Field("pos_session_id") == P()).get_sql(), (sid,)).fetchall()
    pending = sorted(r["id"] for r in in_flight if r["status"] in ("draft", "held") and r["sales_invoice_id"])
    pending = sorted(set(pending) | {r["id"] for r in in_flight if r["status"] == "draft" and r["return_against_id"]})
    voiding = [r["id"] for r in in_flight if is_cancelled_outside_pos(conn, r)]
    pending = sorted(set(pending) | set(voiding))
    if pending:
        err(f"Session {sid} has a POS action in progress ({', '.join(pending)}); finish it before closing")

    closing = _round(_dec(closing_amount))
    opening = _dec(row["opening_amount"])

    # Money is exact text: fetch the rows and total them in Python with
    # Decimal rather than summing a numeric cast in SQL.
    t = Table("pos_transaction")
    q = (Q.from_(t).select(t.status, t.grand_total, t.change_amount)
         .where(t.pos_session_id == P()))
    lines = conn.execute(q.get_sql(), (sid,)).fetchall()

    # A returned original stays a sale; only the return document is the
    # return, reported as a positive figure.
    total_sales = _round(sum(
        (_dec(r["grand_total"]) for r in lines
         if not _is_return_document(r["status"], r["grand_total"])
         and r["status"] in ("submitted", "returned")),
        Decimal("0")))
    total_returns = _round(abs(sum(
        (_dec(r["grand_total"]) for r in lines
         if _is_return_document(r["status"], r["grand_total"])),
        Decimal("0"))))
    txn_count = len(lines)

    # Also account for change given in cash on sales only.
    total_change = _round(sum(
        (_dec(r["change_amount"]) for r in lines
         if not _is_return_document(r["status"], r["grand_total"])
         and r["status"] in ("submitted", "returned")),
        Decimal("0")))

    pp = Table("pos_payment")
    pt = Table("pos_transaction")

    cq = (Q.from_(pp).join(pt).on(pp.pos_transaction_id == pt.id)
          .select(pp.amount, pt.status, pt.grand_total)
          .where(pt.pos_session_id == P())
          .where(pt.status.isin(["submitted", "returned"]))
          .where(pp.payment_method == "cash"))
    cash_rows = conn.execute(cq.get_sql(), (sid,)).fetchall()

    # Calculate cash-only sales for expected amount; a returned original
    # stays a sale and only the return document is the cash return.
    cash_in = _round(sum(
        (_dec(r["amount"]) for r in cash_rows
         if not _is_return_document(r["status"], r["grand_total"])),
        Decimal("0")))

    # Cash returns as a positive figure.
    cash_out = _round(abs(sum(
        (_dec(r["amount"]) for r in cash_rows
         if _is_return_document(r["status"], r["grand_total"])),
        Decimal("0"))))

    expected = opening + cash_in - cash_out - total_change
    expected = _round(expected)
    difference = _round(closing - expected)

    sql, upd_params = dynamic_update("pos_session", {
        "closing_amount": str(closing), "expected_amount": str(expected),
        "difference": str(difference), "total_sales": str(total_sales),
        "total_returns": str(total_returns), "transaction_count": txn_count,
        "closed_at": now(), "status": "closed",
    }, {"id": sid})
    conn.execute(sql, upd_params)

    audit(conn, SKILL, "pos-close-session", "pos_session", sid,
          new_values={"closing_amount": str(closing), "expected_amount": str(expected),
                      "difference": str(difference)})
    conn.commit()
    ok({"id": sid, "session_status": "closed",
        "closing_amount": str(closing), "expected_amount": str(expected),
        "difference": str(difference), "total_sales": str(total_sales),
        "total_returns": str(total_returns), "transaction_count": txn_count})


# ---------------------------------------------------------------------------
# Action Router
# ---------------------------------------------------------------------------
ACTIONS = {
    "pos-open-session": open_session,
    "pos-get-session": get_session,
    "pos-list-sessions": list_sessions,
    "pos-close-session": close_session,
}
