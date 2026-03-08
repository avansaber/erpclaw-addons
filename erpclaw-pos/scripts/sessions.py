#!/usr/bin/env python3
"""erpclaw-pos sessions domain module.

POS session lifecycle — open, close, track cash float and totals.
Imported by the unified erpclaw-pos db_query.py router.
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
    profile = conn.execute(
        "SELECT id, company_id, is_active FROM pos_profile WHERE id = ?",
        (profile_id,)).fetchone()
    if not profile:
        err(f"POS profile {profile_id} not found")
    if not profile["is_active"]:
        err(f"POS profile {profile_id} is not active")

    company_id = profile["company_id"]

    # Only one open session per profile
    existing = conn.execute(
        "SELECT id FROM pos_session WHERE pos_profile_id = ? AND status = 'open'",
        (profile_id,)).fetchone()
    if existing:
        err(f"Profile {profile_id} already has an open session: {existing['id']}")

    opening = str(_round(_dec(opening_amount)))
    session_id = str(uuid.uuid4())
    naming = get_next_name(conn, "pos_session", company_id=company_id)

    try:
        conn.execute(
            """INSERT INTO pos_session
               (id, naming_series, pos_profile_id, cashier_name, opening_amount,
                total_sales, total_returns, transaction_count, status, company_id)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (session_id, naming, profile_id, cashier_name, opening,
             "0", "0", 0, "open", company_id))
    except sqlite3.IntegrityError as e:
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

    row = conn.execute("SELECT * FROM pos_session WHERE id = ?", (sid,)).fetchone()
    if not row:
        err(f"Session {sid} not found")

    data = row_to_dict(row)

    # Compute live totals from transactions
    stats = conn.execute(
        """SELECT
             COUNT(*) as txn_count,
             COALESCE(SUM(CASE WHEN status = 'submitted' THEN CAST(grand_total AS REAL) ELSE 0 END), 0) as live_sales,
             COALESCE(SUM(CASE WHEN status = 'returned' THEN CAST(grand_total AS REAL) ELSE 0 END), 0) as live_returns
           FROM pos_transaction WHERE pos_session_id = ?""",
        (sid,)).fetchone()

    data["live_transaction_count"] = stats["txn_count"]
    data["live_total_sales"] = str(_round(_dec(stats["live_sales"])))
    data["live_total_returns"] = str(_round(_dec(stats["live_returns"])))

    # Rename status to session_status to avoid ok() overwrite
    data["session_status"] = data.pop("status", None)

    ok(data)


# ---------------------------------------------------------------------------
# list-sessions
# ---------------------------------------------------------------------------
def list_sessions(conn, args):
    params = []
    where = ["1=1"]

    profile_id = getattr(args, "pos_profile_id", None)
    status = getattr(args, "status", None)
    company_id = getattr(args, "company_id", None)

    if profile_id:
        where.append("s.pos_profile_id = ?"); params.append(profile_id)
    if status:
        where.append("s.status = ?"); params.append(status)
    if company_id:
        where.append("s.company_id = ?"); params.append(company_id)

    where_clause = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM pos_session s WHERE {where_clause}",
        params).fetchone()[0]

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    rows = conn.execute(
        f"""SELECT s.*, p.name as profile_name
            FROM pos_session s
            LEFT JOIN pos_profile p ON s.pos_profile_id = p.id
            WHERE {where_clause}
            ORDER BY s.opened_at DESC LIMIT ? OFFSET ?""",
        params + [limit, offset]).fetchall()

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

    row = conn.execute("SELECT * FROM pos_session WHERE id = ?", (sid,)).fetchone()
    if not row:
        err(f"Session {sid} not found")
    if row["status"] != "open":
        err(f"Session {sid} is not open (current: {row['status']})")

    closing = _round(_dec(closing_amount))
    opening = _dec(row["opening_amount"])

    # Calculate totals from submitted transactions
    stats = conn.execute(
        """SELECT
             COUNT(*) as txn_count,
             COALESCE(SUM(CASE WHEN status = 'submitted' THEN CAST(grand_total AS REAL) ELSE 0 END), 0) as total_sales,
             COALESCE(SUM(CASE WHEN status = 'returned' THEN CAST(grand_total AS REAL) ELSE 0 END), 0) as total_returns
           FROM pos_transaction WHERE pos_session_id = ?""",
        (sid,)).fetchone()

    total_sales = _round(_dec(stats["total_sales"]))
    total_returns = _round(_dec(stats["total_returns"]))
    txn_count = stats["txn_count"]

    # Calculate cash-only sales for expected amount
    cash_stats = conn.execute(
        """SELECT COALESCE(SUM(CAST(pp.amount AS REAL)), 0) as cash_total
           FROM pos_payment pp
           JOIN pos_transaction pt ON pp.pos_transaction_id = pt.id
           WHERE pt.pos_session_id = ? AND pt.status = 'submitted'
             AND pp.payment_method = 'cash'""",
        (sid,)).fetchone()
    cash_in = _round(_dec(cash_stats["cash_total"]))

    # Cash returns
    cash_returns = conn.execute(
        """SELECT COALESCE(SUM(CAST(pp.amount AS REAL)), 0) as cash_return
           FROM pos_payment pp
           JOIN pos_transaction pt ON pp.pos_transaction_id = pt.id
           WHERE pt.pos_session_id = ? AND pt.status = 'returned'
             AND pp.payment_method = 'cash'""",
        (sid,)).fetchone()
    cash_out = _round(_dec(cash_returns["cash_return"]))

    # Also account for change given in cash
    change_given = conn.execute(
        """SELECT COALESCE(SUM(CAST(change_amount AS REAL)), 0) as total_change
           FROM pos_transaction
           WHERE pos_session_id = ? AND status = 'submitted'""",
        (sid,)).fetchone()
    total_change = _round(_dec(change_given["total_change"]))

    expected = opening + cash_in - abs(cash_out) - total_change
    expected = _round(expected)
    difference = _round(closing - expected)

    conn.execute(
        """UPDATE pos_session
           SET closing_amount = ?, expected_amount = ?, difference = ?,
               total_sales = ?, total_returns = ?, transaction_count = ?,
               closed_at = datetime('now'), status = 'closed'
           WHERE id = ?""",
        (str(closing), str(expected), str(difference),
         str(total_sales), str(total_returns), txn_count, sid))

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
