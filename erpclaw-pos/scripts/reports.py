#!/usr/bin/env python3
"""erpclaw-pos reports domain module.

POS reporting — cash reconciliation, daily reports, hourly sales breakdown,
top items, and cashier performance. Imported by the unified erpclaw-pos
db_query.py router.
"""
import os
import sys
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

import importlib.util
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row
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


def _today():
    return date.today().isoformat()


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
# cash-reconciliation
# ---------------------------------------------------------------------------
def cash_reconciliation(conn, args):
    """Cash reconciliation for a specific session."""
    session_id = getattr(args, "pos_session_id", None) or getattr(args, "id", None)
    if not session_id:
        err("--pos-session-id is required")

    session = conn.execute(Q.from_(Table("pos_session")).select(Table("pos_session").star).where(Field("id") == P()).get_sql(), (session_id,)).fetchone()
    if not session:
        err(f"Session {session_id} not found")

    opening = _dec(session["opening_amount"])

    pp = Table("pos_payment")
    pt = Table("pos_transaction")

    # Sales cancelled outside POS are voids still in progress, not live
    # sales: leave their payments out and point at pos-void-transaction.
    t = Table("pos_transaction")
    cancel_rows = conn.execute(
        Q.from_(t).select(t.id, t.status, t.sales_invoice_id)
        .where(t.pos_session_id == P())
        .where(t.status == "submitted").get_sql(), (session_id,)).fetchall()
    cancelled_ids = {r["id"] for r in cancel_rows if is_cancelled_outside_pos(conn, r)}

    # Money is exact text: fetch both statuses and classify in Python with
    # Decimal rather than summing a numeric cast in SQL. A returned original
    # stays a sale; only the return document is the return.
    cq = (Q.from_(pp).join(pt).on(pp.pos_transaction_id == pt.id)
          .select(pp.amount, pt.status, pt.grand_total,
                  pt.id.as_("pos_transaction_id"))
          .where(pt.pos_session_id == P())
          .where(pt.status.isin(["submitted", "returned"]))
          .where(pp.payment_method == "cash"))
    cash_rows = conn.execute(cq.get_sql(), (session_id,)).fetchall()
    cash_received = _round(sum(
        (_dec(r["amount"]) for r in cash_rows
         if r["pos_transaction_id"] not in cancelled_ids
         and not _is_return_document(r["status"], r["grand_total"])),
        Decimal("0")))
    cash_refunded = _round(abs(sum(
        (_dec(r["amount"]) for r in cash_rows
         if r["pos_transaction_id"] not in cancelled_ids
         and _is_return_document(r["status"], r["grand_total"])),
        Decimal("0"))))

    # Change given on sales only (a returned original stays a sale).
    q = (Q.from_(t).select(t.change_amount, t.status, t.grand_total,
                  t.id.as_("pos_transaction_id"))
         .where(t.pos_session_id == P())
         .where(t.status.isin(["submitted", "returned"])))
    change_rows = conn.execute(q.get_sql(), (session_id,)).fetchall()
    change_given = _round(sum(
        (_dec(r["change_amount"]) for r in change_rows
         if r["pos_transaction_id"] not in cancelled_ids
         and not _is_return_document(r["status"], r["grand_total"])),
        Decimal("0")))

    expected_cash = _round(opening + cash_received - cash_refunded - change_given)

    # Non-cash totals
    q = (Q.from_(pp).join(pt).on(pp.pos_transaction_id == pt.id)
         .select(pp.payment_method, pp.amount, pt.id.as_("pos_transaction_id"))
         .where(pt.pos_session_id == P())
         .where(pt.status.isin(["submitted", "returned"]))
         .where(pp.payment_method != "cash"))
    method_totals = {}
    for r in conn.execute(q.get_sql(), (session_id,)).fetchall():
        if r["pos_transaction_id"] in cancelled_ids:
            continue
        method_totals[r["payment_method"]] = (
            method_totals.get(r["payment_method"], Decimal("0")) + _dec(r["amount"]))
    non_cash_breakdown = {m: str(_round(v)) for m, v in method_totals.items()}

    closing = _dec(session["closing_amount"]) if session["closing_amount"] else None
    variance = str(_round(closing - expected_cash)) if closing is not None else None

    result = {
        "session_id": session_id,
        "session_status": session["status"],
        "cashier_name": session["cashier_name"],
        "opening_amount": str(opening),
        "cash_received": str(cash_received),
        "cash_refunded": str(cash_refunded),
        "change_given": str(change_given),
        "expected_cash": str(expected_cash),
        "closing_amount": str(closing) if closing is not None else None,
        "cash_variance": variance,
        "non_cash_breakdown": non_cash_breakdown,
    }
    if cancelled_ids:
        result["voids_to_finish"] = sorted(cancelled_ids)
        result["next_step"] = "Run pos-void-transaction --id <id> for each transaction in voids_to_finish"
    ok(result)


# ---------------------------------------------------------------------------
# daily-report
# ---------------------------------------------------------------------------
def daily_report(conn, args):
    """Sales summary for a given date across all sessions."""
    report_date = getattr(args, "date", None) or _today()
    company_id = getattr(args, "company_id", None)

    pt = Table("pos_transaction")
    pp = Table("pos_payment")

    # Sales and returns: money is exact text, so fetch both statuses and
    # classify in Python with Decimal rather than summing a numeric cast in
    # SQL. A returned original stays a sale of its day; only the return
    # document is the return, reported as a positive figure.
    q = (Q.from_(pt)
         .select(pt.grand_total, pt.discount_amount, pt.tax_amount,
                 pt.status)
         .where(fn.Date(pt.created_at) == P())
         .where(pt.status.isin(["submitted", "returned"])))
    params = [report_date]
    if company_id:
        q = q.where(pt.company_id == P())
        params.append(company_id)
    day_rows = conn.execute(q.get_sql(), params).fetchall()
    sales = [r for r in day_rows
             if not _is_return_document(r["status"], r["grand_total"])]
    returns = [r for r in day_rows
               if _is_return_document(r["status"], r["grand_total"])]
    transaction_count = len(sales)
    total_sales = _exact_total(sales, "grand_total")
    total_discounts = _exact_total(sales, "discount_amount")
    total_tax = _exact_total(sales, "tax_amount")
    return_count = len(returns)
    total_returns = sum((abs(_dec(r["grand_total"])) for r in returns), Decimal("0"))

    # Payment method breakdown on sales only, ordered by total descending.
    q = (Q.from_(pp).join(pt).on(pp.pos_transaction_id == pt.id)
         .select(pp.payment_method, pp.amount, pt.status, pt.grand_total)
         .where(fn.Date(pt.created_at) == P())
         .where(pt.status.isin(["submitted", "returned"])))
    pay_params = [report_date]
    if company_id:
        q = q.where(pt.company_id == P())
        pay_params.append(company_id)
    pay_totals = {}
    pay_counts = {}
    for r in conn.execute(q.get_sql(), pay_params).fetchall():
        if _is_return_document(r["status"], r["grand_total"]):
            continue
        method = r["payment_method"]
        pay_totals[method] = pay_totals.get(method, Decimal("0")) + _dec(r["amount"])
        pay_counts[method] = pay_counts.get(method, 0) + 1
    pay_breakdown = [
        {"method": method, "count": pay_counts[method],
         "total": str(_round(pay_totals[method]))}
        for method in sorted(pay_totals, key=lambda m: pay_totals[m], reverse=True)
    ]

    # Sessions active that day (a count, never money: unchanged query shape)
    sess_params = [report_date]
    session_company_filter = ""
    if company_id:
        session_company_filter = " AND company_id = ?"
        sess_params.append(company_id)
    sessions = conn.execute(
        f"""SELECT COUNT(*) as count
            FROM pos_session
            WHERE date(opened_at) = ?{session_company_filter}""",
        sess_params).fetchone()

    result = {
        "report_date": report_date,
        "transaction_count": transaction_count,
        "total_sales": str(_round(total_sales)),
        "total_discounts": str(_round(total_discounts)),
        "total_tax": str(_round(total_tax)),
        "return_count": return_count,
        "total_returns": str(_round(total_returns)),
        "net_sales": str(_round(total_sales - total_returns)),
        "payment_methods": pay_breakdown,
        "sessions_count": sessions["count"],
    }
    ok(result)


# ---------------------------------------------------------------------------
# hourly-sales
# ---------------------------------------------------------------------------
def hourly_sales(conn, args):
    """Hourly sales breakdown for a given date."""
    report_date = getattr(args, "date", None) or _today()
    company_id = getattr(args, "company_id", None)

    t = Table("pos_transaction")
    q = (Q.from_(t).select(t.created_at, t.grand_total, t.status)
         .where(fn.Date(t.created_at) == P())
         .where(t.status.isin(["submitted", "returned"])))
    params = [report_date]
    if company_id:
        q = q.where(t.company_id == P())
        params.append(company_id)
    lines = conn.execute(q.get_sql(), params).fetchall()

    # Bucket by hour exactly as strftime('%H', created_at) does for the ISO
    # timestamps stored here, and total each bucket in Python with Decimal
    # rather than summing a numeric cast in SQL. A returned original stays
    # a sale; only the return document is excluded here.
    buckets = {}
    for r in lines:
        if _is_return_document(r["status"], r["grand_total"]):
            continue
        created = str(r["created_at"]) if r["created_at"] else ""
        hour = created[11:13] if len(created) >= 13 else None
        bucket = buckets.setdefault(hour, [0, Decimal("0")])
        bucket[0] += 1
        bucket[1] += _dec(r["grand_total"])

    hourly = []
    total_sales = Decimal("0")
    total_txns = 0
    for hour in sorted(buckets, key=lambda h: (h is not None, h or "")):
        count, subtotal = buckets[hour]
        amt = _round(subtotal)
        total_sales += amt
        total_txns += count
        hourly.append({
            "hour": hour,
            "hour_label": f"{hour}:00-{hour}:59",
            "transaction_count": count,
            "total_sales": str(amt),
        })

    # Find peak hour
    peak = max(hourly, key=lambda x: _dec(x["total_sales"])) if hourly else None

    result = {
        "report_date": report_date,
        "hourly_breakdown": hourly,
        "total_transactions": total_txns,
        "total_sales": str(_round(total_sales)),
        "peak_hour": peak["hour_label"] if peak else None,
        "peak_hour_sales": peak["total_sales"] if peak else "0.00",
    }
    ok(result)


# ---------------------------------------------------------------------------
# top-items
# ---------------------------------------------------------------------------
def top_items(conn, args):
    """Top selling items by quantity for a date range."""
    from_date = getattr(args, "from_date", None)
    to_date = getattr(args, "to_date", None)
    company_id = getattr(args, "company_id", None)
    limit = int(getattr(args, "limit", None) or 20)

    if not from_date:
        from_date = _today()
    if not to_date:
        to_date = _today()

    ti = Table("pos_transaction_item")
    pt = Table("pos_transaction")

    def _scoped(q):
        q = (q.where(fn.Date(pt.created_at) >= P())
              .where(fn.Date(pt.created_at) <= P())
              .where(pt.status.isin(["submitted", "returned"])))
        if company_id:
            q = q.where(pt.company_id == P())
        return q

    # Fetch the lines with their transaction classification and aggregate
    # sales only in Python: a returned original stays a sale, only the
    # return document is excluded. Money is totalled with Decimal; quantity
    # is totalled with Decimal here so the return-document lines can be
    # left out without comparing money in SQL.
    rq = _scoped(
        Q.from_(ti).join(pt).on(ti.pos_transaction_id == pt.id)
        .select(ti.item_id, ti.item_name, ti.item_code, ti.qty, ti.amount,
                ti.pos_transaction_id, pt.status, pt.grand_total))
    rparams = [from_date, to_date]
    if company_id:
        rparams.append(company_id)
    qty = {}
    revenue = {}
    txn_sets = {}
    names = {}
    codes = {}
    for r in conn.execute(rq.get_sql(), rparams).fetchall():
        if _is_return_document(r["status"], r["grand_total"]):
            continue
        item = r["item_id"]
        qty[item] = qty.get(item, Decimal("0")) + _dec(r["qty"])
        revenue[item] = revenue.get(item, Decimal("0")) + _dec(r["amount"])
        txn_sets.setdefault(item, set()).add(r["pos_transaction_id"])
        if item not in names:
            names[item] = r["item_name"]
            codes[item] = r["item_code"]

    ordered = sorted(
        qty,
        key=lambda item: (-qty[item], names[item] or "", item or ""))[:limit]
    items = []
    for item in ordered:
        items.append({
            "item_id": item,
            "item_name": names[item],
            "item_code": codes[item],
            "total_qty": str(_round(qty[item])),
            "total_revenue": str(_round(revenue.get(item, Decimal("0")))),
            "transaction_count": len(txn_sets.get(item, ())),
        })

    result = {
        "from_date": from_date,
        "to_date": to_date,
        "top_items": items,
        "count": len(items),
    }
    ok(result)


# ---------------------------------------------------------------------------
# cashier-performance
# ---------------------------------------------------------------------------
def cashier_performance(conn, args):
    """Per-cashier metrics: transactions, total sales, avg transaction value."""
    from_date = getattr(args, "from_date", None)
    to_date = getattr(args, "to_date", None)
    company_id = getattr(args, "company_id", None)

    if not from_date:
        from_date = _today()
    if not to_date:
        to_date = _today()

    s = Table("pos_session")
    pt = Table("pos_transaction")

    q = (Q.from_(s).select(s.id, s.cashier_name)
         .where(fn.Date(s.opened_at) >= P())
         .where(fn.Date(s.opened_at) <= P()))
    params = [from_date, to_date]
    if company_id:
        q = q.where(s.company_id == P())
        params.append(company_id)
    sessions = conn.execute(q.get_sql(), params).fetchall()

    per_cashier = {}
    session_of = {}
    for row in sessions:
        per_cashier.setdefault(row["cashier_name"],
                              {"sessions": set(), "count": 0,
                               "total": Decimal("0")})
        per_cashier[row["cashier_name"]]["sessions"].add(row["id"])
        session_of[row["id"]] = row["cashier_name"]

    # Sales are money: fetch both statuses and classify in Python with
    # Decimal rather than summing a numeric cast in SQL. A returned original
    # stays a sale; only the return document is excluded. The fetch joins
    # the session so the date range and the company stay bound parameters
    # instead of a literal session-id list; the session query above still
    # decides session_count and keeps cashiers whose sessions have no sales.
    if session_of:
        tq = (Q.from_(pt).join(s).on(pt.pos_session_id == s.id)
              .select(pt.pos_session_id, pt.grand_total, pt.status)
              .where(pt.status.isin(["submitted", "returned"]))
              .where(fn.Date(s.opened_at) >= P())
              .where(fn.Date(s.opened_at) <= P()))
        tparams = [from_date, to_date]
        if company_id:
            tq = tq.where(s.company_id == P())
            tparams.append(company_id)
        for r in conn.execute(tq.get_sql(), tparams).fetchall():
            if _is_return_document(r["status"], r["grand_total"]):
                continue
            entry = per_cashier[session_of[r["pos_session_id"]]]
            entry["count"] += 1
            entry["total"] += _dec(r["grand_total"])

    cashiers = []
    for name, entry in sorted(per_cashier.items(),
                             key=lambda kv: kv[1]["total"], reverse=True):
        # The average is taken in Decimal from the rounded total: the database's
        # AVG is floating point and rounds a half cent the wrong way.
        total = _round(entry["total"])
        count = entry["count"]
        avg = _round(total / count) if count else _round(Decimal("0"))
        cashiers.append({
            "cashier_name": name,
            "session_count": len(entry["sessions"]),
            "transaction_count": count,
            "total_sales": str(total),
            "avg_transaction_value": str(avg),
        })

    result = {
        "from_date": from_date,
        "to_date": to_date,
        "cashiers": cashiers,
        "count": len(cashiers),
    }
    ok(result)


# ---------------------------------------------------------------------------
# Action Router
# ---------------------------------------------------------------------------
ACTIONS = {
    "pos-cash-reconciliation": cash_reconciliation,
    "pos-daily-report": daily_report,
    "pos-hourly-sales": hourly_sales,
    "pos-top-items": top_items,
    "pos-cashier-performance": cashier_performance,
}
