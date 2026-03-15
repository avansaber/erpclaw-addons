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

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row

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

    # Cash received from submitted transactions
    cash_in = conn.execute(
        """SELECT COALESCE(SUM(CAST(pp.amount AS REAL)), 0) as total
           FROM pos_payment pp
           JOIN pos_transaction pt ON pp.pos_transaction_id = pt.id
           WHERE pt.pos_session_id = ? AND pt.status = 'submitted'
             AND pp.payment_method = 'cash'""",
        (session_id,)).fetchone()
    cash_received = _round(_dec(cash_in["total"]))

    # Cash refunded from returned transactions
    cash_out = conn.execute(
        """SELECT COALESCE(SUM(CAST(pp.amount AS REAL)), 0) as total
           FROM pos_payment pp
           JOIN pos_transaction pt ON pp.pos_transaction_id = pt.id
           WHERE pt.pos_session_id = ? AND pt.status = 'returned'
             AND pp.payment_method = 'cash'""",
        (session_id,)).fetchone()
    cash_refunded = _round(abs(_dec(cash_out["total"])))

    # Change given
    change = conn.execute(
        """SELECT COALESCE(SUM(CAST(change_amount AS REAL)), 0) as total
           FROM pos_transaction
           WHERE pos_session_id = ? AND status = 'submitted'""",
        (session_id,)).fetchone()
    change_given = _round(_dec(change["total"]))

    expected_cash = _round(opening + cash_received - cash_refunded - change_given)

    # Non-cash totals
    non_cash = conn.execute(
        """SELECT pp.payment_method,
                  COALESCE(SUM(CAST(pp.amount AS REAL)), 0) as total
           FROM pos_payment pp
           JOIN pos_transaction pt ON pp.pos_transaction_id = pt.id
           WHERE pt.pos_session_id = ? AND pt.status IN ('submitted', 'returned')
             AND pp.payment_method != 'cash'
           GROUP BY pp.payment_method""",
        (session_id,)).fetchall()
    non_cash_breakdown = {r["payment_method"]: str(_round(_dec(r["total"]))) for r in non_cash}

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
    ok(result)


# ---------------------------------------------------------------------------
# daily-report
# ---------------------------------------------------------------------------
def daily_report(conn, args):
    """Sales summary for a given date across all sessions."""
    report_date = getattr(args, "date", None) or _today()
    company_id = getattr(args, "company_id", None)

    params = [report_date]
    company_filter = ""
    if company_id:
        company_filter = " AND pt.company_id = ?"
        params.append(company_id)

    # Total sales
    sales = conn.execute(
        f"""SELECT
              COUNT(*) as transaction_count,
              COALESCE(SUM(CAST(pt.grand_total AS REAL)), 0) as total_sales,
              COALESCE(SUM(CAST(pt.discount_amount AS REAL)), 0) as total_discounts,
              COALESCE(SUM(CAST(pt.tax_amount AS REAL)), 0) as total_tax
            FROM pos_transaction pt
            WHERE date(pt.created_at) = ? AND pt.status = 'submitted'{company_filter}""",
        params).fetchone()

    # Returns
    return_params = [report_date]
    if company_id:
        return_params.append(company_id)
    returns = conn.execute(
        f"""SELECT
              COUNT(*) as return_count,
              COALESCE(SUM(CAST(ABS(CAST(pt.grand_total AS REAL)) AS REAL)), 0) as total_returns
            FROM pos_transaction pt
            WHERE date(pt.created_at) = ? AND pt.status = 'returned'{company_filter}""",
        return_params).fetchone()

    # Payment method breakdown
    pay_params = [report_date]
    if company_id:
        pay_params.append(company_id)
    pay_breakdown = conn.execute(
        f"""SELECT pp.payment_method,
                   COUNT(*) as count,
                   COALESCE(SUM(CAST(pp.amount AS REAL)), 0) as total
            FROM pos_payment pp
            JOIN pos_transaction pt ON pp.pos_transaction_id = pt.id
            WHERE date(pt.created_at) = ? AND pt.status = 'submitted'{company_filter}
            GROUP BY pp.payment_method
            ORDER BY total DESC""",
        pay_params).fetchall()

    # Sessions active that day
    sess_params = [report_date]
    if company_id:
        sess_params.append(company_id)
    sessions = conn.execute(
        f"""SELECT COUNT(*) as count
            FROM pos_session
            WHERE date(opened_at) = ?{company_filter.replace('pt.', '')}""",
        sess_params).fetchone()

    result = {
        "report_date": report_date,
        "transaction_count": sales["transaction_count"],
        "total_sales": str(_round(_dec(sales["total_sales"]))),
        "total_discounts": str(_round(_dec(sales["total_discounts"]))),
        "total_tax": str(_round(_dec(sales["total_tax"]))),
        "return_count": returns["return_count"],
        "total_returns": str(_round(_dec(returns["total_returns"]))),
        "net_sales": str(_round(_dec(sales["total_sales"]) - _dec(returns["total_returns"]))),
        "payment_methods": [
            {"method": r["payment_method"],
             "count": r["count"],
             "total": str(_round(_dec(r["total"])))}
            for r in pay_breakdown
        ],
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

    params = [report_date]
    company_filter = ""
    if company_id:
        company_filter = " AND company_id = ?"
        params.append(company_id)

    rows = conn.execute(
        f"""SELECT
              strftime('%H', created_at) as hour,
              COUNT(*) as transaction_count,
              COALESCE(SUM(CAST(grand_total AS REAL)), 0) as total_sales
            FROM pos_transaction
            WHERE date(created_at) = ? AND status = 'submitted'{company_filter}
            GROUP BY strftime('%H', created_at)
            ORDER BY hour""",
        params).fetchall()

    hourly = []
    total_sales = Decimal("0")
    total_txns = 0
    for r in rows:
        amt = _round(_dec(r["total_sales"]))
        total_sales += amt
        total_txns += r["transaction_count"]
        hourly.append({
            "hour": r["hour"],
            "hour_label": f"{r['hour']}:00-{r['hour']}:59",
            "transaction_count": r["transaction_count"],
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

    params = [from_date, to_date]
    company_filter = ""
    if company_id:
        company_filter = " AND pt.company_id = ?"
        params.append(company_id)

    rows = conn.execute(
        f"""SELECT
              ti.item_id, ti.item_name, ti.item_code,
              SUM(CAST(ti.qty AS REAL)) as total_qty,
              SUM(CAST(ti.amount AS REAL)) as total_revenue,
              COUNT(DISTINCT ti.pos_transaction_id) as transaction_count
            FROM pos_transaction_item ti
            JOIN pos_transaction pt ON ti.pos_transaction_id = pt.id
            WHERE date(pt.created_at) BETWEEN ? AND ?
              AND pt.status = 'submitted'{company_filter}
            GROUP BY ti.item_id
            ORDER BY total_qty DESC
            LIMIT ?""",
        params + [limit]).fetchall()

    items = []
    for r in rows:
        items.append({
            "item_id": r["item_id"],
            "item_name": r["item_name"],
            "item_code": r["item_code"],
            "total_qty": str(_round(_dec(r["total_qty"]))),
            "total_revenue": str(_round(_dec(r["total_revenue"]))),
            "transaction_count": r["transaction_count"],
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

    params = [from_date, to_date]
    company_filter = ""
    if company_id:
        company_filter = " AND s.company_id = ?"
        params.append(company_id)

    rows = conn.execute(
        f"""SELECT
              s.cashier_name,
              COUNT(DISTINCT s.id) as session_count,
              COUNT(pt.id) as transaction_count,
              COALESCE(SUM(CAST(pt.grand_total AS REAL)), 0) as total_sales,
              COALESCE(AVG(CAST(pt.grand_total AS REAL)), 0) as avg_transaction
            FROM pos_session s
            LEFT JOIN pos_transaction pt
              ON pt.pos_session_id = s.id AND pt.status = 'submitted'
            WHERE date(s.opened_at) BETWEEN ? AND ?{company_filter}
            GROUP BY s.cashier_name
            ORDER BY total_sales DESC""",
        params).fetchall()

    cashiers = []
    for r in rows:
        cashiers.append({
            "cashier_name": r["cashier_name"],
            "session_count": r["session_count"],
            "transaction_count": r["transaction_count"],
            "total_sales": str(_round(_dec(r["total_sales"]))),
            "avg_transaction_value": str(_round(_dec(r["avg_transaction"]))),
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
