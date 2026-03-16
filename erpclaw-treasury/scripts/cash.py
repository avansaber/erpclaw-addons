"""ERPClaw Treasury -- Cash Management domain module.

Bank accounts, cash positions, cash forecasts, and cash reports.
17 actions exported via ACTIONS dict.
"""
import os
import sys
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name, register_prefix
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, LiteralValue, insert_row, update_row, dynamic_update

register_prefix("bank_account_extended", "BACC-")
register_prefix("cash_forecast", "CFST-")

SKILL = "erpclaw-treasury"

VALID_ACCOUNT_TYPES = (
    "checking", "savings", "money_market", "cd", "line_of_credit", "other",
)
VALID_FORECAST_TYPES = ("short_term", "medium_term", "long_term")


# ---------------------------------------------------------------------------
# add-bank-account
# ---------------------------------------------------------------------------
def add_bank_account(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "bank_name", None):
        err("--bank-name is required")
    if not getattr(args, "account_name", None):
        err("--account-name is required")

    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    account_type = getattr(args, "account_type", None) or "checking"
    if account_type not in VALID_ACCOUNT_TYPES:
        err(f"Invalid account-type: {account_type}")

    currency = getattr(args, "currency", None) or "USD"
    current_balance = getattr(args, "current_balance", None) or "0"

    # Validate balance is valid decimal
    try:
        Decimal(current_balance)
    except Exception:
        err(f"Invalid current-balance: {current_balance}")

    acct_id = str(uuid.uuid4())
    ns = get_next_name(conn, "bank_account_extended", company_id=args.company_id)

    sql, _ = insert_row("bank_account_extended", {
        "id": P(), "naming_series": P(), "bank_name": P(), "account_name": P(),
        "account_number": P(), "routing_number": P(), "account_type": P(),
        "currency": P(), "current_balance": P(), "gl_account_id": P(),
        "is_active": P(), "notes": P(), "company_id": P(),
    })
    conn.execute(sql, (
        acct_id, ns, args.bank_name, args.account_name,
        getattr(args, "account_number", None),
        getattr(args, "routing_number", None),
        account_type, currency, current_balance,
        getattr(args, "gl_account_id", None),
        1,
        getattr(args, "notes", None),
        args.company_id,
    ))
    audit(conn, SKILL, "treasury-add-bank-account", "bank_account_extended", acct_id,
          new_values={"bank_name": args.bank_name, "naming_series": ns})
    conn.commit()
    ok({"account_id": acct_id, "naming_series": ns, "account_type": account_type})


# ---------------------------------------------------------------------------
# update-bank-account
# ---------------------------------------------------------------------------
def update_bank_account(conn, args):
    acct_id = getattr(args, "account_id", None)
    if not acct_id:
        err("--account-id is required")
    row = conn.execute(Q.from_(Table("bank_account_extended")).select(Table("bank_account_extended").star).where(Field("id") == P()).get_sql(), (acct_id,)).fetchone()
    if not row:
        err(f"Bank account {acct_id} not found")

    data, changed = {}, []

    for field, attr in [
        ("bank_name", "bank_name"),
        ("account_name", "account_name"),
        ("account_number", "account_number"),
        ("routing_number", "routing_number"),
        ("currency", "currency"),
        ("gl_account_id", "gl_account_id"),
        ("notes", "notes"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            data[field] = val
            changed.append(field)

    at = getattr(args, "account_type", None)
    if at is not None:
        if at not in VALID_ACCOUNT_TYPES:
            err(f"Invalid account-type: {at}")
        data["account_type"] = at
        changed.append("account_type")

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        if is_active not in ("0", "1"):
            err("--is-active must be 0 or 1")
        data["is_active"] = int(is_active)
        changed.append("is_active")

    if not changed:
        err("No fields to update")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("bank_account_extended", data, {"id": acct_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "treasury-update-bank-account", "bank_account_extended", acct_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"account_id": acct_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# get-bank-account
# ---------------------------------------------------------------------------
def get_bank_account(conn, args):
    acct_id = getattr(args, "account_id", None)
    if not acct_id:
        err("--account-id is required")
    row = conn.execute(Q.from_(Table("bank_account_extended")).select(Table("bank_account_extended").star).where(Field("id") == P()).get_sql(), (acct_id,)).fetchone()
    if not row:
        err(f"Bank account {acct_id} not found")
    data = row_to_dict(row)
    ok(data)


# ---------------------------------------------------------------------------
# list-bank-accounts
# ---------------------------------------------------------------------------
def list_bank_accounts(conn, args):
    t = Table("bank_account_extended")
    params = []

    q = Q.from_(t).select(t.star)
    cq = Q.from_(t).select(fn.Count("*").as_("cnt"))

    company_id = getattr(args, "company_id", None)
    if company_id:
        q = q.where(t.company_id == P())
        cq = cq.where(t.company_id == P())
        params.append(company_id)

    account_type = getattr(args, "account_type", None)
    if account_type:
        q = q.where(t.account_type == P())
        cq = cq.where(t.account_type == P())
        params.append(account_type)

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        q = q.where(t.is_active == P())
        cq = cq.where(t.is_active == P())
        params.append(int(is_active))

    search = getattr(args, "search", None)
    if search:
        search_crit = (t.bank_name.like(P())) | (t.account_name.like(P()))
        q = q.where(search_crit)
        cq = cq.where(search_crit)
        params.extend([f"%{search}%", f"%{search}%"])

    total = conn.execute(cq.get_sql(), params).fetchone()["cnt"]

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()
    ok({"accounts": [row_to_dict(r) for r in rows], "total_count": total})


# ---------------------------------------------------------------------------
# record-bank-balance
# ---------------------------------------------------------------------------
def record_bank_balance(conn, args):
    """Update bank account balance and create a cash_position snapshot."""
    acct_id = getattr(args, "account_id", None)
    if not acct_id:
        err("--account-id is required")
    balance = getattr(args, "current_balance", None)
    if not balance:
        err("--current-balance is required")

    try:
        Decimal(balance)
    except Exception:
        err(f"Invalid current-balance: {balance}")

    row = conn.execute(Q.from_(Table("bank_account_extended")).select(Table("bank_account_extended").star).where(Field("id") == P()).get_sql(), (acct_id,)).fetchone()
    if not row:
        err(f"Bank account {acct_id} not found")

    company_id = row_to_dict(row)["company_id"]

    sql = update_row("bank_account_extended",
        data={"current_balance": P(), "last_reconciled_date": LiteralValue("date('now')"),
              "updated_at": LiteralValue("datetime('now')")},
        where={"id": P()})
    conn.execute(sql, (balance, acct_id))

    # Create a cash_position snapshot
    pos_id = str(uuid.uuid4())
    ns = get_next_name(conn, "cash_position", company_id=company_id)
    total_cash = balance  # simplified: snapshot from this balance update

    sql, _ = insert_row("cash_position", {
        "id": P(), "naming_series": P(), "position_date": LiteralValue("date('now')"),
        "total_cash": P(), "total_receivables": P(), "total_payables": P(),
        "net_position": P(), "notes": P(), "company_id": P(),
    })
    conn.execute(sql, (
        pos_id, ns, total_cash, "0", "0", total_cash,
        f"Balance recorded for bank account {acct_id}",
        company_id,
    ))
    audit(conn, SKILL, "treasury-record-bank-balance", "bank_account_extended", acct_id,
          new_values={"balance": balance, "position_id": pos_id})
    conn.commit()
    ok({"account_id": acct_id, "new_balance": balance, "position_id": pos_id,
        "naming_series": ns})


# ---------------------------------------------------------------------------
# add-cash-position
# ---------------------------------------------------------------------------
def add_cash_position(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    total_cash = getattr(args, "total_cash", None) or "0"
    total_recv = getattr(args, "total_receivables", None) or "0"
    total_pay = getattr(args, "total_payables", None) or "0"

    for label, val in [("total-cash", total_cash), ("total-receivables", total_recv),
                       ("total-payables", total_pay)]:
        try:
            Decimal(val)
        except Exception:
            err(f"Invalid {label}: {val}")

    net = Decimal(total_cash) + Decimal(total_recv) - Decimal(total_pay)

    pos_id = str(uuid.uuid4())
    ns = get_next_name(conn, "cash_position", company_id=args.company_id)

    position_date = getattr(args, "position_date", None) or date.today().isoformat()

    sql, _ = insert_row("cash_position", {"id": P(), "naming_series": P(), "position_date": P(), "total_cash": P(), "total_receivables": P(), "total_payables": P(), "net_position": P(), "notes": P(), "company_id": P()})
    conn.execute(sql,
        (
            pos_id, ns, position_date, total_cash, total_recv, total_pay,
            str(net), getattr(args, "notes", None), args.company_id,
        ),
    )
    audit(conn, SKILL, "treasury-add-cash-position", "cash_position", pos_id,
          new_values={"naming_series": ns, "net_position": str(net)})
    conn.commit()
    ok({"position_id": pos_id, "naming_series": ns, "net_position": str(net)})


# ---------------------------------------------------------------------------
# list-cash-positions
# ---------------------------------------------------------------------------
def list_cash_positions(conn, args):
    t = Table("cash_position")
    params = []

    q = Q.from_(t).select(t.star)
    cq = Q.from_(t).select(fn.Count("*").as_("cnt"))

    company_id = getattr(args, "company_id", None)
    if company_id:
        q = q.where(t.company_id == P())
        cq = cq.where(t.company_id == P())
        params.append(company_id)

    total = conn.execute(cq.get_sql(), params).fetchone()["cnt"]

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.orderby(t.position_date, order=Order.desc).orderby(t.created_at, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()
    ok({"positions": [row_to_dict(r) for r in rows], "total_count": total})


# ---------------------------------------------------------------------------
# get-cash-position
# ---------------------------------------------------------------------------
def get_cash_position(conn, args):
    pos_id = getattr(args, "position_id", None)
    if not pos_id:
        err("--position-id is required")
    row = conn.execute(Q.from_(Table("cash_position")).select(Table("cash_position").star).where(Field("id") == P()).get_sql(), (pos_id,)).fetchone()
    if not row:
        err(f"Cash position {pos_id} not found")
    ok(row_to_dict(row))


# ---------------------------------------------------------------------------
# add-cash-forecast
# ---------------------------------------------------------------------------
def add_cash_forecast(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "forecast_name", None):
        err("--forecast-name is required")
    if not getattr(args, "period_start", None):
        err("--period-start is required")
    if not getattr(args, "period_end", None):
        err("--period-end is required")

    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    forecast_type = getattr(args, "forecast_type", None) or "short_term"
    if forecast_type not in VALID_FORECAST_TYPES:
        err(f"Invalid forecast-type: {forecast_type}")

    inflows = getattr(args, "expected_inflows", None) or "0"
    outflows = getattr(args, "expected_outflows", None) or "0"
    for label, val in [("expected-inflows", inflows), ("expected-outflows", outflows)]:
        try:
            Decimal(val)
        except Exception:
            err(f"Invalid {label}: {val}")

    net = Decimal(inflows) - Decimal(outflows)

    fc_id = str(uuid.uuid4())
    ns = get_next_name(conn, "cash_forecast", company_id=args.company_id)

    sql, _ = insert_row("cash_forecast", {
        "id": P(), "naming_series": P(), "forecast_name": P(),
        "forecast_date": LiteralValue("date('now')"), "period_start": P(),
        "period_end": P(), "expected_inflows": P(), "expected_outflows": P(),
        "net_forecast": P(), "assumptions": P(), "forecast_type": P(), "company_id": P(),
    })
    conn.execute(sql, (
        fc_id, ns, args.forecast_name, args.period_start, args.period_end,
        inflows, outflows, str(net),
        getattr(args, "assumptions", None),
        forecast_type, args.company_id,
    ))
    audit(conn, SKILL, "treasury-add-cash-forecast", "cash_forecast", fc_id,
          new_values={"naming_series": ns, "forecast_name": args.forecast_name})
    conn.commit()
    ok({"forecast_id": fc_id, "naming_series": ns, "net_forecast": str(net)})


# ---------------------------------------------------------------------------
# update-cash-forecast
# ---------------------------------------------------------------------------
def update_cash_forecast(conn, args):
    fc_id = getattr(args, "forecast_id", None)
    if not fc_id:
        err("--forecast-id is required")
    row = conn.execute(Q.from_(Table("cash_forecast")).select(Table("cash_forecast").star).where(Field("id") == P()).get_sql(), (fc_id,)).fetchone()
    if not row:
        err(f"Cash forecast {fc_id} not found")

    upd_data, changed = {}, []

    for field, attr in [
        ("forecast_name", "forecast_name"),
        ("period_start", "period_start"),
        ("period_end", "period_end"),
        ("assumptions", "assumptions"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            upd_data[field] = val
            changed.append(field)

    ft = getattr(args, "forecast_type", None)
    if ft is not None:
        if ft not in VALID_FORECAST_TYPES:
            err(f"Invalid forecast-type: {ft}")
        upd_data["forecast_type"] = ft
        changed.append("forecast_type")

    # Recalculate net if inflows/outflows changed
    data = row_to_dict(row)
    inflows = getattr(args, "expected_inflows", None)
    outflows = getattr(args, "expected_outflows", None)

    if inflows is not None:
        try:
            Decimal(inflows)
        except Exception:
            err(f"Invalid expected-inflows: {inflows}")
        upd_data["expected_inflows"] = inflows
        changed.append("expected_inflows")
    else:
        inflows = data["expected_inflows"]

    if outflows is not None:
        try:
            Decimal(outflows)
        except Exception:
            err(f"Invalid expected-outflows: {outflows}")
        upd_data["expected_outflows"] = outflows
        changed.append("expected_outflows")
    else:
        outflows = data["expected_outflows"]

    if "expected_inflows" in changed or "expected_outflows" in changed:
        net = Decimal(inflows) - Decimal(outflows)
        upd_data["net_forecast"] = str(net)

    if not changed:
        err("No fields to update")

    upd_data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("cash_forecast", upd_data, {"id": fc_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "treasury-update-cash-forecast", "cash_forecast", fc_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"forecast_id": fc_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# list-cash-forecasts
# ---------------------------------------------------------------------------
def list_cash_forecasts(conn, args):
    t = Table("cash_forecast")
    params = []

    q = Q.from_(t).select(t.star)
    cq = Q.from_(t).select(fn.Count("*").as_("cnt"))

    company_id = getattr(args, "company_id", None)
    if company_id:
        q = q.where(t.company_id == P())
        cq = cq.where(t.company_id == P())
        params.append(company_id)

    forecast_type = getattr(args, "forecast_type", None)
    if forecast_type:
        q = q.where(t.forecast_type == P())
        cq = cq.where(t.forecast_type == P())
        params.append(forecast_type)

    search = getattr(args, "search", None)
    if search:
        q = q.where(t.forecast_name.like(P()))
        cq = cq.where(t.forecast_name.like(P()))
        params.append(f"%{search}%")

    total = conn.execute(cq.get_sql(), params).fetchone()["cnt"]

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.orderby(t.forecast_date, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()
    ok({"forecasts": [row_to_dict(r) for r in rows], "total_count": total})


# ---------------------------------------------------------------------------
# get-cash-forecast
# ---------------------------------------------------------------------------
def get_cash_forecast(conn, args):
    fc_id = getattr(args, "forecast_id", None)
    if not fc_id:
        err("--forecast-id is required")
    row = conn.execute(Q.from_(Table("cash_forecast")).select(Table("cash_forecast").star).where(Field("id") == P()).get_sql(), (fc_id,)).fetchone()
    if not row:
        err(f"Cash forecast {fc_id} not found")
    ok(row_to_dict(row))


# ---------------------------------------------------------------------------
# generate-cash-forecast
# ---------------------------------------------------------------------------
def generate_cash_forecast(conn, args):
    """Auto-generate a forecast based on recent cash positions."""
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    forecast_type = getattr(args, "forecast_type", None) or "short_term"
    if forecast_type not in VALID_FORECAST_TYPES:
        err(f"Invalid forecast-type: {forecast_type}")

    # Get recent cash positions to compute averages
    cp = Table("cash_position")
    q = (Q.from_(cp)
         .select(cp.total_cash, cp.total_receivables, cp.total_payables, cp.net_position)
         .where(cp.company_id == P())
         .orderby(cp.position_date, order=Order.desc)
         .limit(10))
    rows = conn.execute(q.get_sql(), (args.company_id,)).fetchall()

    if not rows:
        err("No cash positions found to generate forecast. Create positions first.")

    total_cash_sum = Decimal("0")
    total_recv_sum = Decimal("0")
    total_pay_sum = Decimal("0")
    count = Decimal(str(len(rows)))

    for r in rows:
        d = row_to_dict(r)
        total_cash_sum += Decimal(d["total_cash"] or "0")
        total_recv_sum += Decimal(d["total_receivables"] or "0")
        total_pay_sum += Decimal(d["total_payables"] or "0")

    avg_inflows = (total_cash_sum + total_recv_sum) / count
    avg_outflows = total_pay_sum / count
    net = avg_inflows - avg_outflows

    # Determine period based on forecast type
    today = date.today()
    if forecast_type == "short_term":
        period_end = today + timedelta(days=30)
    elif forecast_type == "medium_term":
        period_end = today + timedelta(days=90)
    else:
        period_end = today + timedelta(days=365)

    fc_id = str(uuid.uuid4())
    ns = get_next_name(conn, "cash_forecast", company_id=args.company_id)
    forecast_name = getattr(args, "forecast_name", None) or f"Auto-forecast ({forecast_type})"

    avg_inflows_str = str(avg_inflows.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    avg_outflows_str = str(avg_outflows.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    net_str = str(net.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    sql, _ = insert_row("cash_forecast", {
        "id": P(), "naming_series": P(), "forecast_name": P(),
        "forecast_date": LiteralValue("date('now')"), "period_start": P(),
        "period_end": P(), "expected_inflows": P(), "expected_outflows": P(),
        "net_forecast": P(), "assumptions": P(), "forecast_type": P(), "company_id": P(),
    })
    conn.execute(sql, (
        fc_id, ns, forecast_name, today.isoformat(), period_end.isoformat(),
        avg_inflows_str, avg_outflows_str, net_str,
        f"Auto-generated from {len(rows)} recent cash positions",
        forecast_type, args.company_id,
    ))
    audit(conn, SKILL, "treasury-generate-cash-forecast", "cash_forecast", fc_id,
          new_values={"naming_series": ns, "positions_analyzed": len(rows)})
    conn.commit()
    ok({
        "forecast_id": fc_id, "naming_series": ns,
        "expected_inflows": avg_inflows_str,
        "expected_outflows": avg_outflows_str,
        "net_forecast": net_str,
        "positions_analyzed": len(rows),
    })


# ---------------------------------------------------------------------------
# cash-dashboard
# ---------------------------------------------------------------------------
def cash_dashboard(conn, args):
    """Summary: total cash across all bank accounts, receivables, payables, net."""
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    # Total cash from active bank accounts (read individual balances for Decimal precision)
    ba = Table("bank_account_extended")
    accts = conn.execute(
        Q.from_(ba).select(ba.current_balance)
        .where(ba.company_id == P()).where(ba.is_active == 1).get_sql(),
        (args.company_id,),
    ).fetchall()
    total_cash = Decimal("0")
    for a in accts:
        total_cash += Decimal(a[0] or "0")

    # Most recent cash position for receivables/payables
    cp = Table("cash_position")
    pos = conn.execute(
        Q.from_(cp).select(cp.total_receivables, cp.total_payables)
        .where(cp.company_id == P())
        .orderby(cp.position_date, order=Order.desc).limit(1).get_sql(),
        (args.company_id,),
    ).fetchone()
    total_recv = Decimal(pos[0] or "0") if pos else Decimal("0")
    total_pay = Decimal(pos[1] or "0") if pos else Decimal("0")
    net = total_cash + total_recv - total_pay

    # Count active bank accounts
    acct_count = conn.execute(
        Q.from_(ba).select(fn.Count("*"))
        .where(ba.company_id == P()).where(ba.is_active == 1).get_sql(),
        (args.company_id,),
    ).fetchone()[0]

    # Active investments
    inv = Table("investment")
    inv_count = conn.execute(
        Q.from_(inv).select(fn.Count("*"))
        .where(inv.company_id == P()).where(inv.status == "active").get_sql(),
        (args.company_id,),
    ).fetchone()[0]

    ok({
        "total_cash": str(total_cash),
        "total_receivables": str(total_recv),
        "total_payables": str(total_pay),
        "net_position": str(net),
        "active_bank_accounts": acct_count,
        "active_investments": inv_count,
    })


# ---------------------------------------------------------------------------
# bank-summary-report
# ---------------------------------------------------------------------------
def bank_summary_report(conn, args):
    """Summary of all bank accounts with balances."""
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    ba = Table("bank_account_extended")
    q = (Q.from_(ba)
         .select(ba.id, ba.naming_series, ba.bank_name, ba.account_name,
                 ba.account_type, ba.currency, ba.current_balance,
                 ba.is_active, ba.last_reconciled_date)
         .where(ba.company_id == P())
         .orderby(ba.bank_name).orderby(ba.account_name))
    rows = conn.execute(q.get_sql(), (args.company_id,)).fetchall()

    accounts = [row_to_dict(r) for r in rows]
    total_balance = Decimal("0")
    active_count = 0
    for a in accounts:
        if a["is_active"]:
            total_balance += Decimal(a["current_balance"] or "0")
            active_count += 1

    ok({
        "accounts": accounts,
        "total_accounts": len(accounts),
        "active_accounts": active_count,
        "total_balance": str(total_balance),
    })


# ---------------------------------------------------------------------------
# liquidity-report
# ---------------------------------------------------------------------------
def liquidity_report(conn, args):
    """Report on liquid assets: checking/savings accounts + short-term investments."""
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    # Liquid bank accounts (checking, savings, money_market)
    ba = Table("bank_account_extended")
    q = (Q.from_(ba)
         .select(ba.id, ba.bank_name, ba.account_name, ba.account_type, ba.current_balance)
         .where(ba.company_id == P())
         .where(ba.is_active == 1)
         .where(ba.account_type.isin(["checking", "savings", "money_market"]))
         .orderby(ba.current_balance, order=Order.desc))
    liquid_accts = conn.execute(q.get_sql(), (args.company_id,)).fetchall()

    liquid_total = Decimal("0")
    liquid_list = []
    for r in liquid_accts:
        d = row_to_dict(r)
        liquid_total += Decimal(d["current_balance"] or "0")
        liquid_list.append(d)

    # Short-term investments (active, maturing within 90 days)
    cutoff = (date.today() + timedelta(days=90)).isoformat()
    inv_t = Table("investment")
    q = (Q.from_(inv_t)
         .select(inv_t.id, inv_t.name, inv_t.investment_type,
                 inv_t.current_value, inv_t.maturity_date)
         .where(inv_t.company_id == P())
         .where(inv_t.status == "active")
         .where(inv_t.maturity_date.isnotnull())
         .where(inv_t.maturity_date <= P())
         .orderby(inv_t.maturity_date))
    short_inv = conn.execute(q.get_sql(), (args.company_id, cutoff)).fetchall()

    inv_total = Decimal("0")
    inv_list = []
    for r in short_inv:
        d = row_to_dict(r)
        inv_total += Decimal(d["current_value"] or "0")
        inv_list.append(d)

    ok({
        "liquid_bank_accounts": liquid_list,
        "liquid_bank_total": str(liquid_total),
        "short_term_investments": inv_list,
        "short_term_investment_total": str(inv_total),
        "total_liquidity": str(liquid_total + inv_total),
    })


# ---------------------------------------------------------------------------
# cash-flow-projection
# ---------------------------------------------------------------------------
def cash_flow_projection(conn, args):
    """Project cash flow for the next 30/60/90 days based on forecasts."""
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    today = date.today().isoformat()

    # Active forecasts with period_end in the future
    cf = Table("cash_forecast")
    q = (Q.from_(cf).select(cf.star)
         .where(cf.company_id == P())
         .where(cf.period_end >= P())
         .orderby(cf.period_start))
    rows = conn.execute(q.get_sql(), (args.company_id, today)).fetchall()

    projections = []
    total_inflows = Decimal("0")
    total_outflows = Decimal("0")
    for r in rows:
        d = row_to_dict(r)
        inf = Decimal(d["expected_inflows"] or "0")
        out = Decimal(d["expected_outflows"] or "0")
        total_inflows += inf
        total_outflows += out
        projections.append({
            "forecast_id": d["id"],
            "forecast_name": d["forecast_name"],
            "period_start": d["period_start"],
            "period_end": d["period_end"],
            "expected_inflows": str(inf),
            "expected_outflows": str(out),
            "net_forecast": d["net_forecast"],
        })

    # Current cash
    ba = Table("bank_account_extended")
    accts = conn.execute(
        Q.from_(ba).select(ba.current_balance)
        .where(ba.company_id == P()).where(ba.is_active == 1).get_sql(),
        (args.company_id,),
    ).fetchall()
    current_cash = Decimal("0")
    for a in accts:
        current_cash += Decimal(a[0] or "0")

    projected_end = current_cash + total_inflows - total_outflows

    ok({
        "current_cash": str(current_cash),
        "total_projected_inflows": str(total_inflows),
        "total_projected_outflows": str(total_outflows),
        "projected_end_balance": str(projected_end),
        "forecasts": projections,
    })


# ---------------------------------------------------------------------------
# ACTIONS export
# ---------------------------------------------------------------------------
ACTIONS = {
    "treasury-add-bank-account": add_bank_account,
    "treasury-update-bank-account": update_bank_account,
    "treasury-get-bank-account": get_bank_account,
    "treasury-list-bank-accounts": list_bank_accounts,
    "treasury-record-bank-balance": record_bank_balance,
    "treasury-add-cash-position": add_cash_position,
    "treasury-list-cash-positions": list_cash_positions,
    "treasury-get-cash-position": get_cash_position,
    "treasury-add-cash-forecast": add_cash_forecast,
    "treasury-update-cash-forecast": update_cash_forecast,
    "treasury-list-cash-forecasts": list_cash_forecasts,
    "treasury-get-cash-forecast": get_cash_forecast,
    "treasury-generate-cash-forecast": generate_cash_forecast,
    "treasury-cash-dashboard": cash_dashboard,
    "treasury-bank-summary-report": bank_summary_report,
    "treasury-liquidity-report": liquidity_report,
    "treasury-cash-flow-projection": cash_flow_projection,
}
