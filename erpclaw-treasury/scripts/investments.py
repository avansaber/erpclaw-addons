"""ERPClaw Treasury -- Investments domain module.

Investment instruments and transactions.
10 actions exported via ACTIONS dict.
"""
import os
import sys
import uuid
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name, register_prefix
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, LiteralValue, insert_row, update_row, dynamic_update

register_prefix("investment_transaction", "ITXN-")

SKILL = "erpclaw-treasury"

VALID_INVESTMENT_TYPES = (
    "cd", "money_market", "treasury_bill", "bond", "mutual_fund", "other",
)
VALID_INVESTMENT_STATUSES = ("active", "matured", "redeemed", "cancelled")
VALID_TRANSACTION_TYPES = (
    "purchase", "interest", "dividend", "redemption", "fee", "transfer",
)


# ---------------------------------------------------------------------------
# add-investment
# ---------------------------------------------------------------------------
def add_investment(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "name", None):
        err("--name is required")

    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    inv_type = getattr(args, "investment_type", None) or "cd"
    if inv_type not in VALID_INVESTMENT_TYPES:
        err(f"Invalid investment-type: {inv_type}")

    principal = getattr(args, "principal", None) or "0"
    current_value = getattr(args, "current_value", None) or principal
    interest_rate = getattr(args, "interest_rate", None) or "0"

    for label, val in [("principal", principal), ("current-value", current_value),
                       ("interest-rate", interest_rate)]:
        try:
            Decimal(val)
        except Exception:
            err(f"Invalid {label}: {val}")

    inv_id = str(uuid.uuid4())
    ns = get_next_name(conn, "investment", company_id=args.company_id)

    sql, _ = insert_row("investment", {"id": P(), "naming_series": P(), "name": P(), "investment_type": P(), "institution": P(), "account_number": P(), "principal": P(), "current_value": P(), "interest_rate": P(), "purchase_date": P(), "maturity_date": P(), "gl_account_id": P(), "status": P(), "notes": P(), "company_id": P()})
    conn.execute(sql,
        (
            inv_id, ns, args.name, inv_type,
            getattr(args, "institution", None),
            getattr(args, "account_number", None),
            principal, current_value, interest_rate,
            getattr(args, "purchase_date", None),
            getattr(args, "maturity_date", None),
            getattr(args, "gl_account_id", None),
            "active",
            getattr(args, "notes", None),
            args.company_id,
        ),
    )
    audit(conn, SKILL, "treasury-add-investment", "investment", inv_id,
          new_values={"name": args.name, "naming_series": ns, "principal": principal})
    conn.commit()
    ok({"investment_id": inv_id, "naming_series": ns, "investment_status": "active"})


# ---------------------------------------------------------------------------
# update-investment
# ---------------------------------------------------------------------------
def update_investment(conn, args):
    inv_id = getattr(args, "investment_id", None)
    if not inv_id:
        err("--investment-id is required")
    row = conn.execute(Q.from_(Table("investment")).select(Table("investment").star).where(Field("id") == P()).get_sql(), (inv_id,)).fetchone()
    if not row:
        err(f"Investment {inv_id} not found")

    data = row_to_dict(row)
    if data["status"] != "active":
        err(f"Cannot update investment with status '{data['status']}'")

    upd_data, changed = {}, []

    for field, attr in [
        ("name", "name"),
        ("institution", "institution"),
        ("account_number", "account_number"),
        ("gl_account_id", "gl_account_id"),
        ("purchase_date", "purchase_date"),
        ("maturity_date", "maturity_date"),
        ("notes", "notes"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            upd_data[field] = val
            changed.append(field)

    it = getattr(args, "investment_type", None)
    if it is not None:
        if it not in VALID_INVESTMENT_TYPES:
            err(f"Invalid investment-type: {it}")
        upd_data["investment_type"] = it
        changed.append("investment_type")

    for field, attr in [("principal", "principal"), ("current_value", "current_value"),
                        ("interest_rate", "interest_rate")]:
        val = getattr(args, attr, None)
        if val is not None:
            try:
                Decimal(val)
            except Exception:
                err(f"Invalid {attr.replace('_', '-')}: {val}")
            upd_data[field] = val
            changed.append(field)

    if not changed:
        err("No fields to update")

    upd_data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("investment", upd_data, {"id": inv_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "treasury-update-investment", "investment", inv_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"investment_id": inv_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# get-investment
# ---------------------------------------------------------------------------
def get_investment(conn, args):
    inv_id = getattr(args, "investment_id", None)
    if not inv_id:
        err("--investment-id is required")
    row = conn.execute(Q.from_(Table("investment")).select(Table("investment").star).where(Field("id") == P()).get_sql(), (inv_id,)).fetchone()
    if not row:
        err(f"Investment {inv_id} not found")

    data = row_to_dict(row)

    # Include transaction count
    it = Table("investment_transaction")
    txn_count = conn.execute(
        Q.from_(it).select(fn.Count("*")).where(it.investment_id == P()).get_sql(),
        (inv_id,),
    ).fetchone()[0]
    data["transaction_count"] = txn_count

    ok(data)


# ---------------------------------------------------------------------------
# list-investments
# ---------------------------------------------------------------------------
def list_investments(conn, args):
    t = Table("investment")
    params = []

    q = Q.from_(t).select(t.star)
    cq = Q.from_(t).select(fn.Count("*").as_("cnt"))

    company_id = getattr(args, "company_id", None)
    if company_id:
        q = q.where(t.company_id == P())
        cq = cq.where(t.company_id == P())
        params.append(company_id)

    inv_type = getattr(args, "investment_type", None)
    if inv_type:
        q = q.where(t.investment_type == P())
        cq = cq.where(t.investment_type == P())
        params.append(inv_type)

    inv_status = getattr(args, "investment_status", None)
    if inv_status:
        q = q.where(t.status == P())
        cq = cq.where(t.status == P())
        params.append(inv_status)

    search = getattr(args, "search", None)
    if search:
        search_crit = (t.name.like(P())) | (t.institution.like(P()))
        q = q.where(search_crit)
        cq = cq.where(search_crit)
        params.extend([f"%{search}%", f"%{search}%"])

    total = conn.execute(cq.get_sql(), params).fetchone()["cnt"]

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()
    ok({"investments": [row_to_dict(r) for r in rows], "total_count": total})


# ---------------------------------------------------------------------------
# add-investment-transaction
# ---------------------------------------------------------------------------
def add_investment_transaction(conn, args):
    inv_id = getattr(args, "investment_id", None)
    if not inv_id:
        err("--investment-id is required")

    row = conn.execute(Q.from_(Table("investment")).select(Table("investment").star).where(Field("id") == P()).get_sql(), (inv_id,)).fetchone()
    if not row:
        err(f"Investment {inv_id} not found")

    inv_data = row_to_dict(row)
    if inv_data["status"] not in ("active",):
        err(f"Cannot add transaction to investment with status '{inv_data['status']}'")

    txn_type = getattr(args, "transaction_type", None) or "purchase"
    if txn_type not in VALID_TRANSACTION_TYPES:
        err(f"Invalid transaction-type: {txn_type}")

    amount = getattr(args, "amount", None) or "0"
    try:
        Decimal(amount)
    except Exception:
        err(f"Invalid amount: {amount}")

    txn_id = str(uuid.uuid4())
    txn_date = getattr(args, "transaction_date", None) or date.today().isoformat()

    sql, _ = insert_row("investment_transaction", {"id": P(), "investment_id": P(), "transaction_type": P(), "transaction_date": P(), "amount": P(), "reference": P(), "notes": P(), "company_id": P()})
    conn.execute(sql,
        (
            txn_id, inv_id, txn_type, txn_date, amount,
            getattr(args, "reference", None),
            getattr(args, "notes", None),
            inv_data["company_id"],
        ),
    )

    # Update current_value based on transaction type
    current = Decimal(inv_data["current_value"] or "0")
    amt = Decimal(amount)
    if txn_type in ("purchase", "interest", "dividend", "transfer"):
        new_value = current + amt
    elif txn_type == "redemption":
        new_value = current - amt
    elif txn_type == "fee":
        new_value = current - amt
    else:
        new_value = current

    sql = update_row("investment",
        data={"current_value": P(), "updated_at": LiteralValue("datetime('now')")},
        where={"id": P()})
    conn.execute(sql, (str(new_value), inv_id))

    audit(conn, SKILL, "treasury-add-investment-transaction", "investment_transaction", txn_id,
          new_values={"type": txn_type, "amount": amount, "investment_id": inv_id})
    conn.commit()
    ok({
        "transaction_id": txn_id, "investment_id": inv_id,
        "transaction_type": txn_type, "amount": amount,
        "new_current_value": str(new_value),
    })


# ---------------------------------------------------------------------------
# list-investment-transactions
# ---------------------------------------------------------------------------
def list_investment_transactions(conn, args):
    inv_id = getattr(args, "investment_id", None)
    t = Table("investment_transaction")
    params = []

    q = Q.from_(t).select(t.star)
    cq = Q.from_(t).select(fn.Count("*").as_("cnt"))

    if inv_id:
        q = q.where(t.investment_id == P())
        cq = cq.where(t.investment_id == P())
        params.append(inv_id)

    company_id = getattr(args, "company_id", None)
    if company_id:
        q = q.where(t.company_id == P())
        cq = cq.where(t.company_id == P())
        params.append(company_id)

    txn_type = getattr(args, "transaction_type", None)
    if txn_type:
        q = q.where(t.transaction_type == P())
        cq = cq.where(t.transaction_type == P())
        params.append(txn_type)

    total = conn.execute(cq.get_sql(), params).fetchone()["cnt"]

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = (q.orderby(t.transaction_date, order=Order.desc)
          .orderby(t.created_at, order=Order.desc)
          .limit(P()).offset(P()))

    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()
    ok({"transactions": [row_to_dict(r) for r in rows], "total_count": total})


# ---------------------------------------------------------------------------
# mature-investment
# ---------------------------------------------------------------------------
def mature_investment(conn, args):
    inv_id = getattr(args, "investment_id", None)
    if not inv_id:
        err("--investment-id is required")
    row = conn.execute(Q.from_(Table("investment")).select(Table("investment").star).where(Field("id") == P()).get_sql(), (inv_id,)).fetchone()
    if not row:
        err(f"Investment {inv_id} not found")

    data = row_to_dict(row)
    if data["status"] != "active":
        err(f"Can only mature an active investment, current status: {data['status']}")

    sql = update_row("investment",
        data={"status": P(), "updated_at": LiteralValue("datetime('now')")},
        where={"id": P()})
    conn.execute(sql, ("matured", inv_id))
    audit(conn, SKILL, "treasury-mature-investment", "investment", inv_id,
          old_values={"investment_status": "active"}, new_values={"investment_status": "matured"})
    conn.commit()
    ok({"investment_id": inv_id, "investment_status": "matured"})


# ---------------------------------------------------------------------------
# redeem-investment
# ---------------------------------------------------------------------------
def redeem_investment(conn, args):
    """Redeem an investment -- status -> redeemed, calculate returns."""
    inv_id = getattr(args, "investment_id", None)
    if not inv_id:
        err("--investment-id is required")
    row = conn.execute(Q.from_(Table("investment")).select(Table("investment").star).where(Field("id") == P()).get_sql(), (inv_id,)).fetchone()
    if not row:
        err(f"Investment {inv_id} not found")

    data = row_to_dict(row)
    if data["status"] not in ("active", "matured"):
        err(f"Can only redeem active/matured investments, current status: {data['status']}")

    principal = Decimal(data["principal"] or "0")
    current_value = Decimal(data["current_value"] or "0")
    returns = current_value - principal
    return_pct = (returns / principal * Decimal("100")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    ) if principal > 0 else Decimal("0")

    # Create a redemption transaction
    txn_id = str(uuid.uuid4())
    sql, _ = insert_row("investment_transaction", {
        "id": P(), "investment_id": P(), "transaction_type": P(),
        "transaction_date": LiteralValue("date('now')"), "amount": P(),
        "reference": P(), "notes": P(), "company_id": P(),
    })
    conn.execute(sql, (
        txn_id, inv_id, "redemption", str(current_value),
        f"Full redemption of {data['name']}",
        f"Returns: {returns}, Return%: {return_pct}%",
        data["company_id"],
    ))

    sql = update_row("investment",
        data={"status": P(), "current_value": P(), "updated_at": LiteralValue("datetime('now')")},
        where={"id": P()})
    conn.execute(sql, ("redeemed", "0", inv_id))

    audit(conn, SKILL, "treasury-redeem-investment", "investment", inv_id,
          old_values={"investment_status": data["status"]},
          new_values={"investment_status": "redeemed", "returns": str(returns)})
    conn.commit()
    ok({
        "investment_id": inv_id, "investment_status": "redeemed",
        "principal": str(principal),
        "redeemed_value": str(current_value),
        "returns": str(returns),
        "return_percentage": str(return_pct),
        "transaction_id": txn_id,
    })


# ---------------------------------------------------------------------------
# investment-portfolio-report
# ---------------------------------------------------------------------------
def investment_portfolio_report(conn, args):
    """Report on all investments grouped by type."""
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    t = Table("investment")
    q = (Q.from_(t).select(t.star)
         .where(t.company_id == P())
         .orderby(t.investment_type).orderby(t.name))
    rows = conn.execute(q.get_sql(), (args.company_id,)).fetchall()

    by_type = {}
    total_principal = Decimal("0")
    total_value = Decimal("0")
    active_count = 0

    for r in rows:
        d = row_to_dict(r)
        itype = d["investment_type"]
        if itype not in by_type:
            by_type[itype] = {"investments": [], "total_principal": Decimal("0"),
                              "total_value": Decimal("0"), "count": 0}
        by_type[itype]["investments"].append(d)
        by_type[itype]["count"] += 1
        p = Decimal(d["principal"] or "0")
        v = Decimal(d["current_value"] or "0")
        by_type[itype]["total_principal"] += p
        by_type[itype]["total_value"] += v
        total_principal += p
        total_value += v
        if d["status"] == "active":
            active_count += 1

    # Convert Decimal to str for JSON
    summary = {}
    for k, v in by_type.items():
        summary[k] = {
            "count": v["count"],
            "total_principal": str(v["total_principal"]),
            "total_value": str(v["total_value"]),
            "investments": v["investments"],
        }

    total_returns = total_value - total_principal
    ok({
        "by_type": summary,
        "total_investments": len(rows),
        "active_investments": active_count,
        "total_principal": str(total_principal),
        "total_current_value": str(total_value),
        "total_returns": str(total_returns),
    })


# ---------------------------------------------------------------------------
# investment-maturity-alerts
# ---------------------------------------------------------------------------
def investment_maturity_alerts(conn, args):
    """List active investments maturing within the next N days (default 30)."""
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    days = int(getattr(args, "days", None) or 30)
    cutoff = (date.today() + timedelta(days=days)).isoformat()
    today = date.today().isoformat()

    t = Table("investment")
    q = (Q.from_(t).select(t.star)
         .where(t.company_id == P())
         .where(t.status == "active")
         .where(t.maturity_date.isnotnull())
         .where(t.maturity_date <= P())
         .orderby(t.maturity_date))
    rows = conn.execute(q.get_sql(), (args.company_id, cutoff)).fetchall()

    alerts = []
    for r in rows:
        d = row_to_dict(r)
        mat = d["maturity_date"]
        days_until = (date.fromisoformat(mat) - date.today()).days if mat else None
        overdue = days_until is not None and days_until < 0
        d["days_until_maturity"] = days_until
        d["is_overdue"] = overdue
        alerts.append(d)

    ok({
        "alerts": alerts,
        "total_alerts": len(alerts),
        "days_window": days,
    })


# ---------------------------------------------------------------------------
# ACTIONS export
# ---------------------------------------------------------------------------
ACTIONS = {
    "treasury-add-investment": add_investment,
    "treasury-update-investment": update_investment,
    "treasury-get-investment": get_investment,
    "treasury-list-investments": list_investments,
    "treasury-add-investment-transaction": add_investment_transaction,
    "treasury-list-investment-transactions": list_investment_transactions,
    "treasury-mature-investment": mature_investment,
    "treasury-redeem-investment": redeem_investment,
    "treasury-investment-portfolio-report": investment_portfolio_report,
    "treasury-investment-maturity-alerts": investment_maturity_alerts,
}
