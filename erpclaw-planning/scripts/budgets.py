"""ERPClaw Planning -- budgets domain module.

Budget actions that operate on the scenario table with scenario_type='budget'.
A "budget version" is a scenario with type 'budget'. Budget lines are scenario_lines.

Also provides budget-vs-actual comparison against GL entries and variance dashboards.

Actions (8): add-budget-version, list-budget-versions, get-budget-version,
approve-budget, lock-budget, compare-budget-versions, budget-vs-actual,
variance-dashboard.

Imported by db_query.py (unified router).
"""
import os
import sys
import uuid
from decimal import Decimal, ROUND_HALF_UP

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.naming import get_next_name, register_prefix
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, LiteralValue, insert_row, update_row, dynamic_update
except ImportError:
    pass

SKILL_NAME = "erpclaw-planning"

VALID_ACCOUNT_TYPES = ("revenue", "expense", "asset", "liability")


def _now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# 1. add-budget-version
# ---------------------------------------------------------------------------
def add_budget_version(conn, args):
    """Create a new budget version (scenario with type='budget') with optional lines."""
    if not getattr(args, "name", None):
        err("--name is required")
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    budget_id = str(uuid.uuid4())
    naming = get_next_name(conn, "planning_scenario", company_id=args.company_id)
    now = _now_iso()

    sql, _ = insert_row("planning_scenario", {
        "id": P(), "naming_series": P(), "name": P(), "scenario_type": P(),
        "description": P(), "assumptions": P(), "fiscal_year": P(),
        "total_revenue": P(), "total_expense": P(), "net_income": P(),
        "status": P(), "company_id": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql,
        (budget_id, naming, args.name, "budget",
         getattr(args, "description", None),
         getattr(args, "assumptions", None),
         getattr(args, "fiscal_year", None),
         "0", "0", "0", "draft",
         args.company_id, now, now)
    )
    audit(conn, SKILL_NAME, "planning-add-budget-version", "planning_scenario", budget_id)
    conn.commit()
    ok({"id": budget_id, "naming_series": naming, "name": args.name,
        "scenario_type": "budget", "scenario_status": "draft"})


# ---------------------------------------------------------------------------
# 2. list-budget-versions
# ---------------------------------------------------------------------------
def list_budget_versions(conn, args):
    """List all budget versions (scenarios with type='budget')."""
    t = Table("planning_scenario")
    q = Q.from_(t).select(t.star).where(t.scenario_type == "budget")
    q_cnt = Q.from_(t).select(fn.Count(t.star)).where(t.scenario_type == "budget")
    params = []

    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(args.company_id)
    status_val = getattr(args, "status", None)
    if status_val:
        q = q.where(t.status == P())
        q_cnt = q_cnt.where(t.status == P())
        params.append(status_val)
    if getattr(args, "fiscal_year", None):
        q = q.where(t.fiscal_year == P())
        q_cnt = q_cnt.where(t.fiscal_year == P())
        params.append(args.fiscal_year)
    if getattr(args, "search", None):
        like = LiteralValue("?")
        crit = (t.name.like(like)) | (t.description.like(like))
        q = q.where(crit)
        q_cnt = q_cnt.where(crit)
        s = f"%{args.search}%"
        params.extend([s, s])

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]
    params.extend([args.limit, args.offset])
    q = q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params).fetchall()
    ok({"rows": [row_to_dict(r) for r in rows], "total_count": total,
        "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total})


# ---------------------------------------------------------------------------
# 3. get-budget-version
# ---------------------------------------------------------------------------
def get_budget_version(conn, args):
    budget_id = getattr(args, "budget_id", None)
    if not budget_id:
        err("--budget-id is required")

    t = Table("planning_scenario")
    row = conn.execute(
        Q.from_(t).select(t.star).where(t.id == P()).where(t.scenario_type == "budget").get_sql(),
        (budget_id,)
    ).fetchone()
    if not row:
        err(f"Budget version {budget_id} not found")

    data = row_to_dict(row)
    data["scenario_status"] = data.pop("status", "draft")

    # Include lines
    lines = conn.execute(Q.from_(Table("planning_scenario_line")).select(Table("planning_scenario_line").star).where(Field("scenario_id") == P()).orderby(Field("period")).orderby(Field("account_name")).get_sql(), (budget_id,)).fetchall()
    data["lines"] = [row_to_dict(l) for l in lines]
    data["line_count"] = len(lines)
    ok(data)


# ---------------------------------------------------------------------------
# 4. approve-budget
# ---------------------------------------------------------------------------
def approve_budget(conn, args):
    budget_id = getattr(args, "budget_id", None)
    if not budget_id:
        err("--budget-id is required")

    row = conn.execute(Q.from_(Table("planning_scenario")).select(Field('status'), Field('scenario_type')).where(Field("id") == P()).get_sql(), (budget_id,)).fetchone()
    if not row:
        err(f"Budget version {budget_id} not found")

    data = row_to_dict(row)
    if data["scenario_type"] != "budget":
        err(f"Scenario {budget_id} is not a budget (type: {data['scenario_type']})")
    if data["status"] == "approved":
        err("Budget is already approved")
    if data["status"] in ("locked", "archived"):
        err(f"Cannot approve budget in '{data['status']}' status")

    sql = update_row("planning_scenario",
                     data={"status": P(), "updated_at": LiteralValue("datetime('now')")},
                     where={"id": P()})
    conn.execute(sql, ("approved", budget_id))
    audit(conn, SKILL_NAME, "planning-approve-budget", "planning_scenario", budget_id)
    conn.commit()
    ok({"id": budget_id, "scenario_status": "approved"})


# ---------------------------------------------------------------------------
# 5. lock-budget
# ---------------------------------------------------------------------------
def lock_budget(conn, args):
    budget_id = getattr(args, "budget_id", None)
    if not budget_id:
        err("--budget-id is required")

    row = conn.execute(Q.from_(Table("planning_scenario")).select(Field('status'), Field('scenario_type')).where(Field("id") == P()).get_sql(), (budget_id,)).fetchone()
    if not row:
        err(f"Budget version {budget_id} not found")

    data = row_to_dict(row)
    if data["scenario_type"] != "budget":
        err(f"Scenario {budget_id} is not a budget (type: {data['scenario_type']})")
    if data["status"] == "locked":
        err("Budget is already locked")
    if data["status"] == "archived":
        err("Cannot lock an archived budget")

    sql = update_row("planning_scenario",
                     data={"status": P(), "updated_at": LiteralValue("datetime('now')")},
                     where={"id": P()})
    conn.execute(sql, ("locked", budget_id))
    audit(conn, SKILL_NAME, "planning-lock-budget", "planning_scenario", budget_id)
    conn.commit()
    ok({"id": budget_id, "scenario_status": "locked"})


# ---------------------------------------------------------------------------
# 6. compare-budget-versions
# ---------------------------------------------------------------------------
def compare_budget_versions(conn, args):
    """Compare two budget versions line-by-line."""
    id_1 = getattr(args, "budget_id_1", None)
    id_2 = getattr(args, "budget_id_2", None)
    if not id_1 or not id_2:
        err("--budget-id-1 and --budget-id-2 are required")

    b1 = conn.execute(Q.from_(Table("planning_scenario")).select(Table("planning_scenario").star).where(Field("id") == P()).get_sql(), (id_1,)).fetchone()
    b2 = conn.execute(Q.from_(Table("planning_scenario")).select(Table("planning_scenario").star).where(Field("id") == P()).get_sql(), (id_2,)).fetchone()
    if not b1:
        err(f"Budget version {id_1} not found")
    if not b2:
        err(f"Budget version {id_2} not found")

    d1, d2 = row_to_dict(b1), row_to_dict(b2)

    lines1 = conn.execute(Q.from_(Table("planning_scenario_line")).select(Table("planning_scenario_line").star).where(Field("scenario_id") == P()).orderby(Field("period")).orderby(Field("account_name")).get_sql(), (id_1,)).fetchall()
    lines2 = conn.execute(Q.from_(Table("planning_scenario_line")).select(Table("planning_scenario_line").star).where(Field("scenario_id") == P()).orderby(Field("period")).orderby(Field("account_name")).get_sql(), (id_2,)).fetchall()

    map1 = {}
    for l in lines1:
        ld = row_to_dict(l)
        key = (ld["period"], ld["account_name"])
        map1[key] = to_decimal(ld["amount"])

    map2 = {}
    for l in lines2:
        ld = row_to_dict(l)
        key = (ld["period"], ld["account_name"])
        map2[key] = to_decimal(ld["amount"])

    all_keys = sorted(set(list(map1.keys()) + list(map2.keys())))
    comparison = []
    for key in all_keys:
        amt1 = map1.get(key, Decimal("0"))
        amt2 = map2.get(key, Decimal("0"))
        diff = amt2 - amt1
        pct = (str(round_currency((diff / amt1) * Decimal("100")))
               if amt1 != Decimal("0") else "N/A")
        comparison.append({
            "period": key[0],
            "account_name": key[1],
            "version_1_amount": str(amt1),
            "version_2_amount": str(amt2),
            "difference": str(diff),
            "difference_pct": pct,
        })

    ok({
        "version_1": {"id": id_1, "name": d1["name"]},
        "version_2": {"id": id_2, "name": d2["name"]},
        "line_comparisons": comparison,
        "total_lines": len(comparison),
    })


# ---------------------------------------------------------------------------
# 7. budget-vs-actual
# ---------------------------------------------------------------------------
def budget_vs_actual(conn, args):
    """Compare budget lines against actual GL entries by account name and period.

    Looks up accounts by name and sums GL entries within the period's month.
    Budget lines use (account_name, period) where period is YYYY-MM format.
    """
    budget_id = getattr(args, "budget_id", None)
    if not budget_id:
        err("--budget-id is required")

    row = conn.execute(Q.from_(Table("planning_scenario")).select(Table("planning_scenario").star).where(Field("id") == P()).get_sql(), (budget_id,)).fetchone()
    if not row:
        err(f"Budget version {budget_id} not found")

    budget_data = row_to_dict(row)
    company_id = budget_data["company_id"]

    lines = conn.execute(Q.from_(Table("planning_scenario_line")).select(Table("planning_scenario_line").star).where(Field("scenario_id") == P()).orderby(Field("period")).orderby(Field("account_name")).get_sql(), (budget_id,)).fetchall()

    results = []
    total_budget = Decimal("0")
    total_actual = Decimal("0")

    for l in lines:
        ld = row_to_dict(l)
        budgeted = to_decimal(ld["amount"])
        total_budget += budgeted

        # Find account by name
        account_row = conn.execute(Q.from_(Table("account")).select(Field('id'), Field('root_type')).where(Field("name") == P()).where(Field("company_id") == P()).get_sql(), (ld["account_name"], company_id)).fetchone()

        actual = Decimal("0")
        if account_row:
            acct = row_to_dict(account_row)
            # Period format: YYYY-MM -- query GL for that month
            period = ld["period"]
            start_date = f"{period}-01"
            # Calculate end date: last day of the month
            parts = period.split("-")
            year, month = int(parts[0]), int(parts[1])
            if month == 12:
                end_date = f"{year + 1}-01-01"
            else:
                end_date = f"{year}-{month + 1:02d}-01"

            # Sum debits and credits for this account in the period
            gl_row = conn.execute(
                """SELECT COALESCE(SUM(CAST(debit AS REAL)), 0) as total_debit,
                          COALESCE(SUM(CAST(credit AS REAL)), 0) as total_credit
                   FROM gl_entry
                   WHERE account_id = ? AND posting_date >= ? AND posting_date < ?
                   AND is_cancelled = 0""",
                (acct["id"], start_date, end_date)
            ).fetchone()

            if gl_row:
                if acct["root_type"] in ("expense", "asset"):
                    actual = to_decimal(str(gl_row[0])) - to_decimal(str(gl_row[1]))
                else:
                    actual = to_decimal(str(gl_row[1])) - to_decimal(str(gl_row[0]))

        total_actual += actual
        variance = actual - budgeted
        variance_pct = (str(round_currency((variance / budgeted) * Decimal("100")))
                        if budgeted != Decimal("0") else "0")

        results.append({
            "period": ld["period"],
            "account_name": ld["account_name"],
            "account_type": ld["account_type"],
            "budgeted": str(round_currency(budgeted)),
            "actual": str(round_currency(actual)),
            "variance": str(round_currency(variance)),
            "variance_pct": variance_pct,
        })

    total_variance = total_actual - total_budget
    ok({
        "budget_id": budget_id,
        "budget_name": budget_data["name"],
        "lines": results,
        "total_lines": len(results),
        "summary": {
            "total_budget": str(round_currency(total_budget)),
            "total_actual": str(round_currency(total_actual)),
            "total_variance": str(round_currency(total_variance)),
        },
    })


# ---------------------------------------------------------------------------
# 8. variance-dashboard
# ---------------------------------------------------------------------------
def variance_dashboard(conn, args):
    """High-level variance summary: total budget, actual, variance, variance%."""
    budget_id = getattr(args, "budget_id", None)
    if not budget_id:
        err("--budget-id is required")

    row = conn.execute(Q.from_(Table("planning_scenario")).select(Table("planning_scenario").star).where(Field("id") == P()).get_sql(), (budget_id,)).fetchone()
    if not row:
        err(f"Budget version {budget_id} not found")

    budget_data = row_to_dict(row)
    company_id = budget_data["company_id"]

    lines = conn.execute(Q.from_(Table("planning_scenario_line")).select(Table("planning_scenario_line").star).where(Field("scenario_id") == P()).get_sql(), (budget_id,)).fetchall()

    total_budget_revenue = Decimal("0")
    total_budget_expense = Decimal("0")
    total_actual_revenue = Decimal("0")
    total_actual_expense = Decimal("0")

    for l in lines:
        ld = row_to_dict(l)
        budgeted = to_decimal(ld["amount"])
        acct_type = ld["account_type"]

        if acct_type == "revenue":
            total_budget_revenue += budgeted
        elif acct_type == "expense":
            total_budget_expense += budgeted

        # Look up actual from GL
        account_row = conn.execute(Q.from_(Table("account")).select(Field('id'), Field('root_type')).where(Field("name") == P()).where(Field("company_id") == P()).get_sql(), (ld["account_name"], company_id)).fetchone()

        actual = Decimal("0")
        if account_row:
            acct = row_to_dict(account_row)
            period = ld["period"]
            start_date = f"{period}-01"
            parts = period.split("-")
            year, month = int(parts[0]), int(parts[1])
            if month == 12:
                end_date = f"{year + 1}-01-01"
            else:
                end_date = f"{year}-{month + 1:02d}-01"

            gl_row = conn.execute(
                """SELECT COALESCE(SUM(CAST(debit AS REAL)), 0),
                          COALESCE(SUM(CAST(credit AS REAL)), 0)
                   FROM gl_entry
                   WHERE account_id = ? AND posting_date >= ? AND posting_date < ?
                   AND is_cancelled = 0""",
                (acct["id"], start_date, end_date)
            ).fetchone()

            if gl_row:
                if acct["root_type"] in ("expense", "asset"):
                    actual = to_decimal(str(gl_row[0])) - to_decimal(str(gl_row[1]))
                else:
                    actual = to_decimal(str(gl_row[1])) - to_decimal(str(gl_row[0]))

        if acct_type == "revenue":
            total_actual_revenue += actual
        elif acct_type == "expense":
            total_actual_expense += actual

    budget_net = total_budget_revenue - total_budget_expense
    actual_net = total_actual_revenue - total_actual_expense
    net_variance = actual_net - budget_net

    rev_variance = total_actual_revenue - total_budget_revenue
    exp_variance = total_actual_expense - total_budget_expense

    rev_var_pct = (str(round_currency((rev_variance / total_budget_revenue) * Decimal("100")))
                   if total_budget_revenue != Decimal("0") else "0")
    exp_var_pct = (str(round_currency((exp_variance / total_budget_expense) * Decimal("100")))
                   if total_budget_expense != Decimal("0") else "0")
    net_var_pct = (str(round_currency((net_variance / budget_net) * Decimal("100")))
                   if budget_net != Decimal("0") else "0")

    ok({
        "budget_id": budget_id,
        "budget_name": budget_data["name"],
        "revenue": {
            "budget": str(round_currency(total_budget_revenue)),
            "actual": str(round_currency(total_actual_revenue)),
            "variance": str(round_currency(rev_variance)),
            "variance_pct": rev_var_pct,
        },
        "expense": {
            "budget": str(round_currency(total_budget_expense)),
            "actual": str(round_currency(total_actual_expense)),
            "variance": str(round_currency(exp_variance)),
            "variance_pct": exp_var_pct,
        },
        "net_income": {
            "budget": str(round_currency(budget_net)),
            "actual": str(round_currency(actual_net)),
            "variance": str(round_currency(net_variance)),
            "variance_pct": net_var_pct,
        },
        "line_count": len(lines),
    })


# ---------------------------------------------------------------------------
# Action Router
# ---------------------------------------------------------------------------
ACTIONS = {
    "planning-add-budget-version": add_budget_version,
    "planning-list-budget-versions": list_budget_versions,
    "planning-get-budget-version": get_budget_version,
    "planning-approve-budget": approve_budget,
    "planning-lock-budget": lock_budget,
    "planning-compare-budget-versions": compare_budget_versions,
    "planning-budget-vs-actual": budget_vs_actual,
    "planning-variance-dashboard": variance_dashboard,
}
