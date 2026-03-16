"""ERPClaw Planning -- scenarios domain module.

Actions for scenario planning (12 actions).
Imported by db_query.py (unified router).
"""
import json
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

# Register naming prefixes
register_prefix("planning_scenario", "SCEN-")
register_prefix("planning_scenario_line", "SCNL-")

VALID_SCENARIO_TYPES = ("base", "best_case", "worst_case", "what_if", "budget", "custom")
VALID_SCENARIO_STATUSES = ("draft", "active", "approved", "locked", "archived")
VALID_ACCOUNT_TYPES = ("revenue", "expense", "asset", "liability")


def _now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _validate_enum(val, choices, label):
    if val not in choices:
        err(f"Invalid {label}: {val}. Must be one of: {', '.join(choices)}")


# ---------------------------------------------------------------------------
# 1. add-scenario
# ---------------------------------------------------------------------------
def add_scenario(conn, args):
    if not getattr(args, "name", None):
        err("--name is required")
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    scenario_type = getattr(args, "scenario_type", None) or "base"
    _validate_enum(scenario_type, VALID_SCENARIO_TYPES, "scenario-type")

    base_scenario_id = getattr(args, "base_scenario_id", None)
    if base_scenario_id:
        if not conn.execute(Q.from_(Table("planning_scenario")).select(Field('id')).where(Field("id") == P()).get_sql(), (base_scenario_id,)).fetchone():
            err(f"Base scenario {base_scenario_id} not found")

    scenario_id = str(uuid.uuid4())
    naming = get_next_name(conn, "planning_scenario", company_id=args.company_id)
    now = _now_iso()

    sql, _ = insert_row("planning_scenario", {
        "id": P(), "naming_series": P(), "name": P(), "scenario_type": P(),
        "description": P(), "assumptions": P(), "base_scenario_id": P(),
        "fiscal_year": P(), "total_revenue": P(), "total_expense": P(),
        "net_income": P(), "status": P(), "company_id": P(),
        "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql,
        (scenario_id, naming, args.name, scenario_type,
         getattr(args, "description", None),
         getattr(args, "assumptions", None),
         base_scenario_id,
         getattr(args, "fiscal_year", None),
         "0", "0", "0", "draft",
         args.company_id, now, now)
    )
    audit(conn, SKILL_NAME, "planning-add-scenario", "planning_scenario", scenario_id)
    conn.commit()
    ok({"id": scenario_id, "naming_series": naming, "name": args.name,
        "scenario_type": scenario_type, "scenario_status": "draft"})


# ---------------------------------------------------------------------------
# 2. update-scenario
# ---------------------------------------------------------------------------
def update_scenario(conn, args):
    scenario_id = getattr(args, "scenario_id", None)
    if not scenario_id:
        err("--scenario-id is required")

    row = conn.execute(Q.from_(Table("planning_scenario")).select(Table("planning_scenario").star).where(Field("id") == P()).get_sql(), (scenario_id,)).fetchone()
    if not row:
        err(f"Scenario {scenario_id} not found")

    current = row_to_dict(row)
    if current["status"] in ("approved", "locked", "archived"):
        err(f"Cannot update scenario in '{current['status']}' status")

    data, changed = {}, []
    for arg_name, col_name in {
        "name": "name", "description": "description",
        "assumptions": "assumptions", "fiscal_year": "fiscal_year",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            data[col_name] = val
            changed.append(col_name)

    scenario_type = getattr(args, "scenario_type", None)
    if scenario_type:
        _validate_enum(scenario_type, VALID_SCENARIO_TYPES, "scenario-type")
        data["scenario_type"] = scenario_type
        changed.append("scenario_type")

    if not changed:
        err("No fields to update")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("planning_scenario", data, {"id": scenario_id})
    conn.execute(sql, params)
    audit(conn, SKILL_NAME, "planning-update-scenario", "planning_scenario", scenario_id)
    conn.commit()
    ok({"id": scenario_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# 3. get-scenario
# ---------------------------------------------------------------------------
def get_scenario(conn, args):
    scenario_id = getattr(args, "scenario_id", None)
    if not scenario_id:
        err("--scenario-id is required")

    row = conn.execute(Q.from_(Table("planning_scenario")).select(Table("planning_scenario").star).where(Field("id") == P()).get_sql(), (scenario_id,)).fetchone()
    if not row:
        err(f"Scenario {scenario_id} not found")

    data = row_to_dict(row)
    # Rename status to avoid ok() collision
    data["scenario_status"] = data.pop("status", "draft")
    ok(data)


# ---------------------------------------------------------------------------
# 4. list-scenarios
# ---------------------------------------------------------------------------
def list_scenarios(conn, args):
    t = Table("planning_scenario")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star))
    params = []

    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "scenario_type", None):
        q = q.where(t.scenario_type == P())
        q_cnt = q_cnt.where(t.scenario_type == P())
        params.append(args.scenario_type)
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
# 5. add-scenario-line
# ---------------------------------------------------------------------------
def add_scenario_line(conn, args):
    scenario_id = getattr(args, "scenario_id", None)
    if not scenario_id:
        err("--scenario-id is required")
    if not getattr(args, "account_name", None):
        err("--account-name is required")
    if not getattr(args, "period", None):
        err("--period is required")
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    row = conn.execute(Q.from_(Table("planning_scenario")).select(Field('id'), Field('status')).where(Field("id") == P()).get_sql(), (scenario_id,)).fetchone()
    if not row:
        err(f"Scenario {scenario_id} not found")
    if row_to_dict(row)["status"] in ("approved", "locked", "archived"):
        err("Cannot add lines to a scenario that is approved, locked, or archived")

    account_type = getattr(args, "account_type", None) or "expense"
    _validate_enum(account_type, VALID_ACCOUNT_TYPES, "account-type")

    amount = str(round_currency(to_decimal(getattr(args, "amount", None) or "0")))
    line_id = str(uuid.uuid4())
    naming = get_next_name(conn, "planning_scenario_line", company_id=args.company_id)
    now = _now_iso()

    sql, _ = insert_row("planning_scenario_line", {"id": P(), "naming_series": P(), "scenario_id": P(), "account_name": P(), "account_type": P(), "period": P(), "amount": P(), "notes": P(), "company_id": P(), "created_at": P()})
    conn.execute(sql,
        (line_id, naming, scenario_id, args.account_name, account_type,
         args.period, amount, getattr(args, "notes", None),
         args.company_id, now)
    )

    # Recalculate scenario totals
    _recalculate_scenario_totals(conn, scenario_id)

    audit(conn, SKILL_NAME, "planning-add-scenario-line", "planning_scenario_line", line_id)
    conn.commit()
    ok({"id": line_id, "naming_series": naming, "scenario_id": scenario_id,
        "account_name": args.account_name, "amount": amount})


# ---------------------------------------------------------------------------
# 6. list-scenario-lines
# ---------------------------------------------------------------------------
def list_scenario_lines(conn, args):
    scenario_id = getattr(args, "scenario_id", None)
    t = Table("planning_scenario_line")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star))
    params = []

    if scenario_id:
        q = q.where(t.scenario_id == P())
        q_cnt = q_cnt.where(t.scenario_id == P())
        params.append(scenario_id)
    if getattr(args, "account_type", None):
        q = q.where(t.account_type == P())
        q_cnt = q_cnt.where(t.account_type == P())
        params.append(args.account_type)
    if getattr(args, "period", None):
        q = q.where(t.period == P())
        q_cnt = q_cnt.where(t.period == P())
        params.append(args.period)
    if getattr(args, "search", None):
        like = LiteralValue("?")
        crit = (t.account_name.like(like)) | (t.notes.like(like))
        q = q.where(crit)
        q_cnt = q_cnt.where(crit)
        s = f"%{args.search}%"
        params.extend([s, s])

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]
    params.extend([args.limit, args.offset])
    q = q.orderby(t.period).orderby(t.account_name).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params).fetchall()
    ok({"rows": [row_to_dict(r) for r in rows], "total_count": total,
        "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total})


# ---------------------------------------------------------------------------
# 7. update-scenario-line
# ---------------------------------------------------------------------------
def update_scenario_line(conn, args):
    line_id = getattr(args, "scenario_line_id", None)
    if not line_id:
        err("--scenario-line-id is required")

    row = conn.execute(Q.from_(Table("planning_scenario_line")).select(Table("planning_scenario_line").star).where(Field("id") == P()).get_sql(), (line_id,)).fetchone()
    if not row:
        err(f"Scenario line {line_id} not found")

    line_data = row_to_dict(row)

    # Check parent scenario status
    parent = conn.execute(Q.from_(Table("planning_scenario")).select(Field('status')).where(Field("id") == P()).get_sql(), (line_data["scenario_id"],)).fetchone()
    if parent and row_to_dict(parent)["status"] in ("approved", "locked", "archived"):
        err("Cannot update lines on a scenario that is approved, locked, or archived")

    data, changed = {}, []
    for arg_name, col_name in {
        "account_name": "account_name", "period": "period", "notes": "notes",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            data[col_name] = val
            changed.append(col_name)

    account_type = getattr(args, "account_type", None)
    if account_type:
        _validate_enum(account_type, VALID_ACCOUNT_TYPES, "account-type")
        data["account_type"] = account_type
        changed.append("account_type")

    amount = getattr(args, "amount", None)
    if amount is not None:
        data["amount"] = str(round_currency(to_decimal(amount)))
        changed.append("amount")

    if not changed:
        err("No fields to update")

    sql, params = dynamic_update("planning_scenario_line", data, {"id": line_id})
    conn.execute(sql, params)

    # Recalculate scenario totals
    _recalculate_scenario_totals(conn, line_data["scenario_id"])

    audit(conn, SKILL_NAME, "planning-update-scenario-line", "planning_scenario_line", line_id)
    conn.commit()
    ok({"id": line_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# 8. clone-scenario
# ---------------------------------------------------------------------------
def clone_scenario(conn, args):
    source_id = getattr(args, "scenario_id", None)
    if not source_id:
        err("--scenario-id is required")
    if not getattr(args, "name", None):
        err("--name is required for the cloned scenario")

    source = conn.execute(Q.from_(Table("planning_scenario")).select(Table("planning_scenario").star).where(Field("id") == P()).get_sql(), (source_id,)).fetchone()
    if not source:
        err(f"Scenario {source_id} not found")

    source_data = row_to_dict(source)
    company_id = source_data["company_id"]

    new_id = str(uuid.uuid4())
    naming = get_next_name(conn, "planning_scenario", company_id=company_id)
    now = _now_iso()

    clone_sql, _ = insert_row("planning_scenario", {
        "id": P(), "naming_series": P(), "name": P(), "scenario_type": P(),
        "description": P(), "assumptions": P(), "base_scenario_id": P(),
        "fiscal_year": P(), "total_revenue": P(), "total_expense": P(),
        "net_income": P(), "status": P(), "company_id": P(),
        "created_at": P(), "updated_at": P(),
    })
    conn.execute(clone_sql,
        (new_id, naming, args.name, source_data["scenario_type"],
         source_data.get("description"), source_data.get("assumptions"),
         source_id, source_data.get("fiscal_year"),
         source_data["total_revenue"], source_data["total_expense"],
         source_data["net_income"], "draft", company_id, now, now)
    )

    # Clone lines
    source_lines = conn.execute(Q.from_(Table("planning_scenario_line")).select(Table("planning_scenario_line").star).where(Field("scenario_id") == P()).get_sql(), (source_id,)).fetchall()
    lines_cloned = 0
    for sl in source_lines:
        sl_data = row_to_dict(sl)
        new_line_id = str(uuid.uuid4())
        line_naming = get_next_name(conn, "planning_scenario_line", company_id=company_id)
        sql, _ = insert_row("planning_scenario_line", {"id": P(), "naming_series": P(), "scenario_id": P(), "account_name": P(), "account_type": P(), "period": P(), "amount": P(), "notes": P(), "company_id": P(), "created_at": P()})
        conn.execute(sql,
            (new_line_id, line_naming, new_id, sl_data["account_name"],
             sl_data["account_type"], sl_data["period"], sl_data["amount"],
             sl_data.get("notes"), company_id, now)
        )
        lines_cloned += 1

    audit(conn, SKILL_NAME, "planning-clone-scenario", "planning_scenario", new_id)
    conn.commit()
    ok({"id": new_id, "naming_series": naming, "name": args.name,
        "source_id": source_id, "lines_cloned": lines_cloned,
        "scenario_status": "draft"})


# ---------------------------------------------------------------------------
# 9. approve-scenario
# ---------------------------------------------------------------------------
def approve_scenario(conn, args):
    scenario_id = getattr(args, "scenario_id", None)
    if not scenario_id:
        err("--scenario-id is required")

    row = conn.execute(Q.from_(Table("planning_scenario")).select(Field('status')).where(Field("id") == P()).get_sql(), (scenario_id,)).fetchone()
    if not row:
        err(f"Scenario {scenario_id} not found")

    current_status = row_to_dict(row)["status"]
    if current_status == "approved":
        err("Scenario is already approved")
    if current_status in ("locked", "archived"):
        err(f"Cannot approve scenario in '{current_status}' status")

    sql = update_row("planning_scenario",
                     data={"status": P(), "updated_at": LiteralValue("datetime('now')")},
                     where={"id": P()})
    conn.execute(sql, ("approved", scenario_id))
    audit(conn, SKILL_NAME, "planning-approve-scenario", "planning_scenario", scenario_id)
    conn.commit()
    ok({"id": scenario_id, "scenario_status": "approved"})


# ---------------------------------------------------------------------------
# 10. archive-scenario
# ---------------------------------------------------------------------------
def archive_scenario(conn, args):
    scenario_id = getattr(args, "scenario_id", None)
    if not scenario_id:
        err("--scenario-id is required")

    row = conn.execute(Q.from_(Table("planning_scenario")).select(Field('status')).where(Field("id") == P()).get_sql(), (scenario_id,)).fetchone()
    if not row:
        err(f"Scenario {scenario_id} not found")

    current_status = row_to_dict(row)["status"]
    if current_status == "archived":
        err("Scenario is already archived")

    sql = update_row("planning_scenario",
                     data={"status": P(), "updated_at": LiteralValue("datetime('now')")},
                     where={"id": P()})
    conn.execute(sql, ("archived", scenario_id))
    audit(conn, SKILL_NAME, "planning-archive-scenario", "planning_scenario", scenario_id)
    conn.commit()
    ok({"id": scenario_id, "scenario_status": "archived"})


# ---------------------------------------------------------------------------
# 11. compare-scenarios
# ---------------------------------------------------------------------------
def compare_scenarios(conn, args):
    id_1 = getattr(args, "scenario_id_1", None)
    id_2 = getattr(args, "scenario_id_2", None)
    if not id_1 or not id_2:
        err("--scenario-id-1 and --scenario-id-2 are required")

    s1 = conn.execute(Q.from_(Table("planning_scenario")).select(Table("planning_scenario").star).where(Field("id") == P()).get_sql(), (id_1,)).fetchone()
    s2 = conn.execute(Q.from_(Table("planning_scenario")).select(Table("planning_scenario").star).where(Field("id") == P()).get_sql(), (id_2,)).fetchone()
    if not s1:
        err(f"Scenario {id_1} not found")
    if not s2:
        err(f"Scenario {id_2} not found")

    d1, d2 = row_to_dict(s1), row_to_dict(s2)

    lines1 = conn.execute(Q.from_(Table("planning_scenario_line")).select(Table("planning_scenario_line").star).where(Field("scenario_id") == P()).orderby(Field("period")).orderby(Field("account_name")).get_sql(), (id_1,)).fetchall()
    lines2 = conn.execute(Q.from_(Table("planning_scenario_line")).select(Table("planning_scenario_line").star).where(Field("scenario_id") == P()).orderby(Field("period")).orderby(Field("account_name")).get_sql(), (id_2,)).fetchall()

    # Build lookup: (period, account_name) -> amount
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
            "scenario_1_amount": str(amt1),
            "scenario_2_amount": str(amt2),
            "difference": str(diff),
            "difference_pct": pct,
        })

    ok({
        "scenario_1": {"id": id_1, "name": d1["name"], "type": d1["scenario_type"]},
        "scenario_2": {"id": id_2, "name": d2["name"], "type": d2["scenario_type"]},
        "line_comparisons": comparison,
        "total_lines": len(comparison),
        "summary": {
            "scenario_1_revenue": d1["total_revenue"],
            "scenario_1_expense": d1["total_expense"],
            "scenario_1_net": d1["net_income"],
            "scenario_2_revenue": d2["total_revenue"],
            "scenario_2_expense": d2["total_expense"],
            "scenario_2_net": d2["net_income"],
        },
    })


# ---------------------------------------------------------------------------
# 12. scenario-summary
# ---------------------------------------------------------------------------
def scenario_summary(conn, args):
    scenario_id = getattr(args, "scenario_id", None)
    if not scenario_id:
        err("--scenario-id is required")

    row = conn.execute(Q.from_(Table("planning_scenario")).select(Table("planning_scenario").star).where(Field("id") == P()).get_sql(), (scenario_id,)).fetchone()
    if not row:
        err(f"Scenario {scenario_id} not found")

    data = row_to_dict(row)

    lines = conn.execute(Q.from_(Table("planning_scenario_line")).select(Table("planning_scenario_line").star).where(Field("scenario_id") == P()).get_sql(), (scenario_id,)).fetchall()

    revenue = Decimal("0")
    expense = Decimal("0")
    by_period = {}
    by_account = {}

    for l in lines:
        ld = row_to_dict(l)
        amt = to_decimal(ld["amount"])
        if ld["account_type"] == "revenue":
            revenue += amt
        elif ld["account_type"] == "expense":
            expense += amt

        period = ld["period"]
        if period not in by_period:
            by_period[period] = {"revenue": Decimal("0"), "expense": Decimal("0")}
        if ld["account_type"] == "revenue":
            by_period[period]["revenue"] += amt
        elif ld["account_type"] == "expense":
            by_period[period]["expense"] += amt

        acct = ld["account_name"]
        if acct not in by_account:
            by_account[acct] = Decimal("0")
        by_account[acct] += amt

    net = revenue - expense

    # Convert by_period Decimals to strings
    period_summary = {}
    for p, vals in sorted(by_period.items()):
        period_summary[p] = {
            "revenue": str(round_currency(vals["revenue"])),
            "expense": str(round_currency(vals["expense"])),
            "net": str(round_currency(vals["revenue"] - vals["expense"])),
        }

    account_summary = {k: str(round_currency(v)) for k, v in sorted(by_account.items())}

    ok({
        "id": scenario_id,
        "name": data["name"],
        "scenario_type": data["scenario_type"],
        "scenario_status": data["status"],
        "total_revenue": str(round_currency(revenue)),
        "total_expense": str(round_currency(expense)),
        "net_income": str(round_currency(net)),
        "line_count": len(lines),
        "by_period": period_summary,
        "by_account": account_summary,
    })


# ---------------------------------------------------------------------------
# Helper: recalculate scenario totals from lines
# ---------------------------------------------------------------------------
def _recalculate_scenario_totals(conn, scenario_id):
    """Recalculate total_revenue, total_expense, net_income from lines."""
    lines = conn.execute(Q.from_(Table("planning_scenario_line")).select(Field('account_type'), Field('amount')).where(Field("scenario_id") == P()).get_sql(), (scenario_id,)).fetchall()

    revenue = Decimal("0")
    expense = Decimal("0")
    for l in lines:
        amt = to_decimal(l[1] if not hasattr(l, 'keys') else l["amount"])
        acct_type = l[0] if not hasattr(l, 'keys') else l["account_type"]
        if acct_type == "revenue":
            revenue += amt
        elif acct_type == "expense":
            expense += amt

    net = revenue - expense
    sql = update_row("planning_scenario",
                     data={"total_revenue": P(), "total_expense": P(),
                           "net_income": P(), "updated_at": LiteralValue("datetime('now')")},
                     where={"id": P()})
    conn.execute(sql,
        (str(round_currency(revenue)), str(round_currency(expense)),
         str(round_currency(net)), scenario_id)
    )


# ---------------------------------------------------------------------------
# Action Router
# ---------------------------------------------------------------------------
ACTIONS = {
    "planning-add-scenario": add_scenario,
    "planning-update-scenario": update_scenario,
    "planning-get-scenario": get_scenario,
    "planning-list-scenarios": list_scenarios,
    "planning-add-scenario-line": add_scenario_line,
    "planning-list-scenario-lines": list_scenario_lines,
    "planning-update-scenario-line": update_scenario_line,
    "planning-clone-scenario": clone_scenario,
    "planning-approve-scenario": approve_scenario,
    "planning-archive-scenario": archive_scenario,
    "planning-compare-scenarios": compare_scenarios,
    "planning-scenario-summary": scenario_summary,
}
