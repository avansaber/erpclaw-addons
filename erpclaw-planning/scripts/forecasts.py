"""ERPClaw Planning -- forecasts domain module.

Actions for financial forecasting (10 actions).
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

# Register naming prefix for forecast lines
register_prefix("forecast_line", "FSTL-")

VALID_FORECAST_TYPES = ("rolling", "static", "driver_based", "custom")
VALID_PERIOD_TYPES = ("weekly", "monthly", "quarterly", "annual")
VALID_FORECAST_STATUSES = ("draft", "active", "locked", "archived")
VALID_ACCOUNT_TYPES = ("revenue", "expense", "asset", "liability")


def _now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _validate_enum(val, choices, label):
    if val not in choices:
        err(f"Invalid {label}: {val}. Must be one of: {', '.join(choices)}")


# ---------------------------------------------------------------------------
# 1. add-forecast
# ---------------------------------------------------------------------------
def add_forecast(conn, args):
    if not getattr(args, "name", None):
        err("--name is required")
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "start_period", None):
        err("--start-period is required")
    if not getattr(args, "end_period", None):
        err("--end-period is required")

    forecast_type = getattr(args, "forecast_type", None) or "rolling"
    _validate_enum(forecast_type, VALID_FORECAST_TYPES, "forecast-type")

    period_type = getattr(args, "period_type", None) or "monthly"
    _validate_enum(period_type, VALID_PERIOD_TYPES, "period-type")

    forecast_id = str(uuid.uuid4())
    naming = get_next_name(conn, "forecast", company_id=args.company_id)
    now = _now_iso()

    sql, _ = insert_row("forecast", {
        "id": P(), "naming_series": P(), "name": P(), "forecast_type": P(),
        "period_type": P(), "start_period": P(), "end_period": P(),
        "description": P(), "status": P(), "company_id": P(),
        "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql,
        (forecast_id, naming, args.name, forecast_type, period_type,
         args.start_period, args.end_period,
         getattr(args, "description", None),
         "draft", args.company_id, now, now)
    )
    audit(conn, SKILL_NAME, "planning-add-forecast", "forecast", forecast_id)
    conn.commit()
    ok({"id": forecast_id, "naming_series": naming, "name": args.name,
        "forecast_type": forecast_type, "period_type": period_type,
        "forecast_status": "draft"})


# ---------------------------------------------------------------------------
# 2. update-forecast
# ---------------------------------------------------------------------------
def update_forecast(conn, args):
    forecast_id = getattr(args, "forecast_id", None)
    if not forecast_id:
        err("--forecast-id is required")

    row = conn.execute(Q.from_(Table("forecast")).select(Table("forecast").star).where(Field("id") == P()).get_sql(), (forecast_id,)).fetchone()
    if not row:
        err(f"Forecast {forecast_id} not found")

    current = row_to_dict(row)
    if current["status"] in ("locked", "archived"):
        err(f"Cannot update forecast in '{current['status']}' status")

    data, changed = {}, []
    for arg_name, col_name in {
        "name": "name", "description": "description",
        "start_period": "start_period", "end_period": "end_period",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            data[col_name] = val
            changed.append(col_name)

    forecast_type = getattr(args, "forecast_type", None)
    if forecast_type:
        _validate_enum(forecast_type, VALID_FORECAST_TYPES, "forecast-type")
        data["forecast_type"] = forecast_type
        changed.append("forecast_type")

    period_type = getattr(args, "period_type", None)
    if period_type:
        _validate_enum(period_type, VALID_PERIOD_TYPES, "period-type")
        data["period_type"] = period_type
        changed.append("period_type")

    if not changed:
        err("No fields to update")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("forecast", data, {"id": forecast_id})
    conn.execute(sql, params)
    audit(conn, SKILL_NAME, "planning-update-forecast", "forecast", forecast_id)
    conn.commit()
    ok({"id": forecast_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# 3. get-forecast
# ---------------------------------------------------------------------------
def get_forecast(conn, args):
    forecast_id = getattr(args, "forecast_id", None)
    if not forecast_id:
        err("--forecast-id is required")

    row = conn.execute(Q.from_(Table("forecast")).select(Table("forecast").star).where(Field("id") == P()).get_sql(), (forecast_id,)).fetchone()
    if not row:
        err(f"Forecast {forecast_id} not found")

    data = row_to_dict(row)
    data["forecast_status"] = data.pop("status", "draft")
    ok(data)


# ---------------------------------------------------------------------------
# 4. list-forecasts
# ---------------------------------------------------------------------------
def list_forecasts(conn, args):
    t = Table("forecast")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star))
    params = []

    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "forecast_type", None):
        q = q.where(t.forecast_type == P())
        q_cnt = q_cnt.where(t.forecast_type == P())
        params.append(args.forecast_type)
    status_val = getattr(args, "status", None)
    if status_val:
        q = q.where(t.status == P())
        q_cnt = q_cnt.where(t.status == P())
        params.append(status_val)
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
# 5. add-forecast-line
# ---------------------------------------------------------------------------
def add_forecast_line(conn, args):
    forecast_id = getattr(args, "forecast_id", None)
    if not forecast_id:
        err("--forecast-id is required")
    if not getattr(args, "account_name", None):
        err("--account-name is required")
    if not getattr(args, "period", None):
        err("--period is required")
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    row = conn.execute(Q.from_(Table("forecast")).select(Field('id'), Field('status')).where(Field("id") == P()).get_sql(), (forecast_id,)).fetchone()
    if not row:
        err(f"Forecast {forecast_id} not found")
    if row_to_dict(row)["status"] in ("locked", "archived"):
        err("Cannot add lines to a forecast that is locked or archived")

    account_type = getattr(args, "account_type", None) or "revenue"
    _validate_enum(account_type, VALID_ACCOUNT_TYPES, "account-type")

    forecast_amount = str(round_currency(to_decimal(
        getattr(args, "forecast_amount", None) or "0")))
    actual_amount = str(round_currency(to_decimal(
        getattr(args, "actual_amount", None) or "0")))

    # Calculate variance
    fa = to_decimal(forecast_amount)
    aa = to_decimal(actual_amount)
    variance = aa - fa
    variance_pct = (str(round_currency((variance / fa) * Decimal("100")))
                    if fa != Decimal("0") else "0")

    line_id = str(uuid.uuid4())
    naming = get_next_name(conn, "forecast_line", company_id=args.company_id)
    now = _now_iso()

    sql, _ = insert_row("forecast_line", {"id": P(), "naming_series": P(), "forecast_id": P(), "account_name": P(), "account_type": P(), "period": P(), "forecast_amount": P(), "actual_amount": P(), "variance": P(), "variance_pct": P(), "notes": P(), "company_id": P(), "created_at": P()})
    conn.execute(sql,
        (line_id, naming, forecast_id, args.account_name, account_type,
         args.period, forecast_amount, actual_amount,
         str(round_currency(variance)), variance_pct,
         getattr(args, "notes", None), args.company_id, now)
    )
    audit(conn, SKILL_NAME, "planning-add-forecast-line", "forecast_line", line_id)
    conn.commit()
    ok({"id": line_id, "naming_series": naming, "forecast_id": forecast_id,
        "account_name": args.account_name, "forecast_amount": forecast_amount,
        "actual_amount": actual_amount, "variance": str(round_currency(variance)),
        "variance_pct": variance_pct})


# ---------------------------------------------------------------------------
# 6. list-forecast-lines
# ---------------------------------------------------------------------------
def list_forecast_lines(conn, args):
    forecast_id = getattr(args, "forecast_id", None)
    t = Table("forecast_line")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star))
    params = []

    if forecast_id:
        q = q.where(t.forecast_id == P())
        q_cnt = q_cnt.where(t.forecast_id == P())
        params.append(forecast_id)
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
# 7. update-forecast-line
# ---------------------------------------------------------------------------
def update_forecast_line(conn, args):
    line_id = getattr(args, "forecast_line_id", None)
    if not line_id:
        err("--forecast-line-id is required")

    row = conn.execute(Q.from_(Table("forecast_line")).select(Table("forecast_line").star).where(Field("id") == P()).get_sql(), (line_id,)).fetchone()
    if not row:
        err(f"Forecast line {line_id} not found")

    line_data = row_to_dict(row)

    # Check parent forecast status
    parent = conn.execute(Q.from_(Table("forecast")).select(Field('status')).where(Field("id") == P()).get_sql(), (line_data["forecast_id"],)).fetchone()
    if parent and row_to_dict(parent)["status"] in ("locked", "archived"):
        err("Cannot update lines on a forecast that is locked or archived")

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

    # Handle money fields and recalculate variance
    forecast_amount = getattr(args, "forecast_amount", None)
    actual_amount = getattr(args, "actual_amount", None)

    fa = to_decimal(forecast_amount) if forecast_amount is not None else to_decimal(line_data["forecast_amount"])
    aa = to_decimal(actual_amount) if actual_amount is not None else to_decimal(line_data["actual_amount"])

    if forecast_amount is not None:
        data["forecast_amount"] = str(round_currency(fa))
        changed.append("forecast_amount")

    if actual_amount is not None:
        data["actual_amount"] = str(round_currency(aa))
        changed.append("actual_amount")

    # Recalculate variance if either amount changed
    if forecast_amount is not None or actual_amount is not None:
        variance = aa - fa
        variance_pct = (str(round_currency((variance / fa) * Decimal("100")))
                        if fa != Decimal("0") else "0")
        data["variance"] = str(round_currency(variance))
        data["variance_pct"] = variance_pct
        changed.extend(["variance", "variance_pct"])

    if not changed:
        err("No fields to update")

    sql, params = dynamic_update("forecast_line", data, {"id": line_id})
    conn.execute(sql, params)
    audit(conn, SKILL_NAME, "planning-update-forecast-line", "forecast_line", line_id)
    conn.commit()
    ok({"id": line_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# 8. lock-forecast
# ---------------------------------------------------------------------------
def lock_forecast(conn, args):
    forecast_id = getattr(args, "forecast_id", None)
    if not forecast_id:
        err("--forecast-id is required")

    row = conn.execute(Q.from_(Table("forecast")).select(Field('status')).where(Field("id") == P()).get_sql(), (forecast_id,)).fetchone()
    if not row:
        err(f"Forecast {forecast_id} not found")

    current_status = row_to_dict(row)["status"]
    if current_status == "locked":
        err("Forecast is already locked")
    if current_status == "archived":
        err("Cannot lock an archived forecast")

    sql = update_row("forecast",
                     data={"status": P(), "updated_at": LiteralValue("datetime('now')")},
                     where={"id": P()})
    conn.execute(sql, ("locked", forecast_id))
    audit(conn, SKILL_NAME, "planning-lock-forecast", "forecast", forecast_id)
    conn.commit()
    ok({"id": forecast_id, "forecast_status": "locked"})


# ---------------------------------------------------------------------------
# 9. calculate-variance
# ---------------------------------------------------------------------------
def calculate_variance(conn, args):
    forecast_id = getattr(args, "forecast_id", None)
    if not forecast_id:
        err("--forecast-id is required")

    row = conn.execute(Q.from_(Table("forecast")).select(Field('id')).where(Field("id") == P()).get_sql(), (forecast_id,)).fetchone()
    if not row:
        err(f"Forecast {forecast_id} not found")

    lines = conn.execute(Q.from_(Table("forecast_line")).select(Table("forecast_line").star).where(Field("forecast_id") == P()).get_sql(), (forecast_id,)).fetchall()

    updated = 0
    for l in lines:
        ld = row_to_dict(l)
        fa = to_decimal(ld["forecast_amount"])
        aa = to_decimal(ld["actual_amount"])
        variance = aa - fa
        variance_pct = (str(round_currency((variance / fa) * Decimal("100")))
                        if fa != Decimal("0") else "0")

        upd_sql = update_row("forecast_line",
                             data={"variance": P(), "variance_pct": P()},
                             where={"id": P()})
        conn.execute(upd_sql, (str(round_currency(variance)), variance_pct, ld["id"]))
        updated += 1

    conn.commit()
    ok({"forecast_id": forecast_id, "lines_updated": updated})


# ---------------------------------------------------------------------------
# 10. forecast-accuracy-report
# ---------------------------------------------------------------------------
def forecast_accuracy_report(conn, args):
    forecast_id = getattr(args, "forecast_id", None)
    if not forecast_id:
        err("--forecast-id is required")

    row = conn.execute(Q.from_(Table("forecast")).select(Table("forecast").star).where(Field("id") == P()).get_sql(), (forecast_id,)).fetchone()
    if not row:
        err(f"Forecast {forecast_id} not found")

    forecast_data = row_to_dict(row)

    lines = conn.execute(Q.from_(Table("forecast_line")).select(Table("forecast_line").star).where(Field("forecast_id") == P()).get_sql(), (forecast_id,)).fetchall()

    if not lines:
        ok({
            "forecast_id": forecast_id,
            "name": forecast_data["name"],
            "line_count": 0,
            "average_absolute_variance_pct": "0",
            "lines_with_variance": [],
        })
        return

    total_abs_var_pct = Decimal("0")
    lines_with_data = 0
    line_details = []

    for l in lines:
        ld = row_to_dict(l)
        fa = to_decimal(ld["forecast_amount"])
        aa = to_decimal(ld["actual_amount"])
        variance = aa - fa
        abs_var_pct = Decimal("0")

        if fa != Decimal("0"):
            abs_var_pct = abs((variance / fa) * Decimal("100"))
            total_abs_var_pct += abs_var_pct
            lines_with_data += 1

        line_details.append({
            "period": ld["period"],
            "account_name": ld["account_name"],
            "forecast_amount": ld["forecast_amount"],
            "actual_amount": ld["actual_amount"],
            "variance": str(round_currency(variance)),
            "absolute_variance_pct": str(round_currency(abs_var_pct)),
        })

    avg_abs_var = (round_currency(total_abs_var_pct / Decimal(str(lines_with_data)))
                   if lines_with_data > 0 else Decimal("0"))

    ok({
        "forecast_id": forecast_id,
        "name": forecast_data["name"],
        "line_count": len(lines),
        "lines_with_forecast": lines_with_data,
        "average_absolute_variance_pct": str(avg_abs_var),
        "lines_with_variance": line_details,
    })


# ---------------------------------------------------------------------------
# Action Router
# ---------------------------------------------------------------------------
ACTIONS = {
    "planning-add-forecast": add_forecast,
    "planning-update-forecast": update_forecast,
    "planning-get-forecast": get_forecast,
    "planning-list-forecasts": list_forecasts,
    "planning-add-forecast-line": add_forecast_line,
    "planning-list-forecast-lines": list_forecast_lines,
    "planning-update-forecast-line": update_forecast_line,
    "planning-lock-forecast": lock_forecast,
    "planning-calculate-variance": calculate_variance,
    "planning-forecast-accuracy-report": forecast_accuracy_report,
}
