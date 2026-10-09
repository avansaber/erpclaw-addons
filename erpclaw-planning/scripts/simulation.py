"""ERPClaw Planning: twenty-four month business simulation report v1.

Deterministic 24-month cash forecast from caller-supplied inputs.
Read-only: performs one SELECT to verify the company exists and
otherwise computes purely from arguments. No tables are written,
no scenarios are stored, no audit rows are emitted.

Growth-rate convention (explicit percentage parsing):
  bare numbers are percent ("5" means 5%); a trailing "%" is also
  accepted ("5%" means 5%). Documented bounds are -100% to +100%
  inclusive. Anything outside that range is refused.

Money convention: Decimal throughout, monthly currency rounding
(ROUND_HALF_UP to cents). Never float.
"""
import os
import re
import sys
from decimal import Decimal, DecimalException, Inexact, InvalidOperation, localcontext

try:
    import importlib.util
    if importlib.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.response import ok, err
    from erpclaw_lib.query import Field, P, Q, Table
except ImportError:
    pass

REPORT_MONTHS = 24

RATE_MIN_PCT = Decimal("-100")
RATE_MAX_PCT = Decimal("100")

LIMITATION = (
    "Deterministic forecast scenario computed from caller-supplied inputs; "
    "not a record of a real business."
)

_MONTH_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


def _parse_money(raw, flag):
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        err("%s is required" % flag)
    if isinstance(raw, float):
        err("Invalid %s %r: must be a Decimal string, not float" % (flag, raw))
    try:
        parsed = to_decimal(raw)
        if not parsed.is_finite():
            err("Invalid %s %r: must be finite" % (flag, raw))
        return round_currency(parsed)
    except (ValueError, TypeError, DecimalException):
        err("Invalid %s %r: must be a valid Decimal" % (flag, raw))


def _add_money(left, right):
    """Refuse a currency sum that loses cents or cannot be represented."""
    with localcontext() as context:
        context.traps[Inexact] = True
        return round_currency(left + right)


def _parse_rate(raw, flag):
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        return Decimal("0"), Decimal("0")
    if isinstance(raw, float):
        err("Invalid %s %r: must be a Decimal string, not float" % (flag, raw))
    text = str(raw).strip()
    if text.endswith("%"):
        text = text[:-1].strip()
    if text == "":
        err("Invalid %s %r: must be a valid percentage" % (flag, raw))
    try:
        pct = to_decimal(text)
    except (ValueError, TypeError, InvalidOperation):
        err("Invalid %s %r: must be a valid percentage" % (flag, raw))
    if not pct.is_finite():
        err("Invalid %s %r: must be finite" % (flag, raw))
    if pct < RATE_MIN_PCT or pct > RATE_MAX_PCT:
        err("Invalid %s %r: outside documented bounds (-100%% to +100%%)" % (flag, raw))
    return pct / Decimal("100"), pct


def _validate_month(raw):
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        err("--start-month is required")
    text = str(raw).strip()
    match = _MONTH_RE.match(text)
    if not match:
        err("Invalid --start-month '%s': expected YYYY-MM" % raw)
    return int(match.group(1)), int(match.group(2)), text


def _add_months(year, month, offset):
    total = (month - 1) + offset
    return year + total // 12, (total % 12) + 1


def business_simulation_report(conn, args):
    try:
        _business_simulation_report(conn, args)
    except DecimalException:
        err("Simulation amounts exceed representable Decimal currency arithmetic")


def _business_simulation_report(conn, args):
    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")
    raw_start = getattr(args, "start_month", None)
    raw_cash = getattr(args, "starting_cash", None)
    raw_rev = getattr(args, "monthly_revenue", None)
    raw_exp = getattr(args, "monthly_expense", None)
    raw_rev_rate = getattr(args, "revenue_growth_rate", None)
    raw_exp_rate = getattr(args, "expense_growth_rate", None)

    if raw_cash is None:
        err("--starting-cash is required")
    if raw_rev is None:
        err("--monthly-revenue is required")
    if raw_exp is None:
        err("--monthly-expense is required")

    year, month, start_month = _validate_month(raw_start)

    found = conn.execute(
        Q.from_(Table("company")).select(Field("id")).where(Field("id") == P()).get_sql(),
        (company_id,)).fetchone()
    if not found:
        err("Company %s not found" % company_id)

    start_cash = _parse_money(raw_cash, "--starting-cash")
    base_rev = _parse_money(raw_rev, "--monthly-revenue")
    base_exp = _parse_money(raw_exp, "--monthly-expense")
    rev_frac, rev_pct = _parse_rate(raw_rev_rate, "--revenue-growth-rate")
    exp_frac, exp_pct = _parse_rate(raw_exp_rate, "--expense-growth-rate")

    rev_factor = Decimal("1") + rev_frac
    exp_factor = Decimal("1") + exp_frac

    months = []
    total_rev = Decimal("0")
    total_exp = Decimal("0")
    minimum_cash = None
    first_negative = None

    opening = start_cash
    cur_rev = base_rev
    cur_exp = base_exp
    for index in range(REPORT_MONTHS):
        y, m = _add_months(year, month, index)
        period = "%04d-%02d" % (y, m)
        revenue = round_currency(cur_rev)
        expense = round_currency(cur_exp)
        net = _add_money(revenue, -expense)
        closing = _add_money(opening, net)
        row = {
            "month": period,
            "opening_cash": str(opening),
            "revenue": str(revenue),
            "expense": str(expense),
            "net_change": str(net),
            "closing_cash": str(closing),
        }
        months.append(row)
        total_rev = _add_money(total_rev, revenue)
        total_exp = _add_money(total_exp, expense)
        if minimum_cash is None or closing < minimum_cash:
            minimum_cash = closing
        if closing < Decimal("0") and first_negative is None:
            first_negative = period
        opening = closing
        cur_rev = revenue * rev_factor
        cur_exp = expense * exp_factor

    total_net = _add_money(total_rev, -total_exp)
    ok({
        "company_id": company_id,
        "start_month": start_month,
        "starting_cash": str(start_cash),
        "monthly_revenue": str(base_rev),
        "monthly_expense": str(base_exp),
        "revenue_growth_rate": "%s%%" % str(rev_pct),
        "expense_growth_rate": "%s%%" % str(exp_pct),
        "months": months,
        "month_count": REPORT_MONTHS,
        "total_revenue": str(round_currency(total_rev)),
        "total_expense": str(round_currency(total_exp)),
        "total_net_change": str(round_currency(total_net)),
        "minimum_cash": str(minimum_cash),
        "first_negative_cash_month": first_negative,
        "limitation": LIMITATION,
    })


ACTIONS = {
    "planning-business-simulation-report": business_simulation_report,
}
