"""ERPClaw Integrations Stripe -- ASC 606 Revenue Recognition bridge.

4 actions bridging Stripe subscriptions to erpclaw-accounting-adv:
  stripe-create-rev-rec-schedule   -- create contract + obligation + schedule from subscription
  stripe-recognize-subscription-revenue -- batch recognize earned revenue for a period
  stripe-rev-rec-status            -- report on ASC 606 status for all subscriptions
  stripe-handle-subscription-change -- handle cancel/upgrade/downgrade

Writes to advacct_revenue_contract, advacct_performance_obligation,
advacct_revenue_schedule are delegated to erpclaw-accounting-adv via
cross_skill.call_skill_action() (Article 5 compliance).  READs are direct.

Imported by db_query.py (unified router).
"""
import argparse
import io
import json
import os
import sys
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.gl_posting import insert_gl_entries
    from erpclaw_lib.cross_skill import call_skill_action, CrossSkillError
    from erpclaw_lib.query import (
        Q, P, Table, Field, fn, Order,
        insert_row, update_row, dynamic_update,
    )
except ImportError:
    pass

# Add scripts directory to path for sibling imports
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_helpers import (
    SKILL, now_iso, validate_stripe_account,
)


# ---------------------------------------------------------------------------
# Cross-module delegation helpers (Article 5 compliance)
# ---------------------------------------------------------------------------

def _delegate_revenue_action(conn, action, args_dict):
    """Delegate a write action to erpclaw-accounting-adv (the advacct table owner).

    Tries call_skill_action() (subprocess) first for production.
    Falls back to direct function import for dev/test environments where
    the erpclaw skill is not installed at ~/clawd/skills/.

    Args:
        conn: Database connection (used for fallback path).
        action: The erpclaw action name (e.g. 'add-revenue-contract').
        args_dict: Dict of action parameters (Python names, no -- prefix).

    Returns:
        Parsed JSON response dict from the delegated action.

    Raises:
        SystemExit via err() on failure.
    """
    # Build CLI args dict with -- prefixes for call_skill_action
    cli_args = {}
    for key, value in args_dict.items():
        cli_key = f"--{key.replace('_', '-')}"
        cli_args[cli_key] = value

    db_path = os.environ.get("ERPCLAW_DB_PATH")

    try:
        result = call_skill_action(
            "erpclaw", action, args=cli_args, db_path=db_path, timeout=30,
        )
        return result
    except (CrossSkillError, Exception):
        # Fallback: call revenue.py functions directly (dev/test environment)
        return _direct_call_revenue_action(conn, action, args_dict)


def _direct_call_revenue_action(conn, action, args_dict):
    """Call erpclaw-accounting-adv revenue.py functions directly (in-process).

    Used as fallback when call_skill_action() cannot resolve the erpclaw skill
    (e.g. local dev/test where ~/clawd/skills/erpclaw doesn't exist).

    Captures stdout to prevent ok()/err() from corrupting the caller's output.
    """
    # Lazy import to avoid circular dependencies at module level
    import importlib.util

    # Find revenue.py relative to the project structure
    revenue_path = _find_revenue_module()
    if not revenue_path:
        err(f"Cannot find erpclaw-accounting-adv revenue.py for action '{action}'")

    spec = importlib.util.spec_from_file_location("_revenue_delegate", revenue_path)
    revenue_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(revenue_mod)

    if action not in revenue_mod.ACTIONS:
        err(f"Action '{action}' not found in erpclaw-accounting-adv revenue module")

    fn = revenue_mod.ACTIONS[action]

    # Build an argparse.Namespace from args_dict
    ns = argparse.Namespace(**args_dict)
    # Add defaults that revenue.py expects
    if not hasattr(ns, "limit"):
        ns.limit = 20
    if not hasattr(ns, "offset"):
        ns.offset = 0

    # Wrap raw sqlite3.Connection in ConnectionWrapper if needed.
    # Revenue functions set conn.company_id for naming series, which requires
    # ConnectionWrapper (Python 3.12+ disallows arbitrary attrs on Connection).
    import sqlite3 as _sqlite3
    if isinstance(conn, _sqlite3.Connection):
        from erpclaw_lib.db import ConnectionWrapper
        wrapped_conn = ConnectionWrapper(conn)
    else:
        wrapped_conn = conn

    # Capture stdout since revenue functions call ok() which prints and exits
    buf = io.StringIO()
    old_stdout = sys.stdout

    def _fake_exit(code=0):
        raise SystemExit(code)

    old_exit = sys.exit
    try:
        sys.stdout = buf
        sys.exit = _fake_exit
        fn(wrapped_conn, ns)
    except SystemExit:
        pass
    finally:
        sys.stdout = old_stdout
        sys.exit = old_exit

    output = buf.getvalue().strip()
    if not output:
        err(f"No output from delegated action '{action}'")

    result = json.loads(output)
    if result.get("status") == "error":
        err(f"Delegated action '{action}' failed: {result.get('message', 'unknown')}")

    return result


def _find_revenue_module():
    """Locate erpclaw-accounting-adv/revenue.py in the project source tree."""
    # Check common locations relative to this file and project root
    candidates = [
        # From src/erpclaw-addons/erpclaw-integrations-stripe/scripts/ -> src/erpclaw/scripts/erpclaw-accounting-adv/revenue.py
        os.path.join(os.path.dirname(_SCRIPTS_DIR), "..", "..", "erpclaw", "scripts",
                     "erpclaw-accounting-adv", "revenue.py"),
        # Server install path
        os.path.expanduser("~/clawd/skills/erpclaw/scripts/erpclaw-accounting-adv/revenue.py"),
    ]
    for candidate in candidates:
        path = os.path.normpath(candidate)
        if os.path.exists(path):
            return path
    return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _today():
    """Return today's date as YYYY-MM-DD string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _resolve_cost_center_id(conn, company_id, explicit_cc_id=None):
    """Resolve cost_center_id: use explicit value if given, else company default."""
    if explicit_cc_id:
        return explicit_cc_id

    t = Table("company")
    row = conn.execute(
        Q.from_(t).select(t.default_cost_center_id)
        .where(t.id == P()).get_sql(),
        (company_id,)
    ).fetchone()

    if row and row["default_cost_center_id"]:
        return row["default_cost_center_id"]

    err("No cost_center_id provided and company has no default_cost_center_id. "
        "Set a default cost center on the company or pass --cost-center-id.")


def _get_stripe_account_gl(conn, stripe_account_id):
    """Load the stripe_account row and return GL account mapping dict."""
    t = Table("stripe_account")
    row = conn.execute(
        Q.from_(t).select(
            t.company_id,
            t.stripe_clearing_account_id,
            t.stripe_fees_account_id,
            t.stripe_payout_account_id,
            t.dispute_expense_account_id,
            t.unearned_revenue_account_id,
            t.platform_revenue_account_id,
        ).where(t.id == P()).get_sql(),
        (stripe_account_id,)
    ).fetchone()
    if not row:
        err(f"Stripe account {stripe_account_id} not found")
    return dict(row)


def _create_journal_entry(conn, company_id, posting_date, total_amount,
                          entry_type="journal", remark=None, currency="USD"):
    """Insert a journal_entry row and return its ID.

    `currency` is the transaction currency for the JE (ISO 4217). Defaults
    to USD for backward compatibility. exchange_rate is always "1": ERPClaw
    books in transaction currency and never converts.
    """
    je_id = str(uuid.uuid4())
    now = now_iso()

    sql, _ = insert_row("journal_entry", {
        "id": P(), "posting_date": P(), "entry_type": P(),
        "total_debit": P(), "total_credit": P(),
        "currency": P(), "exchange_rate": P(), "remark": P(),
        "status": P(), "company_id": P(),
        "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        je_id, posting_date, entry_type,
        str(round_currency(total_amount)), str(round_currency(total_amount)),
        (currency or "USD").upper(), "1", remark or "",
        "submitted", company_id,
        now, now,
    ))
    return je_id


def _calculate_months(start_dt, end_dt):
    """Calculate inclusive month count between two dates."""
    months = (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month) + 1
    return max(months, 1)


# ---------------------------------------------------------------------------
# 1. stripe-create-rev-rec-schedule
# ---------------------------------------------------------------------------
def create_rev_rec_schedule(conn, args):
    """Create an ASC 606 revenue contract + obligation + schedule from a Stripe subscription.

    Links the stripe_subscription to the advacct_revenue_contract via
    the erpclaw_revenue_contract_id FK.

    For monthly subscriptions: 12 schedule entries, each = plan_amount
    For annual subscriptions: 12 schedule entries, each = plan_amount/12

    Writes to advacct_* tables are delegated to erpclaw-accounting-adv
    via cross_skill (Article 5 compliance).
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    subscription_stripe_id = getattr(args, "subscription_stripe_id", None)
    if not subscription_stripe_id:
        err("--subscription-stripe-id is required")

    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")

    # 1. Read stripe subscription
    sub_t = Table("stripe_subscription")
    sub = conn.execute(
        Q.from_(sub_t).select("*")
        .where(sub_t.stripe_account_id == P())
        .where(sub_t.stripe_id == P())
        .get_sql(),
        (stripe_account_id, subscription_stripe_id)
    ).fetchone()
    if not sub:
        err(f"Subscription {subscription_stripe_id} not found")

    sub = dict(sub)

    # Validate status
    if sub["status"] not in ("active", "trialing"):
        err(f"Subscription status must be 'active' or 'trialing', got '{sub['status']}'")

    # Validate not already linked
    if sub["erpclaw_revenue_contract_id"]:
        err(f"Subscription {subscription_stripe_id} already linked to contract "
            f"{sub['erpclaw_revenue_contract_id']}")

    plan_amount = to_decimal(sub["plan_amount"])
    plan_interval = sub["plan_interval"] or "month"

    if plan_amount <= Decimal("0"):
        err("Subscription plan_amount must be greater than zero")

    # 2. Resolve customer name from stripe_customer_map
    customer_name = "Stripe Customer"
    if sub["customer_stripe_id"]:
        cmap_t = Table("stripe_customer_map")
        cmap = conn.execute(
            Q.from_(cmap_t).select(cmap_t.stripe_name)
            .where(cmap_t.stripe_account_id == P())
            .where(cmap_t.stripe_customer_id == P())
            .get_sql(),
            (stripe_account_id, sub["customer_stripe_id"])
        ).fetchone()
        if cmap and cmap["stripe_name"]:
            customer_name = cmap["stripe_name"]

    # 3. Calculate contract dates and total value
    now = now_iso()
    today_str = _today()

    # Use current_period_start if available, else today
    start_date_str = sub.get("current_period_start")
    if start_date_str:
        # Parse ISO datetime to date
        try:
            start_dt = datetime.fromisoformat(start_date_str.replace("Z", "+00:00")).date()
        except (ValueError, AttributeError):
            start_dt = date.fromisoformat(today_str)
    else:
        start_dt = date.fromisoformat(today_str)

    start_date = start_dt.isoformat()

    # Calculate schedule: always 12 months
    schedule_months = 12

    if plan_interval == "year":
        # Annual subscription: total_value = plan_amount, recognize over 12 months
        total_value = plan_amount
        monthly_amount = (total_value / Decimal(str(schedule_months))).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP)
    else:
        # Monthly subscription: total_value = plan_amount * 12
        total_value = plan_amount * Decimal(str(schedule_months))
        monthly_amount = plan_amount

    # End date = start + 12 months - 1 day
    end_year = start_dt.year + (start_dt.month + schedule_months - 2) // 12
    end_month = (start_dt.month + schedule_months - 2) % 12 + 1
    # Last day of the (start_month + 11) month
    import calendar
    end_day = calendar.monthrange(end_year, end_month)[1]
    end_dt = date(end_year, end_month, min(start_dt.day, end_day))
    end_date = end_dt.isoformat()

    # 4. Create revenue contract via erpclaw-accounting-adv (Article 5)
    contract_result = _delegate_revenue_action(conn, "add-revenue-contract", {
        "company_id": company_id,
        "customer_name": customer_name,
        "contract_number": subscription_stripe_id,
        "start_date": start_date,
        "end_date": end_date,
        "total_value": str(round_currency(total_value)),
    })
    contract_id = contract_result["id"]

    # Activate the contract (add-revenue-contract creates as draft)
    _delegate_revenue_action(conn, "update-revenue-contract", {
        "id": contract_id,
        "contract_status": "active",
        "total_value": str(round_currency(total_value)),
    })

    # 5. Create performance obligation via erpclaw-accounting-adv (Article 5)
    ob_name = f"SaaS access - {plan_interval}"
    obligation_result = _delegate_revenue_action(conn, "add-performance-obligation", {
        "contract_id": contract_id,
        "company_id": company_id,
        "name": ob_name,
        "standalone_price": str(round_currency(total_value)),
        "recognition_method": "over_time",
        "recognition_basis": "time",
    })
    obligation_id = obligation_result["id"]

    # 6. Generate revenue schedule via erpclaw-accounting-adv (Article 5)
    _delegate_revenue_action(conn, "calculate-revenue-schedule", {
        "obligation_id": obligation_id,
    })

    # Verify schedule entry count
    sched_t = Table("advacct_revenue_schedule")
    entries = conn.execute(
        Q.from_(sched_t).select(fn.Count("*").as_("cnt"))
        .where(sched_t.obligation_id == P())
        .get_sql(),
        (obligation_id,)
    ).fetchone()
    entries_created = entries["cnt"] if entries else 0

    # 7. Link subscription to contract (stripe_subscription is OUR table)
    conn.execute(
        "UPDATE stripe_subscription SET erpclaw_revenue_contract_id = ? WHERE id = ?",
        (contract_id, sub["id"])
    )

    audit(conn, SKILL, "stripe-create-rev-rec-schedule", "stripe_subscription",
          sub["id"], new_values={
              "contract_id": contract_id,
              "obligation_id": obligation_id,
              "schedule_entries": entries_created,
          })
    conn.commit()

    ok({
        "contract_id": contract_id,
        "obligation_id": obligation_id,
        "schedule_entry_count": entries_created,
        "monthly_recognition_amount": str(round_currency(monthly_amount)),
        "total_contract_value": str(round_currency(total_value)),
        "start_date": start_date,
        "end_date": end_date,
        "plan_interval": plan_interval,
    })


# ---------------------------------------------------------------------------
# 2. stripe-recognize-subscription-revenue
# ---------------------------------------------------------------------------
def recognize_subscription_revenue(conn, args):
    """Batch recognize earned subscription revenue for a period.

    For each subscription with an ASC 606 contract:
    - Find unrecognized schedule entries for the given period
    - Post GL: DR Unearned Revenue, CR Revenue
    - Mark schedule entries as recognized via erpclaw-accounting-adv

    Requires --revenue-account-id (the income account for recognized revenue).
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")

    revenue_account_id = getattr(args, "revenue_account_id", None)
    if not revenue_account_id:
        err("--revenue-account-id is required (the income account for recognized revenue)")

    period_date = getattr(args, "period_date", None)
    if not period_date:
        # Default to first of current month
        today = date.today()
        period_date = f"{today.year}-{today.month:02d}-01"

    # Ensure period_date is first of month for matching
    try:
        pd = date.fromisoformat(period_date)
        period_match = f"{pd.year}-{pd.month:02d}-01"
    except ValueError:
        err(f"Invalid period-date: {period_date}. Use YYYY-MM-DD format.")

    gl = _get_stripe_account_gl(conn, stripe_account_id)
    unearned_account_id = gl["unearned_revenue_account_id"]
    if not unearned_account_id:
        err("Unearned revenue account not configured on stripe account")

    # Auto-resolve cost_center_id for P&L accounts
    explicit_cc = getattr(args, "cost_center_id", None)
    cost_center_id = _resolve_cost_center_id(conn, company_id, explicit_cc)

    # Find all subscriptions with ASC 606 contracts
    sub_t = Table("stripe_subscription")
    subs = conn.execute(
        Q.from_(sub_t).select(
            sub_t.id, sub_t.stripe_id, sub_t.erpclaw_revenue_contract_id,
            sub_t.currency
        )
        .where(sub_t.stripe_account_id == P())
        .where(sub_t.erpclaw_revenue_contract_id.isnotnull())
        .get_sql(),
        (stripe_account_id,)
    ).fetchall()

    total_recognized = Decimal("0")
    gl_entries_created = 0
    subscriptions_processed = 0

    for sub_row in subs:
        sub = dict(sub_row)
        contract_id = sub["erpclaw_revenue_contract_id"]
        # Subscription's transaction currency. ERPClaw books rev rec in
        # the subscription's currency directly (no conversion).
        sub_currency = (sub.get("currency") or "USD").upper()

        # Find obligations for this contract (READ is allowed)
        ob_t = Table("advacct_performance_obligation")
        obligations = conn.execute(
            Q.from_(ob_t).select(ob_t.id)
            .where(ob_t.contract_id == P())
            .get_sql(),
            (contract_id,)
        ).fetchall()

        sub_recognized = Decimal("0")
        sub_entries = 0

        for ob_row in obligations:
            ob_id = ob_row["id"]

            # Find unrecognized schedule entries for this period (READ is allowed)
            sched_t = Table("advacct_revenue_schedule")
            schedules = conn.execute(
                Q.from_(sched_t).select(sched_t.id, sched_t.amount)
                .where(sched_t.obligation_id == P())
                .where(sched_t.period_date == P())
                .where(sched_t.recognized == P())
                .get_sql(),
                (ob_id, period_match, 0)
            ).fetchall()

            if not schedules:
                continue

            for sched in schedules:
                sched_dict = dict(sched)
                amount = to_decimal(sched_dict["amount"])

                if amount <= Decimal("0"):
                    continue

                # Create journal entry as voucher
                je_id = _create_journal_entry(
                    conn, company_id, period_match, amount,
                    remark=f"ASC 606 revenue recognition - {sub['stripe_id']} - {period_match}",
                    currency=sub_currency,
                )

                # Post GL entries: DR Unearned Revenue, CR Revenue
                gl_entry_defs = [
                    {
                        "account_id": unearned_account_id,
                        "debit": str(round_currency(amount)),
                        "credit": "0",
                        "currency": sub_currency,
                        "exchange_rate": "1",
                    },
                    {
                        "account_id": revenue_account_id,
                        "debit": "0",
                        "credit": str(round_currency(amount)),
                        "cost_center_id": cost_center_id,
                        "currency": sub_currency,
                        "exchange_rate": "1",
                    },
                ]

                gl_ids = insert_gl_entries(
                    conn, gl_entry_defs,
                    voucher_type="journal_entry",
                    voucher_id=je_id,
                    posting_date=period_match,
                    company_id=company_id,
                    remarks=f"ASC 606 rev rec: {sub['stripe_id']}",
                )

                # Mark schedule entry as recognized via erpclaw-accounting-adv (Article 5)
                _delegate_revenue_action(conn, "recognize-schedule-entry", {
                    "id": sched_dict["id"],
                })

                sub_recognized += amount
                sub_entries += len(gl_ids)

        if sub_recognized > Decimal("0"):
            subscriptions_processed += 1
            total_recognized += sub_recognized
            gl_entries_created += sub_entries

    audit(conn, SKILL, "stripe-recognize-subscription-revenue",
          "stripe_account", stripe_account_id,
          new_values={
              "period": period_match,
              "subscriptions_processed": subscriptions_processed,
              "total_recognized": str(round_currency(total_recognized)),
          })
    conn.commit()

    ok({
        "period": period_match,
        "subscriptions_processed": subscriptions_processed,
        "total_recognized": str(round_currency(total_recognized)),
        "gl_entries_created": gl_entries_created,
    })


# ---------------------------------------------------------------------------
# 3. stripe-rev-rec-status
# ---------------------------------------------------------------------------
def rev_rec_status(conn, args):
    """Show ASC 606 revenue recognition status for all linked subscriptions.

    Reports: subscription ID, customer, plan, total contract value,
    recognized to date, remaining deferred.

    All queries are READs (allowed for any module).
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")

    # Find all subscriptions with ASC 606 contracts
    sub_t = Table("stripe_subscription")
    subs = conn.execute(
        Q.from_(sub_t).select("*")
        .where(sub_t.stripe_account_id == P())
        .where(sub_t.erpclaw_revenue_contract_id.isnotnull())
        .get_sql(),
        (stripe_account_id,)
    ).fetchall()

    results = []
    total_contract_value = Decimal("0")
    total_recognized = Decimal("0")
    total_deferred = Decimal("0")

    for sub_row in subs:
        sub = dict(sub_row)
        contract_id = sub["erpclaw_revenue_contract_id"]

        # Get contract details (READ)
        contract = conn.execute(
            "SELECT * FROM advacct_revenue_contract WHERE id = ?",
            (contract_id,)
        ).fetchone()
        if not contract:
            continue
        contract = dict(contract)

        contract_value = to_decimal(contract["total_value"])

        # Calculate recognized and deferred from schedule (READ)
        sched_rows = conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN rs.recognized = 1 THEN CAST(rs.amount AS NUMERIC) ELSE 0 END), 0) as recognized_amount,
                COALESCE(SUM(CASE WHEN rs.recognized = 0 THEN CAST(rs.amount AS NUMERIC) ELSE 0 END), 0) as deferred_amount
            FROM advacct_revenue_schedule rs
            JOIN advacct_performance_obligation po ON po.id = rs.obligation_id
            WHERE po.contract_id = ?
        """, (contract_id,)).fetchone()

        recognized = to_decimal(sched_rows["recognized_amount"]) if sched_rows else Decimal("0")
        deferred = to_decimal(sched_rows["deferred_amount"]) if sched_rows else Decimal("0")

        results.append({
            "subscription_stripe_id": sub["stripe_id"],
            "customer_stripe_id": sub["customer_stripe_id"],
            "customer_name": contract["customer_name"],
            "plan_interval": sub["plan_interval"],
            "plan_amount": sub["plan_amount"],
            "contract_id": contract_id,
            "contract_status": contract["contract_status"],
            "total_contract_value": str(round_currency(contract_value)),
            "recognized_to_date": str(round_currency(recognized)),
            "remaining_deferred": str(round_currency(deferred)),
        })

        total_contract_value += contract_value
        total_recognized += recognized
        total_deferred += deferred

    ok({
        "subscriptions": results,
        "subscription_count": len(results),
        "total_contract_value": str(round_currency(total_contract_value)),
        "total_recognized": str(round_currency(total_recognized)),
        "total_deferred": str(round_currency(total_deferred)),
    })


# ---------------------------------------------------------------------------
# 4. stripe-handle-subscription-change
# ---------------------------------------------------------------------------
def handle_subscription_change(conn, args):
    """Handle subscription changes: cancel, upgrade, or downgrade.

    For cancel: mark contract as terminated, recognize remaining if service was provided.
    For upgrade/downgrade: update contract total_value, recalculate remaining schedule entries.

    All writes to advacct_* tables are delegated to erpclaw-accounting-adv
    via cross_skill (Article 5 compliance).
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")
    validate_stripe_account(conn, stripe_account_id)

    subscription_stripe_id = getattr(args, "subscription_stripe_id", None)
    if not subscription_stripe_id:
        err("--subscription-stripe-id is required")

    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")

    change_type = getattr(args, "change_type", None)
    if not change_type:
        err("--change-type is required (cancel, upgrade, or downgrade)")
    if change_type not in ("cancel", "upgrade", "downgrade"):
        err(f"Invalid change-type: {change_type}. Must be cancel, upgrade, or downgrade.")

    # Find subscription
    sub_t = Table("stripe_subscription")
    sub = conn.execute(
        Q.from_(sub_t).select("*")
        .where(sub_t.stripe_account_id == P())
        .where(sub_t.stripe_id == P())
        .get_sql(),
        (stripe_account_id, subscription_stripe_id)
    ).fetchone()
    if not sub:
        err(f"Subscription {subscription_stripe_id} not found")

    sub = dict(sub)
    contract_id = sub["erpclaw_revenue_contract_id"]
    if not contract_id:
        err(f"Subscription {subscription_stripe_id} has no linked revenue contract. "
            f"Run stripe-create-rev-rec-schedule first.")

    # Get contract (READ is allowed)
    contract = conn.execute(
        "SELECT * FROM advacct_revenue_contract WHERE id = ?",
        (contract_id,)
    ).fetchone()
    if not contract:
        err(f"Revenue contract {contract_id} not found")
    contract = dict(contract)

    now = now_iso()

    if change_type == "cancel":
        # Mark contract as terminated via erpclaw-accounting-adv (Article 5)
        _delegate_revenue_action(conn, "update-revenue-contract", {
            "id": contract_id,
            "contract_status": "terminated",
        })

        # Count remaining unrecognized entries (READ is allowed)
        remaining = conn.execute("""
            SELECT COUNT(*) as cnt, COALESCE(SUM(CAST(rs.amount AS NUMERIC)), 0) as total
            FROM advacct_revenue_schedule rs
            JOIN advacct_performance_obligation po ON po.id = rs.obligation_id
            WHERE po.contract_id = ? AND rs.recognized = 0
        """, (contract_id,)).fetchone()

        remaining_count = remaining["cnt"] if remaining else 0
        remaining_amount = str(round_currency(to_decimal(remaining["total"]))) if remaining else "0.00"

        audit(conn, SKILL, "stripe-handle-subscription-change",
              "stripe_subscription", sub["id"],
              new_values={"change_type": "cancel", "contract_id": contract_id,
                          "remaining_entries": remaining_count})
        conn.commit()

        ok({
            "change_type": "cancel",
            "subscription_stripe_id": subscription_stripe_id,
            "contract_id": contract_id,
            "contract_status": "terminated",
            "unrecognized_entries_remaining": remaining_count,
            "unrecognized_amount_remaining": remaining_amount,
        })

    else:
        # Upgrade/downgrade: update contract total_value, recalculate remaining
        new_plan_amount = getattr(args, "new_plan_amount", None)
        if not new_plan_amount:
            err("--new-plan-amount is required for upgrade/downgrade")

        new_amount = to_decimal(new_plan_amount)
        if new_amount <= Decimal("0"):
            err("new-plan-amount must be greater than zero")

        plan_interval = sub["plan_interval"] or "month"

        # Calculate new total value for remaining period
        if plan_interval == "year":
            new_total = new_amount
            new_monthly = (new_amount / Decimal("12")).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP)
        else:
            new_total = new_amount * Decimal("12")
            new_monthly = new_amount

        # Get obligation IDs for this contract (READ is allowed)
        ob_t = Table("advacct_performance_obligation")
        ob_ids = conn.execute(
            Q.from_(ob_t).select(ob_t.id)
            .where(ob_t.contract_id == P())
            .get_sql(),
            (contract_id,)
        ).fetchall()

        # Count unrecognized entries before update (READ)
        entries_updated = 0
        for ob_row in ob_ids:
            ob_id = ob_row["id"]
            sched_t = Table("advacct_revenue_schedule")
            unrec_count = conn.execute(
                Q.from_(sched_t).select(fn.Count("*").as_("cnt"))
                .where(sched_t.obligation_id == P())
                .where(sched_t.recognized == P())
                .get_sql(),
                (ob_id, 0)
            ).fetchone()
            entries_updated += unrec_count["cnt"] if unrec_count else 0

        # Update contract with new total value and modification count via
        # erpclaw-accounting-adv (Article 5)
        old_mod_count = contract["modification_count"]

        # Use modify-contract to increment modification_count and set status
        _delegate_revenue_action(conn, "modify-contract", {
            "id": contract_id,
        })

        # Update total_value
        _delegate_revenue_action(conn, "update-revenue-contract", {
            "id": contract_id,
            "total_value": str(round_currency(new_total)),
        })

        # Update obligation prices and schedule entries via
        # erpclaw-accounting-adv (Article 5)
        for ob_row in ob_ids:
            ob_id = ob_row["id"]

            # Update obligation standalone/allocated price
            _delegate_revenue_action(conn, "update-performance-obligation", {
                "id": ob_id,
                "standalone_price": str(round_currency(new_total)),
                "allocated_price": str(round_currency(new_total)),
            })

            # Update unrecognized schedule entries with new monthly amount
            _delegate_revenue_action(conn, "update-schedule-amounts", {
                "obligation_id": ob_id,
                "amount": str(round_currency(new_monthly)),
            })

        audit(conn, SKILL, "stripe-handle-subscription-change",
              "stripe_subscription", sub["id"],
              new_values={"change_type": change_type, "contract_id": contract_id,
                          "new_plan_amount": str(new_amount),
                          "entries_updated": entries_updated})
        conn.commit()

        ok({
            "change_type": change_type,
            "subscription_stripe_id": subscription_stripe_id,
            "contract_id": contract_id,
            "contract_status": "modified",
            "new_monthly_amount": str(round_currency(new_monthly)),
            "new_total_value": str(round_currency(new_total)),
            "schedule_entries_updated": entries_updated,
            "modification_count": old_mod_count + 1,
        })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "stripe-create-rev-rec-schedule": create_rev_rec_schedule,
    "stripe-recognize-subscription-revenue": recognize_subscription_revenue,
    "stripe-rev-rec-status": rev_rec_status,
    "stripe-handle-subscription-change": handle_subscription_change,
}
