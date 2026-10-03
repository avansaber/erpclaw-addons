"""Money-exactness tests for Stripe revenue-recognition aggregates.

Covers stripe-rev-rec-status and stripe-handle-subscription-change (cancel)
on a 9.99 monthly plan built only through the owning actions, with no direct
writes to the schedule tables. After three periods are recognised the exact
totals are 29.97 recognised and 89.91 deferred from a 119.88 contract. The
previous float-based sums raised a type error on this backend for any schedule
with cents, because a float reached the Decimal conversion. All reads here go
through PyPika with bound parameters.
"""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_test_helpers import (
    call_action, ns, is_ok,
    build_gl_ready_env, seed_subscription, seed_gl_account,
    get_conn,
)
from erpclaw_lib.query import Q, P, Table
from rev_rec import ACTIONS as REV_REC_ACTIONS


def _exact_env(conn):
    """GL-ready environment plus a revenue income account for recognition."""
    env = build_gl_ready_env(conn)
    env["revenue_account_id"] = seed_gl_account(
        conn, env["company_id"],
        name="Subscription Revenue", root_type="income",
        account_type="revenue")
    return env


def _create_schedule(conn, env, sub_id):
    """Create a 12 x 9.99 schedule through the owning action only."""
    seed_subscription(
        conn, env["stripe_account_id"], env["company_id"],
        stripe_id=sub_id, customer_stripe_id="cus_exact_001",
        plan_amount="9.99", plan_interval="month", status="active")
    sched = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
        stripe_account_id=env["stripe_account_id"],
        subscription_stripe_id=sub_id,
        company_id=env["company_id"]))
    assert is_ok(sched), f"Expected ok: {sched}"
    return sched


def _period_dates(conn, obligation_id):
    """Read the 12 schedule period dates through PyPika, oldest first."""
    sched_t = Table("advacct_revenue_schedule")
    rows = conn.execute(
        Q.from_(sched_t).select(sched_t.id, sched_t.period_date)
        .where(sched_t.obligation_id == P())
        .orderby(sched_t.period_date).get_sql(),
        (obligation_id,)).fetchall()
    assert len(rows) == 12
    return [row["period_date"] for row in rows]


def _recognize_first_periods(conn, env, period_dates, count=3):
    """Recognise the first N periods through the owning action only."""
    for period_date in period_dates[:count]:
        rec = call_action(
            REV_REC_ACTIONS["stripe-recognize-subscription-revenue"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                company_id=env["company_id"],
                revenue_account_id=env["revenue_account_id"],
                period_date=period_date,
                cost_center_id=env["cost_center_id"]))
        assert is_ok(rec), f"Expected ok: {rec}"


class TestRevRecStatusExact:

    def test_rev_rec_status_exact(self, conn):
        env = _exact_env(conn)
        sched = _create_schedule(conn, env, "sub_exact_status")
        assert sched["total_contract_value"] == "119.88"
        period_dates = _period_dates(conn, sched["obligation_id"])
        _recognize_first_periods(conn, env, period_dates, 3)

        result = call_action(REV_REC_ACTIONS["stripe-rev-rec-status"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            company_id=env["company_id"]))
        assert is_ok(result), f"Expected ok: {result}"
        assert result["subscription_count"] == 1
        assert result["total_contract_value"] == "119.88"
        assert result["total_recognized"] == "29.97"
        assert result["total_deferred"] == "89.91"
        sub = result["subscriptions"][0]
        assert sub["subscription_stripe_id"] == "sub_exact_status"
        assert sub["total_contract_value"] == "119.88"
        assert sub["recognized_to_date"] == "29.97"
        assert sub["remaining_deferred"] == "89.91"


class TestHandleSubscriptionChangeCancelExact:

    def test_cancel_reports_exact_remaining(self, conn, db_path):
        env = _exact_env(conn)
        sched = _create_schedule(conn, env, "sub_exact_cancel")
        assert sched["total_contract_value"] == "119.88"
        period_dates = _period_dates(conn, sched["obligation_id"])
        _recognize_first_periods(conn, env, period_dates, 3)

        result = call_action(
            REV_REC_ACTIONS["stripe-handle-subscription-change"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                subscription_stripe_id="sub_exact_cancel",
                company_id=env["company_id"],
                change_type="cancel"))
        assert is_ok(result), f"Expected ok: {result}"
        assert result["change_type"] == "cancel"
        assert result["contract_status"] == "terminated"
        assert result["unrecognized_entries_remaining"] == 9
        assert result["unrecognized_amount_remaining"] == "89.91"

        fresh = get_conn(db_path)
        try:
            contract_t = Table("advacct_revenue_contract")
            contract = fresh.execute(
                Q.from_(contract_t).select(contract_t.contract_status)
                .where(contract_t.id == P()).get_sql(),
                (result["contract_id"],)).fetchone()
            assert contract["contract_status"] == "terminated"
        finally:
            fresh.close()
