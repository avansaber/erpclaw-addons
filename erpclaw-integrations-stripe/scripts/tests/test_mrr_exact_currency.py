"""Exact-currency tests for stripe-mrr-report.

Active-only revenue with exact Decimal math: annual is amount / 12,
monthly is amount as-is, day (x30) and week (x4.333) are preserved
approximations. Contributions sum unrounded; displayed totals round
only after grouping by currency and currency/interval. Currencies
group case-insensitively. Invalid active interval/currency/amount
refuse with an ordinary error. mrr_by_currency is authoritative.
"""
import os
import sys
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_test_helpers import (
    call_action, ns, is_ok, is_error,
    build_stripe_env, seed_subscription,
)
from reports import ACTIONS as REPORTS_ACTIONS

MRR = REPORTS_ACTIONS["stripe-mrr-report"]


def _report(conn, acct_id):
    return call_action(MRR, conn, ns(stripe_account_id=acct_id))


def _snapshot(conn):
    subs = [dict(r) for r in conn.execute(
        "SELECT id, stripe_id, stripe_account_id, status, plan_interval,"
        " plan_amount, currency FROM stripe_subscription ORDER BY stripe_id"
    ).fetchall()]
    acct_count = conn.execute("SELECT COUNT(*) AS c FROM account").fetchone()["c"]
    return (subs, acct_count)


def test_annual_12000_is_1000_exact(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_exact_annual",
                      plan_amount="12000.00", plan_interval="year",
                      status="active")
    result = _report(conn, env["stripe_account_id"])
    assert is_ok(result), result
    assert result["report"] == "mrr"
    assert result["total_mrr"] == "1000.00"
    assert result["currency"] == "USD"
    assert len(result["mrr_by_currency"]) == 1
    bucket = result["mrr_by_currency"][0]
    assert bucket["currency"] == "USD"
    assert Decimal(bucket["mrr"]) == Decimal("1000.00")


def test_monthly_plus_annual_is_99_98(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_exact_m1",
                      plan_amount="49.99", plan_interval="month",
                      status="active")
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_exact_y1",
                      plan_amount="599.88", plan_interval="year",
                      status="active")
    result = _report(conn, env["stripe_account_id"])
    assert is_ok(result), result
    assert result["total_mrr"] == "99.98"
    assert result["currency"] == "USD"


def test_two_small_annual_rows_round_after_sum(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_exact_s1",
                      plan_amount="0.05", plan_interval="year",
                      status="active")
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_exact_s2",
                      plan_amount="0.05", plan_interval="year",
                      status="active")
    result = _report(conn, env["stripe_account_id"])
    assert is_ok(result), result
    assert result["total_mrr"] == "0.01"


def test_mixed_currencies_have_no_mixed_total(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_mix_usd",
                      plan_amount="100.00", plan_interval="month",
                      status="active")
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_mix_eur",
                      plan_amount="100.00", plan_interval="month",
                      status="active")
    conn.execute("UPDATE stripe_subscription SET currency = 'EUR'"
                 " WHERE stripe_id = 'sub_mix_eur'")
    conn.commit()
    result = _report(conn, env["stripe_account_id"])
    assert is_ok(result), result
    assert result["total_mrr"] is None
    assert result["currency"] is None
    buckets = result["mrr_by_currency"]
    assert [entry["currency"] for entry in buckets] == ["EUR", "USD"]
    by_cur = {entry["currency"]: entry for entry in buckets}
    assert Decimal(by_cur["USD"]["mrr"]) == Decimal("100.00")
    assert Decimal(by_cur["EUR"]["mrr"]) == Decimal("100.00")


def test_currency_case_normalized(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_case_a",
                      plan_amount="60.00", plan_interval="month",
                      status="active")
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_case_b",
                      plan_amount="40.00", plan_interval="month",
                      status="active")
    conn.execute("UPDATE stripe_subscription SET currency = 'usd'"
                 " WHERE stripe_id = 'sub_case_b'")
    conn.commit()
    result = _report(conn, env["stripe_account_id"])
    assert is_ok(result), result
    assert result["total_mrr"] == "100.00"
    assert result["currency"] == "USD"
    assert len(result["mrr_by_currency"]) == 1
    assert result["mrr_by_currency"][0]["currency"] == "USD"


def test_trialing_counted_not_summed(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_elig_active",
                      plan_amount="100.00", plan_interval="month",
                      status="active")
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_elig_trial",
                      plan_amount="50.00", plan_interval="month",
                      status="trialing")
    result = _report(conn, env["stripe_account_id"])
    assert is_ok(result), result
    assert result["total_mrr"] == "100.00"
    assert result["active_subscriptions"] == 1
    assert result["trialing_subscriptions"] == 1
    assert result["total_subscriptions"] == 2


def test_inactive_statuses_excluded(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_keep_active",
                      plan_amount="100.00", plan_interval="month",
                      status="active")
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_skip_canceled",
                      plan_amount="200.00", plan_interval="month",
                      status="canceled")
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_skip_past_due",
                      plan_amount="300.00", plan_interval="month",
                      status="past_due")
    result = _report(conn, env["stripe_account_id"])
    assert is_ok(result), result
    assert result["total_mrr"] == "100.00"
    assert result["active_subscriptions"] == 1
    assert result["trialing_subscriptions"] == 0


def test_unknown_interval_refused(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_bad_interval",
                      plan_amount="100.00", plan_interval="month",
                      status="active")
    conn.execute("UPDATE stripe_subscription SET plan_interval = 'fortnight'"
                 " WHERE stripe_id = 'sub_bad_interval'")
    conn.commit()
    result = _report(conn, env["stripe_account_id"])
    assert is_error(result), result


def test_empty_interval_refused(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_empty_interval",
                      plan_amount="100.00", plan_interval="month",
                      status="active")
    conn.execute("UPDATE stripe_subscription SET plan_interval = ''"
                 " WHERE stripe_id = 'sub_empty_interval'")
    conn.commit()
    result = _report(conn, env["stripe_account_id"])
    assert is_error(result), result


def test_missing_interval_refused(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_null_interval",
                      plan_amount="100.00", plan_interval="month",
                      status="active")
    conn.execute("UPDATE stripe_subscription SET plan_interval = NULL"
                 " WHERE stripe_id = 'sub_null_interval'")
    conn.commit()
    result = _report(conn, env["stripe_account_id"])
    assert is_error(result), result


def test_blank_currency_refused(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_blank_cur",
                      plan_amount="100.00", plan_interval="month",
                      status="active")
    conn.execute("UPDATE stripe_subscription SET currency = ''"
                 " WHERE stripe_id = 'sub_blank_cur'")
    conn.commit()
    result = _report(conn, env["stripe_account_id"])
    assert is_error(result), result


def test_negative_amount_refused(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_neg_amt",
                      plan_amount="-5.00", plan_interval="month",
                      status="active")
    result = _report(conn, env["stripe_account_id"])
    assert is_error(result), result


def test_nonfinite_amount_refused(conn, db_path):
    for stripe_id, amount in (("sub_nan_amt", "NaN"),
                              ("sub_inf_amt", "Infinity")):
        env = build_stripe_env(conn)
        seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                          stripe_id=stripe_id,
                          plan_amount=amount, plan_interval="month",
                          status="active")
        result = _report(conn, env["stripe_account_id"])
        assert is_error(result), (stripe_id, result)


def test_nonnumeric_amount_refused(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_abc_amt",
                      plan_amount="49.99", plan_interval="month",
                      status="active")
    conn.execute("UPDATE stripe_subscription SET plan_amount = 'abc'"
                 " WHERE stripe_id = 'sub_abc_amt'")
    conn.commit()
    result = _report(conn, env["stripe_account_id"])
    assert is_error(result), result


def test_empty_zero_shape(conn, db_path):
    env = build_stripe_env(conn)
    result = _report(conn, env["stripe_account_id"])
    assert is_ok(result), result
    assert result["total_mrr"] == "0.00"
    assert result["currency"] is None
    assert result["mrr_by_currency"] == []
    assert result["active_subscriptions"] == 0


def test_trialing_only_zero_shape(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_trial_only",
                      plan_amount="50.00", plan_interval="month",
                      status="trialing")
    result = _report(conn, env["stripe_account_id"])
    assert is_ok(result), result
    assert result["total_mrr"] == "0.00"
    assert result["currency"] is None
    assert result["mrr_by_currency"] == []
    assert result["trialing_subscriptions"] == 1


def test_day_week_convention_preserved(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_day_row",
                      plan_amount="10.00", plan_interval="day",
                      status="active")
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_week_row",
                      plan_amount="10.00", plan_interval="week",
                      status="active")
    result = _report(conn, env["stripe_account_id"])
    assert is_ok(result), result
    assert result["total_mrr"] == "343.33"
    by_interval = {entry["interval"]: entry for entry in
                   result["mrr_by_currency"][0]["interval_breakdown"]}
    assert Decimal(by_interval["day"]["mrr_contribution"]) == Decimal("300.00")
    assert Decimal(by_interval["week"]["mrr_contribution"]) == Decimal("43.33")


def test_account_isolation(conn, db_path):
    env_a = build_stripe_env(conn)
    env_b = build_stripe_env(conn)
    seed_subscription(conn, env_a["stripe_account_id"], env_a["company_id"],
                      stripe_id="sub_iso_a",
                      plan_amount="100.00", plan_interval="month",
                      status="active")
    seed_subscription(conn, env_b["stripe_account_id"], env_b["company_id"],
                      stripe_id="sub_iso_b",
                      plan_amount="500.00", plan_interval="month",
                      status="active")
    result = _report(conn, env_a["stripe_account_id"])
    assert is_ok(result), result
    assert result["total_mrr"] == "100.00"


def test_success_leaves_rows_unchanged(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_ro_ok",
                      plan_amount="100.00", plan_interval="month",
                      status="active")
    before = _snapshot(conn)
    result = _report(conn, env["stripe_account_id"])
    assert is_ok(result), result
    assert _snapshot(conn) == before


def test_refusal_leaves_rows_unchanged(conn, db_path):
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_ro_bad",
                      plan_amount="100.00", plan_interval="month",
                      status="active")
    conn.execute("UPDATE stripe_subscription SET plan_interval = 'fortnight'"
                 " WHERE stripe_id = 'sub_ro_bad'")
    conn.commit()
    before = _snapshot(conn)
    result = _report(conn, env["stripe_account_id"])
    assert is_error(result), result
    assert _snapshot(conn) == before


def test_router_dispatch_reports_exact_mrr(conn, db_path):
    import db_query as router
    env = build_stripe_env(conn)
    seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                      stripe_id="sub_router_annual",
                      plan_amount="1200.00", plan_interval="year",
                      status="active")
    result = call_action(router.ACTIONS["stripe-mrr-report"], conn, ns(
        stripe_account_id=env["stripe_account_id"]))
    assert is_ok(result), result
    assert result["report"] == "mrr"
    assert result["total_mrr"] == "100.00"
    assert result["currency"] == "USD"
