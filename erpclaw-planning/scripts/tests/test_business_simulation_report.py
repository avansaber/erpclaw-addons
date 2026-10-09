"""Focused tests for planning-business-simulation-report v1.

Read-only deterministic 24-month cash forecast. Proves exact 500.03
money, 24 ordered rows, growth compounding, negative-cash detection,
company isolation, invalid-rate refusal, identical-call determinism,
and no writes.
"""
from decimal import Decimal

import pytest

from planning_helpers import call_action, ns, is_ok, is_error, load_db_query, seed_company
from erpclaw_lib.query import Q, Table


def _mod():
    return load_db_query()


def _base(company_id, **over):
    params = dict(
        company_id=company_id,
        start_month="2026-01",
        starting_cash="1000.00",
        monthly_revenue="800.00",
        monthly_expense="299.97",
        revenue_growth_rate=None,
        expense_growth_rate=None,
    )
    params.update(over)
    return ns(**params)


class TestExactMoneyAndRows:
    def test_exact_500_03_and_24_ordered_rows(self, conn, env):
        mod = _mod()
        result = call_action(
            mod.ACTIONS["planning-business-simulation-report"],
            conn, _base(env["company_id"]))
        assert is_ok(result), result
        months = result["months"]
        assert len(months) == 24
        assert result["month_count"] == 24
        assert months[0]["month"] == "2026-01"
        assert months[-1]["month"] == "2027-12"
        first = months[0]
        assert first["net_change"] == "500.03"
        assert first["opening_cash"] == "1000.00"
        assert first["revenue"] == "800.00"
        assert first["expense"] == "299.97"
        assert first["closing_cash"] == "1500.03"
        assert result["total_net_change"] == "12000.72"
        assert result["total_revenue"] == "19200.00"
        assert result["total_expense"] == "7199.28"
        assert result["limitation"]
        assert "deterministic forecast" in result["limitation"].lower()


class TestGrowthCompounding:
    def test_revenue_compounds_monthly(self, conn, env):
        mod = _mod()
        result = call_action(
            mod.ACTIONS["planning-business-simulation-report"], conn,
            _base(env["company_id"], monthly_revenue="1000.00",
                  monthly_expense="0", revenue_growth_rate="10%"))
        assert is_ok(result), result
        revenues = [m["revenue"] for m in result["months"]]
        assert revenues[0] == "1000.00"
        assert revenues[1] == "1100.00"
        assert revenues[2] == "1210.00"
        assert revenues[3] == "1331.00"

    def test_bare_number_matches_percent_sign(self, conn, env):
        mod = _mod()
        first = call_action(
            mod.ACTIONS["planning-business-simulation-report"], conn,
            _base(env["company_id"], monthly_revenue="1000.00",
                  monthly_expense="0", revenue_growth_rate="5"))
        second = call_action(
            mod.ACTIONS["planning-business-simulation-report"], conn,
            _base(env["company_id"], monthly_revenue="1000.00",
                  monthly_expense="0", revenue_growth_rate="5%"))
        assert is_ok(first), first
        assert is_ok(second), second
        assert first["months"] == second["months"]


class TestNegativeCash:
    def test_first_negative_month_and_minimum(self, conn, env):
        mod = _mod()
        result = call_action(
            mod.ACTIONS["planning-business-simulation-report"], conn,
            _base(env["company_id"], starting_cash="100.00",
                  monthly_revenue="100.00", monthly_expense="500.00"))
        assert is_ok(result), result
        assert result["months"][0]["closing_cash"] == "-300.00"
        assert result["first_negative_cash_month"] == "2026-01"
        assert result["minimum_cash"] == "-9500.00"
        closings = [Decimal(m["closing_cash"]) for m in result["months"]]
        assert min(closings) == Decimal(result["minimum_cash"])


class TestCompanyIsolation:
    def test_same_inputs_other_company_same_months(self, conn, env):
        mod = _mod()
        other = seed_company(conn)
        first = call_action(
            mod.ACTIONS["planning-business-simulation-report"],
            conn, _base(env["company_id"]))
        second = call_action(
            mod.ACTIONS["planning-business-simulation-report"],
            conn, _base(other))
        assert is_ok(first), first
        assert is_ok(second), second
        assert first["months"] == second["months"]
        assert first["company_id"] != second["company_id"]

    def test_missing_company_refused(self, conn, env):
        mod = _mod()
        result = call_action(
            mod.ACTIONS["planning-business-simulation-report"],
            conn, _base("00000000-0000-0000-0000-000000000000"))
        assert is_error(result)


class TestInvalidRates:
    def test_rate_outside_bounds_refused(self, conn, env):
        mod = _mod()
        for bad in ("250%", "101%", "-101%", "abc", "10%%"):
            result = call_action(
                mod.ACTIONS["planning-business-simulation-report"], conn,
                _base(env["company_id"], revenue_growth_rate=bad))
            assert is_error(result), (bad, result)

    def test_invalid_month_decimal_and_nonfinite_refused(self, conn, env):
        mod = _mod()
        assert is_error(call_action(
            mod.ACTIONS["planning-business-simulation-report"], conn,
            _base(env["company_id"], start_month="2026-13")))
        assert is_error(call_action(
            mod.ACTIONS["planning-business-simulation-report"], conn,
            _base(env["company_id"], start_month="Jan 2026")))
        assert is_error(call_action(
            mod.ACTIONS["planning-business-simulation-report"], conn,
            _base(env["company_id"], monthly_revenue="abc")))
        for bad in ("NaN", "Infinity", "-Infinity"):
            assert is_error(call_action(
                mod.ACTIONS["planning-business-simulation-report"], conn,
                _base(env["company_id"], monthly_revenue=bad))), bad


class TestReadOnlyDeterminism:
    def test_two_identical_calls_match(self, conn, env):
        mod = _mod()
        args = _base(env["company_id"], revenue_growth_rate="2%",
                     expense_growth_rate="1%")
        first = call_action(
            mod.ACTIONS["planning-business-simulation-report"], conn, args)
        second = call_action(
            mod.ACTIONS["planning-business-simulation-report"], conn,
            _base(env["company_id"], revenue_growth_rate="2%",
                  expense_growth_rate="1%"))
        assert is_ok(first), first
        assert is_ok(second), second
        assert first == second

    def test_no_writes(self, conn, env):
        mod = _mod()
        before = conn.total_changes
        result = call_action(
            mod.ACTIONS["planning-business-simulation-report"],
            conn, _base(env["company_id"]))
        assert is_ok(result), result
        assert conn.total_changes == before


def _books_snapshot(conn):
    tables = ("company", "gl_entry", "journal_entry", "sales_invoice",
              "payment_entry", "payment_ledger_entry", "audit_log",
              "planning_scenario", "planning_scenario_line", "forecast", "forecast_line")
    return {name: sorted(tuple(str(value) for value in row)
                         for row in conn.execute(Q.from_(Table(name)).select("*").get_sql()))
            for name in tables}


@pytest.mark.parametrize("field", ["starting_cash", "monthly_revenue", "monthly_expense"])
@pytest.mark.parametrize("amount", ["1E1000", "-1E1000"])
def test_huge_finite_input_returns_json_error_and_preserves_books(conn, env, field, amount):
    mod = _mod()
    before, changes = _books_snapshot(conn), conn.total_changes
    result = call_action(mod.ACTIONS["planning-business-simulation-report"], conn,
                         _base(env["company_id"], **{field: amount}))
    assert is_error(result), result
    assert "Invalid" in result["message"]
    assert "months" not in result
    assert _books_snapshot(conn) == before
    assert conn.total_changes == changes


@pytest.mark.parametrize("inputs", [
    {"starting_cash": "9E25", "monthly_revenue": "9E25", "monthly_expense": "0"},
    {"monthly_revenue": "9E25", "monthly_expense": "-9E25"},
    {"monthly_revenue": "9E24", "monthly_expense": "0"},
    {"monthly_revenue": "9E24", "monthly_expense": "9E24",
     "revenue_growth_rate": "100", "expense_growth_rate": "100"},
])
def test_unrepresentable_forecast_returns_whole_json_refusal(conn, env, inputs):
    mod = _mod()
    before, changes = _books_snapshot(conn), conn.total_changes
    result = call_action(mod.ACTIONS["planning-business-simulation-report"], conn,
                         _base(env["company_id"], **inputs))
    assert is_error(result), result
    assert result["message"] == "Simulation amounts exceed representable Decimal currency arithmetic"
    assert "months" not in result
    assert _books_snapshot(conn) == before
    assert conn.total_changes == changes


def test_large_representable_exact_forecast_is_not_arbitrarily_capped(conn, env):
    mod = _mod()
    result = call_action(mod.ACTIONS["planning-business-simulation-report"], conn,
                         _base(env["company_id"], starting_cash="9E25",
                               monthly_revenue="0.01", monthly_expense="0"))
    assert is_ok(result), result
    assert result["total_revenue"] == "0.24"
    assert result["months"][-1]["closing_cash"] == "90000000000000000000000000.24"
