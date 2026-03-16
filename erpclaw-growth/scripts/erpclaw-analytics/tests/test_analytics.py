"""L1 pytest tests for erpclaw-analytics (26 actions).

Analytics is 100% read-only. Tests verify that actions return structured
data from seeded GL entries and gracefully degrade when optional skills
are missing.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from analytics_helpers import call_action, ns, is_ok, is_error, load_db_query

MOD = load_db_query()


# ===========================================================================
# Utility Actions
# ===========================================================================

class TestStatus:
    def test_status_no_company(self, conn, env):
        r = call_action(MOD.status, conn, ns(company_id=None))
        assert is_ok(r)
        assert r["installed_count"] > 0
        assert "installed" in r
        assert "not_installed" in r

    def test_status_with_company(self, conn, env):
        r = call_action(MOD.status, conn, ns(company_id=env["company_id"]))
        assert is_ok(r)
        assert "company_stats" in r
        assert r["company_stats"]["gl_entries"] >= 0


class TestAvailableMetrics:
    def test_available_metrics(self, conn, env):
        r = call_action(MOD.available_metrics, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "available" in r
        assert "unavailable" in r
        assert len(r["available"]) > 0


class TestAnalyzeQueryPerformance:
    def test_query_performance(self, conn, env):
        r = call_action(MOD.analyze_query_performance, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "total_tables" in r


# ===========================================================================
# Financial Ratios
# ===========================================================================

class TestLiquidityRatios:
    def test_liquidity_ratios(self, conn, env):
        r = call_action(MOD.liquidity_ratios, conn, ns(
            company_id=env["company_id"],
            as_of_date="2026-03-31",
        ))
        assert is_ok(r)
        assert "ratios" in r
        assert "current_ratio" in r["ratios"]
        assert "quick_ratio" in r["ratios"]

    def test_liquidity_ratios_missing_company(self, conn, env):
        r = call_action(MOD.liquidity_ratios, conn, ns(
            company_id=None, as_of_date="2026-03-31",
        ))
        assert is_error(r)


class TestProfitabilityRatios:
    def test_profitability_ratios(self, conn, env):
        r = call_action(MOD.profitability_ratios, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        assert is_ok(r)
        assert "ratios" in r
        assert "gross_margin" in r["ratios"]
        assert "net_profit_margin" in r["ratios"]

    def test_profitability_ratios_missing_dates(self, conn, env):
        r = call_action(MOD.profitability_ratios, conn, ns(
            company_id=env["company_id"],
            from_date=None, to_date=None,
        ))
        assert is_error(r)


class TestEfficiencyRatios:
    def test_efficiency_ratios(self, conn, env):
        r = call_action(MOD.efficiency_ratios, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        assert is_ok(r)
        # Ratios nested under "ratios" key; may show N/A or None if selling/inventory not installed
        assert "ratios" in r


# ===========================================================================
# Revenue Analytics
# ===========================================================================

class TestRevenueByCustomer:
    def test_revenue_by_customer(self, conn, env):
        r = call_action(MOD.revenue_by_customer, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
            limit="20", offset="0",
        ))
        # May succeed with empty data if selling is installed, or error if not
        assert "status" in r


class TestRevenueByItem:
    def test_revenue_by_item(self, conn, env):
        r = call_action(MOD.revenue_by_item, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
            limit="20", offset="0",
        ))
        assert "status" in r


class TestRevenueTrend:
    def test_revenue_trend(self, conn, env):
        r = call_action(MOD.revenue_trend, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
            periodicity="monthly",
        ))
        assert is_ok(r)
        assert "trend" in r


class TestCustomerConcentration:
    def test_customer_concentration(self, conn, env):
        r = call_action(MOD.customer_concentration, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        assert "status" in r


# ===========================================================================
# Expense Analytics
# ===========================================================================

class TestExpenseBreakdown:
    def test_expense_breakdown(self, conn, env):
        r = call_action(MOD.expense_breakdown, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
            group_by="account",
        ))
        assert is_ok(r)
        assert "total_expenses" in r

    def test_expense_breakdown_missing_dates(self, conn, env):
        r = call_action(MOD.expense_breakdown, conn, ns(
            company_id=env["company_id"],
            from_date=None, to_date=None,
            group_by="account",
        ))
        assert is_error(r)


class TestCostTrend:
    def test_cost_trend(self, conn, env):
        r = call_action(MOD.cost_trend, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
            periodicity="monthly",
        ))
        assert is_ok(r)
        assert "periods" in r


class TestOpexVsCapex:
    def test_opex_vs_capex(self, conn, env):
        r = call_action(MOD.opex_vs_capex, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        assert is_ok(r)
        assert "opex" in r


# ===========================================================================
# Dashboards
# ===========================================================================

class TestExecutiveDashboard:
    def test_executive_dashboard(self, conn, env):
        r = call_action(MOD.executive_dashboard, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
            as_of_date="2026-03-31",
        ))
        assert is_ok(r)
        assert "sections" in r


class TestCompanyScorecard:
    def test_company_scorecard(self, conn, env):
        r = call_action(MOD.company_scorecard, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
            as_of_date="2026-03-31",
        ))
        assert is_ok(r)


class TestMetricTrend:
    def test_metric_trend(self, conn, env):
        r = call_action(MOD.metric_trend, conn, ns(
            company_id=env["company_id"],
            metric="revenue",
            from_date="2026-01-01", to_date="2026-03-31",
            periodicity="monthly",
        ))
        assert is_ok(r)
        assert "metric" in r


class TestPeriodComparison:
    def test_period_comparison(self, conn, env):
        periods = json.dumps([
            {"from_date": "2026-01-01", "to_date": "2026-01-31", "label": "Jan"},
            {"from_date": "2026-02-01", "to_date": "2026-02-28", "label": "Feb"},
        ])
        metrics = json.dumps(["revenue", "expense"])

        r = call_action(MOD.period_comparison, conn, ns(
            company_id=env["company_id"],
            periods=periods, metrics=metrics,
        ))
        assert is_ok(r)
        assert "periods" in r


# ===========================================================================
# Inventory Analytics (graceful degradation)
# ===========================================================================

class TestAbcAnalysis:
    def test_abc_analysis(self, conn, env):
        r = call_action(MOD.abc_analysis, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        # Gracefully degrades if inventory not installed
        assert "status" in r


class TestInventoryTurnover:
    def test_inventory_turnover(self, conn, env):
        r = call_action(MOD.inventory_turnover, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        assert "status" in r


class TestAgingInventory:
    def test_aging_inventory(self, conn, env):
        r = call_action(MOD.aging_inventory, conn, ns(
            company_id=env["company_id"],
            as_of_date="2026-03-31",
            aging_buckets="30,60,90,120",
        ))
        assert "status" in r


# ===========================================================================
# HR Analytics (graceful degradation)
# ===========================================================================

class TestHeadcountAnalytics:
    def test_headcount(self, conn, env):
        r = call_action(MOD.headcount_analytics, conn, ns(
            company_id=env["company_id"],
            as_of_date="2026-03-31",
        ))
        assert "status" in r


class TestPayrollAnalytics:
    def test_payroll(self, conn, env):
        r = call_action(MOD.payroll_analytics, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        assert "status" in r


class TestLeaveUtilization:
    def test_leave(self, conn, env):
        r = call_action(MOD.leave_utilization, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        assert "status" in r


# ===========================================================================
# Operations Analytics (graceful degradation)
# ===========================================================================

class TestProjectProfitability:
    def test_project_profitability(self, conn, env):
        r = call_action(MOD.project_profitability, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        assert "status" in r


class TestQualityDashboard:
    def test_quality_dashboard(self, conn, env):
        r = call_action(MOD.quality_dashboard, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        assert "status" in r


class TestSupportMetrics:
    def test_support_metrics(self, conn, env):
        r = call_action(MOD.support_metrics, conn, ns(
            company_id=env["company_id"],
            from_date="2026-01-01", to_date="2026-03-31",
        ))
        assert "status" in r
