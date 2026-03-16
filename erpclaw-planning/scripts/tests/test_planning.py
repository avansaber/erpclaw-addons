"""L1 tests for ERPClaw Planning -- Scenarios, Forecasts, and Budgets.

Covers all 30 actions across 3 domain modules:
  - scenarios (12): add/update/get/list scenario, add/list/update scenario-line,
    clone, approve, archive, compare, summary
  - forecasts (10): add/update/get/list forecast, add/list/update forecast-line,
    lock, calculate-variance, accuracy-report
  - budgets (8): add/list/get budget-version, approve/lock budget,
    compare-budget-versions, budget-vs-actual, variance-dashboard
"""
import pytest
from planning_helpers import call_action, ns, is_ok, is_error, load_db_query, _uuid


@pytest.fixture
def mod():
    return load_db_query()


# ============================================================================
# SCENARIOS
# ============================================================================

class TestAddScenario:
    def test_add_scenario_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="Base Scenario 2026",
            scenario_type="base", description="Annual plan",
            assumptions="5% growth", base_scenario_id=None,
            fiscal_year="2026",
        ))
        assert is_ok(r)
        assert r["id"]
        assert r["naming_series"].startswith("SCEN-")
        assert r["name"] == "Base Scenario 2026"
        assert r["scenario_type"] == "base"
        assert r["scenario_status"] == "draft"

    def test_add_scenario_missing_name(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name=None,
            scenario_type=None, description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        assert is_error(r)

    def test_add_scenario_missing_company(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=None, name="Test",
            scenario_type=None, description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        assert is_error(r)

    def test_add_scenario_invalid_type(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="Bad Type",
            scenario_type="bogus", description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        assert is_error(r)

    def test_add_scenario_default_type(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="Default",
            scenario_type=None, description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        assert is_ok(r)
        assert r["scenario_type"] == "base"


class TestUpdateScenario:
    def _make(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="To Update",
            scenario_type=None, description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        assert is_ok(r)
        return r["id"]

    def test_update_scenario_success(self, conn, env, mod):
        sid = self._make(conn, env, mod)
        r = call_action(mod.ACTIONS["planning-update-scenario"], conn, ns(
            scenario_id=sid, name="Updated Name",
            description="New desc", assumptions=None,
            fiscal_year=None, scenario_type=None,
        ))
        assert is_ok(r)
        assert "name" in r["updated_fields"]

    def test_update_scenario_not_found(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-update-scenario"], conn, ns(
            scenario_id=_uuid(), name="X",
            description=None, assumptions=None,
            fiscal_year=None, scenario_type=None,
        ))
        assert is_error(r)

    def test_update_scenario_no_fields(self, conn, env, mod):
        sid = self._make(conn, env, mod)
        r = call_action(mod.ACTIONS["planning-update-scenario"], conn, ns(
            scenario_id=sid, name=None,
            description=None, assumptions=None,
            fiscal_year=None, scenario_type=None,
        ))
        assert is_error(r)


class TestGetScenario:
    def test_get_scenario_success(self, conn, env, mod):
        add = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="Get Me",
            scenario_type="best_case", description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["planning-get-scenario"], conn, ns(
            scenario_id=add["id"],
        ))
        assert is_ok(r)
        assert r["name"] == "Get Me"
        assert r["scenario_status"] == "draft"

    def test_get_scenario_not_found(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-get-scenario"], conn, ns(
            scenario_id=_uuid(),
        ))
        assert is_error(r)


class TestListScenarios:
    def test_list_scenarios_empty(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-list-scenarios"], conn, ns(
            company_id=env["company_id"], scenario_type=None,
            status=None, fiscal_year=None, search=None,
            limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] == 0

    def test_list_scenarios_with_data(self, conn, env, mod):
        call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="S1",
            scenario_type=None, description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="S2",
            scenario_type="best_case", description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        r = call_action(mod.ACTIONS["planning-list-scenarios"], conn, ns(
            company_id=env["company_id"], scenario_type=None,
            status=None, fiscal_year=None, search=None,
            limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] == 2


class TestScenarioLine:
    def _make_scenario(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="With Lines",
            scenario_type=None, description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        assert is_ok(r)
        return r["id"]

    def test_add_scenario_line_success(self, conn, env, mod):
        sid = self._make_scenario(conn, env, mod)
        r = call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=sid, company_id=env["company_id"],
            account_name="Sales Revenue", account_type="revenue",
            period="2026-01", amount="100000", notes=None,
        ))
        assert is_ok(r)
        assert r["id"]
        assert r["amount"] == "100000.00"

    def test_add_scenario_line_missing_account(self, conn, env, mod):
        sid = self._make_scenario(conn, env, mod)
        r = call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=sid, company_id=env["company_id"],
            account_name=None, account_type=None,
            period="2026-01", amount="100", notes=None,
        ))
        assert is_error(r)

    def test_list_scenario_lines(self, conn, env, mod):
        sid = self._make_scenario(conn, env, mod)
        call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=sid, company_id=env["company_id"],
            account_name="Revenue", account_type="revenue",
            period="2026-01", amount="50000", notes=None,
        ))
        r = call_action(mod.ACTIONS["planning-list-scenario-lines"], conn, ns(
            scenario_id=sid, account_type=None, period=None,
            search=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] == 1

    def test_update_scenario_line(self, conn, env, mod):
        sid = self._make_scenario(conn, env, mod)
        add = call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=sid, company_id=env["company_id"],
            account_name="COGS", account_type="expense",
            period="2026-01", amount="30000", notes=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["planning-update-scenario-line"], conn, ns(
            scenario_line_id=add["id"],
            account_name=None, account_type=None,
            period=None, amount="35000", notes="Adjusted",
        ))
        assert is_ok(r)
        assert "amount" in r["updated_fields"]


class TestCloneScenario:
    def test_clone_scenario(self, conn, env, mod):
        add = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="Original",
            scenario_type="base", description=None,
            assumptions=None, base_scenario_id=None, fiscal_year="2026",
        ))
        assert is_ok(add)
        # Add a line
        call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=add["id"], company_id=env["company_id"],
            account_name="Revenue", account_type="revenue",
            period="2026-01", amount="100000", notes=None,
        ))
        r = call_action(mod.ACTIONS["planning-clone-scenario"], conn, ns(
            scenario_id=add["id"], name="Cloned Version",
        ))
        assert is_ok(r)
        assert r["source_id"] == add["id"]
        assert r["lines_cloned"] == 1
        assert r["scenario_status"] == "draft"


class TestApproveArchiveScenario:
    def _make(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="For Approval",
            scenario_type=None, description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        assert is_ok(r)
        return r["id"]

    def test_approve_scenario(self, conn, env, mod):
        sid = self._make(conn, env, mod)
        r = call_action(mod.ACTIONS["planning-approve-scenario"], conn, ns(
            scenario_id=sid,
        ))
        assert is_ok(r)
        assert r["scenario_status"] == "approved"

    def test_approve_already_approved(self, conn, env, mod):
        sid = self._make(conn, env, mod)
        call_action(mod.ACTIONS["planning-approve-scenario"], conn, ns(scenario_id=sid))
        r = call_action(mod.ACTIONS["planning-approve-scenario"], conn, ns(scenario_id=sid))
        assert is_error(r)

    def test_archive_scenario(self, conn, env, mod):
        sid = self._make(conn, env, mod)
        r = call_action(mod.ACTIONS["planning-archive-scenario"], conn, ns(
            scenario_id=sid,
        ))
        assert is_ok(r)
        assert r["scenario_status"] == "archived"

    def test_cannot_update_approved_scenario(self, conn, env, mod):
        sid = self._make(conn, env, mod)
        call_action(mod.ACTIONS["planning-approve-scenario"], conn, ns(scenario_id=sid))
        r = call_action(mod.ACTIONS["planning-update-scenario"], conn, ns(
            scenario_id=sid, name="Changed",
            description=None, assumptions=None,
            fiscal_year=None, scenario_type=None,
        ))
        assert is_error(r)


class TestCompareScenarios:
    def test_compare_scenarios(self, conn, env, mod):
        s1 = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="Scenario A",
            scenario_type="base", description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        s2 = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="Scenario B",
            scenario_type="best_case", description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        assert is_ok(s1) and is_ok(s2)
        # Add lines to both
        call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=s1["id"], company_id=env["company_id"],
            account_name="Revenue", account_type="revenue",
            period="2026-01", amount="100000", notes=None,
        ))
        call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=s2["id"], company_id=env["company_id"],
            account_name="Revenue", account_type="revenue",
            period="2026-01", amount="120000", notes=None,
        ))
        r = call_action(mod.ACTIONS["planning-compare-scenarios"], conn, ns(
            scenario_id_1=s1["id"], scenario_id_2=s2["id"],
        ))
        assert is_ok(r)
        assert r["total_lines"] == 1
        assert r["line_comparisons"][0]["difference"] == "20000.00"


class TestScenarioSummary:
    def test_scenario_summary(self, conn, env, mod):
        add = call_action(mod.ACTIONS["planning-add-scenario"], conn, ns(
            company_id=env["company_id"], name="Summary Test",
            scenario_type=None, description=None,
            assumptions=None, base_scenario_id=None, fiscal_year=None,
        ))
        assert is_ok(add)
        call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=add["id"], company_id=env["company_id"],
            account_name="Revenue", account_type="revenue",
            period="2026-01", amount="100000", notes=None,
        ))
        call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=add["id"], company_id=env["company_id"],
            account_name="Rent", account_type="expense",
            period="2026-01", amount="20000", notes=None,
        ))
        r = call_action(mod.ACTIONS["planning-scenario-summary"], conn, ns(
            scenario_id=add["id"],
        ))
        assert is_ok(r)
        assert r["total_revenue"] == "100000.00"
        assert r["total_expense"] == "20000.00"
        assert r["net_income"] == "80000.00"
        assert r["line_count"] == 2


# ============================================================================
# FORECASTS
# ============================================================================

class TestAddForecast:
    def test_add_forecast_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-forecast"], conn, ns(
            company_id=env["company_id"], name="Q1 2026 Forecast",
            forecast_type="rolling", period_type="monthly",
            start_period="2026-01", end_period="2026-03",
            description="First quarter rolling forecast",
        ))
        assert is_ok(r)
        assert r["id"]
        assert r["forecast_type"] == "rolling"
        assert r["period_type"] == "monthly"
        assert r["forecast_status"] == "draft"

    def test_add_forecast_missing_name(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-forecast"], conn, ns(
            company_id=env["company_id"], name=None,
            forecast_type=None, period_type=None,
            start_period="2026-01", end_period="2026-03",
            description=None,
        ))
        assert is_error(r)

    def test_add_forecast_invalid_type(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-forecast"], conn, ns(
            company_id=env["company_id"], name="Bad",
            forecast_type="bogus", period_type=None,
            start_period="2026-01", end_period="2026-03",
            description=None,
        ))
        assert is_error(r)


class TestUpdateForecast:
    def _make(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-forecast"], conn, ns(
            company_id=env["company_id"], name="To Update",
            forecast_type=None, period_type=None,
            start_period="2026-01", end_period="2026-06",
            description=None,
        ))
        assert is_ok(r)
        return r["id"]

    def test_update_forecast_success(self, conn, env, mod):
        fid = self._make(conn, env, mod)
        r = call_action(mod.ACTIONS["planning-update-forecast"], conn, ns(
            forecast_id=fid, name="Updated Forecast",
            description=None, start_period=None, end_period=None,
            forecast_type=None, period_type=None,
        ))
        assert is_ok(r)
        assert "name" in r["updated_fields"]

    def test_update_forecast_not_found(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-update-forecast"], conn, ns(
            forecast_id=_uuid(), name="X",
            description=None, start_period=None, end_period=None,
            forecast_type=None, period_type=None,
        ))
        assert is_error(r)


class TestGetForecast:
    def test_get_forecast_success(self, conn, env, mod):
        add = call_action(mod.ACTIONS["planning-add-forecast"], conn, ns(
            company_id=env["company_id"], name="Get This",
            forecast_type="static", period_type="quarterly",
            start_period="2026-Q1", end_period="2026-Q4",
            description=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["planning-get-forecast"], conn, ns(
            forecast_id=add["id"],
        ))
        assert is_ok(r)
        assert r["name"] == "Get This"
        assert r["forecast_status"] == "draft"


class TestListForecasts:
    def test_list_forecasts(self, conn, env, mod):
        call_action(mod.ACTIONS["planning-add-forecast"], conn, ns(
            company_id=env["company_id"], name="F1",
            forecast_type=None, period_type=None,
            start_period="2026-01", end_period="2026-06",
            description=None,
        ))
        r = call_action(mod.ACTIONS["planning-list-forecasts"], conn, ns(
            company_id=env["company_id"], forecast_type=None,
            status=None, search=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] == 1


class TestForecastLine:
    def _make_forecast(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-forecast"], conn, ns(
            company_id=env["company_id"], name="With Lines",
            forecast_type=None, period_type=None,
            start_period="2026-01", end_period="2026-06",
            description=None,
        ))
        assert is_ok(r)
        return r["id"]

    def test_add_forecast_line_success(self, conn, env, mod):
        fid = self._make_forecast(conn, env, mod)
        r = call_action(mod.ACTIONS["planning-add-forecast-line"], conn, ns(
            forecast_id=fid, company_id=env["company_id"],
            account_name="Sales", account_type="revenue",
            period="2026-01", forecast_amount="50000",
            actual_amount="48000", notes=None,
        ))
        assert is_ok(r)
        assert r["forecast_amount"] == "50000.00"
        assert r["actual_amount"] == "48000.00"
        assert r["variance"] == "-2000.00"

    def test_list_forecast_lines(self, conn, env, mod):
        fid = self._make_forecast(conn, env, mod)
        call_action(mod.ACTIONS["planning-add-forecast-line"], conn, ns(
            forecast_id=fid, company_id=env["company_id"],
            account_name="Sales", account_type="revenue",
            period="2026-01", forecast_amount="50000",
            actual_amount="48000", notes=None,
        ))
        r = call_action(mod.ACTIONS["planning-list-forecast-lines"], conn, ns(
            forecast_id=fid, account_type=None, period=None,
            search=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] == 1

    def test_update_forecast_line(self, conn, env, mod):
        fid = self._make_forecast(conn, env, mod)
        add = call_action(mod.ACTIONS["planning-add-forecast-line"], conn, ns(
            forecast_id=fid, company_id=env["company_id"],
            account_name="COGS", account_type="expense",
            period="2026-01", forecast_amount="30000",
            actual_amount="28000", notes=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["planning-update-forecast-line"], conn, ns(
            forecast_line_id=add["id"],
            account_name=None, account_type=None, period=None,
            forecast_amount="32000", actual_amount=None, notes=None,
        ))
        assert is_ok(r)
        assert "forecast_amount" in r["updated_fields"]


class TestLockForecast:
    def test_lock_forecast(self, conn, env, mod):
        add = call_action(mod.ACTIONS["planning-add-forecast"], conn, ns(
            company_id=env["company_id"], name="To Lock",
            forecast_type=None, period_type=None,
            start_period="2026-01", end_period="2026-06",
            description=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["planning-lock-forecast"], conn, ns(
            forecast_id=add["id"],
        ))
        assert is_ok(r)
        assert r["forecast_status"] == "locked"

    def test_lock_already_locked(self, conn, env, mod):
        add = call_action(mod.ACTIONS["planning-add-forecast"], conn, ns(
            company_id=env["company_id"], name="Already Locked",
            forecast_type=None, period_type=None,
            start_period="2026-01", end_period="2026-06",
            description=None,
        ))
        assert is_ok(add)
        call_action(mod.ACTIONS["planning-lock-forecast"], conn, ns(forecast_id=add["id"]))
        r = call_action(mod.ACTIONS["planning-lock-forecast"], conn, ns(forecast_id=add["id"]))
        assert is_error(r)


class TestCalculateVariance:
    def test_calculate_variance(self, conn, env, mod):
        fid = call_action(mod.ACTIONS["planning-add-forecast"], conn, ns(
            company_id=env["company_id"], name="Variance Test",
            forecast_type=None, period_type=None,
            start_period="2026-01", end_period="2026-06",
            description=None,
        ))["id"]
        call_action(mod.ACTIONS["planning-add-forecast-line"], conn, ns(
            forecast_id=fid, company_id=env["company_id"],
            account_name="Sales", account_type="revenue",
            period="2026-01", forecast_amount="50000",
            actual_amount="55000", notes=None,
        ))
        r = call_action(mod.ACTIONS["planning-calculate-variance"], conn, ns(
            forecast_id=fid,
        ))
        assert is_ok(r)
        assert r["lines_updated"] == 1


class TestForecastAccuracyReport:
    def test_accuracy_report(self, conn, env, mod):
        fid = call_action(mod.ACTIONS["planning-add-forecast"], conn, ns(
            company_id=env["company_id"], name="Accuracy Test",
            forecast_type=None, period_type=None,
            start_period="2026-01", end_period="2026-06",
            description=None,
        ))["id"]
        call_action(mod.ACTIONS["planning-add-forecast-line"], conn, ns(
            forecast_id=fid, company_id=env["company_id"],
            account_name="Sales", account_type="revenue",
            period="2026-01", forecast_amount="100000",
            actual_amount="95000", notes=None,
        ))
        r = call_action(mod.ACTIONS["planning-forecast-accuracy-report"], conn, ns(
            forecast_id=fid,
        ))
        assert is_ok(r)
        assert r["line_count"] == 1
        assert r["lines_with_forecast"] == 1
        assert r["average_absolute_variance_pct"] == "5.00"


# ============================================================================
# BUDGETS
# ============================================================================

class TestAddBudgetVersion:
    def test_add_budget_version_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-budget-version"], conn, ns(
            company_id=env["company_id"], name="FY2026 Budget v1",
            description="First draft", assumptions="Conservative",
            fiscal_year="2026",
        ))
        assert is_ok(r)
        assert r["scenario_type"] == "budget"
        assert r["scenario_status"] == "draft"

    def test_add_budget_version_missing_name(self, conn, env, mod):
        r = call_action(mod.ACTIONS["planning-add-budget-version"], conn, ns(
            company_id=env["company_id"], name=None,
            description=None, assumptions=None, fiscal_year=None,
        ))
        assert is_error(r)


class TestListBudgetVersions:
    def test_list_budget_versions(self, conn, env, mod):
        call_action(mod.ACTIONS["planning-add-budget-version"], conn, ns(
            company_id=env["company_id"], name="Budget A",
            description=None, assumptions=None, fiscal_year="2026",
        ))
        r = call_action(mod.ACTIONS["planning-list-budget-versions"], conn, ns(
            company_id=env["company_id"], status=None,
            fiscal_year=None, search=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] == 1


class TestGetBudgetVersion:
    def test_get_budget_version(self, conn, env, mod):
        add = call_action(mod.ACTIONS["planning-add-budget-version"], conn, ns(
            company_id=env["company_id"], name="Budget to Get",
            description=None, assumptions=None, fiscal_year=None,
        ))
        assert is_ok(add)
        # Add a line via scenario-line action
        call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=add["id"], company_id=env["company_id"],
            account_name="Revenue", account_type="revenue",
            period="2026-01", amount="200000", notes=None,
        ))
        r = call_action(mod.ACTIONS["planning-get-budget-version"], conn, ns(
            budget_id=add["id"],
        ))
        assert is_ok(r)
        assert r["line_count"] == 1
        assert r["scenario_status"] == "draft"


class TestApproveBudget:
    def test_approve_budget(self, conn, env, mod):
        add = call_action(mod.ACTIONS["planning-add-budget-version"], conn, ns(
            company_id=env["company_id"], name="For Approval",
            description=None, assumptions=None, fiscal_year=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["planning-approve-budget"], conn, ns(
            budget_id=add["id"],
        ))
        assert is_ok(r)
        assert r["scenario_status"] == "approved"

    def test_approve_budget_already_approved(self, conn, env, mod):
        add = call_action(mod.ACTIONS["planning-add-budget-version"], conn, ns(
            company_id=env["company_id"], name="Double Approve",
            description=None, assumptions=None, fiscal_year=None,
        ))
        assert is_ok(add)
        call_action(mod.ACTIONS["planning-approve-budget"], conn, ns(budget_id=add["id"]))
        r = call_action(mod.ACTIONS["planning-approve-budget"], conn, ns(budget_id=add["id"]))
        assert is_error(r)


class TestLockBudget:
    def test_lock_budget(self, conn, env, mod):
        add = call_action(mod.ACTIONS["planning-add-budget-version"], conn, ns(
            company_id=env["company_id"], name="To Lock",
            description=None, assumptions=None, fiscal_year=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["planning-lock-budget"], conn, ns(
            budget_id=add["id"],
        ))
        assert is_ok(r)
        assert r["scenario_status"] == "locked"


class TestCompareBudgetVersions:
    def test_compare_budget_versions(self, conn, env, mod):
        b1 = call_action(mod.ACTIONS["planning-add-budget-version"], conn, ns(
            company_id=env["company_id"], name="Budget v1",
            description=None, assumptions=None, fiscal_year=None,
        ))
        b2 = call_action(mod.ACTIONS["planning-add-budget-version"], conn, ns(
            company_id=env["company_id"], name="Budget v2",
            description=None, assumptions=None, fiscal_year=None,
        ))
        assert is_ok(b1) and is_ok(b2)

        call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=b1["id"], company_id=env["company_id"],
            account_name="Revenue", account_type="revenue",
            period="2026-01", amount="100000", notes=None,
        ))
        call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=b2["id"], company_id=env["company_id"],
            account_name="Revenue", account_type="revenue",
            period="2026-01", amount="110000", notes=None,
        ))

        r = call_action(mod.ACTIONS["planning-compare-budget-versions"], conn, ns(
            budget_id_1=b1["id"], budget_id_2=b2["id"],
        ))
        assert is_ok(r)
        assert r["total_lines"] == 1


class TestBudgetVsActual:
    def test_budget_vs_actual_no_gl(self, conn, env, mod):
        """Budget vs actual works even without matching GL entries."""
        add = call_action(mod.ACTIONS["planning-add-budget-version"], conn, ns(
            company_id=env["company_id"], name="BvA Test",
            description=None, assumptions=None, fiscal_year=None,
        ))
        assert is_ok(add)
        call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=add["id"], company_id=env["company_id"],
            account_name="Office Rent", account_type="expense",
            period="2026-01", amount="5000", notes=None,
        ))
        r = call_action(mod.ACTIONS["planning-budget-vs-actual"], conn, ns(
            budget_id=add["id"],
        ))
        assert is_ok(r)
        assert r["total_lines"] == 1
        assert r["summary"]["total_budget"] == "5000.00"


class TestVarianceDashboard:
    def test_variance_dashboard(self, conn, env, mod):
        add = call_action(mod.ACTIONS["planning-add-budget-version"], conn, ns(
            company_id=env["company_id"], name="Dashboard Test",
            description=None, assumptions=None, fiscal_year=None,
        ))
        assert is_ok(add)
        call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=add["id"], company_id=env["company_id"],
            account_name="Sales", account_type="revenue",
            period="2026-01", amount="200000", notes=None,
        ))
        call_action(mod.ACTIONS["planning-add-scenario-line"], conn, ns(
            scenario_id=add["id"], company_id=env["company_id"],
            account_name="Rent", account_type="expense",
            period="2026-01", amount="10000", notes=None,
        ))
        r = call_action(mod.ACTIONS["planning-variance-dashboard"], conn, ns(
            budget_id=add["id"],
        ))
        assert is_ok(r)
        assert r["revenue"]["budget"] == "200000.00"
        assert r["expense"]["budget"] == "10000.00"
        assert r["line_count"] == 2
