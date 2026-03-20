"""L1 tests for ERPClaw Treasury -- Cash, Investments, Inter-Company.

Covers all 35 actions across 3 domain modules:
  - cash (17): add/update/get/list bank-account, record-bank-balance,
    add/list/get cash-position, add/update/list/get cash-forecast,
    generate-cash-forecast, cash-dashboard, bank-summary-report,
    liquidity-report, cash-flow-projection
  - investments (10): add/update/get/list investment, add/list
    investment-transaction, mature/redeem investment,
    investment-portfolio-report, investment-maturity-alerts
  - intercompany (8): add/get/list inter-company-transfer,
    approve/complete/cancel transfer, inter-company-balance-report, status
"""
import pytest
from treasury_helpers import (
    call_action, ns, is_ok, is_error, load_db_query, _uuid,
    seed_company, seed_second_company, seed_naming_series,
    seed_bank_account, seed_investment,
)


@pytest.fixture
def mod():
    return load_db_query()


# ============================================================================
# BANK ACCOUNTS
# ============================================================================

class TestAddBankAccount:
    def test_add_bank_account_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-bank-account"], conn, ns(
            company_id=env["company_id"],
            bank_name="Chase", account_name="Payroll Account",
            account_number="9876543210", routing_number="021000021",
            account_type="checking", currency="USD",
            current_balance="25000",
            gl_account_id=None, is_active=None, notes=None,
        ))
        assert is_ok(r)
        assert r["account_id"]
        assert r["account_type"] == "checking"

    def test_add_bank_account_missing_bank(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-bank-account"], conn, ns(
            company_id=env["company_id"],
            bank_name=None, account_name="Test",
            account_number=None, routing_number=None,
            account_type=None, currency=None,
            current_balance=None,
            gl_account_id=None, is_active=None, notes=None,
        ))
        assert is_error(r)

    def test_add_bank_account_invalid_type(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-bank-account"], conn, ns(
            company_id=env["company_id"],
            bank_name="Test Bank", account_name="Test",
            account_number=None, routing_number=None,
            account_type="crypto", currency=None,
            current_balance=None,
            gl_account_id=None, is_active=None, notes=None,
        ))
        assert is_error(r)


class TestUpdateBankAccount:
    def test_update_bank_account_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-update-bank-account"], conn, ns(
            account_id=env["bank_account_id"],
            bank_name="Updated Bank", account_name=None,
            account_number=None, routing_number=None,
            account_type=None, currency=None,
            gl_account_id=None, is_active=None, notes=None,
        ))
        assert is_ok(r)
        assert "bank_name" in r["updated_fields"]

    def test_update_bank_account_not_found(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-update-bank-account"], conn, ns(
            account_id=_uuid(),
            bank_name="X", account_name=None,
            account_number=None, routing_number=None,
            account_type=None, currency=None,
            gl_account_id=None, is_active=None, notes=None,
        ))
        assert is_error(r)

    def test_update_bank_account_no_fields(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-update-bank-account"], conn, ns(
            account_id=env["bank_account_id"],
            bank_name=None, account_name=None,
            account_number=None, routing_number=None,
            account_type=None, currency=None,
            gl_account_id=None, is_active=None, notes=None,
        ))
        assert is_error(r)


class TestGetBankAccount:
    def test_get_bank_account_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-get-bank-account"], conn, ns(
            account_id=env["bank_account_id"],
        ))
        assert is_ok(r)
        assert r["id"] == env["bank_account_id"]
        assert r["bank_name"] == "First National"

    def test_get_bank_account_not_found(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-get-bank-account"], conn, ns(
            account_id=_uuid(),
        ))
        assert is_error(r)


class TestListBankAccounts:
    def test_list_bank_accounts(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-list-bank-accounts"], conn, ns(
            company_id=env["company_id"], account_type=None,
            is_active=None, search=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestRecordBankBalance:
    def test_record_bank_balance(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-record-bank-balance"], conn, ns(
            account_id=env["bank_account_id"],
            current_balance="55000",
        ))
        assert is_ok(r)
        assert r["new_balance"] == "55000"
        assert r["position_id"]


# ============================================================================
# CASH POSITIONS
# ============================================================================

class TestAddCashPosition:
    def test_add_cash_position_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-cash-position"], conn, ns(
            company_id=env["company_id"],
            position_date="2026-03-01",
            total_cash="100000", total_receivables="25000",
            total_payables="15000", notes="Month-end snapshot",
        ))
        assert is_ok(r)
        assert r["net_position"] == "110000"

    def test_add_cash_position_missing_company(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-cash-position"], conn, ns(
            company_id=None,
            position_date=None,
            total_cash=None, total_receivables=None,
            total_payables=None, notes=None,
        ))
        assert is_error(r)


class TestListCashPositions:
    def test_list_cash_positions(self, conn, env, mod):
        call_action(mod.ACTIONS["treasury-add-cash-position"], conn, ns(
            company_id=env["company_id"],
            position_date=None,
            total_cash="50000", total_receivables="0",
            total_payables="0", notes=None,
        ))
        r = call_action(mod.ACTIONS["treasury-list-cash-positions"], conn, ns(
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestGetCashPosition:
    def test_get_cash_position(self, conn, env, mod):
        add = call_action(mod.ACTIONS["treasury-add-cash-position"], conn, ns(
            company_id=env["company_id"],
            position_date=None,
            total_cash="75000", total_receivables="10000",
            total_payables="5000", notes=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["treasury-get-cash-position"], conn, ns(
            position_id=add["position_id"],
        ))
        assert is_ok(r)
        assert r["total_cash"] == "75000"


# ============================================================================
# CASH FORECASTS
# ============================================================================

class TestAddCashForecast:
    def test_add_cash_forecast_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-cash-forecast"], conn, ns(
            company_id=env["company_id"],
            forecast_name="Q1 Cash Forecast",
            forecast_type="short_term",
            period_start="2026-01-01", period_end="2026-03-31",
            expected_inflows="200000", expected_outflows="150000",
            assumptions="Based on recent trends",
        ))
        assert is_ok(r)
        assert r["net_forecast"] == "50000"

    def test_add_cash_forecast_invalid_type(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-cash-forecast"], conn, ns(
            company_id=env["company_id"],
            forecast_name="Bad", forecast_type="bogus",
            period_start="2026-01-01", period_end="2026-03-31",
            expected_inflows="0", expected_outflows="0",
            assumptions=None,
        ))
        assert is_error(r)


class TestUpdateCashForecast:
    def _make(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-cash-forecast"], conn, ns(
            company_id=env["company_id"],
            forecast_name="To Update",
            forecast_type=None,
            period_start="2026-01-01", period_end="2026-06-30",
            expected_inflows="100000", expected_outflows="80000",
            assumptions=None,
        ))
        assert is_ok(r)
        return r["forecast_id"]

    def test_update_cash_forecast(self, conn, env, mod):
        fid = self._make(conn, env, mod)
        r = call_action(mod.ACTIONS["treasury-update-cash-forecast"], conn, ns(
            forecast_id=fid,
            forecast_name="Updated Forecast",
            period_start=None, period_end=None,
            expected_inflows=None, expected_outflows=None,
            assumptions=None, forecast_type=None,
        ))
        assert is_ok(r)
        assert "forecast_name" in r["updated_fields"]


class TestListCashForecasts:
    def test_list_cash_forecasts(self, conn, env, mod):
        call_action(mod.ACTIONS["treasury-add-cash-forecast"], conn, ns(
            company_id=env["company_id"],
            forecast_name="F1", forecast_type=None,
            period_start="2026-01-01", period_end="2026-03-31",
            expected_inflows="100000", expected_outflows="80000",
            assumptions=None,
        ))
        r = call_action(mod.ACTIONS["treasury-list-cash-forecasts"], conn, ns(
            company_id=env["company_id"], forecast_type=None,
            search=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestGetCashForecast:
    def test_get_cash_forecast(self, conn, env, mod):
        add = call_action(mod.ACTIONS["treasury-add-cash-forecast"], conn, ns(
            company_id=env["company_id"],
            forecast_name="Get This", forecast_type=None,
            period_start="2026-01-01", period_end="2026-03-31",
            expected_inflows="50000", expected_outflows="30000",
            assumptions=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["treasury-get-cash-forecast"], conn, ns(
            forecast_id=add["forecast_id"],
        ))
        assert is_ok(r)
        assert r["forecast_name"] == "Get This"


class TestGenerateCashForecast:
    def test_generate_requires_positions(self, conn, env, mod):
        """generate-cash-forecast needs existing cash positions."""
        r = call_action(mod.ACTIONS["treasury-generate-cash-forecast"], conn, ns(
            company_id=env["company_id"],
            forecast_type=None, forecast_name=None,
        ))
        assert is_error(r)  # no positions yet

    def test_generate_with_positions(self, conn, env, mod):
        # Create cash positions first
        call_action(mod.ACTIONS["treasury-add-cash-position"], conn, ns(
            company_id=env["company_id"],
            position_date="2026-01-15",
            total_cash="100000", total_receivables="20000",
            total_payables="10000", notes=None,
        ))
        call_action(mod.ACTIONS["treasury-add-cash-position"], conn, ns(
            company_id=env["company_id"],
            position_date="2026-02-15",
            total_cash="110000", total_receivables="25000",
            total_payables="12000", notes=None,
        ))
        r = call_action(mod.ACTIONS["treasury-generate-cash-forecast"], conn, ns(
            company_id=env["company_id"],
            forecast_type="short_term", forecast_name=None,
        ))
        assert is_ok(r)
        assert r["positions_analyzed"] == 2


class TestCashDashboard:
    def test_cash_dashboard(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-cash-dashboard"], conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "total_cash" in r
        assert "net_position" in r
        assert "active_bank_accounts" in r


class TestBankSummaryReport:
    def test_bank_summary_report(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-bank-summary-report"], conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["total_accounts"] >= 1
        assert "total_balance" in r


class TestLiquidityReport:
    def test_liquidity_report(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-liquidity-report"], conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "liquid_bank_total" in r
        assert "total_liquidity" in r


class TestCashFlowProjection:
    def test_cash_flow_projection(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-cash-flow-projection"], conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "current_cash" in r
        assert "projected_end_balance" in r


# ============================================================================
# INVESTMENTS
# ============================================================================

class TestAddInvestment:
    def test_add_investment_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-investment"], conn, ns(
            company_id=env["company_id"],
            name="Treasury Bill 90-Day",
            investment_type="treasury_bill",
            institution="US Treasury",
            account_number=None,
            principal="50000", current_value="50000",
            interest_rate="5.0",
            purchase_date="2026-01-01", maturity_date="2026-04-01",
            gl_account_id=None, notes=None,
        ))
        assert is_ok(r)
        assert r["investment_id"]
        assert r["investment_status"] == "active"

    def test_add_investment_missing_name(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-investment"], conn, ns(
            company_id=env["company_id"],
            name=None, investment_type=None,
            institution=None, account_number=None,
            principal=None, current_value=None,
            interest_rate=None, purchase_date=None,
            maturity_date=None, gl_account_id=None, notes=None,
        ))
        assert is_error(r)

    def test_add_investment_invalid_type(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-investment"], conn, ns(
            company_id=env["company_id"],
            name="Bad Type", investment_type="crypto",
            institution=None, account_number=None,
            principal="1000", current_value=None,
            interest_rate=None, purchase_date=None,
            maturity_date=None, gl_account_id=None, notes=None,
        ))
        assert is_error(r)


class TestUpdateInvestment:
    def test_update_investment_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-update-investment"], conn, ns(
            investment_id=env["investment_id"],
            name="Updated CD", investment_type=None,
            institution=None, account_number=None,
            principal=None, current_value=None,
            interest_rate=None, purchase_date=None,
            maturity_date=None, gl_account_id=None, notes=None,
        ))
        assert is_ok(r)
        assert "name" in r["updated_fields"]

    def test_update_investment_not_found(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-update-investment"], conn, ns(
            investment_id=_uuid(),
            name="X", investment_type=None,
            institution=None, account_number=None,
            principal=None, current_value=None,
            interest_rate=None, purchase_date=None,
            maturity_date=None, gl_account_id=None, notes=None,
        ))
        assert is_error(r)


class TestGetInvestment:
    def test_get_investment_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-get-investment"], conn, ns(
            investment_id=env["investment_id"],
        ))
        assert is_ok(r)
        assert r["id"] == env["investment_id"]
        assert "transaction_count" in r


class TestListInvestments:
    def test_list_investments(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-list-investments"], conn, ns(
            company_id=env["company_id"], investment_type=None,
            investment_status=None, search=None,
            limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestAddInvestmentTransaction:
    def test_add_investment_transaction_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-investment-transaction"], conn, ns(
            investment_id=env["investment_id"],
            transaction_type="interest",
            transaction_date="2026-02-15",
            amount="375", reference="Monthly interest",
            notes=None,
        ))
        assert is_ok(r)
        assert r["transaction_type"] == "interest"
        assert r["new_current_value"] == "10375"

    def test_add_investment_transaction_invalid_type(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-investment-transaction"], conn, ns(
            investment_id=env["investment_id"],
            transaction_type="bogus",
            transaction_date=None,
            amount="100", reference=None, notes=None,
        ))
        assert is_error(r)


class TestListInvestmentTransactions:
    def test_list_investment_transactions(self, conn, env, mod):
        call_action(mod.ACTIONS["treasury-add-investment-transaction"], conn, ns(
            investment_id=env["investment_id"],
            transaction_type="interest",
            transaction_date=None,
            amount="100", reference=None, notes=None,
        ))
        r = call_action(mod.ACTIONS["treasury-list-investment-transactions"], conn, ns(
            investment_id=env["investment_id"],
            company_id=None, transaction_type=None,
            limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestMatureInvestment:
    def test_mature_investment(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-mature-investment"], conn, ns(
            investment_id=env["investment_id"],
        ))
        assert is_ok(r)
        assert r["investment_status"] == "matured"

    def test_mature_already_matured(self, conn, env, mod):
        call_action(mod.ACTIONS["treasury-mature-investment"], conn, ns(
            investment_id=env["investment_id"],
        ))
        r = call_action(mod.ACTIONS["treasury-mature-investment"], conn, ns(
            investment_id=env["investment_id"],
        ))
        assert is_error(r)


class TestRedeemInvestment:
    def test_redeem_active_investment(self, conn, env, mod):
        # Create a fresh investment to redeem (env's may already be matured)
        inv_id = seed_investment(conn, env["company_id"], "Redeem CD", "20000")
        r = call_action(mod.ACTIONS["treasury-redeem-investment"], conn, ns(
            investment_id=inv_id,
        ))
        assert is_ok(r)
        assert r["investment_status"] == "redeemed"
        assert r["principal"] == "20000"
        assert r["transaction_id"]

    def test_redeem_matured_investment(self, conn, env, mod):
        inv_id = seed_investment(conn, env["company_id"], "Mature then Redeem", "15000")
        call_action(mod.ACTIONS["treasury-mature-investment"], conn, ns(
            investment_id=inv_id,
        ))
        r = call_action(mod.ACTIONS["treasury-redeem-investment"], conn, ns(
            investment_id=inv_id,
        ))
        assert is_ok(r)
        assert r["investment_status"] == "redeemed"


class TestInvestmentPortfolioReport:
    def test_portfolio_report(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-investment-portfolio-report"], conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["total_investments"] >= 1
        assert "total_principal" in r
        assert "by_type" in r


class TestInvestmentMaturityAlerts:
    def test_maturity_alerts(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-investment-maturity-alerts"], conn, ns(
            company_id=env["company_id"], days="365",
        ))
        assert is_ok(r)
        assert "alerts" in r
        assert "days_window" in r


# ============================================================================
# INTER-COMPANY TRANSFERS
# ============================================================================

class TestAddInterCompanyTransfer:
    def test_add_transfer_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-inter-company-transfer"], conn, ns(
            company_id=env["company_id"],
            from_company_id=env["company_id"],
            to_company_id=env["company_id_2"],
            amount="25000",
            transfer_date="2026-03-01",
            reference="Q1 funding",
            reason="Capital injection",
            transfer_status=None,
        ))
        assert is_ok(r)
        assert r["transfer_id"]
        assert r["transfer_status"] == "draft"

    def test_add_transfer_same_company(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-inter-company-transfer"], conn, ns(
            company_id=env["company_id"],
            from_company_id=env["company_id"],
            to_company_id=env["company_id"],
            amount="1000",
            transfer_date=None, reference=None,
            reason=None, transfer_status=None,
        ))
        assert is_error(r)

    def test_add_transfer_missing_amount(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-inter-company-transfer"], conn, ns(
            company_id=env["company_id"],
            from_company_id=env["company_id"],
            to_company_id=env["company_id_2"],
            amount=None,
            transfer_date=None, reference=None,
            reason=None, transfer_status=None,
        ))
        assert is_error(r)


class TestGetInterCompanyTransfer:
    def _make(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-inter-company-transfer"], conn, ns(
            company_id=env["company_id"],
            from_company_id=env["company_id"],
            to_company_id=env["company_id_2"],
            amount="10000",
            transfer_date=None, reference=None,
            reason=None, transfer_status=None,
        ))
        assert is_ok(r)
        return r["transfer_id"]

    def test_get_transfer(self, conn, env, mod):
        xid = self._make(conn, env, mod)
        r = call_action(mod.ACTIONS["treasury-get-inter-company-transfer"], conn, ns(
            transfer_id=xid,
        ))
        assert is_ok(r)
        assert r["amount"] == "10000"
        assert "from_company_name" in r


class TestListInterCompanyTransfers:
    def test_list_transfers(self, conn, env, mod):
        call_action(mod.ACTIONS["treasury-add-inter-company-transfer"], conn, ns(
            company_id=env["company_id"],
            from_company_id=env["company_id"],
            to_company_id=env["company_id_2"],
            amount="5000",
            transfer_date=None, reference=None,
            reason=None, transfer_status=None,
        ))
        r = call_action(mod.ACTIONS["treasury-list-inter-company-transfers"], conn, ns(
            company_id=env["company_id"], transfer_status=None,
            from_company_id=None, to_company_id=None,
            search=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestApproveTransfer:
    def _make(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-add-inter-company-transfer"], conn, ns(
            company_id=env["company_id"],
            from_company_id=env["company_id"],
            to_company_id=env["company_id_2"],
            amount="15000",
            transfer_date=None, reference=None,
            reason=None, transfer_status=None,
        ))
        return r["transfer_id"]

    def test_approve_transfer(self, conn, env, mod):
        xid = self._make(conn, env, mod)
        r = call_action(mod.ACTIONS["treasury-approve-transfer"], conn, ns(
            transfer_id=xid,
        ))
        assert is_ok(r)
        assert r["transfer_status"] == "approved"

    def test_approve_non_draft(self, conn, env, mod):
        xid = self._make(conn, env, mod)
        call_action(mod.ACTIONS["treasury-approve-transfer"], conn, ns(transfer_id=xid))
        r = call_action(mod.ACTIONS["treasury-approve-transfer"], conn, ns(transfer_id=xid))
        assert is_error(r)


class TestCompleteTransfer:
    def _make_approved(self, conn, env, mod):
        add = call_action(mod.ACTIONS["treasury-add-inter-company-transfer"], conn, ns(
            company_id=env["company_id"],
            from_company_id=env["company_id"],
            to_company_id=env["company_id_2"],
            amount="20000",
            transfer_date=None, reference=None,
            reason=None, transfer_status=None,
        ))
        call_action(mod.ACTIONS["treasury-approve-transfer"], conn, ns(
            transfer_id=add["transfer_id"],
        ))
        return add["transfer_id"]

    def test_complete_transfer(self, conn, env, mod):
        xid = self._make_approved(conn, env, mod)
        r = call_action(mod.ACTIONS["treasury-complete-transfer"], conn, ns(
            transfer_id=xid,
            from_account_id=None, to_account_id=None,
        ))
        assert is_ok(r)
        assert r["transfer_status"] == "completed"
        assert r["amount"] == "20000"

    def test_complete_with_bank_accounts(self, conn, env, mod):
        """Complete transfer with bank account adjustments."""
        xid = self._make_approved(conn, env, mod)
        ba2 = seed_bank_account(conn, env["company_id_2"], "Bank B", "Account B", "30000")
        r = call_action(mod.ACTIONS["treasury-complete-transfer"], conn, ns(
            transfer_id=xid,
            from_account_id=env["bank_account_id"],
            to_account_id=ba2,
        ))
        assert is_ok(r)
        assert r["transfer_status"] == "completed"

    def test_complete_unapproved(self, conn, env, mod):
        add = call_action(mod.ACTIONS["treasury-add-inter-company-transfer"], conn, ns(
            company_id=env["company_id"],
            from_company_id=env["company_id"],
            to_company_id=env["company_id_2"],
            amount="5000",
            transfer_date=None, reference=None,
            reason=None, transfer_status=None,
        ))
        r = call_action(mod.ACTIONS["treasury-complete-transfer"], conn, ns(
            transfer_id=add["transfer_id"],
            from_account_id=None, to_account_id=None,
        ))
        assert is_error(r)


class TestCancelTransfer:
    def test_cancel_draft_transfer(self, conn, env, mod):
        add = call_action(mod.ACTIONS["treasury-add-inter-company-transfer"], conn, ns(
            company_id=env["company_id"],
            from_company_id=env["company_id"],
            to_company_id=env["company_id_2"],
            amount="5000",
            transfer_date=None, reference=None,
            reason=None, transfer_status=None,
        ))
        r = call_action(mod.ACTIONS["treasury-cancel-transfer"], conn, ns(
            transfer_id=add["transfer_id"],
        ))
        assert is_ok(r)
        assert r["transfer_status"] == "cancelled"

    def test_cancel_completed_transfer(self, conn, env, mod):
        add = call_action(mod.ACTIONS["treasury-add-inter-company-transfer"], conn, ns(
            company_id=env["company_id"],
            from_company_id=env["company_id"],
            to_company_id=env["company_id_2"],
            amount="5000",
            transfer_date=None, reference=None,
            reason=None, transfer_status=None,
        ))
        call_action(mod.ACTIONS["treasury-approve-transfer"], conn, ns(
            transfer_id=add["transfer_id"],
        ))
        call_action(mod.ACTIONS["treasury-complete-transfer"], conn, ns(
            transfer_id=add["transfer_id"],
            from_account_id=None, to_account_id=None,
        ))
        r = call_action(mod.ACTIONS["treasury-cancel-transfer"], conn, ns(
            transfer_id=add["transfer_id"],
        ))
        assert is_error(r)


class TestInterCompanyBalanceReport:
    def test_balance_report(self, conn, env, mod):
        r = call_action(mod.ACTIONS["treasury-inter-company-balance-report"], conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "total_sent" in r
        assert "total_received" in r


class TestTreasuryStatus:
    def test_status(self, conn, env, mod):
        r = call_action(mod.ACTIONS["status"], conn, ns())
        assert is_ok(r)
        assert r["skill"] == "erpclaw-treasury"
        assert "tables" in r
