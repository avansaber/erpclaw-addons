"""L1 pytest tests for erpclaw-loans (20 actions across loans, repayments, reports).

Tests cover:
  loans.py (13 actions): add/update/list/get loan-application, approve, reject,
    disburse, list/get loans, generate/get repayment schedule, restructure, close
  repayments.py (4 actions): record-repayment, list-repayments, calculate-interest, write-off
  reports.py (3 actions): statement, overdue-loans, status
"""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from loans_helpers import call_action, ns, is_ok, is_error, _uuid


# ===========================================================================
# Loan Application Actions
# ===========================================================================


class TestAddLoanApplication:
    def test_add_loan_application_ok(self, conn, env, mod):
        r = call_action(mod.loan_add_loan_application, conn, ns(
            company_id=env["company_id"],
            applicant_type="customer",
            applicant_id=env["customer_id"],
            loan_type="term_loan",
            requested_amount="50000",
            interest_rate="8.5",
            repayment_method="equal_installment",
            repayment_periods=12,
            purpose="Business expansion",
            collateral_description=None,
            collateral_value=None,
            applicant_name=None,
        ))
        assert is_ok(r), r
        assert "id" in r
        assert r["naming_series"].startswith("LAPP-")

    def test_add_loan_application_missing_company(self, conn, env, mod):
        r = call_action(mod.loan_add_loan_application, conn, ns(
            company_id=None,
            applicant_type="customer",
            applicant_id=env["customer_id"],
            loan_type="term_loan",
            requested_amount="50000",
            interest_rate="8.5",
            repayment_method=None,
            repayment_periods=None,
            purpose=None,
            collateral_description=None,
            collateral_value=None,
            applicant_name=None,
        ))
        assert is_error(r)

    def test_add_loan_application_zero_amount(self, conn, env, mod):
        r = call_action(mod.loan_add_loan_application, conn, ns(
            company_id=env["company_id"],
            applicant_type="customer",
            applicant_id=env["customer_id"],
            loan_type="term_loan",
            requested_amount="0",
            interest_rate="5",
            repayment_method=None,
            repayment_periods=None,
            purpose=None,
            collateral_description=None,
            collateral_value=None,
            applicant_name=None,
        ))
        assert is_error(r)

    def test_add_loan_application_supplier(self, conn, env, mod):
        """Test loan application with supplier applicant type."""
        r = call_action(mod.loan_add_loan_application, conn, ns(
            company_id=env["company_id"],
            applicant_type="supplier",
            applicant_id=env["supplier_id"],
            loan_type="demand_loan",
            requested_amount="25000",
            interest_rate="6.0",
            repayment_method="equal_principal",
            repayment_periods=6,
            purpose="Supplier financing",
            collateral_description=None,
            collateral_value=None,
            applicant_name=None,
        ))
        assert is_ok(r), r


class TestUpdateLoanApplication:
    def test_update_loan_application_ok(self, conn, env, mod):
        # Create first
        r1 = call_action(mod.loan_add_loan_application, conn, ns(
            company_id=env["company_id"],
            applicant_type="customer",
            applicant_id=env["customer_id"],
            loan_type="term_loan",
            requested_amount="50000",
            interest_rate="8.5",
            repayment_method=None,
            repayment_periods=None,
            purpose=None,
            collateral_description=None,
            collateral_value=None,
            applicant_name=None,
        ))
        assert is_ok(r1)
        app_id = r1["id"]

        r2 = call_action(mod.loan_update_loan_application, conn, ns(
            id=app_id,
            requested_amount="60000",
            interest_rate=None,
            applicant_type=None,
            applicant_id=None,
            loan_type=None,
            repayment_method=None,
            repayment_periods=None,
            purpose=None,
            collateral_description=None,
            collateral_value=None,
        ))
        assert is_ok(r2), r2
        assert "requested_amount" in r2["updated_fields"]


class TestListLoanApplications:
    def test_list_loan_applications(self, conn, env, mod):
        # Create two applications
        for amt in ["10000", "20000"]:
            call_action(mod.loan_add_loan_application, conn, ns(
                company_id=env["company_id"],
                applicant_type="customer",
                applicant_id=env["customer_id"],
                loan_type="term_loan",
                requested_amount=amt,
                interest_rate="5",
                repayment_method=None,
                repayment_periods=None,
                purpose=None,
                collateral_description=None,
                collateral_value=None,
                applicant_name=None,
            ))
        r = call_action(mod.loan_list_loan_applications, conn, ns(
            company_id=env["company_id"],
            status=None,
            applicant_type=None,
        ))
        assert is_ok(r), r
        assert r["total"] == 2


class TestGetLoanApplication:
    def test_get_loan_application_ok(self, conn, env, mod):
        r1 = call_action(mod.loan_add_loan_application, conn, ns(
            company_id=env["company_id"],
            applicant_type="customer",
            applicant_id=env["customer_id"],
            loan_type="term_loan",
            requested_amount="50000",
            interest_rate="8.5",
            repayment_method=None,
            repayment_periods=None,
            purpose=None,
            collateral_description=None,
            collateral_value=None,
            applicant_name=None,
        ))
        app_id = r1["id"]
        r2 = call_action(mod.loan_get_loan_application, conn, ns(id=app_id))
        assert is_ok(r2), r2
        assert r2["id"] == app_id

    def test_get_loan_application_not_found(self, conn, env, mod):
        r = call_action(mod.loan_get_loan_application, conn, ns(id=_uuid()))
        assert is_error(r)


# ===========================================================================
# Approve / Reject
# ===========================================================================


class TestApproveLoan:
    def test_approve_loan_ok(self, conn, env, mod):
        r1 = call_action(mod.loan_add_loan_application, conn, ns(
            company_id=env["company_id"],
            applicant_type="customer",
            applicant_id=env["customer_id"],
            loan_type="term_loan",
            requested_amount="50000",
            interest_rate="8.5",
            repayment_method=None,
            repayment_periods=None,
            purpose=None,
            collateral_description=None,
            collateral_value=None,
            applicant_name=None,
        ))
        app_id = r1["id"]

        r2 = call_action(mod.loan_approve_loan, conn, ns(
            id=app_id,
            approved_amount="45000",
        ))
        assert is_ok(r2), r2
        assert r2["loan_status"] == "approved"
        assert r2["approved_amount"] == "45000.00"

    def test_approve_defaults_to_requested(self, conn, env, mod):
        r1 = call_action(mod.loan_add_loan_application, conn, ns(
            company_id=env["company_id"],
            applicant_type="customer",
            applicant_id=env["customer_id"],
            loan_type="term_loan",
            requested_amount="30000",
            interest_rate="5",
            repayment_method=None,
            repayment_periods=None,
            purpose=None,
            collateral_description=None,
            collateral_value=None,
            applicant_name=None,
        ))
        app_id = r1["id"]
        r2 = call_action(mod.loan_approve_loan, conn, ns(
            id=app_id,
            approved_amount=None,
        ))
        assert is_ok(r2), r2
        assert r2["approved_amount"] == "30000.00"


class TestRejectLoan:
    def test_reject_loan_ok(self, conn, env, mod):
        r1 = call_action(mod.loan_add_loan_application, conn, ns(
            company_id=env["company_id"],
            applicant_type="customer",
            applicant_id=env["customer_id"],
            loan_type="term_loan",
            requested_amount="50000",
            interest_rate="8.5",
            repayment_method=None,
            repayment_periods=None,
            purpose=None,
            collateral_description=None,
            collateral_value=None,
            applicant_name=None,
        ))
        app_id = r1["id"]

        r2 = call_action(mod.loan_reject_loan, conn, ns(
            id=app_id,
            reason="Insufficient credit history",
        ))
        assert is_ok(r2), r2
        assert r2["loan_status"] == "rejected"

    def test_reject_loan_missing_reason(self, conn, env, mod):
        r1 = call_action(mod.loan_add_loan_application, conn, ns(
            company_id=env["company_id"],
            applicant_type="customer",
            applicant_id=env["customer_id"],
            loan_type="term_loan",
            requested_amount="50000",
            interest_rate="8.5",
            repayment_method=None,
            repayment_periods=None,
            purpose=None,
            collateral_description=None,
            collateral_value=None,
            applicant_name=None,
        ))
        app_id = r1["id"]

        r2 = call_action(mod.loan_reject_loan, conn, ns(
            id=app_id,
            reason=None,
        ))
        assert is_error(r2)


# ===========================================================================
# Disburse Loan
# ===========================================================================


def _create_approved_app(conn, env, mod, amount="50000", rate="8.5", periods=12, method=None):
    """Helper: create and approve a loan application, returning app_id."""
    r1 = call_action(mod.loan_add_loan_application, conn, ns(
        company_id=env["company_id"],
        applicant_type="customer",
        applicant_id=env["customer_id"],
        loan_type="term_loan",
        requested_amount=amount,
        interest_rate=rate,
        repayment_method=method,
        repayment_periods=periods,
        purpose=None,
        collateral_description=None,
        collateral_value=None,
        applicant_name=None,
    ))
    assert is_ok(r1), r1
    app_id = r1["id"]

    r2 = call_action(mod.loan_approve_loan, conn, ns(
        id=app_id,
        approved_amount=None,
    ))
    assert is_ok(r2), r2
    return app_id


def _disburse_loan(conn, env, mod, app_id):
    """Helper: disburse a loan from an approved application, return result."""
    return call_action(mod.loan_disburse_loan, conn, ns(
        loan_application_id=app_id,
        loan_account_id=env["loan_account_id"],
        interest_income_account_id=env["interest_income_account_id"],
        disbursement_account_id=env["disbursement_account_id"],
        disbursement_date="2025-06-01",
    ))


class TestDisburseLoan:
    def test_disburse_loan_ok(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod)
        r = _disburse_loan(conn, env, mod, app_id)
        assert is_ok(r), r
        assert r["loan_amount"] == "50000.00"
        assert r["installments"] == 12
        assert "loan_id" in r

    def test_disburse_loan_not_approved(self, conn, env, mod):
        # Create application but do not approve
        r1 = call_action(mod.loan_add_loan_application, conn, ns(
            company_id=env["company_id"],
            applicant_type="customer",
            applicant_id=env["customer_id"],
            loan_type="term_loan",
            requested_amount="50000",
            interest_rate="8.5",
            repayment_method=None,
            repayment_periods=None,
            purpose=None,
            collateral_description=None,
            collateral_value=None,
            applicant_name=None,
        ))
        app_id = r1["id"]
        r = _disburse_loan(conn, env, mod, app_id)
        assert is_error(r)


# ===========================================================================
# List / Get Loans
# ===========================================================================


class TestListLoans:
    def test_list_loans(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod)
        _disburse_loan(conn, env, mod, app_id)

        r = call_action(mod.loan_list_loans, conn, ns(
            company_id=env["company_id"],
            status=None,
            applicant_type=None,
        ))
        assert is_ok(r), r
        assert r["total"] >= 1


class TestGetLoan:
    def test_get_loan_with_schedule(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        r = call_action(mod.loan_get_loan, conn, ns(
            loan_id=loan_id,
            id=None,
        ))
        assert is_ok(r), r
        assert r["id"] == loan_id
        assert len(r["repayment_schedule"]) == 12


# ===========================================================================
# Repayment Schedule
# ===========================================================================


class TestGetRepaymentSchedule:
    def test_get_repayment_schedule(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        r = call_action(mod.loan_get_repayment_schedule, conn, ns(
            loan_id=loan_id,
        ))
        assert is_ok(r), r
        assert len(r["schedule"]) == 12


class TestGenerateRepaymentSchedule:
    def test_regenerate_schedule(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        r = call_action(mod.loan_generate_repayment_schedule, conn, ns(
            loan_id=loan_id,
        ))
        assert is_ok(r), r
        assert r["installments"] == 12


# ===========================================================================
# Restructure Loan
# ===========================================================================


class TestRestructureLoan:
    def test_restructure_loan_new_rate(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod, amount="100000", rate="10", periods=24)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        r = call_action(mod.loan_restructure_loan, conn, ns(
            loan_id=loan_id,
            new_interest_rate="7.5",
            new_repayment_periods=None,
        ))
        assert is_ok(r), r
        assert r["installments_regenerated"] == 24

    def test_restructure_loan_missing_params(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        r = call_action(mod.loan_restructure_loan, conn, ns(
            loan_id=loan_id,
            new_interest_rate=None,
            new_repayment_periods=None,
        ))
        assert is_error(r)


# ===========================================================================
# Record Repayment
# ===========================================================================


class TestRecordRepayment:
    def test_record_repayment_ok(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod, amount="12000", rate="0", periods=12)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        r = call_action(mod.loan_record_repayment, conn, ns(
            loan_id=loan_id,
            principal_amount="1000",
            interest_amount="0",
            penalty_amount=None,
            payment_method="bank_transfer",
            repayment_date="2025-07-01",
            reference_number="CHK-001",
            remarks="First payment",
        ))
        assert is_ok(r), r
        assert r["total_amount"] == "1000"
        assert r["loan_status"] == "partially_repaid"

    def test_record_repayment_zero_amount(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod, amount="12000", rate="0", periods=12)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        r = call_action(mod.loan_record_repayment, conn, ns(
            loan_id=loan_id,
            principal_amount="0",
            interest_amount="0",
            penalty_amount=None,
            payment_method=None,
            repayment_date=None,
            reference_number=None,
            remarks=None,
        ))
        assert is_error(r)


class TestListRepayments:
    def test_list_repayments(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod, amount="12000", rate="0", periods=12)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        # Record a repayment
        call_action(mod.loan_record_repayment, conn, ns(
            loan_id=loan_id,
            principal_amount="1000",
            interest_amount="0",
            penalty_amount=None,
            payment_method=None,
            repayment_date=None,
            reference_number=None,
            remarks=None,
        ))

        r = call_action(mod.loan_list_repayments, conn, ns(loan_id=loan_id))
        assert is_ok(r), r
        assert r["total"] == 1


# ===========================================================================
# Calculate Interest
# ===========================================================================


class TestCalculateInterest:
    def test_calculate_interest_ok(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod, amount="100000", rate="12", periods=12)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        r = call_action(mod.loan_calculate_interest, conn, ns(
            loan_id=loan_id,
            as_of_date="2025-12-01",
        ))
        assert is_ok(r), r
        assert r["days"] > 0
        # At 12% annual on 100,000, interest should be substantial
        from decimal import Decimal
        accrued = Decimal(r["accrued_interest"])
        assert accrued > Decimal("0")


# ===========================================================================
# Write Off Loan
# ===========================================================================


class TestWriteOffLoan:
    def test_write_off_loan_ok(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod, amount="50000", rate="10", periods=12)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        r = call_action(mod.loan_write_off_loan, conn, ns(
            loan_id=loan_id,
            bad_debt_account_id=env["bad_debt_account_id"],
            reason="Borrower declared bankruptcy",
            write_off_date="2025-12-31",
        ))
        assert is_ok(r), r
        assert r["write_off_amount"] == "50000.00"

    def test_write_off_missing_bad_debt_account(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        r = call_action(mod.loan_write_off_loan, conn, ns(
            loan_id=loan_id,
            bad_debt_account_id=None,
            reason=None,
            write_off_date=None,
        ))
        assert is_error(r)


# ===========================================================================
# Close Loan
# ===========================================================================


class TestCloseLoan:
    def test_close_fully_repaid_loan(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod, amount="3000", rate="0", periods=3)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        # Repay all 3 installments
        for i in range(3):
            call_action(mod.loan_record_repayment, conn, ns(
                loan_id=loan_id,
                principal_amount="1000",
                interest_amount="0",
                penalty_amount=None,
                payment_method=None,
                repayment_date=None,
                reference_number=None,
                remarks=None,
            ))

        r = call_action(mod.loan_close_loan, conn, ns(loan_id=loan_id))
        assert is_ok(r), r
        assert r["loan_status"] == "closed"

    def test_close_loan_with_outstanding_fails(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod, amount="50000", rate="10", periods=12)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        r = call_action(mod.loan_close_loan, conn, ns(loan_id=loan_id))
        assert is_error(r)


# ===========================================================================
# Reports
# ===========================================================================


class TestLoanStatement:
    def test_loan_statement(self, conn, env, mod):
        app_id = _create_approved_app(conn, env, mod)
        dr = _disburse_loan(conn, env, mod, app_id)
        loan_id = dr["loan_id"]

        r = call_action(mod.loan_statement, conn, ns(loan_id=loan_id))
        assert is_ok(r), r
        assert "loan" in r
        assert "schedule" in r
        assert "repayments" in r


class TestOverdueLoans:
    def test_overdue_loans_report(self, conn, env, mod):
        r = call_action(mod.loan_overdue_loans, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert "total" in r
        assert "total_overdue_amount" in r


class TestStatus:
    def test_status_report(self, conn, env, mod):
        r = call_action(mod.status, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert r["skill"] == "erpclaw-loans"
