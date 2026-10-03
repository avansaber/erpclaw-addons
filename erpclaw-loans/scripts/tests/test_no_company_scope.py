"""No-company scope for loan-overdue-loans plus the loans --company flag."""
import io
import json
import uuid
from datetime import date, timedelta
from unittest.mock import patch

from loans_helpers import (
    call_action, ns, is_error, is_ok,
    seed_naming_series, seed_cost_center, seed_fiscal_year,
    seed_customer, seed_account,
)
from erpclaw_lib.query import Q, Table

MULTI_ERROR = "Multiple companies found. Please specify the company by name."
MULTI_SUGGESTION = "Pass the company name (e.g. --company \"Acme\"), or use --company-id with one of the IDs above."
ZERO_ERROR = "No company found. Create one first."
ZERO_SUGGESTION = "Run 'tutorial' to create a demo company, or 'setup company' to create your own."
NAME_SUGGESTION = "Use one of the available company names exactly, or run 'list-companies' to see them."

_STATE_TABLES = ("loan", "loan_repayment_schedule", "company", "audit_log")


def _rows(conn, table):
    t = Table(table)
    try:
        rows = conn.execute(Q.from_(t).select(t.star).get_sql()).fetchall()
    except Exception:
        return []
    return sorted(json.dumps(dict(r), sort_keys=True, default=str) for r in rows)


def _state(conn):
    return {t: _rows(conn, t) for t in _STATE_TABLES}


def _seed_company(conn, name, abbr):
    cid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO company (id, name, abbr, default_currency, country, fiscal_year_start_month)"
        " VALUES (?, ?, ?, 'USD', 'United States', 1)",
        (cid, name, abbr))
    conn.commit()
    seed_naming_series(conn, cid)
    seed_cost_center(conn, cid)
    seed_fiscal_year(conn, cid)
    cust = seed_customer(conn, cid, "Acme Corp")
    loan_acct = seed_account(conn, cid, "Loan Receivable", "receivable", "asset")
    int_acct = seed_account(conn, cid, "Interest Income", "revenue", "income")
    disb_acct = seed_account(conn, cid, "Bank Account", "bank", "asset")
    return {
        "company_id": cid,
        "customer_id": cust,
        "loan_account_id": loan_acct,
        "interest_income_account_id": int_acct,
        "disbursement_account_id": disb_acct,
    }


def _disburse_one(conn, env, mod, amount):
    disb_date = (date.today() - timedelta(days=60)).isoformat()
    created = call_action(mod.ACTIONS["loan-add-loan-application"], conn, ns(
        company_id=env["company_id"], applicant_type="customer",
        applicant_id=env["customer_id"], loan_type="term_loan",
        requested_amount=amount, interest_rate="0",
        repayment_method="equal_installment", repayment_periods=1,
        purpose=None, collateral_description=None, collateral_value=None,
        applicant_name=None))
    assert is_ok(created), created
    approved = call_action(mod.ACTIONS["loan-approve-loan"], conn, ns(
        id=created["id"], approved_amount=None))
    assert is_ok(approved), approved
    disbursed = call_action(mod.ACTIONS["loan-disburse-loan"], conn, ns(
        loan_application_id=created["id"],
        loan_account_id=env["loan_account_id"],
        interest_income_account_id=env["interest_income_account_id"],
        disbursement_account_id=env["disbursement_account_id"],
        disbursement_date=disb_date))
    assert is_ok(disbursed), disbursed
    return disbursed


def _two(conn, mod):
    acme = _seed_company(conn, "Acme Widgets", "ACME")
    wayne = _seed_company(conn, "Wayne Enterprises", "WAYNE")
    _disburse_one(conn, acme, mod, "1000.00")
    _disburse_one(conn, wayne, mod, "1829.83")
    return acme, wayne


def _multi_expected(acme_id, wayne_id):
    return {
        "status": "error",
        "error": MULTI_ERROR,
        "companies": [
            {"id": acme_id, "name": "Acme Widgets"},
            {"id": wayne_id, "name": "Wayne Enterprises"},
        ],
        "suggestion": MULTI_SUGGESTION,
        "message": MULTI_ERROR,
    }


def test_overdue_two_companies_no_company_refuses(conn, mod):
    acme, wayne = _two(conn, mod)
    before = _state(conn)
    r = call_action(mod.ACTIONS["loan-overdue-loans"], conn,
                    ns(company_id=None, company_name=None))
    assert r == _multi_expected(acme["company_id"], wayne["company_id"])
    assert _state(conn) == before


def test_overdue_zero_companies_refuses(conn, mod):
    r = call_action(mod.ACTIONS["loan-overdue-loans"], conn,
                    ns(company_id=None, company_name=None))
    assert r == {
        "status": "error",
        "error": ZERO_ERROR,
        "suggestion": ZERO_SUGGESTION,
        "message": ZERO_ERROR,
    }


def test_overdue_unknown_company_refuses(conn, mod):
    _two(conn, mod)
    r = call_action(mod.ACTIONS["loan-overdue-loans"], conn,
                    ns(company_id="no-such-company", company_name=None))
    assert r == {
        "status": "error",
        "error": "Company not found: no-such-company",
        "message": "Company not found: no-such-company",
    }


def test_overdue_explicit_company(conn, mod):
    _acme, wayne = _two(conn, mod)
    r = call_action(mod.ACTIONS["loan-overdue-loans"], conn,
                    ns(company_id=wayne["company_id"], company_name=None))
    assert is_ok(r), r
    assert r["total"] == 1
    assert r["total_overdue_amount"] == "1829.83"


def test_overdue_one_company_uses_it(conn, mod):
    acme = _seed_company(conn, "Acme Widgets", "ACME")
    _disburse_one(conn, acme, mod, "1000.00")
    r_none = call_action(mod.ACTIONS["loan-overdue-loans"], conn,
                         ns(company_id=None, company_name=None))
    r_scoped = call_action(mod.ACTIONS["loan-overdue-loans"], conn,
                           ns(company_id=acme["company_id"], company_name=None))
    assert is_ok(r_none), r_none
    assert is_ok(r_scoped), r_scoped
    assert r_none == r_scoped
    assert r_none["total"] == 1
    assert r_none["total_overdue_amount"] == "1000.00"


def _call_flag(mod, conn, args):
    buf = io.StringIO()

    def _fake_exit(code=0):
        raise SystemExit(code)

    try:
        with patch("sys.stdout", buf), patch("sys.exit", side_effect=_fake_exit):
            mod._resolve_company_flag(conn, args)
            return None
    except SystemExit:
        pass
    out = buf.getvalue().strip()
    return json.loads(out) if out else None


def test_company_flag_resolves_name(conn, mod):
    acme = _seed_company(conn, "Acme Widgets", "ACME")
    wayne = _seed_company(conn, "Wayne Enterprises", "WAYNE")
    args = ns(company_id=None, company_name="acme widgets")
    assert _call_flag(mod, conn, args) is None
    assert args.company_id == acme["company_id"]
    args2 = ns(company_id=None, company_name=acme["company_id"])
    assert _call_flag(mod, conn, args2) is None
    assert args2.company_id == acme["company_id"]
    args3 = ns(company_id=None, company_name="Acme")
    r = _call_flag(mod, conn, args3)
    assert r == {
        "status": "error",
        "error": "Company 'Acme' not found.",
        "available_companies": ["Acme Widgets", "Wayne Enterprises"],
        "suggestion": NAME_SUGGESTION,
        "message": "Company 'Acme' not found.",
    }
