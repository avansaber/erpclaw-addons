"""Stale-write (compare-and-set) tests for loan application status changes (m860).

``handle_approve_loan``, ``handle_reject_loan`` and
``handle_update_loan_application`` must only write when the application's
status is still the one they validated. Each test simulates a stale read
deterministically, without threads: the row is fetched, the status is then
changed in the database with a direct UPDATE on the same connection, and the
handler's application read (``_validate_loan_application``) is monkeypatched
to return the row as it was.
"""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from loans_helpers import call_action, ns, is_ok, is_error  # noqa: E402


def _msg(result):
    return result.get("message", "") + result.get("error", "")


def _create_app(conn, env, mod, amount="50000"):
    r = call_action(mod.loan_add_loan_application, conn, ns(
        company_id=env["company_id"],
        applicant_type="customer",
        applicant_id=env["customer_id"],
        loan_type="term_loan",
        requested_amount=amount,
        interest_rate="8.5",
        repayment_method=None,
        repayment_periods=None,
        purpose=None,
        collateral_description=None,
        collateral_value=None,
        applicant_name=None,
    ))
    assert is_ok(r), r
    return r["id"]


def _mark_applied(conn, app_id):
    conn.execute(
        "UPDATE loan_application SET status = 'applied' WHERE id = ?",
        (app_id,))
    conn.commit()


def _stale_row(conn, app_id):
    return conn.execute(
        "SELECT * FROM loan_application WHERE id = ?", (app_id,)).fetchone()


def _plant_stale(monkeypatch, stale):
    loans = sys.modules.get("loans")
    assert loans is not None, "loans module not loaded; stale plant impossible"
    assert hasattr(loans, "_validate_loan_application")
    monkeypatch.setattr(
        loans, "_validate_loan_application", lambda c, i: stale)


def _status(conn, app_id):
    return conn.execute(
        "SELECT status FROM loan_application WHERE id = ?",
        (app_id,)).fetchone()["status"]


def _app(conn, app_id):
    return conn.execute(
        "SELECT * FROM loan_application WHERE id = ?", (app_id,)).fetchone()


def _audit_count(conn, action, entity_id):
    return conn.execute(
        "SELECT COUNT(*) FROM audit_log WHERE action = ? AND entity_id = ?",
        (action, entity_id)).fetchone()[0]


def _table_count(conn, table):
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _expected_msg(app_id, current):
    return (f"Loan application {app_id} changed while this request was "
            f"running (now '{current}'); re-read it and try again.")


def _disburse(conn, env, mod, app_id):
    return call_action(mod.loan_disburse_loan, conn, ns(
        loan_application_id=app_id,
        loan_account_id=env["loan_account_id"],
        interest_income_account_id=env["interest_income_account_id"],
        disbursement_account_id=env["disbursement_account_id"],
        disbursement_date="2025-06-01",
    ))


def test_stale_reject_after_approve(conn, env, mod, monkeypatch):
    app_id = _create_app(conn, env, mod)
    _mark_applied(conn, app_id)
    stale = _stale_row(conn, app_id)
    assert stale["status"] == "applied"

    approved = call_action(mod.loan_approve_loan, conn, ns(
        id=app_id, approved_amount=None))
    assert is_ok(approved), approved

    _plant_stale(monkeypatch, stale)
    r = call_action(mod.loan_reject_loan, conn, ns(
        id=app_id, reason="stale rejection"))
    assert is_error(r), r
    assert _msg(r) == _expected_msg(app_id, "approved"), r
    assert _status(conn, app_id) == "approved"
    assert _audit_count(conn, "loan-reject-loan", app_id) == 0


def test_stale_reject_after_disbursement(conn, env, mod, monkeypatch):
    app_id = _create_app(conn, env, mod)
    _mark_applied(conn, app_id)
    stale = _stale_row(conn, app_id)

    approved = call_action(mod.loan_approve_loan, conn, ns(
        id=app_id, approved_amount=None))
    assert is_ok(approved), approved
    disbursed = _disburse(conn, env, mod, app_id)
    assert is_ok(disbursed), disbursed
    loan_id = disbursed["loan_id"]

    loans_before = _table_count(conn, "loan")
    gl_before = _table_count(conn, "gl_entry")
    loan_status_before = conn.execute(
        "SELECT status FROM loan WHERE id = ?", (loan_id,)).fetchone()["status"]

    _plant_stale(monkeypatch, stale)
    r = call_action(mod.loan_reject_loan, conn, ns(
        id=app_id, reason="stale rejection"))
    assert is_error(r), r
    assert _msg(r) == _expected_msg(app_id, "approved"), r
    assert _status(conn, app_id) == "approved"
    assert _table_count(conn, "loan") == loans_before
    assert _table_count(conn, "gl_entry") == gl_before
    assert conn.execute(
        "SELECT status FROM loan WHERE id = ?", (loan_id,)).fetchone()["status"] == loan_status_before
    assert _audit_count(conn, "loan-reject-loan", app_id) == 0


def test_stale_approve_after_reject(conn, env, mod, monkeypatch):
    app_id = _create_app(conn, env, mod)
    _mark_applied(conn, app_id)
    stale = _stale_row(conn, app_id)

    rejected = call_action(mod.loan_reject_loan, conn, ns(
        id=app_id, reason="No credit history"))
    assert is_ok(rejected), rejected
    approve_audits_before = _audit_count(conn, "loan-approve-loan", app_id)

    _plant_stale(monkeypatch, stale)
    r = call_action(mod.loan_approve_loan, conn, ns(
        id=app_id, approved_amount="40000"))
    assert is_error(r), r
    assert _msg(r) == _expected_msg(app_id, "rejected"), r
    row = _app(conn, app_id)
    assert row["status"] == "rejected"
    assert row["rejection_reason"] == "No credit history"
    assert _audit_count(conn, "loan-approve-loan", app_id) == approve_audits_before


def test_stale_update_after_approve(conn, env, mod, monkeypatch):
    app_id = _create_app(conn, env, mod)
    _mark_applied(conn, app_id)
    stale = _stale_row(conn, app_id)

    approved = call_action(mod.loan_approve_loan, conn, ns(
        id=app_id, approved_amount=None))
    assert is_ok(approved), approved
    before = _app(conn, app_id)
    update_audits_before = _audit_count(conn, "loan-update-loan-application", app_id)

    _plant_stale(monkeypatch, stale)
    r = call_action(mod.loan_update_loan_application, conn, ns(
        id=app_id, requested_amount="60000"))
    assert is_error(r), r
    assert _msg(r) == _expected_msg(app_id, "approved"), r
    after = _app(conn, app_id)
    assert after["requested_amount"] == before["requested_amount"]
    assert after["approved_amount"] == before["approved_amount"]
    assert _audit_count(conn, "loan-update-loan-application", app_id) == update_audits_before


def test_happy_approve_still_succeeds(conn, env, mod):
    app_id = _create_app(conn, env, mod)
    r = call_action(mod.loan_approve_loan, conn, ns(
        id=app_id, approved_amount="45000"))
    assert is_ok(r), r
    assert r == {
        "status": "ok",
        "id": app_id,
        "loan_status": "approved",
        "approved_amount": "45000.00",
    }, r
    assert _status(conn, app_id) == "approved"
    assert _audit_count(conn, "loan-approve-loan", app_id) == 1


def test_happy_reject_still_succeeds(conn, env, mod):
    app_id = _create_app(conn, env, mod)
    r = call_action(mod.loan_reject_loan, conn, ns(
        id=app_id, reason="Insufficient credit history"))
    assert is_ok(r), r
    assert r == {
        "status": "ok",
        "id": app_id,
        "loan_status": "rejected",
        "rejection_reason": "Insufficient credit history",
    }, r
    assert _status(conn, app_id) == "rejected"
    assert _audit_count(conn, "loan-reject-loan", app_id) == 1


def test_happy_update_still_succeeds(conn, env, mod):
    app_id = _create_app(conn, env, mod)
    r = call_action(mod.loan_update_loan_application, conn, ns(
        id=app_id, requested_amount="60000"))
    assert is_ok(r), r
    assert r == {
        "status": "ok",
        "id": app_id,
        "updated_fields": ["requested_amount"],
    }, r
    assert _app(conn, app_id)["requested_amount"] == "60000.00"
    assert _audit_count(conn, "loan-update-loan-application", app_id) == 1


def test_stale_amount_approve_refused(conn, env, mod, monkeypatch):
    app_id = _create_app(conn, env, mod, amount="50000.00")
    _mark_applied(conn, app_id)
    stale = _stale_row(conn, app_id)
    assert stale["status"] == "applied"
    assert stale["requested_amount"] == "50000.00"
    before = _app(conn, app_id)

    conn.execute(
        "UPDATE loan_application SET requested_amount = '30000.00' WHERE id = ?",
        (app_id,))
    conn.commit()

    approve_audits_before = _audit_count(conn, "loan-approve-loan", app_id)
    _plant_stale(monkeypatch, stale)
    r = call_action(mod.loan_approve_loan, conn, ns(
        id=app_id, approved_amount=None))
    assert is_error(r), r
    assert _msg(r) == _expected_msg(app_id, "applied"), r
    row = _app(conn, app_id)
    assert row["status"] == "applied"
    assert row["requested_amount"] == "30000.00"
    assert row["approved_amount"] == before["approved_amount"]
    assert row["approved_amount"] in (None, "0")
    assert _audit_count(conn, "loan-approve-loan", app_id) == approve_audits_before

    monkeypatch.undo()
    fresh = call_action(mod.loan_approve_loan, conn, ns(
        id=app_id, approved_amount=None))
    assert is_ok(fresh), fresh
    assert fresh["approved_amount"] == "30000.00"
    assert _app(conn, app_id)["approved_amount"] == "30000.00"

    disbursed = _disburse(conn, env, mod, app_id)
    assert is_ok(disbursed), disbursed
    loan = conn.execute(
        "SELECT loan_amount FROM loan WHERE id = ?",
        (disbursed["loan_id"],)).fetchone()
    assert loan["loan_amount"] == "30000.00"


def test_explicit_amount_approve_ignores_stale_requested(conn, env, mod, monkeypatch):
    app_id = _create_app(conn, env, mod, amount="50000.00")
    _mark_applied(conn, app_id)
    stale = _stale_row(conn, app_id)
    assert stale["status"] == "applied"
    assert stale["requested_amount"] == "50000.00"

    conn.execute(
        "UPDATE loan_application SET requested_amount = '30000.00' WHERE id = ?",
        (app_id,))
    conn.commit()

    _plant_stale(monkeypatch, stale)
    r = call_action(mod.loan_approve_loan, conn, ns(
        id=app_id, approved_amount="45000.00"))
    assert is_ok(r), r
    assert r["approved_amount"] == "45000.00"
    row = _app(conn, app_id)
    assert row["status"] == "approved"
    assert row["approved_amount"] == "45000.00"


import importlib.util as _importlib_util  # noqa: E402

import pytest as _pytest  # noqa: E402

import loans_helpers as _loans_helpers  # noqa: E402

_PAY_TESTS = os.path.normpath(os.path.join(
    _TESTS_DIR, "..", "..", "..", "..", "erpclaw", "scripts",
    "erpclaw-payments", "tests"))
if _PAY_TESTS not in sys.path:
    sys.path.insert(0, _PAY_TESTS)
_proofs_spec = _importlib_util.spec_from_file_location(
    "loan_race_proofs_src",
    os.path.join(_PAY_TESTS, "test_chain_lock_proofs.py"))
_proofs = _importlib_util.module_from_spec(_proofs_spec)
_proofs_spec.loader.exec_module(_proofs)
_pay_helpers_spec = _importlib_util.spec_from_file_location(
    "loan_race_payments_helpers_pg",
    os.path.join(_PAY_TESTS, "payments_helpers.py"))
_pay_helpers = _importlib_util.module_from_spec(_pay_helpers_spec)
_pay_helpers_spec.loader.exec_module(_pay_helpers)

_LOANS_INIT_PATH = os.path.normpath(
    os.path.join(_TESTS_DIR, "..", "..", "init_db.py"))
_loans_init_spec = _importlib_util.spec_from_file_location(
    "loans_init_pg_race", _LOANS_INIT_PATH)
_loans_init = _importlib_util.module_from_spec(_loans_init_spec)
_loans_init_spec.loader.exec_module(_loans_init)

_pg_only = _proofs._pg_only


def _build_env_pg(conn):
    cid = _loans_helpers.seed_company(conn)
    _loans_helpers.seed_cost_center(conn, cid)
    _loans_helpers.seed_fiscal_year(conn, cid)
    cust = _loans_helpers.seed_customer(conn, cid, "Acme Corp")
    emp = _loans_helpers.seed_employee(conn, cid, "John Doe")
    sup = _loans_helpers.seed_supplier(conn, cid, "Parts Inc")
    loan_acct = _loans_helpers.seed_account(
        conn, cid, "Loan Receivable", "receivable", "asset")
    interest_acct = _loans_helpers.seed_account(
        conn, cid, "Interest Income", "revenue", "income")
    disbursement_acct = _loans_helpers.seed_account(
        conn, cid, "Bank Account", "bank", "asset")
    bad_debt_acct = _loans_helpers.seed_account(
        conn, cid, "Bad Debt Expense", "expense", "expense")
    return {
        "company_id": cid,
        "customer_id": cust,
        "employee_id": emp,
        "supplier_id": sup,
        "loan_account_id": loan_acct,
        "interest_income_account_id": interest_acct,
        "disbursement_account_id": disbursement_acct,
        "bad_debt_account_id": bad_debt_acct,
    }


@_pytest.fixture
def pg_book():
    _pg_only()
    pg_url = os.environ.get("ERPCLAW_PG_TEST_URL")
    if not pg_url:
        _pytest.skip("PostgreSQL legs need ERPCLAW_PG_TEST_URL")
    from erpclaw_lib.db import get_connection as _get_connection  # noqa: E402
    old_url = os.environ.get("ERPCLAW_DB_URL")
    old_path = os.environ.get("ERPCLAW_DB_PATH")
    os.environ["ERPCLAW_DB_URL"] = pg_url
    os.environ.pop("ERPCLAW_DB_PATH", None)
    conn = None
    try:
        try:
            _pay_helpers.init_all_tables(None)
        except Exception:
            _pytest.skip("PostgreSQL foundation build failed")
        try:
            _loans_init.create_loans_tables(
                os.environ["ERPCLAW_PG_TEST_URL"])
        except Exception:
            _pytest.skip("PostgreSQL loans-table build failed")
        conn = _get_connection()
        try:
            env = _build_env_pg(conn)
        except Exception:
            _pytest.skip("PostgreSQL book build failed")
        mod = _loans_helpers.load_db_query()
        yield (conn, env, mod)
    finally:
        try:
            if conn is not None:
                conn.close()
        finally:
            if old_url is None:
                os.environ.pop("ERPCLAW_DB_URL", None)
            else:
                os.environ["ERPCLAW_DB_URL"] = old_url
            if old_path is None:
                os.environ.pop("ERPCLAW_DB_PATH", None)
            else:
                os.environ["ERPCLAW_DB_PATH"] = old_path


def test_stale_reject_after_approve_postgresql(pg_book, monkeypatch):
    conn, env, mod = pg_book
    app_id = _create_app(conn, env, mod)
    _mark_applied(conn, app_id)
    stale = _stale_row(conn, app_id)
    assert stale["status"] == "applied"

    approved = call_action(mod.loan_approve_loan, conn, ns(
        id=app_id, approved_amount=None))
    assert is_ok(approved), approved

    _plant_stale(monkeypatch, stale)
    r = call_action(mod.loan_reject_loan, conn, ns(
        id=app_id, reason="stale rejection"))
    assert is_error(r), r
    assert _msg(r) == _expected_msg(app_id, "approved"), r
    assert _status(conn, app_id) == "approved"
    assert _audit_count(conn, "loan-reject-loan", app_id) == 0


def test_stale_amount_approve_postgresql(pg_book, monkeypatch):
    conn, env, mod = pg_book
    app_id = _create_app(conn, env, mod, amount="50000.00")
    _mark_applied(conn, app_id)
    stale = _stale_row(conn, app_id)
    assert stale["status"] == "applied"
    assert stale["requested_amount"] == "50000.00"

    conn.execute(
        "UPDATE loan_application SET requested_amount = '30000.00' WHERE id = ?",
        (app_id,))
    conn.commit()

    _plant_stale(monkeypatch, stale)
    r = call_action(mod.loan_approve_loan, conn, ns(
        id=app_id, approved_amount=None))
    assert is_error(r), r
    assert _msg(r) == _expected_msg(app_id, "applied"), r
    row = _app(conn, app_id)
    assert row["status"] == "applied"
    assert row["requested_amount"] == "30000.00"
    assert row["approved_amount"] is None or row["approved_amount"] in ("0", "0.00")
    assert _audit_count(conn, "loan-approve-loan", app_id) == 0
