"""Part A — LEDGER tests for `loan-disburse-loan` and `loan-write-off-loan`
(Wave G F21).

Both actions already had value tests (`test_loans.py` asserts the payload's
`loan_amount`, the installment count, the write-off amount), so the test-depth
classifier scores them `behavioral`. Neither test looks at `gl_entry`. That gap
is the point of this file: a loan can be recorded, repaid and written off with
correct-looking documents while the general ledger stays empty, and no existing
assertion would notice.

Register rows: `planning/wave_g/F21_TEST_DEPTH_REGISTER_2026-08-11.json`
(`loan-disburse-loan`, `loan-write-off-loan` — both `behavioral` on payload
asserts alone, ledger reach `gl_entry`). This file is the worked example behind
the digest's "behavioral is not ledger-covered" caveat.

F21-FINDING-4 (`loan-disburse-loan` posted nothing) and the repayment site the
M62 sweep found alongside it are FIXED as of 2026-08-12; the pins below are the
repaired readings plus the rollback proofs. SIM:
`planning/simlogs/m62_SIM_2026-08-12.md`.
"""
import os
import sys
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from loans_helpers import call_action, is_error, is_ok, ns  # noqa: E402

D = Decimal
DISBURSE_DATE = "2026-06-01"


def _msg(result: dict) -> str:
    return result.get("message", "") + result.get("error", "")


def _approved_application(conn, env, mod, amount="50000", rate="8.5", periods=12):
    created = call_action(mod.loan_add_loan_application, conn, ns(
        company_id=env["company_id"], applicant_type="customer",
        applicant_id=env["customer_id"], loan_type="term_loan",
        requested_amount=amount, interest_rate=rate, repayment_method=None,
        repayment_periods=periods, purpose=None, collateral_description=None,
        collateral_value=None, applicant_name=None))
    assert is_ok(created), created
    approved = call_action(mod.loan_approve_loan, conn,
                           ns(id=created["id"], approved_amount=None))
    assert is_ok(approved), approved
    return created["id"]


def _disburse(conn, env, mod, app_id):
    return call_action(mod.loan_disburse_loan, conn, ns(
        loan_application_id=app_id,
        loan_account_id=env["loan_account_id"],
        interest_income_account_id=env["interest_income_account_id"],
        disbursement_account_id=env["disbursement_account_id"],
        disbursement_date=DISBURSE_DATE))


def _gl(conn, voucher_id):
    return conn.execute(
        "SELECT account_id, debit, credit, party_type, party_id, remarks "
        "FROM gl_entry WHERE voucher_id = ? AND is_cancelled = 0 "
        "ORDER BY debit DESC", (voucher_id,)).fetchall()


def _gl_by_account(conn, voucher_id):
    """GL rows for a voucher keyed by account — for postings with >2 legs,
    where ordering by amount is not a stable way to name them."""
    return {r["account_id"]: r for r in _gl(conn, voucher_id)}


def _loan(conn, loan_id):
    return conn.execute(
        "SELECT loan_amount, disbursed_amount, outstanding_amount, "
        "total_interest, status FROM loan WHERE id = ?", (loan_id,)).fetchone()


def _count(conn, table):
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _sum(conn, sql, params=()):
    """Exact Decimal sum of a single money column."""
    # Summed in Python rather than via the decimal_sum() SQL aggregate: this
    # module's test connection comes from loans_helpers.get_conn, which builds a
    # bare driver connection, so the aggregate that erpclaw_lib.db.get_connection
    # registers is not present on it.
    return sum((D(r[0]) for r in conn.execute(sql, params).fetchall()), D("0"))


def _plant_gl_failure(monkeypatch, module_name, message):
    """Make the named domain module's insert_gl_entries raise.

    The domain modules bind `insert_gl_entries` as a module global at import
    time, so patching that global is what the handler actually calls. Both
    modules are already in sys.modules by the time the `mod` fixture has loaded
    db_query.py (which imports them by name off its own directory).
    """
    module = sys.modules[module_name]
    assert hasattr(module, "insert_gl_entries"), (
        f"{module_name} does not bind insert_gl_entries — the plant would be "
        f"a no-op and the rollback proof would pass for the wrong reason")

    def _boom(*args, **kwargs):
        raise ValueError(message)

    monkeypatch.setattr(module, "insert_gl_entries", _boom)


# ── loan-disburse-loan ───────────────────────────────────────────────────────

def test_disbursement_records_exact_amounts_and_a_full_schedule(conn, env, mod):
    app_id = _approved_application(conn, env, mod, amount="50000", periods=12)
    res = _disburse(conn, env, mod, app_id)
    assert is_ok(res), res

    loan = _loan(conn, res["loan_id"])
    assert D(loan["loan_amount"]) == D("50000.00")
    assert D(loan["disbursed_amount"]) == D("50000.00")
    assert D(loan["outstanding_amount"]) == D("50000.00")
    assert loan["status"] == "disbursed"

    rows = conn.execute(
        "SELECT principal_amount, interest_amount, total_amount, status "
        "FROM loan_repayment_schedule WHERE loan_id = ?",
        (res["loan_id"],)).fetchall()
    assert len(rows) == 12
    assert sum((D(r["principal_amount"]) for r in rows), D("0")) == D("50000.00")
    assert sum((D(r["total_amount"]) for r in rows), D("0")) == \
        D("50000.00") + D(loan["total_interest"])
    assert all(r["status"] == "pending" for r in rows)


def test_disbursement_posts_its_gl(conn, env, mod):
    """F21-FINDING-4, repaired: DR loan receivable / CR bank, partied.

    Was xfail(strict) while `handle_disburse_loan` swallowed the posting.
    """
    app_id = _approved_application(conn, env, mod, amount="50000")
    res = _disburse(conn, env, mod, app_id)
    assert is_ok(res), res

    gl = _gl(conn, res["loan_id"])
    assert len(gl) == 2, "DR loan receivable / CR bank"
    assert gl[0]["account_id"] == env["loan_account_id"]
    assert D(gl[0]["debit"]) == D("50000.00")
    assert D(gl[0]["credit"]) == D("0")
    # The receivable leg is attributable (GL validation step 5).
    assert gl[0]["party_type"] == "customer"
    assert gl[0]["party_id"] == env["customer_id"]
    assert gl[1]["account_id"] == env["disbursement_account_id"]
    assert D(gl[1]["credit"]) == D("50000.00")
    assert D(gl[1]["debit"]) == D("0")


def test_the_disbursement_is_the_only_thing_in_the_ledger_and_it_balances(
        conn, env, mod):
    """The repaired reading of the old observed-defect pin.

    That pin asserted `SELECT COUNT(*) FROM gl_entry == 0` — 50,000.00 left the
    bank in the documents and nothing at all reached the books. Same scenario,
    read the other way round now.
    """
    app_id = _approved_application(conn, env, mod, amount="50000")
    res = _disburse(conn, env, mod, app_id)
    assert is_ok(res), res
    assert D(_loan(conn, res["loan_id"])["outstanding_amount"]) == D("50000.00")

    assert _count(conn, "gl_entry") == 2
    assert _sum(conn, "SELECT debit FROM gl_entry WHERE is_cancelled = 0") == \
        D("50000.00")
    assert _sum(conn, "SELECT credit FROM gl_entry WHERE is_cancelled = 0") == \
        D("50000.00")


def test_a_failing_gl_post_rolls_the_whole_disbursement_back(
        conn, env, mod, monkeypatch):
    """Single-transaction rule: GL failure undoes the loan AND its schedule.

    A successful disbursement runs first so the counts the rollback is measured
    against are known to be reachable — otherwise "no rows" would pass for the
    wrong reason.
    """
    good = _disburse(conn, env, mod,
                     _approved_application(conn, env, mod, amount="1000",
                                           periods=6))
    assert is_ok(good), good
    assert _count(conn, "loan") == 1
    assert _count(conn, "loan_repayment_schedule") == 6
    assert _count(conn, "gl_entry") == 2

    doomed_app = _approved_application(conn, env, mod, amount="50000",
                                       periods=12)
    _plant_gl_failure(monkeypatch, "loans", "planted GL failure")
    res = _disburse(conn, env, mod, doomed_app)

    assert is_error(res), res
    assert "planted GL failure" in _msg(res), (
        "the real reason must reach the caller, not a generic message")

    # Nothing from the doomed disbursement survived.
    assert _count(conn, "loan") == 1
    assert _count(conn, "loan_repayment_schedule") == 6
    assert _count(conn, "gl_entry") == 2
    assert conn.execute(
        "SELECT COUNT(*) FROM loan WHERE loan_application_id = ?",
        (doomed_app,)).fetchone()[0] == 0

    # ...and the application is still disbursable once the fault is cleared.
    assert conn.execute(
        "SELECT status FROM loan_application WHERE id = ?",
        (doomed_app,)).fetchone()["status"] == "approved"


def test_disbursing_an_unapproved_application_is_refused(conn, env, mod):
    created = call_action(mod.loan_add_loan_application, conn, ns(
        company_id=env["company_id"], applicant_type="customer",
        applicant_id=env["customer_id"], loan_type="term_loan",
        requested_amount="1000", interest_rate="5", repayment_method=None,
        repayment_periods=6, purpose=None, collateral_description=None,
        collateral_value=None, applicant_name=None))
    assert is_ok(created), created

    bad = _disburse(conn, env, mod, created["id"])
    assert is_error(bad)
    assert "must be 'approved'" in _msg(bad)
    assert conn.execute("SELECT COUNT(*) FROM loan").fetchone()[0] == 0


def test_disbursing_the_same_application_twice_is_refused(conn, env, mod):
    app_id = _approved_application(conn, env, mod, amount="1000", periods=6)
    assert is_ok(_disburse(conn, env, mod, app_id))

    again = _disburse(conn, env, mod, app_id)
    assert is_error(again)
    assert "already disbursed" in _msg(again)
    assert conn.execute("SELECT COUNT(*) FROM loan").fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM loan_repayment_schedule").fetchone()[0] == 6


# ── loan-record-repayment ────────────────────────────────────────────────────
#
# Found by the M62 sweep, same defect class as the disbursement: the block
# called `post_gl_entry`, a symbol that has never existed in
# erpclaw_lib.gl_posting, inside `except Exception: pass`. Every repayment ever
# recorded posted nothing.

def _repay(conn, mod, loan_id, principal, interest="0", penalty=None):
    return call_action(mod.loan_record_repayment, conn, ns(
        loan_id=loan_id, principal_amount=principal, interest_amount=interest,
        penalty_amount=penalty, payment_method="bank_transfer",
        repayment_date="2026-07-01", reference_number="CHK-001",
        remarks="test repayment"))


def test_repayment_posts_principal_and_interest_to_the_ledger(conn, env, mod):
    app_id = _approved_application(conn, env, mod, amount="12000", rate="0",
                                   periods=12)
    loan_id = _disburse(conn, env, mod, app_id)["loan_id"]

    res = _repay(conn, mod, loan_id, principal="1000", interest="50")
    assert is_ok(res), res

    gl = _gl_by_account(conn, res["id"])
    assert len(gl) == 3, "DR bank / CR loan receivable / CR interest income"

    bank = gl[env["disbursement_account_id"]]
    assert D(bank["debit"]) == D("1050.00")
    assert D(bank["credit"]) == D("0")

    receivable = gl[env["loan_account_id"]]
    assert D(receivable["credit"]) == D("1000.00")
    assert D(receivable["debit"]) == D("0")
    assert receivable["party_type"] == "customer"
    assert receivable["party_id"] == env["customer_id"]

    income = gl[env["interest_income_account_id"]]
    assert D(income["credit"]) == D("50.00")
    assert D(income["debit"]) == D("0")


def test_a_penalty_only_repayment_posts_nothing_and_does_not_fail(
        conn, env, mod):
    """There is no penalty-income account on `loan` to post a penalty to.

    Every leg is zero, GL validation step 11 filters them all out, and the
    posting is a no-op rather than an error. Pins the ledger consequence of that
    gap so it stays visible until the account question is answered.
    """
    app_id = _approved_application(conn, env, mod, amount="12000", rate="0",
                                   periods=12)
    loan_id = _disburse(conn, env, mod, app_id)["loan_id"]
    gl_before = _count(conn, "gl_entry")

    res = _repay(conn, mod, loan_id, principal="0", penalty="25")
    assert is_ok(res), res
    assert D(res["total_amount"]) == D("25")
    assert _gl(conn, res["id"]) == []
    assert _count(conn, "gl_entry") == gl_before


def test_a_failing_gl_post_rolls_the_whole_repayment_back(
        conn, env, mod, monkeypatch):
    app_id = _approved_application(conn, env, mod, amount="12000", rate="0",
                                   periods=12)
    loan_id = _disburse(conn, env, mod, app_id)["loan_id"]

    # A good repayment first, so the state the rollback is measured against is
    # known to be reachable rather than merely absent.
    assert is_ok(_repay(conn, mod, loan_id, principal="1000"))
    assert _count(conn, "loan_repayment") == 1
    assert D(_loan(conn, loan_id)["outstanding_amount"]) == D("11000.00")
    gl_before = _count(conn, "gl_entry")

    _plant_gl_failure(monkeypatch, "repayments", "planted repayment GL failure")
    res = _repay(conn, mod, loan_id, principal="2000", interest="75")

    assert is_error(res), res
    assert "planted repayment GL failure" in _msg(res)

    # The document, the loan balance and the schedule are all back where the
    # successful repayment left them.
    assert _count(conn, "loan_repayment") == 1
    assert _count(conn, "gl_entry") == gl_before
    loan = _loan(conn, loan_id)
    assert D(loan["outstanding_amount"]) == D("11000.00")
    assert loan["status"] == "partially_repaid"
    assert _sum(conn, "SELECT paid_amount FROM loan_repayment_schedule "
                      "WHERE loan_id = ?", (loan_id,)) == D("1000.00")


# ── loan-write-off-loan ──────────────────────────────────────────────────────

def test_write_off_posts_a_balanced_partied_gl_pair(conn, env, mod):
    app_id = _approved_application(conn, env, mod, amount="50000")
    loan_id = _disburse(conn, env, mod, app_id)["loan_id"]

    res = call_action(mod.loan_write_off_loan, conn, ns(
        loan_id=loan_id, bad_debt_account_id=env["bad_debt_account_id"],
        reason="Debtor insolvent", write_off_date="2026-09-30"))
    assert is_ok(res), res
    assert D(res["write_off_amount"]) == D("50000.00")

    loan = _loan(conn, loan_id)
    assert loan["status"] == "written_off"
    assert D(loan["outstanding_amount"]) == D("0")

    wo = conn.execute(
        "SELECT write_off_amount, outstanding_at_write_off, reason, status "
        "FROM loan_write_off WHERE id = ?", (res["id"],)).fetchone()
    assert D(wo["write_off_amount"]) == D("50000.00")
    assert D(wo["outstanding_at_write_off"]) == D("50000.00")
    assert wo["reason"] == "Debtor insolvent"
    assert wo["status"] == "submitted"

    # DR bad debt expense / CR loan receivable, and the receivable leg carries
    # the party (GL validation step 5) so the write-off is attributable.
    gl = _gl(conn, res["id"])
    assert len(gl) == 2
    assert gl[0]["account_id"] == env["bad_debt_account_id"]
    assert D(gl[0]["debit"]) == D("50000.00")
    assert gl[1]["account_id"] == env["loan_account_id"]
    assert D(gl[1]["credit"]) == D("50000.00")
    assert gl[1]["party_type"] == "customer"
    assert gl[1]["party_id"] == env["customer_id"]
    assert "Debtor insolvent" in gl[1]["remarks"]


def test_writing_off_twice_is_refused_and_posts_no_second_pair(conn, env, mod):
    app_id = _approved_application(conn, env, mod, amount="2500")
    loan_id = _disburse(conn, env, mod, app_id)["loan_id"]
    first = call_action(mod.loan_write_off_loan, conn, ns(
        loan_id=loan_id, bad_debt_account_id=env["bad_debt_account_id"],
        reason="one", write_off_date="2026-09-30"))
    assert is_ok(first), first
    gl_before = conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0]

    again = call_action(mod.loan_write_off_loan, conn, ns(
        loan_id=loan_id, bad_debt_account_id=env["bad_debt_account_id"],
        reason="two", write_off_date="2026-10-31"))
    assert is_error(again)
    assert "written_off" in _msg(again)
    assert conn.execute("SELECT COUNT(*) FROM loan_write_off").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0] == gl_before


def test_write_off_without_a_bad_debt_account_is_refused(conn, env, mod):
    app_id = _approved_application(conn, env, mod, amount="900")
    loan_id = _disburse(conn, env, mod, app_id)["loan_id"]

    bad = call_action(mod.loan_write_off_loan, conn, ns(
        loan_id=loan_id, bad_debt_account_id=None, reason=None,
        write_off_date=None))
    assert is_error(bad)
    assert "--bad-debt-account-id is required" in _msg(bad)
    assert D(_loan(conn, loan_id)["outstanding_amount"]) == D("900.00")
    assert conn.execute("SELECT COUNT(*) FROM loan_write_off").fetchone()[0] == 0
