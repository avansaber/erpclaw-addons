"""Part A: behaviour of loan-statement and loan-overdue-loans, driven through
the module's own actions and read back from the database.

One loan is driven end to end with fixed dates: 3000.00 at 12% over three
monthly installments, disbursed 2025-01-15. Installment 1 (990.07 principal +
30.00 interest) is paid in full on its due date, installment 2 is paid in part
(400.10 principal + 10.20 interest), and installment 3 is missed.

loan-statement is pinned at each step: the opening position (disbursed amount,
outstanding, the full schedule), every repayment line with its principal and
interest split, the installment each repayment settled, and the closing
balance. Its ledger is pinned leg by leg: the disbursement and both
repayments, by account, balanced. A written-off loan shows its write-off line,
and a missing or unknown loan is refused.

loan-overdue-loans reads the schedule of disbursed and partially repaid loans.
The handler takes no as-of date: it compares each due date with today's date
and computes no days-overdue figure. Every due date used here is in 2025, so
every unpaid installment is overdue on any day these tests run; only facts that
do not depend on the calendar are pinned (which installments, their exact
outstanding amounts, the exact total, company scoping). The handler has no
guard to refuse; an unknown company is pinned as the empty answer it gets.

An installment's outstanding is its principal plus its interest, so a
repayment's principal and interest are both applied to it, and the paid amount
is added as an exact decimal string.
"""
from decimal import Decimal
import json as _json

from loans_helpers import build_env, call_action, is_error, is_ok, ns
from erpclaw_lib.query import Q as _Q, Table as _T

D = Decimal

DISBURSE_DATE = "2025-01-15"
R1_DATE = "2025-02-15"
R2_DATE = "2025-03-20"


# ── helpers ────────────────────────────────────────────────────────────────

def _msg(result):
    return result.get("message", "") + result.get("error", "")


def _approved_application(conn, env, mod, amount, rate, periods, method=None):
    created = call_action(mod.ACTIONS["loan-add-loan-application"], conn, ns(
        company_id=env["company_id"], applicant_type="customer",
        applicant_id=env["customer_id"], loan_type="term_loan",
        requested_amount=amount, interest_rate=rate, repayment_method=method,
        repayment_periods=periods, purpose=None, collateral_description=None,
        collateral_value=None, applicant_name=None))
    assert is_ok(created), created
    approved = call_action(mod.ACTIONS["loan-approve-loan"], conn,
                           ns(id=created["id"], approved_amount=None))
    assert is_ok(approved), approved
    return created["id"]


def _disburse(conn, env, mod, app_id, disbursement_date):
    r = call_action(mod.ACTIONS["loan-disburse-loan"], conn, ns(
        loan_application_id=app_id,
        loan_account_id=env["loan_account_id"],
        interest_income_account_id=env["interest_income_account_id"],
        disbursement_account_id=env["disbursement_account_id"],
        disbursement_date=disbursement_date))
    assert is_ok(r), r
    return r


def _repay(conn, mod, loan_id, principal, interest, repayment_date,
           method="bank_transfer", reference="REF"):
    r = call_action(mod.ACTIONS["loan-record-repayment"], conn, ns(
        loan_id=loan_id, principal_amount=principal, interest_amount=interest,
        penalty_amount=None, payment_method=method,
        repayment_date=repayment_date, reference_number=reference,
        remarks=None))
    assert is_ok(r), r
    return r


def _schedule_payment_dates(conn, loan_id):
    rows = conn.execute(
        "SELECT installment_no, payment_date FROM loan_repayment_schedule "
        "WHERE loan_id = ? ORDER BY installment_no", (loan_id,)).fetchall()
    return [(r["installment_no"], r["payment_date"]) for r in rows]


def _loan_row(conn, loan_id):
    row = conn.execute(
        "SELECT disbursed_amount, total_repaid, outstanding_amount, status "
        "FROM loan WHERE id = ?", (loan_id,)).fetchone()
    return tuple(row)


def _gl(conn, voucher_id):
    rows = conn.execute(
        "SELECT account_id, debit, credit, voucher_type, is_cancelled, "
        "posting_date, party_type, party_id FROM gl_entry WHERE voucher_id = ?",
        (voucher_id,)).fetchall()
    return {tuple(r) for r in rows}


def _count(conn, table):
    return conn.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]


def _schedule_line(no, due, principal, interest, total, paid, outstanding,
                   status):
    return {"installment_no": no, "due_date": due,
            "principal_amount": principal, "interest_amount": interest,
            "total_amount": total, "paid_amount": paid,
            "outstanding": outstanding, "status": status}


def _three_month_loan(conn, env, mod):
    app_id = _approved_application(conn, env, mod, "3000", "12", 3)
    return _disburse(conn, env, mod, app_id, DISBURSE_DATE)


def _drive_scenario(conn, env, mod):
    """Disburse, pay installment 1 in full, pay installment 2 in part."""
    disbursed = _three_month_loan(conn, env, mod)
    loan_id = disbursed["loan_id"]
    r1 = _repay(conn, mod, loan_id, "990.07", "30.00", R1_DATE,
                method="bank_transfer", reference="R1")
    r2 = _repay(conn, mod, loan_id, "400.10", "10.20", R2_DATE,
                method="check", reference="R2")
    return disbursed, r1, r2


# ── loan-statement ─────────────────────────────────────────────────────────

def test_loan_statement_opening_lines_split_and_closing_balance(conn, env, mod):
    disbursed = _three_month_loan(conn, env, mod)
    loan_id = disbursed["loan_id"]

    # Opening position: the whole amount is out, nothing is repaid.
    opening = call_action(mod.ACTIONS["loan-statement"], conn, ns(loan_id=loan_id))
    assert is_ok(opening), opening
    assert opening["loan"] == {
        "id": loan_id, "naming_series": disbursed["naming_series"],
        "applicant_name": "Acme Corp", "loan_type": "term_loan",
        "loan_amount": "3000.00", "disbursed_amount": "3000.00",
        "total_interest": "60.20", "total_repaid": "0.00",
        "outstanding_amount": "3000.00", "interest_rate": "12.00",
        "disbursement_date": "2025-01-15", "maturity_date": "2025-04-15",
        "status": "disbursed"}
    assert opening["schedule"] == [
        _schedule_line(1, "2025-02-15", "990.07", "30.00", "1020.07", "0.00", "1020.07", "pending"),
        _schedule_line(2, "2025-03-15", "999.97", "20.10", "1020.07", "0.00", "1020.07", "pending"),
        _schedule_line(3, "2025-04-15", "1009.96", "10.10", "1020.06", "0.00", "1020.06", "pending"),
    ]
    assert opening["repayments"] == []
    assert opening["write_offs"] == []
    principal_scheduled = sum((D(s["principal_amount"]) for s in opening["schedule"]), D("0"))
    interest_scheduled = sum((D(s["interest_amount"]) for s in opening["schedule"]), D("0"))
    assert str(principal_scheduled) == "3000.00"
    assert str(interest_scheduled) == opening["loan"]["total_interest"]

    # Installment 1 paid in full on its due date: principal and interest both
    # settle it.
    r1 = _repay(conn, mod, loan_id, "990.07", "30.00", R1_DATE,
                method="bank_transfer", reference="R1")
    assert (r1["total_amount"], r1["loan_outstanding"], r1["loan_status"]) == \
        ("1020.07", "2009.93", "partially_repaid")
    after_r1 = call_action(mod.ACTIONS["loan-statement"], conn, ns(loan_id=loan_id))
    assert is_ok(after_r1), after_r1
    assert after_r1["schedule"] == [
        _schedule_line(1, "2025-02-15", "990.07", "30.00", "1020.07", "1020.07", "0.00", "paid"),
        _schedule_line(2, "2025-03-15", "999.97", "20.10", "1020.07", "0.00", "1020.07", "pending"),
        _schedule_line(3, "2025-04-15", "1009.96", "10.10", "1020.06", "0.00", "1020.06", "pending"),
    ]

    # Installment 2 paid in part; installment 3 missed.
    r2 = _repay(conn, mod, loan_id, "400.10", "10.20", R2_DATE,
                method="check", reference="R2")
    assert (r2["total_amount"], r2["loan_outstanding"], r2["loan_status"]) == \
        ("410.30", "1609.83", "partially_repaid")

    closing = call_action(mod.ACTIONS["loan-statement"], conn, ns(loan_id=loan_id))
    assert is_ok(closing), closing
    assert closing["loan"]["disbursed_amount"] == "3000.00"
    assert closing["loan"]["total_repaid"] == "1430.37"
    assert closing["loan"]["outstanding_amount"] == "1609.83"
    assert closing["loan"]["status"] == "partially_repaid"
    assert closing["schedule"] == [
        _schedule_line(1, "2025-02-15", "990.07", "30.00", "1020.07", "1020.07", "0.00", "paid"),
        _schedule_line(2, "2025-03-15", "999.97", "20.10", "1020.07", "410.30", "609.77", "partially_paid"),
        _schedule_line(3, "2025-04-15", "1009.96", "10.10", "1020.06", "0.00", "1020.06", "pending"),
    ]
    assert closing["repayments"] == [
        {"naming_series": r1["naming_series"], "repayment_date": "2025-02-15",
         "principal_amount": "990.07", "interest_amount": "30.00",
         "penalty_amount": "0", "total_amount": "1020.07",
         "payment_method": "bank_transfer", "status": "submitted"},
        {"naming_series": r2["naming_series"], "repayment_date": "2025-03-20",
         "principal_amount": "400.10", "interest_amount": "10.20",
         "penalty_amount": "0", "total_amount": "410.30",
         "payment_method": "check", "status": "submitted"},
    ]
    assert closing["write_offs"] == []

    # Closing balance = opening balance - principal repaid, and the unpaid
    # schedule is that principal plus the interest still due.
    principal_repaid = sum((D(x["principal_amount"]) for x in closing["repayments"]), D("0"))
    interest_repaid = sum((D(x["interest_amount"]) for x in closing["repayments"]), D("0"))
    assert str(principal_repaid) == "1390.17"
    assert str(interest_repaid) == "40.20"
    assert str(D(closing["loan"]["disbursed_amount"]) - principal_repaid) == "1609.83"
    unpaid = sum((D(s["outstanding"]) for s in closing["schedule"]), D("0"))
    assert str(unpaid) == "1629.83"
    assert str(unpaid - D(closing["loan"]["outstanding_amount"])) == \
        str(interest_scheduled - interest_repaid) == "20.00"

    # Read back: the statement is what the tables hold.
    assert _loan_row(conn, loan_id) == ("3000.00", "1430.37", "1609.83", "partially_repaid")
    assert _schedule_payment_dates(conn, loan_id) == [
        (1, "2025-02-15"), (2, "2025-03-20"), (3, None)]


def test_disbursement_and_repayments_post_their_ledger_legs(conn, env, mod):
    disbursed, r1, r2 = _drive_scenario(conn, env, mod)
    loan_acct = env["loan_account_id"]
    bank = env["disbursement_account_id"]
    income = env["interest_income_account_id"]
    cust = env["customer_id"]

    assert _gl(conn, disbursed["loan_id"]) == {
        (loan_acct, "3000.00", "0.00", "journal_entry", 0, "2025-01-15", "customer", cust),
        (bank, "0.00", "3000.00", "journal_entry", 0, "2025-01-15", None, None),
    }
    assert _gl(conn, r1["id"]) == {
        (bank, "1020.07", "0.00", "journal_entry", 0, "2025-02-15", None, None),
        (loan_acct, "0.00", "990.07", "journal_entry", 0, "2025-02-15", "customer", cust),
        (income, "0.00", "30.00", "journal_entry", 0, "2025-02-15", None, None),
    }
    assert _gl(conn, r2["id"]) == {
        (bank, "410.30", "0.00", "journal_entry", 0, "2025-03-20", None, None),
        (loan_acct, "0.00", "400.10", "journal_entry", 0, "2025-03-20", "customer", cust),
        (income, "0.00", "10.20", "journal_entry", 0, "2025-03-20", None, None),
    }

    rows = conn.execute("SELECT account_id, debit, credit FROM gl_entry").fetchall()
    assert len(rows) == 8
    debit = sum((D(r["debit"]) for r in rows), D("0"))
    credit = sum((D(r["credit"]) for r in rows), D("0"))
    assert str(debit) == str(credit) == "4430.37"
    receivable = sum((D(r["debit"]) - D(r["credit"]) for r in rows
                      if r["account_id"] == loan_acct), D("0"))
    assert str(receivable) == "1609.83"
    statement = call_action(mod.ACTIONS["loan-statement"], conn,
                            ns(loan_id=disbursed["loan_id"]))
    assert statement["loan"]["outstanding_amount"] == str(receivable)


def test_loan_statement_shows_the_write_off_line(conn, env, mod):
    app_id = _approved_application(conn, env, mod, "2000", "0", 4)
    loan_id = _disburse(conn, env, mod, app_id, "2025-02-01")["loan_id"]
    _repay(conn, mod, loan_id, "500", "0", "2025-03-01")
    wo = call_action(mod.ACTIONS["loan-write-off-loan"], conn, ns(
        loan_id=loan_id, bad_debt_account_id=env["bad_debt_account_id"],
        reason="Borrower insolvent", write_off_date="2025-06-30"))
    assert is_ok(wo), wo

    r = call_action(mod.ACTIONS["loan-statement"], conn, ns(loan_id=loan_id))
    assert is_ok(r), r
    assert r["write_offs"] == [{
        "write_off_date": "2025-06-30", "write_off_amount": "1500.00",
        "reason": "Borrower insolvent", "status": "submitted"}]
    assert (r["loan"]["total_repaid"], r["loan"]["outstanding_amount"],
            r["loan"]["status"]) == ("500.00", "0", "written_off")
    assert [(s["installment_no"], s["paid_amount"], s["outstanding"], s["status"])
            for s in r["schedule"]] == [
        (1, "500.00", "0.00", "paid"), (2, "0.00", "500.00", "pending"),
        (3, "0.00", "500.00", "pending"), (4, "0.00", "500.00", "pending")]
    assert _gl(conn, wo["id"]) == {
        (env["bad_debt_account_id"], "1500.00", "0.00", "journal_entry", 0,
         "2025-06-30", None, None),
        (env["loan_account_id"], "0.00", "1500.00", "journal_entry", 0,
         "2025-06-30", "customer", env["customer_id"]),
    }


def test_loan_statement_refusals_write_nothing(conn, env, mod):
    _three_month_loan(conn, env, mod)
    counts = tuple(_count(conn, t) for t in
                   ("loan", "loan_repayment", "loan_repayment_schedule",
                    "gl_entry", "audit_log"))

    r = call_action(mod.ACTIONS["loan-statement"], conn, ns(loan_id=None))
    assert is_error(r)
    assert _msg(r) == "--loan-id is required"

    r = call_action(mod.ACTIONS["loan-statement"], conn, ns(loan_id="no-such-loan"))
    assert is_error(r)
    assert _msg(r) == "Loan no-such-loan not found"

    assert tuple(_count(conn, t) for t in
                 ("loan", "loan_repayment", "loan_repayment_schedule",
                  "gl_entry", "audit_log")) == counts


def test_interest_only_payment_settles_its_bullet_installment(conn, env, mod):
    app_id = _approved_application(conn, env, mod, "6000", "6", 3, method="bullet")
    loan_id = _disburse(conn, env, mod, app_id, "2025-01-31")["loan_id"]
    r1 = _repay(conn, mod, loan_id, "0", "30.00", "2025-02-28")

    r = call_action(mod.ACTIONS["loan-statement"], conn, ns(loan_id=loan_id))
    assert is_ok(r), r
    assert r["schedule"] == [
        _schedule_line(1, "2025-02-28", "0.00", "30.00", "30.00", "30.00", "0.00", "paid"),
        _schedule_line(2, "2025-03-31", "0.00", "30.00", "30.00", "0.00", "30.00", "pending"),
        _schedule_line(3, "2025-04-30", "6000.00", "30.00", "6030.00", "0.00", "6030.00", "pending"),
    ]
    assert (r["loan"]["total_repaid"], r["loan"]["outstanding_amount"],
            r["loan"]["status"]) == ("30.00", "6000.00", "partially_repaid")
    assert _gl(conn, r1["id"]) == {
        (env["disbursement_account_id"], "30.00", "0.00", "journal_entry", 0,
         "2025-02-28", None, None),
        (env["interest_income_account_id"], "0.00", "30.00", "journal_entry", 0,
         "2025-02-28", None, None),
    }

    overdue = call_action(mod.ACTIONS["loan-overdue-loans"], conn,
                          ns(company_id=env["company_id"]))
    assert is_ok(overdue), overdue
    assert [(x["installment_no"], x["outstanding"]) for x in overdue["records"]] == \
        [(2, "30.00"), (3, "6030.00")]
    assert overdue["total_overdue_amount"] == "6060.00"


# ── loan-overdue-loans ─────────────────────────────────────────────────────

def test_overdue_loans_exact_amounts_scoped_to_company(conn, env, mod):
    # Company A: the partly repaid loan with a missed installment ...
    disbursed, _r1, _r2 = _drive_scenario(conn, env, mod)
    loan_a = disbursed["loan_id"]
    # ... and a written-off loan whose unpaid installments are not overdue.
    wo_app = _approved_application(conn, env, mod, "2000", "0", 4)
    wo_loan = _disburse(conn, env, mod, wo_app, "2025-02-01")["loan_id"]
    _repay(conn, mod, wo_loan, "500", "0", "2025-03-01")
    assert is_ok(call_action(mod.ACTIONS["loan-write-off-loan"], conn, ns(
        loan_id=wo_loan, bad_debt_account_id=env["bad_debt_account_id"],
        reason="Borrower insolvent", write_off_date="2025-06-30")))

    # Company B: a loan with nothing repaid.
    env_b = build_env(conn)
    app_b = _approved_application(conn, env_b, mod, "1200", "0", 2)
    disbursed_b = _disburse(conn, env_b, mod, app_b, "2025-05-10")
    loan_b = disbursed_b["loan_id"]

    def line(loan_id, name, no, due, total, outstanding):
        return {"loan_id": loan_id, "loan_name": name,
                "applicant_name": "Acme Corp", "loan_type": "term_loan",
                "installment_no": no, "due_date": due,
                "total_amount": total, "outstanding": outstanding}

    a_lines = [
        line(loan_a, disbursed["naming_series"], 2, "2025-03-15", "1020.07", "609.77"),
        line(loan_a, disbursed["naming_series"], 3, "2025-04-15", "1020.06", "1020.06"),
    ]
    b_lines = [
        line(loan_b, disbursed_b["naming_series"], 1, "2025-06-10", "600.00", "600.00"),
        line(loan_b, disbursed_b["naming_series"], 2, "2025-07-10", "600.00", "600.00"),
    ]

    r = call_action(mod.ACTIONS["loan-overdue-loans"], conn,
                    ns(company_id=env["company_id"]))
    assert is_ok(r), r
    # Every due date above is before the report's own date.
    assert r["as_of"] > "2025-07-10"
    assert r["records"] == a_lines
    assert r["total"] == 2
    assert r["total_overdue_amount"] == "1629.83"

    r = call_action(mod.ACTIONS["loan-overdue-loans"], conn,
                    ns(company_id=env_b["company_id"]))
    assert is_ok(r), r
    assert r["records"] == b_lines
    assert r["total"] == 2
    assert r["total_overdue_amount"] == "1200.00"

    # No company on a two-company install refuses; nothing is written.
    snap_before = _snapshot_tables(conn)
    r = call_action(mod.ACTIONS["loan-overdue-loans"], conn, ns(company_id=None))
    assert r == {
        "status": "error",
        "error": "Multiple companies found. Please specify the company by name.",
        "companies": _companies(conn),
        "suggestion": ("Pass the company name (e.g. --company \"Acme\"), "
                       "or use --company-id with one of the IDs above."),
        "message": "Multiple companies found. Please specify the company by name.",
    }
    assert _snapshot_tables(conn) == snap_before

    # Read back: the overdue figure is the schedule's unpaid amount on open loans.
    open_rows = conn.execute(
        "SELECT s.outstanding FROM loan_repayment_schedule s "
        "JOIN loan l ON l.id = s.loan_id "
        "WHERE s.status IN ('pending', 'partially_paid') "
        "AND l.status IN ('disbursed', 'partially_repaid')").fetchall()
    assert str(sum((D(x["outstanding"]) for x in open_rows), D("0"))) == "2829.83"
    assert conn.execute(
        "SELECT status FROM loan WHERE id = ?", (wo_loan,)).fetchone()["status"] == "written_off"


def _companies(conn):
    c = _T("company")
    rows = conn.execute(
        _Q.from_(c).select(c.id, c.name).orderby(c.name).limit(10).get_sql()
    ).fetchall()
    return [{"id": r["id"], "name": r["name"]} for r in rows]


def _snapshot_tables(conn):
    snap = {}
    for _t in ("company", "loan", "loan_application",
               "loan_repayment_schedule", "loan_repayment",
               "loan_write_off", "gl_entry", "audit_log"):
        t = _T(_t)
        try:
            _rows = conn.execute(_Q.from_(t).select(t.star).get_sql()).fetchall()
        except Exception:
            continue
        snap[_t] = sorted(
            _json.dumps(dict(_r), sort_keys=True, default=str) for _r in _rows)
    return snap


def test_overdue_loans_unknown_company_refuses_and_writes_nothing(conn, env, mod):
    _three_month_loan(conn, env, mod)
    counts = tuple(_count(conn, t) for t in ("loan_repayment_schedule", "gl_entry", "audit_log"))

    r = call_action(mod.ACTIONS["loan-overdue-loans"], conn,
                    ns(company_id="no-such-company"))
    assert r == {
        "status": "error",
        "error": "Company not found: no-such-company",
        "message": "Company not found: no-such-company",
    }

    assert tuple(_count(conn, t) for t in
                 ("loan_repayment_schedule", "gl_entry", "audit_log")) == counts
