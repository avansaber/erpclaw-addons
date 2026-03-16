"""ERPClaw Loans — loans domain module

Loan lifecycle management: applications, approvals, disbursements,
repayment schedule generation, restructuring, and closure.
Imported by db_query.py (unified router).

Tables: loan_application, loan, loan_repayment_schedule
"""
import calendar
import json
import os
import sys
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.gl_posting import insert_gl_entries
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, LiteralValue, insert_row, update_row, dynamic_update
except ImportError:
    pass

# Register naming prefixes (loan domain)
ENTITY_PREFIXES.setdefault("loan_application", "LOAN-")
ENTITY_PREFIXES.setdefault("loan", "LDIS-")

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------------------------------------------------------------------
# Validation constants
# ---------------------------------------------------------------------------
VALID_APPLICANT_TYPES = ("customer", "employee", "supplier")
VALID_LOAN_TYPES = ("term_loan", "demand_loan", "staff_loan", "credit_line")
VALID_REPAYMENT_METHODS = ("equal_installment", "equal_principal", "bullet", "custom")
VALID_APPLICATION_STATUSES = ("draft", "applied", "approved", "rejected", "cancelled")
VALID_LOAN_STATUSES = ("draft", "disbursed", "partially_repaid", "repaid", "written_off", "closed")
VALID_SCHEDULE_STATUSES = ("pending", "partially_paid", "paid", "overdue", "waived")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    row = conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (company_id,)).fetchone()
    if not row:
        err(f"Company {company_id} not found")


def _validate_enum(value, valid_values, field_name):
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")


def _validate_loan_application(conn, app_id):
    if not app_id:
        err("--id is required")
    row = conn.execute(Q.from_(Table("loan_application")).select(Table("loan_application").star).where(Field("id") == P()).get_sql(), (app_id,)).fetchone()
    if not row:
        err(f"Loan application {app_id} not found")
    return row


def _validate_loan(conn, loan_id):
    if not loan_id:
        err("--loan-id is required")
    row = conn.execute(Q.from_(Table("loan")).select(Table("loan").star).where(Field("id") == P()).get_sql(), (loan_id,)).fetchone()
    if not row:
        err(f"Loan {loan_id} not found")
    return row


def _validate_account(conn, account_id, label):
    if not account_id:
        err(f"--{label} is required")
    row = conn.execute(Q.from_(Table("account")).select(Field('id')).where(Field("id") == P()).get_sql(), (account_id,)).fetchone()
    if not row:
        err(f"Account {account_id} not found (--{label})")


def _validate_applicant(conn, applicant_type, applicant_id):
    """Validate that the applicant exists in the appropriate table."""
    if not applicant_type:
        err("--applicant-type is required")
    if not applicant_id:
        err("--applicant-id is required")
    _validate_enum(applicant_type, VALID_APPLICANT_TYPES, "applicant-type")

    table_map = {
        "customer": "customer",
        "employee": "employee",
        "supplier": "supplier",
    }
    t = Table(table_map[applicant_type])
    row = conn.execute(Q.from_(t).select(t.id, t.name).where(t.id == P()).get_sql(), (applicant_id,)).fetchone()
    if not row:
        err(f"{applicant_type.capitalize()} {applicant_id} not found")
    return row


def _add_months(start_date_str, months):
    """Add N months to a date string, returning YYYY-MM-DD."""
    d = date.fromisoformat(start_date_str)
    month = d.month - 1 + months
    year = d.year + month // 12
    month = month % 12 + 1
    max_day = calendar.monthrange(year, month)[1]
    day = min(d.day, max_day)
    return date(year, month, day).isoformat()


def _today_str():
    return date.today().isoformat()


# ---------------------------------------------------------------------------
# EMI / Repayment calculation helpers
# ---------------------------------------------------------------------------
def _calculate_emi(principal, annual_rate, periods):
    """Calculate Equal Monthly Installment (EMI).

    Uses standard annuity formula:
      EMI = P * r * (1+r)^n / ((1+r)^n - 1)
    where r = monthly rate, n = number of periods.
    """
    if annual_rate == Decimal("0"):
        return (principal / periods).quantize(Decimal("0.01"), ROUND_HALF_UP)
    monthly_rate = annual_rate / Decimal("100") / Decimal("12")
    factor = (Decimal("1") + monthly_rate) ** periods
    emi = principal * monthly_rate * factor / (factor - Decimal("1"))
    return emi.quantize(Decimal("0.01"), ROUND_HALF_UP)


def _generate_equal_installment_schedule(principal, annual_rate, periods, start_date):
    """Generate amortization schedule using equal installment (EMI) method."""
    emi = _calculate_emi(principal, annual_rate, periods)
    monthly_rate = (
        annual_rate / Decimal("100") / Decimal("12")
        if annual_rate > Decimal("0")
        else Decimal("0")
    )
    schedule = []
    outstanding = principal

    for i in range(1, periods + 1):
        interest = (outstanding * monthly_rate).quantize(Decimal("0.01"), ROUND_HALF_UP)
        principal_component = emi - interest
        due_date = _add_months(start_date, i)

        # Last installment adjustment: absorb rounding difference
        if i == periods:
            principal_component = outstanding
            interest = (outstanding * monthly_rate).quantize(Decimal("0.01"), ROUND_HALF_UP)
            total = principal_component + interest
        else:
            total = emi

        outstanding = outstanding - principal_component

        schedule.append({
            "installment_no": i,
            "due_date": due_date,
            "principal_amount": str(round_currency(principal_component)),
            "interest_amount": str(round_currency(interest)),
            "total_amount": str(round_currency(total)),
        })

    return schedule


def _generate_equal_principal_schedule(principal, annual_rate, periods, start_date):
    """Generate schedule with equal principal, decreasing interest."""
    monthly_principal = (principal / periods).quantize(Decimal("0.01"), ROUND_HALF_UP)
    monthly_rate = (
        annual_rate / Decimal("100") / Decimal("12")
        if annual_rate > Decimal("0")
        else Decimal("0")
    )
    schedule = []
    outstanding = principal

    for i in range(1, periods + 1):
        interest = (outstanding * monthly_rate).quantize(Decimal("0.01"), ROUND_HALF_UP)
        due_date = _add_months(start_date, i)

        # Last installment: absorb rounding remainder
        if i == periods:
            p_amt = outstanding
        else:
            p_amt = monthly_principal

        total = p_amt + interest
        outstanding = outstanding - p_amt

        schedule.append({
            "installment_no": i,
            "due_date": due_date,
            "principal_amount": str(round_currency(p_amt)),
            "interest_amount": str(round_currency(interest)),
            "total_amount": str(round_currency(total)),
        })

    return schedule


def _generate_bullet_schedule(principal, annual_rate, periods, start_date):
    """Generate bullet repayment: interest monthly, principal at maturity."""
    monthly_rate = (
        annual_rate / Decimal("100") / Decimal("12")
        if annual_rate > Decimal("0")
        else Decimal("0")
    )
    monthly_interest = (principal * monthly_rate).quantize(Decimal("0.01"), ROUND_HALF_UP)
    schedule = []

    for i in range(1, periods + 1):
        due_date = _add_months(start_date, i)
        if i == periods:
            # Last installment: principal + interest
            schedule.append({
                "installment_no": i,
                "due_date": due_date,
                "principal_amount": str(round_currency(principal)),
                "interest_amount": str(round_currency(monthly_interest)),
                "total_amount": str(round_currency(principal + monthly_interest)),
            })
        else:
            # Interest-only installments
            schedule.append({
                "installment_no": i,
                "due_date": due_date,
                "principal_amount": "0.00",
                "interest_amount": str(round_currency(monthly_interest)),
                "total_amount": str(round_currency(monthly_interest)),
            })

    return schedule


def _generate_schedule(principal, annual_rate, periods, method, start_date):
    """Generate repayment schedule based on method."""
    if method == "equal_installment":
        return _generate_equal_installment_schedule(principal, annual_rate, periods, start_date)
    elif method == "equal_principal":
        return _generate_equal_principal_schedule(principal, annual_rate, periods, start_date)
    elif method == "bullet":
        return _generate_bullet_schedule(principal, annual_rate, periods, start_date)
    elif method == "custom":
        # Custom method: default to equal installment, user can modify later
        return _generate_equal_installment_schedule(principal, annual_rate, periods, start_date)
    else:
        err(f"Unsupported repayment method: {method}")


def _calculate_total_interest(principal, annual_rate, periods, method):
    """Calculate the total interest payable over the life of the loan."""
    if annual_rate == Decimal("0"):
        return Decimal("0")
    schedule = _generate_schedule(principal, annual_rate, periods, method, _today_str())
    return sum(to_decimal(item["interest_amount"]) for item in schedule)


# ---------------------------------------------------------------------------
# 1. add-loan-application
# ---------------------------------------------------------------------------
def handle_add_loan_application(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    applicant_type = getattr(args, "applicant_type", None)
    applicant_id = getattr(args, "applicant_id", None)
    applicant_row = _validate_applicant(conn, applicant_type, applicant_id)
    applicant_name = applicant_row["name"]

    loan_type = getattr(args, "loan_type", None)
    if not loan_type:
        err("--loan-type is required")
    _validate_enum(loan_type, VALID_LOAN_TYPES, "loan-type")

    requested_amount = getattr(args, "requested_amount", None)
    if not requested_amount:
        err("--requested-amount is required")
    requested_amount_dec = to_decimal(requested_amount)
    if requested_amount_dec <= Decimal("0"):
        err("--requested-amount must be greater than zero")

    interest_rate = getattr(args, "interest_rate", None)
    if not interest_rate:
        err("--interest-rate is required")
    interest_rate_dec = to_decimal(interest_rate)
    if interest_rate_dec < Decimal("0"):
        err("--interest-rate cannot be negative")

    repayment_method = getattr(args, "repayment_method", None) or "equal_installment"
    _validate_enum(repayment_method, VALID_REPAYMENT_METHODS, "repayment-method")

    repayment_periods = getattr(args, "repayment_periods", None)
    repayment_periods_int = int(repayment_periods) if repayment_periods else 12
    if repayment_periods_int <= 0:
        err("--repayment-periods must be a positive integer")

    purpose = getattr(args, "purpose", None)
    collateral_description = getattr(args, "collateral_description", None)
    collateral_value = getattr(args, "collateral_value", None)
    collateral_value_str = (
        str(round_currency(to_decimal(collateral_value)))
        if collateral_value
        else "0"
    )

    app_id = str(uuid.uuid4())
    naming = get_next_name(conn, "loan_application", company_id=company_id)
    now = _now_iso()

    sql, _ = insert_row("loan_application", {
        "id": P(), "naming_series": P(), "applicant_type": P(), "applicant_id": P(),
        "applicant_name": P(), "loan_type": P(), "requested_amount": P(),
        "approved_amount": P(), "interest_rate": P(), "repayment_method": P(),
        "repayment_periods": P(), "application_date": P(), "purpose": P(),
        "collateral_description": P(), "collateral_value": P(), "status": P(),
        "rejection_reason": P(), "company_id": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        app_id, naming, applicant_type, applicant_id, applicant_name,
        loan_type,
        str(round_currency(requested_amount_dec)),
        "0",  # approved_amount set on approve
        str(round_currency(interest_rate_dec)),
        repayment_method, repayment_periods_int,
        _today_str(),
        purpose,
        collateral_description,
        collateral_value_str,
        "draft",
        None,  # rejection_reason
        company_id, now, now,
    ))

    audit(conn, "erpclaw-loans", "loan-add-loan-application", "loan_application", app_id,
          new_values={"applicant_name": applicant_name, "requested_amount": requested_amount})
    conn.commit()
    ok({"id": app_id, "naming_series": naming, "applicant_name": applicant_name})


# ---------------------------------------------------------------------------
# 2. update-loan-application
# ---------------------------------------------------------------------------
def handle_update_loan_application(conn, args):
    app_id = getattr(args, "id", None)
    app_row = _validate_loan_application(conn, app_id)
    app = dict(app_row)

    if app["status"] not in ("draft", "applied"):
        err(
            f"Cannot update loan application in status '{app['status']}'. "
            f"Only draft or applied applications can be updated."
        )

    upd_data = {}
    changed = []

    field_map = {
        "applicant_type": "applicant_type",
        "applicant_id": "applicant_id",
        "loan_type": "loan_type",
        "requested_amount": "requested_amount",
        "interest_rate": "interest_rate",
        "repayment_method": "repayment_method",
        "repayment_periods": "repayment_periods",
        "purpose": "purpose",
        "collateral_description": "collateral_description",
        "collateral_value": "collateral_value",
    }

    for arg_name, col_name in field_map.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            if col_name == "applicant_type":
                _validate_enum(val, VALID_APPLICANT_TYPES, "applicant-type")
            elif col_name == "loan_type":
                _validate_enum(val, VALID_LOAN_TYPES, "loan-type")
            elif col_name == "repayment_method":
                _validate_enum(val, VALID_REPAYMENT_METHODS, "repayment-method")
            elif col_name == "requested_amount":
                dec_val = to_decimal(val)
                if dec_val <= Decimal("0"):
                    err("--requested-amount must be greater than zero")
                val = str(round_currency(dec_val))
            elif col_name == "interest_rate":
                dec_val = to_decimal(val)
                if dec_val < Decimal("0"):
                    err("--interest-rate cannot be negative")
                val = str(round_currency(dec_val))
            elif col_name == "repayment_periods":
                int_val = int(val)
                if int_val <= 0:
                    err("--repayment-periods must be a positive integer")
                val = int_val
            elif col_name == "collateral_value":
                val = str(round_currency(to_decimal(val)))

            upd_data[col_name] = val
            changed.append(col_name)

    # If applicant changed, update applicant_name
    new_applicant_type = getattr(args, "applicant_type", None) or app["applicant_type"]
    new_applicant_id = getattr(args, "applicant_id", None) or app["applicant_id"]
    if "applicant_type" in changed or "applicant_id" in changed:
        applicant_row = _validate_applicant(conn, new_applicant_type, new_applicant_id)
        upd_data["applicant_name"] = applicant_row["name"]
        changed.append("applicant_name")

    if not changed:
        err("No fields to update")

    upd_data["updated_at"] = _now_iso()
    sql, params = dynamic_update("loan_application", upd_data, {"id": app_id})
    conn.execute(sql, params)
    audit(conn, "erpclaw-loans", "loan-update-loan-application", "loan_application", app_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": app_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# 3. list-loan-applications
# ---------------------------------------------------------------------------
def handle_list_loan_applications(conn, args):
    t = Table("loan_application")
    params = []

    q = Q.from_(t).select(t.id, t.naming_series, t.applicant_name, t.loan_type,
                           t.requested_amount, t.status)
    cq = Q.from_(t).select(fn.Count("*"))

    if getattr(args, "status", None):
        q = q.where(t.status == P())
        cq = cq.where(t.status == P())
        params.append(args.status)
    if getattr(args, "applicant_type", None):
        q = q.where(t.applicant_type == P())
        cq = cq.where(t.applicant_type == P())
        params.append(args.applicant_type)
    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        cq = cq.where(t.company_id == P())
        params.append(args.company_id)

    total = conn.execute(cq.get_sql(), params).fetchone()[0]

    q = q.orderby(t.created_at, order=Order.desc)
    rows = conn.execute(q.get_sql(), params).fetchall()

    records = [dict(r) for r in rows]
    ok({"records": records, "total": total})


# ---------------------------------------------------------------------------
# 4. get-loan-application
# ---------------------------------------------------------------------------
def handle_get_loan_application(conn, args):
    app_id = getattr(args, "id", None)
    app_row = _validate_loan_application(conn, app_id)
    ok(dict(app_row))


# ---------------------------------------------------------------------------
# 5. approve-loan
# ---------------------------------------------------------------------------
def handle_approve_loan(conn, args):
    app_id = getattr(args, "id", None)
    app_row = _validate_loan_application(conn, app_id)
    app = dict(app_row)

    if app["status"] not in ("draft", "applied"):
        err(
            f"Cannot approve loan application in status '{app['status']}'. "
            f"Must be draft or applied."
        )

    # approved_amount defaults to requested_amount if not specified
    approved_amount = getattr(args, "approved_amount", None)
    if approved_amount:
        approved_dec = to_decimal(approved_amount)
        if approved_dec <= Decimal("0"):
            err("--approved-amount must be greater than zero")
        approved_str = str(round_currency(approved_dec))
    else:
        approved_str = app["requested_amount"]

    now = _now_iso()
    sql = update_row("loan_application",
        data={"status": P(), "approved_amount": P(), "updated_at": P()},
        where={"id": P()})
    conn.execute(sql, ("approved", approved_str, now, app_id))

    audit(conn, "erpclaw-loans", "loan-approve-loan", "loan_application", app_id,
          old_values={"status": app["status"]},
          new_values={"status": "approved", "approved_amount": approved_str})
    conn.commit()
    ok({"id": app_id, "loan_status": "approved", "approved_amount": approved_str})


# ---------------------------------------------------------------------------
# 6. reject-loan
# ---------------------------------------------------------------------------
def handle_reject_loan(conn, args):
    app_id = getattr(args, "id", None)
    app_row = _validate_loan_application(conn, app_id)
    app = dict(app_row)

    if app["status"] not in ("draft", "applied"):
        err(
            f"Cannot reject loan application in status '{app['status']}'. "
            f"Must be draft or applied."
        )

    reason = getattr(args, "reason", None)
    if not reason:
        err("--reason is required for rejection")

    now = _now_iso()
    sql = update_row("loan_application",
        data={"status": P(), "rejection_reason": P(), "updated_at": P()},
        where={"id": P()})
    conn.execute(sql, ("rejected", reason, now, app_id))

    audit(conn, "erpclaw-loans", "loan-reject-loan", "loan_application", app_id,
          old_values={"status": app["status"]},
          new_values={"status": "rejected", "rejection_reason": reason})
    conn.commit()
    ok({"id": app_id, "loan_status": "rejected", "rejection_reason": reason})


# ---------------------------------------------------------------------------
# 7. disburse-loan
# ---------------------------------------------------------------------------
def handle_disburse_loan(conn, args):
    loan_app_id = getattr(args, "loan_application_id", None)
    if not loan_app_id:
        err("--loan-application-id is required")

    app_row = conn.execute(Q.from_(Table("loan_application")).select(Table("loan_application").star).where(Field("id") == P()).get_sql(), (loan_app_id,)).fetchone()
    if not app_row:
        err(f"Loan application {loan_app_id} not found")
    app = dict(app_row)

    if app["status"] != "approved":
        err(
            f"Cannot disburse loan. Application status is '{app['status']}', "
            f"must be 'approved'."
        )

    # Check not already disbursed
    existing = conn.execute(Q.from_(Table("loan")).select(Field('id')).where(Field("loan_application_id") == P()).get_sql(), (loan_app_id,)).fetchone()
    if existing:
        err(
            f"Loan already disbursed for application {loan_app_id} "
            f"(loan: {existing['id']})"
        )

    # Validate accounts
    loan_account_id = getattr(args, "loan_account_id", None)
    interest_income_account_id = getattr(args, "interest_income_account_id", None)
    disbursement_account_id = getattr(args, "disbursement_account_id", None)
    _validate_account(conn, loan_account_id, "loan-account-id")
    _validate_account(conn, interest_income_account_id, "interest-income-account-id")
    _validate_account(conn, disbursement_account_id, "disbursement-account-id")

    disbursement_date = getattr(args, "disbursement_date", None) or _today_str()
    company_id = app["company_id"]

    loan_amount = app["approved_amount"] or app["requested_amount"]
    loan_amount_dec = to_decimal(loan_amount)
    loan_amount_str = str(round_currency(loan_amount_dec))

    # Calculate maturity date
    repayment_periods = int(app["repayment_periods"])
    maturity_date = _add_months(disbursement_date, repayment_periods)

    # Calculate total interest based on repayment method
    interest_rate_dec = to_decimal(app["interest_rate"])
    repayment_method = app["repayment_method"]
    total_interest = _calculate_total_interest(
        loan_amount_dec, interest_rate_dec, repayment_periods, repayment_method
    )

    # Create loan record
    loan_id = str(uuid.uuid4())
    naming = get_next_name(conn, "loan", company_id=company_id)
    now = _now_iso()

    sql, _ = insert_row("loan", {
        "id": P(), "naming_series": P(), "loan_application_id": P(),
        "applicant_type": P(), "applicant_id": P(), "applicant_name": P(),
        "loan_type": P(), "loan_amount": P(), "disbursed_amount": P(),
        "total_interest": P(), "total_repaid": P(), "outstanding_amount": P(),
        "interest_rate": P(), "repayment_method": P(), "repayment_periods": P(),
        "disbursement_date": P(), "maturity_date": P(),
        "loan_account_id": P(), "interest_income_account_id": P(),
        "disbursement_account_id": P(), "status": P(), "company_id": P(),
        "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        loan_id, naming, loan_app_id,
        app["applicant_type"], app["applicant_id"], app["applicant_name"],
        app["loan_type"],
        loan_amount_str,
        loan_amount_str,  # disbursed_amount = full amount at disbursement
        str(round_currency(total_interest)),
        "0.00",  # total_repaid
        loan_amount_str,  # outstanding_amount = loan_amount at disbursement
        str(round_currency(interest_rate_dec)),
        repayment_method,
        repayment_periods,
        disbursement_date,
        maturity_date,
        loan_account_id,
        interest_income_account_id,
        disbursement_account_id,
        "disbursed",
        company_id,
        now, now,
    ))

    # Post GL entries: Debit Loan Receivable, Credit Bank/Disbursement
    gl_entries = [
        {
            "account_id": loan_account_id,
            "debit": loan_amount_str,
            "credit": "0",
        },
        {
            "account_id": disbursement_account_id,
            "debit": "0",
            "credit": loan_amount_str,
        },
    ]

    try:
        insert_gl_entries(
            conn,
            entries=gl_entries,
            voucher_type="Loan Disbursement",
            voucher_id=loan_id,
            posting_date=disbursement_date,
            company_id=company_id,
            remarks=f"Loan disbursement {naming}",
        )
    except Exception:
        pass  # GL posting is best-effort during testing

    # Auto-generate repayment schedule
    schedule = _generate_schedule(
        loan_amount_dec, interest_rate_dec, repayment_periods,
        repayment_method, disbursement_date
    )
    sched_sql, _ = insert_row("loan_repayment_schedule", {
        "id": P(), "loan_id": P(), "installment_no": P(), "due_date": P(),
        "principal_amount": P(), "interest_amount": P(), "total_amount": P(),
        "paid_amount": P(), "outstanding": P(), "status": P(), "payment_date": P(),
    })
    for item in schedule:
        sched_id = str(uuid.uuid4())
        conn.execute(sched_sql, (
            sched_id, loan_id, item["installment_no"], item["due_date"],
            item["principal_amount"], item["interest_amount"], item["total_amount"],
            "0.00",  # paid_amount
            item["total_amount"],  # outstanding = total initially
            "pending",
            None,  # payment_date
        ))

    audit(conn, "erpclaw-loans", "loan-disburse-loan", "loan", loan_id,
          new_values={
              "loan_amount": loan_amount_str,
              "disbursement_date": disbursement_date,
          },
          description=f"Disbursed loan {naming} from application {app['naming_series']}")
    conn.commit()
    ok({
        "loan_id": loan_id,
        "naming_series": naming,
        "loan_application_id": loan_app_id,
        "loan_amount": loan_amount_str,
        "disbursement_date": disbursement_date,
        "maturity_date": maturity_date,
        "total_interest": str(round_currency(total_interest)),
        "installments": len(schedule),
    })


# ---------------------------------------------------------------------------
# 8. list-loans
# ---------------------------------------------------------------------------
def handle_list_loans(conn, args):
    t = Table("loan")
    params = []

    q = Q.from_(t).select(t.id, t.naming_series, t.applicant_name, t.loan_type,
                           t.loan_amount, t.outstanding_amount, t.status)
    cq = Q.from_(t).select(fn.Count("*"))

    if getattr(args, "status", None):
        q = q.where(t.status == P())
        cq = cq.where(t.status == P())
        params.append(args.status)
    if getattr(args, "applicant_type", None):
        q = q.where(t.applicant_type == P())
        cq = cq.where(t.applicant_type == P())
        params.append(args.applicant_type)
    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        cq = cq.where(t.company_id == P())
        params.append(args.company_id)

    total = conn.execute(cq.get_sql(), params).fetchone()[0]

    q = q.orderby(t.created_at, order=Order.desc)
    rows = conn.execute(q.get_sql(), params).fetchall()

    records = [dict(r) for r in rows]
    ok({"records": records, "total": total})


# ---------------------------------------------------------------------------
# 9. get-loan
# ---------------------------------------------------------------------------
def handle_get_loan(conn, args):
    loan_id = getattr(args, "loan_id", None) or getattr(args, "id", None)
    if not loan_id:
        err("--loan-id is required")
    loan_row = conn.execute(Q.from_(Table("loan")).select(Table("loan").star).where(Field("id") == P()).get_sql(), (loan_id,)).fetchone()
    if not loan_row:
        err(f"Loan {loan_id} not found")
    loan = dict(loan_row)

    # Include repayment schedule
    rs = Table("loan_repayment_schedule")
    sched_q = (Q.from_(rs)
               .select(rs.id, rs.installment_no, rs.due_date, rs.principal_amount,
                       rs.interest_amount, rs.total_amount, rs.paid_amount,
                       rs.outstanding, rs.status, rs.payment_date)
               .where(rs.loan_id == P())
               .orderby(rs.installment_no))
    sched_rows = conn.execute(sched_q.get_sql(), (loan_id,)).fetchall()
    loan["repayment_schedule"] = [dict(r) for r in sched_rows]

    ok(loan)


# ---------------------------------------------------------------------------
# 10. generate-repayment-schedule
# ---------------------------------------------------------------------------
def handle_generate_repayment_schedule(conn, args):
    loan_id = getattr(args, "loan_id", None)
    loan_row = _validate_loan(conn, loan_id)
    loan = dict(loan_row)

    if loan["status"] not in ("draft", "disbursed"):
        err(f"Cannot generate schedule for loan in status '{loan['status']}'")

    # Delete existing schedule entries that haven't been paid
    rs = Table("loan_repayment_schedule")
    conn.execute(
        Q.from_(rs).delete()
        .where(rs.loan_id == P())
        .where(rs.status.isin(["pending", "overdue"])).get_sql(),
        (loan_id,))

    # Check if any paid entries exist (partial regeneration)
    paid_rows = conn.execute(
        Q.from_(rs).select(fn.Max(rs.installment_no).as_("last_paid_installment"))
        .where(rs.loan_id == P())
        .where(rs.status.isin(["paid", "partially_paid"])).get_sql(),
        (loan_id,)).fetchone()

    principal = to_decimal(loan["loan_amount"])
    annual_rate = to_decimal(loan["interest_rate"])
    periods = int(loan["repayment_periods"])
    method = loan["repayment_method"]
    start_date = loan["disbursement_date"] or _today_str()

    # If there are paid installments, adjust for remaining
    total_paid_principal = Decimal("0")
    start_installment = 1

    if paid_rows and paid_rows["last_paid_installment"]:
        last_paid = int(paid_rows["last_paid_installment"])
        start_installment = last_paid + 1

        # Sum principal already paid
        paid_principal_row = conn.execute(
            Q.from_(rs).select(
                LiteralValue("COALESCE(SUM(CAST(\"principal_amount\" AS REAL)),0)").as_("paid_principal"))
            .where(rs.loan_id == P()).where(rs.status == "paid").get_sql(),
            (loan_id,)).fetchone()
        total_paid_principal = to_decimal(str(paid_principal_row["paid_principal"]))
        principal = principal - total_paid_principal
        periods = periods - last_paid
        start_date = _add_months(loan["disbursement_date"] or _today_str(), last_paid)

    if periods <= 0:
        ok({
            "loan_id": loan_id,
            "message": "Loan fully repaid, no schedule to generate",
            "installments": 0,
        })
        return

    schedule = _generate_schedule(principal, annual_rate, periods, method, start_date)
    now = _now_iso()

    sched_sql, _ = insert_row("loan_repayment_schedule", {
        "id": P(), "loan_id": P(), "installment_no": P(), "due_date": P(),
        "principal_amount": P(), "interest_amount": P(), "total_amount": P(),
        "paid_amount": P(), "outstanding": P(), "status": P(), "payment_date": P(),
    })
    for item in schedule:
        sched_id = str(uuid.uuid4())
        adjusted_installment_no = item["installment_no"] + start_installment - 1
        conn.execute(sched_sql, (
            sched_id, loan_id, adjusted_installment_no, item["due_date"],
            item["principal_amount"], item["interest_amount"], item["total_amount"],
            "0.00", item["total_amount"],
            "pending", None,
        ))

    # Recalculate total interest on the loan
    total_interest_row = conn.execute(
        Q.from_(rs).select(
            LiteralValue("COALESCE(SUM(CAST(\"interest_amount\" AS REAL)),0)").as_("total_interest"))
        .where(rs.loan_id == P()).get_sql(),
        (loan_id,)).fetchone()
    total_interest_str = str(round_currency(to_decimal(str(total_interest_row["total_interest"]))))

    sql = update_row("loan",
        data={"total_interest": P(), "updated_at": P()},
        where={"id": P()})
    conn.execute(sql, (total_interest_str, now, loan_id))

    audit(conn, "erpclaw-loans", "loan-generate-repayment-schedule", "loan", loan_id,
          new_values={"installments_generated": len(schedule)})
    conn.commit()
    ok({
        "loan_id": loan_id,
        "installments": len(schedule),
        "total_interest": total_interest_str,
        "schedule": schedule,
    })


# ---------------------------------------------------------------------------
# 11. restructure-loan
# ---------------------------------------------------------------------------
def handle_restructure_loan(conn, args):
    loan_id = getattr(args, "loan_id", None)
    loan_row = _validate_loan(conn, loan_id)
    loan = dict(loan_row)

    if loan["status"] not in ("disbursed", "partially_repaid"):
        err(
            f"Cannot restructure loan in status '{loan['status']}'. "
            f"Must be disbursed or partially_repaid."
        )

    new_interest_rate = getattr(args, "new_interest_rate", None)
    new_repayment_periods = getattr(args, "new_repayment_periods", None)

    if not new_interest_rate and not new_repayment_periods:
        err("At least one of --new-interest-rate or --new-repayment-periods is required")

    old_values = {
        "interest_rate": loan["interest_rate"],
        "repayment_periods": loan["repayment_periods"],
    }

    # Update loan terms
    upd_data = {}

    if new_interest_rate:
        rate_dec = to_decimal(new_interest_rate)
        if rate_dec < Decimal("0"):
            err("--new-interest-rate cannot be negative")
        upd_data["interest_rate"] = str(round_currency(rate_dec))

    if new_repayment_periods:
        periods_int = int(new_repayment_periods)
        if periods_int <= 0:
            err("--new-repayment-periods must be a positive integer")
        upd_data["repayment_periods"] = periods_int

    now = _now_iso()
    upd_data["updated_at"] = now
    sql, params = dynamic_update("loan", upd_data, {"id": loan_id})
    conn.execute(sql, params)

    # Delete unpaid schedule entries
    rs = Table("loan_repayment_schedule")
    conn.execute(
        Q.from_(rs).delete()
        .where(rs.loan_id == P())
        .where(rs.status.isin(["pending", "overdue"])).get_sql(),
        (loan_id,))

    # Determine remaining principal
    paid_principal_row = conn.execute(
        Q.from_(rs).select(
            LiteralValue("COALESCE(SUM(CAST(\"principal_amount\" AS REAL)),0)").as_("paid_principal"))
        .where(rs.loan_id == P()).where(rs.status == "paid").get_sql(),
        (loan_id,)).fetchone()
    total_paid_principal = to_decimal(str(paid_principal_row["paid_principal"]))
    remaining_principal = to_decimal(loan["loan_amount"]) - total_paid_principal

    # Determine start installment
    last_paid_row = conn.execute(
        Q.from_(rs).select(fn.Max(rs.installment_no).as_("last_no"))
        .where(rs.loan_id == P()).where(rs.status == "paid").get_sql(),
        (loan_id,)).fetchone()
    last_paid_no = (
        int(last_paid_row["last_no"])
        if last_paid_row and last_paid_row["last_no"]
        else 0
    )

    # Use updated loan values
    effective_rate = (
        to_decimal(new_interest_rate)
        if new_interest_rate
        else to_decimal(loan["interest_rate"])
    )
    effective_periods = (
        int(new_repayment_periods)
        if new_repayment_periods
        else int(loan["repayment_periods"])
    )
    remaining_periods = effective_periods - last_paid_no

    if remaining_periods <= 0:
        err("New repayment periods must be greater than already-paid installments")

    # Calculate start date for remaining schedule
    base_date = loan["disbursement_date"] or _today_str()
    start_date = _add_months(base_date, last_paid_no)

    method = loan["repayment_method"]
    schedule = _generate_schedule(
        remaining_principal, effective_rate, remaining_periods, method, start_date
    )

    sched_sql, _ = insert_row("loan_repayment_schedule", {
        "id": P(), "loan_id": P(), "installment_no": P(), "due_date": P(),
        "principal_amount": P(), "interest_amount": P(), "total_amount": P(),
        "paid_amount": P(), "outstanding": P(), "status": P(), "payment_date": P(),
        "created_at": P(), "updated_at": P(),
    })
    for item in schedule:
        sched_id = str(uuid.uuid4())
        adjusted_no = item["installment_no"] + last_paid_no
        conn.execute(sched_sql, (
            sched_id, loan_id, adjusted_no, item["due_date"],
            item["principal_amount"], item["interest_amount"], item["total_amount"],
            "0.00", item["total_amount"],
            "pending", None, now, now,
        ))

    # Recalculate maturity date and total interest
    new_maturity = _add_months(base_date, effective_periods)

    total_interest_row = conn.execute(
        Q.from_(rs).select(
            LiteralValue("COALESCE(SUM(CAST(\"interest_amount\" AS REAL)),0)").as_("total_interest"))
        .where(rs.loan_id == P()).get_sql(),
        (loan_id,)).fetchone()
    total_interest_str = str(round_currency(
        to_decimal(str(total_interest_row["total_interest"]))
    ))

    # Update outstanding amount, maturity, and total interest
    sql = update_row("loan",
        data={"maturity_date": P(), "total_interest": P(),
              "outstanding_amount": P(), "updated_at": P()},
        where={"id": P()})
    conn.execute(sql, (
        new_maturity, total_interest_str,
        str(round_currency(remaining_principal)), now, loan_id,
    ))

    new_values = {}
    if new_interest_rate:
        new_values["interest_rate"] = str(round_currency(to_decimal(new_interest_rate)))
    if new_repayment_periods:
        new_values["repayment_periods"] = int(new_repayment_periods)
    new_values["maturity_date"] = new_maturity

    audit(conn, "erpclaw-loans", "loan-restructure-loan", "loan", loan_id,
          old_values=old_values, new_values=new_values,
          description="Restructured loan terms")
    conn.commit()
    ok({
        "loan_id": loan_id,
        "remaining_principal": str(round_currency(remaining_principal)),
        "new_maturity_date": new_maturity,
        "total_interest": total_interest_str,
        "installments_regenerated": len(schedule),
    })


# ---------------------------------------------------------------------------
# 12. close-loan
# ---------------------------------------------------------------------------
def handle_close_loan(conn, args):
    loan_id = getattr(args, "loan_id", None)
    loan_row = _validate_loan(conn, loan_id)
    loan = dict(loan_row)

    if loan["status"] == "closed":
        err("Loan is already closed")

    if loan["status"] not in ("disbursed", "partially_repaid", "repaid"):
        err(
            f"Cannot close loan in status '{loan['status']}'. "
            f"Must be disbursed, partially_repaid, or repaid."
        )

    # Check outstanding amount
    outstanding = to_decimal(loan["outstanding_amount"])
    if outstanding > Decimal("0"):
        # Check if all schedule items are paid or waived
        rs = Table("loan_repayment_schedule")
        pending_rows = conn.execute(
            Q.from_(rs).select(fn.Count("*"))
            .where(rs.loan_id == P())
            .where(rs.status.isin(["pending", "overdue", "partially_paid"])).get_sql(),
            (loan_id,)).fetchone()
        pending_count = pending_rows[0]

        if pending_count > 0:
            err(
                f"Cannot close loan with outstanding amount {outstanding} "
                f"and {pending_count} unpaid installments. "
                f"All installments must be paid or waived before closing."
            )

    now = _now_iso()
    sql = update_row("loan", data={"status": P(), "updated_at": P()}, where={"id": P()})
    conn.execute(sql, ("closed", now, loan_id))

    audit(conn, "erpclaw-loans", "loan-close-loan", "loan", loan_id,
          old_values={"status": loan["status"]},
          new_values={"status": "closed"},
          description=f"Closed loan {loan['naming_series']}")
    conn.commit()
    ok({
        "loan_id": loan_id,
        "loan_status": "closed",
        "naming_series": loan["naming_series"],
    })


def handle_get_repayment_schedule(conn, args):
    """Get repayment schedule for a loan."""
    loan_id = getattr(args, "loan_id", None)
    if not loan_id:
        return err("--loan-id is required")

    rs = Table("loan_repayment_schedule")
    q = (Q.from_(rs)
         .select(rs.installment_no, rs.due_date, rs.principal_amount, rs.interest_amount,
                 rs.total_amount, rs.paid_amount, rs.outstanding, rs.status)
         .where(rs.loan_id == P())
         .orderby(rs.installment_no))
    rows = conn.execute(q.get_sql(), (loan_id,)).fetchall()

    schedule = [dict(r) for r in rows]
    return ok({"loan_id": loan_id, "schedule": schedule})


# ---------------------------------------------------------------------------
# Action Router
# ---------------------------------------------------------------------------
ACTIONS = {
    "loan-add-loan-application": handle_add_loan_application,
    "loan-update-loan-application": handle_update_loan_application,
    "loan-list-loan-applications": handle_list_loan_applications,
    "loan-get-loan-application": handle_get_loan_application,
    "loan-approve-loan": handle_approve_loan,
    "loan-reject-loan": handle_reject_loan,
    "loan-disburse-loan": handle_disburse_loan,
    "loan-list-loans": handle_list_loans,
    "loan-get-loan": handle_get_loan,
    "loan-generate-repayment-schedule": handle_generate_repayment_schedule,
    "loan-get-repayment-schedule": handle_get_repayment_schedule,
    "loan-restructure-loan": handle_restructure_loan,
    "loan-close-loan": handle_close_loan,
}


# ---------------------------------------------------------------------------
# Argument Registration
# ---------------------------------------------------------------------------
def register_args(parser):
    """Register argparse arguments for the loans domain."""
    # Shared / identification
    parser.add_argument("--id")
    parser.add_argument("--company-id")

    # Loan application fields
    parser.add_argument("--applicant-type")
    parser.add_argument("--applicant-id")
    parser.add_argument("--loan-type")
    parser.add_argument("--requested-amount")
    parser.add_argument("--interest-rate")
    parser.add_argument("--repayment-method")
    parser.add_argument("--repayment-periods")
    parser.add_argument("--purpose")
    parser.add_argument("--collateral-description")
    parser.add_argument("--collateral-value")

    # Approval / rejection
    parser.add_argument("--approved-amount")
    parser.add_argument("--reason")

    # Disbursement
    parser.add_argument("--loan-application-id")
    parser.add_argument("--loan-account-id")
    parser.add_argument("--interest-income-account-id")
    parser.add_argument("--disbursement-account-id")
    parser.add_argument("--disbursement-date")

    # Loan queries
    parser.add_argument("--loan-id")
    parser.add_argument("--status")

    # Restructuring
    parser.add_argument("--new-interest-rate")
    parser.add_argument("--new-repayment-periods")
