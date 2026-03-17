"""Domain module: reports — loan statements, overdue reports, portfolio summary."""

import os
import sys
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.response import ok, err
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row


def _dec(val):
    if val is None:
        return Decimal("0")
    return Decimal(str(val))


def handle_loan_statement(conn, args):
    """Full statement for a loan — disbursement, schedule, repayments."""
    loan_id = getattr(args, "loan_id", None)
    if not loan_id:
        return err("--loan-id is required")

    loan = conn.execute(Q.from_(Table("loan")).select(Table("loan").star).where(Field("id") == P()).get_sql(), (loan_id,)).fetchone()
    if not loan:
        return err(f"Loan {loan_id} not found")

    ld = dict(loan)

    # Get schedule
    schedule = conn.execute(
        """SELECT installment_no, due_date, principal_amount, interest_amount,
                  total_amount, paid_amount, outstanding, status
           FROM loan_repayment_schedule WHERE loan_id = ?
           ORDER BY installment_no""",
        (loan_id,),
    ).fetchall()

    schedule_list = [dict(r) for r in schedule]

    # Get repayments
    repayments = conn.execute(
        """SELECT naming_series, repayment_date, principal_amount,
                  interest_amount, penalty_amount, total_amount,
                  payment_method, status
           FROM loan_repayment WHERE loan_id = ?
           ORDER BY repayment_date""",
        (loan_id,),
    ).fetchall()

    repayment_list = [dict(r) for r in repayments]

    # Get write-offs
    write_offs = conn.execute(
        """SELECT write_off_date, write_off_amount, reason, status
           FROM loan_write_off WHERE loan_id = ?""",
        (loan_id,),
    ).fetchall()

    write_off_list = [dict(r) for r in write_offs]

    return ok({
        "loan": {
            "id": ld["id"],
            "naming_series": ld.get("naming_series"),
            "applicant_name": ld.get("applicant_name"),
            "loan_type": ld["loan_type"],
            "loan_amount": ld["loan_amount"],
            "disbursed_amount": ld["disbursed_amount"],
            "total_interest": ld["total_interest"],
            "total_repaid": ld["total_repaid"],
            "outstanding_amount": ld["outstanding_amount"],
            "interest_rate": ld["interest_rate"],
            "disbursement_date": ld.get("disbursement_date"),
            "maturity_date": ld.get("maturity_date"),
            "status": ld["status"],
        },
        "schedule": schedule_list,
        "repayments": repayment_list,
        "write_offs": write_off_list,
    })


def handle_overdue_loans(conn, args):
    """List overdue installments across all loans."""
    company_id = getattr(args, "company_id", None)
    today = date.today().isoformat()

    query = """
        SELECT l.id AS loan_id, l.naming_series AS loan_name,
               l.applicant_name, l.loan_type,
               s.installment_no, s.due_date, s.total_amount, s.outstanding
        FROM loan_repayment_schedule s
        JOIN loan l ON l.id = s.loan_id
        WHERE s.status IN ('pending', 'partially_paid')
          AND s.due_date < ?
          AND l.status IN ('disbursed', 'partially_repaid')
    """
    params = [today]

    if company_id:
        query += " AND l.company_id = ?"
        params.append(company_id)

    query += " ORDER BY s.due_date"

    rows = conn.execute(query, params).fetchall()
    records = [dict(r) for r in rows]

    total_overdue = sum(_dec(r["outstanding"]) for r in records)

    return ok({
        "records": records,
        "total": len(records),
        "total_overdue_amount": str(total_overdue),
        "as_of": today,
    })


def handle_status(conn, args):
    """Module status summary."""
    company_id = getattr(args, "company_id", None)

    query_base = "SELECT COUNT(*) as cnt FROM {} WHERE 1=1"
    params = []
    if company_id:
        query_base += " AND company_id = ?"
        params = [company_id]

    apps = conn.execute(
        query_base.format("loan_application"), params
    ).fetchone()
    loans = conn.execute(
        query_base.format("loan"), params
    ).fetchone()

    active_loans = conn.execute(
        "SELECT COUNT(*) as cnt FROM loan WHERE status IN ('disbursed', 'partially_repaid')"
        + (" AND company_id = ?" if company_id else ""),
        params
    ).fetchone()

    today = date.today().isoformat()
    overdue = conn.execute(
        """SELECT COUNT(*) as cnt FROM loan_repayment_schedule s
           JOIN loan l ON l.id = s.loan_id
           WHERE s.status IN ('pending', 'partially_paid')
             AND s.due_date < ?
             AND l.status IN ('disbursed', 'partially_repaid')"""
        + (" AND l.company_id = ?" if company_id else ""),
        [today] + params
    ).fetchone()

    total_outstanding = conn.execute(
        "SELECT COALESCE(SUM(CAST(outstanding_amount AS NUMERIC)), 0) as total FROM loan"
        " WHERE status IN ('disbursed', 'partially_repaid')"
        + (" AND company_id = ?" if company_id else ""),
        params
    ).fetchone()

    return ok({
        "skill": "erpclaw-loans",
        "version": "1.0.0",
        "applications": apps["cnt"] if hasattr(apps, "keys") else apps[0],
        "total_loans": loans["cnt"] if hasattr(loans, "keys") else loans[0],
        "active_loans": active_loans["cnt"] if hasattr(active_loans, "keys") else active_loans[0],
        "overdue_installments": overdue["cnt"] if hasattr(overdue, "keys") else overdue[0],
        "total_outstanding": str(Decimal(str(
            total_outstanding["total"] if hasattr(total_outstanding, "keys")
            else total_outstanding[0]
        )).quantize(Decimal("0.01"), ROUND_HALF_UP)),
    })


def register_args(parser):
    """Register argparse arguments for reports domain."""
    # No unique args — uses shared --loan-id, --company-id


ACTIONS = {
    "loan-statement": handle_loan_statement,
    "loan-overdue-loans": handle_overdue_loans,
    "status": handle_status,
}
