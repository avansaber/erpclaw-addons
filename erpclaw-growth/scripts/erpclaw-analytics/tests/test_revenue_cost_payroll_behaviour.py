"""Part A: behaviour of the revenue, cost and payroll analytics reports over a
small known book.

Every figure below is computed by hand from the rows the test writes, with
fixed dates and an explicit date range, and compared as an exact string.

Revenue reports (revenue-by-customer, revenue-by-item, revenue-trend, and the
two other readers of the same invoice set, customer-concentration and the
executive dashboard's selling section) count every invoice that has been
posted and not cancelled: submitted, partially paid, overdue and paid. Draft
and cancelled invoices, invoices outside the range and invoices of another
company are left out. The ranked reports order by the numeric value of the
summed TEXT amount: compared as text, "950.00" would rank above "1250.00".

expense-breakdown and cost-trend read expense accounts from the ledger,
excluding cancelled ledger rows; payroll-analytics reads the salary slips of
submitted payroll runs that fall inside the range.
"""
import os
import sys
import uuid

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from analytics_helpers import (call_action, is_error, is_ok,  # noqa: E402
                               load_db_query, ns, seed_accounts, seed_company)
from erpclaw_lib.query import P, Q, Table, insert_row  # noqa: E402

MOD = load_db_query()

JAN_FEB = {"from_date": "2026-01-01", "to_date": "2026-02-28"}
Q1 = {"from_date": "2026-01-01", "to_date": "2026-03-31"}


def _id():
    return str(uuid.uuid4())


def _insert(conn, table, row):
    sql, _ = insert_row(table, {k: P() for k in row})
    conn.execute(sql, tuple(row.values()))


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def _seed_sales(conn):
    """Two companies; company A has two customers, two items, eight invoices."""
    co = seed_company(conn, "Revenue Co", "REV")
    other = seed_company(conn, "Other Co", "OTH")
    cust = {}
    for key, name, company in (("acme", "Acme Stores", co),
                               ("birch", "Birch Supply", co),
                               ("zed", "Zed Other", other)):
        cust[key] = _id()
        _insert(conn, "customer", {"id": cust[key], "name": name, "company_id": company})
    item = {}
    for key, code, name in (("widget", "WIDGET-M347", "Widget"),
                            ("gadget", "GADGET-M347", "Gadget")):
        item[key] = _id()
        _insert(conn, "item", {"id": item[key], "item_code": code, "item_name": name})

    # (customer, company, posting_date, status, item, qty, grand_total)
    invoices = (
        ("acme", co, "2026-01-10", "submitted", "widget", "3", "900.00"),
        ("birch", co, "2026-01-20", "paid", "gadget", "4", "1000.00"),
        ("birch", co, "2026-02-05", "overdue", "gadget", "1", "250.00"),
        ("acme", co, "2026-02-15", "partially_paid", "gadget", "1", "50.00"),
        ("acme", co, "2026-02-20", "cancelled", "widget", "10", "7000.00"),
        ("birch", co, "2026-02-25", "draft", "gadget", "1", "300.00"),
        ("acme", co, "2026-03-05", "submitted", "widget", "1", "111.00"),
        ("zed", other, "2026-01-15", "submitted", "widget", "5", "5000.00"),
    )
    for customer, company, posting_date, status, it, qty, total in invoices:
        si_id = _id()
        _insert(conn, "sales_invoice", {
            "id": si_id, "customer_id": cust[customer], "posting_date": posting_date,
            "total_amount": total, "grand_total": total, "outstanding_amount": total,
            "status": status, "company_id": company,
        })
        _insert(conn, "sales_invoice_item", {
            "id": _id(), "sales_invoice_id": si_id, "item_id": item[it],
            "quantity": qty, "rate": total, "amount": total, "net_amount": total,
        })
    conn.commit()
    return {"company_id": co, "other_id": other, "customer": cust, "item": item}


def _seed_ledger(conn):
    """Expense postings for company A and one for company B."""
    co = seed_company(conn, "Cost Co", "CST")
    other = seed_company(conn, "Cost Other", "CSO")
    acc = seed_accounts(conn, co)
    other_acc = seed_accounts(conn, other)
    travel = _id()
    _insert(conn, "account", {"id": travel, "name": "Travel", "account_number": "6100",
                              "root_type": "expense", "account_type": "expense",
                              "is_group": 0, "company_id": co})
    ops, sales = _id(), _id()
    _insert(conn, "cost_center", {"id": ops, "name": "Ops", "company_id": co})
    _insert(conn, "cost_center", {"id": sales, "name": "Sales", "company_id": co})

    def post(posting_date, expense_account, debit, credit, cost_center=None,
             cash=acc["cash"], cancelled=0):
        voucher = _id()
        _insert(conn, "gl_entry", {
            "id": _id(), "posting_date": posting_date, "account_id": expense_account,
            "debit": debit, "credit": credit, "voucher_type": "journal_entry",
            "voucher_id": voucher, "cost_center_id": cost_center,
            "is_cancelled": cancelled})
        _insert(conn, "gl_entry", {
            "id": _id(), "posting_date": posting_date, "account_id": cash,
            "debit": credit, "credit": debit, "voucher_type": "journal_entry",
            "voucher_id": voucher, "cost_center_id": None,
            "is_cancelled": cancelled})

    post("2026-01-12", acc["expense"], "700.00", "0", ops)
    post("2026-01-18", acc["cost_of_goods_sold"], "1250.00", "0", sales)
    post("2026-02-03", travel, "450.25", "0")
    # A cancelled posting: the original and its mirror are both marked cancelled.
    post("2026-02-10", acc["expense"], "300.00", "0", ops, cancelled=1)
    post("2026-02-10", acc["expense"], "0", "300.00", ops, cancelled=1)
    # A refund credited back to operating expenses.
    post("2026-02-14", acc["expense"], "0", "100.00", ops)
    post("2026-03-02", acc["cost_of_goods_sold"], "999.99", "0", sales)
    post("2026-01-15", other_acc["expense"], "8000.00", "0", cash=other_acc["cash"])
    conn.commit()
    return {"company_id": co, "other_id": other, "accounts": acc,
            "other_accounts": other_acc}


def _seed_payroll(conn):
    co = seed_company(conn, "Payroll Co", "PAY")
    other = seed_company(conn, "Payroll Other", "PAO")
    dept = _id()
    _insert(conn, "department", {"id": dept, "name": "Engineering", "company_id": co})
    emp = {}
    for key, first, company in (("e1", "Ann", co), ("e2", "Ben", co),
                                ("e3", "Cal", co), ("e4", "Dee", other)):
        emp[key] = _id()
        _insert(conn, "employee", {"id": emp[key], "first_name": first, "full_name": first,
                                   "date_of_joining": "2025-01-01", "company_id": company})

    # (company, start, end, run status, department, [(employee, gross, deductions, net)])
    runs = (
        (co, "2026-01-01", "2026-01-31", "submitted", None,
         [("e1", "5000.00", "1200.50", "3799.50"), ("e2", "4200.00", "900.00", "3300.00")]),
        (co, "2026-02-01", "2026-02-28", "submitted", dept,
         [("e1", "5100.00", "1225.00", "3875.00")]),
        (co, "2026-02-01", "2026-02-28", "cancelled", None,
         [("e3", "9999.00", "999.00", "9000.00")]),
        (co, "2026-02-01", "2026-02-28", "draft", None,
         [("e2", "1234.00", "234.00", "1000.00")]),
        (co, "2026-03-01", "2026-03-31", "submitted", None,
         [("e3", "6000.00", "1000.00", "5000.00")]),
        (other, "2026-01-01", "2026-01-31", "submitted", None,
         [("e4", "7777.00", "777.00", "7000.00")]),
    )
    for company, start, end, status, department, slips in runs:
        run_id = _id()
        _insert(conn, "payroll_run", {"id": run_id, "period_start": start, "period_end": end,
                                      "department_id": department, "status": status,
                                      "company_id": company})
        for employee, gross, deductions, net in slips:
            _insert(conn, "salary_slip", {
                "id": _id(), "payroll_run_id": run_id, "employee_id": emp[employee],
                "period_start": start, "period_end": end, "gross_pay": gross,
                "total_deductions": deductions, "net_pay": net, "status": status,
                "company_id": company})
    conn.commit()
    return {"company_id": co, "other_id": other, "department_id": dept}


# ---------------------------------------------------------------------------
# revenue-by-customer
# ---------------------------------------------------------------------------

def test_revenue_by_customer_counts_posted_invoices_ranked_by_amount(conn):
    book = _seed_sales(conn)
    r = call_action(MOD.revenue_by_customer, conn, ns(company_id=book["company_id"], **JAN_FEB))
    assert is_ok(r), r
    assert r["grand_total"] == "2200.00"
    assert r["count"] == 2
    assert [(c["customer_id"], c["customer_name"], c["invoice_count"], c["revenue"], c["share"])
            for c in r["customers"]] == [
        (book["customer"]["birch"], "Birch Supply", 2, "1250.00", "56.8%"),
        (book["customer"]["acme"], "Acme Stores", 2, "950.00", "43.2%"),
    ]


def test_revenue_by_customer_pages_the_ranked_list(conn):
    book = _seed_sales(conn)
    first = call_action(MOD.revenue_by_customer, conn, ns(
        company_id=book["company_id"], limit="1", offset="0", **JAN_FEB))
    second = call_action(MOD.revenue_by_customer, conn, ns(
        company_id=book["company_id"], limit="1", offset="1", **JAN_FEB))
    assert [c["revenue"] for c in first["customers"]] == ["1250.00"]
    assert [c["revenue"] for c in second["customers"]] == ["950.00"]
    assert first["grand_total"] == second["grand_total"] == "2200.00"


def test_revenue_by_customer_is_scoped_to_the_company_and_refuses(conn):
    book = _seed_sales(conn)
    r = call_action(MOD.revenue_by_customer, conn, ns(company_id=book["other_id"], **JAN_FEB))
    assert [(c["customer_name"], c["invoice_count"], c["revenue"], c["share"])
            for c in r["customers"]] == [("Zed Other", 1, "5000.00", "100.0%")]
    assert r["grand_total"] == "5000.00"

    r = call_action(MOD.revenue_by_customer, conn, ns(company_id=None, **JAN_FEB))
    assert is_error(r) and r["message"] == "--company-id is required"
    r = call_action(MOD.revenue_by_customer, conn, ns(
        company_id=book["company_id"], from_date=None, to_date="2026-02-28"))
    assert is_error(r) and r["message"] == "--from-date is required"
    r = call_action(MOD.revenue_by_customer, conn, ns(
        company_id=book["company_id"], from_date="2026-01-01", to_date=None))
    assert is_error(r) and r["message"] == "--to-date is required"


# ---------------------------------------------------------------------------
# revenue-by-item
# ---------------------------------------------------------------------------

def test_revenue_by_item_sums_lines_of_posted_invoices_ranked_by_amount(conn):
    book = _seed_sales(conn)
    r = call_action(MOD.revenue_by_item, conn, ns(company_id=book["company_id"], **JAN_FEB))
    assert is_ok(r), r
    assert r["grand_total"] == "2200.00"
    assert [(i["item_id"], i["item_name"], i["qty_sold"], i["revenue"], i["share"])
            for i in r["items"]] == [
        (book["item"]["gadget"], "Gadget", "6", "1300.00", "59.1%"),
        (book["item"]["widget"], "Widget", "3", "900.00", "40.9%"),
    ]

    other = call_action(MOD.revenue_by_item, conn, ns(company_id=book["other_id"], **JAN_FEB))
    assert [(i["item_name"], i["qty_sold"], i["revenue"]) for i in other["items"]] == [
        ("Widget", "5", "5000.00")]
    assert other["grand_total"] == "5000.00"

    r = call_action(MOD.revenue_by_item, conn, ns(
        company_id=book["company_id"], from_date="2026-01-01", to_date=None))
    assert is_error(r) and r["message"] == "--to-date is required"


# ---------------------------------------------------------------------------
# revenue-trend
# ---------------------------------------------------------------------------

def test_revenue_trend_monthly_and_quarterly(conn):
    book = _seed_sales(conn)
    r = call_action(MOD.revenue_trend, conn, ns(company_id=book["company_id"], **Q1))
    assert is_ok(r), r
    assert r["source"] == "sales_invoice"
    assert r["total_revenue"] == "2311.00"
    assert [(t["period"], t["from_date"], t["to_date"], t["revenue"], t.get("change_pct"))
            for t in r["trend"]] == [
        ("Jan 2026", "2026-01-01", "2026-01-31", "1900.00", None),
        ("Feb 2026", "2026-02-01", "2026-02-28", "300.00", "-84.2%"),
        ("Mar 2026", "2026-03-01", "2026-03-31", "111.00", "-63.0%"),
    ]

    q = call_action(MOD.revenue_trend, conn, ns(
        company_id=book["company_id"], periodicity="quarterly", **Q1))
    assert [(t["period"], t["revenue"]) for t in q["trend"]] == [("Q1 2026", "2311.00")]

    other = call_action(MOD.revenue_trend, conn, ns(company_id=book["other_id"], **Q1))
    assert [t["revenue"] for t in other["trend"]] == ["5000.00", "0.00", "0.00"]

    r = call_action(MOD.revenue_trend, conn, ns(company_id=None, **Q1))
    assert is_error(r) and r["message"] == "--company-id is required"


# ---------------------------------------------------------------------------
# The other two readers of the same invoice set
# ---------------------------------------------------------------------------

def test_customer_concentration_and_dashboard_read_the_same_invoice_set(conn):
    book = _seed_sales(conn)
    r = call_action(MOD.customer_concentration, conn, ns(
        company_id=book["company_id"], **JAN_FEB))
    assert is_ok(r), r
    assert r["total_revenue"] == "2200.00"
    assert [(c["rank"], c["customer"], c["revenue"], c["share"], c["cumulative_share"])
            for c in r["top_customers"]] == [
        (1, "Birch Supply", "1250.00", "56.8%", "56.8%"),
        (2, "Acme Stores", "950.00", "43.2%", "100.0%"),
    ]
    assert r["concentration"]["top_1_share"] == "56.8%"

    d = call_action(MOD.executive_dashboard, conn, ns(
        company_id=book["company_id"], **JAN_FEB))
    assert is_ok(d), d
    selling = d["sections"]["selling"]
    assert (selling["invoices"], selling["invoice_total"]) == (4, "2200.00")


# ---------------------------------------------------------------------------
# expense-breakdown
# ---------------------------------------------------------------------------

def test_expense_breakdown_by_account_and_by_cost_center(conn):
    book = _seed_ledger(conn)
    r = call_action(MOD.expense_breakdown, conn, ns(company_id=book["company_id"], **JAN_FEB))
    assert is_ok(r), r
    assert r["group_by"] == "account"
    assert r["total_expenses"] == "2300.25"
    assert r["count"] == 3
    assert [(b["name"], b["account_number"], b["amount"], b["percentage"])
            for b in r["breakdown"]] == [
        ("COGS", "5000", "1250.00", "54.3%"),
        ("Operating Expenses", "6000", "600.00", "26.1%"),
        ("Travel", "6100", "450.25", "19.6%"),
    ]

    cc = call_action(MOD.expense_breakdown, conn, ns(
        company_id=book["company_id"], group_by="cost_center", **JAN_FEB))
    assert cc["total_expenses"] == "2300.25"
    assert [(b["name"], b["amount"], b["percentage"]) for b in cc["breakdown"]] == [
        ("Sales", "1250.00", "54.3%"),
        ("Ops", "600.00", "26.1%"),
        ("Unassigned", "450.25", "19.6%"),
    ]

    other = call_action(MOD.expense_breakdown, conn, ns(company_id=book["other_id"], **JAN_FEB))
    assert [(b["name"], b["amount"]) for b in other["breakdown"]] == [
        ("Operating Expenses", "8000.00")]
    assert other["total_expenses"] == "8000.00"


def test_expense_breakdown_refusals(conn):
    book = _seed_ledger(conn)
    r = call_action(MOD.expense_breakdown, conn, ns(
        company_id=book["company_id"], group_by="department", **JAN_FEB))
    assert is_error(r) and r["message"] == "--group-by must be 'account' or 'cost_center'"
    r = call_action(MOD.expense_breakdown, conn, ns(
        company_id=book["company_id"], from_date=None, to_date="2026-02-28"))
    assert is_error(r) and r["message"] == "--from-date is required"


# ---------------------------------------------------------------------------
# cost-trend
# ---------------------------------------------------------------------------

def test_cost_trend_all_expenses_single_account_and_company_scope(conn):
    book = _seed_ledger(conn)
    r = call_action(MOD.cost_trend, conn, ns(company_id=book["company_id"], **Q1))
    assert is_ok(r), r
    assert (r["total"], r["average"], r["account_id"]) == ("3300.24", "1100.08", None)
    assert [(t["period"], t["amount"], t.get("change_pct")) for t in r["periods"]] == [
        ("Jan 2026", "1950.00", None),
        ("Feb 2026", "350.25", "-82.0%"),
        ("Mar 2026", "999.99", "185.5%"),
    ]

    one = call_action(MOD.cost_trend, conn, ns(
        company_id=book["company_id"], account_id=book["accounts"]["expense"], **Q1))
    assert (one["total"], one["average"]) == ("600.00", "200.00")
    assert [(t["amount"], t.get("change_pct")) for t in one["periods"]] == [
        ("700.00", None), ("-100.00", "-114.3%"), ("0.00", "-100.0%")]

    q = call_action(MOD.cost_trend, conn, ns(
        company_id=book["company_id"], periodicity="quarterly", **Q1))
    assert [(t["period"], t["amount"]) for t in q["periods"]] == [("Q1 2026", "3300.24")]

    other = call_action(MOD.cost_trend, conn, ns(company_id=book["other_id"], **Q1))
    assert (other["total"], other["average"]) == ("8000.00", "2666.67")
    assert [(t["amount"], t.get("change_pct")) for t in other["periods"]] == [
        ("8000.00", None), ("0.00", "-100.0%"), ("0.00", "N/A")]

    r = call_action(MOD.cost_trend, conn, ns(company_id=None, **Q1))
    assert is_error(r) and r["message"] == "--company-id is required"


def test_cost_trend_refuses_an_account_of_another_company(conn):
    book = _seed_ledger(conn)
    other_expense = book["other_accounts"]["expense"]
    own_expense = book["accounts"]["expense"]

    def _snap():
        snap = {}
        for name in ("gl_entry", "account"):
            tbl = Table(name)
            q = Q.from_(tbl).select("*")
            rows = conn.execute(q.get_sql(), ()).fetchall()
            dumped = []
            for row in rows:
                dumped.append(tuple(sorted(
                    (k, "" if row[k] is None else str(row[k]))
                    for k in row.keys())))
            snap[name] = sorted(dumped)
        return snap

    before = _snap()

    r = call_action(MOD.cost_trend, conn, ns(
        company_id=book["company_id"], account_id=other_expense, **Q1))
    assert is_error(r), r
    assert r["message"] == (
        f"Account {other_expense} does not belong to company "
        f"{book['company_id']}")

    ghost = str(uuid.uuid4())
    r = call_action(MOD.cost_trend, conn, ns(
        company_id=book["company_id"], account_id=ghost, **Q1))
    assert is_error(r), r
    assert r["message"] == f"Account {ghost} not found"

    r = call_action(MOD.cost_trend, conn, ns(
        company_id=book["other_id"], account_id=other_expense, **Q1))
    assert is_ok(r), r
    assert (r["total"], r["average"]) == ("8000.00", "2666.67")
    assert [x["amount"] for x in r["periods"]] == ["8000.00", "0.00", "0.00"]

    r = call_action(MOD.cost_trend, conn, ns(
        company_id=book["company_id"], account_id=own_expense, **Q1))
    assert is_ok(r), r
    assert (r["total"], r["average"]) == ("600.00", "200.00")

    assert _snap() == before


# ---------------------------------------------------------------------------
# payroll-analytics
# ---------------------------------------------------------------------------

def test_payroll_analytics_sums_slips_of_submitted_runs_in_range(conn):
    book = _seed_payroll(conn)
    r = call_action(MOD.payroll_analytics, conn, ns(company_id=book["company_id"], **JAN_FEB))
    assert is_ok(r), r
    assert (r["slip_count"], r["total_gross"], r["total_deductions"], r["total_net"],
            r["avg_gross_per_employee"]) == (3, "14300.00", "3325.50", "10974.50", "4766.67")

    dept = call_action(MOD.payroll_analytics, conn, ns(
        company_id=book["company_id"], department_id=book["department_id"], **JAN_FEB))
    assert (dept["slip_count"], dept["total_gross"], dept["total_deductions"],
            dept["total_net"], dept["avg_gross_per_employee"]) == (
        1, "5100.00", "1225.00", "3875.00", "5100.00")

    other = call_action(MOD.payroll_analytics, conn, ns(company_id=book["other_id"], **JAN_FEB))
    assert (other["slip_count"], other["total_gross"], other["total_net"]) == (
        1, "7777.00", "7000.00")

    empty = call_action(MOD.payroll_analytics, conn, ns(
        company_id=book["company_id"], from_date="2025-01-01", to_date="2025-12-31"))
    assert (empty["slip_count"], empty["total_gross"], empty["avg_gross_per_employee"]) == (
        0, "0.00", "0.00")

    r = call_action(MOD.payroll_analytics, conn, ns(
        company_id=book["company_id"], from_date="2026-01-01", to_date=None))
    assert is_error(r) and r["message"] == "--to-date is required"
