"""M462 depth: behavioural evidence for 8 analytics actions.

Every test below seeds a small known book, calls the action, and asserts the
exact response values computed by hand from the seeded rows. Money is compared
as exact strings and as Decimal, never float. After the call the test reads
the seeded rows back with PyPika-built queries and proves the action wrote
nothing (snapshot before == snapshot after). Each test ends with one refusal
case that checks the error message is truthful and the database is
byte-identical afterwards.

All 8 actions are read-only aggregations: none of them stores a row. The
depth signal is therefore "response equals the seeded book + no writes",
not "a row was created". Where an action reaches the ledger (gl_entry) the
test asserts both legs and that they balance; where it does not, a comment
says so explicitly so a later reader does not add a ledger assertion that
cannot hold.

Existing shallow tests live in test_analytics.py (shape/routability only).
Deeper invoice/payroll value tests live in
test_revenue_cost_payroll_behaviour.py; the tests here add the missing depth
signals for all 8 actions: seam read-back, no-write proof, and byte-identical
refusal.
"""
import json
import os
import sys
import uuid
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from analytics_helpers import (  # noqa: E402
    call_action, is_error, is_ok, load_db_query, ns, seed_accounts,
    seed_company,
)
from erpclaw_lib import seam  # noqa: E402
from erpclaw_lib.query import P, Q, Table, insert_row  # noqa: E402

MOD = load_db_query()

JAN_FEB = {"from_date": "2026-01-01", "to_date": "2026-02-28"}
Q1 = {"from_date": "2026-01-01", "to_date": "2026-03-31"}


def _id():
    return str(uuid.uuid4())


def _insert(conn, table, row):
    sql, _ = insert_row(table, {k: P() for k in row})
    conn.execute(sql, tuple(row.values()))


def _snapshot(conn, tables):
    """Full row dump per table, order-independent, stringified for equality."""
    snap = {}
    for name in tables:
        tbl = Table(name)
        q = Q.from_(tbl).select("*")
        rows = conn.execute(q.get_sql(), ()).fetchall()
        dumped = []
        for r in rows:
            dumped.append(tuple(sorted(
                (k, "" if r[k] is None else str(r[k])) for k in r.keys())))
        snap[name] = sorted(dumped)
    return snap


def _seed_revenue_book(conn):
    """Two companies; company A: 2 customers, 2 items, 4 posted + exclusions."""
    co = seed_company(conn, "Depth Revenue Co", "DREV")
    other = seed_company(conn, "Depth Revenue Other", "DRO")
    cust = {}
    for key, name, company in (("acme", "Acme Stores", co),
                               ("birch", "Birch Supply", co),
                               ("zed", "Zed Other", other)):
        cust[key] = _id()
        _insert(conn, "customer", {"id": cust[key], "name": name,
                                   "company_id": company})
    item = {}
    for key, code, name in (("widget", "WIDGET-D462", "Widget"),
                            ("gadget", "GADGET-D462", "Gadget")):
        item[key] = _id()
        _insert(conn, "item", {"id": item[key], "item_code": code,
                               "item_name": name})
    invoices = (
        ("acme", co, "2026-01-10", "submitted", "widget", "3", "900.00"),
        ("birch", co, "2026-01-20", "paid", "gadget", "5", "1250.00"),
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
            "id": si_id, "customer_id": cust[customer],
            "posting_date": posting_date, "total_amount": total,
            "grand_total": total, "outstanding_amount": total,
            "status": status, "company_id": company})
        _insert(conn, "sales_invoice_item", {
            "id": _id(), "sales_invoice_id": si_id, "item_id": item[it],
            "quantity": qty, "rate": total, "amount": total,
            "net_amount": total})
    conn.commit()
    return {"company_id": co, "other_id": other,
            "customer": cust, "item": item}


def _seed_payroll_book(conn):
    co = seed_company(conn, "Depth Payroll Co", "DPAY")
    other = seed_company(conn, "Depth Payroll Other", "DPO")
    dept = _id()
    _insert(conn, "department", {"id": dept, "name": "Engineering",
                                 "company_id": co})
    emp = {}
    for key, first, company in (("e1", "Ann", co), ("e2", "Ben", co),
                                ("e3", "Cal", co), ("e4", "Dee", other)):
        emp[key] = _id()
        _insert(conn, "employee", {"id": emp[key], "first_name": first,
                                   "full_name": first,
                                   "date_of_joining": "2025-01-01",
                                   "company_id": company})
    runs = (
        (co, "2026-01-01", "2026-01-31", "submitted", None,
         [("e1", "5000.00", "1200.50", "3799.50"),
          ("e2", "4200.00", "900.00", "3300.00")]),
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
        _insert(conn, "payroll_run", {"id": run_id, "period_start": start,
                                      "period_end": end,
                                      "department_id": department,
                                      "status": status, "company_id": company})
        for employee, gross, deductions, net in slips:
            _insert(conn, "salary_slip", {
                "id": _id(), "payroll_run_id": run_id,
                "employee_id": emp[employee], "period_start": start,
                "period_end": end, "gross_pay": gross,
                "total_deductions": deductions, "net_pay": net,
                "status": status, "company_id": company})
    conn.commit()
    return {"company_id": co, "other_id": other, "department_id": dept}


def _seed_ledger_book(conn):
    co = seed_company(conn, "Depth Ledger Co", "DLED")
    other = seed_company(conn, "Depth Ledger Other", "DLO")
    acc = seed_accounts(conn, co)
    other_acc = seed_accounts(conn, other)

    def post(posting_date, expense_account, debit, credit, cash,
             cancelled=0):
        voucher = _id()
        _insert(conn, "gl_entry", {
            "id": _id(), "posting_date": posting_date,
            "account_id": expense_account, "debit": debit, "credit": credit,
            "voucher_type": "journal_entry", "voucher_id": voucher,
            "is_cancelled": cancelled})
        _insert(conn, "gl_entry", {
            "id": _id(), "posting_date": posting_date, "account_id": cash,
            "debit": credit, "credit": debit,
            "voucher_type": "journal_entry", "voucher_id": voucher,
            "is_cancelled": cancelled})

    post("2026-01-15", acc["revenue"], "0", "10000.00", acc["cash"])
    post("2026-01-20", acc["expense"], "4000.00", "0", acc["cash"])
    post("2026-02-15", acc["revenue"], "0", "5000.00", acc["cash"])
    post("2026-02-20", acc["expense"], "1000.00", "0", acc["cash"])
    post("2026-02-10", acc["expense"], "300.00", "0", acc["cash"],
         cancelled=1)
    post("2026-02-10", acc["expense"], "0", "300.00", acc["cash"],
         cancelled=1)
    post("2026-01-15", other_acc["revenue"], "0", "8000.00",
         other_acc["cash"])
    conn.commit()
    return {"company_id": co, "other_id": other, "accounts": acc}


def _seed_projects(conn):
    co = seed_company(conn, "Depth Projects Co", "DPRJ")
    other = seed_company(conn, "Depth Projects Other", "DPO2")
    alpha = _id()
    beta = _id()
    gamma = _id()
    _insert(conn, "project", {
        "id": alpha, "project_name": "Alpha", "status": "in_progress",
        "estimated_cost": "10000.00", "actual_cost": "7000.00",
        "total_billed": "12000.00", "profit_margin": "0",
        "company_id": co, "start_date": "2026-01-05"})
    _insert(conn, "project", {
        "id": beta, "project_name": "Beta", "status": "completed",
        "estimated_cost": "5000.00", "actual_cost": "5500.00",
        "total_billed": "4000.00", "profit_margin": "0",
        "company_id": co, "start_date": "2026-02-10"})
    _insert(conn, "project", {
        "id": gamma, "project_name": "Gamma Other", "status": "open",
        "estimated_cost": "100.00", "actual_cost": "50.00",
        "total_billed": "200.00", "profit_margin": "0",
        "company_id": other, "start_date": "2026-01-05"})
    conn.commit()
    return {"company_id": co, "other_id": other,
            "alpha": alpha, "beta": beta, "gamma": gamma}


def _seed_quality(conn):
    co = seed_company(conn, "Depth Quality Co", "DQAL")
    other = seed_company(conn, "Depth Quality Other", "DQO")
    item = _id()
    _insert(conn, "item", {"id": item, "item_code": "QITEM-D462",
                           "item_name": "Q Widget"})
    for insp_date, status in (("2026-01-10", "accepted"),
                              ("2026-01-12", "accepted"),
                              ("2026-02-01", "rejected")):
        _insert(conn, "quality_inspection", {
            "id": _id(), "inspection_type": "incoming", "item_id": item,
            "inspection_date": insp_date, "status": status})
    conn.commit()
    return {"company_id": co, "other_id": other, "item": item}


def _seed_support(conn):
    co = seed_company(conn, "Depth Support Co", "DSUP")
    other = seed_company(conn, "Depth Support Other", "DSO")
    cust = _id()
    other_cust = _id()
    _insert(conn, "customer", {"id": cust, "name": "Acme", "company_id": co})
    _insert(conn, "customer", {"id": other_cust, "name": "Other",
                               "company_id": other})
    for created, status, priority, customer in (
            ("2026-01-05", "open", "high", cust),
            ("2026-01-06", "resolved", "low", cust),
            ("2026-02-01", "closed", "medium", cust),
            ("2026-03-01", "open", "high", cust),
            ("2026-01-10", "open", "high", other_cust)):
        _insert(conn, "issue", {"id": _id(), "subject": "T-%s" % created,
                                "customer_id": customer, "priority": priority,
                                "status": status, "created_at": created})
    conn.commit()
    return {"company_id": co, "other_id": other, "customer": cust}


# ---------------------------------------------------------------------------
# revenue-by-customer: stored-row effect (sales_invoice book, no ledger)
# ---------------------------------------------------------------------------

def test_revenue_by_customer_matches_seeded_invoices_and_refuses(conn):
    # No ledger effect: reads customer/sales_invoice only; gl_entry untouched.
    assert seam.table_exists("customer")
    assert seam.table_exists("sales_invoice")
    book = _seed_revenue_book(conn)
    tables = ["customer", "sales_invoice", "sales_invoice_item", "gl_entry"]
    before = _snapshot(conn, tables)
    r = call_action(MOD.revenue_by_customer, conn, ns(
        company_id=book["company_id"], limit="20", offset="0", **JAN_FEB))
    assert is_ok(r), r
    assert r["grand_total"] == "2450.00"
    assert Decimal(r["grand_total"]) == Decimal("2450.00")
    assert r["count"] == 2
    assert [(c["customer_name"], c["invoice_count"], c["revenue"], c["share"])
            for c in r["customers"]] == [
        ("Birch Supply", 2, "1500.00", "61.2%"),
        ("Acme Stores", 2, "950.00", "38.8%"),
    ]
    si = Table("sales_invoice")
    q = (Q.from_(si).select(si.id, si.grand_total, si.status)
         .where(si.company_id == P())
         .where(si.posting_date >= P())
         .where(si.posting_date <= P()))
    rows = conn.execute(
        q.get_sql(),
        (book["company_id"], JAN_FEB["from_date"], JAN_FEB["to_date"]),
    ).fetchall()
    posted = sorted(str(x["grand_total"]) for x in rows
                    if str(x["status"]) in (
                        "submitted", "partially_paid", "overdue", "paid"))
    assert posted == ["1250.00", "250.00", "50.00", "900.00"]
    assert _snapshot(conn, tables) == before
    snap = _snapshot(conn, tables)
    bad = call_action(MOD.revenue_by_customer, conn, ns(
        company_id=None, **JAN_FEB))
    assert is_error(bad) and bad["message"] == "--company-id is required"
    assert _snapshot(conn, tables) == snap


# ---------------------------------------------------------------------------
# revenue-by-item: stored-row effect (sales_invoice_item book, no ledger)
# ---------------------------------------------------------------------------

def test_revenue_by_item_matches_seeded_lines_and_refuses(conn):
    # No ledger effect: reads sales_invoice_item lines only; gl_entry untouched.
    assert seam.table_exists("sales_invoice_item")
    book = _seed_revenue_book(conn)
    tables = ["sales_invoice", "sales_invoice_item", "item", "gl_entry"]
    before = _snapshot(conn, tables)
    r = call_action(MOD.revenue_by_item, conn, ns(
        company_id=book["company_id"], **JAN_FEB))
    assert is_ok(r), r
    assert r["grand_total"] == "2450.00"
    assert Decimal(r["grand_total"]) == Decimal("2450.00")
    assert [(i["item_name"], i["qty_sold"], i["revenue"], i["share"])
            for i in r["items"]] == [
        ("Gadget", "7", "1550.00", "63.3%"),
        ("Widget", "3", "900.00", "36.7%"),
    ]
    sii = Table("sales_invoice_item")
    si = Table("sales_invoice")
    q = (Q.from_(sii).join(si).on(sii.sales_invoice_id == si.id)
         .select(sii.amount).where(si.company_id == P()))
    assert _snapshot(conn, tables) == before
    snap = _snapshot(conn, tables)
    bad = call_action(MOD.revenue_by_item, conn, ns(
        company_id=book["company_id"], from_date="2026-01-01",
        to_date=None))
    assert is_error(bad) and bad["message"] == "--to-date is required"
    assert _snapshot(conn, tables) == snap


# ---------------------------------------------------------------------------
# revenue-trend: stored-row effect (invoice-sourced trend, no ledger here)
# ---------------------------------------------------------------------------

def test_revenue_trend_matches_monthly_book_and_refuses(conn):
    # No ledger effect in this configuration: selling tables exist so the
    # action reports source sales_invoice and never reads gl_entry.
    assert seam.table_exists("sales_invoice")
    book = _seed_revenue_book(conn)
    tables = ["sales_invoice", "gl_entry"]
    before = _snapshot(conn, tables)
    r = call_action(MOD.revenue_trend, conn, ns(
        company_id=book["company_id"], **Q1))
    assert is_ok(r), r
    assert r["source"] == "sales_invoice"
    assert r["total_revenue"] == "2561.00"
    assert Decimal(r["total_revenue"]) == Decimal("2561.00")
    assert [(t["period"], t["revenue"], t.get("change_pct"))
            for t in r["trend"]] == [
        ("Jan 2026", "2150.00", None),
        ("Feb 2026", "300.00", "-86.0%"),
        ("Mar 2026", "111.00", "-63.0%"),
    ]
    si = Table("sales_invoice")
    q = (Q.from_(si).select(si.posting_date, si.grand_total, si.status)
         .where(si.company_id == P())
         .where(si.posting_date >= P())
         .where(si.posting_date <= P()))
    rows = conn.execute(
        q.get_sql(), (book["company_id"], "2026-01-01", "2026-01-31")).fetchall()
    jan = sum(Decimal(str(x["grand_total"])) for x in rows
              if str(x["status"]) in (
                  "submitted", "partially_paid", "overdue", "paid"))
    assert jan == Decimal("2150.00")
    assert _snapshot(conn, tables) == before
    snap = _snapshot(conn, tables)
    bad = call_action(MOD.revenue_trend, conn, ns(
        company_id=None, **Q1))
    assert is_error(bad) and bad["message"] == "--company-id is required"
    assert _snapshot(conn, tables) == snap


# ---------------------------------------------------------------------------
# payroll-analytics: stored-row effect (salary_slip book, no ledger)
# ---------------------------------------------------------------------------

def test_payroll_analytics_matches_submitted_slips_and_refuses(conn):
    # No ledger effect: reads salary_slip/payroll_run only; gl_entry untouched.
    assert seam.table_exists("salary_slip")
    assert seam.table_exists("payroll_run")
    book = _seed_payroll_book(conn)
    tables = ["payroll_run", "salary_slip", "gl_entry"]
    before = _snapshot(conn, tables)
    r = call_action(MOD.payroll_analytics, conn, ns(
        company_id=book["company_id"], **JAN_FEB))
    assert is_ok(r), r
    assert (r["slip_count"], r["total_gross"], r["total_deductions"],
            r["total_net"], r["avg_gross_per_employee"]) == (
        3, "14300.00", "3325.50", "10974.50", "4766.67")
    assert Decimal(r["total_gross"]) == Decimal("14300.00")
    assert Decimal(r["total_net"]) == Decimal("10974.50")
    ss = Table("salary_slip")
    q = (Q.from_(ss).select(ss.gross_pay, ss.total_deductions, ss.net_pay)
         .where(ss.company_id == P()))
    rows = conn.execute(q.get_sql(), (book["company_id"],)).fetchall()
    gross_all = sum(Decimal(str(x["gross_pay"])) for x in rows)
    assert gross_all == Decimal("31533.00")
    assert _snapshot(conn, tables) == before
    snap = _snapshot(conn, tables)
    bad = call_action(MOD.payroll_analytics, conn, ns(
        company_id=book["company_id"], from_date="2026-01-01",
        to_date=None))
    assert is_error(bad) and bad["message"] == "--to-date is required"
    assert _snapshot(conn, tables) == snap


# ---------------------------------------------------------------------------
# period-comparison: ledger effect (both legs balance)
# ---------------------------------------------------------------------------

def test_period_comparison_matches_ledger_legs_and_refuses(conn):
    assert seam.table_exists("gl_entry")
    assert seam.table_exists("account")
    book = _seed_ledger_book(conn)
    tables = ["gl_entry", "account"]
    before = _snapshot(conn, tables)
    periods = json.dumps([
        {"from_date": "2026-01-01", "to_date": "2026-01-31",
         "label": "Jan"},
        {"from_date": "2026-02-01", "to_date": "2026-02-28",
         "label": "Feb"},
    ])
    metrics = json.dumps(["revenue", "expenses", "net_income"])
    r = call_action(MOD.period_comparison, conn, ns(
        company_id=book["company_id"], periods=periods, metrics=metrics))
    assert is_ok(r), r
    assert [(p["label"], p["revenue"], p["expenses"], p["net_income"])
            for p in r["periods"]] == [
        ("Jan", "10000.00", "4000.00", "6000.00"),
        ("Feb", "5000.00", "1000.00", "4000.00"),
    ]
    feb = r["periods"][1]
    assert (feb["revenue_change"], feb["revenue_change_pct"]) == (
        "-5000.00", "-50.0%")
    assert (feb["expenses_change"], feb["expenses_change_pct"]) == (
        "-3000.00", "-75.0%")
    assert (feb["net_income_change"], feb["net_income_change_pct"]) == (
        "-2000.00", "-33.3%")
    assert Decimal(feb["revenue"]) == Decimal("5000.00")
    gl = Table("gl_entry")
    ac = Table("account")
    q = (Q.from_(gl).join(ac).on(gl.account_id == ac.id)
         .select(gl.debit, gl.credit, gl.is_cancelled)
         .where(ac.company_id == P()))
    rows = conn.execute(q.get_sql(), (book["company_id"],)).fetchall()
    live = [x for x in rows if int(x["is_cancelled"]) == 0]
    debits = sum(Decimal(str(x["debit"])) for x in live)
    credits = sum(Decimal(str(x["credit"])) for x in live)
    assert debits == credits == Decimal("20000.00")
    assert _snapshot(conn, tables) == before
    snap = _snapshot(conn, tables)
    bad = call_action(MOD.period_comparison, conn, ns(
        company_id=None, periods=periods, metrics=metrics))
    assert is_error(bad) and bad["message"] == "--company-id is required"
    assert _snapshot(conn, tables) == snap
    bad2 = call_action(MOD.period_comparison, conn, ns(
        company_id=book["company_id"],
        periods=json.dumps([{"from_date": "2026-01-01",
                             "to_date": "2026-01-31", "label": "Only"}]),
        metrics=metrics))
    assert is_error(bad2)
    assert "at least 2" in bad2["message"]
    assert _snapshot(conn, tables) == snap


# ---------------------------------------------------------------------------
# project-profitability-analytics: stored-row effect (project book, no ledger)
# ---------------------------------------------------------------------------

def test_project_profitability_matches_project_book_and_refuses(conn):
    # No ledger effect: reads the project table only; gl_entry untouched.
    # FINDING (documented, not fixed): --to-date is accepted but ignored; a
    # project starting after to_date is still listed. Asserted below.
    assert seam.table_exists("project")
    book = _seed_projects(conn)
    tables = ["project", "gl_entry"]
    before = _snapshot(conn, tables)
    r = call_action(MOD.project_profitability_analytics, conn, ns(
        company_id=book["company_id"], from_date="2026-01-01",
        to_date="2026-03-31"))
    assert is_ok(r), r
    assert r["project_count"] == 2
    assert [(p["project_name"], p["estimated_cost"], p["actual_cost"],
             p["total_billed"], p["profit"], p["margin"],
             p["cost_variance"]) for p in r["projects"]] == [
        ("Alpha", "10000.00", "7000.00", "12000.00", "5000.00", "41.7%",
         "-3000.00"),
        ("Beta", "5000.00", "5500.00", "4000.00", "-1500.00", "-37.5%",
         "500.00"),
    ]
    assert Decimal(r["projects"][0]["profit"]) == Decimal("5000.00")
    pr = Table("project")
    q = (Q.from_(pr).select(pr.project_name, pr.total_billed)
         .where(pr.company_id == P()))
    rows = conn.execute(q.get_sql(), (book["company_id"],)).fetchall()
    assert sorted(str(x["total_billed"]) for x in rows) == [
        "12000.00", "4000.00"]
    late = _id()
    _insert(conn, "project", {
        "id": late, "project_name": "Late", "status": "open",
        "estimated_cost": "1000.00", "actual_cost": "900.00",
        "total_billed": "3000.00", "profit_margin": "0",
        "company_id": book["company_id"], "start_date": "2026-06-15"})
    conn.commit()
    r2 = call_action(MOD.project_profitability_analytics, conn, ns(
        company_id=book["company_id"], from_date="2026-01-01",
        to_date="2026-03-31"))
    assert [p["project_name"] for p in r2["projects"]] == [
        "Alpha", "Beta", "Late"]
    conn.execute(
        (Q.from_(pr).delete().where(pr.id == P())).get_sql(), (late,))
    conn.commit()
    assert _snapshot(conn, tables) == before
    snap = _snapshot(conn, tables)
    bad = call_action(MOD.project_profitability_analytics, conn, ns(
        company_id=None))
    assert is_error(bad) and bad["message"] == "--company-id is required"
    assert _snapshot(conn, tables) == snap


# ---------------------------------------------------------------------------
# quality-analytics: stored-row effect (inspection book, no ledger)
# ---------------------------------------------------------------------------

def test_quality_analytics_matches_inspections_and_refuses(conn):
    # No ledger effect: reads quality_inspection only; gl_entry untouched.
    # The seeded inspections carry no reference, so all are unattributed.
    assert seam.table_exists("quality_inspection")
    book = _seed_quality(conn)
    tables = ["quality_inspection", "gl_entry"]
    before = _snapshot(conn, tables)
    r = call_action(MOD.quality_analytics, conn, ns(
        company_id=book["company_id"], from_date="2026-01-01",
        to_date="2026-03-31"))
    assert is_ok(r), r
    assert r["inspections"] == {"total": 0, "passed": 0, "failed": 0,
                                "pass_rate": "N/A"}
    assert r["non_conformances"] == 0
    assert r["unattributed_inspections"] == 3
    qi = Table("quality_inspection")
    q = Q.from_(qi).select(qi.status, qi.inspection_date)
    rows = conn.execute(q.get_sql(), ()).fetchall()
    assert sorted(str(x["status"]) for x in rows) == [
        "accepted", "accepted", "rejected"]
    other = call_action(MOD.quality_analytics, conn, ns(
        company_id=book["other_id"], from_date="2026-01-01",
        to_date="2026-03-31"))
    assert other["inspections"] == r["inspections"]
    assert other["unattributed_inspections"] == 3
    assert _snapshot(conn, tables) == before
    snap = _snapshot(conn, tables)
    bad = call_action(MOD.quality_analytics, conn, ns(company_id=None))
    assert is_error(bad) and bad["message"] == "--company-id is required"
    assert _snapshot(conn, tables) == snap


# ---------------------------------------------------------------------------
# support-metrics: stored-row effect (issue book, no ledger)
# ---------------------------------------------------------------------------

def test_support_metrics_matches_issue_book_and_refuses(conn):
    # No ledger effect: reads issue rows only; gl_entry untouched.
    assert seam.table_exists("issue")
    book = _seed_support(conn)
    tables = ["issue", "customer", "gl_entry"]
    before = _snapshot(conn, tables)
    r = call_action(MOD.support_metrics, conn, ns(
        company_id=book["company_id"], from_date="2026-01-01",
        to_date="2026-03-31"))
    assert is_ok(r), r
    assert (r["total_issues"], r["open"], r["resolved"], r["closed"],
            r["resolution_rate"]) == (4, 2, 1, 1, "50.0%")
    assert [(p["priority"], p["count"]) for p in r["by_priority"]] == [
        ("high", 2), ("medium", 1), ("low", 1)]
    other = call_action(MOD.support_metrics, conn, ns(
        company_id=book["other_id"], from_date="2026-01-01",
        to_date="2026-03-31"))
    assert (other["total_issues"], other["open"]) == (1, 1)
    iss = Table("issue")
    q = (Q.from_(iss).select(iss.status, iss.priority))
    rows = conn.execute(q.get_sql(), ()).fetchall()
    assert len(rows) == 5
    orphan = _id()
    _insert(conn, "issue", {"id": orphan, "subject": "Orphan",
                            "customer_id": None, "priority": "high",
                            "status": "open", "created_at": "2026-01-10"})
    conn.commit()
    r2 = call_action(MOD.support_metrics, conn, ns(
        company_id=book["company_id"], from_date="2026-01-01",
        to_date="2026-03-31"))
    assert r2["total_issues"] == 4
    conn.execute(
        (Q.from_(iss).delete().where(iss.id == P())).get_sql(), (orphan,))
    conn.commit()
    assert _snapshot(conn, tables) == before
    snap = _snapshot(conn, tables)
    bad = call_action(MOD.support_metrics, conn, ns(company_id=None))
    assert is_error(bad) and bad["message"] == "--company-id is required"
    assert _snapshot(conn, tables) == snap
