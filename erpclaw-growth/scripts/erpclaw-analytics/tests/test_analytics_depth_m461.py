"""M461 depth: behavioural evidence for 12 erpclaw-analytics actions.

Each action below already had a test that proved the wrong thing (response
shape in test_analytics.py, or rank/total math without a no-write proof in
test_revenue_cost_payroll_behaviour.py). Each test here proves the database
effect instead: the exact computed values derived from seeded rows, the rows
read back through the seam (PyPika queries over the test connection) and
compared exactly, and the proof that nothing was written (snapshot equality
before/after success and before/after refusal).

Money is TEXT: exact string comparisons, Decimal for arithmetic, never float,
never approximate, never rounded in-test.

Per-action depth signal (erpclaw-analytics owns NO tables and posts NO
vouchers: every action is READ-ONLY, so no success test below asserts a new
ledger leg; where a figure is derived from ledger rows the seeded voucher
legs are asserted to balance instead):
- analyze-query-performance .... READ-ONLY catalog scan (pins plan names and
                                 seam table count; ledgers untouched)
- company-scorecard ............ READ-ONLY grades from GL balances (no ledger
                                 effect; asserts exact grades + legs balance)
- cost-trend ................... READ-ONLY period totals from GL (no ledger
                                 effect; asserts exact totals + legs balance)
- customer-concentration ....... READ-ONLY shares from invoices (no ledger
                                 effect; asserts exact shares + invoices
                                 read back; draft invoices excluded)
- efficiency-ratios ............ READ-ONLY ratios from GL (no ledger effect;
                                 asserts exact ratios + legs balance)
- executive-dashboard .......... READ-ONLY section roll-up (no ledger effect;
                                 asserts every section exactly)
- expense-breakdown ............ READ-ONLY grouped totals from GL (no ledger
                                 effect; asserts both group-bys + legs
                                 balance; cancelled rows excluded)
- headcount-analytics .......... READ-ONLY census from employee rows (no
                                 ledger effect; asserts exact census)
- inventory-turnover ........... READ-ONLY ratio from GL stock/cogs accounts
                                 (no ledger effect; asserts exact ratio)
                                 [FINDING-m461-3: item/warehouse filters
                                 accepted but ignored]
- leave-utilization ............ READ-ONLY sums from allocation/application
                                 rows (no ledger effect; asserts exact sums;
                                 rejected/out-of-range rows excluded)
- metric-trend ................. READ-ONLY series from GL (no ledger effect;
                                 asserts exact series)
                                 [FINDING-m461-2: valid headcount metric
                                 missing from unknown-metric message]
- opex-vs-capex ................ READ-ONLY split from GL (no ledger effect;
                                 asserts exact split + legs balance)

Ledger note: none of these twelve actions posts to the general (or stock)
ledger on its success path -- the module header states it owns no tables --
so no success test below asserts a new ledger leg. Tests over GL-seeded
books assert every seeded voucher's legs balance (sum of debits equals sum
of credits per voucher) and pin the gl_entry/stock_ledger_entry snapshot
unchanged, so a later reader does not add a leg assertion that cannot hold.
"""
import uuid
from decimal import Decimal

from analytics_helpers import (call_action, is_error, is_ok, load_db_query,
                               ns, seed_accounts, seed_company)
from erpclaw_lib import seam
from erpclaw_lib.query import Field, P, Q, Table, insert_row

MOD = load_db_query()

JAN_FEB = {"from_date": "2026-01-01", "to_date": "2026-02-28"}
Q1 = {"from_date": "2026-01-01", "to_date": "2026-03-31"}

_SNAPSHOT_TABLES = (
    "company", "account", "cost_center", "gl_entry", "customer",
    "sales_invoice", "sales_invoice_item", "item", "warehouse",
    "stock_ledger_entry", "employee", "department", "leave_type",
    "leave_allocation", "leave_application", "payroll_run", "salary_slip",
)


def _id():
    return str(uuid.uuid4())


def _insert(conn, table, row):
    sql, _ = insert_row(table, {k: P() for k in row})
    conn.execute(sql, tuple(row.values()))


def _snapshot(conn):
    """Byte-level dump of every table these read-only actions could touch."""
    snap = {}
    for table in _SNAPSHOT_TABLES:
        tbl = Table(table)
        query = Q.from_(tbl).select("*").orderby(Field("id"))
        snap[table] = [tuple(r) for r in
                       conn.execute(query.get_sql(), ()).fetchall()]
    return snap


def _post(conn, posting_date, debit_account, credit_account, amount,
          cost_center=None, cancelled=0):
    """Write one balanced two-leg voucher; returns the voucher id."""
    voucher = _id()
    _insert(conn, "gl_entry", {
        "id": _id(), "posting_date": posting_date, "account_id": debit_account,
        "debit": amount, "credit": "0", "voucher_type": "journal_entry",
        "voucher_id": voucher, "cost_center_id": cost_center,
        "is_cancelled": cancelled})
    _insert(conn, "gl_entry", {
        "id": _id(), "posting_date": posting_date, "account_id": credit_account,
        "debit": "0", "credit": amount, "voucher_type": "journal_entry",
        "voucher_id": voucher, "cost_center_id": None,
        "is_cancelled": cancelled})
    return voucher


def _gl_rows(conn, company_id):
    """Every GL row behind one company's accounts, read back through the seam."""
    gl = Table("gl_entry")
    ac = Table("account")
    query = (Q.from_(gl).join(ac).on(gl.account_id == ac.id)
             .select(gl.voucher_id, gl.account_id, gl.posting_date,
                     gl.debit, gl.credit, gl.cost_center_id, gl.is_cancelled)
             .where(ac.company_id == P()))
    return conn.execute(query.get_sql(), (company_id,)).fetchall()


def _assert_vouchers_balance(conn, company_id):
    """Both legs of every seeded voucher balance; returns the live rows."""
    rows = [r for r in _gl_rows(conn, company_id) if r["is_cancelled"] == 0]
    assert rows, "setup must seed at least one live voucher"
    by_voucher = {}
    for row in rows:
        by_voucher.setdefault(row["voucher_id"], []).append(row)
    for voucher, legs in by_voucher.items():
        debits = sum(Decimal(str(leg["debit"])) for leg in legs)
        credits = sum(Decimal(str(leg["credit"])) for leg in legs)
        assert debits == credits, voucher
    return rows


# ---------------------------------------------------------------------------
# analyze-query-performance: READ-ONLY catalog scan
# ---------------------------------------------------------------------------

class TestAnalyzeQueryPerformanceDepth:
    def test_plan_names_and_table_count_with_no_writes(self, conn, db_path):
        # This action does NOT reach the ledger: it runs EXPLAIN QUERY PLAN
        # over fixed probe queries and counts catalog objects. Both ledger
        # snapshots are pinned unchanged below.
        before = _snapshot(conn)
        r = call_action(MOD.analyze_query_performance, conn, ns())
        assert is_ok(r), r
        assert (r["total_queries_analyzed"], r["queries_using_index"],
                r["full_table_scans"]) == (7, 7, 0)
        assert r["index_utilization_pct"] == 100.0
        # The three authorization envelope tables add three tables and four
        # automatic indexes (envelope primary key and unique key, result
        # primary key, usage primary key). The intercompany_account_map
        # table adds one table and two automatic indexes (primary key and
        # unique key).
        assert r["total_tables"] == 260
        assert r["total_indexes"] == 739
        assert r["full_scan_queries"] == []
        assert r["recommendations"] == []
        assert [e["query"] for e in r["query_plans"]] == [
            "gl_entry_by_account_date", "account_by_company",
            "account_by_root_type", "stock_ledger_by_item", "customer_list",
            "supplier_list", "employee_list",
        ]
        for entry in r["query_plans"]:
            assert entry["uses_index"] is True
            assert entry["full_scan"] is False
        # Read back through the seam: the catalog count is the seam's count.
        assert r["total_tables"] == len(seam.table_names(db_path))
        assert _snapshot(conn) == before

    def test_takes_no_input_so_there_is_no_refusal_to_probe(self, conn):
        # The action reads no args at all (not even company_id): unknown and
        # malformed inputs are ignored rather than refused. There is no
        # refusal case to assert; the database must still be identical.
        before = _snapshot(conn)
        r = call_action(MOD.analyze_query_performance, conn, ns(
            company_id="m461-ghost", from_date="not-a-date"))
        assert is_ok(r), r
        # 260 tables, matching the pin above.
        assert r["total_tables"] == 260
        assert _snapshot(conn) == before


# ---------------------------------------------------------------------------
# company-scorecard: READ-ONLY grades
# ---------------------------------------------------------------------------

class TestCompanyScorecardDepth:
    def test_grades_derive_from_seeded_balances_and_refuses_without_writing(
            self, conn):
        # This action does NOT reach the ledger: it grades GL balances. The
        # seeded voucher legs are asserted to balance; gl counts are pinned.
        co = seed_company(conn, "Score Co", "SCC")
        acc = seed_accounts(conn, co)
        _post(conn, "2026-01-15", acc["cash"], acc["revenue"], "20000.00")
        _post(conn, "2026-01-20", acc["expense"], acc["cash"], "5000.00")
        _post(conn, "2026-01-25", acc["cash"], acc["payable"], "4000.00")
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.company_scorecard, conn, ns(
            company_id=co, as_of_date="2026-03-31"))
        assert is_ok(r), r
        assert r["as_of_date"] == "2026-03-31"
        assert r["period"] == {"from_date": "2026-01-01",
                               "to_date": "2026-03-31"}
        assert r["overall_grade"] == "A"
        assert r["dimensions"]["liquidity"] == {"grade": "A", "ratio": "4.75"}
        assert r["dimensions"]["profitability"] == {
            "grade": "A", "net_margin": "75.0%"}
        assert r["dimensions"]["collections"] == {
            "grade": "A", "ar_to_revenue": "0.0%"}
        assert r["dimensions"]["workforce"]["grade"] == "N/A"
        # Read back through the seam: cash 24000 debit less 5000 credit.
        rows = _assert_vouchers_balance(conn, co)
        cash = [x for x in rows if x["account_id"] == acc["cash"]]
        assert (sum(Decimal(str(x["debit"])) for x in cash)
                - sum(Decimal(str(x["credit"])) for x in cash)) == Decimal(
                    "19000.00")
        assert _snapshot(conn) == before

        bad = call_action(MOD.company_scorecard, conn, ns(
            company_id=None, as_of_date="2026-03-31"))
        assert is_error(bad)
        assert bad["message"] == "--company-id is required"
        assert _snapshot(conn) == before, "a refused call must write nothing"

    def test_receivable_debit_grades_collections(self, conn):
        # Receivable is debit-normal: DR Receivable on invoices, so the
        # balance reads debit minus credit. Before the fix this graded
        # {"grade": "A", "ar_to_revenue": "-16.7%"} (sign-flipped balance).
        co = seed_company(conn, "Recv Score Co", "RSC")
        acc = seed_accounts(conn, co)
        _post(conn, "2026-01-10", acc["cash"], acc["revenue"], "9000.00")
        _post(conn, "2026-02-05", acc["receivable"], acc["revenue"],
              "1800.00")
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.company_scorecard, conn, ns(
            company_id=co, as_of_date="2026-03-31"))
        assert is_ok(r), r
        assert r["dimensions"]["collections"] == {
            "grade": "B", "ar_to_revenue": "16.7%"}
        assert r["dimensions"]["profitability"] == {
            "grade": "A", "net_margin": "100.0%"}
        assert r["overall_grade"] == "A"
        _assert_vouchers_balance(conn, co)
        assert _snapshot(conn) == before


# ---------------------------------------------------------------------------
# cost-trend: READ-ONLY period totals
# ---------------------------------------------------------------------------

class TestCostTrendDepth:
    def test_monthly_totals_exclude_cancelled_and_refuses_without_writing(
            self, conn):
        # This action does NOT reach the ledger: it totals expense postings.
        # The cancelled 999.99 pair must not move any total.
        co = seed_company(conn, "Trend Co", "CTC")
        acc = seed_accounts(conn, co)
        _post(conn, "2026-01-12", acc["expense"], acc["cash"], "1000.00")
        _post(conn, "2026-02-03", acc["expense"], acc["cash"], "500.00")
        _post(conn, "2026-02-10", acc["expense"], acc["cash"], "999.99",
              cancelled=1)
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.cost_trend, conn, ns(company_id=co, **Q1))
        assert is_ok(r), r
        assert (r["total"], r["average"], r["account_id"]) == (
            "1500.00", "500.00", None)
        assert [(t["period"], t["amount"], t.get("change_pct"))
                for t in r["periods"]] == [
            ("Jan 2026", "1000.00", None),
            ("Feb 2026", "500.00", "-50.0%"),
            ("Mar 2026", "0.00", "-100.0%"),
        ]
        rows = _assert_vouchers_balance(conn, co)
        live = [x for x in rows if x["account_id"] == acc["expense"]]
        assert sum(Decimal(str(x["debit"])) for x in live) == Decimal(
            "1500.00")
        assert _snapshot(conn) == before

        bad = call_action(MOD.cost_trend, conn, ns(company_id=None, **Q1))
        assert is_error(bad)
        assert bad["message"] == "--company-id is required"
        assert _snapshot(conn) == before, "a refused call must write nothing"


# ---------------------------------------------------------------------------
# customer-concentration: READ-ONLY shares
# ---------------------------------------------------------------------------

class TestCustomerConcentrationDepth:
    def test_shares_derive_from_posted_invoices_and_refuses_without_writing(
            self, conn):
        # This action does NOT reach the ledger: it shares posted invoices.
        # The draft 7000.00 invoice must not move any share.
        co = seed_company(conn, "Conc Co", "CNC")
        acme, birch = _id(), _id()
        _insert(conn, "customer", {"id": acme, "name": "Acme Stores",
                                  "company_id": co})
        _insert(conn, "customer", {"id": birch, "name": "Birch Supply",
                                  "company_id": co})
        for cust, date, status, total in (
                (acme, "2026-01-10", "submitted", "900.00"),
                (birch, "2026-01-20", "paid", "1000.00"),
                (birch, "2026-02-05", "overdue", "250.00"),
                (acme, "2026-02-20", "draft", "7000.00")):
            _insert(conn, "sales_invoice", {
                "id": _id(), "customer_id": cust, "posting_date": date,
                "total_amount": total, "grand_total": total,
                "outstanding_amount": total, "status": status,
                "company_id": co})
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.customer_concentration, conn, ns(
            company_id=co, **JAN_FEB))
        assert is_ok(r), r
        assert r["total_revenue"] == "2150.00"
        assert r["customer_count"] == 2
        assert [(c["rank"], c["customer"], c["revenue"], c["share"],
                 c["cumulative_share"]) for c in r["top_customers"]] == [
            (1, "Birch Supply", "1250.00", "58.1%", "58.1%"),
            (2, "Acme Stores", "900.00", "41.9%", "100.0%"),
        ]
        assert r["concentration"]["top_1_share"] == "58.1%"
        assert r["concentration"]["top_5_share"] == "100.0%"
        assert r["concentration"]["top_10_share"] == "100.0%"
        assert r["interpretation"] == (
            "High concentration risk \u2014 top customer accounts "
            "for 58.1% of revenue.")
        # Read back through the seam: the posted invoice rows sum the same.
        si = Table("sales_invoice")
        query = (Q.from_(si).select(si.customer_id, si.grand_total)
                 .where(si.company_id == P()))
        posted = [(x["customer_id"], Decimal(str(x["grand_total"])))
                  for x in conn.execute(query.get_sql(), (co,)).fetchall()
                  if x["customer_id"] in (acme, birch)]
        assert sum(v for _, v in posted) == Decimal("9150.00")
        live = [v for _, v in posted if v != Decimal("7000.00")]
        assert sum(live) == Decimal("2150.00")
        assert _snapshot(conn) == before

        bad = call_action(MOD.customer_concentration, conn, ns(
            company_id=None, **JAN_FEB))
        assert is_error(bad)
        assert bad["message"] == "--company-id is required"
        assert _snapshot(conn) == before, "a refused call must write nothing"


# ---------------------------------------------------------------------------
# efficiency-ratios: READ-ONLY ratios
# ---------------------------------------------------------------------------

class TestEfficiencyRatiosDepth:
    def test_ratios_derive_from_seeded_balances_and_refuses_without_writing(
            self, conn):
        # This action does NOT reach the ledger: it ratios GL balances.
        co = seed_company(conn, "Eff Co", "EFC")
        acc = seed_accounts(conn, co)
        stock = _id()
        _insert(conn, "account", {"id": stock, "name": "Inventory",
                                 "account_number": "1400", "root_type": "asset",
                                 "account_type": "stock", "is_group": 0,
                                 "company_id": co})
        _post(conn, "2026-01-10", acc["cash"], acc["revenue"], "9000.00")
        _post(conn, "2026-01-12", acc["cost_of_goods_sold"], acc["cash"],
              "3000.00")
        _post(conn, "2026-01-15", acc["expense"], acc["payable"], "1500.00")
        _post(conn, "2026-01-18", stock, acc["cash"], "6000.00")
        _post(conn, "2026-02-05", acc["receivable"], acc["revenue"],
              "1800.00")
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.efficiency_ratios, conn, ns(
            company_id=co, from_date="2026-01-01", to_date="2026-02-28"))
        assert is_ok(r), r
        assert r["days_in_period"] == 58
        assert (r["revenue"], r["cogs"]) == ("10800.00", "3000.00")
        # Receivable is debit-normal: invoices post DR Receivable, so the
        # balance reads debit minus credit.
        assert r["ratios"]["ar_balance"] == "1800.00"
        assert r["ratios"]["dso"] == "9.7"
        assert (r["ratios"]["dpo"], r["ratios"]["ap_balance"]) == (
            "29.0", "1500.00")
        assert (r["ratios"]["inventory_turnover_days"],
                r["ratios"]["inventory_balance"]) == ("116.0", "6000.00")
        assert r["ratios"]["asset_turnover"] == "1.38"
        assert "notes" not in r
        _assert_vouchers_balance(conn, co)
        assert _snapshot(conn) == before

        bad = call_action(MOD.efficiency_ratios, conn, ns(
            company_id=None, from_date="2026-01-01", to_date="2026-02-28"))
        assert is_error(bad)
        assert bad["message"] == "--company-id is required"
        assert _snapshot(conn) == before, "a refused call must write nothing"


# ---------------------------------------------------------------------------
# executive-dashboard: READ-ONLY roll-up
# ---------------------------------------------------------------------------

class TestExecutiveDashboardDepth:
    def test_sections_derive_from_seeded_rows_and_refuses_without_writing(
            self, conn):
        # This action does NOT reach the ledger: it rolls posted rows up.
        co = seed_company(conn, "Dash Co", "DSC")
        acc = seed_accounts(conn, co)
        _post(conn, "2026-01-10", acc["cash"], acc["revenue"], "5000.00")
        _post(conn, "2026-01-12", acc["expense"], acc["cash"], "2000.00")
        cust = _id()
        _insert(conn, "customer", {"id": cust, "name": "Solo",
                                  "company_id": co})
        _insert(conn, "sales_invoice", {
            "id": _id(), "customer_id": cust, "posting_date": "2026-01-15",
            "total_amount": "1200.00", "grand_total": "1200.00",
            "outstanding_amount": "1200.00", "status": "submitted",
            "company_id": co})
        _insert(conn, "employee", {
            "id": _id(), "first_name": "Zed", "full_name": "Zed",
            "date_of_joining": "2025-01-01", "company_id": co})
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.executive_dashboard, conn, ns(
            company_id=co, **Q1))
        assert is_ok(r), r
        assert r["sections"]["financial"] == {
            "available": True, "revenue": "5000.00", "expenses": "2000.00",
            "net_income": "3000.00", "net_margin": "60.0%",
            "total_assets": "3000.00", "total_liabilities": "0.00",
        }
        assert r["sections"]["selling"] == {
            "available": True, "invoices": 1, "invoice_total": "1200.00",
            "ar_outstanding": "0.00",
        }
        assert r["sections"]["buying"] == {
            "available": True, "ap_outstanding": "0.00"}
        assert r["sections"]["inventory"] == {
            "available": True, "stock_value": "0.00"}
        assert r["sections"]["hr"] == {
            "available": True, "active_employees": 1}
        assert r["sections"]["support"] == {
            "available": True, "open_issues": 0}
        # Read back through the seam: one invoice row, one active employee.
        si = Table("sales_invoice")
        query = (Q.from_(si).select(si.grand_total)
                 .where(si.company_id == P()))
        assert [str(x["grand_total"]) for x in
                conn.execute(query.get_sql(), (co,)).fetchall()] == ["1200.00"]
        emp = Table("employee")
        query = (Q.from_(emp).select(emp.id)
                 .where(emp.company_id == P()))
        assert len(conn.execute(query.get_sql(), (co,)).fetchall()) == 1
        _assert_vouchers_balance(conn, co)
        assert _snapshot(conn) == before

        bad = call_action(MOD.executive_dashboard, conn, ns(
            company_id=None, **Q1))
        assert is_error(bad)
        assert bad["message"] == "--company-id is required"
        assert _snapshot(conn) == before, "a refused call must write nothing"

    def test_receivable_debit_reads_positive(self, conn):
        # Receivable is debit-normal: DR Receivable on invoices, so the
        # balance reads debit minus credit. Before the fix ar_outstanding
        # came back "-1800.00" for a live 1800.00 debit.
        co = seed_company(conn, "Recv Dash Co", "RDC")
        acc = seed_accounts(conn, co)
        _post(conn, "2026-01-10", acc["cash"], acc["revenue"], "9000.00")
        _post(conn, "2026-02-05", acc["receivable"], acc["revenue"],
              "1800.00")
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.executive_dashboard, conn, ns(
            company_id=co, from_date="2026-01-01", to_date="2026-03-31"))
        assert is_ok(r), r
        assert r["sections"]["selling"]["ar_outstanding"] == "1800.00"
        assert r["sections"]["financial"]["revenue"] == "10800.00"
        assert r["sections"]["financial"]["total_assets"] == "10800.00"
        _assert_vouchers_balance(conn, co)
        assert _snapshot(conn) == before


# ---------------------------------------------------------------------------
# expense-breakdown: READ-ONLY grouped totals
# ---------------------------------------------------------------------------

class TestExpenseBreakdownDepth:
    def test_both_group_bys_exclude_cancelled_and_refuse_without_writing(
            self, conn):
        # This action does NOT reach the ledger: it groups expense postings.
        # The cancelled 300.00 pair and the 100.00 refund shape the totals.
        co = seed_company(conn, "Exp Co", "EXC")
        acc = seed_accounts(conn, co)
        travel = _id()
        _insert(conn, "account", {"id": travel, "name": "Travel",
                                 "account_number": "6100",
                                 "root_type": "expense",
                                 "account_type": "expense", "is_group": 0,
                                 "company_id": co})
        ops, sales = _id(), _id()
        _insert(conn, "cost_center", {"id": ops, "name": "Ops",
                                     "company_id": co})
        _insert(conn, "cost_center", {"id": sales, "name": "Sales",
                                     "company_id": co})
        _post(conn, "2026-01-12", acc["expense"], acc["cash"], "600.00", ops)
        _post(conn, "2026-01-18", acc["cost_of_goods_sold"], acc["cash"],
              "1250.00", sales)
        _post(conn, "2026-02-03", travel, acc["cash"], "450.25")
        _post(conn, "2026-02-10", acc["expense"], acc["cash"], "300.00", ops,
              cancelled=1)
        voucher = _id()
        _insert(conn, "gl_entry", {
            "id": _id(), "posting_date": "2026-02-14",
            "account_id": acc["expense"], "debit": "0", "credit": "100.00",
            "voucher_type": "journal_entry", "voucher_id": voucher,
            "cost_center_id": ops, "is_cancelled": 0})
        _insert(conn, "gl_entry", {
            "id": _id(), "posting_date": "2026-02-14",
            "account_id": acc["cash"], "debit": "100.00", "credit": "0",
            "voucher_type": "journal_entry", "voucher_id": voucher,
            "cost_center_id": None, "is_cancelled": 0})
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.expense_breakdown, conn, ns(
            company_id=co, group_by="account", **JAN_FEB))
        assert is_ok(r), r
        assert r["total_expenses"] == "2200.25"
        assert r["count"] == 3
        assert [(b["name"], b["account_number"], b["amount"], b["percentage"])
                for b in r["breakdown"]] == [
            ("COGS", "5000", "1250.00", "56.8%"),
            ("Operating Expenses", "6000", "500.00", "22.7%"),
            ("Travel", "6100", "450.25", "20.5%"),
        ]
        cc = call_action(MOD.expense_breakdown, conn, ns(
            company_id=co, group_by="cost_center", **JAN_FEB))
        assert cc["total_expenses"] == "2200.25"
        assert [(b["name"], b["amount"], b["percentage"])
                for b in cc["breakdown"]] == [
            ("Sales", "1250.00", "56.8%"),
            ("Ops", "500.00", "22.7%"),
            ("Unassigned", "450.25", "20.5%"),
        ]
        _assert_vouchers_balance(conn, co)
        assert _snapshot(conn) == before

        bad = call_action(MOD.expense_breakdown, conn, ns(
            company_id=co, group_by="department", **JAN_FEB))
        assert is_error(bad)
        assert bad["message"] == "--group-by must be 'account' or 'cost_center'"
        assert _snapshot(conn) == before, "a refused call must write nothing"


# ---------------------------------------------------------------------------
# headcount-analytics: READ-ONLY census
# ---------------------------------------------------------------------------

class TestHeadcountAnalyticsDepth:
    def test_census_counts_active_only_and_refuses_without_writing(self, conn):
        # This action does NOT reach the ledger: it counts employee rows.
        co = seed_company(conn, "Head Co", "HDC")
        eng = _id()
        _insert(conn, "department", {"id": eng, "name": "Engineering",
                                    "company_id": co})

        def hire(first, dept, status, doj):
            _insert(conn, "employee", {
                "id": _id(), "first_name": first, "full_name": first,
                "date_of_joining": doj, "status": status,
                "department_id": dept, "company_id": co})

        hire("Ann", eng, "active", "2025-06-01")
        hire("Ben", eng, "active", "2026-01-15")
        hire("Cal", None, "active", "2025-03-01")
        hire("Dee", eng, "inactive", "2025-01-01")
        hire("Eve", eng, "active", "2026-05-01")
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.headcount_analytics, conn, ns(
            company_id=co, as_of_date="2026-03-31"))
        assert is_ok(r), r
        assert r["group_by"] == "department"
        assert r["total_headcount"] == 3
        assert [(b["name"], b["count"], b["share"])
                for b in r["breakdown"]] == [
            ("Engineering", 2, "66.7%"),
            ("Unassigned", 1, "33.3%"),
        ]
        # Read back through the seam: the same three active, joined rows.
        emp = Table("employee")
        query = (Q.from_(emp).select(emp.first_name, emp.status,
                                     emp.date_of_joining)
                 .where(emp.company_id == P()))
        rows = conn.execute(query.get_sql(), (co,)).fetchall()
        live = sorted(x["first_name"] for x in rows
                      if x["status"] == "active"
                      and x["date_of_joining"] <= "2026-03-31")
        assert live == ["Ann", "Ben", "Cal"]
        assert _snapshot(conn) == before

        bad = call_action(MOD.headcount_analytics, conn, ns(
            company_id=None, as_of_date="2026-03-31"))
        assert is_error(bad)
        assert bad["message"] == "--company-id is required"
        assert _snapshot(conn) == before, "a refused call must write nothing"


# ---------------------------------------------------------------------------
# inventory-turnover: READ-ONLY ratio
# ---------------------------------------------------------------------------

class TestInventoryTurnoverDepth:
    def test_ratio_uses_stock_and_cogs_accounts_and_refuses_without_writing(
            self, conn):
        # This action does NOT reach the ledger: it ratios GL stock/cogs.
        co = seed_company(conn, "Turn Co", "TRC")
        acc = seed_accounts(conn, co)
        stock = _id()
        _insert(conn, "account", {"id": stock, "name": "Inventory",
                                 "account_number": "1400", "root_type": "asset",
                                 "account_type": "stock", "is_group": 0,
                                 "company_id": co})
        _post(conn, "2025-12-20", stock, acc["cash"], "4000.00")
        _post(conn, "2026-02-10", stock, acc["cash"], "2000.00")
        _post(conn, "2026-01-15", acc["cost_of_goods_sold"], acc["cash"],
              "10000.00")
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.inventory_turnover, conn, ns(
            company_id=co, from_date="2026-01-01", to_date="2026-02-28"))
        assert is_ok(r), r
        assert (r["cogs"], r["inventory_start"], r["inventory_end"],
                r["average_inventory"]) == (
            "10000.00", "4000.00", "6000.00", "5000.00")
        assert (r["turnover_ratio"], r["turnover_days"],
                r["days_in_period"]) == ("2.00", "29.0", 58)
        # FINDING-m461-3: --item-id/--warehouse-id are accepted but ignored;
        # the filtered call returns byte-identical figures.
        filtered = call_action(MOD.inventory_turnover, conn, ns(
            company_id=co, from_date="2026-01-01", to_date="2026-02-28",
            item_id="m461-ghost-item", warehouse_id="m461-ghost-wh"))
        assert filtered == r
        _assert_vouchers_balance(conn, co)
        assert _snapshot(conn) == before

        bad = call_action(MOD.inventory_turnover, conn, ns(
            company_id=co, from_date="2026-01-01", to_date=None))
        assert is_error(bad)
        assert bad["message"] == "--to-date is required"
        assert _snapshot(conn) == before, "a refused call must write nothing"


# ---------------------------------------------------------------------------
# leave-utilization: READ-ONLY sums
# ---------------------------------------------------------------------------

class TestLeaveUtilizationDepth:
    def test_sums_approved_in_range_only_and_refuses_without_writing(
            self, conn):
        # This action does NOT reach the ledger: it sums leave rows. The
        # rejected and the out-of-range approved rows must not move the sums.
        co = seed_company(conn, "Leave Co", "LVC")
        ann = _id()
        _insert(conn, "employee", {"id": ann, "first_name": "Ann",
                                  "full_name": "Ann",
                                  "date_of_joining": "2025-01-01",
                                  "company_id": co})
        ltype = _id()
        _insert(conn, "leave_type", {"id": ltype, "name": "Annual"})
        _insert(conn, "leave_allocation", {
            "id": _id(), "employee_id": ann, "leave_type_id": ltype,
            "fiscal_year": "2026", "total_leaves": "20.00"})
        _insert(conn, "leave_application", {
            "id": _id(), "employee_id": ann, "leave_type_id": ltype,
            "from_date": "2026-01-10", "to_date": "2026-01-14",
            "total_days": "5.00", "status": "approved"})
        _insert(conn, "leave_application", {
            "id": _id(), "employee_id": ann, "leave_type_id": ltype,
            "from_date": "2026-03-01", "to_date": "2026-03-03",
            "total_days": "3.00", "status": "approved"})
        _insert(conn, "leave_application", {
            "id": _id(), "employee_id": ann, "leave_type_id": ltype,
            "from_date": "2026-01-20", "to_date": "2026-01-23",
            "total_days": "4.00", "status": "rejected"})
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.leave_utilization, conn, ns(
            company_id=co, **JAN_FEB))
        assert is_ok(r), r
        assert (r["total_allocated"], r["total_used"], r["remaining"],
                r["utilization"]) == ("20.00", "5.00", "15.00", "25.0%")
        # Read back through the seam: allocations and approved rows.
        la = Table("leave_allocation")
        query = Q.from_(la).select(la.total_leaves)
        assert sum(Decimal(str(x["total_leaves"])) for x in
                   conn.execute(query.get_sql(), ()).fetchall()) == Decimal(
                       "20.00")
        lap = Table("leave_application")
        query = (Q.from_(lap).select(lap.total_days, lap.status,
                                     lap.from_date, lap.to_date))
        apps = conn.execute(query.get_sql(), ()).fetchall()
        used = sum(Decimal(str(x["total_days"])) for x in apps
                   if x["status"] == "approved"
                   and x["from_date"] >= "2026-01-01"
                   and x["to_date"] <= "2026-02-28")
        assert used == Decimal("5.00")
        assert _snapshot(conn) == before

        bad = call_action(MOD.leave_utilization, conn, ns(company_id=None))
        assert is_error(bad)
        assert bad["message"] == "--company-id is required"
        assert _snapshot(conn) == before, "a refused call must write nothing"


# ---------------------------------------------------------------------------
# metric-trend: READ-ONLY series
# ---------------------------------------------------------------------------

class TestMetricTrendDepth:
    def test_revenue_series_and_unknown_metric_refusal_write_nothing(
            self, conn):
        # This action does NOT reach the ledger: it re-reads GL per period.
        co = seed_company(conn, "Metric Co", "MTC")
        acc = seed_accounts(conn, co)
        _post(conn, "2026-01-10", acc["cash"], acc["revenue"], "1000.00")
        _post(conn, "2026-02-12", acc["cash"], acc["revenue"], "2000.00")
        _insert(conn, "employee", {
            "id": _id(), "first_name": "Zed", "full_name": "Zed",
            "date_of_joining": "2025-01-01", "company_id": co})
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.metric_trend, conn, ns(
            company_id=co, metric="revenue", periodicity="monthly", **Q1))
        assert is_ok(r), r
        assert r["metric"] == "revenue"
        assert [(t["period"], t["value"], t.get("change_pct"))
                for t in r["trend"]] == [
            ("Jan 2026", "1000.00", None),
            ("Feb 2026", "2000.00", "100.0%"),
            ("Mar 2026", "0.00", "-100.0%"),
        ]
        # headcount is a valid metric here, which the refusal below omits.
        hc = call_action(MOD.metric_trend, conn, ns(
            company_id=co, metric="headcount", periodicity="monthly", **Q1))
        assert is_ok(hc), hc
        assert [t["value"] for t in hc["trend"]] == ["1.00", "1.00", "1.00"]
        _assert_vouchers_balance(conn, co)
        assert _snapshot(conn) == before

        # FINDING-m461-2: the advertised list omits the valid headcount
        # metric (it is only registered when requested). Pinned as observed.
        bad = call_action(MOD.metric_trend, conn, ns(
            company_id=co, metric="bogus"))
        assert is_error(bad)
        assert bad["message"] == (
            "Unknown metric: bogus. Available: assets, expenses, "
            "liabilities, net_income, revenue")
        assert _snapshot(conn) == before, "a refused call must write nothing"


# ---------------------------------------------------------------------------
# opex-vs-capex: READ-ONLY split
# ---------------------------------------------------------------------------

class TestOpexVsCapexDepth:
    def test_split_derive_from_expense_and_fixed_asset_posts(self, conn):
        # This action does NOT reach the ledger: it splits GL postings.
        co = seed_company(conn, "Split Co", "SPC")
        acc = seed_accounts(conn, co)
        equip = _id()
        _insert(conn, "account", {"id": equip, "name": "Equipment",
                                 "account_number": "1500",
                                 "root_type": "asset",
                                 "account_type": "fixed_asset", "is_group": 0,
                                 "company_id": co})
        _post(conn, "2026-01-10", acc["expense"], acc["cash"], "3000.00")
        _post(conn, "2026-01-20", equip, acc["cash"], "2000.00")
        conn.commit()
        before = _snapshot(conn)

        r = call_action(MOD.opex_vs_capex, conn, ns(company_id=co, **Q1))
        assert is_ok(r), r
        assert (r["opex"], r["capex"], r["total"]) == (
            "3000.00", "2000.00", "5000.00")
        assert (r["opex_share"], r["capex_share"]) == ("60.0%", "40.0%")
        assert r["capex_source"] == "gl_fixed_asset_accounts"
        assert r["assets_module"] is True
        rows = _assert_vouchers_balance(conn, co)
        opex_rows = [x for x in rows if x["account_id"] == acc["expense"]]
        assert sum(Decimal(str(x["debit"])) for x in opex_rows) == Decimal(
            "3000.00")
        assert _snapshot(conn) == before

        bad = call_action(MOD.opex_vs_capex, conn, ns(
            company_id=co, from_date="2026-01-01", to_date=None))
        assert is_error(bad)
        assert bad["message"] == "--to-date is required"
        assert _snapshot(conn) == before, "a refused call must write nothing"
