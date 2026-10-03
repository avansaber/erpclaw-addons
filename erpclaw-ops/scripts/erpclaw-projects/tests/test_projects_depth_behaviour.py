"""Behavioural depth tests for 9 erpclaw-projects actions previously covered by shape only.

Each action below already has a test asserting the response envelope (``is_ok``)
in ``test_projects.py``. Those tests stay untouched; the tests here prove what
each action actually does to the database: the exact row written (money as exact
``TEXT`` strings compared as ``Decimal``, never float), the rows that must NOT
have changed, and one input-validation refusal that must leave the database
byte-identical.

Covered actions (9): ``add-milestone``, ``gantt-data``, ``get-timesheet``,
``list-timesheets``, ``project-profitability``, ``resource-utilization``,
``update-milestone``, ``update-project``, ``update-task``.

None of these 9 handlers reaches the general ledger: every one of them reads or
writes only ``project`` / ``task`` / ``milestone`` / ``timesheet`` /
``timesheet_detail`` rows (writes also append one ``audit_log`` row) and never
posts journals. Each success test says so in a comment so a later reader does
not add debit/credit assertions that cannot hold.

Reads are built with PyPika through ``erpclaw_lib.query`` and rows are compared
with exact string equality for money columns.
"""
import json
import os
import sys
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from projects_helpers import (  # noqa: E402
    load_db_query, call_action, ns, is_ok, is_error,
    seed_project, seed_employee, _uuid,
)

from erpclaw_lib.query import Q, P, Table  # noqa: E402
from erpclaw_lib.response import row_to_dict  # noqa: E402

M = load_db_query()


_SNAPSHOT_TABLES = (
    "company",
    "naming_series",
    "employee",
    "customer",
    "project",
    "task",
    "milestone",
    "timesheet",
    "timesheet_detail",
    "audit_log",
)


def _snapshot(conn):
    """Full row dump of every owned table (plus audit_log) for before/after compare."""
    snap = {}
    for table in _SNAPSHOT_TABLES:
        tbl = Table(table)
        try:
            rows = conn.execute(
                Q.from_(tbl).select(tbl.star).get_sql(), ()).fetchall()
        except Exception:
            continue
        snap[table] = sorted(
            json.dumps(row_to_dict(r), sort_keys=True, default=str) for r in rows)
    return snap


def _count(conn, table):
    tbl = Table(table)
    rows = conn.execute(Q.from_(tbl).select(tbl.star).get_sql(), ()).fetchall()
    return len(rows)


def _fetch_one(conn, table, where_col, where_val):
    tbl = Table(table)
    col = getattr(tbl, where_col)
    q = Q.from_(tbl).select(tbl.star).where(col == P())
    return row_to_dict(conn.execute(q.get_sql(), (where_val,)).fetchone())


def _add_timesheet(conn, env, hours, rate, billable, date, task_id=None):
    item = {"project_id": env["project_id"], "hours": hours,
            "billing_rate": rate, "billable": billable, "date": date,
            "activity_type": "development"}
    if task_id is not None:
        item["task_id"] = task_id
    r = call_action(M.add_timesheet, conn, ns(
        company_id=env["company_id"], employee_id=env["employee_id"],
        start_date=date, end_date=date, items=json.dumps([item])))
    assert is_ok(r), r
    return r["timesheet"]["id"]


# ---------------------------------------------------------------------------
# add-milestone — stored row
# ---------------------------------------------------------------------------

class TestAddMilestoneDepth:
    def test_writes_pending_milestone_row_with_exact_values(self, conn, env):
        r = call_action(M.add_milestone, conn, ns(
            project_id=env["project_id"], name="Phase 1 Complete",
            target_date="2026-06-30"))
        assert is_ok(r), r
        mid = r["milestone"]["id"]
        assert r["milestone"]["status"] == "pending"
        row = _fetch_one(conn, "milestone", "id", mid)
        assert row["project_id"] == env["project_id"]
        assert row["milestone_name"] == "Phase 1 Complete"
        assert row["target_date"] == "2026-06-30"
        assert row["status"] == "pending"
        assert row["completion_date"] is None
        assert row["description"] is None
        assert _count(conn, "milestone") == 1
        # The parent project row is untouched.
        proj = _fetch_one(conn, "project", "id", env["project_id"])
        assert proj["status"] == "open"
        # No ledger legs: a milestone row plus one audit row, never journals.

    def test_refuses_a_missing_name_and_writes_nothing(self, conn, env):
        before = _snapshot(conn)
        r = call_action(M.add_milestone, conn, ns(
            project_id=env["project_id"], target_date="2026-06-30"))
        assert is_error(r)
        assert r["message"] == "--name is required"
        assert _snapshot(conn) == before
        assert _count(conn, "milestone") == 0


# ---------------------------------------------------------------------------
# update-milestone — stored row (status flip)
# ---------------------------------------------------------------------------

class TestUpdateMilestoneDepth:
    def test_completes_milestone_and_leaves_name_and_target_alone(self, conn, env):
        first = call_action(M.add_milestone, conn, ns(
            project_id=env["project_id"], name="Shippable",
            target_date="2026-07-15"))
        other = call_action(M.add_milestone, conn, ns(
            project_id=env["project_id"], name="Untouched",
            target_date="2026-08-01"))
        assert is_ok(first), first
        mid = first["milestone"]["id"]
        assert _fetch_one(conn, "milestone", "id", mid)["status"] == "pending"
        r = call_action(M.update_milestone, conn, ns(
            milestone_id=mid, status="completed",
            completion_date="2026-07-10"))
        assert is_ok(r), r
        row = _fetch_one(conn, "milestone", "id", mid)
        assert (row["milestone_name"], row["target_date"], row["status"],
                row["completion_date"]) == (
            "Shippable", "2026-07-15", "completed", "2026-07-10")
        sibling = _fetch_one(conn, "milestone", "id", other["milestone"]["id"])
        assert (sibling["milestone_name"], sibling["status"],
                sibling["completion_date"]) == ("Untouched", "pending", None)
        # No ledger legs: a status/date flip only, never journals.

    def test_refuses_a_bad_status_and_changes_nothing(self, conn, env):
        added = call_action(M.add_milestone, conn, ns(
            project_id=env["project_id"], name="Guarded",
            target_date="2026-07-15"))
        mid = added["milestone"]["id"]
        before = _snapshot(conn)
        r = call_action(M.update_milestone, conn, ns(
            milestone_id=mid, status="bogus"))
        assert is_error(r)
        assert r["message"] == (
            "Invalid --status: bogus. Must be one of "
            "('pending', 'completed', 'missed')")
        assert _snapshot(conn) == before
        row = _fetch_one(conn, "milestone", "id", mid)
        assert (row["status"], row["completion_date"]) == ("pending", None)


# ---------------------------------------------------------------------------
# update-project — stored row (field change plus margin recalculation)
# ---------------------------------------------------------------------------

class TestUpdateProjectDepth:
    def test_updates_status_cost_and_progress_and_recalculates_margin(self, conn, env):
        before = _fetch_one(conn, "project", "id", env["project_id"])
        assert before["status"] == "open"
        r = call_action(M.update_project, conn, ns(
            project_id=env["project_id"], status="in_progress",
            estimated_cost="10000.00", percent_complete="25"))
        assert is_ok(r), r
        row = _fetch_one(conn, "project", "id", env["project_id"])
        assert row["status"] == "in_progress"
        assert row["estimated_cost"] == "10000.00"
        assert Decimal(row["estimated_cost"]) == Decimal("10000.00")
        assert row["percent_complete"] == "25.00"
        assert row["profit_margin"] == "0"
        assert row["project_name"] == before["project_name"]
        assert row["actual_cost"] == "0"
        assert row["total_billed"] == "0"
        r2 = call_action(M.update_project, conn, ns(
            project_id=env["project_id"], actual_cost="400.00",
            total_billed="1000.00"))
        assert is_ok(r2), r2
        recalc = _fetch_one(conn, "project", "id", env["project_id"])
        assert recalc["actual_cost"] == "400.00"
        assert recalc["total_billed"] == "1000.00"
        assert recalc["profit_margin"] == "60.00"
        assert Decimal(recalc["profit_margin"]) == Decimal("60.00")
        assert recalc["status"] == "in_progress"
        # No ledger legs: project money columns move, never journals.

    def test_refuses_a_bad_status_and_changes_nothing(self, conn, env):
        before = _snapshot(conn)
        r = call_action(M.update_project, conn, ns(
            project_id=env["project_id"], status="bogus"))
        assert is_error(r)
        assert r["message"] == (
            "Invalid --status: bogus. Must be one of ('open', 'in_progress', "
            "'completed', 'cancelled', 'on_hold')")
        assert _snapshot(conn) == before
        row = _fetch_one(conn, "project", "id", env["project_id"])
        assert row["status"] == "open"


# ---------------------------------------------------------------------------
# update-task — stored row (field change)
# ---------------------------------------------------------------------------

class TestUpdateTaskDepth:
    def test_moves_task_to_in_progress_and_leaves_name_and_hours_alone(self, conn, env):
        added = call_action(M.add_task, conn, ns(
            project_id=env["project_id"], name="Updatable Task"))
        assert is_ok(added), added
        tid = added["task"]["id"]
        assert _fetch_one(conn, "task", "id", tid)["status"] == "open"
        r = call_action(M.update_task, conn, ns(
            task_id=tid, status="in_progress", priority="high"))
        assert is_ok(r), r
        row = _fetch_one(conn, "task", "id", tid)
        assert row["status"] == "in_progress"
        assert row["priority"] == "high"
        assert row["task_name"] == "Updatable Task"
        assert row["project_id"] == env["project_id"]
        assert row["estimated_hours"] == "0"
        assert row["actual_hours"] == "0"
        # No ledger legs: a status/priority flip only, never journals.

    def test_refuses_a_bad_status_and_changes_nothing(self, conn, env):
        added = call_action(M.add_task, conn, ns(
            project_id=env["project_id"], name="Guarded Task"))
        tid = added["task"]["id"]
        before = _snapshot(conn)
        r = call_action(M.update_task, conn, ns(
            task_id=tid, status="bogus"))
        assert is_error(r)
        assert r["message"] == (
            "Invalid --status: bogus. Must be one of ('open', 'in_progress', "
            "'completed', 'cancelled', 'blocked')")
        assert _snapshot(conn) == before
        row = _fetch_one(conn, "task", "id", tid)
        assert (row["status"], row["priority"]) == ("open", "medium")


# ---------------------------------------------------------------------------
# get-timesheet — stored rows (header plus detail lines)
# ---------------------------------------------------------------------------

class TestGetTimesheetDepth:
    def test_returns_the_stored_header_totals_and_both_detail_lines(self, conn, env):
        task = call_action(M.add_task, conn, ns(
            project_id=env["project_id"], name="Billable Work"))
        assert is_ok(task), task
        tid = task["task"]["id"]
        items = json.dumps([
            {"project_id": env["project_id"], "task_id": tid, "hours": "8",
             "billing_rate": "100.00", "billable": 1, "date": "2026-03-10",
             "activity_type": "development"},
            {"project_id": env["project_id"], "hours": "2",
             "billing_rate": "50.00", "billable": 0, "date": "2026-03-10",
             "activity_type": "support"},
        ])
        added = call_action(M.add_timesheet, conn, ns(
            company_id=env["company_id"], employee_id=env["employee_id"],
            start_date="2026-03-10", end_date="2026-03-10", items=items))
        assert is_ok(added), added
        tsid = added["timesheet"]["id"]
        r = call_action(M.get_timesheet, conn, ns(timesheet_id=tsid))
        assert is_ok(r), r
        header = _fetch_one(conn, "timesheet", "id", tsid)
        assert r["timesheet"]["total_hours"] == header["total_hours"] == "10.00"
        assert r["timesheet"]["total_billable_hours"] == "8.00"
        assert Decimal(header["total_billable_hours"]) == Decimal("8.00")
        assert r["timesheet"]["total_cost"] == header["total_cost"] == "900.00"
        assert Decimal(header["total_cost"]) == Decimal("900.00")
        assert r["timesheet"]["total_billable_amount"] == "800.00"
        assert r["timesheet"]["status"] == header["status"] == "draft"
        assert r["timesheet"]["employee_name"] == "John Doe"
        assert len(r["timesheet"]["items"]) == 2
        tdt = Table("timesheet_detail")
        dq = (Q.from_(tdt).select(tdt.star)
              .where(tdt.timesheet_id == P()).orderby(tdt.date))
        stored = [row_to_dict(d) for d in
                  conn.execute(dq.get_sql(), (tsid,)).fetchall()]
        by_hours = {d["hours"]: d for d in stored}
        assert by_hours["8.00"]["task_id"] == tid
        assert by_hours["8.00"]["billing_rate"] == "100.00"
        assert by_hours["8.00"]["billable"] == 1
        assert by_hours["2.00"]["task_id"] is None
        assert by_hours["2.00"]["billing_rate"] == "50.00"
        assert by_hours["2.00"]["billable"] == 0
        assert {i["hours"] for i in r["timesheet"]["items"]} == {"8.00", "2.00"}
        # No ledger legs: a read of stored rows, never journals.

    def test_refuses_an_unknown_timesheet_and_changes_nothing(self, conn, env):
        missing = _uuid()
        before = _snapshot(conn)
        r = call_action(M.get_timesheet, conn, ns(timesheet_id=missing))
        assert is_error(r)
        assert r["message"] == f"Timesheet {missing} not found"
        assert _snapshot(conn) == before


# ---------------------------------------------------------------------------
# list-timesheets — stored rows (listing plus filters)
# ---------------------------------------------------------------------------

class TestListTimesheetsDepth:
    def test_lists_both_sheets_and_filters_by_employee_status_and_project(
            self, conn, env):
        other_emp = seed_employee(conn, env["company_id"], name="Jane Smith")
        first = _add_timesheet(conn, env, "8", "100.00", 1, "2026-03-10")
        items = json.dumps([{"project_id": env["project_id"], "hours": "4",
                             "billing_rate": "200.00", "billable": 1,
                             "date": "2026-03-11",
                             "activity_type": "consulting"}])
        second = call_action(M.add_timesheet, conn, ns(
            company_id=env["company_id"], employee_id=other_emp,
            start_date="2026-03-11", end_date="2026-03-11", items=items))
        assert is_ok(second), second
        second_id = second["timesheet"]["id"]
        r = call_action(M.list_timesheets, conn, ns(
            company_id=env["company_id"]))
        assert is_ok(r), r
        assert r["total"] == 2
        assert {t["id"] for t in r["timesheets"]} == {first, second_id}
        by_emp = call_action(M.list_timesheets, conn, ns(
            company_id=env["company_id"], employee_id=other_emp))
        assert is_ok(by_emp), by_emp
        assert by_emp["total"] == 1
        assert by_emp["timesheets"][0]["id"] == second_id
        assert by_emp["timesheets"][0]["total_hours"] == "4.00"
        by_status = call_action(M.list_timesheets, conn, ns(
            company_id=env["company_id"], status="draft"))
        assert by_status["total"] == 2
        assert call_action(M.submit_timesheet, conn,
                           ns(timesheet_id=first))["status"] == "ok"
        submitted = call_action(M.list_timesheets, conn, ns(
            company_id=env["company_id"], status="submitted"))
        assert submitted["total"] == 1
        assert submitted["timesheets"][0]["id"] == first
        by_project = call_action(M.list_timesheets, conn, ns(
            project_id=env["project_id"]))
        assert by_project["total"] == 2
        # No ledger legs: a read of stored rows, never journals.

    def test_refuses_a_bad_status_and_changes_nothing(self, conn, env):
        _add_timesheet(conn, env, "8", "100.00", 1, "2026-03-10")
        before = _snapshot(conn)
        r = call_action(M.list_timesheets, conn, ns(
            company_id=env["company_id"], status="bogus"))
        assert is_error(r)
        assert r["message"] == (
            "Invalid --status: bogus. Must be one of ('draft', 'submitted', "
            "'billed', 'cancelled')")
        assert _snapshot(conn) == before
        assert _count(conn, "timesheet") == 1


# ---------------------------------------------------------------------------
# gantt-data — stored rows (tasks plus milestone markers)
# ---------------------------------------------------------------------------

class TestGanttDataDepth:
    def test_returns_the_stored_tasks_with_parsed_dependencies_and_markers(
            self, conn, env):
        first = call_action(M.add_task, conn, ns(
            project_id=env["project_id"], name="Foundation",
            start_date="2026-04-01", end_date="2026-04-10"))
        second = call_action(M.add_task, conn, ns(
            project_id=env["project_id"], name="Framing"))
        assert is_ok(first) and is_ok(second), (first, second)
        dep = call_action(M.update_task, conn, ns(
            task_id=second["task"]["id"],
            depends_on=json.dumps([first["task"]["id"]])))
        assert is_ok(dep), dep
        ms = call_action(M.add_milestone, conn, ns(
            project_id=env["project_id"], name="Topped Out",
            target_date="2026-04-15"))
        assert is_ok(ms), ms
        elsewhere = seed_project(conn, env["company_id"], name="Elsewhere")
        stray = call_action(M.add_task, conn, ns(
            project_id=elsewhere, name="Not Ours"))
        assert is_ok(stray), stray
        r = call_action(M.gantt_data, conn, ns(project_id=env["project_id"]))
        assert is_ok(r), r
        assert r["project_id"] == env["project_id"]
        assert r["project_name"].startswith("Test Project")
        by_name = {t["name"]: t for t in r["tasks"]}
        assert set(by_name) == {"Foundation", "Framing"}
        assert by_name["Foundation"]["depends_on"] is None
        assert by_name["Foundation"]["start_date"] == "2026-04-01"
        assert by_name["Foundation"]["end_date"] == "2026-04-10"
        assert by_name["Foundation"]["status"] == "open"
        assert by_name["Framing"]["depends_on"] == [first["task"]["id"]]
        assert by_name["Framing"]["status"] == "open"
        assert [(m["name"], m["target_date"], m["status"])
                for m in r["milestones"]] == [
            ("Topped Out", "2026-04-15", "pending")]
        # The other project's task is not leaked into this chart.
        assert stray["task"]["id"] not in {t["id"] for t in r["tasks"]}
        # No ledger legs: a read of stored rows, never journals.

    def test_refuses_a_missing_project_and_changes_nothing(self, conn, env):
        before = _snapshot(conn)
        r = call_action(M.gantt_data, conn, ns(project_id=None))
        assert is_error(r)
        assert r["message"] == "--project-id is required"
        assert _snapshot(conn) == before


# ---------------------------------------------------------------------------
# project-profitability — stored rows (project money plus timesheet breakdown)
# ---------------------------------------------------------------------------

class TestProjectProfitabilityDepth:
    def test_reports_stored_costs_and_breaks_down_billed_hours_by_employee(
            self, conn, env):
        est = call_action(M.update_project, conn, ns(
            project_id=env["project_id"], estimated_cost="10000.00"))
        assert is_ok(est), est
        items = json.dumps([
            {"project_id": env["project_id"], "hours": "8",
             "billing_rate": "100.00", "billable": 1, "date": "2026-03-10",
             "activity_type": "development"},
            {"project_id": env["project_id"], "hours": "2",
             "billing_rate": "50.00", "billable": 0, "date": "2026-03-10",
             "activity_type": "support"},
        ])
        added = call_action(M.add_timesheet, conn, ns(
            company_id=env["company_id"], employee_id=env["employee_id"],
            start_date="2026-03-10", end_date="2026-03-10", items=items))
        assert is_ok(added), added
        tsid = added["timesheet"]["id"]
        draft_only = call_action(M.project_profitability, conn, ns(
            project_id=env["project_id"]))
        assert is_ok(draft_only), draft_only
        assert draft_only["employees"] == []
        assert draft_only["total_billed"] == "0"
        assert is_ok(call_action(M.submit_timesheet, conn, ns(timesheet_id=tsid)))
        bill = call_action(M.bill_timesheet, conn, ns(timesheet_id=tsid))
        assert is_ok(bill), bill
        proj = _fetch_one(conn, "project", "id", env["project_id"])
        assert proj["actual_cost"] == "900.00"
        assert Decimal(proj["actual_cost"]) == Decimal("900.00")
        assert proj["total_billed"] == "800.00"
        assert proj["profit_margin"] == "-12.50"
        r = call_action(M.project_profitability, conn, ns(
            project_id=env["project_id"]))
        assert is_ok(r), r
        assert r["estimated_cost"] == "10000.00"
        assert r["actual_cost"] == proj["actual_cost"] == "900.00"
        assert r["total_billed"] == proj["total_billed"] == "800.00"
        assert r["profit"] == "-100.00"
        assert Decimal(r["profit"]) == Decimal("-100.00")
        assert r["margin_percent"] == "-12.50"
        assert r["cost_variance"] == "9100.00"
        assert Decimal(r["cost_variance"]) == Decimal("9100.00")
        assert len(r["employees"]) == 1
        emp = r["employees"][0]
        assert emp["employee_id"] == env["employee_id"]
        assert emp["employee_name"] == "John Doe"
        assert emp["total_hours"] == "10.00"
        assert emp["billable_hours"] == "8.00"
        assert emp["total_cost"] == "900.00"
        assert emp["billable_amount"] == "800.00"
        # No ledger legs: project money columns plus a timesheet read,
        # never journals.

    def test_refuses_a_missing_project_and_changes_nothing(self, conn, env):
        before = _snapshot(conn)
        r = call_action(M.project_profitability, conn, ns(project_id=None))
        assert is_error(r)
        assert r["message"] == "--project-id is required"
        assert _snapshot(conn) == before


# ---------------------------------------------------------------------------
# resource-utilization — stored rows (aggregates over submitted timesheets)
# ---------------------------------------------------------------------------

class TestResourceUtilizationDepth:
    def test_aggregates_submitted_hours_per_employee_and_ignores_drafts(
            self, conn, env):
        other_emp = seed_employee(conn, env["company_id"], name="Jane Smith")
        first = _add_timesheet(conn, env, "8", "100.00", 1, "2026-03-10")
        extra = json.dumps([{"project_id": env["project_id"], "hours": "2",
                             "billing_rate": "50.00", "billable": 0,
                             "date": "2026-03-10",
                             "activity_type": "support"}])
        line2 = call_action(M.add_timesheet, conn, ns(
            company_id=env["company_id"], employee_id=env["employee_id"],
            start_date="2026-03-10", end_date="2026-03-10", items=extra))
        assert is_ok(line2), line2
        jane_items = json.dumps([{"project_id": env["project_id"], "hours": "4",
                                  "billing_rate": "200.00", "billable": 1,
                                  "date": "2026-03-11",
                                  "activity_type": "consulting"}])
        jane = call_action(M.add_timesheet, conn, ns(
            company_id=env["company_id"], employee_id=other_emp,
            start_date="2026-03-11", end_date="2026-03-11", items=jane_items))
        assert is_ok(jane), jane
        assert is_ok(call_action(M.submit_timesheet, conn, ns(timesheet_id=first)))
        assert is_ok(call_action(
            M.submit_timesheet, conn, ns(timesheet_id=jane["timesheet"]["id"])))
        r = call_action(M.resource_utilization, conn, ns(
            company_id=env["company_id"]))
        assert is_ok(r), r
        by_emp = {e["employee_id"]: e for e in r["employees"]}
        assert set(by_emp) == {env["employee_id"], other_emp}
        john = by_emp[env["employee_id"]]
        assert john["total_hours"] == "8.00"
        assert john["billable_hours"] == "8.00"
        assert john["non_billable_hours"] == "0.00"
        assert john["utilization_percent"] == "100.00"
        assert john["billable_amount"] == "800.00"
        jane_row = by_emp[other_emp]
        assert jane_row["employee_name"] == "Jane Smith"
        assert jane_row["total_hours"] == "4.00"
        assert jane_row["billable_hours"] == "4.00"
        assert jane_row["utilization_percent"] == "100.00"
        assert jane_row["billable_amount"] == "800.00"
        assert Decimal(jane_row["billable_amount"]) == Decimal("800.00")
        assert r["summary"]["total_hours"] == "12.00"
        assert r["summary"]["total_billable_hours"] == "12.00"
        assert r["summary"]["overall_utilization_percent"] == "100.00"
        # The still-draft 2h non-billable line is not counted anywhere.
        # No ledger legs: an aggregate read over stored rows, never journals.

    def test_refuses_a_missing_company_and_changes_nothing(self, conn, env):
        _add_timesheet(conn, env, "8", "100.00", 1, "2026-03-10")
        before = _snapshot(conn)
        r = call_action(M.resource_utilization, conn, ns(company_id=None))
        assert is_error(r)
        assert r["message"] == "--company-id is required"
        assert _snapshot(conn) == before
