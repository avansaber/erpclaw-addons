"""Timesheet submit and bill keep their bookkeeping after the timestamp conversion.

submit-timesheet adds hours to a task-linked task, and bill-timesheet marks the
timesheet billed and rolls cost and billing onto the project. Both used to write
updated_at with SQLite's datetime('now'); these tests pin what the statements
do, so the move to the dialect helper cannot change it.
"""
import json
import os
import sys
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from projects_helpers import call_action, is_ok, load_db_query, ns  # noqa: E402

M = load_db_query()


def _submitted_timesheet_with_task(conn, env):
    task = call_action(M.add_task, conn, ns(project_id=env["project_id"], name="Build"))
    assert is_ok(task), task
    task_id = task["task"]["id"]
    items = json.dumps([{"project_id": env["project_id"], "task_id": task_id,
                         "hours": "4", "billing_rate": "75", "billable": 1,
                         "date": "2026-03-11", "activity_type": "consulting"}])
    added = call_action(M.add_timesheet, conn, ns(
        company_id=env["company_id"], employee_id=env["employee_id"],
        start_date="2026-03-11", end_date="2026-03-11", items=items))
    assert is_ok(added), added
    ts_id = added["timesheet"]["id"]
    assert is_ok(call_action(M.submit_timesheet, conn, ns(timesheet_id=ts_id)))
    return task_id, ts_id


def test_submit_adds_hours_to_the_linked_task(conn, env):
    task_id, ts_id = _submitted_timesheet_with_task(conn, env)
    task = conn.execute("SELECT actual_hours, updated_at FROM task WHERE id = ?",
                        (task_id,)).fetchone()
    assert Decimal(task["actual_hours"]) == Decimal("4.00")
    assert task["updated_at"] is not None
    ts = conn.execute("SELECT status FROM timesheet WHERE id = ?", (ts_id,)).fetchone()
    assert ts["status"] == "submitted"


def test_bill_marks_billed_and_rolls_onto_the_project(conn, env):
    _task_id, ts_id = _submitted_timesheet_with_task(conn, env)
    r = call_action(M.bill_timesheet, conn, ns(timesheet_id=ts_id))
    assert is_ok(r), r
    ts = conn.execute("SELECT status, total_billed_hours FROM timesheet WHERE id = ?",
                      (ts_id,)).fetchone()
    assert ts["status"] == "billed"
    assert Decimal(ts["total_billed_hours"]) == Decimal("4.00")
    proj = conn.execute("SELECT actual_cost, total_billed, profit_margin FROM project "
                        "WHERE id = ?", (env["project_id"],)).fetchone()
    assert Decimal(proj["total_billed"]) == Decimal("300.00")
    assert Decimal(proj["actual_cost"]) == Decimal("300.00")
    assert Decimal(proj["profit_margin"]) == Decimal("0.00")
