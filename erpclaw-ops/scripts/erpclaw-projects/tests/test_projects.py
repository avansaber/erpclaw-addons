"""L1 tests for ERPClaw Projects skill (19 actions).

Tests cover: projects, tasks, milestones, timesheets, and reports.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from projects_helpers import (
    load_db_query, call_action, ns, is_ok, is_error,
    seed_company, seed_naming_series, seed_employee,
    seed_customer, seed_project, _uuid,
)

M = load_db_query()


# ===================================================================
# Projects
# ===================================================================

class TestAddProject:
    def test_add_project_ok(self, conn, env):
        r = call_action(M.add_project, conn, ns(
            company_id=env["company_id"],
            name="Website Redesign",
        ))
        assert is_ok(r)
        assert r["project"]["id"]
        assert r["project"]["project_name"] == "Website Redesign"
        assert r["project"]["status"] == "open"

    def test_add_project_with_customer(self, conn, env):
        r = call_action(M.add_project, conn, ns(
            company_id=env["company_id"],
            name="Client Project",
            customer_id=env["customer_id"],
            billing_type="time_and_material",
        ))
        assert is_ok(r)
        assert r["project"]["billing_type"] == "time_and_material"

    def test_add_project_missing_name(self, conn, env):
        r = call_action(M.add_project, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_error(r)

    def test_add_project_missing_company(self, conn, env):
        r = call_action(M.add_project, conn, ns(name="NoCompany"))
        assert is_error(r)

    def test_add_project_invalid_type(self, conn, env):
        r = call_action(M.add_project, conn, ns(
            company_id=env["company_id"],
            name="BadType",
            project_type="invalid",
        ))
        assert is_error(r)


class TestUpdateProject:
    def test_update_project_ok(self, conn, env):
        r = call_action(M.update_project, conn, ns(
            project_id=env["project_id"],
            status="in_progress",
        ))
        assert is_ok(r)

    def test_update_project_not_found(self, conn, env):
        r = call_action(M.update_project, conn, ns(
            project_id=_uuid(),
            status="in_progress",
        ))
        assert is_error(r)


class TestGetProject:
    def test_get_project_ok(self, conn, env):
        r = call_action(M.get_project, conn, ns(
            project_id=env["project_id"],
        ))
        assert is_ok(r)
        assert r["project"]["id"] == env["project_id"]

    def test_get_project_not_found(self, conn, env):
        r = call_action(M.get_project, conn, ns(project_id=_uuid()))
        assert is_error(r)


class TestListProjects:
    def test_list_projects(self, conn, env):
        r = call_action(M.list_projects, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["total"] >= 1


# ===================================================================
# Tasks
# ===================================================================

class TestAddTask:
    def test_add_task_ok(self, conn, env):
        r = call_action(M.add_task, conn, ns(
            project_id=env["project_id"],
            name="Design mockups",
        ))
        assert is_ok(r)
        assert r["task"]["id"]
        assert r["task"]["task_name"] == "Design mockups"
        assert r["task"]["status"] == "open"

    def test_add_task_with_priority(self, conn, env):
        r = call_action(M.add_task, conn, ns(
            project_id=env["project_id"],
            name="High Priority Task",
            priority="high",
        ))
        assert is_ok(r)
        assert r["task"]["priority"] == "high"

    def test_add_task_missing_project(self, conn, env):
        r = call_action(M.add_task, conn, ns(name="No Project Task"))
        assert is_error(r)

    def test_add_task_missing_name(self, conn, env):
        r = call_action(M.add_task, conn, ns(
            project_id=env["project_id"],
        ))
        assert is_error(r)

    def test_add_task_invalid_status(self, conn, env):
        r = call_action(M.add_task, conn, ns(
            project_id=env["project_id"],
            name="Bad Status",
            status="invalid",
        ))
        assert is_error(r)


class TestUpdateTask:
    def test_update_task_ok(self, conn, env):
        add_r = call_action(M.add_task, conn, ns(
            project_id=env["project_id"],
            name="Updatable Task",
        ))
        tid = add_r["task"]["id"]

        r = call_action(M.update_task, conn, ns(
            task_id=tid, status="in_progress",
        ))
        assert is_ok(r)

    def test_update_task_not_found(self, conn, env):
        r = call_action(M.update_task, conn, ns(
            task_id=_uuid(), status="in_progress",
        ))
        assert is_error(r)


class TestListTasks:
    def test_list_tasks(self, conn, env):
        call_action(M.add_task, conn, ns(
            project_id=env["project_id"],
            name="Listed Task",
        ))
        r = call_action(M.list_tasks, conn, ns(
            project_id=env["project_id"],
        ))
        assert is_ok(r)
        assert len(r["tasks"]) >= 1


# ===================================================================
# Milestones
# ===================================================================

class TestAddMilestone:
    def test_add_milestone_ok(self, conn, env):
        r = call_action(M.add_milestone, conn, ns(
            project_id=env["project_id"],
            name="Phase 1 Complete",
            target_date="2026-06-30",
        ))
        assert is_ok(r)

    def test_add_milestone_missing_project(self, conn, env):
        r = call_action(M.add_milestone, conn, ns(
            name="No Project Milestone",
            target_date="2026-06-30",
        ))
        assert is_error(r)


class TestUpdateMilestone:
    def test_update_milestone_ok(self, conn, env):
        add_r = call_action(M.add_milestone, conn, ns(
            project_id=env["project_id"],
            name="Updatable Milestone",
            target_date="2026-07-15",
        ))
        mid = add_r["milestone"]["id"]

        r = call_action(M.update_milestone, conn, ns(
            milestone_id=mid, status="completed",
            completion_date="2026-07-10",
        ))
        assert is_ok(r)


# ===================================================================
# Timesheets
# ===================================================================

class TestAddTimesheet:
    def test_add_timesheet_ok(self, conn, env):
        items_json = json.dumps([{
            "project_id": env["project_id"],
            "hours": "8",
            "billing_rate": "100",
            "billable": 1,
            "date": "2026-03-10",
            "activity_type": "development",
        }])
        r = call_action(M.add_timesheet, conn, ns(
            company_id=env["company_id"],
            employee_id=env["employee_id"],
            start_date="2026-03-10",
            end_date="2026-03-10",
            items=items_json,
        ))
        assert is_ok(r)
        assert r["timesheet"]["id"]

    def test_add_timesheet_missing_employee(self, conn, env):
        items_json = json.dumps([{
            "project_id": env["project_id"],
            "hours": "8", "date": "2026-03-10",
        }])
        r = call_action(M.add_timesheet, conn, ns(
            company_id=env["company_id"],
            start_date="2026-03-10",
            end_date="2026-03-10",
            items=items_json,
        ))
        assert is_error(r)


class TestSubmitTimesheet:
    def test_submit_timesheet_ok(self, conn, env):
        items_json = json.dumps([{
            "project_id": env["project_id"],
            "hours": "4",
            "billing_rate": "75",
            "billable": 1,
            "date": "2026-03-11",
            "activity_type": "consulting",
        }])
        add_r = call_action(M.add_timesheet, conn, ns(
            company_id=env["company_id"],
            employee_id=env["employee_id"],
            start_date="2026-03-11",
            end_date="2026-03-11",
            items=items_json,
        ))
        ts_id = add_r["timesheet"]["id"]

        r = call_action(M.submit_timesheet, conn, ns(timesheet_id=ts_id))
        assert is_ok(r)


# ===================================================================
# Reports
# ===================================================================

class TestGanttData:
    def test_gantt_data_ok(self, conn, env):
        r = call_action(M.gantt_data, conn, ns(
            project_id=env["project_id"],
        ))
        assert is_ok(r)


class TestStatus:
    def test_status_ok(self, conn, env):
        r = call_action(M.status, conn, ns())
        assert is_ok(r)
