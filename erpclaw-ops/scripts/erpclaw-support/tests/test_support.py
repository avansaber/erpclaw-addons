"""L1 tests for ERPClaw Support skill (18 actions).

Tests cover: issues, comments, SLAs, warranty claims, maintenance schedules,
maintenance visits, and reports.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from support_helpers import (
    load_db_query, call_action, ns, is_ok, is_error,
    seed_company, seed_naming_series, seed_customer,
    seed_item, _uuid,
)

M = load_db_query()


# ===================================================================
# Issues
# ===================================================================

class TestAddIssue:
    def test_add_issue_ok(self, conn, env):
        r = call_action(M.add_issue, conn, ns(
            subject="Printer not working",
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["issue"]["id"]
        assert r["issue"]["status"] == "open"
        assert r["issue"]["priority"] == "medium"

    def test_add_issue_with_customer(self, conn, env):
        r = call_action(M.add_issue, conn, ns(
            subject="Login failure",
            customer_id=env["customer_id"],
            priority="high",
            issue_type="bug",
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["issue"]["priority"] == "high"

    def test_add_issue_missing_subject(self, conn, env):
        r = call_action(M.add_issue, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_error(r)

    def test_add_issue_invalid_priority(self, conn, env):
        r = call_action(M.add_issue, conn, ns(
            subject="Bad Priority",
            priority="extreme",
            company_id=env["company_id"],
        ))
        assert is_error(r)


class TestUpdateIssue:
    def test_update_issue_ok(self, conn, env):
        add_r = call_action(M.add_issue, conn, ns(
            subject="Update Test Issue",
            company_id=env["company_id"],
        ))
        iid = add_r["issue"]["id"]

        r = call_action(M.update_issue, conn, ns(
            issue_id=iid, priority="high",
        ))
        assert is_ok(r)

    def test_update_issue_not_found(self, conn, env):
        r = call_action(M.update_issue, conn, ns(
            issue_id=_uuid(), priority="low",
        ))
        assert is_error(r)


class TestGetIssue:
    def test_get_issue_ok(self, conn, env):
        add_r = call_action(M.add_issue, conn, ns(
            subject="Get Test Issue",
            company_id=env["company_id"],
        ))
        iid = add_r["issue"]["id"]

        r = call_action(M.get_issue, conn, ns(issue_id=iid))
        assert is_ok(r)
        assert r["issue"]["id"] == iid

    def test_get_issue_not_found(self, conn, env):
        r = call_action(M.get_issue, conn, ns(issue_id=_uuid()))
        assert is_error(r)


class TestListIssues:
    def test_list_issues_empty(self, conn, env):
        r = call_action(M.list_issues, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)

    def test_list_issues_after_add(self, conn, env):
        call_action(M.add_issue, conn, ns(
            subject="Listed Issue",
            company_id=env["company_id"],
        ))
        r = call_action(M.list_issues, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)


# ===================================================================
# Issue Comments
# ===================================================================

class TestAddIssueComment:
    def test_add_comment_ok(self, conn, env):
        add_r = call_action(M.add_issue, conn, ns(
            subject="Comment Test Issue",
            company_id=env["company_id"],
        ))
        iid = add_r["issue"]["id"]

        r = call_action(M.add_issue_comment, conn, ns(
            issue_id=iid, comment="Working on it",
        ))
        assert is_ok(r)
        assert r["comment"]["id"]
        assert r["comment"]["comment_by"] == "employee"

    def test_add_comment_missing_issue(self, conn, env):
        r = call_action(M.add_issue_comment, conn, ns(
            comment="Orphan comment",
        ))
        assert is_error(r)

    def test_add_comment_missing_text(self, conn, env):
        add_r = call_action(M.add_issue, conn, ns(
            subject="No Comment Text",
            company_id=env["company_id"],
        ))
        iid = add_r["issue"]["id"]

        r = call_action(M.add_issue_comment, conn, ns(issue_id=iid))
        assert is_error(r)


# ===================================================================
# Resolve / Reopen
# ===================================================================

class TestResolveIssue:
    def test_resolve_issue_ok(self, conn, env):
        add_r = call_action(M.add_issue, conn, ns(
            subject="Resolve Test Issue",
            company_id=env["company_id"],
        ))
        iid = add_r["issue"]["id"]

        r = call_action(M.resolve_issue, conn, ns(
            issue_id=iid, resolution_notes="Fixed by restarting service",
        ))
        assert is_ok(r)

    def test_resolve_issue_not_found(self, conn, env):
        r = call_action(M.resolve_issue, conn, ns(issue_id=_uuid()))
        assert is_error(r)


class TestReopenIssue:
    def test_reopen_issue_ok(self, conn, env):
        add_r = call_action(M.add_issue, conn, ns(
            subject="Reopen Test Issue",
            company_id=env["company_id"],
        ))
        iid = add_r["issue"]["id"]
        call_action(M.resolve_issue, conn, ns(issue_id=iid))

        r = call_action(M.reopen_issue, conn, ns(
            issue_id=iid, reason="Issue recurred",
        ))
        assert is_ok(r)


# ===================================================================
# SLAs
# ===================================================================

class TestAddSla:
    def test_add_sla_ok(self, conn, env):
        priorities_json = json.dumps({
            "response_times": {"low": 24, "medium": 8, "high": 4, "critical": 1},
            "resolution_times": {"low": 72, "medium": 24, "high": 12, "critical": 4},
        })
        r = call_action(M.add_sla, conn, ns(
            name="Standard SLA",
            priorities=priorities_json,
        ))
        assert is_ok(r)
        assert r["sla"]["id"]
        assert r["sla"]["name"] == "Standard SLA"

    def test_add_sla_missing_name(self, conn, env):
        r = call_action(M.add_sla, conn, ns(
            priorities='{"response_times":{},"resolution_times":{}}',
        ))
        assert is_error(r)

    def test_add_sla_missing_priorities(self, conn, env):
        r = call_action(M.add_sla, conn, ns(name="No Priorities"))
        assert is_error(r)


class TestListSlas:
    def test_list_slas_empty(self, conn, env):
        r = call_action(M.list_slas, conn, ns())
        assert is_ok(r)
        assert r["slas"] == []


# ===================================================================
# Warranty Claims
# ===================================================================

class TestAddWarrantyClaim:
    def test_add_claim_ok(self, conn, env):
        r = call_action(M.add_warranty_claim, conn, ns(
            customer_id=env["customer_id"],
            complaint_description="Screen flickering",
        ))
        assert is_ok(r)
        assert r["warranty_claim"]["id"]

    def test_add_claim_missing_customer(self, conn, env):
        r = call_action(M.add_warranty_claim, conn, ns(
            complaint_description="No customer",
        ))
        assert is_error(r)

    def test_add_claim_missing_complaint(self, conn, env):
        r = call_action(M.add_warranty_claim, conn, ns(
            customer_id=env["customer_id"],
        ))
        assert is_error(r)


class TestListWarrantyClaims:
    def test_list_claims(self, conn, env):
        call_action(M.add_warranty_claim, conn, ns(
            customer_id=env["customer_id"],
            complaint_description="Broken hinge",
        ))
        r = call_action(M.list_warranty_claims, conn, ns())
        assert is_ok(r)
        assert r["total"] >= 1


# ===================================================================
# Maintenance Schedules
# ===================================================================

class TestAddMaintenanceSchedule:
    def test_add_schedule_ok(self, conn, env):
        r = call_action(M.add_maintenance_schedule, conn, ns(
            customer_id=env["customer_id"],
            start_date="2026-01-01",
            end_date="2026-12-31",
        ))
        assert is_ok(r)

    def test_add_schedule_missing_customer(self, conn, env):
        r = call_action(M.add_maintenance_schedule, conn, ns(
            start_date="2026-01-01",
            end_date="2026-12-31",
        ))
        assert is_error(r)

    def test_add_schedule_missing_dates(self, conn, env):
        r = call_action(M.add_maintenance_schedule, conn, ns(
            customer_id=env["customer_id"],
        ))
        assert is_error(r)


class TestListMaintenanceSchedules:
    def test_list_schedules(self, conn, env):
        r = call_action(M.list_maintenance_schedules, conn, ns())
        assert is_ok(r)


# ===================================================================
# Status
# ===================================================================

class TestStatus:
    def test_status_ok(self, conn, env):
        r = call_action(M.status, conn, ns())
        assert is_ok(r)
