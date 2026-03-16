"""Tests for ERPClaw Approvals domain.

Actions tested (13 total):
  - approval-add-approval-rule
  - approval-update-approval-rule
  - approval-get-approval-rule
  - approval-list-approval-rules
  - approval-add-approval-step
  - approval-list-approval-steps
  - approval-submit-for-approval
  - approval-approve-request
  - approval-reject-request
  - approval-cancel-request
  - approval-list-approval-requests
  - approval-get-approval-request
  - status
"""
import json
import pytest
from approvals_helpers import call_action, ns, is_error, is_ok, load_db_query

mod = load_db_query()


# ─────────────────────────────────────────────────────────────────────────────
# Approval Rules
# ─────────────────────────────────────────────────────────────────────────────

class TestAddApprovalRule:
    def test_create_basic(self, conn, env):
        result = call_action(mod.approval_add_approval_rule, conn, ns(
            company_id=env["company_id"],
            name="Purchase Order Approval",
            entity_type="purchase_order",
            conditions='{"amount":">1000"}',
        ))
        assert is_ok(result), result
        assert result["name"] == "Purchase Order Approval"
        assert result["is_active"] == 1
        assert "id" in result

    def test_create_without_entity_type(self, conn, env):
        result = call_action(mod.approval_add_approval_rule, conn, ns(
            company_id=env["company_id"],
            name="Generic Approval",
            entity_type=None,
            conditions=None,
        ))
        assert is_ok(result), result
        assert result["is_active"] == 1

    def test_missing_name_fails(self, conn, env):
        result = call_action(mod.approval_add_approval_rule, conn, ns(
            company_id=env["company_id"],
            name=None,
            entity_type="invoice",
            conditions=None,
        ))
        assert is_error(result), result

    def test_missing_company_fails(self, conn, env):
        result = call_action(mod.approval_add_approval_rule, conn, ns(
            company_id=None,
            name="No Company Rule",
            entity_type=None,
            conditions=None,
        ))
        assert is_error(result), result


class TestUpdateApprovalRule:
    def _create_rule(self, conn, env):
        result = call_action(mod.approval_add_approval_rule, conn, ns(
            company_id=env["company_id"],
            name="Update Me",
            entity_type="sales_order",
            conditions=None,
        ))
        assert is_ok(result), result
        return result["id"]

    def test_update_name(self, conn, env):
        rule_id = self._create_rule(conn, env)
        result = call_action(mod.approval_update_approval_rule, conn, ns(
            id=rule_id,
            name="Updated Rule Name",
            entity_type=None,
            conditions=None,
            is_active=None,
        ))
        assert is_ok(result), result
        assert "name" in result["updated_fields"]

    def test_deactivate_via_update(self, conn, env):
        rule_id = self._create_rule(conn, env)
        result = call_action(mod.approval_update_approval_rule, conn, ns(
            id=rule_id,
            name=None,
            entity_type=None,
            conditions=None,
            is_active=0,
        ))
        assert is_ok(result), result
        assert "is_active" in result["updated_fields"]

    def test_update_no_fields_fails(self, conn, env):
        rule_id = self._create_rule(conn, env)
        result = call_action(mod.approval_update_approval_rule, conn, ns(
            id=rule_id,
            name=None,
            entity_type=None,
            conditions=None,
            is_active=None,
        ))
        assert is_error(result), result

    def test_update_nonexistent_fails(self, conn, env):
        result = call_action(mod.approval_update_approval_rule, conn, ns(
            id="nonexistent-rule-id",
            name="Ghost",
            entity_type=None,
            conditions=None,
            is_active=None,
        ))
        assert is_error(result), result


class TestGetApprovalRule:
    def test_get_existing(self, conn, env):
        create = call_action(mod.approval_add_approval_rule, conn, ns(
            company_id=env["company_id"],
            name="Fetchable Rule",
            entity_type="expense_claim",
            conditions=None,
        ))
        assert is_ok(create), create

        result = call_action(mod.approval_get_approval_rule, conn, ns(
            id=create["id"],
        ))
        assert is_ok(result), result
        assert result["name"] == "Fetchable Rule"
        assert result["steps"] == []
        assert result["step_count"] == 0

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.approval_get_approval_rule, conn, ns(
            id="nonexistent",
        ))
        assert is_error(result), result


class TestListApprovalRules:
    def test_list_empty(self, conn, env):
        result = call_action(mod.approval_list_approval_rules, conn, ns(
            company_id=env["company_id"],
            entity_type=None,
            search=None,
            is_active=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(result), result
        assert result["total_count"] == 0
        assert result["rows"] == []

    def test_list_with_data(self, conn, env):
        call_action(mod.approval_add_approval_rule, conn, ns(
            company_id=env["company_id"],
            name="Rule A",
            entity_type="invoice",
            conditions=None,
        ))
        call_action(mod.approval_add_approval_rule, conn, ns(
            company_id=env["company_id"],
            name="Rule B",
            entity_type="purchase_order",
            conditions=None,
        ))
        result = call_action(mod.approval_list_approval_rules, conn, ns(
            company_id=env["company_id"],
            entity_type=None,
            search=None,
            is_active=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(result), result
        assert result["total_count"] == 2


# ─────────────────────────────────────────────────────────────────────────────
# Approval Steps
# ─────────────────────────────────────────────────────────────────────────────

class TestAddApprovalStep:
    def _create_rule(self, conn, env):
        result = call_action(mod.approval_add_approval_rule, conn, ns(
            company_id=env["company_id"],
            name="Step Test Rule",
            entity_type="invoice",
            conditions=None,
        ))
        assert is_ok(result), result
        return result["id"]

    def test_add_step_basic(self, conn, env):
        rule_id = self._create_rule(conn, env)
        result = call_action(mod.approval_add_approval_step, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            approver="manager@example.com",
            approval_type="sequential",
            step_order=1,
            is_required=1,
        ))
        assert is_ok(result), result
        assert result["rule_id"] == rule_id
        assert result["approver"] == "manager@example.com"
        assert result["step_order"] == 1
        assert result["approval_type"] == "sequential"

    def test_add_parallel_step(self, conn, env):
        rule_id = self._create_rule(conn, env)
        result = call_action(mod.approval_add_approval_step, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            approver="cfo@example.com",
            approval_type="parallel",
            step_order=2,
            is_required=None,
        ))
        assert is_ok(result), result
        assert result["approval_type"] == "parallel"

    def test_missing_approver_fails(self, conn, env):
        rule_id = self._create_rule(conn, env)
        result = call_action(mod.approval_add_approval_step, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            approver=None,
            approval_type=None,
            step_order=1,
            is_required=None,
        ))
        assert is_error(result), result

    def test_missing_rule_id_fails(self, conn, env):
        result = call_action(mod.approval_add_approval_step, conn, ns(
            rule_id=None,
            company_id=env["company_id"],
            approver="someone@example.com",
            approval_type=None,
            step_order=1,
            is_required=None,
        ))
        assert is_error(result), result


class TestListApprovalSteps:
    def test_list_steps_for_rule(self, conn, env):
        rule = call_action(mod.approval_add_approval_rule, conn, ns(
            company_id=env["company_id"],
            name="Steps List Rule",
            entity_type="invoice",
            conditions=None,
        ))
        assert is_ok(rule), rule
        rule_id = rule["id"]

        call_action(mod.approval_add_approval_step, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            approver="step1@example.com",
            approval_type=None,
            step_order=1,
            is_required=None,
        ))
        call_action(mod.approval_add_approval_step, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            approver="step2@example.com",
            approval_type=None,
            step_order=2,
            is_required=None,
        ))

        result = call_action(mod.approval_list_approval_steps, conn, ns(
            rule_id=rule_id,
            company_id=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(result), result
        assert result["total_count"] == 2


# ─────────────────────────────────────────────────────────────────────────────
# Submit / Approve / Reject / Cancel Workflow
# ─────────────────────────────────────────────────────────────────────────────

def _create_rule_with_steps(conn, env, num_steps=2):
    """Helper: create a rule with N sequential steps."""
    rule = call_action(mod.approval_add_approval_rule, conn, ns(
        company_id=env["company_id"],
        name="Workflow Rule",
        entity_type="invoice",
        conditions=None,
    ))
    assert is_ok(rule), rule
    rule_id = rule["id"]

    for i in range(1, num_steps + 1):
        step = call_action(mod.approval_add_approval_step, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            approver=f"approver{i}@example.com",
            approval_type="sequential",
            step_order=i,
            is_required=1,
        ))
        assert is_ok(step), step

    return rule_id


class TestSubmitForApproval:
    def test_submit_basic(self, conn, env):
        rule_id = _create_rule_with_steps(conn, env, num_steps=2)
        result = call_action(mod.approval_submit_for_approval, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            entity_type="invoice",
            entity_id="inv-001",
            requested_by="user@example.com",
            notes="Please approve this invoice",
        ))
        assert is_ok(result), result
        assert result["request_status"] == "pending"
        assert result["current_step"] == 1
        assert "naming_series" in result

    def test_submit_inactive_rule_fails(self, conn, env):
        rule_id = _create_rule_with_steps(conn, env, num_steps=1)
        # Deactivate the rule
        call_action(mod.approval_update_approval_rule, conn, ns(
            id=rule_id,
            name=None,
            entity_type=None,
            conditions=None,
            is_active=0,
        ))
        result = call_action(mod.approval_submit_for_approval, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            entity_type="invoice",
            entity_id="inv-002",
            requested_by="user@example.com",
            notes=None,
        ))
        assert is_error(result), result

    def test_submit_no_steps_fails(self, conn, env):
        rule = call_action(mod.approval_add_approval_rule, conn, ns(
            company_id=env["company_id"],
            name="No Steps Rule",
            entity_type="invoice",
            conditions=None,
        ))
        assert is_ok(rule), rule
        result = call_action(mod.approval_submit_for_approval, conn, ns(
            rule_id=rule["id"],
            company_id=env["company_id"],
            entity_type="invoice",
            entity_id="inv-003",
            requested_by="user@example.com",
            notes=None,
        ))
        assert is_error(result), result


class TestApproveRequest:
    def test_single_step_approve(self, conn, env):
        rule_id = _create_rule_with_steps(conn, env, num_steps=1)
        submit = call_action(mod.approval_submit_for_approval, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            entity_type="invoice",
            entity_id="inv-100",
            requested_by="user@example.com",
            notes=None,
        ))
        assert is_ok(submit), submit

        result = call_action(mod.approval_approve_request, conn, ns(
            id=submit["id"],
            notes="Approved by manager",
        ))
        assert is_ok(result), result
        assert result["request_status"] == "approved"

    def test_multi_step_approve(self, conn, env):
        rule_id = _create_rule_with_steps(conn, env, num_steps=2)
        submit = call_action(mod.approval_submit_for_approval, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            entity_type="invoice",
            entity_id="inv-200",
            requested_by="user@example.com",
            notes=None,
        ))
        assert is_ok(submit), submit

        # First approval -- advances to step 2
        r1 = call_action(mod.approval_approve_request, conn, ns(
            id=submit["id"],
            notes=None,
        ))
        assert is_ok(r1), r1
        assert r1["request_status"] == "in_progress"
        assert r1["current_step"] == 2

        # Second approval -- final approval
        r2 = call_action(mod.approval_approve_request, conn, ns(
            id=submit["id"],
            notes="Final approval",
        ))
        assert is_ok(r2), r2
        assert r2["request_status"] == "approved"


class TestRejectRequest:
    def test_reject(self, conn, env):
        rule_id = _create_rule_with_steps(conn, env, num_steps=1)
        submit = call_action(mod.approval_submit_for_approval, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            entity_type="expense",
            entity_id="exp-001",
            requested_by="employee@example.com",
            notes=None,
        ))
        assert is_ok(submit), submit

        result = call_action(mod.approval_reject_request, conn, ns(
            id=submit["id"],
            notes="Insufficient documentation",
        ))
        assert is_ok(result), result
        assert result["request_status"] == "rejected"

    def test_reject_already_approved_fails(self, conn, env):
        rule_id = _create_rule_with_steps(conn, env, num_steps=1)
        submit = call_action(mod.approval_submit_for_approval, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            entity_type="expense",
            entity_id="exp-002",
            requested_by="employee@example.com",
            notes=None,
        ))
        assert is_ok(submit), submit

        # Approve first
        call_action(mod.approval_approve_request, conn, ns(
            id=submit["id"], notes=None,
        ))
        # Try to reject
        result = call_action(mod.approval_reject_request, conn, ns(
            id=submit["id"],
            notes="Too late",
        ))
        assert is_error(result), result


class TestCancelRequest:
    def test_cancel_pending(self, conn, env):
        rule_id = _create_rule_with_steps(conn, env, num_steps=1)
        submit = call_action(mod.approval_submit_for_approval, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            entity_type="invoice",
            entity_id="inv-cancel",
            requested_by="user@example.com",
            notes=None,
        ))
        assert is_ok(submit), submit

        result = call_action(mod.approval_cancel_request, conn, ns(
            id=submit["id"],
        ))
        assert is_ok(result), result
        assert result["request_status"] == "cancelled"

    def test_cancel_already_cancelled_fails(self, conn, env):
        rule_id = _create_rule_with_steps(conn, env, num_steps=1)
        submit = call_action(mod.approval_submit_for_approval, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            entity_type="invoice",
            entity_id="inv-cc",
            requested_by="user@example.com",
            notes=None,
        ))
        assert is_ok(submit), submit

        call_action(mod.approval_cancel_request, conn, ns(id=submit["id"]))
        result = call_action(mod.approval_cancel_request, conn, ns(id=submit["id"]))
        assert is_error(result), result


# ─────────────────────────────────────────────────────────────────────────────
# List / Get Requests
# ─────────────────────────────────────────────────────────────────────────────

class TestListApprovalRequests:
    def test_list_empty(self, conn, env):
        result = call_action(mod.approval_list_approval_requests, conn, ns(
            company_id=env["company_id"],
            status=None,
            entity_type=None,
            rule_id=None,
            search=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(result), result
        assert result["total_count"] == 0

    def test_list_filter_by_status(self, conn, env):
        rule_id = _create_rule_with_steps(conn, env, num_steps=1)
        submit = call_action(mod.approval_submit_for_approval, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            entity_type="invoice",
            entity_id="inv-filter",
            requested_by="user@example.com",
            notes=None,
        ))
        assert is_ok(submit), submit

        result = call_action(mod.approval_list_approval_requests, conn, ns(
            company_id=env["company_id"],
            status="pending",
            entity_type=None,
            rule_id=None,
            search=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(result), result
        assert result["total_count"] == 1


class TestGetApprovalRequest:
    def test_get_request(self, conn, env):
        rule_id = _create_rule_with_steps(conn, env, num_steps=1)
        submit = call_action(mod.approval_submit_for_approval, conn, ns(
            rule_id=rule_id,
            company_id=env["company_id"],
            entity_type="invoice",
            entity_id="inv-get",
            requested_by="user@example.com",
            notes="Test request",
        ))
        assert is_ok(submit), submit

        result = call_action(mod.approval_get_approval_request, conn, ns(
            id=submit["id"],
        ))
        assert is_ok(result), result
        assert result["request_status"] == "pending"
        assert result["rule_name"] == "Workflow Rule"
        assert result["total_steps"] == 1
        assert len(result["steps"]) == 1

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.approval_get_approval_request, conn, ns(
            id="nonexistent",
        ))
        assert is_error(result), result


# ─────────────────────────────────────────────────────────────────────────────
# Status
# ─────────────────────────────────────────────────────────────────────────────

class TestStatus:
    def test_status(self, conn, env):
        result = call_action(mod.status, conn, ns())
        assert is_ok(result), result
        assert result["skill"] == "erpclaw-approvals"
        assert result["total_tables"] == 3
