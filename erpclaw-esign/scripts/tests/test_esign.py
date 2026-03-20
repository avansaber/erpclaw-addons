"""Tests for ERPClaw E-Sign domain.

Actions tested (13 total):
  - esign-add-signature-request
  - esign-update-signature-request
  - esign-get-signature-request
  - esign-list-signature-requests
  - esign-send-signature-request
  - esign-sign-document
  - esign-decline-signature
  - esign-cancel-signature-request
  - esign-void-signature-request
  - esign-add-reminder
  - esign-get-signature-audit-trail
  - esign-signature-summary-report
  - status
"""
import json
import pytest
from esign_helpers import call_action, ns, is_error, is_ok, load_db_query

mod = load_db_query()

SIGNERS_ONE = json.dumps([{"email": "alice@example.com", "name": "Alice"}])
SIGNERS_TWO = json.dumps([
    {"email": "alice@example.com", "name": "Alice"},
    {"email": "bob@example.com", "name": "Bob"},
])


# ─────────────────────────────────────────────────────────────────────────────
# Signature Requests -- CRUD
# ─────────────────────────────────────────────────────────────────────────────

class TestAddSignatureRequest:
    def test_create_basic(self, conn, env):
        result = call_action(mod.esign_add_signature_request, conn, ns(
            company_id=env["company_id"],
            document_name="Service Agreement",
            document_type="contract",
            document_id="doc-001",
            signers=SIGNERS_ONE,
            requested_by="sender@example.com",
            message="Please sign this agreement",
            expires_at="2026-12-31",
        ))
        assert is_ok(result), result
        assert result["request_status"] == "draft"
        assert result["total_signers"] == 1
        assert "request_id" in result
        assert "naming_series" in result

    def test_create_with_two_signers(self, conn, env):
        result = call_action(mod.esign_add_signature_request, conn, ns(
            company_id=env["company_id"],
            document_name="Partnership Agreement",
            document_type="legal",
            document_id=None,
            signers=SIGNERS_TWO,
            requested_by="legal@example.com",
            message=None,
            expires_at=None,
        ))
        assert is_ok(result), result
        assert result["total_signers"] == 2

    def test_missing_document_name_fails(self, conn, env):
        result = call_action(mod.esign_add_signature_request, conn, ns(
            company_id=env["company_id"],
            document_name=None,
            document_type=None,
            document_id=None,
            signers=SIGNERS_ONE,
            requested_by="sender@example.com",
            message=None,
            expires_at=None,
        ))
        assert is_error(result), result

    def test_missing_signers_fails(self, conn, env):
        result = call_action(mod.esign_add_signature_request, conn, ns(
            company_id=env["company_id"],
            document_name="No Signers Doc",
            document_type=None,
            document_id=None,
            signers=None,
            requested_by="sender@example.com",
            message=None,
            expires_at=None,
        ))
        assert is_error(result), result

    def test_missing_requested_by_fails(self, conn, env):
        result = call_action(mod.esign_add_signature_request, conn, ns(
            company_id=env["company_id"],
            document_name="No Requester Doc",
            document_type=None,
            document_id=None,
            signers=SIGNERS_ONE,
            requested_by=None,
            message=None,
            expires_at=None,
        ))
        assert is_error(result), result

    def test_invalid_signers_json_fails(self, conn, env):
        result = call_action(mod.esign_add_signature_request, conn, ns(
            company_id=env["company_id"],
            document_name="Bad Signers",
            document_type=None,
            document_id=None,
            signers="not-valid-json",
            requested_by="sender@example.com",
            message=None,
            expires_at=None,
        ))
        assert is_error(result), result

    def test_missing_company_fails(self, conn, env):
        result = call_action(mod.esign_add_signature_request, conn, ns(
            company_id=None,
            document_name="No Company",
            document_type=None,
            document_id=None,
            signers=SIGNERS_ONE,
            requested_by="sender@example.com",
            message=None,
            expires_at=None,
        ))
        assert is_error(result), result


def _create_draft(conn, env, signers=None):
    """Helper to create a draft signature request, returns request_id."""
    result = call_action(mod.esign_add_signature_request, conn, ns(
        company_id=env["company_id"],
        document_name="Test Document",
        document_type="general",
        document_id=None,
        signers=signers or SIGNERS_TWO,
        requested_by="sender@example.com",
        message=None,
        expires_at=None,
    ))
    assert is_ok(result), result
    return result["request_id"]


class TestUpdateSignatureRequest:
    def test_update_document_name(self, conn, env):
        req_id = _create_draft(conn, env)
        result = call_action(mod.esign_update_signature_request, conn, ns(
            request_id=req_id,
            document_name="Updated Doc Name",
            document_type=None,
            document_id=None,
            message=None,
            expires_at=None,
            signers=None,
        ))
        assert is_ok(result), result
        assert "document_name" in result["updated_fields"]

    def test_update_no_fields_fails(self, conn, env):
        req_id = _create_draft(conn, env)
        result = call_action(mod.esign_update_signature_request, conn, ns(
            request_id=req_id,
            document_name=None,
            document_type=None,
            document_id=None,
            message=None,
            expires_at=None,
            signers=None,
        ))
        assert is_error(result), result

    def test_update_sent_request_fails(self, conn, env):
        req_id = _create_draft(conn, env)
        # Send it first
        call_action(mod.esign_send_signature_request, conn, ns(request_id=req_id))
        # Try to update
        result = call_action(mod.esign_update_signature_request, conn, ns(
            request_id=req_id,
            document_name="Should Fail",
            document_type=None,
            document_id=None,
            message=None,
            expires_at=None,
            signers=None,
        ))
        assert is_error(result), result


class TestGetSignatureRequest:
    def test_get_existing(self, conn, env):
        req_id = _create_draft(conn, env)
        result = call_action(mod.esign_get_signature_request, conn, ns(
            request_id=req_id,
        ))
        assert is_ok(result), result
        assert result["document_name"] == "Test Document"
        assert result["request_status"] == "draft"
        assert isinstance(result["signers"], list)
        assert len(result["signers"]) == 2
        assert result["event_count"] >= 1  # created event

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.esign_get_signature_request, conn, ns(
            request_id="nonexistent",
        ))
        assert is_error(result), result


class TestListSignatureRequests:
    def test_list_empty(self, conn, env):
        result = call_action(mod.esign_list_signature_requests, conn, ns(
            company_id=env["company_id"],
            request_status=None,
            requested_by=None,
            document_type=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(result), result
        assert result["total_count"] == 0

    def test_list_with_data(self, conn, env):
        _create_draft(conn, env)
        _create_draft(conn, env)
        result = call_action(mod.esign_list_signature_requests, conn, ns(
            company_id=env["company_id"],
            request_status=None,
            requested_by=None,
            document_type=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(result), result
        assert result["total_count"] == 2


# ─────────────────────────────────────────────────────────────────────────────
# Send / Sign / Decline Workflow
# ─────────────────────────────────────────────────────────────────────────────

class TestSendSignatureRequest:
    def test_send_draft(self, conn, env):
        req_id = _create_draft(conn, env)
        result = call_action(mod.esign_send_signature_request, conn, ns(
            request_id=req_id,
        ))
        assert is_ok(result), result
        assert result["request_status"] == "sent"

    def test_send_already_sent_fails(self, conn, env):
        req_id = _create_draft(conn, env)
        call_action(mod.esign_send_signature_request, conn, ns(request_id=req_id))
        result = call_action(mod.esign_send_signature_request, conn, ns(
            request_id=req_id,
        ))
        assert is_error(result), result


class TestSignDocument:
    def _send_request(self, conn, env, signers=None):
        req_id = _create_draft(conn, env, signers=signers)
        call_action(mod.esign_send_signature_request, conn, ns(request_id=req_id))
        return req_id

    def test_sign_single_signer(self, conn, env):
        req_id = self._send_request(conn, env, signers=SIGNERS_ONE)
        result = call_action(mod.esign_sign_document, conn, ns(
            request_id=req_id,
            signer_email="alice@example.com",
            signature_data="base64encodedSignatureData",
            ip_address="192.168.1.1",
            user_agent="TestBrowser/1.0",
        ))
        assert is_ok(result), result
        assert result["request_status"] == "completed"
        assert result["signed_count"] == 1
        assert result["total_signers"] == 1

    def test_sign_first_of_two(self, conn, env):
        req_id = self._send_request(conn, env, signers=SIGNERS_TWO)
        result = call_action(mod.esign_sign_document, conn, ns(
            request_id=req_id,
            signer_email="alice@example.com",
            signature_data="base64aliceSignature",
            ip_address=None,
            user_agent=None,
        ))
        assert is_ok(result), result
        assert result["request_status"] == "partially_signed"
        assert result["signed_count"] == 1

    def test_sign_completes_with_two(self, conn, env):
        req_id = self._send_request(conn, env, signers=SIGNERS_TWO)
        # First signer
        call_action(mod.esign_sign_document, conn, ns(
            request_id=req_id,
            signer_email="alice@example.com",
            signature_data="base64alice",
            ip_address=None,
            user_agent=None,
        ))
        # Second signer
        result = call_action(mod.esign_sign_document, conn, ns(
            request_id=req_id,
            signer_email="bob@example.com",
            signature_data="base64bob",
            ip_address=None,
            user_agent=None,
        ))
        assert is_ok(result), result
        assert result["request_status"] == "completed"
        assert result["signed_count"] == 2

    def test_sign_draft_fails(self, conn, env):
        req_id = _create_draft(conn, env)
        result = call_action(mod.esign_sign_document, conn, ns(
            request_id=req_id,
            signer_email="alice@example.com",
            signature_data="base64data",
            ip_address=None,
            user_agent=None,
        ))
        assert is_error(result), result

    def test_sign_unknown_signer_fails(self, conn, env):
        req_id = self._send_request(conn, env, signers=SIGNERS_ONE)
        result = call_action(mod.esign_sign_document, conn, ns(
            request_id=req_id,
            signer_email="unknown@example.com",
            signature_data="base64data",
            ip_address=None,
            user_agent=None,
        ))
        assert is_error(result), result

    def test_sign_already_signed_fails(self, conn, env):
        req_id = self._send_request(conn, env, signers=SIGNERS_TWO)
        call_action(mod.esign_sign_document, conn, ns(
            request_id=req_id,
            signer_email="alice@example.com",
            signature_data="base64alice",
            ip_address=None,
            user_agent=None,
        ))
        result = call_action(mod.esign_sign_document, conn, ns(
            request_id=req_id,
            signer_email="alice@example.com",
            signature_data="base64alice_again",
            ip_address=None,
            user_agent=None,
        ))
        assert is_error(result), result


class TestDeclineSignature:
    def test_decline(self, conn, env):
        req_id = _create_draft(conn, env, signers=SIGNERS_TWO)
        call_action(mod.esign_send_signature_request, conn, ns(request_id=req_id))

        result = call_action(mod.esign_decline_signature, conn, ns(
            request_id=req_id,
            signer_email="bob@example.com",
            notes="I do not agree with the terms",
            ip_address="10.0.0.1",
            user_agent="DeclineBrowser/1.0",
        ))
        assert is_ok(result), result
        assert result["request_status"] == "declined"
        assert result["signer_email"] == "bob@example.com"

    def test_decline_draft_fails(self, conn, env):
        req_id = _create_draft(conn, env)
        result = call_action(mod.esign_decline_signature, conn, ns(
            request_id=req_id,
            signer_email="alice@example.com",
            notes=None,
            ip_address=None,
            user_agent=None,
        ))
        assert is_error(result), result


# ─────────────────────────────────────────────────────────────────────────────
# Cancel / Void
# ─────────────────────────────────────────────────────────────────────────────

class TestCancelSignatureRequest:
    def test_cancel_draft(self, conn, env):
        req_id = _create_draft(conn, env)
        result = call_action(mod.esign_cancel_signature_request, conn, ns(
            request_id=req_id,
            notes="No longer needed",
        ))
        assert is_ok(result), result
        assert result["request_status"] == "cancelled"

    def test_cancel_sent(self, conn, env):
        req_id = _create_draft(conn, env)
        call_action(mod.esign_send_signature_request, conn, ns(request_id=req_id))
        result = call_action(mod.esign_cancel_signature_request, conn, ns(
            request_id=req_id,
            notes="Cancelled after sending",
        ))
        assert is_ok(result), result
        assert result["request_status"] == "cancelled"

    def test_cancel_already_cancelled_fails(self, conn, env):
        req_id = _create_draft(conn, env)
        call_action(mod.esign_cancel_signature_request, conn, ns(
            request_id=req_id, notes=None,
        ))
        result = call_action(mod.esign_cancel_signature_request, conn, ns(
            request_id=req_id, notes=None,
        ))
        assert is_error(result), result


class TestVoidSignatureRequest:
    def test_void_completed(self, conn, env):
        req_id = _create_draft(conn, env, signers=SIGNERS_ONE)
        call_action(mod.esign_send_signature_request, conn, ns(request_id=req_id))
        call_action(mod.esign_sign_document, conn, ns(
            request_id=req_id,
            signer_email="alice@example.com",
            signature_data="base64sig",
            ip_address=None,
            user_agent=None,
        ))
        result = call_action(mod.esign_void_signature_request, conn, ns(
            request_id=req_id,
            notes="Voiding completed request",
        ))
        assert is_ok(result), result
        assert result["request_status"] == "voided"

    def test_void_draft_fails(self, conn, env):
        req_id = _create_draft(conn, env)
        result = call_action(mod.esign_void_signature_request, conn, ns(
            request_id=req_id,
            notes=None,
        ))
        assert is_error(result), result


# ─────────────────────────────────────────────────────────────────────────────
# Reminders
# ─────────────────────────────────────────────────────────────────────────────

class TestAddReminder:
    def test_remind_sent_request(self, conn, env):
        req_id = _create_draft(conn, env, signers=SIGNERS_TWO)
        call_action(mod.esign_send_signature_request, conn, ns(request_id=req_id))

        result = call_action(mod.esign_add_reminder, conn, ns(
            request_id=req_id,
            signer_email="alice@example.com",
            notes="Please sign ASAP",
        ))
        assert is_ok(result), result
        assert result["reminder_sent"] is True
        assert result["signer_email"] == "alice@example.com"

    def test_remind_draft_fails(self, conn, env):
        req_id = _create_draft(conn, env)
        result = call_action(mod.esign_add_reminder, conn, ns(
            request_id=req_id,
            signer_email=None,
            notes=None,
        ))
        assert is_error(result), result


# ─────────────────────────────────────────────────────────────────────────────
# Audit Trail
# ─────────────────────────────────────────────────────────────────────────────

class TestGetSignatureAuditTrail:
    def test_audit_trail_for_signed_doc(self, conn, env):
        req_id = _create_draft(conn, env, signers=SIGNERS_ONE)
        call_action(mod.esign_send_signature_request, conn, ns(request_id=req_id))
        call_action(mod.esign_sign_document, conn, ns(
            request_id=req_id,
            signer_email="alice@example.com",
            signature_data="base64sig",
            ip_address="192.168.1.1",
            user_agent="Chrome/100",
        ))

        result = call_action(mod.esign_get_signature_audit_trail, conn, ns(
            request_id=req_id,
        ))
        assert is_ok(result), result
        assert result["request_status"] == "completed"
        # Events: created, sent, signed
        assert result["event_count"] >= 3
        event_types = [e["event_type"] for e in result["events"]]
        assert "created" in event_types
        assert "sent" in event_types
        assert "signed" in event_types

    def test_audit_trail_nonexistent_fails(self, conn, env):
        result = call_action(mod.esign_get_signature_audit_trail, conn, ns(
            request_id="nonexistent",
        ))
        assert is_error(result), result


# ─────────────────────────────────────────────────────────────────────────────
# Summary Report
# ─────────────────────────────────────────────────────────────────────────────

class TestSignatureSummaryReport:
    def test_empty_report(self, conn, env):
        result = call_action(mod.esign_signature_summary_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_requests"] == 0
        assert result["total_signatures"] == 0

    def test_report_with_data(self, conn, env):
        # Create, send, and sign one request
        req_id = _create_draft(conn, env, signers=SIGNERS_ONE)
        call_action(mod.esign_send_signature_request, conn, ns(request_id=req_id))
        call_action(mod.esign_sign_document, conn, ns(
            request_id=req_id,
            signer_email="alice@example.com",
            signature_data="base64sig",
            ip_address=None,
            user_agent=None,
        ))

        result = call_action(mod.esign_signature_summary_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_requests"] == 1
        assert result["total_signatures"] == 1
        assert result["by_status"]["completed"] == 1

    def test_missing_company_fails(self, conn, env):
        result = call_action(mod.esign_signature_summary_report, conn, ns(
            company_id=None,
        ))
        assert is_error(result), result


# ─────────────────────────────────────────────────────────────────────────────
# Status
# ─────────────────────────────────────────────────────────────────────────────

class TestStatus:
    def test_status(self, conn, env):
        result = call_action(mod.status, conn, ns())
        assert is_ok(result), result
        assert result["skill"] == "erpclaw-esign"
