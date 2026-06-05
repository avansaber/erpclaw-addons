"""M8 phase A tests — email substrate (account/template/outbox/queue worker).

Provider send is patched (no real SMTP in CI) via the email_sender._send_via_provider
seam. Covers enqueue, successful send, backoff-on-failure, append-only log, and
template merge-var rendering.
"""
import importlib.util
import os
import argparse
import pytest
from unittest.mock import patch

from alerts_helpers import init_all_tables, get_conn, call_action, ns, is_ok, is_error, seed_company

_SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_email():
    spec = importlib.util.spec_from_file_location(
        "email_sender", os.path.join(_SCRIPTS, "email_sender.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


es = _load_email()


@pytest.fixture
def envc(conn):
    cid = seed_company(conn)
    return {"company_id": cid}


def _acct(conn, envc, **extra):
    base = dict(company_id=envc["company_id"], name="Primary SMTP", from_address="ops@acme.test",
                provider="smtp", reply_to=None, is_default=True, smtp_password=None,
                config_json='{"host": "localhost", "port": 1025, "use_tls": false, "username": "u"}',
                from_=None)
    base.update(extra)
    r = call_action(es.add_email_account, conn, ns(**base))
    return r


def test_add_account_and_template(conn, envc):
    r = _acct(conn, envc)
    assert is_ok(r) and r["result"] == "registered" and r["is_default"]
    t = call_action(es.add_email_template, conn, ns(
        name="Welcome", subject="Hi {{name}}", body_text="Hello {{name}}, welcome.",
        body_html="", merge_field_list_json='["name"]', language="en", company_id=envc["company_id"]))
    assert is_ok(t) and t["result"] == "created"
    lst = call_action(es.list_email_templates, conn, ns(company_id=envc["company_id"]))
    assert lst["count"] == 1


def test_send_enqueues_with_template_merge(conn, envc):
    _acct(conn, envc)
    t = call_action(es.add_email_template, conn, ns(
        name="W", subject="Hi {{name}}", body_text="Hello {{name}}", body_html="",
        merge_field_list_json="[]", language="en", company_id=envc["company_id"]))
    tid = t["email_template_id"]
    s = call_action(es.send_email, conn, ns(
        to="cust@acme.test", template_id=tid, subject=None, body_text=None, body_html=None,
        merge_vars='{"name": "Dana"}', from_account=None, company_id=envc["company_id"]))
    assert is_ok(s) and s["result"] == "queued"
    ob = conn.execute("SELECT subject, body_text, status FROM email_outbox WHERE id=?",
                      (s["email_outbox_id"],)).fetchone()
    assert ob["subject"] == "Hi Dana" and ob["body_text"] == "Hello Dana" and ob["status"] == "queued"
    # queued event logged
    assert conn.execute("SELECT COUNT(*) FROM email_log WHERE email_outbox_id=? AND event_type='queued'",
                        (s["email_outbox_id"],)).fetchone()[0] == 1


def test_template_xor_inline_validation(conn, envc):
    _acct(conn, envc)
    # both template and inline -> error
    assert is_error(call_action(es.send_email, conn, ns(
        to="x@y.z", template_id="t", subject="s", body_text="b", body_html=None,
        merge_vars=None, from_account=None, company_id=envc["company_id"])))
    # neither -> error
    assert is_error(call_action(es.send_email, conn, ns(
        to="x@y.z", template_id=None, subject=None, body_text=None, body_html=None,
        merge_vars=None, from_account=None, company_id=envc["company_id"])))


def test_send_requires_an_account(conn, envc):
    # no account configured at all
    assert is_error(call_action(es.send_email, conn, ns(
        to="x@y.z", template_id=None, subject="s", body_text="b", body_html=None,
        merge_vars=None, from_account=None, company_id=envc["company_id"])))


def test_queue_worker_sends_on_success(conn, envc):
    _acct(conn, envc)
    s = call_action(es.send_email, conn, ns(
        to="cust@acme.test", template_id=None, subject="Hi", body_text="Body", body_html=None,
        merge_vars=None, from_account=None, company_id=envc["company_id"]))
    oid = s["email_outbox_id"]
    with patch.object(es, "_send_via_provider", return_value=(True, "msg-123")):
        r = call_action(es.process_email_queue, conn, ns(limit=10, from_account=None))
    assert is_ok(r) and r["sent"] == 1 and r["failed"] == 0
    ob = conn.execute("SELECT status, provider_message_id, attempt_count FROM email_outbox WHERE id=?",
                      (oid,)).fetchone()
    assert ob["status"] == "sent" and ob["provider_message_id"] == "msg-123" and ob["attempt_count"] == 1
    assert conn.execute("SELECT COUNT(*) FROM email_log WHERE email_outbox_id=? AND event_type='sent'",
                        (oid,)).fetchone()[0] == 1


def test_queue_worker_backoff_on_failure(conn, envc):
    _acct(conn, envc)
    s = call_action(es.send_email, conn, ns(
        to="cust@acme.test", template_id=None, subject="Hi", body_text="Body", body_html=None,
        merge_vars=None, from_account=None, company_id=envc["company_id"]))
    oid = s["email_outbox_id"]
    with patch.object(es, "_send_via_provider", return_value=(False, "connection refused")):
        r = call_action(es.process_email_queue, conn, ns(limit=10, from_account=None))
    assert r["retry"] == 1 and r["sent"] == 0
    ob = conn.execute("SELECT status, attempt_count, next_attempt_at, error_message FROM email_outbox WHERE id=?",
                      (oid,)).fetchone()
    assert ob["status"] == "retry" and ob["attempt_count"] == 1 and ob["next_attempt_at"] is not None
    assert "connection refused" in ob["error_message"]
