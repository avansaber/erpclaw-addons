"""Depth tests -- set-default-email-account / test-email-account.

The contract tests under testing/integration/contract/ only prove these two
actions are routable (they assert "Unknown action" is absent from the
response). The tests below prove what each action DOES to the database: which
row changed, from what to what, which rows did not change, and that refusals
write nothing.

Neither action touches money or the ledger (no GL legs exist for email
configuration or probe sends), so instead of balance assertions each test
asserts the email_outbox/email_log counts are unchanged. A comment at each
site records that so a later reader does not add a ledger assertion that
cannot hold.
"""
import importlib.util
import os
import uuid
from unittest.mock import patch

import pytest
from alerts_helpers import call_action, ns, is_ok, is_error, seed_company
from erpclaw_lib import seam

_SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_email():
    spec = importlib.util.spec_from_file_location(
        "email_sender_depth", os.path.join(_SCRIPTS, "email_sender.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


es = _load_email()


@pytest.fixture
def envc(conn):
    cid = seed_company(conn)
    return {"company_id": cid}


def _acct(conn, envc, name, from_address, is_default):
    r = call_action(es.add_email_account, conn, ns(
        company_id=envc["company_id"], name=name, from_address=from_address,
        provider="smtp", reply_to=None, is_default=is_default, smtp_password=None,
        config_json='{"host": "localhost", "port": 1025, "use_tls": false, "username": "u"}',
        from_=None))
    assert is_ok(r), r
    return r["email_account_id"]


def _account_row(conn, account_id):
    row = conn.execute("SELECT * FROM email_account WHERE id = ?", (account_id,)).fetchone()
    assert row is not None
    return dict(row)


def _snapshot_accounts(conn):
    return [dict(r) for r in conn.execute("SELECT * FROM email_account ORDER BY id").fetchall()]


def _queue_counts(conn):
    return {
        "outbox": conn.execute("SELECT COUNT(*) FROM email_outbox").fetchone()[0],
        "log": conn.execute("SELECT COUNT(*) FROM email_log").fetchone()[0],
    }


def test_set_default_moves_flag_and_preserves_other_columns(conn, db_path, envc):
    assert seam.table_exists("email_account", db_path)
    first_id = _acct(conn, envc, "Primary SMTP", "ops@acme.test", True)
    second_id = _acct(conn, envc, "Backup SMTP", "backup@acme.test", False)
    before_first = _account_row(conn, first_id)
    before_second = _account_row(conn, second_id)
    assert before_first["is_default"] == 1
    assert before_second["is_default"] == 0
    counts_before = _queue_counts(conn)

    r = call_action(es.set_default_email_account, conn, ns(account_id=second_id))
    assert is_ok(r), r
    assert r["result"] == "default_set"
    assert r["email_account_id"] == second_id

    after_first = _account_row(conn, first_id)
    after_second = _account_row(conn, second_id)
    assert after_second["is_default"] == 1
    assert after_first["is_default"] == 0
    assert {k: v for k, v in after_first.items() if k != "is_default"} == \
        {k: v for k, v in before_first.items() if k != "is_default"}
    assert {k: v for k, v in after_second.items() if k not in ("is_default", "updated_at")} == \
        {k: v for k, v in before_second.items() if k not in ("is_default", "updated_at")}
    only_default = conn.execute(
        "SELECT id FROM email_account WHERE company_id = ? AND is_default = 1",
        (envc["company_id"],)).fetchall()
    assert [row["id"] for row in only_default] == [second_id]
    assert _queue_counts(conn) == counts_before
    # No ledger or money effect: setting the default email account writes no GL
    # legs and email_account carries no monetary columns, so there is nothing
    # to balance here.


def test_set_default_refuses_unknown_account_without_writing(conn, envc):
    _acct(conn, envc, "Primary SMTP", "ops@acme.test", True)
    before = _snapshot_accounts(conn)
    counts_before = _queue_counts(conn)
    missing = str(uuid.uuid4())

    r = call_action(es.set_default_email_account, conn, ns(account_id=missing))
    assert is_error(r), r
    assert "not found" in r["message"]
    assert missing in r["message"]

    assert _snapshot_accounts(conn) == before
    assert _queue_counts(conn) == counts_before


def test_probe_success_records_ok_health_and_preserves_other_columns(conn, envc):
    account_id = _acct(conn, envc, "Primary SMTP", "ops@acme.test", True)
    before = _account_row(conn, account_id)
    assert before["last_health_status"] is None
    counts_before = _queue_counts(conn)

    with patch.object(es, "_send_via_provider",
                      return_value=(True, "msg-depth-ok-1")) as sender:
        r = call_action(es.test_email_account, conn,
                        ns(account_id=account_id, to="probe@example.test"))
    assert is_ok(r), r
    assert r["result"] == "ok"
    assert r["provider_message_id"] == "msg-depth-ok-1"
    sender.assert_called_once()
    assert sender.call_args[0][2] == "probe@example.test"

    after = _account_row(conn, account_id)
    assert after["last_health_status"] == "ok"
    assert after["last_health_check_at"] is not None
    assert after["last_health_check_at"] != before["last_health_check_at"]
    assert {k: v for k, v in after.items()
            if k not in ("last_health_check_at", "last_health_status", "updated_at")} == \
        {k: v for k, v in before.items()
         if k not in ("last_health_check_at", "last_health_status", "updated_at")}
    assert _queue_counts(conn) == counts_before
    # No ledger or money effect: a probe send records health on the account row
    # only; it enqueues nothing and posts no GL legs, so there is nothing to
    # balance here.


def test_probe_failure_records_error_health_but_reports_error(conn, envc):
    account_id = _acct(conn, envc, "Primary SMTP", "ops@acme.test", True)
    before = _account_row(conn, account_id)

    with patch.object(es, "_send_via_provider",
                      return_value=(False, "smtp refused: connection refused")):
        r = call_action(es.test_email_account, conn,
                        ns(account_id=account_id, to="probe@example.test"))
    assert is_error(r), r
    assert "connection refused" in r["message"]

    after = _account_row(conn, account_id)
    assert after["last_health_status"] == "error"
    assert after["last_health_check_at"] is not None
    assert {k: v for k, v in after.items()
            if k not in ("last_health_check_at", "last_health_status", "updated_at")} == \
        {k: v for k, v in before.items()
         if k not in ("last_health_check_at", "last_health_status", "updated_at")}
    assert _queue_counts(conn) == {"outbox": 0, "log": 0}


def test_probe_refuses_unknown_account_without_writing_or_sending(conn, envc):
    _acct(conn, envc, "Primary SMTP", "ops@acme.test", True)
    before = _snapshot_accounts(conn)
    counts_before = _queue_counts(conn)
    missing = str(uuid.uuid4())

    with patch.object(es, "_send_via_provider",
                      side_effect=AssertionError("provider must not be called on refusal")):
        r = call_action(es.test_email_account, conn,
                        ns(account_id=missing, to="probe@example.test"))
    assert is_error(r), r
    assert "not found" in r["message"]
    assert missing in r["message"]

    assert _snapshot_accounts(conn) == before
    assert _queue_counts(conn) == counts_before


def test_probe_refuses_missing_recipient_without_writing_or_sending(conn, envc):
    account_id = _acct(conn, envc, "Primary SMTP", "ops@acme.test", True)
    before = _snapshot_accounts(conn)
    counts_before = _queue_counts(conn)

    with patch.object(es, "_send_via_provider",
                      side_effect=AssertionError("provider must not be called on refusal")):
        r = call_action(es.test_email_account, conn, ns(account_id=account_id, to=None))
    assert is_error(r), r
    assert "--to" in r["message"]

    assert _snapshot_accounts(conn) == before
    assert _queue_counts(conn) == counts_before
