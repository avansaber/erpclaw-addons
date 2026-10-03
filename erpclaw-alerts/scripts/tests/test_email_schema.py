"""M8 email substrate schema invariants (append-only log, constrained status,
no plaintext secrets). Lives here (not L0/constitution) because the email tables
are part of the erpclaw-alerts addon schema, not the foundation."""
import pytest
from alerts_helpers import get_conn  # noqa: F401  (conn fixture provided by conftest)

from erpclaw_lib import seam


def _cols(conn, table):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def test_email_tables_exist(db_path):
    for t in ("email_account", "email_template", "email_outbox", "email_log"):
        assert seam.table_exists(t, db_path), f"{t} missing on install"


def test_email_log_is_append_only(conn):
    cols = _cols(conn, "email_log")
    assert "updated_at" not in cols, "email_log must be append-only (no updated_at)"
    assert "event_type" in cols and "event_at" in cols


def test_outbox_status_enum_is_constrained(db_path):
    """email_outbox.status is limited to six values by ck_email_outbox_status."""
    checks = seam._inspector(db_path).get_check_constraints("email_outbox")
    match = [c for c in checks if c.get("name") == "ck_email_outbox_status"]
    assert match, "ck_email_outbox_status missing on email_outbox"
    sqltext = match[0].get("sqltext") or ""
    for st in ("queued", "sending", "sent", "bounced", "failed", "retry"):
        assert st in sqltext, f"{st} missing from ck_email_outbox_status: {sqltext}"


def test_account_has_no_plaintext_secret_column(conn):
    cols = _cols(conn, "email_account")
    for forbidden in ("password", "smtp_password", "secret", "api_key"):
        assert forbidden not in cols, f"email_account.{forbidden} must not be a column (use credentials store)"
