"""M8 email substrate schema invariants (append-only log, constrained status,
no plaintext secrets). Lives here (not L0/constitution) because the email tables
are part of the erpclaw-alerts addon schema, not the foundation."""
import pytest
from alerts_helpers import get_conn  # noqa: F401  (conn fixture provided by conftest)


def _cols(conn, table):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def _sql(conn, table):
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row[0] if row else None


def test_email_tables_exist(conn):
    for t in ("email_account", "email_template", "email_outbox", "email_log"):
        assert _sql(conn, t) is not None, f"{t} missing on install"


def test_email_log_is_append_only(conn):
    cols = _cols(conn, "email_log")
    assert "updated_at" not in cols, "email_log must be append-only (no updated_at)"
    assert "event_type" in cols and "event_at" in cols


def test_outbox_status_enum_is_constrained(conn):
    sql = _sql(conn, "email_outbox")
    for st in ("queued", "sending", "sent", "bounced", "failed", "retry"):
        assert st in sql
    assert "CHECK(status IN" in sql


def test_account_has_no_plaintext_secret_column(conn):
    cols = _cols(conn, "email_account")
    for forbidden in ("password", "smtp_password", "secret", "api_key"):
        assert forbidden not in cols, f"email_account.{forbidden} must not be a column (use credentials store)"
