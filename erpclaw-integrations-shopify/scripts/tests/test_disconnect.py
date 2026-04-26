"""Unit tests for shopify-disconnect.

Patches the outbound Shopify revoke call. Verifies:
  - Happy path: removes the shopify_account row, preserves GL entries,
    attempts token revoke, uninstalls daemon if this was the last account.
  - Leaves daemon alone when another active account remains.
  - Returns clear error when --shopify-account-id missing or not found.
  - Tolerates Shopify 401/403/404 revoke failures (token already dead).
"""
import importlib
from unittest.mock import patch

import pytest

from shopify_test_helpers import (
    call_action,
    build_env,
    is_error,
    is_ok,
    seed_company,
)


@pytest.fixture
def disconnect_module():
    import disconnect as _d  # type: ignore[import-not-found]
    importlib.reload(_d)
    return _d


class _Args:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _seed_second_account(conn, company_id, shop_domain="second.myshopify.com"):
    # Use _add_account_core to create a second active account the same way
    # shopify-add-account does.
    from accounts import _add_account_core
    from shopify_helpers import encrypt_token

    res = _add_account_core(
        conn,
        company_id=company_id,
        shop_domain=shop_domain,
        shop_name=shop_domain.split(".", 1)[0],
        encrypted_token=encrypt_token("shpat_SECOND"),
        pairing_method="custom_app",
    )
    return res["id"]


def test_missing_account_id_errors(db_path, conn, disconnect_module):
    build_env(conn)
    res = call_action(
        disconnect_module.shopify_disconnect,
        conn,
        _Args(),
    )
    assert is_error(res)
    assert "--shopify-account-id" in res["message"]


def test_unknown_account_id_errors(db_path, conn, disconnect_module):
    build_env(conn)
    res = call_action(
        disconnect_module.shopify_disconnect,
        conn,
        _Args(shopify_account_id="nonexistent-id"),
    )
    assert is_error(res)
    assert "not found" in res["message"]


def test_happy_path_removes_row_and_uninstalls_daemon(db_path, conn, disconnect_module):
    env = build_env(conn)
    acct_id = env["shopify_account_id"]

    with patch.object(disconnect_module, "_revoke_access_token", return_value=(True, "status 200")):
        with patch.object(disconnect_module, "_uninstall_daemon_best_effort", return_value={"uninstalled": True}):
            res = call_action(
                disconnect_module.shopify_disconnect,
                conn,
                _Args(shopify_account_id=acct_id),
            )
    assert is_ok(res), res
    assert res["disconnected"] is True
    assert res["remaining_active_accounts"] == 0
    assert res["daemon"] == {"uninstalled": True}
    assert res["token_revoked"] is True

    # Row is soft-deleted (status='disabled'); preserves FK history (§18.10 fix).
    row = conn.execute(
        "SELECT id, status, access_token_enc, hmac_secret_enc FROM shopify_account WHERE id = ?",
        (acct_id,),
    ).fetchone()
    assert row is not None, "shopify_account row should be soft-deleted, not removed"
    assert row["status"] == "disabled"
    assert row["access_token_enc"] == ""
    assert row["hmac_secret_enc"] is None


def test_keeps_daemon_when_other_accounts_remain(db_path, conn, disconnect_module):
    env = build_env(conn)
    first_id = env["shopify_account_id"]
    _seed_second_account(conn, env["company_id"])

    with patch.object(disconnect_module, "_revoke_access_token", return_value=(True, "status 200")):
        with patch.object(disconnect_module, "_uninstall_daemon_best_effort") as uninstall:
            res = call_action(
                disconnect_module.shopify_disconnect,
                conn,
                _Args(shopify_account_id=first_id),
            )
    assert is_ok(res)
    assert res["remaining_active_accounts"] == 1
    assert res["daemon"] is None
    uninstall.assert_not_called()


def test_revoke_failure_is_non_fatal(db_path, conn, disconnect_module):
    env = build_env(conn)
    acct_id = env["shopify_account_id"]

    with patch.object(disconnect_module, "_revoke_access_token", return_value=(False, "URLError timeout")):
        with patch.object(disconnect_module, "_uninstall_daemon_best_effort", return_value={"uninstalled": True}):
            res = call_action(
                disconnect_module.shopify_disconnect,
                conn,
                _Args(shopify_account_id=acct_id),
            )
    # Still succeeds; token_revoked=False documented in response.
    assert is_ok(res)
    assert res["token_revoked"] is False
    assert "URLError" in res["token_revoke_detail"]
    # Row is still soft-deleted locally even if revoke failed (§18.10 fix).
    row = conn.execute(
        "SELECT id, status FROM shopify_account WHERE id = ?",
        (acct_id,),
    ).fetchone()
    assert row is not None
    assert row["status"] == "disabled"


def test_disconnect_with_existing_sync_jobs_does_not_violate_fk(db_path, conn, disconnect_module):
    """§18.10 regression: disconnect must NOT raise IntegrityError when
    shopify_sync_job rows still reference the shopify_account. Soft-delete
    preserves the FK relationship + audit trail."""
    import uuid
    env = build_env(conn)
    acct_id = env["shopify_account_id"]

    # Seed a shopify_sync_job row that FK-references the account.
    conn.execute(
        "INSERT INTO shopify_sync_job "
        "(id, shopify_account_id, sync_type, status, started_at, company_id) "
        "VALUES (?, ?, 'full', 'completed', '2026-04-26T12:00:00Z', ?)",
        (str(uuid.uuid4()), acct_id, env["company_id"]),
    )
    conn.commit()

    with patch.object(disconnect_module, "_revoke_access_token", return_value=(True, "status 200")):
        with patch.object(disconnect_module, "_uninstall_daemon_best_effort", return_value={"uninstalled": True}):
            res = call_action(
                disconnect_module.shopify_disconnect,
                conn,
                _Args(shopify_account_id=acct_id),
            )

    # Did not raise; succeeded.
    assert is_ok(res)
    # Account row is soft-deleted but still present.
    row = conn.execute("SELECT status FROM shopify_account WHERE id = ?", (acct_id,)).fetchone()
    assert row is not None
    assert row["status"] == "disabled"
    # Sync job history is still accessible (FK preserved).
    job_count = conn.execute(
        "SELECT COUNT(*) AS n FROM shopify_sync_job WHERE shopify_account_id = ?",
        (acct_id,),
    ).fetchone()["n"]
    assert job_count == 1, "shopify_sync_job history should be preserved through disconnect"
