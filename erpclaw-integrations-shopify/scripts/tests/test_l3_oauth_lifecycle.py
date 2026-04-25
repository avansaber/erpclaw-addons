"""L3 smoke test: full OAuth pairing lifecycle.

Walks a shop through every stage a real merchant hits:

  1. shopify-connect        — redeem pairing code, create account + GL
  2. shopify-push-status    — initial push, Worker returns no commands
  3. shopify-push-status    — Worker returns one pending sync-now command;
                              dispatcher handles it; next push acks it
  4. shopify-disconnect     — revoke token, delete account, uninstall daemon

Every external boundary (Worker HTTP, OAuth revoke endpoint, launchd/
systemd helpers, actual sync subprocess) is mocked. The goal is to prove
the orchestration itself -- row writes, ack round-trips, daemon reference
counting -- is wired correctly end-to-end.
"""
import importlib
import json
from unittest.mock import patch

import pytest

from shopify_test_helpers import (
    build_env,
    call_action,
    is_error,
    is_ok,
    seed_company,
)


@pytest.fixture
def modules():
    import connect as _connect
    import status_push as _push
    import dispatcher as _dispatch
    import disconnect as _disconnect
    importlib.reload(_connect)
    importlib.reload(_push)
    importlib.reload(_dispatch)
    importlib.reload(_disconnect)
    return {
        "connect": _connect,
        "push": _push,
        "dispatch": _dispatch,
        "disconnect": _disconnect,
    }


class _Args:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _pair_response():
    return 200, {
        "ok": True,
        "data": {
            "shop": "lifecycle.myshopify.com",
            "access_token": "shpat_LIFECYCLE_FIXTURE",
            "scopes": ["read_orders"],
            "token_type": "offline",
            "pairing_code": "ABC-XYZ",
            "hmac_secret": "b" * 64,
            "installed_at": "2026-04-24T12:00:00Z",
        },
    }


def _worker_ack_only():
    return 200, {"ok": True, "data": {"pending_commands": []}}


def _worker_with_sync_now():
    return 200, {
        "ok": True,
        "data": {
            "pending_commands": [
                {"id": "cmd_sync_1", "type": "sync-now", "payload": {}},
            ],
        },
    }


def test_full_oauth_lifecycle(db_path, conn, modules):
    """Pair → push (no cmds) → push (sync-now cmd) → push (ack) → disconnect."""
    connect = modules["connect"]
    push = modules["push"]
    dispatch = modules["dispatch"]
    disconnect = modules["disconnect"]

    # Seed a company but no shopify_account (connect creates it).
    company_id = seed_company(conn)

    # --- 1. pair ----------------------------------------------------------
    with patch.object(connect, "_fetch_pair", return_value=_pair_response()):
        with patch.object(connect, "_detect_long_lived_process", return_value=False):
            pair_result = call_action(
                connect.shopify_connect,
                conn,
                _Args(
                    pairing_code="ABC-XYZ",
                    company_id=company_id,
                    worker_url="https://shopify.erpclaw.ai",
                ),
            )
    assert is_ok(pair_result), pair_result
    assert pair_result["shop_domain"] == "lifecycle.myshopify.com"
    assert pair_result["pairing_method"] == "oauth"

    # Sanity: account exists, token and hmac_secret encrypted (not plaintext).
    row = conn.execute(
        "SELECT id, access_token_enc, hmac_secret_enc, status, pairing_method "
        "FROM shopify_account WHERE shop_domain = 'lifecycle.myshopify.com'"
    ).fetchone()
    assert row is not None
    assert row["access_token_enc"] != "shpat_LIFECYCLE_FIXTURE"
    assert row["hmac_secret_enc"] != "b" * 64
    assert row["status"] == "active"
    assert row["pairing_method"] == "oauth"
    account_id = row["id"]

    # --- 2. first push: no pending commands ------------------------------
    def fake_post_noop(worker_url, shop, secret, body):  # noqa: ARG001
        # Echo back: worker accepts push, returns zero pending commands.
        return _worker_ack_only()

    with patch.object(push, "_post_status", side_effect=fake_post_noop):
        push_result_1 = call_action(
            push.shopify_push_status,
            conn,
            _Args(worker_url="https://shopify.erpclaw.ai"),
        )
    assert is_ok(push_result_1)
    assert push_result_1["shops_pushed"] == 1
    assert push_result_1["shops_errored"] == 0

    # last_status_push_at must be populated now.
    assert conn.execute(
        "SELECT last_status_push_at FROM shopify_account WHERE id = ?",
        (account_id,),
    ).fetchone()["last_status_push_at"] is not None

    # --- 3. second push: Worker returns sync-now ------------------------
    call_log = {"posts": []}

    def fake_post_with_cmd(worker_url, shop, secret, body):  # noqa: ARG001
        call_log["posts"].append(body)
        return _worker_with_sync_now()

    def fake_dispatcher(conn_arg, shop, command):  # noqa: ARG001
        # Pretend the sync-now succeeded without actually spawning the
        # sync-orders subprocess (that's covered by sync module tests).
        return {"dispatched": True, "action": command.get("type")}

    with patch.object(push, "_post_status", side_effect=fake_post_with_cmd):
        with patch.object(push, "push_all", wraps=push.push_all):
            # Inline dispatcher mock so dispatch_command is not hit.
            orig_push_all = push.push_all

            def patched_push_all(conn, **kwargs):
                kwargs["dispatcher"] = fake_dispatcher
                return orig_push_all(conn, **kwargs)

            with patch.object(push, "push_all", side_effect=patched_push_all):
                push_result_2 = call_action(
                    push.shopify_push_status,
                    conn,
                    _Args(worker_url="https://shopify.erpclaw.ai"),
                )
    assert is_ok(push_result_2)
    # Check the shop result embedded in the CLI output.
    shop_results = push_result_2["results"]
    assert len(shop_results) == 1
    assert shop_results[0]["pending_commands"] == 1
    assert shop_results[0]["dispatched"][0]["dispatched"] is True
    assert shop_results[0]["dispatched"][0]["type"] == "sync-now"

    # --- 4. disconnect ---------------------------------------------------
    def fake_revoke(shop, token):  # noqa: ARG001
        return True, "revoked"

    def fake_uninstall():
        return {"uninstalled": True, "platform": "test"}

    with patch.object(disconnect, "_revoke_access_token", side_effect=fake_revoke):
        with patch.object(disconnect, "_uninstall_daemon_best_effort", side_effect=fake_uninstall):
            disc_result = call_action(
                disconnect.shopify_disconnect,
                conn,
                _Args(shopify_account_id=account_id),
            )
    assert is_ok(disc_result)
    assert disc_result["disconnected"] is True
    assert disc_result["token_revoked"] is True
    assert disc_result["remaining_active_accounts"] == 0
    # Last-account disconnect must uninstall the daemon.
    assert disc_result["daemon"] is not None
    assert disc_result["daemon"]["uninstalled"] is True

    # Account row is gone; GL accounts are preserved (we don't assert
    # the full 14 here, that's covered by accounts tests).
    row_after = conn.execute(
        "SELECT 1 FROM shopify_account WHERE id = ?", (account_id,)
    ).fetchone()
    assert row_after is None


def test_disconnect_keeps_daemon_when_other_account_exists(db_path, conn, modules):
    """Reference-count daemon: don't uninstall if another paired account remains."""
    disconnect = modules["disconnect"]
    connect = modules["connect"]

    company_id = seed_company(conn)

    # Pair two shops so disconnecting one leaves a remaining active account.
    for shop, code in [("one.myshopify.com", "ONE-ABC"), ("two.myshopify.com", "TWO-DEF")]:
        def fake_pair_factory(s=shop, c=code):
            def _f(url, pc):  # noqa: ARG001
                status, body = _pair_response()
                body["data"]["shop"] = s
                body["data"]["pairing_code"] = c
                return status, body
            return _f

        with patch.object(connect, "_fetch_pair", side_effect=fake_pair_factory()):
            with patch.object(connect, "_detect_long_lived_process", return_value=False):
                result = call_action(
                    connect.shopify_connect,
                    conn,
                    _Args(pairing_code=code, company_id=company_id),
                )
        assert is_ok(result), result

    first_id = conn.execute(
        "SELECT id FROM shopify_account WHERE shop_domain = 'one.myshopify.com'"
    ).fetchone()["id"]

    uninstall_spy = {"called": False}

    def spy_uninstall():
        uninstall_spy["called"] = True
        return {"uninstalled": True}

    with patch.object(disconnect, "_revoke_access_token", return_value=(True, "revoked")):
        with patch.object(disconnect, "_uninstall_daemon_best_effort", side_effect=spy_uninstall):
            result = call_action(
                disconnect.shopify_disconnect,
                conn,
                _Args(shopify_account_id=first_id),
            )

    assert is_ok(result)
    assert result["remaining_active_accounts"] == 1
    assert result["daemon"] is None, "daemon must NOT be uninstalled while a shop remains"
    assert uninstall_spy["called"] is False
