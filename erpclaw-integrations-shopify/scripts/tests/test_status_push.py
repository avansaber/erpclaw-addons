"""Unit tests for shopify-push-status.

All outbound HTTP is monkeypatched. Verifies:
  - Builds the status blob with the right shape + no extraneous fields.
  - Signs the body with the per-shop HMAC secret.
  - Skips accounts with no hmac_secret_enc (pairing_method != oauth).
  - Records last_status_push_at on success.
  - Dispatches pending_commands from the Worker response and acks the ids
    on the next push cycle.
  - Iterates over multiple active shops.
"""
import hashlib
import hmac as hmac_mod
import importlib
import json
from unittest.mock import patch

import pytest

from shopify_test_helpers import (
    build_env,
    call_action,
    is_ok,
    seed_company,
)


@pytest.fixture
def sp_module():
    import status_push as _sp  # type: ignore[import-not-found]
    importlib.reload(_sp)
    return _sp


class _Args:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _seed_oauth_account(conn, company_id, shop_domain="demo.myshopify.com", status_mode="scheduled"):
    from accounts import _add_account_core
    from shopify_helpers import encrypt_token

    res = _add_account_core(
        conn,
        company_id=company_id,
        shop_domain=shop_domain,
        shop_name=shop_domain.split(".", 1)[0],
        encrypted_token=encrypt_token("shpat_FIXTURE"),
        pairing_method="oauth",
        hmac_secret_enc=encrypt_token("a" * 64),
        status_mode=status_mode,
    )
    return res["id"]


def _canned_post_response(pending_commands=None):
    """Factory that returns a mock for _post_status returning 200 ok."""
    def _impl(worker_url, shop, hmac_secret, body):  # noqa: ARG001
        return 200, {
            "ok": True,
            "data": {
                "pending_commands": pending_commands or [],
                "server_time": "2026-04-24T12:00:00Z",
            },
        }
    return _impl


# ---------------------------------------------------------------------------
# Core flow
# ---------------------------------------------------------------------------

def test_push_builds_blob_and_records_timestamp(db_path, conn, sp_module):
    env = build_env(conn)
    acct = _seed_oauth_account(conn, env["company_id"])

    captured = {}

    def fake_post(worker_url, shop, hmac_secret, body):
        captured["worker_url"] = worker_url
        captured["shop"] = shop
        captured["hmac_secret"] = hmac_secret
        captured["body"] = body
        return 200, {"ok": True, "data": {"pending_commands": [], "server_time": "T"}}

    with patch.object(sp_module, "_post_status", side_effect=fake_post):
        result = call_action(sp_module.shopify_push_status, conn, _Args())

    assert is_ok(result)
    assert result["shops_pushed"] == 1
    assert captured["shop"] == "demo.myshopify.com"
    # hmac_secret is the DECRYPTED value (64 'a' chars) the push function uses for signing.
    assert captured["hmac_secret"] == "a" * 64
    blob = captured["body"]["status"]
    assert blob["shop_domain"] == "demo.myshopify.com"
    assert "orders_synced_last_24h" in blob
    assert "status_mode" in blob
    assert blob["status_mode"] == "scheduled"

    # last_status_push_at now populated
    row = conn.execute(
        "SELECT last_status_push_at FROM shopify_account WHERE id = ?",
        (acct,),
    ).fetchone()
    assert row["last_status_push_at"] is not None


def test_skips_account_without_hmac_secret(db_path, conn, sp_module):
    env = build_env(conn)
    # build_env creates a custom_app (shpat_) account with no hmac_secret_enc.
    # push_all should skip it.
    with patch.object(sp_module, "_post_status", side_effect=AssertionError("must not be called")):
        result = call_action(sp_module.shopify_push_status, conn, _Args())
    assert is_ok(result)
    assert result["shops_pushed"] == 0


def test_iterates_multiple_active_shops(db_path, conn, sp_module):
    env = build_env(conn)
    _seed_oauth_account(conn, env["company_id"], shop_domain="shop1.myshopify.com")
    _seed_oauth_account(conn, env["company_id"], shop_domain="shop2.myshopify.com")

    shops_seen = []

    def fake_post(worker_url, shop, hmac_secret, body):  # noqa: ARG001
        shops_seen.append(shop)
        return 200, {"ok": True, "data": {"pending_commands": [], "server_time": "T"}}

    with patch.object(sp_module, "_post_status", side_effect=fake_post):
        result = call_action(sp_module.shopify_push_status, conn, _Args())
    assert is_ok(result)
    assert result["shops_pushed"] == 2
    assert sorted(shops_seen) == ["shop1.myshopify.com", "shop2.myshopify.com"]


# ---------------------------------------------------------------------------
# Command dispatch + ack
# ---------------------------------------------------------------------------

def test_dispatches_pending_commands(db_path, conn, sp_module):
    env = build_env(conn)
    _seed_oauth_account(conn, env["company_id"])

    dispatched_calls = []

    def fake_dispatcher(conn, shop, cmd):  # noqa: ARG001
        dispatched_calls.append(cmd["id"])
        return {"dispatched": True, "action": cmd["type"]}

    sent_body = {}

    def fake_post(worker_url, shop, hmac_secret, body):  # noqa: ARG001
        sent_body.setdefault("bodies", []).append(body)
        # First push returns two commands. Second push (on retry) returns empty.
        cycle = len(sent_body["bodies"])
        if cycle == 1:
            return 200, {
                "ok": True,
                "data": {
                    "pending_commands": [
                        {"id": "cmd_a", "type": "sync-now", "payload": {}, "created_at": "T"},
                        {"id": "cmd_b", "type": "refresh-token", "payload": {}, "created_at": "T"},
                    ],
                    "server_time": "T",
                },
            }
        return 200, {"ok": True, "data": {"pending_commands": [], "server_time": "T"}}

    # Drive push_all directly so we can test ack round-trip across cycles.
    ack_ids_by_shop = {}
    with patch.object(sp_module, "_post_status", side_effect=fake_post):
        results1 = sp_module.push_all(conn, dispatcher=fake_dispatcher, ack_ids_by_shop=ack_ids_by_shop)
        # After cycle 1, ack_ids_by_shop should carry both cmd ids.
        assert ack_ids_by_shop["demo.myshopify.com"] == ["cmd_a", "cmd_b"]
        # Next push cycle sends those acks.
        results2 = sp_module.push_all(conn, dispatcher=fake_dispatcher, ack_ids_by_shop=ack_ids_by_shop)

    assert dispatched_calls == ["cmd_a", "cmd_b"]
    # Check second push body carries the acks.
    second_body = sent_body["bodies"][1]
    assert second_body["ack_command_ids"] == ["cmd_a", "cmd_b"]


def test_errors_from_worker_surface_cleanly(db_path, conn, sp_module):
    env = build_env(conn)
    _seed_oauth_account(conn, env["company_id"])

    def fake_post(worker_url, shop, hmac_secret, body):  # noqa: ARG001
        return 401, {"code": "ERR_UNKNOWN_SHOP", "detail": "HMAC secret revoked"}

    with patch.object(sp_module, "_post_status", side_effect=fake_post):
        result = call_action(sp_module.shopify_push_status, conn, _Args())
    assert is_ok(result)  # overall push still returns ok; per-shop error is in results
    assert result["shops_errored"] == 1
    assert result["results"][0]["code"] == "ERR_UNKNOWN_SHOP"


# ---------------------------------------------------------------------------
# Signing helper
# ---------------------------------------------------------------------------

def test_sign_request_matches_worker_format(sp_module):
    secret = "a" * 64
    shop = "demo.myshopify.com"
    ts = 1700000000
    body = b'{"hello":"world"}'
    sig = sp_module._sign_request(secret, shop, ts, body)

    body_hash = hashlib.sha256(body).hexdigest()
    expected = hmac_mod.new(
        secret.encode(),
        f"{shop}|{ts}|{body_hash}".encode(),
        hashlib.sha256,
    ).hexdigest()
    assert sig == expected
