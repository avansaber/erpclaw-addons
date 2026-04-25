"""Unit tests for shopify-dispatch-command.

We monkeypatch the underlying actions (shopify-start-full-sync, disconnect)
so dispatcher is exercised in isolation. Verifies:
  - sync-now routes to shopify-start-full-sync.
  - disconnect routes to shopify-disconnect.
  - refresh-token is acknowledged (v1.1 stub).
  - gdpr-dispatch returns dispatched=False when gdpr.py not installed.
  - Unknown command type returns dispatched=False.
"""
import importlib
import sys
from unittest.mock import patch

import pytest

from shopify_test_helpers import build_env


@pytest.fixture
def dispatcher_module():
    import dispatcher as _d  # type: ignore[import-not-found]
    importlib.reload(_d)
    return _d


def _seed_oauth_account(conn, company_id, shop_domain="demo.myshopify.com"):
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
        status_mode="scheduled",
    )
    return res["id"]


def test_refresh_token_stub_is_acknowledged(db_path, conn, dispatcher_module):
    env = build_env(conn)
    _seed_oauth_account(conn, env["company_id"])
    result = dispatcher_module.dispatch_command(
        conn,
        "demo.myshopify.com",
        {"id": "cmd_a", "type": "refresh-token", "payload": {}},
    )
    assert result["dispatched"] is True
    assert result["action"] == "refresh-token"


def test_unknown_type_not_dispatched(db_path, conn, dispatcher_module):
    build_env(conn)
    result = dispatcher_module.dispatch_command(
        conn,
        "demo.myshopify.com",
        {"id": "cmd_a", "type": "totally-fake", "payload": {}},
    )
    assert result["dispatched"] is False
    assert "unknown command type" in result["reason"]


def test_non_dict_command_not_dispatched(db_path, conn, dispatcher_module):
    build_env(conn)
    result = dispatcher_module.dispatch_command(conn, "demo.myshopify.com", ["not", "a", "dict"])
    assert result["dispatched"] is False


def test_gdpr_not_dispatched_when_module_missing(db_path, conn, dispatcher_module):
    env = build_env(conn)
    _seed_oauth_account(conn, env["company_id"])
    # Force ImportError on 'gdpr'
    with patch.dict(sys.modules, {"gdpr": None}):
        result = dispatcher_module.dispatch_command(
            conn,
            "demo.myshopify.com",
            {"id": "cmd_a", "type": "gdpr-dispatch", "payload": {"topic": "shop/redact"}},
        )
    assert result["dispatched"] is False
    assert "gdpr" in result["reason"].lower()


def test_sync_now_routes_to_handler(db_path, conn, dispatcher_module, monkeypatch):
    env = build_env(conn)
    acct = _seed_oauth_account(conn, env["company_id"])
    captured = {}

    def fake_sync(conn, args):  # noqa: ARG001
        captured["shopify_account_id"] = getattr(args, "shopify_account_id", None)
        print('{"status":"ok","synced":0}')
        raise SystemExit(0)

    monkeypatch.setitem(
        __import__("sync").ACTIONS,  # type: ignore[attr-defined]
        "shopify-start-full-sync",
        fake_sync,
    )

    result = dispatcher_module.dispatch_command(
        conn,
        "demo.myshopify.com",
        {"id": "cmd_a", "type": "sync-now", "payload": {}},
    )
    assert result["dispatched"] is True
    assert result["action"] == "shopify-start-full-sync"
    assert captured["shopify_account_id"] == acct


def test_disconnect_routes_to_handler(db_path, conn, dispatcher_module, monkeypatch):
    env = build_env(conn)
    acct = _seed_oauth_account(conn, env["company_id"])
    captured = {}

    def fake_disconnect(conn, args):  # noqa: ARG001
        captured["id"] = getattr(args, "shopify_account_id", None)
        print('{"status":"ok","disconnected":true}')
        raise SystemExit(0)

    # Patch the imported-inside-function reference
    import disconnect as disc_mod
    monkeypatch.setattr(disc_mod, "shopify_disconnect", fake_disconnect)

    result = dispatcher_module.dispatch_command(
        conn,
        "demo.myshopify.com",
        {"id": "cmd_a", "type": "disconnect", "payload": {}},
    )
    assert result["dispatched"] is True
    assert result["action"] == "shopify-disconnect"
    assert captured["id"] == acct
