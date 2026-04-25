"""Unit tests for shopify-connect.

Patches _fetch_pair so tests never hit the real Worker. Verifies:
  - Happy path: inserts shopify_account row with pairing_method='oauth',
    encrypted hmac_secret_enc, status_mode set.
  - Auto-resolves single company when --company-id omitted.
  - Rejects when multiple companies exist and no --company-id given.
  - Rejects when shop already connected (double-pair).
  - Handles Worker 404 (ERR_PAIRING_NOT_FOUND) with clear message.
  - Handles Worker 409 (ERR_PAIRING_ALREADY_CONSUMED) with clear message.
"""
import importlib
import json
import urllib.error
from unittest.mock import patch

import pytest

from shopify_test_helpers import (
    call_action,
    build_env,
    is_error,
    is_ok,
    seed_company,
    get_conn,
)


@pytest.fixture
def connect_module():
    import connect as _connect  # type: ignore[import-not-found]
    importlib.reload(_connect)
    return _connect


def _mock_pair_response(shop="demo.myshopify.com", code="ABC-XYZ"):
    return 200, {
        "ok": True,
        "data": {
            "shop": shop,
            "access_token": "shpat_FIXTURE_ACCESS_TOKEN",
            "scopes": ["read_orders", "read_customers"],
            "token_type": "offline",
            "pairing_code": code,
            "hmac_secret": "a" * 64,
            "installed_at": "2026-04-24T12:00:00Z",
        },
    }


def _mock_long_lived_false():
    return False


class _Args:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def test_happy_path_single_company(db_path, conn, connect_module):
    env = build_env(conn)
    company_id = env["company_id"]

    with patch.object(connect_module, "_fetch_pair", return_value=_mock_pair_response()):
        with patch.object(connect_module, "_detect_long_lived_process", _mock_long_lived_false):
            result = call_action(
                connect_module.shopify_connect,
                conn,
                _Args(pairing_code="ABC-XYZ"),
            )

    assert is_ok(result), result
    assert result["shop_domain"] == "demo.myshopify.com"
    assert result["pairing_method"] == "oauth"
    assert result["status_mode"] == "on-demand"
    assert result["gl_accounts_created"] == 14

    row = conn.execute(
        "SELECT pairing_method, hmac_secret_enc, status_mode, company_id FROM shopify_account WHERE shop_domain = ?",
        ("demo.myshopify.com",),
    ).fetchone()
    assert row is not None
    assert row["pairing_method"] == "oauth"
    assert row["hmac_secret_enc"]  # encrypted, non-empty
    assert row["hmac_secret_enc"] != "a" * 64  # must be encrypted, not plaintext
    assert row["status_mode"] == "on-demand"
    assert row["company_id"] == company_id


def test_active_mode_when_webclaw_running(db_path, conn, connect_module):
    build_env(conn)
    with patch.object(connect_module, "_fetch_pair", return_value=_mock_pair_response()):
        with patch.object(connect_module, "_detect_long_lived_process", lambda: True):
            result = call_action(
                connect_module.shopify_connect,
                conn,
                _Args(pairing_code="ABC-XYZ"),
            )
    assert is_ok(result)
    assert result["status_mode"] == "active"


def test_ambiguous_company_requires_flag(db_path, conn, connect_module):
    build_env(conn)
    # Add a second company so auto-resolve fails.
    seed_company(conn, name="Second Co")
    with patch.object(connect_module, "_fetch_pair", return_value=_mock_pair_response()):
        with patch.object(connect_module, "_detect_long_lived_process", _mock_long_lived_false):
            result = call_action(
                connect_module.shopify_connect,
                conn,
                _Args(pairing_code="ABC-XYZ"),
            )
    assert is_error(result)
    assert "companies exist" in result["message"]


def test_explicit_company_id_selects_second_company(db_path, conn, connect_module):
    build_env(conn)
    second = seed_company(conn, name="Second Co")
    with patch.object(connect_module, "_fetch_pair", return_value=_mock_pair_response()):
        with patch.object(connect_module, "_detect_long_lived_process", _mock_long_lived_false):
            result = call_action(
                connect_module.shopify_connect,
                conn,
                _Args(pairing_code="ABC-XYZ", company_id=second),
            )
    assert is_ok(result)
    row = conn.execute(
        "SELECT company_id FROM shopify_account WHERE shop_domain = ?",
        ("demo.myshopify.com",),
    ).fetchone()
    assert row["company_id"] == second


def test_duplicate_shop_rejected(db_path, conn, connect_module):
    build_env(conn)
    with patch.object(connect_module, "_fetch_pair", return_value=_mock_pair_response()):
        with patch.object(connect_module, "_detect_long_lived_process", _mock_long_lived_false):
            first = call_action(
                connect_module.shopify_connect,
                conn,
                _Args(pairing_code="ABC-XYZ"),
            )
    assert is_ok(first)

    with patch.object(connect_module, "_fetch_pair", return_value=_mock_pair_response(code="DEF-GHI")):
        with patch.object(connect_module, "_detect_long_lived_process", _mock_long_lived_false):
            second = call_action(
                connect_module.shopify_connect,
                conn,
                _Args(pairing_code="DEF-GHI"),
            )
    assert is_error(second)
    assert "already connected" in second["message"]


def _http_error(code, body_obj):
    err = urllib.error.HTTPError(
        url="https://w/pair/x",
        code=code,
        msg="err",
        hdrs=None,
        fp=None,
    )
    err.read = lambda: json.dumps(body_obj).encode("utf-8")
    return err


def test_pairing_not_found_returns_clear_message(db_path, conn, connect_module):
    build_env(conn)
    raise_err = _http_error(404, {"code": "ERR_PAIRING_NOT_FOUND"})

    def fake_fetch(url, code):
        raise raise_err

    with patch.object(connect_module, "_fetch_pair", side_effect=fake_fetch):
        result = call_action(
            connect_module.shopify_connect,
            conn,
            _Args(pairing_code="ABC-XYZ"),
        )
    assert is_error(result)
    assert "not found" in result["message"]


def test_pairing_already_consumed_returns_clear_message(db_path, conn, connect_module):
    build_env(conn)
    raise_err = _http_error(409, {"code": "ERR_PAIRING_ALREADY_CONSUMED"})

    def fake_fetch(url, code):
        raise raise_err

    with patch.object(connect_module, "_fetch_pair", side_effect=fake_fetch):
        result = call_action(
            connect_module.shopify_connect,
            conn,
            _Args(pairing_code="ABC-XYZ"),
        )
    assert is_error(result)
    assert "already used" in result["message"]


def test_missing_pairing_code_errors(db_path, conn, connect_module):
    build_env(conn)
    result = call_action(
        connect_module.shopify_connect,
        conn,
        _Args(),
    )
    assert is_error(result)
    assert "--pairing-code" in result["message"]
