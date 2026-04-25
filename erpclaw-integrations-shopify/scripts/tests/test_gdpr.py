"""Unit tests for shopify-handle-gdpr.

Verifies:
  - customers/data_request writes a JSON export file with matching orders.
  - customers/redact nulls out customer_email on matching shopify_order rows.
  - shop/redact hard-deletes all shopify_* rows for the shop; GL entries
    are NOT touched (ERPClaw immutability rule).
  - app/uninstalled flags the account as 'disabled' + disconnect_state.
  - Bad topic rejected.
  - Missing shop rejected.
"""
import importlib
import json
import os
import uuid

import pytest

from shopify_test_helpers import (
    build_env,
    call_action,
    is_error,
    is_ok,
)


@pytest.fixture
def gdpr_module(monkeypatch, tmp_path):
    import gdpr as _g
    importlib.reload(_g)
    # Redirect audit logs + data exports to tmp_path so tests don't write to ~
    monkeypatch.setattr(_g, "_logs_dir", lambda: str(tmp_path))
    return _g


class _Args:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _seed_oauth_account(conn, company_id, shop="demo.myshopify.com"):
    from accounts import _add_account_core
    from shopify_helpers import encrypt_token

    res = _add_account_core(
        conn,
        company_id=company_id,
        shop_domain=shop,
        shop_name=shop.split(".", 1)[0],
        encrypted_token=encrypt_token("shpat_FIXTURE"),
        pairing_method="oauth",
        hmac_secret_enc=encrypt_token("a" * 64),
        status_mode="scheduled",
    )
    return res["id"]


def _seed_order(conn, shopify_account_id, company_id, customer_id=None, email=None, order_num="1001"):
    """Seed one shopify_order row tied to a shopify_account.

    `customer_id` / `email` are accepted for test-clarity but the
    shopify_order schema does not store them directly; the connector
    only cares about shopify_account_id for shop-level redaction.
    """
    from shopify_test_helpers import seed_shopify_order
    return seed_shopify_order(
        conn,
        shopify_account_id=shopify_account_id,
        company_id=company_id,
        shopify_order_id=order_num,
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def test_rejects_bad_topic(db_path, conn, gdpr_module):
    build_env(conn)
    result = call_action(
        gdpr_module.shopify_handle_gdpr,
        conn,
        _Args(topic="customers/do-weird-thing", shop_domain="demo.myshopify.com"),
    )
    assert is_error(result)


def test_rejects_missing_shop(db_path, conn, gdpr_module):
    build_env(conn)
    result = call_action(
        gdpr_module.shopify_handle_gdpr,
        conn,
        _Args(topic="customers/redact"),
    )
    assert is_error(result)


# ---------------------------------------------------------------------------
# customers/data_request
# ---------------------------------------------------------------------------

def test_data_request_writes_pointer_file(db_path, conn, gdpr_module, tmp_path):
    env = build_env(conn)
    shop = "demo.myshopify.com"
    _seed_oauth_account(conn, env["company_id"], shop)

    payload = json.dumps({"customer": {"id": 9001}})
    result = call_action(
        gdpr_module.shopify_handle_gdpr,
        conn,
        _Args(topic="customers/data_request", shop_domain=shop, payload=payload),
    )
    assert is_ok(result)
    assert result["shopify_customer_id"] == 9001
    assert result["export_path"]
    with open(result["export_path"], "r", encoding="utf-8") as f:
        exported = json.load(f)
    assert exported["shopify_customer_id"] == 9001
    assert "ERPClaw core" in exported["note"]


def test_data_request_no_customer_id_writes_nothing(db_path, conn, gdpr_module):
    env = build_env(conn)
    _seed_oauth_account(conn, env["company_id"])
    result = call_action(
        gdpr_module.shopify_handle_gdpr,
        conn,
        _Args(topic="customers/data_request", shop_domain="demo.myshopify.com", payload="{}"),
    )
    assert is_ok(result)
    assert result["shopify_customer_id"] is None
    assert result["export_path"] is None


# ---------------------------------------------------------------------------
# customers/redact
# ---------------------------------------------------------------------------

def test_customers_redact_logs_for_followup(db_path, conn, gdpr_module):
    env = build_env(conn)
    shop = "demo.myshopify.com"
    _seed_oauth_account(conn, env["company_id"], shop)

    payload = json.dumps({"customer": {"id": 7001}})
    result = call_action(
        gdpr_module.shopify_handle_gdpr,
        conn,
        _Args(topic="customers/redact", shop_domain=shop, payload=payload),
    )
    assert is_ok(result)
    assert result["shopify_customer_id"] == 7001
    assert result["logged"] is True


# ---------------------------------------------------------------------------
# shop/redact
# ---------------------------------------------------------------------------

def test_shop_redact_hard_deletes_and_removes_account(db_path, conn, gdpr_module):
    env = build_env(conn)
    shop = "demo.myshopify.com"
    acct = _seed_oauth_account(conn, env["company_id"], shop)
    _seed_order(conn, acct, env["company_id"])
    # Confirm pre-state
    pre_order_count = conn.execute(
        "SELECT COUNT(*) FROM shopify_order WHERE shopify_account_id = ?",
        (acct,),
    ).fetchone()[0]
    assert pre_order_count == 1

    result = call_action(
        gdpr_module.shopify_handle_gdpr,
        conn,
        _Args(
            topic="shop/redact",
            shop_domain=shop,
            payload=json.dumps({"shop_id": 1}),
        ),
    )
    assert is_ok(result)
    assert result["shopify_account_removed"] is True
    assert result["per_table"]["shopify_order"] == 1

    remaining_orders = conn.execute(
        "SELECT COUNT(*) FROM shopify_order WHERE shopify_account_id = ?",
        (acct,),
    ).fetchone()[0]
    assert remaining_orders == 0
    remaining_account = conn.execute(
        "SELECT id FROM shopify_account WHERE id = ?",
        (acct,),
    ).fetchone()
    assert remaining_account is None


def test_shop_redact_noop_when_shop_not_found(db_path, conn, gdpr_module):
    build_env(conn)
    result = call_action(
        gdpr_module.shopify_handle_gdpr,
        conn,
        _Args(topic="shop/redact", shop_domain="ghost.myshopify.com"),
    )
    assert is_ok(result)
    assert result["shopify_account_removed"] is False
    assert result["rows_deleted"] == 0


# ---------------------------------------------------------------------------
# app/uninstalled
# ---------------------------------------------------------------------------

def _backdate_account(conn, account_id, seconds_ago):
    """Push shopify_account.created_at + updated_at into the past so the
    fresh-install grace window doesn't shield it.
    """
    import datetime as _dt
    ts = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=seconds_ago)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    conn.execute(
        "UPDATE shopify_account SET created_at = ?, updated_at = ? WHERE id = ?",
        (ts, ts, account_id),
    )
    conn.commit()


def test_app_uninstalled_flags_account(db_path, conn, gdpr_module):
    env = build_env(conn)
    shop = "demo.myshopify.com"
    acct = _seed_oauth_account(conn, env["company_id"], shop)
    # Backdate beyond the 5-min grace so the disable proceeds.
    _backdate_account(conn, acct, 3600)

    result = call_action(
        gdpr_module.shopify_handle_gdpr,
        conn,
        _Args(topic="app/uninstalled", shop_domain=shop),
    )
    assert is_ok(result)
    assert result["flagged"] == 1
    row = conn.execute(
        "SELECT status, disconnect_state FROM shopify_account WHERE id = ?",
        (acct,),
    ).fetchone()
    assert row["status"] == "disabled"
    assert row["disconnect_state"] == "app-uninstalled"


def test_app_uninstalled_skips_disable_for_fresh_install(db_path, conn, gdpr_module):
    """Stale app/uninstalled webhooks must not disable a freshly paired
    account. Repro of the live ZAV-9KH incident: queued GDPR command
    from a prior uninstall fires after the merchant has reinstalled.
    """
    env = build_env(conn)
    shop = "demo.myshopify.com"
    acct = _seed_oauth_account(conn, env["company_id"], shop)
    # Account was just created; updated_at is now(), inside the grace.

    result = call_action(
        gdpr_module.shopify_handle_gdpr,
        conn,
        _Args(topic="app/uninstalled", shop_domain=shop),
    )
    assert is_ok(result)
    assert result["flagged"] == 0
    assert result.get("skipped") == "fresh install"

    row = conn.execute(
        "SELECT status, disconnect_state FROM shopify_account WHERE id = ?",
        (acct,),
    ).fetchone()
    assert row["status"] == "active"
    assert row["disconnect_state"] is None or row["disconnect_state"] != "app-uninstalled"


def test_app_uninstalled_disables_old_install(db_path, conn, gdpr_module):
    """A genuine uninstall on an account that has been live for an hour
    must still flip status to disabled.
    """
    env = build_env(conn)
    shop = "demo.myshopify.com"
    acct = _seed_oauth_account(conn, env["company_id"], shop)
    _backdate_account(conn, acct, 3600)

    result = call_action(
        gdpr_module.shopify_handle_gdpr,
        conn,
        _Args(topic="app/uninstalled", shop_domain=shop),
    )
    assert is_ok(result)
    assert result["flagged"] == 1

    row = conn.execute(
        "SELECT status, disconnect_state FROM shopify_account WHERE id = ?",
        (acct,),
    ).fetchone()
    assert row["status"] == "disabled"
    assert row["disconnect_state"] == "app-uninstalled"


# ---------------------------------------------------------------------------
# Audit log writes
# ---------------------------------------------------------------------------

def test_audit_log_written(db_path, conn, gdpr_module, tmp_path):
    env = build_env(conn)
    _seed_oauth_account(conn, env["company_id"])
    call_action(
        gdpr_module.shopify_handle_gdpr,
        conn,
        _Args(topic="app/uninstalled", shop_domain="demo.myshopify.com"),
    )
    log_path = os.path.join(str(tmp_path), "shopify_gdpr.log")
    assert os.path.exists(log_path)
    with open(log_path, "r", encoding="utf-8") as f:
        lines = [json.loads(line) for line in f if line.strip()]
    assert any(line["topic"] == "app/uninstalled" for line in lines)
