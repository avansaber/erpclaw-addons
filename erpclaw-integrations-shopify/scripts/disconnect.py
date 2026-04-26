"""shopify-disconnect: tear down a paired Shopify account.

Order of operations:
  1. Flush pending Worker-side events (commands queue) by running one
     manual status-push cycle so any queued gdpr-dispatch gets processed
     before we lose the HMAC secret.
  2. Revoke the Shopify access token via
     POST /admin/oauth/revoke  (Shopify documents a REST endpoint; we
     best-effort since the merchant has uninstalled in some flows).
  3. Delete the shopify_account row. GL entries are preserved per
     ERPClaw immutability rule.
  4. If this was the last active shopify_account and a daemon is
     installed, uninstall it (reference-counted cleanup).

Usage:
  --action shopify-disconnect --shopify-account-id <id>
"""
import json
import os
import ssl
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone


def _utc_iso_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

LIB_PATH = os.path.expanduser("~/.openclaw/erpclaw/lib")
if LIB_PATH not in sys.path:
    sys.path.insert(0, LIB_PATH)

from erpclaw_lib.response import err, ok

from shopify_helpers import decrypt_token


REVOKE_TIMEOUT_SECONDS = 10


def _revoke_access_token(shop_domain, access_token):
    """Best-effort revocation. Returns (ok_bool, detail)."""
    url = f"https://{shop_domain}/admin/oauth/revoke"
    req = urllib.request.Request(
        url,
        method="POST",
        headers={
            "X-Shopify-Access-Token": access_token,
            "accept": "application/json",
            "user-agent": "erpclaw-shopify-disconnect/1.1",
        },
    )
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=REVOKE_TIMEOUT_SECONDS, context=ctx) as resp:
            return True, f"status {resp.status}"
    except urllib.error.HTTPError as exc:
        # 401/403 are expected if the token is already invalidated after uninstall.
        if exc.code in (401, 403, 404):
            return True, f"shopify reports token already invalid ({exc.code})"
        return False, f"HTTPError {exc.code}"
    except urllib.error.URLError as exc:
        return False, f"URLError {exc.reason}"
    except Exception as exc:  # noqa: BLE001
        return False, f"unexpected {exc!r}"


def _count_remaining_accounts(conn, exclude_id):
    row = conn.execute(
        "SELECT COUNT(*) FROM shopify_account WHERE id != ? AND status = 'active'",
        (exclude_id,),
    ).fetchone()
    return int(row[0]) if row else 0


def _uninstall_daemon_best_effort():
    """Try to remove the scheduler entry we installed at connect time.
    Silent no-op if nothing is installed. Implemented once daemon.py
    lands; stub here so tests can patch.
    """
    try:
        from daemon import uninstall_daemon  # type: ignore[import-not-found]
    except Exception:
        return {"skipped": "daemon module not available"}
    try:
        return uninstall_daemon()
    except Exception as exc:  # noqa: BLE001
        return {"error": repr(exc)}


def shopify_disconnect(conn, args):
    account_id = getattr(args, "shopify_account_id", None)
    if not account_id:
        err("--shopify-account-id is required",
            suggestion="list active Shopify accounts with 'shopify-list-accounts'")

    row = conn.execute(
        "SELECT id, shop_domain, shop_name, access_token_enc, pairing_method "
        "FROM shopify_account WHERE id = ?",
        (account_id,),
    ).fetchone()
    if not row:
        err(f"Shopify account {account_id} not found")

    shop_domain = row["shop_domain"]
    access_token = decrypt_token(row["access_token_enc"])

    # 1. Revoke the OAuth token. Failure is logged but not fatal; the user
    #    uninstalled so the token is effectively dead either way.
    revoked_ok, revoke_detail = _revoke_access_token(shop_domain, access_token)

    # 2. Soft-delete the shopify_account row (set status='disabled' +
    #    clear access token + clear HMAC secret). DELETE breaks FK from
    #    shopify_sync_job and other history tables; preserving the row
    #    keeps the audit trail and sync history. Status filter on
    #    push_all + sync actions excludes 'disabled' accounts. Schema
    #    CHECK constrains status to ('active','paused','error','disabled')
    #    so we use 'disabled' as the disconnected sentinel. Fixes §18.10.
    conn.execute(
        "UPDATE shopify_account SET status = 'disabled', "
        "access_token_enc = '', hmac_secret_enc = NULL, "
        "updated_at = ? WHERE id = ?",
        (_utc_iso_now(), account_id),
    )
    conn.commit()

    # 3. Reference-count the daemon: uninstall only if this was the last.
    remaining = _count_remaining_accounts(conn, account_id)
    daemon_result = None
    if remaining == 0:
        daemon_result = _uninstall_daemon_best_effort()

    ok({
        "disconnected": True,
        "shop_domain": shop_domain,
        "shop_name": row["shop_name"],
        "pairing_method": row["pairing_method"] or "custom_app",
        "token_revoked": revoked_ok,
        "token_revoke_detail": revoke_detail,
        "remaining_active_accounts": remaining,
        "daemon": daemon_result,
        "message": (
            "disconnected. Your GL entries were preserved per ERPClaw rules."
        ),
    })


DISCONNECT_ACTIONS = {
    "shopify-disconnect": shopify_disconnect,
}
