"""shopify-push-status: push the status blob for every paired shop to the
Worker and dispatch any commands the Worker returns.

Runs in three situations:
  1. Interactively, whenever a shopify-* action touches the DB (push-on-
     action keeps the embedded UI fresh without waiting for the next tick).
  2. Every 15 min from the OS-level daemon (§8.7).
  3. Manually via `erpclaw shopify-push-status`.

Design invariants (from apps/shopify/API_CONTRACT.md):
  - Sign the body with the per-shop HMAC secret: HMAC-SHA256 over
    `{shop}|{timestamp}|{sha256(body)}`, hex-encoded, in the
    Authorization header as `HMAC-SHA256 <hex>`.
  - Body <= 2KB.
  - Include `ack_command_ids` from the previous dispatch round.
"""
import hashlib
import hmac as hmac_mod
import json
import os
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request

LIB_PATH = os.path.expanduser("~/.openclaw/erpclaw/lib")
if LIB_PATH not in sys.path:
    sys.path.insert(0, LIB_PATH)

from erpclaw_lib.response import err, ok

from shopify_helpers import decrypt_token

DEFAULT_WORKER_URL = "https://shopify.erpclaw.ai"
PUSH_TIMEOUT_SECONDS = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_erpclaw_version():
    """Read core erpclaw SKILL.md frontmatter for version."""
    core = os.path.expanduser("~/.openclaw/erpclaw/skills/erpclaw/SKILL.md")
    for path in (core, os.path.join(os.path.dirname(__file__), "..", "..", "..", "erpclaw", "SKILL.md")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("version:"):
                        return line.split(":", 1)[1].strip()
        except OSError:
            continue
    return "unknown"


def _probe_local_url(override=None, probe_ports=(8000, 8080, 3000)):
    """Determine erpclaw_local_url for the status blob.

    Priority:
      1. --erpclaw-url override (stored on the shopify_account row)
      2. First probe_ports entry where /healthz returns 200 within 500ms
      3. None (omit field)
    """
    if override:
        return override
    for port in probe_ports:
        if _port_healthz(port):
            return f"http://localhost:{port}"
    return None


def _port_healthz(port, host="127.0.0.1", timeout=0.5):
    """Quick health probe without forcing urllib HTTPS."""
    url = f"http://{host}:{port}/healthz"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False


def _counts_for_shop(conn, shopify_account_id):
    """Compute the metric counts the status blob needs.

    We keep this cheap; SQLite COUNT on the already-indexed columns.
    """
    since = _utc_iso_offset_hours(-24)
    row_24h_orders = conn.execute(
        "SELECT COUNT(*) AS c FROM shopify_order WHERE shopify_account_id = ? AND created_at > ?",
        (shopify_account_id, since),
    ).fetchone()
    row_total_orders = conn.execute(
        "SELECT COUNT(*) AS c FROM shopify_order WHERE shopify_account_id = ?",
        (shopify_account_id,),
    ).fetchone()
    row_gl = conn.execute(
        "SELECT COUNT(*) AS c "
        "FROM shopify_order "
        "WHERE shopify_account_id = ? AND gl_status = 'posted' AND created_at > ?",
        (shopify_account_id, since),
    ).fetchone()
    row_errors = conn.execute(
        "SELECT COUNT(*) AS c FROM shopify_sync_job "
        "WHERE shopify_account_id = ? AND status = 'failed' AND started_at > ?",
        (shopify_account_id, since),
    ).fetchone()
    row_last_sync = conn.execute(
        "SELECT status, completed_at, started_at FROM shopify_sync_job "
        "WHERE shopify_account_id = ? "
        "ORDER BY COALESCE(completed_at, started_at) DESC LIMIT 1",
        (shopify_account_id,),
    ).fetchone()
    return {
        "orders_synced_last_24h": int(row_24h_orders[0]) if row_24h_orders else 0,
        "total_orders_synced": int(row_total_orders[0]) if row_total_orders else 0,
        "gl_entries_posted_last_24h": int(row_gl[0]) if row_gl else 0,
        "recent_errors": int(row_errors[0]) if row_errors else 0,
        "last_sync_status": _map_sync_status(row_last_sync),
        "last_sync_at": _last_sync_at(row_last_sync),
    }


def _map_sync_status(row):
    if not row:
        return "ok"
    status = row["status"] if hasattr(row, "keys") else row[0]
    if status == "completed":
        return "ok"
    if status == "failed":
        return "error"
    return "partial"


def _last_sync_at(row):
    if not row:
        return _utc_iso_now()
    completed = row["completed_at"] if hasattr(row, "keys") else row[1]
    started = row["started_at"] if hasattr(row, "keys") else row[2]
    return completed or started or _utc_iso_now()


def _utc_iso_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _utc_iso_offset_hours(hours_delta):
    t = time.gmtime(time.time() + hours_delta * 3600)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", t)


def _build_status_blob(conn, account_row):
    """Assemble the ~500-byte status blob for one shop."""
    counts = _counts_for_shop(conn, account_row["id"])
    override = account_row["erpclaw_url_override"] if "erpclaw_url_override" in account_row.keys() else None
    local_url = _probe_local_url(override)
    blob = {
        "shop_domain": account_row["shop_domain"],
        "erpclaw_version": _get_erpclaw_version(),
        "last_sync_at": counts["last_sync_at"],
        "last_sync_status": counts["last_sync_status"],
        "orders_synced_last_24h": counts["orders_synced_last_24h"],
        "total_orders_synced": counts["total_orders_synced"],
        "gl_entries_posted_last_24h": counts["gl_entries_posted_last_24h"],
        "recent_errors": counts["recent_errors"],
        "status_mode": account_row["status_mode"] or "on-demand",
    }
    if local_url:
        blob["erpclaw_local_url"] = local_url
    return blob


def _sign_request(secret_hex, shop, timestamp, body_bytes):
    """Build the Authorization header value for a status push."""
    body_hash = hashlib.sha256(body_bytes).hexdigest()
    payload = f"{shop}|{timestamp}|{body_hash}"
    sig = hmac_mod.new(
        secret_hex.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return sig


def _post_status(worker_url, shop, hmac_secret, body_obj):
    """POST one shop's status blob + acks. Returns (status_code, parsed_body)."""
    body_bytes = json.dumps(body_obj).encode("utf-8")
    timestamp = int(time.time())
    sig = _sign_request(hmac_secret, shop, timestamp, body_bytes)
    url = f"{worker_url.rstrip('/')}/status/{shop}"
    req = urllib.request.Request(
        url,
        data=body_bytes,
        method="POST",
        headers={
            "authorization": f"HMAC-SHA256 {sig}",
            "x-timestamp": str(timestamp),
            "content-type": "application/json",
            "user-agent": "erpclaw-shopify-status-push/1.1",
        },
    )
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=PUSH_TIMEOUT_SECONDS, context=ctx) as resp:
            parsed = json.loads(resp.read().decode("utf-8"))
            return resp.status, parsed
    except urllib.error.HTTPError as exc:
        try:
            parsed = json.loads(exc.read().decode("utf-8"))
        except Exception:
            parsed = {"code": "UNKNOWN", "status": exc.code}
        return exc.code, parsed


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def _push_one(conn, account_row, worker_url, ack_ids_by_shop, dispatcher):
    shop = account_row["shop_domain"]
    hmac_secret = decrypt_token(account_row["hmac_secret_enc"]) if account_row["hmac_secret_enc"] else None
    if not hmac_secret:
        return {"shop": shop, "skipped": "no hmac_secret (pairing_method != oauth?)"}

    blob = _build_status_blob(conn, account_row)
    ack_ids = ack_ids_by_shop.get(shop, [])
    status_code, parsed = _post_status(
        worker_url,
        shop,
        hmac_secret,
        {"status": blob, "ack_command_ids": ack_ids},
    )

    if status_code != 200 or not parsed.get("ok"):
        return {
            "shop": shop,
            "error": f"worker returned {status_code}",
            "code": parsed.get("code"),
            "detail": parsed.get("detail"),
        }

    # Record successful push timestamp.
    conn.execute(
        "UPDATE shopify_account SET last_status_push_at = ? WHERE id = ?",
        (_utc_iso_now(), account_row["id"]),
    )
    conn.commit()

    pending_commands = parsed.get("data", {}).get("pending_commands", [])
    # Bind dispatched_ids to the shop's slot in the shared dict BEFORE
    # the dispatcher loop runs, so any partial dispatches are preserved
    # if a single dispatcher raises mid-loop. Replace rather than
    # accumulate; each push cycle sends + flushes its own dispatch acks.
    # Fixes §18.11: previously a local list that was lost on exception.
    dispatched_ids = []
    ack_ids_by_shop[shop] = dispatched_ids
    dispatch_results = []
    for cmd in pending_commands:
        cmd_id = cmd.get("id")
        try:
            result = dispatcher(conn, shop, cmd)
        except Exception as exc:  # noqa: BLE001 -- isolate each command's failure from the rest of the loop
            dispatch_results.append({
                "id": cmd_id,
                "type": cmd.get("type"),
                "dispatched": True,  # ack the failed command so it stops re-queuing forever
                "error": repr(exc),
                "ack_reason": "dispatcher exception; acked to prevent infinite retry",
            })
            dispatched_ids.append(cmd_id)
            continue
        dispatch_results.append({"id": cmd_id, "type": cmd.get("type"), **result})
        if result.get("dispatched"):
            dispatched_ids.append(cmd_id)

    return {
        "shop": shop,
        "pushed_at": _utc_iso_now(),
        "orders_synced_last_24h": blob["orders_synced_last_24h"],
        "pending_commands": len(pending_commands),
        "dispatched": dispatch_results,
    }


def push_all(conn, worker_url=None, dispatcher=None, ack_ids_by_shop=None):
    """Push for every active paired shop. Returns a list of results.

    Called directly by the daemon tick; also by shopify-push-status CLI.
    """
    worker_url = worker_url or DEFAULT_WORKER_URL
    if dispatcher is None:
        from dispatcher import dispatch_command
        dispatcher = dispatch_command
    if ack_ids_by_shop is None:
        ack_ids_by_shop = {}

    rows = conn.execute(
        "SELECT id, shop_domain, hmac_secret_enc, status_mode, erpclaw_url_override "
        "FROM shopify_account WHERE status = 'active' AND pairing_method = 'oauth'"
    ).fetchall()
    results = []
    for row in rows:
        try:
            results.append(_push_one(conn, row, worker_url, ack_ids_by_shop, dispatcher))
        except Exception as exc:  # noqa: BLE001
            results.append({"shop": row["shop_domain"], "error": f"unhandled: {exc!r}"})
    return results


# ---------------------------------------------------------------------------
# Action entrypoint
# ---------------------------------------------------------------------------

def shopify_push_status(conn, args):
    worker_url = getattr(args, "worker_url", None) or DEFAULT_WORKER_URL
    results = push_all(conn, worker_url=worker_url)
    ok({
        "worker_url": worker_url,
        "shops_pushed": len([r for r in results if "pushed_at" in r]),
        "shops_errored": len([r for r in results if "error" in r]),
        "shops_skipped": len([r for r in results if "skipped" in r]),
        "results": results,
    })


STATUS_PUSH_ACTIONS = {
    "shopify-push-status": shopify_push_status,
}
