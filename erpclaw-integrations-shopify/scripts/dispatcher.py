"""shopify-dispatch-command: route a Worker-delivered command to the right
local action.

Called by status_push.py for every entry in the Worker's
`pending_commands` response array. Returns a structured ack result so the
caller can include the command id in `ack_command_ids` on the next push.

Supported command types:
  sync-now        -> shopify-start-full-sync (from sync.py)
  disconnect      -> shopify-disconnect      (from disconnect.py)
  gdpr-dispatch   -> shopify-handle-gdpr     (from gdpr.py, when it lands)
  refresh-token   -> future; v1.1 records the attempt and acks

Not a user-facing action. Exposed on the router so the `shopify-dispatch-
command --command-json {...}` variant is available for manual debugging,
but production flow is status_push -> dispatch_command() (Python call).
"""
import json
import os
import sys

LIB_PATH = os.path.expanduser("~/.openclaw/erpclaw/lib")
if LIB_PATH not in sys.path:
    sys.path.insert(0, LIB_PATH)

from erpclaw_lib.response import err, ok


class _DispatchArgs:
    """Minimal argparse-style object for calling legacy actions."""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _dispatch_sync_now(conn, shop_domain, payload):
    """Route sync-now to the existing shopify-start-full-sync action.

    The sync action prints via ok() and sys.exits; we run it with its
    stdout captured so we can ack the command rather than crashing the
    push loop.
    """
    import io
    from contextlib import redirect_stdout

    from sync import ACTIONS as SYNC_ACTIONS

    # Look up the shopify_account_id for this shop.
    row = conn.execute(
        "SELECT id FROM shopify_account WHERE shop_domain = ? AND status = 'active'",
        (shop_domain,),
    ).fetchone()
    if not row:
        return {"dispatched": False, "reason": "shop not paired or inactive"}
    shopify_account_id = row["id"] if isinstance(row, dict) or hasattr(row, "keys") else row[0]

    handler = SYNC_ACTIONS.get("shopify-start-full-sync")
    if not handler:
        return {"dispatched": False, "reason": "shopify-start-full-sync handler missing"}

    args = _DispatchArgs(
        shopify_account_id=shopify_account_id,
        sync_type=payload.get("sync_type", "all"),
    )
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            handler(conn, args)
    except SystemExit as exc:
        code = getattr(exc, "code", 0) or 0
        return {
            "dispatched": True,
            "action": "shopify-start-full-sync",
            "exit_code": int(code) if isinstance(code, int) else 1,
            "output": buf.getvalue()[:4000],
        }
    return {"dispatched": True, "action": "shopify-start-full-sync", "output": buf.getvalue()[:4000]}


def _dispatch_disconnect(conn, shop_domain, _payload):
    """Route disconnect by looking up shopify_account_id and running disconnect."""
    import io
    from contextlib import redirect_stdout

    from disconnect import shopify_disconnect

    row = conn.execute(
        "SELECT id FROM shopify_account WHERE shop_domain = ?",
        (shop_domain,),
    ).fetchone()
    if not row:
        return {"dispatched": False, "reason": "shop not found"}
    shopify_account_id = row["id"] if hasattr(row, "keys") else row[0]

    buf = io.StringIO()
    args = _DispatchArgs(shopify_account_id=shopify_account_id)
    try:
        with redirect_stdout(buf):
            shopify_disconnect(conn, args)
    except SystemExit as exc:
        code = getattr(exc, "code", 0) or 0
        return {"dispatched": True, "action": "shopify-disconnect", "exit_code": int(code) if isinstance(code, int) else 1}
    return {"dispatched": True, "action": "shopify-disconnect"}


def _dispatch_gdpr(conn, shop_domain, payload):
    """Route gdpr-dispatch to the gdpr handler.

    If the gdpr.py module is not yet installed (will land in Phase C),
    the command is left un-acked so the Worker keeps it in the queue
    until the handler is available. Caller treats 'dispatched=false' by
    NOT appending to ack_command_ids.
    """
    try:
        from gdpr import shopify_handle_gdpr  # type: ignore[import-not-found]
    except ImportError:
        return {"dispatched": False, "reason": "gdpr handler not installed yet"}

    import io
    from contextlib import redirect_stdout

    topic = payload.get("topic") if isinstance(payload, dict) else None
    body = payload.get("body") if isinstance(payload, dict) else None
    args = _DispatchArgs(
        shop_domain=shop_domain,
        topic=topic,
        payload=json.dumps(body) if body is not None else "",
    )
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            shopify_handle_gdpr(conn, args)
    except SystemExit as exc:
        code = getattr(exc, "code", 0) or 0
        return {
            "dispatched": True,
            "action": "shopify-handle-gdpr",
            "exit_code": int(code) if isinstance(code, int) else 1,
            "topic": topic,
        }
    return {"dispatched": True, "action": "shopify-handle-gdpr", "topic": topic}


# ---------------------------------------------------------------------------
# Public dispatcher
# ---------------------------------------------------------------------------

def dispatch_command(conn, shop_domain, command):
    """Entry point callable from status_push.

    Returns:
      {dispatched: bool, action?: str, ...}

    Caller acks commands where dispatched=True (whether or not the inner
    action succeeded). dispatched=False means the command stays in the
    Worker queue for a later retry.
    """
    if not isinstance(command, dict):
        return {"dispatched": False, "reason": "command not a dict"}
    cmd_type = command.get("type")
    payload = command.get("payload") or {}

    if cmd_type == "sync-now":
        return _dispatch_sync_now(conn, shop_domain, payload)
    if cmd_type == "disconnect":
        return _dispatch_disconnect(conn, shop_domain, payload)
    if cmd_type == "gdpr-dispatch":
        return _dispatch_gdpr(conn, shop_domain, payload)
    if cmd_type == "refresh-token":
        # v1.1: acknowledge so we don't retry forever. Actual refresh-token
        # handling lands when offline-expiring tokens hit expiry in
        # production (~1 year after first pair).
        return {
            "dispatched": True,
            "action": "refresh-token",
            "note": "refresh-token handling is a v1.2 feature",
        }
    return {"dispatched": False, "reason": f"unknown command type {cmd_type!r}"}


# ---------------------------------------------------------------------------
# CLI entry (manual debugging)
# ---------------------------------------------------------------------------

def shopify_dispatch_command(conn, args):
    shop = getattr(args, "shop_domain", None)
    cmd_json = getattr(args, "command_json", None)
    if not shop or not cmd_json:
        err("--shop-domain and --command-json are both required")
    try:
        cmd = json.loads(cmd_json)
    except Exception as exc:  # noqa: BLE001
        err(f"--command-json is not valid JSON: {exc}")
    ok(dispatch_command(conn, shop, cmd))


DISPATCHER_ACTIONS = {
    "shopify-dispatch-command": shopify_dispatch_command,
}
