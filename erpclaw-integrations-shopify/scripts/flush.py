"""shopify-flush-pending-events: force a full status push cycle right now.

Useful before uninstalling ERPClaw (or tearing down a shopify_account)
so any queued GDPR dispatch commands are processed before we lose the
HMAC secret. Trivial wrapper around status_push.push_all().

Usage:
  --action shopify-flush-pending-events [--worker-url ...]
"""
import os
import sys

LIB_PATH = os.path.expanduser("~/.openclaw/erpclaw/lib")
if LIB_PATH not in sys.path:
    sys.path.insert(0, LIB_PATH)

from erpclaw_lib.response import ok

from status_push import push_all, DEFAULT_WORKER_URL


def shopify_flush_pending_events(conn, args):
    worker_url = getattr(args, "worker_url", None) or DEFAULT_WORKER_URL
    # Drive two consecutive push cycles so any commands returned in the
    # first cycle get acked in the second. Avoids the one-round-trip lag
    # that would otherwise leave commands pending on the Worker.
    ack_ids = {}
    first = push_all(conn, worker_url=worker_url, ack_ids_by_shop=ack_ids)
    second = None
    if any(ack_ids.values()):
        second = push_all(conn, worker_url=worker_url, ack_ids_by_shop=ack_ids)
    ok({
        "worker_url": worker_url,
        "first_cycle": first,
        "second_cycle": second,
        "commands_dispatched": sum(
            sum(1 for d in r.get("dispatched", []) if d.get("dispatched"))
            for r in first
            if isinstance(r, dict)
        ),
    })


FLUSH_ACTIONS = {
    "shopify-flush-pending-events": shopify_flush_pending_events,
}
