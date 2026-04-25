"""Long-lived SSE client for ERPClaw instances in 'active' mode.

This script is NOT an --action; it's meant to be launched as a daemon
subprocess when the primary 15-minute cron also fires (i.e. when an
active-mode ERPClaw has a long-lived host process available, such as
webclaw or Telegram polling).

Connects to the Worker's `GET /events/:shop` endpoint and dispatches
commands as they arrive. Reconnects on error with exponential backoff.

Usage:
  python3 -m sse_client [--worker-url ...]

Runs until killed.
"""
import argparse
import hashlib
import hmac as hmac_mod
import json
import os
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.request

LIB_PATH = os.path.expanduser("~/.openclaw/erpclaw/lib")
if LIB_PATH not in sys.path:
    sys.path.insert(0, LIB_PATH)

from erpclaw_lib.db import setup_pragmas

# scripts/ on path so we can import sibling modules.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from dispatcher import dispatch_command
from shopify_helpers import decrypt_token

DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
DEFAULT_WORKER_URL = "https://shopify.erpclaw.ai"
BACKOFF_SEQUENCE = [1, 2, 4, 8, 15]  # seconds
CONNECT_TIMEOUT = 10
# Stream read chunk size. Keep small so we unblock on partial data.
READ_CHUNK = 4096


def _sign_get(secret_hex, shop, timestamp):
    """HMAC signature for a GET /events/:shop call with empty body."""
    body_hash = hashlib.sha256(b"").hexdigest()
    payload = f"{shop}|{timestamp}|{body_hash}"
    return hmac_mod.new(
        secret_hex.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _active_shops(conn):
    """Yield (shop_domain, hmac_secret_hex) for every active-mode oauth shop."""
    rows = conn.execute(
        "SELECT shop_domain, hmac_secret_enc FROM shopify_account "
        "WHERE status = 'active' AND pairing_method = 'oauth' "
        "AND status_mode = 'active' AND hmac_secret_enc IS NOT NULL"
    ).fetchall()
    for row in rows:
        yield row["shop_domain"], decrypt_token(row["hmac_secret_enc"])


def _open_stream(worker_url, shop, hmac_secret):
    ts = int(time.time())
    sig = _sign_get(hmac_secret, shop, ts)
    url = f"{worker_url.rstrip('/')}/events/{shop}"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "accept": "text/event-stream",
            "authorization": f"HMAC-SHA256 {sig}",
            "x-timestamp": str(ts),
            "user-agent": "erpclaw-shopify-sse/1.1",
        },
    )
    ctx = ssl.create_default_context()
    return urllib.request.urlopen(req, timeout=CONNECT_TIMEOUT, context=ctx)


def _iter_events(resp):
    """Yield {event, data} dicts as SSE events arrive on `resp`."""
    buf = b""
    while True:
        chunk = resp.read(READ_CHUNK)
        if not chunk:
            return
        buf += chunk
        while b"\n\n" in buf:
            frame, buf = buf.split(b"\n\n", 1)
            event_name = "message"
            data_lines = []
            for line in frame.decode("utf-8", errors="replace").split("\n"):
                if line.startswith("event:"):
                    event_name = line[6:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].strip())
            if not data_lines:
                continue
            try:
                data = json.loads("\n".join(data_lines))
            except Exception:  # noqa: BLE001
                data = {"raw": "\n".join(data_lines)}
            yield {"event": event_name, "data": data}


def _run_one_shop(conn, worker_url, shop, hmac_secret):
    """Open a stream and pump events until it closes. Returns without raising."""
    try:
        resp = _open_stream(worker_url, shop, hmac_secret)
    except urllib.error.HTTPError as exc:
        return {"shop": shop, "error": f"HTTP {exc.code}"}
    except urllib.error.URLError as exc:
        return {"shop": shop, "error": f"URLError {exc.reason}"}
    dispatched = 0
    with resp:
        for event in _iter_events(resp):
            if event["event"] == "hello":
                continue
            if event["event"] == "ping":
                continue
            if event["event"] == "command":
                result = dispatch_command(conn, shop, event["data"])
                if result.get("dispatched"):
                    dispatched += 1
                # Acks are sent via the next regular status push (see
                # status_push.push_all). We don't send them here; SSE is
                # delivery-only per the API contract.
    return {"shop": shop, "dispatched": dispatched}


def run_forever(db_path=None, worker_url=None):
    db_path = db_path or DEFAULT_DB_PATH
    worker_url = worker_url or DEFAULT_WORKER_URL
    backoff_idx = 0
    while True:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        setup_pragmas(conn)
        try:
            shops = list(_active_shops(conn))
        finally:
            conn.close()
        if not shops:
            time.sleep(60)
            continue
        # v1: sequential connections. One long-poll at a time is fine for
        # a single-tenant ERPClaw (a merchant rarely has many stores).
        # Multi-shop active merchants can upgrade to asyncio in v1.1.
        for shop, secret in shops:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            setup_pragmas(conn)
            try:
                result = _run_one_shop(conn, worker_url, shop, secret)
            finally:
                conn.close()
            if "error" in result:
                wait = BACKOFF_SEQUENCE[min(backoff_idx, len(BACKOFF_SEQUENCE) - 1)]
                time.sleep(wait)
                backoff_idx = min(backoff_idx + 1, len(BACKOFF_SEQUENCE) - 1)
            else:
                backoff_idx = 0


def _main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--worker-url", default=None)
    args = parser.parse_args()
    try:
        run_forever(db_path=args.db_path, worker_url=args.worker_url)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(_main() or 0)
