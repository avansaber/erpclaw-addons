"""Unit tests for the SSE event-stream parser + signing helper.

We don't spin up a real HTTP server; the parser is pure and can be
exercised against a fake file-like object. The full run_forever loop is
covered by the manual staging test (§4.6-ish, SSE endpoint already
verified in the Worker suite).
"""
import hashlib
import hmac as hmac_mod
import importlib
import io

import pytest


@pytest.fixture
def sse_module():
    import sse_client as _sse
    importlib.reload(_sse)
    return _sse


def test_sign_get_matches_worker_format(sse_module):
    secret = "a" * 64
    shop = "demo.myshopify.com"
    ts = 1700000000
    sig = sse_module._sign_get(secret, shop, ts)
    expected = hmac_mod.new(
        secret.encode(),
        f"{shop}|{ts}|{hashlib.sha256(b'').hexdigest()}".encode(),
        hashlib.sha256,
    ).hexdigest()
    assert sig == expected


class _FakeResp:
    """Fake urllib response for _iter_events. Streams pre-baked bytes in
    chunks."""
    def __init__(self, chunks):
        self._chunks = list(chunks)

    def read(self, _n):
        if not self._chunks:
            return b""
        return self._chunks.pop(0)


def test_iter_events_parses_hello_and_command(sse_module):
    raw = (
        b"event: hello\n"
        b"data: {\"server_time\":\"2026-04-24T00:00:00Z\"}\n"
        b"\n"
        b"event: command\n"
        b"data: {\"id\":\"cmd_a\",\"type\":\"sync-now\",\"payload\":{}}\n"
        b"\n"
    )
    resp = _FakeResp([raw])
    events = list(sse_module._iter_events(resp))
    assert len(events) == 2
    assert events[0]["event"] == "hello"
    assert events[1]["event"] == "command"
    assert events[1]["data"]["id"] == "cmd_a"


def test_iter_events_tolerates_split_chunks(sse_module):
    # Split an event across two reads to confirm buffer handles it.
    chunks = [
        b"event: command\ndata: {\"id\":\"cmd_a\",",
        b"\"type\":\"sync-now\",\"payload\":{}}\n\n",
    ]
    resp = _FakeResp(chunks)
    events = list(sse_module._iter_events(resp))
    assert len(events) == 1
    assert events[0]["data"]["id"] == "cmd_a"


def test_iter_events_handles_malformed_data(sse_module):
    raw = b"event: command\ndata: not-json\n\n"
    resp = _FakeResp([raw])
    events = list(sse_module._iter_events(resp))
    assert len(events) == 1
    assert "raw" in events[0]["data"]


def test_iter_events_empty_stream(sse_module):
    resp = _FakeResp([])
    events = list(sse_module._iter_events(resp))
    assert events == []
