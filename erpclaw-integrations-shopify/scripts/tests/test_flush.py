"""Unit tests for shopify-flush-pending-events.

Since flush is a thin wrapper around push_all we mock push_all directly
and verify:
  - A single cycle runs when no commands come back (nothing to ack).
  - Two cycles run when the first returns commands + ack_ids (round-trips
    acks cleanly).
  - Returns the composite result structure.
"""
import importlib
from unittest.mock import patch

import pytest

from shopify_test_helpers import build_env, call_action, is_ok


@pytest.fixture
def flush_module():
    import flush as _f
    importlib.reload(_f)
    return _f


class _Args:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def test_single_cycle_when_no_commands(db_path, conn, flush_module):
    build_env(conn)

    def fake_push_all(conn_arg, worker_url=None, ack_ids_by_shop=None):  # noqa: ARG001
        return [{"shop": "demo.myshopify.com", "pushed_at": "now", "dispatched": []}]

    with patch.object(flush_module, "push_all", side_effect=fake_push_all):
        result = call_action(flush_module.shopify_flush_pending_events, conn, _Args())

    assert is_ok(result)
    assert result["second_cycle"] is None
    assert result["commands_dispatched"] == 0


def test_two_cycles_when_commands_dispatched(db_path, conn, flush_module):
    build_env(conn)
    call_count = {"n": 0}

    def fake_push_all(conn_arg, worker_url=None, ack_ids_by_shop=None):  # noqa: ARG001
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Simulate dispatch: populate ack_ids_by_shop so flush enters
            # the second cycle.
            ack_ids_by_shop["demo.myshopify.com"] = ["cmd_a", "cmd_b"]
            return [
                {
                    "shop": "demo.myshopify.com",
                    "pushed_at": "now",
                    "dispatched": [
                        {"id": "cmd_a", "type": "sync-now", "dispatched": True},
                        {"id": "cmd_b", "type": "refresh-token", "dispatched": True},
                    ],
                }
            ]
        return [{"shop": "demo.myshopify.com", "pushed_at": "now2", "dispatched": []}]

    with patch.object(flush_module, "push_all", side_effect=fake_push_all):
        result = call_action(flush_module.shopify_flush_pending_events, conn, _Args())

    assert is_ok(result)
    assert call_count["n"] == 2
    assert result["second_cycle"] is not None
    assert result["commands_dispatched"] == 2
