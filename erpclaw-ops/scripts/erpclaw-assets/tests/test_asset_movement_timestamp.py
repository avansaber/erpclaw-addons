"""record-asset-movement keeps moving the asset after the timestamp conversion."""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from assets_helpers import build_gl_env, call_action, is_ok, load_db_query, ns  # noqa: E402

M = load_db_query()


def test_transfer_updates_location_and_stamps_the_asset(conn):
    env = build_gl_env(conn)
    r = call_action(M.record_asset_movement, conn, ns(
        asset_id=env["asset_id"], movement_type="transfer",
        movement_date="2026-03-01", to_location="Warehouse B"))
    assert is_ok(r), r
    row = conn.execute("SELECT location, updated_at FROM asset WHERE id = ?",
                       (env["asset_id"],)).fetchone()
    assert row["location"] == "Warehouse B"
    assert row["updated_at"] is not None
