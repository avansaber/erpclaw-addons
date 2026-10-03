"""update-quality-goal keeps updating after the timestamp conversion."""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from quality_helpers import call_action, is_ok, load_db_query, ns  # noqa: E402

M = load_db_query()


def test_update_goal_name_and_target(conn, env):
    added = call_action(M.add_quality_goal, conn, ns(name="Reduce defects", target_value="2"))
    assert is_ok(added), added
    goal_id = added["quality_goal"]["id"]
    r = call_action(M.update_quality_goal, conn, ns(
        quality_goal_id=goal_id, name="Reduce defects further", target_value="1"))
    assert is_ok(r), r
    row = conn.execute("SELECT name, target_value, updated_at FROM quality_goal WHERE id = ?",
                       (goal_id,)).fetchone()
    assert row["name"] == "Reduce defects further"
    assert row["target_value"] == "1"
    assert row["updated_at"] is not None
