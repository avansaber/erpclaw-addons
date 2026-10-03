"""Recording inspection readings updates the inspection's own status.

Before the fix, record-inspection-readings stored each reading's verdict
but never touched quality_inspection.status, so a failed batch still read
'accepted' until someone ran evaluate-inspection. Every test below reads
the stored rows back and pins exact values.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from quality_helpers import call_action, is_error, is_ok, load_db_query, ns  # noqa: E402
from erpclaw_lib.query import Field, P, Q, Table  # noqa: E402

M = load_db_query()

T_qi = Table("quality_inspection")
T_qir = Table("quality_inspection_reading")

_SINGLE_SPEC = [{"parameter_name": "Diameter",
                 "parameter_type": "numeric",
                 "min_value": "10", "max_value": "20"}]

_PAIR_SPECS = [
    {"parameter_name": "Diameter", "parameter_type": "numeric",
     "min_value": "10", "max_value": "20"},
    {"parameter_name": "Colour", "parameter_type": "non_numeric",
     "acceptance_value": "pass"},
]


def _make_template(conn, specs):
    r = call_action(M.add_inspection_template, conn, ns(
        name="Status template",
        inspection_type="incoming",
        parameters=json.dumps(specs),
    ))
    assert is_ok(r), r
    by_name = {p["parameter_name"]: p["id"]
               for p in r["template"]["parameters"]}
    return r["template"]["id"], by_name


def _make_inspection(conn, env, template_id):
    r = call_action(M.add_quality_inspection, conn, ns(
        item_id=env["item_id"],
        inspection_type="incoming",
        inspection_date="2026-03-10",
        template_id=template_id,
        company_id=env["company_id"],
    ))
    assert is_ok(r), r
    return r["inspection"]["id"]


def _inspection_status(conn, inspection_id):
    q = (Q.from_(T_qi).select(T_qi.status)
         .where(T_qi.id == P()))
    return conn.execute(q.get_sql(), (inspection_id,)).fetchone()["status"]


def _readings(conn, inspection_id):
    q = (Q.from_(T_qir).select(T_qir.star)
         .where(T_qir.quality_inspection_id == P()))
    return [dict(r)
            for r in conn.execute(q.get_sql(), (inspection_id,)).fetchall()]


def _status_audits(conn, inspection_id):
    """Status-change audit rows for one inspection, oldest first."""
    rows = conn.execute(
        """SELECT old_values, new_values FROM audit_log
           WHERE action = ? AND entity_type = ? AND entity_id = ?
           AND old_values IS NOT NULL
           ORDER BY rowid""",
        ("record-inspection-readings", "quality_inspection", inspection_id),
    ).fetchall()
    return [(json.loads(r[0]), json.loads(r[1])) for r in rows]


def test_out_of_spec_reading_rejects_inspection(conn, env):
    template_id, params = _make_template(conn, _SINGLE_SPEC)
    inspection_id = _make_inspection(conn, env, template_id)

    r = call_action(M.record_inspection_readings, conn, ns(
        quality_inspection_id=inspection_id,
        readings=json.dumps([{"parameter_id": params["Diameter"],
                              "reading_value": "5"}]),
    ))
    assert is_ok(r), r
    assert r["inspection_status"] == "rejected"
    assert r["previous_inspection_status"] == "accepted"
    assert _inspection_status(conn, inspection_id) == "rejected"


def test_in_spec_reading_keeps_accepted(conn, env):
    template_id, params = _make_template(conn, _SINGLE_SPEC)
    inspection_id = _make_inspection(conn, env, template_id)

    r = call_action(M.record_inspection_readings, conn, ns(
        quality_inspection_id=inspection_id,
        readings=json.dumps([{"parameter_id": params["Diameter"],
                              "reading_value": "15"}]),
    ))
    assert is_ok(r), r
    assert r["inspection_status"] == "accepted"
    assert r["previous_inspection_status"] == "accepted"
    assert _inspection_status(conn, inspection_id) == "accepted"
    assert _status_audits(conn, inspection_id) == []


def test_mixed_readings_partially_accepted(conn, env):
    template_id, params = _make_template(conn, _PAIR_SPECS)
    inspection_id = _make_inspection(conn, env, template_id)

    r = call_action(M.record_inspection_readings, conn, ns(
        quality_inspection_id=inspection_id,
        readings=json.dumps([
            {"parameter_id": params["Diameter"], "reading_value": "15"},
            {"parameter_id": params["Colour"], "reading_value": "fail"},
        ]),
    ))
    assert is_ok(r), r
    assert r["inspection_status"] == "partially_accepted"
    assert r["previous_inspection_status"] == "accepted"
    assert _inspection_status(conn, inspection_id) == "partially_accepted"


def test_correcting_a_reading_restores_status(conn, env):
    template_id, params = _make_template(conn, _SINGLE_SPEC)
    inspection_id = _make_inspection(conn, env, template_id)

    bad = call_action(M.record_inspection_readings, conn, ns(
        quality_inspection_id=inspection_id,
        readings=json.dumps([{"parameter_id": params["Diameter"],
                              "reading_value": "5"}]),
    ))
    assert is_ok(bad), bad
    assert bad["inspection_status"] == "rejected"

    good = call_action(M.record_inspection_readings, conn, ns(
        quality_inspection_id=inspection_id,
        readings=json.dumps([{"parameter_id": params["Diameter"],
                              "reading_value": "15"}]),
    ))
    assert is_ok(good), good
    assert good["inspection_status"] == "accepted"
    assert good["previous_inspection_status"] == "rejected"
    assert _inspection_status(conn, inspection_id) == "accepted"
    assert _status_audits(conn, inspection_id) == [
        ({"status": "accepted"}, {"status": "rejected"}),
        ({"status": "rejected"}, {"status": "accepted"}),
    ]


def test_status_change_audited_in_same_transaction(conn, env):
    template_id, params = _make_template(conn, _SINGLE_SPEC)
    inspection_id = _make_inspection(conn, env, template_id)

    r = call_action(M.record_inspection_readings, conn, ns(
        quality_inspection_id=inspection_id,
        readings=json.dumps([{"parameter_id": params["Diameter"],
                              "reading_value": "5"}]),
    ))
    assert is_ok(r), r

    rows = conn.execute(
        """SELECT action, entity_type, entity_id, old_values, new_values
           FROM audit_log
           WHERE action = ? AND entity_type = ? AND entity_id = ?
           AND old_values IS NOT NULL""",
        ("record-inspection-readings", "quality_inspection", inspection_id),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "record-inspection-readings"
    assert rows[0][1] == "quality_inspection"
    assert rows[0][2] == inspection_id
    assert json.loads(rows[0][3]) == {"status": "accepted"}
    assert json.loads(rows[0][4]) == {"status": "rejected"}
    assert _inspection_status(conn, inspection_id) == "rejected"


def test_evaluate_inspection_unchanged(conn, env):
    template_id, params = _make_template(conn, _SINGLE_SPEC)
    inspection_id = _make_inspection(conn, env, template_id)

    recorded = call_action(M.record_inspection_readings, conn, ns(
        quality_inspection_id=inspection_id,
        readings=json.dumps([{"parameter_id": params["Diameter"],
                              "reading_value": "5"}]),
    ))
    assert is_ok(recorded), recorded

    r = call_action(M.evaluate_inspection, conn, ns(
        quality_inspection_id=inspection_id,
    ))
    assert is_ok(r), r
    assert set(r.keys()) == {"status", "inspection_id", "old_status",
                             "new_status", "total_readings", "accepted_count",
                             "rejected_count", "message"}
    assert r["new_status"] == "rejected"
    assert r["total_readings"] == 1
    assert r["accepted_count"] == 0
    assert r["rejected_count"] == 1


def test_invalid_reading_changes_nothing(conn, env):
    template_id, params = _make_template(conn, _SINGLE_SPEC)
    inspection_id = _make_inspection(conn, env, template_id)

    r = call_action(M.record_inspection_readings, conn, ns(
        quality_inspection_id=inspection_id,
        readings=json.dumps([{"parameter_id": params["Diameter"],
                              "reading_value": "not-a-number"}]),
    ))
    assert is_error(r)
    assert r["message"] == \
        "Reading 0: reading_value 'not-a-number' is not a valid number"

    stored = {row["parameter_id"]: row
              for row in _readings(conn, inspection_id)}
    assert stored[params["Diameter"]]["reading_value"] is None
    assert stored[params["Diameter"]]["status"] == "accepted"
    assert _inspection_status(conn, inspection_id) == "accepted"
    assert _status_audits(conn, inspection_id) == []
