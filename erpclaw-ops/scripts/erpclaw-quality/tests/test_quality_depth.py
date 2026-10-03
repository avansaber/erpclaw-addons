"""Behavioural depth tests for record-inspection-readings, evaluate-inspection
and the read-only side of quality-dashboard.

The contract suite proves these action names resolve, and the L1 suite proved
the envelopes had the right keys. Neither read the database back, so a
perfectly shaped response with nothing stored behind it still passed. Every
happy-path test below reads the stored rows back through the query builder
and pins exact values; every refusal test proves the message is truthful and
the database is byte-identical afterwards.

Ledger note for all three actions (so a later reader does not add assertions
that cannot hold): none of them reaches the ledger. They write quality rows
(plus audit rows) only. The gl_entry / stock_ledger_entry zero-count pins are
the whole ledger story: there are no debit or credit legs, and no balance to
recompute.

Values are text: reading values, limits and goal figures live in TEXT
columns, so every value assertion compares exact strings. Decimal appears
only to recompute the dashboard pass rate. Never float, never approximate.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from quality_helpers import (  # noqa: E402
    _uuid, call_action, is_error, is_ok, load_db_query, ns,
)
from erpclaw_lib.query import Field, P, Q, Table, fn  # noqa: E402

M = load_db_query()

T_qi = Table("quality_inspection")
T_qir = Table("quality_inspection_reading")
T_gl = Table("gl_entry")
T_sle = Table("stock_ledger_entry")

_SNAPSHOT_TABLES = (
    "quality_inspection",
    "quality_inspection_reading",
    "quality_inspection_template",
    "quality_inspection_parameter",
    "non_conformance",
    "quality_goal",
    "naming_series",
    "audit_log",
    "gl_entry",
    "stock_ledger_entry",
)


def _snapshot(conn):
    """Text-normalised dump of every table these actions can touch."""
    snap = {}
    for name in _SNAPSHOT_TABLES:
        tbl = Table(name)
        rows = conn.execute(Q.from_(tbl).select(tbl.star).get_sql()).fetchall()
        snap[name] = sorted(
            tuple(sorted((key, str(value)) for key, value in dict(row).items()))
            for row in rows
        )
    return snap


def _row(conn, table, row_id):
    tbl = Table(table)
    q = Q.from_(tbl).select(tbl.star).where(Field("id") == P())
    row = conn.execute(q.get_sql(), (row_id,)).fetchone()
    assert row is not None, "expected a row in %s with id %s" % (table, row_id)
    return dict(row)


def _readings(conn, inspection_id):
    q = (Q.from_(T_qir).select(T_qir.star)
         .where(T_qir.quality_inspection_id == P()))
    return [dict(r)
            for r in conn.execute(q.get_sql(), (inspection_id,)).fetchall()]


def _count(conn, table):
    tbl = Table(table)
    q = Q.from_(tbl).select(fn.Count("*").as_("cnt"))
    return conn.execute(q.get_sql()).fetchone()["cnt"]


def _make_template(conn, specs):
    r = call_action(M.add_inspection_template, conn, ns(
        name="Depth template",
        inspection_type="incoming",
        parameters=json.dumps(specs),
    ))
    assert is_ok(r), r
    by_name = {p["parameter_name"]: p["id"]
               for p in r["template"]["parameters"]}
    return r["template"]["id"], by_name


def _make_inspection(conn, env, template_id=None):
    r = call_action(M.add_quality_inspection, conn, ns(
        item_id=env["item_id"],
        inspection_type="incoming",
        inspection_date="2026-03-10",
        template_id=template_id,
        company_id=env["company_id"],
    ))
    assert is_ok(r), r
    return r["inspection"]["id"]


_NUMERIC_SPEC = [{"parameter_name": "Thickness",
                  "parameter_type": "numeric",
                  "min_value": "0.5", "max_value": "1.5", "uom": "mm"}]

_MIXED_SPECS = [
    {"parameter_name": "Thickness", "parameter_type": "numeric",
     "min_value": "0.5", "max_value": "1.5", "uom": "mm"},
    {"parameter_name": "Colour", "parameter_type": "non_numeric",
     "acceptance_value": "pass"},
]


# ===================================================================
# record-inspection-readings (stored rows; no ledger legs by design)
# ===================================================================

class TestRecordInspectionReadingsDepth:
    def test_record_mixed_verdicts_stores_exact_values(self, conn, env):
        template_id, params = _make_template(conn, _MIXED_SPECS)
        inspection_id = _make_inspection(conn, env, template_id)
        decoy_id = _make_inspection(conn, env, template_id)

        r = call_action(M.record_inspection_readings, conn, ns(
            quality_inspection_id=inspection_id,
            readings=json.dumps([
                {"parameter_id": params["Thickness"],
                 "reading_value": "1.0", "remarks": "caliper 1"},
                {"parameter_id": params["Colour"],
                 "reading_value": "fail"},
            ]),
        ))
        assert is_ok(r), r

        # No ledger legs: quality readings never post to the books.
        assert _count(conn, "gl_entry") == 0
        assert _count(conn, "stock_ledger_entry") == 0

        stored = {row["parameter_id"]: row
                  for row in _readings(conn, inspection_id)}
        assert stored[params["Thickness"]]["reading_value"] == "1.0"
        assert stored[params["Thickness"]]["status"] == "accepted"
        assert stored[params["Thickness"]]["remarks"] == "caliper 1"
        assert stored[params["Colour"]]["reading_value"] == "fail"
        assert stored[params["Colour"]]["status"] == "rejected"

        # The envelope mirrors the store; a shaped response alone proves
        # nothing, so pin them against each other.
        echoed = {row["parameter_id"]: row for row in r["readings"]}
        for param_id, row in stored.items():
            assert echoed[param_id]["reading_value"] == row["reading_value"]
            assert echoed[param_id]["status"] == row["status"]
            assert echoed[param_id]["id"] == row["id"]

        # Recording values updates the inspection itself: one rejected
        # reading leaves the parent partially_accepted.
        assert _row(conn, "quality_inspection",
                    inspection_id)["status"] == "partially_accepted"

        # The decoy inspection keeps its pristine auto-created readings.
        for row in _readings(conn, decoy_id):
            assert row["reading_value"] is None
            assert row["status"] == "accepted"

    def test_record_unknown_inspection_refused_without_write(
        self, conn, env,
    ):
        missing = _uuid()
        before = _snapshot(conn)
        r = call_action(M.record_inspection_readings, conn, ns(
            quality_inspection_id=missing,
            readings=json.dumps([{"parameter_id": _uuid(),
                                  "reading_value": "1.0"}]),
        ))
        assert is_error(r)
        assert r["message"] == "Quality inspection %s not found" % missing
        assert _snapshot(conn) == before

    def test_record_missing_readings_refused_without_write(self, conn, env):
        inspection_id = _make_inspection(conn, env)
        before = _snapshot(conn)
        r = call_action(M.record_inspection_readings, conn, ns(
            quality_inspection_id=inspection_id,
        ))
        assert is_error(r)
        assert r["message"] == "--readings is required (JSON array)"
        assert _snapshot(conn) == before

    def test_record_mid_loop_refusal_documents_uncommitted_write(
        self, conn, env,
    ):
        # DOCUMENTED REAL BEHAVIOUR, deliberately not fixed (see CHANGES.md):
        # readings are written one by one inside a single transaction and a
        # later reading can fail validation after an earlier one was already
        # executed. The error is truthful and nothing is committed, but on a
        # shared connection the first write stays visible until rollback.
        template_id, params = _make_template(conn, _NUMERIC_SPEC)
        inspection_id = _make_inspection(conn, env, template_id)

        r = call_action(M.record_inspection_readings, conn, ns(
            quality_inspection_id=inspection_id,
            readings=json.dumps([
                {"parameter_id": params["Thickness"],
                 "reading_value": "9.9"},
                {"parameter_id": "no-such-parameter",
                 "reading_value": "1.0"},
            ]),
        ))
        assert is_error(r)
        assert r["message"] == \
            "Reading 1: parameter no-such-parameter not found"

        dirty = {row["parameter_id"]: row
                 for row in _readings(conn, inspection_id)}
        assert dirty[params["Thickness"]]["reading_value"] == "9.9"
        assert dirty[params["Thickness"]]["status"] == "rejected"

        conn.rollback()
        clean = {row["parameter_id"]: row
                 for row in _readings(conn, inspection_id)}
        assert clean[params["Thickness"]]["reading_value"] is None
        assert clean[params["Thickness"]]["status"] == "accepted"


# ===================================================================
# evaluate-inspection (stored status flip; no ledger legs by design)
# ===================================================================

class TestEvaluateInspectionDepth:
    def test_evaluate_all_rejected_flips_status_to_rejected(
        self, conn, env,
    ):
        template_id, params = _make_template(conn, _NUMERIC_SPEC)
        inspection_id = _make_inspection(conn, env, template_id)
        decoy_id = _make_inspection(conn, env)

        r = call_action(M.record_inspection_readings, conn, ns(
            quality_inspection_id=inspection_id,
            readings=json.dumps([{"parameter_id": params["Thickness"],
                                  "reading_value": "99"}]),
        ))
        assert is_ok(r), r
        assert _row(conn, "quality_inspection",
                    inspection_id)["status"] == "rejected"

        r = call_action(M.evaluate_inspection, conn, ns(
            quality_inspection_id=inspection_id,
        ))
        assert is_ok(r), r
        assert r["old_status"] == "rejected"
        assert r["new_status"] == "rejected"
        assert r["total_readings"] == 1
        assert r["accepted_count"] == 0
        assert r["rejected_count"] == 1

        assert _row(conn, "quality_inspection",
                    inspection_id)["status"] == "rejected"
        assert _count(conn, "gl_entry") == 0
        assert _count(conn, "stock_ledger_entry") == 0
        assert _row(conn, "quality_inspection",
                    decoy_id)["status"] == "accepted"

    def test_evaluate_all_accepted_flips_status_back_to_accepted(
        self, conn, env,
    ):
        template_id, params = _make_template(conn, _NUMERIC_SPEC)
        inspection_id = _make_inspection(conn, env, template_id)

        bad = call_action(M.record_inspection_readings, conn, ns(
            quality_inspection_id=inspection_id,
            readings=json.dumps([{"parameter_id": params["Thickness"],
                                  "reading_value": "99"}]),
        ))
        assert is_ok(bad), bad
        first = call_action(M.evaluate_inspection, conn, ns(
            quality_inspection_id=inspection_id,
        ))
        assert is_ok(first), first
        assert first["new_status"] == "rejected"

        good = call_action(M.record_inspection_readings, conn, ns(
            quality_inspection_id=inspection_id,
            readings=json.dumps([{"parameter_id": params["Thickness"],
                                  "reading_value": "1.0"}]),
        ))
        assert is_ok(good), good
        r = call_action(M.evaluate_inspection, conn, ns(
            quality_inspection_id=inspection_id,
        ))
        assert is_ok(r), r
        assert r["old_status"] == "accepted"
        assert r["new_status"] == "accepted"
        assert _row(conn, "quality_inspection",
                    inspection_id)["status"] == "accepted"

    def test_evaluate_mixed_readings_yields_partially_accepted(
        self, conn, env,
    ):
        template_id, params = _make_template(conn, _MIXED_SPECS)
        inspection_id = _make_inspection(conn, env, template_id)

        rec = call_action(M.record_inspection_readings, conn, ns(
            quality_inspection_id=inspection_id,
            readings=json.dumps([
                {"parameter_id": params["Thickness"],
                 "reading_value": "1.0"},
                {"parameter_id": params["Colour"],
                 "reading_value": "fail"},
            ]),
        ))
        assert is_ok(rec), rec

        r = call_action(M.evaluate_inspection, conn, ns(
            quality_inspection_id=inspection_id,
        ))
        assert is_ok(r), r
        assert r["old_status"] == "partially_accepted"
        assert r["new_status"] == "partially_accepted"
        assert r["total_readings"] == 2
        assert r["accepted_count"] == 1
        assert r["rejected_count"] == 1
        assert _row(conn, "quality_inspection",
                    inspection_id)["status"] == "partially_accepted"

    def test_evaluate_unknown_inspection_refused_without_write(
        self, conn, env,
    ):
        missing = _uuid()
        before = _snapshot(conn)
        r = call_action(M.evaluate_inspection, conn, ns(
            quality_inspection_id=missing,
        ))
        assert is_error(r)
        assert r["message"] == "Quality inspection %s not found" % missing
        assert _snapshot(conn) == before

    def test_evaluate_without_readings_refused_without_write(
        self, conn, env,
    ):
        inspection_id = _make_inspection(conn, env)
        before = _snapshot(conn)
        r = call_action(M.evaluate_inspection, conn, ns(
            quality_inspection_id=inspection_id,
        ))
        assert is_error(r)
        assert r["message"] == \
            "No readings found for inspection %s" % inspection_id
        assert _snapshot(conn) == before
        assert _row(conn, "quality_inspection",
                    inspection_id)["status"] == "accepted"


# ===================================================================
# quality-dashboard read-only proof (the seeded aggregate lives in
# test_quality.py::TestQualityDashboard; this pins the empty zero-state
# and that the call itself stores nothing).
# ===================================================================

class TestQualityDashboardReadOnly:
    def test_dashboard_empty_reports_zeros_and_writes_nothing(
        self, conn, env,
    ):
        before = _snapshot(conn)
        r = call_action(M.quality_dashboard, conn, ns())
        assert is_ok(r), r
        dash = r["dashboard"]

        assert dash["inspections"]["total"] == 0
        assert dash["inspections"]["by_status"] == {}
        assert dash["inspections"]["pass_rate_pct"] == "0.00"
        assert dash["non_conformances"]["total_open"] == 0
        assert dash["non_conformances"]["by_severity"] == {}
        assert dash["quality_goals"]["total"] == 0
        assert dash["quality_goals"]["by_status"] == {}

        # Read-only: not even an audit row may appear.
        assert _snapshot(conn) == before
