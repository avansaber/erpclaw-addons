"""L1 pytest tests for erpclaw-crm Wave 1B F3 (Pipeline stages — 6 actions).

Covers: add-crm-pipeline, add-crm-pipeline-stage, update-crm-pipeline-stage,
list-crm-pipelines, list-crm-pipeline-stages, set-opportunity-pipeline-stage.
Plus the contract negative controls + the dual-write path:
  - stage_order collision rejected (and --shift-existing renumbers)
  - terminal uniqueness (one won + one lost per pipeline)
  - cross-pipeline set-stage blocked
  - update-opportunity --stage / mark-won / mark-lost dual-write pipeline_stage_id
  - pipeline-report dual-path (custom pipeline + legacy text rows coexist)
"""
import os
import sys
import uuid

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest
from crm_helpers import call_action, ns, is_ok, is_error, load_db_query

MOD = load_db_query()


_DEFAULTS = dict(
    name=None, description=None, set_as_default=False, shift_existing=False,
    id=None, pipeline=None, order=None, terminal=None, probability=None,
    is_active=None, opportunity=None, stage=None,
    opportunity_id=None, opportunity_name=None, lead_id=None, customer_id=None,
    opportunity_type=None, expected_revenue=None, expected_closing_date=None,
    assigned_to=None, next_follow_up_date=None, lost_reason=None,
    from_date=None, to_date=None, status=None, search=None,
    limit="20", offset="0", db_path=None, company_id=None,
)


def a(**kw):
    d = dict(_DEFAULTS)
    d.update(kw)
    return ns(**d)


def _default_pipeline_id(conn):
    return conn.execute("SELECT id FROM crm_pipeline WHERE is_default=1").fetchone()["id"]


def _stage_id(conn, pipeline_id, name):
    return conn.execute(
        "SELECT id FROM crm_pipeline_stage WHERE crm_pipeline_id=? AND name=?",
        (pipeline_id, name)).fetchone()["id"]


def _add_pipeline(conn, name="Enterprise B2B", **kw):
    r = call_action(MOD.add_crm_pipeline, conn, a(name=name, **kw))
    assert is_ok(r), r
    return r["crm_pipeline"]["id"]


def _add_stage(conn, pipeline_id, name, **kw):
    r = call_action(MOD.add_crm_pipeline_stage, conn,
                    a(pipeline=pipeline_id, name=name, **kw))
    assert is_ok(r), r
    return r["crm_pipeline_stage"]["id"]


def _seed_opportunity(conn, company_id, name="Acme expansion", revenue="10000"):
    oid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO opportunity (id, opportunity_name, stage, expected_revenue, company_id) "
        "VALUES (?,?,?,?,?)", (oid, name, "new", revenue, company_id))
    conn.commit()
    return oid


# ===========================================================================
# Default pipeline seeded by create_crmadv_tables()
# ===========================================================================

class TestDefaultPipelineSeeded:
    def test_default_pipeline_exists_with_7_stages(self, conn, env):
        pid = _default_pipeline_id(conn)
        assert pid
        n = conn.execute(
            "SELECT COUNT(*) c FROM crm_pipeline_stage WHERE crm_pipeline_id=?", (pid,)
        ).fetchone()["c"]
        assert n == 7
        name = conn.execute("SELECT name FROM crm_pipeline WHERE id=?", (pid,)).fetchone()["name"]
        assert name == "Standard Sales"


# ===========================================================================
# add-crm-pipeline
# ===========================================================================

class TestAddPipeline:
    def test_add_basic(self, conn, env):
        pid = _add_pipeline(conn, name="Enterprise B2B")
        row = conn.execute("SELECT * FROM crm_pipeline WHERE id=?", (pid,)).fetchone()
        assert row["name"] == "Enterprise B2B"
        assert row["is_default"] == 0
        assert row["is_active"] == 1

    def test_missing_name_rejected(self, conn, env):
        r = call_action(MOD.add_crm_pipeline, conn, a(name=None))
        assert is_error(r)

    def test_duplicate_name_case_insensitive_rejected(self, conn, env):
        _add_pipeline(conn, name="Enterprise B2B")
        r = call_action(MOD.add_crm_pipeline, conn, a(name="enterprise b2b"))
        assert is_error(r)

    def test_set_as_default_clears_other_defaults(self, conn, env):
        old_default = _default_pipeline_id(conn)
        pid = _add_pipeline(conn, name="New Default", set_as_default=True)
        assert conn.execute("SELECT is_default FROM crm_pipeline WHERE id=?", (pid,)).fetchone()[0] == 1
        assert conn.execute("SELECT is_default FROM crm_pipeline WHERE id=?", (old_default,)).fetchone()[0] == 0


# ===========================================================================
# add-crm-pipeline-stage
# ===========================================================================

class TestAddPipelineStage:
    def test_add_appends_order(self, conn, env):
        pid = _add_pipeline(conn)
        _add_stage(conn, pid, "Discovery")
        sid2 = _add_stage(conn, pid, "Demo")
        order = conn.execute("SELECT stage_order FROM crm_pipeline_stage WHERE id=?", (sid2,)).fetchone()[0]
        assert order == 2

    def test_explicit_order_collision_rejected(self, conn, env):
        pid = _add_pipeline(conn)
        _add_stage(conn, pid, "Discovery", order="1")
        r = call_action(MOD.add_crm_pipeline_stage, conn, a(pipeline=pid, name="Demo", order="1"))
        assert is_error(r)

    def test_shift_existing_renumbers(self, conn, env):
        pid = _add_pipeline(conn)
        s1 = _add_stage(conn, pid, "Discovery", order="1")
        s2 = _add_stage(conn, pid, "Demo", order="2")
        # Insert at order 1 with shift; Discovery+Demo bump to 2,3
        snew = _add_stage(conn, pid, "Intro", order="1", shift_existing=True)
        orders = {r["name"]: r["stage_order"] for r in conn.execute(
            "SELECT name, stage_order FROM crm_pipeline_stage WHERE crm_pipeline_id=? ORDER BY stage_order", (pid,))}
        assert orders == {"Intro": 1, "Discovery": 2, "Demo": 3}

    def test_duplicate_stage_name_rejected(self, conn, env):
        pid = _add_pipeline(conn)
        _add_stage(conn, pid, "Discovery")
        r = call_action(MOD.add_crm_pipeline_stage, conn, a(pipeline=pid, name="discovery"))
        assert is_error(r)

    def test_terminal_won_uniqueness(self, conn, env):
        pid = _add_pipeline(conn)
        _add_stage(conn, pid, "Closed Won", terminal="won")
        r = call_action(MOD.add_crm_pipeline_stage, conn,
                        a(pipeline=pid, name="Won Too", terminal="won"))
        assert is_error(r)

    def test_terminal_lost_uniqueness(self, conn, env):
        pid = _add_pipeline(conn)
        _add_stage(conn, pid, "Closed Lost", terminal="lost")
        r = call_action(MOD.add_crm_pipeline_stage, conn,
                        a(pipeline=pid, name="Lost Too", terminal="lost"))
        assert is_error(r)

    def test_one_won_and_one_lost_allowed(self, conn, env):
        pid = _add_pipeline(conn)
        _add_stage(conn, pid, "Won", terminal="won")
        _add_stage(conn, pid, "Lost", terminal="lost")  # different flag — allowed
        n = conn.execute(
            "SELECT COUNT(*) c FROM crm_pipeline_stage WHERE crm_pipeline_id=? "
            "AND (is_terminal_won=1 OR is_terminal_lost=1)", (pid,)).fetchone()["c"]
        assert n == 2

    def test_probability_stored_as_text(self, conn, env):
        pid = _add_pipeline(conn)
        sid = _add_stage(conn, pid, "Demo", probability="42.5")
        prob = conn.execute("SELECT default_probability FROM crm_pipeline_stage WHERE id=?", (sid,)).fetchone()[0]
        assert prob == "42.5"
        assert isinstance(prob, str)

    def test_pipeline_not_found_rejected(self, conn, env):
        r = call_action(MOD.add_crm_pipeline_stage, conn,
                        a(pipeline=str(uuid.uuid4()), name="X"))
        assert is_error(r)


# ===========================================================================
# update-crm-pipeline-stage
# ===========================================================================

class TestUpdatePipelineStage:
    def test_update_name(self, conn, env):
        pid = _add_pipeline(conn)
        sid = _add_stage(conn, pid, "Discovery")
        r = call_action(MOD.update_crm_pipeline_stage, conn, a(id=sid, name="Discovery Call"))
        assert is_ok(r), r
        assert conn.execute("SELECT name FROM crm_pipeline_stage WHERE id=?", (sid,)).fetchone()[0] == "Discovery Call"

    def test_update_order_collision_rejected(self, conn, env):
        pid = _add_pipeline(conn)
        _add_stage(conn, pid, "A", order="1")
        sb = _add_stage(conn, pid, "B", order="2")
        r = call_action(MOD.update_crm_pipeline_stage, conn, a(id=sb, order="1"))
        assert is_error(r)

    def test_update_terminal_uniqueness(self, conn, env):
        pid = _add_pipeline(conn)
        _add_stage(conn, pid, "Won", terminal="won")
        sb = _add_stage(conn, pid, "Maybe")
        r = call_action(MOD.update_crm_pipeline_stage, conn, a(id=sb, terminal="won"))
        assert is_error(r)

    def test_update_terminal_none_clears(self, conn, env):
        pid = _add_pipeline(conn)
        sid = _add_stage(conn, pid, "Won", terminal="won")
        r = call_action(MOD.update_crm_pipeline_stage, conn, a(id=sid, terminal="none"))
        assert is_ok(r), r
        row = conn.execute("SELECT is_terminal_won, is_terminal_lost FROM crm_pipeline_stage WHERE id=?", (sid,)).fetchone()
        assert row["is_terminal_won"] == 0 and row["is_terminal_lost"] == 0

    def test_update_no_fields_rejected(self, conn, env):
        pid = _add_pipeline(conn)
        sid = _add_stage(conn, pid, "Discovery")
        r = call_action(MOD.update_crm_pipeline_stage, conn, a(id=sid))
        assert is_error(r)


# ===========================================================================
# list-crm-pipelines / list-crm-pipeline-stages
# ===========================================================================

class TestListing:
    def test_list_pipelines_includes_default_and_stage_count(self, conn, env):
        pid = _add_pipeline(conn)
        _add_stage(conn, pid, "Discovery")
        _add_stage(conn, pid, "Demo")
        r = call_action(MOD.list_crm_pipelines, conn, a())
        assert is_ok(r), r
        names = {p["name"]: p for p in r["crm_pipelines"]}
        assert "Standard Sales" in names and "Enterprise B2B" in names
        assert names["Standard Sales"]["stage_count"] == 7
        assert names["Enterprise B2B"]["stage_count"] == 2
        # default sorts first
        assert r["crm_pipelines"][0]["is_default"] == 1

    def test_list_stages_for_one_pipeline_ordered(self, conn, env):
        pid = _add_pipeline(conn)
        _add_stage(conn, pid, "C", order="3")
        _add_stage(conn, pid, "A", order="1")
        _add_stage(conn, pid, "B", order="2")
        r = call_action(MOD.list_crm_pipeline_stages, conn, a(pipeline=pid))
        assert is_ok(r), r
        order = [s["name"] for s in r["crm_pipeline_stages"]]
        assert order == ["A", "B", "C"]


# ===========================================================================
# set-opportunity-pipeline-stage
# ===========================================================================

class TestSetOpportunityPipelineStage:
    def test_move_within_pipeline_dual_writes(self, conn, env):
        oid = _seed_opportunity(conn, env["company_id"])
        dpid = _default_pipeline_id(conn)
        qualified = _stage_id(conn, dpid, "qualified")
        r = call_action(MOD.set_opportunity_pipeline_stage, conn,
                        a(opportunity=oid, stage=qualified))
        assert is_ok(r), r
        row = conn.execute("SELECT stage, pipeline_stage_id FROM opportunity WHERE id=?", (oid,)).fetchone()
        assert row["stage"] == "qualified"          # legacy text dual-written
        assert row["pipeline_stage_id"] == qualified  # FK set

    def test_cross_pipeline_move_blocked(self, conn, env):
        oid = _seed_opportunity(conn, env["company_id"])
        dpid = _default_pipeline_id(conn)
        # First place it in the default pipeline.
        new_stage = _stage_id(conn, dpid, "new")
        call_action(MOD.set_opportunity_pipeline_stage, conn, a(opportunity=oid, stage=new_stage))
        # Now try to move it to a stage in a DIFFERENT pipeline.
        other = _add_pipeline(conn, name="Other")
        other_stage = _add_stage(conn, other, "Discovery")
        r = call_action(MOD.set_opportunity_pipeline_stage, conn, a(opportunity=oid, stage=other_stage))
        assert is_error(r)

    def test_terminal_won_sets_probability_100(self, conn, env):
        oid = _seed_opportunity(conn, env["company_id"], revenue="5000")
        dpid = _default_pipeline_id(conn)
        won = _stage_id(conn, dpid, "won")
        r = call_action(MOD.set_opportunity_pipeline_stage, conn, a(opportunity=oid, stage=won))
        assert is_ok(r), r
        row = conn.execute("SELECT probability, weighted_revenue FROM opportunity WHERE id=?", (oid,)).fetchone()
        assert row["probability"] == "100"
        assert row["weighted_revenue"] == "5000"

    def test_missing_args_rejected(self, conn, env):
        r = call_action(MOD.set_opportunity_pipeline_stage, conn, a(opportunity=None, stage=None))
        assert is_error(r)

    def test_bad_stage_id_rejected(self, conn, env):
        oid = _seed_opportunity(conn, env["company_id"])
        r = call_action(MOD.set_opportunity_pipeline_stage, conn,
                        a(opportunity=oid, stage=str(uuid.uuid4())))
        assert is_error(r)


# ===========================================================================
# Dual-write via the legacy actions (update-opportunity / mark-won / mark-lost)
# ===========================================================================

class TestLegacyDualWrite:
    def test_update_opportunity_stage_dual_writes_fk(self, conn, env):
        oid = _seed_opportunity(conn, env["company_id"])
        r = call_action(MOD.update_opportunity, conn, a(opportunity_id=oid, stage="qualified"))
        assert is_ok(r), r
        dpid = _default_pipeline_id(conn)
        row = conn.execute("SELECT stage, pipeline_stage_id FROM opportunity WHERE id=?", (oid,)).fetchone()
        assert row["stage"] == "qualified"
        assert row["pipeline_stage_id"] == _stage_id(conn, dpid, "qualified")

    def test_mark_won_dual_writes_terminal_fk(self, conn, env):
        oid = _seed_opportunity(conn, env["company_id"])
        r = call_action(MOD.mark_opportunity_won, conn, a(opportunity_id=oid))
        assert is_ok(r), r
        dpid = _default_pipeline_id(conn)
        row = conn.execute("SELECT stage, pipeline_stage_id FROM opportunity WHERE id=?", (oid,)).fetchone()
        assert row["stage"] == "won"
        assert row["pipeline_stage_id"] == _stage_id(conn, dpid, "won")

    def test_mark_lost_dual_writes_terminal_fk(self, conn, env):
        oid = _seed_opportunity(conn, env["company_id"])
        r = call_action(MOD.mark_opportunity_lost, conn, a(opportunity_id=oid, lost_reason="budget"))
        assert is_ok(r), r
        dpid = _default_pipeline_id(conn)
        row = conn.execute("SELECT stage, pipeline_stage_id FROM opportunity WHERE id=?", (oid,)).fetchone()
        assert row["stage"] == "lost"
        assert row["pipeline_stage_id"] == _stage_id(conn, dpid, "lost")


# ===========================================================================
# pipeline-report dual-path
# ===========================================================================

class TestPipelineReportDualPath:
    def test_report_groups_custom_and_legacy(self, conn, env):
        cid = env["company_id"]
        # One opportunity moved into the default pipeline (FK path).
        o1 = _seed_opportunity(conn, cid, name="FK deal", revenue="1000")
        dpid = _default_pipeline_id(conn)
        call_action(MOD.set_opportunity_pipeline_stage, conn,
                    a(opportunity=o1, stage=_stage_id(conn, dpid, "qualified")))
        # One opportunity left on the legacy text path (pipeline_stage_id NULL).
        o2 = _seed_opportunity(conn, cid, name="Text deal", revenue="2000")
        assert conn.execute("SELECT pipeline_stage_id FROM opportunity WHERE id=?", (o2,)).fetchone()[0] is None

        r = call_action(MOD.pipeline_report, conn, a())
        assert is_ok(r), r
        assert r["pipeline"]["total_opportunities"] == 2
        rows = r["pipeline"]["stages"]
        # FK row surfaces under the pipeline name; legacy row under '(none)'.
        pipelines = {row["pipeline"] for row in rows}
        assert "Standard Sales" in pipelines
        assert "(none)" in pipelines

    def test_report_conversion_rate_counts_terminals(self, conn, env):
        cid = env["company_id"]
        ow = _seed_opportunity(conn, cid, name="W")
        call_action(MOD.mark_opportunity_won, conn, a(opportunity_id=ow))
        ol = _seed_opportunity(conn, cid, name="L")
        call_action(MOD.mark_opportunity_lost, conn, a(opportunity_id=ol, lost_reason="x"))
        r = call_action(MOD.pipeline_report, conn, a())
        assert is_ok(r), r
        assert r["pipeline"]["total_won"] == 1
        assert r["pipeline"]["total_lost"] == 1
        assert r["pipeline"]["conversion_rate_pct"] == "50.00"
