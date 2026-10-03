"""M482 depth: behavioural evidence for 8 erpclaw-crm actions.

Each action below already had a test that proved the wrong thing (response
shape or routability). Each test here proves the database effect instead:
what row exists afterwards with which exact values, what changed from what
to what, and what did not change. Reads go back through PyPika-built queries
(``erpclaw_lib.query``) on a connection from ``erpclaw_lib.db.get_connection``.
Money is TEXT: exact string comparisons plus ``Decimal`` arithmetic, never
float, never ``round``.

Per-action depth (stored row vs ledger effect):

- convert-opportunity-to-quotation: DEFECT, documented (no stored row, no
  ledger effect). A valid call cannot succeed: the action never passes
  ``--company-id`` to the ``add-quotation`` subprocess, so the subprocess
  always refuses and nothing is written. The test pins the truthful error
  and the byte-identical database.
- delete-crm-saved-view: STORED ROW (the view row is gone; a same-owner
  sibling is byte-identical; one audit row records the delete).
- export-crm-companies: FILE MATCHES STORED ROWS (every CSV cell equals the
  stored row; the tables are unchanged by the export).
- export-crm-contacts: FILE MATCHES STORED ROWS (same depth as companies).
- export-opportunities: DEFECT, documented (no stored row, no file). The
  shared export schema selects a ``notes`` column the ``opportunity`` table
  does not have, so every call raises before writing anything.
- get-opportunity: READ-ONLY pin (every response field equals the stored
  opportunity/customer/activity rows; the tables are unchanged).
- list-crm-pipeline-stages: READ-ONLY pin (the 7 seeded stage rows come back
  ordered with exact values; the tables are unchanged).
- list-opportunities: READ-ONLY pin (stage/search filters return exactly the
  stored rows; the tables are unchanged).

Ledger note: none of these eight actions posts to the general, payment, or
stock ledger on any path (the writers touch CRM tables and the audit log
only; the getters/exports/lists are reads; the two defects write nothing),
so no success test below asserts a new ledger leg. Every test pins the
``gl_entry`` count unchanged so a later reader does not add a leg assertion
that cannot hold.
"""
import csv
import json
import os
import sys
from decimal import Decimal, ROUND_HALF_UP

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from crm_helpers import (  # noqa: E402
    call_action, ns, is_ok, is_error, load_db_query, SRC_DIR,
)
from erpclaw_lib.db import db_error_types, get_connection  # noqa: E402
from erpclaw_lib.query import Field, P, Q, Table, fn  # noqa: E402

MOD = load_db_query()
ROUTER = os.path.join(SRC_DIR, "erpclaw", "scripts", "db_query.py")


@pytest.fixture
def conn(db_path):
    connection = get_connection(db_path)
    yield connection
    connection.close()


_DEFAULTS = dict(
    opportunity_id=None, opportunity_name=None, lead_id=None, customer_id=None,
    opportunity_type=None, expected_revenue=None, probability=None,
    expected_closing_date=None, assigned_to=None, next_follow_up_date=None,
    stage=None, status=None, search=None, saved_view_id=None,
    limit="20", offset="0", items=None, db_path=None, company_id=None,
    pipeline=None, opportunity=None, stage_id=None,
    id=None, view=None, name=None, entity_type=None, owner_user_id=None,
    filter_json=None, sort_json=None, group_by_json=None,
    column_order_json=None, is_shared=False, set_shared=None,
    output=None, lifecycle=None, include_udfs=False,
    email=None, phone=None, mobile=None, job_title=None, linkedin_url=None,
    domain=None, industry=None, revenue=None, notes=None,
    activity_type=None, subject=None, activity_date=None, description=None,
    created_by=None, next_action_date=None,
)


def _ns(**kw):
    d = dict(_DEFAULTS)
    d.update(kw)
    return ns(**d)


_SNAPSHOT_TABLES = (
    "opportunity", "quotation", "quotation_item",
    "crm_contact", "crm_company", "crm_saved_view", "crm_activity",
    "crm_pipeline_stage", "audit_log", "naming_series", "gl_entry",
)


def _all(conn, table):
    found = Table(table)
    query = Q.from_(found).select(found.star)
    return sorted(
        (dict(row) for row in conn.execute(query.get_sql()).fetchall()),
        key=repr,
    )


def _snapshot(conn):
    return {table: _all(conn, table) for table in _SNAPSHOT_TABLES}


def _row(conn, table, row_id):
    found = Table(table)
    query = Q.from_(found).select(found.star).where(found.id == P())
    row = conn.execute(query.get_sql(), (row_id,)).fetchone()
    assert row is not None, "%s %s not found" % (table, row_id)
    return dict(row)


def _where(conn, table, **filters):
    found = Table(table)
    query = Q.from_(found).select(found.star)
    params = []
    for column, value in filters.items():
        query = query.where(Field(column) == P())
        params.append(value)
    return [dict(row)
            for row in conn.execute(query.get_sql(), params).fetchall()]


def _count(conn, table):
    found = Table(table)
    query = Q.from_(found).select(fn.Count("*").as_("counted"))
    return conn.execute(query.get_sql()).fetchone()["counted"]


def _weighted(revenue, probability):
    return str((Decimal(revenue) * Decimal(probability) / Decimal("100"))
               .quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _csv_rows(path):
    with open(path, newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _cell(value):
    return "" if value is None else str(value)


def _add_opportunity(conn, company_id, label="Depth Alpha",
                     revenue="20000.00", probability="50", customer_id=None):
    result = call_action(MOD.add_opportunity, conn, _ns(
        opportunity_name=label, lead_id=None, customer_id=customer_id,
        opportunity_type="sales", expected_revenue=revenue,
        probability=probability, expected_closing_date="2026-06-01",
        assigned_to=None, company_id=company_id,
    ))
    assert is_ok(result), result
    return result["opportunity"]["id"]


# ── convert-opportunity-to-quotation: DEFECT ───────────────────────────────

class TestConvertOpportunityToQuotationDepth:
    def test_valid_inputs_report_quotation_failure_and_write_nothing(
            self, conn, env, db_path, monkeypatch):
        # DEFECT (deliberately not fixed): the action builds the
        # ``add-quotation`` subprocess command without ``--company-id``, which
        # ``add-quotation`` requires, so creation always fails and the
        # opportunity is never stamped. This action does NOT reach the ledger:
        # nothing is written anywhere, so the whole snapshot is pinned
        # identical and no ledger assertion can hold.
        import erpclaw_lib.dependencies as deps
        assert os.path.isfile(ROUTER), "in-tree router missing: %s" % ROUTER
        monkeypatch.setattr(
            deps, "resolve_skill_script", lambda skill_name: ROUTER)
        company_id = env["company_id"]
        customer_id = env["customer_id"]
        opp_id = _add_opportunity(conn, company_id, revenue="20000.00",
                                  probability="50", customer_id=customer_id)
        before = _snapshot(conn)

        result = call_action(
            MOD.convert_opportunity_to_quotation, conn, _ns(
                opportunity_id=opp_id,
                items=json.dumps([{"item_id": "plain-widget",
                                   "qty": "2", "rate": "100.00"}]),
                db_path=db_path,
            ))

        assert is_error(result), result
        assert "Failed to create quotation" in result["message"]
        assert "--company-id is required" in result["message"]
        assert _snapshot(conn) == before
        assert _row(conn, "opportunity", opp_id)["quotation_id"] is None
        assert _count(conn, "quotation") == 0
        assert _count(conn, "quotation_item") == 0
        assert _count(conn, "gl_entry") == 0

    def test_missing_items_refused_before_any_write(self, conn, env, db_path):
        company_id = env["company_id"]
        opp_id = _add_opportunity(conn, company_id,
                                  customer_id=env["customer_id"])
        before = _snapshot(conn)

        result = call_action(
            MOD.convert_opportunity_to_quotation, conn, _ns(
                opportunity_id=opp_id, items=None, db_path=db_path))

        assert is_error(result), result
        assert "--items is required" in result["message"]
        assert _snapshot(conn) == before


# ── delete-crm-saved-view: STORED ROW ──────────────────────────────────────

_LEAD_NEW_FILTER = json.dumps({
    "logic": "AND",
    "conditions": [{"field": "status", "op": "eq", "value": "new"}],
})


def _add_view(conn, company_id, label, owner="alice"):
    result = call_action(MOD.add_crm_saved_view, conn, _ns(
        company_id=company_id, name=label, entity_type="lead",
        filter_json=_LEAD_NEW_FILTER, owner_user_id=owner,
    ))
    assert is_ok(result), result
    return result["crm_saved_view"]["id"]


class TestDeleteCrmSavedViewDepth:
    def test_delete_removes_only_the_target_and_audits(self, conn, env):
        # This action does NOT reach the ledger: it deletes one
        # ``crm_saved_view`` row and appends one audit row. The ledger count
        # is pinned unchanged.
        company_id = env["company_id"]
        target_id = _add_view(conn, company_id, "Target view")
        keep_id = _add_view(conn, company_id, "Keep me")
        keep_before = _row(conn, "crm_saved_view", keep_id)

        result = call_action(MOD.delete_crm_saved_view, conn, _ns(
            id=target_id, owner_user_id="alice"))

        assert is_ok(result), result
        assert result["crm_saved_view_id"] == target_id
        assert _where(conn, "crm_saved_view", id=target_id) == []
        assert _row(conn, "crm_saved_view", keep_id) == keep_before
        audits = _where(conn, "audit_log",
                        action="delete-crm-saved-view", entity_id=target_id)
        assert len(audits) == 1
        assert audits[0]["skill"] == "erpclaw-crm"
        assert audits[0]["entity_type"] == "crm_saved_view"
        assert _count(conn, "gl_entry") == 0

    def test_wrong_owner_refused_and_nothing_changes(self, conn, env):
        company_id = env["company_id"]
        view_id = _add_view(conn, company_id, "Guarded view")
        before = _snapshot(conn)

        result = call_action(MOD.delete_crm_saved_view, conn, _ns(
            id=view_id, owner_user_id="mallory"))

        assert is_error(result), result
        assert "Only the owner may delete" in result["message"]
        assert _snapshot(conn) == before


# ── export-crm-companies: FILE MATCHES STORED ROWS ─────────────────────────

class TestExportCrmCompaniesDepth:
    def test_csv_cells_equal_the_stored_row(self, conn, env, tmp_path):
        # Export is a read: the CSV must reproduce the stored row exactly and
        # the tables must be unchanged. No ledger assertion can hold for a
        # pure read, so the ledger count is pinned unchanged instead.
        company_id = env["company_id"]
        result = call_action(MOD.add_crm_company, conn, _ns(
            company_id=company_id, name="Acme Inc", domain="acme.example",
            industry="SaaS", revenue="2500000.75", notes="Key account",
        ))
        assert is_ok(result), result
        stored_id = result["crm_company"]["id"]
        before = _snapshot(conn)
        out = str(tmp_path / "companies.csv")

        exported = call_action(MOD.export_crm_companies, conn, _ns(
            output=out, lifecycle=None, include_udfs=False,
            company_id=company_id))

        assert is_ok(exported), exported
        assert exported["exported"] == 1
        assert exported["output"] == out
        stored = _row(conn, "crm_company", stored_id)
        assert stored["annual_revenue"] == "2500000.75"
        assert Decimal(stored["annual_revenue"]) == Decimal("2500000.75")
        rows = _csv_rows(out)
        assert len(rows) == 1
        for column, cell in rows[0].items():
            assert cell == _cell(stored[column]), column
        assert _snapshot(conn) == before
        assert _count(conn, "gl_entry") == 0

    def test_non_csv_output_refused_and_nothing_changes(
            self, conn, env, tmp_path):
        company_id = env["company_id"]
        call_action(MOD.add_crm_company, conn, _ns(
            company_id=company_id, name="Acme Inc"))
        before = _snapshot(conn)
        out = str(tmp_path / "companies.txt")

        result = call_action(MOD.export_crm_companies, conn, _ns(
            output=out, lifecycle=None, include_udfs=False,
            company_id=company_id))

        assert is_error(result), result
        assert ".csv" in result["message"]
        assert _snapshot(conn) == before
        assert not os.path.exists(out)


# ── export-crm-contacts: FILE MATCHES STORED ROWS ──────────────────────────

class TestExportCrmContactsDepth:
    def test_csv_cells_equal_the_stored_rows(self, conn, env, tmp_path):
        # Same depth as companies: the CSV reproduces the stored rows exactly
        # and the export changes nothing. Reads post no ledger rows.
        company_id = env["company_id"]
        first = call_action(MOD.add_crm_contact, conn, _ns(
            company_id=company_id, name="Ann Lee", email="ann@example.com",
            phone="555-0100", mobile="555-0101", job_title="VP Sales",
            linkedin_url="https://example.com/ann", lifecycle="mql",
            notes="Warm intro",
        ))
        assert is_ok(first), first
        second = call_action(MOD.add_crm_contact, conn, _ns(
            company_id=company_id, name="Bob Ray", email="bob@example.com",
            lifecycle="lead",
        ))
        assert is_ok(second), second
        before = _snapshot(conn)
        out = str(tmp_path / "contacts.csv")

        exported = call_action(MOD.export_crm_contacts, conn, _ns(
            output=out, lifecycle=None, include_udfs=False,
            company_id=company_id))

        assert is_ok(exported), exported
        assert exported["exported"] == 2
        rows = _csv_rows(out)
        assert len(rows) == 2
        by_id = {row["id"]: row for row in rows}
        for stored_id in (first["crm_contact"]["id"],
                          second["crm_contact"]["id"]):
            stored = _row(conn, "crm_contact", stored_id)
            for column, cell in by_id[stored_id].items():
                assert cell == _cell(stored[column]), column
        assert by_id[first["crm_contact"]["id"]]["lifecycle"] == "mql"
        assert by_id[first["crm_contact"]["id"]]["job_title"] == "VP Sales"
        assert _snapshot(conn) == before
        assert _count(conn, "gl_entry") == 0

    def test_missing_output_refused_and_nothing_changes(self, conn, env):
        company_id = env["company_id"]
        call_action(MOD.add_crm_contact, conn, _ns(
            company_id=company_id, name="Ann Lee"))
        before = _snapshot(conn)

        result = call_action(MOD.export_crm_contacts, conn, _ns(
            output=None, lifecycle=None, include_udfs=False,
            company_id=company_id))

        assert is_error(result), result
        assert "Output path is required" in result["message"]
        assert _snapshot(conn) == before


# ── export-opportunities: DEFECT ───────────────────────────────────────────

class TestExportOpportunitiesDepth:
    def test_export_fails_on_missing_notes_column_and_writes_nothing(
            self, conn, env, tmp_path):
        # DEFECT (deliberately not fixed): the shared export schema selects a
        # ``notes`` column the ``opportunity`` table does not have, so the
        # SELECT raises before any file is written. This action therefore has
        # no success path to pin: the test pins the real failure, the missing
        # file, and the unchanged database. No ledger assertion can hold for
        # an action that writes nothing.
        company_id = env["company_id"]
        opp_id = _add_opportunity(conn, company_id, label="Export Deal",
                                  revenue="12345.67", probability="80")
        stored = _row(conn, "opportunity", opp_id)
        assert stored["expected_revenue"] == "12345.67"
        assert stored["weighted_revenue"] == _weighted("12345.67", "80")
        before = _snapshot(conn)
        out = str(tmp_path / "opportunities.csv")
        _missing, base_error = db_error_types()

        with pytest.raises(base_error, match="notes"):
            call_action(MOD.export_opportunities, conn, _ns(
                output=out, stage=None, status=None, include_udfs=False,
                company_id=company_id))

        assert not os.path.exists(out)
        assert _snapshot(conn) == before
        assert _count(conn, "gl_entry") == 0

    def test_missing_directory_refused_and_nothing_changes(
            self, conn, env, tmp_path):
        company_id = env["company_id"]
        _add_opportunity(conn, company_id)
        before = _snapshot(conn)
        out = str(tmp_path / "no-such-dir" / "opportunities.csv")

        result = call_action(MOD.export_opportunities, conn, _ns(
            output=out, stage=None, status=None, include_udfs=False,
            company_id=company_id))

        assert is_error(result), result
        assert "Output directory does not exist" in result["message"]
        assert _snapshot(conn) == before
        assert not os.path.exists(out)


# ── get-opportunity: READ-ONLY pin ─────────────────────────────────────────

class TestGetOpportunityDepth:
    def test_response_pins_the_stored_rows(self, conn, env):
        # Read-only: every response field must equal the stored
        # opportunity/customer/activity rows, and the tables must be
        # unchanged. No ledger assertion can hold for a pure read.
        company_id = env["company_id"]
        opp_id = _add_opportunity(conn, company_id, revenue="20000.00",
                                  probability="50",
                                  customer_id=env["customer_id"])
        activity = call_action(MOD.add_activity, conn, _ns(
            activity_type="call", subject="Intro call",
            activity_date="2026-03-11", description="First contact",
            lead_id=None, opportunity_id=opp_id, customer_id=None,
            created_by="tester", next_action_date=None,
        ))
        assert is_ok(activity), activity
        before = _snapshot(conn)

        result = call_action(MOD.get_opportunity, conn, _ns(
            opportunity_id=opp_id))

        assert is_ok(result), result
        stored = _row(conn, "opportunity", opp_id)
        assert stored["expected_revenue"] == "20000.00"
        assert Decimal(stored["expected_revenue"]) == Decimal("20000.00")
        assert stored["weighted_revenue"] == "10000.00"
        assert stored["weighted_revenue"] == _weighted("20000.00", "50")
        got = result["opportunity"]
        for field in ("id", "naming_series", "opportunity_name", "stage",
                      "probability", "expected_revenue", "weighted_revenue",
                      "customer_id", "company_id"):
            assert got[field] == stored[field], field
        assert got["customer"]["id"] == env["customer_id"]
        assert len(got["activities"]) == 1
        assert got["activities"][0]["subject"] == "Intro call"
        assert got["activities"][0]["activity_type"] == "call"
        assert _snapshot(conn) == before
        assert _count(conn, "gl_entry") == 0

    def test_missing_id_refused_and_nothing_changes(self, conn, env):
        _add_opportunity(conn, env["company_id"])
        before = _snapshot(conn)

        result = call_action(MOD.get_opportunity, conn, _ns(
            opportunity_id=None))

        assert is_error(result), result
        assert "--opportunity-id is required" in result["message"]
        assert _snapshot(conn) == before


# ── list-crm-pipeline-stages: READ-ONLY pin ────────────────────────────────

class TestListCrmPipelineStagesDepth:
    def test_seeded_stages_come_back_ordered_with_exact_values(
            self, conn, env):
        # Read-only: the seeded default-pipeline rows come back ordered by
        # stage_order with exact values, and nothing changes. Reads post no
        # ledger rows.
        before = _snapshot(conn)

        result = call_action(MOD.list_crm_pipeline_stages, conn, _ns(
            pipeline=None))

        assert is_ok(result), result
        assert result["total"] == 7
        names = [stage["name"] for stage in result["crm_pipeline_stages"]]
        assert names == ["new", "contacted", "qualified", "proposal_sent",
                         "negotiation", "won", "lost"]
        orders = [stage["stage_order"]
                  for stage in result["crm_pipeline_stages"]]
        assert orders == [1, 2, 3, 4, 5, 6, 7]
        pipeline_ids = {stage["crm_pipeline_id"]
                        for stage in result["crm_pipeline_stages"]}
        assert len(pipeline_ids) == 1
        stored = _where(conn, "crm_pipeline_stage",
                        crm_pipeline_id=pipeline_ids.pop())
        stored_names = [row["name"] for row in sorted(
            stored, key=lambda row: row["stage_order"])]
        assert names == stored_names
        assert _snapshot(conn) == before
        assert _count(conn, "gl_entry") == 0

    def test_unknown_pipeline_refused_and_nothing_changes(self, conn, env):
        before = _snapshot(conn)

        result = call_action(MOD.list_crm_pipeline_stages, conn, _ns(
            pipeline="no-such-pipeline"))

        assert is_error(result), result
        assert "Pipeline no-such-pipeline not found" in result["message"]
        assert _snapshot(conn) == before


# ── list-opportunities: READ-ONLY pin ──────────────────────────────────────

class TestListOpportunitiesDepth:
    def test_filters_return_exactly_the_stored_rows(self, conn, env):
        # Read-only: stage/search filters return exactly the stored rows with
        # exact money strings, and the tables are unchanged. No ledger
        # assertion can hold for a pure read.
        company_id = env["company_id"]
        alpha_id = _add_opportunity(conn, company_id, label="Alpha Deal",
                                    revenue="20000.00", probability="50")
        beta_id = _add_opportunity(conn, company_id, label="Beta Deal",
                                   revenue="5000.00", probability="80")
        moved = call_action(MOD.update_opportunity, conn, _ns(
            opportunity_id=beta_id, opportunity_name=None, stage="qualified",
            probability=None, expected_revenue=None, expected_closing_date=None,
            assigned_to=None, next_follow_up_date=None, customer_id=None,
        ))
        assert is_ok(moved), moved
        before = _snapshot(conn)

        staged = call_action(MOD.list_opportunities, conn, _ns(
            stage="qualified", search=None, saved_view_id=None,
            limit="20", offset="0"))

        assert is_ok(staged), staged
        assert staged["total"] == 1
        assert staged["opportunities"][0]["id"] == beta_id
        beta = _row(conn, "opportunity", beta_id)
        assert beta["stage"] == "qualified"
        assert beta["expected_revenue"] == "5000.00"
        assert beta["weighted_revenue"] == "4000.00"
        assert staged["opportunities"][0]["expected_revenue"] == "5000.00"
        assert staged["opportunities"][0]["weighted_revenue"] == "4000.00"

        found = call_action(MOD.list_opportunities, conn, _ns(
            stage=None, search="Alpha", saved_view_id=None,
            limit="20", offset="0"))
        assert is_ok(found), found
        assert found["total"] == 1
        assert found["opportunities"][0]["id"] == alpha_id

        everything = call_action(MOD.list_opportunities, conn, _ns(
            stage=None, search=None, saved_view_id=None,
            limit="20", offset="0"))
        assert is_ok(everything), everything
        assert everything["total"] == 2
        assert _snapshot(conn) == before
        assert _count(conn, "gl_entry") == 0

    def test_unknown_saved_view_refused_and_nothing_changes(self, conn, env):
        _add_opportunity(conn, env["company_id"])
        before = _snapshot(conn)

        result = call_action(MOD.list_opportunities, conn, _ns(
            stage=None, search=None, saved_view_id="missing-view",
            limit="20", offset="0"))

        assert is_error(result), result
        assert "Saved view missing-view not found" in result["message"]
        assert _snapshot(conn) == before
