"""L1 tests — M33 Item 6 / B12: lead_source CRM CRUD (empty-dropdown fix).

Covers the two new actions (`add-lead-source`, `list-lead-sources`) and the
`--lead-source-id` wiring on `add-lead` / `update-lead`. The `lead_source`
table's DDL is foundation-owned (erpclaw-setup init_schema); growth's
erpclaw-crm is the CRM writer per ADR-0023 (it already writes the foundation
`lead` table).

House style: crm_helpers.call_action against per-test fresh SQLite (conftest
`conn`/`env` fixtures), exact-value assertions. The list-shape test pins the
dropdown contract (UI.yaml:199 link_search_action: list-lead-sources, plural;
link_display_field: name).
"""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from crm_helpers import call_action, ns, is_ok, is_error, load_db_query

MOD = load_db_query()


def _add_source(conn, name="Referral", description=None):
    return call_action(MOD.add_lead_source, conn, ns(name=name, description=description))


def _add_lead(conn, company_id, lead_name="Jane Prospect", lead_source_id=None,
              source=None):
    return call_action(MOD.add_lead, conn, ns(
        lead_name=lead_name, company_name=None, email=None, phone=None,
        source=source, lead_source_id=lead_source_id, territory=None,
        industry=None, assigned_to=None, notes=None, company_id=company_id,
    ))


# ===========================================================================
# Router registration (dispatchability contract — both new actions defined)
# ===========================================================================

class TestRegistration:
    def test_actions_registered(self):
        assert "add-lead-source" in MOD.ACTIONS
        assert "list-lead-sources" in MOD.ACTIONS


# ===========================================================================
# add-lead-source
# ===========================================================================

class TestAddLeadSource:
    def test_add_basic(self, conn, env):
        r = _add_source(conn, "Referral", "Word of mouth")
        assert is_ok(r)
        src = r["lead_source"]
        assert src["name"] == "Referral"
        assert src["description"] == "Word of mouth"
        assert src["id"]
        # Row is actually persisted
        row = conn.execute(
            "SELECT name, description FROM lead_source WHERE id = ?",
            (src["id"],)).fetchone()
        assert row["name"] == "Referral"
        assert row["description"] == "Word of mouth"

    def test_add_missing_name(self, conn, env):
        r = call_action(MOD.add_lead_source, conn, ns(name=None, description=None))
        assert is_error(r)

    def test_add_duplicate_name_rejected(self, conn, env):
        assert is_ok(_add_source(conn, "Trade Show"))
        r = _add_source(conn, "Trade Show")
        assert is_error(r)
        assert "already exists" in r.get("message", "").lower()

    def test_add_optional_description_omitted(self, conn, env):
        r = _add_source(conn, "Website")
        assert is_ok(r)
        assert r["lead_source"]["description"] is None


# ===========================================================================
# list-lead-sources (plural — the dropdown feed)
# ===========================================================================

class TestListLeadSources:
    def test_empty_returns_empty_list(self, conn, env):
        r = call_action(MOD.list_lead_sources, conn,
                        ns(search=None, limit=None, offset=None))
        assert is_ok(r)
        assert r["lead_sources"] == []
        assert r["total"] == 0

    def test_returns_added_sources(self, conn, env):
        _add_source(conn, "Referral")
        _add_source(conn, "Website")
        r = call_action(MOD.list_lead_sources, conn,
                        ns(search=None, limit=None, offset=None))
        assert is_ok(r)
        names = {s["name"] for s in r["lead_sources"]}
        assert names == {"Referral", "Website"}
        assert r["total"] == 2

    def test_dropdown_shape(self, conn, env):
        """Every row must carry id + name (UI.yaml link_display_field: name)."""
        _add_source(conn, "Cold Call")
        r = call_action(MOD.list_lead_sources, conn,
                        ns(search=None, limit=None, offset=None))
        assert is_ok(r)
        assert "lead_sources" in r          # plural key matches UI.yaml:199
        assert len(r["lead_sources"]) == 1
        row = r["lead_sources"][0]
        assert set(("id", "name")).issubset(row.keys())
        assert row["name"] == "Cold Call"

    def test_search_filters_by_name(self, conn, env):
        _add_source(conn, "Referral Partner")
        _add_source(conn, "Website")
        r = call_action(MOD.list_lead_sources, conn,
                        ns(search="Referral", limit=None, offset=None))
        assert is_ok(r)
        assert r["total"] == 1
        assert r["lead_sources"][0]["name"] == "Referral Partner"


# ===========================================================================
# --lead-source-id wiring on add-lead / update-lead
# ===========================================================================

class TestLeadSourceWiring:
    def test_add_lead_with_lead_source_id_links(self, conn, env):
        src = _add_source(conn, "Referral")["lead_source"]
        r = _add_lead(conn, env["company_id"], "Jane Prospect",
                      lead_source_id=src["id"])
        assert is_ok(r)
        assert r["lead"]["lead_source_id"] == src["id"]
        # get-lead reflects the FK
        got = call_action(MOD.get_lead, conn, ns(lead_id=r["lead"]["id"]))
        assert got["lead"]["lead_source_id"] == src["id"]

    def test_add_lead_without_source_id_is_null(self, conn, env):
        r = _add_lead(conn, env["company_id"], "No Source")
        assert is_ok(r)
        assert r["lead"]["lead_source_id"] is None

    def test_add_lead_invalid_source_id_rejected(self, conn, env):
        r = _add_lead(conn, env["company_id"], "Bad Source",
                      lead_source_id="does-not-exist")
        assert is_error(r)

    def test_update_lead_sets_source_id(self, conn, env):
        src = _add_source(conn, "Trade Show")["lead_source"]
        lead = _add_lead(conn, env["company_id"], "To Update")["lead"]
        r = call_action(MOD.update_lead, conn, ns(
            lead_id=lead["id"], lead_name=None, company_name=None, email=None,
            phone=None, source=None, lead_source_id=src["id"], territory=None,
            industry=None, status=None, assigned_to=None, notes=None,
        ))
        assert is_ok(r)
        assert r["lead"]["lead_source_id"] == src["id"]

    def test_update_lead_invalid_source_id_rejected(self, conn, env):
        lead = _add_lead(conn, env["company_id"], "To Update 2")["lead"]
        r = call_action(MOD.update_lead, conn, ns(
            lead_id=lead["id"], lead_name=None, company_name=None, email=None,
            phone=None, source=None, lead_source_id="nope", territory=None,
            industry=None, status=None, assigned_to=None, notes=None,
        ))
        assert is_error(r)
