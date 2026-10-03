"""L1 pytest tests for erpclaw-crm-adv (48 actions across 5 domains).

Domains: campaigns (12), territories (10), contracts (10), automation (11), reports (5).
"""
import json
import os
import sys
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from crm_adv_helpers import (
    call_action, ns, is_ok, is_error, load_db_query,
    seed_company, seed_naming_series,
)

MOD = load_db_query()

# load_db_query() puts the module dir on sys.path, so the domain module that
# owns process-drip-sends is importable for patching its cross-module send seam.
import automation  # noqa: E402
# campaigns owns send-campaign; imported here so its M8-C send seam is patchable.
import campaigns  # noqa: E402

D = Decimal


def _msg(result):
    """Refusal text wherever err() put it (the message or the error key)."""
    return result.get("message", "") + result.get("error", "")


# Every table a CRM-ADV seed or read action below can touch. Snapshots are plain
# SELECTs on business tables through the test connection; no catalog reads,
# no PRAGMA, no sqlite_master, no information_schema.
_SNAPSHOT_TABLES = (
    "company",
    "naming_series",
    "lead",
    "crmadv_email_campaign",
    "crmadv_campaign_template",
    "crmadv_recipient_list",
    "crmadv_campaign_event",
    "crmadv_territory",
    "crmadv_territory_assignment",
    "crmadv_territory_quota",
    "crmadv_contract",
    "crmadv_contract_obligation",
    "crmadv_automation_workflow",
    "crmadv_lead_score_rule",
    "crmadv_nurture_sequence",
    "audit_log",
)


def _snapshot_tables(conn, tables=_SNAPSHOT_TABLES):
    """Full row dump of the owned tables, ordered by id, for before/after compare."""
    snap = {}
    for table in tables:
        rows = conn.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()
        snap[table] = [tuple(row) for row in rows]
    return snap


# ===========================================================================
# CAMPAIGNS DOMAIN (12 actions)
# ===========================================================================

class TestAddEmailCampaign:
    def test_add_email_campaign(self, conn, env):
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="Spring Promo",
            subject="Spring Sale!", template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["name"] == "Spring Promo"
        assert r["campaign_status"] == "draft"
        assert r["naming_series"].startswith("EMCAMP-")

    def test_add_email_campaign_missing_name(self, conn, env):
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name=None,
            subject=None, template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        assert is_error(r)


class TestUpdateEmailCampaign:
    def test_update_campaign_name(self, conn, env):
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="Old Name",
            subject=None, template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        camp_id = r["id"]

        r2 = call_action(MOD.update_email_campaign, conn, ns(
            campaign_id=camp_id, name="New Name",
            subject=None, template_id=None,
            recipient_list_id=None, scheduled_date=None,
        ))
        assert is_ok(r2)
        assert "name" in r2["updated_fields"]

    def test_update_campaign_no_fields(self, conn, env):
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="No Update",
            subject=None, template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        camp_id = r["id"]

        r2 = call_action(MOD.update_email_campaign, conn, ns(
            campaign_id=camp_id, name=None, subject=None,
            template_id=None, recipient_list_id=None, scheduled_date=None,
        ))
        assert is_error(r2)


class TestGetEmailCampaign:
    def test_get_campaign(self, conn, env):
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="Get Me",
            subject="Test", template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        camp_id = r["id"]

        r2 = call_action(MOD.get_email_campaign, conn, ns(campaign_id=camp_id))
        assert is_ok(r2)
        assert r2["name"] == "Get Me"


class TestListEmailCampaigns:
    def test_list_campaigns(self, conn, env):
        call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="C1",
            subject=None, template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        r = call_action(MOD.list_email_campaigns, conn, ns(
            company_id=env["company_id"], campaign_status_filter=None,
            search=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestCampaignTemplate:
    def test_add_template(self, conn, env):
        r = call_action(MOD.add_campaign_template, conn, ns(
            company_id=env["company_id"], name="Welcome Template",
            subject_template="Welcome {{name}}!", body_html="<h1>Hi</h1>",
            body_text="Hi", template_type="welcome",
        ))
        assert is_ok(r)
        assert r["name"] == "Welcome Template"
        assert r["template_type"] == "welcome"

    def test_add_template_invalid_type(self, conn, env):
        r = call_action(MOD.add_campaign_template, conn, ns(
            company_id=env["company_id"], name="Bad Type",
            subject_template=None, body_html=None, body_text=None,
            template_type="invalid",
        ))
        assert is_error(r)

    def test_list_templates(self, conn, env):
        call_action(MOD.add_campaign_template, conn, ns(
            company_id=env["company_id"], name="T1",
            subject_template=None, body_html=None, body_text=None,
            template_type="newsletter",
        ))
        r = call_action(MOD.list_campaign_templates, conn, ns(
            company_id=env["company_id"], template_type=None,
            limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestRecipientList:
    def test_add_recipient_list(self, conn, env):
        r = call_action(MOD.add_recipient_list, conn, ns(
            company_id=env["company_id"], name="VIP List",
            description="High-value customers",
            list_type="static", filter_criteria=None,
        ))
        assert is_ok(r)
        assert r["name"] == "VIP List"
        assert r["list_type"] == "static"

    def test_list_recipient_lists(self, conn, env):
        call_action(MOD.add_recipient_list, conn, ns(
            company_id=env["company_id"], name="RL1",
            description=None, list_type=None, filter_criteria=None,
        ))
        r = call_action(MOD.list_recipient_lists, conn, ns(
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestScheduleSendCampaign:
    def test_schedule_campaign(self, conn, env):
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="Schedule Me",
            subject=None, template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        camp_id = r["id"]

        r2 = call_action(MOD.schedule_campaign, conn, ns(
            campaign_id=camp_id, scheduled_date="2026-04-01",
        ))
        assert is_ok(r2)
        assert r2["campaign_status"] == "scheduled"

    def test_send_campaign(self, conn, env):
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="Send Me",
            subject="Hello", template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        camp_id = r["id"]

        r2 = call_action(MOD.send_campaign, conn, ns(campaign_id=camp_id, db_path=None))
        assert is_ok(r2)
        assert r2["campaign_status"] == "sent"

    def test_send_already_sent(self, conn, env):
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="Already Sent",
            subject="Hello", template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        camp_id = r["id"]
        call_action(MOD.send_campaign, conn, ns(campaign_id=camp_id, db_path=None))

        r2 = call_action(MOD.send_campaign, conn, ns(campaign_id=camp_id, db_path=None))
        assert is_error(r2)


def _seed_campaign_lead(conn, company_id, lead_name, email):
    """Insert a minimal CRM lead (the campaign's contactable recipient)."""
    lead_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO lead (id, lead_name, email, status, company_id) "
        "VALUES (?, ?, ?, 'new', ?)",
        (lead_id, lead_name, email, company_id))
    conn.commit()
    return lead_id


class TestSendCampaignRetrofit:
    """send-campaign enqueues one email per recipient via the M8-A send-email
    ACTION (mocked seam) and records the returned outbox ids as crmadv_campaign_event
    'sent' rows. Recipients with no email skip-with-note; a provider failure on
    one recipient is a skip, never a whole-campaign failure. Mirrors the dunning
    retrofit + process-drip-sends' cross-module send seam.
    """

    def _campaign(self, conn, company_id, subject="Spring Sale"):
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=company_id, name="Promo", subject=subject,
            template_id=None, recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        return r["id"]

    def test_send_enqueues_per_recipient_and_records_outbox_ids(self, conn, env):
        company_id = env["company_id"]
        _seed_campaign_lead(conn, company_id, "Ann", "ann@acme.example")
        _seed_campaign_lead(conn, company_id, "Bob", "bob@acme.example")
        camp_id = self._campaign(conn, company_id)

        outbox_ids = iter(["OUTBOX-1", "OUTBOX-2"])
        with patch.object(campaigns, "_dispatch_campaign_email",
                          side_effect=lambda *a, **k: (True, next(outbox_ids))) as m:
            result = call_action(MOD.send_campaign, conn,
                                 ns(campaign_id=camp_id, db_path=None))

        assert is_ok(result)
        assert result["campaign_status"] == "sent"
        assert result["recipients"] == 2
        assert result["sent"] == 2
        assert result["skipped"] == 0
        assert set(result["outbox_ids"]) == {"OUTBOX-1", "OUTBOX-2"}
        # seam invoked once per recipient with the resolved address + subject
        assert m.call_count == 2
        recips = {c.args[1] for c in m.call_args_list}
        assert recips == {"ann@acme.example", "bob@acme.example"}
        assert m.call_args_list[0].args[2] == "Spring Sale"  # subject passed
        # 'sent' events recorded carrying the outbox ids; total_sent bumped
        rows = conn.execute(
            "SELECT recipient_email, metadata FROM crmadv_campaign_event "
            "WHERE campaign_id = ? AND event_type = 'sent'", (camp_id,)).fetchall()
        assert len(rows) == 2
        recorded = {r["recipient_email"]: json.loads(r["metadata"])["email_outbox_id"]
                    for r in rows}
        assert recorded == {"ann@acme.example": "OUTBOX-1", "bob@acme.example": "OUTBOX-2"}
        total = conn.execute(
            "SELECT total_sent FROM crmadv_email_campaign WHERE id = ?",
            (camp_id,)).fetchone()["total_sent"]
        assert total == 2

    def test_no_email_recipient_skipped_cleanly(self, conn, env):
        company_id = env["company_id"]
        _seed_campaign_lead(conn, company_id, "Ann", "ann@acme.example")
        _seed_campaign_lead(conn, company_id, "NoMail", None)
        camp_id = self._campaign(conn, company_id)

        with patch.object(campaigns, "_dispatch_campaign_email",
                          return_value=(True, "OUTBOX-9")) as m:
            result = call_action(MOD.send_campaign, conn,
                                 ns(campaign_id=camp_id, db_path=None))

        assert is_ok(result)  # the no-email recipient never fails the campaign
        assert result["recipients"] == 2
        assert result["sent"] == 1
        assert result["skipped"] == 1
        # seam only invoked for the deliverable recipient
        assert m.call_count == 1
        assert m.call_args.args[1] == "ann@acme.example"
        rows = conn.execute(
            "SELECT COUNT(*) c FROM crmadv_campaign_event "
            "WHERE campaign_id = ? AND event_type = 'sent'", (camp_id,)).fetchone()
        assert rows["c"] == 1

    def test_send_failure_skips_with_campaign_still_sent(self, conn, env):
        company_id = env["company_id"]
        _seed_campaign_lead(conn, company_id, "Ann", "ann@acme.example")
        camp_id = self._campaign(conn, company_id)

        with patch.object(campaigns, "_dispatch_campaign_email",
                          return_value=(False, "smtp unreachable")) as m:
            result = call_action(MOD.send_campaign, conn,
                                 ns(campaign_id=camp_id, db_path=None))

        assert is_ok(result)  # provider failure does not fail the campaign
        assert result["campaign_status"] == "sent"
        assert result["sent"] == 0
        assert result["skipped"] == 1
        assert result["outbox_ids"] == []
        assert m.called
        rows = conn.execute(
            "SELECT COUNT(*) c FROM crmadv_campaign_event "
            "WHERE campaign_id = ? AND event_type = 'sent'", (camp_id,)).fetchone()
        assert rows["c"] == 0

    def test_no_content_refuses_to_send(self, conn, env):
        """A campaign with neither subject nor a template body cannot be sent."""
        company_id = env["company_id"]
        _seed_campaign_lead(conn, company_id, "Ann", "ann@acme.example")
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=company_id, name="Empty", subject=None,
            template_id=None, recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        camp_id = r["id"]

        with patch.object(campaigns, "_dispatch_campaign_email") as m:
            result = call_action(MOD.send_campaign, conn,
                                 ns(campaign_id=camp_id, db_path=None))

        assert is_error(result)
        assert not m.called  # never dispatched without resolvable content

    def test_template_body_drives_send_when_no_subject(self, conn, env):
        """Subject/body resolve from the attached campaign template."""
        company_id = env["company_id"]
        _seed_campaign_lead(conn, company_id, "Ann", "ann@acme.example")
        t = call_action(MOD.add_campaign_template, conn, ns(
            company_id=company_id, name="Newsletter", template_type="newsletter",
            subject_template="Monthly News", body_html="<p>Hi</p>",
            body_text="Hi", limit=50, offset=0,
        ))
        tmpl_id = t["id"]
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=company_id, name="From Template", subject=None,
            template_id=tmpl_id, recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        camp_id = r["id"]

        with patch.object(campaigns, "_dispatch_campaign_email",
                          return_value=(True, "OUTBOX-T")) as m:
            result = call_action(MOD.send_campaign, conn,
                                 ns(campaign_id=camp_id, db_path=None))

        assert is_ok(result)
        assert result["sent"] == 1
        # template subject + body forwarded to the send seam
        call = m.call_args
        assert call.args[2] == "Monthly News"   # subject
        assert call.args[3] == "<p>Hi</p>"      # body_html
        assert call.args[4] == "Hi"             # body_text


class TestTrackCampaignEvent:
    def test_track_event(self, conn, env):
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="Track Me",
            subject=None, template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        camp_id = r["id"]

        r2 = call_action(MOD.track_campaign_event, conn, ns(
            campaign_id=camp_id, company_id=env["company_id"],
            event_type="opened", recipient_email="user@test.com",
            event_timestamp=None, metadata=None,
        ))
        assert is_ok(r2)
        assert r2["event_type"] == "opened"

    def test_track_event_invalid_type(self, conn, env):
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="Bad Event",
            subject=None, template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        camp_id = r["id"]

        r2 = call_action(MOD.track_campaign_event, conn, ns(
            campaign_id=camp_id, company_id=env["company_id"],
            event_type="invalid_event", recipient_email=None,
            event_timestamp=None, metadata=None,
        ))
        assert is_error(r2)


class TestCampaignRoiReport:
    def test_roi_report(self, conn, env):
        call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="ROI Test",
            subject=None, template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        r = call_action(MOD.campaign_roi_report, conn, ns(
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


# ===========================================================================
# TERRITORIES DOMAIN (10 actions)
# ===========================================================================

class TestAddTerritory:
    def test_add_territory(self, conn, env):
        r = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="East Coast",
            region="Northeast", parent_territory_id=None,
            territory_type="geographic",
            limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["name"] == "East Coast"
        assert r["territory_type"] == "geographic"
        assert r["territory_status"] == "active"

    def test_add_territory_with_parent(self, conn, env):
        r1 = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="US",
            region="North America", parent_territory_id=None,
            territory_type="geographic",
            limit=50, offset=0,
        ))
        parent_id = r1["id"]

        r2 = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="US-West",
            region="West", parent_territory_id=parent_id,
            territory_type="geographic",
            limit=50, offset=0,
        ))
        assert is_ok(r2)


class TestUpdateTerritory:
    def test_update_territory_name(self, conn, env):
        r = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Old Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic",
            limit=50, offset=0,
        ))
        ter_id = r["id"]

        r2 = call_action(MOD.update_territory, conn, ns(
            territory_id=ter_id, name="New Terr",
            region=None, territory_type=None,
            parent_territory_id=None,
        ))
        assert is_ok(r2)
        assert "name" in r2["updated_fields"]


class TestGetTerritory:
    def test_get_territory(self, conn, env):
        r = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Get Terr",
            region="Southeast", parent_territory_id=None,
            territory_type="geographic",
            limit=50, offset=0,
        ))
        ter_id = r["id"]

        r2 = call_action(MOD.get_territory, conn, ns(territory_id=ter_id))
        assert is_ok(r2)
        assert r2["name"] == "Get Terr"


class TestListTerritories:
    """list-territories is a pure read: it must mirror the stored territory rows
    exactly and write nothing. No ledger is touched (SELECT only; gl_entry is
    never written by this action), so no two-leg balance assertion can hold."""

    def _seed_three(self, conn, company_id):
        north = call_action(MOD.add_territory, conn, ns(
            company_id=company_id, name="North East",
            region="East Coast", parent_territory_id=None,
            territory_type="geographic", limit=50, offset=0))
        assert is_ok(north)
        west = call_action(MOD.add_territory, conn, ns(
            company_id=company_id, name="Westfield",
            region="Midwest", parent_territory_id=None,
            territory_type="industry", limit=50, offset=0))
        assert is_ok(west)
        city = call_action(MOD.add_territory, conn, ns(
            company_id=company_id, name="North East - City",
            region=None, parent_territory_id=north["id"],
            territory_type="named_account", limit=50, offset=0))
        assert is_ok(city)
        return north["id"], west["id"], city["id"]

    def test_list_territories_returns_every_stored_row_exactly(self, conn, env):
        north_id, west_id, city_id = self._seed_three(conn, env["company_id"])
        before = _snapshot_tables(conn)

        r = call_action(MOD.list_territories, conn, ns(
            company_id=env["company_id"], territory_type=None, search=None,
            limit=50, offset=0))
        assert is_ok(r)
        assert r["total_count"] == 3
        assert r["has_more"] is False
        by_id = {row["id"]: row for row in r["rows"]}
        assert set(by_id) == {north_id, west_id, city_id}
        # Each listed row matches the stored row exactly (read back by id).
        for tid, want_name, want_region, want_parent, want_type in (
            (north_id, "North East", "East Coast", None, "geographic"),
            (west_id, "Westfield", "Midwest", None, "industry"),
            (city_id, "North East - City", None, north_id, "named_account"),
        ):
            stored = conn.execute(
                "SELECT name, region, parent_territory_id, territory_type, "
                "territory_status, company_id FROM crmadv_territory "
                "WHERE id = ?", (tid,)).fetchone()
            listed = by_id[tid]
            assert listed["name"] == stored["name"] == want_name
            assert listed["region"] == stored["region"] == want_region
            assert listed["parent_territory_id"] == stored["parent_territory_id"] == want_parent
            assert listed["territory_type"] == stored["territory_type"] == want_type
            assert listed["territory_status"] == stored["territory_status"] == "active"
            assert listed["company_id"] == stored["company_id"] == env["company_id"]
        # Read-only: the list call wrote nothing anywhere.
        assert _snapshot_tables(conn) == before

    def test_list_territories_filters_narrow_to_exact_subset(self, conn, env):
        north_id, west_id, city_id = self._seed_three(conn, env["company_id"])
        other_company = seed_company(conn, name="Other Co", abbr="OC")
        seed_naming_series(conn, other_company)
        foreign = call_action(MOD.add_territory, conn, ns(
            company_id=other_company, name="Foreign Terr",
            region="Elsewhere", parent_territory_id=None,
            territory_type="product", limit=50, offset=0))
        assert is_ok(foreign)

        by_type = call_action(MOD.list_territories, conn, ns(
            company_id=env["company_id"], territory_type="industry", search=None,
            limit=50, offset=0))
        assert is_ok(by_type)
        assert {row["id"] for row in by_type["rows"]} == {west_id}
        assert by_type["total_count"] == 1

        # search matches name OR region: "coast" hits the region branch only.
        by_region = call_action(MOD.list_territories, conn, ns(
            company_id=env["company_id"], territory_type=None, search="coast",
            limit=50, offset=0))
        assert is_ok(by_region)
        assert {row["id"] for row in by_region["rows"]} == {north_id}

        by_name = call_action(MOD.list_territories, conn, ns(
            company_id=env["company_id"], territory_type=None, search="north east",
            limit=50, offset=0))
        assert is_ok(by_name)
        assert {row["id"] for row in by_name["rows"]} == {north_id, city_id}

        # Company scoping: the foreign row never leaks into this company's list.
        scoped = call_action(MOD.list_territories, conn, ns(
            company_id=env["company_id"], territory_type=None, search=None,
            limit=50, offset=0))
        assert is_ok(scoped)
        assert scoped["total_count"] == 3
        assert foreign["id"] not in {row["id"] for row in scoped["rows"]}

    def test_list_territories_unknown_type_filter_returns_empty(self, conn, env):
        # list-territories performs no input validation: every argument is an
        # optional filter, so no refusal case exists without a production change
        # (forbidden to this task). This pins the real behaviour instead: an
        # unknown type is not an error, it matches nothing.
        self._seed_three(conn, env["company_id"])
        before = _snapshot_tables(conn)
        r = call_action(MOD.list_territories, conn, ns(
            company_id=env["company_id"], territory_type="not-a-type", search=None,
            limit=50, offset=0))
        assert is_ok(r)
        assert r["rows"] == []
        assert r["total_count"] == 0
        assert _snapshot_tables(conn) == before


class TestTerritoryAssignment:
    def test_add_assignment(self, conn, env):
        r = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Assign Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic",
            limit=50, offset=0,
        ))
        ter_id = r["id"]

        r2 = call_action(MOD.add_territory_assignment, conn, ns(
            territory_id=ter_id, company_id=env["company_id"],
            salesperson="John Doe", start_date="2026-01-01",
            end_date=None,
        ))
        assert is_ok(r2)
        assert r2["salesperson"] == "John Doe"
        assert r2["assignment_status"] == "active"

    def test_list_assignments(self, conn, env):
        r = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="List Assign Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic",
            limit=50, offset=0,
        ))
        ter_id = r["id"]

        call_action(MOD.add_territory_assignment, conn, ns(
            territory_id=ter_id, company_id=env["company_id"],
            salesperson="Jane", start_date=None, end_date=None,
        ))

        r2 = call_action(MOD.list_territory_assignments, conn, ns(
            territory_id=ter_id, company_id=env["company_id"],
            limit=50, offset=0,
        ))
        assert is_ok(r2)
        assert r2["total_count"] >= 1


class TestTerritoryQuota:
    def test_set_quota(self, conn, env):
        r = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Quota Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic",
            limit=50, offset=0,
        ))
        ter_id = r["id"]

        r2 = call_action(MOD.set_territory_quota, conn, ns(
            territory_id=ter_id, company_id=env["company_id"],
            period="2026-Q1", quota_amount="100000",
        ))
        assert is_ok(r2)
        assert r2["quota_amount"] == "100000"
        assert r2["action"] == "created"

    def test_set_quota_update(self, conn, env):
        r = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Update Quota Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic",
            limit=50, offset=0,
        ))
        ter_id = r["id"]

        call_action(MOD.set_territory_quota, conn, ns(
            territory_id=ter_id, company_id=env["company_id"],
            period="2026-Q1", quota_amount="100000",
        ))
        r2 = call_action(MOD.set_territory_quota, conn, ns(
            territory_id=ter_id, company_id=env["company_id"],
            period="2026-Q1", quota_amount="150000",
        ))
        assert is_ok(r2)
        assert r2["quota_amount"] == "150000"
        assert r2["action"] == "updated"

    def test_list_quotas(self, conn, env):
        r = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="List Quota Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic",
            limit=50, offset=0,
        ))
        ter_id = r["id"]
        call_action(MOD.set_territory_quota, conn, ns(
            territory_id=ter_id, company_id=env["company_id"],
            period="2026-Q1", quota_amount="50000",
        ))

        r2 = call_action(MOD.list_territory_quotas, conn, ns(
            territory_id=ter_id, company_id=env["company_id"],
            limit=50, offset=0,
        ))
        assert is_ok(r2)
        assert r2["total_count"] >= 1


class TestTerritoryReports:
    def test_territory_performance(self, conn, env):
        call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Perf Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic",
            limit=50, offset=0,
        ))
        r = call_action(MOD.territory_performance_report, conn, ns(
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1

    def test_territory_comparison(self, conn, env):
        # Behavioural: one quota + one assignment per territory, so the join
        # fans out to exactly one row and the aggregates equal the stored
        # money exactly. No ledger is touched (SELECT only; gl_entry is never
        # written by this action), so no two-leg balance assertion can hold.
        north = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Compare North",
            region="North", parent_territory_id=None,
            territory_type="geographic", limit=50, offset=0))
        assert is_ok(north)
        south = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Compare South",
            region="South", parent_territory_id=None,
            territory_type="industry", limit=50, offset=0))
        assert is_ok(south)
        for tid, amount in ((north["id"], "100000.00"), (south["id"], "50000.00")):
            q = call_action(MOD.set_territory_quota, conn, ns(
                territory_id=tid, company_id=env["company_id"],
                period="2026-Q1", quota_amount=amount))
            assert is_ok(q)
        a = call_action(MOD.add_territory_assignment, conn, ns(
            territory_id=north["id"], company_id=env["company_id"],
            salesperson="Amy", start_date=None, end_date=None))
        assert is_ok(a)
        before = _snapshot_tables(conn)

        r = call_action(MOD.territory_comparison_report, conn, ns(
            company_id=env["company_id"], limit=50, offset=0))
        assert is_ok(r)
        assert r["total_count"] == 2
        assert r["count"] == 2
        assert r["has_more"] is False
        by_id = {row["id"]: row for row in r["rows"]}
        assert set(by_id) == {north["id"], south["id"]}
        # actual_amount is TEXT money no owner action ever sets, so both legs
        # read back "0.00" and attainment is exactly 0.0.
        assert by_id[north["id"]]["total_quota"] == "100000.00"
        assert D(str(by_id[north["id"]]["total_quota"])) == D("100000.00")
        assert by_id[north["id"]]["total_actual"] == "0.00"
        assert by_id[north["id"]]["total_reps"] == 1
        assert by_id[north["id"]]["quota_periods"] == 1
        assert by_id[north["id"]]["attainment_pct"] == 0.0
        assert by_id[north["id"]]["overall_attainment_pct"] == 0.0
        assert by_id[south["id"]]["total_quota"] == "50000.00"
        assert D(str(by_id[south["id"]]["total_quota"])) == D("50000.00")
        assert by_id[south["id"]]["total_actual"] == "0.00"
        assert by_id[south["id"]]["total_reps"] == 0
        assert by_id[south["id"]]["quota_periods"] == 1
        # Independent read-back: the reported quota equals the stored quota row.
        for tid, want in ((north["id"], "100000.00"), (south["id"], "50000.00")):
            stored = conn.execute(
                "SELECT quota_amount, actual_amount FROM crmadv_territory_quota "
                "WHERE territory_id = ? AND company_id = ?",
                (tid, env["company_id"])).fetchone()
            assert D(str(stored["quota_amount"])) == D(want)
            assert D(str(by_id[tid]["total_quota"])) == D(str(stored["quota_amount"]))
        reps = conn.execute(
            "SELECT COUNT(*) AS c FROM crmadv_territory_assignment "
            "WHERE territory_id = ? AND assignment_status = 'active'",
            (north["id"],)).fetchone()["c"]
        assert reps == by_id[north["id"]]["total_reps"] == 1
        # Read-only: the report wrote nothing anywhere.
        assert _snapshot_tables(conn) == before

    def test_territory_comparison_fanout_double_counts_quota(self, conn, env):
        # FINDING (deliberately not fixed: production changes are forbidden to
        # this task). territory-comparison-report joins assignments x quotas and
        # SUMs over the fanned-out rows while COUNTing DISTINCT ids, so the
        # counts stay right and the money doubles: one quota of 100000.00 with
        # two active assignments reports total_quota "200000.00" although the
        # stored quota row still sums to 100000.00. This test pins the real
        # behaviour and must be rewritten with the fix, not before it.
        terr = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Fanout Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic", limit=50, offset=0))
        assert is_ok(terr)
        q = call_action(MOD.set_territory_quota, conn, ns(
            territory_id=terr["id"], company_id=env["company_id"],
            period="2026-Q1", quota_amount="100000.00"))
        assert is_ok(q)
        for salesperson in ("Amy", "Bob"):
            a = call_action(MOD.add_territory_assignment, conn, ns(
                territory_id=terr["id"], company_id=env["company_id"],
                salesperson=salesperson, start_date=None, end_date=None))
            assert is_ok(a)

        r = call_action(MOD.territory_comparison_report, conn, ns(
            company_id=env["company_id"], limit=50, offset=0))
        assert is_ok(r)
        assert r["total_count"] == 1
        row = r["rows"][0]
        assert row["id"] == terr["id"]
        assert row["total_reps"] == 2
        assert row["quota_periods"] == 1
        assert row["total_quota"] == "200000.00"
        stored_sum = conn.execute(
            "SELECT COALESCE(SUM(CAST(quota_amount AS NUMERIC)), 0) AS s "
            "FROM crmadv_territory_quota WHERE territory_id = ?",
            (terr["id"],)).fetchone()["s"]
        assert D(str(stored_sum)) == D("100000.00")
        assert D(str(row["total_quota"])) == 2 * D(str(stored_sum))

    def test_territory_comparison_refuses_without_company(self, conn, env):
        terr = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Refusal Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic", limit=50, offset=0))
        assert is_ok(terr)
        before = _snapshot_tables(conn)

        missing = call_action(MOD.territory_comparison_report, conn, ns(
            company_id=None, limit=50, offset=0))
        assert is_error(missing)
        assert "--company-id is required" in _msg(missing)

        unknown = call_action(MOD.territory_comparison_report, conn, ns(
            company_id="does-not-exist", limit=50, offset=0))
        assert is_error(unknown)
        assert "does-not-exist" in _msg(unknown)
        assert "not found" in _msg(unknown)
        # A refusal that half-writes is worse than no refusal: byte-identical.
        assert _snapshot_tables(conn) == before


# ===========================================================================
# CONTRACTS DOMAIN (10 actions)
# ===========================================================================

class TestAddContract:
    def test_add_contract(self, conn, env):
        r = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Acme Corp",
            contract_type="service", start_date="2026-01-01",
            end_date="2026-12-31", total_value="120000",
            annual_value="120000", auto_renew="1",
            renewal_terms="Annual auto-renew",
        ))
        assert is_ok(r)
        assert r["customer_name"] == "Acme Corp"
        assert r["contract_status"] == "draft"
        assert r["naming_series"].startswith("CTR-")

    def test_add_contract_invalid_type(self, conn, env):
        r = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Bad Type",
            contract_type="invalid", start_date=None, end_date=None,
            total_value=None, annual_value=None, auto_renew=None,
            renewal_terms=None,
        ))
        assert is_error(r)


class TestUpdateContract:
    def test_update_contract(self, conn, env):
        r = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Upd Corp",
            contract_type="service", start_date=None, end_date=None,
            total_value="50000", annual_value=None, auto_renew=None,
            renewal_terms=None,
        ))
        ctr_id = r["id"]

        r2 = call_action(MOD.update_contract, conn, ns(
            contract_id=ctr_id, customer_name=None,
            contract_type=None, start_date=None, end_date=None,
            total_value="75000", annual_value=None, renewal_terms=None,
        ))
        assert is_ok(r2)
        assert "total_value" in r2["updated_fields"]

    def test_update_terminated_contract(self, conn, env):
        r = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Term Corp",
            contract_type="service", start_date=None, end_date=None,
            total_value="10000", annual_value=None, auto_renew=None,
            renewal_terms=None,
        ))
        ctr_id = r["id"]
        call_action(MOD.terminate_contract, conn, ns(contract_id=ctr_id))

        r2 = call_action(MOD.update_contract, conn, ns(
            contract_id=ctr_id, customer_name="New Name",
            contract_type=None, start_date=None, end_date=None,
            total_value=None, annual_value=None, renewal_terms=None,
        ))
        assert is_error(r2)


class TestGetContract:
    def test_get_contract(self, conn, env):
        r = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Get Corp",
            contract_type="subscription", start_date=None, end_date=None,
            total_value="25000", annual_value=None, auto_renew=None,
            renewal_terms=None,
        ))
        ctr_id = r["id"]

        r2 = call_action(MOD.get_contract, conn, ns(contract_id=ctr_id))
        assert is_ok(r2)
        assert r2["customer_name"] == "Get Corp"


class TestListContracts:
    """list-contracts is a pure read: it must mirror the stored contract rows
    exactly and write nothing. No ledger is touched (SELECT only; gl_entry is
    never written by this action), so no two-leg balance assertion can hold."""

    def _seed_three(self, conn, company_id):
        a = call_action(MOD.add_contract, conn, ns(
            company_id=company_id, customer_name="Acme Corp",
            contract_type="service", start_date="2026-01-01",
            end_date="2026-12-31", total_value="120000.00",
            annual_value="120000.00", auto_renew="1",
            renewal_terms="Annual auto-renew"))
        assert is_ok(a)
        b = call_action(MOD.add_contract, conn, ns(
            company_id=company_id, customer_name="Beta LLC",
            contract_type="subscription", start_date=None, end_date=None,
            total_value="60000.00", annual_value="60000.00", auto_renew=None,
            renewal_terms=None))
        assert is_ok(b)
        renewed = call_action(MOD.renew_contract, conn, ns(
            contract_id=b["id"], end_date="2026-12-31"))
        assert is_ok(renewed)
        c = call_action(MOD.add_contract, conn, ns(
            company_id=company_id, customer_name="Acme Labs",
            contract_type="service", start_date=None, end_date=None,
            total_value="5000.50", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(c)
        return a["id"], b["id"], c["id"]

    def test_list_contracts_returns_every_stored_row_exactly(self, conn, env):
        a_id, b_id, c_id = self._seed_three(conn, env["company_id"])
        before = _snapshot_tables(conn)

        r = call_action(MOD.list_contracts, conn, ns(
            company_id=env["company_id"], contract_type=None,
            contract_status_filter=None, search=None, limit=50, offset=0))
        assert is_ok(r)
        assert r["total_count"] == 3
        assert r["has_more"] is False
        by_id = {row["id"]: row for row in r["rows"]}
        assert set(by_id) == {a_id, b_id, c_id}
        # Each listed row matches the stored row exactly (read back by id);
        # money compares as exact Decimal strings, never float.
        for cid, want_name, want_type, want_status, want_total, want_annual in (
            (a_id, "Acme Corp", "service", "draft", "120000.00", "120000.00"),
            (b_id, "Beta LLC", "subscription", "renewed", "60000.00", "60000.00"),
            (c_id, "Acme Labs", "service", "draft", "5000.50", None),
        ):
            stored = conn.execute(
                "SELECT customer_name, contract_type, contract_status, "
                "total_value, annual_value, company_id FROM crmadv_contract "
                "WHERE id = ?", (cid,)).fetchone()
            listed = by_id[cid]
            assert listed["customer_name"] == stored["customer_name"] == want_name
            assert listed["contract_type"] == stored["contract_type"] == want_type
            assert listed["contract_status"] == stored["contract_status"] == want_status
            assert listed["total_value"] == stored["total_value"] == want_total
            assert D(str(listed["total_value"])) == D(want_total)
            assert listed["annual_value"] == stored["annual_value"] == want_annual
            if want_annual is not None:
                assert D(str(listed["annual_value"])) == D(want_annual)
            assert listed["company_id"] == stored["company_id"] == env["company_id"]
        # Read-only: the list call wrote nothing anywhere.
        assert _snapshot_tables(conn) == before

    def test_list_contracts_filters_narrow_to_exact_subset(self, conn, env):
        a_id, b_id, c_id = self._seed_three(conn, env["company_id"])
        other_company = seed_company(conn, name="Other Co", abbr="OC")
        seed_naming_series(conn, other_company)
        foreign = call_action(MOD.add_contract, conn, ns(
            company_id=other_company, customer_name="Foreign Inc",
            contract_type="service", start_date=None, end_date=None,
            total_value="1.00", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(foreign)

        renewed = call_action(MOD.list_contracts, conn, ns(
            company_id=env["company_id"], contract_type=None,
            contract_status_filter="renewed", search=None, limit=50, offset=0))
        assert is_ok(renewed)
        assert {row["id"] for row in renewed["rows"]} == {b_id}
        assert renewed["total_count"] == 1

        service = call_action(MOD.list_contracts, conn, ns(
            company_id=env["company_id"], contract_type="service",
            contract_status_filter=None, search=None, limit=50, offset=0))
        assert is_ok(service)
        assert {row["id"] for row in service["rows"]} == {a_id, c_id}

        # search is case-insensitive on customer_name: Beta must be absent.
        searched = call_action(MOD.list_contracts, conn, ns(
            company_id=env["company_id"], contract_type=None,
            contract_status_filter=None, search="acme", limit=50, offset=0))
        assert is_ok(searched)
        assert {row["id"] for row in searched["rows"]} == {a_id, c_id}
        assert b_id not in {row["id"] for row in searched["rows"]}

        # Company scoping: the foreign row never leaks into this company's list.
        scoped = call_action(MOD.list_contracts, conn, ns(
            company_id=env["company_id"], contract_type=None,
            contract_status_filter=None, search=None, limit=50, offset=0))
        assert is_ok(scoped)
        assert scoped["total_count"] == 3
        assert foreign["id"] not in {row["id"] for row in scoped["rows"]}

    def test_list_contracts_unknown_status_filter_returns_empty(self, conn, env):
        # list-contracts performs no input validation: every argument is an
        # optional filter, so no refusal case exists without a production change
        # (forbidden to this task). This pins the real behaviour instead: an
        # unknown status is not an error, it matches nothing.
        self._seed_three(conn, env["company_id"])
        before = _snapshot_tables(conn)
        r = call_action(MOD.list_contracts, conn, ns(
            company_id=env["company_id"], contract_type=None,
            contract_status_filter="not-a-status", search=None,
            limit=50, offset=0))
        assert is_ok(r)
        assert r["rows"] == []
        assert r["total_count"] == 0
        assert _snapshot_tables(conn) == before


class TestContractObligation:
    def test_add_obligation(self, conn, env):
        r = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Oblig Corp",
            contract_type="service", start_date=None, end_date=None,
            total_value=None, annual_value=None, auto_renew=None,
            renewal_terms=None,
        ))
        ctr_id = r["id"]

        r2 = call_action(MOD.add_contract_obligation, conn, ns(
            contract_id=ctr_id, company_id=env["company_id"],
            description="Deliver monthly report",
            due_date="2026-04-01", obligee="us",
            obligation_status_filter=None,
        ))
        assert is_ok(r2)
        assert r2["obligation_status"] == "pending"

    def test_list_obligations(self, conn, env):
        r = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="List Oblig",
            contract_type="service", start_date=None, end_date=None,
            total_value=None, annual_value=None, auto_renew=None,
            renewal_terms=None,
        ))
        ctr_id = r["id"]
        call_action(MOD.add_contract_obligation, conn, ns(
            contract_id=ctr_id, company_id=env["company_id"],
            description="SLA compliance", due_date=None, obligee=None,
            obligation_status_filter=None,
        ))

        r2 = call_action(MOD.list_contract_obligations, conn, ns(
            contract_id=ctr_id, company_id=env["company_id"],
            obligation_status_filter=None, limit=50, offset=0,
        ))
        assert is_ok(r2)
        assert r2["total_count"] >= 1


class TestContractLifecycle:
    def test_renew_contract(self, conn, env):
        r = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Renew Corp",
            contract_type="service", start_date="2026-01-01",
            end_date="2026-06-30", total_value="60000",
            annual_value=None, auto_renew=None, renewal_terms=None,
        ))
        ctr_id = r["id"]

        r2 = call_action(MOD.renew_contract, conn, ns(
            contract_id=ctr_id, end_date="2026-12-31",
        ))
        assert is_ok(r2)
        assert r2["contract_status"] == "renewed"

    def test_terminate_contract(self, conn, env):
        r = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Term Corp",
            contract_type="service", start_date=None, end_date=None,
            total_value=None, annual_value=None, auto_renew=None,
            renewal_terms=None,
        ))
        ctr_id = r["id"]

        r2 = call_action(MOD.terminate_contract, conn, ns(contract_id=ctr_id))
        assert is_ok(r2)
        assert r2["contract_status"] == "terminated"

    def test_terminate_already_terminated(self, conn, env):
        r = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Double Term",
            contract_type="service", start_date=None, end_date=None,
            total_value=None, annual_value=None, auto_renew=None,
            renewal_terms=None,
        ))
        ctr_id = r["id"]
        call_action(MOD.terminate_contract, conn, ns(contract_id=ctr_id))

        r2 = call_action(MOD.terminate_contract, conn, ns(contract_id=ctr_id))
        assert is_error(r2)


class TestContractReports:
    def test_expiry_report(self, conn, env):
        call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Expiry Corp",
            contract_type="service", start_date="2026-01-01",
            end_date="2026-06-30", total_value="30000",
            annual_value=None, auto_renew=None, renewal_terms=None,
        ))

        r = call_action(MOD.contract_expiry_report, conn, ns(
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1

    def test_value_report(self, conn, env):
        call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Value Corp",
            contract_type="subscription", start_date=None, end_date=None,
            total_value="100000", annual_value="100000", auto_renew=None,
            renewal_terms=None,
        ))

        r = call_action(MOD.contract_value_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["total_contracts"] >= 1


# ===========================================================================
# AUTOMATION DOMAIN (10 actions)
# ===========================================================================

class TestAutomationWorkflow:
    def test_add_workflow(self, conn, env):
        r = call_action(MOD.add_automation_workflow, conn, ns(
            company_id=env["company_id"], name="Welcome Flow",
            trigger_event="lead_created",
            conditions_json='{"source": "website"}',
            actions_json='[{"action": "send_email", "template": "welcome"}]',
        ))
        assert is_ok(r)
        assert r["name"] == "Welcome Flow"
        assert r["workflow_status"] == "inactive"

    def test_add_workflow_invalid_json(self, conn, env):
        r = call_action(MOD.add_automation_workflow, conn, ns(
            company_id=env["company_id"], name="Bad JSON",
            trigger_event=None,
            conditions_json="not json",
            actions_json="[]",
        ))
        assert is_error(r)

    def test_update_workflow(self, conn, env):
        r = call_action(MOD.add_automation_workflow, conn, ns(
            company_id=env["company_id"], name="Update Flow",
            trigger_event=None, conditions_json=None, actions_json=None,
        ))
        wf_id = r["id"]

        r2 = call_action(MOD.update_automation_workflow, conn, ns(
            workflow_id=wf_id, name="Updated Flow",
            trigger_event=None, conditions_json=None, actions_json=None,
        ))
        assert is_ok(r2)
        assert "name" in r2["updated_fields"]

    def test_activate_deactivate_workflow(self, conn, env):
        r = call_action(MOD.add_automation_workflow, conn, ns(
            company_id=env["company_id"], name="Toggle Flow",
            trigger_event=None, conditions_json=None, actions_json=None,
        ))
        wf_id = r["id"]

        r2 = call_action(MOD.activate_workflow, conn, ns(workflow_id=wf_id))
        assert is_ok(r2)
        assert r2["workflow_status"] == "active"

        r3 = call_action(MOD.deactivate_workflow, conn, ns(workflow_id=wf_id))
        assert is_ok(r3)
        assert r3["workflow_status"] == "inactive"

    def test_activate_already_active(self, conn, env):
        r = call_action(MOD.add_automation_workflow, conn, ns(
            company_id=env["company_id"], name="Already Active",
            trigger_event=None, conditions_json=None, actions_json=None,
        ))
        wf_id = r["id"]
        call_action(MOD.activate_workflow, conn, ns(workflow_id=wf_id))

        r2 = call_action(MOD.activate_workflow, conn, ns(workflow_id=wf_id))
        assert is_error(r2)

    def test_list_workflows(self, conn, env):
        call_action(MOD.add_automation_workflow, conn, ns(
            company_id=env["company_id"], name="WF1",
            trigger_event=None, conditions_json=None, actions_json=None,
        ))
        r = call_action(MOD.list_automation_workflows, conn, ns(
            company_id=env["company_id"], workflow_status_filter=None,
            limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestLeadScoreRule:
    def test_add_lead_score_rule(self, conn, env):
        r = call_action(MOD.add_lead_score_rule, conn, ns(
            company_id=env["company_id"], name="Website Visitor",
            criteria_json='{"source": "website"}', points=10,
        ))
        assert is_ok(r)
        assert r["name"] == "Website Visitor"
        assert r["points"] == 10

    def test_list_lead_score_rules(self, conn, env):
        call_action(MOD.add_lead_score_rule, conn, ns(
            company_id=env["company_id"], name="Rule1",
            criteria_json='{}', points=5,
        ))
        r = call_action(MOD.list_lead_score_rules, conn, ns(
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestNurtureSequence:
    def test_add_nurture_sequence(self, conn, env):
        steps = json.dumps([
            {"day": 0, "action": "send_welcome"},
            {"day": 3, "action": "send_followup"},
        ])
        r = call_action(MOD.add_nurture_sequence, conn, ns(
            company_id=env["company_id"], name="Onboarding Sequence",
            description="New customer onboarding",
            steps_json=steps,
        ))
        assert is_ok(r)
        assert r["name"] == "Onboarding Sequence"
        assert r["total_steps"] == 2
        assert r["sequence_status"] == "draft"

    def test_list_nurture_sequences(self, conn, env):
        call_action(MOD.add_nurture_sequence, conn, ns(
            company_id=env["company_id"], name="NS1",
            description=None, steps_json=None,
        ))
        r = call_action(MOD.list_nurture_sequences, conn, ns(
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestAutomationPerformanceReport:
    def test_performance_report(self, conn, env):
        call_action(MOD.add_automation_workflow, conn, ns(
            company_id=env["company_id"], name="Perf WF",
            trigger_event=None, conditions_json=None, actions_json=None,
        ))
        call_action(MOD.add_lead_score_rule, conn, ns(
            company_id=env["company_id"], name="Perf Rule",
            criteria_json='{}', points=5,
        ))
        call_action(MOD.add_nurture_sequence, conn, ns(
            company_id=env["company_id"], name="Perf NS",
            description=None, steps_json=None,
        ))

        r = call_action(MOD.automation_performance_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["total_workflows"] >= 1
        assert r["total_lead_score_rules"] >= 1
        assert r["total_nurture_sequences"] >= 1


class TestDripSequence:
    def test_add_drip_sequence(self, conn, env):
        r = call_action(MOD.add_drip_sequence, conn, ns(
            company_id=env["company_id"], name="Welcome Drip",
            description="3-email welcome series",
        ))
        assert is_ok(r)
        assert r["name"] == "Welcome Drip"
        assert r["is_active"] == 1
        ds_id = r["id"]

        # Verify the row landed with exact stored values.
        row = conn.execute(
            "SELECT id, company_id, name, description, is_active "
            "FROM crmadv_drip_sequence WHERE id = ?", (ds_id,)
        ).fetchone()
        assert row is not None
        assert row["id"] == ds_id
        assert row["company_id"] == env["company_id"]
        assert row["name"] == "Welcome Drip"
        assert row["description"] == "3-email welcome series"
        assert row["is_active"] == 1

    def test_add_drip_sequence_no_description(self, conn, env):
        r = call_action(MOD.add_drip_sequence, conn, ns(
            company_id=env["company_id"], name="Minimal Drip",
            description=None,
        ))
        assert is_ok(r)
        row = conn.execute(
            "SELECT description, is_active FROM crmadv_drip_sequence WHERE id = ?",
            (r["id"],)
        ).fetchone()
        assert row["description"] is None
        assert row["is_active"] == 1

    def test_add_drip_sequence_missing_name(self, conn, env):
        r = call_action(MOD.add_drip_sequence, conn, ns(
            company_id=env["company_id"], name=None, description=None,
        ))
        assert is_error(r)

    def test_list_drip_sequences(self, conn, env):
        # Seed two rows for this company.
        r1 = call_action(MOD.add_drip_sequence, conn, ns(
            company_id=env["company_id"], name="Drip A", description=None,
        ))
        r2 = call_action(MOD.add_drip_sequence, conn, ns(
            company_id=env["company_id"], name="Drip B", description=None,
        ))
        assert is_ok(r1) and is_ok(r2)
        seeded = {r1["id"], r2["id"]}

        r = call_action(MOD.list_drip_sequences, conn, ns(
            company_id=env["company_id"], is_active=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 2
        listed_ids = {row["id"] for row in r["rows"]}
        assert seeded.issubset(listed_ids)
        # Every returned row belongs to the requested company (owning-module read).
        assert all(row["company_id"] == env["company_id"] for row in r["rows"])

    def test_list_drip_sequences_is_active_filter(self, conn, env):
        r = call_action(MOD.add_drip_sequence, conn, ns(
            company_id=env["company_id"], name="Active Drip", description=None,
        ))
        assert is_ok(r)
        # is_active=1 returns the freshly created (active) row.
        active = call_action(MOD.list_drip_sequences, conn, ns(
            company_id=env["company_id"], is_active=1, limit=50, offset=0,
        ))
        assert is_ok(active)
        assert r["id"] in {row["id"] for row in active["rows"]}
        # is_active=0 excludes it.
        inactive = call_action(MOD.list_drip_sequences, conn, ns(
            company_id=env["company_id"], is_active=0, limit=50, offset=0,
        ))
        assert is_ok(inactive)
        assert r["id"] not in {row["id"] for row in inactive["rows"]}


class TestDripSequenceStep:
    def _seed_sequence(self, conn, env):
        r = call_action(MOD.add_drip_sequence, conn, ns(
            company_id=env["company_id"], name="Step Host Drip", description=None,
        ))
        assert is_ok(r)
        return r["id"]

    def test_add_drip_step(self, conn, env):
        seq_id = self._seed_sequence(conn, env)
        r = call_action(MOD.add_drip_step, conn, ns(
            sequence_id=seq_id, step_order=1, delay_hours=24,
            email_template_id="TPL-001",
        ))
        assert is_ok(r)
        assert r["sequence_id"] == seq_id
        assert r["step_order"] == 1
        assert r["delay_hours"] == 24
        assert r["is_active"] == 1
        step_id = r["id"]

        # Verify the row landed with exact stored values.
        row = conn.execute(
            "SELECT id, sequence_id, step_order, delay_hours, email_template_id, is_active "
            "FROM crmadv_drip_sequence_step WHERE id = ?", (step_id,)
        ).fetchone()
        assert row is not None
        assert row["id"] == step_id
        assert row["sequence_id"] == seq_id
        assert row["step_order"] == 1
        assert row["delay_hours"] == 24
        assert row["email_template_id"] == "TPL-001"
        assert row["is_active"] == 1

    def test_add_drip_step_no_template(self, conn, env):
        seq_id = self._seed_sequence(conn, env)
        r = call_action(MOD.add_drip_step, conn, ns(
            sequence_id=seq_id, step_order=1, delay_hours=0,
            email_template_id=None,
        ))
        assert is_ok(r)
        row = conn.execute(
            "SELECT email_template_id, delay_hours FROM crmadv_drip_sequence_step WHERE id = ?",
            (r["id"],)
        ).fetchone()
        assert row["email_template_id"] is None
        assert row["delay_hours"] == 0

    def test_add_drip_step_missing_step_order(self, conn, env):
        seq_id = self._seed_sequence(conn, env)
        r = call_action(MOD.add_drip_step, conn, ns(
            sequence_id=seq_id, step_order=None, delay_hours=12,
            email_template_id=None,
        ))
        assert is_error(r)

    def test_add_drip_step_invalid_sequence(self, conn, env):
        r = call_action(MOD.add_drip_step, conn, ns(
            sequence_id="does-not-exist", step_order=1, delay_hours=0,
            email_template_id=None,
        ))
        assert is_error(r)

    def test_list_drip_steps_ordered(self, conn, env):
        seq_id = self._seed_sequence(conn, env)
        # Insert out of order; list must return them sorted by step_order.
        for order, delay in ((3, 72), (1, 0), (2, 24)):
            r = call_action(MOD.add_drip_step, conn, ns(
                sequence_id=seq_id, step_order=order, delay_hours=delay,
                email_template_id=None,
            ))
            assert is_ok(r)

        r = call_action(MOD.list_drip_steps, conn, ns(
            sequence_id=seq_id, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] == 3
        orders = [row["step_order"] for row in r["rows"]]
        assert orders == [1, 2, 3]
        # Every returned step belongs to the requested sequence.
        assert all(row["sequence_id"] == seq_id for row in r["rows"])

    def test_list_drip_steps_invalid_sequence(self, conn, env):
        r = call_action(MOD.list_drip_steps, conn, ns(
            sequence_id="does-not-exist", limit=50, offset=0,
        ))
        assert is_error(r)


class TestDripEnrollment:
    def _seed_sequence(self, conn, env, name="Enroll Host Drip"):
        r = call_action(MOD.add_drip_sequence, conn, ns(
            company_id=env["company_id"], name=name, description=None,
        ))
        assert is_ok(r)
        return r["id"]

    def _add_step(self, conn, seq_id, order, delay):
        r = call_action(MOD.add_drip_step, conn, ns(
            sequence_id=seq_id, step_order=order, delay_hours=delay,
            email_template_id=None,
        ))
        assert is_ok(r)
        return r["id"]

    def test_enroll_contact_computes_next_send(self, conn, env):
        seq_id = self._seed_sequence(conn, env)
        # First step (lowest step_order) drives next_send_at; insert out of order.
        self._add_step(conn, seq_id, 2, 999)
        self._add_step(conn, seq_id, 1, 48)

        r = call_action(MOD.enroll_contact, conn, ns(
            sequence_id=seq_id, contact_id="CONTACT-1",
        ))
        assert is_ok(r)
        assert r["sequence_id"] == seq_id
        assert r["contact_id"] == "CONTACT-1"
        assert r["current_step"] == 0
        assert r["enrollment_status"] == "active"
        enr_id = r["id"]

        # Read back exact stored values.
        row = conn.execute(
            "SELECT id, sequence_id, contact_id, current_step, status, "
            "next_send_at, enrolled_at FROM crmadv_drip_enrollment WHERE id = ?",
            (enr_id,)
        ).fetchone()
        assert row is not None
        assert row["id"] == enr_id
        assert row["sequence_id"] == seq_id
        assert row["contact_id"] == "CONTACT-1"
        assert row["current_step"] == 0
        assert row["status"] == "active"
        # next_send_at == enrolled_at + first step's delay_hours (48h), exact.
        enrolled = datetime.strptime(row["enrolled_at"], "%Y-%m-%dT%H:%M:%SZ")
        expected = (enrolled + timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%SZ")
        assert row["next_send_at"] == expected
        assert r["next_send_at"] == expected

    def test_enroll_contact_no_steps_null_next_send(self, conn, env):
        seq_id = self._seed_sequence(conn, env, name="Stepless Drip")
        r = call_action(MOD.enroll_contact, conn, ns(
            sequence_id=seq_id, contact_id="CONTACT-2",
        ))
        assert is_ok(r)
        assert r["next_send_at"] is None
        row = conn.execute(
            "SELECT next_send_at FROM crmadv_drip_enrollment WHERE id = ?",
            (r["id"],)
        ).fetchone()
        assert row["next_send_at"] is None

    def test_enroll_contact_invalid_sequence(self, conn, env):
        r = call_action(MOD.enroll_contact, conn, ns(
            sequence_id="does-not-exist", contact_id="CONTACT-3",
        ))
        assert is_error(r)

    def test_enroll_contact_inactive_sequence(self, conn, env):
        seq_id = self._seed_sequence(conn, env, name="Inactive Drip")
        # No deactivate action yet; flip is_active directly to exercise the guard.
        conn.execute("UPDATE crmadv_drip_sequence SET is_active = 0 WHERE id = ?", (seq_id,))
        conn.commit()
        r = call_action(MOD.enroll_contact, conn, ns(
            sequence_id=seq_id, contact_id="CONTACT-4",
        ))
        assert is_error(r)

    def test_list_enrollments_and_status_filter(self, conn, env):
        seq_id = self._seed_sequence(conn, env, name="List Drip")
        ids = []
        for c in ("CONTACT-A", "CONTACT-B", "CONTACT-C"):
            r = call_action(MOD.enroll_contact, conn, ns(
                sequence_id=seq_id, contact_id=c,
            ))
            assert is_ok(r)
            ids.append(r["id"])

        # Cancel one so we can exercise the status filter.
        cancelled = call_action(MOD.cancel_enrollment, conn, ns(enrollment_id=ids[0]))
        assert is_ok(cancelled)

        allr = call_action(MOD.list_enrollments, conn, ns(
            sequence_id=seq_id, status=None, limit=50, offset=0,
        ))
        assert is_ok(allr)
        assert allr["total_count"] == 3
        assert all(row["sequence_id"] == seq_id for row in allr["rows"])

        active = call_action(MOD.list_enrollments, conn, ns(
            sequence_id=seq_id, status="active", limit=50, offset=0,
        ))
        assert is_ok(active)
        assert active["total_count"] == 2
        assert all(row["status"] == "active" for row in active["rows"])

        cancelled_list = call_action(MOD.list_enrollments, conn, ns(
            sequence_id=seq_id, status="cancelled", limit=50, offset=0,
        ))
        assert is_ok(cancelled_list)
        assert cancelled_list["total_count"] == 1
        assert cancelled_list["rows"][0]["id"] == ids[0]

    def test_list_enrollments_invalid_sequence(self, conn, env):
        r = call_action(MOD.list_enrollments, conn, ns(
            sequence_id="does-not-exist", status=None, limit=50, offset=0,
        ))
        assert is_error(r)

    def test_cancel_enrollment_sets_status(self, conn, env):
        seq_id = self._seed_sequence(conn, env, name="Cancel Drip")
        e = call_action(MOD.enroll_contact, conn, ns(
            sequence_id=seq_id, contact_id="CONTACT-X",
        ))
        assert is_ok(e)
        enr_id = e["id"]

        r = call_action(MOD.cancel_enrollment, conn, ns(enrollment_id=enr_id))
        assert is_ok(r)
        assert r["enrollment_status"] == "cancelled"
        row = conn.execute(
            "SELECT status, next_send_at FROM crmadv_drip_enrollment WHERE id = ?",
            (enr_id,)
        ).fetchone()
        assert row["status"] == "cancelled"
        assert row["next_send_at"] is None

    def test_cancel_enrollment_invalid_id(self, conn, env):
        r = call_action(MOD.cancel_enrollment, conn, ns(enrollment_id="does-not-exist"))
        assert is_error(r)


# ===========================================================================
# DRIP WORKER -- process-drip-sends (M8 phase B, completes M8-B)
# ===========================================================================

def _insert_enrollment(conn, seq_id, contact_id, next_send_at,
                       current_step=0, status="active"):
    """Insert a crmadv_drip_enrollment row directly so tests control
    next_send_at / current_step precisely (the enroll-contact action derives
    next_send_at from the wall clock, which is not deterministic)."""
    eid = str(uuid.uuid4())
    stamp = "2026-01-01T00:00:00Z"
    conn.execute(
        "INSERT INTO crmadv_drip_enrollment "
        "(id, sequence_id, contact_id, current_step, status, next_send_at, "
        " enrolled_at, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
        (eid, seq_id, contact_id, current_step, status, next_send_at,
         stamp, stamp, stamp))
    conn.commit()
    return eid


def _seed_lead(conn, company_id, email="drip@example.com"):
    """Seed a CRM contact (foundation `lead` table) for recipient resolution."""
    lid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO lead (id, lead_name, email, status, company_id) "
        "VALUES (?,?,?,?,?)",
        (lid, "Drip Lead", email, "new", company_id))
    conn.commit()
    return lid


class TestProcessDripSends:
    def _seq_with_steps(self, conn, env, steps):
        """steps: list of (step_order, delay_hours, email_template_id)."""
        seq = call_action(MOD.add_drip_sequence, conn, ns(
            company_id=env["company_id"], name="Worker Drip", description=None))
        assert is_ok(seq)
        for order, delay, tpl in steps:
            r = call_action(MOD.add_drip_step, conn, ns(
                sequence_id=seq["id"], step_order=order, delay_hours=delay,
                email_template_id=tpl))
            assert is_ok(r)
        return seq["id"]

    def test_advances_and_recomputes_next_send(self, conn, env):
        seq_id = self._seq_with_steps(conn, env, [(1, 0, None), (2, 48, None)])
        enr_id = _insert_enrollment(conn, seq_id, "CONTACT-1",
                                    next_send_at="2026-01-01T00:00:00Z")
        r = call_action(MOD.process_drip_sends, conn, ns(
            company_id=env["company_id"], limit=100,
            now="2026-01-02T00:00:00Z", db_path=None))
        assert is_ok(r)
        assert r["processed"] == 1
        assert r["sent"] == 1
        assert r["completed"] == 0
        row = conn.execute(
            "SELECT current_step, status, next_send_at "
            "FROM crmadv_drip_enrollment WHERE id = ?", (enr_id,)).fetchone()
        assert row["current_step"] == 1
        assert row["status"] == "active"
        # next_send_at == now + the NEXT step's delay_hours (48h), exact.
        assert row["next_send_at"] == "2026-01-04T00:00:00Z"

    def test_second_run_completes(self, conn, env):
        seq_id = self._seq_with_steps(conn, env, [(1, 0, None), (2, 48, None)])
        enr_id = _insert_enrollment(conn, seq_id, "CONTACT-1",
                                    next_send_at="2026-01-01T00:00:00Z")
        # Run 1: send step 0, recompute next_send_at to 2026-01-04T00:00:00Z.
        call_action(MOD.process_drip_sends, conn, ns(
            company_id=env["company_id"], limit=100,
            now="2026-01-02T00:00:00Z", db_path=None))
        # Run 2: at the recomputed instant, send last step -> completed.
        r = call_action(MOD.process_drip_sends, conn, ns(
            company_id=env["company_id"], limit=100,
            now="2026-01-04T00:00:00Z", db_path=None))
        assert is_ok(r)
        assert r["sent"] == 1
        assert r["completed"] == 1
        row = conn.execute(
            "SELECT current_step, status, next_send_at "
            "FROM crmadv_drip_enrollment WHERE id = ?", (enr_id,)).fetchone()
        assert row["current_step"] == 2
        assert row["status"] == "completed"
        assert row["next_send_at"] is None

    def test_not_yet_due_untouched(self, conn, env):
        seq_id = self._seq_with_steps(conn, env, [(1, 0, None), (2, 48, None)])
        enr_id = _insert_enrollment(conn, seq_id, "CONTACT-1",
                                    next_send_at="2026-12-31T00:00:00Z")
        r = call_action(MOD.process_drip_sends, conn, ns(
            company_id=env["company_id"], limit=100,
            now="2026-01-02T00:00:00Z", db_path=None))
        assert is_ok(r)
        assert r["processed"] == 0
        row = conn.execute(
            "SELECT current_step, status, next_send_at "
            "FROM crmadv_drip_enrollment WHERE id = ?", (enr_id,)).fetchone()
        assert row["current_step"] == 0
        assert row["status"] == "active"
        assert row["next_send_at"] == "2026-12-31T00:00:00Z"

    def test_send_path_attempted_with_template(self, conn, env):
        seq_id = self._seq_with_steps(conn, env, [(1, 0, "TPL-1"), (2, 48, None)])
        lead_id = _seed_lead(conn, env["company_id"], email="drip@example.com")
        enr_id = _insert_enrollment(conn, seq_id, lead_id,
                                    next_send_at="2026-01-01T00:00:00Z")
        with patch.object(automation, "_dispatch_email",
                          return_value=(True, "outbox-1")) as m:
            r = call_action(MOD.process_drip_sends, conn, ns(
                company_id=env["company_id"], limit=100,
                now="2026-01-02T00:00:00Z", db_path=None))
        assert is_ok(r)
        assert r["sent"] == 1
        assert m.called
        # _dispatch_email(conn, to_address, template_id, company_id, db_path)
        call = m.call_args
        assert call.args[1] == "drip@example.com"
        assert call.args[2] == "TPL-1"
        row = conn.execute(
            "SELECT current_step FROM crmadv_drip_enrollment WHERE id = ?",
            (enr_id,)).fetchone()
        assert row["current_step"] == 1

    def test_no_email_skips_without_advancing(self, conn, env):
        seq_id = self._seq_with_steps(conn, env, [(1, 0, "TPL-1"), (2, 48, None)])
        # Lead exists but has no email -> recipient unresolvable.
        lead_id = _seed_lead(conn, env["company_id"], email=None)
        enr_id = _insert_enrollment(conn, seq_id, lead_id,
                                    next_send_at="2026-01-01T00:00:00Z")
        with patch.object(automation, "_dispatch_email") as m:
            r = call_action(MOD.process_drip_sends, conn, ns(
                company_id=env["company_id"], limit=100,
                now="2026-01-02T00:00:00Z", db_path=None))
        assert is_ok(r)
        assert r["skipped"] == 1
        assert r["sent"] == 0
        assert not m.called  # never attempted dispatch with no address
        row = conn.execute(
            "SELECT current_step, status, next_send_at "
            "FROM crmadv_drip_enrollment WHERE id = ?", (enr_id,)).fetchone()
        assert row["current_step"] == 0
        assert row["status"] == "active"
        assert row["next_send_at"] == "2026-01-01T00:00:00Z"

    def test_caught_up_enrollment_completes(self, conn, env):
        # current_step already at/past the step count -> nothing to send, complete.
        seq_id = self._seq_with_steps(conn, env, [(1, 0, None)])
        enr_id = _insert_enrollment(conn, seq_id, "CONTACT-1",
                                    next_send_at="2026-01-01T00:00:00Z",
                                    current_step=1)
        r = call_action(MOD.process_drip_sends, conn, ns(
            company_id=env["company_id"], limit=100,
            now="2026-01-02T00:00:00Z", db_path=None))
        assert is_ok(r)
        assert r["completed"] == 1
        assert r["sent"] == 0
        row = conn.execute(
            "SELECT status, next_send_at "
            "FROM crmadv_drip_enrollment WHERE id = ?", (enr_id,)).fetchone()
        assert row["status"] == "completed"
        assert row["next_send_at"] is None

    def test_company_scope_excludes_other_company(self, conn, env):
        seq_id = self._seq_with_steps(conn, env, [(1, 0, None), (2, 48, None)])
        enr_id = _insert_enrollment(conn, seq_id, "CONTACT-1",
                                    next_send_at="2026-01-01T00:00:00Z")
        # Scope to a different (non-existent) company -> nothing processed.
        r = call_action(MOD.process_drip_sends, conn, ns(
            company_id="some-other-company", limit=100,
            now="2026-01-02T00:00:00Z", db_path=None))
        assert is_ok(r)
        assert r["processed"] == 0
        row = conn.execute(
            "SELECT current_step FROM crmadv_drip_enrollment WHERE id = ?",
            (enr_id,)).fetchone()
        assert row["current_step"] == 0

    def test_invalid_now_errors(self, conn, env):
        r = call_action(MOD.process_drip_sends, conn, ns(
            company_id=env["company_id"], limit=100,
            now="not-a-date", db_path=None))
        assert is_error(r)


# ===========================================================================
# REPORTS DOMAIN (5 actions)
# ===========================================================================

class TestFunnelAnalysis:
    def test_funnel_empty(self, conn, env):
        r = call_action(MOD.funnel_analysis, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["total_sent"] == 0


class TestPipelineVelocity:
    def test_pipeline_velocity(self, conn, env):
        # Behavioural: two drafts sum to the reported pipeline value exactly as
        # Decimal strings; renewed/terminated rows are excluded from both the
        # counts and the value. No ledger is touched (SELECT only; gl_entry is
        # never written by this action), so no two-leg balance assertion holds.
        a = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Pipe A",
            contract_type="service", start_date=None, end_date=None,
            total_value="100000.00", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(a)
        b = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Pipe B",
            contract_type="subscription", start_date=None, end_date=None,
            total_value="25000.00", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(b)
        c = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Pipe C",
            contract_type="service", start_date=None, end_date=None,
            total_value="99999.99", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(c)
        assert is_ok(call_action(MOD.renew_contract, conn, ns(
            contract_id=c["id"], end_date="2026-12-31")))
        d = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Pipe D",
            contract_type="service", start_date=None, end_date=None,
            total_value="1.00", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(d)
        assert is_ok(call_action(MOD.terminate_contract, conn, ns(
            contract_id=d["id"])))
        # 'active' is not reachable through any owner action (add makes draft,
        # renew makes renewed, terminate makes terminated), so active stays 0.
        quoted = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Quoted Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic", limit=50, offset=0))
        assert is_ok(quoted)
        assert is_ok(call_action(MOD.set_territory_quota, conn, ns(
            territory_id=quoted["id"], company_id=env["company_id"],
            period="2026-Q1", quota_amount="50000.00")))
        unquoted = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Unquoted Terr",
            region=None, parent_territory_id=None,
            territory_type="industry", limit=50, offset=0))
        assert is_ok(unquoted)
        before = _snapshot_tables(conn)

        r = call_action(MOD.pipeline_velocity, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["draft_contracts"] == 2
        assert r["active_contracts"] == 0
        assert r["total_pipeline_value"] == "125000.00"
        assert D(str(r["total_pipeline_value"])) == D("125000.00")
        assert r["territories_with_quota"] == 1
        # Independent read-back with the same predicates the action uses.
        drafts = conn.execute(
            "SELECT COUNT(*) AS c FROM crmadv_contract "
            "WHERE company_id = ? AND contract_status = 'draft'",
            (env["company_id"],)).fetchone()["c"]
        assert drafts == r["draft_contracts"] == 2
        actives = conn.execute(
            "SELECT COUNT(*) AS c FROM crmadv_contract "
            "WHERE company_id = ? AND contract_status = 'active'",
            (env["company_id"],)).fetchone()["c"]
        assert actives == r["active_contracts"] == 0
        value_rows = conn.execute(
            "SELECT total_value FROM crmadv_contract "
            "WHERE company_id = ? AND contract_status IN ('draft','active')",
            (env["company_id"],)).fetchall()
        expected_value = sum((D(str(row["total_value"])) for row in value_rows), D("0"))
        assert expected_value == D("125000.00")
        assert D(str(r["total_pipeline_value"])) == expected_value
        with_quota = conn.execute(
            "SELECT COUNT(DISTINCT territory_id) AS c FROM crmadv_territory_quota "
            "WHERE company_id = ?", (env["company_id"],)).fetchone()["c"]
        assert with_quota == r["territories_with_quota"] == 1
        # Read-only: the report wrote nothing anywhere.
        assert _snapshot_tables(conn) == before

    def test_pipeline_velocity_refuses_without_company(self, conn, env):
        a = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Pipe Refusal",
            contract_type="service", start_date=None, end_date=None,
            total_value="10.00", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(a)
        before = _snapshot_tables(conn)

        missing = call_action(MOD.pipeline_velocity, conn, ns(company_id=None))
        assert is_error(missing)
        assert "--company-id is required" in _msg(missing)

        unknown = call_action(MOD.pipeline_velocity, conn, ns(
            company_id="does-not-exist"))
        assert is_error(unknown)
        assert "does-not-exist" in _msg(unknown)
        assert "not found" in _msg(unknown)
        # A refusal that half-writes is worse than no refusal: byte-identical.
        assert _snapshot_tables(conn) == before


class TestWinLossAnalysis:
    def test_win_loss(self, conn, env):
        # Behavioural: one renewed (win) + one terminated (loss) decide 2 of 3;
        # the draft is undecided and excluded from total_decided and the rate.
        # No ledger is touched (SELECT only; gl_entry is never written by this
        # action), so no two-leg balance assertion can hold.
        draft = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Undecided Corp",
            contract_type="service", start_date=None, end_date=None,
            total_value="10000.00", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(draft)
        won = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Won Corp",
            contract_type="subscription", start_date=None, end_date=None,
            total_value="20000.00", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(won)
        assert is_ok(call_action(MOD.renew_contract, conn, ns(
            contract_id=won["id"], end_date="2026-12-31")))
        lost = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Lost Corp",
            contract_type="service", start_date=None, end_date=None,
            total_value="30000.00", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(lost)
        assert is_ok(call_action(MOD.terminate_contract, conn, ns(
            contract_id=lost["id"])))
        # 'active' and 'expired' are not reachable through any owner action
        # (add makes draft, renew makes renewed, terminate makes terminated).
        before = _snapshot_tables(conn)

        r = call_action(MOD.win_loss_analysis, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["active_contracts"] == 0
        assert r["renewed_contracts"] == 1
        assert r["terminated_contracts"] == 1
        assert r["expired_contracts"] == 0
        assert r["total_decided"] == 2
        assert r["win_rate_pct"] == 50.0
        # Independent read-back with the same predicates the action uses.
        counts = {}
        for status in ("active", "renewed", "terminated", "expired"):
            counts[status] = conn.execute(
                "SELECT COUNT(*) AS c FROM crmadv_contract "
                "WHERE company_id = ? AND contract_status = ?",
                (env["company_id"], status)).fetchone()["c"]
        assert counts == {"active": 0, "renewed": 1, "terminated": 1, "expired": 0}
        assert r["active_contracts"] == counts["active"]
        assert r["renewed_contracts"] == counts["renewed"]
        assert r["terminated_contracts"] == counts["terminated"]
        assert r["expired_contracts"] == counts["expired"]
        assert r["total_decided"] == sum(counts.values()) == 2
        # The undecided draft is stored but decides nothing.
        undecided = conn.execute(
            "SELECT contract_status FROM crmadv_contract WHERE id = ?",
            (draft["id"],)).fetchone()["contract_status"]
        assert undecided == "draft"
        # Read-only: the report wrote nothing anywhere.
        assert _snapshot_tables(conn) == before

    def test_win_loss_refuses_without_company(self, conn, env):
        won = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Won Refusal",
            contract_type="service", start_date=None, end_date=None,
            total_value="10.00", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(won)
        before = _snapshot_tables(conn)

        missing = call_action(MOD.win_loss_analysis, conn, ns(company_id=None))
        assert is_error(missing)
        assert "--company-id is required" in _msg(missing)

        unknown = call_action(MOD.win_loss_analysis, conn, ns(
            company_id="does-not-exist"))
        assert is_error(unknown)
        assert "does-not-exist" in _msg(unknown)
        assert "not found" in _msg(unknown)
        # A refusal that half-writes is worse than no refusal: byte-identical.
        assert _snapshot_tables(conn) == before


class TestMarketingDashboard:
    def test_dashboard(self, conn, env):
        # Behavioural: every dashboard number equals an independent COUNT over
        # the rows the owner actions stored. No ledger is touched (SELECT only;
        # gl_entry is never written by this action), so no balance assertion holds.
        first = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="Dash One", subject="Hello",
            template_id=None, recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0))
        assert is_ok(first)
        second = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="Dash Two", subject="Hi",
            template_id=None, recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0))
        assert is_ok(second)
        # Zero-recipient send still flips the campaign to sent (no send seam
        # is reached, so nothing is mocked here).
        sent = call_action(MOD.send_campaign, conn, ns(
            campaign_id=first["id"], db_path=None))
        assert is_ok(sent)
        assert sent["campaign_status"] == "sent"
        active_wf = call_action(MOD.add_automation_workflow, conn, ns(
            company_id=env["company_id"], name="Dash WF Active",
            trigger_event="lead_created", conditions_json=None, actions_json=None))
        assert is_ok(active_wf)
        assert is_ok(call_action(MOD.activate_workflow, conn, ns(
            workflow_id=active_wf["id"])))
        idle_wf = call_action(MOD.add_automation_workflow, conn, ns(
            company_id=env["company_id"], name="Dash WF Idle",
            trigger_event=None, conditions_json=None, actions_json=None))
        assert is_ok(idle_wf)
        nurture = call_action(MOD.add_nurture_sequence, conn, ns(
            company_id=env["company_id"], name="Dash Nurture",
            description=None, steps_json=None))
        assert is_ok(nurture)
        # No owner action moves a nurture sequence to active (add makes draft),
        # so active_nurture_sequences stays 0 while the row exists as draft.
        terr = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Dash Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic", limit=50, offset=0))
        assert is_ok(terr)
        staying = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Dash Draft",
            contract_type="service", start_date=None, end_date=None,
            total_value="10.00", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(staying)
        counting = call_action(MOD.add_contract, conn, ns(
            company_id=env["company_id"], customer_name="Dash Won",
            contract_type="service", start_date=None, end_date=None,
            total_value="20.00", annual_value=None, auto_renew=None,
            renewal_terms=None))
        assert is_ok(counting)
        assert is_ok(call_action(MOD.renew_contract, conn, ns(
            contract_id=counting["id"], end_date=None)))
        before = _snapshot_tables(conn)

        r = call_action(MOD.marketing_dashboard, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["total_campaigns"] == 2
        assert r["sent_campaigns"] == 1
        assert r["active_workflows"] == 1
        assert r["active_nurture_sequences"] == 0
        assert r["active_territories"] == 1
        assert r["active_contracts"] == 1
        # Independent read-back with the same predicates the action uses.
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM crmadv_email_campaign WHERE company_id = ?",
            (env["company_id"],)).fetchone()["c"] == r["total_campaigns"] == 2
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM crmadv_email_campaign "
            "WHERE company_id = ? AND campaign_status = 'sent'",
            (env["company_id"],)).fetchone()["c"] == r["sent_campaigns"] == 1
        assert conn.execute(
            "SELECT campaign_status FROM crmadv_email_campaign WHERE id = ?",
            (first["id"],)).fetchone()["campaign_status"] == "sent"
        assert conn.execute(
            "SELECT campaign_status FROM crmadv_email_campaign WHERE id = ?",
            (second["id"],)).fetchone()["campaign_status"] == "draft"
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM crmadv_automation_workflow "
            "WHERE company_id = ? AND workflow_status = 'active'",
            (env["company_id"],)).fetchone()["c"] == r["active_workflows"] == 1
        assert conn.execute(
            "SELECT sequence_status FROM crmadv_nurture_sequence WHERE id = ?",
            (nurture["id"],)).fetchone()["sequence_status"] == "draft"
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM crmadv_territory "
            "WHERE company_id = ? AND territory_status = 'active'",
            (env["company_id"],)).fetchone()["c"] == r["active_territories"] == 1
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM crmadv_contract "
            "WHERE company_id = ? AND contract_status IN ('active','renewed')",
            (env["company_id"],)).fetchone()["c"] == r["active_contracts"] == 1
        # Read-only: the dashboard wrote nothing anywhere.
        assert _snapshot_tables(conn) == before

    def test_dashboard_refuses_without_company(self, conn, env):
        terr = call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Dash Refusal Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic", limit=50, offset=0))
        assert is_ok(terr)
        before = _snapshot_tables(conn)

        missing = call_action(MOD.marketing_dashboard, conn, ns(company_id=None))
        assert is_error(missing)
        assert "--company-id is required" in _msg(missing)

        unknown = call_action(MOD.marketing_dashboard, conn, ns(
            company_id="does-not-exist"))
        assert is_error(unknown)
        assert "does-not-exist" in _msg(unknown)
        assert "not found" in _msg(unknown)
        # A refusal that half-writes is worse than no refusal: byte-identical.
        assert _snapshot_tables(conn) == before


class TestStatusAction:
    def test_status(self, conn, env):
        r = call_action(MOD.status, conn, ns(company_id=None))
        assert is_ok(r)
        assert r["skill"] == "erpclaw-crm-adv"
        assert "record_counts" in r
