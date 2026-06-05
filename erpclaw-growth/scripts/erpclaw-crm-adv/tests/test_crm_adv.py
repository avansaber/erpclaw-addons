"""L1 pytest tests for erpclaw-crm-adv (48 actions across 5 domains).

Domains: campaigns (12), territories (10), contracts (10), automation (11), reports (5).
"""
import json
import os
import sys
import uuid
from datetime import datetime, timedelta
from unittest.mock import patch

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from crm_adv_helpers import call_action, ns, is_ok, is_error, load_db_query

MOD = load_db_query()

# load_db_query() puts the module dir on sys.path, so the domain module that
# owns process-drip-sends is importable for patching its cross-module send seam.
import automation  # noqa: E402
# campaigns owns send-campaign; imported here so its M8-C send seam is patchable.
import campaigns  # noqa: E402


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
        call_action(MOD.add_territory, conn, ns(
            company_id=env["company_id"], name="Compare Terr",
            region=None, parent_territory_id=None,
            territory_type="geographic",
            limit=50, offset=0,
        ))
        r = call_action(MOD.territory_comparison_report, conn, ns(
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)


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
        r = call_action(MOD.pipeline_velocity, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "draft_contracts" in r
        assert "active_contracts" in r


class TestWinLossAnalysis:
    def test_win_loss(self, conn, env):
        r = call_action(MOD.win_loss_analysis, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "win_rate_pct" in r


class TestMarketingDashboard:
    def test_dashboard(self, conn, env):
        r = call_action(MOD.marketing_dashboard, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "total_campaigns" in r
        assert "active_territories" in r
        assert "active_contracts" in r


class TestStatusAction:
    def test_status(self, conn, env):
        r = call_action(MOD.status, conn, ns(company_id=None))
        assert is_ok(r)
        assert r["skill"] == "erpclaw-crm-adv"
        assert "record_counts" in r
