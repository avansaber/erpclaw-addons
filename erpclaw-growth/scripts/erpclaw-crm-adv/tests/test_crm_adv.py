"""L1 pytest tests for erpclaw-crm-adv (47 actions across 5 domains).

Domains: campaigns (12), territories (10), contracts (10), automation (10), reports (5).
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from crm_adv_helpers import call_action, ns, is_ok, is_error, load_db_query

MOD = load_db_query()


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
            subject=None, template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        camp_id = r["id"]

        r2 = call_action(MOD.send_campaign, conn, ns(campaign_id=camp_id))
        assert is_ok(r2)
        assert r2["campaign_status"] == "sent"

    def test_send_already_sent(self, conn, env):
        r = call_action(MOD.add_email_campaign, conn, ns(
            company_id=env["company_id"], name="Already Sent",
            subject=None, template_id=None,
            recipient_list_id=None, scheduled_date=None,
            limit=50, offset=0,
        ))
        camp_id = r["id"]
        call_action(MOD.send_campaign, conn, ns(campaign_id=camp_id))

        r2 = call_action(MOD.send_campaign, conn, ns(campaign_id=camp_id))
        assert is_error(r2)


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
