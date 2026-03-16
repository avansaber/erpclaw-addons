"""L1 pytest tests for erpclaw-crm (18 actions).

Tests cover: add/update/get/list leads, add/update/get/list opportunities,
  convert-lead-to-opportunity, add-opportunity, mark-opportunity-won/lost,
  add-campaign, list-campaigns, add-activity, list-activities,
  pipeline-report, status.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from crm_helpers import call_action, ns, is_ok, is_error, load_db_query, seed_customer

MOD = load_db_query()


# ===========================================================================
# Lead Management
# ===========================================================================

class TestAddLead:
    def test_add_lead_basic(self, conn, env):
        r = call_action(MOD.add_lead, conn, ns(
            lead_name="Alice Smith", company_name="Acme Corp",
            email="alice@acme.com", phone="555-0100", source="website",
            territory=None, industry=None, assigned_to=None, notes=None,
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        lead = r["lead"]
        assert lead["lead_name"] == "Alice Smith"
        assert lead["email"] == "alice@acme.com"
        assert lead["status"] == "new"
        assert lead["naming_series"].startswith("LEAD-")

    def test_add_lead_missing_name(self, conn, env):
        r = call_action(MOD.add_lead, conn, ns(
            lead_name=None, company_name=None, email=None, phone=None,
            source=None, territory=None, industry=None, assigned_to=None,
            notes=None, company_id=env["company_id"],
        ))
        assert is_error(r)

    def test_add_lead_invalid_source(self, conn, env):
        r = call_action(MOD.add_lead, conn, ns(
            lead_name="Bob", company_name=None, email=None, phone=None,
            source="invalid_source", territory=None, industry=None,
            assigned_to=None, notes=None, company_id=env["company_id"],
        ))
        assert is_error(r)

    def test_add_lead_invalid_email(self, conn, env):
        r = call_action(MOD.add_lead, conn, ns(
            lead_name="Bob", company_name=None, email="not-an-email",
            phone=None, source=None, territory=None, industry=None,
            assigned_to=None, notes=None, company_id=env["company_id"],
        ))
        assert is_error(r)


class TestUpdateLead:
    def test_update_lead_status(self, conn, env):
        r = call_action(MOD.add_lead, conn, ns(
            lead_name="Update Target", company_name=None, email=None,
            phone=None, source=None, territory=None, industry=None,
            assigned_to=None, notes=None, company_id=env["company_id"],
        ))
        assert is_ok(r)
        lead_id = r["lead"]["id"]

        r2 = call_action(MOD.update_lead, conn, ns(
            lead_id=lead_id, lead_name=None, company_name=None, email=None,
            phone=None, source=None, territory=None, industry=None,
            status="contacted", assigned_to="Sales Rep", notes=None,
        ))
        assert is_ok(r2)
        assert r2["lead"]["status"] == "contacted"

    def test_update_lead_missing_id(self, conn, env):
        r = call_action(MOD.update_lead, conn, ns(
            lead_id=None, lead_name="x", company_name=None, email=None,
            phone=None, source=None, territory=None, industry=None,
            status=None, assigned_to=None, notes=None,
        ))
        assert is_error(r)

    def test_update_lead_no_fields(self, conn, env):
        r = call_action(MOD.add_lead, conn, ns(
            lead_name="No Fields", company_name=None, email=None, phone=None,
            source=None, territory=None, industry=None, assigned_to=None,
            notes=None, company_id=env["company_id"],
        ))
        lead_id = r["lead"]["id"]

        r2 = call_action(MOD.update_lead, conn, ns(
            lead_id=lead_id, lead_name=None, company_name=None, email=None,
            phone=None, source=None, territory=None, industry=None,
            status=None, assigned_to=None, notes=None,
        ))
        assert is_error(r2)


class TestGetLead:
    def test_get_lead(self, conn, env):
        r = call_action(MOD.add_lead, conn, ns(
            lead_name="Get Me", company_name=None, email=None, phone=None,
            source=None, territory=None, industry=None, assigned_to=None,
            notes=None, company_id=env["company_id"],
        ))
        lead_id = r["lead"]["id"]

        r2 = call_action(MOD.get_lead, conn, ns(lead_id=lead_id))
        assert is_ok(r2)
        assert r2["lead"]["lead_name"] == "Get Me"
        assert "activities" in r2["lead"]
        assert "campaigns" in r2["lead"]


class TestListLeads:
    def test_list_leads_empty(self, conn, env):
        r = call_action(MOD.list_leads, conn, ns(
            status=None, source=None, search=None, limit=20, offset=0,
        ))
        assert is_ok(r)
        assert r["total"] == 0

    def test_list_leads_with_data(self, conn, env):
        call_action(MOD.add_lead, conn, ns(
            lead_name="L1", company_name=None, email=None, phone=None,
            source="website", territory=None, industry=None, assigned_to=None,
            notes=None, company_id=env["company_id"],
        ))
        call_action(MOD.add_lead, conn, ns(
            lead_name="L2", company_name=None, email=None, phone=None,
            source="referral", territory=None, industry=None, assigned_to=None,
            notes=None, company_id=env["company_id"],
        ))

        r = call_action(MOD.list_leads, conn, ns(
            status=None, source=None, search=None, limit=20, offset=0,
        ))
        assert is_ok(r)
        assert r["total"] == 2

    def test_list_leads_filter_source(self, conn, env):
        call_action(MOD.add_lead, conn, ns(
            lead_name="Web Lead", company_name=None, email=None, phone=None,
            source="website", territory=None, industry=None, assigned_to=None,
            notes=None, company_id=env["company_id"],
        ))

        r = call_action(MOD.list_leads, conn, ns(
            status=None, source="website", search=None, limit=20, offset=0,
        ))
        assert is_ok(r)
        assert r["total"] == 1


# ===========================================================================
# Opportunity Management
# ===========================================================================

class TestAddOpportunity:
    def test_add_opportunity_basic(self, conn, env):
        r = call_action(MOD.add_opportunity, conn, ns(
            opportunity_name="Big Deal", lead_id=None, customer_id=None,
            opportunity_type="sales", expected_revenue="50000",
            probability="70", expected_closing_date="2026-06-01",
            assigned_to=None, company_id=env["company_id"],
        ))
        assert is_ok(r)
        opp = r["opportunity"]
        assert opp["opportunity_name"] == "Big Deal"
        assert opp["stage"] == "new"
        assert opp["probability"] == "70"
        assert opp["expected_revenue"] == "50000"
        assert opp["weighted_revenue"] == "35000.00"

    def test_add_opportunity_missing_name(self, conn, env):
        r = call_action(MOD.add_opportunity, conn, ns(
            opportunity_name=None, lead_id=None, customer_id=None,
            opportunity_type=None, expected_revenue=None, probability=None,
            expected_closing_date=None, assigned_to=None,
            company_id=env["company_id"],
        ))
        assert is_error(r)


class TestConvertLeadToOpportunity:
    def test_convert_lead(self, conn, env):
        r = call_action(MOD.add_lead, conn, ns(
            lead_name="Convert Me", company_name=None, email=None, phone=None,
            source="referral", territory=None, industry=None, assigned_to=None,
            notes=None, company_id=env["company_id"],
        ))
        lead_id = r["lead"]["id"]

        r2 = call_action(MOD.convert_lead_to_opportunity, conn, ns(
            lead_id=lead_id, opportunity_name="Converted Opp",
            expected_revenue="10000", probability="60",
            opportunity_type="sales", expected_closing_date="2026-07-01",
            company_id=env["company_id"],
        ))
        assert is_ok(r2)
        assert r2["lead_status"] == "converted"
        assert r2["opportunity"]["stage"] == "new"
        assert r2["opportunity"]["weighted_revenue"] == "6000.00"

    def test_convert_lead_already_converted(self, conn, env):
        r = call_action(MOD.add_lead, conn, ns(
            lead_name="Convert Once", company_name=None, email=None,
            phone=None, source=None, territory=None, industry=None,
            assigned_to=None, notes=None, company_id=env["company_id"],
        ))
        lead_id = r["lead"]["id"]

        call_action(MOD.convert_lead_to_opportunity, conn, ns(
            lead_id=lead_id, opportunity_name="First Conv",
            expected_revenue="0", probability="50",
            opportunity_type="sales", expected_closing_date=None,
            company_id=env["company_id"],
        ))

        r2 = call_action(MOD.convert_lead_to_opportunity, conn, ns(
            lead_id=lead_id, opportunity_name="Second Conv",
            expected_revenue="0", probability="50",
            opportunity_type="sales", expected_closing_date=None,
            company_id=env["company_id"],
        ))
        assert is_error(r2)


class TestUpdateOpportunity:
    def test_update_opportunity_stage(self, conn, env):
        r = call_action(MOD.add_opportunity, conn, ns(
            opportunity_name="Updatable Opp", lead_id=None, customer_id=None,
            opportunity_type="sales", expected_revenue="20000",
            probability="50", expected_closing_date=None, assigned_to=None,
            company_id=env["company_id"],
        ))
        opp_id = r["opportunity"]["id"]

        r2 = call_action(MOD.update_opportunity, conn, ns(
            opportunity_id=opp_id, opportunity_name=None,
            stage="qualified", probability="80", expected_revenue=None,
            expected_closing_date=None, assigned_to=None,
            next_follow_up_date=None, customer_id=None,
        ))
        assert is_ok(r2)
        assert r2["opportunity"]["stage"] == "qualified"
        assert r2["opportunity"]["probability"] == "80"

    def test_update_opportunity_terminal_blocked(self, conn, env):
        r = call_action(MOD.add_opportunity, conn, ns(
            opportunity_name="Terminal Test", lead_id=None, customer_id=None,
            opportunity_type="sales", expected_revenue="5000",
            probability="50", expected_closing_date=None, assigned_to=None,
            company_id=env["company_id"],
        ))
        opp_id = r["opportunity"]["id"]

        # Cannot set won/lost via update-opportunity
        r2 = call_action(MOD.update_opportunity, conn, ns(
            opportunity_id=opp_id, opportunity_name=None,
            stage="won", probability=None, expected_revenue=None,
            expected_closing_date=None, assigned_to=None,
            next_follow_up_date=None, customer_id=None,
        ))
        assert is_error(r2)


class TestMarkOpportunityWonLost:
    def test_mark_won(self, conn, env):
        r = call_action(MOD.add_opportunity, conn, ns(
            opportunity_name="Win This", lead_id=None, customer_id=None,
            opportunity_type="sales", expected_revenue="30000",
            probability="50", expected_closing_date=None, assigned_to=None,
            company_id=env["company_id"],
        ))
        opp_id = r["opportunity"]["id"]

        r2 = call_action(MOD.mark_opportunity_won, conn, ns(
            opportunity_id=opp_id,
        ))
        assert is_ok(r2)
        assert r2["opportunity"]["stage"] == "won"
        assert r2["opportunity"]["probability"] == "100"
        assert r2["opportunity"]["weighted_revenue"] == "30000"

    def test_mark_lost(self, conn, env):
        r = call_action(MOD.add_opportunity, conn, ns(
            opportunity_name="Lose This", lead_id=None, customer_id=None,
            opportunity_type="sales", expected_revenue="10000",
            probability="50", expected_closing_date=None, assigned_to=None,
            company_id=env["company_id"],
        ))
        opp_id = r["opportunity"]["id"]

        r2 = call_action(MOD.mark_opportunity_lost, conn, ns(
            opportunity_id=opp_id, lost_reason="Budget constraints",
        ))
        assert is_ok(r2)
        assert r2["opportunity"]["stage"] == "lost"
        assert r2["opportunity"]["probability"] == "0"
        assert r2["opportunity"]["weighted_revenue"] == "0"

    def test_mark_won_already_terminal(self, conn, env):
        r = call_action(MOD.add_opportunity, conn, ns(
            opportunity_name="Already Terminal", lead_id=None, customer_id=None,
            opportunity_type="sales", expected_revenue="5000",
            probability="50", expected_closing_date=None, assigned_to=None,
            company_id=env["company_id"],
        ))
        opp_id = r["opportunity"]["id"]
        call_action(MOD.mark_opportunity_won, conn, ns(opportunity_id=opp_id))

        r2 = call_action(MOD.mark_opportunity_won, conn, ns(opportunity_id=opp_id))
        assert is_error(r2)

    def test_mark_lost_missing_reason(self, conn, env):
        r = call_action(MOD.add_opportunity, conn, ns(
            opportunity_name="No Reason", lead_id=None, customer_id=None,
            opportunity_type="sales", expected_revenue="5000",
            probability="50", expected_closing_date=None, assigned_to=None,
            company_id=env["company_id"],
        ))
        opp_id = r["opportunity"]["id"]

        r2 = call_action(MOD.mark_opportunity_lost, conn, ns(
            opportunity_id=opp_id, lost_reason=None,
        ))
        assert is_error(r2)


# ===========================================================================
# Campaign Management
# ===========================================================================

class TestCampaign:
    def test_add_campaign(self, conn, env):
        r = call_action(MOD.add_campaign, conn, ns(
            name="Spring Campaign", campaign_type="email",
            budget="5000", start_date="2026-03-01", end_date="2026-04-01",
            description="Spring promo", lead_id=None,
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["campaign"]["name"] == "Spring Campaign"
        assert r["campaign"]["status"] == "planned"

    def test_add_campaign_with_lead(self, conn, env):
        lead_r = call_action(MOD.add_lead, conn, ns(
            lead_name="Campaign Lead", company_name=None, email=None,
            phone=None, source=None, territory=None, industry=None,
            assigned_to=None, notes=None, company_id=env["company_id"],
        ))
        lead_id = lead_r["lead"]["id"]

        r = call_action(MOD.add_campaign, conn, ns(
            name="Lead-linked Campaign", campaign_type="referral",
            budget="1000", start_date=None, end_date=None,
            description=None, lead_id=lead_id,
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r.get("lead_linked") == lead_id

    def test_list_campaigns(self, conn, env):
        call_action(MOD.add_campaign, conn, ns(
            name="C1", campaign_type="email", budget="100",
            start_date=None, end_date=None, description=None, lead_id=None,
            company_id=env["company_id"],
        ))
        r = call_action(MOD.list_campaigns, conn, ns(
            status=None, limit=20, offset=0,
        ))
        assert is_ok(r)
        assert r["total"] >= 1


# ===========================================================================
# Activity Management
# ===========================================================================

class TestActivity:
    def test_add_activity(self, conn, env):
        lead_r = call_action(MOD.add_lead, conn, ns(
            lead_name="Activity Lead", company_name=None, email=None,
            phone=None, source=None, territory=None, industry=None,
            assigned_to=None, notes=None, company_id=env["company_id"],
        ))
        lead_id = lead_r["lead"]["id"]

        r = call_action(MOD.add_activity, conn, ns(
            activity_type="call", subject="Initial call",
            activity_date="2026-03-11", description="Called prospect",
            lead_id=lead_id, opportunity_id=None, customer_id=None,
            created_by="tester", next_action_date="2026-03-18",
        ))
        assert is_ok(r)
        assert r["activity"]["activity_type"] == "call"
        assert r["activity"]["lead_id"] == lead_id

    def test_add_activity_no_reference(self, conn, env):
        r = call_action(MOD.add_activity, conn, ns(
            activity_type="call", subject="Orphan call",
            activity_date="2026-03-11", description=None,
            lead_id=None, opportunity_id=None, customer_id=None,
            created_by=None, next_action_date=None,
        ))
        assert is_error(r)

    def test_list_activities_by_lead(self, conn, env):
        lead_r = call_action(MOD.add_lead, conn, ns(
            lead_name="Activity List Lead", company_name=None, email=None,
            phone=None, source=None, territory=None, industry=None,
            assigned_to=None, notes=None, company_id=env["company_id"],
        ))
        lead_id = lead_r["lead"]["id"]

        call_action(MOD.add_activity, conn, ns(
            activity_type="email", subject="Follow up email",
            activity_date="2026-03-10", description=None,
            lead_id=lead_id, opportunity_id=None, customer_id=None,
            created_by=None, next_action_date=None,
        ))

        r = call_action(MOD.list_activities, conn, ns(
            lead_id=lead_id, opportunity_id=None, activity_type=None,
            limit=20, offset=0,
        ))
        assert is_ok(r)
        assert r["total"] == 1


# ===========================================================================
# Reports
# ===========================================================================

class TestPipelineReport:
    def test_pipeline_report_empty(self, conn, env):
        r = call_action(MOD.pipeline_report, conn, ns(
            stage=None, from_date=None, to_date=None,
        ))
        assert is_ok(r)
        assert r["pipeline"]["total_opportunities"] == 0

    def test_pipeline_report_with_data(self, conn, env):
        call_action(MOD.add_opportunity, conn, ns(
            opportunity_name="Pipeline Opp 1", lead_id=None, customer_id=None,
            opportunity_type="sales", expected_revenue="10000",
            probability="50", expected_closing_date=None, assigned_to=None,
            company_id=env["company_id"],
        ))
        call_action(MOD.add_opportunity, conn, ns(
            opportunity_name="Pipeline Opp 2", lead_id=None, customer_id=None,
            opportunity_type="sales", expected_revenue="20000",
            probability="80", expected_closing_date=None, assigned_to=None,
            company_id=env["company_id"],
        ))

        r = call_action(MOD.pipeline_report, conn, ns(
            stage=None, from_date=None, to_date=None,
        ))
        assert is_ok(r)
        assert r["pipeline"]["total_opportunities"] == 2


class TestStatus:
    def test_status_action(self, conn, env):
        r = call_action(MOD.status, conn, ns())
        assert is_ok(r)
        assert "crm_status" in r
        assert "leads" in r["crm_status"]
        assert "opportunities" in r["crm_status"]
