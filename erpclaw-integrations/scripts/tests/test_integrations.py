"""L1 tests for ERPClaw Integrations -- connectors-v2 domains.

Covers booking, delivery, realestate, financial, productivity, and cross-domain reports.

Actions tested:
  Booking:      integration-add-booking-connector, integration-configure-booking-sync,
                integration-sync-reservations, integration-push-rates,
                integration-push-availability, integration-list-booking-syncs,
                integration-booking-revenue-report, integration-booking-channel-report
  Delivery:     integration-add-delivery-connector, integration-configure-delivery-sync,
                integration-ingest-orders, integration-sync-menu,
                integration-update-order-status, integration-list-delivery-syncs,
                integration-delivery-revenue-report, integration-delivery-platform-comparison
  Real Estate:  integration-add-realestate-connector, integration-sync-listings,
                integration-capture-leads, integration-list-realestate-syncs,
                integration-listing-performance-report, integration-lead-source-report
  Financial:    integration-add-financial-connector, integration-sync-bank-feeds,
                integration-sync-transactions, integration-send-sms,
                integration-send-email-delivery, integration-list-financial-syncs,
                integration-bank-feed-reconciliation-report,
                integration-communication-delivery-report
  Productivity: integration-add-productivity-connector, integration-sync-calendar,
                integration-sync-contacts, integration-sync-files,
                integration-list-productivity-syncs, integration-sync-status-report
  Reports:      integration-connector-usage-report, integration-sync-volume-report,
                integration-error-rate-report
"""
import pytest
from integration_helpers import call_action, ns, is_error, is_ok, load_db_query

mod = load_db_query()


# =============================================================================
# Booking domain
# =============================================================================

class TestBookingConnector:
    def test_add_booking_connector(self, conn, env):
        result = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"],
            platform="airbnb",
            property_id="PROP-001",
        ))
        assert is_ok(result), result
        assert result["platform"] == "airbnb"
        assert result["connector_status"] == "inactive"
        assert "id" in result
        assert "naming_series" in result

    def test_missing_platform_fails(self, conn, env):
        result = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_error(result)

    def test_invalid_platform_fails(self, conn, env):
        result = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"],
            platform="invalid",
        ))
        assert is_error(result)

    def test_configure_booking_sync(self, conn, env):
        add = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"],
            platform="booking_com",
        ))
        result = call_action(mod.integration_configure_booking_sync, conn, ns(
            connector_id=add["id"],
            sync_reservations="1",
            connector_status="active",
        ))
        assert is_ok(result), result
        assert "sync_reservations" in result["updated_fields"]
        assert "connector_status" in result["updated_fields"]

    def test_sync_reservations(self, conn, env):
        add = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"],
            platform="expedia",
        ))
        result = call_action(mod.integration_sync_reservations, conn, ns(
            connector_id=add["id"],
            records_synced="15",
            errors="0",
        ))
        assert is_ok(result), result
        assert result["records_synced"] == 15
        assert result["sync_status"] == "completed"

    def test_sync_reservations_with_errors(self, conn, env):
        add = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"],
            platform="expedia",
        ))
        result = call_action(mod.integration_sync_reservations, conn, ns(
            connector_id=add["id"],
            records_synced="10",
            errors="3",
        ))
        assert is_ok(result), result
        assert result["sync_status"] == "failed"

    def test_push_rates(self, conn, env):
        add = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"],
            platform="vrbo",
        ))
        result = call_action(mod.integration_push_rates, conn, ns(
            connector_id=add["id"],
            records_synced="5",
        ))
        assert is_ok(result), result
        assert result["sync_status"] == "completed"

    def test_push_availability(self, conn, env):
        add = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"],
            platform="airbnb",
        ))
        result = call_action(mod.integration_push_availability, conn, ns(
            connector_id=add["id"],
            records_synced="30",
        ))
        assert is_ok(result), result
        assert result["sync_status"] == "completed"

    def test_list_booking_syncs(self, conn, env):
        add = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"],
            platform="airbnb",
        ))
        call_action(mod.integration_sync_reservations, conn, ns(
            connector_id=add["id"],
            records_synced="10",
        ))
        result = call_action(mod.integration_list_booking_syncs, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_booking_revenue_report(self, conn, env):
        add = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"],
            platform="booking_com",
        ))
        call_action(mod.integration_sync_reservations, conn, ns(
            connector_id=add["id"],
            records_synced="20",
        ))
        result = call_action(mod.integration_booking_revenue_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "rows" in result

    def test_booking_channel_report(self, conn, env):
        call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"],
            platform="airbnb",
        ))
        result = call_action(mod.integration_booking_channel_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "rows" in result


# =============================================================================
# Delivery domain
# =============================================================================

class TestDeliveryConnector:
    def test_add_delivery_connector(self, conn, env):
        result = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"],
            platform="doordash",
            store_id="STORE-001",
        ))
        assert is_ok(result), result
        assert result["platform"] == "doordash"
        assert result["connector_status"] == "inactive"

    def test_invalid_platform_fails(self, conn, env):
        result = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"],
            platform="invalid",
        ))
        assert is_error(result)

    def test_configure_delivery_sync(self, conn, env):
        add = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"],
            platform="ubereats",
        ))
        result = call_action(mod.integration_configure_delivery_sync, conn, ns(
            connector_id=add["id"],
            auto_accept="1",
            connector_status="active",
        ))
        assert is_ok(result), result
        assert "auto_accept" in result["updated_fields"]

    def test_ingest_orders(self, conn, env):
        add = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"],
            platform="grubhub",
        ))
        result = call_action(mod.integration_ingest_orders, conn, ns(
            connector_id=add["id"],
            external_order_id="GH-ORD-001",
            total_amount="25.99",
            commission="3.90",
        ))
        assert is_ok(result), result
        assert result["order_status"] == "received"
        assert result["total_amount"] == "25.99"
        assert result["net_amount"] == "22.09"

    def test_sync_menu(self, conn, env):
        add = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"],
            platform="doordash",
        ))
        result = call_action(mod.integration_sync_menu, conn, ns(
            connector_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["sync_type"] == "menu"

    def test_update_order_status(self, conn, env):
        add = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"],
            platform="ubereats",
        ))
        order = call_action(mod.integration_ingest_orders, conn, ns(
            connector_id=add["id"],
            total_amount="15.00",
            commission="2.25",
        ))
        result = call_action(mod.integration_update_order_status, conn, ns(
            order_id=order["id"],
            order_status="confirmed",
        ))
        assert is_ok(result), result
        assert result["order_status"] == "confirmed"

    def test_update_order_invalid_status_fails(self, conn, env):
        add = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"],
            platform="ubereats",
        ))
        order = call_action(mod.integration_ingest_orders, conn, ns(
            connector_id=add["id"],
            total_amount="15.00",
            commission="2.25",
        ))
        result = call_action(mod.integration_update_order_status, conn, ns(
            order_id=order["id"],
            order_status="invalid_status",
        ))
        assert is_error(result)

    def test_list_delivery_syncs(self, conn, env):
        add = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"],
            platform="doordash",
        ))
        call_action(mod.integration_ingest_orders, conn, ns(
            connector_id=add["id"],
            total_amount="10.00",
        ))
        result = call_action(mod.integration_list_delivery_syncs, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_delivery_revenue_report(self, conn, env):
        add = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"],
            platform="doordash",
        ))
        call_action(mod.integration_ingest_orders, conn, ns(
            connector_id=add["id"],
            total_amount="50.00",
            commission="7.50",
        ))
        result = call_action(mod.integration_delivery_revenue_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "rows" in result

    def test_delivery_platform_comparison(self, conn, env):
        call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"],
            platform="doordash",
        ))
        result = call_action(mod.integration_delivery_platform_comparison, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "rows" in result


# =============================================================================
# Real Estate domain
# =============================================================================

class TestRealEstateConnector:
    def test_add_realestate_connector(self, conn, env):
        result = call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=env["company_id"],
            platform="zillow",
            agent_id="AGENT-001",
        ))
        assert is_ok(result), result
        assert result["platform"] == "zillow"
        assert result["connector_status"] == "inactive"

    def test_invalid_platform_fails(self, conn, env):
        result = call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=env["company_id"],
            platform="invalid",
        ))
        assert is_error(result)

    def test_sync_listings(self, conn, env):
        add = call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=env["company_id"],
            platform="mls",
        ))
        result = call_action(mod.integration_sync_listings, conn, ns(
            connector_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["sync_type"] == "listings"

    def test_capture_leads(self, conn, env):
        add = call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=env["company_id"],
            platform="realtor_com",
        ))
        result = call_action(mod.integration_capture_leads, conn, ns(
            connector_id=add["id"],
            contact_name="Jane Doe",
            contact_email="jane@example.com",
            contact_phone="555-0100",
            property_ref="PROP-100",
            inquiry="Interested in 3BR listing",
            lead_source="web_form",
        ))
        assert is_ok(result), result
        assert result["contact_name"] == "Jane Doe"
        assert result["lead_status"] == "new"

    def test_capture_leads_missing_contact_fails(self, conn, env):
        add = call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=env["company_id"],
            platform="trulia",
        ))
        result = call_action(mod.integration_capture_leads, conn, ns(
            connector_id=add["id"],
            contact_email="nope@example.com",
        ))
        assert is_error(result)

    def test_list_realestate_syncs(self, conn, env):
        call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=env["company_id"],
            platform="zillow",
        ))
        result = call_action(mod.integration_list_realestate_syncs, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_listing_performance_report(self, conn, env):
        add = call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=env["company_id"],
            platform="zillow",
        ))
        call_action(mod.integration_capture_leads, conn, ns(
            connector_id=add["id"],
            contact_name="Test Lead",
        ))
        result = call_action(mod.integration_listing_performance_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "rows" in result

    def test_lead_source_report(self, conn, env):
        add = call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=env["company_id"],
            platform="mls",
        ))
        call_action(mod.integration_capture_leads, conn, ns(
            connector_id=add["id"],
            contact_name="Report Lead",
        ))
        result = call_action(mod.integration_lead_source_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "rows" in result


# =============================================================================
# Financial domain
# =============================================================================

class TestFinancialConnector:
    def test_add_financial_connector_plaid(self, conn, env):
        result = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="plaid",
            account_ref="ACC-001",
        ))
        assert is_ok(result), result
        assert result["platform"] == "plaid"
        assert result["connector_status"] == "inactive"

    def test_add_financial_connector_twilio(self, conn, env):
        result = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="twilio",
        ))
        assert is_ok(result), result
        assert result["platform"] == "twilio"

    def test_invalid_platform_fails(self, conn, env):
        result = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="invalid",
        ))
        assert is_error(result)

    def test_sync_bank_feeds(self, conn, env):
        add = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="plaid",
        ))
        result = call_action(mod.integration_sync_bank_feeds, conn, ns(
            connector_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["sync_type"] == "bank_feeds"

    def test_sync_bank_feeds_non_plaid_fails(self, conn, env):
        add = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="twilio",
        ))
        result = call_action(mod.integration_sync_bank_feeds, conn, ns(
            connector_id=add["id"],
        ))
        assert is_error(result)

    def test_sync_transactions(self, conn, env):
        add = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="plaid",
        ))
        result = call_action(mod.integration_sync_transactions, conn, ns(
            connector_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["sync_type"] == "transactions"

    def test_send_sms(self, conn, env):
        add = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="twilio",
        ))
        result = call_action(mod.integration_send_sms, conn, ns(
            connector_id=add["id"],
            recipient="+15551234567",
            message_body="Test message",
        ))
        assert is_ok(result), result
        assert result["recipient"] == "+15551234567"
        assert result["message_type"] == "sms"

    def test_send_sms_non_twilio_fails(self, conn, env):
        add = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="plaid",
        ))
        result = call_action(mod.integration_send_sms, conn, ns(
            connector_id=add["id"],
            recipient="+15551234567",
            message_body="Test",
        ))
        assert is_error(result)

    def test_send_email_delivery(self, conn, env):
        add = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="sendgrid",
        ))
        result = call_action(mod.integration_send_email_delivery, conn, ns(
            connector_id=add["id"],
            recipient="user@example.com",
            subject="Invoice Ready",
        ))
        assert is_ok(result), result
        assert result["recipient"] == "user@example.com"
        assert result["message_type"] == "email"

    def test_send_email_non_email_platform_fails(self, conn, env):
        add = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="plaid",
        ))
        result = call_action(mod.integration_send_email_delivery, conn, ns(
            connector_id=add["id"],
            recipient="user@example.com",
            subject="Test",
        ))
        assert is_error(result)

    def test_list_financial_syncs(self, conn, env):
        call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="plaid",
        ))
        result = call_action(mod.integration_list_financial_syncs, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_bank_feed_reconciliation_report(self, conn, env):
        call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="plaid",
        ))
        result = call_action(mod.integration_bank_feed_reconciliation_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "rows" in result

    def test_communication_delivery_report(self, conn, env):
        call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"],
            platform="twilio",
        ))
        result = call_action(mod.integration_communication_delivery_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "rows" in result


# =============================================================================
# Productivity domain
# =============================================================================

class TestProductivityConnector:
    def test_add_productivity_connector(self, conn, env):
        result = call_action(mod.integration_add_productivity_connector, conn, ns(
            company_id=env["company_id"],
            platform="google_workspace",
            workspace_id="WS-001",
        ))
        assert is_ok(result), result
        assert result["platform"] == "google_workspace"
        assert result["connector_status"] == "inactive"

    def test_invalid_platform_fails(self, conn, env):
        result = call_action(mod.integration_add_productivity_connector, conn, ns(
            company_id=env["company_id"],
            platform="invalid",
        ))
        assert is_error(result)

    def test_sync_calendar(self, conn, env):
        add = call_action(mod.integration_add_productivity_connector, conn, ns(
            company_id=env["company_id"],
            platform="google_workspace",
        ))
        result = call_action(mod.integration_sync_calendar, conn, ns(
            connector_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["sync_type"] == "calendar"

    def test_sync_contacts(self, conn, env):
        add = call_action(mod.integration_add_productivity_connector, conn, ns(
            company_id=env["company_id"],
            platform="microsoft_365",
        ))
        result = call_action(mod.integration_sync_contacts, conn, ns(
            connector_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["sync_type"] == "contacts"

    def test_sync_files(self, conn, env):
        add = call_action(mod.integration_add_productivity_connector, conn, ns(
            company_id=env["company_id"],
            platform="google_workspace",
        ))
        result = call_action(mod.integration_sync_files, conn, ns(
            connector_id=add["id"],
        ))
        assert is_ok(result), result
        assert result["sync_type"] == "files"

    def test_list_productivity_syncs(self, conn, env):
        call_action(mod.integration_add_productivity_connector, conn, ns(
            company_id=env["company_id"],
            platform="slack",
        ))
        result = call_action(mod.integration_list_productivity_syncs, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_sync_status_report(self, conn, env):
        call_action(mod.integration_add_productivity_connector, conn, ns(
            company_id=env["company_id"],
            platform="zoom",
        ))
        result = call_action(mod.integration_sync_status_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "rows" in result


# =============================================================================
# Cross-domain reports (connv2_reports)
# =============================================================================

class TestConnV2Reports:
    def test_connector_usage_report(self, conn, env):
        # Seed one connector in each domain
        call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"], platform="airbnb",
        ))
        call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"], platform="doordash",
        ))
        result = call_action(mod.integration_connector_usage_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["count"] == 5  # 5 domains
        domains = [r["domain"] for r in result["rows"]]
        assert "booking" in domains
        assert "delivery" in domains

    def test_sync_volume_report(self, conn, env):
        result = call_action(mod.integration_sync_volume_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert "booking_syncs" in result
        assert "delivery_orders" in result
        assert "realestate_leads" in result

    def test_error_rate_report(self, conn, env):
        result = call_action(mod.integration_error_rate_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["count"] == 5
        for row in result["rows"]:
            assert "error_rate_pct" in row
