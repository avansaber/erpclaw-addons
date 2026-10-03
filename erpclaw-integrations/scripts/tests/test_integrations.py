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
from decimal import Decimal

import pytest
from integration_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_company, seed_naming_series,
)

mod = load_db_query()


# Tables aggregated by the two read-only reports below. Neither report writes:
# no INSERT/UPDATE, no audit() call — so a snapshot of every row must be
# identical before and after. Neither action reaches the general ledger, so no
# two-leg balance assertion can hold for them.
_READ_REPORT_TABLES = (
    "connv2_productivity_connector",
    "connv2_booking_sync_log",
    "connv2_delivery_order",
    "connv2_realestate_lead",
    "audit_log",
)


def _snapshot(conn):
    """Full ordered row dump of every table the reports below read."""
    return {
        table: [tuple(row) for row in conn.execute(
            "SELECT * FROM %s ORDER BY id" % table).fetchall()]
        for table in _READ_REPORT_TABLES
    }


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
        # Behavioural: the report aggregates the stored connector rows for
        # this company only. Seed two google_workspace connectors (default
        # flags: calendar=1, contacts=1, files=0) and one slack connector,
        # plus one connector on a second company that must NOT leak in.
        cid = env["company_id"]
        for workspace in ("WS-A", "WS-B"):
            added = call_action(mod.integration_add_productivity_connector, conn, ns(
                company_id=cid,
                platform="google_workspace",
                workspace_id=workspace,
            ))
            assert is_ok(added), added
        added = call_action(mod.integration_add_productivity_connector, conn, ns(
            company_id=cid,
            platform="slack",
            workspace_id="WS-C",
        ))
        assert is_ok(added), added
        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        other = call_action(mod.integration_add_productivity_connector, conn, ns(
            company_id=other_company,
            platform="zoom",
            workspace_id="WS-OTHER",
        ))
        assert is_ok(other), other

        # Stored rows the report must aggregate, read back through the seam.
        stored = conn.execute(
            "SELECT platform, connector_status, sync_calendar, sync_contacts,"
            " sync_files FROM connv2_productivity_connector"
            " WHERE company_id = ?", (cid,)).fetchall()
        assert len(stored) == 3
        expected = {}
        for platform, status, cal, con, fil in stored:
            agg = expected.setdefault(platform, [0, 0, 0, 0, 0, 0])
            agg[0] += 1
            agg[1] += 1 if status == "active" else 0
            agg[2] += 1 if status == "error" else 0
            agg[3] += cal
            agg[4] += con
            agg[5] += fil

        before = _snapshot(conn)
        result = call_action(mod.integration_sync_status_report, conn, ns(
            company_id=cid,
        ))
        assert is_ok(result), result
        # Read-only: no row written, changed, or audited — other company's
        # zoom connector included in the snapshot, untouched.
        assert _snapshot(conn) == before

        assert result["count"] == len(expected) == 2
        by_platform = {row["platform"]: row for row in result["rows"]}
        assert set(by_platform) == {"google_workspace", "slack"}
        for platform, (total, active, errors, cal, con, fil) in expected.items():
            row = by_platform[platform]
            assert row["connector_count"] == total
            assert row["active_count"] == active
            assert row["error_count"] == errors
            assert row["calendars_synced"] == cal
            assert row["contacts_synced"] == con
            assert row["files_synced"] == fil
        # Exact values for the seeded shape (calendar+contacts on, files off).
        google = by_platform["google_workspace"]
        assert (google["connector_count"], google["active_count"],
                google["error_count"]) == (2, 0, 0)
        assert (google["calendars_synced"], google["contacts_synced"],
                google["files_synced"]) == (2, 2, 0)
        slack = by_platform["slack"]
        assert (slack["connector_count"], slack["calendars_synced"],
                slack["contacts_synced"], slack["files_synced"]) == (1, 1, 1, 0)
        # Ordered by connector_count DESC.
        assert [row["platform"] for row in result["rows"]] == [
            "google_workspace", "slack"]

    def test_sync_status_report_refuses_bad_company(self, conn, env):
        # Refusal: missing and unknown companies are rejected with a truthful
        # message, and the database is byte-identical afterwards — a refusal
        # that half-writes is worse than no refusal.
        seeded = call_action(mod.integration_add_productivity_connector, conn, ns(
            company_id=env["company_id"],
            platform="zoom",
        ))
        assert is_ok(seeded), seeded
        before = _snapshot(conn)
        missing = call_action(mod.integration_sync_status_report, conn, ns(
            company_id=None,
        ))
        assert is_error(missing), missing
        assert missing["message"] == "--company-id is required"
        unknown = call_action(mod.integration_sync_status_report, conn, ns(
            company_id="no-such-company",
        ))
        assert is_error(unknown), unknown
        assert unknown["message"] == "Company no-such-company not found"
        assert _snapshot(conn) == before


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
        # Behavioural: the report aggregates the stored booking sync logs,
        # delivery orders and real-estate leads for this company only.
        cid = env["company_id"]
        booking = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=cid, platform="airbnb", property_id="PROP-1",
        ))
        assert is_ok(booking), booking
        first = call_action(mod.integration_sync_reservations, conn, ns(
            connector_id=booking["id"], records_synced=10, errors=0,
        ))
        assert is_ok(first), first
        second = call_action(mod.integration_sync_reservations, conn, ns(
            connector_id=booking["id"], records_synced=5, errors=2,
        ))
        assert is_ok(second), second

        delivery = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=cid, platform="doordash", store_id="STORE-1",
        ))
        assert is_ok(delivery), delivery
        order = call_action(mod.integration_ingest_orders, conn, ns(
            connector_id=delivery["id"], external_order_id="EXT-1",
            total_amount="100.00", commission="10.00",
        ))
        assert is_ok(order), order

        estate = call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=cid, platform="zillow", agent_id="AGENT-1",
        ))
        assert is_ok(estate), estate
        for name, source in (("Jane Buyer", "website"), ("Joe Seller", "referral")):
            lead = call_action(mod.integration_capture_leads, conn, ns(
                connector_id=estate["id"], contact_name=name,
                lead_source=source,
            ))
            assert is_ok(lead), lead

        # Second company whose rows must NOT leak into this company's report.
        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        other_booking = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=other_company, platform="vrbo", property_id="PROP-X",
        ))
        assert is_ok(other_booking), other_booking
        other_sync = call_action(mod.integration_sync_reservations, conn, ns(
            connector_id=other_booking["id"], records_synced=99, errors=9,
        ))
        assert is_ok(other_sync), other_sync

        # Stored rows the report must aggregate, read back through the seam.
        logs = [tuple(row) for row in conn.execute(
            "SELECT records_synced, errors, sync_status, company_id"
            " FROM connv2_booking_sync_log WHERE company_id = ?"
            " ORDER BY records_synced", (cid,)).fetchall()]
        assert logs == [(5, 2, "failed", cid), (10, 0, "completed", cid)]
        stored_order = conn.execute(
            "SELECT total_amount, commission, net_amount, order_status"
            " FROM connv2_delivery_order WHERE company_id = ?", (cid,)).fetchone()
        # Money is text: exact strings, Decimal arithmetic, never float.
        assert tuple(stored_order) == ("100.00", "10.00", "90.00", "received")
        assert (Decimal(stored_order["total_amount"])
                - Decimal(stored_order["commission"])
                == Decimal(stored_order["net_amount"]))
        leads = [tuple(row) for row in conn.execute(
            "SELECT contact_name, lead_source, lead_status"
            " FROM connv2_realestate_lead WHERE company_id = ?"
            " ORDER BY contact_name", (cid,)).fetchall()]
        assert leads == [("Jane Buyer", "website", "new"),
                         ("Joe Seller", "referral", "new")]

        before = _snapshot(conn)
        result = call_action(mod.integration_sync_volume_report, conn, ns(
            company_id=cid,
        ))
        assert is_ok(result), result
        # Read-only: stored rows and audit log untouched.
        assert _snapshot(conn) == before

        assert result["booking_syncs"] == {
            "total_syncs": 2, "total_records": 15, "total_errors": 2}
        assert result["delivery_orders"] == {"total_orders": 1}
        assert result["realestate_leads"] == {"total_leads": 2}
        # Neither action reaches the general ledger: no GL postings exist for
        # these tables, so no two-leg balance assertion can hold here.

    def test_sync_volume_report_refuses_bad_company(self, conn, env):
        # Refusal: missing and unknown companies are rejected with a truthful
        # message, and the database is byte-identical afterwards — a refusal
        # that half-writes is worse than no refusal.
        booking = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"], platform="airbnb",
        ))
        assert is_ok(booking), booking
        before = _snapshot(conn)
        missing = call_action(mod.integration_sync_volume_report, conn, ns(
            company_id=None,
        ))
        assert is_error(missing), missing
        assert missing["message"] == "--company-id is required"
        unknown = call_action(mod.integration_sync_volume_report, conn, ns(
            company_id="no-such-company",
        ))
        assert is_error(unknown), unknown
        assert unknown["message"] == "Company no-such-company not found"
        assert _snapshot(conn) == before

    def test_error_rate_report(self, conn, env):
        result = call_action(mod.integration_error_rate_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["count"] == 5
        for row in result["rows"]:
            assert "error_rate_pct" in row
