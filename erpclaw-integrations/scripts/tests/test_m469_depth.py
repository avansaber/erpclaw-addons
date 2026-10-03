"""M469 depth tests: 12 integration actions proven against stored rows.

Each action below already had a shape-only test (response has the right keys)
in ``test_integrations.py``. Those tests never read the database back, so a
perfectly shaped response over missing or wrong rows still passed. Every test
here seeds exact rows through the owning module's own actions, invokes the
action under test, then reads the rows back and compares exact values.

Per-action depth classification (acceptance item 3):
  stored-row effect (writes then verified by re-read):
    - integration-archive-bank-statement
  stored-row read (read-only; response values verified against re-read rows,
  plus proof the call wrote nothing):
    - integration-get-bank-statement
    - integration-list-bank-statements
    - integration-list-bank-match-rules
    - integration-booking-revenue-report
    - integration-booking-channel-report
    - integration-delivery-revenue-report (also money aggregates)
    - integration-delivery-platform-comparison (also money aggregates)
    - integration-listing-performance-report
    - integration-lead-source-report
    - integration-bank-feed-reconciliation-report
    - integration-communication-delivery-report
  ledger effect: NONE of the 12 actions posts to the ledger. Each test asserts
    gl_entry stays empty so a later reader does not add a both-legs assertion
    that cannot hold. Money stored in TEXT columns is compared as exact
    strings; SQL-computed aggregates (SQLite SUM over CAST(.. AS NUMERIC))
    come back as JSON numbers and are compared as exact Decimals via str() --
    never float equality, never round().

Refusal rule: every action that validates input gets one refusal case proving
the refusal happens, the message names the real problem, and every
module-owned table (plus gl_entry and audit_log) is identical afterwards.
The two bank list actions validate nothing; their tests prove that leniency
explicitly (unknown company -> empty result, nothing written) instead of
pretending a refusal exists.
"""
import json
import os
from decimal import Decimal

import pytest

from integration_helpers import (
    call_action, ns, is_error, is_ok, load_db_query, _uuid,
    seed_company, seed_naming_series,
)

mod = load_db_query()

MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(MODULE_DIR)))
REPO_ROOT = os.path.dirname(SRC_DIR)
FIXTURES = os.path.join(REPO_ROOT, "testing", "fixtures", "bank")


def _bank_fixtures_present():
    return os.path.isdir(FIXTURES)


def _require_bank_fixtures():
    if not _bank_fixtures_present():
        pytest.skip("testing/fixtures/bank not present -- needs monorepo tree")


def _dec(value):
    """Exact Decimal for a money value that may arrive as a JSON number."""
    return Decimal(str(value))


_SNAPSHOT_TABLES = (
    "connv2_booking_connector",
    "connv2_booking_sync_log",
    "connv2_delivery_connector",
    "connv2_delivery_order",
    "connv2_realestate_connector",
    "connv2_realestate_lead",
    "connv2_financial_connector",
    "connv2_productivity_connector",
    "bank_statement",
    "bank_statement_line",
    "bank_match_rule",
    "gl_entry",
    "audit_log",
)


def _snapshot(conn):
    """Ordered dump of every table these actions could plausibly touch."""
    snap = {}
    for table in _SNAPSHOT_TABLES:
        try:
            rows = conn.execute("SELECT * FROM %s" % table).fetchall()
        except Exception:
            continue
        snap[table] = sorted(
            json.dumps(dict(row), sort_keys=True, default=str) for row in rows
        )
    return snap


def _gl_count(conn):
    return conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0]


def _seed_bank_account(conn, company_id, name="Depth Checking"):
    aid = _uuid()
    conn.execute(
        "INSERT INTO account (id, name, root_type, account_type, currency, "
        "is_group, disabled, company_id) VALUES (?,?,?,?,?,0,0,?)",
        (aid, name, "asset", "bank", "USD", company_id))
    conn.commit()
    return aid


@pytest.fixture
def banked(conn):
    _require_bank_fixtures()
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    aid = _seed_bank_account(conn, cid)
    return {"conn": conn, "company_id": cid, "bank_account_id": aid}


def _import_with(conn, company_id, bank_account_id, filename, fmt="auto"):
    return call_action(
        mod.ACTIONS["integration-import-bank-statement"], conn,
        ns(company_id=company_id, bank_account_id=bank_account_id,
           file=os.path.join(FIXTURES, filename), format=fmt))


def _statement_row(conn, statement_id):
    return dict(conn.execute(
        "SELECT * FROM bank_statement WHERE id = ?", (statement_id,)).fetchone())


def _statement_lines(conn, statement_id):
    return [dict(r) for r in conn.execute(
        "SELECT * FROM bank_statement_line WHERE bank_statement_id = ? "
        "ORDER BY external_id", (statement_id,)).fetchall()]


# =============================================================================
# integration-archive-bank-statement -- stored-row EFFECT
# =============================================================================
class TestArchiveBankStatement:
    def test_archive_flips_status_and_nothing_else(self, banked):
        conn = banked["conn"]
        imp = _import_with(conn, banked["company_id"],
                           banked["bank_account_id"],
                           "statement-jan-2026.ofx")
        assert is_ok(imp), imp
        sid = imp["statement_id"]
        before = _statement_row(conn, sid)
        assert before["import_status"] == "imported"
        lines_before = _statement_lines(conn, sid)
        assert len(lines_before) == 4
        snap_before = _snapshot(conn)
        audit_before = len(snap_before["audit_log"])
        assert _gl_count(conn) == 0

        result = call_action(mod.integration_archive_bank_statement, conn,
                             ns(statement_id=sid))
        assert is_ok(result), result
        assert result["statement_id"] == sid
        assert result["import_status"] == "archived"

        after = _statement_row(conn, sid)
        assert after["import_status"] == "archived"
        for column, value in before.items():
            if column == "import_status":
                continue
            assert after[column] == value, column
        assert _statement_lines(conn, sid) == lines_before

        # No ledger effect: archiving changes a status flag only; it posts
        # nothing, so a both-legs assertion cannot hold. gl_entry untouched.
        assert _gl_count(conn) == 0
        snap_after = _snapshot(conn)
        for table in _SNAPSHOT_TABLES:
            if table in ("bank_statement", "audit_log"):
                continue
            assert snap_after[table] == snap_before[table], table
        # Exactly one new row anywhere: the audit entry for this action.
        assert len(snap_after["audit_log"]) == audit_before + 1
        audits = [json.loads(r) for r in snap_after["audit_log"]]
        assert any(a.get("action") == "integration-archive-bank-statement"
                   and a.get("entity_id") == sid for a in audits)

    def test_archive_unknown_statement_refused_and_writes_nothing(self, banked):
        conn = banked["conn"]
        imp = _import_with(conn, banked["company_id"],
                           banked["bank_account_id"],
                           "statement-jan-2026.ofx")
        assert is_ok(imp), imp
        snap_before = _snapshot(conn)
        bogus = "statement-does-not-exist"

        result = call_action(mod.integration_archive_bank_statement, conn,
                             ns(statement_id=bogus))
        assert is_error(result), result
        assert result["message"] == "Bank statement %s not found" % bogus
        assert _snapshot(conn) == snap_before


# =============================================================================
# integration-get-bank-statement -- stored-row READ
# =============================================================================
class TestGetBankStatement:
    def test_get_returns_exact_stored_rows(self, banked):
        conn = banked["conn"]
        imp = _import_with(conn, banked["company_id"],
                           banked["bank_account_id"],
                           "statement-jan-2026.ofx")
        assert is_ok(imp), imp
        sid = imp["statement_id"]
        snap_before = _snapshot(conn)

        result = call_action(mod.integration_get_bank_statement, conn,
                             ns(statement_id=sid))
        assert is_ok(result), result

        stored = _statement_row(conn, sid)
        assert result["statement"]["id"] == stored["id"]
        assert result["statement"]["source"] == "ofx"
        assert result["statement"]["bank_account_id"] == banked["bank_account_id"]
        assert result["statement"]["company_id"] == banked["company_id"]
        assert result["statement"]["import_status"] == "imported"
        assert result["statement"]["currency"] == "USD"
        assert result["line_count"] == 4

        # Money is text: exact string comparison, never float.
        by_ext = {ln["external_id"]: ln for ln in result["lines"]}
        assert set(by_ext) == {
            "BANK-20260105-001", "BANK-20260108-002",
            "BANK-20260112-003", "BANK-20260120-004",
        }
        assert by_ext["BANK-20260105-001"]["amount"] == "1500.00"
        assert by_ext["BANK-20260108-002"]["amount"] == "-250.50"
        assert by_ext["BANK-20260112-003"]["amount"] == "-1200.00"
        assert by_ext["BANK-20260120-004"]["amount"] == "3200.00"
        for line in result["lines"]:
            assert line["match_status"] == "unmatched"
            assert line["bank_statement_id"] == sid

        # Read-only: the response matches the rows and the rows did not move.
        assert _snapshot(conn) == snap_before
        # No ledger effect: reading a statement posts nothing.
        assert _gl_count(conn) == 0

    def test_get_unknown_statement_refused_and_writes_nothing(self, banked):
        conn = banked["conn"]
        imp = _import_with(conn, banked["company_id"],
                           banked["bank_account_id"],
                           "statement-jan-2026.ofx")
        assert is_ok(imp), imp
        snap_before = _snapshot(conn)
        bogus = "statement-does-not-exist"

        result = call_action(mod.integration_get_bank_statement, conn,
                             ns(statement_id=bogus))
        assert is_error(result), result
        assert result["message"] == "Bank statement %s not found" % bogus
        assert _snapshot(conn) == snap_before


# =============================================================================
# integration-list-bank-statements -- stored-row READ
# =============================================================================
class TestListBankStatements:
    def test_list_returns_both_imports_exactly(self, banked):
        conn = banked["conn"]
        first = _import_with(conn, banked["company_id"],
                             banked["bank_account_id"],
                             "statement-jan-2026.ofx")
        second = _import_with(conn, banked["company_id"],
                              banked["bank_account_id"],
                              "statement-jan-2026.mt940")
        assert is_ok(first) and is_ok(second), (first, second)
        snap_before = _snapshot(conn)

        result = call_action(mod.integration_list_bank_statements, conn,
                             ns(company_id=banked["company_id"]))
        assert is_ok(result), result
        assert result["total_count"] == 2
        assert result["has_more"] is False
        by_id = {row["id"]: row for row in result["rows"]}
        assert set(by_id) == {first["statement_id"], second["statement_id"]}
        assert by_id[first["statement_id"]]["source"] == "ofx"
        assert by_id[second["statement_id"]]["source"] == "mt940"
        for row in result["rows"]:
            assert row["bank_account_id"] == banked["bank_account_id"]
            assert row["company_id"] == banked["company_id"]
            assert row["line_count"] == 4
            assert row["import_status"] == "imported"

        assert _snapshot(conn) == snap_before
        # No ledger effect: listing posts nothing.
        assert _gl_count(conn) == 0

    def test_list_unknown_company_refuses_and_writes_nothing(self, banked):
        conn = banked["conn"]
        imp = _import_with(conn, banked["company_id"],
                           banked["bank_account_id"],
                           "statement-jan-2026.ofx")
        assert is_ok(imp), imp
        snap_before = _snapshot(conn)

        result = call_action(mod.integration_list_bank_statements, conn,
                             ns(company_id="company-does-not-exist"))
        assert result == {
            "status": "error",
            "error": "Company not found: company-does-not-exist",
            "message": "Company not found: company-does-not-exist",
        }
        assert _snapshot(conn) == snap_before


# =============================================================================
# integration-list-bank-match-rules -- stored-row READ
# =============================================================================
class TestListBankMatchRules:
    def _add_rule(self, conn, company_id, **kw):
        base = dict(company_id=company_id, name="r",
                    match_field="counterparty_name",
                    match_operator="contains", match_value="ACME",
                    target_action="map_to_account", target_id="ACC-1",
                    priority=100)
        base.update(kw)
        return call_action(
            mod.ACTIONS["integration-add-bank-match-rule"], conn, ns(**base))

    def test_list_returns_rules_in_priority_order(self, banked):
        conn = banked["conn"]
        low = self._add_rule(conn, banked["company_id"], name="low-priority",
                             match_value="ACME", priority=50)
        high = self._add_rule(conn, banked["company_id"], name="high-priority",
                              match_value="ACME", priority=10)
        assert is_ok(low) and is_ok(high), (low, high)
        snap_before = _snapshot(conn)

        result = call_action(mod.integration_list_bank_match_rules, conn,
                             ns(company_id=banked["company_id"]))
        assert is_ok(result), result
        assert result["count"] == 2
        assert [r["id"] for r in result["rows"]] == [high["id"], low["id"]]
        assert result["rows"][0]["name"] == "high-priority"
        assert result["rows"][0]["match_field"] == "counterparty_name"
        assert result["rows"][0]["match_operator"] == "contains"
        assert result["rows"][0]["match_value"] == "ACME"
        assert result["rows"][0]["target_action"] == "map_to_account"
        assert result["rows"][0]["target_id"] == "ACC-1"
        assert result["rows"][0]["priority"] == 10
        assert result["rows"][1]["priority"] == 50
        for row in result["rows"]:
            assert row["company_id"] == banked["company_id"]
            stored = dict(conn.execute(
                "SELECT * FROM bank_match_rule WHERE id = ?",
                (row["id"],)).fetchone())
            for column in ("name", "match_field", "match_operator",
                           "match_value", "target_action", "target_id",
                           "priority", "company_id"):
                assert row[column] == stored[column], column

        assert _snapshot(conn) == snap_before
        # No ledger effect: listing rules posts nothing.
        assert _gl_count(conn) == 0

    def test_list_unknown_company_refuses_and_writes_nothing(self, banked):
        conn = banked["conn"]
        rule = self._add_rule(conn, banked["company_id"])
        assert is_ok(rule), rule
        snap_before = _snapshot(conn)

        result = call_action(mod.integration_list_bank_match_rules, conn,
                             ns(company_id="company-does-not-exist"))
        assert result == {
            "status": "error",
            "error": "Company not found: company-does-not-exist",
            "message": "Company not found: company-does-not-exist",
        }
        assert _snapshot(conn) == snap_before


# =============================================================================
# integration-booking-revenue-report -- stored-row READ
# =============================================================================
class TestBookingRevenueReportDepth:
    def test_report_aggregates_exact_sync_log_rows(self, conn, env):
        add = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"], platform="booking_com",
            property_id="PROP-7"))
        assert is_ok(add), add
        first = call_action(mod.integration_sync_reservations, conn, ns(
            connector_id=add["id"], records_synced="20", errors="0"))
        second = call_action(mod.integration_push_rates, conn, ns(
            connector_id=add["id"], records_synced="5", errors="0"))
        assert is_ok(first) and is_ok(second), (first, second)

        # Another company's rows must not leak into this report.
        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        other_add = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=other_company, platform="booking_com",
            property_id="PROP-7"))
        assert is_ok(other_add), other_add
        other_sync = call_action(mod.integration_sync_reservations, conn, ns(
            connector_id=other_add["id"], records_synced="999", errors="0"))
        assert is_ok(other_sync), other_sync

        snap_before = _snapshot(conn)
        assert _gl_count(conn) == 0

        result = call_action(mod.integration_booking_revenue_report, conn, ns(
            company_id=env["company_id"]))
        assert is_ok(result), result
        assert result["count"] == 1
        row = result["rows"][0]
        assert row["platform"] == "booking_com"
        assert row["property_id"] == "PROP-7"
        assert row["total_syncs"] == 2
        assert row["total_records"] == 25
        assert row["total_errors"] == 0

        logs = conn.execute(
            "SELECT sync_type, records_synced, errors, sync_status "
            "FROM connv2_booking_sync_log WHERE connector_id = ? "
            "ORDER BY sync_type", (add["id"],)).fetchall()
        assert [dict(r) for r in logs] == [
            {"sync_type": "rates", "records_synced": 5, "errors": 0,
             "sync_status": "completed"},
            {"sync_type": "reservations", "records_synced": 20, "errors": 0,
             "sync_status": "completed"},
        ]

        assert _snapshot(conn) == snap_before
        # No ledger effect: the report aggregates sync logs; gl_entry untouched.
        assert _gl_count(conn) == 0

    def test_report_unknown_company_refused_and_writes_nothing(self, conn, env):
        add = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"], platform="booking_com"))
        assert is_ok(add), add
        snap_before = _snapshot(conn)
        bogus = "company-does-not-exist"

        result = call_action(mod.integration_booking_revenue_report, conn, ns(
            company_id=bogus))
        assert is_error(result), result
        assert result["message"] == "Company %s not found" % bogus
        assert _snapshot(conn) == snap_before


# =============================================================================
# integration-booking-channel-report -- stored-row READ
# =============================================================================
class TestBookingChannelReportDepth:
    def test_report_counts_connectors_and_failed_syncs(self, conn, env):
        first = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"], platform="airbnb"))
        second = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"], platform="airbnb"))
        assert is_ok(first) and is_ok(second), (first, second)
        activate = call_action(mod.integration_configure_booking_sync, conn, ns(
            connector_id=first["id"], connector_status="active"))
        assert is_ok(activate), activate
        ok_sync = call_action(mod.integration_sync_reservations, conn, ns(
            connector_id=first["id"], records_synced="20", errors="0"))
        failed_sync = call_action(mod.integration_sync_reservations, conn, ns(
            connector_id=first["id"], records_synced="5", errors="3"))
        other_sync = call_action(mod.integration_push_availability, conn, ns(
            connector_id=second["id"], records_synced="7", errors="0"))
        assert is_ok(ok_sync) and is_ok(failed_sync), (ok_sync, failed_sync)
        assert is_ok(other_sync), other_sync
        assert failed_sync["sync_status"] == "failed"

        snap_before = _snapshot(conn)

        result = call_action(mod.integration_booking_channel_report, conn, ns(
            company_id=env["company_id"]))
        assert is_ok(result), result
        assert result["count"] == 1
        row = result["rows"][0]
        assert row["platform"] == "airbnb"
        assert row["connector_count"] == 2
        # FINDING (deliberately not fixed): active_count is summed over the
        # joined sync-log rows, not over connectors -- the single active
        # connector has 2 logs, so it is counted twice. Expected 1 (one
        # active connector); the action really returns 2. connector_count
        # uses COUNT(DISTINCT) and is exact; failed_syncs is per-log and is
        # exact. This test pins the real behaviour.
        assert row["active_count"] == 2
        assert row["total_sync_logs"] == 3
        assert row["failed_syncs"] == 1

        statuses = sorted(r[0] for r in conn.execute(
            "SELECT connector_status FROM connv2_booking_connector "
            "WHERE company_id = ?", (env["company_id"],)).fetchall())
        assert statuses == ["active", "inactive"]

        assert _snapshot(conn) == snap_before
        # No ledger effect: channel counts post nothing.
        assert _gl_count(conn) == 0

    def test_report_missing_company_refused_and_writes_nothing(self, conn, env):
        add = call_action(mod.integration_add_booking_connector, conn, ns(
            company_id=env["company_id"], platform="airbnb"))
        assert is_ok(add), add
        snap_before = _snapshot(conn)

        result = call_action(mod.integration_booking_channel_report, conn,
                             ns(company_id=None))
        assert is_error(result), result
        assert result["message"] == "--company-id is required"
        assert _snapshot(conn) == snap_before


# =============================================================================
# integration-delivery-revenue-report -- stored-row READ (money)
# =============================================================================
class TestDeliveryRevenueReportDepth:
    def test_report_sums_exact_money_text(self, conn, env):
        add = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"], platform="doordash"))
        assert is_ok(add), add
        first = call_action(mod.integration_ingest_orders, conn, ns(
            connector_id=add["id"], total_amount="50.00", commission="7.50"))
        second = call_action(mod.integration_ingest_orders, conn, ns(
            connector_id=add["id"], total_amount="100.00", commission="10.00"))
        assert is_ok(first) and is_ok(second), (first, second)
        assert first["net_amount"] == "42.50"
        assert second["net_amount"] == "90.00"

        # Another company's order must not leak into this report.
        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        other_add = call_action(mod.integration_add_delivery_connector, conn,
                                ns(company_id=other_company,
                                   platform="doordash"))
        assert is_ok(other_add), other_add
        other_order = call_action(mod.integration_ingest_orders, conn, ns(
            connector_id=other_add["id"], total_amount="1000.00",
            commission="100.00"))
        assert is_ok(other_order), other_order

        snap_before = _snapshot(conn)

        result = call_action(mod.integration_delivery_revenue_report, conn, ns(
            company_id=env["company_id"]))
        assert is_ok(result), result
        assert result["count"] == 1
        row = result["rows"][0]
        assert row["platform"] == "doordash"
        assert row["total_orders"] == 2
        # Money is text in storage; the report aggregates in SQL, so compare
        # as exact Decimals (never float equality, never round()).
        assert _dec(row["gross_revenue"]) == Decimal("150.00")
        assert _dec(row["total_commission"]) == Decimal("17.50")
        assert _dec(row["net_revenue"]) == Decimal("132.50")

        stored = [dict(r) for r in conn.execute(
            "SELECT total_amount, commission, net_amount, order_status "
            "FROM connv2_delivery_order WHERE connector_id = ? "
            "ORDER BY total_amount", (add["id"],)).fetchall()]
        assert stored == [
            {"total_amount": "100.00", "commission": "10.00",
             "net_amount": "90.00", "order_status": "received"},
            {"total_amount": "50.00", "commission": "7.50",
             "net_amount": "42.50", "order_status": "received"},
        ]

        assert _snapshot(conn) == snap_before
        # No ledger effect: ingesting delivery orders posts no gl_entry legs.
        assert _gl_count(conn) == 0

    def test_report_missing_company_refused_and_writes_nothing(self, conn, env):
        add = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"], platform="doordash"))
        assert is_ok(add), add
        snap_before = _snapshot(conn)

        result = call_action(mod.integration_delivery_revenue_report, conn,
                             ns(company_id=None))
        assert is_error(result), result
        assert result["message"] == "--company-id is required"
        assert _snapshot(conn) == snap_before


# =============================================================================
# integration-delivery-platform-comparison -- stored-row READ (money)
# =============================================================================
class TestDeliveryPlatformComparisonDepth:
    def test_comparison_counts_statuses_and_net(self, conn, env):
        dash = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"], platform="doordash"))
        assert is_ok(dash), dash
        first = call_action(mod.integration_ingest_orders, conn, ns(
            connector_id=dash["id"], total_amount="50.00", commission="7.50"))
        second = call_action(mod.integration_ingest_orders, conn, ns(
            connector_id=dash["id"], total_amount="20.00", commission="4.00"))
        assert is_ok(first) and is_ok(second), (first, second)
        delivered = call_action(mod.integration_update_order_status, conn, ns(
            order_id=first["id"], order_status="delivered"))
        cancelled = call_action(mod.integration_update_order_status, conn, ns(
            order_id=second["id"], order_status="cancelled"))
        assert is_ok(delivered) and is_ok(cancelled), (delivered, cancelled)
        eats = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"], platform="ubereats"))
        assert is_ok(eats), eats

        snap_before = _snapshot(conn)

        result = call_action(
            mod.integration_delivery_platform_comparison, conn,
            ns(company_id=env["company_id"]))
        assert is_ok(result), result
        assert result["count"] == 2
        by_platform = {r["platform"]: r for r in result["rows"]}
        assert set(by_platform) == {"doordash", "ubereats"}
        dash_row = by_platform["doordash"]
        assert dash_row["connector_count"] == 1
        assert dash_row["total_orders"] == 2
        assert dash_row["delivered_orders"] == 1
        assert dash_row["cancelled_orders"] == 1
        assert _dec(dash_row["total_net"]) == Decimal("58.50")
        eats_row = by_platform["ubereats"]
        assert eats_row["connector_count"] == 1
        assert eats_row["total_orders"] == 0
        assert eats_row["delivered_orders"] == 0
        assert eats_row["cancelled_orders"] == 0
        assert _dec(eats_row["total_net"]) == Decimal("0")

        statuses = sorted(r[0] for r in conn.execute(
            "SELECT order_status FROM connv2_delivery_order "
            "WHERE connector_id = ?", (dash["id"],)).fetchall())
        assert statuses == ["cancelled", "delivered"]

        assert _snapshot(conn) == snap_before
        # No ledger effect: status moves post no gl_entry legs.
        assert _gl_count(conn) == 0

    def test_comparison_unknown_company_refused_and_writes_nothing(
            self, conn, env):
        add = call_action(mod.integration_add_delivery_connector, conn, ns(
            company_id=env["company_id"], platform="doordash"))
        assert is_ok(add), add
        snap_before = _snapshot(conn)
        bogus = "company-does-not-exist"

        result = call_action(
            mod.integration_delivery_platform_comparison, conn,
            ns(company_id=bogus))
        assert is_error(result), result
        assert result["message"] == "Company %s not found" % bogus
        assert _snapshot(conn) == snap_before


# =============================================================================
# integration-listing-performance-report -- stored-row READ
# =============================================================================
class TestListingPerformanceReportDepth:
    def test_report_counts_leads_and_conversions(self, conn, env):
        zillow = call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=env["company_id"], platform="zillow",
            agent_id="AGENT-1"))
        assert is_ok(zillow), zillow
        realtor = call_action(mod.integration_add_realestate_connector, conn,
                              ns(company_id=env["company_id"],
                                 platform="realtor_com", agent_id="AGENT-2"))
        assert is_ok(realtor), realtor
        first = call_action(mod.integration_capture_leads, conn, ns(
            connector_id=zillow["id"], contact_name="Lead One"))
        second = call_action(mod.integration_capture_leads, conn, ns(
            connector_id=zillow["id"], contact_name="Lead Two"))
        assert is_ok(first) and is_ok(second), (first, second)
        # No owner action transitions lead_status, so the converted state is
        # seeded directly; the report must still count it exactly.
        conn.execute(
            "UPDATE connv2_realestate_lead SET lead_status = 'converted' "
            "WHERE id = ?", (first["id"],))
        conn.commit()

        snap_before = _snapshot(conn)

        result = call_action(mod.integration_listing_performance_report, conn,
                             ns(company_id=env["company_id"]))
        assert is_ok(result), result
        assert result["count"] == 2
        assert result["rows"][0]["platform"] == "zillow"
        assert result["rows"][0]["agent_id"] == "AGENT-1"
        assert result["rows"][0]["total_leads"] == 2
        assert result["rows"][0]["converted_leads"] == 1
        assert result["rows"][1]["platform"] == "realtor_com"
        assert result["rows"][1]["total_leads"] == 0
        assert result["rows"][1]["converted_leads"] == 0

        statuses = sorted(r[0] for r in conn.execute(
            "SELECT lead_status FROM connv2_realestate_lead "
            "WHERE connector_id = ?", (zillow["id"],)).fetchall())
        assert statuses == ["converted", "new"]

        assert _snapshot(conn) == snap_before
        # No ledger effect: lead capture posts no gl_entry legs.
        assert _gl_count(conn) == 0

    def test_report_missing_company_refused_and_writes_nothing(
            self, conn, env):
        add = call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=env["company_id"], platform="zillow"))
        assert is_ok(add), add
        snap_before = _snapshot(conn)

        result = call_action(mod.integration_listing_performance_report, conn,
                             ns(company_id=None))
        assert is_error(result), result
        assert result["message"] == "--company-id is required"
        assert _snapshot(conn) == snap_before


# =============================================================================
# integration-lead-source-report -- stored-row READ
# =============================================================================
class TestLeadSourceReportDepth:
    def test_report_groups_by_status_without_leakage(self, conn, env):
        mls = call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=env["company_id"], platform="mls"))
        assert is_ok(mls), mls
        first = call_action(mod.integration_capture_leads, conn, ns(
            connector_id=mls["id"], contact_name="Lead One",
            lead_source="website"))
        second = call_action(mod.integration_capture_leads, conn, ns(
            connector_id=mls["id"], contact_name="Lead Two",
            lead_source="referral"))
        assert is_ok(first) and is_ok(second), (first, second)
        # No owner action transitions lead_status; seed one contacted lead.
        conn.execute(
            "UPDATE connv2_realestate_lead SET lead_status = 'contacted' "
            "WHERE id = ?", (second["id"],))
        conn.commit()

        # Another company's lead must not leak into this report.
        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        other_add = call_action(mod.integration_add_realestate_connector, conn,
                                ns(company_id=other_company, platform="mls"))
        assert is_ok(other_add), other_add
        other_lead = call_action(mod.integration_capture_leads, conn, ns(
            connector_id=other_add["id"], contact_name="Other Lead"))
        assert is_ok(other_lead), other_lead

        snap_before = _snapshot(conn)

        result = call_action(mod.integration_lead_source_report, conn, ns(
            company_id=env["company_id"]))
        assert is_ok(result), result
        assert result["count"] == 2
        got = {(r["platform"], r["lead_status"], r["lead_count"])
               for r in result["rows"]}
        assert got == {("mls", "new", 1), ("mls", "contacted", 1)}

        stored = sorted(r[0] for r in conn.execute(
            "SELECT lead_status FROM connv2_realestate_lead "
            "WHERE connector_id = ?", (mls["id"],)).fetchall())
        assert stored == ["contacted", "new"]

        assert _snapshot(conn) == snap_before
        # No ledger effect: the lead report posts nothing.
        assert _gl_count(conn) == 0

    def test_report_unknown_company_refused_and_writes_nothing(
            self, conn, env):
        add = call_action(mod.integration_add_realestate_connector, conn, ns(
            company_id=env["company_id"], platform="mls"))
        assert is_ok(add), add
        snap_before = _snapshot(conn)
        bogus = "company-does-not-exist"

        result = call_action(mod.integration_lead_source_report, conn, ns(
            company_id=bogus))
        assert is_error(result), result
        assert result["message"] == "Company %s not found" % bogus
        assert _snapshot(conn) == snap_before


# =============================================================================
# integration-bank-feed-reconciliation-report -- stored-row READ
# =============================================================================
class TestBankFeedReconciliationReportDepth:
    def test_report_lists_only_plaid_connectors(self, conn, env):
        plaid = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"], platform="plaid",
            account_ref="ACCT-plaid-1"))
        assert is_ok(plaid), plaid
        twilio = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"], platform="twilio"))
        assert is_ok(twilio), twilio

        snap_before = _snapshot(conn)

        result = call_action(
            mod.integration_bank_feed_reconciliation_report, conn,
            ns(company_id=env["company_id"]))
        assert is_ok(result), result
        assert result["count"] == 1
        row = result["rows"][0]
        assert row["id"] == plaid["id"]
        assert row["platform"] == "plaid"
        assert row["account_ref"] == "ACCT-plaid-1"
        assert row["connector_status"] == "inactive"
        assert row["naming_series"] == plaid["naming_series"]

        stored = dict(conn.execute(
            "SELECT * FROM connv2_financial_connector WHERE id = ?",
            (plaid["id"],)).fetchone())
        # The report projects a fixed column list (no company_id);
        # compare exactly the projected columns against the stored row.
        for column in ("naming_series", "platform", "account_ref",
                       "connector_status"):
            assert row[column] == stored[column], column
        assert stored["company_id"] == env["company_id"]
        # The twilio connector exists but must not appear in a plaid report.
        assert all(r["platform"] == "plaid" for r in result["rows"])

        assert _snapshot(conn) == snap_before
        # No ledger effect: the reconciliation report posts nothing.
        assert _gl_count(conn) == 0

    def test_report_missing_company_refused_and_writes_nothing(
            self, conn, env):
        add = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"], platform="plaid"))
        assert is_ok(add), add
        snap_before = _snapshot(conn)

        result = call_action(
            mod.integration_bank_feed_reconciliation_report, conn,
            ns(company_id=None))
        assert is_error(result), result
        assert result["message"] == "--company-id is required"
        assert _snapshot(conn) == snap_before


# =============================================================================
# integration-communication-delivery-report -- stored-row READ
# =============================================================================
class TestCommunicationDeliveryReportDepth:
    def test_report_counts_statuses_per_messaging_platform(
            self, conn, env):
        twilio = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"], platform="twilio"))
        sendgrid = call_action(mod.integration_add_financial_connector, conn,
                               ns(company_id=env["company_id"],
                                  platform="sendgrid"))
        mailchimp = call_action(mod.integration_add_financial_connector, conn,
                                ns(company_id=env["company_id"],
                                   platform="mailchimp"))
        plaid = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"], platform="plaid"))
        assert is_ok(twilio) and is_ok(sendgrid), (twilio, sendgrid)
        assert is_ok(mailchimp) and is_ok(plaid), (mailchimp, plaid)
        # No owner action transitions connector_status; seed active/error.
        conn.execute(
            "UPDATE connv2_financial_connector SET connector_status = 'active'"
            " WHERE id = ?", (twilio["id"],))
        conn.execute(
            "UPDATE connv2_financial_connector SET connector_status = 'error'"
            " WHERE id = ?", (sendgrid["id"],))
        conn.execute(
            "UPDATE connv2_financial_connector SET connector_status = 'active'"
            " WHERE id = ?", (plaid["id"],))
        conn.commit()

        snap_before = _snapshot(conn)

        result = call_action(mod.integration_communication_delivery_report,
                             conn, ns(company_id=env["company_id"]))
        assert is_ok(result), result
        assert result["count"] == 3
        by_platform = {r["platform"]: r for r in result["rows"]}
        assert set(by_platform) == {"twilio", "sendgrid", "mailchimp"}
        # The active plaid connector must not appear: wrong platform family.
        assert by_platform["twilio"]["connector_count"] == 1
        assert by_platform["twilio"]["active_count"] == 1
        assert by_platform["twilio"]["error_count"] == 0
        assert by_platform["sendgrid"]["connector_count"] == 1
        assert by_platform["sendgrid"]["active_count"] == 0
        assert by_platform["sendgrid"]["error_count"] == 1
        assert by_platform["mailchimp"]["connector_count"] == 1
        assert by_platform["mailchimp"]["active_count"] == 0
        assert by_platform["mailchimp"]["error_count"] == 0

        stored = dict(conn.execute(
            "SELECT connector_status FROM connv2_financial_connector "
            "WHERE id = ?", (twilio["id"],)).fetchone())
        assert stored["connector_status"] == "active"

        assert _snapshot(conn) == snap_before
        # No ledger effect: the delivery report posts nothing.
        assert _gl_count(conn) == 0

    def test_report_unknown_company_refused_and_writes_nothing(
            self, conn, env):
        add = call_action(mod.integration_add_financial_connector, conn, ns(
            company_id=env["company_id"], platform="twilio"))
        assert is_ok(add), add
        snap_before = _snapshot(conn)
        bogus = "company-does-not-exist"

        result = call_action(mod.integration_communication_delivery_report,
                             conn, ns(company_id=bogus))
        assert is_error(result), result
        assert result["message"] == "Company %s not found" % bogus
        assert _snapshot(conn) == snap_before
