"""Behavioural depth for four erpclaw-ai-engine actions (task m494).

Each action below previously had only a shape test (asserts on the response
envelope) or a routability test (the contract suite's "Unknown action" check).
Neither observes the database, so an action could return a perfect envelope
while writing nothing -- or the wrong thing -- and stay green. Every test here
drives the REAL action against a fresh database, reads the stored rows back
with PyPika-built queries through ``erpclaw_lib.query`` on a connection from
``erpclaw_lib.db.get_connection``, and compares exact values. Money is
compared as exact ``Decimal`` strings, never estimated, never cast.

Existing tests read before writing anything (kept untouched in
``test_ai_engine.py``):

- ``TestGetForecast.test_get_forecast_latest`` only asserts the call is ok.
- ``TestListCorrelations.test_list_correlations`` only asserts the call is ok.
- ``score-relationship`` and ``list-relationship-scores`` have no module
  test at all, only contract routability checks.

Per-action depth (stored row vs ledger effect):

- score-relationship: stored row. The new relationship_score row is read back
  with every score, the trend, the lifetime value and the factors pinned
  exactly; sibling tables are snapshotted to prove nothing else moved. This
  action reaches no ledger, so no ledger assertion can hold for it.
- list-relationship-scores: stored row. The listed rows are compared field by
  field against the stored relationship_score rows, including the company
  filter; the call is read-only so the snapshot is the no-write proof and no
  ledger assertion can hold for it.
- get-forecast: stored row. The returned forecasts are compared field by
  field against the stored cash_flow_forecast rows written by
  forecast-cash-flow, with every balance pinned as an exact Decimal string.
  The call is read-only so the snapshot is the no-write proof and no ledger
  assertion can hold for it.
- list-correlations: stored row. The listed rows are compared field by field
  against the stored correlation rows written by discover-correlations. The
  call is read-only so the snapshot is the no-write proof and no ledger
  assertion can hold for it.

No test in this file inspects catalog tables directly or sets connection
options; catalog questions go through ``erpclaw_lib.seam`` and every row read
is a PyPika-built query run on a connection from
``erpclaw_lib.db.get_connection``.
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import ai_helpers  # noqa: E402  (binds the tree under test for erpclaw_lib)
from ai_helpers import (  # noqa: E402
    call_action,
    is_error,
    is_ok,
    load_db_query,
    ns,
    seed_accounts,
    seed_company,
    seed_customer,
    seed_gl_entries,
)

from erpclaw_lib import seam  # noqa: E402
from erpclaw_lib.db import get_connection  # noqa: E402
from erpclaw_lib.query import Field, P, Q, Table, fn, insert_row  # noqa: E402

MOD = load_db_query()


@pytest.fixture
def db_path(tmp_path):
    """Per-test fresh database with foundation plus growth schema."""
    path = str(tmp_path / "test.sqlite")
    ai_helpers.init_all_tables(path)
    os.environ["ERPCLAW_DB_PATH"] = path
    yield path
    os.environ.pop("ERPCLAW_DB_PATH", None)


@pytest.fixture
def conn(db_path):
    """Per-test connection opened through the shared seam helper."""
    connection = get_connection(db_path)
    yield connection
    connection.close()


def _today():
    """Current UTC date, the same clock the actions stamp rows with."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _insert(conn, table, row):
    """Insert one row with a PyPika-built statement and commit."""
    sql, _columns = insert_row(table, {key: P() for key in row})
    conn.execute(sql, tuple(row.values()))
    conn.commit()


def _all(conn, table):
    """Every row of a table in stable id order, as plain dicts."""
    tab = Table(table)
    query = Q.from_(tab).select(tab.star).orderby(tab.id)
    return [dict(found) for found in conn.execute(query.get_sql()).fetchall()]


def _snapshot(conn, tables):
    """Full content snapshot used as the write-nothing proof."""
    return {name: _all(conn, name) for name in tables}


def _by_id(rows):
    """Index response or stored rows by id for order-free comparison."""
    return {entry["id"]: entry for entry in rows}


def _count(conn, table):
    """Exact row count through a PyPika-built aggregate."""
    tab = Table(table)
    query = Q.from_(tab).select(fn.Count("*").as_("total"))
    return conn.execute(query.get_sql()).fetchone()["total"]


def _seed_supplier(conn, company_id, name="Depth Supplier"):
    """Insert a supplier and return its id."""
    supplier_id = str(uuid.uuid4())
    _insert(conn, "supplier", {
        "id": supplier_id,
        "name": "%s %s" % (name, supplier_id[:6]),
        "company_id": company_id,
    })
    return supplier_id


def _seed_sales_invoice(conn, company_id, customer_id, posting_date,
                        due_date, grand_total, outstanding,
                        status="submitted"):
    """Insert a sales invoice carrying exact Decimal-string money."""
    invoice_id = str(uuid.uuid4())
    _insert(conn, "sales_invoice", {
        "id": invoice_id,
        "customer_id": customer_id,
        "posting_date": posting_date,
        "due_date": due_date,
        "grand_total": grand_total,
        "outstanding_amount": outstanding,
        "status": status,
        "company_id": company_id,
    })
    return invoice_id


def _seed_purchase_invoice(conn, company_id, supplier_id, posting_date,
                           due_date, grand_total, outstanding,
                           status="submitted"):
    """Insert a purchase invoice carrying exact Decimal-string money."""
    invoice_id = str(uuid.uuid4())
    _insert(conn, "purchase_invoice", {
        "id": invoice_id,
        "supplier_id": supplier_id,
        "posting_date": posting_date,
        "due_date": due_date,
        "grand_total": grand_total,
        "outstanding_amount": outstanding,
        "status": status,
        "company_id": company_id,
    })
    return invoice_id


def test_owned_tables_exist_through_seam(db_path):
    """Catalog evidence for the three tables these tests read back."""
    assert seam.table_exists("relationship_score", db_path)
    assert seam.table_exists("correlation", db_path)
    assert seam.table_exists("cash_flow_forecast", db_path)


class TestScoreRelationshipDepth:
    """score-relationship pins the stored relationship_score row exactly."""

    def test_score_customer_with_history_writes_exact_row(self, conn):
        company_id = seed_company(conn)
        customer_id = seed_customer(conn, company_id)
        today = _today()
        self._seed_paid_pair(conn, company_id, customer_id, today)
        before = _snapshot(
            conn,
            ("relationship_score", "sales_invoice", "customer", "gl_entry"))

        result = call_action(MOD.score_relationship, conn, ns(
            party_type="customer", party_id=customer_id))
        assert is_ok(result), result

        after = _snapshot(
            conn,
            ("relationship_score", "sales_invoice", "customer", "gl_entry"))
        assert len(after["relationship_score"]) == (
            len(before["relationship_score"]) + 1)
        stored = _by_id(after["relationship_score"])[
            result["relationship_score"]["id"]]
        assert result["relationship_score"] == stored
        assert stored["party_type"] == "customer"
        assert stored["party_id"] == customer_id
        assert stored["score_date"] == today
        assert stored["overall_score"] == "90"
        assert stored["payment_score"] == "100"
        assert stored["volume_trend"] == "growing"
        assert stored["profitability_score"] == "70"
        assert stored["risk_score"] == "100"
        assert stored["lifetime_value"] == "300.00"
        assert Decimal(stored["lifetime_value"]) == Decimal("300.00")
        assert json.loads(stored["factors"]) == {
            "payment_score": 100,
            "volume_score": 90,
            "profitability_score": 70,
            "risk_score": 100,
            "total_invoices": 2,
            "overdue_invoices": 0,
            "volume_trend": "growing",
        }
        assert stored["ai_summary"] == (
            "Customer relationship score: 90/100. "
            "consistent payment history, growing transaction volume, "
            "low risk profile.")
        assert after["sales_invoice"] == before["sales_invoice"]
        assert after["customer"] == before["customer"]
        assert after["gl_entry"] == before["gl_entry"]

    def test_score_customer_without_history_writes_defaults(self, conn):
        company_id = seed_company(conn)
        customer_id = seed_customer(conn, company_id)
        today = _today()
        before_count = _count(conn, "relationship_score")

        result = call_action(MOD.score_relationship, conn, ns(
            party_type="customer", party_id=customer_id))
        assert is_ok(result), result

        assert _count(conn, "relationship_score") == before_count + 1
        stored = _by_id(_all(conn, "relationship_score"))[
            result["relationship_score"]["id"]]
        assert result["relationship_score"] == stored
        assert stored["score_date"] == today
        assert stored["overall_score"] == "50"
        assert stored["payment_score"] == "50"
        assert stored["volume_trend"] == "stable"
        assert stored["profitability_score"] == "50"
        assert stored["risk_score"] == "50"
        assert stored["lifetime_value"] == "0"
        assert json.loads(stored["factors"]) == {
            "note": "No transaction history"}
        assert stored["ai_summary"] == (
            "No transaction history available for scoring.")

    def test_score_refusal_invalid_party_type_writes_nothing(self, conn):
        company_id = seed_company(conn)
        customer_id = seed_customer(conn, company_id)
        tables = ("relationship_score", "sales_invoice", "customer")
        before = _snapshot(conn, tables)

        result = call_action(MOD.score_relationship, conn, ns(
            party_type="partner", party_id=customer_id))
        assert is_error(result), result
        assert "partner" in result["message"]
        assert "customer" in result["message"]
        assert "supplier" in result["message"]
        assert _snapshot(conn, tables) == before

    def test_score_refusal_unknown_party_writes_nothing(self, conn):
        company_id = seed_company(conn)
        seed_customer(conn, company_id)
        missing_id = str(uuid.uuid4())
        tables = ("relationship_score", "sales_invoice", "customer")
        before = _snapshot(conn, tables)

        result = call_action(MOD.score_relationship, conn, ns(
            party_type="customer", party_id=missing_id))
        assert is_error(result), result
        assert missing_id in result["message"]
        assert _snapshot(conn, tables) == before

    def _seed_paid_pair(self, conn, company_id, customer_id, today):
        """Two settled invoices of 100.00 plus 200.00 posted today.

        Settled means no overdue leg, so payment is 100 and risk is 100;
        posting today means all volume is recent, so the trend is growing
        and the lifetime value is exactly 300.00.
        """
        self._seed_invoice(
            conn, company_id, customer_id, today, "100.00")
        self._seed_invoice(
            conn, company_id, customer_id, today, "200.00")

    def _seed_invoice(self, conn, company_id, customer_id, today, total):
        return _seed_sales_invoice(
            conn, company_id, customer_id, today, today, total, "0.00")


class TestListRelationshipScoresDepth:
    """list-relationship-scores returns the stored rows, nothing more."""

    def test_list_returns_exact_stored_rows_and_company_filter(
            self, conn):
        first_company = seed_company(conn)
        first_customer = seed_customer(conn, first_company, name="First Cust")
        today = _today()
        _seed_sales_invoice(conn, first_company, first_customer, today,
                            today, "150.00", "0.00")
        scored_first = call_action(MOD.score_relationship, conn, ns(
            party_type="customer", party_id=first_customer))
        assert is_ok(scored_first), scored_first
        second_customer = seed_customer(
            conn, first_company, name="Second Cust")
        scored_second = call_action(MOD.score_relationship, conn, ns(
            party_type="customer", party_id=second_customer))
        assert is_ok(scored_second), scored_second
        other_company = seed_company(conn)
        other_customer = seed_customer(conn, other_company, name="Other Cust")
        scored_other = call_action(MOD.score_relationship, conn, ns(
            party_type="customer", party_id=other_customer))
        assert is_ok(scored_other), scored_other
        before = _snapshot(conn, ("relationship_score", "customer"))

        result = call_action(MOD.list_relationship_scores, conn, ns(
            party_type=None, company_id=None, limit="20", offset="0"))
        assert is_ok(result), result
        assert result["total_count"] == 3
        assert result["has_more"] is False
        stored = _by_id(_all(conn, "relationship_score"))
        assert _by_id(result["relationship_scores"]) == stored
        assert stored[scored_first["relationship_score"]["id"]][
            "lifetime_value"] == "150.00"
        assert stored[scored_second["relationship_score"]["id"]][
            "overall_score"] == "50"

        filtered = call_action(MOD.list_relationship_scores, conn, ns(
            party_type=None, company_id=first_company,
            limit="20", offset="0"))
        assert is_ok(filtered), filtered
        assert filtered["total_count"] == 2
        assert set(entry["id"]
                   for entry in filtered["relationship_scores"]) == {
            scored_first["relationship_score"]["id"],
            scored_second["relationship_score"]["id"],
        }
        assert _snapshot(conn, ("relationship_score", "customer")) == before

    def test_list_empty_for_unknown_company_writes_nothing(self, conn):
        # NOTE: this read path exposes no input-validation branch, so a
        # refusal assertion cannot hold; the truthful empty set plus the
        # unchanged snapshot is the evidence a bad filter writes nothing.
        company_id = seed_company(conn)
        customer_id = seed_customer(conn, company_id)
        scored = call_action(MOD.score_relationship, conn, ns(
            party_type="customer", party_id=customer_id))
        assert is_ok(scored), scored
        tables = ("relationship_score", "customer")
        before = _snapshot(conn, tables)

        result = call_action(MOD.list_relationship_scores, conn, ns(
            party_type=None, company_id=str(uuid.uuid4()),
            limit="20", offset="0"))
        assert is_ok(result), result
        assert result["relationship_scores"] == []
        assert result["total_count"] == 0
        assert _snapshot(conn, tables) == before


class TestGetForecastDepth:
    """get-forecast returns the stored cash_flow_forecast rows exactly."""

    def test_get_forecast_returns_exact_stored_rows(self, conn):
        company_id = seed_company(conn)
        accounts = seed_accounts(conn, company_id)
        seed_gl_entries(conn, company_id, accounts)
        customer_id = seed_customer(conn, company_id, name="Forecast Cust")
        _seed_sales_invoice(conn, company_id, customer_id, "2026-01-05",
                            "2026-02-15", "120.00", "120.00")
        supplier_id = _seed_supplier(conn, company_id)
        _seed_purchase_invoice(conn, company_id, supplier_id, "2026-01-06",
                               "2026-02-15", "50.00", "50.00")
        today = _today()
        written = call_action(MOD.forecast_cash_flow, conn, ns(
            company_id=company_id, horizon_days="30",
            from_date=None, to_date=None))
        assert is_ok(written), written
        assert written["total_ar"] == "120.00"
        assert written["total_ap"] == "50.00"
        assert written["starting_balance"] == "45000.00"
        assert written["scenarios"] == {
            "pessimistic": "45024.00",
            "expected": "45058.00",
            "optimistic": "45080.00",
        }
        tables = ("cash_flow_forecast", "gl_entry", "sales_invoice",
                  "purchase_invoice")
        before = _snapshot(conn, tables)

        result = call_action(MOD.get_forecast, conn, ns(
            company_id=company_id))
        assert is_ok(result), result
        assert result["count"] == 3
        stored = _by_id(_all(conn, "cash_flow_forecast"))
        assert set(entry["id"]
                   for entry in result["forecasts"]) == set(stored)
        assert _by_id(result["forecasts"]) == stored
        expected_balances = {
            "pessimistic": "45024.00",
            "expected": "45058.00",
            "optimistic": "45080.00",
        }
        expected_multipliers = {
            "pessimistic": ("0.7", "1.2"),
            "expected": ("0.9", "1.0"),
            "optimistic": ("1.0", "0.8"),
        }
        for entry in result["forecasts"]:
            scenario = entry["scenario"]
            assert entry["forecast_date"] == today
            assert entry["horizon_days"] == 30
            assert entry["starting_balance"] == "45000.00"
            assert Decimal(entry["starting_balance"]) == Decimal("45000.00")
            assert entry["projected_balance"] == expected_balances[scenario]
            assert Decimal(entry["projected_balance"]) == Decimal(
                expected_balances[scenario])
            assert json.loads(entry["projected_inflows"]) == [
                {"date": "2026-02-15", "amount": "120.00"}]
            assert json.loads(entry["projected_outflows"]) == [
                {"date": "2026-02-15", "amount": "50.00"}]
            assert json.loads(entry["confidence_interval"]) == {
                "low": "45024.00",
                "mid": "45058.00",
                "high": "45080.00",
            }
            inflow_mult, outflow_mult = expected_multipliers[scenario]
            assert json.loads(entry["assumptions"]) == {
                "company_id": company_id,
                "inflow_multiplier": inflow_mult,
                "outflow_multiplier": outflow_mult,
            }
        assert _snapshot(conn, tables) == before

    def test_get_forecast_empty_company_reports_truthfully(self, conn):
        # NOTE: this read path exposes no input-validation branch, so a
        # refusal assertion cannot hold; the truthful empty message plus the
        # unchanged snapshot is the evidence a miss writes nothing.
        company_id = seed_company(conn)
        tables = ("cash_flow_forecast", "gl_entry")
        before = _snapshot(conn, tables)

        result = call_action(MOD.get_forecast, conn, ns(
            company_id=company_id))
        assert is_ok(result), result
        assert result["forecasts"] == []
        assert result["message"] == "No forecasts found"
        assert _snapshot(conn, tables) == before


class TestListCorrelationsDepth:
    """list-correlations returns the stored correlation rows exactly."""

    def test_list_returns_exact_discovered_row(self, conn):
        company_id = seed_company(conn)
        customer_id = seed_customer(conn, company_id, name="Ratio Cust")
        supplier_id = _seed_supplier(conn, company_id, name="Ratio Supplier")
        _seed_sales_invoice(conn, company_id, customer_id, "2026-02-01",
                            "2026-02-20", "1000.00", "1000.00")
        _seed_purchase_invoice(conn, company_id, supplier_id, "2026-02-01",
                               "2026-02-20", "800.00", "800.00")
        found = call_action(MOD.discover_correlations, conn, ns(
            company_id=company_id, from_date="2026-01-01",
            to_date="2026-03-31"))
        assert is_ok(found), found
        assert found["correlations_discovered"] == 1
        tables = ("correlation", "sales_invoice", "purchase_invoice")
        before = _snapshot(conn, tables)

        result = call_action(MOD.list_correlations, conn, ns(
            company_id=company_id, min_strength=None,
            limit="20", offset="0"))
        assert is_ok(result), result
        assert result["total_count"] == 1
        assert result["has_more"] is False
        stored = _by_id(_all(conn, "correlation"))
        assert found["correlation_ids"] == list(stored)
        assert _by_id(result["correlations"]) == stored
        entry = result["correlations"][0]
        assert entry["module_a"] == "selling"
        assert entry["module_b"] == "buying"
        assert entry["description"] == (
            "Sales-to-purchase ratio of 0.80 detected. "
            "Sales: $1000.00, Purchases: $800.00")
        evidence = json.loads(entry["evidence"])
        assert evidence == {
            "company_id": company_id,
            "sales_total": "1000.00",
            "purchases_total": "800.00",
            "ratio": "0.80",
        }
        assert Decimal(evidence["sales_total"]) == Decimal("1000.00")
        assert Decimal(evidence["purchases_total"]) == Decimal("800.00")
        assert entry["strength"] == "strong"
        assert entry["statistical_confidence"] == "10"
        assert entry["actionable"] == 1
        assert entry["suggested_action"] == (
            "Review procurement efficiency relative to sales volume")
        assert entry["status"] == "new"

        strong_only = call_action(MOD.list_correlations, conn, ns(
            company_id=company_id, min_strength="strong",
            limit="20", offset="0"))
        assert is_ok(strong_only), strong_only
        assert strong_only["total_count"] == 1
        assert _snapshot(conn, tables) == before

    def test_list_empty_for_unknown_company_writes_nothing(self, conn):
        # NOTE: this read path exposes no input-validation branch, so a
        # refusal assertion cannot hold; the truthful empty set plus the
        # unchanged snapshot is the evidence a bad filter writes nothing.
        company_id = seed_company(conn)
        customer_id = seed_customer(conn, company_id, name="Quiet Cust")
        supplier_id = _seed_supplier(conn, company_id, name="Quiet Supplier")
        _seed_sales_invoice(conn, company_id, customer_id, "2026-02-01",
                            "2026-02-20", "1000.00", "1000.00")
        _seed_purchase_invoice(conn, company_id, supplier_id, "2026-02-01",
                               "2026-02-20", "800.00", "800.00")
        found = call_action(MOD.discover_correlations, conn, ns(
            company_id=company_id, from_date="2026-01-01",
            to_date="2026-03-31"))
        assert is_ok(found), found
        tables = ("correlation", "sales_invoice", "purchase_invoice")
        before = _snapshot(conn, tables)

        result = call_action(MOD.list_correlations, conn, ns(
            company_id=str(uuid.uuid4()), min_strength=None,
            limit="20", offset="0"))
        assert is_ok(result), result
        assert result["correlations"] == []
        assert result["total_count"] == 0
        assert _snapshot(conn, tables) == before
