"""Tests for erpclaw-integrations-stripe sync engine actions.

Covers all 8 sync actions:
  stripe-start-sync, stripe-start-full-sync, stripe-get-sync-status,
  stripe-list-sync-jobs, stripe-cancel-sync, stripe-process-webhook,
  stripe-replay-webhook, stripe-list-webhook-events

All Stripe API calls are mocked — no real API calls are made in unit tests.
"""
import json
import os
import sys
from decimal import Decimal
from unittest.mock import patch, MagicMock, PropertyMock

# Ensure test helpers and scripts are importable
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_test_helpers import (
    call_action, ns, is_ok, is_error,
    seed_stripe_account, seed_erpclaw_customer, build_stripe_env,
)
from sync import ACTIONS
from stripe_helpers import cents_to_decimal


# ---------------------------------------------------------------------------
# Mock Stripe objects — simulate Stripe API responses
# ---------------------------------------------------------------------------

def _make_stripe_obj(data):
    """Create a mock Stripe object with attribute access from a dict."""
    obj = MagicMock()
    for k, v in data.items():
        if isinstance(v, dict):
            setattr(obj, k, _make_stripe_obj(v))
        elif isinstance(v, list):
            setattr(obj, k, [_make_stripe_obj(i) if isinstance(i, dict) else i for i in v])
        else:
            setattr(obj, k, v)
    # Also support dict-style .get()
    obj.get = lambda key, default=None: data.get(key, default)
    return obj


def _make_auto_paging_iter(items):
    """Create a mock auto_paging_iter() that yields items."""
    mock_list = MagicMock()
    mock_list.auto_paging_iter.return_value = iter(
        [_make_stripe_obj(i) for i in items]
    )
    return mock_list


MOCK_CHARGES = [
    {
        "id": "ch_test001",
        "amount": 699,  # $6.99 in cents
        "currency": "usd",
        "customer": "cus_test456",
        "status": "succeeded",
        "description": "Test charge one",
        "payment_method_types": ["card"],
        "payment_intent": "pi_test001",
        "invoice": None,
        "amount_refunded": 0,
        "disputed": False,
        "failure_code": None,
        "metadata": {},
        "created": 1760918400,
    },
    {
        "id": "ch_test002",
        "amount": 2500,  # $25.00
        "currency": "usd",
        "customer": "cus_test789",
        "status": "succeeded",
        "description": "Test charge two",
        "payment_method_types": ["card"],
        "payment_intent": "pi_test002",
        "invoice": "in_test001",
        "amount_refunded": 0,
        "disputed": False,
        "failure_code": None,
        "metadata": {"order_id": "ORD-123"},
        "created": 1760918500,
    },
    {
        "id": "ch_test003",
        "amount": 10050,  # $100.50
        "currency": "usd",
        "customer": "cus_test456",
        "status": "succeeded",
        "description": "Test charge three",
        "payment_method_types": ["card"],
        "payment_intent": "pi_test003",
        "invoice": None,
        "amount_refunded": 500,
        "disputed": False,
        "failure_code": None,
        "metadata": {},
        "created": 1760918600,
    },
]

MOCK_BALANCE_TXNS = [
    {
        "id": "txn_test001",
        "type": "charge",
        "reporting_category": "charge",
        "source": "ch_test001",
        "amount": 699,
        "fee": 50,
        "net": 649,
        "currency": "usd",
        "description": "Charge for test",
        "available_on": 1760918400,
        "created": 1760918400,
        "payout": None,
        "status": "available",
    },
    {
        "id": "txn_test002",
        "type": "charge",
        "reporting_category": "charge",
        "source": "ch_test002",
        "amount": 2500,
        "fee": 102,
        "net": 2398,
        "currency": "usd",
        "description": "Charge for order",
        "available_on": 1760918500,
        "created": 1760918500,
        "payout": "po_test001",
        "status": "available",
    },
]

MOCK_PAYOUTS = [
    {
        "id": "po_test001",
        "amount": 5000,  # $50.00
        "currency": "usd",
        "arrival_date": 1761004800,
        "method": "standard",
        "description": "STRIPE PAYOUT",
        "status": "paid",
        "failure_code": None,
        "destination": None,
        "created": 1760918400,
    },
]

MOCK_CUSTOMERS = [
    {
        "id": "cus_test456",
        "email": "alice@example.com",
        "name": "Alice Smith",
        "created": 1760918400,
    },
    {
        "id": "cus_test789",
        "email": "bob@example.com",
        "name": "Bob Jones",
        "created": 1760918500,
    },
]

MOCK_REFUNDS = [
    {
        "id": "re_test001",
        "charge": "ch_test003",
        "amount": 500,
        "currency": "usd",
        "reason": "requested_by_customer",
        "status": "succeeded",
        "metadata": {},
        "created": 1760918700,
    },
]

MOCK_DISPUTES = [
    {
        "id": "dp_test001",
        "charge": "ch_test001",
        "amount": 699,
        "currency": "usd",
        "reason": "fraudulent",
        "status": "needs_response",
        "evidence_due_by": 1761523200,
        "metadata": {},
        "created": 1760918800,
    },
]

MOCK_INVOICES = [
    {
        "id": "in_test001",
        "customer": "cus_test789",
        "number": "INV-0001",
        "amount_due": 2500,
        "amount_paid": 2500,
        "amount_remaining": 0,
        "currency": "usd",
        "status": "paid",
        "subscription": "sub_test001",
        "period_start": 1760918400,
        "period_end": 1763510400,
        "created": 1760918400,
    },
]

MOCK_SUBSCRIPTIONS = [
    {
        "id": "sub_test001",
        "customer": "cus_test789",
        "status": "active",
        "current_period_start": 1760918400,
        "current_period_end": 1763510400,
        "cancel_at_period_end": False,
        "canceled_at": None,
        "currency": "usd",
        "items": {"data": [{"price": {"unit_amount": 2500, "interval": "month"}}]},
        "created": 1760918400,
    },
]


def _build_mock_stripe(charges=None, balance_txns=None, payouts=None,
                       customers=None, refunds=None, disputes=None,
                       invoices=None, subscriptions=None):
    """Build a fully mocked stripe module with configurable list responses."""
    mock_stripe = MagicMock()

    mock_stripe.Charge.list.return_value = _make_auto_paging_iter(
        charges if charges is not None else [])
    mock_stripe.BalanceTransaction.list.return_value = _make_auto_paging_iter(
        balance_txns if balance_txns is not None else [])
    mock_stripe.Payout.list.return_value = _make_auto_paging_iter(
        payouts if payouts is not None else [])
    mock_stripe.Customer.list.return_value = _make_auto_paging_iter(
        customers if customers is not None else [])
    mock_stripe.Refund.list.return_value = _make_auto_paging_iter(
        refunds if refunds is not None else [])
    mock_stripe.Dispute.list.return_value = _make_auto_paging_iter(
        disputes if disputes is not None else [])
    mock_stripe.Invoice.list.return_value = _make_auto_paging_iter(
        invoices if invoices is not None else [])
    mock_stripe.Subscription.list.return_value = _make_auto_paging_iter(
        subscriptions if subscriptions is not None else [])

    # Mock api_key setter
    mock_stripe.api_key = None

    return mock_stripe


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

def _env_and_stripe(conn):
    """Build test environment and return (env, mock_stripe)."""
    env = build_stripe_env(conn)
    mock_stripe = _build_mock_stripe(
        charges=MOCK_CHARGES,
        balance_txns=MOCK_BALANCE_TXNS,
        payouts=MOCK_PAYOUTS,
        customers=MOCK_CUSTOMERS,
        refunds=MOCK_REFUNDS,
        disputes=MOCK_DISPUTES,
        invoices=MOCK_INVOICES,
        subscriptions=MOCK_SUBSCRIPTIONS,
    )
    return env, mock_stripe


# ===========================================================================
# Test 1: test_sync_charges_happy_path
# ===========================================================================
class TestSyncCharges:

    def test_sync_charges_happy_path(self, conn):
        """Syncing 3 charges should store all 3 in stripe_charge table."""
        env, mock_stripe = _env_and_stripe(conn)

        with patch.dict("sys.modules", {"stripe": mock_stripe}):
            result = call_action(ACTIONS["stripe-start-sync"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                object_type="charge",
            ))

        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["object_type"] == "charge"
        assert result["records_processed"] == 3

        # Verify all 3 charges are in the DB
        rows = conn.execute(
            "SELECT stripe_id, amount, status FROM stripe_charge WHERE stripe_account_id = ?",
            (env["stripe_account_id"],)
        ).fetchall()
        assert len(rows) == 3
        stripe_ids = {r["stripe_id"] for r in rows}
        assert stripe_ids == {"ch_test001", "ch_test002", "ch_test003"}


# ===========================================================================
# Test 2: test_sync_charges_cents_to_decimal
# ===========================================================================
class TestSyncCentsToDecimal:

    def test_sync_charges_cents_to_decimal(self, conn):
        """Verify 699 cents stores as '6.99' (TEXT Decimal), not '6.990...' float."""
        env, mock_stripe = _env_and_stripe(conn)

        with patch.dict("sys.modules", {"stripe": mock_stripe}):
            result = call_action(ACTIONS["stripe-start-sync"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                object_type="charge",
            ))

        assert is_ok(result)

        row = conn.execute(
            "SELECT amount FROM stripe_charge WHERE stripe_id = 'ch_test001'"
        ).fetchone()
        assert row is not None
        # Must be exact Decimal text — 699 cents = $6.99
        assert Decimal(row["amount"]) == Decimal("6.99")

        row2 = conn.execute(
            "SELECT amount FROM stripe_charge WHERE stripe_id = 'ch_test002'"
        ).fetchone()
        assert Decimal(row2["amount"]) == Decimal("25.00")

        row3 = conn.execute(
            "SELECT amount FROM stripe_charge WHERE stripe_id = 'ch_test003'"
        ).fetchone()
        assert Decimal(row3["amount"]) == Decimal("100.50")

        # Also verify amount_refunded
        refund_row = conn.execute(
            "SELECT amount_refunded FROM stripe_charge WHERE stripe_id = 'ch_test003'"
        ).fetchone()
        assert Decimal(refund_row["amount_refunded"]) == Decimal("5.00")


# ===========================================================================
# Test 3: test_sync_charges_idempotent
# ===========================================================================
class TestSyncIdempotent:

    def test_sync_charges_idempotent(self, conn):
        """Running sync twice should NOT create duplicate rows (INSERT OR REPLACE)."""
        env, mock_stripe = _env_and_stripe(conn)

        with patch.dict("sys.modules", {"stripe": mock_stripe}):
            r1 = call_action(ACTIONS["stripe-start-sync"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                object_type="charge",
            ))
        assert is_ok(r1)
        assert r1["records_processed"] == 3

        # Build fresh mock for second sync (same data)
        mock_stripe2 = _build_mock_stripe(charges=MOCK_CHARGES)
        with patch.dict("sys.modules", {"stripe": mock_stripe2}):
            r2 = call_action(ACTIONS["stripe-start-sync"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                object_type="charge",
            ))
        assert is_ok(r2)
        assert r2["records_processed"] == 3

        # Still only 3 rows — no duplicates
        count = conn.execute(
            "SELECT COUNT(*) as cnt FROM stripe_charge WHERE stripe_account_id = ?",
            (env["stripe_account_id"],)
        ).fetchone()["cnt"]
        assert count == 3


# ===========================================================================
# Test 4: test_sync_balance_transactions
# ===========================================================================
class TestSyncBalanceTransactions:

    def test_sync_balance_transactions(self, conn):
        """Syncing 2 balance transactions should store both with correct amounts."""
        env, mock_stripe = _env_and_stripe(conn)

        with patch.dict("sys.modules", {"stripe": mock_stripe}):
            result = call_action(ACTIONS["stripe-start-sync"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                object_type="balance_transaction",
            ))

        assert is_ok(result)
        assert result["records_processed"] == 2

        rows = conn.execute(
            "SELECT stripe_id, amount, fee, net FROM stripe_balance_transaction WHERE stripe_account_id = ?",
            (env["stripe_account_id"],)
        ).fetchall()
        assert len(rows) == 2

        # Verify txn_test001: 699 cents → $6.99, fee 50 cents → $0.50, net 649 → $6.49
        bt1 = [r for r in rows if r["stripe_id"] == "txn_test001"][0]
        assert Decimal(bt1["amount"]) == Decimal("6.99")
        assert Decimal(bt1["fee"]) == Decimal("0.50")
        assert Decimal(bt1["net"]) == Decimal("6.49")

        # Verify txn_test002: 2500 cents → $25.00, fee 102 cents → $1.02, net 2398 → $23.98
        bt2 = [r for r in rows if r["stripe_id"] == "txn_test002"][0]
        assert Decimal(bt2["amount"]) == Decimal("25.00")
        assert Decimal(bt2["fee"]) == Decimal("1.02")
        assert Decimal(bt2["net"]) == Decimal("23.98")


# ===========================================================================
# Test 5: test_sync_payouts
# ===========================================================================
class TestSyncPayouts:

    def test_sync_payouts(self, conn):
        """Syncing 1 payout should store it with correct amount."""
        env, mock_stripe = _env_and_stripe(conn)

        with patch.dict("sys.modules", {"stripe": mock_stripe}):
            result = call_action(ACTIONS["stripe-start-sync"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                object_type="payout",
            ))

        assert is_ok(result)
        assert result["records_processed"] == 1

        row = conn.execute(
            "SELECT stripe_id, amount, status FROM stripe_payout WHERE stripe_account_id = ?",
            (env["stripe_account_id"],)
        ).fetchone()
        assert row is not None
        assert row["stripe_id"] == "po_test001"
        assert Decimal(row["amount"]) == Decimal("50.00")
        assert row["status"] == "paid"


# ===========================================================================
# Test 6: test_sync_customers_auto_match
# ===========================================================================
class TestSyncCustomersAutoMatch:

    def test_sync_customers_auto_match(self, conn):
        """Syncing customers should auto-match by name to erpclaw customer."""
        env, mock_stripe = _env_and_stripe(conn)

        # Seed an erpclaw customer with matching name (customer table has no email)
        erpclaw_cust_id = seed_erpclaw_customer(
            conn, env["company_id"], name="Alice Smith"
        )

        with patch.dict("sys.modules", {"stripe": mock_stripe}):
            result = call_action(ACTIONS["stripe-start-sync"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                object_type="customer",
            ))

        assert is_ok(result)
        assert result["records_processed"] == 2

        # Check alice was matched by name
        alice = conn.execute(
            """SELECT erpclaw_customer_id, match_method, match_confidence
               FROM stripe_customer_map
               WHERE stripe_customer_id = 'cus_test456'
               AND stripe_account_id = ?""",
            (env["stripe_account_id"],)
        ).fetchone()
        assert alice is not None
        assert alice["erpclaw_customer_id"] == erpclaw_cust_id
        assert alice["match_method"] == "name"
        assert alice["match_confidence"] == "0.8"

        # Check bob was NOT matched (no erpclaw customer with bob's name)
        bob = conn.execute(
            """SELECT erpclaw_customer_id, match_method, match_confidence
               FROM stripe_customer_map
               WHERE stripe_customer_id = 'cus_test789'
               AND stripe_account_id = ?""",
            (env["stripe_account_id"],)
        ).fetchone()
        assert bob is not None
        assert bob["erpclaw_customer_id"] is None
        assert bob["match_method"] == "manual"
        assert bob["match_confidence"] == "0.0"


# ===========================================================================
# Test 7: test_sync_job_created_and_completed
# ===========================================================================
class TestSyncJobLifecycle:

    def test_sync_job_created_and_completed(self, conn):
        """Starting a sync should create a sync_job that ends in 'completed' status."""
        env, mock_stripe = _env_and_stripe(conn)

        with patch.dict("sys.modules", {"stripe": mock_stripe}):
            result = call_action(ACTIONS["stripe-start-sync"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                object_type="charge",
            ))

        assert is_ok(result)
        job_id = result["sync_job_id"]

        # Verify the sync job exists and is completed
        job = conn.execute(
            "SELECT * FROM stripe_sync_job WHERE id = ?",
            (job_id,)
        ).fetchone()
        assert job is not None
        assert job["status"] == "completed"
        assert job["object_type"] == "charge"
        assert job["records_processed"] == 3
        assert job["started_at"] is not None
        assert job["completed_at"] is not None

        # Also test get-sync-status action
        status_result = call_action(ACTIONS["stripe-get-sync-status"], conn, ns(
            sync_job_id=job_id,
        ))
        assert is_ok(status_result)
        assert status_result["sync_status"] == "completed"
        assert status_result["object_type"] == "charge"
        assert status_result["records_processed"] == 3


# ===========================================================================
# Test 8: test_sync_job_failed_on_error
# ===========================================================================
class TestSyncJobFailed:

    def test_sync_job_failed_on_error(self, conn):
        """If the Stripe API raises an error, the sync job should be 'failed'."""
        env, _ = _env_and_stripe(conn)

        # Create a mock stripe that raises on Charge.list
        mock_stripe_err = MagicMock()
        mock_stripe_err.api_key = None
        mock_stripe_err.Charge.list.side_effect = Exception("API rate limit exceeded")

        with patch.dict("sys.modules", {"stripe": mock_stripe_err}):
            result = call_action(ACTIONS["stripe-start-sync"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                object_type="charge",
            ))

        assert is_ok(result)
        job_id = result["sync_job_id"]
        # Records should be 0 since the API call failed
        assert result["records_processed"] == 0

        # Verify the sync job is marked failed
        job = conn.execute(
            "SELECT status, error_message FROM stripe_sync_job WHERE id = ?",
            (job_id,)
        ).fetchone()
        assert job["status"] == "failed"
        assert "API rate limit exceeded" in job["error_message"]


# ===========================================================================
# Test 9: test_full_sync_creates_multiple_jobs
# ===========================================================================
class TestFullSync:

    def test_full_sync_creates_multiple_jobs(self, conn):
        """Full sync should create one sync_job per object type (8 total)."""
        env, mock_stripe = _env_and_stripe(conn)

        with patch.dict("sys.modules", {"stripe": mock_stripe}):
            result = call_action(ACTIONS["stripe-start-full-sync"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
            ))

        assert is_ok(result)
        assert result["job_count"] == 8

        # Verify each object type got a job
        jobs = result["jobs"]
        object_types = {j["object_type"] for j in jobs}
        expected = {
            "customer", "charge", "refund", "dispute",
            "payout", "balance_transaction", "invoice", "subscription",
        }
        assert object_types == expected

        # Total records should be sum of all mocks
        # customers=2, charges=3, refunds=1, disputes=1, payouts=1,
        # balance_transactions=2, invoices=1, subscriptions=1 = 12
        assert result["total_records"] == 12

        # Verify jobs exist in DB
        db_jobs = conn.execute(
            "SELECT COUNT(*) as cnt FROM stripe_sync_job WHERE stripe_account_id = ?",
            (env["stripe_account_id"],)
        ).fetchone()["cnt"]
        assert db_jobs == 8


# ===========================================================================
# Test 10: test_list_sync_jobs_filters
# ===========================================================================
class TestListSyncJobs:

    def test_list_sync_jobs_filters(self, conn):
        """Listing sync jobs should support filtering by status and object_type."""
        env, mock_stripe = _env_and_stripe(conn)

        # Run two different syncs
        with patch.dict("sys.modules", {"stripe": mock_stripe}):
            call_action(ACTIONS["stripe-start-sync"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                object_type="charge",
            ))

        mock_stripe2 = _build_mock_stripe(payouts=MOCK_PAYOUTS)
        with patch.dict("sys.modules", {"stripe": mock_stripe2}):
            call_action(ACTIONS["stripe-start-sync"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                object_type="payout",
            ))

        # List all
        result = call_action(ACTIONS["stripe-list-sync-jobs"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
        ))
        assert is_ok(result)
        assert result["count"] == 2

        # Filter by object_type
        result_charges = call_action(ACTIONS["stripe-list-sync-jobs"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            object_type="charge",
        ))
        assert is_ok(result_charges)
        assert result_charges["count"] == 1
        assert result_charges["sync_jobs"][0]["object_type"] == "charge"

        # Filter by status
        result_completed = call_action(ACTIONS["stripe-list-sync-jobs"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            status="completed",
        ))
        assert is_ok(result_completed)
        assert result_completed["count"] == 2
