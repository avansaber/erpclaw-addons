"""Tests for erpclaw-integrations-stripe reconciliation engine.

Covers all 3 reconciliation layers and 8 actions:
  Layer 1: balance_txn -> source matching
  Layer 2: charge -> customer matching
  Layer 3: payout -> constituent verification
  Plus: run-reconciliation lifecycle, manual match/unmatch,
        list-unreconciled, reconciliation-summary
"""
import os
import sys
from decimal import Decimal

# Ensure test helpers and scripts are importable
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_test_helpers import (
    call_action, ns, is_ok, is_error,
    seed_stripe_account, seed_erpclaw_customer, build_stripe_env, _uuid,
)
from reconciliation import ACTIONS
from stripe_helpers import now_iso


# ---------------------------------------------------------------------------
# Seed helpers for reconciliation tests
# ---------------------------------------------------------------------------

def _seed_charge(conn, stripe_account_id, company_id, stripe_id,
                 customer_stripe_id="", amount="50.00",
                 erpclaw_customer_id=None):
    """Insert a stripe_charge row."""
    charge_id = _uuid()
    now = now_iso()
    conn.execute(
        """INSERT INTO stripe_charge
            (id, stripe_id, stripe_account_id, amount, currency,
             customer_stripe_id, erpclaw_customer_id, status,
             amount_refunded, disputed,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, 'usd', ?, ?, 'succeeded', '0', 0, ?, ?, ?)""",
        (charge_id, stripe_id, stripe_account_id, amount,
         customer_stripe_id, erpclaw_customer_id, company_id, now, now)
    )
    conn.commit()
    return charge_id


def _seed_refund(conn, stripe_account_id, company_id, stripe_id,
                 charge_stripe_id="", amount="10.00"):
    """Insert a stripe_refund row."""
    refund_id = _uuid()
    now = now_iso()
    conn.execute(
        """INSERT INTO stripe_refund
            (id, stripe_id, stripe_account_id, charge_stripe_id,
             amount, currency, status,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, ?, 'usd', 'succeeded', ?, ?, ?)""",
        (refund_id, stripe_id, stripe_account_id, charge_stripe_id,
         amount, company_id, now, now)
    )
    conn.commit()
    return refund_id


def _seed_balance_txn(conn, stripe_account_id, company_id, stripe_id,
                      source_id, amount="50.00", fee="1.50", net="48.50",
                      payout_id=None, bt_type="charge", reconciled=0):
    """Insert a stripe_balance_transaction row."""
    bt_id = _uuid()
    now = now_iso()
    conn.execute(
        """INSERT INTO stripe_balance_transaction
            (id, stripe_id, stripe_account_id, type, source_id, source_type,
             amount, fee, net, currency, status, reconciled, payout_id,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, ?, ?,
             ?, ?, ?, 'usd', 'available', ?, ?,
             ?, ?, ?)""",
        (bt_id, stripe_id, stripe_account_id, bt_type, source_id, bt_type,
         amount, fee, net, reconciled, payout_id,
         company_id, now, now)
    )
    conn.commit()
    return bt_id


def _seed_payout(conn, stripe_account_id, company_id, stripe_id,
                 amount="50.00", reconciled=0):
    """Insert a stripe_payout row."""
    payout_id = _uuid()
    now = now_iso()
    conn.execute(
        """INSERT INTO stripe_payout
            (id, stripe_id, stripe_account_id, amount, currency,
             status, reconciled, transaction_count,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, 'usd', 'paid', ?, 0, ?, ?, ?)""",
        (payout_id, stripe_id, stripe_account_id, amount,
         reconciled, company_id, now, now)
    )
    conn.commit()
    return payout_id


def _seed_customer_map(conn, stripe_account_id, company_id,
                       stripe_customer_id, erpclaw_customer_id=None):
    """Insert a stripe_customer_map row."""
    map_id = _uuid()
    now = now_iso()
    conn.execute(
        """INSERT INTO stripe_customer_map
            (id, stripe_account_id, stripe_customer_id,
             erpclaw_customer_id, stripe_email, stripe_name,
             match_method, match_confidence, company_id, created_at)
           VALUES (?, ?, ?, ?, '', '', 'manual', '1.0', ?, ?)""",
        (map_id, stripe_account_id, stripe_customer_id,
         erpclaw_customer_id, company_id, now)
    )
    conn.commit()
    return map_id


def _seed_sales_invoice(conn, company_id, amount="50.00"):
    """Insert a minimal sales_invoice row for match testing.

    Also creates a customer since sales_invoice has FK to customer.
    """
    cust_id = _uuid()
    now = now_iso()
    conn.execute(
        """INSERT INTO customer (id, name, company_id, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?)""",
        (cust_id, "Invoice Test Customer", company_id, now, now)
    )

    inv_id = _uuid()
    conn.execute(
        """INSERT INTO sales_invoice
            (id, company_id, customer_id, posting_date, due_date,
             total_amount, outstanding_amount, status, currency,
             created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'draft', 'USD', ?, ?)""",
        (inv_id, company_id, cust_id, now, now,
         amount, amount, now, now)
    )
    conn.commit()
    return inv_id


# ===========================================================================
# 1. test_layer1_source_matching
# ===========================================================================
class TestLayer1SourceMatching:

    def test_layer1_source_matching(self, conn):
        """Layer 1: balance_txn with source_id=ch_xxx should match to charge."""
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        co = env["company_id"]

        # Seed charge and balance_txn pointing to it
        _seed_charge(conn, acct, co, "ch_l1_001", amount="25.00")
        _seed_balance_txn(conn, acct, co, "txn_l1_001", "ch_l1_001",
                          amount="25.00", fee="0.75", net="24.25")

        result = call_action(ACTIONS["stripe-run-reconciliation"], conn, ns(
            stripe_account_id=acct,
            date_from=None,
            date_to=None,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["layer1_source_matching"]["matched"] == 1

        # Verify balance_txn is now reconciled
        bt = conn.execute(
            "SELECT reconciled FROM stripe_balance_transaction WHERE stripe_id = 'txn_l1_001'"
        ).fetchone()
        assert bt["reconciled"] == 1


# ===========================================================================
# 2. test_layer2_charge_to_customer
# ===========================================================================
class TestLayer2ChargeToCustomer:

    def test_layer2_charge_to_customer(self, conn):
        """Layer 2: charge with customer_stripe_id should match via customer_map."""
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        co = env["company_id"]

        # Seed erpclaw customer + mapping
        erpclaw_cust = seed_erpclaw_customer(conn, co, name="L2 Customer")
        _seed_customer_map(conn, acct, co, "cus_l2_001", erpclaw_customer_id=erpclaw_cust)

        # Seed charge with the stripe customer ID but no erpclaw customer yet
        _seed_charge(conn, acct, co, "ch_l2_001",
                     customer_stripe_id="cus_l2_001", amount="75.00")

        result = call_action(ACTIONS["stripe-run-reconciliation"], conn, ns(
            stripe_account_id=acct,
            date_from=None,
            date_to=None,
        ))
        assert is_ok(result)
        assert result["layer2_charge_matching"]["matched"] == 1

        # Verify charge now has erpclaw_customer_id
        charge = conn.execute(
            "SELECT erpclaw_customer_id FROM stripe_charge WHERE stripe_id = 'ch_l2_001'"
        ).fetchone()
        assert charge["erpclaw_customer_id"] == erpclaw_cust


# ===========================================================================
# 3. test_layer2_charge_unmatched
# ===========================================================================
class TestLayer2ChargeUnmatched:

    def test_layer2_charge_unmatched(self, conn):
        """Layer 2: charge with no customer mapping should remain unmatched."""
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        co = env["company_id"]

        # Seed charge with a customer ID that has no mapping
        _seed_charge(conn, acct, co, "ch_l2_nomatch",
                     customer_stripe_id="cus_unknown_999", amount="30.00")

        result = call_action(ACTIONS["stripe-run-reconciliation"], conn, ns(
            stripe_account_id=acct,
            date_from=None,
            date_to=None,
        ))
        assert is_ok(result)
        assert result["layer2_charge_matching"]["total"] == 1
        assert result["layer2_charge_matching"]["matched"] == 0


# ===========================================================================
# 4. test_layer3_payout_sum_matches
# ===========================================================================
class TestLayer3PayoutSumMatches:

    def test_layer3_payout_sum_matches(self, conn):
        """Layer 3: payout amount should equal SUM(balance_txn.net) for constituents."""
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        co = env["company_id"]

        # Seed payout for $100
        _seed_payout(conn, acct, co, "po_l3_001", amount="100.00")

        # Seed 2 balance transactions that sum to $100 net
        _seed_balance_txn(conn, acct, co, "txn_l3_001", "ch_l3_a",
                          amount="60.00", fee="1.80", net="58.20",
                          payout_id="po_l3_001")
        _seed_balance_txn(conn, acct, co, "txn_l3_002", "ch_l3_b",
                          amount="43.00", fee="1.20", net="41.80",
                          payout_id="po_l3_001")

        result = call_action(ACTIONS["stripe-run-reconciliation"], conn, ns(
            stripe_account_id=acct,
            date_from=None,
            date_to=None,
        ))
        assert is_ok(result)
        assert result["layer3_payout_verification"]["matched"] == 1
        assert result["layer3_payout_verification"]["mismatched"] == 0

        # Verify payout is reconciled
        payout = conn.execute(
            "SELECT reconciled, transaction_count FROM stripe_payout WHERE stripe_id = 'po_l3_001'"
        ).fetchone()
        assert payout["reconciled"] == 1
        assert payout["transaction_count"] == 2


# ===========================================================================
# 5. test_layer3_payout_sum_mismatch
# ===========================================================================
class TestLayer3PayoutSumMismatch:

    def test_layer3_payout_sum_mismatch(self, conn):
        """Layer 3: mismatched payout should not be reconciled."""
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        co = env["company_id"]

        # Seed payout for $100
        _seed_payout(conn, acct, co, "po_l3_bad", amount="100.00")

        # Seed only $50 worth of balance transactions (mismatch)
        _seed_balance_txn(conn, acct, co, "txn_l3_bad", "ch_l3_bad",
                          amount="52.00", fee="2.00", net="50.00",
                          payout_id="po_l3_bad")

        result = call_action(ACTIONS["stripe-run-reconciliation"], conn, ns(
            stripe_account_id=acct,
            date_from=None,
            date_to=None,
        ))
        assert is_ok(result)
        assert result["layer3_payout_verification"]["mismatched"] == 1
        assert result["layer3_payout_verification"]["matched"] == 0

        # Verify payout is NOT reconciled
        payout = conn.execute(
            "SELECT reconciled FROM stripe_payout WHERE stripe_id = 'po_l3_bad'"
        ).fetchone()
        assert payout["reconciled"] == 0


# ===========================================================================
# 6. test_reconciliation_run_lifecycle
# ===========================================================================
class TestReconciliationRunLifecycle:

    def test_reconciliation_run_lifecycle(self, conn):
        """Running reconciliation should create a run record and update it."""
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]

        result = call_action(ACTIONS["stripe-run-reconciliation"], conn, ns(
            stripe_account_id=acct,
            date_from=None,
            date_to=None,
        ))
        assert is_ok(result)
        run_id = result["reconciliation_run_id"]
        assert result["totals"] is not None

        # Fetch the run record
        get_result = call_action(ACTIONS["stripe-get-reconciliation-run"], conn, ns(
            reconciliation_run_id=run_id,
        ))
        assert is_ok(get_result)
        assert get_result["stripe_account_id"] == acct
        assert get_result["id"] == run_id

        # Should appear in list
        list_result = call_action(ACTIONS["stripe-list-reconciliation-runs"], conn, ns(
            stripe_account_id=acct,
            status=None,
            limit=50,
        ))
        assert is_ok(list_result)
        assert list_result["count"] >= 1
        run_ids = {r["id"] for r in list_result["reconciliation_runs"]}
        assert run_id in run_ids


# ===========================================================================
# 7. test_manual_match_charge
# ===========================================================================
class TestManualMatchCharge:

    def test_manual_match_charge(self, conn):
        """Manually matching a charge to an invoice should update the charge."""
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        co = env["company_id"]

        _seed_charge(conn, acct, co, "ch_match001", amount="200.00")
        inv_id = _seed_sales_invoice(conn, co, amount="200.00")

        result = call_action(ACTIONS["stripe-match-charge"], conn, ns(
            charge_stripe_id="ch_match001",
            erpclaw_invoice_id=inv_id,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["charge_stripe_id"] == "ch_match001"
        assert result["erpclaw_invoice_id"] == inv_id

        # Verify in DB
        charge = conn.execute(
            "SELECT erpclaw_invoice_id FROM stripe_charge WHERE stripe_id = 'ch_match001'"
        ).fetchone()
        assert charge["erpclaw_invoice_id"] == inv_id


# ===========================================================================
# 8. test_unmatch_charge
# ===========================================================================
class TestUnmatchCharge:

    def test_unmatch_charge(self, conn):
        """Unmatching a charge should clear the invoice link."""
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        co = env["company_id"]

        _seed_charge(conn, acct, co, "ch_unmatch001", amount="150.00")
        inv_id = _seed_sales_invoice(conn, co, amount="150.00")

        # First match
        call_action(ACTIONS["stripe-match-charge"], conn, ns(
            charge_stripe_id="ch_unmatch001",
            erpclaw_invoice_id=inv_id,
        ))

        # Then unmatch
        result = call_action(ACTIONS["stripe-unmatch-charge"], conn, ns(
            charge_stripe_id="ch_unmatch001",
        ))
        assert is_ok(result)
        assert result["charge_stripe_id"] == "ch_unmatch001"
        assert result["erpclaw_invoice_id"] is None

        # Verify in DB
        charge = conn.execute(
            "SELECT erpclaw_invoice_id FROM stripe_charge WHERE stripe_id = 'ch_unmatch001'"
        ).fetchone()
        assert charge["erpclaw_invoice_id"] is None


# ===========================================================================
# 9. test_list_unreconciled
# ===========================================================================
class TestListUnreconciled:

    def test_list_unreconciled(self, conn):
        """Listing unreconciled should return only non-reconciled balance_txns."""
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        co = env["company_id"]

        # Seed 2 unreconciled and 1 reconciled
        _seed_balance_txn(conn, acct, co, "txn_unrec001", "ch_unrec001",
                          amount="10.00", fee="0.30", net="9.70", reconciled=0)
        _seed_balance_txn(conn, acct, co, "txn_unrec002", "ch_unrec002",
                          amount="20.00", fee="0.60", net="19.40", reconciled=0)
        _seed_balance_txn(conn, acct, co, "txn_rec001", "ch_rec001",
                          amount="30.00", fee="0.90", net="29.10", reconciled=1)

        result = call_action(ACTIONS["stripe-list-unreconciled"], conn, ns(
            stripe_account_id=acct,
            type=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(result)
        assert result["count"] == 2
        stripe_ids = {r["stripe_id"] for r in result["unreconciled"]}
        assert "txn_unrec001" in stripe_ids
        assert "txn_unrec002" in stripe_ids
        assert "txn_rec001" not in stripe_ids


# ===========================================================================
# 10. test_reconciliation_summary
# ===========================================================================
class TestReconciliationSummary:

    def test_reconciliation_summary(self, conn):
        """Summary should show correct counts and amounts for all categories."""
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        co = env["company_id"]

        erpclaw_cust = seed_erpclaw_customer(conn, co, name="Summary Customer")

        # Seed balance transactions: 1 reconciled, 2 unreconciled
        _seed_balance_txn(conn, acct, co, "txn_sum001", "ch_sum001",
                          amount="50.00", fee="1.50", net="48.50", reconciled=1)
        _seed_balance_txn(conn, acct, co, "txn_sum002", "ch_sum002",
                          amount="30.00", fee="0.90", net="29.10", reconciled=0)
        _seed_balance_txn(conn, acct, co, "txn_sum003", "ch_sum003",
                          amount="20.00", fee="0.60", net="19.40", reconciled=0)

        # Seed charges: 1 matched to erpclaw customer, 1 unmatched
        _seed_charge(conn, acct, co, "ch_sum_a",
                     customer_stripe_id="cus_sum001", amount="50.00",
                     erpclaw_customer_id=erpclaw_cust)
        _seed_charge(conn, acct, co, "ch_sum_b",
                     customer_stripe_id="cus_sum002", amount="30.00",
                     erpclaw_customer_id=None)

        # Seed payouts: 1 reconciled, 1 unreconciled
        _seed_payout(conn, acct, co, "po_sum001", amount="40.00", reconciled=1)
        _seed_payout(conn, acct, co, "po_sum002", amount="60.00", reconciled=0)

        result = call_action(ACTIONS["stripe-reconciliation-summary"], conn, ns(
            stripe_account_id=acct,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"

        bt = result["balance_transactions"]
        assert bt["matched_count"] == 1
        assert Decimal(bt["matched_amount"]) == Decimal("50.00")
        assert bt["unmatched_count"] == 2
        assert Decimal(bt["unmatched_amount"]) == Decimal("50.00")

        charges = result["charges"]
        assert charges["matched_count"] == 1
        assert Decimal(charges["matched_amount"]) == Decimal("50.00")
        assert charges["unmatched_count"] == 1
        assert Decimal(charges["unmatched_amount"]) == Decimal("30.00")

        payouts = result["payouts"]
        assert payouts["reconciled_count"] == 1
        assert Decimal(payouts["reconciled_amount"]) == Decimal("40.00")
        assert payouts["unreconciled_count"] == 1
        assert Decimal(payouts["unreconciled_amount"]) == Decimal("60.00")
