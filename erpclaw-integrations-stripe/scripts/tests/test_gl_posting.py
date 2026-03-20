"""Tests for erpclaw-integrations-stripe GL posting actions (Sprint S5).

12 tests covering charge, refund, dispute, payout, connect fee, and bulk GL posting.
All tests use erpclaw_lib.gl_posting (Article 6) — never direct INSERT to gl_entry.
"""
import os
import sys
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_test_helpers import (
    call_action, ns, is_ok, is_error,
    build_gl_ready_env, seed_charge, seed_balance_transaction,
    seed_refund, seed_dispute, seed_payout, seed_application_fee,
    seed_customer_map, seed_erpclaw_customer, seed_gl_account,
)
from gl_posting import ACTIONS


# ===========================================================================
# Helper: verify GL balance (debit == credit for a voucher)
# ===========================================================================
def _gl_entries_for_voucher(conn, voucher_id):
    """Fetch all active GL entries for a given voucher_id."""
    return conn.execute(
        "SELECT * FROM gl_entry WHERE voucher_id = ? AND is_cancelled = 0",
        (voucher_id,)
    ).fetchall()


def _assert_gl_balanced(conn, voucher_id):
    """Assert that total debits == total credits for a voucher."""
    entries = _gl_entries_for_voucher(conn, voucher_id)
    assert len(entries) > 0, f"No GL entries found for voucher {voucher_id}"
    total_debit = sum(Decimal(e["debit"]) for e in entries)
    total_credit = sum(Decimal(e["credit"]) for e in entries)
    assert total_debit == total_credit, (
        f"GL imbalanced: debit={total_debit}, credit={total_credit}"
    )
    return entries


# ===========================================================================
# 1. test_post_charge_gl_creates_payment_entry
# ===========================================================================
class TestPostChargeGL:

    def test_post_charge_gl_creates_payment_entry(self, conn, db_path):
        """Posting a charge GL should create a payment_entry row."""
        env = build_gl_ready_env(conn)
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_test_pe", amount="100.00")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_pe", source_id="ch_test_pe",
                                 amount="100.00", fee="3.20", net="96.80")

        result = call_action(ACTIONS["stripe-post-charge-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            charge_stripe_id="ch_test_pe",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"
        assert "payment_entry_id" in result

        # Verify payment_entry row exists
        pe = conn.execute(
            "SELECT * FROM payment_entry WHERE id = ?",
            (result["payment_entry_id"],)
        ).fetchone()
        assert pe is not None
        assert pe["payment_type"] == "receive"
        assert pe["status"] == "submitted"

    def test_post_charge_gl_entries_balance(self, conn, db_path):
        """GL entries for a charge must balance: total debits == total credits."""
        env = build_gl_ready_env(conn)
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_test_bal", amount="250.00")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_bal", source_id="ch_test_bal",
                                 amount="250.00", fee="7.55", net="242.45")

        result = call_action(ACTIONS["stripe-post-charge-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            charge_stripe_id="ch_test_bal",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result)
        _assert_gl_balanced(conn, result["payment_entry_id"])

    def test_post_charge_gl_fee_extracted(self, conn, db_path):
        """GL entries should include a separate debit for Stripe fees."""
        env = build_gl_ready_env(conn)
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_test_fee", amount="100.00")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_fee", source_id="ch_test_fee",
                                 amount="100.00", fee="3.20", net="96.80")

        result = call_action(ACTIONS["stripe-post-charge-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            charge_stripe_id="ch_test_fee",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result)
        assert result["fee"] == "3.20"
        assert result["net"] == "96.80"

        # Verify fee account has a debit entry
        entries = _gl_entries_for_voucher(conn, result["payment_entry_id"])
        fee_entries = [e for e in entries
                       if e["account_id"] == env["fees_id"]
                       and Decimal(e["debit"]) > 0]
        assert len(fee_entries) == 1
        assert Decimal(fee_entries[0]["debit"]) == Decimal("3.20")

    def test_post_charge_gl_with_customer(self, conn, db_path):
        """Charge with mapped customer should record customer on payment_entry."""
        env = build_gl_ready_env(conn)
        cust_id = seed_erpclaw_customer(conn, env["company_id"], "Acme Corp")
        seed_customer_map(conn, env["stripe_account_id"], env["company_id"],
                          stripe_customer_id="cus_acme",
                          erpclaw_customer_id=cust_id)
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_test_cust", amount="200.00",
                    customer_stripe_id="cus_acme")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_cust", source_id="ch_test_cust",
                                 amount="200.00", fee="6.10", net="193.90")

        result = call_action(ACTIONS["stripe-post-charge-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            charge_stripe_id="ch_test_cust",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result)
        assert result["customer_mapped"] is True

        # Customer info recorded on payment_entry
        pe = conn.execute(
            "SELECT party_type, party_id FROM payment_entry WHERE id = ?",
            (result["payment_entry_id"],)
        ).fetchone()
        assert pe["party_type"] == "customer"
        assert pe["party_id"] == cust_id

        # GL entries should still be balanced
        _assert_gl_balanced(conn, result["payment_entry_id"])

    def test_post_charge_gl_without_customer(self, conn, db_path):
        """Charge without customer mapping should CR Unearned Revenue."""
        env = build_gl_ready_env(conn)
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_test_nocu", amount="75.00")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_nocu", source_id="ch_test_nocu",
                                 amount="75.00", fee="2.48", net="72.52")

        result = call_action(ACTIONS["stripe-post-charge-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            charge_stripe_id="ch_test_nocu",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result)
        assert result["customer_mapped"] is False

        # CR entry should be on unearned revenue
        entries = _gl_entries_for_voucher(conn, result["payment_entry_id"])
        cr_entries = [e for e in entries
                      if e["account_id"] == env["unearned_id"]
                      and Decimal(e["credit"]) > 0]
        assert len(cr_entries) == 1
        assert Decimal(cr_entries[0]["credit"]) == Decimal("75.00")


# ===========================================================================
# 2. test_post_refund_gl
# ===========================================================================
class TestPostRefundGL:

    def test_post_refund_gl(self, conn, db_path):
        """Posting a refund GL should create a payment_entry (type=pay) and balanced GL."""
        env = build_gl_ready_env(conn)
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_for_refund", amount="100.00")
        seed_refund(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="re_test_gl", charge_stripe_id="ch_for_refund",
                    amount="50.00")

        result = call_action(ACTIONS["stripe-post-refund-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            refund_stripe_id="re_test_gl",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"
        assert "payment_entry_id" in result
        assert result["refund_amount"] == "50.00"

        # Verify payment type
        pe = conn.execute(
            "SELECT payment_type FROM payment_entry WHERE id = ?",
            (result["payment_entry_id"],)
        ).fetchone()
        assert pe["payment_type"] == "pay"

        _assert_gl_balanced(conn, result["payment_entry_id"])


# ===========================================================================
# 3. test_post_dispute_gl
# ===========================================================================
class TestPostDisputeGL:

    def test_post_dispute_gl_open(self, conn, db_path):
        """Posting an open dispute should hold funds: DR Dispute + Fee, CR Clearing."""
        env = build_gl_ready_env(conn)
        seed_dispute(conn, env["stripe_account_id"], env["company_id"],
                     stripe_id="dp_open_test", amount="100.00",
                     status="needs_response")

        result = call_action(ACTIONS["stripe-post-dispute-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            dispute_stripe_id="dp_open_test",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"
        assert result["action"] == "open_hold"
        assert result["dispute_fee"] == "15.00"
        assert result["total_held"] == "115.00"

        _assert_gl_balanced(conn, result["journal_entry_id"])

    def test_post_dispute_gl_lost(self, conn, db_path):
        """Lost dispute should create DR Dispute Losses, CR Clearing."""
        env = build_gl_ready_env(conn)
        # First post the open dispute to get a journal entry
        seed_dispute(conn, env["stripe_account_id"], env["company_id"],
                     stripe_id="dp_lost_test", amount="80.00",
                     status="needs_response")

        # Post the open dispute first
        open_result = call_action(ACTIONS["stripe-post-dispute-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            dispute_stripe_id="dp_lost_test",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(open_result)

        # Now update status to lost
        conn.execute(
            "UPDATE stripe_dispute SET status = 'lost' WHERE stripe_id = ?",
            ("dp_lost_test",)
        )
        conn.commit()

        result = call_action(ACTIONS["stripe-post-dispute-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            dispute_stripe_id="dp_lost_test",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"
        assert result["action"] == "lost"
        assert "journal_entry_id" in result

        _assert_gl_balanced(conn, result["journal_entry_id"])


# ===========================================================================
# 4. test_post_payout_gl
# ===========================================================================
class TestPostPayoutGL:

    def test_post_payout_gl_internal_transfer(self, conn, db_path):
        """Payout GL should create payment_entry (internal_transfer): DR Bank, CR Clearing."""
        env = build_gl_ready_env(conn)
        seed_payout(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="po_test_gl", amount="500.00", status="paid")

        result = call_action(ACTIONS["stripe-post-payout-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            payout_stripe_id="po_test_gl",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"
        assert result["payout_amount"] == "500.00"

        # Verify payment_entry type
        pe = conn.execute(
            "SELECT payment_type FROM payment_entry WHERE id = ?",
            (result["payment_entry_id"],)
        ).fetchone()
        assert pe["payment_type"] == "internal_transfer"

        _assert_gl_balanced(conn, result["payment_entry_id"])


# ===========================================================================
# 5. test_post_connect_fee_gl
# ===========================================================================
class TestPostConnectFeeGL:

    def test_post_connect_fee_gl(self, conn, db_path):
        """Connect fee GL should DR Clearing, CR Platform Revenue."""
        env = build_gl_ready_env(conn)

        # Need a platform revenue account
        platform_rev_id = seed_gl_account(conn, env["company_id"],
                                           "Platform Revenue", "income", "revenue")

        # Configure platform revenue account on stripe_account
        conn.execute(
            "UPDATE stripe_account SET platform_revenue_account_id = ? WHERE id = ?",
            (platform_rev_id, env["stripe_account_id"])
        )
        conn.commit()

        seed_application_fee(conn, env["stripe_account_id"], env["company_id"],
                             stripe_id="fee_test_gl", amount="10.00")

        result = call_action(ACTIONS["stripe-post-connect-fee-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            app_fee_stripe_id="fee_test_gl",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"
        assert result["fee_amount"] == "10.00"
        assert "journal_entry_id" in result

        _assert_gl_balanced(conn, result["journal_entry_id"])


# ===========================================================================
# 6. test_bulk_post_gl
# ===========================================================================
class TestBulkPostGL:

    def test_bulk_post_gl(self, conn, db_path):
        """Bulk post should process all unposted charges and payouts."""
        env = build_gl_ready_env(conn)

        # Create 2 unposted charges
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_bulk_1", amount="50.00")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_b1", source_id="ch_bulk_1",
                                 amount="50.00", fee="1.75", net="48.25")

        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_bulk_2", amount="75.00")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_b2", source_id="ch_bulk_2",
                                 amount="75.00", fee="2.48", net="72.52")

        # Create 1 unposted payout
        seed_payout(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="po_bulk_1", amount="120.77", status="paid")

        result = call_action(ACTIONS["stripe-bulk-post-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            date_from=None, date_to=None,
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"
        assert result["charges_posted"] == 2
        assert result["payouts_posted"] == 1
        assert result["total_posted"] >= 3

    def test_bulk_post_gl_skips_already_posted(self, conn, db_path):
        """Bulk post should skip charges that already have a payment_entry_id."""
        env = build_gl_ready_env(conn)

        # Create an already-posted charge (has erpclaw_payment_entry_id)
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_already", amount="100.00",
                    erpclaw_payment_entry_id="existing-pe-id")

        # Create one unposted charge
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_new", amount="60.00")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_new", source_id="ch_new",
                                 amount="60.00", fee="2.05", net="57.95")

        result = call_action(ACTIONS["stripe-bulk-post-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            date_from=None, date_to=None,
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"
        # Only the new charge should be posted
        assert result["charges_posted"] == 1
