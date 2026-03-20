"""Tests for erpclaw-integrations-stripe browse, reports, connect, and utils actions (Sprint S6).

12 tests covering list/get actions, revenue/fee/MRR reports, and status/health.
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
    build_stripe_env, build_gl_ready_env,
    seed_charge, seed_balance_transaction, seed_refund, seed_dispute,
    seed_payout, seed_subscription, seed_customer_map,
    seed_erpclaw_customer, seed_application_fee, seed_gl_account,
    seed_invoice, seed_transfer, seed_fiscal_year, seed_cost_center,
)
from browse import ACTIONS as BROWSE_ACTIONS
from reports import ACTIONS as REPORTS_ACTIONS
from connect import ACTIONS as CONNECT_ACTIONS
from utils import ACTIONS as UTILS_ACTIONS


# ===========================================================================
# BROWSE TESTS
# ===========================================================================

class TestListCharges:

    def test_list_charges(self, conn, db_path):
        """Should list charges for a stripe account."""
        env = build_stripe_env(conn)
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_list_1", amount="100.00")
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_list_2", amount="200.00")

        result = call_action(BROWSE_ACTIONS["stripe-list-charges"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            status=None, customer_stripe_id=None,
            date_from=None, date_to=None, limit=50,
        ))
        assert is_ok(result)
        assert result["count"] == 2


class TestGetCharge:

    def test_get_charge_with_refunds(self, conn, db_path):
        """Get charge should include related refunds and balance transactions."""
        env = build_stripe_env(conn)
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_detail", amount="100.00")
        seed_refund(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="re_detail", charge_stripe_id="ch_detail",
                    amount="25.00")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_detail", source_id="ch_detail",
                                 amount="100.00", fee="3.20", net="96.80")

        result = call_action(BROWSE_ACTIONS["stripe-get-charge"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            charge_stripe_id="ch_detail",
        ))
        assert is_ok(result)
        assert result["stripe_id"] == "ch_detail"
        assert len(result["refunds"]) == 1
        assert len(result["balance_transactions"]) == 1


class TestListPayouts:

    def test_list_payouts(self, conn, db_path):
        """Should list payouts for a stripe account."""
        env = build_stripe_env(conn)
        seed_payout(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="po_list_1", amount="500.00")

        result = call_action(BROWSE_ACTIONS["stripe-list-payouts"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            status=None, limit=50,
        ))
        assert is_ok(result)
        assert result["count"] == 1


class TestGetPayout:

    def test_get_payout_with_transactions(self, conn, db_path):
        """Get payout should include related balance transactions."""
        env = build_stripe_env(conn)
        seed_payout(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="po_detail_1", amount="500.00")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_po1", source_id="ch_x",
                                 amount="100.00", fee="3.00", net="97.00",
                                 payout_id="po_detail_1")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_po2", source_id="ch_y",
                                 amount="400.00", fee="12.00", net="388.00",
                                 payout_id="po_detail_1")

        result = call_action(BROWSE_ACTIONS["stripe-get-payout"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            payout_stripe_id="po_detail_1",
        ))
        assert is_ok(result)
        assert result["transaction_count"] == 2


class TestListDisputes:

    def test_list_disputes(self, conn, db_path):
        """Should list disputes for a stripe account."""
        env = build_stripe_env(conn)
        seed_dispute(conn, env["stripe_account_id"], env["company_id"],
                     stripe_id="dp_list_1", amount="50.00")
        seed_dispute(conn, env["stripe_account_id"], env["company_id"],
                     stripe_id="dp_list_2", amount="75.00", status="under_review")

        result = call_action(BROWSE_ACTIONS["stripe-list-disputes"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            status=None, limit=50,
        ))
        assert is_ok(result)
        assert result["count"] == 2


class TestListSubscriptions:

    def test_list_subscriptions(self, conn, db_path):
        """Should list subscriptions for a stripe account."""
        env = build_stripe_env(conn)
        seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                          stripe_id="sub_list_1", plan_amount="49.99")
        seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                          stripe_id="sub_list_2", plan_amount="99.99",
                          status="trialing")

        result = call_action(BROWSE_ACTIONS["stripe-list-subscriptions"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            status=None, limit=50,
        ))
        assert is_ok(result)
        assert result["count"] == 2


# ===========================================================================
# REPORTS TESTS
# ===========================================================================

class TestRevenueReport:

    def test_revenue_report(self, conn, db_path):
        """Revenue report should group charges by month with fee breakdown."""
        env = build_stripe_env(conn)
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_rev1", source_id="ch_rev1",
                                 amount="100.00", fee="3.20", net="96.80")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_rev2", source_id="ch_rev2",
                                 amount="200.00", fee="6.10", net="193.90")

        result = call_action(REPORTS_ACTIONS["stripe-revenue-report"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
        ))
        assert is_ok(result)
        assert result["report"] == "revenue"
        assert Decimal(result["totals"]["gross"]) == Decimal("300.00")
        assert Decimal(result["totals"]["fees"]) == Decimal("9.30")
        assert Decimal(result["totals"]["net"]) == Decimal("290.70")


class TestFeeReport:

    def test_fee_report(self, conn, db_path):
        """Fee report should show fees grouped by transaction type."""
        env = build_stripe_env(conn)
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_fr1", source_id="ch_fr1",
                                 amount="100.00", fee="3.20", net="96.80")
        # Add a refund type balance transaction with fee
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_fr2", source_id="re_fr1",
                                 amount="-50.00", fee="0.50", net="-50.50",
                                 bt_type="refund")

        result = call_action(REPORTS_ACTIONS["stripe-fee-report"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
        ))
        assert is_ok(result)
        assert result["report"] == "fees"
        assert len(result["fee_types"]) >= 1
        assert Decimal(result["grand_total"]) == Decimal("3.70")


class TestCustomerRevenueReport:

    def test_customer_revenue_report(self, conn, db_path):
        """Customer revenue report should group charges by customer."""
        env = build_stripe_env(conn)
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_cr1", amount="100.00",
                    customer_stripe_id="cus_rev_1")
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_cr2", amount="200.00",
                    customer_stripe_id="cus_rev_1")
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_cr3", amount="50.00",
                    customer_stripe_id="cus_rev_2")

        result = call_action(REPORTS_ACTIONS["stripe-customer-revenue-report"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
        ))
        assert is_ok(result)
        assert result["report"] == "customer_revenue"
        assert result["customer_count"] == 2
        assert Decimal(result["grand_total"]) == Decimal("350.00")


class TestMRRReport:

    def test_mrr_report(self, conn, db_path):
        """MRR report should calculate monthly recurring revenue."""
        env = build_stripe_env(conn)
        seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                          stripe_id="sub_mrr1", plan_amount="49.99",
                          plan_interval="month", status="active")
        seed_subscription(conn, env["stripe_account_id"], env["company_id"],
                          stripe_id="sub_mrr2", plan_amount="599.88",
                          plan_interval="year", status="active")

        result = call_action(REPORTS_ACTIONS["stripe-mrr-report"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
        ))
        assert is_ok(result)
        assert result["report"] == "mrr"
        assert result["active_subscriptions"] == 2
        # Monthly sub = $49.99, yearly = $599.88 / 12 ~= $49.99
        mrr = Decimal(result["total_mrr"])
        assert mrr > Decimal("90.00"), f"MRR too low: {mrr}"


# ===========================================================================
# UTILS TESTS
# ===========================================================================

class TestStripeStatus:

    def test_status(self, conn, db_path):
        """Status dashboard should report account and transaction counts."""
        env = build_gl_ready_env(conn)
        seed_charge(conn, env["stripe_account_id"], env["company_id"],
                    stripe_id="ch_status", amount="100.00")
        seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                                 stripe_id="txn_status", source_id="ch_status",
                                 amount="100.00", fee="3.20", net="96.80")

        result = call_action(UTILS_ACTIONS["stripe-status"], conn, ns())
        assert is_ok(result)
        assert result["stripe_accounts"] >= 1
        assert result["unreconciled_transactions"] >= 1
        assert result["pending_charges"] >= 1


class TestVerifyGLBalance:

    def test_verify_gl_balance(self, conn, db_path):
        """GL balance verification should report clearing account balance."""
        env = build_gl_ready_env(conn)

        result = call_action(UTILS_ACTIONS["stripe-verify-gl-balance"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
        ))
        assert is_ok(result)
        # Initially no GL entries, so balance should be 0
        assert result["balance"] == "0.00"
        assert result["balanced"] is True
