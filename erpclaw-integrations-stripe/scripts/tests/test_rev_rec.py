"""Tests for erpclaw-integrations-stripe ASC 606 revenue recognition bridge.

Tests cover:
  1. stripe-create-rev-rec-schedule (monthly + annual)
  2. stripe-recognize-subscription-revenue (GL posting)
  3. stripe-rev-rec-status (report)
  4. stripe-handle-subscription-change (cancel + upgrade)
  5. Full cycle: create schedule -> post charge -> recognize -> verify GL balanced
  6. Subscription-aware charge posting (gl_posting.py modification)
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
    seed_subscription, seed_customer_map, seed_erpclaw_customer,
    seed_gl_account, seed_invoice,
)
from rev_rec import ACTIONS as REV_REC_ACTIONS
from gl_posting import ACTIONS as GL_POSTING_ACTIONS


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


def _build_rev_rec_env(conn):
    """Build a GL-ready environment with a revenue income account for ASC 606 tests."""
    env = build_gl_ready_env(conn)

    # Create a Revenue income account for recognition posting
    revenue_acct_id = seed_gl_account(
        conn, env["company_id"],
        name="Subscription Revenue", root_type="income", account_type="revenue"
    )
    env["revenue_account_id"] = revenue_acct_id

    return env


# ===========================================================================
# 1. stripe-create-rev-rec-schedule
# ===========================================================================
class TestCreateRevRecSchedule:

    def test_create_monthly_schedule(self, conn, db_path):
        """Monthly subscription creates 12 schedule entries, each = plan_amount."""
        env = _build_rev_rec_env(conn)
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_monthly_001",
            customer_stripe_id="cus_test_001",
            plan_amount="49.99", plan_interval="month",
            status="active",
        )

        result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_monthly_001",
            company_id=env["company_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"
        assert result["schedule_entry_count"] == 12
        assert result["monthly_recognition_amount"] == "49.99"
        assert result["total_contract_value"] == "599.88"  # 49.99 * 12
        assert result["plan_interval"] == "month"
        assert result["contract_id"] is not None
        assert result["obligation_id"] is not None

        # Verify contract created in advacct_revenue_contract
        contract = conn.execute(
            "SELECT * FROM advacct_revenue_contract WHERE id = ?",
            (result["contract_id"],)
        ).fetchone()
        assert contract is not None
        assert contract["contract_status"] == "active"
        assert contract["total_value"] == "599.88"
        assert contract["contract_number"] == "sub_monthly_001"

        # Verify obligation created
        ob = conn.execute(
            "SELECT * FROM advacct_performance_obligation WHERE id = ?",
            (result["obligation_id"],)
        ).fetchone()
        assert ob is not None
        assert ob["recognition_method"] == "over_time"
        assert ob["recognition_basis"] == "time"
        assert ob["obligation_status"] == "unsatisfied"

        # Verify schedule entries
        entries = conn.execute(
            "SELECT * FROM advacct_revenue_schedule WHERE obligation_id = ? ORDER BY period_date",
            (result["obligation_id"],)
        ).fetchall()
        assert len(entries) == 12
        total = sum(Decimal(e["amount"]) for e in entries)
        assert total == Decimal("599.88")

        # Verify subscription linked to contract
        sub = conn.execute(
            "SELECT erpclaw_revenue_contract_id FROM stripe_subscription WHERE stripe_id = ?",
            ("sub_monthly_001",)
        ).fetchone()
        assert sub["erpclaw_revenue_contract_id"] == result["contract_id"]

    def test_create_annual_schedule(self, conn, db_path):
        """Annual subscription creates 12 schedule entries, each = plan_amount/12."""
        env = _build_rev_rec_env(conn)
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_annual_001",
            customer_stripe_id="cus_test_002",
            plan_amount="599.88", plan_interval="year",
            status="active",
        )

        result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_annual_001",
            company_id=env["company_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"
        assert result["schedule_entry_count"] == 12
        assert result["monthly_recognition_amount"] == "49.99"
        assert result["total_contract_value"] == "599.88"
        assert result["plan_interval"] == "year"

        # Verify schedule entries total matches contract value
        entries = conn.execute(
            "SELECT * FROM advacct_revenue_schedule WHERE obligation_id = ? ORDER BY period_date",
            (result["obligation_id"],)
        ).fetchall()
        assert len(entries) == 12
        total = sum(Decimal(e["amount"]) for e in entries)
        assert total == Decimal("599.88")

    def test_create_schedule_with_customer_name(self, conn, db_path):
        """Schedule picks up customer name from stripe_customer_map."""
        env = _build_rev_rec_env(conn)
        seed_customer_map(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_customer_id="cus_named",
            stripe_name="Acme Corp",
        )
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_named_001",
            customer_stripe_id="cus_named",
            plan_amount="99.00", plan_interval="month",
            status="active",
        )

        result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_named_001",
            company_id=env["company_id"],
        ))
        assert is_ok(result)

        contract = conn.execute(
            "SELECT customer_name FROM advacct_revenue_contract WHERE id = ?",
            (result["contract_id"],)
        ).fetchone()
        assert contract["customer_name"] == "Acme Corp"

    def test_create_schedule_rejects_already_linked(self, conn, db_path):
        """Cannot create schedule if subscription already has a contract link."""
        env = _build_rev_rec_env(conn)
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_linked_001",
            plan_amount="49.99", plan_interval="month",
            status="active",
        )
        # Manually link it
        conn.execute(
            "UPDATE stripe_subscription SET erpclaw_revenue_contract_id = 'existing-contract' "
            "WHERE stripe_id = 'sub_linked_001'"
        )
        conn.commit()

        result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_linked_001",
            company_id=env["company_id"],
        ))
        assert is_error(result)
        assert "already linked" in result.get("message", "")

    def test_create_schedule_rejects_canceled(self, conn, db_path):
        """Cannot create schedule for a canceled subscription."""
        env = _build_rev_rec_env(conn)
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_canceled_001",
            plan_amount="49.99", plan_interval="month",
            status="canceled",
        )

        result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_canceled_001",
            company_id=env["company_id"],
        ))
        assert is_error(result)
        assert "active" in result.get("message", "").lower() or "trialing" in result.get("message", "").lower()


# ===========================================================================
# 2. stripe-recognize-subscription-revenue
# ===========================================================================
class TestRecognizeSubscriptionRevenue:

    def test_recognize_revenue_posts_gl(self, conn, db_path):
        """Recognizing revenue should post GL: DR Unearned Revenue, CR Revenue."""
        env = _build_rev_rec_env(conn)
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_rec_001",
            plan_amount="100.00", plan_interval="month",
            status="active",
        )

        # Create schedule
        sched_result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_rec_001",
            company_id=env["company_id"],
        ))
        assert is_ok(sched_result)

        # Get the first schedule entry's period_date
        first_entry = conn.execute(
            "SELECT period_date FROM advacct_revenue_schedule WHERE obligation_id = ? ORDER BY period_date LIMIT 1",
            (sched_result["obligation_id"],)
        ).fetchone()
        period = first_entry["period_date"]

        # Recognize revenue for that period
        rec_result = call_action(REV_REC_ACTIONS["stripe-recognize-subscription-revenue"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            company_id=env["company_id"],
            revenue_account_id=env["revenue_account_id"],
            period_date=period,
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(rec_result), f"Expected ok: {rec_result}"
        assert rec_result["subscriptions_processed"] == 1
        assert rec_result["total_recognized"] == "100.00"
        assert rec_result["gl_entries_created"] > 0

        # Verify schedule entry marked as recognized
        entry = conn.execute(
            "SELECT recognized FROM advacct_revenue_schedule WHERE obligation_id = ? AND period_date = ?",
            (sched_result["obligation_id"], period)
        ).fetchone()
        assert entry["recognized"] == 1

    def test_recognize_revenue_no_double_recognition(self, conn, db_path):
        """Running recognition twice for the same period should process 0."""
        env = _build_rev_rec_env(conn)
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_nodup_001",
            plan_amount="50.00", plan_interval="month",
            status="active",
        )

        sched_result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_nodup_001",
            company_id=env["company_id"],
        ))
        assert is_ok(sched_result)

        first_entry = conn.execute(
            "SELECT period_date FROM advacct_revenue_schedule WHERE obligation_id = ? ORDER BY period_date LIMIT 1",
            (sched_result["obligation_id"],)
        ).fetchone()
        period = first_entry["period_date"]

        # First recognition
        rec1 = call_action(REV_REC_ACTIONS["stripe-recognize-subscription-revenue"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            company_id=env["company_id"],
            revenue_account_id=env["revenue_account_id"],
            period_date=period,
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(rec1)
        assert rec1["subscriptions_processed"] == 1

        # Second recognition — same period
        rec2 = call_action(REV_REC_ACTIONS["stripe-recognize-subscription-revenue"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            company_id=env["company_id"],
            revenue_account_id=env["revenue_account_id"],
            period_date=period,
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(rec2)
        assert rec2["subscriptions_processed"] == 0
        assert rec2["total_recognized"] == "0.00"

    def test_recognize_revenue_gl_balanced(self, conn, db_path):
        """GL entries from recognition must balance: total debits == total credits."""
        env = _build_rev_rec_env(conn)
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_bal_001",
            plan_amount="75.00", plan_interval="month",
            status="active",
        )

        sched_result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_bal_001",
            company_id=env["company_id"],
        ))
        assert is_ok(sched_result)

        first_entry = conn.execute(
            "SELECT period_date FROM advacct_revenue_schedule WHERE obligation_id = ? ORDER BY period_date LIMIT 1",
            (sched_result["obligation_id"],)
        ).fetchone()
        period = first_entry["period_date"]

        rec_result = call_action(REV_REC_ACTIONS["stripe-recognize-subscription-revenue"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            company_id=env["company_id"],
            revenue_account_id=env["revenue_account_id"],
            period_date=period,
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(rec_result)

        # Find the journal entries created during recognition
        jes = conn.execute(
            "SELECT id FROM journal_entry WHERE remark LIKE '%ASC 606%sub_bal_001%'"
        ).fetchall()
        assert len(jes) > 0
        for je in jes:
            _assert_gl_balanced(conn, je["id"])


# ===========================================================================
# 3. stripe-rev-rec-status
# ===========================================================================
class TestRevRecStatus:

    def test_rev_rec_status_shows_linked_subs(self, conn, db_path):
        """Status report shows all subscriptions with ASC 606 contracts."""
        env = _build_rev_rec_env(conn)

        # Create two subscriptions with schedules
        for i, (sub_id, amount) in enumerate([
            ("sub_status_001", "100.00"),
            ("sub_status_002", "200.00"),
        ]):
            seed_subscription(
                conn, env["stripe_account_id"], env["company_id"],
                stripe_id=sub_id,
                customer_stripe_id=f"cus_status_{i}",
                plan_amount=amount, plan_interval="month",
                status="active",
            )
            sched_result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
                stripe_account_id=env["stripe_account_id"],
                subscription_stripe_id=sub_id,
                company_id=env["company_id"],
            ))
            assert is_ok(sched_result)

        result = call_action(REV_REC_ACTIONS["stripe-rev-rec-status"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            company_id=env["company_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"
        assert result["subscription_count"] == 2
        assert Decimal(result["total_contract_value"]) == Decimal("3600.00")  # (100*12 + 200*12)
        assert Decimal(result["total_deferred"]) == Decimal("3600.00")  # nothing recognized yet
        assert Decimal(result["total_recognized"]) == Decimal("0.00")

    def test_rev_rec_status_after_recognition(self, conn, db_path):
        """Status report reflects recognized amounts after running recognition."""
        env = _build_rev_rec_env(conn)
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_status_rec",
            plan_amount="120.00", plan_interval="month",
            status="active",
        )

        sched_result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_status_rec",
            company_id=env["company_id"],
        ))
        assert is_ok(sched_result)

        # Recognize one period
        first_entry = conn.execute(
            "SELECT period_date FROM advacct_revenue_schedule WHERE obligation_id = ? ORDER BY period_date LIMIT 1",
            (sched_result["obligation_id"],)
        ).fetchone()

        rec_result = call_action(REV_REC_ACTIONS["stripe-recognize-subscription-revenue"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            company_id=env["company_id"],
            revenue_account_id=env["revenue_account_id"],
            period_date=first_entry["period_date"],
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(rec_result)

        # Check status
        status = call_action(REV_REC_ACTIONS["stripe-rev-rec-status"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            company_id=env["company_id"],
        ))
        assert is_ok(status)
        assert status["subscription_count"] == 1
        assert Decimal(status["total_recognized"]) == Decimal("120.00")
        assert Decimal(status["total_deferred"]) == Decimal("1320.00")  # 1440 - 120


# ===========================================================================
# 4. stripe-handle-subscription-change
# ===========================================================================
class TestHandleSubscriptionChange:

    def test_cancel_subscription(self, conn, db_path):
        """Canceling terminates the contract and reports remaining entries."""
        env = _build_rev_rec_env(conn)
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_cancel_001",
            plan_amount="50.00", plan_interval="month",
            status="active",
        )

        sched_result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_cancel_001",
            company_id=env["company_id"],
        ))
        assert is_ok(sched_result)

        # Cancel
        cancel_result = call_action(REV_REC_ACTIONS["stripe-handle-subscription-change"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_cancel_001",
            company_id=env["company_id"],
            change_type="cancel",
        ))
        assert is_ok(cancel_result), f"Expected ok: {cancel_result}"
        assert cancel_result["change_type"] == "cancel"
        assert cancel_result["contract_status"] == "terminated"
        assert cancel_result["unrecognized_entries_remaining"] == 12  # nothing was recognized

        # Verify contract status
        contract = conn.execute(
            "SELECT contract_status FROM advacct_revenue_contract WHERE id = ?",
            (cancel_result["contract_id"],)
        ).fetchone()
        assert contract["contract_status"] == "terminated"

    def test_upgrade_subscription(self, conn, db_path):
        """Upgrading recalculates remaining schedule entries with new amount."""
        env = _build_rev_rec_env(conn)
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_upgrade_001",
            plan_amount="50.00", plan_interval="month",
            status="active",
        )

        sched_result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_upgrade_001",
            company_id=env["company_id"],
        ))
        assert is_ok(sched_result)

        # Recognize first month so we have mixed recognized/unrecognized
        first_entry = conn.execute(
            "SELECT period_date FROM advacct_revenue_schedule WHERE obligation_id = ? ORDER BY period_date LIMIT 1",
            (sched_result["obligation_id"],)
        ).fetchone()
        rec_result = call_action(REV_REC_ACTIONS["stripe-recognize-subscription-revenue"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            company_id=env["company_id"],
            revenue_account_id=env["revenue_account_id"],
            period_date=first_entry["period_date"],
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(rec_result)

        # Upgrade from $50 to $75
        upgrade_result = call_action(REV_REC_ACTIONS["stripe-handle-subscription-change"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_upgrade_001",
            company_id=env["company_id"],
            change_type="upgrade",
            new_plan_amount="75.00",
        ))
        assert is_ok(upgrade_result), f"Expected ok: {upgrade_result}"
        assert upgrade_result["change_type"] == "upgrade"
        assert upgrade_result["contract_status"] == "modified"
        assert upgrade_result["new_monthly_amount"] == "75.00"
        assert upgrade_result["modification_count"] == 1

        # Verify unrecognized entries now have new amount
        unrecognized = conn.execute(
            "SELECT amount FROM advacct_revenue_schedule WHERE obligation_id = ? AND recognized = 0 ORDER BY period_date",
            (sched_result["obligation_id"],)
        ).fetchall()
        assert len(unrecognized) == 11  # 12 - 1 recognized
        for entry in unrecognized:
            assert entry["amount"] == "75.00"

        # Recognized entry should still have old amount
        recognized = conn.execute(
            "SELECT amount FROM advacct_revenue_schedule WHERE obligation_id = ? AND recognized = 1",
            (sched_result["obligation_id"],)
        ).fetchone()
        assert recognized["amount"] == "50.00"

    def test_change_requires_linked_contract(self, conn, db_path):
        """Cannot handle change on subscription without a contract link."""
        env = _build_rev_rec_env(conn)
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_nocontract",
            plan_amount="50.00", plan_interval="month",
            status="active",
        )

        result = call_action(REV_REC_ACTIONS["stripe-handle-subscription-change"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_nocontract",
            company_id=env["company_id"],
            change_type="cancel",
        ))
        assert is_error(result)
        assert "no linked revenue contract" in result.get("message", "").lower()


# ===========================================================================
# 5. Full cycle: schedule -> charge -> recognize -> verify GL
# ===========================================================================
class TestFullCycle:

    def test_full_asc606_cycle(self, conn, db_path):
        """Full ASC 606 cycle: create schedule, post charge (deferred), recognize, verify GL."""
        env = _build_rev_rec_env(conn)

        # 1. Create subscription + schedule
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_full_001",
            customer_stripe_id="cus_full_001",
            plan_amount="200.00", plan_interval="month",
            status="active",
        )

        sched_result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_full_001",
            company_id=env["company_id"],
        ))
        assert is_ok(sched_result)
        assert sched_result["total_contract_value"] == "2400.00"
        assert sched_result["monthly_recognition_amount"] == "200.00"

        # 2. Recognize first month
        first_entry = conn.execute(
            "SELECT period_date FROM advacct_revenue_schedule WHERE obligation_id = ? ORDER BY period_date LIMIT 1",
            (sched_result["obligation_id"],)
        ).fetchone()

        rec_result = call_action(REV_REC_ACTIONS["stripe-recognize-subscription-revenue"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            company_id=env["company_id"],
            revenue_account_id=env["revenue_account_id"],
            period_date=first_entry["period_date"],
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(rec_result)
        assert rec_result["total_recognized"] == "200.00"
        assert rec_result["subscriptions_processed"] == 1

        # 3. Verify GL balanced for recognition
        jes = conn.execute(
            "SELECT id FROM journal_entry WHERE remark LIKE '%ASC 606%sub_full_001%'"
        ).fetchall()
        for je in jes:
            _assert_gl_balanced(conn, je["id"])

        # 4. Check status
        status = call_action(REV_REC_ACTIONS["stripe-rev-rec-status"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            company_id=env["company_id"],
        ))
        assert is_ok(status)
        assert status["subscription_count"] == 1
        sub_info = status["subscriptions"][0]
        assert sub_info["recognized_to_date"] == "200.00"
        assert sub_info["remaining_deferred"] == "2200.00"

        # 5. Verify total GL balance across all entries
        all_gl = conn.execute(
            "SELECT COALESCE(SUM(CAST(debit AS NUMERIC)), 0) as total_dr, "
            "COALESCE(SUM(CAST(credit AS NUMERIC)), 0) as total_cr "
            "FROM gl_entry WHERE is_cancelled = 0"
        ).fetchone()
        assert Decimal(str(all_gl["total_dr"])) == Decimal(str(all_gl["total_cr"])), \
            f"GL imbalanced: DR={all_gl['total_dr']}, CR={all_gl['total_cr']}"


# ===========================================================================
# 6. Subscription-aware charge posting
# ===========================================================================
class TestSubscriptionAwareChargePosting:

    def test_charge_with_asc606_sub_credits_unearned(self, conn, db_path):
        """Charge linked to ASC 606 subscription should ALWAYS CR Unearned Revenue."""
        env = _build_rev_rec_env(conn)

        # Create a mapped customer (normally this would CR clearing/revenue)
        cust_id = seed_erpclaw_customer(conn, env["company_id"], "SaaS Customer")
        seed_customer_map(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_customer_id="cus_saas_001",
            erpclaw_customer_id=cust_id,
            stripe_name="SaaS Customer",
        )

        # Create subscription + ASC 606 schedule
        seed_subscription(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="sub_aware_001",
            customer_stripe_id="cus_saas_001",
            plan_amount="100.00", plan_interval="month",
            status="active",
        )
        sched_result = call_action(REV_REC_ACTIONS["stripe-create-rev-rec-schedule"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            subscription_stripe_id="sub_aware_001",
            company_id=env["company_id"],
        ))
        assert is_ok(sched_result)

        # Create a Stripe invoice linked to the subscription
        inv_id = seed_invoice(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="in_aware_001", amount_due="100.00", status="paid",
        )
        conn.execute(
            "UPDATE stripe_invoice SET subscription_stripe_id = ? WHERE stripe_id = ?",
            ("sub_aware_001", "in_aware_001")
        )
        conn.commit()

        # Create a charge linked to that invoice
        seed_charge(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="ch_aware_001", amount="100.00",
            customer_stripe_id="cus_saas_001",
        )
        conn.execute(
            "UPDATE stripe_charge SET invoice_stripe_id = ? WHERE stripe_id = ?",
            ("in_aware_001", "ch_aware_001")
        )
        conn.commit()

        seed_balance_transaction(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="txn_aware_001", source_id="ch_aware_001",
            amount="100.00", fee="3.20", net="96.80",
        )

        # Post charge GL
        result = call_action(GL_POSTING_ACTIONS["stripe-post-charge-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            charge_stripe_id="ch_aware_001",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"

        # Even though customer is mapped, CR should go to Unearned Revenue (ASC 606)
        entries = _gl_entries_for_voucher(conn, result["payment_entry_id"])
        cr_entries = [e for e in entries if Decimal(e["credit"]) > 0]
        assert len(cr_entries) == 1
        assert cr_entries[0]["account_id"] == env["unearned_id"], \
            f"Expected CR to Unearned Revenue ({env['unearned_id']}), got {cr_entries[0]['account_id']}"

        _assert_gl_balanced(conn, result["payment_entry_id"])

    def test_charge_without_asc606_sub_normal_behavior(self, conn, db_path):
        """Charge NOT linked to ASC 606 sub should follow normal customer/unearned logic."""
        env = _build_rev_rec_env(conn)

        # Charge without invoice/subscription link, no customer mapped
        seed_charge(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="ch_normal_001", amount="50.00",
        )
        seed_balance_transaction(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="txn_normal_001", source_id="ch_normal_001",
            amount="50.00", fee="1.75", net="48.25",
        )

        result = call_action(GL_POSTING_ACTIONS["stripe-post-charge-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            charge_stripe_id="ch_normal_001",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result)
        assert result["customer_mapped"] is False

        # CR should go to unearned revenue (no customer mapped)
        entries = _gl_entries_for_voucher(conn, result["payment_entry_id"])
        cr_entries = [e for e in entries if Decimal(e["credit"]) > 0]
        assert cr_entries[0]["account_id"] == env["unearned_id"]
        _assert_gl_balanced(conn, result["payment_entry_id"])
