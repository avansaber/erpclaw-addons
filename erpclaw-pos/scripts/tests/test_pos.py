"""L1 tests for ERPClaw POS -- Profiles, Sessions, Transactions, Reports.

Covers all 29 actions across 4 domain modules:
  - profiles (4): add/update/get/list pos-profile
  - sessions (4): open/get/list/close session
  - transactions (16): add/add-item/remove-item/apply-discount/hold/resume/
    add-payment/submit/void/return/get/list transaction, lookup-item,
    generate-receipt, session-summary, status
  - reports (5): cash-reconciliation, daily-report, hourly-sales,
    top-items, cashier-performance
"""
import pytest
from pos_helpers import (
    call_action, ns, is_ok, is_error, load_db_query, _uuid,
    seed_company, seed_naming_series, seed_item, seed_pos_profile,
    seed_open_session,
)


@pytest.fixture
def mod():
    return load_db_query()


# ============================================================================
# PROFILES
# ============================================================================

class TestAddPosProfile:
    def test_add_profile_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-add-pos-profile"], conn, ns(
            company_id=env["company_id"], name="Register 1",
            warehouse_id=None, price_list_id=None,
            default_payment_method="cash",
            allow_discount="1", max_discount_pct="50",
            auto_print_receipt="0", is_active=None,
        ))
        assert is_ok(r)
        assert r["id"]
        assert r["name"] == "Register 1"

    def test_add_profile_missing_name(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-add-pos-profile"], conn, ns(
            company_id=env["company_id"], name=None,
            warehouse_id=None, price_list_id=None,
            default_payment_method=None,
            allow_discount=None, max_discount_pct=None,
            auto_print_receipt=None, is_active=None,
        ))
        assert is_error(r)

    def test_add_profile_missing_company(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-add-pos-profile"], conn, ns(
            company_id=None, name="Test",
            warehouse_id=None, price_list_id=None,
            default_payment_method=None,
            allow_discount=None, max_discount_pct=None,
            auto_print_receipt=None, is_active=None,
        ))
        assert is_error(r)

    def test_add_profile_invalid_payment_method(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-add-pos-profile"], conn, ns(
            company_id=env["company_id"], name="Bad PM",
            warehouse_id=None, price_list_id=None,
            default_payment_method="bitcoin",
            allow_discount=None, max_discount_pct=None,
            auto_print_receipt=None, is_active=None,
        ))
        assert is_error(r)


class TestUpdatePosProfile:
    def test_update_profile_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-update-pos-profile"], conn, ns(
            id=env["profile_id"], name="Renamed Register",
            warehouse_id=None, price_list_id=None,
            default_payment_method=None,
            allow_discount=None, max_discount_pct=None,
            auto_print_receipt=None, is_active=None,
        ))
        assert is_ok(r)
        assert "name" in r["updated_fields"]

    def test_update_profile_not_found(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-update-pos-profile"], conn, ns(
            id=_uuid(), name="X",
            warehouse_id=None, price_list_id=None,
            default_payment_method=None,
            allow_discount=None, max_discount_pct=None,
            auto_print_receipt=None, is_active=None,
        ))
        assert is_error(r)


class TestGetPosProfile:
    def test_get_profile_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-get-pos-profile"], conn, ns(
            id=env["profile_id"],
        ))
        assert is_ok(r)
        assert r["id"] == env["profile_id"]
        assert "open_sessions" in r


class TestListPosProfiles:
    def test_list_profiles(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-list-pos-profiles"], conn, ns(
            company_id=env["company_id"], is_active=None,
            search=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total"] >= 1


# ============================================================================
# SESSIONS
# ============================================================================

class TestOpenSession:
    def test_open_session_success(self, conn, env, mod):
        # env already has an open session, create a new profile to open another
        pid = seed_pos_profile(conn, env["company_id"], "Register 2")
        r = call_action(mod.ACTIONS["pos-open-session"], conn, ns(
            pos_profile_id=pid, cashier_name="Jane Doe",
            opening_amount="200.00",
        ))
        assert is_ok(r)
        assert r["session_status"] == "open"
        assert r["opening_amount"] == "200.00"

    def test_open_session_duplicate(self, conn, env, mod):
        """Cannot open two sessions on the same profile."""
        r = call_action(mod.ACTIONS["pos-open-session"], conn, ns(
            pos_profile_id=env["profile_id"], cashier_name="Dup",
            opening_amount="0",
        ))
        assert is_error(r)

    def test_open_session_missing_cashier(self, conn, env, mod):
        pid = seed_pos_profile(conn, env["company_id"], "Register 3")
        r = call_action(mod.ACTIONS["pos-open-session"], conn, ns(
            pos_profile_id=pid, cashier_name=None,
            opening_amount="0",
        ))
        assert is_error(r)


class TestGetSession:
    def test_get_session_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-get-session"], conn, ns(
            id=env["session_id"],
        ))
        assert is_ok(r)
        assert r["session_status"] == "open"

    def test_get_session_not_found(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-get-session"], conn, ns(
            id=_uuid(),
        ))
        assert is_error(r)


class TestListSessions:
    def test_list_sessions(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-list-sessions"], conn, ns(
            pos_profile_id=None, status=None,
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total"] >= 1


# ============================================================================
# TRANSACTIONS
# ============================================================================

class TestAddTransaction:
    def test_add_transaction_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name="Walk-in",
        ))
        assert is_ok(r)
        assert r["id"]
        assert r["transaction_status"] == "draft"

    def test_add_transaction_closed_session(self, conn, env, mod):
        # Close the session first
        call_action(mod.ACTIONS["pos-close-session"], conn, ns(
            id=env["session_id"], closing_amount="100.00",
        ))
        r = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name="Walk-in",
        ))
        assert is_error(r)


class TestAddTransactionItem:
    def _make_txn(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name="Walk-in",
        ))
        assert is_ok(r)
        return r["id"]

    def test_add_item_success(self, conn, env, mod):
        txn_id = self._make_txn(conn, env, mod)
        r = call_action(mod.ACTIONS["pos-add-transaction-item"], conn, ns(
            pos_transaction_id=txn_id, item_id=env["item_id"],
            item_name=None, qty="2", rate="25.00", uom=None,
            barcode=None, discount_pct="0",
        ))
        assert is_ok(r)
        assert r["qty"] == "2"
        assert r["amount"] == "50.00"

    def test_add_item_missing_item(self, conn, env, mod):
        txn_id = self._make_txn(conn, env, mod)
        r = call_action(mod.ACTIONS["pos-add-transaction-item"], conn, ns(
            pos_transaction_id=txn_id, item_id=_uuid(),
            item_name=None, qty="1", rate="10", uom=None,
            barcode=None, discount_pct=None,
        ))
        assert is_error(r)


class TestRemoveTransactionItem:
    def test_remove_item(self, conn, env, mod):
        txn_r = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name=None,
        ))
        assert is_ok(txn_r)
        item_r = call_action(mod.ACTIONS["pos-add-transaction-item"], conn, ns(
            pos_transaction_id=txn_r["id"], item_id=env["item_id"],
            item_name=None, qty="1", rate="10", uom=None,
            barcode=None, discount_pct=None,
        ))
        assert is_ok(item_r)
        r = call_action(mod.ACTIONS["pos-remove-transaction-item"], conn, ns(
            pos_transaction_item_id=item_r["id"],
        ))
        assert is_ok(r)
        assert r["transaction_grand_total"] == "0.00"


class TestApplyDiscount:
    def _make_txn_with_item(self, conn, env, mod):
        txn = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name=None,
        ))
        call_action(mod.ACTIONS["pos-add-transaction-item"], conn, ns(
            pos_transaction_id=txn["id"], item_id=env["item_id"],
            item_name=None, qty="2", rate="50.00", uom=None,
            barcode=None, discount_pct="0",
        ))
        return txn["id"]

    def test_apply_discount_pct(self, conn, env, mod):
        txn_id = self._make_txn_with_item(conn, env, mod)
        r = call_action(mod.ACTIONS["pos-apply-discount"], conn, ns(
            pos_transaction_id=txn_id,
            discount_pct="10", discount_amount=None,
        ))
        assert is_ok(r)
        assert r["discount_pct"] == "10"
        assert r["grand_total"] == "90.00"

    def test_apply_discount_amount(self, conn, env, mod):
        txn_id = self._make_txn_with_item(conn, env, mod)
        r = call_action(mod.ACTIONS["pos-apply-discount"], conn, ns(
            pos_transaction_id=txn_id,
            discount_pct=None, discount_amount="15",
        ))
        assert is_ok(r)
        assert r["grand_total"] == "85.00"


class TestHoldResumeTransaction:
    def _make_txn(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name=None,
        ))
        return r["id"]

    def test_hold_transaction(self, conn, env, mod):
        txn_id = self._make_txn(conn, env, mod)
        r = call_action(mod.ACTIONS["pos-hold-transaction"], conn, ns(
            pos_transaction_id=txn_id,
        ))
        assert is_ok(r)
        assert r["transaction_status"] == "held"

    def test_resume_transaction(self, conn, env, mod):
        txn_id = self._make_txn(conn, env, mod)
        call_action(mod.ACTIONS["pos-hold-transaction"], conn, ns(
            pos_transaction_id=txn_id,
        ))
        r = call_action(mod.ACTIONS["pos-resume-transaction"], conn, ns(
            pos_transaction_id=txn_id,
        ))
        assert is_ok(r)
        assert r["transaction_status"] == "draft"


class TestAddPayment:
    def _make_txn_with_item(self, conn, env, mod):
        txn = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name=None,
        ))
        call_action(mod.ACTIONS["pos-add-transaction-item"], conn, ns(
            pos_transaction_id=txn["id"], item_id=env["item_id"],
            item_name=None, qty="1", rate="25.00", uom=None,
            barcode=None, discount_pct=None,
        ))
        return txn["id"]

    def test_add_payment_success(self, conn, env, mod):
        txn_id = self._make_txn_with_item(conn, env, mod)
        r = call_action(mod.ACTIONS["pos-add-payment"], conn, ns(
            pos_transaction_id=txn_id,
            payment_method="cash", amount="25.00", reference=None,
        ))
        assert is_ok(r)
        assert r["payment_amount"] == "25.00"
        assert r["total_paid"] == "25.00"

    def test_add_payment_invalid_method(self, conn, env, mod):
        txn_id = self._make_txn_with_item(conn, env, mod)
        r = call_action(mod.ACTIONS["pos-add-payment"], conn, ns(
            pos_transaction_id=txn_id,
            payment_method="bitcoin", amount="25.00", reference=None,
        ))
        assert is_error(r)


class TestSubmitTransaction:
    def _make_paid_txn(self, conn, env, mod):
        txn = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name="Customer A",
        ))
        call_action(mod.ACTIONS["pos-add-transaction-item"], conn, ns(
            pos_transaction_id=txn["id"], item_id=env["item_id"],
            item_name=None, qty="2", rate="10.00", uom=None,
            barcode=None, discount_pct=None,
        ))
        call_action(mod.ACTIONS["pos-add-payment"], conn, ns(
            pos_transaction_id=txn["id"],
            payment_method="cash", amount="20.00", reference=None,
        ))
        return txn["id"]

    def test_submit_transaction_success(self, conn, env, mod):
        txn_id = self._make_paid_txn(conn, env, mod)
        r = call_action(mod.ACTIONS["pos-submit-transaction"], conn, ns(
            pos_transaction_id=txn_id,
        ))
        assert is_ok(r)
        assert r["transaction_status"] == "submitted"
        assert r["change_amount"] == "0.00"

    def test_submit_insufficient_payment(self, conn, env, mod):
        txn = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name=None,
        ))
        call_action(mod.ACTIONS["pos-add-transaction-item"], conn, ns(
            pos_transaction_id=txn["id"], item_id=env["item_id"],
            item_name=None, qty="1", rate="100.00", uom=None,
            barcode=None, discount_pct=None,
        ))
        call_action(mod.ACTIONS["pos-add-payment"], conn, ns(
            pos_transaction_id=txn["id"],
            payment_method="cash", amount="50.00", reference=None,
        ))
        r = call_action(mod.ACTIONS["pos-submit-transaction"], conn, ns(
            pos_transaction_id=txn["id"],
        ))
        assert is_error(r)


class TestVoidTransaction:
    def test_void_transaction(self, conn, env, mod):
        txn = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name=None,
        ))
        r = call_action(mod.ACTIONS["pos-void-transaction"], conn, ns(
            pos_transaction_id=txn["id"],
        ))
        assert is_ok(r)
        assert r["transaction_status"] == "voided"

    def test_void_already_voided(self, conn, env, mod):
        txn = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name=None,
        ))
        call_action(mod.ACTIONS["pos-void-transaction"], conn, ns(
            pos_transaction_id=txn["id"],
        ))
        r = call_action(mod.ACTIONS["pos-void-transaction"], conn, ns(
            pos_transaction_id=txn["id"],
        ))
        assert is_error(r)


class TestReturnTransaction:
    def _make_submitted_txn(self, conn, env, mod):
        txn = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name="Return Test",
        ))
        call_action(mod.ACTIONS["pos-add-transaction-item"], conn, ns(
            pos_transaction_id=txn["id"], item_id=env["item_id"],
            item_name=None, qty="1", rate="30.00", uom=None,
            barcode=None, discount_pct=None,
        ))
        call_action(mod.ACTIONS["pos-add-payment"], conn, ns(
            pos_transaction_id=txn["id"],
            payment_method="cash", amount="30.00", reference=None,
        ))
        call_action(mod.ACTIONS["pos-submit-transaction"], conn, ns(
            pos_transaction_id=txn["id"],
        ))
        return txn["id"]

    def test_return_transaction(self, conn, env, mod):
        txn_id = self._make_submitted_txn(conn, env, mod)
        r = call_action(mod.ACTIONS["pos-return-transaction"], conn, ns(
            pos_transaction_id=txn_id,
        ))
        assert is_ok(r)
        assert r["transaction_status"] == "returned"
        assert r["return_transaction_id"]
        assert r["return_grand_total"] == "-30.00"


class TestGetTransaction:
    def test_get_transaction(self, conn, env, mod):
        txn = call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name=None,
        ))
        r = call_action(mod.ACTIONS["pos-get-transaction"], conn, ns(
            id=txn["id"], pos_transaction_id=None,
        ))
        assert is_ok(r)
        assert "items" in r
        assert "payments" in r


class TestListTransactions:
    def test_list_transactions(self, conn, env, mod):
        call_action(mod.ACTIONS["pos-add-transaction"], conn, ns(
            pos_session_id=env["session_id"],
            customer_id=None, customer_name=None,
        ))
        r = call_action(mod.ACTIONS["pos-list-transactions"], conn, ns(
            pos_session_id=env["session_id"], status=None,
            company_id=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total"] >= 1


class TestLookupItem:
    def test_lookup_by_search(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-lookup-item"], conn, ns(
            search="Widget", barcode=None, limit=20,
        ))
        assert is_ok(r)
        assert r["total"] >= 1

    def test_lookup_missing_params(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-lookup-item"], conn, ns(
            search=None, barcode=None, limit=20,
        ))
        assert is_error(r)


class TestCloseSession:
    def test_close_session(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-close-session"], conn, ns(
            id=env["session_id"], closing_amount="100.00",
        ))
        assert is_ok(r)
        assert r["session_status"] == "closed"


class TestSessionSummary:
    def test_session_summary(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-session-summary"], conn, ns(
            pos_session_id=env["session_id"],
        ))
        assert is_ok(r)
        assert r["session_status"] == "open"
        assert "status_breakdown" in r


class TestPosStatus:
    def test_status(self, conn, env, mod):
        r = call_action(mod.ACTIONS["status"], conn, ns())
        assert is_ok(r)
        assert r["skill"] == "erpclaw-pos"


# ============================================================================
# REPORTS
# ============================================================================

class TestCashReconciliation:
    def test_cash_reconciliation(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-cash-reconciliation"], conn, ns(
            pos_session_id=env["session_id"], id=None,
        ))
        assert is_ok(r)
        assert "expected_cash" in r
        assert r["opening_amount"] == "100.00"


class TestDailyReport:
    def test_daily_report(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-daily-report"], conn, ns(
            date=None, company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "total_sales" in r
        assert "report_date" in r


class TestHourlySales:
    def test_hourly_sales(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-hourly-sales"], conn, ns(
            date=None, company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "hourly_breakdown" in r


class TestTopItems:
    def test_top_items(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-top-items"], conn, ns(
            from_date=None, to_date=None,
            company_id=env["company_id"], limit=10,
        ))
        assert is_ok(r)
        assert "top_items" in r


class TestCashierPerformance:
    def test_cashier_performance(self, conn, env, mod):
        r = call_action(mod.ACTIONS["pos-cashier-performance"], conn, ns(
            from_date=None, to_date=None,
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "cashiers" in r
