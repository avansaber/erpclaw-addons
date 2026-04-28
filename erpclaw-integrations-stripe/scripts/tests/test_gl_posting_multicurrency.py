"""Tests for ERPClaw Stripe multi-currency GL posting (Phase 1, 2026-04-27).

Per Nik's directive: invoice currency MUST equal payment currency. ERPClaw
does not convert. Stripe and the cardholder bank handle conversion. We just
record what happened in the currency it happened.

These tests assert:
  - GL postings for non-USD charges/refunds/disputes/payouts/connect-fees
    book in the transaction currency (payment_entry.payment_currency,
    journal_entry.currency, and every gl_entry.currency).
  - exchange_rate is always "1" (no conversion).
  - USD baseline still works byte-identically.
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

import pytest

from stripe_test_helpers import (
    call_action, ns, is_ok, is_error,
    build_gl_ready_env, seed_charge, seed_balance_transaction,
    seed_refund, seed_dispute, seed_payout, seed_application_fee,
)
from gl_posting import ACTIONS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
SUPPORTED_NON_USD = ["EUR", "GBP", "CAD", "INR", "SGD", "AED"]


def _gl_entries(conn, voucher_id):
    return conn.execute(
        "SELECT * FROM gl_entry WHERE voucher_id = ? AND is_cancelled = 0",
        (voucher_id,)
    ).fetchall()


def _payment_entry(conn, pe_id):
    return conn.execute(
        "SELECT * FROM payment_entry WHERE id = ?", (pe_id,)
    ).fetchone()


def _journal_entry(conn, je_id):
    return conn.execute(
        "SELECT * FROM journal_entry WHERE id = ?", (je_id,)
    ).fetchone()


# ---------------------------------------------------------------------------
# Charge GL: each currency
# ---------------------------------------------------------------------------
class TestPostChargeGLMulticurrency:

    @pytest.mark.parametrize("ccy", SUPPORTED_NON_USD)
    def test_charge_books_in_transaction_currency(self, conn, db_path, ccy):
        env = build_gl_ready_env(conn)
        seed_charge(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id=f"ch_test_{ccy.lower()}",
            amount="100.00",
            currency=ccy,
        )
        seed_balance_transaction(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id=f"txn_{ccy.lower()}",
            source_id=f"ch_test_{ccy.lower()}",
            amount="100.00", fee="3.20", net="96.80",
            currency=ccy,
        )
        result = call_action(ACTIONS["stripe-post-charge-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            charge_stripe_id=f"ch_test_{ccy.lower()}",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result), f"Expected ok for {ccy}: {result}"

        pe = _payment_entry(conn, result["payment_entry_id"])
        assert pe is not None
        assert pe["payment_currency"] == ccy, (
            f"payment_entry.payment_currency for {ccy}: got {pe['payment_currency']}"
        )
        assert pe["exchange_rate"] == "1"

        gl = _gl_entries(conn, result["payment_entry_id"])
        assert len(gl) == 3
        for row in gl:
            assert row["currency"] == ccy, (
                f"gl_entry.currency for {ccy}: got {row['currency']}"
            )
            assert row["exchange_rate"] == "1"

    def test_usd_charge_baseline_unchanged(self, conn, db_path):
        """USD charge: baseline still produces USD GL entries with rate 1."""
        env = build_gl_ready_env(conn)
        seed_charge(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="ch_baseline_usd", amount="250.00",
            currency="USD",
        )
        seed_balance_transaction(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="txn_baseline_usd", source_id="ch_baseline_usd",
            amount="250.00", fee="7.55", net="242.45",
        )
        result = call_action(ACTIONS["stripe-post-charge-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            charge_stripe_id="ch_baseline_usd",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result)
        pe = _payment_entry(conn, result["payment_entry_id"])
        assert pe["payment_currency"] == "USD"
        assert pe["exchange_rate"] == "1"
        for row in _gl_entries(conn, result["payment_entry_id"]):
            assert row["currency"] == "USD"
            assert row["exchange_rate"] == "1"

        # GL balance: in single-currency posting, debit == credit holds
        # exactly because exchange_rate=1.
        total_debit = sum(Decimal(r["debit"]) for r in _gl_entries(conn, result["payment_entry_id"]))
        total_credit = sum(Decimal(r["credit"]) for r in _gl_entries(conn, result["payment_entry_id"]))
        assert total_debit == total_credit


# ---------------------------------------------------------------------------
# Refund GL
# ---------------------------------------------------------------------------
class TestPostRefundGLMulticurrency:

    @pytest.mark.parametrize("ccy", ["EUR", "INR", "AED"])
    def test_refund_books_in_transaction_currency(self, conn, db_path, ccy):
        env = build_gl_ready_env(conn)
        # Need a parent charge for customer lookup
        seed_charge(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id=f"ch_parent_{ccy.lower()}", amount="100.00",
            currency=ccy,
        )
        seed_refund(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id=f"re_{ccy.lower()}",
            charge_stripe_id=f"ch_parent_{ccy.lower()}",
            amount="40.00", currency=ccy,
        )
        result = call_action(ACTIONS["stripe-post-refund-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            refund_stripe_id=f"re_{ccy.lower()}",
        ))
        assert is_ok(result), f"Expected ok for refund {ccy}: {result}"

        pe = _payment_entry(conn, result["payment_entry_id"])
        assert pe["payment_currency"] == ccy
        assert pe["exchange_rate"] == "1"
        for row in _gl_entries(conn, result["payment_entry_id"]):
            assert row["currency"] == ccy
            assert row["exchange_rate"] == "1"


# ---------------------------------------------------------------------------
# Dispute GL (open hold)
# ---------------------------------------------------------------------------
class TestPostDisputeGLMulticurrency:

    @pytest.mark.parametrize("ccy", ["EUR", "GBP", "SGD"])
    def test_dispute_open_hold_books_in_transaction_currency(self, conn, db_path, ccy):
        env = build_gl_ready_env(conn)
        seed_charge(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id=f"ch_dp_{ccy.lower()}", amount="200.00",
            currency=ccy,
        )
        seed_dispute(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id=f"dp_{ccy.lower()}",
            charge_stripe_id=f"ch_dp_{ccy.lower()}",
            amount="200.00", status="needs_response",
            currency=ccy,
        )
        result = call_action(ACTIONS["stripe-post-dispute-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            dispute_stripe_id=f"dp_{ccy.lower()}",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result), f"Expected ok for dispute {ccy}: {result}"

        je = _journal_entry(conn, result["journal_entry_id"])
        assert je is not None
        assert je["currency"] == ccy
        assert je["exchange_rate"] == "1"
        for row in _gl_entries(conn, result["journal_entry_id"]):
            assert row["currency"] == ccy
            assert row["exchange_rate"] == "1"


# ---------------------------------------------------------------------------
# Payout GL
# ---------------------------------------------------------------------------
class TestPostPayoutGLMulticurrency:

    @pytest.mark.parametrize("ccy", ["EUR", "CAD", "AED"])
    def test_payout_books_in_settlement_currency(self, conn, db_path, ccy):
        env = build_gl_ready_env(conn)
        seed_payout(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id=f"po_{ccy.lower()}", amount="1500.00",
            currency=ccy,
        )
        result = call_action(ACTIONS["stripe-post-payout-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            payout_stripe_id=f"po_{ccy.lower()}",
        ))
        assert is_ok(result), f"Expected ok for payout {ccy}: {result}"

        pe = _payment_entry(conn, result["payment_entry_id"])
        assert pe["payment_currency"] == ccy
        assert pe["exchange_rate"] == "1"
        for row in _gl_entries(conn, result["payment_entry_id"]):
            assert row["currency"] == ccy


# ---------------------------------------------------------------------------
# Connect fee GL
# ---------------------------------------------------------------------------
class TestPostConnectFeeGLMulticurrency:

    def test_connect_fee_books_in_transaction_currency(self, conn, db_path):
        env = build_gl_ready_env(conn)
        # Seed a platform_revenue_account_id on stripe_account
        rev_id = "00000000-0000-0000-0000-000000000999"
        conn.execute(
            """INSERT INTO account (id, name, root_type, account_type,
               currency, is_group, balance_direction, company_id)
               VALUES (?, 'Platform Revenue', 'income', 'revenue', 'USD',
                       0, 'credit_normal', ?)""",
            (rev_id, env["company_id"])
        )
        conn.execute(
            "UPDATE stripe_account SET platform_revenue_account_id = ? WHERE id = ?",
            (rev_id, env["stripe_account_id"])
        )
        conn.commit()

        seed_application_fee(
            conn, env["stripe_account_id"], env["company_id"],
            stripe_id="fee_eur_001", amount="25.00",
            currency="EUR",
        )
        result = call_action(ACTIONS["stripe-post-connect-fee-gl"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            app_fee_stripe_id="fee_eur_001",
            cost_center_id=env["cost_center_id"],
        ))
        assert is_ok(result), f"Expected ok: {result}"

        je = _journal_entry(conn, result["journal_entry_id"])
        assert je["currency"] == "EUR"
        for row in _gl_entries(conn, result["journal_entry_id"]):
            assert row["currency"] == "EUR"
            assert row["exchange_rate"] == "1"


# ---------------------------------------------------------------------------
# Currency-mismatch validation (core erpclaw-payments)
# ---------------------------------------------------------------------------
class TestCurrencyMismatchValidation:
    """The submit-payment action must reject a payment whose payment_currency
    differs from the allocated invoice's currency. ERPClaw does not convert.
    """

    def _import_payments(self):
        # Import erpclaw-payments db_query as a module
        import importlib.util
        path = os.path.join(
            os.path.dirname(_SCRIPTS_DIR), "..", "..",
            "erpclaw", "scripts", "erpclaw-payments", "db_query.py"
        )
        path = os.path.abspath(path)
        spec = importlib.util.spec_from_file_location("erpclaw_payments_dbq", path)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        return m

    def test_eur_invoice_with_usd_payment_rejected(self, conn, db_path):
        """Payment in USD allocated to a EUR invoice returns clean error."""
        env = build_gl_ready_env(conn)
        company_id = env["company_id"]

        # Seed customer
        cust_id = "cust-mc-001"
        conn.execute(
            """INSERT INTO customer (id, name, company_id)
               VALUES (?, 'Test Cust', ?)""",
            (cust_id, company_id)
        )
        # Seed a EUR sales_invoice (submitted)
        si_id = "si-eur-001"
        conn.execute(
            """INSERT INTO sales_invoice
                (id, customer_id, posting_date, currency, exchange_rate,
                 total_amount, grand_total, outstanding_amount, status,
                 company_id)
               VALUES (?, ?, '2026-04-01', 'EUR', '1', '100.00', '100.00',
                       '100.00', 'submitted', ?)""",
            (si_id, cust_id, company_id)
        )
        # Seed a receivable + bank account
        ar_id = "acc-ar-001"
        bank_id = "acc-bank-001"
        conn.execute(
            """INSERT INTO account (id, name, root_type, account_type,
               currency, is_group, balance_direction, company_id)
               VALUES (?, 'AR', 'asset', 'receivable', 'USD', 0,
                       'debit_normal', ?)""",
            (ar_id, company_id)
        )
        conn.execute(
            """INSERT INTO account (id, name, root_type, account_type,
               currency, is_group, balance_direction, company_id)
               VALUES (?, 'Bank', 'asset', 'bank', 'USD', 0,
                       'debit_normal', ?)""",
            (bank_id, company_id)
        )
        # Seed a USD payment_entry (draft) that allocates against the EUR invoice
        pe_id = "pe-mismatch-001"
        conn.execute(
            """INSERT INTO payment_entry
                (id, payment_type, posting_date, party_type, party_id,
                 paid_from_account, paid_to_account,
                 paid_amount, received_amount,
                 payment_currency, exchange_rate,
                 status, unallocated_amount, company_id)
               VALUES (?, 'receive', '2026-04-15', 'customer', ?,
                       ?, ?, '100.00', '100.00', 'USD', '1',
                       'draft', '0', ?)""",
            (pe_id, cust_id, ar_id, bank_id, company_id)
        )
        # Seed allocation
        conn.execute(
            """INSERT INTO payment_allocation
                (id, payment_entry_id, voucher_type, voucher_id,
                 allocated_amount)
               VALUES (?, ?, 'sales_invoice', ?, '100.00')""",
            ("alloc-mismatch-001", pe_id, si_id)
        )
        conn.commit()

        m = self._import_payments()
        result = call_action(
            m.submit_payment, conn,
            ns(payment_entry_id=pe_id),
        )
        assert is_error(result), f"Expected error, got {result}"
        msg = (result.get("message") or "").lower()
        assert "currency mismatch" in msg
        assert "eur" in msg
        assert "usd" in msg
        # Status should remain draft (no GL writes happened)
        row = conn.execute(
            "SELECT status FROM payment_entry WHERE id = ?", (pe_id,)
        ).fetchone()
        assert row["status"] == "draft"

    def test_eur_invoice_with_eur_payment_passes_currency_check(self, conn, db_path):
        """Matching currencies pass the assertion (full submit may still fail
        on other validations; we just verify _assert_currency_match returns)."""
        env = build_gl_ready_env(conn)
        m = self._import_payments()
        # Build a stub PE dict and allocations that match
        pe = {"payment_currency": "EUR"}
        # Seed a sales_invoice for the lookup
        company_id = env["company_id"]
        cust_id = "cust-ok-001"
        conn.execute(
            "INSERT INTO customer (id, name, company_id) VALUES (?, 'Cx', ?)",
            (cust_id, company_id)
        )
        si_id = "si-ok-eur-001"
        conn.execute(
            """INSERT INTO sales_invoice
                (id, customer_id, posting_date, currency, exchange_rate,
                 total_amount, grand_total, outstanding_amount, status,
                 company_id)
               VALUES (?, ?, '2026-04-01', 'EUR', '1', '50.00', '50.00',
                       '50.00', 'submitted', ?)""",
            (si_id, cust_id, company_id)
        )
        conn.commit()
        allocations = [{
            "voucher_type": "sales_invoice",
            "voucher_id": si_id,
            "allocated_amount": "50.00",
        }]
        # Should not raise
        m._assert_currency_match(conn, pe, allocations)
