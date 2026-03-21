"""L1 tests for ERPClaw Integrations Shopify -- reconciliation domain.

10 tests covering payout verification, clearing balance checks,
reconciliation lifecycle, and bank transaction matching.
"""
import pytest
from decimal import Decimal
from shopify_test_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_shopify_order, seed_shopify_payout, seed_shopify_refund,
    seed_shopify_dispute, _uuid, _now, _today,
)

mod = load_db_query()


def _seed_payout_transaction(conn, payout_id, company_id,
                              txn_type="charge", gross="100.00",
                              fee="2.90", net=None,
                              source_order_id=None):
    """Insert a shopify_payout_transaction for testing."""
    txn_id = _uuid()
    net_val = net or str(Decimal(gross) - Decimal(fee))
    conn.execute(
        """INSERT INTO shopify_payout_transaction (
            id, shopify_payout_id_local, shopify_balance_txn_id,
            transaction_type, gross_amount, fee_amount, net_amount,
            source_order_id, source_type, processed_at,
            company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (txn_id, payout_id, _uuid()[:12],
         txn_type, gross, fee, net_val,
         source_order_id, "order" if source_order_id else None,
         _today(),
         company_id, _now())
    )
    conn.commit()
    return txn_id


class TestPayoutVerification:

    def test_payout_sum_matches(self, conn, env):
        """Payout transaction sums match payout net_amount."""
        payout_id = seed_shopify_payout(
            conn, env["shopify_account_id"], env["company_id"],
            gross="200.00", fee="5.80", net="194.20",
        )

        # Add transactions that sum to 194.20 net
        _seed_payout_transaction(
            conn, payout_id, env["company_id"],
            gross="120.00", fee="3.48", net="116.52")
        _seed_payout_transaction(
            conn, payout_id, env["company_id"],
            gross="80.00", fee="2.32", net="77.68")

        result = call_action(mod.shopify_verify_payout, conn, ns(
            shopify_payout_id=payout_id,
        ))
        assert is_ok(result), result
        assert result["verification_status"] == "matched"
        assert result["net_matches"] is True
        assert result["transaction_count"] == 2

    def test_payout_sum_mismatch(self, conn, env):
        """Payout verification detects transaction sum mismatch."""
        payout_id = seed_shopify_payout(
            conn, env["shopify_account_id"], env["company_id"],
            gross="500.00", fee="14.50", net="485.50",
        )

        # Add transaction that doesn't match
        _seed_payout_transaction(
            conn, payout_id, env["company_id"],
            gross="400.00", fee="11.60", net="388.40")

        result = call_action(mod.shopify_verify_payout, conn, ns(
            shopify_payout_id=payout_id,
        ))
        assert is_ok(result), result
        assert result["verification_status"] == "mismatch"
        assert result["net_matches"] is False


class TestClearingBalance:

    def test_clearing_balance_zero(self, conn, env):
        """Clearing balance is zero when all orders are settled via payouts."""
        # Post an order
        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            subtotal="100.00", shipping="0", tax="0",
            total="100.00",
            shopify_order_id="CLR-001",
        )
        call_action(mod.shopify_post_order_gl, conn, ns(
            shopify_order_id=order_id,
        ))

        # Post a payout for the same amount
        payout_id = seed_shopify_payout(
            conn, env["shopify_account_id"], env["company_id"],
            gross="100.00", fee="0", net="100.00",
        )
        call_action(mod.shopify_post_payout_gl, conn, ns(
            shopify_payout_id=payout_id,
        ))

        result = call_action(mod.shopify_clearing_balance, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(result), result
        assert result["is_zero"] is True

    def test_clearing_balance_nonzero(self, conn, env):
        """Clearing balance is nonzero when orders await payout."""
        # Post an order but no payout
        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            subtotal="250.00", shipping="0", tax="0",
            total="250.00",
            shopify_order_id="CLR-NZERO",
        )
        call_action(mod.shopify_post_order_gl, conn, ns(
            shopify_order_id=order_id,
        ))

        result = call_action(mod.shopify_clearing_balance, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(result), result
        assert result["is_zero"] is False
        assert Decimal(result["balance"]) != Decimal("0")


class TestReconciliationLifecycle:

    def test_run_reconciliation_lifecycle(self, conn, env):
        """Full reconciliation run creates a reconciliation_run record."""
        # Create some orders and payouts
        seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            shopify_order_id="RECON-001",
        )
        seed_shopify_payout(
            conn, env["shopify_account_id"], env["company_id"],
        )

        result = call_action(mod.shopify_run_reconciliation, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(result), result
        assert "run_id" in result
        assert result["total_orders"] >= 1
        assert result["total_payouts"] >= 1
        assert result["run_status"] in ("completed", "discrepancy")

    def test_verify_payout(self, conn, env):
        """Verify payout works for payouts with no transactions."""
        payout_id = seed_shopify_payout(
            conn, env["shopify_account_id"], env["company_id"],
            gross="100.00", fee="2.90",
        )

        result = call_action(mod.shopify_verify_payout, conn, ns(
            shopify_payout_id=payout_id,
        ))
        assert is_ok(result), result
        # No transactions = matched (self-consistent)
        assert result["verification_status"] == "matched"
        assert result["transaction_count"] == 0

    def test_match_bank_transaction(self, conn, env):
        """Manual bank transaction matching updates reconciliation_status."""
        payout_id = seed_shopify_payout(
            conn, env["shopify_account_id"], env["company_id"],
        )

        result = call_action(mod.shopify_match_bank_transaction, conn, ns(
            shopify_payout_id=payout_id,
            bank_reference="CHK-2026-03-21-001",
        ))
        assert is_ok(result), result
        assert result["reconciliation_status"] == "manual_matched"
        assert result["bank_reference"] == "CHK-2026-03-21-001"

        # Verify DB updated
        payout = conn.execute(
            "SELECT reconciliation_status FROM shopify_payout WHERE id = ?",
            (payout_id,)
        ).fetchone()
        assert payout["reconciliation_status"] == "manual_matched"

    def test_list_reconciliations(self, conn, env):
        """List reconciliations returns all runs for an account."""
        # Run a reconciliation first
        call_action(mod.shopify_run_reconciliation, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))

        result = call_action(mod.shopify_list_reconciliations, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(result), result
        assert result["count"] >= 1

    def test_get_reconciliation(self, conn, env):
        """Get reconciliation returns details for a specific run."""
        run_result = call_action(mod.shopify_run_reconciliation, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(run_result), run_result

        result = call_action(mod.shopify_get_reconciliation, conn, ns(
            reconciliation_id=run_result["run_id"],
        ))
        assert is_ok(result), result
        assert result["id"] == run_result["run_id"]
        assert "period_start" in result
        assert "period_end" in result

    def test_reconciliation_with_refunds(self, conn, env):
        """Reconciliation accounts for refunds in clearing balance."""
        # Post an order
        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            subtotal="200.00", shipping="0", tax="0",
            total="200.00",
            shopify_order_id="RECON-REF-001",
        )
        call_action(mod.shopify_post_order_gl, conn, ns(
            shopify_order_id=order_id,
        ))

        # Post a refund
        refund_id = seed_shopify_refund(
            conn, order_id, env["company_id"],
            refund_amount="50.00",
        )
        call_action(mod.shopify_post_refund_gl, conn, ns(
            shopify_refund_id=refund_id,
        ))

        # Post a payout for net (200 - 50 = 150)
        payout_id = seed_shopify_payout(
            conn, env["shopify_account_id"], env["company_id"],
            gross="150.00", fee="0", net="150.00",
        )
        call_action(mod.shopify_post_payout_gl, conn, ns(
            shopify_payout_id=payout_id,
        ))

        # Run reconciliation
        result = call_action(mod.shopify_run_reconciliation, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(result), result
        # Expected clearing: 200 (order) - 150 (payout) - 50 (refund) = 0
        assert result["expected_clearing_balance"] == "0.00"
