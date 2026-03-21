"""L1 tests for ERPClaw Integrations Shopify -- GL posting domain.

14 tests covering all GL posting scenarios. MOST CRITICAL test file.
Verifies exact Decimal amounts and double-entry balance invariants.
"""
import pytest
from decimal import Decimal
from shopify_test_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_shopify_order, seed_shopify_order_line_item,
    seed_shopify_refund, seed_shopify_payout, seed_shopify_dispute,
    seed_item,
)

mod = load_db_query()


def _gl_balance(conn, voucher_id):
    """Sum debits and credits for a journal_entry voucher. Returns (debit, credit)."""
    row = conn.execute(
        """SELECT COALESCE(decimal_sum(debit), '0') as total_debit,
                  COALESCE(decimal_sum(credit), '0') as total_credit
           FROM gl_entry
           WHERE voucher_id = ? AND is_cancelled = 0""",
        (voucher_id,)
    ).fetchone()
    return Decimal(str(row["total_debit"])), Decimal(str(row["total_credit"]))


def _gl_entry_count(conn, voucher_id):
    """Count non-cancelled GL entries for a voucher."""
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM gl_entry WHERE voucher_id = ? AND is_cancelled = 0",
        (voucher_id,)
    ).fetchone()
    return row["cnt"]


class TestPostOrderGL:

    def test_post_order_gl_creates_entries(self, conn, env):
        """Posting order GL creates journal_entry and gl_entry rows."""
        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            subtotal="100.00", shipping="10.00", tax="8.00",
        )
        result = call_action(mod.shopify_post_order_gl, conn, ns(
            shopify_order_id=order_id,
        ))
        assert is_ok(result), result
        assert result["gl_entry_count"] >= 3
        assert result["journal_entry_id"] is not None
        assert result["total_amount"] == "118.00"

        # Verify order GL status updated
        order = conn.execute(
            "SELECT gl_status, gl_voucher_id FROM shopify_order WHERE id = ?",
            (order_id,)
        ).fetchone()
        assert order["gl_status"] == "posted"
        assert order["gl_voucher_id"] == result["journal_entry_id"]

    def test_post_order_gl_balance(self, conn, env):
        """GL entries must be perfectly balanced (debit = credit)."""
        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            subtotal="250.00", shipping="20.00", tax="21.50",
        )
        result = call_action(mod.shopify_post_order_gl, conn, ns(
            shopify_order_id=order_id,
        ))
        assert is_ok(result), result

        debit, credit = _gl_balance(conn, result["journal_entry_id"])
        assert debit == credit, f"GL imbalance: debit={debit}, credit={credit}"
        assert debit == Decimal("291.50")  # 250 + 20 + 21.50

    def test_post_order_gl_with_cogs(self, conn, env):
        """When track_cogs=1, COGS entries are posted in separate entry_set."""
        # Enable track_cogs
        call_action(mod.shopify_update_account, conn, ns(
            shopify_account_id=env["shopify_account_id"],
            track_cogs=1,
        ))

        # Create item with a purchase rate for valuation
        item_id = seed_item(conn, env["company_id"], "COGS-ITEM-001")
        conn.execute(
            "UPDATE item SET last_purchase_rate = '25.00' WHERE id = ?",
            (item_id,)
        )
        conn.commit()

        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            subtotal="50.00", shipping="5.00", tax="4.00",
            shopify_order_id="COGS-1001",
        )
        seed_shopify_order_line_item(
            conn, order_id, env["company_id"],
            sku="COGS-ITEM-001", quantity=2, unit_price="25.00",
            item_id=item_id,
        )

        result = call_action(mod.shopify_post_order_gl, conn, ns(
            shopify_order_id=order_id,
        ))
        assert is_ok(result), result
        assert Decimal(result["cogs_amount"]) == Decimal("50.00")

        # Verify primary entries balance
        debit, credit = _gl_balance(conn, result["journal_entry_id"])
        assert debit == credit

    def test_post_order_gl_with_discount_net(self, conn, env):
        """Net discount method: discount is deducted from revenue."""
        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            subtotal="100.00", shipping="0", tax="0", discount="15.00",
            total="85.00",
            shopify_order_id="NET-DISC-001",
        )
        result = call_action(mod.shopify_post_order_gl, conn, ns(
            shopify_order_id=order_id,
        ))
        assert is_ok(result), result
        assert result["discount_method"] == "net"

        debit, credit = _gl_balance(conn, result["journal_entry_id"])
        assert debit == credit
        # Clearing DR = 85, Revenue CR = 85 (100 - 15)
        assert debit == Decimal("85.00")

    def test_post_order_gl_with_discount_gross(self, conn, env):
        """Gross discount method: discount posted as separate DR line."""
        # Set discount_method to gross
        call_action(mod.shopify_update_account, conn, ns(
            shopify_account_id=env["shopify_account_id"],
            discount_method="gross",
        ))

        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            subtotal="100.00", shipping="0", tax="0", discount="20.00",
            total="80.00",
            shopify_order_id="GROSS-DISC-001",
        )
        result = call_action(mod.shopify_post_order_gl, conn, ns(
            shopify_order_id=order_id,
        ))
        assert is_ok(result), result
        assert result["discount_method"] == "gross"

        debit, credit = _gl_balance(conn, result["journal_entry_id"])
        assert debit == credit
        # Clearing DR = 80, Discount DR = 20, Revenue CR = 100
        assert debit == Decimal("100.00")

    def test_post_order_gl_with_shipping_and_tax(self, conn, env):
        """Shipping and tax post to separate GL accounts."""
        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            subtotal="500.00", shipping="25.00", tax="42.50",
            shopify_order_id="SHIP-TAX-001",
        )
        result = call_action(mod.shopify_post_order_gl, conn, ns(
            shopify_order_id=order_id,
        ))
        assert is_ok(result), result

        # Verify individual GL lines exist
        entries = conn.execute(
            "SELECT * FROM gl_entry WHERE voucher_id = ? AND is_cancelled = 0",
            (result["journal_entry_id"],)
        ).fetchall()

        acct = env["shopify_account"]
        acct_ids = [e["account_id"] for e in entries]
        assert acct["shipping_revenue_account_id"] in acct_ids
        assert acct["tax_payable_account_id"] in acct_ids

        debit, credit = _gl_balance(conn, result["journal_entry_id"])
        assert debit == credit
        assert debit == Decimal("567.50")  # 500 + 25 + 42.50


class TestPostRefundGL:

    def test_post_refund_gl(self, conn, env):
        """Posting refund GL creates correct reversal entries."""
        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            shopify_order_id="REF-ORDER-001",
        )
        refund_id = seed_shopify_refund(
            conn, order_id, env["company_id"],
            refund_amount="50.00",
        )

        result = call_action(mod.shopify_post_refund_gl, conn, ns(
            shopify_refund_id=refund_id,
        ))
        assert is_ok(result), result
        assert result["refund_amount"] == "50.00"

        debit, credit = _gl_balance(conn, result["journal_entry_id"])
        assert debit == credit
        assert debit == Decimal("50.00")

        # Verify refund GL status
        refund = conn.execute(
            "SELECT gl_status FROM shopify_refund WHERE id = ?",
            (refund_id,)
        ).fetchone()
        assert refund["gl_status"] == "posted"

    def test_post_refund_gl_with_restock(self, conn, env):
        """Refund with restock reverses COGS entries."""
        # Enable track_cogs
        call_action(mod.shopify_update_account, conn, ns(
            shopify_account_id=env["shopify_account_id"],
            track_cogs=1,
        ))

        item_id = seed_item(conn, env["company_id"], "RESTOCK-ITEM")
        conn.execute(
            "UPDATE item SET last_purchase_rate = '30.00' WHERE id = ?",
            (item_id,)
        )
        conn.commit()

        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            shopify_order_id="RESTOCK-ORDER",
        )
        refund_id = seed_shopify_refund(
            conn, order_id, env["company_id"],
            refund_amount="60.00",
        )

        # Add refund line item with restock
        from shopify_test_helpers import _uuid, _now
        rli_id = _uuid()
        conn.execute(
            """INSERT INTO shopify_refund_line_item (
                id, shopify_refund_id_local, shopify_line_item_id,
                quantity, subtotal_amount, restock_type, item_id,
                company_id, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?)""",
            (rli_id, refund_id, "LI-001",
             2, "60.00", "return", item_id,
             env["company_id"], _now())
        )
        conn.commit()

        result = call_action(mod.shopify_post_refund_gl, conn, ns(
            shopify_refund_id=refund_id,
        ))
        assert is_ok(result), result
        assert Decimal(result["restock_cogs"]) == Decimal("60.00")

        debit, credit = _gl_balance(conn, result["journal_entry_id"])
        assert debit == credit


class TestPostPayoutGL:

    def test_post_payout_gl(self, conn, env):
        """Payout posting: DR Bank, CR Clearing."""
        payout_id = seed_shopify_payout(
            conn, env["shopify_account_id"], env["company_id"],
            gross="1000.00", fee="0", net="1000.00",
        )

        result = call_action(mod.shopify_post_payout_gl, conn, ns(
            shopify_payout_id=payout_id,
        ))
        assert is_ok(result), result
        assert result["net_amount"] == "1000.00"
        assert result["gross_amount"] == "1000.00"

        debit, credit = _gl_balance(conn, result["journal_entry_id"])
        assert debit == credit
        assert debit == Decimal("1000.00")

    def test_post_payout_gl_with_fees(self, conn, env):
        """Payout with fees: DR Bank + DR Fees, CR Clearing."""
        payout_id = seed_shopify_payout(
            conn, env["shopify_account_id"], env["company_id"],
            gross="500.00", fee="14.50",
        )

        result = call_action(mod.shopify_post_payout_gl, conn, ns(
            shopify_payout_id=payout_id,
        ))
        assert is_ok(result), result
        assert result["net_amount"] == "485.50"
        assert result["fee_amount"] == "14.50"
        assert result["gross_amount"] == "500.00"

        debit, credit = _gl_balance(conn, result["journal_entry_id"])
        assert debit == credit
        # Bank DR = 485.50 + Fees DR = 14.50 = 500.00
        assert debit == Decimal("500.00")


class TestPostDisputeGL:

    def test_post_dispute_gl_open(self, conn, env):
        """Open dispute: DR Chargeback Loss + DR Fee, CR Clearing."""
        dispute_id = seed_shopify_dispute(
            conn, env["shopify_account_id"], env["company_id"],
            amount="75.00", fee_amount="15.00",
            status="needs_response",
        )

        result = call_action(mod.shopify_post_dispute_gl, conn, ns(
            shopify_dispute_id=dispute_id,
        ))
        assert is_ok(result), result
        assert result["dispute_amount"] == "75.00"
        assert result["fee_amount"] == "15.00"
        assert result["dispute_status"] == "needs_response"

        debit, credit = _gl_balance(conn, result["journal_entry_id"])
        assert debit == credit
        # Chargeback DR = 75 + Fee DR = 15 = 90
        assert debit == Decimal("90.00")

    def test_post_dispute_gl_won(self, conn, env):
        """Won dispute reverses previously posted GL entries."""
        # First post the dispute
        dispute_id = seed_shopify_dispute(
            conn, env["shopify_account_id"], env["company_id"],
            amount="50.00", fee_amount="15.00",
            status="needs_response",
        )

        post_result = call_action(mod.shopify_post_dispute_gl, conn, ns(
            shopify_dispute_id=dispute_id,
        ))
        assert is_ok(post_result), post_result

        # Update dispute status to won
        conn.execute(
            "UPDATE shopify_dispute SET status = 'won' WHERE id = ?",
            (dispute_id,)
        )
        conn.commit()

        # Post again -- should reverse
        result = call_action(mod.shopify_post_dispute_gl, conn, ns(
            shopify_dispute_id=dispute_id,
        ))
        assert is_ok(result), result
        assert result["action"] == "reversed"
        assert result["reversal_gl_entry_count"] >= 2


class TestPostGiftCardGL:

    def test_post_gift_card_sold(self, conn, env):
        """Gift card sold: DR Clearing, CR Gift Card Liability (deferred revenue)."""
        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            subtotal="50.00", shipping="0", tax="0",
            total="50.00",
            shopify_order_id="GC-SOLD-001",
        )

        result = call_action(mod.shopify_post_gift_card_gl, conn, ns(
            shopify_order_id=order_id,
            gift_card_type="sold",
        ))
        assert is_ok(result), result
        assert result["gift_card_type"] == "sold"
        assert result["amount"] == "50.00"

        debit, credit = _gl_balance(conn, result["journal_entry_id"])
        assert debit == credit
        assert debit == Decimal("50.00")

        # Verify the credit goes to gift card liability account
        acct = env["shopify_account"]
        gc_entries = conn.execute(
            """SELECT credit FROM gl_entry
               WHERE voucher_id = ? AND account_id = ? AND is_cancelled = 0""",
            (result["journal_entry_id"], acct["gift_card_liability_account_id"])
        ).fetchall()
        assert len(gc_entries) == 1
        assert Decimal(str(gc_entries[0]["credit"])) == Decimal("50.00")


class TestBulkPostGL:

    def test_bulk_post_gl(self, conn, env):
        """Bulk posting processes all unposted orders and payouts."""
        # Create 3 unposted orders
        for i in range(3):
            seed_shopify_order(
                conn, env["shopify_account_id"], env["company_id"],
                shopify_order_id=f"BULK-{i+1}",
                subtotal="100.00", shipping="0", tax="0",
                total="100.00",
            )

        # Create 1 unposted payout
        seed_shopify_payout(
            conn, env["shopify_account_id"], env["company_id"],
            gross="300.00", fee="8.70",
        )

        result = call_action(mod.shopify_bulk_post_gl, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(result), result
        assert result["orders_posted"] == 3
        assert result["payouts_posted"] == 1
        assert result["total_posted"] == 4
