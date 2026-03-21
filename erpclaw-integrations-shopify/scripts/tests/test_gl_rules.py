"""L1 tests for ERPClaw Integrations Shopify -- GL rules domain.

5 tests covering GL routing rule CRUD and preview.
"""
import pytest
from shopify_test_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_gl_account, seed_shopify_order,
)

mod = load_db_query()


class TestGLRules:

    def test_add_gl_rule(self, conn, env):
        """Adding a GL rule creates a shopify_gl_rule record."""
        debit_acct = seed_gl_account(
            conn, env["company_id"], "Custom Clearing", "asset", "bank")
        credit_acct = seed_gl_account(
            conn, env["company_id"], "Custom Revenue", "income", "revenue",
            "credit_normal")

        result = call_action(mod.shopify_add_gl_rule, conn, ns(
            shopify_account_id=env["shopify_account_id"],
            rule_name="Custom Order Rule",
            transaction_type="order",
            debit_account_id=debit_acct,
            credit_account_id=credit_acct,
            priority=10,
        ))
        assert is_ok(result), result
        assert result["rule_name"] == "Custom Order Rule"
        assert result["transaction_type"] == "order"
        assert result["is_active"] == 1
        assert result["priority"] == 10
        assert "id" in result

    def test_update_gl_rule(self, conn, env):
        """Updating a GL rule modifies its fields."""
        debit_acct = seed_gl_account(
            conn, env["company_id"], "DR Acct", "asset", "bank")
        credit_acct = seed_gl_account(
            conn, env["company_id"], "CR Acct", "income", "revenue",
            "credit_normal")

        add_result = call_action(mod.shopify_add_gl_rule, conn, ns(
            shopify_account_id=env["shopify_account_id"],
            rule_name="Update Test Rule",
            transaction_type="refund",
            debit_account_id=debit_acct,
            credit_account_id=credit_acct,
        ))
        assert is_ok(add_result), add_result

        result = call_action(mod.shopify_update_gl_rule, conn, ns(
            gl_rule_id=add_result["id"],
            priority=99,
        ))
        assert is_ok(result), result
        assert "priority" in result["updated_fields"]

    def test_list_gl_rules(self, conn, env):
        """Listing GL rules returns rules for the account."""
        debit_acct = seed_gl_account(
            conn, env["company_id"], "List DR", "asset", "bank")
        credit_acct = seed_gl_account(
            conn, env["company_id"], "List CR", "income", "revenue",
            "credit_normal")

        call_action(mod.shopify_add_gl_rule, conn, ns(
            shopify_account_id=env["shopify_account_id"],
            rule_name="List Test Rule",
            transaction_type="payout",
            debit_account_id=debit_acct,
            credit_account_id=credit_acct,
        ))

        result = call_action(mod.shopify_list_gl_rules, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(result), result
        assert result["count"] >= 1
        assert any(r["rule_name"] == "List Test Rule" for r in result["gl_rules"])

    def test_delete_gl_rule(self, conn, env):
        """Deleting a GL rule sets is_active=0 (soft delete)."""
        debit_acct = seed_gl_account(
            conn, env["company_id"], "Del DR", "asset", "bank")
        credit_acct = seed_gl_account(
            conn, env["company_id"], "Del CR", "income", "revenue",
            "credit_normal")

        add_result = call_action(mod.shopify_add_gl_rule, conn, ns(
            shopify_account_id=env["shopify_account_id"],
            rule_name="Delete Test Rule",
            transaction_type="dispute",
            debit_account_id=debit_acct,
            credit_account_id=credit_acct,
        ))

        result = call_action(mod.shopify_delete_gl_rule, conn, ns(
            gl_rule_id=add_result["id"],
        ))
        assert is_ok(result), result
        assert result["is_active"] == 0

        # Verify double-delete fails
        result2 = call_action(mod.shopify_delete_gl_rule, conn, ns(
            gl_rule_id=add_result["id"],
        ))
        assert is_error(result2)

    def test_preview_gl(self, conn, env):
        """Preview GL shows expected entries without posting."""
        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            subtotal="200.00", shipping="15.00", tax="16.00",
            discount="10.00",
        )

        result = call_action(mod.shopify_preview_gl, conn, ns(
            shopify_order_id=order_id,
        ))
        assert is_ok(result), result
        assert result["note"] == "Dry-run preview. No GL entries were posted."
        assert len(result["gl_entries"]) >= 3  # Clearing, Revenue, Shipping, Tax
        assert result["total_amount"] == "221.00"  # 200 + 15 + 16 - 10

        # Verify order GL status hasn't changed
        order = conn.execute(
            "SELECT gl_status FROM shopify_order WHERE id = ?",
            (order_id,)
        ).fetchone()
        assert order["gl_status"] == "pending"
