"""Tests for erpclaw-integrations-stripe GL rule actions.

Covers: add-gl-rule, add-gl-rule (invalid account), list-gl-rules,
        update-gl-rule, delete-gl-rule, preview-gl-posting.
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
    seed_gl_account, build_stripe_env, _uuid,
)
from gl_rules import ACTIONS
from stripe_helpers import now_iso


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _add_rule(conn, env, debit_id, credit_id, txn_type="charge",
              fee_id=None, priority=0, match_field=None, match_value=None):
    """Add a GL rule and return the result."""
    return call_action(ACTIONS["stripe-add-gl-rule"], conn, ns(
        stripe_account_id=env["stripe_account_id"],
        transaction_type=txn_type,
        debit_account_id=debit_id,
        credit_account_id=credit_id,
        fee_account_id=fee_id,
        cost_center_id=None,
        priority=priority,
        match_field=match_field,
        match_value=match_value,
    ))


def _seed_charge(conn, stripe_account_id, company_id, stripe_id,
                 amount="50.00", customer_stripe_id="cus_test"):
    """Insert a stripe_charge row for test setup."""
    charge_id = _uuid()
    now = now_iso()
    conn.execute(
        """INSERT INTO stripe_charge
            (id, stripe_id, stripe_account_id, amount, currency,
             customer_stripe_id, status, amount_refunded, disputed,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, ?, 'usd', ?, 'succeeded', '0', 0, ?, ?, ?)""",
        (charge_id, stripe_id, stripe_account_id, amount,
         customer_stripe_id, company_id, now, now)
    )
    conn.commit()
    return charge_id


def _seed_balance_txn(conn, stripe_account_id, company_id, stripe_id,
                      source_id, amount="50.00", fee="1.50", net="48.50"):
    """Insert a stripe_balance_transaction row for test setup."""
    bt_id = _uuid()
    now = now_iso()
    conn.execute(
        """INSERT INTO stripe_balance_transaction
            (id, stripe_id, stripe_account_id, type, source_id, source_type,
             amount, fee, net, currency, status, reconciled,
             company_id, created_stripe, created_at)
           VALUES (?, ?, ?, 'charge', ?, 'charge',
             ?, ?, ?, 'usd', 'available', 0,
             ?, ?, ?)""",
        (bt_id, stripe_id, stripe_account_id, source_id,
         amount, fee, net, company_id, now, now)
    )
    conn.commit()
    return bt_id


# ===========================================================================
# 1. test_add_gl_rule
# ===========================================================================
class TestAddGLRule:

    def test_add_gl_rule(self, conn):
        """Adding a GL rule should create a new rule with correct fields."""
        env = build_stripe_env(conn)
        debit_id = seed_gl_account(conn, env["company_id"], "Clearing", "asset", "receivable")
        credit_id = seed_gl_account(conn, env["company_id"], "Revenue", "income", "revenue")

        result = _add_rule(conn, env, debit_id, credit_id, priority=10)
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["transaction_type"] == "charge"
        assert result["debit_account_id"] == debit_id
        assert result["credit_account_id"] == credit_id
        assert result["priority"] == 10
        assert "gl_rule_id" in result

        # Verify in DB
        row = conn.execute(
            "SELECT * FROM stripe_gl_rule WHERE id = ?",
            (result["gl_rule_id"],)
        ).fetchone()
        assert row is not None
        assert row["transaction_type"] == "charge"
        assert row["is_active"] == 1
        assert row["priority"] == 10


# ===========================================================================
# 2. test_add_gl_rule_invalid_account
# ===========================================================================
class TestAddGLRuleInvalidAccount:

    def test_add_gl_rule_invalid_account(self, conn):
        """Adding a GL rule with a nonexistent account should fail."""
        env = build_stripe_env(conn)
        valid_id = seed_gl_account(conn, env["company_id"], "Valid", "asset", "receivable")

        result = _add_rule(conn, env, "nonexistent-id", valid_id)
        assert is_error(result)

        result2 = _add_rule(conn, env, valid_id, "nonexistent-id")
        assert is_error(result2)


# ===========================================================================
# 3. test_list_gl_rules
# ===========================================================================
class TestListGLRules:

    def test_list_gl_rules(self, conn):
        """Listing GL rules should return active rules ordered by priority desc."""
        env = build_stripe_env(conn)
        debit_id = seed_gl_account(conn, env["company_id"], "Clearing", "asset", "receivable")
        credit_id = seed_gl_account(conn, env["company_id"], "Revenue", "income", "revenue")

        _add_rule(conn, env, debit_id, credit_id, txn_type="charge", priority=5)
        _add_rule(conn, env, debit_id, credit_id, txn_type="refund", priority=10)

        result = call_action(ACTIONS["stripe-list-gl-rules"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
        ))
        assert is_ok(result)
        assert result["count"] == 2
        # Higher priority first
        assert result["gl_rules"][0]["priority"] == 10
        assert result["gl_rules"][1]["priority"] == 5


# ===========================================================================
# 4. test_update_gl_rule
# ===========================================================================
class TestUpdateGLRule:

    def test_update_gl_rule(self, conn):
        """Updating a GL rule should persist the changes."""
        env = build_stripe_env(conn)
        debit_id = seed_gl_account(conn, env["company_id"], "Clearing", "asset", "receivable")
        credit_id = seed_gl_account(conn, env["company_id"], "Revenue", "income", "revenue")
        new_credit = seed_gl_account(conn, env["company_id"], "New Revenue", "income", "revenue")

        add_result = _add_rule(conn, env, debit_id, credit_id, priority=5)
        assert is_ok(add_result)
        rule_id = add_result["gl_rule_id"]

        result = call_action(ACTIONS["stripe-update-gl-rule"], conn, ns(
            gl_rule_id=rule_id,
            transaction_type=None,
            debit_account_id=None,
            credit_account_id=new_credit,
            fee_account_id=None,
            match_field=None,
            match_value=None,
            cost_center_id=None,
            priority=20,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert "credit_account_id" in result["updated_fields"]
        assert "priority" in result["updated_fields"]

        # Verify in DB
        row = conn.execute(
            "SELECT credit_account_id, priority FROM stripe_gl_rule WHERE id = ?",
            (rule_id,)
        ).fetchone()
        assert row["credit_account_id"] == new_credit
        assert row["priority"] == 20


# ===========================================================================
# 5. test_delete_gl_rule
# ===========================================================================
class TestDeleteGLRule:

    def test_delete_gl_rule(self, conn):
        """Deleting a GL rule should set is_active=0 (soft delete)."""
        env = build_stripe_env(conn)
        debit_id = seed_gl_account(conn, env["company_id"], "Clearing", "asset", "receivable")
        credit_id = seed_gl_account(conn, env["company_id"], "Revenue", "income", "revenue")

        add_result = _add_rule(conn, env, debit_id, credit_id)
        assert is_ok(add_result)
        rule_id = add_result["gl_rule_id"]

        result = call_action(ACTIONS["stripe-delete-gl-rule"], conn, ns(
            gl_rule_id=rule_id,
        ))
        assert is_ok(result)
        assert result["gl_rule_id"] == rule_id

        # Verify soft delete in DB
        row = conn.execute(
            "SELECT is_active FROM stripe_gl_rule WHERE id = ?",
            (rule_id,)
        ).fetchone()
        assert row["is_active"] == 0

        # Should not appear in list-gl-rules (which filters is_active=1)
        list_result = call_action(ACTIONS["stripe-list-gl-rules"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
        ))
        assert is_ok(list_result)
        assert list_result["count"] == 0


# ===========================================================================
# 6. test_preview_gl_posting
# ===========================================================================
class TestPreviewGLPosting:

    def test_preview_gl_posting(self, conn):
        """Preview should show what GL entries WOULD be created, without posting."""
        env = build_stripe_env(conn)
        debit_id = seed_gl_account(conn, env["company_id"], "Clearing", "asset", "receivable")
        credit_id = seed_gl_account(conn, env["company_id"], "Revenue", "income", "revenue")
        fee_id = seed_gl_account(conn, env["company_id"], "Fees", "expense", "expense")

        # Add a GL rule for charges with fee split
        _add_rule(conn, env, debit_id, credit_id, fee_id=fee_id, priority=10)

        # Seed a charge and its balance transaction
        _seed_charge(conn, env["stripe_account_id"], env["company_id"],
                     "ch_preview001", amount="100.00")
        _seed_balance_txn(conn, env["stripe_account_id"], env["company_id"],
                          "txn_preview001", "ch_preview001",
                          amount="100.00", fee="2.90", net="97.10")

        result = call_action(ACTIONS["stripe-preview-gl-posting"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            charge_stripe_id="ch_preview001",
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["is_preview"] is True
        assert result["charge_amount"] == "100.00"
        assert result["fee_amount"] == "2.90"
        assert result["net_amount"] == "97.10"
        assert result["rule_applied"] is not None
        assert result["rule_applied"]["transaction_type"] == "charge"

        # Should have 3 entries: debit clearing, credit revenue (net), debit fees
        entries = result["gl_entries"]
        assert len(entries) == 3

        # First entry: debit clearing for gross amount
        assert entries[0]["account_id"] == debit_id
        assert entries[0]["debit"] == "100.00"
        assert entries[0]["credit"] == "0"

        # Second entry: credit revenue for net amount
        assert entries[1]["account_id"] == credit_id
        assert entries[1]["credit"] == "97.10"

        # Third entry: debit fees
        assert entries[2]["account_id"] == fee_id
        assert entries[2]["debit"] == "2.90"
