"""Tests for erpclaw-integrations-stripe account management actions.

Covers: add, update, get, list, configure-gl-mapping, test-connection (mocked).
"""
import os
import sys
from unittest.mock import patch, MagicMock

# Ensure test helpers and scripts are importable
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_test_helpers import call_action, ns, is_ok, is_error, seed_gl_account
from accounts import ACTIONS
from stripe_helpers import encrypt_key, decrypt_key, mask_key


# ---------------------------------------------------------------------------
# Helper to add a stripe account via the action (reusable across tests)
# ---------------------------------------------------------------------------
def _add_stripe_account(conn, company_id, name="Test Stripe", key="rk_test_abc123xyz789def456"):
    """Add a Stripe account and return the result dict."""
    return call_action(ACTIONS["stripe-add-account"], conn, ns(
        company_id=company_id,
        account_name=name,
        api_key=key,
        mode="test",
        webhook_secret=None,
        is_connect_platform=None,
    ))


# ===========================================================================
# 1. test_add_account_happy_path
# ===========================================================================
class TestAddAccount:

    def test_add_account_happy_path(self, conn, company_id):
        """Adding a Stripe account should succeed and return the account ID."""
        result = _add_stripe_account(conn, company_id)
        assert is_ok(result), f"Expected ok, got: {result}"
        assert "id" in result
        assert result["account_name"] == "Test Stripe"
        assert result["mode"] == "test"
        assert result["account_status"] == "active"
        assert result["gl_accounts_created"] == 5

    def test_add_account_encrypts_key(self, conn, company_id):
        """The stored API key must be encrypted, not plaintext."""
        raw_key = "rk_test_abc123xyz789def456"
        result = _add_stripe_account(conn, company_id, key=raw_key)
        assert is_ok(result)

        # Read from DB directly
        row = conn.execute(
            "SELECT restricted_key_enc FROM stripe_account WHERE id = ?",
            (result["id"],)
        ).fetchone()
        stored_enc = row["restricted_key_enc"]

        # Encrypted value must NOT equal the raw key
        assert stored_enc != raw_key
        # But decryption must round-trip correctly
        assert decrypt_key(stored_enc) == raw_key

    def test_add_account_creates_gl_accounts(self, conn, company_id):
        """Adding a Stripe account should auto-create 5 GL accounts."""
        result = _add_stripe_account(conn, company_id)
        assert is_ok(result)

        gl_mapping = result["gl_mapping"]
        assert len(gl_mapping) == 5

        # Verify each GL account exists in the account table
        for field_name, gl_id in gl_mapping.items():
            row = conn.execute(
                "SELECT id, name, root_type FROM account WHERE id = ?",
                (gl_id,)
            ).fetchone()
            assert row is not None, f"GL account {field_name} ({gl_id}) not found in account table"

        # Check specific GL account types
        clearing_id = gl_mapping["stripe_clearing_account_id"]
        clearing = conn.execute(
            "SELECT root_type, account_type FROM account WHERE id = ?",
            (clearing_id,)
        ).fetchone()
        assert clearing["root_type"] == "asset"
        assert clearing["account_type"] == "bank"

        fees_id = gl_mapping["stripe_fees_account_id"]
        fees = conn.execute(
            "SELECT root_type, account_type FROM account WHERE id = ?",
            (fees_id,)
        ).fetchone()
        assert fees["root_type"] == "expense"
        assert fees["account_type"] == "expense"

    def test_add_account_masked_key_in_response(self, conn, company_id):
        """The response should contain a masked key, not the raw key."""
        raw_key = "rk_test_abc123xyz789def456"
        result = _add_stripe_account(conn, company_id, key=raw_key)
        assert is_ok(result)
        assert result["api_key"] == mask_key(raw_key)
        assert raw_key not in str(result)

    def test_add_account_missing_company(self, conn):
        """Adding without company-id should fail."""
        result = call_action(ACTIONS["stripe-add-account"], conn, ns(
            company_id=None,
            account_name="Test",
            api_key="rk_test_xyz",
            mode="test",
            webhook_secret=None,
            is_connect_platform=None,
        ))
        assert is_error(result)

    def test_add_account_missing_name(self, conn, company_id):
        """Adding without account-name should fail."""
        result = call_action(ACTIONS["stripe-add-account"], conn, ns(
            company_id=company_id,
            account_name=None,
            api_key="rk_test_xyz",
            mode="test",
            webhook_secret=None,
            is_connect_platform=None,
        ))
        assert is_error(result)

    def test_add_account_missing_key(self, conn, company_id):
        """Adding without api-key should fail."""
        result = call_action(ACTIONS["stripe-add-account"], conn, ns(
            company_id=company_id,
            account_name="Test",
            api_key=None,
            mode="test",
            webhook_secret=None,
            is_connect_platform=None,
        ))
        assert is_error(result)


# ===========================================================================
# 2. test_update_account
# ===========================================================================
class TestUpdateAccount:

    def test_update_account_name(self, conn, company_id):
        """Updating account name should persist the change."""
        add_result = _add_stripe_account(conn, company_id)
        assert is_ok(add_result)
        acct_id = add_result["id"]

        result = call_action(ACTIONS["stripe-update-account"], conn, ns(
            stripe_account_id=acct_id,
            account_name="Updated Stripe Account",
            api_key=None,
            mode=None,
            status=None,
            webhook_secret=None,
        ))
        assert is_ok(result)
        assert "account_name" in result["updated_fields"]

        # Verify in DB
        row = conn.execute(
            "SELECT account_name FROM stripe_account WHERE id = ?",
            (acct_id,)
        ).fetchone()
        assert row["account_name"] == "Updated Stripe Account"

    def test_update_account_reencrypts_key(self, conn, company_id):
        """Updating the API key should store the new encrypted value."""
        add_result = _add_stripe_account(conn, company_id)
        assert is_ok(add_result)
        acct_id = add_result["id"]

        new_key = "rk_test_newkey999"
        result = call_action(ACTIONS["stripe-update-account"], conn, ns(
            stripe_account_id=acct_id,
            account_name=None,
            api_key=new_key,
            mode=None,
            status=None,
            webhook_secret=None,
        ))
        assert is_ok(result)

        row = conn.execute(
            "SELECT restricted_key_enc FROM stripe_account WHERE id = ?",
            (acct_id,)
        ).fetchone()
        assert decrypt_key(row["restricted_key_enc"]) == new_key

    def test_update_account_not_found(self, conn, company_id):
        """Updating a nonexistent account should fail."""
        result = call_action(ACTIONS["stripe-update-account"], conn, ns(
            stripe_account_id="nonexistent-id",
            account_name="X",
            api_key=None,
            mode=None,
            status=None,
            webhook_secret=None,
        ))
        assert is_error(result)

    def test_update_account_no_fields(self, conn, company_id):
        """Updating with no fields should fail."""
        add_result = _add_stripe_account(conn, company_id)
        assert is_ok(add_result)

        result = call_action(ACTIONS["stripe-update-account"], conn, ns(
            stripe_account_id=add_result["id"],
            account_name=None,
            api_key=None,
            mode=None,
            status=None,
            webhook_secret=None,
        ))
        assert is_error(result)


# ===========================================================================
# 3. test_get_account
# ===========================================================================
class TestGetAccount:

    def test_get_account_masks_key(self, conn, company_id):
        """Get account should return masked key, never the raw key."""
        raw_key = "rk_test_secretkey123456"
        add_result = _add_stripe_account(conn, company_id, key=raw_key)
        assert is_ok(add_result)

        result = call_action(ACTIONS["stripe-get-account"], conn, ns(
            stripe_account_id=add_result["id"],
        ))
        assert is_ok(result)
        assert "api_key_masked" in result
        assert result["api_key_masked"] == mask_key(raw_key)
        # Raw key must never appear
        assert raw_key not in str(result)
        # Encrypted key must not appear
        assert "restricted_key_enc" not in result

    def test_get_account_not_found(self, conn, company_id):
        """Getting a nonexistent account should fail."""
        result = call_action(ACTIONS["stripe-get-account"], conn, ns(
            stripe_account_id="nonexistent-id",
        ))
        assert is_error(result)


# ===========================================================================
# 4. test_list_accounts
# ===========================================================================
class TestListAccounts:

    def test_list_accounts(self, conn, company_id):
        """Listing accounts should return all accounts for the company."""
        _add_stripe_account(conn, company_id, name="Account A")
        _add_stripe_account(conn, company_id, name="Account B")

        result = call_action(ACTIONS["stripe-list-accounts"], conn, ns(
            company_id=company_id,
        ))
        assert is_ok(result)
        assert result["count"] == 2
        names = [a["account_name"] for a in result["accounts"]]
        assert "Account A" in names
        assert "Account B" in names

    def test_list_accounts_empty(self, conn, company_id):
        """Listing with no accounts should return empty list."""
        result = call_action(ACTIONS["stripe-list-accounts"], conn, ns(
            company_id=company_id,
        ))
        assert is_ok(result)
        assert result["count"] == 0


# ===========================================================================
# 5. test_configure_gl_mapping
# ===========================================================================
class TestConfigureGLMapping:

    def test_configure_gl_mapping(self, conn, company_id):
        """Configuring GL mapping should update the stripe_account record."""
        add_result = _add_stripe_account(conn, company_id)
        assert is_ok(add_result)
        acct_id = add_result["id"]

        # Create a new GL account to use as replacement
        new_clearing = seed_gl_account(conn, company_id, "New Clearing", "asset", "receivable")

        result = call_action(ACTIONS["stripe-configure-gl-mapping"], conn, ns(
            stripe_account_id=acct_id,
            clearing_account_id=new_clearing,
            fees_account_id=None,
            payout_account_id=None,
            dispute_account_id=None,
            unearned_revenue_account_id=None,
            platform_revenue_account_id=None,
        ))
        assert is_ok(result)
        assert "stripe_clearing_account_id" in result["updated_mappings"]

        # Verify in DB
        row = conn.execute(
            "SELECT stripe_clearing_account_id FROM stripe_account WHERE id = ?",
            (acct_id,)
        ).fetchone()
        assert row["stripe_clearing_account_id"] == new_clearing

    def test_configure_gl_mapping_invalid_account(self, conn, company_id):
        """Configuring with a nonexistent GL account should fail."""
        add_result = _add_stripe_account(conn, company_id)
        assert is_ok(add_result)

        result = call_action(ACTIONS["stripe-configure-gl-mapping"], conn, ns(
            stripe_account_id=add_result["id"],
            clearing_account_id="nonexistent-gl-id",
            fees_account_id=None,
            payout_account_id=None,
            dispute_account_id=None,
            unearned_revenue_account_id=None,
            platform_revenue_account_id=None,
        ))
        assert is_error(result)

    def test_configure_gl_mapping_no_fields(self, conn, company_id):
        """Configuring with no mapping fields should fail."""
        add_result = _add_stripe_account(conn, company_id)
        assert is_ok(add_result)

        result = call_action(ACTIONS["stripe-configure-gl-mapping"], conn, ns(
            stripe_account_id=add_result["id"],
            clearing_account_id=None,
            fees_account_id=None,
            payout_account_id=None,
            dispute_account_id=None,
            unearned_revenue_account_id=None,
            platform_revenue_account_id=None,
        ))
        assert is_error(result)


# ===========================================================================
# 6. test_test_connection (mocked Stripe API)
# ===========================================================================
class TestTestConnection:

    def test_connection_success(self, conn, company_id):
        """Test connection with mocked Stripe API should succeed."""
        add_result = _add_stripe_account(conn, company_id)
        assert is_ok(add_result)

        # Mock the stripe module
        mock_stripe = MagicMock()
        mock_stripe.Account.retrieve.return_value = {
            "id": "acct_1234567890",
            "business_profile": {"name": "Test Business"},
            "charges_enabled": True,
            "payouts_enabled": True,
        }
        mock_stripe.error = MagicMock()
        mock_stripe.error.AuthenticationError = Exception
        mock_stripe.error.APIConnectionError = Exception

        with patch.dict("sys.modules", {"stripe": mock_stripe}):
            result = call_action(ACTIONS["stripe-test-connection"], conn, ns(
                stripe_account_id=add_result["id"],
            ))

        assert is_ok(result)
        assert result["connection"] == "success"
        assert result["stripe_account_id"] == "acct_1234567890"
        assert result["charges_enabled"] is True

    def test_connection_not_found(self, conn, company_id):
        """Test connection with nonexistent account should fail."""
        result = call_action(ACTIONS["stripe-test-connection"], conn, ns(
            stripe_account_id="nonexistent-id",
        ))
        assert is_error(result)


# ===========================================================================
# 7. Encryption round-trip
# ===========================================================================
class TestEncryption:

    def test_encrypt_decrypt_roundtrip(self):
        """Encrypting then decrypting should return the original value."""
        test_keys = [
            "rk_test_abc123",
            "rk_live_very_long_key_with_special_chars_!@#$%",
            "sk_test_short",
            "",
        ]
        for key in test_keys:
            encrypted = encrypt_key(key)
            decrypted = decrypt_key(encrypted)
            assert decrypted == key, f"Round-trip failed for: {key}"

    def test_mask_key_format(self):
        """Mask should show prefix...suffix pattern."""
        assert mask_key("rk_test_abc123xyz") == "rk_test_...xyz"
        assert mask_key("short") == "***"
        assert mask_key("") == "***"
        assert mask_key(None) == "***"
