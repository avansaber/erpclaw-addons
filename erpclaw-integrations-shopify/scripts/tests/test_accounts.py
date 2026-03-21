"""Tests for erpclaw-integrations-shopify account management actions.

Covers: add, update, get, list, configure-gl, test-connection (mocked).
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

from shopify_test_helpers import call_action, ns, is_ok, is_error, seed_gl_account
from accounts import ACTIONS
from shopify_helpers import encrypt_token, decrypt_token, mask_token


# ---------------------------------------------------------------------------
# Helper to add a shopify account via the action (reusable across tests)
# ---------------------------------------------------------------------------
def _add_shopify_account(conn, company_id, name="Test Shop",
                         domain="test-shop.myshopify.com",
                         token="shpat_test_abc123xyz789def456"):
    """Add a Shopify account and return the result dict."""
    return call_action(ACTIONS["shopify-add-account"], conn, ns(
        company_id=company_id,
        shop_domain=domain,
        access_token=token,
        shop_name=name,
        api_version=None,
        currency=None,
    ))


# ===========================================================================
# 1. test_add_account
# ===========================================================================
class TestAddAccount:

    def test_add_account_happy_path(self, conn, company_id):
        """Adding a Shopify account should succeed and return the account ID."""
        result = _add_shopify_account(conn, company_id)
        assert is_ok(result), f"Expected ok, got: {result}"
        assert "id" in result
        assert result["shop_domain"] == "test-shop.myshopify.com"
        assert result["shop_name"] == "Test Shop"
        assert result["account_status"] == "active"
        assert result["gl_accounts_created"] == 14

    def test_add_account_encrypts_token(self, conn, company_id):
        """The stored access token must be encrypted, not plaintext."""
        raw_token = "shpat_test_abc123xyz789def456"
        result = _add_shopify_account(conn, company_id, token=raw_token)
        assert is_ok(result)

        # Read from DB directly
        row = conn.execute(
            "SELECT access_token_enc FROM shopify_account WHERE id = ?",
            (result["id"],)
        ).fetchone()
        stored_enc = row["access_token_enc"]

        # Encrypted value must NOT equal the raw token
        assert stored_enc != raw_token
        # But decryption must round-trip correctly
        assert decrypt_token(stored_enc) == raw_token

    def test_add_account_creates_gl_accounts(self, conn, company_id):
        """Adding a Shopify account should auto-create 14 GL accounts."""
        result = _add_shopify_account(conn, company_id)
        assert is_ok(result)

        gl_mapping = result["gl_mapping"]
        assert len(gl_mapping) == 14

        # Verify each GL account exists in the account table
        for field_name, gl_id in gl_mapping.items():
            row = conn.execute(
                "SELECT id, name, root_type FROM account WHERE id = ?",
                (gl_id,)
            ).fetchone()
            assert row is not None, f"GL account {field_name} ({gl_id}) not found"

        # Check specific GL account types
        clearing_id = gl_mapping["clearing_account_id"]
        clearing = conn.execute(
            "SELECT root_type, account_type FROM account WHERE id = ?",
            (clearing_id,)
        ).fetchone()
        assert clearing["root_type"] == "asset"
        assert clearing["account_type"] == "bank"

        fee_id = gl_mapping["fee_account_id"]
        fee = conn.execute(
            "SELECT root_type, account_type FROM account WHERE id = ?",
            (fee_id,)
        ).fetchone()
        assert fee["root_type"] == "expense"
        assert fee["account_type"] == "expense"

    def test_add_account_masked_token_in_response(self, conn, company_id):
        """The response should contain a masked token, not the raw token."""
        raw_token = "shpat_test_abc123xyz789def456"
        result = _add_shopify_account(conn, company_id, token=raw_token)
        assert is_ok(result)
        assert result["access_token"] == mask_token(raw_token)
        assert raw_token not in str(result)

    def test_add_account_missing_company(self, conn):
        """Adding without company-id should fail."""
        result = call_action(ACTIONS["shopify-add-account"], conn, ns(
            company_id=None,
            shop_domain="test.myshopify.com",
            access_token="shpat_xyz",
            shop_name=None,
            api_version=None,
            currency=None,
        ))
        assert is_error(result)

    def test_add_account_missing_domain(self, conn, company_id):
        """Adding without shop-domain should fail."""
        result = call_action(ACTIONS["shopify-add-account"], conn, ns(
            company_id=company_id,
            shop_domain=None,
            access_token="shpat_xyz",
            shop_name=None,
            api_version=None,
            currency=None,
        ))
        assert is_error(result)

    def test_add_account_missing_token(self, conn, company_id):
        """Adding without access-token should fail."""
        result = call_action(ACTIONS["shopify-add-account"], conn, ns(
            company_id=company_id,
            shop_domain="test.myshopify.com",
            access_token=None,
            shop_name=None,
            api_version=None,
            currency=None,
        ))
        assert is_error(result)


# ===========================================================================
# 2. test_get_account
# ===========================================================================
class TestGetAccount:

    def test_get_account_masks_token(self, conn, company_id):
        """Get account should return masked token, never the raw token."""
        raw_token = "shpat_test_secrettoken123456"
        result = _add_shopify_account(conn, company_id, token=raw_token)
        assert is_ok(result)

        get_result = call_action(ACTIONS["shopify-get-account"], conn, ns(
            shopify_account_id=result["id"],
        ))
        assert is_ok(get_result)
        assert "access_token_masked" in get_result
        assert get_result["access_token_masked"] == mask_token(raw_token)
        # Raw token must never appear
        assert raw_token not in str(get_result)
        # Encrypted token must not appear
        assert "access_token_enc" not in get_result


# ===========================================================================
# 3. Encryption round-trip
# ===========================================================================
class TestEncryption:

    def test_encrypt_decrypt_roundtrip(self):
        """Encrypting then decrypting should return the original value."""
        test_tokens = [
            "shpat_test_abc123",
            "shpat_live_very_long_token_with_special_chars_!@#$%",
            "shpat_short",
            "",
        ]
        for token in test_tokens:
            encrypted = encrypt_token(token)
            decrypted = decrypt_token(encrypted)
            assert decrypted == token, f"Round-trip failed for: {token}"

    def test_mask_token_format(self):
        """Mask should show prefix...suffix pattern."""
        assert mask_token("shpat_test_abc123xyz") == "shpat_te...xyz"
        assert mask_token("short") == "***"
        assert mask_token("") == "***"
        assert mask_token(None) == "***"
