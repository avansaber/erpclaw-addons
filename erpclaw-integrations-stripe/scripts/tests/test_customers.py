"""Tests for erpclaw-integrations-stripe customer mapping actions.

Covers: map-customer, auto-map-customers, list-customer-maps,
        unmap-customer, get-customer-detail.
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
    seed_stripe_account, seed_erpclaw_customer, build_stripe_env, _uuid,
)
from customers import ACTIONS
from stripe_helpers import now_iso


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_customer_map(conn, stripe_account_id, stripe_customer_id,
                       stripe_name="", erpclaw_customer_id=None, company_id=None):
    """Insert a stripe_customer_map row directly for test setup."""
    map_id = _uuid()
    now = now_iso()
    conn.execute(
        """INSERT INTO stripe_customer_map
            (id, stripe_account_id, stripe_customer_id,
             erpclaw_customer_id, stripe_email, stripe_name,
             match_method, match_confidence, company_id, created_at)
           VALUES (?, ?, ?, ?, '', ?, 'manual', '0.0', ?, ?)""",
        (map_id, stripe_account_id, stripe_customer_id,
         erpclaw_customer_id, stripe_name, company_id, now)
    )
    conn.commit()
    return map_id


def _seed_charge(conn, stripe_account_id, company_id, stripe_id,
                 customer_stripe_id, amount="10.00", created_stripe=None):
    """Insert a stripe_charge row for test setup."""
    charge_id = _uuid()
    now = created_stripe or now_iso()
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


# ===========================================================================
# 1. test_map_customer_manual
# ===========================================================================
class TestMapCustomer:

    def test_map_customer_manual(self, conn):
        """Manually mapping a Stripe customer to an ERPClaw customer should succeed."""
        env = build_stripe_env(conn)
        erpclaw_cust = seed_erpclaw_customer(conn, env["company_id"], name="Manual Map Test")

        result = call_action(ACTIONS["stripe-map-customer"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            stripe_customer_id="cus_manual001",
            erpclaw_customer_id=erpclaw_cust,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["stripe_customer_id"] == "cus_manual001"
        assert result["erpclaw_customer_id"] == erpclaw_cust
        assert result["match_method"] == "manual"
        assert result["match_confidence"] == "1.0"

        # Verify in DB
        row = conn.execute(
            "SELECT * FROM stripe_customer_map WHERE stripe_customer_id = 'cus_manual001' "
            "AND stripe_account_id = ?",
            (env["stripe_account_id"],)
        ).fetchone()
        assert row is not None
        assert row["erpclaw_customer_id"] == erpclaw_cust
        assert row["match_method"] == "manual"


# ===========================================================================
# 2. test_auto_map_by_name
# ===========================================================================
class TestAutoMapByName:

    def test_auto_map_by_name(self, conn):
        """Auto-map should match unmatched customers by name."""
        env = build_stripe_env(conn)

        # Seed an erpclaw customer
        erpclaw_cust = seed_erpclaw_customer(conn, env["company_id"], name="Alice AutoMap")

        # Seed an unmatched stripe customer mapping with matching name
        _seed_customer_map(
            conn, env["stripe_account_id"], "cus_auto001",
            stripe_name="Alice AutoMap", erpclaw_customer_id=None,
            company_id=env["company_id"],
        )

        result = call_action(ACTIONS["stripe-auto-map-customers"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["matched_count"] == 1
        assert result["unmatched_scanned"] == 1
        assert len(result["matches"]) == 1
        assert result["matches"][0]["erpclaw_customer_id"] == erpclaw_cust

        # Verify in DB
        row = conn.execute(
            "SELECT erpclaw_customer_id, match_method, match_confidence "
            "FROM stripe_customer_map WHERE stripe_customer_id = 'cus_auto001' "
            "AND stripe_account_id = ?",
            (env["stripe_account_id"],)
        ).fetchone()
        assert row["erpclaw_customer_id"] == erpclaw_cust
        assert row["match_method"] == "name"
        assert row["match_confidence"] == "0.8"


# ===========================================================================
# 3. test_auto_map_no_match
# ===========================================================================
class TestAutoMapNoMatch:

    def test_auto_map_no_match(self, conn):
        """Auto-map should skip when no matching erpclaw customer exists."""
        env = build_stripe_env(conn)

        # Seed an unmatched stripe customer with a name that doesn't match anything
        _seed_customer_map(
            conn, env["stripe_account_id"], "cus_nomatch001",
            stripe_name="Nonexistent Person", erpclaw_customer_id=None,
            company_id=env["company_id"],
        )

        result = call_action(ACTIONS["stripe-auto-map-customers"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
        ))
        assert is_ok(result)
        assert result["matched_count"] == 0
        assert result["skipped_count"] == 1
        assert len(result["matches"]) == 0


# ===========================================================================
# 4. test_list_customer_maps
# ===========================================================================
class TestListCustomerMaps:

    def test_list_customer_maps(self, conn):
        """Listing customer maps should return all mappings for the account."""
        env = build_stripe_env(conn)

        _seed_customer_map(
            conn, env["stripe_account_id"], "cus_list001",
            stripe_name="Customer A", company_id=env["company_id"],
        )
        _seed_customer_map(
            conn, env["stripe_account_id"], "cus_list002",
            stripe_name="Customer B", company_id=env["company_id"],
        )

        result = call_action(ACTIONS["stripe-list-customer-maps"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            match_method=None,
            limit=50,
            offset=0,
        ))
        assert is_ok(result)
        assert result["count"] == 2
        stripe_ids = {m["stripe_customer_id"] for m in result["customer_maps"]}
        assert "cus_list001" in stripe_ids
        assert "cus_list002" in stripe_ids


# ===========================================================================
# 5. test_unmap_customer
# ===========================================================================
class TestUnmapCustomer:

    def test_unmap_customer(self, conn):
        """Unmapping should clear erpclaw_customer_id and reset match info."""
        env = build_stripe_env(conn)
        erpclaw_cust = seed_erpclaw_customer(conn, env["company_id"], name="Unmap Test")

        map_id = _seed_customer_map(
            conn, env["stripe_account_id"], "cus_unmap001",
            stripe_name="Unmap Test", erpclaw_customer_id=erpclaw_cust,
            company_id=env["company_id"],
        )

        result = call_action(ACTIONS["stripe-unmap-customer"], conn, ns(
            customer_map_id=map_id,
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["erpclaw_customer_id"] is None
        assert result["stripe_customer_id"] == "cus_unmap001"

        # Verify in DB
        row = conn.execute(
            "SELECT erpclaw_customer_id, match_confidence FROM stripe_customer_map WHERE id = ?",
            (map_id,)
        ).fetchone()
        assert row["erpclaw_customer_id"] is None
        assert row["match_confidence"] == "0.0"


# ===========================================================================
# 6. test_get_customer_detail
# ===========================================================================
class TestGetCustomerDetail:

    def test_get_customer_detail(self, conn):
        """Getting customer detail should return mapping info and charge summary."""
        env = build_stripe_env(conn)

        _seed_customer_map(
            conn, env["stripe_account_id"], "cus_detail001",
            stripe_name="Detail Test", company_id=env["company_id"],
        )

        result = call_action(ACTIONS["stripe-get-customer-detail"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            stripe_customer_id="cus_detail001",
        ))
        assert is_ok(result), f"Expected ok, got: {result}"
        assert result["stripe_customer_id"] == "cus_detail001"
        assert "charge_summary" in result
        assert result["charge_summary"]["total_charges"] == 0
        assert result["charge_summary"]["total_amount"] == "0"


# ===========================================================================
# 7. test_get_customer_detail_with_charges
# ===========================================================================
class TestGetCustomerDetailWithCharges:

    def test_get_customer_detail_with_charges(self, conn):
        """Customer detail should include charge aggregation when charges exist."""
        env = build_stripe_env(conn)

        _seed_customer_map(
            conn, env["stripe_account_id"], "cus_charges001",
            stripe_name="Charges Test", company_id=env["company_id"],
        )

        # Seed some charges for this customer
        _seed_charge(conn, env["stripe_account_id"], env["company_id"],
                     "ch_detail001", "cus_charges001", amount="25.50",
                     created_stripe="2026-01-15T10:00:00Z")
        _seed_charge(conn, env["stripe_account_id"], env["company_id"],
                     "ch_detail002", "cus_charges001", amount="100.00",
                     created_stripe="2026-02-20T10:00:00Z")
        _seed_charge(conn, env["stripe_account_id"], env["company_id"],
                     "ch_detail003", "cus_charges001", amount="7.49",
                     created_stripe="2026-03-01T10:00:00Z")

        result = call_action(ACTIONS["stripe-get-customer-detail"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            stripe_customer_id="cus_charges001",
        ))
        assert is_ok(result)
        summary = result["charge_summary"]
        assert summary["total_charges"] == 3
        assert Decimal(summary["total_amount"]) == Decimal("132.99")
        assert summary["last_charge_date"] == "2026-03-01T10:00:00Z"
