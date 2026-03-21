"""L1 tests for ERPClaw Integrations Shopify -- mapping domain.

6 tests covering product and customer mapping actions.
"""
import pytest
from shopify_test_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_item, seed_customer, seed_shopify_order,
    seed_shopify_order_line_item,
)

mod = load_db_query()


class TestProductMapping:

    def test_map_product_manual(self, conn, env):
        """Manual product mapping creates integration_entity_map record."""
        item_id = seed_item(conn, env["company_id"], "SKU-WIDGET-001")
        result = call_action(mod.shopify_map_product, conn, ns(
            shopify_account_id=env["shopify_account_id"],
            shopify_product_id="gid://shopify/Product/12345",
            item_id=item_id,
        ))
        assert is_ok(result), result
        assert result["item_id"] == item_id
        assert result["shopify_product_id"] == "gid://shopify/Product/12345"
        assert result["item_code"] == "SKU-WIDGET-001"
        assert "map_id" in result

    def test_auto_map_by_sku(self, conn, env):
        """Auto-mapping matches Shopify line item SKUs to erpclaw items."""
        # Create items
        item_id_1 = seed_item(conn, env["company_id"], "TSHIRT-BLU-M")
        item_id_2 = seed_item(conn, env["company_id"], "TSHIRT-RED-L")

        # Create order with line items that have matching SKUs
        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"])
        seed_shopify_order_line_item(
            conn, order_id, env["company_id"], sku="TSHIRT-BLU-M")
        seed_shopify_order_line_item(
            conn, order_id, env["company_id"], sku="TSHIRT-RED-L")
        seed_shopify_order_line_item(
            conn, order_id, env["company_id"], sku="UNKNOWN-SKU")

        result = call_action(mod.shopify_auto_map_products, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(result), result
        assert result["matched"] == 2
        assert result["skipped"] == 1  # UNKNOWN-SKU
        assert result["total_skus"] == 3

    def test_list_product_maps(self, conn, env):
        """Listing product maps returns all mapped products."""
        item_id = seed_item(conn, env["company_id"], "SKU-LIST-TEST")

        # Map a product
        call_action(mod.shopify_map_product, conn, ns(
            shopify_account_id=env["shopify_account_id"],
            shopify_product_id="PROD-999",
            item_id=item_id,
        ))

        result = call_action(mod.shopify_list_product_maps, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(result), result
        assert result["count"] >= 1
        assert any(m["item_id"] == item_id for m in result["product_maps"])


class TestCustomerMapping:

    def test_map_customer(self, conn, env):
        """Manual customer mapping creates integration_entity_map record."""
        cust_id = seed_customer(conn, env["company_id"], "Jane Doe")
        result = call_action(mod.shopify_map_customer, conn, ns(
            shopify_account_id=env["shopify_account_id"],
            shopify_customer_id="gid://shopify/Customer/67890",
            customer_id=cust_id,
        ))
        assert is_ok(result), result
        assert result["customer_id"] == cust_id
        assert result["customer_name"] == "Jane Doe"
        assert "map_id" in result

    def test_auto_map_customer_by_name(self, conn, env):
        """Auto-mapping matches erpclaw customers by name."""
        seed_customer(conn, env["company_id"], "Alice Smith")
        seed_customer(conn, env["company_id"], "Bob Johnson")

        result = call_action(mod.shopify_auto_map_customers, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(result), result
        assert result["matched"] >= 2
        assert result["total_customers"] >= 2

    def test_list_customer_maps(self, conn, env):
        """Listing customer maps returns all mapped customers."""
        cust_id = seed_customer(conn, env["company_id"], "Test Customer")
        call_action(mod.shopify_map_customer, conn, ns(
            shopify_account_id=env["shopify_account_id"],
            shopify_customer_id="CUST-123",
            customer_id=cust_id,
        ))

        result = call_action(mod.shopify_list_customer_maps, conn, ns(
            shopify_account_id=env["shopify_account_id"],
        ))
        assert is_ok(result), result
        assert result["count"] >= 1
        assert any(m["customer_id"] == cust_id for m in result["customer_maps"])
