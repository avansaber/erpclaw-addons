"""L1 tests for ERPClaw Advanced Manufacturing skill (35 actions).

Tests cover: Shop Floor, Tools, ECOs, Process Recipes.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from advmfg_helpers import (
    load_db_query, call_action, ns, is_ok, is_error, _uuid,
)

M = load_db_query()


# ===================================================================
# Shop Floor Entries
# ===================================================================

class TestAddShopFloorEntry:
    def test_add_entry_ok(self, conn, env):
        r = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["entry_id"]
        assert r["entry_type"] == "production"
        assert r["machine_status_value"] == "running"

    def test_add_entry_custom_type(self, conn, env):
        r = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"],
            entry_type="setup",
            machine_status="setup",
        ))
        assert is_ok(r)
        assert r["entry_type"] == "setup"

    def test_add_entry_missing_company(self, conn, env):
        r = call_action(M.add_shop_floor_entry, conn, ns())
        assert is_error(r)

    def test_add_entry_invalid_type(self, conn, env):
        r = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"],
            entry_type="invalid",
        ))
        assert is_error(r)


class TestUpdateShopFloorEntry:
    def test_update_entry_ok(self, conn, env):
        add_r = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"],
        ))
        eid = add_r["entry_id"]

        r = call_action(M.update_shop_floor_entry, conn, ns(
            entry_id=eid, operator="John Doe",
        ))
        assert is_ok(r)

    def test_update_entry_not_found(self, conn, env):
        r = call_action(M.update_shop_floor_entry, conn, ns(
            entry_id=_uuid(),
        ))
        assert is_error(r)

    def test_update_entry_missing_id(self, conn, env):
        r = call_action(M.update_shop_floor_entry, conn, ns())
        assert is_error(r)


class TestGetShopFloorEntry:
    def test_get_entry_ok(self, conn, env):
        add_r = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"],
        ))
        eid = add_r["entry_id"]

        r = call_action(M.get_shop_floor_entry, conn, ns(entry_id=eid))
        assert is_ok(r)

    def test_get_entry_not_found(self, conn, env):
        r = call_action(M.get_shop_floor_entry, conn, ns(entry_id=_uuid()))
        assert is_error(r)


class TestListShopFloorEntries:
    def test_list_entries_empty(self, conn, env):
        r = call_action(M.list_shop_floor_entries, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)

    def test_list_entries_after_add(self, conn, env):
        call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"],
        ))
        r = call_action(M.list_shop_floor_entries, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)


# ===================================================================
# Tools
# ===================================================================

class TestAddTool:
    def test_add_tool_ok(self, conn, env):
        r = call_action(M.add_tool, conn, ns(
            company_id=env["company_id"],
            name="Drill Bit 10mm",
        ))
        assert is_ok(r)
        assert r["tool_id"]
        assert r["tool_status"] == "available"
        assert r["condition_value"] == "good"

    def test_add_tool_custom_type(self, conn, env):
        r = call_action(M.add_tool, conn, ns(
            company_id=env["company_id"],
            name="Caliper",
            tool_type="measuring",
        ))
        assert is_ok(r)

    def test_add_tool_missing_name(self, conn, env):
        r = call_action(M.add_tool, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_error(r)

    def test_add_tool_missing_company(self, conn, env):
        r = call_action(M.add_tool, conn, ns(name="Wrench"))
        assert is_error(r)


class TestUpdateTool:
    def test_update_tool_ok(self, conn, env):
        add_r = call_action(M.add_tool, conn, ns(
            company_id=env["company_id"], name="Hammer",
        ))
        tid = add_r["tool_id"]

        r = call_action(M.update_tool, conn, ns(
            tool_id=tid, location="Shelf B2",
        ))
        assert is_ok(r)

    def test_update_tool_not_found(self, conn, env):
        r = call_action(M.update_tool, conn, ns(tool_id=_uuid()))
        assert is_error(r)


class TestGetTool:
    def test_get_tool_ok(self, conn, env):
        add_r = call_action(M.add_tool, conn, ns(
            company_id=env["company_id"], name="Gauge",
        ))
        tid = add_r["tool_id"]

        r = call_action(M.get_tool, conn, ns(tool_id=tid))
        assert is_ok(r)

    def test_get_tool_not_found(self, conn, env):
        r = call_action(M.get_tool, conn, ns(tool_id=_uuid()))
        assert is_error(r)


class TestListTools:
    def test_list_tools_empty(self, conn, env):
        r = call_action(M.list_tools, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)


# ===================================================================
# ECOs
# ===================================================================

class TestAddEco:
    def test_add_eco_ok(self, conn, env):
        r = call_action(M.add_eco, conn, ns(
            company_id=env["company_id"],
            title="Redesign bracket",
        ))
        assert is_ok(r)
        assert r["eco_id"]
        assert r["eco_status"] == "draft"

    def test_add_eco_custom_type(self, conn, env):
        r = call_action(M.add_eco, conn, ns(
            company_id=env["company_id"],
            title="Material change",
            eco_type="material",
            priority="high",
        ))
        assert is_ok(r)

    def test_add_eco_missing_title(self, conn, env):
        r = call_action(M.add_eco, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_error(r)

    def test_add_eco_missing_company(self, conn, env):
        r = call_action(M.add_eco, conn, ns(title="Test ECO"))
        assert is_error(r)


class TestUpdateEco:
    def test_update_eco_ok(self, conn, env):
        add_r = call_action(M.add_eco, conn, ns(
            company_id=env["company_id"],
            title="ECO Update Test",
        ))
        eid = add_r["eco_id"]

        r = call_action(M.update_eco, conn, ns(
            eco_id=eid, description="Updated description",
        ))
        assert is_ok(r)

    def test_update_eco_not_found(self, conn, env):
        r = call_action(M.update_eco, conn, ns(eco_id=_uuid()))
        assert is_error(r)


class TestGetEco:
    def test_get_eco_ok(self, conn, env):
        add_r = call_action(M.add_eco, conn, ns(
            company_id=env["company_id"],
            title="Get Test ECO",
        ))
        eid = add_r["eco_id"]

        r = call_action(M.get_eco, conn, ns(eco_id=eid))
        assert is_ok(r)

    def test_get_eco_not_found(self, conn, env):
        r = call_action(M.get_eco, conn, ns(eco_id=_uuid()))
        assert is_error(r)


class TestListEcos:
    def test_list_ecos_empty(self, conn, env):
        r = call_action(M.list_ecos, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)


class TestEcoWorkflow:
    def test_submit_for_review(self, conn, env):
        add_r = call_action(M.add_eco, conn, ns(
            company_id=env["company_id"],
            title="Review ECO",
        ))
        eid = add_r["eco_id"]

        r = call_action(M.submit_eco_for_review, conn, ns(eco_id=eid))
        assert is_ok(r)
        assert r["eco_status"] == "review"

    def test_approve_eco(self, conn, env):
        add_r = call_action(M.add_eco, conn, ns(
            company_id=env["company_id"],
            title="Approve ECO",
        ))
        eid = add_r["eco_id"]
        call_action(M.submit_eco_for_review, conn, ns(eco_id=eid))

        r = call_action(M.approve_eco, conn, ns(
            eco_id=eid, approved_by="Manager",
        ))
        assert is_ok(r)
        assert r["eco_status"] == "approved"

    def test_reject_eco(self, conn, env):
        add_r = call_action(M.add_eco, conn, ns(
            company_id=env["company_id"],
            title="Reject ECO",
        ))
        eid = add_r["eco_id"]
        call_action(M.submit_eco_for_review, conn, ns(eco_id=eid))

        r = call_action(M.reject_eco, conn, ns(eco_id=eid))
        assert is_ok(r)
        assert r["eco_status"] == "rejected"


# ===================================================================
# Recipes
# ===================================================================

class TestAddRecipe:
    def test_add_recipe_ok(self, conn, env):
        r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="Widget Recipe",
            product_name="Widget X",
        ))
        assert is_ok(r)
        assert r["recipe_id"]
        assert r["recipe_status"] == "active"

    def test_add_recipe_missing_name(self, conn, env):
        r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            product_name="Product",
        ))
        assert is_error(r)

    def test_add_recipe_missing_product(self, conn, env):
        r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="Recipe",
        ))
        assert is_error(r)


class TestUpdateRecipe:
    def test_update_recipe_ok(self, conn, env):
        add_r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="Updatable Recipe",
            product_name="Product Y",
        ))
        rid = add_r["recipe_id"]

        r = call_action(M.update_recipe, conn, ns(
            recipe_id=rid, batch_size="100",
        ))
        assert is_ok(r)

    def test_update_recipe_not_found(self, conn, env):
        r = call_action(M.update_recipe, conn, ns(recipe_id=_uuid()))
        assert is_error(r)


class TestGetRecipe:
    def test_get_recipe_ok(self, conn, env):
        add_r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="Gettable Recipe",
            product_name="Product Z",
        ))
        rid = add_r["recipe_id"]

        r = call_action(M.get_recipe, conn, ns(recipe_id=rid))
        assert is_ok(r)

    def test_get_recipe_not_found(self, conn, env):
        r = call_action(M.get_recipe, conn, ns(recipe_id=_uuid()))
        assert is_error(r)


class TestListRecipes:
    def test_list_recipes_empty(self, conn, env):
        r = call_action(M.list_recipes, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)


class TestRecipeIngredients:
    def test_add_ingredient_ok(self, conn, env):
        add_r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="Ingredient Recipe",
            product_name="Product I",
        ))
        rid = add_r["recipe_id"]

        r = call_action(M.add_recipe_ingredient, conn, ns(
            recipe_id=rid,
            ingredient_name="Flour",
            quantity="500",
            unit="grams",
            company_id=env["company_id"],
        ))
        assert is_ok(r)

    def test_list_ingredients(self, conn, env):
        add_r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="List Ingredients Recipe",
            product_name="Product L",
        ))
        rid = add_r["recipe_id"]
        call_action(M.add_recipe_ingredient, conn, ns(
            recipe_id=rid,
            ingredient_name="Sugar",
            quantity="200",
            unit="grams",
            company_id=env["company_id"],
        ))

        r = call_action(M.list_recipe_ingredients, conn, ns(recipe_id=rid))
        assert is_ok(r)
        assert len(r["ingredients"]) >= 1


class TestCloneRecipe:
    def test_clone_recipe_ok(self, conn, env):
        add_r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="Original Recipe",
            product_name="Original Product",
        ))
        rid = add_r["recipe_id"]

        r = call_action(M.clone_recipe, conn, ns(
            recipe_id=rid,
            name="Cloned Recipe",
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["recipe_id"] != rid


class TestModuleStatus:
    def test_status_ok(self, conn, env):
        r = call_action(M.status, conn, ns())
        assert is_ok(r)
        assert r["actions_available"] == 35
