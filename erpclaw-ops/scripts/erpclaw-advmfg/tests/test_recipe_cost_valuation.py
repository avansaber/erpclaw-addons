"""Recipe costing prices item-linked ingredients from the real valuation rate.

calculate-recipe-cost uses the shared get_valuation_rate helper (moving
average over the item's stock ledger rows, falling back to the item's
standard rate when there is no stock). Items are created through the
inventory module's own add-item action; stock receipts go through the
inventory module's add-warehouse / add-stock-entry / submit-stock-entry
actions, with the Stock-in-Hand, Stock-Received-Not-Billed and fiscal-year
setup they need seeded through the GL module's own add-account and
add-fiscal-year actions.

Money is text: every money assertion compares exact strings, never floats.
"""
import argparse
import importlib.util
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from advmfg_helpers import (  # noqa: E402
    SRC_DIR,
    call_action,
    is_error,
    is_ok,
    load_db_query,
    ns,
)

from erpclaw_lib.query import Field, P, Q, Table, dynamic_update, fn  # noqa: E402

M = load_db_query()


def _load(domain):
    path = os.path.join(SRC_DIR, "erpclaw", "scripts", domain, "db_query.py")
    spec = importlib.util.spec_from_file_location(f"_fnd_{domain}_m661", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


INV = _load("erpclaw-inventory")
GL = _load("erpclaw-gl")

_SNAPSHOT_TABLES = (
    "process_recipe",
    "recipe_ingredient",
    "item",
    "stock_ledger_entry",
    "audit_log",
    "gl_entry",
)


def _snapshot(conn):
    snap = {}
    for table in _SNAPSHOT_TABLES:
        t = Table(table)
        rows = conn.execute(Q.from_(t).select(t.star).get_sql()).fetchall()
        snap[table] = sorted(
            tuple(sorted((key, str(value)) for key, value in dict(r).items()))
            for r in rows
        )
    return snap


def _inv_item_ns(**kwargs):
    base = {
        "item_code": None, "item_name": None, "item_type": None,
        "valuation_method": None, "item_group": None, "stock_uom": None,
        "has_batch": None, "has_serial": None, "standard_rate": None,
        "custom_fields": None,
    }
    base.update(kwargs)
    return argparse.Namespace(**base)


def _inv_wh_ns(**kwargs):
    base = {
        "name": None, "company_id": None, "warehouse_type": None,
        "parent_id": None, "account_id": None, "is_group": None,
    }
    base.update(kwargs)
    return argparse.Namespace(**base)


def _inv_se_ns(**kwargs):
    base = {
        "entry_type": None, "company_id": None, "posting_date": None,
        "items": None, "supplier_warehouse_id": None, "work_order_id": None,
    }
    base.update(kwargs)
    return argparse.Namespace(**base)


def _inv_submit_ns(**kwargs):
    base = {"stock_entry_id": None}
    base.update(kwargs)
    return argparse.Namespace(**base)


def _gl_account_ns(**kwargs):
    base = {
        "name": None, "company_id": None, "root_type": None,
        "account_type": None, "account_number": None, "parent_id": None,
        "currency": None, "is_group": None,
    }
    base.update(kwargs)
    return argparse.Namespace(**base)


def _gl_fy_ns(**kwargs):
    base = {
        "name": None, "start_date": None, "end_date": None,
        "company_id": None,
    }
    base.update(kwargs)
    return argparse.Namespace(**base)


def _add_item(conn, **kwargs):
    r = call_action(INV.add_item, conn, _inv_item_ns(**kwargs))
    assert is_ok(r), r
    return r["item_id"]


def _seed_gl_setup(conn, company_id):
    r = call_action(GL.add_account, conn, _gl_account_ns(
        name="Stock In Hand", company_id=company_id,
        root_type="asset", account_type="stock",
    ))
    assert is_ok(r), r
    stock_acct = r["account_id"]
    r = call_action(GL.add_account, conn, _gl_account_ns(
        name="Stock Received Not Billed", company_id=company_id,
        root_type="liability", account_type="stock_received_not_billed",
    ))
    assert is_ok(r), r
    r = call_action(GL.add_fiscal_year, conn, _gl_fy_ns(
        name="FY2026", start_date="2026-01-01", end_date="2026-12-31",
        company_id=company_id,
    ))
    assert is_ok(r), r
    return stock_acct


def _add_warehouse(conn, company_id, name, account_id=None):
    r = call_action(INV.add_warehouse, conn, _inv_wh_ns(
        name=name, company_id=company_id, account_id=account_id,
    ))
    assert is_ok(r), r
    return r["warehouse_id"]


def _receive(conn, company_id, item_id, warehouse_id, qty, rate,
             posting_date="2026-06-15"):
    items = json.dumps([{
        "item_id": item_id, "qty": qty, "rate": rate,
        "to_warehouse_id": warehouse_id,
    }])
    r = call_action(INV.add_stock_entry, conn, _inv_se_ns(
        entry_type="receive", company_id=company_id,
        posting_date=posting_date, items=items,
    ))
    assert is_ok(r), r
    r = call_action(INV.submit_stock_entry, conn, _inv_submit_ns(
        stock_entry_id=r["stock_entry_id"],
    ))
    assert is_ok(r), r


def _add_recipe(conn, company_id, name, product_name, batch_size):
    r = call_action(M.add_recipe, conn, ns(
        company_id=company_id, name=name,
        product_name=product_name, batch_size=batch_size,
    ))
    assert is_ok(r), r
    return r["recipe_id"]


def _add_ingredient(conn, recipe_id, company_id, name, quantity,
                    item_id=None, unit="grams", sequence="1"):
    r = call_action(M.add_recipe_ingredient, conn, ns(
        recipe_id=recipe_id, ingredient_name=name,
        item_id=item_id, quantity=quantity, unit=unit,
        sequence=sequence, company_id=company_id,
    ))
    return r


class TestRecipeCostStandardRateFallback:
    def test_standard_rate_prices_linked_and_zeroes_unlinked(self, conn, env):
        cid = env["company_id"]
        item_id = _add_item(
            conn, item_code="FLOUR-M343", item_name="Flour",
            standard_rate="12.50",
        )
        rid = _add_recipe(conn, cid, "Big Loaf", "Bread", "100")
        assert is_ok(_add_ingredient(
            conn, rid, cid, "Flour", "500", item_id=item_id,
        ))
        assert is_ok(_add_ingredient(
            conn, rid, cid, "Water", "300", sequence="2",
        ))
        before = _snapshot(conn)

        r = call_action(M.calculate_recipe_cost, conn, ns(recipe_id=rid))
        assert is_ok(r), r
        assert r["recipe_id"] == rid
        assert r["batch_size"] == "100"
        assert len(r["ingredients"]) == 2
        assert r["ingredients"][0]["ingredient_name"] == "Flour"
        assert r["ingredients"][0]["quantity"] == "500"
        assert r["ingredients"][0]["unit_cost"] == "12.50"
        assert r["ingredients"][0]["line_cost"] == "6250.00"
        assert r["ingredients"][1]["ingredient_name"] == "Water"
        assert r["ingredients"][1]["quantity"] == "300"
        assert r["ingredients"][1]["unit_cost"] == "0"
        assert r["ingredients"][1]["line_cost"] == "0.00"
        assert r["total_cost"] == "6250.00"
        assert r["cost_per_unit"] == "62.50"
        assert r["has_pricing_data"] is True
        assert _snapshot(conn) == before


class TestRecipeCostMovingAverage:
    def test_moving_average_wins_over_standard_rate(self, conn, env):
        cid = env["company_id"]
        stock_acct = _seed_gl_setup(conn, cid)
        item_id = _add_item(
            conn, item_code="SUGAR-M343", item_name="Sugar",
            standard_rate="9.00",
        )
        wh = _add_warehouse(conn, cid, "Main Store", account_id=stock_acct)
        _receive(conn, cid, item_id, wh, "10", "4.00")
        _receive(conn, cid, item_id, wh, "30", "6.00")
        rid = _add_recipe(conn, cid, "Sweet Batch", "Candy", "1")
        assert is_ok(_add_ingredient(
            conn, rid, cid, "Sugar", "3", item_id=item_id,
        ))
        before = _snapshot(conn)

        r = call_action(M.calculate_recipe_cost, conn, ns(recipe_id=rid))
        assert is_ok(r), r
        assert r["recipe_id"] == rid
        assert len(r["ingredients"]) == 1
        assert r["ingredients"][0]["ingredient_name"] == "Sugar"
        assert r["ingredients"][0]["quantity"] == "3"
        assert r["ingredients"][0]["unit_cost"] == "5.50"
        assert r["ingredients"][0]["line_cost"] == "16.50"
        assert r["total_cost"] == "16.50"
        assert r["cost_per_unit"] == "16.50"
        assert r["has_pricing_data"] is True
        assert _snapshot(conn) == before


class TestRecipeCostQuantityRefusals:
    def test_add_ingredient_refuses_non_numeric_quantity(self, conn, env):
        cid = env["company_id"]
        rid = _add_recipe(conn, cid, "Test Cake", "Cake", "10")
        before = _snapshot(conn)

        r = _add_ingredient(conn, rid, cid, "Flour", "abc")
        assert is_error(r)
        assert r["message"] == "--quantity must be a non-negative number"
        assert _snapshot(conn) == before

    def test_add_ingredient_refuses_negative_quantity(self, conn, env):
        cid = env["company_id"]
        rid = _add_recipe(conn, cid, "Test Cake", "Cake", "10")
        before = _snapshot(conn)

        r = _add_ingredient(conn, rid, cid, "Flour", "-1")
        assert is_error(r)
        assert r["message"] == "--quantity must be a non-negative number"
        assert _snapshot(conn) == before

    def test_update_ingredient_refuses_non_numeric_quantity(self, conn, env):
        cid = env["company_id"]
        rid = _add_recipe(conn, cid, "Test Cake", "Cake", "10")
        ing_id = _add_ingredient(conn, rid, cid, "Flour", "500")[
            "ingredient_id"]
        before = _snapshot(conn)

        r = call_action(M.update_recipe_ingredient, conn, ns(
            ingredient_id=ing_id, quantity="abc",
        ))
        assert is_error(r)
        assert r["message"] == "--quantity must be a non-negative number"
        assert _snapshot(conn) == before

    def test_cost_refuses_stored_invalid_quantity(self, conn, env):
        cid = env["company_id"]
        rid = _add_recipe(conn, cid, "Test Cake", "Cake", "10")
        ing_id = _add_ingredient(conn, rid, cid, "Flour", "500")[
            "ingredient_id"]
        sql, params = dynamic_update(
            "recipe_ingredient", {"quantity": "abc"}, {"id": ing_id})
        conn.execute(sql, params)
        conn.commit()
        before = _snapshot(conn)

        r = call_action(M.calculate_recipe_cost, conn, ns(recipe_id=rid))
        assert is_error(r)
        assert r["message"] == (
            "Ingredient Flour has an invalid quantity: abc")
        assert _snapshot(conn) == before
