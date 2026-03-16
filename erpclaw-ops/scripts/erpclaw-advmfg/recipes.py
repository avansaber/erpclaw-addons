"""ERPClaw Advanced Manufacturing -- Recipes domain module.

Process recipes with versioned ingredient lists, cloning, cost calculation.
11 actions exported via ACTIONS dict.
"""
import os
import sys
import uuid
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit
from erpclaw_lib.db import DEFAULT_DB_PATH
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, LiteralValue, insert_row, update_row, dynamic_update

SKILL = "erpclaw-advmfg"

VALID_RECIPE_TYPES = ("standard", "alternative", "trial", "obsolete")


# ---------------------------------------------------------------------------
# add-recipe
# ---------------------------------------------------------------------------
def add_recipe(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "name", None):
        err("--name is required")
    if not getattr(args, "product_name", None):
        err("--product-name is required")

    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    recipe_type = getattr(args, "recipe_type", None) or "standard"
    if recipe_type not in VALID_RECIPE_TYPES:
        err(f"Invalid recipe-type: {recipe_type}")

    recipe_id = str(uuid.uuid4())
    ns = get_next_name(conn, "process_recipe", company_id=args.company_id)

    sql, _ = insert_row("process_recipe", {
        "id": P(), "naming_series": P(), "name": P(), "product_name": P(),
        "recipe_type": P(), "version": P(), "batch_size": P(), "batch_unit": P(),
        "expected_yield": P(), "description": P(), "instructions": P(),
        "is_active": P(), "company_id": P(),
    })
    conn.execute(sql,
        (
            recipe_id, ns, args.name, args.product_name, recipe_type,
            getattr(args, "version", None) or "1.0",
            getattr(args, "batch_size", None) or "1",
            getattr(args, "batch_unit", None) or "unit",
            getattr(args, "expected_yield", None) or "100",
            getattr(args, "description", None),
            getattr(args, "instructions", None),
            1,
            args.company_id,
        ),
    )
    audit(conn, SKILL, "add-recipe", "process_recipe", recipe_id,
          new_values={"name": args.name, "naming_series": ns})
    conn.commit()
    ok({"recipe_id": recipe_id, "naming_series": ns, "recipe_status": "active"})


# ---------------------------------------------------------------------------
# update-recipe
# ---------------------------------------------------------------------------
def update_recipe(conn, args):
    recipe_id = getattr(args, "recipe_id", None)
    if not recipe_id:
        err("--recipe-id is required")
    row = conn.execute(Q.from_(Table("process_recipe")).select(Table("process_recipe").star).where(Field("id") == P()).get_sql(), (recipe_id,)).fetchone()
    if not row:
        err(f"Recipe {recipe_id} not found")

    data, changed = {}, []

    for field, attr in [
        ("name", "name"),
        ("product_name", "product_name"),
        ("version", "version"),
        ("batch_size", "batch_size"),
        ("batch_unit", "batch_unit"),
        ("expected_yield", "expected_yield"),
        ("description", "description"),
        ("instructions", "instructions"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            data[field] = val
            changed.append(field)

    rt = getattr(args, "recipe_type", None)
    if rt is not None:
        if rt not in VALID_RECIPE_TYPES:
            err(f"Invalid recipe-type: {rt}")
        data["recipe_type"] = rt
        changed.append("recipe_type")

    ia = getattr(args, "is_active", None)
    if ia is not None:
        data["is_active"] = int(ia)
        changed.append("is_active")

    if not changed:
        err("No fields to update")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("process_recipe", data, {"id": recipe_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "update-recipe", "process_recipe", recipe_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"recipe_id": recipe_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# get-recipe
# ---------------------------------------------------------------------------
def get_recipe(conn, args):
    recipe_id = getattr(args, "recipe_id", None)
    if not recipe_id:
        err("--recipe-id is required")
    row = conn.execute(Q.from_(Table("process_recipe")).select(Table("process_recipe").star).where(Field("id") == P()).get_sql(), (recipe_id,)).fetchone()
    if not row:
        err(f"Recipe {recipe_id} not found")

    data = row_to_dict(row)
    data["recipe_status"] = "active" if data.get("is_active") else "inactive"

    # Get ingredients
    ingredients = conn.execute(Q.from_(Table("recipe_ingredient")).select(Table("recipe_ingredient").star).where(Field("recipe_id") == P()).orderby(Field("sequence")).orderby(Field("created_at")).get_sql(), (recipe_id,)).fetchall()
    data["ingredients"] = [row_to_dict(i) for i in ingredients]
    data["ingredient_count"] = len(ingredients)

    ok(data)


# ---------------------------------------------------------------------------
# list-recipes
# ---------------------------------------------------------------------------
def list_recipes(conn, args):
    t = Table("process_recipe")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star).as_("cnt"))
    params = []

    company_id = getattr(args, "company_id", None)
    if company_id:
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(company_id)
    recipe_type = getattr(args, "recipe_type", None)
    if recipe_type:
        q = q.where(t.recipe_type == P())
        q_cnt = q_cnt.where(t.recipe_type == P())
        params.append(recipe_type)
    product_name = getattr(args, "product_name", None)
    if product_name:
        like = LiteralValue("?")
        q = q.where(t.product_name.like(like))
        q_cnt = q_cnt.where(t.product_name.like(like))
        params.append(f"%{product_name}%")
    search = getattr(args, "search", None)
    if search:
        like = LiteralValue("?")
        crit = (t.name.like(like)) | (t.product_name.like(like)) | (t.description.like(like))
        q = q.where(crit)
        q_cnt = q_cnt.where(crit)
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    total = conn.execute(q_cnt.get_sql(), params).fetchone()["cnt"]

    q = q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()

    recipes = []
    for r in rows:
        d = row_to_dict(r)
        d["recipe_status"] = "active" if d.get("is_active") else "inactive"
        recipes.append(d)

    ok({"recipes": recipes, "total_count": total, "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# add-recipe-ingredient
# ---------------------------------------------------------------------------
def add_recipe_ingredient(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "recipe_id", None):
        err("--recipe-id is required")
    if not getattr(args, "ingredient_name", None):
        err("--ingredient-name is required")

    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    if not conn.execute(Q.from_(Table("process_recipe")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.recipe_id,)).fetchone():
        err(f"Recipe {args.recipe_id} not found")

    ingredient_id = str(uuid.uuid4())

    sql, _ = insert_row("recipe_ingredient", {"id": P(), "recipe_id": P(), "ingredient_name": P(), "item_id": P(), "quantity": P(), "unit": P(), "sequence": P(), "is_optional": P(), "notes": P(), "company_id": P()})
    conn.execute(sql,
        (
            ingredient_id, args.recipe_id, args.ingredient_name,
            getattr(args, "item_id", None),
            getattr(args, "quantity", None) or "0",
            getattr(args, "unit", None) or "unit",
            int(getattr(args, "sequence", None) or 0),
            int(getattr(args, "is_optional", None) or 0),
            getattr(args, "notes", None),
            args.company_id,
        ),
    )
    audit(conn, SKILL, "add-recipe-ingredient", "recipe_ingredient", ingredient_id,
          new_values={"recipe_id": args.recipe_id, "ingredient": args.ingredient_name})
    conn.commit()
    ok({"ingredient_id": ingredient_id, "recipe_id": args.recipe_id,
        "ingredient_name": args.ingredient_name})


# ---------------------------------------------------------------------------
# update-recipe-ingredient
# ---------------------------------------------------------------------------
def update_recipe_ingredient(conn, args):
    ingredient_id = getattr(args, "ingredient_id", None)
    if not ingredient_id:
        err("--ingredient-id is required")
    row = conn.execute(Q.from_(Table("recipe_ingredient")).select(Table("recipe_ingredient").star).where(Field("id") == P()).get_sql(), (ingredient_id,)).fetchone()
    if not row:
        err(f"Ingredient {ingredient_id} not found")

    data, changed = {}, []

    for field, attr in [
        ("ingredient_name", "ingredient_name"),
        ("item_id", "item_id"),
        ("quantity", "quantity"),
        ("unit", "unit"),
        ("notes", "notes"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            data[field] = val
            changed.append(field)

    seq = getattr(args, "sequence", None)
    if seq is not None:
        data["sequence"] = int(seq)
        changed.append("sequence")

    opt = getattr(args, "is_optional", None)
    if opt is not None:
        data["is_optional"] = int(opt)
        changed.append("is_optional")

    if not changed:
        err("No fields to update")

    sql, params = dynamic_update("recipe_ingredient", data, {"id": ingredient_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "update-recipe-ingredient", "recipe_ingredient", ingredient_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"ingredient_id": ingredient_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# list-recipe-ingredients
# ---------------------------------------------------------------------------
def list_recipe_ingredients(conn, args):
    recipe_id = getattr(args, "recipe_id", None)
    if not recipe_id:
        err("--recipe-id is required")

    if not conn.execute(Q.from_(Table("process_recipe")).select(Field('id')).where(Field("id") == P()).get_sql(), (recipe_id,)).fetchone():
        err(f"Recipe {recipe_id} not found")

    rows = conn.execute(Q.from_(Table("recipe_ingredient")).select(Table("recipe_ingredient").star).where(Field("recipe_id") == P()).orderby(Field("sequence")).orderby(Field("created_at")).get_sql(), (recipe_id,)).fetchall()

    ingredients = [row_to_dict(r) for r in rows]
    ok({"ingredients": ingredients, "total_count": len(ingredients), "recipe_id": recipe_id})


# ---------------------------------------------------------------------------
# remove-recipe-ingredient
# ---------------------------------------------------------------------------
def remove_recipe_ingredient(conn, args):
    ingredient_id = getattr(args, "ingredient_id", None)
    if not ingredient_id:
        err("--ingredient-id is required")

    row = conn.execute(Q.from_(Table("recipe_ingredient")).select(Table("recipe_ingredient").star).where(Field("id") == P()).get_sql(), (ingredient_id,)).fetchone()
    if not row:
        err(f"Ingredient {ingredient_id} not found")

    recipe_id = row["recipe_id"]
    conn.execute(Q.from_(Table("recipe_ingredient")).delete().where(Field("id") == P()).get_sql(), (ingredient_id,))
    audit(conn, SKILL, "remove-recipe-ingredient", "recipe_ingredient", ingredient_id,
          new_values={"removed": True, "recipe_id": recipe_id})
    conn.commit()
    ok({"ingredient_id": ingredient_id, "recipe_id": recipe_id, "removed": True})


# ---------------------------------------------------------------------------
# clone-recipe
# ---------------------------------------------------------------------------
def clone_recipe(conn, args):
    recipe_id = getattr(args, "recipe_id", None)
    if not recipe_id:
        err("--recipe-id is required")

    row = conn.execute(Q.from_(Table("process_recipe")).select(Table("process_recipe").star).where(Field("id") == P()).get_sql(), (recipe_id,)).fetchone()
    if not row:
        err(f"Recipe {recipe_id} not found")

    source = row_to_dict(row)
    new_id = str(uuid.uuid4())
    ns = get_next_name(conn, "process_recipe", company_id=source["company_id"])
    new_version = getattr(args, "version", None) or str(
        Decimal(source.get("version") or "1.0") + Decimal("0.1")
    )

    clone_sql, _ = insert_row("process_recipe", {
        "id": P(), "naming_series": P(), "name": P(), "product_name": P(),
        "recipe_type": P(), "version": P(), "batch_size": P(), "batch_unit": P(),
        "expected_yield": P(), "description": P(), "instructions": P(),
        "is_active": P(), "company_id": P(),
    })
    conn.execute(clone_sql,
        (
            new_id, ns,
            source["name"] + " (Clone)",
            source["product_name"],
            "trial",
            new_version,
            source.get("batch_size") or "1",
            source.get("batch_unit") or "unit",
            source.get("expected_yield") or "100",
            source.get("description"),
            source.get("instructions"),
            1,
            source["company_id"],
        ),
    )

    # Clone ingredients
    ingredients = conn.execute(Q.from_(Table("recipe_ingredient")).select(Table("recipe_ingredient").star).where(Field("recipe_id") == P()).get_sql(), (recipe_id,)).fetchall()
    cloned_count = 0
    for ing in ingredients:
        ing_data = row_to_dict(ing)
        new_ing_id = str(uuid.uuid4())
        sql, _ = insert_row("recipe_ingredient", {"id": P(), "recipe_id": P(), "ingredient_name": P(), "item_id": P(), "quantity": P(), "unit": P(), "sequence": P(), "is_optional": P(), "notes": P(), "company_id": P()})
        conn.execute(sql,
            (
                new_ing_id, new_id,
                ing_data["ingredient_name"],
                ing_data.get("item_id"),
                ing_data.get("quantity") or "0",
                ing_data.get("unit") or "unit",
                ing_data.get("sequence") or 0,
                ing_data.get("is_optional") or 0,
                ing_data.get("notes"),
                ing_data["company_id"],
            ),
        )
        cloned_count += 1

    audit(conn, SKILL, "clone-recipe", "process_recipe", new_id,
          new_values={"source_recipe_id": recipe_id, "version": new_version})
    conn.commit()
    ok({
        "recipe_id": new_id,
        "naming_series": ns,
        "source_recipe_id": recipe_id,
        "version": new_version,
        "recipe_type": "trial",
        "ingredients_cloned": cloned_count,
    })


# ---------------------------------------------------------------------------
# calculate-recipe-cost
# ---------------------------------------------------------------------------
def calculate_recipe_cost(conn, args):
    recipe_id = getattr(args, "recipe_id", None)
    if not recipe_id:
        err("--recipe-id is required")

    recipe = conn.execute(Q.from_(Table("process_recipe")).select(Table("process_recipe").star).where(Field("id") == P()).get_sql(), (recipe_id,)).fetchone()
    if not recipe:
        err(f"Recipe {recipe_id} not found")

    ingredients = conn.execute(Q.from_(Table("recipe_ingredient")).select(Table("recipe_ingredient").star).where(Field("recipe_id") == P()).orderby(Field("sequence")).get_sql(), (recipe_id,)).fetchall()

    total_cost = Decimal("0")
    ingredient_costs = []
    has_pricing = False

    for ing in ingredients:
        ing_data = row_to_dict(ing)
        qty = Decimal(ing_data.get("quantity") or "0")

        # Try to find item cost if item_id is provided
        unit_cost = Decimal("0")
        if ing_data.get("item_id"):
            # Try to look up item valuation rate from item table
            item_t = Table("item")
            item_row = conn.execute(
                Q.from_(item_t).select(item_t.valuation_rate).where(item_t.id == P()).get_sql(),
                (ing_data["item_id"],),
            ).fetchone()
            if item_row and item_row["valuation_rate"]:
                unit_cost = Decimal(str(item_row["valuation_rate"]))
                has_pricing = True

        line_cost = (qty * unit_cost).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        total_cost += line_cost

        ingredient_costs.append({
            "ingredient_name": ing_data["ingredient_name"],
            "quantity": str(qty),
            "unit": ing_data.get("unit") or "unit",
            "unit_cost": str(unit_cost),
            "line_cost": str(line_cost),
        })

    batch_size = Decimal(row_to_dict(recipe).get("batch_size") or "1")
    cost_per_unit = Decimal("0")
    if batch_size > 0:
        cost_per_unit = (total_cost / batch_size).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

    ok({
        "recipe_id": recipe_id,
        "recipe_name": recipe["name"],
        "batch_size": str(batch_size),
        "ingredients": ingredient_costs,
        "total_cost": str(total_cost),
        "cost_per_unit": str(cost_per_unit),
        "has_pricing_data": has_pricing,
    })


# ---------------------------------------------------------------------------
# status (module status)
# ---------------------------------------------------------------------------
def module_status(conn, args):
    ok({
        "skill": SKILL,
        "version": "1.0.0",
        "actions_available": 35,
        "domains": ["shop_floor", "tools", "eco", "recipes"],
        "tables": [
            "shop_floor_entry", "tool", "tool_usage",
            "engineering_change_order", "process_recipe", "recipe_ingredient",
        ],
        "database": DEFAULT_DB_PATH,
    })


# ---------------------------------------------------------------------------
# ACTIONS registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "add-recipe": add_recipe,
    "update-recipe": update_recipe,
    "get-recipe": get_recipe,
    "list-recipes": list_recipes,
    "add-recipe-ingredient": add_recipe_ingredient,
    "update-recipe-ingredient": update_recipe_ingredient,
    "list-recipe-ingredients": list_recipe_ingredients,
    "remove-recipe-ingredient": remove_recipe_ingredient,
    "clone-recipe": clone_recipe,
    "calculate-recipe-cost": calculate_recipe_cost,
    "status": module_status,
}
