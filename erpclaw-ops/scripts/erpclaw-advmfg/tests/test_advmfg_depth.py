"""Behavioural depth tests for twelve erpclaw-advmfg actions.

Each action below previously had only shape (response keys) or routability
coverage: the old tests never read the database back, so a perfectly shaped
response with no write behind it still passed. Every happy-path test here
reads the stored rows back through PyPika-built queries and pins exact
values; every refusal test proves the error message is truthful and the
database is byte-identical afterwards (snapshot over all advmfg, naming,
audit and ledger tables).

Ledger note per action (so a later reader does not add assertions that
cannot hold): none of these twelve actions reaches the ledger. The three
write actions (add-tool-usage, complete-shop-floor-entry, implement-eco)
update their own domain rows only; the other nine are read-only and write
nothing at all, not even an audit row. The zero-count checks below pin that;
no debit/credit assertions apply.

Money is text: purchase_cost and recipe-cost assertions compare exact
Decimal strings. Never float, never approximate, never round.

One documented defect (now fixed): calculate-recipe-cost used to select
item.valuation_rate, which does not exist, so any ingredient linked to an
item crashed with IndexError. It now prices from the shared
get_valuation_rate helper and refuses a linked item with no rate. See
TestCalculateRecipeCostDepth.test_linked_item_without_rate_refuses_without_writing.
"""
import os
import sys
from datetime import datetime
from decimal import Decimal

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from advmfg_helpers import (  # noqa: E402
    _uuid,
    call_action,
    is_error,
    is_ok,
    load_db_query,
    ns,
    seed_company,
    seed_naming_series,
)

from erpclaw_lib.query import Field, P, Q, Table, fn, insert_row  # noqa: E402

M = load_db_query()

_SNAPSHOT_TABLES = (
    "shop_floor_entry",
    "tool",
    "tool_usage",
    "engineering_change_order",
    "process_recipe",
    "recipe_ingredient",
    "naming_series",
    "audit_log",
    "gl_entry",
    "stock_ledger_entry",
)


def _snapshot(conn):
    """Full content of every advmfg/naming/audit/ledger table, normalised."""
    snap = {}
    for table in _SNAPSHOT_TABLES:
        t = Table(table)
        rows = conn.execute(Q.from_(t).select(t.star).get_sql()).fetchall()
        snap[table] = sorted(
            tuple(sorted((key, str(value)) for key, value in dict(r).items()))
            for r in rows
        )
    return snap


def _row(conn, table, row_id):
    t = Table(table)
    row = conn.execute(
        Q.from_(t).select(t.star).where(Field("id") == P()).get_sql(),
        (row_id,),
    ).fetchone()
    assert row is not None, f"expected a row in {table} with id {row_id}"
    return dict(row)


def _rows_where(conn, table, field, value):
    t = Table(table)
    return [
        dict(r)
        for r in conn.execute(
            Q.from_(t).select(t.star).where(Field(field) == P()).get_sql(),
            (value,),
        ).fetchall()
    ]


def _count(conn, table):
    t = Table(table)
    return conn.execute(
        Q.from_(t).select(fn.Count(t.star).as_("cnt")).get_sql()
    ).fetchone()["cnt"]


def _second_company(conn):
    cid = seed_company(conn, name="AdvMfg Other Co", abbr="OTH")
    seed_naming_series(conn, cid)
    return cid


# ===================================================================
# add-tool-usage (stored row; no ledger legs by design)
# ===================================================================

class TestAddToolUsageDepth:
    def test_usage_inserts_row_and_advances_tool_counters(self, conn, env):
        cid = env["company_id"]
        add_r = call_action(M.add_tool, conn, ns(
            company_id=cid, name="Torque Wrench",
            tool_type="measuring", purchase_cost="250.00",
            max_usage_count="10", calibration_due="2099-06-01",
        ))
        assert is_ok(add_r), add_r
        tid = add_r["tool_id"]
        tool_before = _row(conn, "tool", tid)
        assert tool_before["current_usage_count"] == 0
        assert tool_before["condition"] == "good"
        decoy = call_action(M.add_tool, conn, ns(
            company_id=cid, name="Untouched Hammer",
        ))["tool_id"]
        decoy_before = _row(conn, "tool", decoy)

        r = call_action(M.add_tool_usage, conn, ns(
            company_id=cid, tool_id=tid, usage_count="3",
            usage_duration_minutes="45", operator="Ravi",
            condition_after="worn",
        ))
        assert is_ok(r), r
        assert r["tool_id"] == tid
        assert r["new_usage_count"] == 3
        assert r["condition_after"] == "worn"

        usage = _row(conn, "tool_usage", r["usage_id"])
        assert usage["tool_id"] == tid
        assert usage["usage_count"] == 3
        assert usage["usage_duration_minutes"] == 45
        assert usage["condition_after"] == "worn"
        assert usage["operator"] == "Ravi"
        assert usage["company_id"] == cid

        tool_after = _row(conn, "tool", tid)
        assert tool_after["current_usage_count"] == 3  # from 0
        assert tool_after["condition"] == "worn"  # from "good"
        assert tool_after["status"] == "available"  # unchanged
        # Money is text: exact Decimal string, never float.
        assert tool_after["purchase_cost"] == "250.00"
        assert Decimal(tool_after["purchase_cost"]) == Decimal("250.00")
        # A tool that was not named is byte-identical.
        assert _row(conn, "tool", decoy) == decoy_before
        # Usage tracking posts no ledger legs.
        assert _count(conn, "gl_entry") == 0
        assert _count(conn, "stock_ledger_entry") == 0

    def test_usage_on_scrapped_tool_refuses_without_writing(self, conn, env):
        cid = env["company_id"]
        tid = call_action(M.add_tool, conn, ns(
            company_id=cid, name="Doomed Drill",
        ))["tool_id"]
        call_action(M.add_tool_usage, conn, ns(
            company_id=cid, tool_id=tid, usage_count="3",
        ))
        call_action(M.add_tool_usage, conn, ns(
            company_id=cid, tool_id=tid, usage_count="1",
            condition_after="scrapped",
        ))
        assert _row(conn, "tool", tid)["status"] == "scrapped"
        before = _snapshot(conn)

        r = call_action(M.add_tool_usage, conn, ns(
            company_id=cid, tool_id=tid, usage_count="1",
        ))
        assert is_error(r)
        assert r["message"] == "Cannot log usage for a scrapped tool"
        assert _snapshot(conn) == before
        assert _row(conn, "tool", tid)["current_usage_count"] == 4


# ===================================================================
# calculate-recipe-cost (computed response only; writes nothing)
# ===================================================================

class TestCalculateRecipeCostDepth:
    def test_unpriced_recipe_costs_zero_and_writes_nothing(self, conn, env):
        cid = env["company_id"]
        rid = call_action(M.add_recipe, conn, ns(
            company_id=cid, name="Plain Cake",
            product_name="Cake", batch_size="100",
        ))["recipe_id"]
        call_action(M.add_recipe_ingredient, conn, ns(
            recipe_id=rid, ingredient_name="Flour",
            quantity="500", unit="grams", sequence="1",
            company_id=cid,
        ))
        call_action(M.add_recipe_ingredient, conn, ns(
            recipe_id=rid, ingredient_name="Sugar",
            quantity="200", unit="grams", sequence="2",
            company_id=cid,
        ))
        before = _snapshot(conn)

        r = call_action(M.calculate_recipe_cost, conn, ns(recipe_id=rid))
        assert is_ok(r), r
        assert r["recipe_id"] == rid
        assert r["recipe_name"] == "Plain Cake"
        assert r["batch_size"] == "100"
        assert r["has_pricing_data"] is False
        assert len(r["ingredients"]) == 2
        assert r["ingredients"][0]["ingredient_name"] == "Flour"
        assert r["ingredients"][0]["quantity"] == "500"
        assert r["ingredients"][0]["unit"] == "grams"
        assert r["ingredients"][0]["unit_cost"] == "0"
        assert r["ingredients"][0]["line_cost"] == "0.00"
        assert r["ingredients"][1]["ingredient_name"] == "Sugar"
        assert r["ingredients"][1]["line_cost"] == "0.00"
        # Money is text: exact Decimal strings, never float.
        assert r["total_cost"] == "0.00"
        assert r["cost_per_unit"] == "0.00"
        assert Decimal(r["total_cost"]) == Decimal("0.00")
        assert Decimal(r["cost_per_unit"]) == Decimal("0.00")
        # Pure computation: the call itself writes nothing, not even audit.
        assert _snapshot(conn) == before

    def test_linked_item_without_rate_refuses_without_writing(self, conn, env):
        """FIXED DEFECT: the pricing lookup used to select
        item.valuation_rate, which does not exist in this tree, so any
        ingredient linked to an item crashed the action with IndexError
        instead of returning a cost. The action now prices from the shared
        get_valuation_rate helper; an item with no stock and a zero
        standard rate is refused with an exact message and nothing is
        written, not even an audit row."""
        cid = env["company_id"]
        item_id = _uuid()
        sql, _ = insert_row("item", {"id": P(), "item_code": P(),
                                     "item_name": P()})
        conn.execute(sql, (item_id, "IT-" + item_id[:6], "Steel Rod"))
        conn.commit()
        rid = call_action(M.add_recipe, conn, ns(
            company_id=cid, name="Costed Widget",
            product_name="Widget", batch_size="100",
        ))["recipe_id"]
        call_action(M.add_recipe_ingredient, conn, ns(
            recipe_id=rid, ingredient_name="Steel",
            item_id=item_id, quantity="500", unit="grams",
            company_id=cid,
        ))
        before = _snapshot(conn)

        r = call_action(M.calculate_recipe_cost, conn, ns(recipe_id=rid))
        assert is_error(r)
        assert r["message"] == (
            f"Ingredient Steel is linked to item {item_id}, "
            "which has no valuation rate or standard rate")
        assert _snapshot(conn) == before

    def test_cost_missing_recipe_id_refuses_without_writing(self, conn, env):
        before = _snapshot(conn)

        r = call_action(M.calculate_recipe_cost, conn, ns())
        assert is_error(r)
        assert r["message"] == "--recipe-id is required"
        assert _snapshot(conn) == before


# ===================================================================
# calibration-due-report (computed response only; writes nothing)
# ===================================================================

class TestCalibrationDueReportDepth:
    def test_report_lists_due_tools_and_skips_scrapped_and_undated(
        self, conn, env,
    ):
        cid = env["company_id"]
        overdue = call_action(M.add_tool, conn, ns(
            company_id=cid, name="Old Caliper",
            tool_type="measuring", calibration_due="2020-01-01",
        ))["tool_id"]
        upcoming = call_action(M.add_tool, conn, ns(
            company_id=cid, name="New Gauge",
            tool_type="measuring", calibration_due="2099-01-01",
        ))["tool_id"]
        undated = call_action(M.add_tool, conn, ns(
            company_id=cid, name="No Due Hammer",
        ))["tool_id"]
        scrapped = call_action(M.add_tool, conn, ns(
            company_id=cid, name="Dead Mic",
            tool_type="measuring", calibration_due="2020-06-01",
        ))["tool_id"]
        assert is_ok(call_action(M.update_tool, conn, ns(
            tool_id=scrapped, tool_status="scrapped",
        )))
        before = _snapshot(conn)

        r = call_action(M.calibration_due_report, conn, ns(company_id=cid))
        assert is_ok(r), r
        assert r["total_count"] == 2
        assert r["overdue_count"] == 1
        by_id = {t["id"]: t for t in r["tools"]}
        assert set(by_id) == {overdue, upcoming}
        assert undated not in by_id  # no due date: excluded
        assert scrapped not in by_id  # scrapped: excluded
        assert by_id[overdue]["is_overdue"] is True
        assert by_id[overdue]["calibration_due"] == "2020-01-01"
        assert by_id[upcoming]["is_overdue"] is False
        assert by_id[upcoming]["calibration_due"] == "2099-01-01"
        # Read-only report: writes nothing, not even an audit row.
        assert _snapshot(conn) == before

    def test_report_missing_company_refuses_without_writing(self, conn, env):
        before = _snapshot(conn)

        r = call_action(M.calibration_due_report, conn, ns())
        assert is_error(r)
        assert r["message"] == "--company-id is required"
        assert _snapshot(conn) == before


# ===================================================================
# complete-shop-floor-entry (stored row change; no ledger legs by design)
# ===================================================================

class TestCompleteShopFloorEntryDepth:
    def test_complete_closes_entry_with_quantities(self, conn, env):
        cid = env["company_id"]
        eid = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=cid, equipment_id="Line-1",
            operator="Asha", entry_type="production",
            machine_status="running",
        ))["entry_id"]
        sibling = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=cid, equipment_id="Line-2",
            entry_type="downtime", machine_status="breakdown",
        ))["entry_id"]
        before = _row(conn, "shop_floor_entry", eid)
        assert before["end_time"] is None
        assert before["machine_status"] == "running"
        sibling_before = _row(conn, "shop_floor_entry", sibling)

        r = call_action(M.complete_shop_floor_entry, conn, ns(
            entry_id=eid, quantity_produced="50",
            quantity_rejected="2", notes="shift done",
        ))
        assert is_ok(r), r
        assert r["entry_id"] == eid
        assert r["machine_status_value"] == "idle"
        assert r["end_time"]
        expected = int((
            datetime.strptime(r["end_time"], "%Y-%m-%d %H:%M:%S")
            - datetime.strptime(before["start_time"], "%Y-%m-%d %H:%M:%S")
        ).total_seconds() / 60)
        assert r["duration_minutes"] == expected

        after = _row(conn, "shop_floor_entry", eid)
        assert after["machine_status"] == "idle"  # from "running"
        assert after["end_time"] == r["end_time"]  # from None
        assert after["duration_minutes"] == expected
        assert after["quantity_produced"] == 50  # from 0
        assert after["quantity_rejected"] == 2  # from 0
        assert after["notes"] == "shift done"
        assert after["operator"] == "Asha"  # untouched
        assert after["equipment_id"] == "Line-1"  # untouched
        # The sibling entry is byte-identical.
        assert _row(conn, "shop_floor_entry", sibling) == sibling_before
        # Closing an entry posts no ledger legs.
        assert _count(conn, "gl_entry") == 0
        assert _count(conn, "stock_ledger_entry") == 0

    def test_double_complete_refuses_without_writing(self, conn, env):
        cid = env["company_id"]
        eid = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=cid, equipment_id="Line-9",
        ))["entry_id"]
        assert is_ok(call_action(M.complete_shop_floor_entry, conn, ns(
            entry_id=eid, quantity_produced="5",
        )))
        before = _snapshot(conn)

        r = call_action(M.complete_shop_floor_entry, conn, ns(entry_id=eid))
        assert is_error(r)
        assert r["message"] == (
            f"Shop floor entry {eid} is already completed"
        )
        assert _snapshot(conn) == before
        assert _row(conn, "shop_floor_entry", eid)["quantity_produced"] == 5


# ===================================================================
# get-eco (read-only; writes nothing)
# ===================================================================

class TestGetEcoDepth:
    def test_get_eco_returns_exact_stored_values(self, conn, env):
        cid = env["company_id"]
        eid = call_action(M.add_eco, conn, ns(
            company_id=cid, title="Housing v2",
            eco_type="design", priority="high",
            description="thicker wall", requested_by="Deva",
        ))["eco_id"]
        stored = _row(conn, "engineering_change_order", eid)
        before = _snapshot(conn)

        r = call_action(M.get_eco, conn, ns(eco_id=eid))
        assert is_ok(r), r
        assert r["id"] == eid
        assert r["title"] == "Housing v2" == stored["title"]
        assert r["eco_type"] == "design" == stored["eco_type"]
        assert r["priority"] == "high" == stored["priority"]
        assert r["description"] == "thicker wall" == stored["description"]
        assert r["requested_by"] == "Deva" == stored["requested_by"]
        assert r["eco_status"] == "draft" == stored["status"]
        assert r["company_id"] == cid == stored["company_id"]
        assert r["naming_series"] == stored["naming_series"]
        assert r["naming_series"].startswith("ECO-")
        # Read-only fetch: writes nothing, not even an audit row.
        assert _snapshot(conn) == before

    def test_get_eco_not_found_refuses_without_writing(self, conn, env):
        missing = _uuid()
        before = _snapshot(conn)

        r = call_action(M.get_eco, conn, ns(eco_id=missing))
        assert is_error(r)
        assert r["message"] == f"ECO {missing} not found"
        assert _snapshot(conn) == before


# ===================================================================
# get-recipe (read-only; writes nothing)
# ===================================================================

class TestGetRecipeDepth:
    def test_get_recipe_returns_recipe_and_exact_ingredients(self, conn, env):
        cid = env["company_id"]
        rid = call_action(M.add_recipe, conn, ns(
            company_id=cid, name="Sponge",
            product_name="Cake", batch_size="100",
        ))["recipe_id"]
        call_action(M.add_recipe_ingredient, conn, ns(
            recipe_id=rid, ingredient_name="Flour",
            quantity="500", unit="grams", sequence="1",
            company_id=cid,
        ))
        call_action(M.add_recipe_ingredient, conn, ns(
            recipe_id=rid, ingredient_name="Sugar",
            quantity="200", unit="grams", sequence="2",
            company_id=cid,
        ))
        stored = _row(conn, "process_recipe", rid)
        before = _snapshot(conn)

        r = call_action(M.get_recipe, conn, ns(recipe_id=rid))
        assert is_ok(r), r
        assert r["id"] == rid
        assert r["name"] == "Sponge" == stored["name"]
        assert r["product_name"] == "Cake" == stored["product_name"]
        assert r["batch_size"] == "100" == stored["batch_size"]
        assert r["recipe_status"] == "active"
        assert r["ingredient_count"] == 2
        assert [i["ingredient_name"] for i in r["ingredients"]] == [
            "Flour", "Sugar",
        ]
        assert r["ingredients"][0]["quantity"] == "500"
        assert r["ingredients"][0]["unit"] == "grams"
        assert r["ingredients"][1]["quantity"] == "200"
        assert r["ingredients"][1]["unit"] == "grams"
        for ingredient in r["ingredients"]:
            match = [
                s for s in _rows_where(conn, "recipe_ingredient",
                                       "recipe_id", rid)
                if s["id"] == ingredient["id"]
            ]
            assert len(match) == 1
            assert ingredient["quantity"] == match[0]["quantity"]
            assert ingredient["unit"] == match[0]["unit"]
        # Read-only fetch: writes nothing, not even an audit row.
        assert _snapshot(conn) == before

    def test_get_recipe_not_found_refuses_without_writing(self, conn, env):
        missing = _uuid()
        before = _snapshot(conn)

        r = call_action(M.get_recipe, conn, ns(recipe_id=missing))
        assert is_error(r)
        assert r["message"] == f"Recipe {missing} not found"
        assert _snapshot(conn) == before


# ===================================================================
# get-shop-floor-entry (read-only; writes nothing)
# ===================================================================

class TestGetShopFloorEntryDepth:
    def test_get_entry_returns_exact_stored_values(self, conn, env):
        cid = env["company_id"]
        eid = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=cid, equipment_id="Press-3",
            operator="Mira", entry_type="setup",
            machine_status="setup", batch_number="B-77",
        ))["entry_id"]
        stored = _row(conn, "shop_floor_entry", eid)
        before = _snapshot(conn)

        r = call_action(M.get_shop_floor_entry, conn, ns(entry_id=eid))
        assert is_ok(r), r
        assert r["id"] == eid
        assert r["equipment_id"] == "Press-3" == stored["equipment_id"]
        assert r["operator"] == "Mira" == stored["operator"]
        assert r["entry_type"] == "setup" == stored["entry_type"]
        assert r["batch_number"] == "B-77" == stored["batch_number"]
        assert r["machine_status_value"] == "setup" == stored["machine_status"]
        assert r["company_id"] == cid == stored["company_id"]
        assert r["end_time"] is None
        assert stored["end_time"] is None
        # The header row carries no naming series (it lives only in the
        # response envelope and the audit row); assert the envelope only.
        assert r["start_time"] == stored["start_time"]
        # Read-only fetch: writes nothing, not even an audit row.
        assert _snapshot(conn) == before

    def test_get_entry_not_found_refuses_without_writing(self, conn, env):
        missing = _uuid()
        before = _snapshot(conn)

        r = call_action(M.get_shop_floor_entry, conn, ns(entry_id=missing))
        assert is_error(r)
        assert r["message"] == f"Shop floor entry {missing} not found"
        assert _snapshot(conn) == before


# ===================================================================
# get-tool (read-only; writes nothing)
# ===================================================================

class TestGetToolDepth:
    def test_get_tool_returns_exact_stored_values(self, conn, env):
        cid = env["company_id"]
        tid = call_action(M.add_tool, conn, ns(
            company_id=cid, name="Torque Wrench",
            tool_type="measuring", purchase_cost="250.00",
            max_usage_count="10", calibration_due="2099-06-01",
            location="Shelf A1",
        ))["tool_id"]
        stored = _row(conn, "tool", tid)
        before = _snapshot(conn)

        r = call_action(M.get_tool, conn, ns(tool_id=tid))
        assert is_ok(r), r
        assert r["id"] == tid
        assert r["name"] == "Torque Wrench" == stored["name"]
        assert r["tool_type"] == "measuring" == stored["tool_type"]
        assert r["tool_status"] == "available" == stored["status"]
        assert r["condition"] == "good" == stored["condition"]
        assert r["max_usage_count"] == 10 == stored["max_usage_count"]
        assert r["current_usage_count"] == 0 == stored["current_usage_count"]
        assert r["calibration_due"] == "2099-06-01" == stored["calibration_due"]
        assert r["location"] == "Shelf A1" == stored["location"]
        assert r["usage_records"] == 0
        # Money is text: exact Decimal string, never float.
        assert r["purchase_cost"] == "250.00" == stored["purchase_cost"]
        assert Decimal(r["purchase_cost"]) == Decimal("250.00")
        # Read-only fetch: writes nothing, not even an audit row.
        assert _snapshot(conn) == before

    def test_get_tool_counts_usage_records_without_writing(self, conn, env):
        cid = env["company_id"]
        tid = call_action(M.add_tool, conn, ns(
            company_id=cid, name="Counted Saw",
        ))["tool_id"]
        call_action(M.add_tool_usage, conn, ns(
            company_id=cid, tool_id=tid, usage_count="2",
        ))
        before = _snapshot(conn)

        r = call_action(M.get_tool, conn, ns(tool_id=tid))
        assert is_ok(r), r
        assert r["usage_records"] == 1
        assert r["current_usage_count"] == 2
        assert _snapshot(conn) == before

    def test_get_tool_not_found_refuses_without_writing(self, conn, env):
        missing = _uuid()
        before = _snapshot(conn)

        r = call_action(M.get_tool, conn, ns(tool_id=missing))
        assert is_error(r)
        assert r["message"] == f"Tool {missing} not found"
        assert _snapshot(conn) == before


# ===================================================================
# implement-eco (stored row change; no ledger legs by design)
# ===================================================================

class TestImplementEcoDepth:
    def test_implement_moves_approved_eco_to_implemented(self, conn, env):
        cid = env["company_id"]
        eid = call_action(M.add_eco, conn, ns(
            company_id=cid, title="Ship It",
        ))["eco_id"]
        decoy = call_action(M.add_eco, conn, ns(
            company_id=cid, title="Stay Draft",
        ))["eco_id"]
        assert _row(conn, "engineering_change_order", eid)["status"] == "draft"

        review = call_action(M.submit_eco_for_review, conn, ns(eco_id=eid))
        assert is_ok(review) and review["eco_status"] == "review"
        assert _row(conn, "engineering_change_order", eid)["status"] == "review"

        approved = call_action(M.approve_eco, conn, ns(
            eco_id=eid, approved_by="Kiran",
        ))
        assert is_ok(approved) and approved["eco_status"] == "approved"
        mid = _row(conn, "engineering_change_order", eid)
        assert mid["status"] == "approved"
        assert mid["approved_by"] == "Kiran"

        r = call_action(M.implement_eco, conn, ns(eco_id=eid))
        assert is_ok(r), r
        assert r["eco_id"] == eid
        assert r["eco_status"] == "implemented"

        after = _row(conn, "engineering_change_order", eid)
        assert after["status"] == "implemented"  # from "approved"
        assert after["approved_by"] == "Kiran"  # untouched
        assert after["title"] == "Ship It"  # untouched
        # The decoy ECO never left draft.
        assert _row(conn, "engineering_change_order", decoy)["status"] == "draft"
        # Status moves post no ledger legs.
        assert _count(conn, "gl_entry") == 0
        assert _count(conn, "stock_ledger_entry") == 0

    def test_implement_from_draft_refuses_without_writing(self, conn, env):
        cid = env["company_id"]
        eid = call_action(M.add_eco, conn, ns(
            company_id=cid, title="Too Early",
        ))["eco_id"]
        before = _snapshot(conn)

        r = call_action(M.implement_eco, conn, ns(eco_id=eid))
        assert is_error(r)
        assert r["message"] == (
            "Cannot implement ECO in status 'draft'. "
            "Must be approved or in_progress"
        )
        assert _snapshot(conn) == before
        assert _row(conn, "engineering_change_order", eid)["status"] == "draft"


# ===================================================================
# list-ecos (read-only; validates no input, so purity instead of refusal)
# ===================================================================

class TestListEcosDepth:
    def test_list_ecos_returns_exact_set_with_filters(self, conn, env):
        cid = env["company_id"]
        first = call_action(M.add_eco, conn, ns(
            company_id=cid, title="Frame Change",
            eco_type="design", priority="high",
        ))["eco_id"]
        second = call_action(M.add_eco, conn, ns(
            company_id=cid, title="Alloy Swap",
            eco_type="material", priority="low",
        ))["eco_id"]
        other_cid = _second_company(conn)
        foreign = call_action(M.add_eco, conn, ns(
            company_id=other_cid, title="Foreign ECO",
        ))["eco_id"]
        before = _snapshot(conn)

        r = call_action(M.list_ecos, conn, ns(company_id=cid))
        assert is_ok(r), r
        assert r["total_count"] == 2
        assert {e["id"] for e in r["ecos"]} == {first, second}
        assert all(e["eco_status"] == "draft" for e in r["ecos"])
        assert all(e["company_id"] == cid for e in r["ecos"])
        assert foreign not in {e["id"] for e in r["ecos"]}

        typed = call_action(M.list_ecos, conn, ns(
            company_id=cid, eco_type="material",
        ))
        assert is_ok(typed), typed
        assert typed["total_count"] == 1
        assert typed["ecos"][0]["id"] == second
        assert typed["ecos"][0]["title"] == "Alloy Swap"
        # Read-only list: writes nothing, not even an audit row.
        assert _snapshot(conn) == before

    def test_list_ecos_unknown_company_is_empty_and_writes_nothing(
        self, conn, env,
    ):
        # list-ecos validates no input and has no error path; the paired
        # check is that an empty read is still a pure read.
        call_action(M.add_eco, conn, ns(
            company_id=env["company_id"], title="Real ECO",
        ))
        before = _snapshot(conn)

        r = call_action(M.list_ecos, conn, ns(company_id=_uuid()))
        assert is_ok(r), r
        assert r["total_count"] == 0
        assert r["ecos"] == []
        assert _snapshot(conn) == before


# ===================================================================
# list-recipes (read-only; validates no input, so purity instead of refusal)
# ===================================================================

class TestListRecipesDepth:
    def test_list_recipes_returns_exact_set_with_filter(self, conn, env):
        cid = env["company_id"]
        first = call_action(M.add_recipe, conn, ns(
            company_id=cid, name="Base Loaf",
            product_name="Bread",
        ))["recipe_id"]
        second = call_action(M.add_recipe, conn, ns(
            company_id=cid, name="Trial Loaf",
            product_name="Bread", recipe_type="alternative",
        ))["recipe_id"]
        before = _snapshot(conn)

        r = call_action(M.list_recipes, conn, ns(company_id=cid))
        assert is_ok(r), r
        assert r["total_count"] == 2
        assert {rec["id"] for rec in r["recipes"]} == {first, second}
        assert {rec["name"] for rec in r["recipes"]} == {
            "Base Loaf", "Trial Loaf",
        }
        assert all(rec["recipe_status"] == "active" for rec in r["recipes"])

        alt = call_action(M.list_recipes, conn, ns(
            company_id=cid, recipe_type="alternative",
        ))
        assert is_ok(alt), alt
        assert alt["total_count"] == 1
        assert alt["recipes"][0]["id"] == second
        # Read-only list: writes nothing, not even an audit row.
        assert _snapshot(conn) == before

    def test_list_recipes_no_match_is_empty_and_writes_nothing(
        self, conn, env,
    ):
        # list-recipes validates no input and has no error path; the paired
        # check is that an empty read is still a pure read.
        call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"], name="Real Recipe",
            product_name="Pie",
        ))
        before = _snapshot(conn)

        r = call_action(M.list_recipes, conn, ns(
            company_id=env["company_id"], recipe_type="obsolete",
        ))
        assert is_ok(r), r
        assert r["total_count"] == 0
        assert r["recipes"] == []
        assert _snapshot(conn) == before


# ===================================================================
# list-shop-floor-entries (read-only; validates no input, purity not refusal)
# ===================================================================

class TestListShopFloorEntriesDepth:
    def test_list_entries_returns_exact_rows_with_filter(self, conn, env):
        cid = env["company_id"]
        prod = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=cid, equipment_id="Oven-1",
            operator="Leela", entry_type="production",
        ))["entry_id"]
        assert is_ok(call_action(M.update_shop_floor_entry, conn, ns(
            entry_id=prod, quantity_produced="10",
            quantity_rejected="1",
        )))
        down = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=cid, equipment_id="Oven-2",
            entry_type="downtime", machine_status="breakdown",
        ))["entry_id"]
        before = _snapshot(conn)

        r = call_action(M.list_shop_floor_entries, conn, ns(company_id=cid))
        assert is_ok(r), r
        assert r["total_count"] == 2
        by_id = {e["id"]: e for e in r["entries"]}
        assert set(by_id) == {prod, down}
        assert by_id[prod]["equipment_id"] == "Oven-1"
        assert by_id[prod]["operator"] == "Leela"
        assert by_id[prod]["entry_type"] == "production"
        assert by_id[prod]["quantity_produced"] == 10
        assert by_id[prod]["quantity_rejected"] == 1
        assert by_id[down]["machine_status_value"] == "breakdown"

        filtered = call_action(M.list_shop_floor_entries, conn, ns(
            company_id=cid, entry_type="downtime",
        ))
        assert is_ok(filtered), filtered
        assert filtered["total_count"] == 1
        assert filtered["entries"][0]["id"] == down
        assert filtered["entries"][0]["equipment_id"] == "Oven-2"
        # Read-only list: writes nothing, not even an audit row.
        assert _snapshot(conn) == before

    def test_list_entries_no_match_is_empty_and_writes_nothing(
        self, conn, env,
    ):
        # list-shop-floor-entries validates no input and has no error path;
        # the paired check is that an empty read is still a pure read.
        call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"], equipment_id="Oven-9",
        ))
        before = _snapshot(conn)

        r = call_action(M.list_shop_floor_entries, conn, ns(
            company_id=env["company_id"], equipment_id="No-Such-Press",
        ))
        assert is_ok(r), r
        assert r["total_count"] == 0
        assert r["entries"] == []
        assert _snapshot(conn) == before
