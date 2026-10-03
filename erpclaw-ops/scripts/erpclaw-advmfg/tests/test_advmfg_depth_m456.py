"""M456 depth: behavioural evidence for 12 advmfg actions.

Each action below already had a test that proved the wrong thing (response
shape or routability). Each test here proves the database effect instead:
what row exists afterwards with which exact values, what changed from what
to what, and what did not change. Money is TEXT: exact string comparisons,
Decimal for arithmetic, never float, never round().

Per-action depth signal:
- list-tool-usage ............ STORED ROWS readback (tool_usage rows logged
                                via add-tool-usage surface exactly)
- list-tools .................. STORED ROWS readback (tool rows surface exactly)
- oee-report .................. STORED ROWS computed readback (aggregates over
                                shop_floor_entry rows; exact Decimal strings)
- production-log-report ....... STORED ROWS readback (entry rows + totals)
- remove-recipe-ingredient .... STORED ROW deletion (one recipe_ingredient row
                                gone, sibling and recipe intact)
- shop-floor-dashboard ........ STORED ROWS aggregation readback (counts and
                                sums over shop_floor_entry rows)
- tool-utilization-report ..... STORED ROWS aggregation readback (per-tool
                                usage sums + utilization_pct, exact strings)
- update-eco .................. STORED ROW change (engineering_change_order
                                before -> after)
- update-recipe ............... STORED ROW change (process_recipe before ->
                                after; batch_size compared as exact text)
- update-recipe-ingredient .... STORED ROW change (recipe_ingredient before ->
                                after)
- update-shop-floor-entry ..... STORED ROW change (shop_floor_entry before ->
                                after)
- update-tool ................. STORED ROW change (tool before -> after;
                                purchase_cost compared as exact Decimal text)

Ledger note: none of these twelve actions reaches the ledger on its success
path. The writers touch only their own domain tables (plus one audit_log
row each); the lists, reports and dashboard are pure reads. No success test
below asserts a new ledger leg, and every test pins the gl_entry count (or
full snapshot) unchanged so a later reader does not add a leg assertion
that cannot hold.

Refusal note: list-tools and list-tool-usage validate nothing -- every
input combination returns ok, so no refusal exists to test. Their second
test pins the truthful-empty behaviour (unknown filter -> total_count 0,
empty list, database identical) instead. The other ten actions each get one
refusal case: the refusal happens, the message is truthful, and the
database snapshot is byte-identical afterwards.
"""
import os
import sys
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from advmfg_helpers import (
    load_db_query, call_action, ns, is_ok, is_error, _uuid,
)

from erpclaw_lib.query import Field, P, Q, Table, dynamic_update

M = load_db_query()

_SNAPSHOT_TABLES = (
    "shop_floor_entry",
    "tool",
    "tool_usage",
    "engineering_change_order",
    "process_recipe",
    "recipe_ingredient",
    "gl_entry",
    "audit_log",
)


def _snapshot(conn):
    """Byte-level dump of every table these actions could touch."""
    snap = {}
    for name in _SNAPSHOT_TABLES:
        tbl = Table(name)
        rows = conn.execute(
            Q.from_(tbl).select(tbl.star).orderby(Field("id")).get_sql()
        ).fetchall()
        snap[name] = [tuple(r) for r in rows]
    return snap


def _counts(conn):
    return {t: len(v) for t, v in _snapshot(conn).items()}


def _read(conn, table, rid):
    """Read one stored row back through the seam."""
    tbl = Table(table)
    return conn.execute(
        Q.from_(tbl).select(tbl.star).where(Field("id") == P()).get_sql(),
        (rid,),
    ).fetchone()


def _set_duration(conn, entry_id, minutes):
    """Deterministic setup: duration_minutes is otherwise stamped by the
    clock inside complete-shop-floor-entry, so tests pin it directly."""
    sql, params = dynamic_update(
        "shop_floor_entry", {"duration_minutes": minutes}, {"id": entry_id})
    conn.execute(sql, params)
    conn.commit()


# ── update-shop-floor-entry: STORED ROW ───────────────────────────────────

class TestUpdateShopFloorEntryDepth:
    def test_update_rewrites_exact_columns_and_nothing_else(self, conn, env):
        # This action does NOT reach the ledger: one shop_floor_entry row
        # changes (plus its audit row). gl_entry is pinned unchanged.
        add_r = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"],
            operator="Op Before",
            batch_number="B-001",
        ))
        assert is_ok(add_r), add_r
        eid = add_r["entry_id"]
        before_row = dict(_read(conn, "shop_floor_entry", eid))
        assert before_row["operator"] == "Op Before"
        assert before_row["quantity_produced"] == 0
        assert before_row["quantity_rejected"] == 0
        before = _counts(conn)

        r = call_action(M.update_shop_floor_entry, conn, ns(
            entry_id=eid,
            operator="John Doe",
            quantity_produced="7",
            quantity_rejected="1",
        ))
        assert is_ok(r), r
        assert sorted(r["updated_fields"]) == [
            "operator", "quantity_produced", "quantity_rejected"]

        after_row = dict(_read(conn, "shop_floor_entry", eid))
        assert after_row["operator"] == "John Doe"
        assert before_row["operator"] == "Op Before"
        assert after_row["quantity_produced"] == 7
        assert after_row["quantity_rejected"] == 1
        assert after_row["entry_type"] == before_row["entry_type"] == "production"
        assert after_row["machine_status"] == before_row["machine_status"]
        assert after_row["batch_number"] == "B-001"
        assert after_row["company_id"] == env["company_id"]

        after = _counts(conn)
        assert after["shop_floor_entry"] == before["shop_floor_entry"]
        assert after["audit_log"] == before["audit_log"] + 1
        assert after["gl_entry"] == before["gl_entry"]

    def test_invalid_entry_type_refused_truthfully_and_writes_nothing(self, conn, env):
        add_r = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"], operator="Op Before",
        ))
        assert is_ok(add_r), add_r
        before = _snapshot(conn)

        r = call_action(M.update_shop_floor_entry, conn, ns(
            entry_id=add_r["entry_id"], entry_type="bogus-type",
        ))
        assert is_error(r)
        assert r["message"] == "Invalid entry-type: bogus-type"

        assert _snapshot(conn) == before, "a refused update must half-write nothing"


# ── update-tool: STORED ROW ───────────────────────────────────────────────

class TestUpdateToolDepth:
    def test_update_rewrites_exact_columns_money_is_text(self, conn, env):
        # This action does NOT reach the ledger: one tool row changes (plus
        # its audit row). purchase_cost is TEXT money: exact Decimal strings.
        add_r = call_action(M.add_tool, conn, ns(
            company_id=env["company_id"],
            name="Torque Wrench",
            purchase_cost="1299.99",
            location="Shelf A1",
        ))
        assert is_ok(add_r), add_r
        tid = add_r["tool_id"]
        stored = _read(conn, "tool", tid)
        assert stored["purchase_cost"] == "1299.99"
        assert Decimal(stored["purchase_cost"]) == Decimal("1299.99")
        before = _counts(conn)

        r = call_action(M.update_tool, conn, ns(
            tool_id=tid,
            location="Shelf B2",
            purchase_cost="1499.50",
        ))
        assert is_ok(r), r
        assert sorted(r["updated_fields"]) == ["location", "purchase_cost"]

        after_row = dict(_read(conn, "tool", tid))
        assert after_row["location"] == "Shelf B2"
        assert stored["location"] == "Shelf A1"
        assert after_row["purchase_cost"] == "1499.50"
        assert Decimal(after_row["purchase_cost"]) == Decimal("1499.50")
        assert after_row["name"] == "Torque Wrench"
        assert after_row["status"] == "available"
        assert after_row["condition"] == "good"
        assert after_row["company_id"] == env["company_id"]

        after = _counts(conn)
        assert after["tool"] == before["tool"]
        assert after["audit_log"] == before["audit_log"] + 1
        assert after["gl_entry"] == before["gl_entry"]

    def test_invalid_tool_status_refused_truthfully_and_writes_nothing(self, conn, env):
        add_r = call_action(M.add_tool, conn, ns(
            company_id=env["company_id"], name="Hammer",
        ))
        assert is_ok(add_r), add_r
        before = _snapshot(conn)

        r = call_action(M.update_tool, conn, ns(
            tool_id=add_r["tool_id"], tool_status="bogus",
        ))
        assert is_error(r)
        assert r["message"] == "Invalid tool-status: bogus"

        assert _snapshot(conn) == before, "a refused update must half-write nothing"


# ── list-tools: STORED ROWS ───────────────────────────────────────────────

class TestListToolsDepth:
    def test_list_surfaces_exact_stored_rows(self, conn, env):
        # Read-only: no stored row, no ledger effect. Behaviour = the exact
        # tool rows grounded against what add-tool stored.
        a = call_action(M.add_tool, conn, ns(
            company_id=env["company_id"], name="Alpha Cutter",
            tool_type="cutting",
        ))
        assert is_ok(a), a
        b = call_action(M.add_tool, conn, ns(
            company_id=env["company_id"], name="Beta Gauge",
            tool_type="measuring",
        ))
        assert is_ok(b), b
        before = _snapshot(conn)

        r = call_action(M.list_tools, conn, ns(company_id=env["company_id"]))
        assert is_ok(r), r
        assert r["total_count"] == 2
        assert {t["id"] for t in r["tools"]} == {a["tool_id"], b["tool_id"]}
        assert {t["name"] for t in r["tools"]} == {"Alpha Cutter", "Beta Gauge"}
        for t in r["tools"]:
            assert t["company_id"] == env["company_id"]
            assert _read(conn, "tool", t["id"])["name"] == t["name"]

        f = call_action(M.list_tools, conn, ns(
            company_id=env["company_id"], tool_type="measuring",
        ))
        assert is_ok(f), f
        assert f["total_count"] == 1
        assert f["tools"][0]["id"] == b["tool_id"]
        assert f["tools"][0]["name"] == "Beta Gauge"

        assert _snapshot(conn) == before, "a list must write nothing"

    def test_unknown_filter_returns_truthful_empty_and_writes_nothing(self, conn, env):
        # list-tools validates nothing, so no refusal exists; the boundary
        # is a truthful empty result with the database identical.
        a = call_action(M.add_tool, conn, ns(
            company_id=env["company_id"], name="Alpha Cutter",
        ))
        assert is_ok(a), a
        before = _snapshot(conn)

        r = call_action(M.list_tools, conn, ns(
            company_id=env["company_id"], search="zzz-no-such-tool",
        ))
        assert is_ok(r), r
        assert r["total_count"] == 0
        assert r["tools"] == []

        assert _snapshot(conn) == before, "a list must write nothing"


# ── list-tool-usage: STORED ROWS ──────────────────────────────────────────

class TestListToolUsageDepth:
    def test_list_surfaces_exact_usage_rows(self, conn, env):
        # Read-only: no stored row, no ledger effect. Behaviour = the exact
        # tool_usage rows grounded against what add-tool-usage stored.
        a = call_action(M.add_tool, conn, ns(
            company_id=env["company_id"], name="Drill",
        ))
        assert is_ok(a), a
        tid = a["tool_id"]
        u1 = call_action(M.add_tool_usage, conn, ns(
            company_id=env["company_id"], tool_id=tid,
            usage_count="3", usage_duration_minutes="30", operator="Op1",
        ))
        assert is_ok(u1), u1
        u2 = call_action(M.add_tool_usage, conn, ns(
            company_id=env["company_id"], tool_id=tid,
            usage_count="2", usage_duration_minutes="15", operator="Op2",
        ))
        assert is_ok(u2), u2
        assert _read(conn, "tool", tid)["current_usage_count"] == 5
        before = _snapshot(conn)

        r = call_action(M.list_tool_usage, conn, ns(tool_id=tid))
        assert is_ok(r), r
        assert r["total_count"] == 2
        assert {u["id"] for u in r["usages"]} == {
            u1["usage_id"], u2["usage_id"]}
        assert sorted(u["usage_count"] for u in r["usages"]) == [2, 3]
        for u in r["usages"]:
            assert u["tool_id"] == tid
            assert _read(conn, "tool_usage", u["id"])["operator"] in (
                "Op1", "Op2")

        assert _snapshot(conn) == before, "a list must write nothing"

    def test_unknown_tool_returns_truthful_empty_and_writes_nothing(self, conn, env):
        # list-tool-usage validates nothing, so no refusal exists; the
        # boundary is a truthful empty result with the database identical.
        before = _snapshot(conn)

        r = call_action(M.list_tool_usage, conn, ns(tool_id=_uuid()))
        assert is_ok(r), r
        assert r["total_count"] == 0
        assert r["usages"] == []

        assert _snapshot(conn) == before, "a list must write nothing"


# ── tool-utilization-report: STORED ROWS ──────────────────────────────────

class TestToolUtilizationReportDepth:
    def test_report_aggregates_exact_stored_usage(self, conn, env):
        # Read-only: no stored row, no ledger effect. Behaviour = exact
        # per-tool sums grounded against the logged usage rows.
        a = call_action(M.add_tool, conn, ns(
            company_id=env["company_id"], name="Press Die",
            max_usage_count="100",
        ))
        assert is_ok(a), a
        u = call_action(M.add_tool_usage, conn, ns(
            company_id=env["company_id"], tool_id=a["tool_id"],
            usage_count="25", usage_duration_minutes="120",
        ))
        assert is_ok(u), u
        assert u["new_usage_count"] == 25
        before = _snapshot(conn)

        r = call_action(M.tool_utilization_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert r["total_count"] == 1
        row = r["tools"][0]
        assert row["id"] == a["tool_id"]
        assert row["name"] == "Press Die"
        assert row["current_usage_count"] == 25
        assert row["max_usage_count"] == 100
        assert row["usage_records"] == 1
        assert row["total_usages"] == 25
        assert row["total_duration"] == 120
        assert row["utilization_pct"] == "25.00"
        assert Decimal(row["utilization_pct"]) == Decimal("25.00")

        assert _snapshot(conn) == before, "a report must write nothing"

    def test_missing_company_refused_truthfully_and_writes_nothing(self, conn, env):
        before = _snapshot(conn)

        r = call_action(M.tool_utilization_report, conn, ns())
        assert is_error(r)
        assert r["message"] == "--company-id is required"

        assert _snapshot(conn) == before, "a refused report must half-write nothing"


# ── shop-floor-dashboard: STORED ROWS ─────────────────────────────────────

class TestShopFloorDashboardDepth:
    def test_dashboard_aggregates_exact_stored_entries(self, conn, env):
        # Read-only: no stored row, no ledger effect. Behaviour = exact
        # counts and sums grounded against the stored entry rows.
        e1 = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"], entry_type="production",
            operator="Op1",
        ))
        assert is_ok(e1), e1
        e2 = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"], entry_type="production",
            operator="Op2",
        ))
        assert is_ok(e2), e2
        e3 = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"], entry_type="downtime",
            machine_status="breakdown",
        ))
        assert is_ok(e3), e3
        assert is_ok(call_action(M.update_shop_floor_entry, conn, ns(
            entry_id=e1["entry_id"],
            quantity_produced="100", quantity_rejected="5",
        )))
        assert is_ok(call_action(M.update_shop_floor_entry, conn, ns(
            entry_id=e2["entry_id"],
            quantity_produced="50", quantity_rejected="0",
        )))
        before = _snapshot(conn)

        r = call_action(M.shop_floor_dashboard, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert r["total_entries"] == 3
        assert r["active_entries"] == 3
        assert r["by_entry_type"] == {"production": 2, "downtime": 1}
        assert r["total_produced"] == 150
        assert r["total_rejected"] == 5
        assert r["rejection_rate_pct"] == "3.23"
        assert Decimal(r["rejection_rate_pct"]) == Decimal("3.23")

        assert _snapshot(conn) == before, "a dashboard must write nothing"

    def test_missing_company_refused_truthfully_and_writes_nothing(self, conn, env):
        before = _snapshot(conn)

        r = call_action(M.shop_floor_dashboard, conn, ns())
        assert is_error(r)
        assert r["message"] == "--company-id is required"

        assert _snapshot(conn) == before, "a refused dashboard must half-write nothing"


# ── production-log-report: STORED ROWS ────────────────────────────────────

class TestProductionLogReportDepth:
    def test_report_lists_exact_stored_entries_with_totals(self, conn, env):
        # Read-only: no stored row, no ledger effect. Behaviour = the exact
        # entry rows plus exact totals grounded against them.
        e1 = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"], entry_type="production",
            operator="Op1", batch_number="B-101",
        ))
        assert is_ok(e1), e1
        e2 = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"], entry_type="production",
            operator="Op2", batch_number="B-102",
        ))
        assert is_ok(e2), e2
        assert is_ok(call_action(M.update_shop_floor_entry, conn, ns(
            entry_id=e1["entry_id"],
            quantity_produced="100", quantity_rejected="5",
        )))
        assert is_ok(call_action(M.update_shop_floor_entry, conn, ns(
            entry_id=e2["entry_id"],
            quantity_produced="50", quantity_rejected="0",
        )))
        before = _snapshot(conn)

        r = call_action(M.production_log_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert r["total_count"] == 2
        assert {e["id"] for e in r["entries"]} == {
            e1["entry_id"], e2["entry_id"]}
        assert {e["batch_number"] for e in r["entries"]} == {"B-101", "B-102"}
        assert r["total_produced"] == 150
        assert r["total_rejected"] == 5
        assert r["total_duration_minutes"] == 0
        for e in r["entries"]:
            assert e["company_id"] == env["company_id"]

        assert _snapshot(conn) == before, "a report must write nothing"

    def test_missing_company_refused_truthfully_and_writes_nothing(self, conn, env):
        before = _snapshot(conn)

        r = call_action(M.production_log_report, conn, ns())
        assert is_error(r)
        assert r["message"] == "--company-id is required"

        assert _snapshot(conn) == before, "a refused report must half-write nothing"


# ── oee-report: STORED ROWS ───────────────────────────────────────────────

class TestOeeReportDepth:
    def test_oee_computed_exactly_from_stored_entries(self, conn, env):
        # Read-only: no stored row, no ledger effect. Behaviour = exact OEE
        # factors grounded against the stored entry rows. Durations are
        # pinned directly because complete-shop-floor-entry stamps them from
        # the clock and would make the math nondeterministic.
        a = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"], entry_type="production",
            equipment_id="EQ-1", start_time="2026-01-05 08:00:00",
        ))
        assert is_ok(a), a
        b = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"], entry_type="production",
            equipment_id="EQ-1", start_time="2026-01-05 09:00:00",
        ))
        assert is_ok(b), b
        c = call_action(M.add_shop_floor_entry, conn, ns(
            company_id=env["company_id"], entry_type="downtime",
            equipment_id="EQ-1", machine_status="breakdown",
            start_time="2026-01-05 10:00:00",
        ))
        assert is_ok(c), c
        assert is_ok(call_action(M.update_shop_floor_entry, conn, ns(
            entry_id=a["entry_id"],
            quantity_produced="100", quantity_rejected="10",
        )))
        assert is_ok(call_action(M.update_shop_floor_entry, conn, ns(
            entry_id=b["entry_id"],
            quantity_produced="100", quantity_rejected="0",
        )))
        _set_duration(conn, a["entry_id"], 60)
        _set_duration(conn, b["entry_id"], 60)
        _set_duration(conn, c["entry_id"], 30)
        before = _snapshot(conn)

        r = call_action(M.oee_report, conn, ns(
            company_id=env["company_id"], equipment_id="EQ-1",
        ))
        assert is_ok(r), r
        assert r["equipment_id"] == "EQ-1"
        assert r["total_produced"] == "200"
        assert r["total_good"] == "190"
        assert r["downtime_minutes"] == "30"
        assert r["availability"] == "80.00"
        assert r["performance"] == "100.00"
        assert r["quality"] == "95.00"
        assert r["oee"] == "76.00"
        assert Decimal(r["oee"]) == Decimal("76.00")

        assert _snapshot(conn) == before, "a report must write nothing"

    def test_missing_equipment_refused_truthfully_and_writes_nothing(self, conn, env):
        before = _snapshot(conn)

        r = call_action(M.oee_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_error(r)
        assert r["message"] == "--equipment-id is required"

        assert _snapshot(conn) == before, "a refused report must half-write nothing"


# ── update-eco: STORED ROW ────────────────────────────────────────────────

class TestUpdateEcoDepth:
    def test_update_rewrites_exact_columns_and_nothing_else(self, conn, env):
        # This action does NOT reach the ledger: one
        # engineering_change_order row changes (plus its audit row).
        # gl_entry is pinned unchanged.
        add_r = call_action(M.add_eco, conn, ns(
            company_id=env["company_id"], title="Bracket v2",
        ))
        assert is_ok(add_r), add_r
        eid = add_r["eco_id"]
        stored = dict(_read(conn, "engineering_change_order", eid))
        assert stored["description"] is None
        assert stored["priority"] == "medium"
        before = _counts(conn)

        r = call_action(M.update_eco, conn, ns(
            eco_id=eid,
            description="Swap to 6061 alloy",
            priority="high",
        ))
        assert is_ok(r), r
        assert sorted(r["updated_fields"]) == ["description", "priority"]

        after_row = dict(_read(conn, "engineering_change_order", eid))
        assert after_row["description"] == "Swap to 6061 alloy"
        assert after_row["priority"] == "high"
        assert after_row["title"] == "Bracket v2"
        assert after_row["eco_type"] == "design"
        assert after_row["status"] == "draft"
        assert after_row["company_id"] == env["company_id"]

        after = _counts(conn)
        assert after["engineering_change_order"] == before["engineering_change_order"]
        assert after["audit_log"] == before["audit_log"] + 1
        assert after["gl_entry"] == before["gl_entry"]

    def test_invalid_priority_refused_truthfully_and_writes_nothing(self, conn, env):
        add_r = call_action(M.add_eco, conn, ns(
            company_id=env["company_id"], title="Bracket v2",
        ))
        assert is_ok(add_r), add_r
        before = _snapshot(conn)

        r = call_action(M.update_eco, conn, ns(
            eco_id=add_r["eco_id"], priority="urgent",
        ))
        assert is_error(r)
        assert r["message"] == "Invalid priority: urgent"

        assert _snapshot(conn) == before, "a refused update must half-write nothing"


# ── update-recipe: STORED ROW ─────────────────────────────────────────────

class TestUpdateRecipeDepth:
    def test_update_rewrites_exact_columns_quantities_are_text(self, conn, env):
        # This action does NOT reach the ledger: one process_recipe row
        # changes (plus its audit row). batch_size is TEXT: exact strings.
        add_r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="Cake Base",
            product_name="Cake",
        ))
        assert is_ok(add_r), add_r
        rid = add_r["recipe_id"]
        stored = dict(_read(conn, "process_recipe", rid))
        assert stored["batch_size"] == "1"
        assert stored["expected_yield"] == "100"
        before = _counts(conn)

        r = call_action(M.update_recipe, conn, ns(
            recipe_id=rid, batch_size="100", expected_yield="95",
        ))
        assert is_ok(r), r
        assert sorted(r["updated_fields"]) == ["batch_size", "expected_yield"]

        after_row = dict(_read(conn, "process_recipe", rid))
        assert after_row["batch_size"] == "100"
        assert Decimal(after_row["batch_size"]) == Decimal("100")
        assert after_row["expected_yield"] == "95"
        assert after_row["name"] == "Cake Base"
        assert after_row["product_name"] == "Cake"
        assert after_row["is_active"] == 1
        assert after_row["company_id"] == env["company_id"]

        after = _counts(conn)
        assert after["process_recipe"] == before["process_recipe"]
        assert after["audit_log"] == before["audit_log"] + 1
        assert after["gl_entry"] == before["gl_entry"]

    def test_invalid_recipe_type_refused_truthfully_and_writes_nothing(self, conn, env):
        add_r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="Cake Base",
            product_name="Cake",
        ))
        assert is_ok(add_r), add_r
        before = _snapshot(conn)

        r = call_action(M.update_recipe, conn, ns(
            recipe_id=add_r["recipe_id"], recipe_type="bogus",
        ))
        assert is_error(r)
        assert r["message"] == "Invalid recipe-type: bogus"

        assert _snapshot(conn) == before, "a refused update must half-write nothing"


# ── update-recipe-ingredient: STORED ROW ──────────────────────────────────

class TestUpdateRecipeIngredientDepth:
    def test_update_rewrites_exact_columns_and_nothing_else(self, conn, env):
        # This action does NOT reach the ledger: one recipe_ingredient row
        # changes (plus its audit row). quantity is TEXT: exact strings.
        add_r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="Bread",
            product_name="Loaf",
        ))
        assert is_ok(add_r), add_r
        ing = call_action(M.add_recipe_ingredient, conn, ns(
            recipe_id=add_r["recipe_id"],
            ingredient_name="Flour",
            quantity="500",
            unit="grams",
            company_id=env["company_id"],
        ))
        assert is_ok(ing), ing
        iid = ing["ingredient_id"]
        stored = dict(_read(conn, "recipe_ingredient", iid))
        assert stored["quantity"] == "500"
        assert stored["unit"] == "grams"
        before = _counts(conn)

        r = call_action(M.update_recipe_ingredient, conn, ns(
            ingredient_id=iid, quantity="750", unit="kg",
        ))
        assert is_ok(r), r
        assert sorted(r["updated_fields"]) == ["quantity", "unit"]

        after_row = dict(_read(conn, "recipe_ingredient", iid))
        assert after_row["quantity"] == "750"
        assert Decimal(after_row["quantity"]) == Decimal("750")
        assert after_row["unit"] == "kg"
        assert after_row["ingredient_name"] == "Flour"
        assert after_row["recipe_id"] == add_r["recipe_id"]

        after = _counts(conn)
        assert after["recipe_ingredient"] == before["recipe_ingredient"]
        assert after["audit_log"] == before["audit_log"] + 1
        assert after["gl_entry"] == before["gl_entry"]

    def test_empty_update_refused_truthfully_and_writes_nothing(self, conn, env):
        add_r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="Bread",
            product_name="Loaf",
        ))
        assert is_ok(add_r), add_r
        ing = call_action(M.add_recipe_ingredient, conn, ns(
            recipe_id=add_r["recipe_id"],
            ingredient_name="Flour",
            quantity="500",
            unit="grams",
            company_id=env["company_id"],
        ))
        assert is_ok(ing), ing
        before = _snapshot(conn)

        r = call_action(M.update_recipe_ingredient, conn, ns(
            ingredient_id=ing["ingredient_id"],
        ))
        assert is_error(r)
        assert r["message"] == "No fields to update"

        assert _snapshot(conn) == before, "a refused update must half-write nothing"


# ── remove-recipe-ingredient: STORED ROW ──────────────────────────────────

class TestRemoveRecipeIngredientDepth:
    def test_remove_deletes_exact_row_and_keeps_sibling(self, conn, env):
        # This action does NOT reach the ledger: one recipe_ingredient row
        # is deleted (plus its audit row). The sibling row and the parent
        # recipe row must be byte-identical afterwards.
        add_r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="Cookies",
            product_name="Batch",
        ))
        assert is_ok(add_r), add_r
        rid = add_r["recipe_id"]
        flour = call_action(M.add_recipe_ingredient, conn, ns(
            recipe_id=rid, ingredient_name="Flour",
            quantity="500", unit="grams",
            company_id=env["company_id"],
        ))
        assert is_ok(flour), flour
        sugar = call_action(M.add_recipe_ingredient, conn, ns(
            recipe_id=rid, ingredient_name="Sugar",
            quantity="200", unit="grams",
            company_id=env["company_id"],
        ))
        assert is_ok(sugar), sugar
        sibling_before = dict(_read(conn, "recipe_ingredient", sugar["ingredient_id"]))
        recipe_before = dict(_read(conn, "process_recipe", rid))
        before = _counts(conn)

        r = call_action(M.remove_recipe_ingredient, conn, ns(
            ingredient_id=flour["ingredient_id"],
        ))
        assert is_ok(r), r
        assert r["removed"] is True
        assert r["recipe_id"] == rid

        assert _read(conn, "recipe_ingredient", flour["ingredient_id"]) is None
        assert dict(_read(conn, "recipe_ingredient", sugar["ingredient_id"])) == sibling_before
        assert dict(_read(conn, "process_recipe", rid)) == recipe_before

        listed = call_action(M.list_recipe_ingredients, conn, ns(recipe_id=rid))
        assert is_ok(listed), listed
        assert listed["total_count"] == 1
        assert listed["ingredients"][0]["ingredient_name"] == "Sugar"

        after = _counts(conn)
        assert after["recipe_ingredient"] == before["recipe_ingredient"] - 1
        assert after["audit_log"] == before["audit_log"] + 1
        assert after["gl_entry"] == before["gl_entry"]

    def test_unknown_ingredient_refused_truthfully_and_writes_nothing(self, conn, env):
        add_r = call_action(M.add_recipe, conn, ns(
            company_id=env["company_id"],
            name="Cookies",
            product_name="Batch",
        ))
        assert is_ok(add_r), add_r
        ing = call_action(M.add_recipe_ingredient, conn, ns(
            recipe_id=add_r["recipe_id"], ingredient_name="Flour",
            quantity="500", unit="grams",
            company_id=env["company_id"],
        ))
        assert is_ok(ing), ing
        bad_id = _uuid()
        before = _snapshot(conn)

        r = call_action(M.remove_recipe_ingredient, conn, ns(
            ingredient_id=bad_id,
        ))
        assert is_error(r)
        assert r["message"] == f"Ingredient {bad_id} not found"

        assert _snapshot(conn) == before, "a refused removal must half-write nothing"
