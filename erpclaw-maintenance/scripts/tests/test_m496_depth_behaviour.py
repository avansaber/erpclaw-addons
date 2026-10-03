"""M496 depth — behavioural evidence for the 4 maintenance report actions.

Prior state (read before writing): each action below already has a test in
test_maintenance.py, and every one of them proves only shape/routability:
  TestMaintenanceCostReport.test_cost_report          asserts "grand_total" in r
  TestPMComplianceReport.test_pm_compliance_report    asserts "compliance_pct" in r
  TestDowntimeReport.test_downtime_report             asserts "grand_total_hours" in r
  TestSparePartsUsage.test_spare_parts_usage          asserts "items" in r
None seeds domain rows, reads a row back, or observes the database, so a
routed action returning a well-shaped but wrong or empty aggregate passes
them all. The tests below deepen the assertion: seeded rows in, exact
aggregates out, rows re-read through the seam, and a byte-identical snapshot
proving the success path writes nothing.

Depth classification per action (stored row vs ledger effect):
  maintenance-cost-report         read-only aggregate; the payload mirrors
      stored maintenance_work_order rows. No stored-row write, no ledger
      effect: reports.py has no audit/posting path and the snapshot below
      stays byte-identical, so a later reader must not add a balance
      assertion here — there are no legs to balance.
  maintenance-downtime-report     read-only aggregate; mirrors stored
      downtime_record rows. No stored-row write, no ledger effect (same
      reason as above — nothing to balance).
  maintenance-pm-compliance-report read-only aggregate; mirrors stored
      maintenance_plan rows. No stored-row write, no ledger effect (same
      reason as above — nothing to balance).
  maintenance-spare-parts-usage   read-only aggregate; mirrors stored
      maintenance_work_order_item rows. No stored-row write, no ledger
      effect (same reason as above — nothing to balance).

Conventions:
  - Reads go back through PyPika (erpclaw_lib.query). No catalog-table
    probes, no pragma statements, no schema-dictionary queries below.
  - Money compares exact Decimal strings; never float, never approximate,
    never round. (compliance_pct is a percentage, not money.)
  - Every negative case asserts the truthful outcome AND a byte-identical DB
    snapshot. A refusal that half-writes is worse than no refusal.
"""
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from maintenance_helpers import (
    call_action, ns, is_ok, is_error, build_env, _uuid,
)
from erpclaw_lib.query import Q, P, Table


SNAPSHOT_TABLES = (
    "equipment",
    "equipment_reading",
    "maintenance_plan",
    "maintenance_plan_item",
    "maintenance_work_order",
    "maintenance_work_order_item",
    "maintenance_checklist",
    "maintenance_checklist_item",
    "downtime_record",
    "company",
    "audit_log",
    "naming_series",
    "gl_entry",
    "journal_entry",
)


def _snapshot(conn):
    snap = {}
    for name in SNAPSHOT_TABLES:
        t = Table(name)
        rows = conn.execute(Q.from_(t).select(t.star).get_sql()).fetchall()
        snap[name] = sorted(repr(dict(r)) for r in rows)
    return snap


def _read(conn, table, row_id):
    t = Table(table)
    return conn.execute(
        Q.from_(t).select(t.star).where(t.id == P()).get_sql(),
        (row_id,)).fetchone()


def _add_eq(conn, mod, company_id, name):
    r = call_action(mod.maintenance_add_equipment, conn, ns(
        name=name,
        company_id=company_id,
        equipment_type="machine",
        model="Model X",
        manufacturer="Acme Mfg",
        serial_number=None,
        location="Bay 1",
        parent_equipment_id=None,
        asset_id=None,
        item_id=None,
        purchase_date="2024-01-15",
        warranty_expiry="2027-01-15",
        criticality="medium",
        equipment_status="operational",
        notes=None,
    ))
    assert is_ok(r), r
    return r


def _add_wo(conn, mod, company_id, eq_id):
    r = call_action(mod.maintenance_add_maintenance_work_order, conn, ns(
        equipment_id=eq_id,
        company_id=company_id,
        plan_id=None,
        work_order_type="corrective",
        priority="medium",
        description="Replace worn bearing",
        assigned_to="Tech B",
        scheduled_date="2025-07-01",
        failure_mode=None,
        wo_status=None,
    ))
    assert is_ok(r), r
    return r


def _start(conn, mod, wo_id):
    r = call_action(mod.maintenance_start_maintenance_work_order, conn, ns(
        work_order_id=wo_id,
    ))
    assert is_ok(r), r
    return r


def _complete(conn, mod, wo_id, actual_cost):
    r = call_action(mod.maintenance_complete_maintenance_work_order, conn, ns(
        work_order_id=wo_id,
        actual_cost=actual_cost,
        actual_duration=None,
        resolution=None,
        root_cause=None,
    ))
    assert is_ok(r), r
    return r


def _add_dt(conn, mod, company_id, eq_id, start, hours, reason="breakdown"):
    r = call_action(mod.maintenance_add_downtime_record, conn, ns(
        equipment_id=eq_id,
        company_id=company_id,
        work_order_id=None,
        start_time=start,
        end_time=None,
        duration_hours=hours,
        reason=reason,
        description="probe",
        impact=None,
    ))
    assert is_ok(r), r
    return r


def _add_item(conn, mod, company_id, wo_id, name, qty, unit_cost):
    r = call_action(mod.maintenance_add_wo_item, conn, ns(
        work_order_id=wo_id,
        item_name=name,
        company_id=company_id,
        item_id=None,
        quantity=qty,
        unit_cost=unit_cost,
        notes=None,
    ))
    assert is_ok(r), r
    return r


def _add_plan(conn, mod, company_id, eq_id, name, next_due):
    r = call_action(mod.maintenance_add_maintenance_plan, conn, ns(
        plan_name=name,
        equipment_id=eq_id,
        company_id=company_id,
        plan_type="preventive",
        frequency="monthly",
        frequency_days=None,
        last_performed=None,
        next_due=next_due,
        estimated_duration="2h",
        estimated_cost="150.00",
        assigned_to="Technician A",
        instructions="Lubricate all moving parts",
        is_active=None,
        item_id=None,
    ))
    assert is_ok(r), r
    return r


# ===========================================================================
# maintenance-cost-report — read-only aggregate over maintenance_work_order.
# No ledger effect (see module docstring): nothing is posted, so there are
# no legs to balance; the snapshot assertions below prove it.
# ===========================================================================
class TestCostReportBehaviour:

    def test_aggregates_completed_costs_and_ignores_open_orders(self, conn, env, mod):
        cid = env["company_id"]
        eq_a = _add_eq(conn, mod, cid, "Cost Mill A")["id"]
        eq_b = _add_eq(conn, mod, cid, "Cost Mill B")["id"]
        wa1 = _add_wo(conn, mod, cid, eq_a)["id"]
        wa2 = _add_wo(conn, mod, cid, eq_a)["id"]
        wb1 = _add_wo(conn, mod, cid, eq_b)["id"]
        wb2 = _add_wo(conn, mod, cid, eq_b)["id"]
        for wid in (wa1, wa2, wb1, wb2):
            _start(conn, mod, wid)
        _complete(conn, mod, wa1, "100.00")
        _complete(conn, mod, wa2, "250.50")
        _complete(conn, mod, wb1, "50.00")
        # wb2 stays in_progress: open orders must not feed the report.

        for wid, cost, status in (
                (wa1, "100.00", "completed"), (wa2, "250.50", "completed"),
                (wb1, "50.00", "completed"), (wb2, None, "in_progress")):
            row = _read(conn, "maintenance_work_order", wid)
            assert row["status"] == status
            if cost is not None:
                assert row["actual_cost"] == cost
                assert Decimal(row["actual_cost"]) == Decimal(cost)

        snap_before = _snapshot(conn)
        r = call_action(mod.maintenance_cost_report, conn, ns(
            company_id=cid,
            equipment_id=None,
            from_date=None,
            to_date=None,
        ))
        assert is_ok(r), r
        assert len(r["items"]) == 2
        by_eq = {it["equipment_id"]: it for it in r["items"]}
        assert by_eq[eq_a]["equipment_name"] == "Cost Mill A"
        assert by_eq[eq_a]["work_order_count"] == 2
        assert by_eq[eq_a]["total_cost"] == "350.50"
        assert Decimal(by_eq[eq_a]["total_cost"]) == Decimal("350.50")
        assert by_eq[eq_b]["equipment_name"] == "Cost Mill B"
        assert by_eq[eq_b]["work_order_count"] == 1
        assert by_eq[eq_b]["total_cost"] == "50.00"
        assert Decimal(by_eq[eq_b]["total_cost"]) == Decimal("50.00")
        assert r["grand_total"] == "400.50"
        assert Decimal(r["grand_total"]) == Decimal("400.50")
        # Read-only proof: the underlying rows are untouched afterwards.
        assert _read(conn, "maintenance_work_order", wa1)["actual_cost"] == "100.00"
        assert _read(conn, "maintenance_work_order", wb2)["status"] == "in_progress"
        assert _snapshot(conn) == snap_before

    def test_excludes_other_company_orders(self, conn, env, mod):
        cid = env["company_id"]
        eq_a = _add_eq(conn, mod, cid, "Cost Mill A")["id"]
        wa = _add_wo(conn, mod, cid, eq_a)["id"]
        _start(conn, mod, wa)
        _complete(conn, mod, wa, "400.50")

        env2 = build_env(conn)
        eq_other = _add_eq(conn, mod, env2["company_id"], "Other Mill")["id"]
        wo_other = _add_wo(conn, mod, env2["company_id"], eq_other)["id"]
        _start(conn, mod, wo_other)
        _complete(conn, mod, wo_other, "999.99")

        snap_before = _snapshot(conn)
        r = call_action(mod.maintenance_cost_report, conn, ns(
            company_id=cid,
            equipment_id=None,
            from_date=None,
            to_date=None,
        ))
        assert is_ok(r), r
        assert r["grand_total"] == "400.50"
        assert Decimal(r["grand_total"]) == Decimal("400.50")
        assert [it["equipment_id"] for it in r["items"]] == [eq_a]
        r2 = call_action(mod.maintenance_cost_report, conn, ns(
            company_id=env2["company_id"],
            equipment_id=None,
            from_date=None,
            to_date=None,
        ))
        assert is_ok(r2), r2
        assert r2["grand_total"] == "999.99"
        assert Decimal(r2["grand_total"]) == Decimal("999.99")
        assert _snapshot(conn) == snap_before

    def test_unknown_equipment_returns_truthful_empty_and_writes_nothing(
            self, conn, env, mod):
        cid = env["company_id"]
        eq_a = _add_eq(conn, mod, cid, "Cost Mill A")["id"]
        wa = _add_wo(conn, mod, cid, eq_a)["id"]
        _start(conn, mod, wa)
        _complete(conn, mod, wa, "10.00")
        # This action has no input validation and therefore no refusal path;
        # an unknown filter must return a truthful empty, never an error.
        snap_before = _snapshot(conn)
        r = call_action(mod.maintenance_cost_report, conn, ns(
            company_id=cid,
            equipment_id=_uuid(),
            from_date=None,
            to_date=None,
        ))
        assert is_ok(r), r
        assert r["items"] == []
        assert r["grand_total"] == "0"
        assert _snapshot(conn) == snap_before

    def test_to_date_same_day_exclusion_documents_defect(self, conn, env, mod):
        # DOCUMENTED DEFECT, deliberately not fixed (this task buys the
        # signal; acting on it is the next task): completed_at is stored as
        # an ISO datetime ("2026-09-18T16:32:41...") while to_date arrives as
        # a bare date ("2026-09-18"), so the string comparison
        # completed_at <= to_date is False for same-day completions and the
        # report wrongly excludes them. Correct behaviour would include the
        # same-day order (items == 1, grand_total == "100.00"); the assertions
        # below pin the real behaviour instead.
        cid = env["company_id"]
        eq_a = _add_eq(conn, mod, cid, "Cost Mill A")["id"]
        wa = _add_wo(conn, mod, cid, eq_a)["id"]
        _start(conn, mod, wa)
        _complete(conn, mod, wa, "100.00")
        today = _read(conn, "maintenance_work_order", wa)["completed_at"][:10]
        snap_before = _snapshot(conn)
        r = call_action(mod.maintenance_cost_report, conn, ns(
            company_id=cid,
            equipment_id=None,
            from_date=None,
            to_date=today,
        ))
        assert is_ok(r), r
        assert r["items"] == []
        assert r["grand_total"] == "0"
        assert _snapshot(conn) == snap_before


# ===========================================================================
# maintenance-downtime-report — read-only aggregate over downtime_record.
# No ledger effect (see module docstring): nothing is posted, so there are
# no legs to balance; the snapshot assertions below prove it.
# ===========================================================================
class TestDowntimeReportBehaviour:

    def test_aggregates_hours_per_equipment(self, conn, env, mod):
        cid = env["company_id"]
        eq_a = _add_eq(conn, mod, cid, "Down Mill A")["id"]
        eq_b = _add_eq(conn, mod, cid, "Down Mill B")["id"]
        _add_dt(conn, mod, cid, eq_a, "2025-03-01T08:00:00", "2.50")
        _add_dt(conn, mod, cid, eq_a, "2025-03-02T08:00:00", "1.00")
        _add_dt(conn, mod, cid, eq_b, "2025-03-01T09:00:00", "4.25")

        dt = Table("downtime_record")
        stored = conn.execute(
            Q.from_(dt).select(dt.equipment_id, dt.duration_hours).get_sql()
        ).fetchall()
        assert sorted(
            (s["equipment_id"], s["duration_hours"]) for s in stored
        ) == sorted([
            (eq_a, "2.50"), (eq_a, "1.00"), (eq_b, "4.25"),
        ])

        snap_before = _snapshot(conn)
        r = call_action(mod.maintenance_downtime_report, conn, ns(
            company_id=cid,
            from_date=None,
            to_date=None,
        ))
        assert is_ok(r), r
        assert len(r["items"]) == 2
        by_eq = {it["equipment_id"]: it for it in r["items"]}
        assert by_eq[eq_a]["equipment_name"] == "Down Mill A"
        assert by_eq[eq_a]["incident_count"] == 2
        assert by_eq[eq_a]["total_hours"] == "3.50"
        assert Decimal(by_eq[eq_a]["total_hours"]) == Decimal("3.50")
        assert by_eq[eq_b]["incident_count"] == 1
        assert by_eq[eq_b]["total_hours"] == "4.25"
        assert Decimal(by_eq[eq_b]["total_hours"]) == Decimal("4.25")
        assert r["grand_total_hours"] == "7.75"
        assert Decimal(r["grand_total_hours"]) == Decimal("7.75")
        assert _snapshot(conn) == snap_before

    def test_unknown_company_returns_truthful_empty_and_writes_nothing(
            self, conn, env, mod):
        cid = env["company_id"]
        eq_a = _add_eq(conn, mod, cid, "Down Mill A")["id"]
        _add_dt(conn, mod, cid, eq_a, "2025-03-01T08:00:00", "2.00")
        # This action has no input validation and therefore no refusal path;
        # an unknown filter must return a truthful empty, never an error.
        snap_before = _snapshot(conn)
        r = call_action(mod.maintenance_downtime_report, conn, ns(
            company_id=_uuid(),
            from_date=None,
            to_date=None,
        ))
        assert is_ok(r), r
        assert r["items"] == []
        assert r["grand_total_hours"] == "0"
        assert _snapshot(conn) == snap_before

    def test_to_date_same_day_exclusion_documents_defect(self, conn, env, mod):
        # DOCUMENTED DEFECT, deliberately not fixed (same root cause as the
        # cost report above): start_time is stored as an ISO datetime while
        # to_date arrives as a bare date, so the string comparison
        # start_time <= to_date is False for same-day records. Correct
        # behaviour would include the record (grand_total_hours == "2.00");
        # the assertions below pin the real behaviour instead.
        cid = env["company_id"]
        eq_a = _add_eq(conn, mod, cid, "Down Mill A")["id"]
        _add_dt(conn, mod, cid, eq_a, "2025-06-15T08:00:00", "2.00")
        snap_before = _snapshot(conn)
        r = call_action(mod.maintenance_downtime_report, conn, ns(
            company_id=cid,
            from_date=None,
            to_date="2025-06-15",
        ))
        assert is_ok(r), r
        assert r["items"] == []
        assert r["grand_total_hours"] == "0"
        assert _snapshot(conn) == snap_before


# ===========================================================================
# maintenance-pm-compliance-report — read-only aggregate over
# maintenance_plan. No ledger effect (see module docstring): nothing is
# posted, so there are no legs to balance.
# ===========================================================================
class TestPMComplianceReportBehaviour:

    def test_counts_overdue_on_time_and_unscheduled_exactly(
            self, conn, env, mod):
        cid = env["company_id"]
        eq_a = _add_eq(conn, mod, cid, "PM Mill A")["id"]
        overdue_id = _add_plan(
            conn, mod, cid, eq_a, "Past Lube", "2020-01-01")["id"]
        _add_plan(conn, mod, cid, eq_a, "Future Lube", "2999-01-01")
        _add_plan(conn, mod, cid, eq_a, "Unscheduled Lube", None)

        plans = conn.execute(
            Q.from_(Table("maintenance_plan")).select(
                Table("maintenance_plan").star).get_sql()
        ).fetchall()
        assert len(plans) == 3

        snap_before = _snapshot(conn)
        r = call_action(mod.maintenance_pm_compliance_report, conn, ns(
            company_id=cid,
        ))
        assert is_ok(r), r
        assert r["total_plans"] == 3
        assert r["on_time"] == 1
        assert r["overdue"] == 1
        assert r["no_schedule"] == 1
        # compliance_pct is a percentage, not money: exact float compare.
        assert r["compliance_pct"] == 33.3
        assert len(r["overdue_plans"]) == 1
        entry = r["overdue_plans"][0]
        assert entry["plan_id"] == overdue_id
        assert entry["plan_name"] == "Past Lube"
        assert entry["equipment_name"] == "PM Mill A"
        assert entry["next_due"] == "2020-01-01"
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        expected_days = (
            datetime.strptime(today, "%Y-%m-%d")
            - datetime.strptime("2020-01-01", "%Y-%m-%d")
        ).days
        assert entry["days_overdue"] == expected_days
        assert _snapshot(conn) == snap_before

    def test_refuses_missing_company_id_and_writes_nothing(
            self, conn, env, mod):
        cid = env["company_id"]
        eq_a = _add_eq(conn, mod, cid, "PM Mill A")["id"]
        _add_plan(conn, mod, cid, eq_a, "Past Lube", "2020-01-01")
        snap_before = _snapshot(conn)
        r = call_action(mod.maintenance_pm_compliance_report, conn, ns(
            company_id=None,
        ))
        assert is_error(r), r
        assert r["message"] == "--company-id is required"
        assert _snapshot(conn) == snap_before


# ===========================================================================
# maintenance-spare-parts-usage — read-only aggregate over
# maintenance_work_order_item. No ledger effect (see module docstring):
# nothing is posted, so there are no legs to balance.
# ===========================================================================
class TestSparePartsUsageBehaviour:

    def test_aggregates_quantity_and_cost_across_orders(self, conn, env, mod):
        cid = env["company_id"]
        eq_a = _add_eq(conn, mod, cid, "Spares Mill A")["id"]
        wo1 = _add_wo(conn, mod, cid, eq_a)["id"]
        wo2 = _add_wo(conn, mod, cid, eq_a)["id"]
        _add_item(conn, mod, cid, wo1, "Bearing X", "2", "25.00")
        _add_item(conn, mod, cid, wo1, "Bearing X", "3", "25.00")
        _add_item(conn, mod, cid, wo2, "Bearing X", "1", "25.00")
        _add_item(conn, mod, cid, wo2, "Seal Y", "4", "1.50")

        woi = Table("maintenance_work_order_item")
        stored = conn.execute(
            Q.from_(woi).select(
                woi.item_name, woi.quantity, woi.total_cost,
                woi.work_order_id).get_sql()
        ).fetchall()
        assert len(stored) == 4

        snap_before = _snapshot(conn)
        r = call_action(mod.maintenance_spare_parts_usage, conn, ns(
            company_id=cid,
            limit=20,
        ))
        assert is_ok(r), r
        assert r["total_parts"] == 2
        by_name = {it["item_name"]: it for it in r["items"]}
        assert by_name["Bearing X"]["total_quantity"] == "6.00"
        assert Decimal(by_name["Bearing X"]["total_quantity"]) == Decimal("6.00")
        assert by_name["Bearing X"]["total_cost"] == "150.00"
        assert Decimal(by_name["Bearing X"]["total_cost"]) == Decimal("150.00")
        assert by_name["Bearing X"]["used_in_orders"] == 2
        assert by_name["Seal Y"]["total_quantity"] == "4.00"
        assert Decimal(by_name["Seal Y"]["total_quantity"]) == Decimal("4.00")
        assert by_name["Seal Y"]["total_cost"] == "6.00"
        assert Decimal(by_name["Seal Y"]["total_cost"]) == Decimal("6.00")
        assert by_name["Seal Y"]["used_in_orders"] == 1
        assert _snapshot(conn) == snap_before

    def test_unknown_company_returns_truthful_empty_and_writes_nothing(
            self, conn, env, mod):
        cid = env["company_id"]
        eq_a = _add_eq(conn, mod, cid, "Spares Mill A")["id"]
        wo1 = _add_wo(conn, mod, cid, eq_a)["id"]
        _add_item(conn, mod, cid, wo1, "Bearing X", "2", "25.00")
        # This action has no input validation and therefore no refusal path;
        # an unknown filter must return a truthful empty, never an error.
        snap_before = _snapshot(conn)
        r = call_action(mod.maintenance_spare_parts_usage, conn, ns(
            company_id=_uuid(),
            limit=20,
        ))
        assert is_ok(r), r
        assert r["items"] == []
        assert r["total_parts"] == 0
        assert _snapshot(conn) == snap_before

    def test_limit_zero_returns_all_documents_defect(self, conn, env, mod):
        # DOCUMENTED DEFECT, deliberately not fixed: the action resolves the
        # limit with `getattr(args, "limit", None) or 20`, so an explicit
        # limit=0 is falsy and silently replaced by the default 20. Correct
        # behaviour would return no rows (total_parts == 0); the assertions
        # below pin the real behaviour instead.
        cid = env["company_id"]
        eq_a = _add_eq(conn, mod, cid, "Spares Mill A")["id"]
        wo1 = _add_wo(conn, mod, cid, eq_a)["id"]
        _add_item(conn, mod, cid, wo1, "Bearing X", "2", "25.00")
        _add_item(conn, mod, cid, wo1, "Seal Y", "1", "2.00")
        snap_before = _snapshot(conn)
        r = call_action(mod.maintenance_spare_parts_usage, conn, ns(
            company_id=cid,
            limit=0,
        ))
        assert is_ok(r), r
        assert r["total_parts"] == 2
        assert sorted(it["item_name"] for it in r["items"]) == [
            "Bearing X", "Seal Y",
        ]
        assert _snapshot(conn) == snap_before
