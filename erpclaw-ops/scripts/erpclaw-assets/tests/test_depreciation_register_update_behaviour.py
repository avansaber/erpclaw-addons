"""Behaviour tests for depreciation-summary, asset-register-report and update-asset.

One small, fully known fixed-asset book is posted through the module's own
actions: two categories with their own ledger accounts, three depreciating
assets, one draft asset, a batch depreciation run, a single depreciation
posting, and a second company. Every figure the two reports return is asserted
as an exact string, against the depreciation_schedule, asset and gl_entry rows
read back from the database. update-asset is pinned by the row it stores, by
the fields it refuses to touch, and by the statuses only a posting action may
set.

All dates are fixed. The naming series embeds the current year, so it is
compared with what add-asset returned, never written literally.
"""
import json
import os
import sys
from decimal import Decimal

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from assets_helpers import (call_action, is_error, is_ok, load_db_query, ns,  # noqa: E402
                           seed_account, seed_company, seed_cost_center,
                           seed_fiscal_year, seed_naming_series)

M = load_db_query()


# ── helpers ──────────────────────────────────────────────────────────────────

def _ok(result):
    assert is_ok(result), result
    return result


def _company(conn, name, abbr):
    cid = seed_company(conn, name, abbr)
    seed_naming_series(conn, cid)
    seed_fiscal_year(conn, cid)
    cost_center_id = seed_cost_center(conn, cid)
    return cid, cost_center_id


def _category(conn, cid, name, life):
    accounts = {
        "asset": seed_account(conn, cid, f"{name} at cost", "asset", "asset"),
        "expense": seed_account(conn, cid, f"{name} depreciation", "expense", "expense"),
        "accum": seed_account(conn, cid, f"{name} accumulated depreciation", "asset", "asset"),
    }
    r = _ok(call_action(M.add_asset_category, conn, ns(
        company_id=cid, name=name, depreciation_method="straight_line",
        useful_life_years=str(life), asset_account_id=accounts["asset"],
        depreciation_account_id=accounts["expense"],
        accumulated_depreciation_account_id=accounts["accum"])))
    accounts["id"] = r["asset_category_id"]
    return accounts


def _asset(conn, cid, category_id, name, gross, *, salvage=None, life=None,
           start=None, status=None, schedule=True, **extra):
    r = _ok(call_action(M.add_asset, conn, ns(
        company_id=cid, name=name, asset_category_id=category_id,
        gross_value=gross, salvage_value=salvage, useful_life_years=life,
        depreciation_start_date=start, **extra)))
    if status:
        _ok(call_action(M.update_asset, conn, ns(asset_id=r["asset_id"], status=status)))
    if schedule:
        _ok(call_action(M.generate_depreciation_schedule, conn, ns(asset_id=r["asset_id"])))
    return r["asset_id"], r["naming_series"]


def _asset_row(conn, asset_id):
    r = conn.execute(
        "SELECT asset_name, status, gross_value, salvage_value, depreciation_method, "
        "useful_life_years, depreciation_start_date, accumulated_depreciation, "
        "current_book_value, location, custodian_employee_id, warranty_expiry_date "
        "FROM asset WHERE id = ?", (asset_id,)).fetchone()
    return dict(r)


def _schedule(conn, asset_id):
    return [tuple(r) for r in conn.execute(
        "SELECT id, schedule_date, depreciation_amount, status, journal_entry_id "
        "FROM depreciation_schedule WHERE asset_id = ? ORDER BY schedule_date",
        (asset_id,)).fetchall()]


_COUNT_SQL = {
    "gl_entry": "SELECT COUNT(*) FROM gl_entry",
    "audit_log": "SELECT COUNT(*) FROM audit_log",
    "depreciation_schedule": "SELECT COUNT(*) FROM depreciation_schedule",
    "asset": "SELECT COUNT(*) FROM asset",
    "asset_disposal": "SELECT COUNT(*) FROM asset_disposal",
    "asset_impairment": "SELECT COUNT(*) FROM asset_impairment",
}


def _counts(conn):
    return {t: conn.execute(sql).fetchone()[0] for t, sql in _COUNT_SQL.items()}


@pytest.fixture
def book(conn):
    """Company A (Machinery + Vehicles) and company B (Trucks), posted through
    the module's actions.

    Machinery, 5 years straight line:
      M1 Lathe       5000.00, starts 2026-01-01, in_use    -> 83.33 a month
      M2 Drill Press 1830.00, salvage 30.00, 3 years, starts 2026-02-01,
                     submitted                              -> 50.00 a month
      SP Spare Motor  700.00, draft, no schedule
    Vehicles, 4 years straight line:
      V1 Van         9650.00, salvage 50.00, starts 2026-01-15, in_use -> 200.00 a month
    Company B, Trucks (category names are unique across companies):
      T1 Truck       2400.00, 1 year, starts 2026-01-01, in_use -> 200.00 a month

    run-depreciation for A on 2026-02-28 posts M1 Jan+Feb, M2 Feb, V1 Jan+Feb.
    post-depreciation M1 on 2026-03-31 posts M1 Mar.
    Left pending inside the first quarter: M2 2026-03-01, V1 2026-03-15.
    run-depreciation for B on 2026-01-31 posts T1 Jan.
    """
    a, a_cc = _company(conn, "Northwind Fabrication", "NWF")
    mach = _category(conn, a, "Machinery", 5)
    veh = _category(conn, a, "Vehicles", 4)
    m1, m1_ns = _asset(conn, a, mach["id"], "Lathe", "5000.00", start="2026-01-01",
                       status="in_use", purchase_date="2025-12-20", location="Bay 1")
    m2, m2_ns = _asset(conn, a, mach["id"], "Drill Press", "1830.00", salvage="30.00",
                       life="3", start="2026-02-01", status="submitted")
    v1, v1_ns = _asset(conn, a, veh["id"], "Van", "9650.00", salvage="50.00",
                       start="2026-01-15", status="in_use")
    sp, sp_ns = _asset(conn, a, mach["id"], "Spare Motor", "700.00", schedule=False)

    run = _ok(call_action(M.run_depreciation, conn, ns(
        company_id=a, posting_date="2026-02-28")))
    assert run["entries_posted"] == 5
    single = _ok(call_action(M.post_depreciation, conn, ns(
        asset_id=m1, posting_date="2026-03-31")))
    assert single["depreciation_amount"] == "83.33"

    b, b_cc = _company(conn, "Harbor Haulage", "HBH")
    b_veh = _category(conn, b, "Trucks", 4)
    t1, t1_ns = _asset(conn, b, b_veh["id"], "Truck", "2400.00", life="1",
                       start="2026-01-01", status="in_use")
    _ok(call_action(M.run_depreciation, conn, ns(company_id=b, posting_date="2026-01-31")))

    return {
        "a": a, "a_cc": a_cc, "mach": mach, "veh": veh,
        "m1": m1, "m1_ns": m1_ns, "m2": m2, "m2_ns": m2_ns,
        "v1": v1, "v1_ns": v1_ns, "sp": sp, "sp_ns": sp_ns,
        "b": b, "b_cc": b_cc, "b_veh": b_veh, "t1": t1, "t1_ns": t1_ns,
    }


# ── the depreciation postings the reports read ───────────────────────────────

def test_depreciation_postings_write_one_balanced_pair_per_schedule_row(conn, book):
    m1 = _schedule(conn, book["m1"])
    m2 = _schedule(conn, book["m2"])
    v1 = _schedule(conn, book["v1"])
    assert (len(m1), len(m2), len(v1)) == (60, 36, 48)
    assert _schedule(conn, book["sp"]) == []

    posted = {
        m1[0][0]: (book["mach"], "2026-01-01", "83.33", "2026-02-28"),
        m1[1][0]: (book["mach"], "2026-02-01", "83.33", "2026-02-28"),
        m1[2][0]: (book["mach"], "2026-03-01", "83.33", "2026-03-31"),
        m2[0][0]: (book["mach"], "2026-02-01", "50.00", "2026-02-28"),
        v1[0][0]: (book["veh"], "2026-01-15", "200.00", "2026-02-28"),
        v1[1][0]: (book["veh"], "2026-02-15", "200.00", "2026-02-28"),
    }
    # Schedule rows: exactly these are posted, each pointing at its own voucher.
    for rows, n_posted in ((m1, 3), (m2, 1), (v1, 2)):
        for sid, sdate, amount, status, je in rows[:n_posted]:
            assert sid in posted
            assert (sdate, amount, status, je) == (posted[sid][1], posted[sid][2], "posted", sid)
        assert {r[3] for r in rows[n_posted:]} == {"pending"}
        assert {r[4] for r in rows[n_posted:]} == {None}
    assert m1[59][1:3] == ("2030-12-01", "83.53")
    assert (m2[1][1:4], v1[2][1:4]) == (("2026-03-01", "50.00", "pending"),
                                        ("2026-03-15", "200.00", "pending"))

    # Every ledger leg company A holds, by voucher and account.
    fy = f"FY2026-{book['a'][:6]}"
    legs = conn.execute(
        "SELECT voucher_type, voucher_id, account_id, debit, credit, posting_date, "
        "fiscal_year, cost_center_id, is_cancelled FROM gl_entry g "
        "JOIN account ac ON ac.id = g.account_id WHERE ac.company_id = ?",
        (book["a"],)).fetchall()
    expected = set()
    for sid, (cat, _sdate, amount, pdate) in posted.items():
        expected.add(("depreciation_entry", sid, cat["expense"], amount, "0.00", pdate,
                      fy, book["a_cc"], 0))
        expected.add(("depreciation_entry", sid, cat["accum"], "0.00", amount, pdate,
                      fy, book["a_cc"], 0))
    assert len(legs) == 12
    assert {tuple(r) for r in legs} == expected

    total_debit = sum((Decimal(r["debit"]) for r in legs), Decimal("0"))
    total_credit = sum((Decimal(r["credit"]) for r in legs), Decimal("0"))
    assert (str(total_debit), str(total_credit)) == ("699.99", "699.99")

    def net_credit(account_id):
        return str(sum((Decimal(r["credit"]) - Decimal(r["debit"])
                        for r in legs if r["account_id"] == account_id), Decimal("0")))

    assert net_credit(book["mach"]["accum"]) == "299.99"
    assert net_credit(book["veh"]["accum"]) == "400.00"
    assert net_credit(book["mach"]["expense"]) == "-299.99"
    assert net_credit(book["veh"]["expense"]) == "-400.00"

    # The asset rows carry the same accumulation, and gross = accumulated + book.
    carried = {k: _asset_row(conn, book[k]) for k in ("m1", "m2", "v1", "sp")}
    assert {k: (r["status"], r["gross_value"], r["accumulated_depreciation"],
                r["current_book_value"]) for k, r in carried.items()} == {
        "m1": ("in_use", "5000.00", "249.99", "4750.01"),
        "m2": ("submitted", "1830.00", "50.00", "1780.00"),
        "v1": ("in_use", "9650.00", "400.00", "9250.00"),
        "sp": ("draft", "700.00", "0", "700.00"),
    }


# ── depreciation-summary ─────────────────────────────────────────────────────

def _summary(conn, company_id, **kw):
    return _ok(call_action(M.depreciation_summary, conn, ns(company_id=company_id, **kw)))


def test_depreciation_summary_totals_by_category_and_asset(conn, book):
    r = _summary(conn, book["a"])
    assert (r["report"], r["company_id"], r["from_date"], r["to_date"]) == (
        "Depreciation Summary", book["a"], None, None)
    assert r["categories"] == [
        {"category_id": book["mach"]["id"], "category_name": "Machinery",
         "total_depreciation": "299.99",
         "assets": [
             {"asset_id": book["m1"], "naming_series": book["m1_ns"], "asset_name": "Lathe",
              "total_depreciation": "249.99", "entries_count": 3},
             {"asset_id": book["m2"], "naming_series": book["m2_ns"],
              "asset_name": "Drill Press", "total_depreciation": "50.00", "entries_count": 1},
         ]},
        {"category_id": book["veh"]["id"], "category_name": "Vehicles",
         "total_depreciation": "400.00",
         "assets": [
             {"asset_id": book["v1"], "naming_series": book["v1_ns"], "asset_name": "Van",
              "total_depreciation": "400.00", "entries_count": 2},
         ]},
    ]
    assert r["grand_total_depreciation"] == "699.99"

    # Each asset's total is its carried accumulation, and the category totals
    # are what the category's accumulated-depreciation account was credited.
    for cat in r["categories"]:
        for line in cat["assets"]:
            assert line["total_depreciation"] == \
                _asset_row(conn, line["asset_id"])["accumulated_depreciation"]
    for cat_key, cat in (("mach", r["categories"][0]), ("veh", r["categories"][1])):
        credited = conn.execute(
            "SELECT credit FROM gl_entry WHERE account_id = ? AND is_cancelled = 0",
            (book[cat_key]["accum"],)).fetchall()
        assert str(sum((Decimal(c["credit"]) for c in credited), Decimal("0"))) == \
            cat["total_depreciation"]

    # Company B's book is its own.
    rb = _summary(conn, book["b"])
    assert rb["categories"] == [
        {"category_id": book["b_veh"]["id"], "category_name": "Trucks",
         "total_depreciation": "200.00",
         "assets": [{"asset_id": book["t1"], "naming_series": book["t1_ns"],
                     "asset_name": "Truck", "total_depreciation": "200.00",
                     "entries_count": 1}]},
    ]
    assert rb["grand_total_depreciation"] == "200.00"


def test_depreciation_summary_window_is_inclusive_and_skips_unposted_rows(conn, book):
    def compact(r):
        return ([(c["category_name"], c["total_depreciation"],
                  [(a["asset_name"], a["total_depreciation"], a["entries_count"])
                   for a in c["assets"]]) for c in r["categories"]],
                r["grand_total_depreciation"])

    # Both bounds are inclusive: the 2026-02-01 rows and the 2026-02-15 row count.
    feb = _summary(conn, book["a"], from_date="2026-02-01", to_date="2026-02-15")
    assert (feb["from_date"], feb["to_date"]) == ("2026-02-01", "2026-02-15")
    assert compact(feb) == (
        [("Machinery", "133.33", [("Lathe", "83.33", 1), ("Drill Press", "50.00", 1)]),
         ("Vehicles", "200.00", [("Van", "200.00", 1)])],
        "333.33")

    # The first quarter holds M2's 2026-03-01 and V1's 2026-03-15 rows, still
    # pending: they are not depreciation until they are posted.
    q1 = _summary(conn, book["a"], from_date="2026-01-01", to_date="2026-03-31")
    assert compact(q1) == (
        [("Machinery", "299.99", [("Lathe", "249.99", 3), ("Drill Press", "50.00", 1)]),
         ("Vehicles", "400.00", [("Van", "400.00", 2)])],
        "699.99")

    march_on = _summary(conn, book["a"], from_date="2026-03-01")
    assert compact(march_on) == ([("Machinery", "83.33", [("Lathe", "83.33", 1)])], "83.33")

    before_van = _summary(conn, book["a"], to_date="2026-01-14")
    assert compact(before_van) == ([("Machinery", "83.33", [("Lathe", "83.33", 1)])], "83.33")

    nothing = _summary(conn, book["a"], from_date="2026-06-01", to_date="2026-06-30")
    assert compact(nothing) == ([], "0")

    # Company B's 2026-01-01 row is in the window but not in company A's report.
    jan = _summary(conn, book["a"], from_date="2026-01-01", to_date="2026-01-31")
    assert compact(jan) == (
        [("Machinery", "83.33", [("Lathe", "83.33", 1)]),
         ("Vehicles", "200.00", [("Van", "200.00", 1)])],
        "283.33")


def test_depreciation_summary_refusals_write_nothing(conn, book):
    before = _counts(conn)
    r = call_action(M.depreciation_summary, conn, ns(company_id=None))
    assert is_error(r) and r["message"] == "--company-id is required"
    r = call_action(M.depreciation_summary, conn, ns(company_id="no-such-company"))
    assert is_error(r) and r["message"] == "Company no-such-company not found"
    assert _counts(conn) == before


# ── asset-register-report ────────────────────────────────────────────────────

def _register(conn, company_id, as_of):
    return _ok(call_action(M.asset_register_report, conn, ns(
        company_id=company_id, as_of_date=as_of)))


def _lines(r):
    return [(a["asset_name"], a["category_name"], a["status"], a["gross_value"],
             a["accumulated_depreciation"], a["current_book_value"]) for a in r["assets"]]


def test_asset_register_values_and_totals_agree_with_the_asset_rows(conn, book):
    r = _register(conn, book["a"], "2026-12-31")
    assert (r["report"], r["company_id"], r["as_of_date"]) == (
        "Asset Register", book["a"], "2026-12-31")
    # Ordered by category name, then naming series. Pending rows scheduled
    # before the as-of date (M1 April onwards, M2 March, V1 March) do not count.
    assert _lines(r) == [
        ("Lathe", "Machinery", "in_use", "5000.00", "249.99", "4750.01"),
        ("Drill Press", "Machinery", "submitted", "1830.00", "50.00", "1780.00"),
        ("Spare Motor", "Machinery", "draft", "700.00", "0", "700.00"),
        ("Van", "Vehicles", "in_use", "9650.00", "400.00", "9250.00"),
    ]
    assert [a["asset_id"] for a in r["assets"]] == [book["m1"], book["m2"], book["sp"], book["v1"]]
    assert [a["naming_series"] for a in r["assets"]] == \
        [book["m1_ns"], book["m2_ns"], book["sp_ns"], book["v1_ns"]]
    assert (r["assets"][0]["purchase_date"], r["assets"][0]["location"]) == ("2025-12-20", "Bay 1")
    assert r["summary"] == {
        "total_assets": 4,
        "total_gross_value": "17180.00",
        "total_accumulated_depreciation": "699.99",
        "total_book_value": "16480.01",
    }

    # With every posted row inside the window, each line is the asset row.
    for line in r["assets"]:
        row = _asset_row(conn, line["asset_id"])
        assert (line["gross_value"], line["accumulated_depreciation"],
                line["current_book_value"]) == (
            row["gross_value"], row["accumulated_depreciation"], row["current_book_value"])

    rb = _register(conn, book["b"], "2026-12-31")
    assert _lines(rb) == [("Truck", "Trucks", "in_use", "2400.00", "200.00", "2200.00")]
    assert rb["summary"] == {"total_assets": 1, "total_gross_value": "2400.00",
                             "total_accumulated_depreciation": "200.00",
                             "total_book_value": "2200.00"}


def test_asset_register_as_of_date_counts_rows_scheduled_on_or_before_it(conn, book):
    r = _register(conn, book["a"], "2026-02-01")
    assert _lines(r) == [
        ("Lathe", "Machinery", "in_use", "5000.00", "166.66", "4833.34"),
        ("Drill Press", "Machinery", "submitted", "1830.00", "50.00", "1780.00"),
        ("Spare Motor", "Machinery", "draft", "700.00", "0", "700.00"),
        ("Van", "Vehicles", "in_use", "9650.00", "200.00", "9450.00"),
    ]
    assert r["summary"] == {"total_assets": 4, "total_gross_value": "17180.00",
                            "total_accumulated_depreciation": "416.66",
                            "total_book_value": "16763.34"}

    before = _register(conn, book["a"], "2025-12-31")
    assert _lines(before) == [
        ("Lathe", "Machinery", "in_use", "5000.00", "0", "5000.00"),
        ("Drill Press", "Machinery", "submitted", "1830.00", "0", "1830.00"),
        ("Spare Motor", "Machinery", "draft", "700.00", "0", "700.00"),
        ("Van", "Vehicles", "in_use", "9650.00", "0", "9650.00"),
    ]
    assert before["summary"] == {"total_assets": 4, "total_gross_value": "17180.00",
                                 "total_accumulated_depreciation": "0.00",
                                 "total_book_value": "17180.00"}


def test_asset_register_refusals_write_nothing(conn, book):
    before = _counts(conn)
    r = call_action(M.asset_register_report, conn, ns(company_id=None, as_of_date="2026-12-31"))
    assert is_error(r) and r["message"] == "--company-id is required"
    r = call_action(M.asset_register_report, conn, ns(company_id="no-such-company",
                                                      as_of_date="2026-12-31"))
    assert is_error(r) and r["message"] == "Company no-such-company not found"
    assert _counts(conn) == before


# ── update-asset ─────────────────────────────────────────────────────────────

def _audits(conn, asset_id):
    return {r["id"]: r for r in conn.execute(
        "SELECT id, skill, action, entity_type, entity_id, old_values, new_values "
        "FROM audit_log WHERE action = 'update-asset' AND entity_id = ?",
        (asset_id,)).fetchall()}


def test_update_asset_stores_the_named_fields_and_leaves_the_money_alone(conn, book):
    m2 = book["m2"]
    before = _asset_row(conn, m2)
    schedule_before = _schedule(conn, m2)
    counts = _counts(conn)
    audits_before = _audits(conn, m2)

    r = _ok(call_action(M.update_asset, conn, ns(
        asset_id=m2, name="Drill Press 2", location="Bay 4",
        custodian_employee_id="EMP-0042", warranty_expiry_date="2028-02-01")))
    assert r["updated_fields"] == ["asset_name", "location", "custodian_employee_id",
                                   "warranty_expiry_date"]

    after = _asset_row(conn, m2)
    assert (after["asset_name"], after["location"], after["custodian_employee_id"],
            after["warranty_expiry_date"]) == ("Drill Press 2", "Bay 4", "EMP-0042", "2028-02-01")
    untouched = ("status", "gross_value", "salvage_value", "depreciation_method",
                 "useful_life_years", "depreciation_start_date",
                 "accumulated_depreciation", "current_book_value")
    assert {k: after[k] for k in untouched} == {
        "status": "submitted", "gross_value": "1830.00", "salvage_value": "30.00",
        "depreciation_method": "straight_line", "useful_life_years": 3,
        "depreciation_start_date": "2026-02-01",
        "accumulated_depreciation": "50.00", "current_book_value": "1780.00"}
    assert {k: before[k] for k in untouched} == {k: after[k] for k in untouched}
    assert _schedule(conn, m2) == schedule_before

    # One audit row and nothing else written.
    new = [v for k, v in _audits(conn, m2).items() if k not in audits_before]
    assert len(new) == 1
    assert (new[0]["skill"], new[0]["entity_type"]) == ("erpclaw-assets", "asset")
    assert json.loads(new[0]["old_values"]) == {
        "asset_name": "Drill Press", "location": None, "custodian_employee_id": None,
        "warranty_expiry_date": None}
    assert json.loads(new[0]["new_values"]) == {
        "asset_name": "Drill Press 2", "location": "Bay 4",
        "custodian_employee_id": "EMP-0042", "warranty_expiry_date": "2028-02-01"}
    counts["audit_log"] += 1
    assert _counts(conn) == counts

    # The draft asset moves forward through update-asset's status field.
    _ok(call_action(M.update_asset, conn, ns(asset_id=book["sp"], status="submitted")))
    assert _asset_row(conn, book["sp"])["status"] == "submitted"


def test_update_asset_cannot_change_money_or_depreciation_terms(conn, book):
    m2 = book["m2"]
    before = _asset_row(conn, m2)
    counts = _counts(conn)

    # There is no field for gross value, salvage, method or useful life: alone
    # they are refused, and the stored row and the ledger do not move.
    r = call_action(M.update_asset, conn, ns(
        asset_id=m2, gross_value="9999.00", salvage_value="0",
        depreciation_method="double_declining", useful_life_years="10"))
    assert is_error(r)
    assert r["message"] == (
        "No fields to update. Provide at least one of: --name, --location, "
        "--custodian-employee-id, --warranty-expiry-date, --depreciation-start-date, --status")
    assert _asset_row(conn, m2) == before
    assert _counts(conn) == counts

    # Next to an updatable field they are ignored, not applied.
    _ok(call_action(M.update_asset, conn, ns(asset_id=m2, location="Bay 2",
                                             gross_value="9999.00")))
    after = _asset_row(conn, m2)
    assert (after["location"], after["gross_value"], after["accumulated_depreciation"],
            after["current_book_value"]) == ("Bay 2", "1830.00", "50.00", "1780.00")

    # An in-use asset is not editable at all.
    m1 = _asset_row(conn, book["m1"])
    counts = _counts(conn)
    r = call_action(M.update_asset, conn, ns(asset_id=book["m1"], location="Bay 9"))
    assert is_error(r)
    assert r["message"] == ("Cannot update asset in 'in_use' status. "
                            "Only draft or submitted assets can be updated.")
    assert _asset_row(conn, book["m1"]) == m1

    r = call_action(M.update_asset, conn, ns(asset_id=m2, status="retired"))
    assert is_error(r)
    assert r["message"] == ("Invalid status 'retired'. Register it in asset_status_registry "
                            "or use a standard state: draft, submitted, in_use, scrapped, sold")

    r = call_action(M.update_asset, conn, ns(asset_id=None, location="Bay 9"))
    assert is_error(r) and r["message"] == "--asset-id is required"
    assert _counts(conn) == counts


def test_update_asset_refuses_statuses_only_a_posting_action_sets(conn, book):
    m2 = book["m2"]
    before = _asset_row(conn, m2)
    counts = _counts(conn)
    for status, owner in (("scrapped", "dispose-asset"), ("sold", "dispose-asset"),
                          ("impaired", "impair-asset")):
        r = call_action(M.update_asset, conn, ns(asset_id=m2, status=status))
        assert is_error(r), (status, r)
        assert r["message"] == (f"Status '{status}' is set only by {owner}, which posts it "
                                f"to the ledger. Use {owner} instead of update-asset.")
        assert _asset_row(conn, m2) == before
    # The draft asset has no ledger yet, and is refused just the same.
    r = call_action(M.update_asset, conn, ns(asset_id=book["sp"], status="sold"))
    assert is_error(r)
    assert _asset_row(conn, book["sp"])["status"] == "draft"
    assert _counts(conn) == counts
