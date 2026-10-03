"""Posted depreciation is never scheduled again, and the register follows the book."""

import os
import sys
from decimal import Decimal

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from assets_helpers import (call_action, is_error, is_ok, load_db_query, ns,  # noqa: E402
                            seed_account, seed_company, seed_cost_center,
                            seed_disposal_accounts, seed_fiscal_year,
                            seed_naming_series)

M = load_db_query()

from erpclaw_lib.query import P, Q, Table, fn  # noqa: E402
from erpclaw_lib.vendor.pypika.terms import ValueWrapper  # noqa: E402


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
    t = Table("asset")
    q = (Q.from_(t).select(t.status, t.gross_value, t.accumulated_depreciation, t.current_book_value).where(t.id == P()))
    return dict(conn.execute(q.get_sql(), (asset_id,)).fetchone())


def _asset_dict(conn, asset_id):
    t = Table("asset")
    q = Q.from_(t).select(t.star).where(t.id == P())
    return dict(conn.execute(q.get_sql(), (asset_id,)).fetchone())


def _sched(conn, asset_id):
    t = Table("depreciation_schedule")
    q = (Q.from_(t).select(t.id, t.schedule_date, t.depreciation_amount, t.accumulated_amount, t.book_value_after, t.status, t.journal_entry_id).where(t.asset_id == P()).orderby(t.schedule_date))
    return [dict(r) for r in conn.execute(q.get_sql(), (asset_id,)).fetchall()]


def _audit_count(conn):
    t = Table("audit_log")
    q = Q.from_(t).select(fn.Count("*").as_("c"))
    return conn.execute(q.get_sql(), ()).fetchone()["c"]


def _gl_depreciation(conn):
    t = Table("gl_entry")
    q = (Q.from_(t).select(t.voucher_id, t.account_id, t.debit, t.credit, t.posting_date).where(t.voucher_type == P()))
    return [dict(r) for r in conn.execute(q.get_sql(), ("depreciation_entry",)).fetchall()]


def _register(conn, company_id, as_of):
    return _ok(call_action(M.asset_register_report, conn, ns(company_id=company_id, as_of_date=as_of)))


def _setup_press(conn):
    cid, _cc = _company(conn, "Acme Pressing", "ACP")
    mach = _category(conn, cid, "Machinery", 5)
    press, _ns = _asset(conn, cid, mach["id"], "Press", "1200.00", life="1", start="2026-01-01", status="in_use")
    return cid, mach, press


def test_regenerate_after_posting_rebases_from_book_value(conn):
    cid, mach, press = _setup_press(conn)
    run = _ok(call_action(M.run_depreciation, conn, ns(company_id=cid, posting_date="2026-02-28")))
    assert run["entries_posted"] == 2
    before = _sched(conn, press)
    posted_before = [r for r in before if r["status"] == "posted"]
    assert len(posted_before) == 2
    posted_ids = [r["id"] for r in posted_before]
    regen = _ok(call_action(M.generate_depreciation_schedule, conn, ns(asset_id=press)))
    assert regen["entries_generated"] == 10
    assert regen["posted_entries_kept"] == 2
    assert regen["rebased_from_book_value"] == "1000.00"
    sched = regen["schedule"]
    assert [r["schedule_date"] for r in sched] == ["2026-03-01", "2026-04-01", "2026-05-01", "2026-06-01", "2026-07-01", "2026-08-01", "2026-09-01", "2026-10-01", "2026-11-01", "2026-12-01"]
    assert [r["depreciation_amount"] for r in sched] == ["100.00"] * 10
    assert [r["accumulated_amount"] for r in sched] == ["300.00", "400.00", "500.00", "600.00", "700.00", "800.00", "900.00", "1000.00", "1100.00", "1200.00"]
    assert [r["book_value_after"] for r in sched] == ["900.00", "800.00", "700.00", "600.00", "500.00", "400.00", "300.00", "200.00", "100.00", "0.00"]
    after = _sched(conn, press)
    posted_after = [r for r in after if r["status"] == "posted"]
    assert [r["id"] for r in posted_after] == posted_ids
    for r in posted_after:
        assert r["status"] == "posted"
        assert r["journal_entry_id"] == r["id"]
    run2 = _ok(call_action(M.run_depreciation, conn, ns(company_id=cid, posting_date="2026-12-31")))
    assert run2["entries_posted"] == 10
    a = _asset_row(conn, press)
    assert a["accumulated_depreciation"] == "1200.00"
    assert a["current_book_value"] == "0.00"
    all_ids = {r["id"] for r in _sched(conn, press)}
    legs = [r for r in _gl_depreciation(conn) if r["voucher_id"] in all_ids]
    assert len(legs) == 24
    exp = [r for r in legs if r["account_id"] == mach["expense"]]
    acc = [r for r in legs if r["account_id"] == mach["accum"]]
    assert len(exp) == 12
    assert len(acc) == 12
    assert [r["debit"] for r in exp] == ["100.00"] * 12
    assert sorted([r["posting_date"] for r in exp]) == ["2026-02-28"] * 2 + ["2026-12-31"] * 10
    assert [r["credit"] for r in acc] == ["100.00"] * 12
    assert sorted([r["posting_date"] for r in acc]) == ["2026-02-28"] * 2 + ["2026-12-31"] * 10
    assert sum((Decimal(r["debit"]) for r in exp), Decimal("0")) == Decimal("1200.00")
    assert sum((Decimal(r["credit"]) for r in acc), Decimal("0")) == Decimal("1200.00")


def test_regenerate_before_posting_is_unchanged(conn):
    cid, mach, press = _setup_press(conn)
    first = _ok(call_action(M.generate_depreciation_schedule, conn, ns(asset_id=press)))
    assert first["entries_generated"] == 12
    assert "rebased_from_book_value" not in first
    second = _ok(call_action(M.generate_depreciation_schedule, conn, ns(asset_id=press)))
    assert second["entries_generated"] == 12
    assert "rebased_from_book_value" not in second
    rows = _sched(conn, press)
    assert len(rows) == 12
    assert {r["status"] for r in rows} == {"pending"}


def test_regenerate_refuses_a_disposed_asset(conn):
    cid, _cc = _company(conn, "Acme Bench", "ACB")
    mach = _category(conn, cid, "Machinery", 5)
    bench, _ns = _asset(conn, cid, mach["id"], "Bench", "600.00", life="1", start="2026-01-01", status="in_use")
    run = _ok(call_action(M.run_depreciation, conn, ns(company_id=cid, posting_date="2026-01-31")))
    assert run["entries_posted"] == 1
    accts = seed_disposal_accounts(conn, cid)
    _ok(call_action(M.dispose_asset, conn, ns(asset_id=bench, disposal_date="2026-03-01", disposal_method="scrap", sale_amount=None, buyer_details=None, cost_center_id=None, proceeds_account_id=None, gain_loss_account_id=accts["loss_account_id"])))
    before_rows = _sched(conn, bench)
    before_audits = _audit_count(conn)
    res = call_action(M.generate_depreciation_schedule, conn, ns(asset_id=bench))
    assert is_error(res)
    assert res["message"] == "Asset is 'scrapped'. Cannot generate a depreciation schedule for a disposed asset."
    assert _sched(conn, bench) == before_rows
    assert _audit_count(conn) == before_audits


def test_core_refuses_to_restack(conn):
    _cid, _mach, press = _setup_press(conn)
    run = _ok(call_action(M.run_depreciation, conn, ns(company_id=_cid, posting_date="2026-02-28")))
    assert run["entries_posted"] == 2
    before = _sched(conn, press)
    asset_dict = _asset_dict(conn, press)
    with pytest.raises(ValueError) as exc:
        M._generate_schedule_core(conn, asset_dict)
    assert str(exc.value) == f"Asset {press} has posted depreciation; its schedule can only be rebased from book value"
    after = _sched(conn, press)
    assert after == before


def _setup_lathe_bench(conn):
    cid, _cc = _company(conn, "Acme Turning", "ACT")
    mach = _category(conn, cid, "Machinery", 5)
    lathe, _lns = _asset(conn, cid, mach["id"], "Lathe", "5000.00", start="2026-01-01", status="in_use")
    bench, _bns = _asset(conn, cid, mach["id"], "Bench", "600.00", life="1", start="2026-01-01", status="in_use")
    run = _ok(call_action(M.run_depreciation, conn, ns(company_id=cid, posting_date="2026-01-31")))
    assert run["entries_posted"] == 2
    _ok(call_action(M.impair_asset, conn, ns(asset_id=lathe, impairment_amount="1000.00", recoverable_amount="3000.00", impairment_date="2026-02-15")))
    accts = seed_disposal_accounts(conn, cid)
    _ok(call_action(M.dispose_asset, conn, ns(asset_id=bench, disposal_date="2026-03-01", disposal_method="scrap", sale_amount=None, buyer_details=None, cost_center_id=None, proceeds_account_id=None, gain_loss_account_id=accts["loss_account_id"])))
    return cid, lathe, bench


def test_register_counts_impairment_and_disposal(conn):
    cid, lathe, bench = _setup_lathe_bench(conn)
    la = _asset_row(conn, lathe)
    ba = _asset_row(conn, bench)
    assert (la["status"], la["gross_value"], la["accumulated_depreciation"], la["current_book_value"]) == ("impaired", "5000.00", "1083.33", "3916.67")
    assert (ba["status"], ba["gross_value"], ba["accumulated_depreciation"], ba["current_book_value"]) == ("scrapped", "600.00", "50.00", "0")
    r = _register(conn, cid, "2026-12-31")
    lines = [(a["asset_name"], a["status"], a["gross_value"], a["accumulated_depreciation"], a["current_book_value"]) for a in r["assets"]]
    assert lines == [("Lathe", "impaired", "5000.00", "1083.33", "3916.67"), ("Bench", "scrapped", "600.00", "50.00", "0.00")]
    assert r["summary"] == {"total_assets": 2, "total_gross_value": "5600.00", "total_accumulated_depreciation": "1133.33", "total_book_value": "3916.67"}
    early = _register(conn, cid, "2026-02-01")
    early_lines = [(a["asset_name"], a["gross_value"], a["accumulated_depreciation"], a["current_book_value"]) for a in early["assets"]]
    assert early_lines == [("Lathe", "5000.00", "83.33", "4916.67"), ("Bench", "600.00", "50.00", "550.00")]
    assert early["summary"]["total_book_value"] == "5466.67"
    mid = _register(conn, cid, "2026-02-15")
    by_name = {a["asset_name"]: a for a in mid["assets"]}
    assert (by_name["Lathe"]["accumulated_depreciation"], by_name["Lathe"]["current_book_value"]) == ("1083.33", "3916.67")
    assert by_name["Bench"]["current_book_value"] == "550.00"


def test_register_ignores_a_reversed_impairment(conn):
    cid, _cc = _company(conn, "Acme Turning", "ACT")
    mach = _category(conn, cid, "Machinery", 5)
    lathe, _lns = _asset(conn, cid, mach["id"], "Lathe", "5000.00", start="2026-01-01", status="in_use")
    run = _ok(call_action(M.run_depreciation, conn, ns(company_id=cid, posting_date="2026-01-31")))
    assert run["entries_posted"] == 1
    imp = _ok(call_action(M.impair_asset, conn, ns(asset_id=lathe, impairment_amount="1000.00", recoverable_amount="3000.00", impairment_date="2026-02-15")))
    _ok(call_action(M.reverse_impairment, conn, ns(impairment_id=imp["impairment_id"], posting_date="2026-03-31")))
    r = _register(conn, cid, "2026-12-31")
    assert len(r["assets"]) == 1
    line = r["assets"][0]
    assert (line["accumulated_depreciation"], line["current_book_value"]) == ("83.33", "4916.67")
    a = _asset_row(conn, lathe)
    assert (a["accumulated_depreciation"], a["current_book_value"]) == ("83.33", "4916.67")


def test_regenerate_refuses_an_accelerated_method_after_posting(conn):
    cid, _cc = _company(conn, "Acme Kiln", "ACK")
    mach = _category(conn, cid, "Machinery", 5)
    kiln, _kns = _asset(conn, cid, mach["id"], "Kiln", "1200.00", salvage="100.00", life="1", start="2026-01-01", status="in_use", depreciation_method="double_declining")
    run = _ok(call_action(M.run_depreciation, conn, ns(company_id=cid, posting_date="2026-01-31")))
    assert run["entries_posted"] == 1
    before_rows = _sched(conn, kiln)
    before_audits = _audit_count(conn)
    res = call_action(M.generate_depreciation_schedule, conn, ns(asset_id=kiln))
    assert is_error(res)
    assert res["message"] == f"Asset {kiln} has posted double_declining depreciation; only a straight_line schedule is rebased from book value"
    assert _sched(conn, kiln) == before_rows
    assert _audit_count(conn) == before_audits
