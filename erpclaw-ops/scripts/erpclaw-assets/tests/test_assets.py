"""L1 tests for ERPClaw Assets skill (16 actions).

Tests cover: asset categories, assets, depreciation, movements,
maintenance, disposal, and reports.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from assets_helpers import (
    load_db_query, call_action, ns, is_ok, is_error,
    seed_company, seed_naming_series, seed_account,
    seed_asset_category, seed_asset, _uuid,
    build_gl_env, set_asset_status,
)

M = load_db_query()


def _gl_sums(conn, voucher_type, voucher_id):
    """Return (total_debit, total_credit) for a voucher's *active* GL entries."""
    from decimal import Decimal
    rows = conn.execute(
        "SELECT debit, credit FROM gl_entry WHERE voucher_type = ? AND voucher_id = ? "
        "AND is_cancelled = 0", (voucher_type, voucher_id)).fetchall()
    d = sum((Decimal(r["debit"]) for r in rows), Decimal("0"))
    c = sum((Decimal(r["credit"]) for r in rows), Decimal("0"))
    return d, c


def _book_value(conn, asset_id):
    from decimal import Decimal
    r = conn.execute("SELECT current_book_value FROM asset WHERE id = ?", (asset_id,)).fetchone()
    return Decimal(r["current_book_value"])


# ===================================================================
# Asset Categories
# ===================================================================

class TestAddAssetCategory:
    def test_add_category_ok(self, conn, env):
        r = call_action(M.add_asset_category, conn, ns(
            company_id=env["company_id"],
            name="Vehicles",
            depreciation_method="straight_line",
            useful_life_years="10",
        ))
        assert is_ok(r)
        assert r["asset_category_id"]
        assert r["name"] == "Vehicles"

    def test_add_category_missing_name(self, conn, env):
        r = call_action(M.add_asset_category, conn, ns(
            company_id=env["company_id"],
            depreciation_method="straight_line",
            useful_life_years="5",
        ))
        assert is_error(r)

    def test_add_category_missing_method(self, conn, env):
        r = call_action(M.add_asset_category, conn, ns(
            company_id=env["company_id"],
            name="Machinery",
            useful_life_years="5",
        ))
        assert is_error(r)

    def test_add_category_invalid_method(self, conn, env):
        r = call_action(M.add_asset_category, conn, ns(
            company_id=env["company_id"],
            name="Test",
            depreciation_method="invalid_method",
            useful_life_years="5",
        ))
        assert is_error(r)

    def test_add_category_duplicate_name(self, conn, env):
        call_action(M.add_asset_category, conn, ns(
            company_id=env["company_id"],
            name="DupCat",
            depreciation_method="straight_line",
            useful_life_years="5",
        ))
        r = call_action(M.add_asset_category, conn, ns(
            company_id=env["company_id"],
            name="DupCat",
            depreciation_method="straight_line",
            useful_life_years="5",
        ))
        assert is_error(r)


class TestListAssetCategories:
    def test_list_categories(self, conn, env):
        r = call_action(M.list_asset_categories, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        # build_env seeds one category
        assert r["total"] >= 1


# ===================================================================
# Assets
# ===================================================================

class TestAddAsset:
    def test_add_asset_ok(self, conn, env):
        r = call_action(M.add_asset, conn, ns(
            company_id=env["company_id"],
            name="Desktop Computer",
            asset_category_id=env["category_id"],
            gross_value="3000.00",
        ))
        assert is_ok(r)
        assert r["asset_id"]
        assert r["gross_value"] == "3000.00"
        assert r["current_book_value"] == "3000.00"

    def test_add_asset_missing_name(self, conn, env):
        r = call_action(M.add_asset, conn, ns(
            company_id=env["company_id"],
            asset_category_id=env["category_id"],
            gross_value="1000",
        ))
        assert is_error(r)

    def test_add_asset_missing_category(self, conn, env):
        r = call_action(M.add_asset, conn, ns(
            company_id=env["company_id"],
            name="Test Asset",
            gross_value="1000",
        ))
        assert is_error(r)

    def test_add_asset_zero_value(self, conn, env):
        r = call_action(M.add_asset, conn, ns(
            company_id=env["company_id"],
            name="Zero Asset",
            asset_category_id=env["category_id"],
            gross_value="0",
        ))
        assert is_error(r)

    def test_add_asset_salvage_exceeds_gross(self, conn, env):
        r = call_action(M.add_asset, conn, ns(
            company_id=env["company_id"],
            name="Bad Salvage",
            asset_category_id=env["category_id"],
            gross_value="1000",
            salvage_value="2000",
        ))
        assert is_error(r)


class TestGetAsset:
    def test_get_asset_ok(self, conn, env):
        r = call_action(M.get_asset, conn, ns(asset_id=env["asset_id"]))
        assert is_ok(r)
        assert r["asset"]["id"] == env["asset_id"]

    def test_get_asset_not_found(self, conn, env):
        r = call_action(M.get_asset, conn, ns(asset_id=_uuid()))
        assert is_error(r)


class TestListAssets:
    def test_list_assets(self, conn, env):
        r = call_action(M.list_assets, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert r["total"] >= 1


class TestUpdateAsset:
    def test_update_asset_location(self, conn, env):
        r = call_action(M.update_asset, conn, ns(
            asset_id=env["asset_id"],
            location="Building A - Floor 2",
        ))
        assert is_ok(r)

    def test_update_asset_not_found(self, conn, env):
        r = call_action(M.update_asset, conn, ns(
            asset_id=_uuid(),
            location="Nowhere",
        ))
        assert is_error(r)


# ===================================================================
# Depreciation
# ===================================================================

class TestGenerateDepreciationSchedule:
    def test_generate_schedule_ok(self, conn, env):
        r = call_action(M.generate_depreciation_schedule, conn, ns(
            asset_id=env["asset_id"],
        ))
        assert is_ok(r)
        assert r["entries_generated"] > 0


# ===================================================================
# Maintenance
# ===================================================================

class TestScheduleMaintenance:
    def test_schedule_maintenance_ok(self, conn, env):
        r = call_action(M.schedule_maintenance, conn, ns(
            asset_id=env["asset_id"],
            maintenance_type="preventive",
            scheduled_date="2026-06-01",
        ))
        assert is_ok(r)
        assert r["maintenance_id"]

    def test_schedule_maintenance_missing_asset(self, conn, env):
        r = call_action(M.schedule_maintenance, conn, ns(
            maintenance_type="preventive",
            scheduled_date="2026-06-01",
        ))
        assert is_error(r)


# ===================================================================
# Reports
# ===================================================================

class TestAssetRegisterReport:
    def test_report_ok(self, conn, env):
        r = call_action(M.asset_register_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r)
        assert "assets" in r


class TestStatus:
    def test_status_ok(self, conn, env):
        r = call_action(M.status, conn, ns())
        assert is_ok(r)


# ===================================================================
# M7 — Impairment
# ===================================================================

class TestImpairAsset:
    def test_impair_ok_balanced_gl(self, conn):
        env = build_gl_env(conn)
        r = call_action(M.impair_asset, conn, ns(
            asset_id=env["asset_id"], impairment_amount="1000.00",
            recoverable_amount="3000.00", impairment_date="2026-03-01"))
        assert is_ok(r), r
        assert r["book_value_after"] == "4000.00"
        assert r["new_status"] == "impaired"
        d, c = _gl_sums(conn, "asset_impairment", r["impairment_id"])
        assert d == c and d > 0
        assert _book_value(conn, env["asset_id"]) == __import__("decimal").Decimal("4000.00")

    def test_impair_below_recoverable_floor_rejects(self, conn):
        """NEGATIVE CONTROL: cannot impair below the recoverable amount."""
        env = build_gl_env(conn)
        r = call_action(M.impair_asset, conn, ns(
            asset_id=env["asset_id"], impairment_amount="3000.00",
            recoverable_amount="4000.00", impairment_date="2026-03-01"))
        assert is_error(r)
        # Asset untouched.
        assert _book_value(conn, env["asset_id"]) == __import__("decimal").Decimal("5000.00")

    def test_impair_disposed_asset_rejects(self, conn):
        env = build_gl_env(conn)
        set_asset_status(conn, env["asset_id"], "scrapped")
        r = call_action(M.impair_asset, conn, ns(
            asset_id=env["asset_id"], impairment_amount="100",
            recoverable_amount="0"))
        assert is_error(r)

    def test_impair_missing_amount(self, conn):
        env = build_gl_env(conn)
        r = call_action(M.impair_asset, conn, ns(
            asset_id=env["asset_id"], recoverable_amount="0"))
        assert is_error(r)

    def test_impairment_table_immutable(self, conn):
        """L0: asset_impairment carries no updated_at (cancel = reverse)."""
        cols = [r[1] for r in conn.execute("PRAGMA table_info(asset_impairment)")]
        assert "updated_at" not in cols


class TestReverseImpairment:
    def test_reverse_round_trip(self, conn):
        env = build_gl_env(conn)
        imp = call_action(M.impair_asset, conn, ns(
            asset_id=env["asset_id"], impairment_amount="1000.00",
            recoverable_amount="3000.00"))
        assert is_ok(imp)
        rev = call_action(M.reverse_impairment, conn, ns(
            impairment_id=imp["impairment_id"], posting_date="2026-04-01"))
        assert is_ok(rev), rev
        # Book value restored, status back to in_use.
        assert _book_value(conn, env["asset_id"]) == __import__("decimal").Decimal("5000.00")
        assert rev["new_status"] == "in_use"
        # GL net effect zero: original cancelled, mirror posted.
        d, c = _gl_sums(conn, "asset_impairment", imp["impairment_id"])
        assert d == c

    def test_double_reverse_rejects(self, conn):
        env = build_gl_env(conn)
        imp = call_action(M.impair_asset, conn, ns(
            asset_id=env["asset_id"], impairment_amount="500",
            recoverable_amount="0"))
        call_action(M.reverse_impairment, conn, ns(impairment_id=imp["impairment_id"]))
        r = call_action(M.reverse_impairment, conn, ns(impairment_id=imp["impairment_id"]))
        assert is_error(r)


# ===================================================================
# M7 — Capitalization
# ===================================================================

class TestCapitalizeAsset:
    def test_capitalize_ok_balanced_gl(self, conn):
        env = build_gl_env(conn)
        src = seed_account(conn, env["company_id"], "CWIP Clearing", "asset", "asset")
        r = call_action(M.capitalize_asset, conn, ns(
            company_id=env["company_id"], name="New Machine",
            asset_category_id=env["category_id"], capitalized_amount="12000.00",
            source_account_id=src, purchase_invoice_id="PI-001",
            capitalization_date="2026-02-01"))
        assert is_ok(r), r
        assert r["new_status"] == "submitted"
        d, c = _gl_sums(conn, "asset_capitalization", r["capitalization_id"])
        assert d == c and d == __import__("decimal").Decimal("12000.00")

    def test_capitalize_duplicate_pi_rejects(self, conn):
        env = build_gl_env(conn)
        src = seed_account(conn, env["company_id"], "CWIP", "asset", "asset")
        ok1 = call_action(M.capitalize_asset, conn, ns(
            company_id=env["company_id"], name="M1",
            asset_category_id=env["category_id"], capitalized_amount="100",
            source_account_id=src, purchase_invoice_id="PI-DUP"))
        assert is_ok(ok1)
        r = call_action(M.capitalize_asset, conn, ns(
            company_id=env["company_id"], name="M2",
            asset_category_id=env["category_id"], capitalized_amount="100",
            source_account_id=src, purchase_invoice_id="PI-DUP"))
        assert is_error(r)

    def test_capitalize_missing_source_account(self, conn):
        env = build_gl_env(conn)
        r = call_action(M.capitalize_asset, conn, ns(
            company_id=env["company_id"], name="M",
            asset_category_id=env["category_id"], capitalized_amount="100"))
        assert is_error(r)


# ===================================================================
# M7 — Revaluation
# ===================================================================

class TestRevalueAsset:
    def test_revalue_up_adjusts_gross_and_recomputes(self, conn):
        from decimal import Decimal
        env = build_gl_env(conn)
        # Give the asset a depreciation schedule + one posted entry.
        call_action(M.generate_depreciation_schedule, conn, ns(asset_id=env["asset_id"]))
        reserve = seed_account(conn, env["company_id"], "Reval Reserve", "equity", "equity")
        before = conn.execute(
            "SELECT COUNT(*) c FROM depreciation_schedule WHERE asset_id=? AND status='pending'",
            (env["asset_id"],)).fetchone()["c"]
        r = call_action(M.revalue_asset, conn, ns(
            asset_id=env["asset_id"], new_value="6000.00",
            reserve_account_id=reserve, revaluation_date="2026-03-01"))
        assert is_ok(r), r
        assert r["direction"] == "up"
        assert r["book_value_after"] == "6000.00"
        # gross_value bumped by the +1000 delta.
        gross = conn.execute("SELECT gross_value FROM asset WHERE id=?",
                             (env["asset_id"],)).fetchone()["gross_value"]
        assert Decimal(gross) == Decimal("6000.00")
        d, c = _gl_sums(conn, "asset_revaluation", r["revaluation_id"])
        assert d == c and d == Decimal("1000.00")
        # Schedule was recomputed (not skipped).
        assert r["schedule_recompute"]["regenerated"] > 0
        after_first = conn.execute(
            "SELECT book_value_after FROM depreciation_schedule WHERE asset_id=? "
            "AND status='pending' ORDER BY schedule_date LIMIT 1",
            (env["asset_id"],)).fetchone()["book_value_after"]
        # First pending entry now reflects the higher basis.
        assert Decimal(after_first) > Decimal("5000.00") - Decimal("100")

    def test_revalue_negative_rejects(self, conn):
        env = build_gl_env(conn)
        reserve = seed_account(conn, env["company_id"], "RR", "equity", "equity")
        r = call_action(M.revalue_asset, conn, ns(
            asset_id=env["asset_id"], new_value="-1", reserve_account_id=reserve))
        assert is_error(r)

    def test_revalue_under_construction_rejects(self, conn):
        env = build_gl_env(conn)
        set_asset_status(conn, env["asset_id"], "under_construction")
        reserve = seed_account(conn, env["company_id"], "RR", "equity", "equity")
        r = call_action(M.revalue_asset, conn, ns(
            asset_id=env["asset_id"], new_value="6000", reserve_account_id=reserve))
        assert is_error(r)


# ===================================================================
# M7 — Capex vs opex maintenance
# ===================================================================

class TestCompleteMaintenanceCapex:
    def _schedule(self, conn, env, is_capex=None):
        m = call_action(M.schedule_maintenance, conn, ns(
            asset_id=env["asset_id"], maintenance_type="corrective",
            scheduled_date="2026-02-01", is_capex=is_capex))
        assert is_ok(m), m
        return m["maintenance_id"]

    def test_capex_capitalizes_and_recomputes(self, conn):
        """NEGATIVE CONTROL: the capex path must NOT skip the depreciation recompute."""
        from decimal import Decimal
        env = build_gl_env(conn)
        call_action(M.generate_depreciation_schedule, conn, ns(asset_id=env["asset_id"]))
        cash = seed_account(conn, env["company_id"], "Cash", "asset", "asset")
        mid = self._schedule(conn, env, is_capex="1")
        r = call_action(M.complete_maintenance, conn, ns(
            maintenance_id=mid, cost="800.00", actual_date="2026-02-15",
            cash_account_id=cash))
        assert is_ok(r), r
        assert r["branch"] == "capex"
        # Cost capitalized: book value up by 800.
        assert _book_value(conn, env["asset_id"]) == Decimal("5800.00")
        d, c = _gl_sums(conn, "asset_repair_capex", mid)
        assert d == c and d == Decimal("800.00")
        # Recompute happened — the guard against silently skipping it.
        assert r["schedule_recompute"] is not None
        assert r["schedule_recompute"]["regenerated"] > 0

    def test_capex_requires_in_use(self, conn):
        env = build_gl_env(conn)
        set_asset_status(conn, env["asset_id"], "submitted")
        cash = seed_account(conn, env["company_id"], "Cash", "asset", "asset")
        mid = self._schedule(conn, env, is_capex="1")
        r = call_action(M.complete_maintenance, conn, ns(
            maintenance_id=mid, cost="800.00", cash_account_id=cash))
        assert is_error(r)

    def test_capex_requires_cash_account(self, conn):
        env = build_gl_env(conn)
        mid = self._schedule(conn, env, is_capex="1")
        r = call_action(M.complete_maintenance, conn, ns(
            maintenance_id=mid, cost="800.00"))
        assert is_error(r)

    def test_opex_legacy_no_accounts_still_completes(self, conn):
        """Backward-compat: opex with no accounts just completes (no GL)."""
        env = build_gl_env(conn)
        mid = self._schedule(conn, env, is_capex="0")
        r = call_action(M.complete_maintenance, conn, ns(
            maintenance_id=mid, cost="50.00"))
        assert is_ok(r), r
        assert r["branch"] == "opex"
        assert r["gl_entry_ids"] == []
        # Book value untouched.
        assert _book_value(conn, env["asset_id"]) == __import__("decimal").Decimal("5000.00")

    def test_opex_with_accounts_posts_expense(self, conn):
        from decimal import Decimal
        env = build_gl_env(conn)
        cash = seed_account(conn, env["company_id"], "Cash", "asset", "asset")
        exp = seed_account(conn, env["company_id"], "Repair Expense", "expense", "expense")
        mid = self._schedule(conn, env, is_capex="0")
        r = call_action(M.complete_maintenance, conn, ns(
            maintenance_id=mid, cost="120.00", cash_account_id=cash,
            expense_account_id=exp))
        assert is_ok(r)
        d, c = _gl_sums(conn, "asset_repair_capex", mid)
        assert d == c and d == Decimal("120.00")
        # Opex does NOT capitalize.
        assert _book_value(conn, env["asset_id"]) == Decimal("5000.00")


# ===================================================================
# S3 — CWIP (Construction-in-Progress)
# ===================================================================

def _cwip_env(conn):
    """build_gl_env + a CWIP account and a Cash/AP source account."""
    env = build_gl_env(conn)
    env["cwip_account_id"] = seed_account(
        conn, env["company_id"], "CWIP", "capital_work_in_progress", "asset")
    # Cash source (asset) avoids GL Step 5's party-mandatory rule for AR/AP accounts;
    # the action accepts any account as the credit leg.
    env["source_account_id"] = seed_account(
        conn, env["company_id"], "Cash", "asset", "asset")
    return env


def _new_cwip_asset(conn, env, project_id=None):
    r = call_action(M.add_cwip, conn, ns(
        company_id=env["company_id"], asset_category_id=env["category_id"],
        name="Plant Under Construction", project_id=project_id))
    assert is_ok(r), r
    return r["asset_id"]


def _accumulate(conn, env, asset_id, amount, vtype="purchase_invoice", vid=None):
    return call_action(M.accumulate_cwip_cost, conn, ns(
        asset_id=asset_id, source_voucher_type=vtype, source_voucher_id=vid,
        amount=amount, cwip_account_id=env["cwip_account_id"],
        source_account_id=env["source_account_id"], posting_date="2026-03-01"))


class TestAddCwip:
    def test_add_cwip_ok_under_construction_zero_value(self, conn):
        env = _cwip_env(conn)
        aid = _new_cwip_asset(conn, env)
        row = conn.execute("SELECT status, current_book_value, gross_value FROM asset WHERE id=?",
                           (aid,)).fetchone()
        assert row["status"] == "under_construction"
        assert row["current_book_value"] == "0" and row["gross_value"] == "0"

    def test_add_cwip_missing_category(self, conn):
        env = _cwip_env(conn)
        r = call_action(M.add_cwip, conn, ns(company_id=env["company_id"]))
        assert is_error(r)

    def test_add_cwip_bad_project_rejects(self, conn):
        env = _cwip_env(conn)
        r = call_action(M.add_cwip, conn, ns(
            company_id=env["company_id"], asset_category_id=env["category_id"],
            project_id="no-such-project"))
        assert is_error(r)


class TestAccumulateCwip:
    def test_accumulate_rollup_and_balanced_gl(self, conn):
        """Three vouchers totaling 50000 → book value 50000; each GL leg balances
        and debits the CWIP account."""
        from decimal import Decimal
        env = _cwip_env(conn)
        aid = _new_cwip_asset(conn, env)
        ids = []
        for amt, vid in [("20000.00", "PI-1"), ("18000.00", "PI-2"), ("12000.00", "PI-3")]:
            r = _accumulate(conn, env, aid, amt, vid=vid)
            assert is_ok(r), r
            ids.append(r["accumulation_id"])
        assert _book_value(conn, aid) == Decimal("50000.00")
        # Each accumulation posts a balanced DR CWIP / CR source pair.
        for acc_id in ids:
            d, c = _gl_sums(conn, "cwip_capitalization", acc_id)
            assert d == c
        # All CWIP debits land on the CWIP account.
        dr_acct = conn.execute(
            "SELECT DISTINCT account_id FROM gl_entry WHERE voucher_type='cwip_capitalization' "
            "AND CAST(debit AS REAL) > 0").fetchall()
        assert len(dr_acct) == 1 and dr_acct[0]["account_id"] == env["cwip_account_id"]

    def test_accumulate_requires_under_construction(self, conn):
        """NEGATIVE CONTROL: accumulating against a normal in_use asset rejects."""
        env = _cwip_env(conn)
        r = _accumulate(conn, env, env["asset_id"], "100.00")  # build_gl_env asset is in_use
        assert is_error(r)

    def test_accumulate_negative_amount_rejects(self, conn):
        env = _cwip_env(conn)
        aid = _new_cwip_asset(conn, env)
        r = _accumulate(conn, env, aid, "-5.00")
        assert is_error(r)

    def test_accumulate_non_cwip_account_rejects(self, conn):
        """NEGATIVE CONTROL: --cwip-account-id must be a capital_work_in_progress
        account; a plain asset account is rejected."""
        env = _cwip_env(conn)
        aid = _new_cwip_asset(conn, env)
        r = call_action(M.accumulate_cwip_cost, conn, ns(
            asset_id=aid, source_voucher_type="purchase_invoice", amount="100.00",
            cwip_account_id=env["asset_account_id"],  # NOT a CWIP account
            source_account_id=env["source_account_id"], posting_date="2026-03-01"))
        assert is_error(r)

    def test_accumulate_mixed_cwip_account_rejects(self, conn):
        env = _cwip_env(conn)
        aid = _new_cwip_asset(conn, env)
        assert is_ok(_accumulate(conn, env, aid, "100.00"))
        other_cwip = seed_account(conn, env["company_id"], "CWIP 2",
                                  "capital_work_in_progress", "asset")
        r = call_action(M.accumulate_cwip_cost, conn, ns(
            asset_id=aid, source_voucher_type="purchase_invoice", amount="50.00",
            cwip_account_id=other_cwip, source_account_id=env["source_account_id"],
            posting_date="2026-03-01"))
        assert is_error(r)

    def test_je_guard_now_reachable(self, conn):
        """NEGATIVE CONTROL: a direct journal_entry to a CWIP account is rejected
        by the gl_posting.py guard — now reachable because the account_type
        registry lets a capital_work_in_progress account exist."""
        from erpclaw_lib.gl_posting import insert_gl_entries
        env = _cwip_env(conn)
        try:
            insert_gl_entries(
                conn,
                [{"account_id": env["cwip_account_id"], "debit": "100", "credit": "0",
                  "cost_center_id": env["cost_center_id"], "fiscal_year": "FY2026"},
                 {"account_id": env["source_account_id"], "debit": "0", "credit": "100",
                  "cost_center_id": env["cost_center_id"], "fiscal_year": "FY2026"}],
                voucher_type="journal_entry", voucher_id="JE-CWIP",
                posting_date="2026-03-01", company_id=env["company_id"])
            assert False, "expected the CWIP guard to reject a journal_entry to a CWIP account"
        except ValueError as e:
            assert "Capital Work in Progress" in str(e)


class TestTransferCwip:
    def test_transfer_balanced_status_and_schedule(self, conn):
        """transfer flips to in_use, posts balanced DR Fixed Asset / CR CWIP, and
        generates a schedule starting from the transfer date."""
        from decimal import Decimal
        env = _cwip_env(conn)
        aid = _new_cwip_asset(conn, env)
        _accumulate(conn, env, aid, "30000.00", vid="PI-A")
        _accumulate(conn, env, aid, "20000.00", vid="PI-B")
        r = call_action(M.transfer_cwip_to_asset, conn, ns(
            asset_id=aid, depreciation_start_date="2026-04-01"))
        assert is_ok(r), r
        assert r["new_status"] == "in_use"
        assert r["capitalized_amount"] == "50000.00"
        assert r["depreciation_entries_generated"] > 0
        d, c = _gl_sums(conn, "asset_capitalization", r["capitalization_id"])
        assert d == c and d == Decimal("50000.00")
        # The CR leg credits the CWIP account.
        cr = conn.execute(
            "SELECT account_id FROM gl_entry WHERE voucher_type='asset_capitalization' "
            "AND CAST(credit AS REAL) > 0").fetchone()
        assert cr["account_id"] == env["cwip_account_id"]
        # Schedule begins on the transfer date.
        first = conn.execute(
            "SELECT schedule_date FROM depreciation_schedule WHERE asset_id=? "
            "ORDER BY schedule_date LIMIT 1", (aid,)).fetchone()
        assert first["schedule_date"] == "2026-04-01"
        assert conn.execute("SELECT status FROM asset WHERE id=?", (aid,)).fetchone()["status"] == "in_use"

    def test_transfer_zero_accumulation_rejects(self, conn):
        """NEGATIVE CONTROL: transfer with nothing accumulated is rejected."""
        env = _cwip_env(conn)
        aid = _new_cwip_asset(conn, env)
        r = call_action(M.transfer_cwip_to_asset, conn, ns(asset_id=aid))
        assert is_error(r)

    def test_transfer_with_final_additional_cost(self, conn):
        from decimal import Decimal
        env = _cwip_env(conn)
        aid = _new_cwip_asset(conn, env)
        _accumulate(conn, env, aid, "10000.00", vid="PI-X")
        r = call_action(M.transfer_cwip_to_asset, conn, ns(
            asset_id=aid, final_additional_cost="2500.00",
            source_account_id=env["source_account_id"],
            depreciation_start_date="2026-05-01"))
        assert is_ok(r), r
        assert r["capitalized_amount"] == "12500.00"
        d, c = _gl_sums(conn, "asset_capitalization", r["capitalization_id"])
        assert d == c and d == Decimal("12500.00")

    def test_transfer_requires_under_construction(self, conn):
        env = _cwip_env(conn)
        r = call_action(M.transfer_cwip_to_asset, conn, ns(asset_id=env["asset_id"]))
        assert is_error(r)


class TestCancelCwip:
    def test_cancel_reverses_gl_and_sets_cancelled(self, conn):
        """cancel reverses every accumulation (net GL zero) and flips to cancelled."""
        from decimal import Decimal
        env = _cwip_env(conn)
        aid = _new_cwip_asset(conn, env)
        _accumulate(conn, env, aid, "5000.00", vid="PI-1")
        _accumulate(conn, env, aid, "3000.00", vid="PI-2")
        r = call_action(M.cancel_cwip, conn, ns(asset_id=aid, reason="project scrapped"))
        assert is_ok(r), r
        assert r["new_status"] == "cancelled"
        assert r["accumulations_reversed"] == 2
        # Net GL across the original + reversal entries nets to zero.
        net = conn.execute(
            "SELECT COALESCE(decimal_sum(debit),'0') d, COALESCE(decimal_sum(credit),'0') c "
            "FROM gl_entry WHERE voucher_type='cwip_capitalization'").fetchone()
        assert Decimal(net["d"]) == Decimal(net["c"])
        row = conn.execute("SELECT status, current_book_value FROM asset WHERE id=?",
                           (aid,)).fetchone()
        assert row["status"] == "cancelled" and row["current_book_value"] == "0"
        # Accumulations marked reversed.
        n_sub = conn.execute(
            "SELECT COUNT(*) c FROM cwip_cost_accumulation WHERE asset_id=? AND status='submitted'",
            (aid,)).fetchone()["c"]
        assert n_sub == 0

    def test_cancel_after_transfer_rejects(self, conn):
        env = _cwip_env(conn)
        aid = _new_cwip_asset(conn, env)
        _accumulate(conn, env, aid, "1000.00")
        assert is_ok(call_action(M.transfer_cwip_to_asset, conn, ns(asset_id=aid)))
        r = call_action(M.cancel_cwip, conn, ns(asset_id=aid, reason="too late"))
        assert is_error(r)

    def test_cancel_requires_reason(self, conn):
        env = _cwip_env(conn)
        aid = _new_cwip_asset(conn, env)
        r = call_action(M.cancel_cwip, conn, ns(asset_id=aid))
        assert is_error(r)


class TestListCwipProjects:
    def test_list_shows_in_progress_with_cost(self, conn):
        env = _cwip_env(conn)
        a1 = _new_cwip_asset(conn, env)
        _accumulate(conn, env, a1, "7000.00")
        # A second CWIP asset, then transfer it — should drop off the list.
        a2 = _new_cwip_asset(conn, env)
        _accumulate(conn, env, a2, "100.00")
        call_action(M.transfer_cwip_to_asset, conn, ns(asset_id=a2))
        r = call_action(M.list_cwip_projects, conn, ns(company_id=env["company_id"]))
        assert is_ok(r), r
        ids = {p["asset_id"]: p for p in r["cwip_projects"]}
        assert a1 in ids and a2 not in ids
        assert ids[a1]["accumulated_cost"] == "7000.00"
        assert ids[a1]["accumulation_count"] == 1

    def test_per_project_tagging_on_gl(self, conn):
        """add-cwip --project-id flows to gl_entry.project_id on every accumulation
        (per-project CWIP roll-up, Wave-1 Q7)."""
        env = _cwip_env(conn)
        # Seed a project to reference.
        import uuid as _uuid
        pid = str(_uuid.uuid4())
        conn.execute(
            "INSERT INTO project (id, project_name, company_id) VALUES (?, ?, ?)",
            (pid, "Project Alpha", env["company_id"]))
        conn.commit()
        aid = _new_cwip_asset(conn, env, project_id=pid)
        _accumulate(conn, env, aid, "4000.00")
        tagged = conn.execute(
            "SELECT COUNT(*) c FROM gl_entry WHERE voucher_type='cwip_capitalization' "
            "AND project_id=?", (pid,)).fetchone()["c"]
        assert tagged == 2  # both legs carry the project
