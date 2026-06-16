#!/usr/bin/env python3
"""ERPClaw Assets Skill -- db_query.py

Fixed asset management, depreciation scheduling, asset movements,
maintenance tracking, disposal, and asset-depth lifecycles (impairment,
capitalization, revaluation, capex maintenance) and construction-in-progress
(CWIP cost accumulation + capitalization) with GL posting.
All 25 actions are routed through this single entry point.

Usage: python3 db_query.py --action <action-name> [--flags ...]
Output: JSON to stdout, exit 0 on success, exit 1 on error.
"""
import argparse
import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

# Add shared lib to path
try:
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.db import get_connection, ensure_db_exists, DEFAULT_DB_PATH
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.naming import get_next_name
    from erpclaw_lib.gl_posting import insert_gl_entries, reverse_gl_entries
    from erpclaw_lib.cwip_posting import record_cwip_accumulation, cwip_account_for_asset
    from erpclaw_lib.validation import check_input_lengths
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.dependencies import check_required_tables
    from erpclaw_lib.query import Q, P, Table, Field, fn, Case, Order, Criterion, Not, NULL, DecimalSum, DecimalAbs
    from erpclaw_lib.vendor.pypika.terms import LiteralValue, ValueWrapper
    from erpclaw_lib.args import SafeArgumentParser, check_unknown_args
except ImportError:
    import json as _json
    print(_json.dumps({"status": "error", "error": "ERPClaw foundation not installed. Install erpclaw first: clawhub install erpclaw", "suggestion": "clawhub install erpclaw"}))
    sys.exit(1)

REQUIRED_TABLES = ["company", "account"]

VALID_DEPRECIATION_METHODS = ("straight_line", "written_down_value", "double_declining")
# Standard asset states (error-message hint). Authoritative validity now comes from
# asset_status_registry via _asset_status_registered (M0 phase 4): the hardcoded CHECK
# on asset.status was dropped so states (e.g. under_construction for CWIP) are
# registry-sourced + addable at runtime without a migration.
VALID_ASSET_STATUSES = ("draft", "submitted", "in_use", "scrapped", "sold")
VALID_MOVEMENT_TYPES = ("transfer", "issue", "receipt")
VALID_MAINTENANCE_TYPES = ("preventive", "corrective")
VALID_MAINTENANCE_STATUSES = ("planned", "overdue", "completed")
VALID_DISPOSAL_METHODS = ("sale", "scrap", "write_off")
VALID_SCHEDULE_STATUSES = ("pending", "posted", "skipped")


def _parse_json_arg(value, name):
    if value is None:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        err(f"Invalid JSON for --{name}: {value}")


def _get_fiscal_year(conn, posting_date: str) -> str | None:
    """Return the fiscal year name for a posting date, or None."""
    t = Table("fiscal_year")
    q = (Q.from_(t).select(t.name)
         .where(t.start_date <= P())
         .where(t.end_date >= P())
         .where(t.is_closed == 0))
    fy = conn.execute(q.get_sql(), (posting_date, posting_date)).fetchone()
    return fy["name"] if fy else None


def _get_cost_center(conn, company_id: str) -> str | None:
    """Return the first non-group cost center for a company, or None."""
    t = Table("cost_center")
    q = (Q.from_(t).select(t.id)
         .where(t.company_id == P())
         .where(t.is_group == 0)
         .limit(1))
    cc = conn.execute(q.get_sql(), (company_id,)).fetchone()
    return cc["id"] if cc else None


def _validate_company_exists(conn, company_id: str):
    """Validate that a company exists and return the row, or error."""
    t = Table("company")
    q = Q.from_(t).select(t.id).where(t.id == P())
    company = conn.execute(q.get_sql(), (company_id,)).fetchone()
    if not company:
        err(f"Company {company_id} not found")
    return company


def _asset_status_registered(conn, status):
    """True if status exists in asset_status_registry (M0 phase 4 source of truth)."""
    return conn.execute(
        "SELECT 1 FROM asset_status_registry WHERE status = ? AND is_active = 1", (status,)
    ).fetchone() is not None


def _validate_asset_exists(conn, asset_id: str):
    """Validate that an asset exists and return the row, or error."""
    t = Table("asset")
    q = Q.from_(t).select(t.star).where(t.id == P())
    asset = conn.execute(q.get_sql(), (asset_id,)).fetchone()
    if not asset:
        err(f"Asset {asset_id} not found",
             suggestion="Use 'list assets' to see available assets.")
    return asset


def _validate_asset_category_exists(conn, category_id: str):
    """Validate that an asset category exists and return the row, or error."""
    t = Table("asset_category")
    q = Q.from_(t).select(t.star).where(t.id == P())
    cat = conn.execute(q.get_sql(), (category_id,)).fetchone()
    if not cat:
        err(f"Asset category {category_id} not found")
    return cat


def _today_str() -> str:
    """Return today's date as YYYY-MM-DD string."""
    return date.today().isoformat()


def _parse_is_capex(value):
    """Normalize a --is-capex flag to 0/1. Accepts 1/0/true/false/yes/no.
    None defaults to 0 (opex). Errors on anything else."""
    if value is None:
        return 0
    v = str(value).strip().lower()
    if v in ("1", "true", "yes", "y"):
        return 1
    if v in ("0", "false", "no", "n"):
        return 0
    err("--is-capex must be one of: 1/0/true/false/yes/no")


def _add_months(start_date_str: str, months: int) -> str:
    """Add N months to a date string, returning YYYY-MM-DD."""
    d = date.fromisoformat(start_date_str)
    month = d.month - 1 + months
    year = d.year + month // 12
    month = month % 12 + 1
    # Clamp day to valid range for the target month
    import calendar
    max_day = calendar.monthrange(year, month)[1]
    day = min(d.day, max_day)
    return date(year, month, day).isoformat()


# ---------------------------------------------------------------------------
# 1. add-asset-category
# ---------------------------------------------------------------------------

def add_asset_category(conn, args):
    """Create an asset category.

    Required: --company-id, --name, --depreciation-method, --useful-life-years
    Optional: --asset-account-id, --depreciation-account-id,
              --accumulated-depreciation-account-id
    """
    if not args.company_id:
        err("--company-id is required")
    if not args.name:
        err("--name is required")
    if not args.depreciation_method:
        err("--depreciation-method is required")
    if not args.useful_life_years:
        err("--useful-life-years is required")

    _validate_company_exists(conn, args.company_id)

    method = args.depreciation_method
    if method not in VALID_DEPRECIATION_METHODS:
        err(f"Invalid depreciation method '{method}'. Must be one of: {', '.join(VALID_DEPRECIATION_METHODS)}")

    try:
        useful_life = int(args.useful_life_years)
    except (ValueError, TypeError):
        err("--useful-life-years must be an integer")
    if useful_life <= 0:
        err("--useful-life-years must be greater than 0")

    # Validate account references if provided
    acct_t = Table("account")
    _acct_q = Q.from_(acct_t).select(acct_t.id).where(
        (acct_t.id == P()) | (acct_t.name == P())
    )
    _acct_sql = _acct_q.get_sql()

    if args.asset_account_id:
        acct = conn.execute(_acct_sql, (args.asset_account_id, args.asset_account_id)).fetchone()
        if not acct:
            err(f"Asset account {args.asset_account_id} not found")
        args.asset_account_id = acct["id"]
    if args.depreciation_account_id:
        acct = conn.execute(_acct_sql, (args.depreciation_account_id, args.depreciation_account_id)).fetchone()
        if not acct:
            err(f"Depreciation account {args.depreciation_account_id} not found")
        args.depreciation_account_id = acct["id"]
    if args.accumulated_depreciation_account_id:
        acct = conn.execute(_acct_sql, (args.accumulated_depreciation_account_id, args.accumulated_depreciation_account_id)).fetchone()
        if not acct:
            err(f"Accumulated depreciation account {args.accumulated_depreciation_account_id} not found")
        args.accumulated_depreciation_account_id = acct["id"]

    # Check for duplicate name in same company
    ac_t = Table("asset_category")
    dup_q = (Q.from_(ac_t).select(ac_t.id)
             .where(ac_t.name == P())
             .where(ac_t.company_id == P()))
    existing = conn.execute(dup_q.get_sql(), (args.name, args.company_id)).fetchone()
    if existing:
        err(f"Asset category '{args.name}' already exists in this company")

    cat_id = str(uuid.uuid4())
    ins_q = (Q.into(ac_t)
             .columns("id", "name", "depreciation_method", "useful_life_years",
                       "asset_account_id", "depreciation_account_id",
                       "accumulated_depreciation_account_id", "company_id")
             .insert(P(), P(), P(), P(), P(), P(), P(), P()))
    conn.execute(ins_q.get_sql(),
        (cat_id, args.name, method, useful_life,
         args.asset_account_id, args.depreciation_account_id,
         args.accumulated_depreciation_account_id, args.company_id),
    )

    audit(conn, "erpclaw-assets", "add-asset-category", "asset_category", cat_id,
           new_values={"name": args.name, "depreciation_method": method},
           description=f"Created asset category: {args.name}")

    conn.commit()
    ok({"asset_category_id": cat_id, "name": args.name,
         "message": f"Asset category '{args.name}' created"})


# ---------------------------------------------------------------------------
# 2. list-asset-categories
# ---------------------------------------------------------------------------

def list_asset_categories(conn, args):
    """List all asset categories for a company.

    Required: --company-id
    """
    if not args.company_id:
        err("--company-id is required")

    _validate_company_exists(conn, args.company_id)

    limit = int(args.limit or "20")
    offset = int(args.offset or "0")

    ac_t = Table("asset_category")
    cnt_q = (Q.from_(ac_t).select(fn.Count("*").as_("cnt"))
             .where(ac_t.company_id == P()))
    count_row = conn.execute(cnt_q.get_sql(), (args.company_id,)).fetchone()
    total = count_row["cnt"]

    list_q = (Q.from_(ac_t).select(ac_t.star)
              .where(ac_t.company_id == P())
              .orderby(ac_t.name)
              .limit(P()).offset(P()))
    rows = conn.execute(list_q.get_sql(), (args.company_id, limit, offset)).fetchall()

    categories = [row_to_dict(r) for r in rows]
    ok({"categories": categories, "total": total, "limit": limit, "offset": offset,
         "has_more": offset + limit < total})


# ---------------------------------------------------------------------------
# 3. add-asset
# ---------------------------------------------------------------------------

def add_asset(conn, args):
    """Create a new asset in draft status.

    Required: --company-id, --name, --asset-category-id, --gross-value
    Optional: --salvage-value, --item-id, --purchase-date, --purchase-invoice-id,
              --depreciation-method, --useful-life-years, --depreciation-start-date,
              --location, --custodian-employee-id, --warranty-expiry-date
    """
    if not args.company_id:
        err("--company-id is required")
    if not args.name:
        err("--name is required")
    if not args.asset_category_id:
        err("--asset-category-id is required")
    if not args.gross_value:
        err("--gross-value is required")

    _validate_company_exists(conn, args.company_id)
    category = _validate_asset_category_exists(conn, args.asset_category_id)
    cat_dict = row_to_dict(category)

    gross_value = to_decimal(args.gross_value)
    if gross_value <= 0:
        err("--gross-value must be greater than 0")

    salvage_value = to_decimal(args.salvage_value or "0")
    if salvage_value < 0:
        err("--salvage-value must be >= 0")
    if salvage_value >= gross_value:
        err("--salvage-value must be less than --gross-value")

    # Depreciation method: override from category if not specified
    dep_method = args.depreciation_method or cat_dict["depreciation_method"]
    if dep_method not in VALID_DEPRECIATION_METHODS:
        err(f"Invalid depreciation method '{dep_method}'. Must be one of: {', '.join(VALID_DEPRECIATION_METHODS)}")

    # Useful life: override from category if not specified
    if args.useful_life_years:
        try:
            useful_life = int(args.useful_life_years)
        except (ValueError, TypeError):
            err("--useful-life-years must be an integer")
        if useful_life <= 0:
            err("--useful-life-years must be greater than 0")
    else:
        useful_life = cat_dict["useful_life_years"]

    # Validate item reference if provided
    if args.item_id:
        item_t = Table("item")
        item_q = Q.from_(item_t).select(item_t.id).where(item_t.id == P())
        item = conn.execute(item_q.get_sql(), (args.item_id,)).fetchone()
        if not item:
            err(f"Item {args.item_id} not found")

    # Generate naming series
    naming = get_next_name(conn, "asset", company_id=args.company_id)

    asset_id = str(uuid.uuid4())
    current_book_value = str(round_currency(gross_value))

    asset_t = Table("asset")
    ins_q = (Q.into(asset_t)
             .columns("id", "naming_series", "asset_name", "asset_category_id", "item_id",
                       "purchase_date", "purchase_invoice_id", "gross_value", "salvage_value",
                       "depreciation_method", "useful_life_years", "depreciation_start_date",
                       "current_book_value", "accumulated_depreciation", "status",
                       "location", "custodian_employee_id", "warranty_expiry_date", "company_id")
             .insert(P(), P(), P(), P(), P(), P(), P(), P(), P(), P(), P(), P(),
                     P(), ValueWrapper("0"), ValueWrapper("draft"),
                     P(), P(), P(), P()))
    conn.execute(ins_q.get_sql(),
        (asset_id, naming, args.name, args.asset_category_id, args.item_id,
         args.purchase_date, args.purchase_invoice_id,
         str(round_currency(gross_value)), str(round_currency(salvage_value)),
         dep_method, useful_life, args.depreciation_start_date,
         current_book_value,
         args.location, args.custodian_employee_id, args.warranty_expiry_date,
         args.company_id),
    )

    audit(conn, "erpclaw-assets", "add-asset", "asset", asset_id,
           new_values={"asset_name": args.name, "naming_series": naming,
                       "gross_value": str(round_currency(gross_value))},
           description=f"Created asset: {naming} - {args.name}")

    conn.commit()
    ok({"asset_id": asset_id, "naming_series": naming, "asset_name": args.name,
         "gross_value": str(round_currency(gross_value)),
         "current_book_value": current_book_value,
         "message": f"Asset '{naming}' created in draft status"})


# ---------------------------------------------------------------------------
# 4. update-asset
# ---------------------------------------------------------------------------

def update_asset(conn, args):
    """Update an asset (only draft or submitted).

    Required: --asset-id
    Optional: --name, --location, --custodian-employee-id,
              --warranty-expiry-date, --status
    """
    if not args.asset_id:
        err("--asset-id is required")

    asset = _validate_asset_exists(conn, args.asset_id)
    asset_dict = row_to_dict(asset)

    if asset_dict["status"] not in ("draft", "submitted"):
        err(f"Cannot update asset in '{asset_dict['status']}' status. "
             f"Only draft or submitted assets can be updated.",
             suggestion="Cancel the document first, then make changes.")

    updates = []
    params = []
    old_values = {}
    new_values = {}

    if args.name is not None:
        old_values["asset_name"] = asset_dict["asset_name"]
        new_values["asset_name"] = args.name
        updates.append("asset_name = ?")
        params.append(args.name)

    if args.location is not None:
        old_values["location"] = asset_dict["location"]
        new_values["location"] = args.location
        updates.append("location = ?")
        params.append(args.location)

    if args.custodian_employee_id is not None:
        old_values["custodian_employee_id"] = asset_dict["custodian_employee_id"]
        new_values["custodian_employee_id"] = args.custodian_employee_id
        updates.append("custodian_employee_id = ?")
        params.append(args.custodian_employee_id)

    if args.warranty_expiry_date is not None:
        old_values["warranty_expiry_date"] = asset_dict["warranty_expiry_date"]
        new_values["warranty_expiry_date"] = args.warranty_expiry_date
        updates.append("warranty_expiry_date = ?")
        params.append(args.warranty_expiry_date)

    if args.depreciation_start_date is not None:
        old_values["depreciation_start_date"] = asset_dict["depreciation_start_date"]
        new_values["depreciation_start_date"] = args.depreciation_start_date
        updates.append("depreciation_start_date = ?")
        params.append(args.depreciation_start_date)

    if args.status is not None:
        if not _asset_status_registered(conn, args.status):
            err(f"Invalid status '{args.status}'. Register it in asset_status_registry "
                f"or use a standard state: {', '.join(VALID_ASSET_STATUSES)}")
        old_values["status"] = asset_dict["status"]
        new_values["status"] = args.status
        updates.append("status = ?")
        params.append(args.status)

    if not updates:
        err("No fields to update. Provide at least one of: --name, --location, "
             "--custodian-employee-id, --warranty-expiry-date, --depreciation-start-date, --status")

    updates.append("updated_at = datetime('now')")
    params.append(args.asset_id)

    # raw SQL — dynamic column building based on which args are provided
    conn.execute(
        f"UPDATE asset SET {', '.join(updates)} WHERE id = ?",
        params,
    )

    audit(conn, "erpclaw-assets", "update-asset", "asset", args.asset_id,
           old_values=old_values, new_values=new_values,
           description=f"Updated asset {asset_dict['naming_series']}")

    conn.commit()
    ok({"asset_id": args.asset_id, "updated_fields": list(new_values.keys()),
         "message": f"Asset {asset_dict['naming_series']} updated"})


# ---------------------------------------------------------------------------
# 5. get-asset
# ---------------------------------------------------------------------------

def get_asset(conn, args):
    """Get an asset with depreciation schedule, movements, maintenance records.

    Required: --asset-id
    """
    if not args.asset_id:
        err("--asset-id is required")

    asset = _validate_asset_exists(conn, args.asset_id)
    asset_dict = row_to_dict(asset)

    # Fetch depreciation schedule
    ds_t = Table("depreciation_schedule")
    ds_q = (Q.from_(ds_t).select(ds_t.star)
            .where(ds_t.asset_id == P())
            .orderby(ds_t.schedule_date))
    schedule_rows = conn.execute(ds_q.get_sql(), (args.asset_id,)).fetchall()
    asset_dict["depreciation_schedule"] = [row_to_dict(r) for r in schedule_rows]

    # Fetch asset movements
    am_t = Table("asset_movement")
    am_q = (Q.from_(am_t).select(am_t.star)
            .where(am_t.asset_id == P())
            .orderby(am_t.movement_date, order=Order.desc))
    movement_rows = conn.execute(am_q.get_sql(), (args.asset_id,)).fetchall()
    asset_dict["movements"] = [row_to_dict(r) for r in movement_rows]

    # Fetch maintenance records
    mnt_t = Table("asset_maintenance")
    mnt_q = (Q.from_(mnt_t).select(mnt_t.star)
             .where(mnt_t.asset_id == P())
             .orderby(mnt_t.scheduled_date, order=Order.desc))
    maintenance_rows = conn.execute(mnt_q.get_sql(), (args.asset_id,)).fetchall()
    asset_dict["maintenance"] = [row_to_dict(r) for r in maintenance_rows]

    # Fetch disposal record if any
    ad_t = Table("asset_disposal")
    ad_q = Q.from_(ad_t).select(ad_t.star).where(ad_t.asset_id == P())
    disposal_row = conn.execute(ad_q.get_sql(), (args.asset_id,)).fetchone()
    asset_dict["disposal"] = row_to_dict(disposal_row) if disposal_row else None

    # Fetch category info
    ac_t = Table("asset_category")
    ac_q = Q.from_(ac_t).select(ac_t.star).where(ac_t.id == P())
    cat_row = conn.execute(ac_q.get_sql(), (asset_dict["asset_category_id"],)).fetchone()
    asset_dict["category"] = row_to_dict(cat_row) if cat_row else None

    ok({"asset": asset_dict})


# ---------------------------------------------------------------------------
# 6. list-assets
# ---------------------------------------------------------------------------

def list_assets(conn, args):
    """List assets with filters.

    Optional: --company-id, --asset-category-id, --status, --search,
              --limit, --offset
    """
    a = Table("asset").as_("a")
    ac = Table("asset_category").as_("ac")
    params = []

    # Build dynamic WHERE with PyPika
    cnt_q = Q.from_(a).select(fn.Count("*").as_("cnt"))
    if args.company_id:
        cnt_q = cnt_q.where(a.company_id == P())
        params.append(args.company_id)
    if args.asset_category_id:
        cnt_q = cnt_q.where(a.asset_category_id == P())
        params.append(args.asset_category_id)
    if args.status:
        cnt_q = cnt_q.where(a.status == P())
        params.append(args.status)
    if args.search:
        cnt_q = cnt_q.where(a.asset_name.like(P()))
        params.append(f"%{args.search}%")

    limit = int(args.limit or "20")
    offset = int(args.offset or "0")

    # Get total count
    count_row = conn.execute(cnt_q.get_sql(), params).fetchone()
    total = count_row["cnt"]

    # Fetch assets with JOIN
    list_q = (Q.from_(a).select(a.star, ac.name.as_("category_name"))
              .left_join(ac).on(ac.id == a.asset_category_id))
    if args.company_id:
        list_q = list_q.where(a.company_id == P())
    if args.asset_category_id:
        list_q = list_q.where(a.asset_category_id == P())
    if args.status:
        list_q = list_q.where(a.status == P())
    if args.search:
        list_q = list_q.where(a.asset_name.like(P()))
    list_q = (list_q.orderby(a.created_at, order=Order.desc)
              .limit(P()).offset(P()))

    rows = conn.execute(list_q.get_sql(), params + [limit, offset]).fetchall()

    assets = [row_to_dict(r) for r in rows]
    ok({"assets": assets, "total": total, "limit": limit, "offset": offset,
         "has_more": offset + limit < total})


# ---------------------------------------------------------------------------
# 7. generate-depreciation-schedule
# ---------------------------------------------------------------------------

def _generate_schedule_core(conn, asset_dict):
    """Build + insert the full pending depreciation schedule for an asset from its
    gross value over its useful life. Deletes any existing pending rows first
    (idempotent regeneration). Raises ValueError on invalid config (caller maps to
    err). Does NOT commit — the caller owns the transaction. Returns
    (schedule_entries, dep_method, depreciable_amount).

    Shared by generate-depreciation-schedule (initial schedule) and
    transfer-cwip-to-asset (schedule seeded from the transfer date)."""
    dep_method = asset_dict["depreciation_method"]
    if not dep_method:
        raise ValueError("Asset has no depreciation method set")

    useful_life = asset_dict["useful_life_years"]
    if not useful_life or useful_life <= 0:
        raise ValueError("Asset has no valid useful life years set")

    start_date = asset_dict["depreciation_start_date"]
    if not start_date:
        raise ValueError("Asset has no depreciation_start_date set. "
                         "Update the asset with --depreciation-start-date first.")

    gross_value = to_decimal(asset_dict["gross_value"])
    salvage_value = to_decimal(asset_dict["salvage_value"])
    depreciable_amount = gross_value - salvage_value

    if depreciable_amount <= 0:
        raise ValueError("Depreciable amount (gross_value - salvage_value) must be > 0")

    total_months = useful_life * 12

    # Delete existing pending schedule entries (allow regeneration)
    ds_t = Table("depreciation_schedule")
    del_q = (Q.from_(ds_t).delete()
             .where(ds_t.asset_id == P())
             .where(ds_t.status == ValueWrapper("pending")))
    conn.execute(del_q.get_sql(), (asset_dict["id"],))

    schedule_entries = []
    accumulated = Decimal("0")
    book_value = gross_value

    if dep_method == "straight_line":
        # Fixed monthly amount
        monthly_amount = round_currency(depreciable_amount / Decimal(str(total_months)))

        for i in range(total_months):
            schedule_date = _add_months(start_date, i)

            # Last month: adjust for rounding to exactly match depreciable amount
            if i == total_months - 1:
                this_amount = depreciable_amount - accumulated
            else:
                this_amount = monthly_amount

            this_amount = round_currency(this_amount)
            accumulated = round_currency(accumulated + this_amount)
            book_value = round_currency(gross_value - accumulated)

            # Don't go below salvage value
            if book_value < salvage_value:
                this_amount = round_currency(this_amount - (salvage_value - book_value))
                accumulated = round_currency(gross_value - salvage_value)
                book_value = salvage_value

            if this_amount <= 0:
                break

            entry_id = str(uuid.uuid4())
            schedule_entries.append({
                "id": entry_id,
                "asset_id": asset_dict["id"],
                "schedule_date": schedule_date,
                "depreciation_amount": str(this_amount),
                "accumulated_amount": str(accumulated),
                "book_value_after": str(book_value),
                "status": "pending",
                "fiscal_year": schedule_date[:4],
            })

    elif dep_method == "written_down_value":
        # annual_rate = 1 - (salvage / gross) ^ (1 / useful_life)
        if salvage_value <= 0:
            # If salvage is zero, WDV rate is undefined; use straight_line
            # fallback or use a high rate
            raise ValueError("Written down value method requires salvage_value > 0")

        ratio = salvage_value / gross_value
        # ratio ^ (1/useful_life) using Decimal
        exponent = Decimal("1") / Decimal(str(useful_life))
        # Use float for power calculation, then convert back
        ratio_float = float(ratio)
        exponent_float = float(exponent)
        annual_rate = Decimal(str(1 - ratio_float ** exponent_float))

        for i in range(total_months):
            schedule_date = _add_months(start_date, i)

            # Monthly depreciation = book_value * annual_rate / 12
            this_amount = round_currency(book_value * annual_rate / Decimal("12"))

            # Don't depreciate below salvage value
            if book_value - this_amount < salvage_value:
                this_amount = round_currency(book_value - salvage_value)

            if this_amount <= 0:
                break

            accumulated = round_currency(accumulated + this_amount)
            book_value = round_currency(book_value - this_amount)

            entry_id = str(uuid.uuid4())
            schedule_entries.append({
                "id": entry_id,
                "asset_id": asset_dict["id"],
                "schedule_date": schedule_date,
                "depreciation_amount": str(this_amount),
                "accumulated_amount": str(accumulated),
                "book_value_after": str(book_value),
                "status": "pending",
                "fiscal_year": schedule_date[:4],
            })

    elif dep_method == "double_declining":
        # annual_rate = 2 / useful_life
        annual_rate = Decimal("2") / Decimal(str(useful_life))

        for i in range(total_months):
            schedule_date = _add_months(start_date, i)

            # Monthly depreciation = book_value * annual_rate / 12
            this_amount = round_currency(book_value * annual_rate / Decimal("12"))

            # Don't depreciate below salvage value
            if book_value - this_amount < salvage_value:
                this_amount = round_currency(book_value - salvage_value)

            if this_amount <= 0:
                break

            accumulated = round_currency(accumulated + this_amount)
            book_value = round_currency(book_value - this_amount)

            entry_id = str(uuid.uuid4())
            schedule_entries.append({
                "id": entry_id,
                "asset_id": asset_dict["id"],
                "schedule_date": schedule_date,
                "depreciation_amount": str(this_amount),
                "accumulated_amount": str(accumulated),
                "book_value_after": str(book_value),
                "status": "pending",
                "fiscal_year": schedule_date[:4],
            })

    else:
        raise ValueError(f"Unsupported depreciation method: {dep_method}")

    # Insert schedule entries
    ds_ins_q = (Q.into(ds_t)
                .columns("id", "asset_id", "schedule_date", "depreciation_amount",
                          "accumulated_amount", "book_value_after", "status", "fiscal_year")
                .insert(P(), P(), P(), P(), P(), P(), P(), P()))
    ds_ins_sql = ds_ins_q.get_sql()
    for entry in schedule_entries:
        conn.execute(ds_ins_sql,
            (entry["id"], entry["asset_id"], entry["schedule_date"],
             entry["depreciation_amount"], entry["accumulated_amount"],
             entry["book_value_after"], entry["status"], entry["fiscal_year"]),
        )

    return schedule_entries, dep_method, depreciable_amount


def generate_depreciation_schedule(conn, args):
    """Generate monthly depreciation schedule for an asset.

    Required: --asset-id

    Depreciation methods:
    - straight_line: monthly = (gross - salvage) / (years * 12)
    - written_down_value: annual_rate = 1 - (salvage/gross)^(1/years),
                          monthly = book_value * annual_rate / 12
    - double_declining: annual_rate = 2 / years,
                        monthly = book_value * annual_rate / 12
    """
    if not args.asset_id:
        err("--asset-id is required")

    asset = _validate_asset_exists(conn, args.asset_id)
    asset_dict = row_to_dict(asset)

    try:
        schedule_entries, dep_method, depreciable_amount = _generate_schedule_core(conn, asset_dict)
    except ValueError as e:
        err(str(e))

    total_months = (asset_dict["useful_life_years"] or 0) * 12
    audit(conn, "erpclaw-assets", "generate-depreciation-schedule", "asset", args.asset_id,
           new_values={"entries_count": len(schedule_entries),
                       "method": dep_method,
                       "total_months": total_months},
           description=f"Generated {len(schedule_entries)} depreciation schedule entries")

    conn.commit()
    ok({"asset_id": args.asset_id,
         "entries_generated": len(schedule_entries),
         "depreciation_method": dep_method,
         "total_depreciable_amount": str(depreciable_amount),
         "schedule": schedule_entries,
         "message": f"Generated {len(schedule_entries)} depreciation schedule entries"})


# ---------------------------------------------------------------------------
# 8. post-depreciation
# ---------------------------------------------------------------------------

def post_depreciation(conn, args):
    """Post a single depreciation entry with GL entries.

    Required: --depreciation-schedule-id (or --asset-id + --posting-date)
    Optional: --cost-center-id

    GL entries:
    - DR Depreciation Expense account (from category)
    - CR Accumulated Depreciation account (from category)
    """
    schedule_entry = None
    ds_t = Table("depreciation_schedule")

    if args.depreciation_schedule_id:
        ds_q = Q.from_(ds_t).select(ds_t.star).where(ds_t.id == P())
        schedule_entry = conn.execute(ds_q.get_sql(), (args.depreciation_schedule_id,)).fetchone()
        if not schedule_entry:
            err(f"Depreciation schedule entry {args.depreciation_schedule_id} not found")
    elif args.asset_id and args.posting_date:
        # Find the schedule entry for this asset on or before posting_date
        ds_q = (Q.from_(ds_t).select(ds_t.star)
                .where(ds_t.asset_id == P())
                .where(ds_t.schedule_date <= P())
                .where(ds_t.status == ValueWrapper("pending"))
                .orderby(ds_t.schedule_date)
                .limit(1))
        schedule_entry = conn.execute(ds_q.get_sql(), (args.asset_id, args.posting_date)).fetchone()
        if not schedule_entry:
            err(f"No pending depreciation schedule entry found for asset {args.asset_id} "
                 f"on or before {args.posting_date}")
    else:
        err("Provide --depreciation-schedule-id, or --asset-id + --posting-date")

    sched_dict = row_to_dict(schedule_entry)

    if sched_dict["status"] != "pending":
        err(f"Schedule entry is already '{sched_dict['status']}', not 'pending'")

    # Fetch asset
    asset = _validate_asset_exists(conn, sched_dict["asset_id"])
    asset_dict = row_to_dict(asset)

    if asset_dict["status"] not in ("submitted", "in_use"):
        err(f"Asset status is '{asset_dict['status']}'. "
             f"Only submitted or in_use assets can have depreciation posted.")

    # Fetch category for accounts
    category = _validate_asset_category_exists(conn, asset_dict["asset_category_id"])
    cat_dict = row_to_dict(category)

    dep_account_id = cat_dict.get("depreciation_account_id")
    accum_dep_account_id = cat_dict.get("accumulated_depreciation_account_id")

    if not dep_account_id:
        err("Asset category has no depreciation_account_id set")
    if not accum_dep_account_id:
        err("Asset category has no accumulated_depreciation_account_id set")

    posting_date = args.posting_date or sched_dict["schedule_date"]
    dep_amount = sched_dict["depreciation_amount"]

    # Fiscal year
    fiscal_year = _get_fiscal_year(conn, posting_date)

    # Cost center
    cost_center_id = args.cost_center_id or _get_cost_center(conn, asset_dict["company_id"])

    # Prepare GL entries
    voucher_id = sched_dict["id"]
    gl_entries = [
        {
            "account_id": dep_account_id,
            "debit": dep_amount,
            "credit": "0",
            "cost_center_id": cost_center_id,
            "fiscal_year": fiscal_year,
        },
        {
            "account_id": accum_dep_account_id,
            "debit": "0",
            "credit": dep_amount,
            "cost_center_id": cost_center_id,
            "fiscal_year": fiscal_year,
        },
    ]

    try:
        gl_ids = insert_gl_entries(
            conn, gl_entries,
            voucher_type="depreciation_entry",
            voucher_id=voucher_id,
            posting_date=posting_date,
            company_id=asset_dict["company_id"],
            remarks=f"Depreciation for {asset_dict['naming_series']} on {posting_date}",
        )
    except (ValueError, NotImplementedError) as e:
        sys.stderr.write(f"[erpclaw-assets] {e}\n")
        err(f"GL posting failed: {e}")

    # Update depreciation_schedule entry
    ds_upd_q = (Q.update(ds_t)
                .set(Field("status"), ValueWrapper("posted"))
                .set(Field("journal_entry_id"), P())
                .where(ds_t.id == P()))
    conn.execute(ds_upd_q.get_sql(), (voucher_id, sched_dict["id"]))

    # Update asset: current_book_value and accumulated_depreciation
    new_accum = round_currency(
        to_decimal(asset_dict["accumulated_depreciation"]) + to_decimal(dep_amount)
    )
    new_book_value = round_currency(
        to_decimal(asset_dict["gross_value"]) - new_accum
    )

    asset_t = Table("asset")
    asset_upd_q = (Q.update(asset_t)
                   .set(Field("current_book_value"), P())
                   .set(Field("accumulated_depreciation"), P())
                   .set(Field("updated_at"), LiteralValue("datetime('now')"))
                   .where(asset_t.id == P()))
    conn.execute(asset_upd_q.get_sql(), (str(new_book_value), str(new_accum), sched_dict["asset_id"]))

    audit(conn, "erpclaw-assets", "post-depreciation", "asset", sched_dict["asset_id"],
           old_values={"current_book_value": asset_dict["current_book_value"],
                       "accumulated_depreciation": asset_dict["accumulated_depreciation"]},
           new_values={"current_book_value": str(new_book_value),
                       "accumulated_depreciation": str(new_accum)},
           description=f"Posted depreciation of {dep_amount} for {asset_dict['naming_series']}")

    conn.commit()
    ok({"asset_id": sched_dict["asset_id"],
         "schedule_id": sched_dict["id"],
         "depreciation_amount": dep_amount,
         "new_book_value": str(new_book_value),
         "new_accumulated_depreciation": str(new_accum),
         "gl_entry_ids": gl_ids,
         "message": f"Depreciation of {dep_amount} posted for {asset_dict['naming_series']}"})


# ---------------------------------------------------------------------------
# 9. run-depreciation
# ---------------------------------------------------------------------------

def run_depreciation(conn, args):
    """Batch depreciation posting for all pending entries up to a date.

    Required: --company-id, --posting-date
    Optional: --cost-center-id

    Finds all pending depreciation_schedule entries with schedule_date <= posting_date
    for assets in this company and posts each one.
    """
    if not args.company_id:
        err("--company-id is required")
    if not args.posting_date:
        err("--posting-date is required")

    _validate_company_exists(conn, args.company_id)

    # Find all pending entries for assets in this company
    ds_t = Table("depreciation_schedule").as_("ds")
    a_t = Table("asset").as_("a")
    pend_q = (Q.from_(ds_t)
              .join(a_t).on(a_t.id == ds_t.asset_id)
              .select(ds_t.star, a_t.company_id, a_t.asset_category_id,
                      a_t.naming_series, a_t.gross_value,
                      a_t.accumulated_depreciation, a_t.current_book_value,
                      a_t.status.as_("asset_status"))
              .where(a_t.company_id == P())
              .where(ds_t.schedule_date <= P())
              .where(ds_t.status == ValueWrapper("pending"))
              .where(a_t.status.isin([ValueWrapper("submitted"), ValueWrapper("in_use")]))
              .orderby(ds_t.schedule_date))
    pending_entries = conn.execute(pend_q.get_sql(), (args.company_id, args.posting_date)).fetchall()

    if not pending_entries:
        ok({"entries_posted": 0, "message": "No pending depreciation entries found"})

    cost_center_id = args.cost_center_id or _get_cost_center(conn, args.company_id)
    fiscal_year = _get_fiscal_year(conn, args.posting_date)

    posted_count = 0
    errors = []
    posted_details = []

    for entry_row in pending_entries:
        entry = row_to_dict(entry_row)

        # Fetch category for accounts
        ac_t = Table("asset_category")
        ac_q = Q.from_(ac_t).select(ac_t.star).where(ac_t.id == P())
        cat = conn.execute(ac_q.get_sql(), (entry["asset_category_id"],)).fetchone()
        if not cat:
            errors.append(f"Category not found for asset {entry['asset_id']}")
            continue
        cat_dict = row_to_dict(cat)

        dep_account_id = cat_dict.get("depreciation_account_id")
        accum_dep_account_id = cat_dict.get("accumulated_depreciation_account_id")

        if not dep_account_id or not accum_dep_account_id:
            errors.append(
                f"Missing depreciation accounts for category {cat_dict['name']} "
                f"(asset {entry['naming_series']})"
            )
            continue

        dep_amount = entry["depreciation_amount"]
        voucher_id = entry["id"]

        gl_entries = [
            {
                "account_id": dep_account_id,
                "debit": dep_amount,
                "credit": "0",
                "cost_center_id": cost_center_id,
                "fiscal_year": fiscal_year,
            },
            {
                "account_id": accum_dep_account_id,
                "debit": "0",
                "credit": dep_amount,
                "cost_center_id": cost_center_id,
                "fiscal_year": fiscal_year,
            },
        ]

        try:
            gl_ids = insert_gl_entries(
                conn, gl_entries,
                voucher_type="depreciation_entry",
                voucher_id=voucher_id,
                posting_date=args.posting_date,
                company_id=args.company_id,
                remarks=f"Batch depreciation for {entry['naming_series']}",
            )
        except (ValueError, NotImplementedError) as e:
            sys.stderr.write(f"[erpclaw-assets] GL posting for {entry['naming_series']}: {e}\n")
            errors.append(f"GL posting failed for {entry['naming_series']}")
            continue

        # Update schedule entry
        ds_upd_t = Table("depreciation_schedule")
        ds_upd_q = (Q.update(ds_upd_t)
                    .set(Field("status"), ValueWrapper("posted"))
                    .set(Field("journal_entry_id"), P())
                    .where(ds_upd_t.id == P()))
        conn.execute(ds_upd_q.get_sql(), (voucher_id, entry["id"]))

        # Re-read asset for current values (may have been updated by previous entry in batch)
        asset_rd_t = Table("asset")
        asset_rd_q = (Q.from_(asset_rd_t)
                      .select(asset_rd_t.accumulated_depreciation, asset_rd_t.gross_value)
                      .where(asset_rd_t.id == P()))
        current_asset = conn.execute(asset_rd_q.get_sql(), (entry["asset_id"],)).fetchone()
        current_asset_dict = row_to_dict(current_asset)

        new_accum = round_currency(
            to_decimal(current_asset_dict["accumulated_depreciation"]) + to_decimal(dep_amount)
        )
        new_book_value = round_currency(
            to_decimal(current_asset_dict["gross_value"]) - new_accum
        )

        asset_upd_q = (Q.update(asset_rd_t)
                       .set(Field("current_book_value"), P())
                       .set(Field("accumulated_depreciation"), P())
                       .set(Field("updated_at"), LiteralValue("datetime('now')"))
                       .where(asset_rd_t.id == P()))
        conn.execute(asset_upd_q.get_sql(), (str(new_book_value), str(new_accum), entry["asset_id"]))

        posted_count += 1
        posted_details.append({
            "asset_id": entry["asset_id"],
            "naming_series": entry["naming_series"],
            "schedule_id": entry["id"],
            "amount": dep_amount,
            "new_book_value": str(new_book_value),
        })

    audit(conn, "erpclaw-assets", "run-depreciation", "asset", args.company_id,
           new_values={"posted_count": posted_count, "posting_date": args.posting_date},
           description=f"Batch depreciation: {posted_count} entries posted")

    conn.commit()

    result = {
        "entries_posted": posted_count,
        "posting_date": args.posting_date,
        "details": posted_details,
        "message": f"Posted {posted_count} depreciation entries",
    }
    if errors:
        result["errors"] = errors

    ok(result)


# ---------------------------------------------------------------------------
# 10. record-asset-movement
# ---------------------------------------------------------------------------

def record_asset_movement(conn, args):
    """Record an asset movement (transfer, issue, receipt).

    Required: --asset-id, --movement-type, --movement-date
    Optional: --from-location, --to-location, --from-employee-id,
              --to-employee-id, --reason
    """
    if not args.asset_id:
        err("--asset-id is required")
    if not args.movement_type:
        err("--movement-type is required")
    if not args.movement_date:
        err("--movement-date is required")

    asset = _validate_asset_exists(conn, args.asset_id)
    asset_dict = row_to_dict(asset)

    movement_type = args.movement_type
    if movement_type not in VALID_MOVEMENT_TYPES:
        err(f"Invalid movement type '{movement_type}'. "
             f"Must be one of: {', '.join(VALID_MOVEMENT_TYPES)}")

    # Validate movement makes sense
    if movement_type == "transfer":
        if not args.to_location and not args.to_employee_id:
            err("Transfer requires --to-location or --to-employee-id")

    movement_id = str(uuid.uuid4())

    am_t = Table("asset_movement")
    am_ins_q = (Q.into(am_t)
                .columns("id", "asset_id", "movement_type", "from_location", "to_location",
                          "from_employee_id", "to_employee_id", "movement_date", "reason")
                .insert(P(), P(), P(), P(), P(), P(), P(), P(), P()))
    conn.execute(am_ins_q.get_sql(),
        (movement_id, args.asset_id, movement_type,
         args.from_location or asset_dict.get("location"),
         args.to_location,
         args.from_employee_id or asset_dict.get("custodian_employee_id"),
         args.to_employee_id,
         args.movement_date, args.reason),
    )

    # Update asset location and custodian based on movement
    update_fields = []
    update_params = []
    old_values = {}
    new_values = {}

    if args.to_location is not None:
        old_values["location"] = asset_dict.get("location")
        new_values["location"] = args.to_location
        update_fields.append("location = ?")
        update_params.append(args.to_location)

    if args.to_employee_id is not None:
        old_values["custodian_employee_id"] = asset_dict.get("custodian_employee_id")
        new_values["custodian_employee_id"] = args.to_employee_id
        update_fields.append("custodian_employee_id = ?")
        update_params.append(args.to_employee_id)

    if update_fields:
        update_fields.append("updated_at = datetime('now')")
        update_params.append(args.asset_id)
        # raw SQL — dynamic column building based on which movement fields are provided
        conn.execute(
            f"UPDATE asset SET {', '.join(update_fields)} WHERE id = ?",
            update_params,
        )

    audit(conn, "erpclaw-assets", "record-asset-movement", "asset_movement", movement_id,
           old_values=old_values, new_values=new_values,
           description=f"Asset movement ({movement_type}) for {asset_dict['naming_series']}")

    conn.commit()
    ok({"movement_id": movement_id, "asset_id": args.asset_id,
         "movement_type": movement_type,
         "message": f"Asset movement recorded for {asset_dict['naming_series']}"})


# ---------------------------------------------------------------------------
# 11. schedule-maintenance
# ---------------------------------------------------------------------------

def schedule_maintenance(conn, args):
    """Schedule a maintenance task for an asset.

    Required: --asset-id, --maintenance-type, --scheduled-date
    Optional: --description, --next-due-date, --is-capex (0/1; default 0=opex)
    """
    if not args.asset_id:
        err("--asset-id is required")
    if not args.maintenance_type:
        err("--maintenance-type is required")
    if not args.scheduled_date:
        err("--scheduled-date is required")

    asset = _validate_asset_exists(conn, args.asset_id)
    asset_dict = row_to_dict(asset)

    maint_type = args.maintenance_type
    if maint_type not in VALID_MAINTENANCE_TYPES:
        err(f"Invalid maintenance type '{maint_type}'. "
             f"Must be one of: {', '.join(VALID_MAINTENANCE_TYPES)}")

    is_capex = _parse_is_capex(args.is_capex)

    maint_id = str(uuid.uuid4())

    mnt_t = Table("asset_maintenance")
    mnt_ins_q = (Q.into(mnt_t)
                 .columns("id", "asset_id", "maintenance_type", "scheduled_date",
                           "description", "next_due_date", "status", "is_capex")
                 .insert(P(), P(), P(), P(), P(), P(), ValueWrapper("planned"), P()))
    conn.execute(mnt_ins_q.get_sql(),
        (maint_id, args.asset_id, maint_type, args.scheduled_date,
         args.description, args.next_due_date, is_capex),
    )

    audit(conn, "erpclaw-assets", "schedule-maintenance", "asset_maintenance", maint_id,
           new_values={"maintenance_type": maint_type,
                       "scheduled_date": args.scheduled_date},
           description=f"Scheduled {maint_type} maintenance for {asset_dict['naming_series']}")

    conn.commit()
    ok({"maintenance_id": maint_id, "asset_id": args.asset_id,
         "maintenance_type": maint_type,
         "scheduled_date": args.scheduled_date,
         "is_capex": is_capex,
         "message": f"Maintenance scheduled for {asset_dict['naming_series']}"})


# ---------------------------------------------------------------------------
# 12. complete-maintenance
# ---------------------------------------------------------------------------

def complete_maintenance(conn, args):
    """Mark a maintenance task as completed, branching on capex vs opex (M7).

    Required: --maintenance-id
    Optional: --actual-date (defaults to today), --cost, --performed-by,
              --description, --is-capex (override the stored flag),
              --cash-account-id (credit leg), --expense-account-id (opex DR),
              --cost-center-id

    capex (is_capex=1): requires the asset in_use + a positive cost.
      GL: DR Fixed Asset (category asset_account) / CR --cash-account-id; the
      cost is capitalized into gross_value and the depreciation schedule is
      recomputed from the new book value (voucher_type asset_repair_capex).
    opex (is_capex=0): if a positive cost + --cash-account-id + --expense-account-id
      are given, GL: DR Repair Expense / CR Cash (voucher_type asset_repair_capex);
      otherwise the task is just marked completed (legacy behavior preserved).
    """
    if not args.maintenance_id:
        err("--maintenance-id is required")

    mnt_t = Table("asset_maintenance")
    mnt_q = Q.from_(mnt_t).select(mnt_t.star).where(mnt_t.id == P())
    maint = conn.execute(mnt_q.get_sql(), (args.maintenance_id,)).fetchone()
    if not maint:
        err(f"Maintenance record {args.maintenance_id} not found")

    maint_dict = row_to_dict(maint)

    if maint_dict["status"] == "completed":
        err("Maintenance is already completed")

    actual_date = args.actual_date or _today_str()
    cost = args.cost or maint_dict.get("cost", "0")
    performed_by = args.performed_by
    description = args.description or maint_dict.get("description")

    # Effective capex flag: an explicit --is-capex overrides the stored column.
    if args.is_capex is not None:
        is_capex = _parse_is_capex(args.is_capex)
    else:
        is_capex = int(maint_dict.get("is_capex") or 0)

    cost_dec = to_decimal(cost or "0")
    gl_ids = []
    recompute = None
    branch = "opex"

    # Load the owning asset (needed for both GL paths).
    asset = _validate_asset_exists(conn, maint_dict["asset_id"])
    asset_dict = row_to_dict(asset)
    cat_dict, asset_acct, dep_acct, accum_acct = _category_accounts(conn, asset_dict)
    cost_center_id = args.cost_center_id or _get_cost_center(conn, asset_dict["company_id"])
    fiscal_year = _get_fiscal_year(conn, actual_date)

    if is_capex == 1:
        branch = "capex"
        # Validation: capitalizing into an asset requires it to be in use.
        if asset_dict["status"] != "in_use":
            err(f"Capex maintenance requires the asset in 'in_use' status "
                f"(currently '{asset_dict['status']}').")
        if cost_dec <= 0:
            err("Capex maintenance requires a positive --cost to capitalize.")
        if not args.cash_account_id:
            err("--cash-account-id is required for capex maintenance (credit leg).")
        if not asset_acct:
            err("Asset category has no asset_account_id set")

        gl_entries = [
            {"account_id": asset_acct, "debit": str(round_currency(cost_dec)), "credit": "0",
             "cost_center_id": cost_center_id, "fiscal_year": fiscal_year},
            {"account_id": args.cash_account_id, "debit": "0", "credit": str(round_currency(cost_dec)),
             "cost_center_id": cost_center_id, "fiscal_year": fiscal_year},
        ]
        try:
            gl_ids = insert_gl_entries(
                conn, gl_entries, voucher_type="asset_repair_capex",
                voucher_id=args.maintenance_id, posting_date=actual_date,
                company_id=asset_dict["company_id"],
                remarks=f"Capex maintenance on {asset_dict['naming_series']}")
        except (ValueError, NotImplementedError) as e:
            sys.stderr.write(f"[erpclaw-assets] {e}\n")
            err(f"GL posting failed: {e}")

        # Capitalize into gross_value (carrying-value invariant) ...
        new_gross = round_currency(to_decimal(asset_dict["gross_value"]) + cost_dec)
        new_book = round_currency(to_decimal(asset_dict["current_book_value"]) + cost_dec)
        asset_t = Table("asset")
        conn.execute((Q.update(asset_t)
                      .set(Field("gross_value"), P())
                      .set(Field("current_book_value"), P())
                      .set(Field("updated_at"), LiteralValue("datetime('now')"))
                      .where(asset_t.id == P())).get_sql(),
                     (str(new_gross), str(new_book), maint_dict["asset_id"]))
        # ... and ALWAYS recompute depreciation (the capex path must not skip it).
        recompute = _recompute_pending_depreciation(conn, maint_dict["asset_id"])

    elif cost_dec > 0 and args.cash_account_id and args.expense_account_id:
        # Opex with explicit accounts: expense it. (Without accounts, legacy
        # behavior — just mark completed, no GL — is preserved.)
        gl_entries = [
            {"account_id": args.expense_account_id, "debit": str(round_currency(cost_dec)), "credit": "0",
             "cost_center_id": cost_center_id, "fiscal_year": fiscal_year},
            {"account_id": args.cash_account_id, "debit": "0", "credit": str(round_currency(cost_dec)),
             "cost_center_id": cost_center_id, "fiscal_year": fiscal_year},
        ]
        try:
            gl_ids = insert_gl_entries(
                conn, gl_entries, voucher_type="asset_repair_capex",
                voucher_id=args.maintenance_id, posting_date=actual_date,
                company_id=asset_dict["company_id"],
                remarks=f"Repair (opex) on {asset_dict['naming_series']}")
        except (ValueError, NotImplementedError) as e:
            sys.stderr.write(f"[erpclaw-assets] {e}\n")
            err(f"GL posting failed: {e}")

    old_values = {"status": maint_dict["status"]}

    mnt_upd_q = (Q.update(mnt_t)
                 .set(Field("status"), ValueWrapper("completed"))
                 .set(Field("actual_date"), P())
                 .set(Field("cost"), P())
                 .set(Field("performed_by"), P())
                 .set(Field("description"), P())
                 .set(Field("is_capex"), P())
                 .set(Field("updated_at"), LiteralValue("datetime('now')"))
                 .where(mnt_t.id == P()))
    conn.execute(mnt_upd_q.get_sql(),
        (actual_date, cost, performed_by, description, is_capex, args.maintenance_id),
    )

    audit(conn, "erpclaw-assets", "complete-maintenance", "asset_maintenance", args.maintenance_id,
           old_values=old_values,
           new_values={"status": "completed", "actual_date": actual_date, "cost": cost,
                       "is_capex": is_capex, "branch": branch},
           description=f"Completed maintenance {args.maintenance_id} ({branch})")

    conn.commit()
    ok({"maintenance_id": args.maintenance_id,
         "actual_date": actual_date,
         "cost": cost,
         "is_capex": is_capex,
         "branch": branch,
         "schedule_recompute": recompute,
         "gl_entry_ids": gl_ids,
         "message": f"Maintenance completed ({branch})"})


# ---------------------------------------------------------------------------
# 13. dispose-asset
# ---------------------------------------------------------------------------

def dispose_asset(conn, args):
    """Dispose of an asset (sale, scrap, or write_off).

    Required: --asset-id, --disposal-date, --disposal-method
    Optional: --sale-amount, --buyer-details, --cost-center-id

    GL entries for disposal:
    - DR Accumulated Depreciation (full accumulated amount)
    - DR Asset Disposal/Loss (if loss) OR CR Gain on Disposal (if gain)
    - CR Asset Account (gross_value)

    For scrap/write_off: sale_amount = 0, loss = current_book_value
    """
    if not args.asset_id:
        err("--asset-id is required")
    if not args.disposal_date:
        err("--disposal-date is required")
    if not args.disposal_method:
        err("--disposal-method is required")

    asset = _validate_asset_exists(conn, args.asset_id)
    asset_dict = row_to_dict(asset)

    if asset_dict["status"] in ("scrapped", "sold"):
        err(f"Asset is already '{asset_dict['status']}'. Cannot dispose again.")

    if asset_dict["status"] == "draft":
        err("Cannot dispose a draft asset. Submit the asset first.")

    disposal_method = args.disposal_method
    if disposal_method not in VALID_DISPOSAL_METHODS:
        err(f"Invalid disposal method '{disposal_method}'. "
             f"Must be one of: {', '.join(VALID_DISPOSAL_METHODS)}")

    # Fetch category for accounts
    category = _validate_asset_category_exists(conn, asset_dict["asset_category_id"])
    cat_dict = row_to_dict(category)

    asset_account_id = cat_dict.get("asset_account_id")
    accum_dep_account_id = cat_dict.get("accumulated_depreciation_account_id")
    dep_account_id = cat_dict.get("depreciation_account_id")

    if not asset_account_id:
        err("Asset category has no asset_account_id set")
    if not accum_dep_account_id:
        err("Asset category has no accumulated_depreciation_account_id set")
    if not dep_account_id:
        err("Asset category has no depreciation_account_id set (needed for gain/loss)")

    gross_value = to_decimal(asset_dict["gross_value"])
    current_book_value = to_decimal(asset_dict["current_book_value"])
    accumulated_depreciation = to_decimal(asset_dict["accumulated_depreciation"])

    # Determine sale amount
    if disposal_method == "sale":
        sale_amount = to_decimal(args.sale_amount or "0")
    else:
        # Scrap / write_off: no sale proceeds
        sale_amount = Decimal("0")

    # Calculate gain or loss
    # gain_or_loss = sale_amount - current_book_value
    # Positive = gain, Negative = loss
    gain_or_loss = round_currency(sale_amount - current_book_value)

    # Create disposal record
    disposal_id = str(uuid.uuid4())

    ad_t = Table("asset_disposal")
    ad_ins_q = (Q.into(ad_t)
                .columns("id", "asset_id", "disposal_date", "disposal_method",
                          "sale_amount", "book_value_at_disposal", "gain_or_loss",
                          "buyer_details")
                .insert(P(), P(), P(), P(), P(), P(), P(), P()))
    conn.execute(ad_ins_q.get_sql(),
        (disposal_id, args.asset_id, args.disposal_date, disposal_method,
         str(round_currency(sale_amount)),
         str(round_currency(current_book_value)),
         str(gain_or_loss),
         args.buyer_details),
    )

    # Post GL entries for disposal
    #
    # GL layout (double-entry balanced):
    #
    # Sale at gain:
    #   DR Accumulated Depreciation   (accumulated_depreciation)
    #   DR Cash/Receivable            (sale_amount)
    #   CR Fixed Asset Account        (gross_value)
    #   CR Gain on Disposal           (gain_or_loss)
    #
    # Sale at loss:
    #   DR Accumulated Depreciation   (accumulated_depreciation)
    #   DR Cash/Receivable            (sale_amount)
    #   DR Loss on Disposal           (abs(gain_or_loss))
    #   CR Fixed Asset Account        (gross_value)
    #
    # Scrap (sale=0, loss = book_value):
    #   DR Accumulated Depreciation   (accumulated_depreciation)
    #   DR Loss on Disposal           (book_value)
    #   CR Fixed Asset Account        (gross_value)
    #
    cost_center_id = args.cost_center_id or _get_cost_center(conn, asset_dict["company_id"])
    fiscal_year = _get_fiscal_year(conn, args.disposal_date)

    gl_entries = []

    # DR Accumulated Depreciation
    if accumulated_depreciation > 0:
        gl_entries.append({
            "account_id": accum_dep_account_id,
            "debit": str(round_currency(accumulated_depreciation)),
            "credit": "0",
            "cost_center_id": cost_center_id,
            "fiscal_year": fiscal_year,
        })

    # CR Fixed Asset Account (at gross value)
    gl_entries.append({
        "account_id": asset_account_id,
        "debit": "0",
        "credit": str(round_currency(gross_value)),
        "cost_center_id": cost_center_id,
        "fiscal_year": fiscal_year,
    })

    if gain_or_loss < Decimal("0"):
        # Loss on disposal
        # DR Loss account for abs(gain_or_loss)
        gl_entries.append({
            "account_id": dep_account_id,
            "debit": str(round_currency(abs(gain_or_loss))),
            "credit": "0",
            "cost_center_id": cost_center_id,
            "fiscal_year": fiscal_year,
        })
        # If there's a sale amount, DR the sale proceeds too
        if sale_amount > 0:
            gl_entries.append({
                "account_id": dep_account_id,
                "debit": str(round_currency(sale_amount)),
                "credit": "0",
                "cost_center_id": cost_center_id,
                "fiscal_year": fiscal_year,
            })
    elif gain_or_loss > Decimal("0"):
        # Gain on disposal: sale > book_value
        # DR Sale proceeds
        gl_entries.append({
            "account_id": dep_account_id,
            "debit": str(round_currency(sale_amount)),
            "credit": "0",
            "cost_center_id": cost_center_id,
            "fiscal_year": fiscal_year,
        })
        # CR Gain
        gl_entries.append({
            "account_id": dep_account_id,
            "debit": "0",
            "credit": str(round_currency(gain_or_loss)),
            "cost_center_id": cost_center_id,
            "fiscal_year": fiscal_year,
        })
    else:
        # No gain/loss: sale_amount == book_value
        if sale_amount > 0:
            gl_entries.append({
                "account_id": dep_account_id,
                "debit": str(round_currency(sale_amount)),
                "credit": "0",
                "cost_center_id": cost_center_id,
                "fiscal_year": fiscal_year,
            })

    try:
        gl_ids = insert_gl_entries(
            conn, gl_entries,
            voucher_type="asset_disposal",
            voucher_id=disposal_id,
            posting_date=args.disposal_date,
            company_id=asset_dict["company_id"],
            remarks=f"Disposal ({disposal_method}) of {asset_dict['naming_series']}",
        )
    except (ValueError, NotImplementedError) as e:
        sys.stderr.write(f"[erpclaw-assets] {e}\n")
        err(f"GL posting failed: {e}")

    # Update disposal record with journal reference
    ad_upd_q = (Q.update(ad_t)
                .set(Field("journal_entry_id"), P())
                .where(ad_t.id == P()))
    conn.execute(ad_upd_q.get_sql(), (disposal_id, disposal_id))

    # Update asset status
    new_status = "sold" if disposal_method == "sale" else "scrapped"
    asset_t = Table("asset")
    asset_disp_q = (Q.update(asset_t)
                    .set(Field("status"), P())
                    .set(Field("current_book_value"), ValueWrapper("0"))
                    .set(Field("updated_at"), LiteralValue("datetime('now')"))
                    .where(asset_t.id == P()))
    conn.execute(asset_disp_q.get_sql(), (new_status, args.asset_id))

    audit(conn, "erpclaw-assets", "dispose-asset", "asset", args.asset_id,
           old_values={"status": asset_dict["status"],
                       "current_book_value": asset_dict["current_book_value"]},
           new_values={"status": new_status, "disposal_method": disposal_method,
                       "sale_amount": str(round_currency(sale_amount)),
                       "gain_or_loss": str(gain_or_loss)},
           description=f"Disposed asset {asset_dict['naming_series']} via {disposal_method}")

    conn.commit()
    ok({"disposal_id": disposal_id,
         "asset_id": args.asset_id,
         "disposal_method": disposal_method,
         "sale_amount": str(round_currency(sale_amount)),
         "book_value_at_disposal": str(round_currency(current_book_value)),
         "gain_or_loss": str(gain_or_loss),
         "new_status": new_status,
         "gl_entry_ids": gl_ids,
         "message": f"Asset {asset_dict['naming_series']} disposed via {disposal_method}"})


# ---------------------------------------------------------------------------
# 14. asset-register-report
# ---------------------------------------------------------------------------

def asset_register_report(conn, args):
    """Generate an asset register report.

    Required: --company-id
    Optional: --as-of-date (defaults to today)

    Returns all assets with gross_value, accumulated_depreciation, current_book_value.
    """
    if not args.company_id:
        err("--company-id is required")

    _validate_company_exists(conn, args.company_id)
    as_of_date = args.as_of_date or _today_str()

    # Fetch all assets for the company
    a = Table("asset").as_("a")
    ac = Table("asset_category").as_("ac")
    reg_q = (Q.from_(a)
             .select(a.id, a.naming_series, a.asset_name, a.asset_category_id,
                     a.purchase_date, a.gross_value, a.salvage_value,
                     a.depreciation_method, a.useful_life_years,
                     a.status, a.location, a.custodian_employee_id,
                     ac.name.as_("category_name"))
             .left_join(ac).on(ac.id == a.asset_category_id)
             .where(a.company_id == P())
             .orderby(ac.name).orderby(a.naming_series))
    assets = conn.execute(reg_q.get_sql(), (args.company_id,)).fetchall()

    register = []
    total_gross = Decimal("0")
    total_accum_dep = Decimal("0")
    total_book_value = Decimal("0")

    for asset_row in assets:
        asset = row_to_dict(asset_row)
        asset_id = asset["id"]
        gross = to_decimal(asset["gross_value"])

        # Calculate accumulated depreciation as of the date using Decimal
        ds_t = Table("depreciation_schedule")
        dep_q = (Q.from_(ds_t).select(ds_t.depreciation_amount)
                 .where(ds_t.asset_id == P())
                 .where(ds_t.status == ValueWrapper("posted"))
                 .where(ds_t.schedule_date <= P()))
        posted_dep_entries = conn.execute(dep_q.get_sql(), (asset_id, as_of_date)).fetchall()

        accum_dep = Decimal("0")
        for dep_row in posted_dep_entries:
            accum_dep = round_currency(accum_dep + to_decimal(dep_row["depreciation_amount"]))

        book_value = round_currency(gross - accum_dep)

        total_gross = round_currency(total_gross + gross)
        total_accum_dep = round_currency(total_accum_dep + accum_dep)
        total_book_value = round_currency(total_book_value + book_value)

        register.append({
            "asset_id": asset_id,
            "naming_series": asset["naming_series"],
            "asset_name": asset["asset_name"],
            "category_name": asset["category_name"],
            "purchase_date": asset["purchase_date"],
            "gross_value": str(gross),
            "accumulated_depreciation": str(accum_dep),
            "current_book_value": str(book_value),
            "status": asset["status"],
            "location": asset["location"],
        })

    ok({
        "report": "Asset Register",
        "company_id": args.company_id,
        "as_of_date": as_of_date,
        "assets": register,
        "summary": {
            "total_assets": len(register),
            "total_gross_value": str(total_gross),
            "total_accumulated_depreciation": str(total_accum_dep),
            "total_book_value": str(total_book_value),
        },
    })


# ---------------------------------------------------------------------------
# 15. depreciation-summary
# ---------------------------------------------------------------------------

def depreciation_summary(conn, args):
    """Depreciation summary report grouped by asset category.

    Required: --company-id
    Optional: --from-date, --to-date
    """
    if not args.company_id:
        err("--company-id is required")

    _validate_company_exists(conn, args.company_id)

    ds = Table("depreciation_schedule").as_("ds")
    a = Table("asset").as_("a")
    ac = Table("asset_category").as_("ac")
    params = [args.company_id]

    dep_sum_q = (Q.from_(ds)
                 .join(a).on(a.id == ds.asset_id)
                 .join(ac).on(ac.id == a.asset_category_id)
                 .select(ac.id.as_("category_id"), ac.name.as_("category_name"),
                         a.id.as_("asset_id"), a.naming_series, a.asset_name,
                         ds.depreciation_amount, ds.schedule_date)
                 .where(a.company_id == P())
                 .where(ds.status == ValueWrapper("posted")))

    if args.from_date:
        dep_sum_q = dep_sum_q.where(ds.schedule_date >= P())
        params.append(args.from_date)

    if args.to_date:
        dep_sum_q = dep_sum_q.where(ds.schedule_date <= P())
        params.append(args.to_date)

    dep_sum_q = dep_sum_q.orderby(ac.name).orderby(a.naming_series).orderby(ds.schedule_date)

    # Fetch posted depreciation entries grouped by category
    rows = conn.execute(dep_sum_q.get_sql(), params).fetchall()

    # Group by category
    categories = {}
    grand_total = Decimal("0")

    for row in rows:
        r = row_to_dict(row)
        cat_id = r["category_id"]
        cat_name = r["category_name"]
        dep_amount = to_decimal(r["depreciation_amount"])

        if cat_id not in categories:
            categories[cat_id] = {
                "category_id": cat_id,
                "category_name": cat_name,
                "total_depreciation": Decimal("0"),
                "assets": {},
            }

        categories[cat_id]["total_depreciation"] = round_currency(
            categories[cat_id]["total_depreciation"] + dep_amount
        )

        asset_id = r["asset_id"]
        if asset_id not in categories[cat_id]["assets"]:
            categories[cat_id]["assets"][asset_id] = {
                "asset_id": asset_id,
                "naming_series": r["naming_series"],
                "asset_name": r["asset_name"],
                "total_depreciation": Decimal("0"),
                "entries_count": 0,
            }

        categories[cat_id]["assets"][asset_id]["total_depreciation"] = round_currency(
            categories[cat_id]["assets"][asset_id]["total_depreciation"] + dep_amount
        )
        categories[cat_id]["assets"][asset_id]["entries_count"] += 1
        grand_total = round_currency(grand_total + dep_amount)

    # Convert to serializable format
    summary = []
    for cat_id, cat_data in categories.items():
        assets_list = []
        for asset_data in cat_data["assets"].values():
            assets_list.append({
                "asset_id": asset_data["asset_id"],
                "naming_series": asset_data["naming_series"],
                "asset_name": asset_data["asset_name"],
                "total_depreciation": str(asset_data["total_depreciation"]),
                "entries_count": asset_data["entries_count"],
            })
        summary.append({
            "category_id": cat_data["category_id"],
            "category_name": cat_data["category_name"],
            "total_depreciation": str(cat_data["total_depreciation"]),
            "assets": assets_list,
        })

    ok({
        "report": "Depreciation Summary",
        "company_id": args.company_id,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "categories": summary,
        "grand_total_depreciation": str(grand_total),
    })


# ---------------------------------------------------------------------------
# 16. status
# ---------------------------------------------------------------------------

def status_action(conn, args):
    """Dashboard: total assets by status, total book value, pending depreciation,
    upcoming maintenance.

    Required: --company-id
    """
    company_id = args.company_id
    if not company_id:
        co_t = Table("company")
        co_q = Q.from_(co_t).select(co_t.id).limit(1)
        row = conn.execute(co_q.get_sql()).fetchone()
        if not row:
            err("No company found. Create one with erpclaw first.",
                 suggestion="Run 'tutorial' to create a demo company, or 'setup company' to create your own.")
        company_id = row["id"]

    _validate_company_exists(conn, company_id)

    # Assets by status
    asset_t = Table("asset")
    stat_q = (Q.from_(asset_t)
              .select(asset_t.status, fn.Count("*").as_("count"))
              .where(asset_t.company_id == P())
              .groupby(asset_t.status))
    status_rows = conn.execute(stat_q.get_sql(), (company_id,)).fetchall()
    assets_by_status = {r["status"]: r["count"] for r in status_rows}

    # Total book value
    bv_q = (Q.from_(asset_t)
            .select(fn.Coalesce(fn.Count("*"), 0).as_("total_assets"))
            .where(asset_t.company_id == P()))
    bv_row = conn.execute(bv_q.get_sql(), (company_id,)).fetchone()

    # Calculate total book value using Decimal
    bv_sel_q = (Q.from_(asset_t).select(asset_t.current_book_value)
                .where(asset_t.company_id == P()))
    asset_bv_rows = conn.execute(bv_sel_q.get_sql(), (company_id,)).fetchall()
    total_book_value = Decimal("0")
    for r in asset_bv_rows:
        total_book_value = round_currency(total_book_value + to_decimal(r["current_book_value"]))

    # Total gross value
    gv_sel_q = (Q.from_(asset_t).select(asset_t.gross_value)
                .where(asset_t.company_id == P()))
    asset_gv_rows = conn.execute(gv_sel_q.get_sql(), (company_id,)).fetchall()
    total_gross_value = Decimal("0")
    for r in asset_gv_rows:
        total_gross_value = round_currency(total_gross_value + to_decimal(r["gross_value"]))

    # Pending depreciation entries
    ds_t = Table("depreciation_schedule").as_("ds")
    a_t = Table("asset").as_("a")
    pend_q = (Q.from_(ds_t)
              .join(a_t).on(a_t.id == ds_t.asset_id)
              .select(fn.Count("*").as_("count"))
              .where(a_t.company_id == P())
              .where(ds_t.status == ValueWrapper("pending")))
    pending_dep = conn.execute(pend_q.get_sql(), (company_id,)).fetchone()

    # Overdue depreciation (pending entries with schedule_date < today)
    today = _today_str()
    overdue_q = (Q.from_(ds_t)
                 .join(a_t).on(a_t.id == ds_t.asset_id)
                 .select(fn.Count("*").as_("count"))
                 .where(a_t.company_id == P())
                 .where(ds_t.status == ValueWrapper("pending"))
                 .where(ds_t.schedule_date < P()))
    overdue_dep = conn.execute(overdue_q.get_sql(), (company_id, today)).fetchone()

    # Upcoming maintenance (next 30 days)
    thirty_days = (date.today() + timedelta(days=30)).isoformat()
    am_t = Table("asset_maintenance").as_("am")
    a2_t = Table("asset").as_("a")
    up_q = (Q.from_(am_t)
            .join(a2_t).on(a2_t.id == am_t.asset_id)
            .select(am_t.star, a2_t.naming_series, a2_t.asset_name)
            .where(a2_t.company_id == P())
            .where(am_t.status.isin([ValueWrapper("planned"), ValueWrapper("overdue")]))
            .where(am_t.scheduled_date <= P())
            .orderby(am_t.scheduled_date)
            .limit(10))
    upcoming_maint = conn.execute(up_q.get_sql(), (company_id, thirty_days)).fetchall()
    upcoming_maint_list = [row_to_dict(r) for r in upcoming_maint]

    # Overdue maintenance
    od_mnt_q = (Q.from_(am_t)
                .join(a2_t).on(a2_t.id == am_t.asset_id)
                .select(fn.Count("*").as_("count"))
                .where(a2_t.company_id == P())
                .where(am_t.status.isin([ValueWrapper("planned"), ValueWrapper("overdue")]))
                .where(am_t.scheduled_date < P()))
    overdue_maint = conn.execute(od_mnt_q.get_sql(), (company_id, today)).fetchone()

    ok({
        "dashboard": "Asset Management Status",
        "company_id": company_id,
        "assets_by_status": assets_by_status,
        "total_assets": bv_row["total_assets"],
        "total_gross_value": str(total_gross_value),
        "total_book_value": str(total_book_value),
        "total_accumulated_depreciation": str(
            round_currency(total_gross_value - total_book_value)
        ),
        "pending_depreciation_entries": pending_dep["count"],
        "overdue_depreciation_entries": overdue_dep["count"],
        "upcoming_maintenance": upcoming_maint_list,
        "overdue_maintenance_count": overdue_maint["count"],
    })


# ===========================================================================
# M7 — Asset depth: impairment, capitalization, revaluation, capex maintenance
# ===========================================================================
#
# Carrying-value invariant. post-depreciation derives
#   current_book_value = gross_value - accumulated_depreciation
# on every post, so M7 write-downs/ups must keep that identity true or the next
# depreciation posting silently clobbers them:
#   * impairment       -> increases accumulated_depreciation (a write-down)
#   * revaluation up    -> increases gross_value      (DR Asset / CR Reserve)
#   * revaluation down  -> decreases gross_value      (DR Reserve / CR Asset)
#   * capex maintenance -> increases gross_value      (DR Asset / CR Cash)
# Either way current_book_value = gross_value - accumulated_depreciation holds.


def _category_accounts(conn, asset_dict):
    """Return (category_dict, asset_acct, dep_acct, accum_dep_acct) or error."""
    category = _validate_asset_category_exists(conn, asset_dict["asset_category_id"])
    cat_dict = row_to_dict(category)
    return (cat_dict,
            cat_dict.get("asset_account_id"),
            cat_dict.get("depreciation_account_id"),
            cat_dict.get("accumulated_depreciation_account_id"))


def _build_schedule_rows(method, basis, salvage, start_date, n_months, asset_id, prior_accum):
    """Build pending depreciation_schedule rows for the *remaining* life, seeded
    from ``basis`` (the current book value) over ``n_months``.

    Used by the M7 recompute after a revaluation / capex capitalization changes
    the carrying amount. ``accumulated_amount`` is reported as an absolute figure
    (prior posted accumulation + running), ``book_value_after`` walks ``basis``
    down toward ``salvage``. Mirrors generate-depreciation-schedule's per-method
    math, but the original function (initial schedule from gross) is left
    untouched. Returns [] when there is nothing depreciable.
    """
    rows = []
    depreciable = basis - salvage
    if depreciable <= 0 or n_months <= 0:
        return rows

    accumulated = Decimal("0")          # running depreciation within remaining life
    book_value = basis

    if method == "straight_line":
        monthly = round_currency(depreciable / Decimal(str(n_months)))
        for i in range(n_months):
            this = (depreciable - accumulated) if i == n_months - 1 else monthly
            this = round_currency(this)
            accumulated = round_currency(accumulated + this)
            book_value = round_currency(basis - accumulated)
            if book_value < salvage:
                this = round_currency(this - (salvage - book_value))
                accumulated = round_currency(depreciable)
                book_value = salvage
            if this <= 0:
                break
            rows.append(_sched_row(asset_id, _add_months(start_date, i), this,
                                   round_currency(prior_accum + accumulated), book_value))
    else:
        # written_down_value / double_declining: iterate book value with an
        # annual rate re-derived over the REMAINING life (prospective change).
        years_remaining = Decimal(str(n_months)) / Decimal("12")
        if method == "double_declining":
            annual_rate = Decimal("2") / years_remaining
        else:  # written_down_value
            ratio_float = float(salvage / basis) if salvage > 0 else 0.0
            exp_float = float(Decimal("1") / years_remaining)
            annual_rate = Decimal(str(1 - ratio_float ** exp_float))
        for i in range(n_months):
            this = round_currency(book_value * annual_rate / Decimal("12"))
            if book_value - this < salvage:
                this = round_currency(book_value - salvage)
            if this <= 0:
                break
            accumulated = round_currency(accumulated + this)
            book_value = round_currency(book_value - this)
            rows.append(_sched_row(asset_id, _add_months(start_date, i), this,
                                   round_currency(prior_accum + accumulated), book_value))
    return rows


def _sched_row(asset_id, schedule_date, amount, accumulated, book_value_after):
    return {
        "id": str(uuid.uuid4()),
        "asset_id": asset_id,
        "schedule_date": schedule_date,
        "depreciation_amount": str(amount),
        "accumulated_amount": str(accumulated),
        "book_value_after": str(book_value_after),
        "status": "pending",
        "fiscal_year": schedule_date[:4],
    }


def _recompute_pending_depreciation(conn, asset_id):
    """Regenerate the pending depreciation tail from the asset's *current* book
    value over its remaining life. Called by revalue-asset and the capex branch
    of complete-maintenance so a changed carrying amount flows into future
    depreciation. No-op (skipped) when the asset has no schedule yet or no
    method/start-date. Caller manages the transaction (no commit here)."""
    asset = _validate_asset_exists(conn, asset_id)
    a = row_to_dict(asset)

    method = a.get("depreciation_method")
    useful_life = a.get("useful_life_years")
    start_date = a.get("depreciation_start_date")
    if not method or not useful_life or not start_date:
        return {"regenerated": 0, "skipped": "no_depreciation_config"}

    ds_t = Table("depreciation_schedule")
    total_existing = conn.execute(
        Q.from_(ds_t).select(fn.Count("*").as_("c")).where(ds_t.asset_id == P()).get_sql(),
        (asset_id,)).fetchone()["c"]
    if total_existing == 0:
        return {"regenerated": 0, "skipped": "no_schedule"}

    posted_count = conn.execute(
        (Q.from_(ds_t).select(fn.Count("*").as_("c"))
         .where(ds_t.asset_id == P())
         .where(ds_t.status == ValueWrapper("posted"))).get_sql(),
        (asset_id,)).fetchone()["c"]

    # Drop the stale pending tail.
    conn.execute((Q.from_(ds_t).delete()
                  .where(ds_t.asset_id == P())
                  .where(ds_t.status == ValueWrapper("pending"))).get_sql(), (asset_id,))

    total_months = int(useful_life) * 12
    remaining = total_months - posted_count
    if remaining <= 0:
        return {"regenerated": 0, "skipped": "fully_scheduled"}

    basis = to_decimal(a["current_book_value"])
    salvage = to_decimal(a["salvage_value"])
    prior_accum = to_decimal(a["accumulated_depreciation"])
    rows = _build_schedule_rows(method, basis, salvage,
                                _add_months(start_date, posted_count),
                                remaining, asset_id, prior_accum)

    ins_q = (Q.into(ds_t)
             .columns("id", "asset_id", "schedule_date", "depreciation_amount",
                      "accumulated_amount", "book_value_after", "status", "fiscal_year")
             .insert(P(), P(), P(), P(), P(), P(), P(), P())).get_sql()
    for r in rows:
        conn.execute(ins_q, (r["id"], r["asset_id"], r["schedule_date"],
                             r["depreciation_amount"], r["accumulated_amount"],
                             r["book_value_after"], r["status"], r["fiscal_year"]))
    return {"regenerated": len(rows), "skipped": None}


# ---------------------------------------------------------------------------
# M7.1 impair-asset
# ---------------------------------------------------------------------------

def impair_asset(conn, args):
    """Record an impairment write-down and post its GL.

    Required: --asset-id, --impairment-amount, --recoverable-amount
    Optional: --impairment-date (default today), --reason, --cost-center-id

    GL: DR Impairment Loss (category depreciation_account) /
        CR Accumulated Impairment (category accumulated_depreciation_account).
    Increases accumulated_depreciation so the carrying-value invariant holds.
    """
    if not args.asset_id:
        err("--asset-id is required")
    if not args.impairment_amount:
        err("--impairment-amount is required")
    if args.recoverable_amount is None:
        err("--recoverable-amount is required")

    asset = _validate_asset_exists(conn, args.asset_id)
    asset_dict = row_to_dict(asset)

    if asset_dict["status"] in ("scrapped", "sold"):
        err(f"Asset is '{asset_dict['status']}'. Cannot impair a disposed asset.")
    if asset_dict["status"] == "draft":
        err("Cannot impair a draft asset. Submit the asset first.")

    try:
        amount = to_decimal(args.impairment_amount)
        recoverable = to_decimal(args.recoverable_amount)
    except (InvalidOperation, ValueError):
        err("--impairment-amount and --recoverable-amount must be numeric")
    if amount <= 0:
        err("--impairment-amount must be greater than 0")
    if recoverable < 0:
        err("--recoverable-amount must be >= 0")

    book_before = to_decimal(asset_dict["current_book_value"])
    new_book = round_currency(book_before - amount)
    # NEGATIVE CONTROL: an impairment may not write the asset below its
    # recoverable amount (the floor). Reject rather than over-impair.
    if new_book < recoverable:
        err(f"Impairment of {amount} would drop book value to {new_book}, below the "
            f"recoverable amount {recoverable}. Reduce --impairment-amount.")

    cat_dict, asset_acct, dep_acct, accum_acct = _category_accounts(conn, asset_dict)
    if not dep_acct:
        err("Asset category has no depreciation_account_id (impairment loss account)")
    if not accum_acct:
        err("Asset category has no accumulated_depreciation_account_id (accumulated impairment)")

    impairment_date = args.impairment_date or _today_str()
    cost_center_id = args.cost_center_id or _get_cost_center(conn, asset_dict["company_id"])
    fiscal_year = _get_fiscal_year(conn, impairment_date)

    impairment_id = str(uuid.uuid4())
    imp_t = Table("asset_impairment")
    conn.execute((Q.into(imp_t)
                  .columns("id", "asset_id", "impairment_date", "impairment_amount",
                           "recoverable_amount", "book_value_before", "reason", "status")
                  .insert(P(), P(), P(), P(), P(), P(), P(), ValueWrapper("submitted"))).get_sql(),
                 (impairment_id, args.asset_id, impairment_date, str(round_currency(amount)),
                  str(round_currency(recoverable)), str(round_currency(book_before)), args.reason))

    gl_entries = [
        {"account_id": dep_acct, "debit": str(round_currency(amount)), "credit": "0",
         "cost_center_id": cost_center_id, "fiscal_year": fiscal_year},
        {"account_id": accum_acct, "debit": "0", "credit": str(round_currency(amount)),
         "cost_center_id": cost_center_id, "fiscal_year": fiscal_year},
    ]
    try:
        gl_ids = insert_gl_entries(
            conn, gl_entries, voucher_type="asset_impairment", voucher_id=impairment_id,
            posting_date=impairment_date, company_id=asset_dict["company_id"],
            remarks=f"Impairment of {asset_dict['naming_series']}")
    except (ValueError, NotImplementedError) as e:
        sys.stderr.write(f"[erpclaw-assets] {e}\n")
        err(f"GL posting failed: {e}")

    conn.execute((Q.update(imp_t).set(Field("gl_entry_id"), P())
                  .where(imp_t.id == P())).get_sql(), (gl_ids[0], impairment_id))

    new_accum = round_currency(to_decimal(asset_dict["accumulated_depreciation"]) + amount)
    asset_t = Table("asset")
    conn.execute((Q.update(asset_t)
                  .set(Field("accumulated_depreciation"), P())
                  .set(Field("current_book_value"), P())
                  .set(Field("status"), ValueWrapper("impaired"))
                  .set(Field("updated_at"), LiteralValue("datetime('now')"))
                  .where(asset_t.id == P())).get_sql(),
                 (str(new_accum), str(new_book), args.asset_id))

    audit(conn, "erpclaw-assets", "impair-asset", "asset", args.asset_id,
          old_values={"current_book_value": asset_dict["current_book_value"],
                      "status": asset_dict["status"]},
          new_values={"current_book_value": str(new_book), "status": "impaired",
                      "impairment_amount": str(round_currency(amount))},
          description=f"Impaired {asset_dict['naming_series']} by {amount}")

    conn.commit()
    ok({"impairment_id": impairment_id, "asset_id": args.asset_id,
        "impairment_amount": str(round_currency(amount)),
        "recoverable_amount": str(round_currency(recoverable)),
        "book_value_before": str(round_currency(book_before)),
        "book_value_after": str(new_book), "new_status": "impaired",
        "gl_entry_ids": gl_ids,
        "message": f"Asset {asset_dict['naming_series']} impaired by {amount}"})


# ---------------------------------------------------------------------------
# M7.2 reverse-impairment
# ---------------------------------------------------------------------------

def reverse_impairment(conn, args):
    """Reverse a prior impairment (cancel = reverse, per the coding rules).

    Required: --impairment-id
    Optional: --posting-date (default today)

    Posts mirror GL entries, unwinds the accumulated_depreciation bump, and
    resets the asset to in_use when no other active impairment remains.
    """
    if not args.impairment_id:
        err("--impairment-id is required")

    imp_t = Table("asset_impairment")
    imp = conn.execute(Q.from_(imp_t).select(imp_t.star).where(imp_t.id == P()).get_sql(),
                       (args.impairment_id,)).fetchone()
    if not imp:
        err(f"Impairment {args.impairment_id} not found")
    imp_dict = row_to_dict(imp)
    if imp_dict["status"] == "reversed":
        err("Impairment is already reversed")

    asset = _validate_asset_exists(conn, imp_dict["asset_id"])
    asset_dict = row_to_dict(asset)
    amount = to_decimal(imp_dict["impairment_amount"])
    posting_date = args.posting_date or _today_str()

    try:
        reversal_ids = reverse_gl_entries(
            conn, voucher_type="asset_impairment", voucher_id=args.impairment_id,
            posting_date=posting_date)
    except (ValueError, NotImplementedError) as e:
        sys.stderr.write(f"[erpclaw-assets] {e}\n")
        err(f"GL reversal failed: {e}")

    new_accum = round_currency(to_decimal(asset_dict["accumulated_depreciation"]) - amount)
    new_book = round_currency(to_decimal(asset_dict["current_book_value"]) + amount)

    conn.execute((Q.update(imp_t).set(Field("status"), ValueWrapper("reversed"))
                  .where(imp_t.id == P())).get_sql(), (args.impairment_id,))

    # Any other still-active impairment on this asset keeps it impaired.
    remaining = conn.execute(
        (Q.from_(imp_t).select(fn.Count("*").as_("c"))
         .where(imp_t.asset_id == P())
         .where(imp_t.status == ValueWrapper("submitted"))).get_sql(),
        (imp_dict["asset_id"],)).fetchone()["c"]
    new_status = asset_dict["status"]
    if asset_dict["status"] == "impaired" and remaining == 0:
        new_status = "in_use"

    asset_t = Table("asset")
    conn.execute((Q.update(asset_t)
                  .set(Field("accumulated_depreciation"), P())
                  .set(Field("current_book_value"), P())
                  .set(Field("status"), P())
                  .set(Field("updated_at"), LiteralValue("datetime('now')"))
                  .where(asset_t.id == P())).get_sql(),
                 (str(new_accum), str(new_book), new_status, imp_dict["asset_id"]))

    audit(conn, "erpclaw-assets", "reverse-impairment", "asset", imp_dict["asset_id"],
          old_values={"current_book_value": asset_dict["current_book_value"],
                      "status": asset_dict["status"]},
          new_values={"current_book_value": str(new_book), "status": new_status},
          description=f"Reversed impairment {args.impairment_id}")

    conn.commit()
    ok({"impairment_id": args.impairment_id, "asset_id": imp_dict["asset_id"],
        "reversed_amount": str(round_currency(amount)),
        "book_value_after": str(new_book), "new_status": new_status,
        "reversal_gl_entry_ids": reversal_ids,
        "message": f"Impairment {args.impairment_id} reversed"})


# ---------------------------------------------------------------------------
# M7.3 capitalize-asset
# ---------------------------------------------------------------------------

def capitalize_asset(conn, args):
    """Initial recognition: capitalize a purchase cost into a new asset + post GL.

    Required: --company-id, --name, --asset-category-id, --capitalized-amount,
              --source-account-id (the account the cost currently sits in, e.g.
              a CWIP or asset-clearing account — the credit leg).
    Optional: --purchase-invoice-id (linkage + dedup), --capitalization-date
              (default today), --salvage-value, --useful-life-years,
              --depreciation-start-date, --cost-center-id

    GL: DR Fixed Asset (category asset_account) / CR --source-account-id.
    Creates a submitted asset (not draft) since it is already recognized.
    """
    if not args.company_id:
        err("--company-id is required")
    if not args.name:
        err("--name is required")
    if not args.asset_category_id:
        err("--asset-category-id is required")
    if not args.capitalized_amount:
        err("--capitalized-amount is required")
    if not args.source_account_id:
        err("--source-account-id is required (credit leg for the cost transfer)")

    _validate_company_exists(conn, args.company_id)
    category = _validate_asset_category_exists(conn, args.asset_category_id)
    cat_dict = row_to_dict(category)
    asset_acct = cat_dict.get("asset_account_id")
    if not asset_acct:
        err("Asset category has no asset_account_id set")

    try:
        amount = to_decimal(args.capitalized_amount)
    except (InvalidOperation, ValueError):
        err("--capitalized-amount must be numeric")
    if amount <= 0:
        err("--capitalized-amount must be greater than 0")

    salvage = to_decimal(args.salvage_value or "0")
    if salvage < 0 or salvage >= amount:
        err("--salvage-value must be >= 0 and less than --capitalized-amount")

    # Dedup: a purchase-invoice line may back only one asset.
    if args.purchase_invoice_id:
        a_t = Table("asset")
        clash = conn.execute(
            Q.from_(a_t).select(a_t.id).where(a_t.purchase_invoice_id == P()).get_sql(),
            (args.purchase_invoice_id,)).fetchone()
        cap_t = Table("asset_capitalization")
        clash2 = conn.execute(
            Q.from_(cap_t).select(cap_t.id).where(cap_t.purchase_invoice_id == P()).get_sql(),
            (args.purchase_invoice_id,)).fetchone()
        if clash or clash2:
            err(f"Purchase invoice {args.purchase_invoice_id} is already capitalized "
                f"into an asset.")

    # Validate the source account exists.
    acct_t = Table("account")
    if not conn.execute(Q.from_(acct_t).select(acct_t.id).where(acct_t.id == P()).get_sql(),
                        (args.source_account_id,)).fetchone():
        err(f"Source account {args.source_account_id} not found")

    cap_date = args.capitalization_date or _today_str()
    useful_life = int(args.useful_life_years) if args.useful_life_years else cat_dict["useful_life_years"]
    dep_method = cat_dict["depreciation_method"]
    naming = get_next_name(conn, "asset", company_id=args.company_id)
    asset_id = str(uuid.uuid4())

    asset_t = Table("asset")
    conn.execute((Q.into(asset_t)
                  .columns("id", "naming_series", "asset_name", "asset_category_id",
                           "purchase_date", "purchase_invoice_id", "gross_value", "salvage_value",
                           "depreciation_method", "useful_life_years", "depreciation_start_date",
                           "current_book_value", "accumulated_depreciation", "status", "company_id")
                  .insert(P(), P(), P(), P(), P(), P(), P(), P(), P(), P(), P(), P(),
                          ValueWrapper("0"), ValueWrapper("submitted"), P())).get_sql(),
                 (asset_id, naming, args.name, args.asset_category_id, cap_date,
                  args.purchase_invoice_id, str(round_currency(amount)), str(round_currency(salvage)),
                  dep_method, useful_life, args.depreciation_start_date,
                  str(round_currency(amount)), args.company_id))

    cap_id = str(uuid.uuid4())
    cap_t = Table("asset_capitalization")
    conn.execute((Q.into(cap_t)
                  .columns("id", "asset_id", "purchase_invoice_id", "capitalized_amount",
                           "capitalization_date", "source_account_id")
                  .insert(P(), P(), P(), P(), P(), P())).get_sql(),
                 (cap_id, asset_id, args.purchase_invoice_id, str(round_currency(amount)),
                  cap_date, args.source_account_id))

    cost_center_id = args.cost_center_id or _get_cost_center(conn, args.company_id)
    fiscal_year = _get_fiscal_year(conn, cap_date)
    gl_entries = [
        {"account_id": asset_acct, "debit": str(round_currency(amount)), "credit": "0",
         "cost_center_id": cost_center_id, "fiscal_year": fiscal_year},
        {"account_id": args.source_account_id, "debit": "0", "credit": str(round_currency(amount)),
         "cost_center_id": cost_center_id, "fiscal_year": fiscal_year},
    ]
    try:
        gl_ids = insert_gl_entries(
            conn, gl_entries, voucher_type="asset_capitalization", voucher_id=cap_id,
            posting_date=cap_date, company_id=args.company_id,
            remarks=f"Capitalization of {naming}")
    except (ValueError, NotImplementedError) as e:
        sys.stderr.write(f"[erpclaw-assets] {e}\n")
        err(f"GL posting failed: {e}")

    conn.execute((Q.update(cap_t).set(Field("gl_entry_id"), P())
                  .where(cap_t.id == P())).get_sql(), (gl_ids[0], cap_id))

    audit(conn, "erpclaw-assets", "capitalize-asset", "asset", asset_id,
          new_values={"capitalized_amount": str(round_currency(amount)),
                      "naming_series": naming},
          description=f"Capitalized {naming} for {amount}")

    conn.commit()
    ok({"asset_id": asset_id, "capitalization_id": cap_id, "naming_series": naming,
        "capitalized_amount": str(round_currency(amount)), "new_status": "submitted",
        "gl_entry_ids": gl_ids,
        "message": f"Asset {naming} capitalized for {amount}"})


# ---------------------------------------------------------------------------
# M7.4 revalue-asset
# ---------------------------------------------------------------------------

def revalue_asset(conn, args):
    """Revalue an asset up or down and recompute its depreciation schedule.

    Required: --asset-id, --new-value, --reserve-account-id (Revaluation Reserve)
    Optional: --revaluation-date (default today), --reason, --cost-center-id

    GL (upward):   DR Fixed Asset / CR Revaluation Reserve.
    GL (downward): DR Revaluation Reserve / CR Fixed Asset.
    Adjusts gross_value by the delta (carrying-value invariant) and regenerates
    the pending depreciation tail from the new book value.
    """
    if not args.asset_id:
        err("--asset-id is required")
    if args.new_value is None:
        err("--new-value is required")
    if not args.reserve_account_id:
        err("--reserve-account-id is required (Revaluation Reserve account)")

    asset = _validate_asset_exists(conn, args.asset_id)
    asset_dict = row_to_dict(asset)

    if asset_dict["status"] in ("scrapped", "sold"):
        err(f"Asset is '{asset_dict['status']}'. Cannot revalue a disposed asset.")
    if asset_dict["status"] == "draft":
        err("Cannot revalue a draft asset. Submit the asset first.")
    if asset_dict["status"] == "under_construction":
        err("Cannot revalue an under_construction asset. Use CWIP cost accumulation instead.")

    try:
        new_value = to_decimal(args.new_value)
    except (InvalidOperation, ValueError):
        err("--new-value must be numeric")
    if new_value < 0:
        err("--new-value must be >= 0")

    book_before = to_decimal(asset_dict["current_book_value"])
    delta = round_currency(new_value - book_before)
    if delta == 0:
        err("--new-value equals the current book value; nothing to revalue")

    cat_dict, asset_acct, dep_acct, accum_acct = _category_accounts(conn, asset_dict)
    if not asset_acct:
        err("Asset category has no asset_account_id set")
    acct_t = Table("account")
    if not conn.execute(Q.from_(acct_t).select(acct_t.id).where(acct_t.id == P()).get_sql(),
                        (args.reserve_account_id,)).fetchone():
        err(f"Reserve account {args.reserve_account_id} not found")

    reval_date = args.revaluation_date or _today_str()
    cost_center_id = args.cost_center_id or _get_cost_center(conn, asset_dict["company_id"])
    fiscal_year = _get_fiscal_year(conn, reval_date)
    reval_id = str(uuid.uuid4())

    if delta > 0:
        gl_entries = [
            {"account_id": asset_acct, "debit": str(delta), "credit": "0",
             "cost_center_id": cost_center_id, "fiscal_year": fiscal_year},
            {"account_id": args.reserve_account_id, "debit": "0", "credit": str(delta),
             "cost_center_id": cost_center_id, "fiscal_year": fiscal_year},
        ]
    else:
        mag = round_currency(abs(delta))
        gl_entries = [
            {"account_id": args.reserve_account_id, "debit": str(mag), "credit": "0",
             "cost_center_id": cost_center_id, "fiscal_year": fiscal_year},
            {"account_id": asset_acct, "debit": "0", "credit": str(mag),
             "cost_center_id": cost_center_id, "fiscal_year": fiscal_year},
        ]
    try:
        gl_ids = insert_gl_entries(
            conn, gl_entries, voucher_type="asset_revaluation", voucher_id=reval_id,
            posting_date=reval_date, company_id=asset_dict["company_id"],
            remarks=f"Revaluation of {asset_dict['naming_series']} to {new_value}")
    except (ValueError, NotImplementedError) as e:
        sys.stderr.write(f"[erpclaw-assets] {e}\n")
        err(f"GL posting failed: {e}")

    new_gross = round_currency(to_decimal(asset_dict["gross_value"]) + delta)
    asset_t = Table("asset")
    conn.execute((Q.update(asset_t)
                  .set(Field("gross_value"), P())
                  .set(Field("current_book_value"), P())
                  .set(Field("updated_at"), LiteralValue("datetime('now')"))
                  .where(asset_t.id == P())).get_sql(),
                 (str(new_gross), str(round_currency(new_value)), args.asset_id))

    recompute = _recompute_pending_depreciation(conn, args.asset_id)

    audit(conn, "erpclaw-assets", "revalue-asset", "asset", args.asset_id,
          old_values={"gross_value": asset_dict["gross_value"],
                      "current_book_value": asset_dict["current_book_value"]},
          new_values={"gross_value": str(new_gross),
                      "current_book_value": str(round_currency(new_value))},
          description=f"Revalued {asset_dict['naming_series']} to {new_value}")

    conn.commit()
    ok({"asset_id": args.asset_id, "revaluation_id": reval_id,
        "direction": "up" if delta > 0 else "down",
        "book_value_before": str(round_currency(book_before)),
        "book_value_after": str(round_currency(new_value)),
        "delta": str(delta), "schedule_recompute": recompute,
        "gl_entry_ids": gl_ids,
        "message": f"Asset {asset_dict['naming_series']} revalued to {new_value}"})


# ---------------------------------------------------------------------------
# S3 — CWIP (Construction-in-Progress)
# ---------------------------------------------------------------------------

def _validate_cwip_account(conn, account_id):
    """Validate an account exists and is a capital_work_in_progress account."""
    acct_t = Table("account")
    row = conn.execute(
        (Q.from_(acct_t).select(acct_t.id, acct_t.account_type, acct_t.name)
         .where(acct_t.id == P())).get_sql(), (account_id,)).fetchone()
    if not row:
        err(f"CWIP account {account_id} not found")
    if row["account_type"] != "capital_work_in_progress":
        err(f"Account {account_id} is not a capital_work_in_progress account "
            f"(type='{row['account_type']}'). CWIP accumulation must debit a CWIP account.")
    return row


def _account_exists(conn, account_id):
    acct_t = Table("account")
    return conn.execute(
        Q.from_(acct_t).select(acct_t.id).where(acct_t.id == P()).get_sql(),
        (account_id,)).fetchone() is not None


def _cwip_account_for_asset(conn, asset_id):
    """The single capital_work_in_progress account this asset has accumulated to.

    Delegates to the shared erpclaw_lib.cwip_posting.cwip_account_for_asset so the
    standalone accumulate-cwip-cost path and the AVA-43 invoice/JE hooks derive the
    CWIP account identically (from each accumulation row's gl_entry_id DR leg) —
    transfer-cwip-to-asset must credit the right CWIP account regardless of how the
    cost was accumulated."""
    return cwip_account_for_asset(conn, asset_id)


def _post_accumulation(conn, asset_dict, amount, cwip_acct_id, source_acct_id,
                       source_voucher_type, source_voucher_id, posting_date,
                       cost_center_id, notes):
    """Insert one cwip_cost_accumulation row + its GL (DR CWIP / CR source) and
    bump the asset's gross/current_book value. Caller owns the transaction (no
    commit). Tags both legs with the asset's cwip_project_id for per-project
    roll-up. Returns (accumulation_id, gl_ids). Raises on GL failure.

    Mutates asset_dict's gross_value/current_book_value in place so a caller that
    accumulates then reads the carrying amount sees the updated figures.

    Posts its own GL (DR CWIP / CR source under voucher_type='cwip_capitalization',
    voucher_id=accum_id) then delegates the row insert + asset bump to the shared
    erpclaw_lib.cwip_posting.record_cwip_accumulation — the same helper the
    buying/journals invoice/JE hooks call in-transaction (AVA-43)."""
    accum_id = str(uuid.uuid4())
    fiscal_year = _get_fiscal_year(conn, posting_date)
    project_id = asset_dict.get("cwip_project_id")
    gl_entries = [
        {"account_id": cwip_acct_id, "debit": str(round_currency(amount)), "credit": "0",
         "cost_center_id": cost_center_id, "fiscal_year": fiscal_year, "project_id": project_id},
        {"account_id": source_acct_id, "debit": "0", "credit": str(round_currency(amount)),
         "cost_center_id": cost_center_id, "fiscal_year": fiscal_year, "project_id": project_id},
    ]
    gl_ids = insert_gl_entries(
        conn, gl_entries, voucher_type="cwip_capitalization", voucher_id=accum_id,
        posting_date=posting_date, company_id=asset_dict["company_id"],
        remarks=f"CWIP accumulation for {asset_dict['naming_series']}")

    record_cwip_accumulation(
        conn, asset_dict, amount, source_voucher_type=source_voucher_type,
        source_voucher_id=source_voucher_id, gl_entry_id=gl_ids[0],
        accumulated_at=posting_date, notes=notes, accum_id=accum_id)
    return accum_id, gl_ids


def add_cwip(conn, args):
    """Start a construction-in-progress (CWIP) asset.

    Required: --company-id, --asset-category-id
    Optional: --name (default 'CWIP <category>'), --project-id, --description

    Creates an asset in status='under_construction' with gross_value=0 and
    current_book_value=0. Costs are added later via accumulate-cwip-cost, then
    transfer-cwip-to-asset capitalizes it into a depreciable fixed asset.
    Returns asset_id.
    """
    if not args.company_id:
        err("--company-id is required")
    if not args.asset_category_id:
        err("--asset-category-id is required")

    _validate_company_exists(conn, args.company_id)
    category = _validate_asset_category_exists(conn, args.asset_category_id)
    cat_dict = row_to_dict(category)

    if args.project_id:
        proj_t = Table("project")
        if not conn.execute(Q.from_(proj_t).select(proj_t.id).where(proj_t.id == P()).get_sql(),
                            (args.project_id,)).fetchone():
            err(f"Project {args.project_id} not found")

    name = args.name or f"CWIP {cat_dict['name']}"
    naming = get_next_name(conn, "asset", company_id=args.company_id)
    asset_id = str(uuid.uuid4())

    asset_t = Table("asset")
    conn.execute((Q.into(asset_t)
                  .columns("id", "naming_series", "asset_name", "asset_category_id",
                           "gross_value", "salvage_value", "current_book_value",
                           "accumulated_depreciation", "status", "cwip_project_id", "company_id")
                  .insert(P(), P(), P(), P(), ValueWrapper("0"), ValueWrapper("0"),
                          ValueWrapper("0"), ValueWrapper("0"),
                          ValueWrapper("under_construction"), P(), P())).get_sql(),
                 (asset_id, naming, name, args.asset_category_id, args.project_id, args.company_id))

    audit(conn, "erpclaw-assets", "add-cwip", "asset", asset_id,
          new_values={"asset_name": name, "naming_series": naming,
                      "status": "under_construction", "cwip_project_id": args.project_id},
          description=args.description or f"Started CWIP asset {naming}")

    conn.commit()
    ok({"asset_id": asset_id, "naming_series": naming, "asset_name": name,
        "status": "under_construction", "cwip_project_id": args.project_id,
        "current_book_value": "0",
        "message": f"CWIP asset '{naming}' started (under_construction)"})


def accumulate_cwip_cost(conn, args):
    """Accumulate a cost against an under-construction (CWIP) asset + post GL.

    Required: --asset-id, --source-voucher-type, --amount, --cwip-account-id,
              --source-account-id
    Optional: --source-voucher-id, --posting-date (default today), --notes,
              --cost-center-id

    GL: DR --cwip-account-id (capital_work_in_progress) / CR --source-account-id
        (AP or Cash), voucher_type='cwip_capitalization', tagged with the asset's
        cwip_project_id. Bumps the asset's gross_value + current_book_value.
    Rejected if the asset is not under_construction.
    """
    if not args.asset_id:
        err("--asset-id is required")
    if not args.source_voucher_type:
        err("--source-voucher-type is required (e.g. purchase_invoice, journal_entry)")
    if args.amount is None:
        err("--amount is required")
    if not args.cwip_account_id:
        err("--cwip-account-id is required (the capital_work_in_progress account to debit)")
    if not args.source_account_id:
        err("--source-account-id is required (credit leg, e.g. AP or Cash)")

    asset = _validate_asset_exists(conn, args.asset_id)
    asset_dict = row_to_dict(asset)
    if asset_dict["status"] != "under_construction":
        err(f"accumulate-cwip-cost requires an under_construction asset; "
            f"'{asset_dict['naming_series']}' is '{asset_dict['status']}'. Start one with add-cwip.")

    try:
        amount = to_decimal(args.amount)
    except (InvalidOperation, ValueError):
        err("--amount must be numeric")
    if amount <= 0:
        err("--amount must be greater than 0")

    _validate_cwip_account(conn, args.cwip_account_id)
    if not _account_exists(conn, args.source_account_id):
        err(f"Source account {args.source_account_id} not found")

    try:
        prior = _cwip_account_for_asset(conn, args.asset_id)
    except ValueError as e:
        err(str(e))
    if prior and prior != args.cwip_account_id:
        err(f"Asset is already accumulating to CWIP account {prior}; "
            f"pass the same --cwip-account-id (one CWIP account per asset).")

    posting_date = args.posting_date or _today_str()
    cost_center_id = args.cost_center_id or _get_cost_center(conn, asset_dict["company_id"])

    try:
        accum_id, gl_ids = _post_accumulation(
            conn, asset_dict, amount, args.cwip_account_id, args.source_account_id,
            args.source_voucher_type, args.source_voucher_id, posting_date,
            cost_center_id, args.notes)
    except (ValueError, NotImplementedError) as e:
        sys.stderr.write(f"[erpclaw-assets] {e}\n")
        err(f"GL posting failed: {e}")

    audit(conn, "erpclaw-assets", "accumulate-cwip-cost", "asset", args.asset_id,
          new_values={"amount": str(round_currency(amount)),
                      "source_voucher_type": args.source_voucher_type,
                      "accumulated_total": asset_dict["current_book_value"]},
          description=f"Accumulated {amount} into CWIP {asset_dict['naming_series']}")

    conn.commit()
    ok({"accumulation_id": accum_id, "asset_id": args.asset_id,
        "amount": str(round_currency(amount)),
        "accumulated_total": asset_dict["current_book_value"],
        "new_status": "under_construction", "gl_entry_ids": gl_ids,
        "message": f"Accumulated {amount} into CWIP {asset_dict['naming_series']}"})


def transfer_cwip_to_asset(conn, args):
    """Capitalize a finished CWIP asset into a depreciable fixed asset + post GL.

    Required: --asset-id
    Optional: --final-additional-cost X (+ --source-account-id for its credit leg;
              + --cwip-account-id if nothing was accumulated yet),
              --depreciation-start-date D (default today), --useful-life-years,
              --salvage-value, --cost-center-id

    Flips status under_construction -> in_use, posts DR Fixed Asset / CR CWIP for
    the accumulated cost, writes an asset_capitalization row (cwip_source_id =
    the CWIP asset id), and generates the initial depreciation schedule from the
    transfer date. Rejected if nothing was accumulated or depreciation was posted.
    """
    if not args.asset_id:
        err("--asset-id is required")

    asset = _validate_asset_exists(conn, args.asset_id)
    asset_dict = row_to_dict(asset)
    if asset_dict["status"] != "under_construction":
        err(f"transfer-cwip-to-asset requires an under_construction asset; "
            f"'{asset_dict['naming_series']}' is '{asset_dict['status']}'.")

    # Defensive: no depreciation may have been posted before transfer.
    ds_t = Table("depreciation_schedule")
    posted = conn.execute(
        (Q.from_(ds_t).select(fn.Count("*").as_("c"))
         .where(ds_t.asset_id == P())
         .where(ds_t.status == ValueWrapper("posted"))).get_sql(),
        (args.asset_id,)).fetchone()["c"]
    if posted:
        err("Cannot transfer: depreciation has already been posted against this asset.")

    cost_center_id = args.cost_center_id or _get_cost_center(conn, asset_dict["company_id"])

    # Optional final additional cost: route through the same accumulation path so
    # it lands in CWIP before the transfer (one transaction).
    if args.final_additional_cost is not None:
        try:
            final_amt = to_decimal(args.final_additional_cost)
        except (InvalidOperation, ValueError):
            err("--final-additional-cost must be numeric")
        if final_amt <= 0:
            err("--final-additional-cost must be greater than 0")
        if not args.source_account_id:
            err("--source-account-id is required with --final-additional-cost (its credit leg)")
        try:
            existing_cwip = _cwip_account_for_asset(conn, args.asset_id)
        except ValueError as e:
            err(str(e))
        final_cwip_acct = existing_cwip or args.cwip_account_id
        if not final_cwip_acct:
            err("--cwip-account-id is required for --final-additional-cost when no "
                "cost has been accumulated yet.")
        _validate_cwip_account(conn, final_cwip_acct)
        if not _account_exists(conn, args.source_account_id):
            err(f"Source account {args.source_account_id} not found")
        try:
            _post_accumulation(conn, asset_dict, final_amt, final_cwip_acct,
                               args.source_account_id, "transfer_final_cost", None,
                               args.depreciation_start_date or _today_str(),
                               cost_center_id, "Final additional cost at transfer")
        except (ValueError, NotImplementedError) as e:
            sys.stderr.write(f"[erpclaw-assets] {e}\n")
            err(f"GL posting failed: {e}")

    total = to_decimal(asset_dict["current_book_value"])
    if total <= 0:
        err("transfer-cwip-to-asset requires accumulated cost > 0; nothing to capitalize.")

    try:
        cwip_acct = _cwip_account_for_asset(conn, args.asset_id)
    except ValueError as e:
        err(str(e))
    if not cwip_acct:
        err("No CWIP account found for this asset (no accumulations). Accumulate cost first.")

    cat_dict, asset_acct, dep_acct, accum_acct = _category_accounts(conn, asset_dict)
    if not asset_acct:
        err("Asset category has no asset_account_id set")

    salvage = to_decimal(args.salvage_value or "0")
    if salvage < 0 or salvage >= total:
        err("--salvage-value must be >= 0 and less than the accumulated cost")
    useful_life = int(args.useful_life_years) if args.useful_life_years else cat_dict["useful_life_years"]
    dep_method = cat_dict["depreciation_method"]
    transfer_date = args.depreciation_start_date or _today_str()
    fiscal_year = _get_fiscal_year(conn, transfer_date)

    # Flip the asset to a normal, depreciable fixed asset.
    asset_t = Table("asset")
    conn.execute((Q.update(asset_t)
                  .set(Field("status"), ValueWrapper("in_use"))
                  .set(Field("gross_value"), P())
                  .set(Field("current_book_value"), P())
                  .set(Field("salvage_value"), P())
                  .set(Field("depreciation_method"), P())
                  .set(Field("useful_life_years"), P())
                  .set(Field("depreciation_start_date"), P())
                  .set(Field("updated_at"), LiteralValue("datetime('now')"))
                  .where(asset_t.id == P())).get_sql(),
                 (str(round_currency(total)), str(round_currency(total)),
                  str(round_currency(salvage)), dep_method, useful_life,
                  transfer_date, args.asset_id))

    cap_id = str(uuid.uuid4())
    cap_t = Table("asset_capitalization")
    conn.execute((Q.into(cap_t)
                  .columns("id", "asset_id", "cwip_source_id", "capitalized_amount",
                           "capitalization_date", "source_account_id")
                  .insert(P(), P(), P(), P(), P(), P())).get_sql(),
                 (cap_id, args.asset_id, args.asset_id, str(round_currency(total)),
                  transfer_date, cwip_acct))

    gl_entries = [
        {"account_id": asset_acct, "debit": str(round_currency(total)), "credit": "0",
         "cost_center_id": cost_center_id, "fiscal_year": fiscal_year,
         "project_id": asset_dict.get("cwip_project_id")},
        {"account_id": cwip_acct, "debit": "0", "credit": str(round_currency(total)),
         "cost_center_id": cost_center_id, "fiscal_year": fiscal_year,
         "project_id": asset_dict.get("cwip_project_id")},
    ]
    try:
        gl_ids = insert_gl_entries(
            conn, gl_entries, voucher_type="asset_capitalization", voucher_id=cap_id,
            posting_date=transfer_date, company_id=asset_dict["company_id"],
            remarks=f"CWIP transfer/capitalization of {asset_dict['naming_series']}")
    except (ValueError, NotImplementedError) as e:
        sys.stderr.write(f"[erpclaw-assets] {e}\n")
        err(f"GL posting failed: {e}")
    conn.execute((Q.update(cap_t).set(Field("gl_entry_id"), P())
                  .where(cap_t.id == P())).get_sql(), (gl_ids[0], cap_id))

    # Initial depreciation schedule from the transfer date.
    updated = row_to_dict(_validate_asset_exists(conn, args.asset_id))
    try:
        schedule_entries, _m, _d = _generate_schedule_core(conn, updated)
    except ValueError as e:
        err(f"Depreciation schedule generation failed: {e}")

    audit(conn, "erpclaw-assets", "transfer-cwip-to-asset", "asset", args.asset_id,
          old_values={"status": "under_construction"},
          new_values={"status": "in_use", "gross_value": str(round_currency(total)),
                      "capitalized_amount": str(round_currency(total))},
          description=f"Transferred CWIP {asset_dict['naming_series']} to fixed asset for {total}")

    conn.commit()
    ok({"asset_id": args.asset_id, "capitalization_id": cap_id,
        "capitalized_amount": str(round_currency(total)), "new_status": "in_use",
        "depreciation_start_date": transfer_date,
        "depreciation_entries_generated": len(schedule_entries),
        "gl_entry_ids": gl_ids,
        "message": f"CWIP {asset_dict['naming_series']} capitalized for {total} and placed in use"})


def cancel_cwip(conn, args):
    """Cancel an under-construction CWIP asset (cancel = reverse).

    Required: --asset-id, --reason
    Optional: --posting-date (default today)

    Posts mirror GL entries for every submitted accumulation, marks each
    accumulation 'reversed', and sets asset.status='cancelled' with zero carrying
    value. Only allowed before transfer-cwip-to-asset (and if no depreciation was
    posted — defensive).
    """
    if not args.asset_id:
        err("--asset-id is required")
    if not args.reason:
        err("--reason is required")

    asset = _validate_asset_exists(conn, args.asset_id)
    asset_dict = row_to_dict(asset)
    if asset_dict["status"] != "under_construction":
        err(f"cancel-cwip is only allowed before transfer; "
            f"'{asset_dict['naming_series']}' is '{asset_dict['status']}'.")

    ds_t = Table("depreciation_schedule")
    posted = conn.execute(
        (Q.from_(ds_t).select(fn.Count("*").as_("c"))
         .where(ds_t.asset_id == P())
         .where(ds_t.status == ValueWrapper("posted"))).get_sql(),
        (args.asset_id,)).fetchone()["c"]
    if posted:
        err("Cannot cancel: depreciation has already been posted against this asset.")

    # AVA-43: accumulations sourced from a submitted purchase invoice / journal
    # entry are backed by that document's GL (and its AP liability). cancel-cwip
    # reverses only its own cwip_capitalization legs (voucher_id=accum_id), so it
    # would strand a document's CWIP debit. Reject — the source document must be
    # cancelled instead (its own cancel reverses the CWIP leg).
    doc_sourced = conn.execute(
        "SELECT a.source_voucher_type, a.source_voucher_id FROM cwip_cost_accumulation a "
        "JOIN gl_entry g ON g.id = a.gl_entry_id "
        "WHERE a.asset_id = ? AND a.status = 'submitted' "
        "AND g.voucher_type != 'cwip_capitalization'",
        (args.asset_id,)).fetchall()
    if doc_sourced:
        refs = ", ".join(sorted({f"{r['source_voucher_type']} {r['source_voucher_id']}"
                                 for r in doc_sourced}))
        err(f"Cannot cancel-cwip: costs were accumulated from submitted documents "
            f"({refs}). Cancel those documents first — each reverses its own CWIP GL.")

    posting_date = args.posting_date or _today_str()
    accum_t = Table("cwip_cost_accumulation")
    rows = conn.execute(
        (Q.from_(accum_t).select(accum_t.id)
         .where(accum_t.asset_id == P())
         .where(accum_t.status == ValueWrapper("submitted"))).get_sql(),
        (args.asset_id,)).fetchall()

    reversal_ids = []
    for r in rows:
        try:
            rev = reverse_gl_entries(
                conn, voucher_type="cwip_capitalization", voucher_id=r["id"],
                posting_date=posting_date)
        except (ValueError, NotImplementedError) as e:
            sys.stderr.write(f"[erpclaw-assets] {e}\n")
            err(f"GL reversal failed: {e}")
        reversal_ids.extend(rev)
        conn.execute((Q.update(accum_t).set(Field("status"), ValueWrapper("reversed"))
                      .where(accum_t.id == P())).get_sql(), (r["id"],))

    asset_t = Table("asset")
    conn.execute((Q.update(asset_t)
                  .set(Field("status"), ValueWrapper("cancelled"))
                  .set(Field("gross_value"), ValueWrapper("0"))
                  .set(Field("current_book_value"), ValueWrapper("0"))
                  .set(Field("updated_at"), LiteralValue("datetime('now')"))
                  .where(asset_t.id == P())).get_sql(), (args.asset_id,))

    audit(conn, "erpclaw-assets", "cancel-cwip", "asset", args.asset_id,
          old_values={"status": "under_construction",
                      "current_book_value": asset_dict["current_book_value"]},
          new_values={"status": "cancelled", "current_book_value": "0"},
          description=f"Cancelled CWIP {asset_dict['naming_series']}: {args.reason}")

    conn.commit()
    ok({"asset_id": args.asset_id, "new_status": "cancelled",
        "accumulations_reversed": len(rows), "reversal_gl_entry_ids": reversal_ids,
        "message": f"CWIP {asset_dict['naming_series']} cancelled "
                   f"({len(rows)} accumulation(s) reversed)"})


def list_cwip_projects(conn, args):
    """List in-progress (under_construction) CWIP assets with cumulative cost.

    Optional: --company-id, --limit, --offset

    Returns each under-construction asset, its project (if any), the number of
    submitted accumulations, and the cumulative accumulated cost.
    """
    asset_t = Table("asset")
    q = (Q.from_(asset_t)
         .select(asset_t.id, asset_t.naming_series, asset_t.asset_name,
                 asset_t.asset_category_id, asset_t.cwip_project_id,
                 asset_t.current_book_value, asset_t.company_id)
         .where(asset_t.status == ValueWrapper("under_construction")))
    params = []
    if args.company_id:
        q = q.where(asset_t.company_id == P())
        params.append(args.company_id)
    q = q.orderby(asset_t.naming_series, order=Order.asc)
    try:
        limit = int(args.limit)
        offset = int(args.offset)
    except (ValueError, TypeError):
        limit, offset = 20, 0
    q = q.limit(limit).offset(offset)
    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()

    accum_t = Table("cwip_cost_accumulation")
    proj_t = Table("project")
    projects = []
    for r in rows:
        rd = row_to_dict(r)
        agg = conn.execute(
            (Q.from_(accum_t)
             .select(fn.Count("*").as_("n"), DecimalSum(accum_t.accumulated_amount).as_("total"))
             .where(accum_t.asset_id == P())
             .where(accum_t.status == ValueWrapper("submitted"))).get_sql(),
            (rd["id"],)).fetchone()
        project_name = None
        if rd.get("cwip_project_id"):
            prow = conn.execute(
                Q.from_(proj_t).select(proj_t.project_name).where(proj_t.id == P()).get_sql(),
                (rd["cwip_project_id"],)).fetchone()
            project_name = prow["project_name"] if prow else None
        projects.append({
            "asset_id": rd["id"], "naming_series": rd["naming_series"],
            "asset_name": rd["asset_name"], "asset_category_id": rd["asset_category_id"],
            "project_id": rd.get("cwip_project_id"), "project_name": project_name,
            "accumulation_count": agg["n"] or 0,
            "accumulated_cost": str(round_currency(to_decimal(agg["total"] or "0"))),
            "current_book_value": rd["current_book_value"],
        })

    ok({"cwip_projects": projects, "count": len(projects),
        "message": f"{len(projects)} CWIP asset(s) in progress"})


# ---------------------------------------------------------------------------
# ACTIONS dict
# ---------------------------------------------------------------------------

ACTIONS = {
    "add-asset-category": add_asset_category,
    "list-asset-categories": list_asset_categories,
    "add-asset": add_asset,
    "update-asset": update_asset,
    "get-asset": get_asset,
    "list-assets": list_assets,
    "generate-depreciation-schedule": generate_depreciation_schedule,
    "post-depreciation": post_depreciation,
    "run-depreciation": run_depreciation,
    "record-asset-movement": record_asset_movement,
    "schedule-maintenance": schedule_maintenance,
    "complete-maintenance": complete_maintenance,
    "dispose-asset": dispose_asset,
    "impair-asset": impair_asset,
    "reverse-impairment": reverse_impairment,
    "capitalize-asset": capitalize_asset,
    "revalue-asset": revalue_asset,
    "add-cwip": add_cwip,
    "accumulate-cwip-cost": accumulate_cwip_cost,
    "transfer-cwip-to-asset": transfer_cwip_to_asset,
    "cancel-cwip": cancel_cwip,
    "list-cwip-projects": list_cwip_projects,
    "asset-register-report": asset_register_report,
    "depreciation-summary": depreciation_summary,
    "status": status_action,
}


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def main():
    parser = SafeArgumentParser(description="ERPClaw Assets Skill")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # Entity IDs
    parser.add_argument("--company-id")
    parser.add_argument("--asset-id")
    parser.add_argument("--asset-category-id")
    parser.add_argument("--item-id")
    parser.add_argument("--depreciation-schedule-id")
    parser.add_argument("--maintenance-id")

    # Asset category fields
    parser.add_argument("--name")
    parser.add_argument("--depreciation-method")
    parser.add_argument("--useful-life-years")
    parser.add_argument("--asset-account-id")
    parser.add_argument("--depreciation-account-id")
    parser.add_argument("--accumulated-depreciation-account-id")

    # Asset fields
    parser.add_argument("--gross-value")
    parser.add_argument("--salvage-value")
    parser.add_argument("--purchase-date")
    parser.add_argument("--purchase-invoice-id")
    parser.add_argument("--depreciation-start-date")
    parser.add_argument("--location")
    parser.add_argument("--custodian-employee-id")
    parser.add_argument("--warranty-expiry-date")

    # Movement fields
    parser.add_argument("--movement-type")
    parser.add_argument("--movement-date")
    parser.add_argument("--from-location")
    parser.add_argument("--to-location")
    parser.add_argument("--from-employee-id")
    parser.add_argument("--to-employee-id")
    parser.add_argument("--reason")

    # Maintenance fields
    parser.add_argument("--maintenance-type")
    parser.add_argument("--scheduled-date")
    parser.add_argument("--actual-date")
    parser.add_argument("--cost")
    parser.add_argument("--performed-by")
    parser.add_argument("--description")
    parser.add_argument("--next-due-date")

    # Disposal fields
    parser.add_argument("--disposal-date")
    parser.add_argument("--disposal-method")
    parser.add_argument("--sale-amount")
    parser.add_argument("--buyer-details")

    # M7 asset-depth fields
    parser.add_argument("--impairment-id")
    parser.add_argument("--impairment-amount")
    parser.add_argument("--recoverable-amount")
    parser.add_argument("--impairment-date")
    parser.add_argument("--capitalized-amount")
    parser.add_argument("--capitalization-date")
    parser.add_argument("--source-account-id")
    parser.add_argument("--new-value")
    parser.add_argument("--reserve-account-id")
    parser.add_argument("--revaluation-date")
    parser.add_argument("--is-capex")
    parser.add_argument("--cash-account-id")
    parser.add_argument("--expense-account-id")

    # S3 CWIP fields
    parser.add_argument("--project-id")
    parser.add_argument("--amount")
    parser.add_argument("--cwip-account-id")
    parser.add_argument("--source-voucher-type")
    parser.add_argument("--source-voucher-id")
    parser.add_argument("--notes")
    parser.add_argument("--final-additional-cost")

    # GL / posting fields
    parser.add_argument("--posting-date")
    parser.add_argument("--cost-center-id")

    # Report fields
    parser.add_argument("--as-of-date")

    # Filters
    parser.add_argument("--status")
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--limit", default="20")
    parser.add_argument("--offset", default="0")
    parser.add_argument("--search")

    args, unknown = parser.parse_known_args()
    check_unknown_args(parser, unknown)
    check_input_lengths(args)

    db_path = args.db_path or DEFAULT_DB_PATH
    ensure_db_exists(db_path)
    conn = get_connection(db_path)

    # Dependency check
    _dep = check_required_tables(conn, REQUIRED_TABLES)
    if _dep:
        _dep["suggestion"] = "clawhub install " + " ".join(_dep.get("missing_skills", []))
        print(json.dumps(_dep, indent=2))
        conn.close()
        sys.exit(1)

    try:
        ACTIONS[args.action](conn, args)
    except Exception as e:
        conn.rollback()
        sys.stderr.write(f"[erpclaw-assets] {e}\n")
        err("An unexpected error occurred")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
