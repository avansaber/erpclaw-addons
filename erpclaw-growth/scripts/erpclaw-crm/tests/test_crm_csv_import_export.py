"""L1 pytest tests for erpclaw-crm Wave 1B F6 (CSV Import / Export — 8 actions).

Covers import-{leads,opportunities,crm-contacts,crm-companies} +
export-{leads,opportunities,crm-contacts,crm-companies}:

  - import happy path (counts match the committed fixtures)
  - missing-required-column rejected before any insert
  - all 3 --on-duplicate modes (skip / update / fail)
  - email-dedup (case-insensitive)
  - money-invalid rejected (no partial import)
  - --on-duplicate REQUIRED (missing flag → error)
  - export round-trip (export then re-import skip = 0 new)
  - udf-export (cf_<name> column present when M1 data exists)
  - export status/lifecycle filter
  - export overwrite-not-append
  - path-traversal blocked (../../etc/passwd)

Fixtures live at testing/fixtures/csv/ (committed FIRST, SIM-0 slice 0).
"""
import csv
import os
import sys
import uuid

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest
from crm_helpers import call_action, ns, is_ok, is_error, load_db_query, SRC_DIR

MOD = load_db_query()

# Repo root = parent of source/. Fixtures dir is repo-relative.
_REPO_ROOT = os.path.dirname(SRC_DIR)
_FIXTURES = os.path.join(_REPO_ROOT, "testing", "fixtures", "csv")


def fx(name):
    return os.path.join(_FIXTURES, name)


_DEFAULTS = dict(
    file=None, output=None, on_duplicate=None, include_udfs=False,
    status=None, stage=None, lifecycle=None,
    limit="20", offset="0", db_path=None, company_id=None,
)


def a(**kw):
    d = dict(_DEFAULTS)
    d.update(kw)
    return ns(**d)


# ---------------------------------------------------------------------------
# Seeds
# ---------------------------------------------------------------------------

def _seed_company(conn):
    cid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO company (id, name, abbr, default_currency, country, "
        "fiscal_year_start_month) VALUES (?,?,?,'USD','United States',1)",
        (cid, "Acme " + cid[:6], "A" + cid[:4]))
    for et, pfx in (("lead", "LEAD-"), ("opportunity", "OPP-")):
        conn.execute(
            "INSERT INTO naming_series (id, entity_type, prefix, "
            "current_value, company_id) VALUES (?,?,?,0,?)",
            (str(uuid.uuid4()), et, pfx, cid))
    conn.commit()
    conn.company_id = cid
    return cid


def _count(conn, table, cid):
    return conn.execute(
        f"SELECT COUNT(*) AS n FROM {table} WHERE company_id = ?",
        (cid,)).fetchone()["n"]


# ---------------------------------------------------------------------------
# Import — happy path (fixtures have known row counts)
# ---------------------------------------------------------------------------

def test_import_leads_happy(conn):
    cid = _seed_company(conn)
    r = call_action(MOD.ACTIONS["import-leads"], conn,
                    a(file=fx("leads-valid.csv"), on_duplicate="skip", company_id=cid))
    assert is_ok(r)
    assert r["imported"] == 5 and r["skipped"] == 0
    assert _count(conn, "lead", cid) == 5


def test_import_contacts_happy(conn):
    cid = _seed_company(conn)
    r = call_action(MOD.ACTIONS["import-crm-contacts"], conn,
                    a(file=fx("contacts-valid.csv"), on_duplicate="skip", company_id=cid))
    assert is_ok(r) and r["imported"] == 3
    assert _count(conn, "crm_contact", cid) == 3


def test_import_companies_happy(conn):
    cid = _seed_company(conn)
    r = call_action(MOD.ACTIONS["import-crm-companies"], conn,
                    a(file=fx("companies-valid.csv"), on_duplicate="skip", company_id=cid))
    assert is_ok(r) and r["imported"] == 3
    # annual_revenue is Decimal-normalized
    row = conn.execute(
        "SELECT annual_revenue FROM crm_company WHERE name='Acme Corporation' "
        "AND company_id=?", (cid,)).fetchone()
    assert row["annual_revenue"] == "5000000.00"


def test_import_opportunities_happy_weighted_revenue(conn):
    cid = _seed_company(conn)
    r = call_action(MOD.ACTIONS["import-opportunities"], conn,
                    a(file=fx("opportunities-valid.csv"), on_duplicate="skip", company_id=cid))
    assert is_ok(r) and r["imported"] == 3
    # weighted = expected_revenue * probability/100 = 50000.00 * 0.75
    row = conn.execute(
        "SELECT expected_revenue, weighted_revenue FROM opportunity "
        "WHERE opportunity_name='Acme renewal' AND company_id=?", (cid,)).fetchone()
    assert row["expected_revenue"] == "50000.00"
    assert row["weighted_revenue"] == "37500.00"


# ---------------------------------------------------------------------------
# Missing required column — rejected before any insert
# ---------------------------------------------------------------------------

def test_import_missing_required_column_rejected(conn):
    cid = _seed_company(conn)
    r = call_action(MOD.ACTIONS["import-leads"], conn,
                    a(file=fx("leads-missing-required.csv"), on_duplicate="skip", company_id=cid))
    assert is_error(r)
    assert "lead_name" in r["message"]
    assert _count(conn, "lead", cid) == 0  # nothing inserted


# ---------------------------------------------------------------------------
# --on-duplicate REQUIRED
# ---------------------------------------------------------------------------

def test_import_requires_on_duplicate(conn):
    cid = _seed_company(conn)
    r = call_action(MOD.ACTIONS["import-leads"], conn,
                    a(file=fx("leads-valid.csv"), on_duplicate=None, company_id=cid))
    assert is_error(r)
    assert "on-duplicate" in r["message"]
    assert _count(conn, "lead", cid) == 0


def test_import_bad_on_duplicate_rejected(conn):
    cid = _seed_company(conn)
    r = call_action(MOD.ACTIONS["import-leads"], conn,
                    a(file=fx("leads-valid.csv"), on_duplicate="merge", company_id=cid))
    assert is_error(r)


# ---------------------------------------------------------------------------
# Email dedup (case-insensitive) + all 3 modes
# ---------------------------------------------------------------------------

def test_contacts_skip_dedup_case_insensitive(conn):
    cid = _seed_company(conn)
    # contacts-duplicate-email.csv: jane@acme.com, JANE@ACME.COM (dup), bob@globex.com
    r = call_action(MOD.ACTIONS["import-crm-contacts"], conn,
                    a(file=fx("contacts-duplicate-email.csv"), on_duplicate="skip", company_id=cid))
    assert is_ok(r)
    assert r["imported"] == 2 and r["skipped"] == 1   # jane + bob in, JANE dup skipped
    assert _count(conn, "crm_contact", cid) == 2


def test_contacts_update_in_place_not_second_row(conn):
    cid = _seed_company(conn)
    # First load jane + bob.
    call_action(MOD.ACTIONS["import-crm-contacts"], conn,
                a(file=fx("contacts-duplicate-email.csv"), on_duplicate="skip", company_id=cid))
    before = _count(conn, "crm_contact", cid)
    # Re-import in update mode: all 3 rows are now duplicates → updates, no inserts.
    r = call_action(MOD.ACTIONS["import-crm-contacts"], conn,
                    a(file=fx("contacts-duplicate-email.csv"), on_duplicate="update", company_id=cid))
    assert is_ok(r) and r["imported"] == 0 and r["updated"] == 3
    assert _count(conn, "crm_contact", cid) == before   # no new row
    # jane's name was overwritten in place by the 2nd CSV row's value.
    row = conn.execute(
        "SELECT name FROM crm_contact WHERE lower(email)='jane@acme.com' "
        "AND company_id=?", (cid,)).fetchone()
    assert row["name"] == "Jane D. Updated"


def test_contacts_fail_mode_rolls_back(conn):
    cid = _seed_company(conn)
    call_action(MOD.ACTIONS["import-crm-contacts"], conn,
                a(file=fx("contacts-valid.csv"), on_duplicate="skip", company_id=cid))
    before = _count(conn, "crm_contact", cid)
    # contacts-duplicate-email shares jane@acme.com with contacts-valid → fail.
    r = call_action(MOD.ACTIONS["import-crm-contacts"], conn,
                    a(file=fx("contacts-duplicate-email.csv"), on_duplicate="fail", company_id=cid))
    assert is_error(r)
    assert _count(conn, "crm_contact", cid) == before   # full rollback


# ---------------------------------------------------------------------------
# Money invalid — rejected, no partial import
# ---------------------------------------------------------------------------

def test_import_money_invalid_rejected(conn):
    cid = _seed_company(conn)
    r = call_action(MOD.ACTIONS["import-opportunities"], conn,
                    a(file=fx("opportunities-money-invalid.csv"), on_duplicate="skip", company_id=cid))
    assert is_error(r)
    assert "expected_revenue" in r["message"]
    assert _count(conn, "opportunity", cid) == 0


# ---------------------------------------------------------------------------
# Export round-trip (export then re-import skip = 0 new)
# ---------------------------------------------------------------------------

def test_export_then_reimport_skip_is_noop(conn, tmp_path):
    cid = _seed_company(conn)
    call_action(MOD.ACTIONS["import-leads"], conn,
                a(file=fx("leads-valid.csv"), on_duplicate="skip", company_id=cid))
    out = str(tmp_path / "leads-out.csv")
    r = call_action(MOD.ACTIONS["export-leads"], conn, a(output=out, company_id=cid))
    assert is_ok(r) and r["exported"] == 5
    # Re-import the exported file with skip → everything is already present.
    r2 = call_action(MOD.ACTIONS["import-leads"], conn,
                     a(file=out, on_duplicate="skip", company_id=cid))
    assert is_ok(r2) and r2["imported"] == 0 and r2["skipped"] == 5
    assert _count(conn, "lead", cid) == 5


# ---------------------------------------------------------------------------
# Export filter (status / lifecycle)
# ---------------------------------------------------------------------------

def test_export_status_filter(conn, tmp_path):
    cid = _seed_company(conn)
    # Seed 2 leads: one 'new', one 'qualified'.
    for name, st in (("New One", "new"), ("Qual One", "qualified")):
        conn.execute(
            "INSERT INTO lead (id, lead_name, status, company_id) VALUES (?,?,?,?)",
            (str(uuid.uuid4()), name, st, cid))
    conn.commit()
    out = str(tmp_path / "qual.csv")
    r = call_action(MOD.ACTIONS["export-leads"], conn,
                    a(output=out, status="qualified", company_id=cid))
    assert is_ok(r) and r["exported"] == 1
    rows = list(csv.DictReader(open(out, encoding="utf-8-sig")))
    assert len(rows) == 1 and rows[0]["lead_name"] == "Qual One"


# ---------------------------------------------------------------------------
# Export overwrite (mode 'w', never append)
# ---------------------------------------------------------------------------

def test_export_overwrites_not_appends(conn, tmp_path):
    cid = _seed_company(conn)
    call_action(MOD.ACTIONS["import-crm-companies"], conn,
                a(file=fx("companies-valid.csv"), on_duplicate="skip", company_id=cid))
    out = str(tmp_path / "co.csv")
    call_action(MOD.ACTIONS["export-crm-companies"], conn, a(output=out, company_id=cid))
    first = list(csv.DictReader(open(out, encoding="utf-8-sig")))
    assert len(first) == 3
    # Filter to one lifecycle and re-export to the SAME path → file is replaced.
    call_action(MOD.ACTIONS["export-crm-companies"], conn,
                a(output=out, lifecycle="customer", company_id=cid))
    second = list(csv.DictReader(open(out, encoding="utf-8-sig")))
    assert len(second) == 1   # overwritten, not appended (would be 4 if appended)


# ---------------------------------------------------------------------------
# UDF export (cf_<name> column when M1 data present)
# ---------------------------------------------------------------------------

def test_export_includes_udf_columns(conn, tmp_path):
    from erpclaw_lib.custom_fields import add_custom_field, store_custom_field_values
    cid = _seed_company(conn)
    call_action(MOD.ACTIONS["import-leads"], conn,
                a(file=fx("leads-valid.csv"), on_duplicate="skip", company_id=cid))
    # Register a UDF on the lead table + set a value on one row.
    add_custom_field(conn, "lead", "segment", "text", "erpclaw-crm")
    lead = conn.execute(
        "SELECT id FROM lead WHERE lead_name='Acme expansion' AND company_id=?",
        (cid,)).fetchone()
    store_custom_field_values(conn, "lead", lead["id"], {"segment": "enterprise"})
    conn.commit()

    out = str(tmp_path / "leads-udf.csv")
    r = call_action(MOD.ACTIONS["export-leads"], conn,
                    a(output=out, include_udfs=True, company_id=cid))
    assert is_ok(r)
    assert "cf_segment" in r["udf_columns"]
    rows = list(csv.DictReader(open(out, encoding="utf-8-sig")))
    assert "cf_segment" in rows[0].keys()
    seg = {row["lead_name"]: row["cf_segment"] for row in rows}
    assert seg["Acme expansion"] == "enterprise"

    # Without --include-udfs the cf_ column is absent.
    out2 = str(tmp_path / "leads-noudf.csv")
    call_action(MOD.ACTIONS["export-leads"], conn, a(output=out2, company_id=cid))
    rows2 = list(csv.DictReader(open(out2, encoding="utf-8-sig")))
    assert "cf_segment" not in rows2[0].keys()


# ---------------------------------------------------------------------------
# Path-traversal blocked (import + export)
# ---------------------------------------------------------------------------

def test_import_path_traversal_blocked(conn):
    cid = _seed_company(conn)
    r = call_action(MOD.ACTIONS["import-leads"], conn,
                    a(file="../../../../etc/passwd", on_duplicate="skip", company_id=cid))
    assert is_error(r)
    assert ".csv" in r["message"]


def test_export_path_traversal_blocked(conn):
    cid = _seed_company(conn)
    r = call_action(MOD.ACTIONS["export-leads"], conn,
                    a(output="../../../../etc/passwd", company_id=cid))
    assert is_error(r)
