"""Company scope for quality-dashboard: inspections and open
non-conformances count only the asking company's reference documents.

An inspection belongs to company C when its reference_type/reference_id
names a purchase_receipt, delivery_note or stock_entry row whose company_id
is C. Inspections with no reference (or a dangling reference) are
unattributed. A non-conformance belongs to C through its
quality_inspection_id. Quality goals have no company link and stay
install-wide.
"""
import os
import sys
import uuid

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from quality_helpers import (  # noqa: E402
    call_action, is_error, is_ok, load_db_query, ns, seed_company, _uuid,
)
from erpclaw_lib.query import Field, P, Q, Table, fn, insert_row  # noqa: E402

M = load_db_query()

_SNAPSHOT_TABLES = (
    "quality_inspection",
    "quality_inspection_reading",
    "quality_inspection_template",
    "quality_inspection_parameter",
    "non_conformance",
    "quality_goal",
    "naming_series",
    "audit_log",
    "gl_entry",
    "stock_ledger_entry",
)


def _snapshot(conn):
    snap = {}
    for name in _SNAPSHOT_TABLES:
        tbl = Table(name)
        rows = conn.execute(Q.from_(tbl).select(tbl.star).get_sql()).fetchall()
        snap[name] = sorted(
            tuple(sorted((key, str(value)) for key, value in dict(row).items()))
            for row in rows
        )
    return snap


def _insert(conn, table, row):
    sql, _ = insert_row(table, {k: P() for k in row})
    conn.execute(sql, tuple(row.values()))


def _seed_two_company_book(conn):
    comp_a = seed_company(conn, "Scope Dash A", "SDA")
    comp_b = seed_company(conn, "Scope Dash B", "SDB")
    item = _uuid()
    _insert(conn, "item", {"id": item, "item_code": "QD-SCOPE",
                           "item_name": "Dash Scope Widget"})
    supplier_a = _uuid()
    _insert(conn, "supplier", {"id": supplier_a, "name": "Supplier A",
                               "company_id": comp_a})
    supplier_b = _uuid()
    _insert(conn, "supplier", {"id": supplier_b, "name": "Supplier B",
                               "company_id": comp_b})
    receipt_a = _uuid()
    _insert(conn, "purchase_receipt", {
        "id": receipt_a, "supplier_id": supplier_a,
        "posting_date": "2026-01-05", "company_id": comp_a})
    receipt_b = _uuid()
    _insert(conn, "purchase_receipt", {
        "id": receipt_b, "supplier_id": supplier_b,
        "posting_date": "2026-01-06", "company_id": comp_b})
    insp_a = _uuid()
    _insert(conn, "quality_inspection", {
        "id": insp_a, "inspection_type": "incoming", "item_id": item,
        "inspection_date": "2026-01-10", "status": "accepted",
        "reference_type": "purchase_receipt", "reference_id": receipt_a})
    insp_b = _uuid()
    _insert(conn, "quality_inspection", {
        "id": insp_b, "inspection_type": "incoming", "item_id": item,
        "inspection_date": "2026-01-11", "status": "rejected",
        "reference_type": "purchase_receipt", "reference_id": receipt_b})
    _insert(conn, "non_conformance", {
        "id": _uuid(), "description": "B NCR",
        "quality_inspection_id": insp_b, "severity": "major",
        "status": "open"})
    _insert(conn, "quality_goal", {
        "id": _uuid(), "name": "Install goal", "target_value": "5",
        "current_value": "1"})
    conn.commit()
    return {"company_a": comp_a, "company_b": comp_b, "item": item,
            "insp_a": insp_a, "insp_b": insp_b}


def test_dashboard_scoped_by_company(conn):
    book = _seed_two_company_book(conn)
    before = _snapshot(conn)
    r = call_action(M.quality_dashboard, conn, ns(
        company_id=book["company_a"]))
    assert is_ok(r), r
    dash = r["dashboard"]
    assert dash["inspections"]["total"] == 1
    assert dash["inspections"]["by_status"] == {"accepted": 1}
    assert dash["non_conformances"]["total_open"] == 0
    assert dash["non_conformances"]["by_severity"] == {}
    assert dash["company_id"] == book["company_a"]
    assert dash["unattributed_inspections"] == 0
    assert dash["quality_goals_scope"] == "install"
    assert dash["quality_goals"]["total"] == 1
    assert _snapshot(conn) == before


def test_dashboard_refuses_without_company_when_two_exist(conn):
    book = _seed_two_company_book(conn)
    before = _snapshot(conn)
    r = call_action(M.quality_dashboard, conn, ns())
    assert is_error(r)
    assert r["message"] == (
        "--company-id is required when more than one company exists")
    assert _snapshot(conn) == before


def test_dashboard_single_company_unchanged(conn, env):
    _insert(conn, "quality_inspection", {
        "id": _uuid(), "inspection_type": "incoming",
        "item_id": env["item_id"], "inspection_date": "2026-01-10",
        "status": "accepted"})
    _insert(conn, "quality_inspection", {
        "id": _uuid(), "inspection_type": "incoming",
        "item_id": env["item_id"], "inspection_date": "2026-01-11",
        "status": "rejected"})
    conn.commit()
    before = _snapshot(conn)
    r = call_action(M.quality_dashboard, conn, ns())
    assert is_ok(r), r
    dash = r["dashboard"]
    assert dash["inspections"]["total"] == 2
    assert dash["inspections"]["by_status"] == {
        "accepted": 1, "rejected": 1}
    assert dash["company_id"] is None
    assert dash["unattributed_inspections"] == 2
    assert _snapshot(conn) == before
