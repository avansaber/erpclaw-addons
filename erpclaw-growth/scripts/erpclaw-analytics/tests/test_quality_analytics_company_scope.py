"""Company scope for quality-analytics: inspections and non-conformances
count only the asking company's reference documents.

An inspection belongs to company C when its reference_type/reference_id
names a purchase_receipt, delivery_note or stock_entry row whose company_id
is C. Inspections with no reference (or a dangling reference) are
unattributed: reported in unattributed_inspections, never counted.
A non-conformance belongs to C through its quality_inspection_id.
"""
import os
import sys
import uuid

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from analytics_helpers import (  # noqa: E402
    call_action, is_error, is_ok, load_db_query, ns, seed_company,
)
from erpclaw_lib.query import P, Q, Table, insert_row  # noqa: E402

MOD = load_db_query()

WINDOW = {"from_date": "2026-01-01", "to_date": "2026-03-31"}


def _id():
    return str(uuid.uuid4())


def _insert(conn, table, row):
    sql, _ = insert_row(table, {k: P() for k in row})
    conn.execute(sql, tuple(row.values()))


def _snapshot(conn, tables):
    snap = {}
    for name in tables:
        tbl = Table(name)
        q = Q.from_(tbl).select("*")
        rows = conn.execute(q.get_sql(), ()).fetchall()
        dumped = []
        for r in rows:
            dumped.append(tuple(sorted(
                (k, "" if r[k] is None else str(r[k])) for k in r.keys())))
        snap[name] = sorted(dumped)
    return snap


def _seed_two_company_book(conn):
    comp_a = seed_company(conn, "Scope Quality A", "SQA")
    comp_b = seed_company(conn, "Scope Quality B", "SQB")
    item = _id()
    _insert(conn, "item", {"id": item, "item_code": "QITEM-SCOPE",
                           "item_name": "Scope Widget"})
    supplier_a = _id()
    _insert(conn, "supplier", {"id": supplier_a, "name": "Supplier A",
                               "company_id": comp_a})
    supplier_b = _id()
    _insert(conn, "supplier", {"id": supplier_b, "name": "Supplier B",
                               "company_id": comp_b})
    receipt_a = _id()
    _insert(conn, "purchase_receipt", {
        "id": receipt_a, "supplier_id": supplier_a,
        "posting_date": "2026-01-05", "company_id": comp_a})
    receipt_b = _id()
    _insert(conn, "purchase_receipt", {
        "id": receipt_b, "supplier_id": supplier_b,
        "posting_date": "2026-01-06", "company_id": comp_b})
    insp_a = _id()
    _insert(conn, "quality_inspection", {
        "id": insp_a, "inspection_type": "incoming", "item_id": item,
        "inspection_date": "2026-01-10", "status": "accepted",
        "reference_type": "purchase_receipt", "reference_id": receipt_a})
    insp_b = _id()
    _insert(conn, "quality_inspection", {
        "id": insp_b, "inspection_type": "incoming", "item_id": item,
        "inspection_date": "2026-01-11", "status": "rejected",
        "reference_type": "purchase_receipt", "reference_id": receipt_b})
    conn.commit()
    return {"company_a": comp_a, "company_b": comp_b, "item": item,
            "receipt_a": receipt_a, "receipt_b": receipt_b,
            "insp_a": insp_a, "insp_b": insp_b}


def test_other_company_inspection_not_counted(conn):
    book = _seed_two_company_book(conn)
    r_a = call_action(MOD.quality_analytics, conn, ns(
        company_id=book["company_a"], **WINDOW))
    assert is_ok(r_a), r_a
    assert r_a["inspections"] == {"total": 1, "passed": 1, "failed": 0,
                                  "pass_rate": "100.0%"}
    assert r_a["company_id"] == book["company_a"]
    assert r_a["unattributed_inspections"] == 0
    r_b = call_action(MOD.quality_analytics, conn, ns(
        company_id=book["company_b"], **WINDOW))
    assert is_ok(r_b), r_b
    assert r_b["inspections"] == {"total": 1, "passed": 0, "failed": 1,
                                  "pass_rate": "0.0%"}
    assert r_b["company_id"] == book["company_b"]
    assert r_b["unattributed_inspections"] == 0


def test_unattributed_inspection_reported_not_counted(conn):
    book = _seed_two_company_book(conn)
    _insert(conn, "quality_inspection", {
        "id": _id(), "inspection_type": "incoming", "item_id": book["item"],
        "inspection_date": "2026-01-12", "status": "accepted"})
    conn.commit()
    r_a = call_action(MOD.quality_analytics, conn, ns(
        company_id=book["company_a"], **WINDOW))
    assert is_ok(r_a), r_a
    assert r_a["inspections"] == {"total": 1, "passed": 1, "failed": 0,
                                  "pass_rate": "100.0%"}
    assert r_a["unattributed_inspections"] == 1


def test_non_conformance_scoped_through_inspection(conn):
    book = _seed_two_company_book(conn)
    _insert(conn, "non_conformance", {
        "id": _id(), "description": "Scoped NCR",
        "quality_inspection_id": book["insp_b"], "severity": "major",
        "status": "open", "created_at": "2026-01-15"})
    conn.commit()
    r_a = call_action(MOD.quality_analytics, conn, ns(
        company_id=book["company_a"], **WINDOW))
    assert is_ok(r_a), r_a
    assert r_a["non_conformances"] == 0
    r_b = call_action(MOD.quality_analytics, conn, ns(
        company_id=book["company_b"], **WINDOW))
    assert is_ok(r_b), r_b
    assert r_b["non_conformances"] == 1


def test_delivery_note_and_stock_entry_references(conn):
    book = _seed_two_company_book(conn)
    customer_a = _id()
    _insert(conn, "customer", {"id": customer_a, "name": "Customer A",
                               "company_id": book["company_a"]})
    delivery_a = _id()
    _insert(conn, "delivery_note", {
        "id": delivery_a, "customer_id": customer_a,
        "posting_date": "2026-01-07", "company_id": book["company_a"]})
    stock_a = _id()
    _insert(conn, "stock_entry", {
        "id": stock_a, "stock_entry_type": "material_receipt",
        "posting_date": "2026-01-08", "company_id": book["company_a"]})
    _insert(conn, "quality_inspection", {
        "id": _id(), "inspection_type": "outgoing", "item_id": book["item"],
        "inspection_date": "2026-01-13", "status": "accepted",
        "reference_type": "delivery_note", "reference_id": delivery_a})
    _insert(conn, "quality_inspection", {
        "id": _id(), "inspection_type": "in_process", "item_id": book["item"],
        "inspection_date": "2026-01-14", "status": "accepted",
        "reference_type": "stock_entry", "reference_id": stock_a})
    conn.commit()
    r_a = call_action(MOD.quality_analytics, conn, ns(
        company_id=book["company_a"], **WINDOW))
    assert is_ok(r_a), r_a
    assert r_a["inspections"] == {"total": 3, "passed": 3, "failed": 0,
                                  "pass_rate": "100.0%"}
    assert r_a["unattributed_inspections"] == 0


def test_writes_nothing(conn):
    book = _seed_two_company_book(conn)
    tables = ["quality_inspection", "non_conformance", "quality_goal",
              "gl_entry"]
    before = _snapshot(conn, tables)
    r = call_action(MOD.quality_analytics, conn, ns(
        company_id=book["company_a"], **WINDOW))
    assert is_ok(r), r
    assert _snapshot(conn, tables) == before
