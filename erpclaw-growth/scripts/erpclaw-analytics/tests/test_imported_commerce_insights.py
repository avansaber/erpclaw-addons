"""Imported commerce insights v1: deterministic local report over already
imported Stripe, Shopify and bank rows.

Covers: exact 500.03 Decimal totals, stable ordering, date bounds, company
isolation, missing optional tables, malformed stored money refusal, two
identical read-only calls, and no writes. The report never connects to a
provider, calls a model, moves money, or sends data over a network.
"""
import importlib.util
import os
import sys
import uuid

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from analytics_helpers import (  # noqa: E402
    call_action, is_error, is_ok, load_db_query, ns,
    seed_accounts, seed_company,
)
from erpclaw_lib.query import P, Q, Table, insert_row  # noqa: E402
from erpclaw_lib.seam import get_engine  # noqa: E402

MOD = load_db_query()

Q1 = {"from_date": "2026-01-01", "to_date": "2026-03-31"}
_ADDONS_DIR = os.path.abspath(os.path.join(_TESTS_DIR, "..", "..", "..", ".."))


def _load_schema(name, folder):
    path = os.path.join(_ADDONS_DIR, folder, "init_db.py")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_STRIPE_SCHEMA = _load_schema(
    "commerce_insights_stripe_schema", "erpclaw-integrations-stripe")
_SHOPIFY_SCHEMA = _load_schema(
    "commerce_insights_shopify_schema", "erpclaw-integrations-shopify")


def _id():
    return str(uuid.uuid4())


def _insert(conn, table, row):
    sql, _ = insert_row(table, {k: P() for k in row})
    conn.execute(sql, tuple(row.values()))


def _ensure_imported_tables(conn):
    engine = get_engine(conn.db_path)
    for module, names in (
        (_STRIPE_SCHEMA, (
            "stripe_charge", "stripe_refund", "stripe_payout",
            "stripe_balance_transaction")),
        (_SHOPIFY_SCHEMA, (
            "shopify_order", "shopify_refund", "shopify_payout")),
    ):
        module.METADATA.create_all(
            engine, tables=[module.METADATA.tables[name] for name in names],
            checkfirst=True)


def _seed_bank(conn, company_id, cash_account_id, entries):
    stmt_id = _id()
    conn.execute(
        "INSERT INTO bank_statement (id, bank_account_id, company_id,"
        " source, currency, import_status, line_count, imported_at)"
        " VALUES (?, ?, ?, 'manual_csv', 'USD', 'imported', ?, '2026-01-01')",
        (stmt_id, cash_account_id, company_id, len(entries)),
    )
    for amount, txn_date in entries:
        _insert(conn, "bank_statement_line", {
            "id": _id(), "bank_statement_id": stmt_id,
            "bank_account_id": cash_account_id, "source": "manual_csv",
            "txn_date": txn_date, "amount": amount,
            "currency": "USD", "external_id": _id(),
        })
    conn.commit()
    return stmt_id


def _seed_500(conn):
    co = seed_company(conn, "Commerce Co", "CCO")
    acc = seed_accounts(conn, co)
    _ensure_imported_tables(conn)
    for amount in ("100.01", "200.01", "200.01"):
        _insert(conn, "stripe_charge", {
            "id": _id(), "company_id": co, "amount": amount,
            "created_stripe": "2026-01-10",
        })
    for amount in ("300.02", "200.01"):
        _insert(conn, "shopify_order", {
            "id": _id(), "company_id": co, "total_amount": amount,
            "order_date": "2026-01-12",
        })
    _seed_bank(conn, co, acc["cash"], [
        ("400.00", "2026-01-15"), ("100.03", "2026-01-16"),
        ("-50.00", "2026-01-17"),
    ])
    conn.commit()
    return {"company_id": co, "accounts": acc}


def _snapshot(conn, tables):
    snap = {}
    for name in tables:
        try:
            tbl = Table(name)
            query = Q.from_(tbl).select("*")
            rows = conn.execute(query.get_sql(), ()).fetchall()
        except Exception:
            snap[name] = "missing"
            continue
        dumped = []
        for row in rows:
            dumped.append(tuple(sorted(
                (k, "" if row[k] is None else str(row[k])) for k in row.keys())))
        snap[name] = sorted(dumped)
    return snap


def test_exact_500_03_totals(conn):
    book = _seed_500(conn)
    result = call_action(MOD.imported_commerce_insights, conn, ns(
        company_id=book["company_id"], **Q1))
    assert is_ok(result), result
    assert result["totals"]["stripe_revenue"] == "500.03"
    assert result["totals"]["shopify_revenue"] == "500.03"
    assert result["totals"]["bank_inflows"] == "500.03"
    assert result["totals"]["bank_outflows"] == "-50.00"
    assert result["totals"]["bank_net"] == "450.03"
    assert result["sources"]["stripe"]["totals"]["revenue"] == "500.03"
    assert result["sources"]["shopify"]["totals"]["revenue"] == "500.03"
    assert result["sources"]["bank"]["totals"]["inflows"] == "500.03"
    assert result["counts"]["stripe_charges"] == 3
    assert result["counts"]["shopify_orders"] == 2
    assert result["counts"]["bank_lines"] == 3


def test_stable_ordering(conn):
    co = seed_company(conn, "Commerce Order", "COR")
    _ensure_imported_tables(conn)
    first = _id()
    second = _id()
    _insert(conn, "stripe_charge", {
        "id": second, "company_id": co, "amount": "1500.00",
        "created_stripe": "2026-01-10",
    })
    _insert(conn, "stripe_charge", {
        "id": first, "company_id": co, "amount": "1200.00",
        "created_stripe": "2026-01-11",
    })
    _insert(conn, "stripe_refund", {
        "id": _id(), "company_id": co, "amount": "150.00",
        "created_stripe": "2026-01-12",
    })
    conn.commit()
    first_call = call_action(MOD.imported_commerce_insights, conn, ns(
        company_id=co, **Q1))
    second_call = call_action(MOD.imported_commerce_insights, conn, ns(
        company_id=co, **Q1))
    assert is_ok(first_call), first_call
    assert first_call == second_call
    kinds = [(f["finding"], tuple(f["ids"])) for f in first_call["findings"]]
    assert kinds == sorted(kinds)
    large = [f for f in first_call["findings"]
             if f["finding"] == "large_stripe_charge"]
    assert [f["ids"] for f in large] == sorted(f["ids"] for f in large)


def test_date_bounds(conn):
    co = seed_company(conn, "Commerce Dates", "CDT")
    _ensure_imported_tables(conn)
    _insert(conn, "stripe_charge", {
        "id": _id(), "company_id": co, "amount": "100.00",
        "created_stripe": "2026-01-10",
    })
    _insert(conn, "stripe_charge", {
        "id": _id(), "company_id": co, "amount": "200.00",
        "created_stripe": "2026-02-15",
    })
    _insert(conn, "stripe_charge", {
        "id": _id(), "company_id": co, "amount": "400.00",
        "created_stripe": "2026-03-05",
    })
    conn.commit()
    jan_feb = call_action(MOD.imported_commerce_insights, conn, ns(
        company_id=co, from_date="2026-01-01", to_date="2026-02-28"))
    assert is_ok(jan_feb), jan_feb
    assert jan_feb["totals"]["stripe_revenue"] == "300.00"
    assert jan_feb["counts"]["stripe_charges"] == 2
    march = call_action(MOD.imported_commerce_insights, conn, ns(
        company_id=co, from_date="2026-03-01", to_date="2026-03-31"))
    assert is_ok(march), march
    assert march["totals"]["stripe_revenue"] == "400.00"


def test_company_isolation(conn):
    co_a = seed_company(conn, "Commerce A", "CCA")
    co_b = seed_company(conn, "Commerce B", "CCB")
    acc_a = seed_accounts(conn, co_a)
    acc_b = seed_accounts(conn, co_b)
    _ensure_imported_tables(conn)
    _insert(conn, "stripe_charge", {
        "id": _id(), "company_id": co_a, "amount": "111.11",
        "created_stripe": "2026-01-10",
    })
    _insert(conn, "stripe_charge", {
        "id": _id(), "company_id": co_b, "amount": "9999.99",
        "created_stripe": "2026-01-10",
    })
    _insert(conn, "shopify_order", {
        "id": _id(), "company_id": co_b, "total_amount": "8888.88",
        "order_date": "2026-01-10",
    })
    _seed_bank(conn, co_a, acc_a["cash"], [("10.00", "2026-01-10")])
    _seed_bank(conn, co_b, acc_b["cash"], [("20.00", "2026-01-10")])
    conn.commit()
    result_a = call_action(MOD.imported_commerce_insights, conn, ns(
        company_id=co_a, **Q1))
    assert is_ok(result_a), result_a
    assert result_a["totals"]["stripe_revenue"] == "111.11"
    assert result_a["totals"]["shopify_revenue"] == "0.00"
    assert result_a["totals"]["bank_inflows"] == "10.00"
    result_b = call_action(MOD.imported_commerce_insights, conn, ns(
        company_id=co_b, **Q1))
    assert is_ok(result_b), result_b
    assert result_b["totals"]["stripe_revenue"] == "9999.99"
    assert result_b["totals"]["shopify_revenue"] == "8888.88"
    assert result_b["totals"]["bank_inflows"] == "20.00"


def test_missing_optional_tables(conn):
    co = seed_company(conn, "Commerce Bare", "CBR")
    result = call_action(MOD.imported_commerce_insights, conn, ns(
        company_id=co, **Q1))
    assert is_ok(result), result
    assert result["sources"]["stripe"]["status"] == "unavailable"
    assert result["sources"]["shopify"]["status"] == "unavailable"
    assert result["totals"]["stripe_revenue"] == "0.00"
    assert result["totals"]["shopify_revenue"] == "0.00"


def test_malformed_stored_money_refusal(conn):
    co = seed_company(conn, "Commerce Bad", "CBD")
    _ensure_imported_tables(conn)
    _insert(conn, "stripe_charge", {
        "id": _id(), "company_id": co, "amount": "not-a-number",
        "created_stripe": "2026-01-10",
    })
    conn.commit()
    result = call_action(MOD.imported_commerce_insights, conn, ns(
        company_id=co, **Q1))
    assert is_error(result), result


def test_two_identical_read_only_calls(conn):
    book = _seed_500(conn)
    args = ns(company_id=book["company_id"], **Q1)
    first = call_action(MOD.imported_commerce_insights, conn, args)
    second = call_action(MOD.imported_commerce_insights, conn, args)
    assert is_ok(first), first
    assert is_ok(second), second
    assert first == second


def test_no_writes(conn):
    book = _seed_500(conn)
    tables = ["stripe_charge", "stripe_refund", "stripe_payout",
              "stripe_balance_transaction", "shopify_order",
              "shopify_refund", "shopify_payout",
              "bank_statement", "bank_statement_line",
              "company", "account", "gl_entry"]
    before = _snapshot(conn, tables)
    result = call_action(MOD.imported_commerce_insights, conn, ns(
        company_id=book["company_id"], **Q1))
    assert is_ok(result), result
    assert _snapshot(conn, tables) == before
