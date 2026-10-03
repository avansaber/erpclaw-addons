"""Stock analytics filter and rank on the numeric value of TEXT totals.

abc-analysis keeps only items whose summed stock value is positive and ranks
them by that value; aging-inventory keeps only items with stock on hand. The
sums come back as TEXT from decimal_sum on both backends, so the comparison
must be numeric: compared as text, "9.00" ranks above "50.00", and an item
whose movements net to zero would stay in the report.
"""
import uuid

from erpclaw_lib.query import P, insert_row

from analytics_helpers import call_action, is_ok, load_db_query, ns

MOD = load_db_query()


def _seed_stock(conn, company_id):
    warehouse_id = str(uuid.uuid4())
    sql, _ = insert_row("warehouse", {"id": P(), "name": P(), "company_id": P()})
    conn.execute(sql, (warehouse_id, "Main Store", company_id))

    items = {}
    sql, _ = insert_row("item", {"id": P(), "item_code": P(), "item_name": P()})
    for code, name in (("BIG", "Big Seller"), ("SMALL", "Small Seller"), ("GONE", "Sold Out")):
        items[code] = str(uuid.uuid4())
        conn.execute(sql, (items[code], code, name))

    sql, _ = insert_row("stock_ledger_entry", {
        "id": P(), "posting_date": P(), "item_id": P(), "warehouse_id": P(),
        "voucher_type": P(), "voucher_id": P(), "actual_qty": P(),
        "stock_value_difference": P(), "is_cancelled": P(),
    })
    for code, posting_date, qty, value in (
            ("BIG", "2026-01-10", "5", "50.00"),
            ("SMALL", "2026-02-10", "3", "9.00"),
            ("GONE", "2026-01-15", "4", "40.00"),
            ("GONE", "2026-02-15", "-4", "-40.00")):
        conn.execute(sql, (str(uuid.uuid4()), posting_date, items[code], warehouse_id,
                           "stock_entry", str(uuid.uuid4()), qty, value, 0))
    conn.commit()
    return items


def test_abc_analysis_ranks_by_numeric_value_and_drops_empty_items(conn, env):
    items = _seed_stock(conn, env["company_id"])

    result = call_action(MOD.abc_analysis, conn, ns(
        company_id=env["company_id"], as_of_date="2026-03-31"))

    assert is_ok(result), result
    assert [row["item_id"] for row in result["items"]] == [items["BIG"], items["SMALL"]]
    assert [row["value"] for row in result["items"]] == ["50.00", "9.00"]
    assert result["total_value"] == "59.00"


def test_aging_inventory_keeps_only_items_on_hand(conn, env):
    items = _seed_stock(conn, env["company_id"])

    result = call_action(MOD.aging_inventory, conn, ns(
        company_id=env["company_id"], as_of_date="2026-03-31",
        aging_buckets="30,60,90,120"))

    assert is_ok(result), result
    assert sorted(row["item_id"] for row in result["items"]) == sorted(
        [items["BIG"], items["SMALL"]])
    assert result["total_value"] == "59.00"
