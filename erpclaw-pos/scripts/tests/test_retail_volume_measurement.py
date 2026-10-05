"""Retail volume measurement v1 for erpclaw-pos.

Proves the read-only pos-retail-volume-measurement action: exact Decimal
totals (500.03), company and location isolation, inclusive date bounds,
refund handling, deterministic ordering, missing-table unavailability,
identical repeat reads, and no writes.
"""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from pos_helpers import (  # noqa: E402
    call_action, get_conn, is_error, is_ok, load_db_query, ns,
    seed_company, seed_item, seed_naming_series, seed_open_session,
    seed_pos_profile, seed_return_document,
)
from erpclaw_lib.query import Q, P, Table  # noqa: E402

MOD = load_db_query()
A = MOD.ACTIONS

ACTION = "pos-retail-volume-measurement"
FROM = "2026-03-01"
TO = "2026-03-31"


def _ok(action, conn, **kw):
    r = call_action(A[action], conn, ns(**kw))
    assert is_ok(r), f"{action} failed: {r}"
    return r


def _set(conn, table, row_id, **cols):
    for column, value in cols.items():
        t = Table(table)
        conn.execute(
            Q.update(t).set(t[column], P()).where(t.id == P()).get_sql(),
            (value, row_id))
    conn.commit()


def _sale(conn, session_id, item_id, qty, rate, ts):
    r = _ok("pos-add-transaction", conn, pos_session_id=session_id,
            customer_id=None, customer_name="Volume Sam")
    txn = r["id"]
    _ok("pos-add-transaction-item", conn, pos_transaction_id=txn,
        item_id=item_id, item_name=None, qty=qty, rate=rate, uom=None,
        barcode=None, discount_pct=None)
    _set(conn, "pos_transaction", txn, created_at=ts, status="submitted")
    return txn


def _seed_500_03(conn, session_id, item_id):
    t1 = _sale(conn, session_id, item_id, "1", "100.01",
               "2026-03-10 09:15:00")
    t2 = _sale(conn, session_id, item_id, "2", "200.01",
               "2026-03-10 14:40:00")
    return t1, t2


def _measure(conn, company_id, from_date=FROM, to_date=TO, **kw):
    return _ok(ACTION, conn, company_id=company_id,
               from_date=from_date, to_date=to_date, **kw)


def _dump(conn):
    snap = {}
    for table in ("pos_profile", "pos_session", "pos_transaction",
                  "pos_transaction_item", "pos_payment",
                  "audit_log", "company"):
        try:
            rows = conn.execute(
                Q.from_(Table(table)).select(Table(table).star)
                .orderby(Table(table).id).get_sql()).fetchall()
        except Exception:
            continue
        snap[table] = [tuple("" if v is None else str(v) for v in tuple(r))
                       for r in rows]
    return snap


# ---------------------------------------------------------------------------
# exact totals
# ---------------------------------------------------------------------------

def test_exact_500_03_totals(conn, env):
    _seed_500_03(conn, env["session_id"], env["item_id"])
    r = _measure(conn, env["company_id"])
    assert r["measurement_scope"] == "recorded_local_pos_rows"
    assert r["available"] is True
    assert r["transaction_count"] == 2
    assert r["line_count"] == 2
    assert r["units"] == "3.00"
    assert r["gross_sales"] == "500.03"
    assert r["discounts"] == "0.00"
    assert r["tax"] == "0.00"
    assert r["refunds"] == "0.00"
    assert r["refund_count"] == 0
    assert r["net_sales"] == "500.03"
    assert r["first_timestamp"] == "2026-03-10 09:15:00"
    assert r["last_timestamp"] == "2026-03-10 14:40:00"
    assert r["throughput_guarantee"] is False
    assert "not a throughput guarantee" in r["note"]
    assert r["company_id"] == env["company_id"]
    assert (r["from_date"], r["to_date"]) == (FROM, TO)


def test_action_aliases_route_to_same_function():
    assert A["pos-retail-volume"] is A[ACTION]
    assert A["pos-retail-volume-report"] is A[ACTION]
    assert A["pos-measure-retail-volume"] is A[ACTION]


# ---------------------------------------------------------------------------
def test_gross_discount_tax_and_net_keep_distinct_money_meanings(conn, env):
    txn = _sale(conn, env["session_id"], env["item_id"], "1", "100.00",
                "2026-03-10 09:15:00")
    _set(conn, "pos_transaction", txn, subtotal="100.00",
         discount_amount="10.00", tax_amount="4.50", grand_total="94.50")
    r = _measure(conn, env["company_id"])
    assert (r["gross_sales"], r["discounts"], r["tax"], r["net_sales"]) == (
        "100.00", "10.00", "4.50", "94.50")
    assert r["by_day"][0]["net_sales"] == "94.50"
    assert r["by_location"][0]["net_sales"] == "94.50"



# isolation
# ---------------------------------------------------------------------------

def test_company_isolation(conn, env):
    _seed_500_03(conn, env["session_id"], env["item_id"])
    other = seed_company(conn)
    seed_naming_series(conn, other)
    oprof = seed_pos_profile(conn, other, name="Other Register")
    osess = seed_open_session(conn, oprof, cashier="Other Cashier")
    _sale(conn, osess, env["item_id"], "1", "999.99",
          "2026-03-11 10:00:00")
    r = _measure(conn, env["company_id"])
    assert (r["gross_sales"], r["net_sales"]) == ("500.03", "500.03")
    assert r["transaction_count"] == 2
    ro = _measure(conn, other)
    assert (ro["gross_sales"], ro["transaction_count"]) == ("999.99", 1)


def test_location_isolation_and_filter(conn, env):
    _seed_500_03(conn, env["session_id"], env["item_id"])
    prof_b = seed_pos_profile(conn, env["company_id"], name="Second Register")
    sess_b = seed_open_session(conn, prof_b, cashier="Second Cashier")
    _sale(conn, sess_b, env["item_id"], "1", "50.00",
          "2026-03-12 11:00:00")

    everything = _measure(conn, env["company_id"])
    assert everything["gross_sales"] == "550.03"
    assert everything["transaction_count"] == 3
    loc_ids = [row["location_id"] for row in everything["by_location"]]
    assert loc_ids == sorted(loc_ids)
    assert len(loc_ids) == 2

    only_a = _measure(conn, env["company_id"],
                      location_id=env["profile_id"])
    assert (only_a["gross_sales"], only_a["transaction_count"]) == ("500.03", 2)
    assert [row["location_id"] for row in only_a["by_location"]] == [env["profile_id"]]

    only_b = _measure(conn, env["company_id"], location_id=prof_b)
    assert (only_b["gross_sales"], only_b["transaction_count"]) == ("50.00", 1)


def test_foreign_location_refused(conn, env):
    other = seed_company(conn)
    seed_naming_series(conn, other)
    oprof = seed_pos_profile(conn, other, name="Foreign Register")
    r = call_action(A[ACTION], conn, ns(
        company_id=env["company_id"], from_date=FROM, to_date=TO,
        location_id=oprof))
    assert is_error(r)


def test_unknown_location_refused(conn, env):
    r = call_action(A[ACTION], conn, ns(
        company_id=env["company_id"], from_date=FROM, to_date=TO,
        location_id="no-such-location"))
    assert is_error(r)


# ---------------------------------------------------------------------------
# date bounds and validation
# ---------------------------------------------------------------------------

def test_date_bounds(conn, env):
    _seed_500_03(conn, env["session_id"], env["item_id"])
    _sale(conn, env["session_id"], env["item_id"], "1", "10.00",
          "2026-02-28 23:59:00")
    _sale(conn, env["session_id"], env["item_id"], "1", "20.00",
          "2026-04-01 00:01:00")
    r = _measure(conn, env["company_id"])
    assert (r["gross_sales"], r["transaction_count"]) == ("500.03", 2)

    day = _measure(conn, env["company_id"],
                   from_date="2026-03-10", to_date="2026-03-10")
    assert (day["gross_sales"], day["transaction_count"]) == ("500.03", 2)

    empty = _measure(conn, env["company_id"],
                     from_date="2026-03-11", to_date="2026-03-12")
    assert empty["transaction_count"] == 0
    assert empty["gross_sales"] == "0.00"
    assert empty["net_sales"] == "0.00"
    assert empty["first_timestamp"] is None
    assert empty["last_timestamp"] is None


def test_invalid_and_reversed_dates_refused(conn, env):
    for bad in ("not-a-date", "2026-13-01", "2026-03-35", "10/03/2026"):
        r = call_action(A[ACTION], conn, ns(
            company_id=env["company_id"], from_date=bad, to_date=TO))
        assert is_error(r), bad
    r = call_action(A[ACTION], conn, ns(
        company_id=env["company_id"], from_date=TO, to_date=FROM))
    assert is_error(r)


def test_missing_and_unknown_company_refused(conn, env):
    r = call_action(A[ACTION], conn, ns(
        company_id=None, from_date=FROM, to_date=TO))
    assert is_error(r)
    r = call_action(A[ACTION], conn, ns(
        company_id="no-such-company", from_date=FROM, to_date=TO))
    assert is_error(r)


# ---------------------------------------------------------------------------
# refunds
# ---------------------------------------------------------------------------

def test_refund_handling(conn, env):
    t1, t2 = _seed_500_03(conn, env["session_id"], env["item_id"])
    rid = seed_return_document(conn, t2)
    _set(conn, "pos_transaction", rid, created_at="2026-03-15 12:00:00")
    r = _measure(conn, env["company_id"])
    assert r["gross_sales"] == "500.03"
    assert r["transaction_count"] == 2
    assert r["refunds"] == "400.02"
    assert r["refund_count"] == 1
    assert r["return_count"] == 1
    assert r["net_sales"] == "100.01"
    assert r["line_count"] == 2
    assert r["units"] == "3.00"
    assert r["first_timestamp"] == "2026-03-10 09:15:00"
    assert r["last_timestamp"] == "2026-03-10 14:40:00"
    days = {row["date"]: row for row in r["by_day"]}
    assert days["2026-03-15"]["refunds"] == "400.02"
    assert days["2026-03-15"]["net_sales"] == "-400.02"


# ---------------------------------------------------------------------------
# deterministic ordering
# ---------------------------------------------------------------------------

def test_stable_ordering(conn, env):
    prof_b = seed_pos_profile(conn, env["company_id"], name="Second Register")
    sess_b = seed_open_session(conn, prof_b, cashier="Second Cashier")
    _sale(conn, sess_b, env["item_id"], "1", "30.00",
          "2026-03-20 10:00:00")
    _sale(conn, env["session_id"], env["item_id"], "1", "10.00",
          "2026-03-05 10:00:00")
    _sale(conn, sess_b, env["item_id"], "1", "20.00",
          "2026-03-12 10:00:00")
    r = _measure(conn, env["company_id"])
    assert [row["date"] for row in r["by_day"]] == [
        "2026-03-05", "2026-03-12", "2026-03-20"]
    assert [row["date"] for row in r["days"]] == [
        "2026-03-05", "2026-03-12", "2026-03-20"]
    loc_ids = [row["location_id"] for row in r["by_location"]]
    assert loc_ids == sorted(loc_ids)
    assert r["gross_sales"] == "60.00"


# ---------------------------------------------------------------------------
# missing tables, repeatability, read-only
# ---------------------------------------------------------------------------

def test_missing_table_is_unavailable(conn, env):
    _seed_500_03(conn, env["session_id"], env["item_id"])
    conn.execute("DROP TABLE pos_payment")
    conn.commit()
    r = call_action(A[ACTION], conn, ns(
        company_id=env["company_id"], from_date=FROM, to_date=TO))
    assert is_ok(r), r
    assert r["available"] is False
    assert r["measurement_scope"] == "recorded_local_pos_rows"
    assert "unavailable" in r["reason"].lower()


def test_two_identical_calls_and_no_writes(conn, env):
    _seed_500_03(conn, env["session_id"], env["item_id"])
    before = _dump(conn)
    first = _measure(conn, env["company_id"])
    second = _measure(conn, env["company_id"])
    assert first == second
    assert _dump(conn) == before
    filtered = _measure(conn, env["company_id"],
                        location_id=env["profile_id"])
    assert _dump(conn) == before
    assert filtered["transaction_count"] == 2
