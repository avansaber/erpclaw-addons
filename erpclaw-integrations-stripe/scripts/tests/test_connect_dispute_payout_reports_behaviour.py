"""Connect, dispute, payout-detail and reconciliation reports, pinned by value.

Every report here reads rows of one Stripe account and computes money totals
from them. The tests seed exact amounts on a Stripe account, plus rows that
must stay out of its figures: another Stripe account of the same company, and
a Stripe account of another company. The monthly Connect reports bucket by the
first seven characters of created_stripe, so rows are seeded in several months
with fixed dates. None of these handlers accepts a date window or a company
filter; the Stripe account is the only scope, and that is what is pinned.

Every figure is an exact decimal string. Each report is read-only: the stored
rows it reports are read back from the database before and after, unchanged.

stripe-payout-detail-report lists the balance transactions of one payout. A
balance transaction on another Stripe account that carries the same payout id
is not part of that payout, as stripe-reconcile-payout already treats it.
"""
import os
import sys
from decimal import Decimal

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_test_helpers import (  # noqa: E402
    build_stripe_env, call_action, is_error, is_ok, ns,
    seed_application_fee, seed_balance_transaction, seed_dispute, seed_payout,
    seed_stripe_account, seed_transfer,
)
from erpclaw_lib.query import P, Q, Table, update_row  # noqa: E402
from connect import ACTIONS as CONNECT_ACTIONS  # noqa: E402
from reconciliation import ACTIONS as RECON_ACTIONS  # noqa: E402
from reports import ACTIONS as REPORT_ACTIONS  # noqa: E402


def _set(conn, table, stripe_id, **values):
    conn.execute(
        update_row(table, {col: P() for col in values}, {"stripe_id": P()}),
        tuple(values.values()) + (stripe_id,))
    conn.commit()


def _stored(conn, table, columns):
    t = Table(table)
    rows = conn.execute(
        Q.from_(t).select("stripe_id", *columns).orderby(t.stripe_id).get_sql()
    ).fetchall()
    return [tuple(r) for r in rows]


def _sum(conn, table, column, stripe_account_id):
    t = Table(table)
    rows = conn.execute(
        Q.from_(t).select(column).where(t.stripe_account_id == P()).get_sql(),
        (stripe_account_id,)).fetchall()
    return str(sum((Decimal(r[0]) for r in rows), Decimal("0")))


def _accounts(conn):
    """Account A (under test), account A2 in the same company, account B in
    another company, and an account with no rows at all."""
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    same_co_acct = seed_stripe_account(conn, co, name="Other Stripe")
    other = build_stripe_env(conn)
    empty_acct = seed_stripe_account(conn, co, name="Empty Stripe")
    return acct, co, same_co_acct, other["stripe_account_id"], other["company_id"], empty_acct


# ── Connect: application fees and transfers ────────────────────────────────

def _seed_fees(conn):
    acct, co, a2, b, bco, empty = _accounts(conn)
    for sid, account, company, amount, created, refunded, created_at in [
        ("fee_m351_1", acct, co, "12.35", "2026-01-05T10:00:00Z", "0", "2026-01-05 10:00:01"),
        ("fee_m351_2", acct, co, "7.80", "2026-01-31T23:59:59Z", "2.15", "2026-01-31 23:59:59"),
        ("fee_m351_3", acct, co, "105.05", "2026-02-01T00:00:00Z", "0.00", "2026-02-01 00:00:05"),
        ("fee_m351_4", acct, co, "0.65", "2026-03-15T12:00:00Z", "0.65", "2026-03-15 12:00:00"),
        ("fee_m351_a2", a2, co, "500.00", "2026-01-10T00:00:00Z", "1.00", "2026-03-20 00:00:00"),
        ("fee_m351_b", b, bco, "250.00", "2026-02-10T00:00:00Z", "0", "2026-03-21 00:00:00"),
    ]:
        seed_application_fee(conn, account, company, stripe_id=sid, amount=amount)
        _set(conn, "stripe_application_fee", sid, created_stripe=created,
             refunded_amount=refunded, created_at=created_at)
    return acct, a2, b, empty


FEE_COLUMNS = ("stripe_account_id", "amount", "refunded_amount", "created_stripe")


def test_connect_revenue_report_sums_fees_by_month_for_one_account(conn):
    acct, a2, b, empty = _seed_fees(conn)
    before = _stored(conn, "stripe_application_fee", FEE_COLUMNS)

    r = call_action(CONNECT_ACTIONS["stripe-connect-revenue-report"], conn,
                    ns(stripe_account_id=acct))

    assert is_ok(r), r
    assert r["report"] == "connect_revenue"
    assert r["months"] == [
        {"month": "2026-03", "fee_count": 1, "total_amount": "0.65"},
        {"month": "2026-02", "fee_count": 1, "total_amount": "105.05"},
        {"month": "2026-01", "fee_count": 2, "total_amount": "20.15"},
    ]
    assert r["grand_total"] == "125.85"
    assert r["month_count"] == 3
    assert r["grand_total"] == _sum(conn, "stripe_application_fee", "amount", acct)

    other = call_action(CONNECT_ACTIONS["stripe-connect-revenue-report"], conn,
                        ns(stripe_account_id=a2))
    assert (other["months"], other["grand_total"]) == (
        [{"month": "2026-01", "fee_count": 1, "total_amount": "500.00"}], "500.00")

    none = call_action(CONNECT_ACTIONS["stripe-connect-revenue-report"], conn,
                       ns(stripe_account_id=empty))
    assert (none["months"], none["grand_total"], none["month_count"]) == ([], "0.00", 0)

    assert _stored(conn, "stripe_application_fee", FEE_COLUMNS) == before


def test_connect_fee_summary_nets_refunded_fees_for_one_account(conn):
    acct, a2, b, empty = _seed_fees(conn)
    before = _stored(conn, "stripe_application_fee", FEE_COLUMNS)

    r = call_action(CONNECT_ACTIONS["stripe-connect-fee-summary"], conn,
                    ns(stripe_account_id=acct))

    assert is_ok(r), r
    assert r["report"] == "connect_fee_summary"
    assert r["fee_count"] == 4
    assert r["total_earned"] == "125.85"
    assert r["total_refunded"] == "2.80"
    assert r["net_earned"] == "123.05"
    assert r["total_refunded"] == _sum(conn, "stripe_application_fee",
                                       "refunded_amount", acct)

    b_r = call_action(CONNECT_ACTIONS["stripe-connect-fee-summary"], conn,
                      ns(stripe_account_id=b))
    assert (b_r["fee_count"], b_r["total_earned"], b_r["total_refunded"],
            b_r["net_earned"]) == (1, "250.00", "0.00", "250.00")

    none = call_action(CONNECT_ACTIONS["stripe-connect-fee-summary"], conn,
                       ns(stripe_account_id=empty))
    assert (none["fee_count"], none["total_earned"], none["total_refunded"],
            none["net_earned"]) == (0, "0.00", "0.00", "0.00")

    assert _stored(conn, "stripe_application_fee", FEE_COLUMNS) == before


def test_list_application_fees_returns_this_accounts_fees_newest_first(conn):
    acct, a2, b, empty = _seed_fees(conn)

    r = call_action(CONNECT_ACTIONS["stripe-list-application-fees"], conn,
                    ns(stripe_account_id=acct, limit=50))

    assert is_ok(r), r
    assert r["count"] == 4
    assert [(f["stripe_id"], f["amount"], f["refunded_amount"], f["created_stripe"],
             f["stripe_account_id"]) for f in r["application_fees"]] == [
        ("fee_m351_4", "0.65", "0.65", "2026-03-15T12:00:00Z", acct),
        ("fee_m351_3", "105.05", "0.00", "2026-02-01T00:00:00Z", acct),
        ("fee_m351_2", "7.80", "2.15", "2026-01-31T23:59:59Z", acct),
        ("fee_m351_1", "12.35", "0", "2026-01-05T10:00:00Z", acct),
    ]

    r = call_action(CONNECT_ACTIONS["stripe-list-application-fees"], conn,
                    ns(stripe_account_id=acct, limit=2))
    assert is_ok(r), r
    assert [f["stripe_id"] for f in r["application_fees"]] == ["fee_m351_4", "fee_m351_3"]

    r = call_action(CONNECT_ACTIONS["stripe-list-application-fees"], conn,
                    ns(stripe_account_id=b, limit=50))
    assert [(f["stripe_id"], f["amount"]) for f in r["application_fees"]] == [
        ("fee_m351_b", "250.00")]


def test_connect_payout_report_sums_transfers_by_month_for_one_account(conn):
    acct, co, a2, b, bco, empty = _accounts(conn)
    for sid, account, company, amount, created in [
        ("tr_m351_1", acct, co, "200.00", "2026-01-15T08:00:00Z"),
        ("tr_m351_2", acct, co, "49.99", "2026-01-31T23:59:59Z"),
        ("tr_m351_3", acct, co, "1000.01", "2026-02-02T00:00:00Z"),
        ("tr_m351_a2", a2, co, "300.00", "2026-01-20T00:00:00Z"),
        ("tr_m351_b", b, bco, "75.00", "2026-02-20T00:00:00Z"),
    ]:
        seed_transfer(conn, account, company, stripe_id=sid, amount=amount)
        _set(conn, "stripe_transfer", sid, created_stripe=created)
    columns = ("stripe_account_id", "amount", "reversed", "created_stripe")
    before = _stored(conn, "stripe_transfer", columns)

    r = call_action(CONNECT_ACTIONS["stripe-connect-payout-report"], conn,
                    ns(stripe_account_id=acct))

    assert is_ok(r), r
    assert r["report"] == "connect_payouts"
    assert r["months"] == [
        {"month": "2026-02", "transfer_count": 1, "total_amount": "1000.01",
         "reversed_count": 0, "reversed_amount": "0.00"},
        {"month": "2026-01", "transfer_count": 2, "total_amount": "249.99",
         "reversed_count": 0, "reversed_amount": "0.00"},
    ]
    assert r["grand_total"] == "1250.00"
    assert r["reversed_count"] == 0
    assert r["reversed_total"] == "0.00"
    assert r["month_count"] == 2
    assert r["grand_total"] == _sum(conn, "stripe_transfer", "amount", acct)

    other = call_action(CONNECT_ACTIONS["stripe-connect-payout-report"], conn,
                        ns(stripe_account_id=b))
    assert (other["months"], other["grand_total"]) == (
        [{"month": "2026-02", "transfer_count": 1, "total_amount": "75.00",
          "reversed_count": 0, "reversed_amount": "0.00"}], "75.00")

    none = call_action(CONNECT_ACTIONS["stripe-connect-payout-report"], conn,
                       ns(stripe_account_id=empty))
    assert (none["months"], none["grand_total"], none["month_count"]) == ([], "0.00", 0)

    assert _stored(conn, "stripe_transfer", columns) == before


def test_connect_payout_report_reversed_only_month_and_decoys(conn):
    acct, co, a2, b, bco, empty = _accounts(conn)
    for sid, account, company, amount, created in [
        ("tr_m663_live", acct, co, "200.00", "2026-01-15T08:00:00Z"),
        ("tr_m663_rev", acct, co, "75.00", "2026-01-20T08:00:00Z"),
        ("tr_m663_febrev", acct, co, "40.00", "2026-02-03T08:00:00Z"),
        ("tr_m663_a2rev", a2, co, "300.00", "2026-01-22T08:00:00Z"),
        ("tr_m663_b_live", b, bco, "50.00", "2026-01-10T08:00:00Z"),
        ("tr_m663_b_rev", b, bco, "25.00", "2026-01-25T08:00:00Z"),
    ]:
        seed_transfer(conn, account, company, stripe_id=sid, amount=amount)
        _set(conn, "stripe_transfer", sid, created_stripe=created)
    for sid in ("tr_m663_rev", "tr_m663_febrev", "tr_m663_a2rev",
                "tr_m663_b_rev"):
        _set(conn, "stripe_transfer", sid, reversed=1)
    columns = ("stripe_account_id", "amount", "reversed", "created_stripe")
    before = _stored(conn, "stripe_transfer", columns)

    r = call_action(CONNECT_ACTIONS["stripe-connect-payout-report"], conn,
                    ns(stripe_account_id=acct))

    assert is_ok(r), r
    assert r["report"] == "connect_payouts"
    assert r["months"] == [
        {"month": "2026-02", "transfer_count": 0, "total_amount": "0.00",
         "reversed_count": 1, "reversed_amount": "40.00"},
        {"month": "2026-01", "transfer_count": 1, "total_amount": "200.00",
         "reversed_count": 1, "reversed_amount": "75.00"},
    ]
    assert r["grand_total"] == "200.00"
    assert r["reversed_count"] == 2
    assert r["reversed_total"] == "115.00"
    assert r["month_count"] == 2

    b_r = call_action(CONNECT_ACTIONS["stripe-connect-payout-report"], conn,
                      ns(stripe_account_id=b))
    assert is_ok(b_r), b_r
    assert b_r["grand_total"] == "50.00"
    assert b_r["reversed_total"] == "25.00"

    assert _stored(conn, "stripe_transfer", columns) == before


# ── Reports: disputes and reconciliation ───────────────────────────────────

def test_dispute_report_totals_by_status_for_one_account(conn):
    acct, co, a2, b, bco, empty = _accounts(conn)
    for sid, account, company, amount, status in [
        ("dp_m351_1", acct, co, "150.00", "needs_response"),
        ("dp_m351_2", acct, co, "75.50", "needs_response"),
        ("dp_m351_3", acct, co, "0.99", "needs_response"),
        ("dp_m351_4", acct, co, "1200.00", "won"),
        ("dp_m351_5", acct, co, "34.01", "won"),
        ("dp_m351_6", acct, co, "89.90", "lost"),
        ("dp_m351_a2", a2, co, "999.00", "lost"),
        ("dp_m351_b", b, bco, "500.00", "needs_response"),
    ]:
        seed_dispute(conn, account, company, stripe_id=sid,
                     charge_stripe_id=sid.replace("dp_", "ch_"),
                     amount=amount, status=status)
    columns = ("stripe_account_id", "amount", "status")
    before = _stored(conn, "stripe_dispute", columns)

    r = call_action(REPORT_ACTIONS["stripe-dispute-report"], conn,
                    ns(stripe_account_id=acct))

    assert is_ok(r), r
    assert r["report"] == "disputes"
    assert r["statuses"] == [
        {"status": "needs_response", "count": 3, "total_amount": "226.49"},
        {"status": "won", "count": 2, "total_amount": "1234.01"},
        {"status": "lost", "count": 1, "total_amount": "89.90"},
    ]
    assert r["total_disputes"] == 6
    assert r["total_amount"] == "1550.40"
    assert r["total_amount"] == _sum(conn, "stripe_dispute", "amount", acct)

    other = call_action(REPORT_ACTIONS["stripe-dispute-report"], conn,
                        ns(stripe_account_id=a2))
    assert (other["statuses"], other["total_disputes"], other["total_amount"]) == (
        [{"status": "lost", "count": 1, "total_amount": "999.00"}], 1, "999.00")

    none = call_action(REPORT_ACTIONS["stripe-dispute-report"], conn,
                       ns(stripe_account_id=empty))
    assert (none["statuses"], none["total_disputes"], none["total_amount"]) == (
        [], 0, "0.00")

    assert _stored(conn, "stripe_dispute", columns) == before


BT_COLUMNS = ("stripe_account_id", "type", "amount", "fee", "net", "payout_id",
              "reconciled")


def test_reconciliation_report_matched_and_unmatched_for_one_account(conn):
    acct, co, a2, b, bco, empty = _accounts(conn)
    for sid, account, company, bt_type, amount, fee, net, reconciled in [
        ("txn_m351_1", acct, co, "charge", "200.00", "6.10", "193.90", 1),
        ("txn_m351_2", acct, co, "charge", "120.00", "3.78", "116.22", 1),
        ("txn_m351_3", acct, co, "refund", "-15.00", "0.00", "-15.00", 1),
        ("txn_m351_4", acct, co, "charge", "80.00", "2.62", "77.38", 0),
        ("txn_m351_5", acct, co, "refund", "-25.50", "0.00", "-25.50", 0),
        ("txn_m351_6", acct, co, "charge", "9.99", "0.59", "9.40", 0),
        ("txn_m351_7", acct, co, "dispute", "-40.00", "15.00", "-55.00", 0),
        ("txn_m351_a2", a2, co, "charge", "999.00", "0.00", "999.00", 1),
        ("txn_m351_b", b, bco, "charge", "500.00", "0.00", "500.00", 0),
    ]:
        seed_balance_transaction(conn, account, company, stripe_id=sid,
                                 source_id=sid.replace("txn_", "src_"),
                                 amount=amount, fee=fee, net=net, bt_type=bt_type,
                                 reconciled=reconciled)
    before = _stored(conn, "stripe_balance_transaction", BT_COLUMNS)

    r = call_action(REPORT_ACTIONS["stripe-reconciliation-report"], conn,
                    ns(stripe_account_id=acct))

    assert is_ok(r), r
    assert r["report"] == "reconciliation"
    assert r["total_transactions"] == 7
    assert r["matched"] == 3
    assert r["unmatched"] == 4
    assert r["matched_amount"] == "305.00"
    assert r["unmatched_amount"] == "24.49"
    assert r["match_rate_pct"] == "42.86"
    stored = conn.execute(
        "SELECT reconciled, amount FROM stripe_balance_transaction "
        "WHERE stripe_account_id = ?", (acct,)).fetchall()
    assert str(sum((Decimal(s["amount"]) for s in stored if s["reconciled"] == 1),
                   Decimal("0"))) == "305.00"

    b_r = call_action(REPORT_ACTIONS["stripe-reconciliation-report"], conn,
                      ns(stripe_account_id=b))
    assert (b_r["total_transactions"], b_r["matched"], b_r["unmatched"],
            b_r["matched_amount"], b_r["unmatched_amount"], b_r["match_rate_pct"]) == (
        1, 0, 1, "0.00", "500.00", "0.00")

    none = call_action(REPORT_ACTIONS["stripe-reconciliation-report"], conn,
                       ns(stripe_account_id=empty))
    assert (none["total_transactions"], none["matched"], none["unmatched"],
            none["matched_amount"], none["unmatched_amount"], none["match_rate_pct"]) == (
        0, 0, 0, "0.00", "0.00", "0.00")

    assert _stored(conn, "stripe_balance_transaction", BT_COLUMNS) == before


# ── Reports: payout detail ─────────────────────────────────────────────────

PAYOUT = "po_m351_a"


def _seed_payout(conn):
    acct, co, a2, b, bco, empty = _accounts(conn)
    seed_payout(conn, acct, co, stripe_id=PAYOUT, amount="295.12")
    seed_payout(conn, acct, co, stripe_id="po_m351_other", amount="48.25")
    seed_payout(conn, acct, co, stripe_id="po_m351_failed", amount="60.00",
                status="failed")
    for sid, account, company, bt_type, amount, fee, net, payout in [
        ("txn_m351_ch1", acct, co, "charge", "200.00", "6.10", "193.90", PAYOUT),
        ("txn_m351_ch2", acct, co, "charge", "120.00", "3.78", "116.22", PAYOUT),
        ("txn_m351_re1", acct, co, "refund", "-15.00", "0.00", "-15.00", PAYOUT),
        ("txn_m351_loose", acct, co, "charge", "80.00", "2.62", "77.38", None),
        ("txn_m351_other", acct, co, "charge", "50.00", "1.75", "48.25", "po_m351_other"),
        ("txn_m351_a2", a2, co, "charge", "999.00", "0.00", "999.00", PAYOUT),
    ]:
        seed_balance_transaction(conn, account, company, stripe_id=sid,
                                 source_id=sid.replace("txn_", "src_"),
                                 amount=amount, fee=fee, net=net, bt_type=bt_type,
                                 payout_id=payout)
    return acct, co


def test_payout_detail_report_lists_the_payouts_own_transactions(conn):
    acct, co = _seed_payout(conn)
    before = (_stored(conn, "stripe_balance_transaction", BT_COLUMNS),
              _stored(conn, "stripe_payout", ("amount", "status", "reconciled",
                                              "transaction_count")))

    r = call_action(REPORT_ACTIONS["stripe-payout-detail-report"], conn,
                    ns(payout_stripe_id=PAYOUT))

    assert is_ok(r), r
    assert (r["stripe_id"], r["stripe_account_id"], r["company_id"], r["amount"],
            r["document_status"], r["reconciled"]) == (PAYOUT, acct, co, "295.12", "paid", 0)
    assert sorted((t["stripe_id"], t["type"], t["amount"], t["fee"], t["net"],
                   t["stripe_account_id"]) for t in r["transactions"]) == [
        ("txn_m351_ch1", "charge", "200.00", "6.10", "193.90", acct),
        ("txn_m351_ch2", "charge", "120.00", "3.78", "116.22", acct),
        ("txn_m351_re1", "refund", "-15.00", "0.00", "-15.00", acct),
    ]
    assert r["transaction_count"] == 3
    assert sorted(r["type_summary"], key=lambda s: s["type"]) == [
        {"type": "charge", "count": 2, "amount": "320.00", "fee": "9.88"},
        {"type": "refund", "count": 1, "amount": "-15.00", "fee": "0.00"},
    ]
    net = sum((Decimal(t["net"]) for t in r["transactions"]), Decimal("0"))
    assert str(net) == r["amount"]

    # The same payout, reconciled: both actions agree on its transactions.
    rec = call_action(RECON_ACTIONS["stripe-reconcile-payout"], conn,
                      ns(stripe_account_id=acct, payout_stripe_id=PAYOUT))
    assert is_ok(rec), rec
    assert rec["transaction_count"] == r["transaction_count"]
    assert rec["constituent_net_total"] == "295.12"

    after_rec = call_action(REPORT_ACTIONS["stripe-payout-detail-report"], conn,
                            ns(payout_stripe_id=PAYOUT))
    assert (after_rec["reconciled"], after_rec["transaction_count"]) == (1, 3)

    before_bt, _ = before
    assert _stored(conn, "stripe_balance_transaction", BT_COLUMNS) == before_bt


def test_payout_detail_report_failed_payout_and_refusals(conn):
    acct, co = _seed_payout(conn)
    before = _stored(conn, "stripe_payout", ("amount", "status", "reconciled",
                                             "transaction_count"))

    r = call_action(REPORT_ACTIONS["stripe-payout-detail-report"], conn,
                    ns(payout_stripe_id="po_m351_failed"))
    assert is_ok(r), r
    assert (r["amount"], r["document_status"], r["transactions"], r["transaction_count"],
            r["type_summary"]) == ("60.00", "failed", [], 0, [])

    r = call_action(REPORT_ACTIONS["stripe-payout-detail-report"], conn,
                    ns(payout_stripe_id="po_m351_missing"))
    assert is_error(r)
    assert r["message"] == "Payout po_m351_missing not found"

    r = call_action(REPORT_ACTIONS["stripe-payout-detail-report"], conn,
                    ns(payout_stripe_id=None))
    assert is_error(r)
    assert r["message"] == "--payout-stripe-id is required"

    assert _stored(conn, "stripe_payout", ("amount", "status", "reconciled",
                                           "transaction_count")) == before


# ── Refusals for every account-scoped report ───────────────────────────────

@pytest.mark.parametrize("registry,action", [
    (CONNECT_ACTIONS, "stripe-connect-fee-summary"),
    (CONNECT_ACTIONS, "stripe-connect-payout-report"),
    (CONNECT_ACTIONS, "stripe-connect-revenue-report"),
    (CONNECT_ACTIONS, "stripe-list-application-fees"),
    (REPORT_ACTIONS, "stripe-dispute-report"),
    (REPORT_ACTIONS, "stripe-reconciliation-report"),
])
def test_account_scoped_reports_refuse_missing_and_unknown_account(conn, registry,
                                                                   action):
    acct, a2, b, empty = _seed_fees(conn)
    before = _stored(conn, "stripe_application_fee", FEE_COLUMNS)

    r = call_action(registry[action], conn, ns(stripe_account_id=None, limit=50))
    assert is_error(r)
    assert r["message"] == "--stripe-account-id is required"

    r = call_action(registry[action], conn,
                    ns(stripe_account_id="acct-m351-missing", limit=50))
    assert is_error(r)
    assert r["message"] == "Stripe account acct-m351-missing not found"

    assert _stored(conn, "stripe_application_fee", FEE_COLUMNS) == before
