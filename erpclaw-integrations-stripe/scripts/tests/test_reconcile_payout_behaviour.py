"""stripe-reconcile-payout and stripe-list-refunds, read back from the database.

A payout is reconciled when the net of the balance transactions carrying its
Stripe id, on the same Stripe account, equals the payout amount. The action
writes the payout's reconciled flag and transaction count and nothing else:
it posts no ledger rows. These tests seed balance transactions with exact
amounts, fees and nets, and pin the totals, the variance, the payout row, the
absence of ledger writes, a second run, and every refusal.
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
    seed_balance_transaction, seed_payout, seed_refund, seed_stripe_account,
)
from erpclaw_lib.query import P, update_row  # noqa: E402
from browse import ACTIONS as BROWSE_ACTIONS  # noqa: E402
from reconciliation import ACTIONS as RECON_ACTIONS  # noqa: E402

PAYOUT = "po_m333_a"


def _reconcile(conn, stripe_account_id, payout_stripe_id):
    return call_action(RECON_ACTIONS["stripe-reconcile-payout"], conn, ns(
        stripe_account_id=stripe_account_id,
        payout_stripe_id=payout_stripe_id,
    ))


def _list_refunds(conn, stripe_account_id, limit=50):
    return call_action(BROWSE_ACTIONS["stripe-list-refunds"], conn, ns(
        stripe_account_id=stripe_account_id,
        limit=limit,
    ))


def _payout_row(conn, payout_stripe_id):
    row = conn.execute(
        "SELECT reconciled, transaction_count, amount, erpclaw_payment_entry_id "
        "FROM stripe_payout WHERE stripe_id = ?",
        (payout_stripe_id,),
    ).fetchone()
    return (row["reconciled"], row["transaction_count"], row["amount"],
            row["erpclaw_payment_entry_id"])


def _ledger_counts(conn):
    gl = conn.execute("SELECT COUNT(*) AS n FROM gl_entry").fetchone()["n"]
    pe = conn.execute("SELECT COUNT(*) AS n FROM payment_entry").fetchone()["n"]
    return gl, pe


def _seed(conn, payout_amount="295.12"):
    """Account A holds the payout under test with two charges and a refund,
    a loose charge in no payout, and a charge in a different payout. Account B,
    in the same company, holds a transaction that carries the same payout id."""
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    other_acct = seed_stripe_account(conn, co, name="Other Stripe")
    seed_payout(conn, acct, co, stripe_id=PAYOUT, amount=payout_amount)
    seed_payout(conn, acct, co, stripe_id="po_m333_other", amount="48.25")
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m333_ch1",
                             source_id="ch_m333_1", amount="200.00", fee="6.10",
                             net="193.90", bt_type="charge", payout_id=PAYOUT)
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m333_ch2",
                             source_id="ch_m333_2", amount="120.00", fee="3.78",
                             net="116.22", bt_type="charge", payout_id=PAYOUT)
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m333_re1",
                             source_id="re_m333_1", amount="-15.00", fee="0.00",
                             net="-15.00", bt_type="refund", payout_id=PAYOUT)
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m333_loose",
                             source_id="ch_m333_3", amount="80.00", fee="2.62",
                             net="77.38", bt_type="charge", payout_id=None)
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m333_other",
                             source_id="ch_m333_4", amount="50.00", fee="1.75",
                             net="48.25", bt_type="charge",
                             payout_id="po_m333_other")
    seed_balance_transaction(conn, other_acct, co, stripe_id="txn_m333_b1",
                             source_id="ch_m333_b1", amount="999.00", fee="0.00",
                             net="999.00", bt_type="charge", payout_id=PAYOUT)
    return acct, other_acct, co


EXPECTED_TRANSACTIONS = [
    ("txn_m333_ch1", "charge", "200.00", "6.10", "193.90", "ch_m333_1"),
    ("txn_m333_ch2", "charge", "120.00", "3.78", "116.22", "ch_m333_2"),
    ("txn_m333_re1", "refund", "-15.00", "0.00", "-15.00", "re_m333_1"),
]


def _transactions(result):
    return sorted((t["stripe_id"], t["type"], t["amount"], t["fee"], t["net"],
                   t["source_id"]) for t in result["transactions"])


def test_balanced_payout_reconciles_with_exact_totals(conn):
    acct, _, _ = _seed(conn)

    r = _reconcile(conn, acct, PAYOUT)

    assert is_ok(r), r
    assert r["payout_stripe_id"] == PAYOUT
    assert r["payout_amount"] == "295.12"
    assert r["constituent_net_total"] == "295.12"
    assert r["difference"] == "0.00"
    assert r["balanced"] is True
    assert r["reconciled"] is True
    assert r["flag_cleared"] is False
    assert r["transaction_count"] == 3
    assert _transactions(r) == EXPECTED_TRANSACTIONS
    gross = sum((Decimal(t["amount"]) for t in r["transactions"]), Decimal("0"))
    fees = sum((Decimal(t["fee"]) for t in r["transactions"]), Decimal("0"))
    refunds = sum((Decimal(t["amount"]) for t in r["transactions"]
                   if t["type"] == "refund"), Decimal("0"))
    assert (str(gross), str(fees), str(refunds)) == ("305.00", "9.88", "-15.00")
    assert str(gross - fees) == r["constituent_net_total"]

    assert _payout_row(conn, PAYOUT) == (1, 3, "295.12", None)
    assert _payout_row(conn, "po_m333_other") == (0, 0, "48.25", None)
    assert _ledger_counts(conn) == (0, 0)


def test_second_run_is_idempotent(conn):
    acct, _, _ = _seed(conn)

    first = _reconcile(conn, acct, PAYOUT)
    second = _reconcile(conn, acct, PAYOUT)

    assert is_ok(first) and is_ok(second), (first, second)
    assert second == first
    assert second["constituent_net_total"] == "295.12"
    assert _payout_row(conn, PAYOUT) == (1, 3, "295.12", None)
    count = conn.execute("SELECT COUNT(*) AS n FROM stripe_payout WHERE stripe_id = ?",
                         (PAYOUT,)).fetchone()["n"]
    assert count == 1
    assert _ledger_counts(conn) == (0, 0)


@pytest.mark.parametrize("payout_amount,difference", [
    ("300.00", "4.88"),
    ("290.00", "-5.12"),
])
def test_net_mismatch_reports_variance_and_leaves_payout_open(conn, payout_amount,
                                                              difference):
    acct, _, _ = _seed(conn, payout_amount=payout_amount)

    r = _reconcile(conn, acct, PAYOUT)

    assert is_ok(r), r
    assert r["payout_amount"] == payout_amount
    assert r["constituent_net_total"] == "295.12"
    assert r["difference"] == difference
    assert r["balanced"] is False
    assert r["reconciled"] is False
    assert r["flag_cleared"] is False
    assert r["transaction_count"] == 3
    assert _transactions(r) == EXPECTED_TRANSACTIONS
    assert _payout_row(conn, PAYOUT) == (0, 0, payout_amount, None)
    assert _ledger_counts(conn) == (0, 0)


def test_refusals_write_nothing(conn):
    acct, other_acct, co = _seed(conn)
    seed_payout(conn, other_acct, co, stripe_id="po_m333_b", amount="10.00")
    seed_balance_transaction(conn, other_acct, co, stripe_id="txn_m333_b2",
                             source_id="ch_m333_b2", amount="10.30", fee="0.30",
                             net="10.00", bt_type="charge", payout_id="po_m333_b")

    r = _reconcile(conn, acct, "po_m333_missing")
    assert is_error(r)
    assert r["message"] == "Payout po_m333_missing not found"

    # A payout that exists, balanced, on another Stripe account is not found here.
    r = _reconcile(conn, acct, "po_m333_b")
    assert is_error(r)
    assert r["message"] == "Payout po_m333_b not found"

    r = _reconcile(conn, acct, None)
    assert is_error(r)
    assert r["message"] == "--payout-stripe-id is required"

    r = _reconcile(conn, "acct-m333-missing", PAYOUT)
    assert is_error(r)
    assert r["message"] == "Stripe account acct-m333-missing not found"

    r = _reconcile(conn, None, PAYOUT)
    assert is_error(r)
    assert r["message"] == "--stripe-account-id is required"

    assert _payout_row(conn, PAYOUT) == (0, 0, "295.12", None)
    assert _payout_row(conn, "po_m333_b") == (0, 0, "10.00", None)
    assert _payout_row(conn, "po_m333_other") == (0, 0, "48.25", None)
    assert _ledger_counts(conn) == (0, 0)


def _set_created_at(conn, refund_stripe_id, created_at):
    conn.execute(update_row("stripe_refund", {"created_at": P()}, {"stripe_id": P()}),
                 (created_at, refund_stripe_id))
    conn.commit()


def test_list_refunds_returns_this_accounts_refunds_newest_first(conn):
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    other_acct = seed_stripe_account(conn, co, name="Other Stripe")
    seed_refund(conn, acct, co, stripe_id="re_m333_old", charge_stripe_id="ch_m333_1",
                amount="15.00", status="succeeded")
    seed_refund(conn, acct, co, stripe_id="re_m333_new", charge_stripe_id="ch_m333_2",
                amount="40.25", status="pending")
    seed_refund(conn, other_acct, co, stripe_id="re_m333_b", charge_stripe_id="ch_m333_b",
                amount="99.99", status="succeeded")
    _set_created_at(conn, "re_m333_old", "2026-03-10 09:00:00")
    _set_created_at(conn, "re_m333_new", "2026-03-12 09:00:00")
    _set_created_at(conn, "re_m333_b", "2026-03-11 09:00:00")

    r = _list_refunds(conn, acct)
    assert is_ok(r), r
    assert r["count"] == 2
    assert [(x["stripe_id"], x["amount"], x["charge_stripe_id"], x["status"],
             x["stripe_account_id"]) for x in r["refunds"]] == [
        ("re_m333_new", "40.25", "ch_m333_2", "pending", acct),
        ("re_m333_old", "15.00", "ch_m333_1", "succeeded", acct),
    ]
    stored = conn.execute(
        "SELECT stripe_id, amount FROM stripe_refund WHERE stripe_account_id = ? "
        "ORDER BY stripe_id", (acct,)).fetchall()
    assert [(s["stripe_id"], s["amount"]) for s in stored] == [
        ("re_m333_new", "40.25"), ("re_m333_old", "15.00")]

    r = _list_refunds(conn, acct, limit=1)
    assert is_ok(r), r
    assert r["count"] == 1
    assert [x["stripe_id"] for x in r["refunds"]] == ["re_m333_new"]

    r = _list_refunds(conn, None)
    assert is_error(r)
    assert r["message"] == "--stripe-account-id is required"

    r = _list_refunds(conn, "acct-m333-missing")
    assert is_error(r)
    assert r["message"] == "Stripe account acct-m333-missing not found"


def test_out_of_balance_rerun_clears_a_reconciled_payout(conn):
    acct, _, co = _seed(conn)

    first = _reconcile(conn, acct, PAYOUT)
    assert is_ok(first), first
    assert first["balanced"] is True
    assert first["flag_cleared"] is False
    assert _payout_row(conn, PAYOUT) == (1, 3, "295.12", None)

    seed_balance_transaction(conn, acct, co, stripe_id="txn_m665_late",
                             source_id="ch_m665_late", amount="5.00", fee="0.00",
                             net="5.00", bt_type="charge", payout_id=PAYOUT)

    second = _reconcile(conn, acct, PAYOUT)
    assert is_ok(second), second
    assert second["balanced"] is False
    assert second["reconciled"] is False
    assert second["flag_cleared"] is True
    assert second["constituent_net_total"] == "300.12"
    assert second["difference"] == "-5.00"
    assert second["transaction_count"] == 4
    assert _payout_row(conn, PAYOUT) == (0, 4, "295.12", None)
    assert _payout_row(conn, "po_m333_other") == (0, 0, "48.25", None)

    summary = call_action(RECON_ACTIONS["stripe-reconciliation-summary"], conn,
                          ns(stripe_account_id=acct))
    assert is_ok(summary), summary
    assert summary["payouts"]["reconciled_count"] == 0
    assert summary["payouts"]["reconciled_amount"] == "0"
    assert summary["payouts"]["unreconciled_count"] == 2
    assert summary["payouts"]["unreconciled_amount"] == "343.37"

    third = _reconcile(conn, acct, PAYOUT)
    assert is_ok(third), third
    assert third["flag_cleared"] is False
    assert _payout_row(conn, PAYOUT) == (0, 4, "295.12", None)
    assert _ledger_counts(conn) == (0, 0)


def test_run_reconciliation_rechecks_and_clears_a_stale_payout(conn):
    acct, _, co = _seed(conn)

    r = _reconcile(conn, acct, PAYOUT)
    assert is_ok(r), r
    assert r["balanced"] is True

    seed_balance_transaction(conn, acct, co, stripe_id="txn_m665_late",
                             source_id="ch_m665_late", amount="5.00", fee="0.00",
                             net="5.00", bt_type="charge", payout_id=PAYOUT)

    run = call_action(RECON_ACTIONS["stripe-run-reconciliation"], conn, ns(
        stripe_account_id=acct,
        date_from=None,
        date_to=None,
    ))
    assert is_ok(run), run
    assert run["layer3_payout_verification"] == {
        "total": 1, "matched": 1, "mismatched": 0,
        "rechecked": 1, "cleared": 1,
    }
    assert _payout_row(conn, PAYOUT) == (0, 4, "295.12", None)
    assert _payout_row(conn, "po_m333_other") == (1, 1, "48.25", None)

    summary = call_action(RECON_ACTIONS["stripe-reconciliation-summary"], conn,
                          ns(stripe_account_id=acct))
    assert is_ok(summary), summary
    assert summary["payouts"]["reconciled_count"] == 1
    assert summary["payouts"]["reconciled_amount"] == "48.25"
    assert summary["payouts"]["unreconciled_count"] == 1
    assert summary["payouts"]["unreconciled_amount"] == "295.12"

    rerun = call_action(RECON_ACTIONS["stripe-run-reconciliation"], conn, ns(
        stripe_account_id=acct,
        date_from=None,
        date_to=None,
    ))
    assert is_ok(rerun), rerun
    assert rerun["layer3_payout_verification"] == {
        "total": 1, "matched": 0, "mismatched": 1,
        "rechecked": 1, "cleared": 0,
    }
    assert _payout_row(conn, PAYOUT) == (0, 4, "295.12", None)
    assert _payout_row(conn, "po_m333_other") == (1, 1, "48.25", None)
    assert _ledger_counts(conn) == (0, 0)
