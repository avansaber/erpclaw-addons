"""m853: Stripe fee report keeps unitemized fees alongside itemized lines."""
import os
import sys
import uuid

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_test_helpers import (
    build_stripe_env, seed_stripe_account, seed_balance_transaction,
    call_action, ns, is_ok,
)
from reports import ACTIONS as REPORTS_ACTIONS

from erpclaw_lib.query import insert_row, P


def _seed_fee_detail(conn, balance_transaction_id, fee_type, amount):
    sql, _ = insert_row("stripe_fee_detail", {
        "id": P(), "balance_transaction_id": P(),
        "fee_type": P(), "amount": P(),
    })
    conn.execute(sql, (str(uuid.uuid4()), balance_transaction_id, fee_type, amount))
    conn.commit()


def _snapshot_balance_transactions(conn):
    return [dict(r) for r in conn.execute(
        'SELECT * FROM stripe_balance_transaction ORDER BY stripe_id').fetchall()]


def _snapshot_fee_details(conn):
    return [dict(r) for r in conn.execute(
        'SELECT * FROM stripe_fee_detail ORDER BY fee_type, amount').fetchall()]


def test_mixed_itemized_and_unitemized(conn, db_path):
    env = build_stripe_env(conn)
    acct = env["stripe_account_id"]
    company = env["company_id"]
    other = seed_stripe_account(conn, company)

    bt1 = seed_balance_transaction(conn, acct, company,
                                   stripe_id="txn_m853_bt1", source_id="ch_m853_bt1",
                                   amount="100.00", fee="13.00", net="87.00",
                                   bt_type="charge")
    _seed_fee_detail(conn, bt1, "stripe_fee", "9.00")
    _seed_fee_detail(conn, bt1, "application_fee", "4.00")
    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m853_bt2", source_id="ch_m853_bt2",
                             amount="100.00", fee="3.20", net="96.80",
                             bt_type="charge")
    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m853_bt3", source_id="re_m853_bt3",
                             amount="-20.00", fee="0.50", net="-20.50",
                             bt_type="refund")
    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m853_bt4", source_id="po_m853_bt4",
                             amount="-100.00", fee="0.00", net="-100.00",
                             bt_type="payout")
    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m853_bt5", source_id="aj_m853_bt5",
                             amount="10.00", fee="1.00", net="9.00",
                             bt_type="application_fee")

    other_bt = seed_balance_transaction(conn, other, company,
                                        stripe_id="txn_m853_x1", source_id="ch_m853_x1",
                                        amount="500.00", fee="70.00", net="430.00",
                                        bt_type="charge")
    _seed_fee_detail(conn, other_bt, "stripe_fee", "70.00")

    before_bt = _snapshot_balance_transactions(conn)
    before_fd = _snapshot_fee_details(conn)
    result = call_action(REPORTS_ACTIONS["stripe-fee-report"], conn, ns(
        stripe_account_id=acct,
    ))
    assert is_ok(result)
    assert result["report"] == "fees"
    assert result["fee_types"] == [
        {"fee_type": "stripe_fee", "source": "fee_detail", "count": 1, "total": "9.00"},
        {"fee_type": "application_fee", "source": "fee_detail", "count": 1, "total": "4.00"},
        {"fee_type": "charge", "source": "balance_transaction", "count": 1, "total": "3.20"},
        {"fee_type": "application_fee", "source": "balance_transaction", "count": 1, "total": "1.00"},
        {"fee_type": "refund", "source": "balance_transaction", "count": 1, "total": "0.50"},
    ]
    assert result["grand_total"] == "17.70"
    assert _snapshot_balance_transactions(conn) == before_bt
    assert _snapshot_fee_details(conn) == before_fd


def test_detail_only_small_case(conn, db_path):
    env = build_stripe_env(conn)
    acct = env["stripe_account_id"]
    company = env["company_id"]

    bt = seed_balance_transaction(conn, acct, company,
                                  stripe_id="txn_m853_d1", source_id="ch_m853_d1",
                                  amount="100.00", fee="3.00", net="97.00",
                                  bt_type="charge")
    _seed_fee_detail(conn, bt, "stripe_fee", "3.00")

    result = call_action(REPORTS_ACTIONS["stripe-fee-report"], conn, ns(
        stripe_account_id=acct,
    ))
    assert is_ok(result)
    assert result["report"] == "fees"
    assert result["fee_types"] == [
        {"fee_type": "stripe_fee", "source": "fee_detail", "count": 1, "total": "3.00"},
    ]
    assert result["grand_total"] == "3.00"


def test_fallback_only_small_case(conn, db_path):
    env = build_stripe_env(conn)
    acct = env["stripe_account_id"]
    company = env["company_id"]

    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m853_f1", source_id="ch_m853_f1",
                             amount="100.00", fee="2.00", net="98.00",
                             bt_type="charge")
    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m853_f2", source_id="re_m853_f2",
                             amount="-20.00", fee="1.00", net="-21.00",
                             bt_type="refund")

    result = call_action(REPORTS_ACTIONS["stripe-fee-report"], conn, ns(
        stripe_account_id=acct,
    ))
    assert is_ok(result)
    assert result["report"] == "fees"
    assert result["fee_types"] == [
        {"fee_type": "charge", "source": "balance_transaction", "count": 1, "total": "2.00"},
        {"fee_type": "refund", "source": "balance_transaction", "count": 1, "total": "1.00"},
    ]
    assert result["grand_total"] == "3.00"


def test_same_fee_type_across_itemized_transactions_merges(conn, db_path):
    env = build_stripe_env(conn)
    acct = env["stripe_account_id"]
    company = env["company_id"]

    bt1 = seed_balance_transaction(conn, acct, company,
                                   stripe_id="txn_m853_s1", source_id="ch_m853_s1",
                                   amount="100.00", fee="2.00", net="98.00",
                                   bt_type="charge")
    bt2 = seed_balance_transaction(conn, acct, company,
                                   stripe_id="txn_m853_s2", source_id="ch_m853_s2",
                                   amount="100.00", fee="3.00", net="97.00",
                                   bt_type="charge")
    _seed_fee_detail(conn, bt1, "stripe_fee", "2.00")
    _seed_fee_detail(conn, bt2, "stripe_fee", "3.00")

    result = call_action(REPORTS_ACTIONS["stripe-fee-report"], conn, ns(
        stripe_account_id=acct,
    ))
    assert is_ok(result)
    assert result["fee_types"] == [
        {"fee_type": "stripe_fee", "source": "fee_detail", "count": 2, "total": "5.00"},
    ]
    assert result["grand_total"] == "5.00"
