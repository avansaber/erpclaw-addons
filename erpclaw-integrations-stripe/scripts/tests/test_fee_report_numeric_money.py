"""m667: Stripe fee report filters and orders fees by numeric value, not text."""
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


def test_fallback_excludes_zero_fees_and_orders_numerically(conn, db_path):
    env = build_stripe_env(conn)
    acct = env["stripe_account_id"]
    company = env["company_id"]
    other = seed_stripe_account(conn, company)

    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m667_c1", source_id="ch_m667_c1",
                             amount="100.00", fee="9.00", net="91.00")
    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m667_r1", source_id="re_m667_r1",
                             amount="-50.00", fee="4.00", net="-54.00",
                             bt_type="refund")
    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m667_r2", source_id="re_m667_r2",
                             amount="-60.00", fee="6.00", net="-66.00",
                             bt_type="refund")
    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m667_p1", source_id="po_m667_p1",
                             amount="-100.00", fee="0.00", net="-100.00",
                             bt_type="payout")
    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m667_a0", source_id="aj_m667_a0",
                             amount="0.00", fee="0", net="0.00",
                             bt_type="adjustment")
    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m667_a1", source_id="aj_m667_a1",
                             amount="-10.00", fee="-1.50", net="-8.50",
                             bt_type="adjustment")
    seed_balance_transaction(conn, other, company,
                             stripe_id="txn_m667_x1", source_id="ch_m667_x1",
                             amount="500.00", fee="50.00", net="450.00")

    before = _snapshot_balance_transactions(conn)
    result = call_action(REPORTS_ACTIONS["stripe-fee-report"], conn, ns(
        stripe_account_id=acct,
    ))
    assert is_ok(result)
    assert result["fee_types"] == [
        {"fee_type": "refund", "source": "balance_transaction", "count": 2, "total": "10.00"},
        {"fee_type": "charge", "source": "balance_transaction", "count": 1, "total": "9.00"},
        {"fee_type": "adjustment", "source": "balance_transaction", "count": 1, "total": "-1.50"},
    ]
    assert result["grand_total"] == "17.50"
    assert _snapshot_balance_transactions(conn) == before


def test_fallback_ties_order_by_type_name(conn, db_path):
    env = build_stripe_env(conn)
    acct = env["stripe_account_id"]
    company = env["company_id"]

    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m667_t1", source_id="ch_m667_t1",
                             amount="100.00", fee="2.00", net="98.00")
    seed_balance_transaction(conn, acct, company,
                             stripe_id="txn_m667_t2", source_id="re_m667_t2",
                             amount="-20.00", fee="2.00", net="-22.00",
                             bt_type="refund")

    result = call_action(REPORTS_ACTIONS["stripe-fee-report"], conn, ns(
        stripe_account_id=acct,
    ))
    assert is_ok(result)
    assert result["fee_types"] == [
        {"fee_type": "charge", "source": "balance_transaction", "count": 1, "total": "2.00"},
        {"fee_type": "refund", "source": "balance_transaction", "count": 1, "total": "2.00"},
    ]
    assert result["grand_total"] == "4.00"


def test_fee_detail_branch_orders_numerically(conn, db_path):
    env = build_stripe_env(conn)
    acct = env["stripe_account_id"]
    company = env["company_id"]
    other = seed_stripe_account(conn, company)

    bt1 = seed_balance_transaction(conn, acct, company,
                                   stripe_id="txn_m667_d1", source_id="ch_m667_d1",
                                   amount="100.00", fee="13.00", net="87.00")
    bt2 = seed_balance_transaction(conn, acct, company,
                                   stripe_id="txn_m667_d2", source_id="ch_m667_d2",
                                   amount="200.00", fee="6.00", net="194.00")
    _seed_fee_detail(conn, bt1, "stripe_fee", "9.00")
    _seed_fee_detail(conn, bt1, "application_fee", "4.00")
    _seed_fee_detail(conn, bt2, "application_fee", "6.00")
    _seed_fee_detail(conn, bt2, "tax", "0.00")

    other_bt = seed_balance_transaction(conn, other, company,
                                        stripe_id="txn_m667_d9", source_id="ch_m667_d9",
                                        amount="700.00", fee="70.00", net="630.00")
    _seed_fee_detail(conn, other_bt, "stripe_fee", "70.00")

    result = call_action(REPORTS_ACTIONS["stripe-fee-report"], conn, ns(
        stripe_account_id=acct,
    ))
    assert is_ok(result)
    assert result["fee_types"] == [
        {"fee_type": "application_fee", "source": "fee_detail", "count": 2, "total": "10.00"},
        {"fee_type": "stripe_fee", "source": "fee_detail", "count": 1, "total": "9.00"},
        {"fee_type": "tax", "source": "fee_detail", "count": 1, "total": "0.00"},
    ]
    assert result["grand_total"] == "19.00"
