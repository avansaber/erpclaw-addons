"""A re-sync refreshes what Stripe owns and keeps what ERPClaw recorded.

Every synced Stripe table carries columns that ERPClaw writes after the sync:
the payment or journal entry a posting created, the reconciliation markers,
the linked ERPClaw documents, a manual customer mapping, and the time the row
was first recorded. Re-syncing the same Stripe object must leave all of them
alone. When the sync replaced the whole row, an already-posted charge lost its
payment entry and could be posted to the ledger a second time.
"""
from unittest.mock import patch

import pytest

from stripe_test_helpers import (
    build_gl_ready_env, build_stripe_env, call_action, is_error, is_ok, ns,
    seed_balance_transaction, seed_charge, seed_erpclaw_customer,
)
from test_sync import (
    MOCK_BALANCE_TXNS, MOCK_CHARGES, MOCK_CREDIT_NOTES, MOCK_CUSTOMERS,
    MOCK_DISPUTES, MOCK_INVOICES, MOCK_PAYOUTS, MOCK_REFUNDS,
    MOCK_SUBSCRIPTIONS, MOCK_TRANSFERS, _build_mock_stripe,
)
from erpclaw_lib.query import P, Q, Table, Field, update_row
from gl_posting import ACTIONS as GL_ACTIONS
from sync import ACTIONS as SYNC_ACTIONS

FIRST_SEEN = "2020-01-01 00:00:00"

# object type, mock list keyword, mock objects, table, local columns to plant
CASES = [
    ("balance_transaction", "balance_txns", MOCK_BALANCE_TXNS, "stripe_balance_transaction",
     {"reconciled": 1, "reconciled_at": "2026-03-20T00:00:00Z",
      "gl_voucher_id": "pe-local", "gl_voucher_type": "payment_entry"}),
    ("charge", "charges", MOCK_CHARGES, "stripe_charge",
     {"erpclaw_customer_id": "cust-local", "erpclaw_invoice_id": "inv-local",
      "erpclaw_payment_entry_id": "pe-local"}),
    ("refund", "refunds", MOCK_REFUNDS, "stripe_refund",
     {"erpclaw_credit_note_id": "cn-local", "erpclaw_payment_entry_id": "pe-local"}),
    ("dispute", "disputes", MOCK_DISPUTES, "stripe_dispute",
     {"erpclaw_journal_entry_id": "je-local", "resolution_amount": "12.50"}),
    ("payout", "payouts", MOCK_PAYOUTS, "stripe_payout",
     {"reconciled": 1, "transaction_count": 7, "erpclaw_payment_entry_id": "pe-local"}),
    ("invoice", "invoices", MOCK_INVOICES, "stripe_invoice",
     {"erpclaw_invoice_id": "inv-local"}),
    ("subscription", "subscriptions", MOCK_SUBSCRIPTIONS, "stripe_subscription",
     {"erpclaw_revenue_contract_id": "rc-local"}),
    ("transfer", "transfers", MOCK_TRANSFERS, "stripe_transfer",
     {"erpclaw_journal_entry_id": "je-local"}),
    ("credit_note", "credit_notes", MOCK_CREDIT_NOTES, "stripe_credit_note",
     {"erpclaw_credit_note_id": "cn-local"}),
]


def _sync(conn, env, object_type, **lists):
    with patch.dict("sys.modules", {"stripe": _build_mock_stripe(**lists)}):
        result = call_action(SYNC_ACTIONS["stripe-start-sync"], conn, ns(
            stripe_account_id=env["stripe_account_id"],
            object_type=object_type,
        ))
    assert is_ok(result), result
    return result


def _row(conn, table, where):
    t = Table(table)
    query = Q.from_(t).select(t.star)
    for column in where:
        query = query.where(Field(column) == P())
    return conn.execute(query.get_sql(), tuple(where.values())).fetchone()


def _plant(conn, table, values, where):
    conn.execute(update_row(table, {column: P() for column in values},
                            {column: P() for column in where}),
                 tuple(values.values()) + tuple(where.values()))
    conn.commit()


@pytest.mark.parametrize("object_type,list_name,objects,table,local",
                         CASES, ids=[case[0] for case in CASES])
def test_resync_keeps_local_columns(conn, object_type, list_name, objects, table, local):
    env = build_stripe_env(conn)
    _sync(conn, env, object_type, **{list_name: objects})
    where = {"stripe_id": objects[0]["id"]}
    row_id = _row(conn, table, where)["id"]
    _plant(conn, table, dict(local, created_at=FIRST_SEEN), where)

    _sync(conn, env, object_type, **{list_name: objects})

    row = _row(conn, table, where)
    assert row["id"] == row_id
    assert row["created_at"] == FIRST_SEEN
    for column, value in local.items():
        assert row[column] == value, column


def test_resync_keeps_a_manual_customer_mapping(conn):
    env = build_stripe_env(conn)
    _sync(conn, env, "customer", customers=MOCK_CUSTOMERS)
    where = {"stripe_account_id": env["stripe_account_id"],
             "stripe_customer_id": MOCK_CUSTOMERS[0]["id"]}
    customer_id = seed_erpclaw_customer(conn, env["company_id"], name="Mapped By Hand Ltd")
    _plant(conn, "stripe_customer_map",
           {"erpclaw_customer_id": customer_id, "match_method": "manual",
            "match_confidence": "1.0"}, where)

    _sync(conn, env, "customer", customers=MOCK_CUSTOMERS)

    row = _row(conn, "stripe_customer_map", where)
    assert row["erpclaw_customer_id"] == customer_id
    assert row["match_method"] == "manual"
    assert row["match_confidence"] == "1.0"


def test_resync_still_refreshes_what_stripe_owns(conn):
    env = build_stripe_env(conn)
    charge = dict(MOCK_CHARGES[0])
    _sync(conn, env, "charge", charges=[charge])
    refunded = dict(charge, status="refunded", amount_refunded=charge["amount"])

    _sync(conn, env, "charge", charges=[refunded])

    row = _row(conn, "stripe_charge", {"stripe_id": charge["id"]})
    assert row["status"] == "refunded"
    assert row["amount_refunded"] == "6.99"


def test_resync_of_a_posted_charge_cannot_post_it_twice(conn):
    env = build_gl_ready_env(conn)
    seed_charge(conn, env["stripe_account_id"], env["company_id"],
                stripe_id="ch_resync", amount="100.00")
    seed_balance_transaction(conn, env["stripe_account_id"], env["company_id"],
                             stripe_id="txn_resync", source_id="ch_resync",
                             amount="100.00", fee="3.20", net="96.80")
    post = ns(stripe_account_id=env["stripe_account_id"],
              charge_stripe_id="ch_resync", cost_center_id=env["cost_center_id"])
    first = call_action(GL_ACTIONS["stripe-post-charge-gl"], conn, post)
    assert is_ok(first), first

    _sync(conn, env, "charge", charges=[dict(
        MOCK_CHARGES[0], id="ch_resync", amount=10000, customer="",
        created=1773576000)])

    second = call_action(GL_ACTIONS["stripe-post-charge-gl"], conn, post)
    assert is_error(second), second
    assert "already posted" in second["message"]
    vouchers = conn.execute(
        "SELECT DISTINCT voucher_id FROM gl_entry "
        "WHERE voucher_type = 'payment_entry' AND is_cancelled = 0").fetchall()
    assert [v["voucher_id"] for v in vouchers] == [first["payment_entry_id"]]
