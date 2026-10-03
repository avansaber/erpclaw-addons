"""Behavioural depth tests for four Stripe actions (m466).

Every test below calls the action and then reads the affected rows back from
the database, comparing exact values (money as exact decimal strings, never
float). Each action also has a refusal test proving the refusal message is
truthful and the database is unchanged afterwards.

Per-action write effects, verified against the scripts/ source:
- stripe-process-webhook stores one stripe_deep_webhook_event row and, for a
  known event type, runs the mapped sync, which writes mirror rows plus one
  stripe_sync_job row.
- stripe-replay-webhook bumps process_attempts on the stored
  stripe_deep_webhook_event row, marks it processed, and re-runs the mapped
  sync (again one stripe_sync_job row plus mirror rows).
- stripe-reconcile-payout flips stripe_payout.reconciled 0 -> 1 and sets
  transaction_count. It never touches balance transactions or the ledger.
- stripe-reconciliation-report is read-only: it aggregates
  stripe_balance_transaction by the reconciled flag.

None of these four actions reaches the ledger, so there are no debit/credit
legs to balance here. Every test pins gl_entry, payment_entry and
journal_entry at zero rows before and after; this comment is the marker a
later reader needs, so no ledger assertion is added that cannot hold.
"""
import json
import os
import sys
from decimal import Decimal
from unittest.mock import MagicMock, patch

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_test_helpers import (  # noqa: E402
    build_stripe_env, call_action, is_error, is_ok, ns,
    seed_balance_transaction, seed_charge, seed_payout, seed_stripe_account,
    _uuid,
)
from erpclaw_lib.query import P, Q, Table, insert_row  # noqa: E402
from sync import ACTIONS as SYNC_ACTIONS  # noqa: E402
from reconciliation import ACTIONS as RECON_ACTIONS  # noqa: E402
from reports import ACTIONS as REPORT_ACTIONS  # noqa: E402


# ---------------------------------------------------------------------------
# Read-back helpers (PyPika through erpclaw_lib.query; no catalog queries)
# ---------------------------------------------------------------------------

def _snapshot(conn, table, columns, order_column):
    t = Table(table)
    query = Q.from_(t).select(*columns).orderby(getattr(t, order_column))
    return [tuple(row) for row in conn.execute(query.get_sql()).fetchall()]


WEBHOOK_COLUMNS = (
    "id", "stripe_account_id", "stripe_event_id", "event_type",
    "api_version", "object_id", "object_type", "payload", "processed",
    "process_attempts", "max_attempts", "processed_at", "error_message",
    "created_stripe", "created_at",
)
CHARGE_COLUMNS = (
    "stripe_id", "stripe_account_id", "amount", "currency", "status",
)
SYNC_JOB_COLUMNS = (
    "id", "stripe_account_id", "object_type", "status", "records_processed",
)
PAYOUT_COLUMNS = (
    "stripe_id", "stripe_account_id", "amount", "reconciled",
    "transaction_count",
)
BT_COLUMNS = (
    "stripe_id", "stripe_account_id", "type", "amount", "fee", "net",
    "reconciled", "payout_id",
)


def _module_snapshot(conn):
    return (
        _snapshot(conn, "stripe_deep_webhook_event", WEBHOOK_COLUMNS,
                  "stripe_event_id"),
        _snapshot(conn, "stripe_charge", CHARGE_COLUMNS, "stripe_id"),
        _snapshot(conn, "stripe_sync_job", SYNC_JOB_COLUMNS, "id"),
        _snapshot(conn, "stripe_payout", PAYOUT_COLUMNS, "stripe_id"),
        _snapshot(conn, "stripe_balance_transaction", BT_COLUMNS, "stripe_id"),
    )


def _ledger_counts(conn):
    counts = []
    for table in ("gl_entry", "payment_entry", "journal_entry"):
        t = Table(table)
        rows = conn.execute(Q.from_(t).select(t.id).get_sql()).fetchall()
        counts.append(len(rows))
    return tuple(counts)


def _webhook_row(conn, stripe_event_id):
    t = Table("stripe_deep_webhook_event")
    return conn.execute(
        Q.from_(t).select("*").where(t.stripe_event_id == P()).get_sql(),
        (stripe_event_id,),
    ).fetchone()


def _charge_row(conn, stripe_id):
    t = Table("stripe_charge")
    return conn.execute(
        Q.from_(t).select("*").where(t.stripe_id == P()).get_sql(),
        (stripe_id,),
    ).fetchone()


def _sync_jobs(conn, stripe_account_id):
    t = Table("stripe_sync_job")
    return conn.execute(
        Q.from_(t).select("*").where(t.stripe_account_id == P()).get_sql(),
        (stripe_account_id,),
    ).fetchall()


def _payout_row(conn, stripe_id):
    t = Table("stripe_payout")
    return conn.execute(
        Q.from_(t).select("*").where(t.stripe_id == P()).get_sql(),
        (stripe_id,),
    ).fetchone()


def _seed_webhook_event(conn, stripe_account_id, stripe_event_id, event_type,
                        object_id, object_type, payload, processed=0,
                        process_attempts=0):
    wid = _uuid()
    sql, _ = insert_row("stripe_deep_webhook_event", {
        "id": P(), "stripe_account_id": P(), "stripe_event_id": P(),
        "event_type": P(), "api_version": P(), "object_id": P(),
        "object_type": P(), "payload": P(), "processed": P(),
        "process_attempts": P(), "max_attempts": P(),
        "created_stripe": P(),
    })
    conn.execute(sql, (
        wid, stripe_account_id, stripe_event_id, event_type, "2024-06-20",
        object_id, object_type, payload, processed, process_attempts, 3,
        "2025-10-20T00:00:00Z",
    ))
    conn.commit()
    return wid


# ---------------------------------------------------------------------------
# Mock Stripe API objects (attribute access, as the sync handlers expect)
# ---------------------------------------------------------------------------

def _make_stripe_obj(data):
    obj = MagicMock()
    for key, value in data.items():
        if isinstance(value, dict):
            setattr(obj, key, _make_stripe_obj(value))
        elif isinstance(value, list):
            setattr(obj, key, [
                _make_stripe_obj(i) if isinstance(i, dict) else i
                for i in value
            ])
        else:
            setattr(obj, key, value)
    obj.get = lambda key, default=None: data.get(key, default)
    return obj


def _mock_stripe_with_charges(charges):
    mock = MagicMock()
    listed = MagicMock()
    listed.auto_paging_iter.return_value = iter(
        [_make_stripe_obj(c) for c in charges]
    )
    mock.Charge.list.return_value = listed
    mock.api_key = None
    return mock


CHARGE_M466 = {
    "id": "ch_m466_wh1",
    "amount": 699,
    "currency": "usd",
    "customer": "cus_m466_1",
    "status": "succeeded",
    "description": "M466 webhook charge",
    "payment_method_types": ["card"],
    "payment_intent": "pi_m466_1",
    "invoice": None,
    "amount_refunded": 0,
    "disputed": False,
    "failure_code": None,
    "metadata": {},
    "created": 1760918400,
}

EVENT_M466_CHARGE = {
    "id": "evt_m466_1",
    "type": "charge.succeeded",
    "api_version": "2024-06-20",
    "created": 1760918400,
    "data": {"object": {"id": "ch_m466_wh1", "object": "charge"}},
}

EVENT_M466_UNKNOWN = {
    "id": "evt_m466_2",
    "type": "account.updated",
    "api_version": "2024-06-20",
    "created": 1760918400,
    "data": {"object": {"id": "acct_m466_1", "object": "account"}},
}


# ===========================================================================
# stripe-process-webhook
# ===========================================================================

def _process(conn, stripe_account_id, event_data):
    return call_action(SYNC_ACTIONS["stripe-process-webhook"], conn, ns(
        stripe_account_id=stripe_account_id,
        event_data=event_data,
    ))


class TestProcessWebhook:

    def test_known_type_stores_event_and_syncs_charge(self, conn):
        env = build_stripe_env(conn)
        acct, co = env["stripe_account_id"], env["company_id"]
        other_acct = seed_stripe_account(conn, co, name="Other Stripe")
        seed_charge(conn, other_acct, co, stripe_id="ch_m466_decoy",
                    amount="10.00")
        before_ledger = _ledger_counts(conn)
        assert before_ledger == (0, 0, 0)

        mock = _mock_stripe_with_charges([CHARGE_M466])
        with patch.dict("sys.modules", {"stripe": mock}):
            r = _process(conn, acct, json.dumps(EVENT_M466_CHARGE))

        assert is_ok(r), r
        assert r["stripe_event_id"] == "evt_m466_1"
        assert r["event_type"] == "charge.succeeded"
        assert r["processed"] is True
        assert r["sync_object_type"] == "charge"
        assert r["error"] is None

        row = _webhook_row(conn, "evt_m466_1")
        assert row["event_type"] == "charge.succeeded"
        assert row["object_id"] == "ch_m466_wh1"
        assert row["object_type"] == "charge"
        assert row["api_version"] == "2024-06-20"
        assert json.loads(row["payload"]) == EVENT_M466_CHARGE
        assert row["processed"] == 1
        assert row["process_attempts"] == 1
        assert row["error_message"] is None
        assert row["created_stripe"] == "2025-10-20T00:00:00Z"
        assert row["processed_at"] is not None
        assert row["stripe_account_id"] == acct
        assert r["webhook_event_id"] == row["id"]

        charge = _charge_row(conn, "ch_m466_wh1")
        assert charge["amount"] == "6.99"
        assert Decimal(charge["amount"]) == Decimal("6.99")
        assert charge["currency"] == "usd"
        assert charge["customer_stripe_id"] == "cus_m466_1"
        assert charge["status"] == "succeeded"
        assert charge["amount_refunded"] == "0"
        assert charge["disputed"] == 0
        assert charge["description"] == "M466 webhook charge"
        assert charge["payment_intent_id"] == "pi_m466_1"
        assert charge["invoice_stripe_id"] == ""
        assert charge["failure_code"] is None
        assert charge["metadata"] == "{}"
        assert charge["created_stripe"] == "2025-10-20T00:00:00Z"
        assert charge["stripe_account_id"] == acct

        jobs = _sync_jobs(conn, acct)
        assert len(jobs) == 1
        assert jobs[0]["status"] == "completed"
        assert jobs[0]["object_type"] == "charge"
        assert jobs[0]["sync_type"] == "webhook"
        assert jobs[0]["records_processed"] == 1
        assert jobs[0]["records_failed"] == 0

        decoy = _charge_row(conn, "ch_m466_decoy")
        assert decoy["amount"] == "10.00"
        assert decoy["stripe_account_id"] == other_acct
        assert _sync_jobs(conn, other_acct) == []
        assert _ledger_counts(conn) == (0, 0, 0)

    def test_unknown_type_stores_event_without_sync(self, conn):
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]

        r = _process(conn, acct, json.dumps(EVENT_M466_UNKNOWN))

        assert is_ok(r), r
        assert r["stripe_event_id"] == "evt_m466_2"
        assert r["processed"] is True
        assert r["sync_object_type"] is None
        assert r["error"] is None

        row = _webhook_row(conn, "evt_m466_2")
        assert row["event_type"] == "account.updated"
        assert row["object_id"] == "acct_m466_1"
        assert row["object_type"] == "account"
        assert row["processed"] == 1
        assert row["process_attempts"] == 0
        assert row["error_message"] is None
        assert row["stripe_account_id"] == acct

        assert _sync_jobs(conn, acct) == []
        assert _charge_row(conn, "ch_m466_wh1") is None
        assert _ledger_counts(conn) == (0, 0, 0)

    def test_second_delivery_is_idempotent(self, conn):
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]

        mock = _mock_stripe_with_charges([CHARGE_M466])
        with patch.dict("sys.modules", {"stripe": mock}):
            first = _process(conn, acct, json.dumps(EVENT_M466_CHARGE))
        assert is_ok(first), first

        second = _process(conn, acct, json.dumps(EVENT_M466_CHARGE))

        assert is_ok(second), second
        # The ok() envelope keeps top-level status "ok" and moves the
        # handler's own state word to document_status.
        assert second["document_status"] == "already_processed"
        assert second["webhook_event_id"] == first["webhook_event_id"]
        assert second["stripe_event_id"] == "evt_m466_1"

        wh = Table("stripe_deep_webhook_event")
        rows = conn.execute(
            Q.from_(wh).select(wh.id).get_sql()
        ).fetchall()
        assert len(rows) == 1
        assert _webhook_row(conn, "evt_m466_1")["process_attempts"] == 1
        assert len(_sync_jobs(conn, acct)) == 1
        assert _charge_row(conn, "ch_m466_wh1")["amount"] == "6.99"
        assert _ledger_counts(conn) == (0, 0, 0)


class TestProcessWebhookRefusals:

    def test_refusals_write_nothing(self, conn):
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        before = _module_snapshot(conn)
        assert _ledger_counts(conn) == (0, 0, 0)

        r = call_action(SYNC_ACTIONS["stripe-process-webhook"], conn, ns(
            stripe_account_id=acct,
        ))
        assert is_error(r)
        assert r["message"] == "--event-data is required (JSON string of the Stripe event)"

        r = _process(conn, acct, "{not-json")
        assert is_error(r)
        assert r["message"] == "--event-data must be valid JSON"

        r = _process(conn, acct, json.dumps({"id": "evt_m466_x"}))
        assert is_error(r)
        assert r["message"] == "Event must contain 'id' and 'type' fields"

        r = _process(conn, "acct-m466-missing",
                     json.dumps(EVENT_M466_CHARGE))
        assert is_error(r)
        assert r["message"] == "Stripe account acct-m466-missing not found"

        r = _process(conn, None, json.dumps(EVENT_M466_CHARGE))
        assert is_error(r)
        assert r["message"] == "--stripe-account-id is required"

        assert _module_snapshot(conn) == before
        assert _ledger_counts(conn) == (0, 0, 0)


# ===========================================================================
# stripe-replay-webhook
# ===========================================================================

CHARGE_M466_REPLAY = {
    "id": "ch_m466_rp1",
    "amount": 2500,
    "currency": "usd",
    "customer": "cus_m466_rp",
    "status": "succeeded",
    "description": "M466 replay charge",
    "payment_method_types": ["card"],
    "payment_intent": "pi_m466_rp1",
    "invoice": None,
    "amount_refunded": 0,
    "disputed": False,
    "failure_code": None,
    "metadata": {},
    "created": 1760918500,
}

EVENT_M466_REPLAY = {
    "id": "evt_m466_rp1",
    "type": "charge.succeeded",
    "api_version": "2024-06-20",
    "created": 1760918500,
    "data": {"object": {"id": "ch_m466_rp1", "object": "charge"}},
}


def _replay(conn, webhook_event_id):
    return call_action(SYNC_ACTIONS["stripe-replay-webhook"], conn, ns(
        webhook_event_id=webhook_event_id,
    ))


class TestReplayWebhook:

    def test_replay_unknown_type_marks_processed_and_bumps_attempts(self, conn):
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        wid = _seed_webhook_event(
            conn, acct, "evt_m466_rp0", "account.updated",
            "acct_m466_1", "account", json.dumps(EVENT_M466_UNKNOWN))

        r = _replay(conn, wid)

        assert is_ok(r), r
        assert r["webhook_event_id"] == wid
        assert r["event_type"] == "account.updated"
        assert r["processed"] is True
        assert r["records_processed"] == 0
        assert r["error"] is None

        row = _webhook_row(conn, "evt_m466_rp0")
        assert row["processed"] == 1
        assert row["process_attempts"] == 1
        assert row["processed_at"] is not None
        assert row["error_message"] is None

        assert _sync_jobs(conn, acct) == []
        assert _charge_row(conn, "ch_m466_rp1") is None
        assert _ledger_counts(conn) == (0, 0, 0)

    def test_replay_known_type_resyncs_charge(self, conn):
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        wid = _seed_webhook_event(
            conn, acct, "evt_m466_rp1", "charge.succeeded",
            "ch_m466_rp1", "charge", json.dumps(EVENT_M466_REPLAY))

        mock = _mock_stripe_with_charges([CHARGE_M466_REPLAY])
        with patch.dict("sys.modules", {"stripe": mock}):
            r = _replay(conn, wid)

        assert is_ok(r), r
        assert r["processed"] is True
        assert r["records_processed"] == 1
        assert r["error"] is None

        row = _webhook_row(conn, "evt_m466_rp1")
        assert row["processed"] == 1
        assert row["process_attempts"] == 1
        assert row["processed_at"] is not None
        assert row["error_message"] is None

        charge = _charge_row(conn, "ch_m466_rp1")
        # The sync stores str(cents_to_decimal(...)), so a whole-dollar
        # amount lands as "25", not "25.00": value-equal under Decimal,
        # string-different. Pinned here as shipped, not as wished.
        assert charge["amount"] == "25"
        assert Decimal(charge["amount"]) == Decimal("25.00")
        assert charge["currency"] == "usd"
        assert charge["customer_stripe_id"] == "cus_m466_rp"
        assert charge["status"] == "succeeded"
        assert charge["description"] == "M466 replay charge"
        assert charge["created_stripe"] == "2025-10-20T00:01:40Z"
        assert charge["stripe_account_id"] == acct

        jobs = _sync_jobs(conn, acct)
        assert len(jobs) == 1
        assert jobs[0]["status"] == "completed"
        assert jobs[0]["records_processed"] == 1
        assert _ledger_counts(conn) == (0, 0, 0)


class TestReplayWebhookRefusals:

    def test_refusals_write_nothing(self, conn):
        env = build_stripe_env(conn)
        acct = env["stripe_account_id"]
        wid = _seed_webhook_event(
            conn, acct, "evt_m466_maxed", "charge.succeeded",
            "ch_m466_maxed", "charge", json.dumps(EVENT_M466_REPLAY),
            processed=0, process_attempts=3)
        before = _module_snapshot(conn)
        assert _ledger_counts(conn) == (0, 0, 0)

        r = call_action(SYNC_ACTIONS["stripe-replay-webhook"], conn, ns(
            webhook_event_id=None,
        ))
        assert is_error(r)
        assert r["message"] == "--webhook-event-id is required"

        r = _replay(conn, "wh-m466-missing")
        assert is_error(r)
        assert r["message"] == "Webhook event wh-m466-missing not found"

        r = _replay(conn, wid)
        assert is_error(r)
        assert r["message"] == "Webhook event has reached max attempts (3)"

        maxed = _webhook_row(conn, "evt_m466_maxed")
        assert (maxed["processed"], maxed["process_attempts"]) == (0, 3)
        assert _module_snapshot(conn) == before
        assert _ledger_counts(conn) == (0, 0, 0)


# ===========================================================================
# stripe-reconcile-payout
# ===========================================================================

PAYOUT_M466 = "po_m466_a"


def _seed_payout_m466(conn, payout_amount="295.12"):
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    other_acct = seed_stripe_account(conn, co, name="Other Stripe")
    seed_payout(conn, acct, co, stripe_id=PAYOUT_M466, amount=payout_amount)
    seed_payout(conn, acct, co, stripe_id="po_m466_other", amount="48.25")
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m466_ch1",
                             source_id="ch_m466_1", amount="200.00",
                             fee="6.10", net="193.90", bt_type="charge",
                             payout_id=PAYOUT_M466)
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m466_ch2",
                             source_id="ch_m466_2", amount="120.00",
                             fee="3.78", net="116.22", bt_type="charge",
                             payout_id=PAYOUT_M466)
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m466_re1",
                             source_id="re_m466_1", amount="-15.00",
                             fee="0.00", net="-15.00", bt_type="refund",
                             payout_id=PAYOUT_M466)
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m466_loose",
                             source_id="ch_m466_3", amount="80.00",
                             fee="2.62", net="77.38", bt_type="charge",
                             payout_id=None)
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m466_other",
                             source_id="ch_m466_4", amount="50.00",
                             fee="1.75", net="48.25", bt_type="charge",
                             payout_id="po_m466_other")
    seed_balance_transaction(conn, other_acct, co, stripe_id="txn_m466_b1",
                             source_id="ch_m466_b1", amount="999.00",
                             fee="0.00", net="999.00", bt_type="charge",
                             payout_id=PAYOUT_M466)
    return acct, other_acct, co


def _reconcile(conn, stripe_account_id, payout_stripe_id):
    return call_action(RECON_ACTIONS["stripe-reconcile-payout"], conn, ns(
        stripe_account_id=stripe_account_id,
        payout_stripe_id=payout_stripe_id,
    ))


class TestReconcilePayout:

    def test_balanced_payout_moves_row_from_open_to_reconciled(self, conn):
        acct, _, _ = _seed_payout_m466(conn)
        assert _payout_row(conn, PAYOUT_M466)["reconciled"] == 0
        assert _payout_row(conn, PAYOUT_M466)["transaction_count"] == 0
        before_bt = _snapshot(conn, "stripe_balance_transaction", BT_COLUMNS,
                              "stripe_id")
        assert _ledger_counts(conn) == (0, 0, 0)

        r = _reconcile(conn, acct, PAYOUT_M466)

        assert is_ok(r), r
        assert r["payout_stripe_id"] == PAYOUT_M466
        assert r["payout_amount"] == "295.12"
        assert r["constituent_net_total"] == "295.12"
        assert r["difference"] == "0.00"
        assert r["balanced"] is True
        assert r["reconciled"] is True
        assert r["transaction_count"] == 3
        assert sorted(
            (t["stripe_id"], t["type"], t["amount"], t["fee"], t["net"],
             t["source_id"]) for t in r["transactions"]
        ) == [
            ("txn_m466_ch1", "charge", "200.00", "6.10", "193.90",
             "ch_m466_1"),
            ("txn_m466_ch2", "charge", "120.00", "3.78", "116.22",
             "ch_m466_2"),
            ("txn_m466_re1", "refund", "-15.00", "0.00", "-15.00",
             "re_m466_1"),
        ]
        gross = sum((Decimal(t["amount"]) for t in r["transactions"]),
                    Decimal("0"))
        fees = sum((Decimal(t["fee"]) for t in r["transactions"]),
                   Decimal("0"))
        assert (str(gross), str(fees)) == ("305.00", "9.88")
        assert str(gross - fees) == r["constituent_net_total"]
        assert Decimal(r["payout_amount"]) == Decimal("295.12")

        payout = _payout_row(conn, PAYOUT_M466)
        assert (payout["reconciled"], payout["transaction_count"],
                payout["amount"]) == (1, 3, "295.12")
        other = _payout_row(conn, "po_m466_other")
        assert (other["reconciled"], other["transaction_count"],
                other["amount"]) == (0, 0, "48.25")
        assert _snapshot(conn, "stripe_balance_transaction", BT_COLUMNS,
                         "stripe_id") == before_bt
        assert _ledger_counts(conn) == (0, 0, 0)

    def test_mismatch_reports_variance_and_leaves_payout_open(self, conn):
        acct, _, _ = _seed_payout_m466(conn, payout_amount="300.00")

        r = _reconcile(conn, acct, PAYOUT_M466)

        assert is_ok(r), r
        assert r["payout_amount"] == "300.00"
        assert r["constituent_net_total"] == "295.12"
        assert r["difference"] == "4.88"
        assert r["balanced"] is False
        assert r["reconciled"] is False
        payout = _payout_row(conn, PAYOUT_M466)
        assert (payout["reconciled"], payout["transaction_count"],
                payout["amount"]) == (0, 0, "300.00")
        assert _ledger_counts(conn) == (0, 0, 0)


class TestReconcilePayoutRefusals:

    def test_refusals_write_nothing(self, conn):
        acct, other_acct, co = _seed_payout_m466(conn)
        seed_payout(conn, other_acct, co, stripe_id="po_m466_b",
                    amount="10.00")
        before = _module_snapshot(conn)
        assert _ledger_counts(conn) == (0, 0, 0)

        r = _reconcile(conn, acct, "po_m466_missing")
        assert is_error(r)
        assert r["message"] == "Payout po_m466_missing not found"

        r = _reconcile(conn, acct, "po_m466_b")
        assert is_error(r)
        assert r["message"] == "Payout po_m466_b not found"

        r = _reconcile(conn, acct, None)
        assert is_error(r)
        assert r["message"] == "--payout-stripe-id is required"

        r = _reconcile(conn, "acct-m466-missing", PAYOUT_M466)
        assert is_error(r)
        assert r["message"] == "Stripe account acct-m466-missing not found"

        r = _reconcile(conn, None, PAYOUT_M466)
        assert is_error(r)
        assert r["message"] == "--stripe-account-id is required"

        assert _payout_row(conn, PAYOUT_M466)["reconciled"] == 0
        assert _module_snapshot(conn) == before
        assert _ledger_counts(conn) == (0, 0, 0)


# ===========================================================================
# stripe-reconciliation-report
# ===========================================================================

def _seed_report_m466(conn):
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    other_acct = seed_stripe_account(conn, co, name="Other Stripe")
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m466_r1",
                             source_id="ch_m466_r1", amount="100.00",
                             fee="3.00", net="97.00", bt_type="charge",
                             reconciled=1)
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m466_r2",
                             source_id="ch_m466_r2", amount="50.00",
                             fee="1.50", net="48.50", bt_type="charge",
                             reconciled=1)
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m466_u1",
                             source_id="re_m466_u1", amount="-25.50",
                             fee="0.00", net="-25.50", bt_type="refund",
                             reconciled=0)
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m466_u2",
                             source_id="ch_m466_u2", amount="60.99",
                             fee="1.82", net="59.17", bt_type="charge",
                             reconciled=0)
    seed_balance_transaction(conn, other_acct, co, stripe_id="txn_m466_b1",
                             source_id="ch_m466_b1", amount="999.00",
                             fee="0.00", net="999.00", bt_type="charge",
                             reconciled=1)
    return acct, other_acct


def _report(conn, stripe_account_id):
    return call_action(REPORT_ACTIONS["stripe-reconciliation-report"], conn,
                       ns(stripe_account_id=stripe_account_id))


class TestReconciliationReport:

    def test_pins_matched_unmatched_amounts_and_is_read_only(self, conn):
        acct, other_acct = _seed_report_m466(conn)
        before = _module_snapshot(conn)
        assert _ledger_counts(conn) == (0, 0, 0)

        r = _report(conn, acct)

        assert is_ok(r), r
        assert r["report"] == "reconciliation"
        assert r["total_transactions"] == 4
        assert r["matched"] == 2
        assert r["unmatched"] == 2
        assert r["matched_amount"] == "150.00"
        assert r["unmatched_amount"] == "35.49"
        assert r["match_rate_pct"] == "50.00"

        bt = Table("stripe_balance_transaction")
        stored = conn.execute(
            Q.from_(bt).select("amount", "reconciled")
            .where(bt.stripe_account_id == P()).get_sql(),
            (acct,),
        ).fetchall()
        matched = sum((Decimal(s["amount"]) for s in stored
                       if s["reconciled"] == 1), Decimal("0"))
        unmatched = sum((Decimal(s["amount"]) for s in stored
                         if s["reconciled"] == 0), Decimal("0"))
        assert str(matched) == r["matched_amount"] == "150.00"
        assert str(unmatched) == r["unmatched_amount"] == "35.49"

        b_r = _report(conn, other_acct)
        assert (b_r["total_transactions"], b_r["matched"], b_r["unmatched"],
                b_r["matched_amount"], b_r["unmatched_amount"],
                b_r["match_rate_pct"]) == (
            1, 1, 0, "999.00", "0.00", "100.00")

        assert _module_snapshot(conn) == before
        assert _ledger_counts(conn) == (0, 0, 0)


class TestReconciliationReportRefusals:

    def test_refusals_write_nothing(self, conn):
        acct, _ = _seed_report_m466(conn)
        before = _module_snapshot(conn)
        assert _ledger_counts(conn) == (0, 0, 0)

        r = _report(conn, None)
        assert is_error(r)
        assert r["message"] == "--stripe-account-id is required"

        r = _report(conn, "acct-m466-missing")
        assert is_error(r)
        assert r["message"] == "Stripe account acct-m466-missing not found"

        assert _report(conn, acct)["total_transactions"] == 4
        assert _module_snapshot(conn) == before
        assert _ledger_counts(conn) == (0, 0, 0)
