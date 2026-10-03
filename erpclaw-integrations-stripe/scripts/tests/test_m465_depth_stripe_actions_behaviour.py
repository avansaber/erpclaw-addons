"""Depth tests for 12 stripe actions previously covered only by shape/routing.

Every test here pins the database effect, not the response envelope: exact
stored rows read back through PyPika queries on the test connection, exact
Decimal money strings, what must NOT have changed, and one refusal case that
leaves the database byte-identical.

No action in this file reaches the GL ledger (none writes journal entries);
each test says so where the assertion would otherwise be expected. The only
writer among the twelve is stripe-cancel-sync, which moves a stripe_sync_job
row running/pending -> cancelled and appends one audit_log row.
"""
import importlib.util
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_test_helpers import (  # noqa: E402
    build_stripe_env, call_action, is_error, is_ok, ns,
    seed_application_fee, seed_customer_map, seed_dispute, seed_invoice,
    seed_payout, seed_balance_transaction, seed_refund, seed_stripe_account,
    seed_transfer,
)
from erpclaw_lib.query import P, Q, Table, insert_row, update_row  # noqa: E402
from sync import ACTIONS as SYNC_ACTIONS  # noqa: E402
from connect import ACTIONS as CONNECT_ACTIONS  # noqa: E402
from reports import ACTIONS as REPORT_ACTIONS  # noqa: E402
from browse import ACTIONS as BROWSE_ACTIONS  # noqa: E402
from utils import ACTIONS as UTILS_ACTIONS  # noqa: E402


def _dump(conn, table, order_col):
    t = Table(table)
    rows = conn.execute(
        Q.from_(t).select("*").orderby(getattr(t, order_col)).get_sql()
    ).fetchall()
    return [tuple(r) for r in rows]


def _one(conn, table, column, value):
    t = Table(table)
    return conn.execute(
        Q.from_(t).select("*").where(getattr(t, column) == P()).get_sql(),
        (value,)
    ).fetchone()


def _set(conn, table, column, value, where_col, where_val):
    conn.execute(
        update_row(table, {column: P()}, {where_col: P()}),
        (value, where_val))
    conn.commit()


def _seed_sync_job(conn, acct, company, job_id, status, started_at=None):
    sql, _ = insert_row("stripe_sync_job", {
        "id": P(), "stripe_account_id": P(), "sync_type": P(),
        "object_type": P(), "status": P(), "started_at": P(),
        "company_id": P(),
    })
    conn.execute(sql, (job_id, acct, "incremental", "charge",
                       status, started_at, company))
    conn.commit()


def _seed_webhook(conn, acct, event_id, event_type, processed, created_at):
    import uuid as _uuid
    sql, _ = insert_row("stripe_deep_webhook_event", {
        "id": P(), "stripe_account_id": P(), "stripe_event_id": P(),
        "event_type": P(), "object_id": P(), "object_type": P(),
        "processed": P(), "created_at": P(),
    })
    conn.execute(sql, (_uuid.uuid4().hex, acct, event_id, event_type,
                       "ch_m465_1", "charge", processed, created_at))
    conn.commit()


# ── stripe-cancel-sync ───────────────────────────────────────────────────

def test_cancel_sync_moves_running_job_to_cancelled(conn):
    # No GL leg: cancel writes stripe_sync_job + audit_log only, never journals.
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    _seed_sync_job(conn, acct, co, "job-m465-running", "running",
                   "2026-03-15T12:00:00Z")
    _seed_sync_job(conn, acct, co, "job-m465-done", "completed",
                   "2026-03-14T12:00:00Z")
    _set(conn, "stripe_sync_job", "completed_at", "2026-03-14T12:05:00Z",
         "id", "job-m465-done")
    jobs_before = _dump(conn, "stripe_sync_job", "id")
    audit_before = _dump(conn, "audit_log", "id")

    r = call_action(SYNC_ACTIONS["stripe-cancel-sync"], conn,
                    ns(sync_job_id="job-m465-running"))

    assert is_ok(r), r
    assert r["sync_job_id"] == "job-m465-running"
    # Real behaviour: ok() injects status="ok", clobbering the action's own
    # status="cancelled" payload field, so the envelope says "ok" while the
    # stored row says "cancelled". Pinned as found, not fixed.
    assert r["status"] == "ok"

    running = _one(conn, "stripe_sync_job", "id", "job-m465-running")
    assert running["status"] == "cancelled"
    assert running["started_at"] == "2026-03-15T12:00:00Z"
    assert running["completed_at"] is not None
    assert running["stripe_account_id"] == acct

    done = _one(conn, "stripe_sync_job", "id", "job-m465-done")
    assert tuple(done) == [t for t in jobs_before if t[0] == "job-m465-done"][0]

    audit_after = _dump(conn, "audit_log", "id")
    assert len(audit_after) == len(audit_before) + 1
    new_rows = [t for t in audit_after if t not in audit_before]
    assert len(new_rows) == 1
    entry = _one(conn, "audit_log", "id", new_rows[0][0])
    assert (entry["skill"], entry["action"], entry["entity_type"],
            entry["entity_id"]) == (
        "erpclaw-integrations-stripe", "stripe-cancel-sync",
        "stripe_sync_job", "job-m465-running")
    assert "cancelled" in (entry["new_values"] or "")


def test_cancel_sync_refusals_write_nothing(conn):
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    _seed_sync_job(conn, acct, co, "job-m465-done", "completed")
    _seed_sync_job(conn, acct, co, "job-m465-failed", "failed")
    _seed_sync_job(conn, acct, co, "job-m465-cancelled", "cancelled")
    _seed_sync_job(conn, acct, co, "job-m465-pending", "pending")
    jobs_before = _dump(conn, "stripe_sync_job", "id")
    audit_before = _dump(conn, "audit_log", "id")

    r = call_action(SYNC_ACTIONS["stripe-cancel-sync"], conn, ns(sync_job_id=None))
    assert is_error(r)
    assert r["message"] == "--sync-job-id is required"

    r = call_action(SYNC_ACTIONS["stripe-cancel-sync"], conn,
                    ns(sync_job_id="job-m465-missing"))
    assert is_error(r)
    assert r["message"] == "Sync job job-m465-missing not found"

    for jid, state in [("job-m465-done", "completed"),
                       ("job-m465-failed", "failed"),
                       ("job-m465-cancelled", "cancelled")]:
        r = call_action(SYNC_ACTIONS["stripe-cancel-sync"], conn,
                        ns(sync_job_id=jid))
        assert is_error(r)
        assert r["message"] == f"Cannot cancel sync job in '{state}' state"

    assert _dump(conn, "stripe_sync_job", "id") == jobs_before
    assert _dump(conn, "audit_log", "id") == audit_before


# ── stripe-list-connected-accounts ───────────────────────────────────────

def test_list_connected_accounts_returns_own_maps_newest_first(conn):
    # No GL leg: read-only over stripe_customer_map.
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    other = seed_stripe_account(conn, co, name="Other Stripe")
    seed_customer_map(conn, acct, co, stripe_customer_id="cus_m465_alpha",
                      stripe_name="Alpha")
    seed_customer_map(conn, acct, co, stripe_customer_id="cus_m465_beta",
                      stripe_name="Beta")
    seed_customer_map(conn, acct, co, stripe_customer_id="cus_m465_gamma",
                      stripe_name="Gamma")
    seed_customer_map(conn, other, co, stripe_customer_id="cus_m465_other",
                      stripe_name="Other")
    _set(conn, "stripe_customer_map", "created_at", "2026-03-01 10:00:00",
         "stripe_customer_id", "cus_m465_alpha")
    _set(conn, "stripe_customer_map", "created_at", "2026-03-03 10:00:00",
         "stripe_customer_id", "cus_m465_beta")
    _set(conn, "stripe_customer_map", "created_at", "2026-03-02 10:00:00",
         "stripe_customer_id", "cus_m465_gamma")
    before = _dump(conn, "stripe_customer_map", "stripe_customer_id")

    r = call_action(CONNECT_ACTIONS["stripe-list-connected-accounts"], conn,
                    ns(stripe_account_id=acct, limit=50))

    assert is_ok(r), r
    assert r["count"] == 3
    assert [(c["stripe_customer_id"], c["stripe_name"], c["stripe_account_id"])
            for c in r["connected_accounts"]] == [
        ("cus_m465_beta", "Beta", acct),
        ("cus_m465_gamma", "Gamma", acct),
        ("cus_m465_alpha", "Alpha", acct),
    ]
    for entry in r["connected_accounts"]:
        stored = _one(conn, "stripe_customer_map", "stripe_customer_id",
                      entry["stripe_customer_id"])
        assert entry["stripe_name"] == stored["stripe_name"]
        assert entry["company_id"] == co

    r = call_action(CONNECT_ACTIONS["stripe-list-connected-accounts"], conn,
                    ns(stripe_account_id=acct, limit=2))
    assert [c["stripe_customer_id"] for c in r["connected_accounts"]] == [
        "cus_m465_beta", "cus_m465_gamma"]

    assert _dump(conn, "stripe_customer_map", "stripe_customer_id") == before


def test_list_connected_accounts_refusals_write_nothing(conn):
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    seed_customer_map(conn, acct, co, stripe_customer_id="cus_m465_kept")
    before = _dump(conn, "stripe_customer_map", "stripe_customer_id")

    r = call_action(CONNECT_ACTIONS["stripe-list-connected-accounts"], conn,
                    ns(stripe_account_id=None, limit=50))
    assert is_error(r)
    assert r["message"] == "--stripe-account-id is required"

    r = call_action(CONNECT_ACTIONS["stripe-list-connected-accounts"], conn,
                    ns(stripe_account_id="acct-m465-missing", limit=50))
    assert is_error(r)
    assert r["message"] == "Stripe account acct-m465-missing not found"

    assert _dump(conn, "stripe_customer_map", "stripe_customer_id") == before


# ── stripe-list-invoices ─────────────────────────────────────────────────

def test_list_invoices_returns_own_invoices_with_exact_money(conn):
    # No GL leg: read-only over stripe_invoice.
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    other = seed_stripe_account(conn, co, name="Other Stripe")
    seed_invoice(conn, acct, co, stripe_id="in_m465_open1",
                 amount_due="250.00", status="open")
    seed_invoice(conn, acct, co, stripe_id="in_m465_paid1",
                 amount_due="99.99", status="paid")
    seed_invoice(conn, acct, co, stripe_id="in_m465_open2",
                 amount_due="10.50", status="open")
    seed_invoice(conn, other, co, stripe_id="in_m465_other",
                 amount_due="500.00", status="open")
    _set(conn, "stripe_invoice", "created_at", "2026-03-10 09:00:00",
         "stripe_id", "in_m465_open1")
    _set(conn, "stripe_invoice", "created_at", "2026-03-12 09:00:00",
         "stripe_id", "in_m465_paid1")
    _set(conn, "stripe_invoice", "created_at", "2026-03-11 09:00:00",
         "stripe_id", "in_m465_open2")
    before = _dump(conn, "stripe_invoice", "stripe_id")

    r = call_action(BROWSE_ACTIONS["stripe-list-invoices"], conn,
                    ns(stripe_account_id=acct, status=None, limit=50))

    assert is_ok(r), r
    assert r["count"] == 3
    assert [(i["stripe_id"], i["amount_due"], i["amount_paid"],
             i["amount_remaining"], i["status"], i["stripe_account_id"])
            for i in r["invoices"]] == [
        ("in_m465_paid1", "99.99", "99.99", "0", "paid", acct),
        ("in_m465_open2", "10.50", "10.50", "0", "open", acct),
        ("in_m465_open1", "250.00", "250.00", "0", "open", acct),
    ]
    for entry in r["invoices"]:
        stored = _one(conn, "stripe_invoice", "stripe_id", entry["stripe_id"])
        assert Decimal(entry["amount_due"]) == Decimal(stored["amount_due"])
        assert Decimal(entry["amount_paid"]) == Decimal(stored["amount_paid"])
        assert entry["amount_due"] == stored["amount_due"]

    f = call_action(BROWSE_ACTIONS["stripe-list-invoices"], conn,
                    ns(stripe_account_id=acct, status="open", limit=50))
    assert is_ok(f), f
    assert sorted(i["stripe_id"] for i in f["invoices"]) == [
        "in_m465_open1", "in_m465_open2"]

    one = call_action(BROWSE_ACTIONS["stripe-list-invoices"], conn,
                      ns(stripe_account_id=acct, status=None, limit=1))
    assert [i["stripe_id"] for i in one["invoices"]] == ["in_m465_paid1"]

    assert _dump(conn, "stripe_invoice", "stripe_id") == before


def test_list_invoices_refusals_write_nothing(conn):
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    seed_invoice(conn, acct, co, stripe_id="in_m465_kept", amount_due="5.00")
    before = _dump(conn, "stripe_invoice", "stripe_id")

    r = call_action(BROWSE_ACTIONS["stripe-list-invoices"], conn,
                    ns(stripe_account_id=None, status=None, limit=50))
    assert is_error(r)
    assert r["message"] == "--stripe-account-id is required"

    r = call_action(BROWSE_ACTIONS["stripe-list-invoices"], conn,
                    ns(stripe_account_id="acct-m465-missing", status=None,
                       limit=50))
    assert is_error(r)
    assert r["message"] == "Stripe account acct-m465-missing not found"

    assert _dump(conn, "stripe_invoice", "stripe_id") == before


# ── stripe-list-webhook-events ───────────────────────────────────────────

def test_list_webhook_events_filters_and_paginates(conn):
    # No GL leg: read-only over stripe_deep_webhook_event.
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    other = seed_stripe_account(conn, co, name="Other Stripe")
    _seed_webhook(conn, acct, "evt_m465_1", "charge.succeeded", 1,
                  "2026-03-10 09:00:00")
    _seed_webhook(conn, acct, "evt_m465_2", "charge.succeeded", 0,
                  "2026-03-12 09:00:00")
    _seed_webhook(conn, acct, "evt_m465_3", "payout.paid", 1,
                  "2026-03-11 09:00:00")
    _seed_webhook(conn, other, "evt_m465_4", "charge.succeeded", 1,
                  "2026-03-13 09:00:00")
    before = _dump(conn, "stripe_deep_webhook_event", "stripe_event_id")

    r = call_action(SYNC_ACTIONS["stripe-list-webhook-events"], conn,
                    ns(stripe_account_id=acct, event_type=None,
                       processed=None, limit=50, offset=0))

    assert is_ok(r), r
    assert r["count"] == 3
    assert [(e["stripe_event_id"], e["event_type"], e["processed"],
             e["stripe_account_id"]) for e in r["webhook_events"]] == [
        ("evt_m465_2", "charge.succeeded", 0, acct),
        ("evt_m465_3", "payout.paid", 1, acct),
        ("evt_m465_1", "charge.succeeded", 1, acct),
    ]
    for entry in r["webhook_events"]:
        stored = _one(conn, "stripe_deep_webhook_event", "stripe_event_id",
                      entry["stripe_event_id"])
        assert entry["event_type"] == stored["event_type"]
        assert entry["processed"] == stored["processed"]

    f = call_action(SYNC_ACTIONS["stripe-list-webhook-events"], conn,
                    ns(stripe_account_id=acct, event_type="payout.paid",
                       processed=None, limit=50, offset=0))
    assert [(e["stripe_event_id"]) for e in f["webhook_events"]] == ["evt_m465_3"]

    p = call_action(SYNC_ACTIONS["stripe-list-webhook-events"], conn,
                    ns(stripe_account_id=acct, event_type=None,
                       processed=1, limit=50, offset=0))
    assert sorted(e["stripe_event_id"] for e in p["webhook_events"]) == [
        "evt_m465_1", "evt_m465_3"]

    page = call_action(SYNC_ACTIONS["stripe-list-webhook-events"], conn,
                       ns(stripe_account_id=acct, event_type=None,
                          processed=None, limit=1, offset=1))
    assert [e["stripe_event_id"] for e in page["webhook_events"]] == ["evt_m465_3"]

    assert _dump(conn, "stripe_deep_webhook_event",
                 "stripe_event_id") == before


def test_list_webhook_events_refusal_writes_nothing(conn):
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    _seed_webhook(conn, acct, "evt_m465_kept", "charge.succeeded", 0,
                  "2026-03-10 09:00:00")
    before = _dump(conn, "stripe_deep_webhook_event", "stripe_event_id")

    r = call_action(SYNC_ACTIONS["stripe-list-webhook-events"], conn,
                    ns(stripe_account_id=None, event_type=None,
                       processed=None, limit=50, offset=0))
    assert is_error(r)
    assert r["message"] == "--stripe-account-id is required"

    # The handler does not validate that the account exists: an unknown
    # account returns an empty list, not an error. Pinned as real behaviour.
    r = call_action(SYNC_ACTIONS["stripe-list-webhook-events"], conn,
                    ns(stripe_account_id="acct-m465-missing", event_type=None,
                       processed=None, limit=50, offset=0))
    assert is_ok(r), r
    assert (r["count"], r["webhook_events"]) == (0, [])

    assert _dump(conn, "stripe_deep_webhook_event",
                 "stripe_event_id") == before


# ── stripe-health-check ──────────────────────────────────────────────────

def test_health_check_derives_status_from_stored_accounts(conn):
    # No GL leg: read-only over stripe_account; validates no input at all.
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    stored = _one(conn, "stripe_account", "id", acct)
    name = stored["account_name"]
    accounts_before = _dump(conn, "stripe_account", "id")

    r = call_action(UTILS_ACTIONS["stripe-health-check"], conn, ns())
    assert is_ok(r), r
    assert r["active_accounts"] == 1
    assert r["overall_health"] == "warnings"
    assert r["accounts"] == [{
        "stripe_account_id": acct,
        "account_name": name,
        "last_sync_at": stored["last_sync_at"],
        "has_api_key": True,
        "sync_status": "never_synced",
    }]
    assert r["warnings"] == [f"Account '{name}' has never been synced"]
    assert r["issues"] == []
    assert r["stripe_package_available"] == (
        importlib.util.find_spec("stripe") is not None)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _set(conn, "stripe_account", "last_sync_at", now, "id", acct)
    r = call_action(UTILS_ACTIONS["stripe-health-check"], conn, ns())
    assert (r["overall_health"], r["warnings"],
            r["accounts"][0]["sync_status"]) == ("healthy", [], "ok")
    assert r["accounts"][0]["last_sync_at"] == now

    _set(conn, "stripe_account", "last_sync_at", "2020-01-01T00:00:00Z",
         "id", acct)
    r = call_action(UTILS_ACTIONS["stripe-health-check"], conn, ns())
    assert r["overall_health"] == "warnings"
    assert r["accounts"][0]["sync_status"] == "stale"
    assert r["warnings"] == [
        f"Account '{name}' last synced 2020-01-01T00:00:00Z (>24h ago)"]

    _set(conn, "stripe_account", "restricted_key_enc", "", "id", acct)
    r = call_action(UTILS_ACTIONS["stripe-health-check"], conn, ns())
    assert r["overall_health"] == "unhealthy"
    assert r["accounts"][0]["has_api_key"] is False
    assert f"Account '{name}' has no API key configured" in r["issues"]

    # Unknown/extra arguments are ignored, never refused, never written.
    r = call_action(UTILS_ACTIONS["stripe-health-check"], conn,
                    ns(stripe_account_id="acct-m465-missing"))
    assert is_ok(r), r
    assert r["active_accounts"] == 1

    _set(conn, "stripe_account", "restricted_key_enc",
         stored["restricted_key_enc"], "id", acct)
    _set(conn, "stripe_account", "last_sync_at", stored["last_sync_at"],
         "id", acct)
    assert _dump(conn, "stripe_account", "id") == accounts_before


# ── stripe-connect-revenue-report: month-boundary bucketing ──────────────

def test_connect_revenue_report_buckets_month_boundaries(conn):
    # No GL leg: read-only over stripe_application_fee.
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    seed_application_fee(conn, acct, co, stripe_id="fee_m465_jan",
                         amount="20.15")
    seed_application_fee(conn, acct, co, stripe_id="fee_m465_feb",
                         amount="105.05")
    _set(conn, "stripe_application_fee", "created_stripe",
         "2026-01-31T23:59:59Z", "stripe_id", "fee_m465_jan")
    _set(conn, "stripe_application_fee", "created_stripe",
         "2026-02-01T00:00:00Z", "stripe_id", "fee_m465_feb")
    before = _dump(conn, "stripe_application_fee", "stripe_id")

    r = call_action(CONNECT_ACTIONS["stripe-connect-revenue-report"], conn,
                    ns(stripe_account_id=acct))

    assert is_ok(r), r
    assert r["months"] == [
        {"month": "2026-02", "fee_count": 1, "total_amount": "105.05"},
        {"month": "2026-01", "fee_count": 1, "total_amount": "20.15"},
    ]
    assert r["month_count"] == len(r["months"])
    assert Decimal(r["grand_total"]) == sum(
        (Decimal(m["total_amount"]) for m in r["months"]), Decimal("0"))
    assert r["grand_total"] == "125.20"

    assert _dump(conn, "stripe_application_fee", "stripe_id") == before

    r = call_action(CONNECT_ACTIONS["stripe-connect-revenue-report"], conn,
                    ns(stripe_account_id=None))
    assert is_error(r)
    assert r["message"] == "--stripe-account-id is required"
    r = call_action(CONNECT_ACTIONS["stripe-connect-revenue-report"], conn,
                    ns(stripe_account_id="acct-m465-missing"))
    assert is_error(r)
    assert r["message"] == "Stripe account acct-m465-missing not found"
    assert _dump(conn, "stripe_application_fee", "stripe_id") == before


# ── stripe-connect-fee-summary: exact net arithmetic ─────────────────────

def test_connect_fee_summary_net_arithmetic_is_exact(conn):
    # No GL leg: read-only over stripe_application_fee.
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    seed_application_fee(conn, acct, co, stripe_id="fee_m465_full",
                         amount="100.00")
    seed_application_fee(conn, acct, co, stripe_id="fee_m465_part",
                         amount="50.25")
    _set(conn, "stripe_application_fee", "refunded_amount", "100.00",
         "stripe_id", "fee_m465_full")
    _set(conn, "stripe_application_fee", "refunded_amount", "0.25",
         "stripe_id", "fee_m465_part")
    before = _dump(conn, "stripe_application_fee", "stripe_id")

    r = call_action(CONNECT_ACTIONS["stripe-connect-fee-summary"], conn,
                    ns(stripe_account_id=acct))

    assert is_ok(r), r
    assert r["fee_count"] == 2
    assert r["total_earned"] == "150.25"
    assert r["total_refunded"] == "100.25"
    assert r["net_earned"] == "50.00"
    assert Decimal(r["total_earned"]) - Decimal(r["total_refunded"]) == \
        Decimal(r["net_earned"])

    assert _dump(conn, "stripe_application_fee", "stripe_id") == before

    r = call_action(CONNECT_ACTIONS["stripe-connect-fee-summary"], conn,
                    ns(stripe_account_id=None))
    assert is_error(r)
    assert r["message"] == "--stripe-account-id is required"
    r = call_action(CONNECT_ACTIONS["stripe-connect-fee-summary"], conn,
                    ns(stripe_account_id="acct-m465-missing"))
    assert is_error(r)
    assert r["message"] == "Stripe account acct-m465-missing not found"
    assert _dump(conn, "stripe_application_fee", "stripe_id") == before


# ── stripe-connect-payout-report: reversed transfers ─────────────────────

def test_connect_payout_report_nets_out_reversed_transfers(conn):
    # No GL leg: read-only over stripe_transfer. Fully reversed transfers
    # are excluded from the paid-out totals and reported separately under
    # reversed_count / reversed_amount.
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    seed_transfer(conn, acct, co, stripe_id="tr_m465_live", amount="200.00")
    seed_transfer(conn, acct, co, stripe_id="tr_m465_reversed", amount="75.00")
    _set(conn, "stripe_transfer", "created_stripe", "2026-01-15T08:00:00Z",
         "stripe_id", "tr_m465_live")
    _set(conn, "stripe_transfer", "created_stripe", "2026-01-20T08:00:00Z",
         "stripe_id", "tr_m465_reversed")
    _set(conn, "stripe_transfer", "reversed", 1,
         "stripe_id", "tr_m465_reversed")
    before = _dump(conn, "stripe_transfer", "stripe_id")

    r = call_action(CONNECT_ACTIONS["stripe-connect-payout-report"], conn,
                    ns(stripe_account_id=acct))

    assert is_ok(r), r
    assert r["months"] == [
        {"month": "2026-01", "transfer_count": 1, "total_amount": "200.00",
         "reversed_count": 1, "reversed_amount": "75.00"},
    ]
    assert r["grand_total"] == "200.00"
    assert r["reversed_count"] == 1
    assert r["reversed_total"] == "75.00"
    assert Decimal(r["grand_total"]) + Decimal(r["reversed_total"]) == Decimal("275.00")

    assert _dump(conn, "stripe_transfer", "stripe_id") == before

    r = call_action(CONNECT_ACTIONS["stripe-connect-payout-report"], conn,
                    ns(stripe_account_id=None))
    assert is_error(r)
    assert r["message"] == "--stripe-account-id is required"
    r = call_action(CONNECT_ACTIONS["stripe-connect-payout-report"], conn,
                    ns(stripe_account_id="acct-m465-missing"))
    assert is_error(r)
    assert r["message"] == "Stripe account acct-m465-missing not found"
    assert _dump(conn, "stripe_transfer", "stripe_id") == before


# ── stripe-dispute-report: totals invariant ──────────────────────────────

def test_dispute_report_grand_total_equals_sum_of_status_totals(conn):
    # No GL leg: read-only over stripe_dispute.
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    seed_dispute(conn, acct, co, stripe_id="dp_m465_1",
                 charge_stripe_id="ch_m465_1", amount="150.00",
                 status="needs_response")
    seed_dispute(conn, acct, co, stripe_id="dp_m465_2",
                 charge_stripe_id="ch_m465_2", amount="75.50",
                 status="needs_response")
    seed_dispute(conn, acct, co, stripe_id="dp_m465_3",
                 charge_stripe_id="ch_m465_3", amount="1200.00", status="won")
    before = _dump(conn, "stripe_dispute", "stripe_id")

    r = call_action(REPORT_ACTIONS["stripe-dispute-report"], conn,
                    ns(stripe_account_id=acct))

    assert is_ok(r), r
    assert r["statuses"] == [
        {"status": "needs_response", "count": 2, "total_amount": "225.50"},
        {"status": "won", "count": 1, "total_amount": "1200.00"},
    ]
    assert r["total_disputes"] == sum(s["count"] for s in r["statuses"])
    assert Decimal(r["total_amount"]) == sum(
        (Decimal(s["total_amount"]) for s in r["statuses"]), Decimal("0"))
    assert r["total_amount"] == "1425.50"

    assert _dump(conn, "stripe_dispute", "stripe_id") == before

    r = call_action(REPORT_ACTIONS["stripe-dispute-report"], conn,
                    ns(stripe_account_id=None))
    assert is_error(r)
    assert r["message"] == "--stripe-account-id is required"
    r = call_action(REPORT_ACTIONS["stripe-dispute-report"], conn,
                    ns(stripe_account_id="acct-m465-missing"))
    assert is_error(r)
    assert r["message"] == "Stripe account acct-m465-missing not found"
    assert _dump(conn, "stripe_dispute", "stripe_id") == before


# ── stripe-payout-detail-report: single-charge payout ────────────────────

def test_payout_detail_report_single_charge_payout(conn):
    # No GL leg: read-only over stripe_payout + stripe_balance_transaction.
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    seed_payout(conn, acct, co, stripe_id="po_m465_single", amount="97.00")
    seed_balance_transaction(conn, acct, co, stripe_id="txn_m465_single",
                             source_id="ch_m465_single", amount="100.00",
                             fee="3.00", net="97.00", bt_type="charge",
                             payout_id="po_m465_single")
    before_payouts = _dump(conn, "stripe_payout", "stripe_id")
    before_txns = _dump(conn, "stripe_balance_transaction", "stripe_id")

    r = call_action(REPORT_ACTIONS["stripe-payout-detail-report"], conn,
                    ns(payout_stripe_id="po_m465_single"))

    assert is_ok(r), r
    assert (r["stripe_id"], r["amount"], r["document_status"],
            r["reconciled"], r["transaction_count"]) == (
        "po_m465_single", "97.00", "paid", 0, 1)
    assert [(t["stripe_id"], t["type"], t["amount"], t["fee"], t["net"])
            for t in r["transactions"]] == [
        ("txn_m465_single", "charge", "100.00", "3.00", "97.00")]
    assert r["type_summary"] == [
        {"type": "charge", "count": 1, "amount": "100.00", "fee": "3.00"}]
    assert str(sum((Decimal(t["net"]) for t in r["transactions"]),
                   Decimal("0"))) == r["amount"]

    assert _dump(conn, "stripe_payout", "stripe_id") == before_payouts
    assert _dump(conn, "stripe_balance_transaction",
                 "stripe_id") == before_txns

    r = call_action(REPORT_ACTIONS["stripe-payout-detail-report"], conn,
                    ns(payout_stripe_id="po_m465_missing"))
    assert is_error(r)
    assert r["message"] == "Payout po_m465_missing not found"
    r = call_action(REPORT_ACTIONS["stripe-payout-detail-report"], conn,
                    ns(payout_stripe_id=None))
    assert is_error(r)
    assert r["message"] == "--payout-stripe-id is required"
    assert _dump(conn, "stripe_payout", "stripe_id") == before_payouts
    assert _dump(conn, "stripe_balance_transaction",
                 "stripe_id") == before_txns


# ── stripe-list-application-fees: created_at ordering ────────────────────

def test_list_application_fees_orders_by_created_at(conn):
    # No GL leg: read-only over stripe_application_fee. The three rows share
    # one created_stripe and their stripe_ids sort opposite to created_at, so
    # the newest-first order below can only come from created_at.
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    seed_application_fee(conn, acct, co, stripe_id="fee_m465_alpha",
                         amount="12.35")
    seed_application_fee(conn, acct, co, stripe_id="fee_m465_beta",
                         amount="7.80")
    seed_application_fee(conn, acct, co, stripe_id="fee_m465_gamma",
                         amount="105.05")
    _set(conn, "stripe_application_fee", "created_at", "2026-03-01 10:00:00",
         "stripe_id", "fee_m465_alpha")
    _set(conn, "stripe_application_fee", "created_at", "2026-03-03 10:00:00",
         "stripe_id", "fee_m465_beta")
    _set(conn, "stripe_application_fee", "created_at", "2026-03-02 10:00:00",
         "stripe_id", "fee_m465_gamma")
    before = _dump(conn, "stripe_application_fee", "stripe_id")

    r = call_action(CONNECT_ACTIONS["stripe-list-application-fees"], conn,
                    ns(stripe_account_id=acct, limit=50))

    assert is_ok(r), r
    assert r["count"] == 3
    assert [(f["stripe_id"], f["amount"]) for f in r["application_fees"]] == [
        ("fee_m465_beta", "7.80"),
        ("fee_m465_gamma", "105.05"),
        ("fee_m465_alpha", "12.35"),
    ]
    for entry in r["application_fees"]:
        stored = _one(conn, "stripe_application_fee", "stripe_id",
                      entry["stripe_id"])
        assert Decimal(entry["amount"]) == Decimal(stored["amount"])
        assert entry["amount"] == stored["amount"]

    assert _dump(conn, "stripe_application_fee", "stripe_id") == before

    r = call_action(CONNECT_ACTIONS["stripe-list-application-fees"], conn,
                    ns(stripe_account_id=None, limit=50))
    assert is_error(r)
    assert r["message"] == "--stripe-account-id is required"
    r = call_action(CONNECT_ACTIONS["stripe-list-application-fees"], conn,
                    ns(stripe_account_id="acct-m465-missing", limit=50))
    assert is_error(r)
    assert r["message"] == "Stripe account acct-m465-missing not found"
    assert _dump(conn, "stripe_application_fee", "stripe_id") == before


# ── stripe-list-refunds: created_at ordering ─────────────────────────────

def test_list_refunds_orders_by_created_at(conn):
    # No GL leg: read-only over stripe_refund. Same created_at-vs-stripe_id
    # divergence as the application-fee test above.
    env = build_stripe_env(conn)
    acct, co = env["stripe_account_id"], env["company_id"]
    seed_refund(conn, acct, co, stripe_id="re_m465_alpha",
                charge_stripe_id="ch_m465_1", amount="15.00")
    seed_refund(conn, acct, co, stripe_id="re_m465_beta",
                charge_stripe_id="ch_m465_2", amount="40.25")
    seed_refund(conn, acct, co, stripe_id="re_m465_gamma",
                charge_stripe_id="ch_m465_3", amount="5.75")
    _set(conn, "stripe_refund", "created_at", "2026-03-01 10:00:00",
         "stripe_id", "re_m465_alpha")
    _set(conn, "stripe_refund", "created_at", "2026-03-03 10:00:00",
         "stripe_id", "re_m465_beta")
    _set(conn, "stripe_refund", "created_at", "2026-03-02 10:00:00",
         "stripe_id", "re_m465_gamma")
    before = _dump(conn, "stripe_refund", "stripe_id")

    r = call_action(BROWSE_ACTIONS["stripe-list-refunds"], conn,
                    ns(stripe_account_id=acct, limit=50))

    assert is_ok(r), r
    assert r["count"] == 3
    assert [(x["stripe_id"], x["amount"]) for x in r["refunds"]] == [
        ("re_m465_beta", "40.25"),
        ("re_m465_gamma", "5.75"),
        ("re_m465_alpha", "15.00"),
    ]
    for entry in r["refunds"]:
        stored = _one(conn, "stripe_refund", "stripe_id", entry["stripe_id"])
        assert Decimal(entry["amount"]) == Decimal(stored["amount"])
        assert entry["amount"] == stored["amount"]

    assert _dump(conn, "stripe_refund", "stripe_id") == before

    r = call_action(BROWSE_ACTIONS["stripe-list-refunds"], conn,
                    ns(stripe_account_id=None, limit=50))
    assert is_error(r)
    assert r["message"] == "--stripe-account-id is required"
    r = call_action(BROWSE_ACTIONS["stripe-list-refunds"], conn,
                    ns(stripe_account_id="acct-m465-missing", limit=50))
    assert is_error(r)
    assert r["message"] == "Stripe account acct-m465-missing not found"
    assert _dump(conn, "stripe_refund", "stripe_id") == before
