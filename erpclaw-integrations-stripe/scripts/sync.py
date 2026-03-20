"""ERPClaw Integrations Stripe — sync engine actions.

8 actions for pulling data from the Stripe API into local mirror tables,
processing webhooks, and managing sync job lifecycle.

Imported by db_query.py (unified router).
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.response import ok, err, row_to_dict, rows_to_list
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import (
        Q, P, Table, Field, fn, Order,
        insert_row, update_row, dynamic_update,
    )
except ImportError:
    pass

# Add scripts directory to path for sibling imports
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_helpers import (
    SKILL, now_iso, cents_to_decimal, timestamp_to_iso,
    get_stripe_client, validate_stripe_account,
)

VALID_OBJECT_TYPES = (
    "balance_transaction", "charge", "refund", "dispute",
    "payout", "customer", "invoice", "subscription",
)

# Order matters for full sync — customers first so matching can work,
# then transactional objects in dependency order.
FULL_SYNC_ORDER = (
    "customer", "charge", "refund", "dispute",
    "payout", "balance_transaction", "invoice", "subscription",
)


# ---------------------------------------------------------------------------
# Internal: sync job lifecycle
# ---------------------------------------------------------------------------

def _create_sync_job(conn, stripe_account_id, company_id, object_type,
                     sync_type="incremental", sync_from=None, sync_to=None):
    """Create a stripe_sync_job record with status='running'."""
    job_id = str(uuid.uuid4())
    now = now_iso()
    sql, _ = insert_row("stripe_sync_job", {
        "id": P(), "stripe_account_id": P(), "sync_type": P(),
        "object_type": P(), "status": P(), "records_fetched": P(),
        "records_processed": P(), "records_failed": P(),
        "sync_from": P(), "sync_to": P(),
        "started_at": P(), "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        job_id, stripe_account_id, sync_type,
        object_type, "running", 0,
        0, 0,
        sync_from, sync_to,
        now, company_id, now,
    ))
    conn.commit()
    return job_id


def _complete_sync_job(conn, job_id, records_processed, records_failed=0):
    """Mark a sync job as completed with final counts."""
    now = now_iso()
    sql, params = dynamic_update("stripe_sync_job", {
        "status": "completed",
        "records_fetched": records_processed + records_failed,
        "records_processed": records_processed,
        "records_failed": records_failed,
        "completed_at": now,
    }, {"id": job_id})
    conn.execute(sql, params)
    conn.commit()


def _fail_sync_job(conn, job_id, error_message, records_processed=0):
    """Mark a sync job as failed with error details."""
    now = now_iso()
    sql, params = dynamic_update("stripe_sync_job", {
        "status": "failed",
        "records_processed": records_processed,
        "error_message": str(error_message)[:2000],
        "completed_at": now,
    }, {"id": job_id})
    conn.execute(sql, params)
    conn.commit()


# ---------------------------------------------------------------------------
# Internal: object-type-specific sync handlers
# ---------------------------------------------------------------------------

def _sync_balance_transactions(conn, stripe_client, acct_id, company_id, since=None):
    """Sync balance_transaction objects from Stripe into stripe_balance_transaction."""
    params = {"limit": 100}
    if since:
        params["created"] = {"gte": int(since)}

    count = 0
    for bt in stripe_client.BalanceTransaction.list(**params).auto_paging_iter():
        row_id = str(uuid.uuid4())
        conn.execute(
            """INSERT OR REPLACE INTO stripe_balance_transaction
                (id, stripe_id, stripe_account_id, type, reporting_category,
                 source_id, source_type, amount, fee, net, currency,
                 description, available_on, created_stripe, payout_id,
                 status, reconciled, company_id, created_at)
            VALUES (
                COALESCE((SELECT id FROM stripe_balance_transaction WHERE stripe_id=?), ?),
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, 0, ?, datetime('now'))""",
            (
                bt.id, row_id,
                bt.id, acct_id, bt.type, getattr(bt, "reporting_category", "") or "",
                getattr(bt, "source", None), bt.type,
                str(cents_to_decimal(bt.amount)),
                str(cents_to_decimal(bt.fee)),
                str(cents_to_decimal(bt.net)),
                bt.currency,
                getattr(bt, "description", None) or "",
                timestamp_to_iso(getattr(bt, "available_on", None)),
                timestamp_to_iso(bt.created),
                getattr(bt, "payout", None),
                bt.status,
                company_id,
            )
        )
        count += 1
    return count


def _sync_charges(conn, stripe_client, acct_id, company_id, since=None):
    """Sync charge objects from Stripe into stripe_charge."""
    params = {"limit": 100}
    if since:
        params["created"] = {"gte": int(since)}

    count = 0
    for ch in stripe_client.Charge.list(**params).auto_paging_iter():
        row_id = str(uuid.uuid4())
        # Extract payment method type
        pmt_types = getattr(ch, "payment_method_types", None)
        if isinstance(pmt_types, list) and pmt_types:
            pmt_type = pmt_types[0]
        else:
            pmt_type = getattr(ch, "payment_method_type", None) or ""

        conn.execute(
            """INSERT OR REPLACE INTO stripe_charge
                (id, stripe_id, stripe_account_id, amount, currency,
                 customer_stripe_id, description, payment_method_type,
                 payment_intent_id, invoice_stripe_id, status,
                 amount_refunded, disputed, failure_code,
                 metadata, company_id, created_stripe, created_at)
            VALUES (
                COALESCE((SELECT id FROM stripe_charge WHERE stripe_id=?), ?),
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, datetime('now'))""",
            (
                ch.id, row_id,
                ch.id, acct_id,
                str(cents_to_decimal(ch.amount)),
                ch.currency,
                getattr(ch, "customer", None) or "",
                getattr(ch, "description", None) or "",
                pmt_type,
                getattr(ch, "payment_intent", None) or "",
                getattr(ch, "invoice", None) or "",
                ch.status,
                str(cents_to_decimal(getattr(ch, "amount_refunded", 0))),
                1 if getattr(ch, "disputed", False) else 0,
                getattr(ch, "failure_code", None),
                json.dumps(dict(getattr(ch, "metadata", {}) or {})),
                company_id,
                timestamp_to_iso(ch.created),
            )
        )
        count += 1
    return count


def _sync_refunds(conn, stripe_client, acct_id, company_id, since=None):
    """Sync refund objects from Stripe into stripe_refund."""
    params = {"limit": 100}
    if since:
        params["created"] = {"gte": int(since)}

    count = 0
    for rf in stripe_client.Refund.list(**params).auto_paging_iter():
        row_id = str(uuid.uuid4())
        conn.execute(
            """INSERT OR REPLACE INTO stripe_refund
                (id, stripe_id, stripe_account_id, charge_stripe_id,
                 amount, currency, reason, status,
                 metadata, company_id, created_stripe, created_at)
            VALUES (
                COALESCE((SELECT id FROM stripe_refund WHERE stripe_id=?), ?),
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, datetime('now'))""",
            (
                rf.id, row_id,
                rf.id, acct_id,
                getattr(rf, "charge", None) or "",
                str(cents_to_decimal(rf.amount)),
                rf.currency,
                getattr(rf, "reason", None) or "",
                rf.status,
                json.dumps(dict(getattr(rf, "metadata", {}) or {})),
                company_id,
                timestamp_to_iso(rf.created),
            )
        )
        count += 1
    return count


def _sync_disputes(conn, stripe_client, acct_id, company_id, since=None):
    """Sync dispute objects from Stripe into stripe_dispute."""
    params = {"limit": 100}
    if since:
        params["created"] = {"gte": int(since)}

    count = 0
    for dp in stripe_client.Dispute.list(**params).auto_paging_iter():
        row_id = str(uuid.uuid4())
        conn.execute(
            """INSERT OR REPLACE INTO stripe_dispute
                (id, stripe_id, stripe_account_id, charge_stripe_id,
                 amount, currency, reason, status,
                 evidence_due_by, metadata, company_id,
                 created_stripe, created_at)
            VALUES (
                COALESCE((SELECT id FROM stripe_dispute WHERE stripe_id=?), ?),
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, datetime('now'))""",
            (
                dp.id, row_id,
                dp.id, acct_id,
                getattr(dp, "charge", None) or "",
                str(cents_to_decimal(dp.amount)),
                dp.currency,
                getattr(dp, "reason", None) or "",
                dp.status,
                timestamp_to_iso(getattr(dp, "evidence_due_by", None)),
                json.dumps(dict(getattr(dp, "metadata", {}) or {})),
                company_id,
                timestamp_to_iso(dp.created),
            )
        )
        count += 1
    return count


def _sync_payouts(conn, stripe_client, acct_id, company_id, since=None):
    """Sync payout objects from Stripe into stripe_payout."""
    params = {"limit": 100}
    if since:
        params["created"] = {"gte": int(since)}

    count = 0
    for po in stripe_client.Payout.list(**params).auto_paging_iter():
        row_id = str(uuid.uuid4())
        # Get bank last 4 digits from destination if available
        dest = getattr(po, "destination", None)
        last4 = ""
        if dest and hasattr(dest, "last4"):
            last4 = dest.last4

        conn.execute(
            """INSERT OR REPLACE INTO stripe_payout
                (id, stripe_id, stripe_account_id, amount, currency,
                 arrival_date, method, description, status,
                 failure_code, destination_bank_last4,
                 reconciled, company_id, created_stripe, created_at)
            VALUES (
                COALESCE((SELECT id FROM stripe_payout WHERE stripe_id=?), ?),
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                0, ?, ?, datetime('now'))""",
            (
                po.id, row_id,
                po.id, acct_id,
                str(cents_to_decimal(po.amount)),
                po.currency,
                timestamp_to_iso(getattr(po, "arrival_date", None)),
                getattr(po, "method", None) or "",
                getattr(po, "description", None) or "",
                po.status,
                getattr(po, "failure_code", None),
                last4,
                company_id,
                timestamp_to_iso(po.created),
            )
        )
        count += 1
    return count


def _sync_customers(conn, stripe_client, acct_id, company_id, since=None):
    """Sync customer objects from Stripe — auto-match by email to erpclaw customer.

    Creates/updates stripe_customer_map entries. Attempts to match each Stripe
    customer to an erpclaw customer by email address.
    """
    params = {"limit": 100}
    if since:
        params["created"] = {"gte": int(since)}

    cust_table = Table("customer")
    count = 0
    for cu in stripe_client.Customer.list(**params).auto_paging_iter():
        row_id = str(uuid.uuid4())
        stripe_email = getattr(cu, "email", None) or ""
        stripe_name = getattr(cu, "name", None) or ""

        # Attempt to match to erpclaw customer by name (customer table has no email column).
        # Name match has lower confidence than email would.
        erpclaw_customer_id = None
        match_method = "manual"
        match_confidence = "0.0"

        if stripe_name:
            match_row = conn.execute(
                Q.from_(cust_table).select(cust_table.id)
                .where(cust_table.name == P())
                .where(cust_table.company_id == P())
                .get_sql(),
                (stripe_name, company_id)
            ).fetchone()
            if match_row:
                erpclaw_customer_id = match_row["id"]
                match_method = "name"
                match_confidence = "0.8"

        conn.execute(
            """INSERT OR REPLACE INTO stripe_customer_map
                (id, stripe_account_id, stripe_customer_id,
                 erpclaw_customer_id, stripe_email, stripe_name,
                 match_method, match_confidence, company_id, created_at)
            VALUES (
                COALESCE(
                    (SELECT id FROM stripe_customer_map
                     WHERE stripe_account_id=? AND stripe_customer_id=?),
                    ?
                ),
                ?, ?,
                ?, ?, ?,
                ?, ?, ?, datetime('now'))""",
            (
                acct_id, cu.id, row_id,
                acct_id, cu.id,
                erpclaw_customer_id, stripe_email, stripe_name,
                match_method, match_confidence, company_id,
            )
        )
        count += 1
    return count


def _sync_invoices(conn, stripe_client, acct_id, company_id, since=None):
    """Sync invoice objects from Stripe into stripe_invoice."""
    params = {"limit": 100}
    if since:
        params["created"] = {"gte": int(since)}

    count = 0
    for inv in stripe_client.Invoice.list(**params).auto_paging_iter():
        row_id = str(uuid.uuid4())
        conn.execute(
            """INSERT OR REPLACE INTO stripe_invoice
                (id, stripe_id, stripe_account_id, customer_stripe_id,
                 number, amount_due, amount_paid, amount_remaining,
                 currency, status, subscription_stripe_id,
                 period_start, period_end, company_id,
                 created_stripe, created_at)
            VALUES (
                COALESCE((SELECT id FROM stripe_invoice WHERE stripe_id=?), ?),
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, datetime('now'))""",
            (
                inv.id, row_id,
                inv.id, acct_id,
                getattr(inv, "customer", None) or "",
                getattr(inv, "number", None) or "",
                str(cents_to_decimal(getattr(inv, "amount_due", 0))),
                str(cents_to_decimal(getattr(inv, "amount_paid", 0))),
                str(cents_to_decimal(getattr(inv, "amount_remaining", 0))),
                inv.currency,
                getattr(inv, "status", "draft"),
                getattr(inv, "subscription", None) or "",
                timestamp_to_iso(getattr(inv, "period_start", None)),
                timestamp_to_iso(getattr(inv, "period_end", None)),
                company_id,
                timestamp_to_iso(inv.created),
            )
        )
        count += 1
    return count


def _sync_subscriptions(conn, stripe_client, acct_id, company_id, since=None):
    """Sync subscription objects from Stripe into stripe_subscription."""
    params = {"limit": 100}
    if since:
        params["created"] = {"gte": int(since)}

    count = 0
    for sub in stripe_client.Subscription.list(**params).auto_paging_iter():
        row_id = str(uuid.uuid4())
        # Extract plan amount from items if available
        plan_amount = Decimal("0")
        plan_interval = ""
        items = getattr(sub, "items", None)
        if items and hasattr(items, "data") and items.data:
            first_item = items.data[0]
            price = getattr(first_item, "price", None) or getattr(first_item, "plan", None)
            if price:
                plan_amount = cents_to_decimal(getattr(price, "unit_amount", 0))
                plan_interval = getattr(price, "interval", "") or ""

        conn.execute(
            """INSERT OR REPLACE INTO stripe_subscription
                (id, stripe_id, stripe_account_id, customer_stripe_id,
                 status, current_period_start, current_period_end,
                 cancel_at_period_end, canceled_at,
                 plan_interval, plan_amount, currency,
                 company_id, created_stripe, created_at)
            VALUES (
                COALESCE((SELECT id FROM stripe_subscription WHERE stripe_id=?), ?),
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, datetime('now'))""",
            (
                sub.id, row_id,
                sub.id, acct_id,
                getattr(sub, "customer", None) or "",
                sub.status,
                timestamp_to_iso(getattr(sub, "current_period_start", None)),
                timestamp_to_iso(getattr(sub, "current_period_end", None)),
                1 if getattr(sub, "cancel_at_period_end", False) else 0,
                timestamp_to_iso(getattr(sub, "canceled_at", None)),
                plan_interval,
                str(plan_amount),
                getattr(sub, "currency", "usd"),
                company_id,
                timestamp_to_iso(sub.created),
            )
        )
        count += 1
    return count


# ---------------------------------------------------------------------------
# Handler dispatch table
# ---------------------------------------------------------------------------

_SYNC_HANDLERS = {
    "balance_transaction": _sync_balance_transactions,
    "charge": _sync_charges,
    "refund": _sync_refunds,
    "dispute": _sync_disputes,
    "payout": _sync_payouts,
    "customer": _sync_customers,
    "invoice": _sync_invoices,
    "subscription": _sync_subscriptions,
}


# ---------------------------------------------------------------------------
# Internal: generic sync orchestrator
# ---------------------------------------------------------------------------

def _sync_object_type(conn, stripe_account_id, company_id, object_type,
                      sync_type="incremental", sync_from=None, sync_to=None):
    """Generic paginated sync from Stripe API.

    1. Create stripe_sync_job record (status='running')
    2. Get stripe client via get_stripe_client()
    3. Call the object-type-specific handler
    4. Update sync_job with counts
    5. Set sync_job status='completed' or 'failed'

    Returns (sync_job_id, records_processed).
    """
    job_id = _create_sync_job(
        conn, stripe_account_id, company_id,
        object_type, sync_type, sync_from, sync_to,
    )

    stripe_client = get_stripe_client(conn, stripe_account_id)
    if not stripe_client:
        _fail_sync_job(conn, job_id, "Could not initialize Stripe client")
        return job_id, 0

    handler = _SYNC_HANDLERS.get(object_type)
    if not handler:
        _fail_sync_job(conn, job_id, f"Unknown object type: {object_type}")
        return job_id, 0

    # Convert ISO date to Unix timestamp for Stripe API filtering
    since_ts = None
    if sync_from:
        try:
            dt = datetime.fromisoformat(sync_from.replace("Z", "+00:00"))
            since_ts = int(dt.timestamp())
        except (ValueError, TypeError):
            pass

    try:
        count = handler(conn, stripe_client, stripe_account_id, company_id, since=since_ts)
        _complete_sync_job(conn, job_id, count)

        # Update last_sync_at on the stripe_account
        sql, params = dynamic_update("stripe_account", {
            "last_sync_at": now_iso(),
            "updated_at": now_iso(),
        }, {"id": stripe_account_id})
        conn.execute(sql, params)
        conn.commit()

        return job_id, count
    except Exception as e:
        _fail_sync_job(conn, job_id, str(e))
        return job_id, 0


# ---------------------------------------------------------------------------
# Webhook event dispatch table — maps Stripe event types to object sync types
# ---------------------------------------------------------------------------

_WEBHOOK_EVENT_MAP = {
    "charge.succeeded": "charge",
    "charge.failed": "charge",
    "charge.refunded": "charge",
    "charge.updated": "charge",
    "charge.dispute.created": "dispute",
    "charge.dispute.updated": "dispute",
    "charge.dispute.closed": "dispute",
    "refund.created": "refund",
    "refund.updated": "refund",
    "payout.created": "payout",
    "payout.paid": "payout",
    "payout.failed": "payout",
    "customer.created": "customer",
    "customer.updated": "customer",
    "customer.deleted": "customer",
    "invoice.created": "invoice",
    "invoice.paid": "invoice",
    "invoice.payment_failed": "invoice",
    "invoice.finalized": "invoice",
    "customer.subscription.created": "subscription",
    "customer.subscription.updated": "subscription",
    "customer.subscription.deleted": "subscription",
    "balance.available": "balance_transaction",
}


# ===========================================================================
# PUBLIC ACTIONS
# ===========================================================================


# ---------------------------------------------------------------------------
# 1. stripe-start-sync
# ---------------------------------------------------------------------------
def start_sync(conn, args):
    """Start a sync for a specific Stripe object type.

    Pulls data from the Stripe API into local mirror tables.
    Creates a sync_job to track progress.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    acct_row = validate_stripe_account(conn, stripe_account_id)
    company_id = acct_row["company_id"]

    object_type = getattr(args, "object_type", None)
    if not object_type:
        err("--object-type is required. Must be one of: " + ", ".join(VALID_OBJECT_TYPES))
    if object_type not in VALID_OBJECT_TYPES:
        err(f"Invalid object type: {object_type}. Must be one of: {', '.join(VALID_OBJECT_TYPES)}")

    sync_from = getattr(args, "sync_from", None)
    sync_to = getattr(args, "sync_to", None)
    sync_type = "incremental" if sync_from else "full"

    job_id, count = _sync_object_type(
        conn, stripe_account_id, company_id,
        object_type, sync_type, sync_from, sync_to,
    )

    audit(conn, SKILL, "stripe-start-sync", "stripe_sync_job", job_id,
          new_values={"object_type": object_type, "sync_type": sync_type})
    conn.commit()

    ok({
        "sync_job_id": job_id,
        "object_type": object_type,
        "sync_type": sync_type,
        "records_processed": count,
    })


# ---------------------------------------------------------------------------
# 2. stripe-start-full-sync
# ---------------------------------------------------------------------------
def start_full_sync(conn, args):
    """Start a full sync for ALL Stripe object types.

    Syncs in dependency order: customers, charges, refunds, disputes,
    payouts, balance_transactions, invoices, subscriptions.
    Creates one sync_job per object type.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    acct_row = validate_stripe_account(conn, stripe_account_id)
    company_id = acct_row["company_id"]

    sync_from = getattr(args, "sync_from", None)
    sync_to = getattr(args, "sync_to", None)

    results = []
    total_records = 0
    for obj_type in FULL_SYNC_ORDER:
        job_id, count = _sync_object_type(
            conn, stripe_account_id, company_id,
            obj_type, "full", sync_from, sync_to,
        )
        results.append({
            "object_type": obj_type,
            "sync_job_id": job_id,
            "records_processed": count,
        })
        total_records += count

    audit(conn, SKILL, "stripe-start-full-sync", "stripe_account", stripe_account_id,
          new_values={"total_records": total_records, "job_count": len(results)})
    conn.commit()

    ok({
        "stripe_account_id": stripe_account_id,
        "jobs": results,
        "total_records": total_records,
        "job_count": len(results),
    })


# ---------------------------------------------------------------------------
# 3. stripe-get-sync-status
# ---------------------------------------------------------------------------
def get_sync_status(conn, args):
    """Get details of a specific sync job."""
    sync_job_id = getattr(args, "sync_job_id", None)
    if not sync_job_id:
        err("--sync-job-id is required")

    t = Table("stripe_sync_job")
    row = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (sync_job_id,)
    ).fetchone()
    if not row:
        err(f"Sync job {sync_job_id} not found")

    data = row_to_dict(row)
    # Rename 'status' to 'sync_status' to avoid collision with ok() response status
    data["sync_status"] = data.pop("status", None)
    ok(data)


# ---------------------------------------------------------------------------
# 4. stripe-list-sync-jobs
# ---------------------------------------------------------------------------
def list_sync_jobs(conn, args):
    """List sync jobs for a Stripe account with optional filters."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")

    t = Table("stripe_sync_job")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)

    params = [stripe_account_id]

    status = getattr(args, "status", None)
    if status:
        q = q.where(t.status == P())
        params.append(status)

    object_type = getattr(args, "object_type", None)
    if object_type:
        q = q.where(t.object_type == P())
        params.append(object_type)

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.limit(limit).offset(offset)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({
        "sync_jobs": rows_to_list(rows),
        "count": len(rows),
    })


# ---------------------------------------------------------------------------
# 5. stripe-cancel-sync
# ---------------------------------------------------------------------------
def cancel_sync(conn, args):
    """Cancel a running or pending sync job."""
    sync_job_id = getattr(args, "sync_job_id", None)
    if not sync_job_id:
        err("--sync-job-id is required")

    t = Table("stripe_sync_job")
    row = conn.execute(
        Q.from_(t).select(t.id, t.status).where(t.id == P()).get_sql(),
        (sync_job_id,)
    ).fetchone()
    if not row:
        err(f"Sync job {sync_job_id} not found")

    if row["status"] in ("completed", "failed", "cancelled"):
        err(f"Cannot cancel sync job in '{row['status']}' state")

    now = now_iso()
    sql, params = dynamic_update("stripe_sync_job", {
        "status": "cancelled",
        "completed_at": now,
    }, {"id": sync_job_id})
    conn.execute(sql, params)

    audit(conn, SKILL, "stripe-cancel-sync", "stripe_sync_job", sync_job_id,
          new_values={"status": "cancelled"})
    conn.commit()

    ok({"sync_job_id": sync_job_id, "status": "cancelled"})


# ---------------------------------------------------------------------------
# 6. stripe-process-webhook
# ---------------------------------------------------------------------------
def process_webhook(conn, args):
    """Process an incoming Stripe webhook event.

    Stores the event in stripe_deep_webhook_event and dispatches to the
    appropriate sync handler based on event type.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    acct_row = validate_stripe_account(conn, stripe_account_id)
    company_id = acct_row["company_id"]

    event_data_raw = getattr(args, "event_data", None)
    if not event_data_raw:
        err("--event-data is required (JSON string of the Stripe event)")

    try:
        event = json.loads(event_data_raw) if isinstance(event_data_raw, str) else event_data_raw
    except json.JSONDecodeError:
        err("--event-data must be valid JSON")

    stripe_event_id = event.get("id", "")
    event_type = event.get("type", "")
    api_version = event.get("api_version", "")

    if not stripe_event_id or not event_type:
        err("Event must contain 'id' and 'type' fields")

    # Extract object info from event data
    obj_data = event.get("data", {}).get("object", {})
    object_id = obj_data.get("id", "")
    object_type = obj_data.get("object", "")

    # Check for idempotency — skip if already processed
    wh_table = Table("stripe_deep_webhook_event")
    existing = conn.execute(
        Q.from_(wh_table).select(wh_table.id, wh_table.processed)
        .where(wh_table.stripe_event_id == P()).get_sql(),
        (stripe_event_id,)
    ).fetchone()

    if existing and existing["processed"] == 1:
        ok({
            "webhook_event_id": existing["id"],
            "stripe_event_id": stripe_event_id,
            "status": "already_processed",
        })

    # Store the event
    event_row_id = existing["id"] if existing else str(uuid.uuid4())
    if not existing:
        sql, _ = insert_row("stripe_deep_webhook_event", {
            "id": P(), "stripe_account_id": P(), "stripe_event_id": P(),
            "event_type": P(), "api_version": P(), "object_id": P(),
            "object_type": P(), "payload": P(), "processed": P(),
            "process_attempts": P(), "created_stripe": P(), "created_at": P(),
        })
        conn.execute(sql, (
            event_row_id, stripe_account_id, stripe_event_id,
            event_type, api_version, object_id,
            object_type, json.dumps(event), 0,
            0, timestamp_to_iso(event.get("created")), now_iso(),
        ))
        conn.commit()

    # Dispatch to appropriate handler
    sync_object_type = _WEBHOOK_EVENT_MAP.get(event_type)
    processed = 0
    error_msg = None

    if sync_object_type:
        try:
            # Update process attempts
            conn.execute(
                """UPDATE stripe_deep_webhook_event
                   SET process_attempts = process_attempts + 1
                   WHERE id = ?""",
                (event_row_id,)
            )
            conn.commit()

            # Process the event by syncing the affected object type
            job_id, count = _sync_object_type(
                conn, stripe_account_id, company_id,
                sync_object_type, "webhook",
            )
            processed = 1
        except Exception as e:
            error_msg = str(e)
    else:
        # Unknown event type — store but don't process
        processed = 1  # Mark as processed (nothing to do)
        error_msg = None

    # Update webhook event status
    now = now_iso()
    sql, params = dynamic_update("stripe_deep_webhook_event", {
        "processed": processed,
        "processed_at": now if processed else None,
        "error_message": error_msg,
    }, {"id": event_row_id})
    conn.execute(sql, params)
    conn.commit()

    audit(conn, SKILL, "stripe-process-webhook", "stripe_deep_webhook_event", event_row_id,
          new_values={"event_type": event_type, "processed": processed})
    conn.commit()

    ok({
        "webhook_event_id": event_row_id,
        "stripe_event_id": stripe_event_id,
        "event_type": event_type,
        "processed": bool(processed),
        "sync_object_type": sync_object_type,
        "error": error_msg,
    })


# ---------------------------------------------------------------------------
# 7. stripe-replay-webhook
# ---------------------------------------------------------------------------
def replay_webhook(conn, args):
    """Re-process a previously stored webhook event."""
    webhook_event_id = getattr(args, "webhook_event_id", None)
    if not webhook_event_id:
        err("--webhook-event-id is required")

    wh_table = Table("stripe_deep_webhook_event")
    row = conn.execute(
        Q.from_(wh_table).select("*").where(wh_table.id == P()).get_sql(),
        (webhook_event_id,)
    ).fetchone()
    if not row:
        err(f"Webhook event {webhook_event_id} not found")

    # Check max attempts
    if row["process_attempts"] >= row["max_attempts"]:
        err(f"Webhook event has reached max attempts ({row['max_attempts']})")

    # Get stripe account info
    acct_row = validate_stripe_account(conn, row["stripe_account_id"])
    company_id = acct_row["company_id"]

    event_type = row["event_type"]
    sync_object_type = _WEBHOOK_EVENT_MAP.get(event_type)

    # Update attempt count
    conn.execute(
        """UPDATE stripe_deep_webhook_event
           SET process_attempts = process_attempts + 1
           WHERE id = ?""",
        (webhook_event_id,)
    )
    conn.commit()

    processed = 0
    error_msg = None
    records = 0

    if sync_object_type:
        try:
            job_id, count = _sync_object_type(
                conn, row["stripe_account_id"], company_id,
                sync_object_type, "webhook",
            )
            processed = 1
            records = count
        except Exception as e:
            error_msg = str(e)
    else:
        processed = 1

    now = now_iso()
    sql, params = dynamic_update("stripe_deep_webhook_event", {
        "processed": processed,
        "processed_at": now if processed else None,
        "error_message": error_msg,
    }, {"id": webhook_event_id})
    conn.execute(sql, params)

    audit(conn, SKILL, "stripe-replay-webhook", "stripe_deep_webhook_event", webhook_event_id,
          new_values={"processed": processed, "attempt": row["process_attempts"] + 1})
    conn.commit()

    ok({
        "webhook_event_id": webhook_event_id,
        "event_type": event_type,
        "processed": bool(processed),
        "records_processed": records,
        "error": error_msg,
    })


# ---------------------------------------------------------------------------
# 8. stripe-list-webhook-events
# ---------------------------------------------------------------------------
def list_webhook_events(conn, args):
    """List webhook events for a Stripe account with optional filters."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    if not stripe_account_id:
        err("--stripe-account-id is required")

    t = Table("stripe_deep_webhook_event")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)

    params = [stripe_account_id]

    event_type = getattr(args, "event_type", None)
    if event_type:
        q = q.where(t.event_type == P())
        params.append(event_type)

    processed = getattr(args, "processed", None)
    if processed is not None:
        q = q.where(t.processed == P())
        params.append(int(processed))

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.limit(limit).offset(offset)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({
        "webhook_events": rows_to_list(rows),
        "count": len(rows),
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "stripe-start-sync": start_sync,
    "stripe-start-full-sync": start_full_sync,
    "stripe-get-sync-status": get_sync_status,
    "stripe-list-sync-jobs": list_sync_jobs,
    "stripe-cancel-sync": cancel_sync,
    "stripe-process-webhook": process_webhook,
    "stripe-replay-webhook": replay_webhook,
    "stripe-list-webhook-events": list_webhook_events,
}
