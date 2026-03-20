"""ERPClaw Integrations Stripe — reconciliation engine actions.

8 actions for 3-layer reconciliation between Stripe and ERPClaw:
  stripe-run-reconciliation, stripe-reconcile-payout, stripe-match-charge,
  stripe-unmatch-charge, stripe-list-unreconciled, stripe-get-reconciliation-run,
  stripe-list-reconciliation-runs, stripe-reconciliation-summary

Reconciliation layers:
  L1: Balance Transaction -> Source matching (charge, refund, payout FK linkage)
  L2: Charge -> ERP customer/invoice matching (via customer_map)
  L3: Payout -> Constituent transaction verification (sum check)

Imported by db_query.py (unified router).
"""
import os
import sys
import uuid
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
    SKILL, now_iso,
    validate_stripe_account,
)


# ---------------------------------------------------------------------------
# Internal reconciliation helpers
# ---------------------------------------------------------------------------

def _layer1_source_matching(conn, stripe_account_id, date_from=None, date_to=None):
    """Layer 1: Match balance transactions to their source objects.

    Every balance_transaction has a source_id that links to a charge (ch_*),
    refund (re_*), payout (po_*), etc. This uses Stripe's own FK linkage
    for near-100% match rate.

    Returns (matched_count, total_count).
    """
    bt_table = Table("stripe_balance_transaction")

    # Build query for unreconciled balance transactions
    q = Q.from_(bt_table).select("*").where(
        bt_table.stripe_account_id == P()
    ).where(
        bt_table.reconciled == 0
    )
    params = [stripe_account_id]

    if date_from:
        q = q.where(bt_table.created_stripe >= P())
        params.append(date_from)
    if date_to:
        q = q.where(bt_table.created_stripe <= P())
        params.append(date_to)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()

    matched = 0
    charge_table = Table("stripe_charge")
    refund_table = Table("stripe_refund")
    payout_table = Table("stripe_payout")

    for bt in rows:
        source_id = bt["source_id"]
        if not source_id:
            continue

        # Determine source type from prefix
        source_found = False
        if source_id.startswith("ch_"):
            row = conn.execute(
                Q.from_(charge_table).select(charge_table.id)
                .where(charge_table.stripe_id == P())
                .where(charge_table.stripe_account_id == P())
                .get_sql(),
                (source_id, stripe_account_id)
            ).fetchone()
            source_found = row is not None
        elif source_id.startswith("re_"):
            row = conn.execute(
                Q.from_(refund_table).select(refund_table.id)
                .where(refund_table.stripe_id == P())
                .where(refund_table.stripe_account_id == P())
                .get_sql(),
                (source_id, stripe_account_id)
            ).fetchone()
            source_found = row is not None
        elif source_id.startswith("po_"):
            row = conn.execute(
                Q.from_(payout_table).select(payout_table.id)
                .where(payout_table.stripe_id == P())
                .where(payout_table.stripe_account_id == P())
                .get_sql(),
                (source_id, stripe_account_id)
            ).fetchone()
            source_found = row is not None
        else:
            # For other types (disputes, etc.), mark as matched by source_id presence
            source_found = True

        if source_found:
            now = now_iso()
            sql, update_params = dynamic_update("stripe_balance_transaction", {
                "reconciled": 1,
                "reconciled_at": now,
            }, {"id": bt["id"]})
            conn.execute(sql, update_params)
            matched += 1

    return matched, len(rows)


def _layer2_charge_matching(conn, stripe_account_id, date_from=None, date_to=None):
    """Layer 2: Match charges to ERPClaw customers via customer_map.

    For each charge with a customer_stripe_id, look up the stripe_customer_map
    to find the erpclaw_customer_id. Update stripe_charge.erpclaw_customer_id.

    Returns (matched_count, total_count).
    """
    charge_table = Table("stripe_charge")
    map_table = Table("stripe_customer_map")

    # Get charges that have a stripe customer but no erpclaw customer yet
    q = Q.from_(charge_table).select("*").where(
        charge_table.stripe_account_id == P()
    ).where(
        charge_table.erpclaw_customer_id.isnull()
    ).where(
        charge_table.customer_stripe_id.isnotnull()
    ).where(
        charge_table.customer_stripe_id != P()
    )
    params = [stripe_account_id, ""]

    if date_from:
        q = q.where(charge_table.created_stripe >= P())
        params.append(date_from)
    if date_to:
        q = q.where(charge_table.created_stripe <= P())
        params.append(date_to)

    charges = conn.execute(q.get_sql(), tuple(params)).fetchall()

    matched = 0
    for charge in charges:
        cust_stripe_id = charge["customer_stripe_id"]

        # Look up customer mapping
        mapping = conn.execute(
            Q.from_(map_table).select(map_table.erpclaw_customer_id)
            .where(map_table.stripe_account_id == P())
            .where(map_table.stripe_customer_id == P())
            .where(map_table.erpclaw_customer_id.isnotnull())
            .get_sql(),
            (stripe_account_id, cust_stripe_id)
        ).fetchone()

        if mapping and mapping["erpclaw_customer_id"]:
            sql, update_params = dynamic_update("stripe_charge", {
                "erpclaw_customer_id": mapping["erpclaw_customer_id"],
            }, {"id": charge["id"]})
            conn.execute(sql, update_params)
            matched += 1

    return matched, len(charges)


def _layer3_payout_verification(conn, stripe_account_id, date_from=None, date_to=None):
    """Layer 3: Verify payout amounts match constituent balance transactions.

    For each payout, SUM(stripe_balance_transaction.net WHERE payout_id = payout.stripe_id)
    should equal the payout amount. Mark reconciled if matched.

    Returns (matched_count, mismatched_count, total_count).
    """
    payout_table = Table("stripe_payout")

    q = Q.from_(payout_table).select("*").where(
        payout_table.stripe_account_id == P()
    ).where(
        payout_table.reconciled == 0
    )
    params = [stripe_account_id]

    if date_from:
        q = q.where(payout_table.created_stripe >= P())
        params.append(date_from)
    if date_to:
        q = q.where(payout_table.created_stripe <= P())
        params.append(date_to)

    payouts = conn.execute(q.get_sql(), tuple(params)).fetchall()

    matched = 0
    mismatched = 0

    for payout in payouts:
        payout_stripe_id = payout["stripe_id"]
        payout_amount = Decimal(payout["amount"])

        # Sum constituent balance transactions
        sum_row = conn.execute(
            "SELECT decimal_sum(net) as total_net FROM stripe_balance_transaction "
            "WHERE payout_id = ? AND stripe_account_id = ?",
            (payout_stripe_id, stripe_account_id)
        ).fetchone()

        total_net = Decimal(sum_row["total_net"]) if sum_row and sum_row["total_net"] else Decimal("0")

        if total_net == payout_amount:
            now = now_iso()
            sql, update_params = dynamic_update("stripe_payout", {
                "reconciled": 1,
                "transaction_count": conn.execute(
                    "SELECT COUNT(*) as cnt FROM stripe_balance_transaction "
                    "WHERE payout_id = ? AND stripe_account_id = ?",
                    (payout_stripe_id, stripe_account_id)
                ).fetchone()["cnt"],
            }, {"id": payout["id"]})
            conn.execute(sql, update_params)
            matched += 1
        else:
            mismatched += 1

    return matched, mismatched, len(payouts)


# ===========================================================================
# PUBLIC ACTIONS
# ===========================================================================


# ---------------------------------------------------------------------------
# 1. stripe-run-reconciliation
# ---------------------------------------------------------------------------
def run_reconciliation(conn, args):
    """Run all 3 reconciliation layers for a date range.

    Creates a stripe_reconciliation_run record tracking the results.
    Layer 1: Balance Transaction -> Source matching
    Layer 2: Charge -> Customer matching (via customer_map)
    Layer 3: Payout -> Constituent verification
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    acct_row = validate_stripe_account(conn, stripe_account_id)
    company_id = acct_row["company_id"]

    date_from = getattr(args, "date_from", None)
    date_to = getattr(args, "date_to", None)

    now = now_iso()
    run_id = str(uuid.uuid4())

    # Create reconciliation run record
    sql, _ = insert_row("stripe_reconciliation_run", {
        "id": P(), "stripe_account_id": P(), "run_date": P(),
        "period_start": P(), "period_end": P(),
        "status": P(), "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        run_id, stripe_account_id, now,
        date_from or "1970-01-01", date_to or "2099-12-31",
        "running", company_id, now,
    ))
    conn.commit()

    # Run Layer 1
    l1_matched, l1_total = _layer1_source_matching(
        conn, stripe_account_id, date_from, date_to)

    # Run Layer 2
    l2_matched, l2_total = _layer2_charge_matching(
        conn, stripe_account_id, date_from, date_to)

    # Run Layer 3
    l3_matched, l3_mismatched, l3_total = _layer3_payout_verification(
        conn, stripe_account_id, date_from, date_to)

    total_processed = l1_total + l2_total + l3_total
    total_matched = l1_matched + l2_matched + l3_matched
    total_unmatched = total_processed - total_matched

    # Calculate reconciled amount (sum of reconciled balance transactions)
    amount_row = conn.execute(
        "SELECT decimal_sum(amount) as total FROM stripe_balance_transaction "
        "WHERE stripe_account_id = ? AND reconciled = 1",
        (stripe_account_id,)
    ).fetchone()
    amount_reconciled = amount_row["total"] if amount_row and amount_row["total"] else "0"

    unreconciled_row = conn.execute(
        "SELECT decimal_sum(amount) as total FROM stripe_balance_transaction "
        "WHERE stripe_account_id = ? AND reconciled = 0",
        (stripe_account_id,)
    ).fetchone()
    amount_unreconciled = unreconciled_row["total"] if unreconciled_row and unreconciled_row["total"] else "0"

    # Update run record
    sql, params = dynamic_update("stripe_reconciliation_run", {
        "transactions_processed": total_processed,
        "transactions_matched": total_matched,
        "transactions_unmatched": total_unmatched,
        "amount_reconciled": amount_reconciled,
        "amount_unreconciled": amount_unreconciled,
        "status": "completed",
    }, {"id": run_id})
    conn.execute(sql, params)
    conn.commit()

    audit(conn, SKILL, "stripe-run-reconciliation", "stripe_reconciliation_run", run_id,
          new_values={
              "total_processed": total_processed,
              "total_matched": total_matched,
          })
    conn.commit()

    ok({
        "reconciliation_run_id": run_id,
        "stripe_account_id": stripe_account_id,
        "status": "completed",
        "layer1_source_matching": {
            "total": l1_total,
            "matched": l1_matched,
        },
        "layer2_charge_matching": {
            "total": l2_total,
            "matched": l2_matched,
        },
        "layer3_payout_verification": {
            "total": l3_total,
            "matched": l3_matched,
            "mismatched": l3_mismatched,
        },
        "totals": {
            "processed": total_processed,
            "matched": total_matched,
            "unmatched": total_unmatched,
            "amount_reconciled": amount_reconciled,
            "amount_unreconciled": amount_unreconciled,
        },
    })


# ---------------------------------------------------------------------------
# 2. stripe-reconcile-payout
# ---------------------------------------------------------------------------
def reconcile_payout(conn, args):
    """Reconcile a specific payout by verifying constituent transactions.

    Checks that SUM(balance_transaction.net WHERE payout_id = payout.stripe_id)
    equals the payout amount.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    validate_stripe_account(conn, stripe_account_id)

    payout_stripe_id = getattr(args, "payout_stripe_id", None)
    if not payout_stripe_id:
        err("--payout-stripe-id is required")

    payout_table = Table("stripe_payout")
    payout = conn.execute(
        Q.from_(payout_table).select("*")
        .where(payout_table.stripe_account_id == P())
        .where(payout_table.stripe_id == P())
        .get_sql(),
        (stripe_account_id, payout_stripe_id)
    ).fetchone()
    if not payout:
        err(f"Payout {payout_stripe_id} not found")

    payout_amount = Decimal(payout["amount"])

    # Get constituent balance transactions
    bt_rows = conn.execute(
        "SELECT stripe_id, amount, fee, net, type, source_id "
        "FROM stripe_balance_transaction "
        "WHERE payout_id = ? AND stripe_account_id = ?",
        (payout_stripe_id, stripe_account_id)
    ).fetchall()

    sum_row = conn.execute(
        "SELECT decimal_sum(net) as total_net FROM stripe_balance_transaction "
        "WHERE payout_id = ? AND stripe_account_id = ?",
        (payout_stripe_id, stripe_account_id)
    ).fetchone()

    total_net = Decimal(sum_row["total_net"]) if sum_row and sum_row["total_net"] else Decimal("0")
    balanced = total_net == payout_amount

    if balanced:
        now = now_iso()
        sql, params = dynamic_update("stripe_payout", {
            "reconciled": 1,
            "transaction_count": len(bt_rows),
        }, {"id": payout["id"]})
        conn.execute(sql, params)
        conn.commit()

    transactions = []
    for bt in bt_rows:
        transactions.append({
            "stripe_id": bt["stripe_id"],
            "type": bt["type"],
            "amount": bt["amount"],
            "fee": bt["fee"],
            "net": bt["net"],
            "source_id": bt["source_id"],
        })

    ok({
        "payout_stripe_id": payout_stripe_id,
        "payout_amount": str(payout_amount),
        "constituent_net_total": str(total_net),
        "balanced": balanced,
        "difference": str(payout_amount - total_net),
        "transaction_count": len(bt_rows),
        "transactions": transactions,
        "reconciled": balanced,
    })


# ---------------------------------------------------------------------------
# 3. stripe-match-charge
# ---------------------------------------------------------------------------
def match_charge(conn, args):
    """Manually match a Stripe charge to an ERPClaw invoice."""
    charge_stripe_id = getattr(args, "charge_stripe_id", None)
    if not charge_stripe_id:
        err("--charge-stripe-id is required")

    erpclaw_invoice_id = getattr(args, "erpclaw_invoice_id", None)
    if not erpclaw_invoice_id:
        err("--erpclaw-invoice-id is required")

    charge_table = Table("stripe_charge")
    charge = conn.execute(
        Q.from_(charge_table).select(charge_table.id, charge_table.stripe_id)
        .where(charge_table.stripe_id == P())
        .get_sql(),
        (charge_stripe_id,)
    ).fetchone()
    if not charge:
        err(f"Charge {charge_stripe_id} not found")

    # Validate invoice exists
    inv_table = Table("sales_invoice")
    inv = conn.execute(
        Q.from_(inv_table).select(inv_table.id)
        .where(inv_table.id == P())
        .get_sql(),
        (erpclaw_invoice_id,)
    ).fetchone()
    if not inv:
        err(f"ERPClaw invoice {erpclaw_invoice_id} not found")

    sql, params = dynamic_update("stripe_charge", {
        "erpclaw_invoice_id": erpclaw_invoice_id,
    }, {"id": charge["id"]})
    conn.execute(sql, params)

    audit(conn, SKILL, "stripe-match-charge", "stripe_charge", charge["id"],
          new_values={"erpclaw_invoice_id": erpclaw_invoice_id})
    conn.commit()

    ok({
        "charge_stripe_id": charge_stripe_id,
        "erpclaw_invoice_id": erpclaw_invoice_id,
        "status": "matched",
    })


# ---------------------------------------------------------------------------
# 4. stripe-unmatch-charge
# ---------------------------------------------------------------------------
def unmatch_charge(conn, args):
    """Clear the invoice match on a Stripe charge."""
    charge_stripe_id = getattr(args, "charge_stripe_id", None)
    if not charge_stripe_id:
        err("--charge-stripe-id is required")

    charge_table = Table("stripe_charge")
    charge = conn.execute(
        Q.from_(charge_table).select(charge_table.id, charge_table.stripe_id, charge_table.erpclaw_invoice_id)
        .where(charge_table.stripe_id == P())
        .get_sql(),
        (charge_stripe_id,)
    ).fetchone()
    if not charge:
        err(f"Charge {charge_stripe_id} not found")

    sql, params = dynamic_update("stripe_charge", {
        "erpclaw_invoice_id": None,
        "erpclaw_customer_id": None,
    }, {"id": charge["id"]})
    conn.execute(sql, params)

    audit(conn, SKILL, "stripe-unmatch-charge", "stripe_charge", charge["id"],
          new_values={"erpclaw_invoice_id": None})
    conn.commit()

    ok({
        "charge_stripe_id": charge_stripe_id,
        "erpclaw_invoice_id": None,
        "status": "unmatched",
    })


# ---------------------------------------------------------------------------
# 5. stripe-list-unreconciled
# ---------------------------------------------------------------------------
def list_unreconciled(conn, args):
    """List unreconciled balance transactions for a Stripe account."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_balance_transaction")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).where(
        t.reconciled == 0
    ).orderby(t.created_stripe, order=Order.desc)

    params = [stripe_account_id]

    txn_type = getattr(args, "type", None)
    if txn_type:
        q = q.where(t.type == P())
        params.append(txn_type)

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.limit(limit).offset(offset)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({
        "unreconciled": rows_to_list(rows),
        "count": len(rows),
    })


# ---------------------------------------------------------------------------
# 6. stripe-get-reconciliation-run
# ---------------------------------------------------------------------------
def get_reconciliation_run(conn, args):
    """Get details of a specific reconciliation run."""
    reconciliation_run_id = getattr(args, "reconciliation_run_id", None)
    if not reconciliation_run_id:
        err("--reconciliation-run-id is required")

    t = Table("stripe_reconciliation_run")
    row = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (reconciliation_run_id,)
    ).fetchone()
    if not row:
        err(f"Reconciliation run {reconciliation_run_id} not found")

    ok(row_to_dict(row))


# ---------------------------------------------------------------------------
# 7. stripe-list-reconciliation-runs
# ---------------------------------------------------------------------------
def list_reconciliation_runs(conn, args):
    """List reconciliation runs for a Stripe account."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_reconciliation_run")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)

    params = [stripe_account_id]

    status = getattr(args, "status", None)
    if status:
        q = q.where(t.status == P())
        params.append(status)

    limit = getattr(args, "limit", 50) or 50
    q = q.limit(limit)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({
        "reconciliation_runs": rows_to_list(rows),
        "count": len(rows),
    })


# ---------------------------------------------------------------------------
# 8. stripe-reconciliation-summary
# ---------------------------------------------------------------------------
def reconciliation_summary(conn, args):
    """Aggregate summary of reconciliation status for a Stripe account.

    Returns counts and amounts for matched/unmatched/partial balance transactions,
    charges, and payouts.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    validate_stripe_account(conn, stripe_account_id)

    # Balance transaction summary
    bt_matched = conn.execute(
        "SELECT COUNT(*) as cnt, decimal_sum(amount) as total "
        "FROM stripe_balance_transaction "
        "WHERE stripe_account_id = ? AND reconciled = 1",
        (stripe_account_id,)
    ).fetchone()

    bt_unmatched = conn.execute(
        "SELECT COUNT(*) as cnt, decimal_sum(amount) as total "
        "FROM stripe_balance_transaction "
        "WHERE stripe_account_id = ? AND reconciled = 0",
        (stripe_account_id,)
    ).fetchone()

    # Charge matching summary
    charges_matched = conn.execute(
        "SELECT COUNT(*) as cnt, decimal_sum(amount) as total "
        "FROM stripe_charge "
        "WHERE stripe_account_id = ? AND erpclaw_customer_id IS NOT NULL",
        (stripe_account_id,)
    ).fetchone()

    charges_unmatched = conn.execute(
        "SELECT COUNT(*) as cnt, decimal_sum(amount) as total "
        "FROM stripe_charge "
        "WHERE stripe_account_id = ? AND (erpclaw_customer_id IS NULL OR erpclaw_customer_id = '')",
        (stripe_account_id,)
    ).fetchone()

    # Payout summary
    payouts_reconciled = conn.execute(
        "SELECT COUNT(*) as cnt, decimal_sum(amount) as total "
        "FROM stripe_payout "
        "WHERE stripe_account_id = ? AND reconciled = 1",
        (stripe_account_id,)
    ).fetchone()

    payouts_unreconciled = conn.execute(
        "SELECT COUNT(*) as cnt, decimal_sum(amount) as total "
        "FROM stripe_payout "
        "WHERE stripe_account_id = ? AND reconciled = 0",
        (stripe_account_id,)
    ).fetchone()

    def _safe(row, field):
        return row[field] if row and row[field] else "0"

    ok({
        "stripe_account_id": stripe_account_id,
        "balance_transactions": {
            "matched_count": bt_matched["cnt"] if bt_matched else 0,
            "matched_amount": _safe(bt_matched, "total"),
            "unmatched_count": bt_unmatched["cnt"] if bt_unmatched else 0,
            "unmatched_amount": _safe(bt_unmatched, "total"),
        },
        "charges": {
            "matched_count": charges_matched["cnt"] if charges_matched else 0,
            "matched_amount": _safe(charges_matched, "total"),
            "unmatched_count": charges_unmatched["cnt"] if charges_unmatched else 0,
            "unmatched_amount": _safe(charges_unmatched, "total"),
        },
        "payouts": {
            "reconciled_count": payouts_reconciled["cnt"] if payouts_reconciled else 0,
            "reconciled_amount": _safe(payouts_reconciled, "total"),
            "unreconciled_count": payouts_unreconciled["cnt"] if payouts_unreconciled else 0,
            "unreconciled_amount": _safe(payouts_unreconciled, "total"),
        },
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "stripe-run-reconciliation": run_reconciliation,
    "stripe-reconcile-payout": reconcile_payout,
    "stripe-match-charge": match_charge,
    "stripe-unmatch-charge": unmatch_charge,
    "stripe-list-unreconciled": list_unreconciled,
    "stripe-get-reconciliation-run": get_reconciliation_run,
    "stripe-list-reconciliation-runs": list_reconciliation_runs,
    "stripe-reconciliation-summary": reconciliation_summary,
}
