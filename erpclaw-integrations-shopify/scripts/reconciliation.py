"""ERPClaw Integrations Shopify -- reconciliation actions.

6 actions for reconciling Shopify payouts against GL entries and bank
statements. Three-layer verification:
  Layer 1: Payout transaction sums match payout net_amount
  Layer 2: Every order appears in at least one payout
  Layer 3: Clearing account balance matches expected
Imported by db_query.py (unified router).
"""
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.decimal_utils import to_decimal, round_currency, amounts_equal
    from erpclaw_lib.response import ok, err, row_to_dict, rows_to_list
    from erpclaw_lib.audit import audit
    from erpclaw_lib.gl_posting import get_account_balance
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

from shopify_helpers import (
    SKILL, now_iso, shopify_amount_to_decimal, validate_shopify_account,
)


# ---------------------------------------------------------------------------
# 1. shopify-run-reconciliation
# ---------------------------------------------------------------------------
def run_reconciliation(conn, args):
    """Run a full reconciliation for a Shopify account.

    Three-layer verification:
      Layer 1: For each payout, verify SUM(payout_transaction.net) == payout.net_amount
      Layer 2: For each order, verify it appears in at least one payout's transactions
      Layer 3: Verify Shopify Clearing GL balance matches expected
    Creates a shopify_reconciliation_run record.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)
    company_id = acct_row["company_id"]

    period_start = getattr(args, "period_start", None) or "2000-01-01"
    period_end = getattr(args, "period_end", None) or "2099-12-31"

    run_id = str(uuid.uuid4())
    now = now_iso()

    # -- Layer 1: Payout transaction sums --
    payouts = conn.execute(
        """SELECT id, shopify_payout_id, net_amount, gross_amount, fee_amount
           FROM shopify_payout
           WHERE shopify_account_id = ?
             AND issued_at >= ? AND issued_at <= ?""",
        (shopify_account_id, period_start, period_end)
    ).fetchall()

    total_payouts = len(payouts)
    payouts_matched = 0
    payouts_unmatched = 0
    payout_discrepancies = []

    for p in payouts:
        expected_net = shopify_amount_to_decimal(p["net_amount"])
        # Sum transactions for this payout
        txn_row = conn.execute(
            """SELECT COALESCE(SUM(CAST(net_amount AS REAL)), 0) as txn_sum
               FROM shopify_payout_transaction
               WHERE shopify_payout_id_local = ?""",
            (p["id"],)
        ).fetchone()
        txn_sum = to_decimal(str(txn_row["txn_sum"])) if txn_row else Decimal("0")

        # If no transactions exist, consider the payout self-consistent
        has_txns = conn.execute(
            "SELECT COUNT(*) as cnt FROM shopify_payout_transaction WHERE shopify_payout_id_local = ?",
            (p["id"],)
        ).fetchone()

        if has_txns["cnt"] == 0:
            # No transactions to reconcile -- mark as matched (self-consistent)
            payouts_matched += 1
        elif amounts_equal(txn_sum, expected_net):
            payouts_matched += 1
        else:
            payouts_unmatched += 1
            payout_discrepancies.append({
                "payout_id": p["id"],
                "expected": str(expected_net),
                "actual": str(txn_sum),
                "diff": str(expected_net - txn_sum),
            })

    # -- Layer 2: Order coverage in payouts --
    orders = conn.execute(
        """SELECT id, shopify_order_id, total_amount, gl_status
           FROM shopify_order
           WHERE shopify_account_id = ?
             AND order_date >= ? AND order_date <= ?""",
        (shopify_account_id, period_start, period_end)
    ).fetchall()

    total_orders = len(orders)
    orders_matched = 0
    orders_unmatched = 0

    for o in orders:
        # Check if this order appears in any payout transaction
        txn = conn.execute(
            """SELECT COUNT(*) as cnt FROM shopify_payout_transaction
               WHERE source_order_id = ?""",
            (o["id"],)
        ).fetchone()
        if txn["cnt"] > 0:
            orders_matched += 1
        else:
            # Orders with posted GL but no payout yet are expected
            orders_unmatched += 1

    # -- Layer 3: Clearing account balance --
    clearing_account_id = acct_row["clearing_account_id"]
    clearing_balance = Decimal("0")
    if clearing_account_id:
        bal = get_account_balance(conn, clearing_account_id)
        clearing_balance = to_decimal(bal["balance"])

    # Expected clearing balance = sum of posted order totals - sum of posted
    # payout gross amounts - sum of posted refunds
    posted_orders_sum = Decimal("0")
    for o in orders:
        if o["gl_status"] == "posted":
            posted_orders_sum += shopify_amount_to_decimal(o["total_amount"])

    posted_payouts_sum = Decimal("0")
    for p in payouts:
        pg = conn.execute(
            "SELECT gl_status, gross_amount FROM shopify_payout WHERE id = ?",
            (p["id"],)
        ).fetchone()
        if pg and pg["gl_status"] == "posted":
            posted_payouts_sum += shopify_amount_to_decimal(pg["gross_amount"])

    posted_refunds_sum = Decimal("0")
    refunds = conn.execute(
        """SELECT r.refund_amount, r.gl_status FROM shopify_refund r
           JOIN shopify_order o ON r.shopify_order_id_local = o.id
           WHERE o.shopify_account_id = ?
             AND r.refund_date >= ? AND r.refund_date <= ?""",
        (shopify_account_id, period_start, period_end)
    ).fetchall()
    for r in refunds:
        if r["gl_status"] == "posted":
            posted_refunds_sum += shopify_amount_to_decimal(r["refund_amount"])

    expected_clearing = posted_orders_sum - posted_payouts_sum - posted_refunds_sum
    discrepancy = clearing_balance - expected_clearing

    # Determine status
    if payouts_unmatched > 0 or not amounts_equal(discrepancy, Decimal("0")):
        run_status = "discrepancy"
    else:
        run_status = "completed"

    # Insert reconciliation run record
    sql, _ = insert_row("shopify_reconciliation_run", {
        "id": P(), "shopify_account_id": P(), "run_date": P(),
        "period_start": P(), "period_end": P(),
        "total_orders": P(), "total_payouts": P(),
        "orders_matched": P(), "orders_unmatched": P(),
        "payouts_matched": P(), "payouts_unmatched": P(),
        "expected_clearing_balance": P(), "actual_clearing_balance": P(),
        "discrepancy_amount": P(), "status": P(),
        "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        run_id, shopify_account_id, now,
        period_start, period_end,
        total_orders, total_payouts,
        orders_matched, orders_unmatched,
        payouts_matched, payouts_unmatched,
        str(round_currency(expected_clearing)),
        str(round_currency(clearing_balance)),
        str(round_currency(discrepancy)),
        run_status,
        company_id, now,
    ))

    audit(conn, SKILL, "shopify-run-reconciliation",
          "shopify_reconciliation_run", run_id,
          new_values={"status": run_status, "discrepancy": str(discrepancy)})
    conn.commit()

    ok({
        "run_id": run_id,
        "run_status": run_status,
        "period_start": period_start,
        "period_end": period_end,
        "total_orders": total_orders,
        "total_payouts": total_payouts,
        "orders_matched": orders_matched,
        "orders_unmatched": orders_unmatched,
        "payouts_matched": payouts_matched,
        "payouts_unmatched": payouts_unmatched,
        "expected_clearing_balance": str(round_currency(expected_clearing)),
        "actual_clearing_balance": str(round_currency(clearing_balance)),
        "discrepancy_amount": str(round_currency(discrepancy)),
        "payout_discrepancies": payout_discrepancies,
    })


# ---------------------------------------------------------------------------
# 2. shopify-verify-payout
# ---------------------------------------------------------------------------
def verify_payout(conn, args):
    """Verify that a payout's constituent transactions sum to the payout amount."""
    shopify_payout_id = getattr(args, "shopify_payout_id", None)
    if not shopify_payout_id:
        err("--shopify-payout-id is required (local UUID)")

    payout = conn.execute(
        "SELECT * FROM shopify_payout WHERE id = ?",
        (shopify_payout_id,)
    ).fetchone()
    if not payout:
        err(f"Shopify payout {shopify_payout_id} not found")

    expected_net = shopify_amount_to_decimal(payout["net_amount"])
    expected_gross = shopify_amount_to_decimal(payout["gross_amount"])
    expected_fee = shopify_amount_to_decimal(payout["fee_amount"])

    # Sum transactions
    txns = conn.execute(
        "SELECT * FROM shopify_payout_transaction WHERE shopify_payout_id_local = ?",
        (shopify_payout_id,)
    ).fetchall()

    txn_gross_sum = Decimal("0")
    txn_fee_sum = Decimal("0")
    txn_net_sum = Decimal("0")

    for t in txns:
        txn_gross_sum += shopify_amount_to_decimal(t["gross_amount"])
        txn_fee_sum += shopify_amount_to_decimal(t["fee_amount"])
        txn_net_sum += shopify_amount_to_decimal(t["net_amount"])

    net_matches = amounts_equal(txn_net_sum, expected_net)
    gross_matches = amounts_equal(txn_gross_sum, expected_gross)

    verification_status = "matched" if (net_matches or len(txns) == 0) else "mismatch"

    ok({
        "shopify_payout_id": shopify_payout_id,
        "verification_status": verification_status,
        "transaction_count": len(txns),
        "expected_net": str(expected_net),
        "actual_net": str(txn_net_sum),
        "net_matches": net_matches,
        "expected_gross": str(expected_gross),
        "actual_gross": str(txn_gross_sum),
        "gross_matches": gross_matches,
    })


# ---------------------------------------------------------------------------
# 3. shopify-clearing-balance
# ---------------------------------------------------------------------------
def clearing_balance(conn, args):
    """Return the current Shopify Clearing GL account balance.

    Should approach zero when all orders have been settled via payouts.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)

    clearing_account_id = acct_row["clearing_account_id"]
    if not clearing_account_id:
        err("No clearing account configured for this Shopify account")

    bal = get_account_balance(conn, clearing_account_id)
    balance = to_decimal(bal["balance"])

    ok({
        "shopify_account_id": shopify_account_id,
        "clearing_account_id": clearing_account_id,
        "debit_total": bal["debit_total"],
        "credit_total": bal["credit_total"],
        "balance": str(balance),
        "is_zero": amounts_equal(balance, Decimal("0")),
    })


# ---------------------------------------------------------------------------
# 4. shopify-match-bank-transaction
# ---------------------------------------------------------------------------
def match_bank_transaction(conn, args):
    """Manually match a Shopify payout to a bank statement reference."""
    shopify_payout_id = getattr(args, "shopify_payout_id", None)
    if not shopify_payout_id:
        err("--shopify-payout-id is required (local UUID)")

    bank_reference = getattr(args, "bank_reference", None)
    if not bank_reference:
        err("--bank-reference is required")

    payout = conn.execute(
        "SELECT * FROM shopify_payout WHERE id = ?",
        (shopify_payout_id,)
    ).fetchone()
    if not payout:
        err(f"Shopify payout {shopify_payout_id} not found")

    # Update reconciliation status
    sql, params = dynamic_update("shopify_payout", {
        "reconciliation_status": "manual_matched",
    }, {"id": shopify_payout_id})
    conn.execute(sql, params)

    audit(conn, SKILL, "shopify-match-bank-transaction", "shopify_payout",
          shopify_payout_id,
          new_values={"bank_reference": bank_reference,
                      "reconciliation_status": "manual_matched"})
    conn.commit()

    ok({
        "shopify_payout_id": shopify_payout_id,
        "bank_reference": bank_reference,
        "net_amount": payout["net_amount"],
        "reconciliation_status": "manual_matched",
    })


# ---------------------------------------------------------------------------
# 5. shopify-list-reconciliations
# ---------------------------------------------------------------------------
def list_reconciliations(conn, args):
    """List reconciliation runs for a Shopify account."""
    shopify_account_id = getattr(args, "shopify_account_id", None)
    if not shopify_account_id:
        err("--shopify-account-id is required")

    t = Table("shopify_reconciliation_run")
    rows = conn.execute(
        Q.from_(t).select("*")
        .where(t.shopify_account_id == P())
        .orderby(t.created_at, order=Order.desc)
        .get_sql(),
        (shopify_account_id,)
    ).fetchall()

    runs = [row_to_dict(r) for r in rows]
    ok({"reconciliation_runs": runs, "count": len(runs)})


# ---------------------------------------------------------------------------
# 6. shopify-get-reconciliation
# ---------------------------------------------------------------------------
def get_reconciliation(conn, args):
    """Get details of a specific reconciliation run."""
    reconciliation_id = getattr(args, "reconciliation_id", None)
    if not reconciliation_id:
        err("--reconciliation-id is required")

    t = Table("shopify_reconciliation_run")
    row = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (reconciliation_id,)
    ).fetchone()
    if not row:
        err(f"Reconciliation run {reconciliation_id} not found")

    ok(row_to_dict(row))


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "shopify-run-reconciliation": run_reconciliation,
    "shopify-verify-payout": verify_payout,
    "shopify-clearing-balance": clearing_balance,
    "shopify-match-bank-transaction": match_bank_transaction,
    "shopify-list-reconciliations": list_reconciliations,
    "shopify-get-reconciliation": get_reconciliation,
}
