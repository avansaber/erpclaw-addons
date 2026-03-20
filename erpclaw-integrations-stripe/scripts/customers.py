"""ERPClaw Integrations Stripe — customer mapping actions.

5 actions for mapping Stripe customers to ERPClaw customers:
  stripe-map-customer, stripe-auto-map-customers, stripe-list-customer-maps,
  stripe-unmap-customer, stripe-get-customer-detail

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
# 1. stripe-map-customer
# ---------------------------------------------------------------------------
def map_customer(conn, args):
    """Manually link a Stripe customer to an ERPClaw customer.

    Creates or updates a stripe_customer_map row with match_method='manual'
    and match_confidence='1.0'.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    acct_row = validate_stripe_account(conn, stripe_account_id)
    company_id = acct_row["company_id"]

    stripe_customer_id = getattr(args, "stripe_customer_id", None)
    if not stripe_customer_id:
        err("--stripe-customer-id is required")

    erpclaw_customer_id = getattr(args, "erpclaw_customer_id", None)
    if not erpclaw_customer_id:
        err("--erpclaw-customer-id is required")

    # Validate erpclaw customer exists
    cust_table = Table("customer")
    cust_row = conn.execute(
        Q.from_(cust_table).select(cust_table.id, cust_table.name)
        .where(cust_table.id == P()).get_sql(),
        (erpclaw_customer_id,)
    ).fetchone()
    if not cust_row:
        err(f"ERPClaw customer {erpclaw_customer_id} not found")

    # Check if mapping already exists
    map_table = Table("stripe_customer_map")
    existing = conn.execute(
        Q.from_(map_table).select(map_table.id)
        .where(map_table.stripe_account_id == P())
        .where(map_table.stripe_customer_id == P())
        .get_sql(),
        (stripe_account_id, stripe_customer_id)
    ).fetchone()

    now = now_iso()

    if existing:
        # Update existing mapping
        sql, params = dynamic_update("stripe_customer_map", {
            "erpclaw_customer_id": erpclaw_customer_id,
            "match_method": "manual",
            "match_confidence": "1.0",
        }, {"id": existing["id"]})
        conn.execute(sql, params)
        map_id = existing["id"]
    else:
        # Create new mapping
        map_id = str(uuid.uuid4())
        sql, _ = insert_row("stripe_customer_map", {
            "id": P(), "stripe_account_id": P(), "stripe_customer_id": P(),
            "erpclaw_customer_id": P(), "stripe_email": P(), "stripe_name": P(),
            "match_method": P(), "match_confidence": P(),
            "company_id": P(), "created_at": P(),
        })
        conn.execute(sql, (
            map_id, stripe_account_id, stripe_customer_id,
            erpclaw_customer_id, "", "",
            "manual", "1.0",
            company_id, now,
        ))

    audit(conn, SKILL, "stripe-map-customer", "stripe_customer_map", map_id,
          new_values={
              "stripe_customer_id": stripe_customer_id,
              "erpclaw_customer_id": erpclaw_customer_id,
          })
    conn.commit()

    ok({
        "customer_map_id": map_id,
        "stripe_customer_id": stripe_customer_id,
        "erpclaw_customer_id": erpclaw_customer_id,
        "match_method": "manual",
        "match_confidence": "1.0",
    })


# ---------------------------------------------------------------------------
# 2. stripe-auto-map-customers
# ---------------------------------------------------------------------------
def auto_map_customers(conn, args):
    """Auto-match unmatched Stripe customers to ERPClaw customers by name.

    Scans stripe_customer_map rows where erpclaw_customer_id IS NULL,
    matches against the erpclaw customer table by name.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    acct_row = validate_stripe_account(conn, stripe_account_id)
    company_id = acct_row["company_id"]

    map_table = Table("stripe_customer_map")
    cust_table = Table("customer")

    # Get all unmatched mappings for this account
    unmatched = conn.execute(
        Q.from_(map_table)
        .select(map_table.id, map_table.stripe_customer_id, map_table.stripe_name)
        .where(map_table.stripe_account_id == P())
        .where(map_table.erpclaw_customer_id.isnull())
        .get_sql(),
        (stripe_account_id,)
    ).fetchall()

    matched_count = 0
    skipped_count = 0
    matches = []

    for row in unmatched:
        stripe_name = row["stripe_name"]
        if not stripe_name:
            skipped_count += 1
            continue

        # Try to match by name
        match_row = conn.execute(
            Q.from_(cust_table).select(cust_table.id, cust_table.name)
            .where(cust_table.name == P())
            .where(cust_table.company_id == P())
            .get_sql(),
            (stripe_name, company_id)
        ).fetchone()

        if match_row:
            sql, params = dynamic_update("stripe_customer_map", {
                "erpclaw_customer_id": match_row["id"],
                "match_method": "name",
                "match_confidence": "0.8",
            }, {"id": row["id"]})
            conn.execute(sql, params)
            matched_count += 1
            matches.append({
                "stripe_customer_id": row["stripe_customer_id"],
                "erpclaw_customer_id": match_row["id"],
                "matched_name": stripe_name,
            })
        else:
            skipped_count += 1

    conn.commit()

    ok({
        "stripe_account_id": stripe_account_id,
        "unmatched_scanned": len(unmatched),
        "matched_count": matched_count,
        "skipped_count": skipped_count,
        "matches": matches,
    })


# ---------------------------------------------------------------------------
# 3. stripe-list-customer-maps
# ---------------------------------------------------------------------------
def list_customer_maps(conn, args):
    """List customer mappings for a Stripe account.

    Optional filters: --match-method, --limit, --offset.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_customer_map")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).orderby(t.created_at, order=Order.desc)

    params = [stripe_account_id]

    match_method = getattr(args, "match_method", None)
    if match_method:
        q = q.where(t.match_method == P())
        params.append(match_method)

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    q = q.limit(limit).offset(offset)

    rows = conn.execute(q.get_sql(), tuple(params)).fetchall()
    ok({
        "customer_maps": rows_to_list(rows),
        "count": len(rows),
    })


# ---------------------------------------------------------------------------
# 4. stripe-unmap-customer
# ---------------------------------------------------------------------------
def unmap_customer(conn, args):
    """Remove a customer mapping (set erpclaw_customer_id to NULL)."""
    customer_map_id = getattr(args, "customer_map_id", None)
    if not customer_map_id:
        err("--customer-map-id is required")

    t = Table("stripe_customer_map")
    existing = conn.execute(
        Q.from_(t).select(t.id, t.stripe_customer_id, t.erpclaw_customer_id)
        .where(t.id == P()).get_sql(),
        (customer_map_id,)
    ).fetchone()
    if not existing:
        err(f"Customer mapping {customer_map_id} not found")

    sql, params = dynamic_update("stripe_customer_map", {
        "erpclaw_customer_id": None,
        "match_method": "manual",
        "match_confidence": "0.0",
    }, {"id": customer_map_id})
    conn.execute(sql, params)

    audit(conn, SKILL, "stripe-unmap-customer", "stripe_customer_map", customer_map_id,
          new_values={"erpclaw_customer_id": None})
    conn.commit()

    ok({
        "customer_map_id": customer_map_id,
        "stripe_customer_id": existing["stripe_customer_id"],
        "erpclaw_customer_id": None,
        "status": "unmapped",
    })


# ---------------------------------------------------------------------------
# 5. stripe-get-customer-detail
# ---------------------------------------------------------------------------
def get_customer_detail(conn, args):
    """Get detailed view of a Stripe customer mapping including charge summary.

    Returns mapping info plus aggregated charge data: total charges,
    total amount, and last charge date.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    validate_stripe_account(conn, stripe_account_id)

    stripe_customer_id = getattr(args, "stripe_customer_id", None)
    if not stripe_customer_id:
        err("--stripe-customer-id is required")

    map_table = Table("stripe_customer_map")
    mapping = conn.execute(
        Q.from_(map_table).select("*")
        .where(map_table.stripe_account_id == P())
        .where(map_table.stripe_customer_id == P())
        .get_sql(),
        (stripe_account_id, stripe_customer_id)
    ).fetchone()
    if not mapping:
        err(f"No mapping found for Stripe customer {stripe_customer_id}")

    result = row_to_dict(mapping)

    # Get charge summary
    charge_table = Table("stripe_charge")
    charge_summary = conn.execute(
        Q.from_(charge_table)
        .select(
            fn.Count("*").as_("total_charges"),
            fn.Max(charge_table.created_stripe).as_("last_charge_date"),
        )
        .where(charge_table.stripe_account_id == P())
        .where(charge_table.customer_stripe_id == P())
        .get_sql(),
        (stripe_account_id, stripe_customer_id)
    ).fetchone()

    # Use separate query for sum to handle Decimal properly
    amount_row = conn.execute(
        "SELECT decimal_sum(amount) as total_amount FROM stripe_charge "
        "WHERE stripe_account_id = ? AND customer_stripe_id = ?",
        (stripe_account_id, stripe_customer_id)
    ).fetchone()

    total_amount = amount_row["total_amount"] if amount_row and amount_row["total_amount"] else "0"

    result["charge_summary"] = {
        "total_charges": charge_summary["total_charges"] if charge_summary else 0,
        "total_amount": total_amount,
        "last_charge_date": charge_summary["last_charge_date"] if charge_summary else None,
    }

    ok(result)


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "stripe-map-customer": map_customer,
    "stripe-auto-map-customers": auto_map_customers,
    "stripe-list-customer-maps": list_customer_maps,
    "stripe-unmap-customer": unmap_customer,
    "stripe-get-customer-detail": get_customer_detail,
}
