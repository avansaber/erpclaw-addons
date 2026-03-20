"""ERPClaw Integrations Stripe — GL rule management actions.

5 actions for configurable GL posting rules:
  stripe-add-gl-rule, stripe-update-gl-rule, stripe-list-gl-rules,
  stripe-delete-gl-rule, stripe-preview-gl-posting

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
    validate_stripe_account, validate_account_exists, validate_enum,
)

VALID_TRANSACTION_TYPES = ("charge", "refund", "dispute", "payout", "connect_fee", "other")


# ---------------------------------------------------------------------------
# 1. stripe-add-gl-rule
# ---------------------------------------------------------------------------
def add_gl_rule(conn, args):
    """Add a custom GL mapping rule for Stripe transactions.

    Rules map transaction types (charge, refund, etc.) to specific debit/credit
    GL accounts. Higher priority rules are evaluated first. Optional match_field
    and match_value allow fine-grained matching (e.g., by payment method).
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    acct_row = validate_stripe_account(conn, stripe_account_id)
    company_id = acct_row["company_id"]

    transaction_type = getattr(args, "transaction_type", None)
    if not transaction_type:
        err("--transaction-type is required")
    validate_enum(transaction_type, VALID_TRANSACTION_TYPES, "transaction_type")

    debit_account_id = getattr(args, "debit_account_id", None)
    if not debit_account_id:
        err("--debit-account-id is required")
    validate_account_exists(conn, debit_account_id, label="Debit account")

    credit_account_id = getattr(args, "credit_account_id", None)
    if not credit_account_id:
        err("--credit-account-id is required")
    validate_account_exists(conn, credit_account_id, label="Credit account")

    # Optional fields
    match_field = getattr(args, "match_field", None)
    match_value = getattr(args, "match_value", None)
    fee_account_id = getattr(args, "fee_account_id", None)
    if fee_account_id:
        validate_account_exists(conn, fee_account_id, label="Fee account")

    cost_center_id = getattr(args, "cost_center_id", None)
    priority = getattr(args, "priority", 0) or 0

    now = now_iso()
    rule_id = str(uuid.uuid4())

    sql, _ = insert_row("stripe_gl_rule", {
        "id": P(), "stripe_account_id": P(), "transaction_type": P(),
        "match_field": P(), "match_value": P(),
        "debit_account_id": P(), "credit_account_id": P(),
        "fee_account_id": P(), "cost_center_id": P(),
        "priority": P(), "is_active": P(),
        "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        rule_id, stripe_account_id, transaction_type,
        match_field, match_value,
        debit_account_id, credit_account_id,
        fee_account_id, cost_center_id,
        priority, 1,
        company_id, now,
    ))

    audit(conn, SKILL, "stripe-add-gl-rule", "stripe_gl_rule", rule_id,
          new_values={
              "transaction_type": transaction_type,
              "debit_account_id": debit_account_id,
              "credit_account_id": credit_account_id,
          })
    conn.commit()

    ok({
        "gl_rule_id": rule_id,
        "stripe_account_id": stripe_account_id,
        "transaction_type": transaction_type,
        "debit_account_id": debit_account_id,
        "credit_account_id": credit_account_id,
        "fee_account_id": fee_account_id,
        "priority": priority,
    })


# ---------------------------------------------------------------------------
# 2. stripe-update-gl-rule
# ---------------------------------------------------------------------------
def update_gl_rule(conn, args):
    """Update an existing GL rule.

    Supports updating transaction_type, match fields, accounts, and priority.
    """
    gl_rule_id = getattr(args, "gl_rule_id", None)
    if not gl_rule_id:
        err("--gl-rule-id is required")

    t = Table("stripe_gl_rule")
    existing = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (gl_rule_id,)
    ).fetchone()
    if not existing:
        err(f"GL rule {gl_rule_id} not found")

    data = {}

    transaction_type = getattr(args, "transaction_type", None)
    if transaction_type is not None:
        validate_enum(transaction_type, VALID_TRANSACTION_TYPES, "transaction_type")
        data["transaction_type"] = transaction_type

    debit_account_id = getattr(args, "debit_account_id", None)
    if debit_account_id is not None:
        validate_account_exists(conn, debit_account_id, label="Debit account")
        data["debit_account_id"] = debit_account_id

    credit_account_id = getattr(args, "credit_account_id", None)
    if credit_account_id is not None:
        validate_account_exists(conn, credit_account_id, label="Credit account")
        data["credit_account_id"] = credit_account_id

    fee_account_id = getattr(args, "fee_account_id", None)
    if fee_account_id is not None:
        validate_account_exists(conn, fee_account_id, label="Fee account")
        data["fee_account_id"] = fee_account_id

    match_field = getattr(args, "match_field", None)
    if match_field is not None:
        data["match_field"] = match_field

    match_value = getattr(args, "match_value", None)
    if match_value is not None:
        data["match_value"] = match_value

    cost_center_id = getattr(args, "cost_center_id", None)
    if cost_center_id is not None:
        data["cost_center_id"] = cost_center_id

    priority = getattr(args, "priority", None)
    if priority is not None:
        data["priority"] = priority

    if not data:
        err("No fields to update")

    sql, params = dynamic_update("stripe_gl_rule", data, {"id": gl_rule_id})
    conn.execute(sql, params)

    audit(conn, SKILL, "stripe-update-gl-rule", "stripe_gl_rule", gl_rule_id,
          new_values=data)
    conn.commit()

    ok({
        "gl_rule_id": gl_rule_id,
        "updated_fields": list(data.keys()),
    })


# ---------------------------------------------------------------------------
# 3. stripe-list-gl-rules
# ---------------------------------------------------------------------------
def list_gl_rules(conn, args):
    """List GL rules for a Stripe account, ordered by priority descending."""
    stripe_account_id = getattr(args, "stripe_account_id", None)
    validate_stripe_account(conn, stripe_account_id)

    t = Table("stripe_gl_rule")
    q = Q.from_(t).select("*").where(
        t.stripe_account_id == P()
    ).where(
        t.is_active == 1
    ).orderby(t.priority, order=Order.desc)

    rows = conn.execute(q.get_sql(), (stripe_account_id,)).fetchall()
    ok({
        "gl_rules": rows_to_list(rows),
        "count": len(rows),
    })


# ---------------------------------------------------------------------------
# 4. stripe-delete-gl-rule
# ---------------------------------------------------------------------------
def delete_gl_rule(conn, args):
    """Soft-delete a GL rule by setting is_active=0."""
    gl_rule_id = getattr(args, "gl_rule_id", None)
    if not gl_rule_id:
        err("--gl-rule-id is required")

    t = Table("stripe_gl_rule")
    existing = conn.execute(
        Q.from_(t).select(t.id, t.is_active).where(t.id == P()).get_sql(),
        (gl_rule_id,)
    ).fetchone()
    if not existing:
        err(f"GL rule {gl_rule_id} not found")

    if existing["is_active"] == 0:
        err(f"GL rule {gl_rule_id} is already deleted")

    sql, params = dynamic_update("stripe_gl_rule", {
        "is_active": 0,
    }, {"id": gl_rule_id})
    conn.execute(sql, params)

    audit(conn, SKILL, "stripe-delete-gl-rule", "stripe_gl_rule", gl_rule_id,
          new_values={"is_active": 0})
    conn.commit()

    ok({
        "gl_rule_id": gl_rule_id,
        "status": "deleted",
    })


# ---------------------------------------------------------------------------
# 5. stripe-preview-gl-posting
# ---------------------------------------------------------------------------
def preview_gl_posting(conn, args):
    """Dry-run: show what GL entries WOULD be created for a given charge.

    No actual posting happens. Finds the applicable GL rule (highest priority
    match) and constructs the preview entries.
    """
    stripe_account_id = getattr(args, "stripe_account_id", None)
    acct_row = validate_stripe_account(conn, stripe_account_id)

    charge_stripe_id = getattr(args, "charge_stripe_id", None)
    if not charge_stripe_id:
        err("--charge-stripe-id is required")

    # Look up the charge
    charge_table = Table("stripe_charge")
    charge = conn.execute(
        Q.from_(charge_table).select("*")
        .where(charge_table.stripe_account_id == P())
        .where(charge_table.stripe_id == P())
        .get_sql(),
        (stripe_account_id, charge_stripe_id)
    ).fetchone()
    if not charge:
        err(f"Charge {charge_stripe_id} not found for account {stripe_account_id}")

    charge_amount = Decimal(charge["amount"])

    # Find the associated balance transaction for fee info
    bt_table = Table("stripe_balance_transaction")
    bt_row = conn.execute(
        Q.from_(bt_table).select(bt_table.fee, bt_table.net)
        .where(bt_table.source_id == P())
        .where(bt_table.stripe_account_id == P())
        .get_sql(),
        (charge_stripe_id, stripe_account_id)
    ).fetchone()

    fee_amount = Decimal(bt_row["fee"]) if bt_row else Decimal("0")
    net_amount = Decimal(bt_row["net"]) if bt_row else charge_amount

    # Find the best matching GL rule
    rule_table = Table("stripe_gl_rule")
    rules = conn.execute(
        Q.from_(rule_table).select("*")
        .where(rule_table.stripe_account_id == P())
        .where(rule_table.transaction_type == P())
        .where(rule_table.is_active == 1)
        .orderby(rule_table.priority, order=Order.desc)
        .get_sql(),
        (stripe_account_id, "charge")
    ).fetchall()

    matched_rule = None
    for rule in rules:
        match_field = rule["match_field"]
        match_value = rule["match_value"]
        if match_field and match_value:
            # Check if the charge matches the rule's criteria
            charge_val = charge[match_field] if match_field in charge.keys() else None
            if charge_val == match_value:
                matched_rule = rule
                break
        else:
            # No match criteria — this is a catch-all rule
            matched_rule = rule
            break

    if not matched_rule:
        # Fall back to account-level defaults
        ok({
            "charge_stripe_id": charge_stripe_id,
            "charge_amount": str(charge_amount),
            "fee_amount": str(fee_amount),
            "net_amount": str(net_amount),
            "rule_applied": None,
            "gl_entries": [],
            "message": "No matching GL rule found. Configure a rule with stripe-add-gl-rule.",
        })
        return

    # Build preview GL entries
    entries = []

    # Debit entry (e.g., debit clearing account for gross amount)
    entries.append({
        "account_id": matched_rule["debit_account_id"],
        "debit": str(charge_amount),
        "credit": "0",
        "description": f"Stripe charge {charge_stripe_id}",
    })

    # Credit entry (e.g., credit revenue for net amount)
    if fee_amount > 0 and matched_rule["fee_account_id"]:
        # Split: credit revenue for net, debit fees for fee
        entries.append({
            "account_id": matched_rule["credit_account_id"],
            "debit": "0",
            "credit": str(net_amount),
            "description": f"Stripe charge {charge_stripe_id} (net)",
        })
        entries.append({
            "account_id": matched_rule["fee_account_id"],
            "debit": str(fee_amount),
            "credit": "0",
            "description": f"Stripe processing fee for {charge_stripe_id}",
        })
    else:
        # No fee split — credit full amount
        entries.append({
            "account_id": matched_rule["credit_account_id"],
            "debit": "0",
            "credit": str(charge_amount),
            "description": f"Stripe charge {charge_stripe_id}",
        })

    ok({
        "charge_stripe_id": charge_stripe_id,
        "charge_amount": str(charge_amount),
        "fee_amount": str(fee_amount),
        "net_amount": str(net_amount),
        "rule_applied": {
            "gl_rule_id": matched_rule["id"],
            "transaction_type": matched_rule["transaction_type"],
            "priority": matched_rule["priority"],
        },
        "gl_entries": entries,
        "is_preview": True,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "stripe-add-gl-rule": add_gl_rule,
    "stripe-update-gl-rule": update_gl_rule,
    "stripe-list-gl-rules": list_gl_rules,
    "stripe-delete-gl-rule": delete_gl_rule,
    "stripe-preview-gl-posting": preview_gl_posting,
}
