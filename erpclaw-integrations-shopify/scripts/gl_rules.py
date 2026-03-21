"""ERPClaw Integrations Shopify -- GL routing rules actions.

5 actions for managing configurable GL routing rules that control which
accounts Shopify transactions post to, plus a dry-run preview.
Imported by db_query.py (unified router).
"""
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

from shopify_helpers import (
    SKILL, now_iso, validate_shopify_account,
    validate_account_exists, shopify_amount_to_decimal,
)

VALID_TRANSACTION_TYPES = (
    "order", "refund", "payout", "dispute",
    "gift_card_sale", "gift_card_redeem", "fee", "reserve",
)


# ---------------------------------------------------------------------------
# 1. shopify-add-gl-rule
# ---------------------------------------------------------------------------
def add_gl_rule(conn, args):
    """Add a GL routing rule for a specific Shopify transaction type.

    Rules override the default account mappings on shopify_account for
    specific transaction types. Higher priority rules take precedence.
    """
    shopify_account_id = getattr(args, "shopify_account_id", None)
    acct_row = validate_shopify_account(conn, shopify_account_id)
    company_id = acct_row["company_id"]

    rule_name = getattr(args, "rule_name", None)
    if not rule_name:
        err("--rule-name is required")

    transaction_type = getattr(args, "transaction_type", None)
    if not transaction_type:
        err("--transaction-type is required")
    if transaction_type not in VALID_TRANSACTION_TYPES:
        err(f"Invalid transaction type: {transaction_type}. "
            f"Must be one of: {', '.join(VALID_TRANSACTION_TYPES)}")

    debit_account_id = getattr(args, "debit_account_id", None)
    credit_account_id = getattr(args, "credit_account_id", None)
    if not debit_account_id:
        err("--debit-account-id is required")
    if not credit_account_id:
        err("--credit-account-id is required")

    validate_account_exists(conn, debit_account_id, label="Debit account")
    validate_account_exists(conn, credit_account_id, label="Credit account")

    priority = getattr(args, "priority", None)
    priority = int(priority) if priority is not None else 0

    rule_id = str(uuid.uuid4())
    now = now_iso()

    sql, _ = insert_row("shopify_gl_rule", {
        "id": P(), "shopify_account_id": P(), "rule_name": P(),
        "transaction_type": P(), "debit_account_id": P(),
        "credit_account_id": P(), "is_active": P(), "priority": P(),
        "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        rule_id, shopify_account_id, rule_name,
        transaction_type, debit_account_id,
        credit_account_id, 1, priority,
        company_id, now,
    ))

    audit(conn, SKILL, "shopify-add-gl-rule", "shopify_gl_rule", rule_id,
          new_values={"rule_name": rule_name, "transaction_type": transaction_type})
    conn.commit()

    ok({
        "id": rule_id,
        "rule_name": rule_name,
        "transaction_type": transaction_type,
        "debit_account_id": debit_account_id,
        "credit_account_id": credit_account_id,
        "priority": priority,
        "is_active": 1,
    })


# ---------------------------------------------------------------------------
# 2. shopify-update-gl-rule
# ---------------------------------------------------------------------------
def update_gl_rule(conn, args):
    """Update an existing GL routing rule."""
    gl_rule_id = getattr(args, "gl_rule_id", None)
    if not gl_rule_id:
        err("--gl-rule-id is required")

    t = Table("shopify_gl_rule")
    existing = conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(),
        (gl_rule_id,)
    ).fetchone()
    if not existing:
        err(f"GL rule {gl_rule_id} not found")

    data = {}

    rule_name = getattr(args, "rule_name", None)
    if rule_name is not None:
        data["rule_name"] = rule_name

    transaction_type = getattr(args, "transaction_type", None)
    if transaction_type is not None:
        if transaction_type not in VALID_TRANSACTION_TYPES:
            err(f"Invalid transaction type: {transaction_type}. "
                f"Must be one of: {', '.join(VALID_TRANSACTION_TYPES)}")
        data["transaction_type"] = transaction_type

    debit_account_id = getattr(args, "debit_account_id", None)
    if debit_account_id is not None:
        validate_account_exists(conn, debit_account_id, label="Debit account")
        data["debit_account_id"] = debit_account_id

    credit_account_id = getattr(args, "credit_account_id", None)
    if credit_account_id is not None:
        validate_account_exists(conn, credit_account_id, label="Credit account")
        data["credit_account_id"] = credit_account_id

    priority = getattr(args, "priority", None)
    if priority is not None:
        data["priority"] = int(priority)

    if not data:
        err("No fields to update")

    sql, params = dynamic_update("shopify_gl_rule", data, {"id": gl_rule_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "shopify-update-gl-rule", "shopify_gl_rule", gl_rule_id,
          new_values=data)
    conn.commit()

    ok({
        "id": gl_rule_id,
        "updated_fields": list(data.keys()),
    })


# ---------------------------------------------------------------------------
# 3. shopify-list-gl-rules
# ---------------------------------------------------------------------------
def list_gl_rules(conn, args):
    """List GL routing rules for a Shopify account."""
    shopify_account_id = getattr(args, "shopify_account_id", None)
    if not shopify_account_id:
        err("--shopify-account-id is required")

    t = Table("shopify_gl_rule")
    rows = conn.execute(
        Q.from_(t).select("*")
        .where(t.shopify_account_id == P())
        .orderby(t.priority, order=Order.desc)
        .get_sql(),
        (shopify_account_id,)
    ).fetchall()

    rules = [row_to_dict(r) for r in rows]
    ok({"gl_rules": rules, "count": len(rules)})


# ---------------------------------------------------------------------------
# 4. shopify-delete-gl-rule
# ---------------------------------------------------------------------------
def delete_gl_rule(conn, args):
    """Soft-delete a GL routing rule (set is_active=0)."""
    gl_rule_id = getattr(args, "gl_rule_id", None)
    if not gl_rule_id:
        err("--gl-rule-id is required")

    t = Table("shopify_gl_rule")
    existing = conn.execute(
        Q.from_(t).select(t.id, t.is_active).where(t.id == P()).get_sql(),
        (gl_rule_id,)
    ).fetchone()
    if not existing:
        err(f"GL rule {gl_rule_id} not found")

    if existing["is_active"] == 0:
        err(f"GL rule {gl_rule_id} is already inactive")

    sql, params = dynamic_update("shopify_gl_rule",
                                  {"is_active": 0}, {"id": gl_rule_id})
    conn.execute(sql, params)
    audit(conn, SKILL, "shopify-delete-gl-rule", "shopify_gl_rule", gl_rule_id,
          new_values={"is_active": 0})
    conn.commit()

    ok({"id": gl_rule_id, "is_active": 0})


# ---------------------------------------------------------------------------
# 5. shopify-preview-gl
# ---------------------------------------------------------------------------
def preview_gl(conn, args):
    """Dry-run: show what GL entries would be posted for a Shopify order.

    Does NOT write anything. Resolves GL accounts from shopify_account
    configuration and any matching GL rules, then builds the entry list.
    """
    shopify_order_id = getattr(args, "shopify_order_id", None)
    if not shopify_order_id:
        err("--shopify-order-id is required (local UUID)")

    t_order = Table("shopify_order")
    order = conn.execute(
        Q.from_(t_order).select("*").where(t_order.id == P()).get_sql(),
        (shopify_order_id,)
    ).fetchone()
    if not order:
        err(f"Shopify order {shopify_order_id} not found")

    acct_row = validate_shopify_account(conn, order["shopify_account_id"])

    # Resolve GL accounts
    subtotal = shopify_amount_to_decimal(order["subtotal_amount"])
    shipping = shopify_amount_to_decimal(order["shipping_amount"])
    tax = shopify_amount_to_decimal(order["tax_amount"])
    discount = shopify_amount_to_decimal(order["discount_amount"])
    total = shopify_amount_to_decimal(order["total_amount"])

    entries = []

    # DR Clearing for total
    entries.append({
        "account_id": acct_row["clearing_account_id"],
        "account_label": "Shopify Clearing",
        "debit": str(total),
        "credit": "0",
    })

    # CR Revenue for subtotal (net of discount if discount_method='net')
    discount_method = acct_row["discount_method"]
    if discount_method == "net":
        revenue_credit = subtotal - discount
    else:
        revenue_credit = subtotal

    entries.append({
        "account_id": acct_row["revenue_account_id"],
        "account_label": "Shopify Revenue",
        "debit": "0",
        "credit": str(revenue_credit),
    })

    # If gross discount method, show separate discount debit
    if discount_method == "gross" and discount > Decimal("0"):
        entries.append({
            "account_id": acct_row["discount_account_id"],
            "account_label": "Shopify Discounts",
            "debit": str(discount),
            "credit": "0",
        })

    # CR Shipping Revenue
    if shipping > Decimal("0"):
        entries.append({
            "account_id": acct_row["shipping_revenue_account_id"],
            "account_label": "Shopify Shipping Revenue",
            "debit": "0",
            "credit": str(shipping),
        })

    # CR Tax Payable
    if tax > Decimal("0"):
        entries.append({
            "account_id": acct_row["tax_payable_account_id"],
            "account_label": "Shopify Tax Payable",
            "debit": "0",
            "credit": str(tax),
        })

    # COGS entries if track_cogs enabled
    cogs_entries = []
    if acct_row["track_cogs"]:
        cogs_entries.append({
            "account_id": acct_row["cogs_account_id"],
            "account_label": "Shopify COGS",
            "debit": "estimated",
            "credit": "0",
            "note": "COGS amount requires item valuation lookup",
        })
        cogs_entries.append({
            "account_id": acct_row["inventory_account_id"],
            "account_label": "Shopify Inventory",
            "debit": "0",
            "credit": "estimated",
            "note": "Inventory credit mirrors COGS debit",
        })

    # Verify preview balance
    preview_debit = sum(
        shopify_amount_to_decimal(e["debit"])
        for e in entries if e["debit"] != "estimated"
    )
    preview_credit = sum(
        shopify_amount_to_decimal(e["credit"])
        for e in entries if e["credit"] != "estimated"
    )

    ok({
        "order_id": shopify_order_id,
        "shopify_order_id": order["shopify_order_id"],
        "total_amount": str(total),
        "discount_method": discount_method,
        "track_cogs": bool(acct_row["track_cogs"]),
        "gl_entries": entries,
        "cogs_entries": cogs_entries,
        "preview_debit": str(preview_debit),
        "preview_credit": str(preview_credit),
        "is_balanced": preview_debit == preview_credit,
        "note": "Dry-run preview. No GL entries were posted.",
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "shopify-add-gl-rule": add_gl_rule,
    "shopify-update-gl-rule": update_gl_rule,
    "shopify-list-gl-rules": list_gl_rules,
    "shopify-delete-gl-rule": delete_gl_rule,
    "shopify-preview-gl": preview_gl,
}
