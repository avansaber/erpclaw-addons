#!/usr/bin/env python3
"""erpclaw-pos transactions domain module.

Transaction lifecycle — create, add items, apply discounts, hold/resume,
add payments, submit, void, return. Plus item lookup, receipts, and session
summary. Imported by the unified erpclaw-pos db_query.py router.
"""
import os
import sqlite3
import sys
import uuid
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit
from erpclaw_lib.db import DEFAULT_DB_PATH

SKILL = "erpclaw-pos"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _dec(val):
    if val is None:
        return Decimal("0")
    return Decimal(str(val))


def _round(val):
    return val.quantize(Decimal("0.01"), ROUND_HALF_UP)


def _recalc_totals(conn, txn_id):
    """Recalculate subtotal and grand_total from line items and transaction-level discount."""
    items = conn.execute(
        "SELECT amount FROM pos_transaction_item WHERE pos_transaction_id = ?",
        (txn_id,)).fetchall()
    subtotal = _round(sum((_dec(r["amount"]) for r in items), Decimal("0")))

    txn = conn.execute(
        "SELECT discount_pct, discount_amount, tax_amount FROM pos_transaction WHERE id = ?",
        (txn_id,)).fetchone()

    disc_pct = _dec(txn["discount_pct"])
    disc_amt = _dec(txn["discount_amount"])
    tax_amt = _dec(txn["tax_amount"])

    # If discount_pct > 0, recalculate discount_amount from subtotal
    if disc_pct > Decimal("0"):
        disc_amt = _round(subtotal * disc_pct / Decimal("100"))

    grand_total = _round(subtotal - disc_amt + tax_amt)
    if grand_total < Decimal("0"):
        grand_total = Decimal("0")

    conn.execute(
        """UPDATE pos_transaction
           SET subtotal = ?, discount_amount = ?, grand_total = ?,
               updated_at = datetime('now')
           WHERE id = ?""",
        (str(subtotal), str(disc_amt), str(grand_total), txn_id))
    return subtotal, disc_amt, grand_total


# ---------------------------------------------------------------------------
# add-transaction
# ---------------------------------------------------------------------------
def add_transaction(conn, args):
    session_id = getattr(args, "pos_session_id", None)
    if not session_id:
        err("--pos-session-id is required")

    session = conn.execute(
        "SELECT id, company_id, status FROM pos_session WHERE id = ?",
        (session_id,)).fetchone()
    if not session:
        err(f"Session {session_id} not found")
    if session["status"] != "open":
        err(f"Session {session_id} is not open (current: {session['status']})")

    company_id = session["company_id"]
    customer_id = getattr(args, "customer_id", None)
    customer_name = getattr(args, "customer_name", None)

    txn_id = str(uuid.uuid4())
    naming = get_next_name(conn, "pos_transaction", company_id=company_id)

    try:
        conn.execute(
            """INSERT INTO pos_transaction
               (id, naming_series, pos_session_id, customer_id, customer_name,
                subtotal, discount_amount, discount_pct, tax_amount,
                grand_total, paid_amount, change_amount, status, company_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (txn_id, naming, session_id, customer_id, customer_name,
             "0", "0", "0", "0", "0", "0", "0", "draft", company_id))
    except sqlite3.IntegrityError as e:
        sys.stderr.write(f"[{SKILL}] {e}\n")
        err("Transaction creation failed")

    audit(conn, SKILL, "pos-add-transaction", "pos_transaction", txn_id,
          new_values={"naming_series": naming, "session_id": session_id})
    conn.commit()
    ok({"id": txn_id, "naming_series": naming,
        "transaction_status": "draft"})


# ---------------------------------------------------------------------------
# add-transaction-item
# ---------------------------------------------------------------------------
def add_transaction_item(conn, args):
    txn_id = getattr(args, "pos_transaction_id", None)
    item_id = getattr(args, "item_id", None)

    if not txn_id:
        err("--pos-transaction-id is required")
    if not item_id:
        err("--item-id is required")

    txn = conn.execute(
        "SELECT id, status FROM pos_transaction WHERE id = ?",
        (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] not in ("draft", "held"):
        err(f"Cannot add items to transaction in '{txn['status']}' status")

    # Validate item exists
    item = conn.execute(
        "SELECT id, item_name, item_code FROM item WHERE id = ?",
        (item_id,)).fetchone()
    if not item:
        err(f"Item {item_id} not found")

    item_name = getattr(args, "item_name", None) or item["item_name"]
    item_code = item["item_code"]

    # Try to get barcode (item_barcode table may not exist)
    barcode = getattr(args, "barcode", None)
    if not barcode:
        try:
            bc_row = conn.execute(
                "SELECT barcode FROM item_barcode WHERE item_id = ? LIMIT 1",
                (item_id,)).fetchone()
            if bc_row:
                barcode = bc_row["barcode"]
        except sqlite3.OperationalError:
            pass  # item_barcode table may not exist

    qty = _dec(getattr(args, "qty", None) or "1")
    rate = _dec(getattr(args, "rate", None) or "0")
    uom = getattr(args, "uom", None) or "Nos"
    disc_pct = _dec(getattr(args, "discount_pct", None) or "0")

    if qty <= Decimal("0"):
        err("--qty must be positive")
    if rate < Decimal("0"):
        err("--rate must be non-negative")
    if disc_pct < Decimal("0") or disc_pct > Decimal("100"):
        err("--discount-pct must be between 0 and 100")

    line_subtotal = _round(qty * rate)
    disc_amt = _round(line_subtotal * disc_pct / Decimal("100"))
    amount = _round(line_subtotal - disc_amt)

    line_id = str(uuid.uuid4())
    try:
        conn.execute(
            """INSERT INTO pos_transaction_item
               (id, pos_transaction_id, item_id, item_name, item_code, barcode,
                qty, rate, discount_pct, discount_amount, amount, uom)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (line_id, txn_id, item_id, item_name, item_code, barcode,
             str(qty), str(rate), str(disc_pct), str(disc_amt), str(amount), uom))
    except sqlite3.IntegrityError as e:
        sys.stderr.write(f"[{SKILL}] {e}\n")
        err("Failed to add item to transaction")

    subtotal, _, grand_total = _recalc_totals(conn, txn_id)
    conn.commit()

    ok({"id": line_id, "item_id": item_id, "item_name": item_name,
        "qty": str(qty), "rate": str(rate), "amount": str(amount),
        "transaction_subtotal": str(subtotal), "transaction_grand_total": str(grand_total)})


# ---------------------------------------------------------------------------
# remove-transaction-item
# ---------------------------------------------------------------------------
def remove_transaction_item(conn, args):
    line_id = getattr(args, "pos_transaction_item_id", None)
    if not line_id:
        err("--pos-transaction-item-id is required")

    line = conn.execute(
        "SELECT id, pos_transaction_id FROM pos_transaction_item WHERE id = ?",
        (line_id,)).fetchone()
    if not line:
        err(f"Transaction item {line_id} not found")

    txn_id = line["pos_transaction_id"]

    txn = conn.execute(
        "SELECT id, status FROM pos_transaction WHERE id = ?",
        (txn_id,)).fetchone()
    if txn["status"] not in ("draft", "held"):
        err(f"Cannot remove items from transaction in '{txn['status']}' status")

    conn.execute("DELETE FROM pos_transaction_item WHERE id = ?", (line_id,))
    subtotal, _, grand_total = _recalc_totals(conn, txn_id)
    conn.commit()

    ok({"removed": line_id, "id": txn_id,
        "transaction_subtotal": str(subtotal), "transaction_grand_total": str(grand_total)})


# ---------------------------------------------------------------------------
# apply-discount
# ---------------------------------------------------------------------------
def apply_discount(conn, args):
    txn_id = getattr(args, "pos_transaction_id", None)
    if not txn_id:
        err("--pos-transaction-id is required")

    txn = conn.execute(
        "SELECT * FROM pos_transaction WHERE id = ?", (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] not in ("draft", "held"):
        err(f"Cannot apply discount to transaction in '{txn['status']}' status")

    disc_pct = getattr(args, "discount_pct", None)
    disc_amt = getattr(args, "discount_amount", None)

    if disc_pct is None and disc_amt is None:
        err("--discount-pct or --discount-amount is required")

    # Check profile discount rules
    session = conn.execute(
        "SELECT pos_profile_id FROM pos_session WHERE id = ?",
        (txn["pos_session_id"],)).fetchone()
    if session:
        profile = conn.execute(
            "SELECT allow_discount, max_discount_pct FROM pos_profile WHERE id = ?",
            (session["pos_profile_id"],)).fetchone()
        if profile and not profile["allow_discount"]:
            err("Discounts are not allowed for this POS profile")
        if profile and disc_pct is not None:
            max_pct = _dec(profile["max_discount_pct"])
            if _dec(disc_pct) > max_pct:
                err(f"Discount exceeds maximum allowed: {max_pct}%")

    subtotal = _dec(txn["subtotal"])

    if disc_pct is not None:
        pct_val = _dec(disc_pct)
        if pct_val < Decimal("0") or pct_val > Decimal("100"):
            err("--discount-pct must be between 0 and 100")
        computed_amt = _round(subtotal * pct_val / Decimal("100"))
        conn.execute(
            """UPDATE pos_transaction
               SET discount_pct = ?, discount_amount = ?,
                   grand_total = ?, updated_at = datetime('now')
               WHERE id = ?""",
            (str(pct_val), str(computed_amt),
             str(_round(subtotal - computed_amt + _dec(txn["tax_amount"]))),
             txn_id))
    else:
        amt_val = _dec(disc_amt)
        if amt_val < Decimal("0"):
            err("--discount-amount must be non-negative")
        if amt_val > subtotal:
            amt_val = subtotal
        conn.execute(
            """UPDATE pos_transaction
               SET discount_pct = '0', discount_amount = ?,
                   grand_total = ?, updated_at = datetime('now')
               WHERE id = ?""",
            (str(_round(amt_val)),
             str(_round(subtotal - amt_val + _dec(txn["tax_amount"]))),
             txn_id))

    conn.commit()
    updated = conn.execute(
        "SELECT subtotal, discount_pct, discount_amount, grand_total FROM pos_transaction WHERE id = ?",
        (txn_id,)).fetchone()
    ok({"id": txn_id,
        "subtotal": updated["subtotal"],
        "discount_pct": updated["discount_pct"],
        "discount_amount": updated["discount_amount"],
        "grand_total": updated["grand_total"]})


# ---------------------------------------------------------------------------
# hold-transaction
# ---------------------------------------------------------------------------
def hold_transaction(conn, args):
    txn_id = getattr(args, "pos_transaction_id", None)
    if not txn_id:
        err("--pos-transaction-id is required")

    txn = conn.execute(
        "SELECT id, status FROM pos_transaction WHERE id = ?",
        (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] != "draft":
        err(f"Only draft transactions can be held (current: {txn['status']})")

    conn.execute(
        "UPDATE pos_transaction SET status = 'held', updated_at = datetime('now') WHERE id = ?",
        (txn_id,))
    audit(conn, SKILL, "pos-hold-transaction", "pos_transaction", txn_id)
    conn.commit()
    ok({"id": txn_id, "transaction_status": "held"})


# ---------------------------------------------------------------------------
# resume-transaction
# ---------------------------------------------------------------------------
def resume_transaction(conn, args):
    txn_id = getattr(args, "pos_transaction_id", None)
    if not txn_id:
        err("--pos-transaction-id is required")

    txn = conn.execute(
        "SELECT id, status FROM pos_transaction WHERE id = ?",
        (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] != "held":
        err(f"Only held transactions can be resumed (current: {txn['status']})")

    conn.execute(
        "UPDATE pos_transaction SET status = 'draft', updated_at = datetime('now') WHERE id = ?",
        (txn_id,))
    audit(conn, SKILL, "pos-resume-transaction", "pos_transaction", txn_id)
    conn.commit()
    ok({"id": txn_id, "transaction_status": "draft"})


# ---------------------------------------------------------------------------
# add-payment
# ---------------------------------------------------------------------------
def add_payment(conn, args):
    txn_id = getattr(args, "pos_transaction_id", None)
    payment_method = getattr(args, "payment_method", None) or "cash"
    amount = getattr(args, "amount", None)
    reference = getattr(args, "reference", None)

    if not txn_id:
        err("--pos-transaction-id is required")
    if not amount:
        err("--amount is required")

    valid_methods = ("cash", "card", "mobile", "check", "gift_card", "other")
    if payment_method not in valid_methods:
        err(f"--payment-method must be one of: {', '.join(valid_methods)}")

    txn = conn.execute(
        "SELECT id, status, grand_total FROM pos_transaction WHERE id = ?",
        (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] not in ("draft", "held"):
        err(f"Cannot add payment to transaction in '{txn['status']}' status")

    pay_amt = _dec(amount)
    if pay_amt <= Decimal("0"):
        err("--amount must be positive")

    payment_id = str(uuid.uuid4())
    try:
        conn.execute(
            """INSERT INTO pos_payment
               (id, pos_transaction_id, payment_method, amount, reference)
               VALUES (?,?,?,?,?)""",
            (payment_id, txn_id, payment_method, str(_round(pay_amt)), reference))
    except sqlite3.IntegrityError as e:
        sys.stderr.write(f"[{SKILL}] {e}\n")
        err("Failed to add payment")

    # Update paid_amount on transaction
    total_paid = conn.execute(
        "SELECT COALESCE(SUM(CAST(amount AS REAL)), 0) as total FROM pos_payment WHERE pos_transaction_id = ?",
        (txn_id,)).fetchone()
    paid = str(_round(_dec(total_paid["total"])))
    conn.execute(
        "UPDATE pos_transaction SET paid_amount = ?, updated_at = datetime('now') WHERE id = ?",
        (paid, txn_id))

    conn.commit()
    ok({"payment_id": payment_id, "payment_method": payment_method,
        "payment_amount": str(_round(pay_amt)), "total_paid": paid,
        "grand_total": txn["grand_total"]})


# ---------------------------------------------------------------------------
# submit-transaction
# ---------------------------------------------------------------------------
def submit_transaction(conn, args):
    txn_id = getattr(args, "pos_transaction_id", None)
    if not txn_id:
        err("--pos-transaction-id is required")

    txn = conn.execute(
        "SELECT * FROM pos_transaction WHERE id = ?", (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] not in ("draft", "held"):
        err(f"Only draft/held transactions can be submitted (current: {txn['status']})")

    grand_total = _dec(txn["grand_total"])
    paid_amount = _dec(txn["paid_amount"])

    if paid_amount < grand_total:
        err(f"Insufficient payment: paid {paid_amount}, required {grand_total}")

    change = _round(paid_amount - grand_total)

    # Use the transaction naming series as receipt number
    receipt_number = txn["naming_series"] or txn_id[:8]

    # Submit in a single transaction
    conn.execute(
        """UPDATE pos_transaction
           SET change_amount = ?, receipt_number = ?, status = 'submitted',
               updated_at = datetime('now')
           WHERE id = ?""",
        (str(change), receipt_number, txn_id))

    # Best-effort cross-skill: create sales invoice
    invoice_id = None
    try:
        # Check if selling skill tables exist
        has_invoice = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sales_invoice'"
        ).fetchone()
        if has_invoice:
            invoice_id = str(uuid.uuid4())
            inv_naming = get_next_name(conn, "sales_invoice")
            conn.execute(
                """INSERT INTO sales_invoice
                   (id, naming_series, customer_id, customer_name,
                    subtotal, discount_amount, tax_amount, grand_total,
                    paid_amount, outstanding_amount, status, company_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (invoice_id, inv_naming, txn["customer_id"], txn["customer_name"],
                 txn["subtotal"], txn["discount_amount"], txn["tax_amount"],
                 txn["grand_total"], txn["paid_amount"], "0", "submitted",
                 txn["company_id"]))
            conn.execute(
                "UPDATE pos_transaction SET sales_invoice_id = ? WHERE id = ?",
                (invoice_id, txn_id))
    except Exception:
        # Cross-skill invoice creation is best-effort
        invoice_id = None

    audit(conn, SKILL, "pos-submit-transaction", "pos_transaction", txn_id,
          new_values={"receipt_number": receipt_number, "change_amount": str(change)})
    conn.commit()

    result = {"id": txn_id, "transaction_status": "submitted",
              "receipt_number": receipt_number, "grand_total": str(grand_total),
              "paid_amount": str(paid_amount), "change_amount": str(change)}
    if invoice_id:
        result["sales_invoice_id"] = invoice_id
    ok(result)


# ---------------------------------------------------------------------------
# void-transaction
# ---------------------------------------------------------------------------
def void_transaction(conn, args):
    txn_id = getattr(args, "pos_transaction_id", None)
    if not txn_id:
        err("--pos-transaction-id is required")

    txn = conn.execute(
        "SELECT id, status FROM pos_transaction WHERE id = ?",
        (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] == "voided":
        err("Transaction is already voided")
    if txn["status"] == "returned":
        err("Cannot void a returned transaction")

    conn.execute(
        "UPDATE pos_transaction SET status = 'voided', updated_at = datetime('now') WHERE id = ?",
        (txn_id,))
    audit(conn, SKILL, "pos-void-transaction", "pos_transaction", txn_id)
    conn.commit()
    ok({"id": txn_id, "transaction_status": "voided"})


# ---------------------------------------------------------------------------
# return-transaction
# ---------------------------------------------------------------------------
def return_transaction(conn, args):
    txn_id = getattr(args, "pos_transaction_id", None)
    if not txn_id:
        err("--pos-transaction-id is required")

    txn = conn.execute(
        "SELECT * FROM pos_transaction WHERE id = ?", (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] != "submitted":
        err(f"Only submitted transactions can be returned (current: {txn['status']})")

    # Mark original as returned
    conn.execute(
        "UPDATE pos_transaction SET status = 'returned', updated_at = datetime('now') WHERE id = ?",
        (txn_id,))

    # Create negative return transaction
    return_id = str(uuid.uuid4())
    return_naming = get_next_name(conn, "pos_transaction", company_id=txn["company_id"])

    # Negate amounts
    subtotal = str(_round(-_dec(txn["subtotal"])))
    discount_amount = str(_round(-_dec(txn["discount_amount"])))
    tax_amount = str(_round(-_dec(txn["tax_amount"])))
    grand_total = str(_round(-_dec(txn["grand_total"])))

    conn.execute(
        """INSERT INTO pos_transaction
           (id, naming_series, pos_session_id, customer_id, customer_name,
            subtotal, discount_amount, discount_pct, tax_amount,
            grand_total, paid_amount, change_amount, status, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (return_id, return_naming, txn["pos_session_id"],
         txn["customer_id"], txn["customer_name"],
         subtotal, discount_amount, txn["discount_pct"], tax_amount,
         grand_total, grand_total, "0", "returned", txn["company_id"]))

    # Copy items as negatives
    items = conn.execute(
        "SELECT * FROM pos_transaction_item WHERE pos_transaction_id = ?",
        (txn_id,)).fetchall()
    for item in items:
        neg_qty = str(_round(-_dec(item["qty"])))
        neg_amount = str(_round(-_dec(item["amount"])))
        neg_disc = str(_round(-_dec(item["discount_amount"])))
        conn.execute(
            """INSERT INTO pos_transaction_item
               (id, pos_transaction_id, item_id, item_name, item_code, barcode,
                qty, rate, discount_pct, discount_amount, amount, uom)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (str(uuid.uuid4()), return_id, item["item_id"], item["item_name"],
             item["item_code"], item["barcode"], neg_qty, item["rate"],
             item["discount_pct"], neg_disc, neg_amount, item["uom"]))

    # Copy payments as negatives
    payments = conn.execute(
        "SELECT * FROM pos_payment WHERE pos_transaction_id = ?",
        (txn_id,)).fetchall()
    for pay in payments:
        neg_pay = str(_round(-_dec(pay["amount"])))
        conn.execute(
            """INSERT INTO pos_payment
               (id, pos_transaction_id, payment_method, amount, reference)
               VALUES (?,?,?,?,?)""",
            (str(uuid.uuid4()), return_id, pay["payment_method"],
             neg_pay, f"Return of {txn_id}"))

    audit(conn, SKILL, "pos-return-transaction", "pos_transaction", txn_id,
          new_values={"return_transaction_id": return_id})
    conn.commit()
    ok({"original_transaction_id": txn_id, "return_transaction_id": return_id,
        "return_naming_series": return_naming,
        "return_grand_total": grand_total,
        "transaction_status": "returned"})


# ---------------------------------------------------------------------------
# get-transaction
# ---------------------------------------------------------------------------
def get_transaction(conn, args):
    txn_id = getattr(args, "id", None) or getattr(args, "pos_transaction_id", None)
    if not txn_id:
        err("--id is required")

    txn = conn.execute(
        "SELECT * FROM pos_transaction WHERE id = ?", (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")

    data = row_to_dict(txn)
    data["transaction_status"] = data.pop("status", None)

    # Items
    items = conn.execute(
        "SELECT * FROM pos_transaction_item WHERE pos_transaction_id = ? ORDER BY created_at",
        (txn_id,)).fetchall()
    data["items"] = [row_to_dict(i) for i in items]

    # Payments
    payments = conn.execute(
        "SELECT * FROM pos_payment WHERE pos_transaction_id = ? ORDER BY created_at",
        (txn_id,)).fetchall()
    data["payments"] = [row_to_dict(p) for p in payments]

    ok(data)


# ---------------------------------------------------------------------------
# list-transactions
# ---------------------------------------------------------------------------
def list_transactions(conn, args):
    params = []
    where = ["1=1"]

    session_id = getattr(args, "pos_session_id", None)
    status = getattr(args, "status", None)
    company_id = getattr(args, "company_id", None)

    if session_id:
        where.append("t.pos_session_id = ?"); params.append(session_id)
    if status:
        where.append("t.status = ?"); params.append(status)
    if company_id:
        where.append("t.company_id = ?"); params.append(company_id)

    where_clause = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM pos_transaction t WHERE {where_clause}",
        params).fetchone()[0]

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    rows = conn.execute(
        f"""SELECT t.* FROM pos_transaction t
            WHERE {where_clause}
            ORDER BY t.created_at DESC LIMIT ? OFFSET ?""",
        params + [limit, offset]).fetchall()

    transactions = []
    for r in rows:
        d = row_to_dict(r)
        d["transaction_status"] = d.pop("status", None)
        transactions.append(d)

    ok({"transactions": transactions, "total": total,
        "limit": limit, "offset": offset,
        "has_more": offset + limit < total})


# ---------------------------------------------------------------------------
# lookup-item
# ---------------------------------------------------------------------------
def lookup_item(conn, args):
    search = getattr(args, "search", None)
    barcode = getattr(args, "barcode", None)

    if not search and not barcode:
        err("--search or --barcode is required")

    limit = int(getattr(args, "limit", None) or 20)
    results = []

    if barcode:
        # Try item_barcode table first
        try:
            bc_rows = conn.execute(
                """SELECT ib.item_id, ib.barcode, i.item_name, i.item_code
                   FROM item_barcode ib
                   JOIN item i ON ib.item_id = i.id
                   WHERE ib.barcode = ?
                   LIMIT ?""",
                (barcode, limit)).fetchall()
            results.extend([row_to_dict(r) for r in bc_rows])
        except sqlite3.OperationalError:
            pass  # item_barcode table may not exist

        # If no results from barcode table, search item.item_code
        if not results:
            rows = conn.execute(
                "SELECT id as item_id, item_name, item_code FROM item WHERE item_code = ? LIMIT ?",
                (barcode, limit)).fetchall()
            results.extend([row_to_dict(r) for r in rows])

    if search:
        like_term = f"%{search}%"
        rows = conn.execute(
            """SELECT id as item_id, item_name, item_code
               FROM item
               WHERE item_name LIKE ? OR item_code LIKE ?
               LIMIT ?""",
            (like_term, like_term, limit)).fetchall()

        # Deduplicate with any barcode results
        existing_ids = {r.get("item_id") for r in results}
        for r in rows:
            d = row_to_dict(r)
            if d.get("item_id") not in existing_ids:
                results.append(d)
                existing_ids.add(d.get("item_id"))

    ok({"items": results[:limit], "total": len(results[:limit])})


# ---------------------------------------------------------------------------
# generate-receipt
# ---------------------------------------------------------------------------
def generate_receipt(conn, args):
    txn_id = getattr(args, "pos_transaction_id", None)
    if not txn_id:
        err("--pos-transaction-id is required")

    txn = conn.execute(
        "SELECT * FROM pos_transaction WHERE id = ?", (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] not in ("submitted", "returned"):
        err(f"Receipt can only be generated for submitted/returned transactions (current: {txn['status']})")

    items = conn.execute(
        "SELECT item_name, item_code, qty, rate, discount_amount, amount, uom FROM pos_transaction_item WHERE pos_transaction_id = ? ORDER BY created_at",
        (txn_id,)).fetchall()

    payments = conn.execute(
        "SELECT payment_method, amount, reference FROM pos_payment WHERE pos_transaction_id = ? ORDER BY created_at",
        (txn_id,)).fetchall()

    # Get company info
    company = conn.execute(
        "SELECT name as company_name FROM company WHERE id = ?",
        (txn["company_id"],)).fetchone()

    receipt = {
        "receipt_number": txn["receipt_number"],
        "id": txn_id,
        "naming_series": txn["naming_series"],
        "date": txn["created_at"],
        "company_name": company["company_name"] if company else None,
        "customer_name": txn["customer_name"],
        "items": [row_to_dict(i) for i in items],
        "subtotal": txn["subtotal"],
        "discount_pct": txn["discount_pct"],
        "discount_amount": txn["discount_amount"],
        "tax_amount": txn["tax_amount"],
        "grand_total": txn["grand_total"],
        "payments": [row_to_dict(p) for p in payments],
        "paid_amount": txn["paid_amount"],
        "change_amount": txn["change_amount"],
        "item_count": len(items),
    }
    ok(receipt)


# ---------------------------------------------------------------------------
# session-summary
# ---------------------------------------------------------------------------
def session_summary(conn, args):
    session_id = getattr(args, "pos_session_id", None)
    if not session_id:
        err("--pos-session-id is required")

    session = conn.execute(
        "SELECT * FROM pos_session WHERE id = ?", (session_id,)).fetchone()
    if not session:
        err(f"Session {session_id} not found")

    # Transaction breakdown
    txn_stats = conn.execute(
        """SELECT
             status,
             COUNT(*) as count,
             COALESCE(SUM(CAST(grand_total AS REAL)), 0) as total
           FROM pos_transaction
           WHERE pos_session_id = ?
           GROUP BY status""",
        (session_id,)).fetchall()

    breakdown = {}
    total_transactions = 0
    for row in txn_stats:
        breakdown[row["status"]] = {
            "count": row["count"],
            "total": str(_round(_dec(row["total"])))
        }
        total_transactions += row["count"]

    # Payment method breakdown (submitted transactions only)
    pay_stats = conn.execute(
        """SELECT pp.payment_method,
                  COUNT(*) as count,
                  COALESCE(SUM(CAST(pp.amount AS REAL)), 0) as total
           FROM pos_payment pp
           JOIN pos_transaction pt ON pp.pos_transaction_id = pt.id
           WHERE pt.pos_session_id = ? AND pt.status IN ('submitted', 'returned')
           GROUP BY pp.payment_method""",
        (session_id,)).fetchall()

    payment_breakdown = {}
    for row in pay_stats:
        payment_breakdown[row["payment_method"]] = {
            "count": row["count"],
            "total": str(_round(_dec(row["total"])))
        }

    # Top items
    top_items = conn.execute(
        """SELECT ti.item_name, ti.item_code,
                  SUM(CAST(ti.qty AS REAL)) as total_qty,
                  SUM(CAST(ti.amount AS REAL)) as total_amount
           FROM pos_transaction_item ti
           JOIN pos_transaction pt ON ti.pos_transaction_id = pt.id
           WHERE pt.pos_session_id = ? AND pt.status = 'submitted'
           GROUP BY ti.item_id
           ORDER BY total_qty DESC
           LIMIT 10""",
        (session_id,)).fetchall()

    summary = {
        "session_id": session_id,
        "session_status": session["status"],
        "cashier_name": session["cashier_name"],
        "opening_amount": session["opening_amount"],
        "total_transactions": total_transactions,
        "status_breakdown": breakdown,
        "payment_breakdown": payment_breakdown,
        "top_items": [
            {"item_name": r["item_name"], "item_code": r["item_code"],
             "total_qty": str(_round(_dec(r["total_qty"]))),
             "total_amount": str(_round(_dec(r["total_amount"])))}
            for r in top_items
        ],
    }

    if session["status"] in ("closed", "reconciled"):
        summary["closing_amount"] = session["closing_amount"]
        summary["expected_amount"] = session["expected_amount"]
        summary["difference"] = session["difference"]
        summary["total_sales"] = session["total_sales"]
        summary["total_returns"] = session["total_returns"]

    ok(summary)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
def pos_status(conn, args):
    # Count profiles, sessions, transactions
    profiles = conn.execute("SELECT COUNT(*) FROM pos_profile").fetchone()[0]
    open_sessions = conn.execute(
        "SELECT COUNT(*) FROM pos_session WHERE status = 'open'").fetchone()[0]
    today_txns = conn.execute(
        "SELECT COUNT(*) FROM pos_transaction WHERE date(created_at) = date('now')").fetchone()[0]
    today_sales = conn.execute(
        """SELECT COALESCE(SUM(CAST(grand_total AS REAL)), 0)
           FROM pos_transaction
           WHERE date(created_at) = date('now') AND status = 'submitted'""").fetchone()[0]

    ok({
        "skill": "erpclaw-pos",
        "version": "1.0.0",
        "profiles": profiles,
        "open_sessions": open_sessions,
        "today_transactions": today_txns,
        "today_sales": str(_round(_dec(today_sales))),
        "domains": ["profiles", "sessions", "transactions", "reports"],
    })


# ---------------------------------------------------------------------------
# Action Router
# ---------------------------------------------------------------------------
ACTIONS = {
    "pos-add-transaction": add_transaction,
    "pos-add-transaction-item": add_transaction_item,
    "pos-remove-transaction-item": remove_transaction_item,
    "pos-apply-discount": apply_discount,
    "pos-hold-transaction": hold_transaction,
    "pos-resume-transaction": resume_transaction,
    "pos-add-payment": add_payment,
    "pos-submit-transaction": submit_transaction,
    "pos-void-transaction": void_transaction,
    "pos-return-transaction": return_transaction,
    "pos-get-transaction": get_transaction,
    "pos-list-transactions": list_transactions,
    "pos-lookup-item": lookup_item,
    "pos-generate-receipt": generate_receipt,
    "pos-session-summary": session_summary,
    "status": pos_status,
}
