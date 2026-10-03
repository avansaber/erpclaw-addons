#!/usr/bin/env python3
"""erpclaw-pos transactions domain module.

Transaction lifecycle — create, add items, apply discounts, hold/resume,
add payments, submit, void, return. Plus item lookup, receipts, and session
summary. Imported by the unified erpclaw-pos db_query.py router.
"""
import json
import os
import sys
import uuid
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

import importlib.util
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib import cross_skill
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit
from erpclaw_lib.db import DEFAULT_DB_PATH
from erpclaw_lib.db import get_dialect
from erpclaw_lib.db import integrity_error_types
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row, dynamic_update, now

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


def _fmt(val):
    """Short exact text for refusal messages: rounded money, no padding."""
    return format(_dec(val).normalize(), "f")


def _exact_total(rows, column):
    """Sum a fetched money column exactly with Decimal, never float."""
    return sum((_dec(r[column]) for r in rows), Decimal("0"))


def _is_return_document(status, grand_total) -> bool:
    """A return document is a returned row whose stored total is signed.

    The original sale keeps status returned with its unsigned total and
    counts as a sale; only the negated return document (including -0.00)
    counts as the return, reported as a positive figure."""
    return status == "returned" and _dec(grand_total).is_signed()


def is_cancelled_outside_pos(conn, txn_row) -> bool:
    """A submitted sale cancelled outside POS: its invoice or any receipt is cancelled.

    Same rule as close_session's voiding list. Reads only (PyPika, bound
    parameters), never writes; finish the row with pos-void-transaction."""
    try:
        status = txn_row["status"]
    except (KeyError, TypeError, IndexError):
        return False
    if status != "submitted":
        return False
    try:
        invoice_id = txn_row["sales_invoice_id"]
    except (KeyError, TypeError, IndexError):
        invoice_id = None
    if invoice_id:
        inv = conn.execute(Q.from_(Table("sales_invoice")).select(Field("status")).where(Field("id") == P()).get_sql(), (invoice_id,)).fetchone()
        if inv is not None and inv["status"] == "cancelled":
            return True
    try:
        txn_id = txn_row["id"]
    except (KeyError, TypeError, IndexError):
        return False
    pays = conn.execute(Q.from_(Table("pos_payment")).select(Field("payment_entry_id")).where(Field("pos_transaction_id") == P()).get_sql(), (txn_id,)).fetchall()
    for pay in pays:
        pe_id = pay["payment_entry_id"]
        if not pe_id:
            continue
        pe = conn.execute(Q.from_(Table("payment_entry")).select(Field("status")).where(Field("id") == P()).get_sql(), (pe_id,)).fetchone()
        if pe is not None and pe["status"] == "cancelled":
            return True
    return False


def _child_db_path(args):
    """Database pointer forwarded to owning-module child processes.

    On PostgreSQL an explicit --db-path would outrank ERPCLAW_DB_URL, so the
    child gets None and resolves the URL itself; on SQLite the file path.
    """
    if get_dialect() == "postgresql":
        return None
    return getattr(args, "db_path", None) or os.environ.get("ERPCLAW_DB_PATH") or None


def _line_amounts(qty, rate, disc_pct):
    """POS line amounts under the selling formula: eff = round(rate*(1-pct/100)).

    Returns (effective rate, amount, discount amount)."""
    if disc_pct > Decimal("0"):
        eff = _round(rate * (Decimal("1") - disc_pct / Decimal("100")))
    else:
        eff = rate
    line_subtotal = _round(qty * rate)
    amount = _round(qty * eff)
    return eff, amount, _round(line_subtotal - amount)


def _posting_guard(conn, txn_id):
    """Refuse cart edits while a sale is mid-chain (invoice linked, not submitted)."""
    row = conn.execute(Q.from_(Table("pos_transaction")).select(Field("id"), Field("status"), Field("sales_invoice_id")).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if row is not None and row["status"] in ("draft", "held") and row["sales_invoice_id"]:
        err(f"Transaction {txn_id} is being posted (sales invoice {row['sales_invoice_id']}); finish with pos-submit-transaction")
    return row


def _recalc_totals(conn, txn_id):
    """Recalculate subtotal and grand_total from line items and transaction-level discount."""
    items = conn.execute(Q.from_(Table("pos_transaction_item")).select(Field('amount')).where(Field("pos_transaction_id") == P()).get_sql(), (txn_id,)).fetchall()
    subtotal = _round(sum((_dec(r["amount"]) for r in items), Decimal("0")))

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Field('discount_pct'), Field('discount_amount'), Field('tax_amount')).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()

    disc_pct = _dec(txn["discount_pct"])
    disc_amt = _dec(txn["discount_amount"])
    tax_amt = _dec(txn["tax_amount"])

    # If discount_pct > 0, recalculate discount_amount from subtotal
    if disc_pct > Decimal("0"):
        disc_amt = _round(subtotal * disc_pct / Decimal("100"))

    grand_total = _round(subtotal - disc_amt + tax_amt)
    if grand_total < Decimal("0"):
        grand_total = Decimal("0")

    sql, upd_params = dynamic_update("pos_transaction", {
        "subtotal": str(subtotal), "discount_amount": str(disc_amt),
        "grand_total": str(grand_total), "updated_at": now(),
    }, {"id": txn_id})
    conn.execute(sql, upd_params)
    return subtotal, disc_amt, grand_total


# ---------------------------------------------------------------------------
# add-transaction
# ---------------------------------------------------------------------------
def add_transaction(conn, args):
    session_id = getattr(args, "pos_session_id", None)
    if not session_id:
        err("--pos-session-id is required")

    session = conn.execute(Q.from_(Table("pos_session")).select(Field('id'), Field('company_id'), Field('status')).where(Field("id") == P()).get_sql(), (session_id,)).fetchone()
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
        sql, _ = insert_row("pos_transaction", {"id": P(), "naming_series": P(), "pos_session_id": P(), "customer_id": P(), "customer_name": P(), "subtotal": P(), "discount_amount": P(), "discount_pct": P(), "tax_amount": P(), "grand_total": P(), "paid_amount": P(), "change_amount": P(), "status": P(), "company_id": P()})
        conn.execute(sql,
            (txn_id, naming, session_id, customer_id, customer_name,
             "0", "0", "0", "0", "0", "0", "0", "draft", company_id))
    except integrity_error_types() as e:
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

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Field('id'), Field('status'), Field('return_against_id')).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] == "draft" and txn["return_against_id"]:
        err(f"Transaction {txn_id} is a return document; finish with pos-return-transaction")
    if txn["status"] not in ("draft", "held"):
        err(f"Cannot add items to transaction in '{txn['status']}' status")
    _posting_guard(conn, txn_id)

    # Validate item exists
    item = conn.execute(Q.from_(Table("item")).select(Field('id'), Field('item_name'), Field('item_code')).where(Field("id") == P()).get_sql(), (item_id,)).fetchone()
    if not item:
        err(f"Item {item_id} not found")

    item_name = getattr(args, "item_name", None) or item["item_name"]
    item_code = item["item_code"]

    # Barcode is caller-supplied. The `item_barcode` fast-lookup table exists in
    # no schema today (F19: created by no init/migration); its lookup here was
    # dead code behind a live fallback. The real table is a Phase-3 (P3-1) build.
    barcode = getattr(args, "barcode", None)

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

    _eff, amount, disc_amt = _line_amounts(qty, rate, disc_pct)

    line_id = str(uuid.uuid4())
    try:
        sql, _ = insert_row("pos_transaction_item", {"id": P(), "pos_transaction_id": P(), "item_id": P(), "item_name": P(), "item_code": P(), "barcode": P(), "qty": P(), "rate": P(), "discount_pct": P(), "discount_amount": P(), "amount": P(), "uom": P()})
        conn.execute(sql,
            (line_id, txn_id, item_id, item_name, item_code, barcode,
             str(qty), str(rate), str(disc_pct), str(disc_amt), str(amount), uom))
    except integrity_error_types() as e:
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

    line = conn.execute(Q.from_(Table("pos_transaction_item")).select(Field('id'), Field('pos_transaction_id')).where(Field("id") == P()).get_sql(), (line_id,)).fetchone()
    if not line:
        err(f"Transaction item {line_id} not found")

    txn_id = line["pos_transaction_id"]

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Field('id'), Field('status'), Field('return_against_id')).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if txn["status"] == "draft" and txn["return_against_id"]:
        err(f"Transaction {txn_id} is a return document; finish with pos-return-transaction")
    if txn["status"] not in ("draft", "held"):
        err(f"Cannot remove items from transaction in '{txn['status']}' status")
    _posting_guard(conn, txn_id)

    full = conn.execute(Q.from_(Table("pos_transaction")).select(Field('discount_pct'), Field('discount_amount')).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if _dec(full["discount_pct"]) == Decimal("0") and _dec(full["discount_amount"]) > Decimal("0"):
        siblings = conn.execute(Q.from_(Table("pos_transaction_item")).select(Field('id'), Field('amount')).where(Field("pos_transaction_id") == P()).get_sql(), (txn_id,)).fetchall()
        new_subtotal = _round(sum((_dec(r["amount"]) for r in siblings if r["id"] != line_id), Decimal("0")))
        if _dec(full["discount_amount"]) > new_subtotal:
            err(f"Removing line {line_id} leaves discount {str(_round(_dec(full['discount_amount'])))} above subtotal {str(new_subtotal)}; lower the discount first")

    conn.execute(Q.from_(Table("pos_transaction_item")).delete().where(Field("id") == P()).get_sql(), (line_id,))
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

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Table("pos_transaction").star).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] == "draft" and txn["return_against_id"]:
        err(f"Transaction {txn_id} is a return document; finish with pos-return-transaction")
    if txn["status"] not in ("draft", "held"):
        err(f"Cannot apply discount to transaction in '{txn['status']}' status")
    _posting_guard(conn, txn_id)

    disc_pct = getattr(args, "discount_pct", None)
    disc_amt = getattr(args, "discount_amount", None)

    if disc_pct is None and disc_amt is None:
        err("--discount-pct or --discount-amount is required")

    # Check profile discount rules
    session = conn.execute(Q.from_(Table("pos_session")).select(Field('pos_profile_id')).where(Field("id") == P()).get_sql(), (txn["pos_session_id"],)).fetchone()
    if session:
        profile = conn.execute(Q.from_(Table("pos_profile")).select(Field('allow_discount'), Field('max_discount_pct')).where(Field("id") == P()).get_sql(), (session["pos_profile_id"],)).fetchone()
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
        sql, upd_params = dynamic_update("pos_transaction", {
            "discount_pct": str(pct_val), "discount_amount": str(computed_amt),
            "grand_total": str(_round(subtotal - computed_amt + _dec(txn["tax_amount"]))),
            "updated_at": now(),
        }, {"id": txn_id})
        conn.execute(sql, upd_params)
    else:
        amt_val = _dec(disc_amt)
        if amt_val < Decimal("0"):
            err("--discount-amount must be non-negative")
        if amt_val > subtotal:
            err(f"--discount-amount {str(_round(amt_val))} exceeds subtotal {str(_round(subtotal))}")
        sql, upd_params = dynamic_update("pos_transaction", {
            "discount_pct": "0", "discount_amount": str(_round(amt_val)),
            "grand_total": str(_round(subtotal - amt_val + _dec(txn["tax_amount"]))),
            "updated_at": now(),
        }, {"id": txn_id})
        conn.execute(sql, upd_params)

    conn.commit()
    updated = conn.execute(Q.from_(Table("pos_transaction")).select(Field('subtotal'), Field('discount_pct'), Field('discount_amount'), Field('grand_total')).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
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

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Field('id'), Field('status'), Field('return_against_id')).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] == "draft" and txn["return_against_id"]:
        err(f"Transaction {txn_id} is a return document; finish with pos-return-transaction")
    if txn["status"] != "draft":
        err(f"Only draft transactions can be held (current: {txn['status']})")
    _posting_guard(conn, txn_id)

    sql, upd_params = dynamic_update("pos_transaction",
        {"status": "held", "updated_at": now()}, {"id": txn_id})
    conn.execute(sql, upd_params)
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

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Field('id'), Field('status'), Field('return_against_id')).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] == "draft" and txn["return_against_id"]:
        err(f"Transaction {txn_id} is a return document; finish with pos-return-transaction")
    if txn["status"] != "held":
        err(f"Only held transactions can be resumed (current: {txn['status']})")
    _posting_guard(conn, txn_id)

    sql, upd_params = dynamic_update("pos_transaction",
        {"status": "draft", "updated_at": now()}, {"id": txn_id})
    conn.execute(sql, upd_params)
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

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Field('id'), Field('status'), Field('grand_total'), Field('return_against_id')).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] == "draft" and txn["return_against_id"]:
        err(f"Transaction {txn_id} is a return document; finish with pos-return-transaction")
    if txn["status"] not in ("draft", "held"):
        err(f"Cannot add payment to transaction in '{txn['status']}' status")
    _posting_guard(conn, txn_id)

    pay_amt = _dec(amount)
    if pay_amt <= Decimal("0"):
        err("--amount must be positive")

    payment_id = str(uuid.uuid4())
    try:
        sql, _ = insert_row("pos_payment", {"id": P(), "pos_transaction_id": P(), "payment_method": P(), "amount": P(), "reference": P()})
        conn.execute(sql,
            (payment_id, txn_id, payment_method, str(_round(pay_amt)), reference))
    except integrity_error_types() as e:
        sys.stderr.write(f"[{SKILL}] {e}\n")
        err("Failed to add payment")

    # Update paid_amount on transaction. Money is exact text: fetch the rows
    # and total them in Python with Decimal rather than summing a numeric
    # cast in SQL.
    pp = Table("pos_payment")
    q = Q.from_(pp).select(pp.amount).where(pp.pos_transaction_id == P())
    paid = str(_round(_exact_total(
        conn.execute(q.get_sql(), (txn_id,)).fetchall(), "amount")))
    sql, upd_params = dynamic_update("pos_transaction",
        {"paid_amount": paid, "updated_at": now()}, {"id": txn_id})
    conn.execute(sql, upd_params)

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

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Table("pos_transaction").star).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] == "draft" and txn["return_against_id"]:
        err(f"Transaction {txn_id} is a return document; finish with pos-return-transaction")
    if txn["status"] not in ("draft", "held"):
        err(f"Only draft/held transactions can be submitted (current: {txn['status']})")

    grand_total = _dec(txn["grand_total"])
    paid_amount = _dec(txn["paid_amount"])

    if paid_amount < grand_total:
        err(f"Insufficient payment: paid {paid_amount}, required {grand_total}")

    if not txn["customer_id"]:
        err("POS submit needs a customer on the transaction; set --customer-id before submitting (no walk-in default customer is configured)")

    # ---- submit pre-checks a-f: pure reads, in order, before any child call ----
    tax_check = _dec(txn["tax_amount"])
    if tax_check != Decimal("0"):
        err(f"POS submit refused: transaction carries tax {str(_round(tax_check))} but POS posts no tax")

    line_rows = conn.execute(Q.from_(Table("pos_transaction_item")).select(Table("pos_transaction_item").star).where(Field("pos_transaction_id") == P()).get_sql(), (txn_id,)).fetchall()
    for line in line_rows:
        _want_eff, want_amount, _want_disc = _line_amounts(_dec(line["qty"]), _dec(line["rate"]), _dec(line["discount_pct"]))
        if _dec(line["amount"]) != want_amount:
            err(f"POS submit refused: line {line['id']} amount {str(_round(_dec(line['amount'])))} does not match {str(want_amount)}; remove and re-add line {line['id']}")

    lines_total = _round(sum((_dec(r["amount"]) for r in line_rows), Decimal("0")))
    disc_total = _dec(txn["discount_amount"])
    computed_total = _round(lines_total - disc_total)
    if computed_total != _round(grand_total):
        err(f"POS submit refused: POS total {str(_round(grand_total))} does not match invoice total {str(computed_total)}")

    pay_rows = conn.execute(Q.from_(Table("pos_payment")).select(Table("pos_payment").star).where(Field("pos_transaction_id") == P()).get_sql(), (txn_id,)).fetchall()
    for prow in pay_rows:
        if prow["payment_method"] in ("gift_card", "other"):
            err(f"POS submit refused: no ledger account is mapped for payment method {prow['payment_method']}")

    change = _round(paid_amount - grand_total)

    cash_tendered = _round(sum((_dec(r["amount"]) for r in pay_rows if r["payment_method"] == "cash"), Decimal("0")))
    if change > cash_tendered:
        err(f"POS submit refused: change {str(change)} exceeds cash tendered {str(cash_tendered)}")

    method_totals = {}
    for prow in pay_rows:
        method_totals[prow["payment_method"]] = _round(method_totals.get(prow["payment_method"], Decimal("0")) + _dec(prow["amount"]))
    posted = {}
    for _method, _total in method_totals.items():
        posted[_method] = _round(_total - change) if _method == "cash" else _total
    company = conn.execute(Q.from_(Table("company")).select(Field("default_cash_account_id"), Field("default_bank_account_id")).where(Field("id") == P()).get_sql(), (txn["company_id"],)).fetchone()
    if company is None:
        err(f"Company {txn['company_id']} not found")
    account_of = {}
    for _method, _net_amount in posted.items():
        if _net_amount == Decimal("0"):
            continue
        if _method == "cash":
            _acct = company["default_cash_account_id"]
            _kind = "cash"
        else:
            _acct = company["default_bank_account_id"]
            _kind = "bank"
        if not _acct:
            err(f"POS submit refused: company has no default {_kind} account for {_method} payments")
        account_of[_method] = _acct

    # Use the transaction naming series as receipt number
    receipt_number = txn["naming_series"] or txn_id[:8]

    db_path = _child_db_path(args)
    posting_date = datetime.now().strftime("%Y-%m-%d")

    invoice_id = txn["sales_invoice_id"]

    def _chain_ids():
        held = []
        if invoice_id:
            held.append(invoice_id)
        current = conn.execute(Q.from_(Table("pos_payment")).select(Field("payment_method"), Field("payment_entry_id")).where(Field("pos_transaction_id") == P()).get_sql(), (txn_id,)).fetchall()
        for _m in ("cash", "card", "mobile", "check"):
            for _r in current:
                if _r["payment_method"] == _m and _r["payment_entry_id"] and _r["payment_entry_id"] not in held:
                    held.append(_r["payment_entry_id"])
                    break
        return held

    def _stopped(step, error):
        held = _chain_ids()
        text = ", ".join(held) if held else "none"
        err(f"POS submit stopped at {step}: {error}. Done so far: {text}. Retry the same action to finish")

    # 1. create the sales invoice once; the transaction discount rides as one
    # negative line. The id is recorded before anything else can fail.
    if not invoice_id:
        # Each line rides at its effective rate with no line percentage: the
        # POS amount then equals the invoice amount and the net, so the
        # invoice total matches the POS total line for line and the ledger
        # stays total-consistent. For pct 0 this is byte-identical to the
        # historic contract.
        items = []
        for line in line_rows:
            eff_rate, _line_amount, _line_disc = _line_amounts(
                _dec(line["qty"]), _dec(line["rate"]),
                _dec(line["discount_pct"]))
            items.append({
                "item_id": line["item_id"],
                "qty": str(line["qty"]),
                "rate": str(eff_rate),
                "uom": line["uom"],
                "description": line["item_name"],
                "discount_percentage": "0",
            })
        if disc_total > Decimal("0"):
            conn.commit()
            try:
                disc_item_id = cross_skill.ensure_service_item(
                    txn["company_id"],
                    item_code=f"POS-DISC-{txn['company_id']}",
                    item_name="POS Transaction Discount",
                    db_path=db_path)
            except cross_skill.CrossSkillError as e:
                _stopped(getattr(e, "action", None) or "add-item", e)
            items.append({
                "item_id": disc_item_id,
                "qty": "1",
                "rate": "-" + str(_round(disc_total)),
                "description": "POS transaction discount",
            })
        conn.commit()
        try:
            created = cross_skill.create_invoice(
                customer_id=txn["customer_id"], items=items,
                company_id=txn["company_id"], posting_date=posting_date,
                db_path=db_path)
        except cross_skill.CrossSkillError as e:
            _stopped("create-sales-invoice", e)
        found_id = created.get("sales_invoice_id")
        if not found_id and isinstance(created.get("sales_invoice"), dict):
            found_id = created["sales_invoice"].get("id") or created["sales_invoice"].get("sales_invoice_id")
        if not found_id:
            _stopped("create-sales-invoice", cross_skill.CrossSkillError("sales invoice created but no invoice id returned"))
        invoice_id = found_id
        sql, upd_params = dynamic_update("pos_transaction", {
            "sales_invoice_id": invoice_id, "updated_at": now(),
        }, {"id": txn_id})
        conn.execute(sql, upd_params)
        audit(conn, SKILL, "pos-submit-transaction", "pos_transaction", txn_id,
              new_values={"sales_invoice_id": invoice_id})
        conn.commit()

    # 2. submit the invoice unless a previous attempt already did.
    inv = conn.execute(Q.from_(Table("sales_invoice")).select(Field("id"), Field("status"), Field("grand_total"), Field("posting_date"), Field("outstanding_amount")).where(Field("id") == P()).get_sql(), (invoice_id,)).fetchone()
    if not inv:
        err(f"POS submit refused: sales invoice {invoice_id} not found after submit")
    if inv["status"] == "draft":
        if _round(_dec(inv["grand_total"])) != _round(grand_total):
            err(f"POS submit refused: POS total {str(_round(grand_total))} does not match invoice total {str(_round(_dec(inv['grand_total'])))}; draft invoice {invoice_id} is linked and holds no ledger rows")
        conn.commit()
        try:
            cross_skill.submit_invoice(invoice_id, db_path=db_path)
        except cross_skill.CrossSkillError as e:
            _stopped("submit-sales-invoice", e)

    # 3. one receipt per tender method, allocated to the invoice.
    inv_live = conn.execute(Q.from_(Table("sales_invoice")).select(Field("posting_date")).where(Field("id") == P()).get_sql(), (invoice_id,)).fetchone()
    ple_rows = conn.execute(Q.from_(Table("payment_ledger_entry")).select(Field("account_id")).where(Field("voucher_type") == P()).where(Field("voucher_id") == P()).where(Field("delinked") == P()).get_sql(), ("sales_invoice", invoice_id, 0)).fetchall()
    if not ple_rows:
        err(f"POS submit refused: sales invoice {invoice_id} has no payment ledger row")
    ple_account = ple_rows[0]["account_id"]
    invoice_posting_date = inv_live["posting_date"]

    pay_rows = conn.execute(Q.from_(Table("pos_payment")).select(Table("pos_payment").star).where(Field("pos_transaction_id") == P()).get_sql(), (txn_id,)).fetchall()
    settled = Decimal("0")
    payment_ids = []
    for _m in ("cash", "card", "mobile", "check"):
        rows_m = [r for r in pay_rows if r["payment_method"] == _m]
        if not rows_m:
            continue
        _due = posted.get(_m, Decimal("0"))
        if _due == Decimal("0"):
            continue
        have = sorted({r["payment_entry_id"] for r in rows_m if r["payment_entry_id"]})
        if have:
            for _pe in have:
                pe_row = conn.execute(Q.from_(Table("payment_entry")).select(Field("id"), Field("status")).where(Field("id") == P()).get_sql(), (_pe,)).fetchone()
                if pe_row is None:
                    err(f"POS submit refused: payment entry {_pe} not found")
                if pe_row["status"] == "draft":
                    conn.commit()
                    try:
                        cross_skill.call_skill_action("erpclaw", "submit-payment", {"--payment-entry-id": _pe, "--user-confirmed": None}, db_path=db_path)
                    except cross_skill.CrossSkillError as e:
                        _stopped("submit-payment", e)
            payment_ids.extend([p for p in have if p not in payment_ids])
            settled = _round(settled + _due)
            continue
        conn.commit()
        try:
            made = cross_skill.call_skill_action(
                "erpclaw", "add-payment",
                {"--payment-type": "receive", "--party-type": "customer",
                 "--party-id": txn["customer_id"],
                 "--company-id": txn["company_id"],
                 "--posting-date": invoice_posting_date,
                 "--paid-from-account": ple_account,
                 "--paid-to-account": account_of[_m],
                 "--paid-amount": str(_due),
                 "--allocations": json.dumps([{"voucher_type": "sales_invoice", "voucher_id": invoice_id, "allocated_amount": str(_due)}])},
                db_path=db_path)
        except cross_skill.CrossSkillError as e:
            _stopped("add-payment", e)
        _pe = made.get("payment_entry_id")
        if not _pe:
            _stopped("add-payment", cross_skill.CrossSkillError("payment entry created but no payment entry id returned"))
        for _r in rows_m:
            _sql, _params = dynamic_update("pos_payment", {"payment_entry_id": _pe}, {"id": _r["id"]})
            conn.execute(_sql, _params)
        audit(conn, SKILL, "pos-submit-transaction", "pos_transaction", txn_id,
              new_values={"payment_entry_id": _pe, "payment_method": _m})
        conn.commit()
        try:
            cross_skill.call_skill_action("erpclaw", "submit-payment", {"--payment-entry-id": _pe, "--user-confirmed": None}, db_path=db_path)
        except cross_skill.CrossSkillError as e:
            _stopped("submit-payment", e)
        payment_ids.append(_pe)
        settled = _round(settled + _due)

    # 4. last local transaction: the status flip and audit.
    final_inv = conn.execute(Q.from_(Table("sales_invoice")).select(Field("id"), Field("status"), Field("outstanding_amount")).where(Field("id") == P()).get_sql(), (invoice_id,)).fetchone()
    if not final_inv:
        err(f"POS submit refused: sales invoice {invoice_id} not found after submit")
    discount_line = "-" + str(_round(disc_total)) if disc_total > Decimal("0") else "0.00"
    settled_text = str(_round(settled))
    sql, upd_params = dynamic_update("pos_transaction", {
        "change_amount": str(change), "receipt_number": receipt_number,
        "status": "submitted", "sales_invoice_id": invoice_id,
        "updated_at": now(),
    }, {"id": txn_id})
    conn.execute(sql, upd_params)

    audit(conn, SKILL, "pos-submit-transaction", "pos_transaction", txn_id,
          new_values={"receipt_number": receipt_number, "change_amount": str(change),
                      "sales_invoice_id": invoice_id,
                      "payment_entry_ids": payment_ids,
                      "discount_line_amount": discount_line,
                      "settled_amount": settled_text})
    conn.commit()

    ok({"id": txn_id, "transaction_status": "submitted",
        "receipt_number": receipt_number, "grand_total": str(grand_total),
        "paid_amount": str(paid_amount), "change_amount": str(change),
        "sales_invoice_id": invoice_id,
        "sales_invoice_status": final_inv["status"],
        "sales_invoice_outstanding_amount": str(final_inv["outstanding_amount"]),
        "payment_entry_ids": payment_ids})


# ---------------------------------------------------------------------------
# abandon-posting
# ---------------------------------------------------------------------------
def abandon_posting(conn, args):
    """Abandon a sale whose posting cannot finish.

    Only draft documents, which hold no ledger rows, are deleted, each by the
    module that owns it: draft payment entries through the payments module's
    delete-payment, then the draft sales invoice through selling's
    delete-sales-invoice. A submitted invoice or payment means the chain must
    be finished with pos-submit-transaction, never abandoned. The transaction
    keeps its status, lines and payments and becomes an ordinary unposted sale.
    """
    txn_id = getattr(args, "pos_transaction_id", None)
    if not txn_id:
        err("--pos-transaction-id is required")

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Table("pos_transaction").star).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] == "draft" and txn["return_against_id"]:
        err(f"Transaction {txn_id} is a return document; finish with pos-return-transaction")
    if txn["status"] not in ("draft", "held") or not txn["sales_invoice_id"]:
        err(f"Transaction {txn_id} has no posting to abandon")
    invoice_id = txn["sales_invoice_id"]
    txn_status = txn["status"]

    inv = conn.execute(Q.from_(Table("sales_invoice")).select(Field("id"), Field("status")).where(Field("id") == P()).get_sql(), (invoice_id,)).fetchone()
    if inv is not None and inv["status"] != "draft":
        err(f"Transaction {txn_id} cannot be abandoned: sales invoice {invoice_id} is '{inv['status']}'; finish with pos-submit-transaction")

    pay_rows = conn.execute(Q.from_(Table("pos_payment")).select(Table("pos_payment").star).where(Field("pos_transaction_id") == P()).get_sql(), (txn_id,)).fetchall()
    distinct_pes = []
    for _r in sorted(pay_rows, key=lambda r: r["id"]):
        _pe = _r["payment_entry_id"]
        if _pe and _pe not in distinct_pes:
            distinct_pes.append(_pe)
    for _pe in distinct_pes:
        pe_row = conn.execute(Q.from_(Table("payment_entry")).select(Field("id"), Field("status")).where(Field("id") == P()).get_sql(), (_pe,)).fetchone()
        if pe_row is not None and pe_row["status"] != "draft":
            err(f"Transaction {txn_id} cannot be abandoned: payment {_pe} is '{pe_row['status']}'; finish with pos-submit-transaction")

    db_path = _child_db_path(args)

    def _abandon_stopped(action, error):
        err(f"POS abandon stopped at {action}: {error}. Retry the same action to finish")

    deleted_payments = []
    for _pe in distinct_pes:
        exists = conn.execute(Q.from_(Table("payment_entry")).select(Field("id")).where(Field("id") == P()).get_sql(), (_pe,)).fetchone()
        if exists is not None:
            conn.commit()
            try:
                cross_skill.call_skill_action("erpclaw", "delete-payment", {"--payment-entry-id": _pe, "--user-confirmed": None}, db_path=db_path)
            except cross_skill.CrossSkillError as e:
                _abandon_stopped(getattr(e, "action", None) or "delete-payment", e)
        linked = conn.execute(Q.from_(Table("pos_payment")).select(Field("id"), Field("payment_entry_id")).where(Field("pos_transaction_id") == P()).get_sql(), (txn_id,)).fetchall()
        linked = [r for r in linked if r["payment_entry_id"] == _pe]
        if linked:
            for _lr in linked:
                _sql, _params = dynamic_update("pos_payment", {"payment_entry_id": None}, {"id": _lr["id"]})
                conn.execute(_sql, _params)
            audit(conn, SKILL, "pos-abandon-posting", "pos_transaction", txn_id,
                  old_values={"payment_entry_id": _pe})
            conn.commit()
        if _pe not in deleted_payments:
            deleted_payments.append(_pe)

    current = conn.execute(Q.from_(Table("pos_transaction")).select(Field("sales_invoice_id")).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    current_inv = current["sales_invoice_id"] if current else None
    deleted_invoice = None
    if current_inv:
        inv_exists = conn.execute(Q.from_(Table("sales_invoice")).select(Field("id")).where(Field("id") == P()).get_sql(), (current_inv,)).fetchone()
        if inv_exists is not None:
            conn.commit()
            try:
                cross_skill.call_skill_action("erpclaw", "delete-sales-invoice", {"--sales-invoice-id": current_inv, "--user-confirmed": None}, db_path=db_path)
            except cross_skill.CrossSkillError as e:
                _abandon_stopped(getattr(e, "action", None) or "delete-sales-invoice", e)
        sql, upd_params = dynamic_update("pos_transaction", {
            "sales_invoice_id": None, "updated_at": now(),
        }, {"id": txn_id})
        conn.execute(sql, upd_params)
        audit(conn, SKILL, "pos-abandon-posting", "pos_transaction", txn_id,
              old_values={"sales_invoice_id": current_inv})
        conn.commit()
        deleted_invoice = current_inv

    ok({"id": txn_id, "transaction_status": txn_status,
        "deleted_sales_invoice_id": deleted_invoice,
        "deleted_payment_entry_ids": deleted_payments})


# ---------------------------------------------------------------------------
# void-transaction
# ---------------------------------------------------------------------------
def void_transaction(conn, args):
    txn_id = getattr(args, "pos_transaction_id", None)
    if not txn_id:
        err("--pos-transaction-id is required")

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Field('id'), Field('status'), Field('pos_session_id'), Field('sales_invoice_id'), Field('return_against_id')).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] == "draft" and txn["return_against_id"]:
        err(f"Transaction {txn_id} is a return document; finish with pos-return-transaction")
    if txn["status"] in ("draft", "held"):
        _posting_guard(conn, txn_id)
        sql, upd_params = dynamic_update("pos_transaction",
            {"status": "voided", "updated_at": now()}, {"id": txn_id})
        conn.execute(sql, upd_params)
        audit(conn, SKILL, "pos-void-transaction", "pos_transaction", txn_id)
        conn.commit()
        ok({"id": txn_id, "transaction_status": "voided"})
    if txn["status"] == "voided":
        err("Transaction is already voided")
    if txn["status"] == "returned":
        err("Cannot void a returned transaction")

    # ---- void pre-checks a-d: pure reads, in order, before any child call ----
    session = conn.execute(Q.from_(Table("pos_session")).select(Field('id'), Field('status')).where(Field("id") == P()).get_sql(), (txn["pos_session_id"],)).fetchone()
    if session["status"] != "open":
        err(f"Cannot void: session {txn['pos_session_id']} is {session['status']}; use pos-return-transaction")

    returns = conn.execute(Q.from_(Table("pos_transaction")).select(Field('id')).where(Field("return_against_id") == P()).get_sql(), (txn_id,)).fetchall()
    if returns:
        err(f"Cannot void: transaction {txn_id} has returns; return the remaining items instead")

    invoice_id = txn["sales_invoice_id"]
    if not invoice_id:
        err(f"Transaction {txn_id} has no sales invoice (submitted before POS posted to the ledger); correct it in selling")

    own_rows = conn.execute(Q.from_(Table("pos_payment")).select(Field('payment_entry_id')).where(Field("pos_transaction_id") == P()).get_sql(), (txn_id,)).fetchall()
    own_ids = {r["payment_entry_id"] for r in own_rows if r["payment_entry_id"]}

    allocs = conn.execute(Q.from_(Table("payment_allocation")).select(Field('payment_entry_id')).where(Field("voucher_type") == P()).where(Field("voucher_id") == P()).where(Field("delinked") == P()).get_sql(), ("sales_invoice", invoice_id, 0)).fetchall()
    foreign = set()
    for alloc in allocs:
        alloc_pe = alloc["payment_entry_id"]
        if alloc_pe in own_ids:
            continue
        pe_row = conn.execute(Q.from_(Table("payment_entry")).select(Field('id'), Field('status')).where(Field("id") == P()).get_sql(), (alloc_pe,)).fetchone()
        if pe_row is not None and pe_row["status"] == "submitted":
            foreign.add(alloc_pe)
    if foreign:
        err(f"Cannot void: sales invoice {invoice_id} carries allocations from payments outside this sale ({', '.join(sorted(foreign))})")

    # ---- the chain: each receipt, then the invoice, each through its owner ----
    db_path = _child_db_path(args)

    pay_rows = conn.execute(Q.from_(Table("pos_payment")).select(Field('id'), Field('payment_entry_id')).where(Field("pos_transaction_id") == P()).orderby(Field("created_at")).orderby(Field("id")).get_sql(), (txn_id,)).fetchall()
    ordered = []
    for prow in pay_rows:
        linked = prow["payment_entry_id"]
        if linked and linked not in ordered:
            ordered.append(linked)

    def _cancelled_payments():
        done = []
        for linked in ordered:
            live = conn.execute(Q.from_(Table("payment_entry")).select(Field('id'), Field('status')).where(Field("id") == P()).get_sql(), (linked,)).fetchone()
            if live is not None and live["status"] == "cancelled":
                done.append(linked)
        return done

    def _chain_ids():
        done = _cancelled_payments()
        inv = conn.execute(Q.from_(Table("sales_invoice")).select(Field('id'), Field('status')).where(Field("id") == P()).get_sql(), (invoice_id,)).fetchone()
        if inv is not None and inv["status"] == "cancelled":
            done.append(invoice_id)
        return done

    def _stopped(step, error):
        done = _chain_ids()
        text = ", ".join(done) if done else "none"
        err(f"POS void stopped at {step}: {error}. Done so far: {text}. Retry the same action to finish")

    for linked in ordered:
        live = conn.execute(Q.from_(Table("payment_entry")).select(Field('id'), Field('status')).where(Field("id") == P()).get_sql(), (linked,)).fetchone()
        if live is None or live["status"] != "submitted":
            continue
        conn.commit()
        try:
            cross_skill.call_skill_action("erpclaw", "cancel-payment", {"--payment-entry-id": linked, "--user-confirmed": None}, db_path=db_path)
        except cross_skill.CrossSkillError as e:
            _stopped("cancel-payment", e)

    inv = conn.execute(Q.from_(Table("sales_invoice")).select(Field('id'), Field('status')).where(Field("id") == P()).get_sql(), (invoice_id,)).fetchone()
    if inv is not None and inv["status"] != "cancelled":
        conn.commit()
        try:
            cross_skill.call_skill_action("erpclaw", "cancel-sales-invoice", {"--sales-invoice-id": invoice_id, "--user-confirmed": None}, db_path=db_path)
        except cross_skill.CrossSkillError as e:
            _stopped("cancel-sales-invoice", e)

    # 3. last local transaction: the status flip and audit.
    cancelled_pe = _cancelled_payments()
    sql, upd_params = dynamic_update("pos_transaction",
        {"status": "voided", "updated_at": now()}, {"id": txn_id})
    conn.execute(sql, upd_params)
    audit(conn, SKILL, "pos-void-transaction", "pos_transaction", txn_id,
          old_values={"status": "submitted"},
          new_values={"status": "voided",
                      "cancelled_sales_invoice_id": invoice_id,
                      "cancelled_payment_entry_ids": cancelled_pe})
    conn.commit()
    ok({"id": txn_id, "transaction_status": "voided",
        "cancelled_sales_invoice_id": invoice_id,
        "cancelled_payment_entry_ids": cancelled_pe})


# ---------------------------------------------------------------------------
# return-transaction
# ---------------------------------------------------------------------------
def return_transaction(conn, args):
    txn_id = getattr(args, "pos_transaction_id", None)
    if not txn_id:
        err("--pos-transaction-id is required")

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Table("pos_transaction").star).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] != "submitted":
        err(f"Only submitted transactions can be returned (current: {txn['status']})")
    invoice_id = txn["sales_invoice_id"]
    if not invoice_id:
        err(f"Transaction {txn_id} has no sales invoice (submitted before POS posted to the ledger); correct it in selling")

    grand_total = _dec(txn["grand_total"])
    subtotal = _dec(txn["subtotal"])
    discount_total = _dec(txn["discount_amount"])

    ti = Table("pos_transaction_item")
    sale_lines = conn.execute(
        Q.from_(ti).select(ti.star)
        .where(ti.pos_transaction_id == P())
        .orderby(ti.id).get_sql(), (txn_id,)).fetchall()
    sale_line_by_id = {r["id"]: r for r in sale_lines}

    pp = Table("pos_payment")
    sale_pays = conn.execute(
        Q.from_(pp).select(pp.star)
        .where(pp.pos_transaction_id == P()).get_sql(), (txn_id,)).fetchall()
    if grand_total > Decimal("0") and not any(
            r["payment_entry_id"] for r in sale_pays):
        err(f"Return refused: transaction {txn_id} was never settled (no payment entry)")

    sii = Table("sales_invoice_item")
    inv_lines = conn.execute(
        Q.from_(sii).select(sii.star)
        .where(sii.sales_invoice_id == P()).get_sql(), (invoice_id,)).fetchall()
    inv_by_item = {}
    for _il in inv_lines:
        if _il["item_id"] in inv_by_item:
            err(f"Return refused: item {_il['item_id']} appears on more than one invoice line")
        inv_by_item[_il["item_id"]] = _il

    pt = Table("pos_transaction")
    in_progress = conn.execute(
        Q.from_(pt).select(pt.star)
        .where(pt.return_against_id == P())
        .where(pt.status == P())
        .orderby(pt.created_at).orderby(pt.id).get_sql(),
        (txn_id, "draft")).fetchall()
    draft = in_progress[0] if in_progress else None
    raw_items = getattr(args, "items", None)
    if draft is not None and raw_items:
        err(f"Return {draft['id']} is in progress for this sale; call pos-return-transaction without --items to finish it")

    def _prior_docs():
        return conn.execute(
            Q.from_(pt).select(pt.id, pt.discount_amount)
            .where(pt.return_against_id == P())
            .where(pt.status.isin(["draft", "returned"])).get_sql(),
            (txn_id,)).fetchall()

    def _already_by_line():
        docs = _prior_docs()
        ids = [d["id"] for d in docs]
        already = {}
        if not ids:
            return already
        for _doc_id in ids:
            for _l in conn.execute(
                    Q.from_(ti).select(ti.qty, ti.return_against_item_id)
                    .where(ti.pos_transaction_id == P()).get_sql(),
                    (_doc_id,)).fetchall():
                if _l["return_against_item_id"]:
                    already[_l["return_against_item_id"]] = already.get(
                        _l["return_against_item_id"], Decimal("0")) + abs(
                        _dec(_l["qty"]))
        return already

    if draft is not None:
        rid = draft["id"]
        return_naming = draft["naming_series"]
        session_id = draft["pos_session_id"]
        doc_pays = conn.execute(
            Q.from_(pp).select(pp.star)
            .where(pp.pos_transaction_id == P()).get_sql(), (rid,)).fetchall()
        method = doc_pays[0]["payment_method"]
        _company = conn.execute(
            Q.from_(Table("company"))
            .select(Field("default_cash_account_id"),
                    Field("default_bank_account_id"))
            .where(Field("id") == P()).get_sql(),
            (txn["company_id"],)).fetchone()
        if _company is None:
            err(f"Company {txn['company_id']} not found")
        if method == "cash":
            till_acct = _company["default_cash_account_id"]
        else:
            till_acct = _company["default_bank_account_id"]
        if not till_acct:
            _kind = "cash" if method == "cash" else "bank"
            err(f"POS return refused: company has no default {_kind} account for {method} payments")
        returned_net = -_dec(draft["subtotal"])
        share = -_dec(draft["discount_amount"])
        refund = -_dec(draft["grand_total"])
        req = []
        for _dl in conn.execute(
                Q.from_(ti).select(ti.star)
                .where(ti.pos_transaction_id == P())
                .orderby(ti.id).get_sql(), (rid,)).fetchall():
            _sl = sale_line_by_id.get(_dl["return_against_item_id"])
            if _sl is None:
                err(f"POS return refused: return line {_dl['id']} has no sale line link")
            _il = inv_by_item.get(_sl["item_id"])
            _net_rate = _dec(_il["net_amount"]) / _dec(_il["quantity"])
            req.append({"sale_line": _sl, "qty": -_dec(_dl["qty"]),
                        "net_rate": _net_rate})
    else:
        already = _already_by_line()

        def _returnable(_line_id):
            return _dec(sale_line_by_id[_line_id]["qty"]) - already.get(
                _line_id, Decimal("0"))

        if raw_items:
            try:
                _parsed = json.loads(raw_items)
            except (TypeError, ValueError):
                err(f"Invalid JSON for --items: {raw_items}")
            if not isinstance(_parsed, list) or not _parsed:
                err("--items must be a non-empty JSON array")
            specs = _parsed
        else:
            specs = [{"pos_transaction_item_id": _l["id"],
                      "qty": str(_returnable(_l["id"]))}
                     for _l in sale_lines if _returnable(_l["id"]) > 0]
        req = []
        _seen_lines = set()
        for _spec in specs:
            _lid = _spec.get("pos_transaction_item_id") if isinstance(
                _spec, dict) else None
            if not _lid:
                err("--items entries need pos_transaction_item_id and qty")
            if _lid in _seen_lines:
                err(f"Return refused: line {_lid} appears more than once in --items")
            _seen_lines.add(_lid)
            _sl = sale_line_by_id.get(_lid)
            if _sl is None:
                err(f"Line {_lid} does not belong to transaction {txn_id}")
            _raw_qty = _spec.get("qty")
            try:
                _q = Decimal(str(_raw_qty))
            except Exception:
                err(f"Return qty {_raw_qty} for line {_lid} must be positive")
            if _q <= 0:
                err(f"Return qty {_raw_qty} for line {_lid} must be positive")
            _sold = _dec(_sl["qty"])
            _got = already.get(_lid, Decimal("0"))
            _retable = _sold - _got
            if _q > _retable:
                err(f"Return qty {_raw_qty} for line {_lid} exceeds returnable "
                    f"{_fmt(_retable)} (sold {_fmt(_sold)}, already returned {_fmt(_got)})")
            if _sold % 1 != 0 and _q < _retable:
                err(f"Return refused: line {_lid} has a fractional quantity; return the whole line")
            _item = conn.execute(
                Q.from_(Table("item")).select(Field("is_stock_item"))
                .where(Field("id") == P()).get_sql(),
                (_sl["item_id"],)).fetchone()
            if _item is not None and int(_item["is_stock_item"] or 0) == 1:
                err(f"Return refused: {_sl['item_name']} is a stock item; stock returns wait for the selling credit-note valuation fix")
            _il = inv_by_item.get(_sl["item_id"])
            if _il is None:
                err(f"Return refused: line {_lid} has no invoice line for item {_sl['item_id']}")
            req.append({"sale_line": _sl, "qty": _q,
                        "net_rate": _dec(_il["net_amount"]) / _dec(_il["quantity"])})

        session_arg = getattr(args, "pos_session_id", None)
        session_id = session_arg or txn["pos_session_id"]
        sess = conn.execute(
            Q.from_(Table("pos_session")).select(Table("pos_session").star)
            .where(Field("id") == P()).get_sql(), (session_id,)).fetchone()
        if not sess:
            err(f"Session {session_id} not found")
        if sess["company_id"] != txn["company_id"]:
            err(f"Session {session_id} belongs to another company")
        if sess["status"] != "open":
            err(f"Return needs an open session: session {session_id} is {sess['status']}; pass --pos-session-id of an open session")

        valid_methods = ("cash", "card", "mobile", "check", "gift_card", "other")
        method_arg = getattr(args, "refund_method", None)
        if method_arg is not None and method_arg not in valid_methods:
            err(f"--refund-method must be one of: {', '.join(valid_methods)}")
        sale_methods = sorted({r["payment_method"] for r in sale_pays})
        if method_arg is not None:
            method = method_arg
        elif len(sale_methods) == 1:
            method = sale_methods[0]
        else:
            err(f"Return refused: transaction {txn_id} was paid by more than one method ({', '.join(sale_methods)}); pass --refund-method")
        if method in ("gift_card", "other"):
            err(f"POS return refused: no ledger account is mapped for payment method {method}")
        company = conn.execute(
            Q.from_(Table("company"))
            .select(Field("default_cash_account_id"),
                    Field("default_bank_account_id"))
            .where(Field("id") == P()).get_sql(),
            (txn["company_id"],)).fetchone()
        if company is None:
            err(f"Company {txn['company_id']} not found")
        if method == "cash":
            till_acct = company["default_cash_account_id"]
            _kind = "cash"
        else:
            till_acct = company["default_bank_account_id"]
            _kind = "bank"
        if not till_acct:
            err(f"POS return refused: company has no default {_kind} account for {method} payments")

        returned_net = sum((_round(r["qty"] * r["net_rate"]) for r in req),
                           Decimal("0"))
        if discount_total == Decimal("0") or subtotal == Decimal("0"):
            share = Decimal("0")
        else:
            _prev = sum((-_dec(d["discount_amount"]) for d in _prior_docs()),
                        Decimal("0"))
            _wanted = {}
            for r in req:
                _lid = r["sale_line"]["id"]
                _wanted[_lid] = _wanted.get(_lid, Decimal("0")) + r["qty"]
            _open = [(_sold2, _got2) for _lid2 in sale_line_by_id
                     for _sold2 in [_dec(sale_line_by_id[_lid2]["qty"])]
                     for _got2 in [already.get(_lid2, Decimal("0"))]
                     if _sold2 - _got2 - _wanted.get(_lid2, Decimal("0")) != 0]
            if not _open:
                share = discount_total - _prev
            else:
                share = _round(discount_total * returned_net / subtotal)
        refund = returned_net - share
        if refund <= Decimal("0"):
            err(f"Nothing to refund: the returned lines total {str(_round(refund))}")

    db_path = _child_db_path(args)
    today = datetime.now().strftime("%Y-%m-%d")

    def _chain_ids(_rid):
        held = []
        _cur = conn.execute(
            Q.from_(pt).select(pt.sales_invoice_id)
            .where(pt.id == P()).get_sql(), (_rid,)).fetchone()
        if _cur is not None and _cur["sales_invoice_id"]:
            held.append(_cur["sales_invoice_id"])
        for _pr in conn.execute(
                Q.from_(pp).select(pp.payment_entry_id)
                .where(pp.pos_transaction_id == P())
                .orderby(pp.id).get_sql(), (_rid,)).fetchall():
            if _pr["payment_entry_id"] and _pr["payment_entry_id"] not in held:
                held.append(_pr["payment_entry_id"])
        return held

    def _stopped(_rid, step, error):
        held = _chain_ids(_rid)
        text = ", ".join(held) if held else "none"
        err(f"POS return stopped at {step}: {error}. Done so far: {text}. Retry the same action to finish")

    if draft is None:
        rid = str(uuid.uuid4())
        return_naming = get_next_name(conn, "pos_transaction",
                                      company_id=txn["company_id"])
        neg_refund = str(-refund)
        neg_share = "0.00" if share == Decimal("0") else str(-share)
        sql, _ = insert_row("pos_transaction", {"id": P(), "naming_series": P(), "pos_session_id": P(), "customer_id": P(), "customer_name": P(), "subtotal": P(), "discount_amount": P(), "discount_pct": P(), "tax_amount": P(), "grand_total": P(), "paid_amount": P(), "change_amount": P(), "status": P(), "company_id": P(), "return_against_id": P()})
        conn.execute(sql,
            (rid, return_naming, session_id, txn["customer_id"],
             txn["customer_name"], str(-returned_net), neg_share, "0",
             "0.00", neg_refund, neg_refund, "0", "draft",
             txn["company_id"], txn_id))
        for r in req:
            _sl = r["sale_line"]
            _q = r["qty"]
            _line_net = _round(_q * r["net_rate"])
            _line_gross = _round(_q * _dec(_sl["rate"]))
            _line_disc = "0.00" if _line_gross == _line_net else str(
                -(_line_gross - _line_net))
            sql, _ = insert_row("pos_transaction_item", {"id": P(), "pos_transaction_id": P(), "item_id": P(), "item_name": P(), "item_code": P(), "barcode": P(), "qty": P(), "rate": P(), "discount_pct": P(), "discount_amount": P(), "amount": P(), "uom": P(), "return_against_item_id": P()})
            conn.execute(sql,
                (str(uuid.uuid4()), rid, _sl["item_id"], _sl["item_name"],
                 _sl["item_code"], _sl["barcode"], str(_round(-_q)),
                 _sl["rate"], _sl["discount_pct"], _line_disc,
                 str(-_line_net), _sl["uom"], _sl["id"]))
        sql, _ = insert_row("pos_payment", {"id": P(), "pos_transaction_id": P(), "payment_method": P(), "amount": P(), "reference": P()})
        conn.execute(sql,
            (str(uuid.uuid4()), rid, method, neg_refund,
             f"Return of {txn_id}"))
        audit(conn, SKILL, "pos-return-transaction", "pos_transaction", rid,
              new_values={"return_against_id": txn_id})
        conn.commit()

    cur = conn.execute(
        Q.from_(pt).select(pt.sales_invoice_id)
        .where(pt.id == P()).get_sql(), (rid,)).fetchone()
    if cur is not None and cur["sales_invoice_id"]:
        cn = cur["sales_invoice_id"]
    else:
        cn_items = [{"item_id": r["sale_line"]["item_id"],
                     "qty": str(r["qty"]), "rate": str(r["net_rate"])}
                    for r in req]
        if share > Decimal("0"):
            conn.commit()
            try:
                disc_item_id = cross_skill.ensure_service_item(
                    txn["company_id"],
                    item_code=f"POS-DISC-{txn['company_id']}",
                    item_name="POS Transaction Discount",
                    db_path=db_path)
            except cross_skill.CrossSkillError as e:
                _stopped(rid, "create-credit-note", e)
            cn_items.append({"item_id": disc_item_id, "qty": "1",
                             "rate": "-" + str(share)})
        conn.commit()
        try:
            created = cross_skill.call_skill_action(
                "erpclaw", "create-credit-note",
                {"--against-invoice-id": invoice_id,
                 "--posting-date": today,
                 "--reason": f"POS return {rid}",
                 "--items": json.dumps(cn_items)},
                db_path=db_path)
        except cross_skill.CrossSkillError as e:
            _stopped(rid, "create-credit-note", e)
        cn = created.get("credit_note_id")
        if not cn:
            _stopped(rid, "create-credit-note", cross_skill.CrossSkillError(
                "credit note created but no credit note id returned"))
        sql, upd_params = dynamic_update("pos_transaction", {
            "sales_invoice_id": cn, "updated_at": now(),
        }, {"id": rid})
        conn.execute(sql, upd_params)
        audit(conn, SKILL, "pos-return-transaction", "pos_transaction", rid,
              new_values={"sales_invoice_id": cn})
        conn.commit()

    cn_row = conn.execute(
        Q.from_(Table("sales_invoice"))
        .select(Field("id"), Field("status"), Field("grand_total"))
        .where(Field("id") == P()).get_sql(), (cn,)).fetchone()
    if draft is not None and (cn_row is None or cn_row["status"] == "cancelled"):
        _cn_status = cn_row["status"] if cn_row is not None else "missing"
        err(f"POS return refused: credit note {cn} linked to return {rid} is {_cn_status}; correct it in selling")
    if cn_row is not None and cn_row["status"] == "draft":
        if _round(_dec(cn_row["grand_total"])) != _round(-refund):
            err(f"POS return refused: return total {str(_round(-refund))} does not match credit note total {str(_round(_dec(cn_row['grand_total'])))}; draft credit note {cn} is linked and holds no ledger rows")
        conn.commit()
        try:
            cross_skill.submit_invoice(cn, db_path=db_path)
        except cross_skill.CrossSkillError as e:
            _stopped(rid, "submit-sales-invoice", e)

    doc_pays = conn.execute(
        Q.from_(pp).select(pp.star)
        .where(pp.pos_transaction_id == P()).get_sql(), (rid,)).fetchall()
    linked = [r["payment_entry_id"] for r in doc_pays
              if r["payment_entry_id"]]
    pe = linked[0] if linked else None
    if pe is None:
        cn_live = conn.execute(
            Q.from_(Table("sales_invoice")).select(Field("posting_date"))
            .where(Field("id") == P()).get_sql(), (cn,)).fetchone()
        ple = Table("payment_ledger_entry")
        ple_rows = conn.execute(
            Q.from_(ple).select(ple.account_id)
            .where(ple.voucher_type == P())
            .where(ple.voucher_id == P())
            .where(ple.delinked == P()).get_sql(),
            ("credit_note", cn, 0)).fetchall()
        if not ple_rows:
            err(f"POS return refused: credit note {cn} has no payment ledger row")
        ple_account = ple_rows[0]["account_id"]
        conn.commit()
        try:
            made = cross_skill.call_skill_action(
                "erpclaw", "add-payment",
                {"--payment-type": "pay", "--party-type": "customer",
                 "--party-id": txn["customer_id"],
                 "--company-id": txn["company_id"],
                 "--posting-date": cn_live["posting_date"],
                 "--paid-from-account": till_acct,
                 "--paid-to-account": ple_account,
                 "--paid-amount": str(refund),
                 "--allocations": json.dumps(
                     [{"voucher_type": "credit_note", "voucher_id": cn,
                       "allocated_amount": str(refund)}])},
                db_path=db_path)
        except cross_skill.CrossSkillError as e:
            _stopped(rid, "add-payment", e)
        pe = made.get("payment_entry_id")
        if not pe:
            _stopped(rid, "add-payment", cross_skill.CrossSkillError(
                "payment entry created but no payment entry id returned"))
        for _pr in doc_pays:
            _sql, _params = dynamic_update("pos_payment",
                                           {"payment_entry_id": pe},
                                           {"id": _pr["id"]})
            conn.execute(_sql, _params)
        audit(conn, SKILL, "pos-return-transaction", "pos_transaction", rid,
              new_values={"payment_entry_id": pe})
        conn.commit()
    pe_row = conn.execute(
        Q.from_(Table("payment_entry")).select(Field("id"), Field("status"))
        .where(Field("id") == P()).get_sql(), (pe,)).fetchone()
    if pe_row is None:
        if draft is not None:
            err(f"POS return refused: refund {pe} linked to return {rid} is missing; correct it in payments")
        err(f"POS return refused: payment entry {pe} not found")
    if draft is not None and pe_row["status"] == "cancelled":
        err(f"POS return refused: refund {pe} linked to return {rid} is cancelled; correct it in payments")
    if pe_row["status"] == "draft":
        conn.commit()
        try:
            cross_skill.call_skill_action(
                "erpclaw", "submit-payment",
                {"--payment-entry-id": pe, "--user-confirmed": None},
                db_path=db_path)
        except cross_skill.CrossSkillError as e:
            _stopped(rid, "submit-payment", e)
    pe_row = conn.execute(
        Q.from_(Table("payment_entry")).select(Field("id"), Field("status"))
        .where(Field("id") == P()).get_sql(), (pe,)).fetchone()
    if pe_row is None or pe_row["status"] != "submitted":
        _pe_status = pe_row["status"] if pe_row is not None else "missing"
        err(f"POS return refused: refund {pe} linked to return {rid} is {_pe_status}; correct it in payments")

    already = _already_by_line()
    nothing_left = all(
        _dec(_sl["qty"]) - already.get(_sl["id"], Decimal("0")) == 0
        for _sl in sale_lines)
    sale_status = "returned" if nothing_left else "submitted"
    sql, upd_params = dynamic_update("pos_transaction",
        {"status": "returned", "updated_at": now()}, {"id": rid})
    conn.execute(sql, upd_params)
    if nothing_left:
        sql, upd_params = dynamic_update("pos_transaction",
            {"status": "returned", "updated_at": now()}, {"id": txn_id})
        conn.execute(sql, upd_params)
    returned_qty_by_line = {
        _sl["id"]: str(already.get(_sl["id"], Decimal("0")))
        for _sl in sale_lines}
    audit(conn, SKILL, "pos-return-transaction", "pos_transaction", rid,
          new_values={"return_against_id": txn_id, "credit_note_id": cn,
                      "refund_payment_entry_id": pe,
                      "refund_method": method,
                      "grand_total": str(-refund),
                      "discount_share": str(share)})
    audit(conn, SKILL, "pos-return-transaction", "pos_transaction", txn_id,
          old_values={"status": "submitted"},
          new_values={"status": sale_status,
                      "returned_qty_by_line": returned_qty_by_line})
    conn.commit()
    ok({"original_transaction_id": txn_id, "return_transaction_id": rid,
        "return_naming_series": return_naming,
        "return_grand_total": str(-refund),
        "credit_note_id": cn, "refund_payment_entry_id": pe,
        "transaction_status": sale_status})



# ---------------------------------------------------------------------------
# get-transaction
# ---------------------------------------------------------------------------
def get_transaction(conn, args):
    txn_id = getattr(args, "id", None) or getattr(args, "pos_transaction_id", None)
    if not txn_id:
        err("--id is required")

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Table("pos_transaction").star).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")

    data = row_to_dict(txn)
    data["transaction_status"] = data.pop("status", None)

    # Items
    items = conn.execute(Q.from_(Table("pos_transaction_item")).select(Table("pos_transaction_item").star).where(Field("pos_transaction_id") == P()).orderby(Field("created_at")).get_sql(), (txn_id,)).fetchall()
    data["items"] = [row_to_dict(i) for i in items]

    # Payments
    payments = conn.execute(Q.from_(Table("pos_payment")).select(Table("pos_payment").star).where(Field("pos_transaction_id") == P()).orderby(Field("created_at")).get_sql(), (txn_id,)).fetchall()
    data["payments"] = [row_to_dict(p) for p in payments]

    ok(data)


# ---------------------------------------------------------------------------
# list-transactions
# ---------------------------------------------------------------------------
def list_transactions(conn, args):
    t = Table("pos_transaction")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star))
    params = []

    session_id = getattr(args, "pos_session_id", None)
    status = getattr(args, "status", None)
    company_id = getattr(args, "company_id", None)

    if session_id:
        q = q.where(t.pos_session_id == P())
        q_cnt = q_cnt.where(t.pos_session_id == P())
        params.append(session_id)
    if status:
        q = q.where(t.status == P())
        q_cnt = q_cnt.where(t.status == P())
        params.append(status)
    if company_id:
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(company_id)

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]

    limit = int(getattr(args, "limit", None) or 50)
    offset = int(getattr(args, "offset", None) or 0)

    q = q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()

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
        # The `item_barcode` fast-lookup table exists in no schema today (F19:
        # created by no init/migration); the barcode search resolves against
        # item.item_code. The real table is a Phase-3 (P3-1) build.
        if not results:
            t_item = Table("item")
            q_ic = (Q.from_(t_item)
                    .select(t_item.id.as_("item_id"), t_item.item_name, t_item.item_code)
                    .where(t_item.item_code == P()).limit(P()))
            rows = conn.execute(q_ic.get_sql(), (barcode, limit)).fetchall()
            results.extend([row_to_dict(r) for r in rows])

    if search:
        like_term = f"%{search}%"
        t_item = Table("item")
        q_srch = (Q.from_(t_item)
                  .select(t_item.id.as_("item_id"), t_item.item_name, t_item.item_code)
                  .where(t_item.item_name.like(P()) | t_item.item_code.like(P()))
                  .limit(P()))
        rows = conn.execute(q_srch.get_sql(), (like_term, like_term, limit)).fetchall()

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

    txn = conn.execute(Q.from_(Table("pos_transaction")).select(Table("pos_transaction").star).where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()
    if not txn:
        err(f"Transaction {txn_id} not found")
    if txn["status"] not in ("submitted", "returned"):
        err(f"Receipt can only be generated for submitted/returned transactions (current: {txn['status']})")

    items = conn.execute(Q.from_(Table("pos_transaction_item")).select(Field('item_name'), Field('item_code'), Field('qty'), Field('rate'), Field('discount_amount'), Field('amount'), Field('uom')).where(Field("pos_transaction_id") == P()).orderby(Field("created_at")).get_sql(), (txn_id,)).fetchall()

    payments = conn.execute(Q.from_(Table("pos_payment")).select(Field('payment_method'), Field('amount'), Field('reference')).where(Field("pos_transaction_id") == P()).orderby(Field("created_at")).get_sql(), (txn_id,)).fetchall()

    # Get company info
    t_co = Table("company")
    company = conn.execute(
        Q.from_(t_co).select(t_co.name.as_("company_name"))
        .where(t_co.id == P()).get_sql(),
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

    session = conn.execute(Q.from_(Table("pos_session")).select(Table("pos_session").star).where(Field("id") == P()).get_sql(), (session_id,)).fetchone()
    if not session:
        err(f"Session {session_id} not found")

    # Transaction breakdown. Money is exact text: fetch the rows and total
    # them in Python with Decimal rather than summing a numeric cast in SQL.
    # A returned original stays a sale (counted under submitted); only the
    # return document counts under returned, as a positive figure.
    t = Table("pos_transaction")
    tq = (Q.from_(t).select(t.id, t.status, t.grand_total, t.sales_invoice_id)
          .where(t.pos_session_id == P()))
    breakdown = {}
    total_transactions = 0
    voids_to_finish = []
    for row in conn.execute(tq.get_sql(), (session_id,)).fetchall():
        if row["status"] == "submitted" and is_cancelled_outside_pos(conn, row):
            key = "cancelled_outside_pos"
            amount = _dec(row["grand_total"])
            voids_to_finish.append(row["id"])
        elif _is_return_document(row["status"], row["grand_total"]):
            key = "returned"
            amount = abs(_dec(row["grand_total"]))
        elif row["status"] == "returned":
            key = "submitted"
            amount = _dec(row["grand_total"])
        else:
            key = row["status"]
            amount = _dec(row["grand_total"])
        entry = breakdown.setdefault(key, {"count": 0, "total": Decimal("0")})
        entry["count"] += 1
        entry["total"] += amount
        total_transactions += 1
    cancelled_ids = set(voids_to_finish)
    breakdown = {
        status: {"count": entry["count"], "total": str(_round(entry["total"]))}
        for status, entry in breakdown.items()
    }

    # Payment method breakdown (submitted transactions only)
    pp = Table("pos_payment")
    pt = Table("pos_transaction")
    pq = (Q.from_(pp).join(pt).on(pp.pos_transaction_id == pt.id)
          .select(pp.payment_method, pp.amount, pt.status, pt.grand_total,
                  pt.id.as_("pos_transaction_id"))
          .where(pt.pos_session_id == P())
          .where(pt.status.isin(["submitted", "returned"])))
    pay_tally = {}
    for row in conn.execute(pq.get_sql(), (session_id,)).fetchall():
        if row["pos_transaction_id"] in cancelled_ids:
            continue
        entry = pay_tally.setdefault(
            row["payment_method"],
            {"count": 0, "received": Decimal("0"),
             "refunded": Decimal("0")})
        entry["count"] += 1
        if _is_return_document(row["status"], row["grand_total"]):
            entry["refunded"] += _dec(row["amount"])
        else:
            entry["received"] += _dec(row["amount"])
    cq = (Q.from_(pt).select(pt.status, pt.grand_total, pt.change_amount,
                  pt.id.as_("pos_transaction_id"))
          .where(pt.pos_session_id == P())
          .where(pt.status.isin(["submitted", "returned"])))
    change_total = sum(
        (_dec(r["change_amount"]) for r in
         conn.execute(cq.get_sql(), (session_id,)).fetchall()
         if r["pos_transaction_id"] not in cancelled_ids
         and not _is_return_document(r["status"], r["grand_total"])),
        Decimal("0"))
    payment_breakdown = {}
    for method, entry in pay_tally.items():
        received = entry["received"]
        refunded = abs(entry["refunded"])
        change_given = change_total if method == "cash" else Decimal("0")
        total = received - refunded - change_given
        payment_breakdown[method] = {
            "count": entry["count"],
            "received": str(_round(received)),
            "refunded": str(_round(refunded)),
            "change_given": str(_round(change_given)),
            "total": str(_round(total)),
        }

    # Top items on sales only: a returned original stays a sale and only
    # the return document is excluded. Lines are fetched with both statuses
    # and classified in Python so money is never compared in SQL; quantity
    # is totalled in Python here for the same reason.
    ti = Table("pos_transaction_item")
    rq = (Q.from_(ti).join(pt).on(ti.pos_transaction_id == pt.id)
          .select(ti.item_id, ti.item_name, ti.item_code, ti.qty, ti.amount,
                  pt.status, pt.grand_total, pt.id.as_("pos_transaction_id"))
          .where(pt.pos_session_id == P())
          .where(pt.status.isin(["submitted", "returned"])))
    qty = {}
    revenue = {}
    names = {}
    codes = {}
    for r in conn.execute(rq.get_sql(), (session_id,)).fetchall():
        if r["pos_transaction_id"] in cancelled_ids:
            continue
        if _is_return_document(r["status"], r["grand_total"]):
            continue
        item = r["item_id"]
        qty[item] = qty.get(item, Decimal("0")) + _dec(r["qty"])
        revenue[item] = revenue.get(item, Decimal("0")) + _dec(r["amount"])
        if item not in names:
            names[item] = r["item_name"]
            codes[item] = r["item_code"]
    ordered = sorted(
        qty, key=lambda item: (-qty[item], names[item] or "", item or ""))[:10]
    top_items = [
        {"item_name": names[item], "item_code": codes[item],
         "total_qty": str(_round(qty[item])),
         "total_amount": str(_round(revenue.get(item, Decimal("0"))))}
        for item in ordered
    ]

    summary = {
        "session_id": session_id,
        "session_status": session["status"],
        "cashier_name": session["cashier_name"],
        "opening_amount": session["opening_amount"],
        "total_transactions": total_transactions,
        "status_breakdown": breakdown,
        "payment_breakdown": payment_breakdown,
        "top_items": top_items,
    }

    if voids_to_finish:
        summary["voids_to_finish"] = sorted(voids_to_finish)
        summary["next_step"] = "Run pos-void-transaction --id <id> for each transaction in voids_to_finish"

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
    t_prof = Table("pos_profile")
    t_sess = Table("pos_session")
    t_txn = Table("pos_transaction")
    profiles = conn.execute(
        Q.from_(t_prof).select(fn.Count(t_prof.star)).get_sql()).fetchone()[0]
    open_sessions = conn.execute(
        Q.from_(t_sess).select(fn.Count(t_sess.star))
        .where(t_sess.status == "open").get_sql()).fetchone()[0]
    _today = datetime.now().strftime('%Y-%m-%d')
    tq = (Q.from_(t_txn).select(fn.Count(t_txn.star))
          .where(fn.Date(t_txn.created_at) == P()))
    today_txns = conn.execute(tq.get_sql(), (_today,)).fetchone()[0]
    # Money is exact text: fetch the rows and total them in Python with
    # Decimal rather than summing a numeric cast in SQL.
    sq = (Q.from_(t_txn).select(t_txn.grand_total, t_txn.status)
          .where(fn.Date(t_txn.created_at) == P())
          .where(t_txn.status.isin(["submitted", "returned"])))
    today_sales = str(_round(sum(
        (_dec(r["grand_total"])
         for r in conn.execute(sq.get_sql(), (_today,)).fetchall()
         if not _is_return_document(r["status"], r["grand_total"])),
        Decimal("0"))))

    ok({
        "skill": "erpclaw-pos",
        "version": "1.0.0",
        "profiles": profiles,
        "open_sessions": open_sessions,
        "today_transactions": today_txns,
        "today_sales": today_sales,
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
    "pos-abandon-posting": abandon_posting,
    "pos-void-transaction": void_transaction,
    "pos-return-transaction": return_transaction,
    "pos-get-transaction": get_transaction,
    "pos-list-transactions": list_transactions,
    "pos-lookup-item": lookup_item,
    "pos-generate-receipt": generate_receipt,
    "pos-session-summary": session_summary,
    "pos-status": pos_status,
}
