"""ERPClaw Integrations -- M2 bank statement import + matching.

File-import path (OFX / CAMT.053 / MT940 / BAI2) + a configurable matching
engine, owning the bank_statement / bank_statement_line / bank_match_rule
tables. Source-agnostic: the Plaid stub (financial.py) is untouched; 'plaid' is
only a reserved source value.

Design invariants (coding rules):
  - amount is a signed Decimal-as-TEXT, never float.
  - import is parse-fully-then-write-once: a malformed file raises during parse,
    before any row is written, so no partial statement is ever persisted.
  - re-import is idempotent via the (source, bank_account_id, external_id)
    UNIQUE: duplicate lines are skipped, never double-booked.
  - matching only WRITES bank_* tables; it READS gl_entry / account for the
    reconciliation summary (any module may read).
"""
import os
import re
import sys
import uuid
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

try:
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query_helpers import resolve_company_id, resolve_account_by_name
    # All bank-table WRITES live in this foundation lib (the tables are foundation-
    # owned; this module owns the action surface + parsers). Mirrors gl_posting /
    # cwip_posting. Reads (SELECT) stay here.
    from erpclaw_lib import bank_import as bw
except ImportError:
    pass

# Parsers live next to this module (added to sys.path by db_query.py).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parsers  # noqa: E402

SKILL = "erpclaw-integrations"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _verify_account(conn, account_id, company_id):
    row = conn.execute(
        "SELECT id, name, account_type FROM account WHERE id = ? AND company_id = ?",
        (account_id, company_id)).fetchone()
    if not row:
        err(f"Bank account {account_id} not found for this company.")
    return row


def _statement(conn, statement_id):
    if not statement_id:
        err("--statement-id is required")
    row = conn.execute(
        "SELECT * FROM bank_statement WHERE id = ?", (statement_id,)).fetchone()
    if not row:
        err(f"Bank statement {statement_id} not found")
    return row


def _to_dec(s):
    try:
        return Decimal(str(s))
    except (InvalidOperation, ValueError, ArithmeticError):
        return Decimal("0")


# ===========================================================================
# 1. import-bank-statement
# ===========================================================================
def import_bank_statement(conn, args):
    file_path = getattr(args, "file", None)
    if not file_path:
        err("--file is required")
    if not os.path.exists(file_path):
        err(f"File not found: {file_path}")

    company_id = resolve_company_id(conn, getattr(args, "company_id", None),
                                    getattr(args, "company_name", None))
    bank_account_id = resolve_account_by_name(
        conn, company_id,
        getattr(args, "bank_account_id", None),
        getattr(args, "bank_account_name", None), "bank")
    _verify_account(conn, bank_account_id, company_id)

    fmt = (getattr(args, "format", None) or "auto").lower()
    try:
        text = open(file_path, "r", encoding="utf-8", errors="replace").read()
        parsed = parsers.parse(text, fmt)
    except parsers.BankStatementParseError as e:
        # Parse failed BEFORE any write -> no partial statement persisted.
        err(f"Could not parse statement: {e}")
        return

    source = parsed["source"]
    if fmt != "auto" and fmt != source:
        err(f"--format {fmt} does not match detected format {source}")

    lines = parsed["lines"]
    # Idempotency: which external_ids already exist for this (source, account)?
    existing = {r["external_id"] for r in conn.execute(
        "SELECT external_id FROM bank_statement_line "
        "WHERE source = ? AND bank_account_id = ?",
        (source, bank_account_id)).fetchall()}
    new_lines = [ln for ln in lines if ln["external_id"] not in existing]
    skipped = len(lines) - len(new_lines)

    if not new_lines:
        ok({"statement_id": None, "source": source,
            "lines_imported": 0, "lines_skipped_duplicate": skipped,
            "message": "No-op: all lines already imported (idempotent re-import)."})
        return

    # Single transaction: header + new lines, all-or-nothing.
    try:
        statement_id = str(uuid.uuid4())
        now = _now_iso()
        bw.insert_statement(
            conn, statement_id=statement_id, bank_account_id=bank_account_id,
            company_id=company_id, source=source, file_path=file_path,
            period_start=parsed.get("period_start"),
            period_end=parsed.get("period_end"),
            opening_balance=parsed.get("opening_balance"),
            closing_balance=parsed.get("closing_balance"),
            currency=parsed.get("currency"), line_count=len(new_lines),
            imported_at=now, user_id=getattr(args, "user_id", None))
        for ln in new_lines:
            bw.insert_line(
                conn, line_id=str(uuid.uuid4()), statement_id=statement_id,
                bank_account_id=bank_account_id, source=source,
                txn_date=ln["txn_date"], value_date=ln["value_date"],
                amount=ln["amount"], currency=ln["currency"],
                description=ln["description"],
                counterparty_name=ln["counterparty_name"],
                counterparty_account=ln["counterparty_account"],
                reference=ln["reference"], external_id=ln["external_id"])
        audit(conn, SKILL, "integration-import-bank-statement", "bank_statement",
              statement_id, new_values={"source": source, "lines": len(new_lines)})
        conn.commit()
    except Exception as e:
        conn.rollback()
        err(f"Import failed, rolled back (no partial statement written): {e}")
        return

    ok({"statement_id": statement_id, "source": source,
        "bank_account_id": bank_account_id,
        "lines_imported": len(new_lines), "lines_skipped_duplicate": skipped,
        "opening_balance": parsed.get("opening_balance"),
        "closing_balance": parsed.get("closing_balance"),
        "period_start": parsed.get("period_start"),
        "period_end": parsed.get("period_end")})


# ===========================================================================
# 2. list-bank-statements
# ===========================================================================
def list_bank_statements(conn, args):
    # Static, fully-parameterized SQL with NULL-guarded optional filters.
    company_id = getattr(args, "company_id", None)
    account_id = getattr(args, "bank_account_id", None)
    flt = (company_id, company_id, account_id, account_id)
    where = ("(? IS NULL OR company_id = ?) AND "
             "(? IS NULL OR bank_account_id = ?)")
    total = conn.execute(
        "SELECT COUNT(*) FROM bank_statement WHERE " + where, flt).fetchone()[0]
    rows = conn.execute(
        "SELECT * FROM bank_statement WHERE " + where +
        " ORDER BY imported_at DESC LIMIT ? OFFSET ?",
        flt + (args.limit, args.offset)).fetchall()
    ok({"rows": [row_to_dict(r) for r in rows], "total_count": total,
        "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total})


# ===========================================================================
# 3. get-bank-statement
# ===========================================================================
def get_bank_statement(conn, args):
    stmt = _statement(conn, getattr(args, "statement_id", None))
    lines = conn.execute(
        "SELECT * FROM bank_statement_line WHERE bank_statement_id = ? "
        "ORDER BY txn_date, id", (stmt["id"],)).fetchall()
    ok({"statement": row_to_dict(stmt),
        "lines": [row_to_dict(r) for r in lines],
        "line_count": len(lines)})


# ===========================================================================
# 4. archive-bank-statement
# ===========================================================================
def archive_bank_statement(conn, args):
    stmt = _statement(conn, getattr(args, "statement_id", None))
    bw.archive_statement(conn, stmt["id"])
    audit(conn, SKILL, "integration-archive-bank-statement", "bank_statement",
          stmt["id"], new_values={"import_status": "archived"})
    conn.commit()
    ok({"statement_id": stmt["id"], "import_status": "archived"})


# ===========================================================================
# 5. add-bank-match-rule
# ===========================================================================
_VALID_FIELDS = ("description", "counterparty_name", "reference", "amount")
_VALID_OPERATORS = ("equals", "contains", "regex", "amount_range")
_VALID_TARGETS = ("map_to_account", "map_to_vendor", "map_to_customer", "ignore")


def add_bank_match_rule(conn, args):
    company_id = resolve_company_id(conn, getattr(args, "company_id", None),
                                    getattr(args, "company_name", None))
    name = getattr(args, "name", None) or getattr(args, "rule_name", None)
    if not name:
        err("--name is required")
    field = getattr(args, "match_field", None)
    if field not in _VALID_FIELDS:
        err(f"--match-field must be one of: {', '.join(_VALID_FIELDS)}")
    operator = getattr(args, "match_operator", None)
    if operator not in _VALID_OPERATORS:
        err(f"--match-operator must be one of: {', '.join(_VALID_OPERATORS)}")
    value = getattr(args, "match_value", None)
    if not value:
        err("--match-value is required")
    target = getattr(args, "target_action", None)
    if target not in _VALID_TARGETS:
        err(f"--target-action must be one of: {', '.join(_VALID_TARGETS)}")
    if target != "ignore" and not getattr(args, "target_id", None):
        err(f"--target-id is required for target action {target}")
    if operator == "regex":
        try:
            re.compile(value)
        except re.error as e:
            err(f"--match-value is not a valid regex: {e}")

    rule_id = str(uuid.uuid4())
    now = _now_iso()
    priority = int(getattr(args, "priority", None) or 100)
    bw.insert_match_rule(
        conn, rule_id=rule_id, company_id=company_id, name=name,
        match_field=field, match_operator=operator, match_value=value,
        target_action=target, target_id=getattr(args, "target_id", None),
        priority=priority, now=now)
    audit(conn, SKILL, "integration-add-bank-match-rule", "bank_match_rule",
          rule_id, new_values={"name": name, "field": field, "operator": operator})
    conn.commit()
    ok({"id": rule_id, "name": name, "match_field": field,
        "match_operator": operator, "target_action": target, "priority": priority})


# ===========================================================================
# 6. list-bank-match-rules
# ===========================================================================
def list_bank_match_rules(conn, args):
    company_id = getattr(args, "company_id", None)
    raw_active = getattr(args, "is_active", None)
    active = None
    if raw_active is not None:
        active = 0 if str(raw_active) in ("0", "false", "False") else 1
    rows = conn.execute(
        "SELECT * FROM bank_match_rule WHERE "
        "(? IS NULL OR company_id = ?) AND (? IS NULL OR is_active = ?) "
        "ORDER BY priority ASC, created_at ASC",
        (company_id, company_id, active, active)).fetchall()
    ok({"rows": [row_to_dict(r) for r in rows], "count": len(rows)})


# ===========================================================================
# matching engine
# ===========================================================================
def _line_matches(line_row, rule):
    """True if a statement line satisfies a rule's predicate."""
    field = rule["match_field"]
    op = rule["match_operator"]
    value = rule["match_value"]
    cell = line_row[field] if field in line_row.keys() else None
    if op == "amount_range":
        # value "min:max" (inclusive) compared against the signed amount.
        try:
            lo, _, hi = value.partition(":")
            amt = _to_dec(line_row["amount"])
            return _to_dec(lo) <= amt <= _to_dec(hi)
        except Exception:
            return False
    if cell is None:
        return False
    cell = str(cell)
    if op == "equals":
        return cell.strip().lower() == value.strip().lower()
    if op == "contains":
        return value.lower() in cell.lower()
    if op == "regex":
        try:
            return re.search(value, cell) is not None
        except re.error:
            return False
    return False


# ===========================================================================
# 7. auto-match-bank-statement
# ===========================================================================
def auto_match_bank_statement(conn, args):
    stmt = _statement(conn, getattr(args, "statement_id", None))
    rules = conn.execute(
        "SELECT * FROM bank_match_rule WHERE company_id = ? AND is_active = 1 "
        "ORDER BY priority ASC, created_at ASC", (stmt["company_id"],)).fetchall()
    lines = conn.execute(
        "SELECT * FROM bank_statement_line WHERE bank_statement_id = ? "
        "AND match_status = 'unmatched'", (stmt["id"],)).fetchall()

    matched, ignored = 0, 0
    for ln in lines:
        for rule in rules:
            if not _line_matches(ln, rule):
                continue
            if rule["target_action"] == "ignore":
                bw.update_line_match(conn, ln["id"], match_status="ignored",
                                     match_rule_id=rule["id"], match_confidence="1.0")
                ignored += 1
            else:
                gl_id = rule["target_id"] if rule["target_action"] == "map_to_account" else None
                bw.update_line_match(conn, ln["id"], match_status="auto_matched",
                                     match_rule_id=rule["id"],
                                     matched_gl_entry_id=gl_id, match_confidence="1.0")
                matched += 1
            break  # first (highest-priority) matching rule wins
    bw.refresh_statement_status(conn, stmt["id"])
    audit(conn, SKILL, "integration-auto-match-bank-statement", "bank_statement",
          stmt["id"], new_values={"auto_matched": matched, "ignored": ignored})
    conn.commit()
    remaining = conn.execute(
        "SELECT COUNT(*) FROM bank_statement_line WHERE bank_statement_id = ? "
        "AND match_status = 'unmatched'", (stmt["id"],)).fetchone()[0]
    ok({"statement_id": stmt["id"], "auto_matched": matched, "ignored": ignored,
        "rules_evaluated": len(rules), "unmatched_remaining": remaining})


# ===========================================================================
# 8. manual-match-bank-line
# ===========================================================================
def manual_match_bank_line(conn, args):
    line_id = getattr(args, "line_id", None)
    if not line_id:
        err("--line-id is required")
    ln = conn.execute("SELECT * FROM bank_statement_line WHERE id = ?",
                      (line_id,)).fetchone()
    if not ln:
        err(f"Bank statement line {line_id} not found")
    if ln["match_status"] != "unmatched":
        err(f"Line already {ln['match_status']}. Clear it first with "
            "clear-bank-line-match.")
    target = getattr(args, "target_action", None)
    if target not in _VALID_TARGETS:
        err(f"--target-action must be one of: {', '.join(_VALID_TARGETS)}")
    target_id = getattr(args, "target_id", None)
    if target != "ignore" and not target_id:
        err(f"--target-id is required for target action {target}")

    if target == "ignore":
        bw.update_line_match(conn, line_id, match_status="ignored",
                             match_confidence="1.0")
        new_status = "ignored"
    else:
        gl_id = target_id if target == "map_to_account" else None
        bw.update_line_match(conn, line_id, match_status="manual_matched",
                             matched_gl_entry_id=gl_id, match_confidence="1.0")
        new_status = "manual_matched"
    bw.refresh_statement_status(conn, ln["bank_statement_id"])
    audit(conn, SKILL, "integration-manual-match-bank-line", "bank_statement_line",
          line_id, new_values={"match_status": new_status, "target": target})
    conn.commit()
    ok({"line_id": line_id, "match_status": new_status,
        "target_action": target, "target_id": target_id})


# ===========================================================================
# 9. clear-bank-line-match
# ===========================================================================
def clear_bank_line_match(conn, args):
    line_id = getattr(args, "line_id", None)
    if not line_id:
        err("--line-id is required")
    ln = conn.execute("SELECT * FROM bank_statement_line WHERE id = ?",
                      (line_id,)).fetchone()
    if not ln:
        err(f"Bank statement line {line_id} not found")
    bw.clear_line_match(conn, line_id)
    bw.refresh_statement_status(conn, ln["bank_statement_id"])
    audit(conn, SKILL, "integration-clear-bank-line-match", "bank_statement_line",
          line_id, new_values={"match_status": "unmatched"})
    conn.commit()
    ok({"line_id": line_id, "match_status": "unmatched"})


# ===========================================================================
# 10. unmatched-bank-lines
# ===========================================================================
def unmatched_bank_lines(conn, args):
    statement_id = getattr(args, "statement_id", None)
    account_id = getattr(args, "bank_account_id", None)
    rows = conn.execute(
        "SELECT * FROM bank_statement_line WHERE match_status = 'unmatched' "
        "AND (? IS NULL OR bank_statement_id = ?) "
        "AND (? IS NULL OR bank_account_id = ?) "
        "ORDER BY txn_date, id LIMIT ? OFFSET ?",
        (statement_id, statement_id, account_id, account_id,
         args.limit, args.offset)).fetchall()
    total = sum((_to_dec(r["amount"]) for r in rows), Decimal("0"))
    ok({"rows": [row_to_dict(r) for r in rows], "count": len(rows),
        "unmatched_total": str(total)})


# ===========================================================================
# 11. bank-reconciliation-summary
# ===========================================================================
def bank_reconciliation_summary(conn, args):
    company_id = resolve_company_id(conn, getattr(args, "company_id", None),
                                    getattr(args, "company_name", None))
    bank_account_id = resolve_account_by_name(
        conn, company_id,
        getattr(args, "bank_account_id", None),
        getattr(args, "bank_account_name", None), "bank")
    acct = _verify_account(conn, bank_account_id, company_id)
    as_of = getattr(args, "as_of", None) or getattr(args, "end_date", None)

    # Ledger balance = SUM(debit - credit) over non-cancelled GL up to as_of.
    gl_rows = conn.execute(
        "SELECT debit, credit FROM gl_entry WHERE account_id = ? "
        "AND is_cancelled = 0 AND (? IS NULL OR posting_date <= ?)",
        (bank_account_id, as_of, as_of)).fetchall()
    ledger_balance = sum((_to_dec(r["debit"]) - _to_dec(r["credit"]) for r in gl_rows),
                         Decimal("0"))

    # Statement closing balance = latest non-archived statement for the account.
    stmt = conn.execute(
        "SELECT * FROM bank_statement WHERE bank_account_id = ? "
        "AND import_status != 'archived' "
        "AND (? IS NULL OR period_end IS NULL OR period_end <= ?) "
        "ORDER BY period_end DESC, imported_at DESC LIMIT 1",
        (bank_account_id, as_of, as_of)).fetchone()
    statement_balance = _to_dec(stmt["closing_balance"]) if stmt and stmt["closing_balance"] else Decimal("0")

    # Matched vs unmatched line totals across the account's statements.
    line_rows = conn.execute(
        "SELECT match_status, amount FROM bank_statement_line WHERE bank_account_id = ?",
        (bank_account_id,)).fetchall()
    reconciled = sum((_to_dec(r["amount"]) for r in line_rows
                      if r["match_status"] in ("auto_matched", "manual_matched")),
                     Decimal("0"))
    unmatched = sum((_to_dec(r["amount"]) for r in line_rows
                     if r["match_status"] == "unmatched"), Decimal("0"))

    ok({"bank_account_id": bank_account_id,
        "bank_account_name": acct["name"],
        "as_of": as_of,
        "ledger_balance": str(ledger_balance),
        "statement_balance": str(statement_balance),
        "reconciled_balance": str(reconciled),
        "unmatched_total": str(unmatched),
        "difference": str(statement_balance - reconciled)})


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "integration-import-bank-statement": import_bank_statement,
    "integration-list-bank-statements": list_bank_statements,
    "integration-get-bank-statement": get_bank_statement,
    "integration-archive-bank-statement": archive_bank_statement,
    "integration-add-bank-match-rule": add_bank_match_rule,
    "integration-list-bank-match-rules": list_bank_match_rules,
    "integration-auto-match-bank-statement": auto_match_bank_statement,
    "integration-manual-match-bank-line": manual_match_bank_line,
    "integration-clear-bank-line-match": clear_bank_line_match,
    "integration-unmatched-bank-lines": unmatched_bank_lines,
    "integration-bank-reconciliation-summary": bank_reconciliation_summary,
}
