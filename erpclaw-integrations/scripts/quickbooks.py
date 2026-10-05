"""ERPClaw Integrations: QuickBooks trial balance import (v1).

Local-only staging for a caller-supplied QuickBooks trial balance. Version 1
never calls QuickBooks, stores no credentials, and never posts staged rows to
the general ledger: an import batch only proves whether debits tie credits
(`verified` vs `out_of_balance`), and every response carries `posted: false`.

Design invariants (coding rules):
  - money is Decimal-as-TEXT, never float; every amount is quantized to two
    places before it is stored or totalled.
  - import is parse-fully-then-write-once: records are validated before any
    row is written, so a refused import leaves no batch, line, or audit rows.
  - the batch and all of its lines are stored in one transaction.
  - retrieval is company-scoped: a foreign-company batch reads as not found.
"""
import json
import os
import sys
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

try:
    import importlib.util
    if importlib.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Q, P, Table, Field, insert_row
except ImportError:
    pass

SKILL = "erpclaw-integrations"
SOURCE = "quickbooks"
STATUS_VERIFIED = "verified"
STATUS_OUT_OF_BALANCE = "out_of_balance"

_CENT = Decimal("0.01")

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    found = conn.execute(
        Q.from_(Table("company")).select(Field("id")).where(
            Field("id") == P()).get_sql(), (company_id,)).fetchone()
    if not found:
        err(f"Company {company_id} not found")


def _parse_money(raw, label):
    if raw is None:
        err(f"{label} is required")
    if isinstance(raw, bool):
        err(f"{label} must be an exact decimal string, got boolean")
    if isinstance(raw, float):
        err(f"{label} must be an exact decimal string, got float")
    try:
        parsed = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError, ArithmeticError):
        err(f"{label} must be an exact decimal string")
    if not parsed.is_finite():
        err(f"{label} must be a finite decimal value")
    if parsed < 0:
        err(f"{label} must be nonnegative")
    quantized = parsed.quantize(_CENT, rounding=ROUND_HALF_UP)
    if quantized == 0:
        quantized = Decimal("0.00")
    return quantized


def _money_str(value):
    return str(value.quantize(_CENT, rounding=ROUND_HALF_UP))


def _parse_records(raw_json):
    if raw_json is None or (isinstance(raw_json, str) and not raw_json.strip()):
        err("--records-json is required")
    if not isinstance(raw_json, str):
        err("--records-json must be a JSON array string")
    try:
        records = json.loads(raw_json)
    except (json.JSONDecodeError, ValueError) as exc:
        err(f"--records-json is not valid JSON: {exc}")
    if not isinstance(records, list) or not records:
        err("--records-json must be a nonempty JSON array of objects")
    parsed = []
    for pos, rec in enumerate(records):
        label = f"records[{pos}]"
        if not isinstance(rec, dict):
            err(f"{label} must be an object")
        name = rec.get("account_name")
        if not isinstance(name, str) or not name.strip():
            err(f"{label}.account_name is required")
        number = rec.get("account_number")
        if number is None or (isinstance(number, str) and not number.strip()):
            number = None
        elif not isinstance(number, str):
            number = str(number)
        debit = _parse_money(rec.get("debit"), f"{label}.debit")
        credit = _parse_money(rec.get("credit"), f"{label}.credit")
        if debit > 0 and credit > 0:
            err(f"{label} must not have both debit and credit positive")
        parsed.append({
            "account_name": name.strip(),
            "account_number": number,
            "debit": debit,
            "credit": credit,
        })
    return parsed


# ===========================================================================
# 1. import-quickbooks-trial-balance
# ===========================================================================
def import_quickbooks_trial_balance(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    source_label = getattr(args, "source_label", None)
    if source_label is None:
        source_label = getattr(args, "source", None)
    if not isinstance(source_label, str) or not source_label.strip():
        err("--source-label is required")
    source_label = source_label.strip()

    as_of_raw = getattr(args, "as_of_date", None)
    if as_of_raw is None:
        as_of_raw = getattr(args, "as_of", None)
    if not isinstance(as_of_raw, str) or not as_of_raw.strip():
        err("--as-of-date is required (ISO YYYY-MM-DD)")
    try:
        as_of_day = date.fromisoformat(as_of_raw.strip())
    except ValueError:
        err("--as-of-date must be an ISO date (YYYY-MM-DD)")
    as_of_str = as_of_day.isoformat()

    records_raw = getattr(args, "records_json", None)
    if records_raw is None:
        records_raw = getattr(args, "records", None)
    parsed = _parse_records(records_raw)

    debit_total = sum((rec["debit"] for rec in parsed), Decimal("0.00"))
    credit_total = sum((rec["credit"] for rec in parsed), Decimal("0.00"))
    difference = debit_total - credit_total
    debit_s = _money_str(debit_total)
    credit_s = _money_str(credit_total)
    diff_s = _money_str(difference)
    batch_status = (STATUS_VERIFIED if debit_total == credit_total
                    else STATUS_OUT_OF_BALANCE)

    batch_id = str(uuid.uuid4())
    now = _now_iso()
    try:
        batch_sql, _ = insert_row("integration_quickbooks_batch", {
            "id": P(), "company_id": P(), "source": P(),
            "source_label": P(), "as_of_date": P(), "created_at": P(),
            "debit_total": P(), "credit_total": P(), "difference": P(),
            "status": P(),
        })
        conn.execute(batch_sql, (
            batch_id, company_id, SOURCE, source_label, as_of_str, now,
            debit_s, credit_s, diff_s, batch_status,
        ))
        for seq, rec in enumerate(parsed, start=1):
            line_sql, _ = insert_row("integration_quickbooks_line", {
                "id": P(), "batch_id": P(), "company_id": P(),
                "account_name": P(), "account_number": P(),
                "debit": P(), "credit": P(), "sequence": P(),
            })
            conn.execute(line_sql, (
                str(uuid.uuid4()), batch_id, company_id,
                rec["account_name"], rec["account_number"],
                _money_str(rec["debit"]), _money_str(rec["credit"]), seq,
            ))
        audit(conn, SKILL, "integration-import-quickbooks-trial-balance",
              "integration_quickbooks_batch", batch_id,
              new_values={"source_label": source_label,
                          "as_of_date": as_of_str,
                          "lines": len(parsed),
                          "status": batch_status})
        conn.commit()
    except Exception as exc:
        conn.rollback()
        err(f"QuickBooks import failed, rolled back (no partial batch kept): {exc}")
        return

    ok({"batch_id": batch_id, "id": batch_id,
        "company_id": company_id,
        "source": SOURCE, "source_label": source_label,
        "as_of_date": as_of_str,
        "line_count": len(parsed), "lines_imported": len(parsed),
        "debit_total": debit_s, "credit_total": credit_s,
        "difference": diff_s,
        "batch_status": batch_status, "import_status": batch_status,
        "verification_status": batch_status, "status": batch_status,
        "posted": False})


# ===========================================================================
# 2. get-quickbooks-import
# ===========================================================================
def get_quickbooks_import(conn, args):
    batch_id = getattr(args, "batch_id", None)
    if batch_id is None:
        batch_id = getattr(args, "batch", None)
    if batch_id is None:
        batch_id = getattr(args, "import_id", None)
    if not batch_id:
        err("--batch-id is required")
    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")

    batch_table = Table("integration_quickbooks_batch")
    row = conn.execute(
        Q.from_(batch_table).select(batch_table.star).where(
            batch_table.id == P()).where(
            batch_table.company_id == P()).get_sql(),
        (batch_id, company_id)).fetchone()
    if not row:
        err(f"QuickBooks import batch {batch_id} not found")

    line_table = Table("integration_quickbooks_line")
    lines = conn.execute(
        Q.from_(line_table).select(line_table.star).where(
            line_table.batch_id == P()).orderby(
            line_table.sequence).get_sql(), (batch_id,)).fetchall()

    batch = row_to_dict(row)
    line_dicts = [row_to_dict(r) for r in lines]
    ok({"batch_id": batch["id"], "id": batch["id"],
        "batch": batch, "lines": line_dicts,
        "line_count": len(line_dicts), "posted": False})


ACTIONS = {
    "integration-import-quickbooks-trial-balance": import_quickbooks_trial_balance,
    "integration-get-quickbooks-import": get_quickbooks_import,
}
