"""ERPClaw Integrations: offline bank feed import (v1).

Local-only staging for a caller-supplied bank-feed CSV export from NetSuite
or Xero. Version 1 never signs in remotely, calls neither provider, and never
writes staged rows to the general ledger: an import batch only stages exact
Decimal-as-TEXT amounts plus one durable batch receipt.

Explicit provider column mapping (headers match case-insensitively; common
aliases accepted, documented in SKILL.md):

    canonical field   NetSuite header    Xero header
    ----------------  -----------------  ----------------
    date              Date               Date
    description       Description        Description
    external ID       Transaction ID     Reference
    amount            Amount             Amount

Alias sets applied to every provider (normalized: trimmed, lowercased,
non-alphanumeric characters removed):

    date:         date, transactiondate, postingdate, transdate, posteddate,
                  entrydate, bookingdate
    description:  description, memo, narrative, narration, details, detail,
                  particulars, payee, label, transactiondescription
    external ID:  externalid, transactionid, transactionnumber, documentnumber,
                  reference, referencenumber, receiptid, receiptnumber, txnid,
                  transid, id, number, checknumber, vouchernumber
    amount:       amount, total, netamount, value, nettotal, transactionamount

Design invariants (coding rules):
  - money is Decimal-as-TEXT, never float; every amount is quantized to two
    places before it is stored or totalled.
  - import is parse-fully-then-write-once: rows are validated before any row
    is written, so a refused import leaves no batch, line, or audit rows.
  - the batch and all of its new lines are stored in one transaction.
  - retries of an identical (company, provider, account, external ID) are
    idempotent: the duplicate line is skipped and counted, never staged
    twice. A fully-duplicate file returns the existing batch receipt.
  - debit_total is the absolute sum of negative (money-out) amounts and
    credit_total the sum of positive (money-in) amounts, both two-place TEXT.
  - retrieval order is deterministic: lines carry the 1-based file order in
    `sequence` and are always read back ordered by it.
"""
import csv
import os
import re
import sys
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

try:
    import importlib.util
    if importlib.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.response import ok, err
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Q, P, Table, Field, insert_row
except ImportError:
    pass

SKILL = "erpclaw-integrations"

PROVIDERS = ("netsuite", "xero")

PROVIDER_COLUMNS = {
    "netsuite": {
        "date": "Date",
        "description": "Description",
        "external_id": "Transaction ID",
        "amount": "Amount",
    },
    "xero": {
        "date": "Date",
        "description": "Description",
        "external_id": "Reference",
        "amount": "Amount",
    },
}

_ALIASES = {
    "date": {
        "date", "transactiondate", "postingdate", "transdate", "posteddate",
        "entrydate", "bookingdate",
    },
    "description": {
        "description", "memo", "narrative", "narration", "details", "detail",
        "particulars", "payee", "label", "transactiondescription",
    },
    "external_id": {
        "externalid", "transactionid", "transactionnumber", "documentnumber",
        "reference", "referencenumber", "receiptid", "receiptnumber",
        "txnid", "transid", "id", "number", "checknumber", "vouchernumber",
    },
    "amount": {
        "amount", "total", "netamount", "value", "nettotal",
        "transactionamount",
    },
}

_DATE_FORMATS = (
    "%Y/%m/%d", "%m/%d/%Y", "%d-%b-%Y", "%b %d %Y", "%d %b %Y", "%Y%m%d",
)

_CENT = Decimal("0.01")

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _norm_header(value):
    return re.sub(r"[^a-z0-9]", "", (value or "").strip().lower())


def _map_columns(headers, provider):
    found = {}
    by_norm = {}
    for header in headers or []:
        key = _norm_header(header)
        if key and key not in by_norm:
            by_norm[key] = header
    missing = []
    for field in ("date", "description", "external_id", "amount"):
        match = next((by_norm[a] for a in _ALIASES[field] if a in by_norm), None)
        if match is None:
            missing.append(PROVIDER_COLUMNS[provider][field])
        else:
            found[field] = match
    if missing:
        err("CSV is missing required columns for provider "
            f"'{provider}': {', '.join(missing)} "
            f"(required: {', '.join(PROVIDER_COLUMNS[provider].values())})")
    return found


def _resolve_csv_path(raw_path):
    if not raw_path:
        err("--file is required (existing local .csv export)")
    if ".." in str(raw_path).split(os.sep):
        err(f"Refusing unsafe CSV path: {raw_path}")
    real = os.path.realpath(raw_path)
    if ".." in real.split(os.sep):
        err(f"Refusing unsafe CSV path: {raw_path}")
    if not real.lower().endswith(".csv"):
        err("--file must point to a .csv file")
    if not os.path.isfile(real):
        err(f"File not found: {raw_path}")
    return real


def _validate_company(conn, company_id, company_name=None):
    if not company_id and company_name:
        row = conn.execute(
            Q.from_(Table("company")).select(
                Field("id")).where(
                Field("name") == P()).get_sql(), (company_name,)).fetchone()
        if row:
            company_id = row["id"]
    if not company_id:
        err("--company-id is required")
    found = conn.execute(
        Q.from_(Table("company")).select(Field("id")).where(
            Field("id") == P()).get_sql(), (company_id,)).fetchone()
    if not found:
        err(f"Company {company_id} not found")
    return company_id


def _parse_date(raw, pos):
    text = (raw or "").strip()
    if not text:
        err(f"Row {pos}: date is required")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        pass
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    err(f"Row {pos}: malformed date {text!r} (expected ISO YYYY-MM-DD)")


def _parse_amount(raw, pos):
    if raw is None:
        err(f"Row {pos}: amount is required")
    text = str(raw).strip().replace(",", "").replace(" ", "")
    if not text:
        err(f"Row {pos}: amount is required")
    negative = False
    if text.startswith("(") and text.endswith(")") and len(text) > 2:
        negative = True
        text = text[1:-1].strip()
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError, ArithmeticError):
        err(f"Row {pos}: invalid amount {str(raw).strip()!r} "
            "(must be an exact decimal string)")
    if not parsed.is_finite():
        err(f"Row {pos}: invalid amount {str(raw).strip()!r} "
            "(must be a finite decimal value)")
    if negative:
        parsed = -abs(parsed)
    quantized = parsed.quantize(_CENT, rounding=ROUND_HALF_UP)
    if quantized == 0:
        quantized = Decimal("0.00")
    return quantized


def _money_str(value):
    quantized = value.quantize(_CENT, rounding=ROUND_HALF_UP)
    if quantized == 0:
        quantized = Decimal("0.00")
    return str(quantized)


def _read_rows(real_path, provider):
    try:
        with open(real_path, "r", newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            headers = reader.fieldnames or []
            columns = _map_columns(headers, provider)
            raw_rows = list(reader)
    except FileNotFoundError:
        err(f"File not found: {real_path}")
        return [], {}
    parsed = []
    seen = set()
    for offset, raw in enumerate(raw_rows, start=2):
        if all((value is None or str(value).strip() == "")
               for value in raw.values()):
            continue
        external_id = (raw.get(columns["external_id"]) or "").strip()
        if not external_id:
            err(f"Row {offset}: external ID is required")
        if external_id in seen:
            err(f"Row {offset}: duplicate external ID "
                f"{external_id!r} inside the file")
        seen.add(external_id)
        txn_date = _parse_date(raw.get(columns["date"]), offset)
        amount = _parse_amount(raw.get(columns["amount"]), offset)
        description = (raw.get(columns["description"]) or "").strip() or None
        parsed.append({
            "txn_date": txn_date,
            "description": description,
            "external_id": external_id,
            "amount": amount,
        })
    if not parsed:
        err("CSV file is empty (no data rows)")
    return parsed


def _existing_external_ids(conn, company_id, provider, account_ref):
    line = Table("integration_offline_bank_line")
    rows = conn.execute(
        Q.from_(line).select(line.external_id).where(
            line.company_id == P()).where(
            line.provider == P()).where(
            line.account_ref == P()).get_sql(),
        (company_id, provider, account_ref)).fetchall()
    return {row["external_id"] for row in rows}


def _batch_payload(conn, batch_id):
    batch_table = Table("integration_offline_bank_batch")
    batch = conn.execute(
        Q.from_(batch_table).select(batch_table.star).where(
            batch_table.id == P()).get_sql(), (batch_id,)).fetchone()
    line_table = Table("integration_offline_bank_line")
    lines = conn.execute(
        Q.from_(line_table).select(line_table.star).where(
            line_table.batch_id == P()).orderby(
            line_table.sequence).get_sql(), (batch_id,)).fetchall()
    ordered = [{
        "sequence": row["sequence"],
        "txn_date": row["txn_date"],
        "description": row["description"],
        "external_id": row["external_id"],
        "amount": row["amount"],
    } for row in lines]
    return {
        "batch_id": batch["id"],
        "id": batch["id"],
        "company_id": batch["company_id"],
        "provider": batch["provider"],
        "source": batch["provider"],
        "account_ref": batch["account_ref"],
        "file_path": batch["file_path"],
        "row_count": batch["row_count"],
        "line_count": batch["row_count"],
        "lines_imported": batch["row_count"],
        "rows_imported": batch["row_count"],
        "debit_total": batch["debit_total"],
        "credit_total": batch["credit_total"],
        "lines": ordered,
        "batch_status": "imported",
        "import_status": "imported",
        "status": "imported",
        "posted": False,
    }


def import_offline_bank_feed(conn, args):
    company_id = _validate_company(
        conn, getattr(args, "company_id", None),
        getattr(args, "company_name", None) or getattr(args, "company", None))

    provider = (getattr(args, "provider", None)
                or getattr(args, "platform", None)
                or getattr(args, "source", None))
    provider = provider.strip().lower() if isinstance(provider, str) else None
    if provider not in PROVIDERS:
        err("--provider must be one of: netsuite, xero")

    account_ref = (getattr(args, "account_ref", None)
                   or getattr(args, "account", None)
                   or getattr(args, "account_name", None))
    if not isinstance(account_ref, str) or not account_ref.strip():
        err("--account-ref is required")
    account_ref = account_ref.strip()

    real_path = _resolve_csv_path(
        getattr(args, "file", None)
        or getattr(args, "csv_path", None)
        or getattr(args, "path", None)
        or getattr(args, "csv_file", None))

    rows = _read_rows(real_path, provider)

    existing = _existing_external_ids(conn, company_id, provider, account_ref)
    new_rows = [row for row in rows if row["external_id"] not in existing]
    skipped = len(rows) - len(new_rows)

    if not new_rows:
        line_table = Table("integration_offline_bank_line")
        prior = conn.execute(
            Q.from_(line_table).select(line_table.batch_id).where(
                line_table.company_id == P()).where(
                line_table.provider == P()).where(
                line_table.account_ref == P()).where(
                line_table.external_id == P()).get_sql(),
            (company_id, provider, account_ref,
             rows[0]["external_id"])).fetchone()
        payload = _batch_payload(conn, prior["batch_id"])
        payload["skipped_duplicate_count"] = skipped
        payload["lines_skipped_duplicate"] = skipped
        payload["skipped"] = skipped
        ok(payload)
        return

    debit_total = sum((-row["amount"] for row in new_rows
                       if row["amount"] < 0), Decimal("0.00"))
    credit_total = sum((row["amount"] for row in new_rows
                        if row["amount"] > 0), Decimal("0.00"))
    debit_s = _money_str(debit_total)
    credit_s = _money_str(credit_total)

    batch_id = str(uuid.uuid4())
    now = _now_iso()
    try:
        batch_sql, _ = insert_row("integration_offline_bank_batch", {
            "id": P(), "company_id": P(), "provider": P(),
            "account_ref": P(), "file_path": P(), "row_count": P(),
            "debit_total": P(), "credit_total": P(), "created_at": P(),
        })
        conn.execute(batch_sql, (
            batch_id, company_id, provider, account_ref, real_path,
            len(new_rows), debit_s, credit_s, now,
        ))
        for seq, row in enumerate(new_rows, start=1):
            line_sql, _ = insert_row("integration_offline_bank_line", {
                "id": P(), "batch_id": P(), "company_id": P(),
                "provider": P(), "account_ref": P(), "txn_date": P(),
                "description": P(), "external_id": P(), "amount": P(),
                "sequence": P(),
            })
            conn.execute(line_sql, (
                str(uuid.uuid4()), batch_id, company_id, provider,
                account_ref, row["txn_date"], row["description"],
                row["external_id"], str(row["amount"]), seq,
            ))
        audit(conn, SKILL, "integration-import-offline-bank-feed",
              "integration_offline_bank_batch", batch_id,
              new_values={"provider": provider, "account_ref": account_ref,
                          "rows": len(new_rows), "skipped": skipped})
        conn.commit()
    except Exception as exc:
        conn.rollback()
        err("Offline bank feed import failed, rolled back "
            f"(no partial batch kept): {exc}")
        return

    payload = _batch_payload(conn, batch_id)
    payload["skipped_duplicate_count"] = skipped
    payload["lines_skipped_duplicate"] = skipped
    payload["skipped"] = skipped
    ok(payload)


ACTIONS = {
    "integration-import-offline-bank-feed": import_offline_bank_feed,
    "integration-import-bank-feed": import_offline_bank_feed,
}
