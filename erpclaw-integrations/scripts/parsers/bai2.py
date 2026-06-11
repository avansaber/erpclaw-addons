"""BAI2 (Bank Administration Institute v2) parser.

Comma-delimited records keyed by a leading record code:
  01 file header, 02 group header, 03 account identifier,
  16 transaction detail, 49 account trailer, 98 group trailer, 99 file trailer.

Amounts are in integer minor units (cents). Transaction type codes classify
sign: 1xx/2xx = credits, 3xx/4xx/5xx = debits (BAI2 convention). Records may end
with a trailing ``/``.
"""
from . import BankStatementParseError, norm_amount, line


def _date(yymmdd):
    if not yymmdd or len(yymmdd) < 6:
        return None
    return f"20{yymmdd[0:2]}-{yymmdd[2:4]}-{yymmdd[4:6]}"


def _fields(rec):
    return [f.strip() for f in rec.rstrip("/").split(",")]


def _is_credit(type_code):
    """BAI2 detail type code → True if a credit (money in)."""
    try:
        n = int(type_code)
    except (TypeError, ValueError):
        raise BankStatementParseError(f"malformed BAI2 type code: {type_code!r}")
    return 100 <= n < 300


def parse(text: str) -> dict:
    records = [r for r in (ln.strip() for ln in text.splitlines()) if r]
    if not any(r.startswith("16,") for r in records):
        raise BankStatementParseError("not a BAI2 file (no 16 detail records)")

    currency = "USD"
    account_hint = None
    opening_balance = closing_balance = None
    period_start = period_end = None

    lines = []
    for rec in records:
        f = _fields(rec)
        code = f[0]
        if code == "02":
            # 02,receiver,originator,group_status,as_of_date,as_of_time,ccy,...
            if len(f) > 6 and f[6]:
                currency = f[6]
            if len(f) > 4:
                period_end = _date(f[4]) or period_end  # statement as-of date
        elif code == "03":
            account_hint = f[1] or account_hint
            if len(f) > 2 and f[2]:
                currency = f[2]
            # 03,acct,ccy,type,amount,... — type 010 = opening ledger balance
            if len(f) > 4 and f[3] == "010" and f[4]:
                opening_balance = norm_amount(f[4], scale=2)
        elif code == "16":
            if len(f) < 5:
                raise BankStatementParseError(f"malformed BAI2 detail record: {rec!r}")
            type_code, amount_cents, _funds, bank_ref = f[1], f[2], f[3], f[4]
            text_field = f[5] if len(f) > 5 else None
            amount = norm_amount(amount_cents, scale=2)
            if not _is_credit(type_code) and not amount.startswith("-"):
                amount = "-" + amount
            lines.append(line(
                external_id=bank_ref,
                txn_date=period_end,  # BAI2 details inherit the statement date
                amount=amount,
                currency=currency,
                description=text_field,
                counterparty_name=None,
                reference=bank_ref or None,
            ))

    if not lines:
        raise BankStatementParseError("BAI2 file contained no transactions")

    return {
        "source": "bai2",
        "currency": currency,
        "account_hint": account_hint,
        "period_start": period_start,
        "period_end": period_end,
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "lines": lines,
    }
