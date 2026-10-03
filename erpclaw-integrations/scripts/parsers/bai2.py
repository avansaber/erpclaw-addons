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
            # 03,acct,ccy,(type,amount,count,funds[,extra...])* — type 010 is
            # the opening ledger balance, type 015 the closing ledger
            # balance. Funds types carry extra sub-fields per BAI2: S +3, V
            # +2, D +count and that many pairs, 0/1/2/Z +none. Other type
            # codes are ignored; 49/98/99 trailers are never read here.
            idx = 3
            if idx < len(f) and all(g == "" for g in f[idx:]):
                idx = len(f)
            while idx < len(f):
                if idx + 3 >= len(f):
                    raise BankStatementParseError(
                        f"malformed BAI2 03 record: {rec!r}")
                type_code = f[idx]
                amount_raw = f[idx + 1]
                funds = f[idx + 3]
                idx += 4
                if funds == "S":
                    if idx + 3 > len(f):
                        raise BankStatementParseError(
                            f"malformed BAI2 03 record: {rec!r}")
                    idx += 3
                elif funds == "V":
                    if idx + 2 > len(f):
                        raise BankStatementParseError(
                            f"malformed BAI2 03 record: {rec!r}")
                    idx += 2
                elif funds == "D":
                    if idx >= len(f):
                        raise BankStatementParseError(
                            f"malformed BAI2 03 record: {rec!r}")
                    try:
                        pairs = int(f[idx])
                    except (TypeError, ValueError):
                        raise BankStatementParseError(
                            f"malformed BAI2 03 record: {rec!r}")
                    idx += 1
                    if idx + 2 * pairs > len(f):
                        raise BankStatementParseError(
                            f"malformed BAI2 03 record: {rec!r}")
                    idx += 2 * pairs
                elif funds in ("", "0", "1", "2", "Z"):
                    pass
                else:
                    pass
                if not amount_raw:
                    continue
                if type_code == "010":
                    opening_balance = norm_amount(amount_raw, scale=2)
                elif type_code == "015":
                    closing_balance = norm_amount(amount_raw, scale=2)
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
