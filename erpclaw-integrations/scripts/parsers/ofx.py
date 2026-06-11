"""OFX parser (SGML 1.x and XML 2.x).

OFX 1.x is SGML: leaf tags usually have no closing tag, so a value runs from
``<TAG>`` until the next ``<``. This handles both that and the XML 2.x form
(``<TAG>v</TAG>``) with the same value extractor.
"""
import re

from . import BankStatementParseError, norm_amount, line


def _val(block: str, tag: str):
    """Return the value of ``<TAG>`` in ``block`` (SGML or XML), or None."""
    m = re.search(rf"<{tag}>\s*([^<\r\n]*)", block, re.IGNORECASE)
    return m.group(1).strip() if m and m.group(1).strip() else None


def _date(raw):
    """OFX date: YYYYMMDD[HHMMSS[.xxx]][tz]. Return YYYY-MM-DD."""
    if not raw:
        return None
    digits = re.sub(r"[^0-9]", "", raw)
    if len(digits) < 8:
        return None
    return f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"


def parse(text: str) -> dict:
    if "<STMTTRN" not in text.upper() and "<BANKTRANLIST" not in text.upper():
        raise BankStatementParseError("not an OFX bank statement (no <STMTTRN>)")

    currency = _val(text, "CURDEF") or "USD"
    account_hint = _val(text, "ACCTID")
    period_start = _date(_val(text, "DTSTART"))
    period_end = _date(_val(text, "DTEND"))
    closing_balance = _val(text, "BALAMT")

    blocks = re.findall(r"<STMTTRN>(.*?)</STMTTRN>", text,
                        re.IGNORECASE | re.DOTALL)
    # Truncated/unterminated file: an opening <STMTTRN> with no matching close.
    opens = len(re.findall(r"<STMTTRN>", text, re.IGNORECASE))
    if opens != len(blocks):
        raise BankStatementParseError(
            "malformed OFX: unterminated <STMTTRN> (truncated file)")

    lines = []
    for block in blocks:
        amt = _val(block, "TRNAMT")
        if amt is None:
            raise BankStatementParseError("OFX transaction missing <TRNAMT>")
        lines.append(line(
            external_id=_val(block, "FITID"),
            txn_date=_date(_val(block, "DTPOSTED")),
            amount=norm_amount(amt),
            currency=currency,
            description=_val(block, "MEMO") or _val(block, "NAME"),
            counterparty_name=_val(block, "NAME"),
            reference=_val(block, "CHECKNUM") or _val(block, "REFNUM"),
        ))

    if not lines:
        raise BankStatementParseError("OFX statement contained no transactions")

    return {
        "source": "ofx",
        "currency": currency,
        "account_hint": account_hint,
        "period_start": period_start,
        "period_end": period_end,
        "opening_balance": None,
        "closing_balance": closing_balance,
        "lines": lines,
    }
