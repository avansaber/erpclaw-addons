"""MT940 (SWIFT customer statement) parser.

Tag-oriented text format. Records begin with ``:NN[a]:`` at line start;
continuation lines (no leading ``:``) belong to the previous tag. We pair each
:61: statement line with the :86: information line that follows it, and read
opening/closing balances from :60F:/:62F:.
"""
import re

from . import BankStatementParseError, norm_amount, line

_TAG_RE = re.compile(r"^:(\d{2}[A-Z]?):(.*)$")
# :61: value-date(YYMMDD) [entry-date(MMDD)] mark(C/D/RC/RD) amount type rest
_61_RE = re.compile(
    r"^(?P<vdate>\d{6})(?P<edate>\d{4})?(?P<mark>RC|RD|C|D)"
    r"(?P<amount>[0-9][0-9,]*)(?P<ttype>[A-Z][A-Z0-9]{3})?(?P<rest>.*)$")
# :60F:/:62F: mark(C/D) date(YYMMDD) currency(3) amount
_BAL_RE = re.compile(r"^(?P<mark>C|D)(?P<date>\d{6})(?P<ccy>[A-Z]{3})(?P<amount>[0-9][0-9,]*)$")


def _date(yymmdd):
    if not yymmdd or len(yymmdd) < 6:
        return None
    return f"20{yymmdd[0:2]}-{yymmdd[2:4]}-{yymmdd[4:6]}"


def _amt(raw):
    return norm_amount(raw.replace(",", "."))


def _records(text):
    """Yield (tag, value) honoring continuation lines."""
    recs, tag, buf = [], None, []
    for raw in text.splitlines():
        m = _TAG_RE.match(raw)
        if m:
            if tag is not None:
                recs.append((tag, "\n".join(buf)))
            tag, buf = m.group(1), [m.group(2)]
        elif tag is not None:
            buf.append(raw)
    if tag is not None:
        recs.append((tag, "\n".join(buf)))
    return recs


def parse(text: str) -> dict:
    recs = _records(text)
    if not any(t == "61" for t, _ in recs):
        raise BankStatementParseError("not an MT940 statement (no :61: lines)")

    currency = "USD"
    opening_balance = closing_balance = None
    period_start = period_end = None
    account_hint = None

    lines = []
    i = 0
    while i < len(recs):
        tag, val = recs[i]
        if tag == "25":
            account_hint = val.split("/")[-1].strip() or None
        elif tag in ("60F", "60M"):
            m = _BAL_RE.match(val.strip())
            if m:
                currency = m.group("ccy")
                opening_balance = _amt(m.group("amount"))
                period_start = _date(m.group("date"))
        elif tag in ("62F", "62M"):
            m = _BAL_RE.match(val.strip())
            if m:
                currency = m.group("ccy")
                closing_balance = _amt(m.group("amount"))
                period_end = _date(m.group("date"))
        elif tag == "61":
            m = _61_RE.match(val.strip())
            if not m:
                raise BankStatementParseError(f"malformed MT940 :61: line: {val!r}")
            mark = m.group("mark")
            amount = _amt(m.group("amount"))
            if mark in ("D", "RC") and not amount.startswith("-"):
                amount = "-" + amount
            rest = m.group("rest")
            ref, _, bankref = rest.partition("//")
            # :86: info line(s) follow the :61:
            info = None
            if i + 1 < len(recs) and recs[i + 1][0] == "86":
                info = recs[i + 1][1].replace("\n", " ").strip()
                i += 1
            external_id = (bankref.strip() or ref.strip() or "").strip()
            lines.append(line(
                external_id=external_id,
                txn_date=_date(m.group("vdate")),
                amount=amount,
                currency=currency,
                description=info,
                # MT940 :86: is bank-specific free text; counterparty is not a
                # reliably-delimited subfield, so it stays in description only.
                counterparty_name=None,
                reference=ref.strip() or None,
            ))
        i += 1

    if not lines:
        raise BankStatementParseError("MT940 statement contained no transactions")

    return {
        "source": "mt940",
        "currency": currency,
        "account_hint": account_hint,
        "period_start": period_start,
        "period_end": period_end,
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "lines": lines,
    }
