"""CAMT.053 (ISO 20022 bank-to-customer statement) parser.

Namespace-aware XML via stdlib ElementTree. Handles the common
camt.053.001.02/.08 layouts: Stmt > Ntry with Amt/CdtDbtInd/BookgDt and
opening (OPBD) / closing (CLBD) balances.
"""
import xml.etree.ElementTree as ET

from . import BankStatementParseError, norm_amount, line


def _local(tag: str) -> str:
    """Strip the ``{namespace}`` prefix ElementTree prepends to tags."""
    return tag.rsplit("}", 1)[-1]


def _find(el, *names):
    """First descendant whose local tag matches any of ``names``, or None."""
    want = set(names)
    for child in el.iter():
        if _local(child.tag) in want:
            return child
    return None


def _text(el, *names):
    """Text of the first descendant matching ``names`` that actually has
    non-empty text (skips structural wrapper elements with the same local tag,
    e.g. the nested <Id> wrappers in <Acct><Id><Othr><Id>...)."""
    if el is None:
        return None
    want = set(names)
    for child in el.iter():
        if _local(child.tag) in want and child.text and child.text.strip():
            return child.text.strip()
    return None


def parse(text: str) -> dict:
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        raise BankStatementParseError(f"malformed CAMT.053 XML: {e}")

    stmt = _find(root, "Stmt")
    if stmt is None:
        raise BankStatementParseError("not a CAMT.053 statement (no <Stmt>)")

    acct = _find(stmt, "Acct")
    currency = (_text(acct, "Ccy") if acct is not None else None) or "USD"
    account_hint = None
    if acct is not None:
        account_hint = _text(acct, "IBAN") or _text(acct, "Id")

    # Balances: OPBD = opening, CLBD = closing.
    opening_balance = closing_balance = None
    period_start = period_end = None
    for bal in stmt.iter():
        if _local(bal.tag) != "Bal":
            continue
        code = _text(bal, "Cd")
        amt = _text(bal, "Amt")
        dt = _text(bal, "Dt")
        if code in ("OPBD", "PRCD"):
            opening_balance, period_start = amt, dt or period_start
        elif code in ("CLBD", "CLAV"):
            closing_balance, period_end = amt, dt or period_end

    lines = []
    for ntry in stmt.iter():
        if _local(ntry.tag) != "Ntry":
            continue
        amt = _text(ntry, "Amt")
        if amt is None:
            raise BankStatementParseError("CAMT.053 entry missing <Amt>")
        ind = (_text(ntry, "CdtDbtInd") or "CRDT").upper()
        signed = norm_amount(amt)
        if ind == "DBIT" and not signed.startswith("-"):
            signed = "-" + signed
        ccy = None
        amt_el = _find(ntry, "Amt")
        if amt_el is not None:
            ccy = amt_el.attrib.get("Ccy")
        bookg = _find(ntry, "BookgDt")
        vald = _find(ntry, "ValDt")
        lines.append(line(
            external_id=_text(ntry, "NtryRef", "AcctSvcrRef") or _text(ntry, "TxId"),
            txn_date=_text(bookg, "Dt", "DtTm") if bookg is not None else None,
            value_date=_text(vald, "Dt", "DtTm") if vald is not None else None,
            amount=signed,
            currency=ccy or currency,
            description=_text(ntry, "Ustrd", "AddtlNtryInf"),
            counterparty_name=_text(ntry, "Nm"),
        ))

    if not lines:
        raise BankStatementParseError("CAMT.053 statement contained no entries")

    return {
        "source": "camt053",
        "currency": currency,
        "account_hint": account_hint,
        "period_start": period_start,
        "period_end": period_end,
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "lines": lines,
    }
