"""Bank statement parsers (M2).

Pure-Python, stdlib-only parsers for the four file formats SAP B1 / ERPNext
support: OFX, CAMT.053 (ISO 20022), MT940 (SWIFT) and BAI2. We deliberately do
NOT depend on ofxparse / mt-940 / bai2: the coding rules require external IO to
be a mockable seam and CI to run with no real dependency, and these formats are
small/well-specified enough to parse correctly in stdlib. This keeps the import
path testable offline and avoids an unvendored runtime dep.

Each ``parse(text)`` returns a normalized dict::

    {
      "source": "ofx",
      "currency": "USD",
      "account_hint": "0001234567",   # account id as stated in the file (info)
      "period_start": "2026-01-01" | None,
      "period_end":   "2026-01-31" | None,
      "opening_balance": "3000.00" | None,
      "closing_balance": "6249.50" | None,
      "lines": [ {external_id, txn_date, value_date, amount, currency,
                  description, counterparty_name, counterparty_account,
                  reference}, ... ],
    }

``amount`` is a SIGNED Decimal-as-TEXT (+ receipts / − payments). A malformed
file raises :class:`BankStatementParseError` BEFORE any line is yielded, so the
import action (which parses fully, then writes in one transaction) never leaves a
partial statement.
"""
from decimal import Decimal, InvalidOperation


class BankStatementParseError(ValueError):
    """Raised when a statement file cannot be parsed. The import action turns
    this into a clean error WITHOUT writing any bank_statement / line rows."""


def norm_amount(raw, *, scale=None) -> str:
    """Return a signed Decimal-as-TEXT amount. ``scale`` (e.g. 2) divides an
    integer-minor-unit value (BAI2 cents) and quantizes to that many places.
    Raises BankStatementParseError on a non-numeric value."""
    try:
        d = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError, ArithmeticError):
        raise BankStatementParseError(f"non-numeric amount: {raw!r}")
    if scale is not None:
        d = (d / (Decimal(10) ** scale)).quantize(Decimal(1).scaleb(-scale))
    return str(d)


def line(*, external_id, txn_date, amount, currency,
         value_date=None, description=None, counterparty_name=None,
         counterparty_account=None, reference=None) -> dict:
    """Build one normalized line dict, validating the required fields."""
    if not external_id:
        raise BankStatementParseError("transaction missing an external id (FITID/ref)")
    if not txn_date:
        raise BankStatementParseError(f"transaction {external_id} missing a date")
    return {
        "external_id": str(external_id).strip(),
        "txn_date": txn_date,
        "value_date": value_date or txn_date,
        "amount": amount,
        "currency": currency,
        "description": (description or "").strip() or None,
        "counterparty_name": (counterparty_name or "").strip() or None,
        "counterparty_account": (counterparty_account or "").strip() or None,
        "reference": (reference or "").strip() or None,
    }


def detect_format(text: str) -> str:
    """Best-effort format sniff from file content. Returns one of
    ofx/camt053/mt940/bai2, or raises if nothing matches."""
    head = text.lstrip()[:4096]
    upper = head.upper()
    if "OFXHEADER" in upper or "<OFX>" in upper or "<OFX " in upper:
        return "ofx"
    if "CAMT.053" in upper or ("<DOCUMENT" in upper and "BKTOCSTMRSTMT" in upper):
        return "camt053"
    if head.startswith("01,") or "\n01," in head[:8]:
        return "bai2"
    if ":20:" in head and ":61:" in text:
        return "mt940"
    raise BankStatementParseError(
        "could not auto-detect statement format; pass --format explicitly "
        "(ofx|camt053|mt940|bai2)")


def parse(text: str, fmt: str = "auto") -> dict:
    """Parse statement ``text`` in the given format ('auto' to sniff). Raises
    BankStatementParseError on an unknown format or malformed content."""
    from . import ofx, camt053, mt940, bai2
    fmt = (fmt or "auto").lower()
    if fmt == "auto":
        fmt = detect_format(text)
    parsers = {"ofx": ofx, "camt053": camt053, "mt940": mt940, "bai2": bai2}
    if fmt not in parsers:
        raise BankStatementParseError(f"unknown format: {fmt}")
    result = parsers[fmt].parse(text)
    result.setdefault("source", fmt)
    return result
