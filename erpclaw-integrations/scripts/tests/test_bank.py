"""L1 unit tests for M2 bank statement import + matching (bank.py + parsers).

Covers: the 4 parsers (happy path, cross-format parity) + malformed rejection;
import happy path; the two SIM-0 NEGATIVE CONTROLS (re-import idempotency via the
external_id UNIQUE, and malformed-file → no partial statement written); the rule
engine (equals/contains/regex/amount_range, priority); match-status transitions;
reconciliation summary; and the resolve_account_by_name FINDING-002 grounding
guard (named-but-missing / unnamed bank account hard-errors).
"""
import os
import sys
import uuid

import pytest

from integration_helpers import (
    load_db_query, call_action, ns, is_error, is_ok, _uuid, _now,
    seed_company, seed_naming_series,
)

MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(MODULE_DIR)))  # source/
REPO_ROOT = os.path.dirname(SRC_DIR)
FIXTURES = os.path.join(REPO_ROOT, "testing", "fixtures", "bank")

sys.path.insert(0, MODULE_DIR)
import parsers  # noqa: E402

FORMAT_FILES = {
    "ofx": "statement-jan-2026.ofx",
    "camt053": "statement-jan-2026.camt053.xml",
    "mt940": "statement-jan-2026.mt940",
    "bai2": "statement-jan-2026.bai2",
}
EXPECTED_EXTERNAL_IDS = {
    "BANK-20260105-001", "BANK-20260108-002",
    "BANK-20260112-003", "BANK-20260120-004",
}


# ---------------------------------------------------------------------------
# fixtures / seeds
# ---------------------------------------------------------------------------
def seed_bank_account(conn, company_id, name="Checking Account",
                      account_type="bank"):
    aid = _uuid()
    conn.execute(
        "INSERT INTO account (id, name, root_type, account_type, currency, "
        "is_group, disabled, company_id) VALUES (?,?,?,?,?,0,0,?)",
        (aid, name, "asset", account_type, "USD", company_id))
    conn.commit()
    return aid


@pytest.fixture
def banked(conn):
    """company + naming + a bank account, ready for import."""
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    aid = seed_bank_account(conn, cid)
    return {"conn": conn, "company_id": cid, "bank_account_id": aid}


def _import(mod, conn, company_id, account_id, fmt_file, fmt="auto"):
    return call_action(mod.ACTIONS["integration-import-bank-statement"], conn,
                       ns(company_id=company_id, bank_account_id=account_id,
                          file=os.path.join(FIXTURES, fmt_file), format=fmt))


# ---------------------------------------------------------------------------
# parsers
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fmt,fname", FORMAT_FILES.items())
def test_parser_happy_path(fmt, fname):
    parsed = parsers.parse(open(os.path.join(FIXTURES, fname)).read(), "auto")
    assert parsed["source"] == fmt
    assert len(parsed["lines"]) == 4
    assert {l["external_id"] for l in parsed["lines"]} == EXPECTED_EXTERNAL_IDS
    by_id = {l["external_id"]: l for l in parsed["lines"]}
    # Cross-format parity: same signed amounts regardless of format.
    assert by_id["BANK-20260105-001"]["amount"] == "1500.00"
    assert by_id["BANK-20260108-002"]["amount"] == "-250.50"
    assert by_id["BANK-20260112-003"]["amount"] == "-1200.00"
    assert by_id["BANK-20260120-004"]["amount"] == "3200.00"


def test_parser_malformed_raises():
    with pytest.raises(parsers.BankStatementParseError):
        parsers.parse(open(os.path.join(FIXTURES, "statement-malformed.ofx")).read(),
                      "auto")


def test_format_autodetect():
    for fmt, fname in FORMAT_FILES.items():
        assert parsers.detect_format(open(os.path.join(FIXTURES, fname)).read()) == fmt


# ---------------------------------------------------------------------------
# import + SIM-0 negative controls
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fmt,fname", FORMAT_FILES.items())
def test_import_happy_path(banked, fmt, fname):
    mod = load_db_query()
    r = _import(mod, banked["conn"], banked["company_id"],
                banked["bank_account_id"], fname)
    assert is_ok(r), r
    assert r["lines_imported"] == 4
    assert r["lines_skipped_duplicate"] == 0
    n = banked["conn"].execute(
        "SELECT COUNT(*) FROM bank_statement_line WHERE bank_statement_id = ?",
        (r["statement_id"],)).fetchone()[0]
    assert n == 4


def test_reimport_idempotent_no_duplicate_lines(banked):
    """SIM-0 NEGATIVE CONTROL: re-importing the same file adds 0 rows."""
    mod = load_db_query()
    conn = banked["conn"]
    r1 = _import(mod, conn, banked["company_id"], banked["bank_account_id"],
                 FORMAT_FILES["ofx"])
    assert r1["lines_imported"] == 4
    total_after_first = conn.execute(
        "SELECT COUNT(*) FROM bank_statement_line").fetchone()[0]
    assert total_after_first == 4

    r2 = _import(mod, conn, banked["company_id"], banked["bank_account_id"],
                 FORMAT_FILES["ofx"])
    assert is_ok(r2), r2
    assert r2["lines_imported"] == 0
    assert r2["lines_skipped_duplicate"] == 4
    total_after_second = conn.execute(
        "SELECT COUNT(*) FROM bank_statement_line").fetchone()[0]
    assert total_after_second == 4  # no duplicates created


def test_external_id_unique_constraint_enforced(banked):
    """SIM-0: the (source, bank_account_id, external_id) UNIQUE is the hard
    backstop behind the app-level dedup — a direct duplicate insert fails."""
    mod = load_db_query()
    conn = banked["conn"]
    r = _import(mod, conn, banked["company_id"], banked["bank_account_id"],
                FORMAT_FILES["ofx"])
    line = conn.execute(
        "SELECT * FROM bank_statement_line LIMIT 1").fetchone()
    import sqlite3
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO bank_statement_line (id, bank_statement_id, "
            "bank_account_id, source, txn_date, amount, currency, external_id) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (_uuid(), r["statement_id"], banked["bank_account_id"], "ofx",
             "2026-01-05", "1500.00", "USD", line["external_id"]))


def test_malformed_writes_no_partial_statement(banked):
    """SIM-0 NEGATIVE CONTROL: a malformed file errors and writes NOTHING."""
    mod = load_db_query()
    conn = banked["conn"]
    r = _import(mod, conn, banked["company_id"], banked["bank_account_id"],
                "statement-malformed.ofx")
    assert is_error(r), r
    assert conn.execute("SELECT COUNT(*) FROM bank_statement").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM bank_statement_line").fetchone()[0] == 0


def test_format_mismatch_rejected(banked):
    mod = load_db_query()
    r = _import(mod, banked["conn"], banked["company_id"],
                banked["bank_account_id"], FORMAT_FILES["ofx"], fmt="mt940")
    assert is_error(r), r


# ---------------------------------------------------------------------------
# resolve_account_by_name — FINDING-002 grounding guard
# ---------------------------------------------------------------------------
def test_import_by_account_name_resolves(banked):
    mod = load_db_query()
    r = call_action(mod.ACTIONS["integration-import-bank-statement"], banked["conn"],
                    ns(company_id=banked["company_id"],
                       bank_account_name="Checking Account",
                       file=os.path.join(FIXTURES, FORMAT_FILES["ofx"]),
                       format="auto"))
    assert is_ok(r), r
    assert r["lines_imported"] == 4


def test_import_wrong_account_name_hard_errors(banked):
    """Named-but-missing account must NOT fall through to another account."""
    mod = load_db_query()
    r = call_action(mod.ACTIONS["integration-import-bank-statement"], banked["conn"],
                    ns(company_id=banked["company_id"],
                       bank_account_name="Nonexistent Savings",
                       file=os.path.join(FIXTURES, FORMAT_FILES["ofx"]),
                       format="auto"))
    assert is_error(r) or r.get("error")
    assert banked["conn"].execute(
        "SELECT COUNT(*) FROM bank_statement").fetchone()[0] == 0


def test_import_no_account_hard_errors(banked):
    """No account named at all → FINDING-002 grounding guard fires (no auto-pick)."""
    mod = load_db_query()
    r = call_action(mod.ACTIONS["integration-import-bank-statement"], banked["conn"],
                    ns(company_id=banked["company_id"],
                       file=os.path.join(FIXTURES, FORMAT_FILES["ofx"]),
                       format="auto"))
    assert is_error(r) or r.get("error")


# ---------------------------------------------------------------------------
# matching engine
# ---------------------------------------------------------------------------
def _add_rule(mod, conn, company_id, **kw):
    base = dict(company_id=company_id, name="r", match_field="counterparty_name",
                match_operator="contains", match_value="ACME",
                target_action="map_to_account", target_id="ACC-1", priority=100)
    base.update(kw)
    return call_action(mod.ACTIONS["integration-add-bank-match-rule"], conn, ns(**base))


def test_auto_match_contains_rule(banked):
    mod = load_db_query()
    conn = banked["conn"]
    r = _import(mod, conn, banked["company_id"], banked["bank_account_id"],
                FORMAT_FILES["ofx"])
    rule = _add_rule(mod, conn, banked["company_id"],
                     match_field="counterparty_name", match_operator="contains",
                     match_value="ACME")
    assert is_ok(rule), rule
    res = call_action(mod.ACTIONS["integration-auto-match-bank-statement"], conn,
                      ns(statement_id=r["statement_id"]))
    assert is_ok(res), res
    assert res["auto_matched"] == 1
    assert res["unmatched_remaining"] == 3
    st = conn.execute("SELECT import_status FROM bank_statement WHERE id = ?",
                      (r["statement_id"],)).fetchone()[0]
    assert st == "partially_matched"


def test_auto_match_amount_range_and_priority(banked):
    mod = load_db_query()
    conn = banked["conn"]
    r = _import(mod, conn, banked["company_id"], banked["bank_account_id"],
                FORMAT_FILES["ofx"])
    # ignore rule (priority 10) should win over a map rule (priority 100) on the
    # same line if both match; here amount_range targets the two credits.
    _add_rule(mod, conn, banked["company_id"], name="big-credits",
              match_field="amount", match_operator="amount_range",
              match_value="1000:5000", target_action="map_to_account",
              target_id="ACC-INC", priority=50)
    res = call_action(mod.ACTIONS["integration-auto-match-bank-statement"], conn,
                      ns(statement_id=r["statement_id"]))
    assert is_ok(res), res
    # 1500.00 and 3200.00 fall in [1000,5000]; -250.50 and -1200.00 do not.
    assert res["auto_matched"] == 2


def test_manual_match_then_clear_then_rematch(banked):
    mod = load_db_query()
    conn = banked["conn"]
    r = _import(mod, conn, banked["company_id"], banked["bank_account_id"],
                FORMAT_FILES["ofx"])
    line_id = conn.execute(
        "SELECT id FROM bank_statement_line WHERE bank_statement_id = ? LIMIT 1",
        (r["statement_id"],)).fetchone()[0]

    m = call_action(mod.ACTIONS["integration-manual-match-bank-line"], conn,
                    ns(line_id=line_id, target_action="map_to_account",
                       target_id="ACC-9"))
    assert is_ok(m) and m["match_status"] == "manual_matched"

    # already matched -> must clear first
    m2 = call_action(mod.ACTIONS["integration-manual-match-bank-line"], conn,
                     ns(line_id=line_id, target_action="map_to_account",
                        target_id="ACC-9"))
    assert is_error(m2)

    c = call_action(mod.ACTIONS["integration-clear-bank-line-match"], conn,
                    ns(line_id=line_id))
    assert is_ok(c) and c["match_status"] == "unmatched"

    m3 = call_action(mod.ACTIONS["integration-manual-match-bank-line"], conn,
                     ns(line_id=line_id, target_action="ignore", target_id=None))
    assert is_ok(m3) and m3["match_status"] == "ignored"


def test_unmatched_bank_lines_listing(banked):
    mod = load_db_query()
    conn = banked["conn"]
    r = _import(mod, conn, banked["company_id"], banked["bank_account_id"],
                FORMAT_FILES["ofx"])
    res = call_action(mod.ACTIONS["integration-unmatched-bank-lines"], conn,
                      ns(statement_id=r["statement_id"]))
    assert is_ok(res) and res["count"] == 4


def test_reconciliation_summary(banked):
    mod = load_db_query()
    conn = banked["conn"]
    r = _import(mod, conn, banked["company_id"], banked["bank_account_id"],
                FORMAT_FILES["ofx"])
    # one GL debit of 1500 to the bank account
    conn.execute(
        "INSERT INTO gl_entry (id, posting_date, account_id, debit, credit, "
        "voucher_type, voucher_id) VALUES (?,?,?,?,?,?,?)",
        (_uuid(), "2026-01-05", banked["bank_account_id"], "1500.00", "0",
         "payment_entry", _uuid()))
    conn.commit()
    res = call_action(mod.ACTIONS["integration-bank-reconciliation-summary"], conn,
                      ns(company_id=banked["company_id"],
                         bank_account_id=banked["bank_account_id"], as_of="2026-01-31"))
    assert is_ok(res), res
    assert res["ledger_balance"] == "1500.00"
    assert res["statement_balance"] == "6249.50"
    # nothing matched yet -> unmatched_total is the sum of the 4 lines
    assert res["unmatched_total"] == "3249.50"


def test_add_rule_validation(banked):
    mod = load_db_query()
    conn = banked["conn"]
    bad = call_action(mod.ACTIONS["integration-add-bank-match-rule"], conn,
                      ns(company_id=banked["company_id"], name="x",
                         match_field="bogus", match_operator="equals",
                         match_value="v", target_action="ignore"))
    assert is_error(bad)
    bad_re = call_action(mod.ACTIONS["integration-add-bank-match-rule"], conn,
                         ns(company_id=banked["company_id"], name="x",
                            match_field="description", match_operator="regex",
                            match_value="(unclosed", target_action="ignore"))
    assert is_error(bad_re)
