"""L1 unit tests (m761): BAI2 closing balances + bank-minus-books difference.

Covers: the BAI2 `03` multi-group balance parse (010 opening / 015 closing,
funds-type extras, trailers never read as balances) and the
`integration-bank-reconciliation-summary` contract (`difference` is the
statement balance minus the ledger balance; a missing closing balance yields
null balances with `statement_balance_missing: true`, never 0).
"""
import os
import sys

import pytest

from integration_helpers import (
    load_db_query, call_action, ns, is_ok, _uuid,
    seed_company, seed_naming_series,
)
from erpclaw_lib.query import Q, P, Table, insert_row

MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(MODULE_DIR)))  # source/
REPO_ROOT = os.path.dirname(SRC_DIR)
FIXTURES = os.path.join(REPO_ROOT, "testing", "fixtures", "bank")

pytestmark = pytest.mark.skipif(
    not os.path.isdir(FIXTURES),
    reason="testing/fixtures/bank not present — bank-import tests need the "
           "monorepo fixture set")

sys.path.insert(0, MODULE_DIR)
import parsers  # noqa: E402

FORMAT_FILES = {
    "ofx": "statement-jan-2026.ofx",
    "camt053": "statement-jan-2026.camt053.xml",
    "mt940": "statement-jan-2026.mt940",
    "bai2": "statement-jan-2026.bai2",
}


# ---------------------------------------------------------------------------
# fixtures / seeds (mirrors test_bank.py; copied, never imported across files)
# ---------------------------------------------------------------------------
def seed_bank_account(conn, company_id, name="Checking Account",
                      account_type="bank"):
    aid = _uuid()
    sql, _cols = insert_row("account", {
        "id": P(), "name": P(), "root_type": P(), "account_type": P(),
        "currency": P(), "is_group": P(), "disabled": P(), "company_id": P()})
    conn.execute(sql, (aid, name, "asset", account_type, "USD", 0, 0,
                       company_id))
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


def _import_text(mod, conn, company_id, account_id, text, tmp_path,
                 name="stmt.bai2", fmt="bai2"):
    path = str(tmp_path / name)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return call_action(mod.ACTIONS["integration-import-bank-statement"], conn,
                       ns(company_id=company_id, bank_account_id=account_id,
                          file=path, format=fmt))


def _add_rule(mod, conn, company_id, **kw):
    base = dict(company_id=company_id, name="r", match_field="counterparty_name",
                match_operator="contains", match_value="ACME",
                target_action="map_to_account", target_id="ACC-1", priority=100)
    base.update(kw)
    return call_action(mod.ACTIONS["integration-add-bank-match-rule"], conn,
                       ns(**base))


def _insert_gl_debit(conn, account_id, debit="1500.00", posting_date="2026-01-05"):
    sql, _cols = insert_row("gl_entry", {
        "id": P(), "posting_date": P(), "account_id": P(), "debit": P(),
        "credit": P(), "voucher_type": P(), "voucher_id": P()})
    conn.execute(sql, (_uuid(), posting_date, account_id, debit, "0",
                       "payment_entry", _uuid()))
    conn.commit()


BAI2_NO_CLOSE = """\
01,123456789,0001234567,260201,1200,1,,,2/
02,0001234567,123456789,1,260131,,USD,2/
03,0001234567,USD,010,300000,,/
16,165,150000,0,BANK-20260105-001,ACME CORP PAYMENT Invoice INV-1001/
16,475,25050,0,BANK-20260108-002,OFFICE SUPPLIES CO Staples order 88213/
16,475,120000,0,BANK-20260112-003,METRO PROPERTIES January office rent/
16,165,320000,0,BANK-20260120-004,GLOBEX LLC Invoice INV-1002/
49,999900,1/
98,999900,1,1/
99,999900,1,1/
"""


# ---------------------------------------------------------------------------
# parser
# ---------------------------------------------------------------------------
def test_bai2_closing_balance_parsed(banked):
    mod = load_db_query()
    text = open(os.path.join(FIXTURES, FORMAT_FILES["bai2"])).read()
    parsed = parsers.parse(text, "bai2")
    assert parsed["opening_balance"] == "3000.00"
    assert parsed["closing_balance"] == "6249.50"
    r = _import(mod, banked["conn"], banked["company_id"],
                banked["bank_account_id"], FORMAT_FILES["bai2"])
    assert is_ok(r), r
    stmt_t = Table("bank_statement")
    sql = (Q.from_(stmt_t).select(stmt_t.closing_balance)
           .where(stmt_t.id == P()).get_sql())
    row = banked["conn"].execute(sql, (r["statement_id"],)).fetchone()
    assert row["closing_balance"] == "6249.50"


def test_every_format_same_balances():
    for fmt, fname in FORMAT_FILES.items():
        parsed = parsers.parse(open(os.path.join(FIXTURES, fname)).read(),
                               "auto")
        assert parsed["closing_balance"] == "6249.50"
        if fmt == "ofx":
            assert parsed["opening_balance"] is None
        else:
            assert parsed["opening_balance"] == "3000.00"


def test_bai2_multiple_groups_and_funds_types():
    text = (
        "01,123456789,ACC,260201,1200,1,,,2/\n"
        "02,ACC,123456789,1,260131,,USD,2/\n"
        "03,ACC,USD,010,100000,,S,100,200,300,015,250000,2,V,260131,1200,400,5000,,/\n"
        "16,165,100000,0,TEST-001,Test payment/\n"
        "49,350000,2/\n"
        "98,350000,2,1/\n"
        "99,350000,2,1/\n"
    )
    parsed = parsers.parse(text, "bai2")
    assert parsed["opening_balance"] == "1000.00"
    assert parsed["closing_balance"] == "2500.00"
    short = (
        "01,123456789,ACC,260201,1200,1,,,2/\n"
        "02,ACC,123456789,1,260131,,USD,2/\n"
        "03,ACC,USD,015/\n"
        "16,165,100000,0,TEST-001,Test payment/\n"
        "49,100000,1/\n"
        "98,100000,1,1/\n"
        "99,100000,1,1/\n"
    )
    with pytest.raises(parsers.BankStatementParseError) as excinfo:
        parsers.parse(short, "bai2")
    assert "03" in str(excinfo.value)


def test_bai2_trailers_are_not_balances():
    parsed = parsers.parse(BAI2_NO_CLOSE, "bai2")
    assert parsed["opening_balance"] == "3000.00"
    assert parsed["closing_balance"] is None


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------
def test_difference_is_bank_minus_books(banked):
    mod = load_db_query()
    conn = banked["conn"]
    r = _import(mod, conn, banked["company_id"], banked["bank_account_id"],
                FORMAT_FILES["ofx"])
    assert is_ok(r), r
    office = _add_rule(mod, conn, banked["company_id"], name="office-supplies",
                       match_field="counterparty_name",
                       match_operator="contains",
                       match_value="OFFICE SUPPLIES")
    assert is_ok(office), office
    metro = _add_rule(mod, conn, banked["company_id"], name="metro",
                      match_field="counterparty_name",
                      match_operator="contains", match_value="METRO")
    assert is_ok(metro), metro
    matched = call_action(
        mod.ACTIONS["integration-auto-match-bank-statement"], conn,
        ns(statement_id=r["statement_id"]))
    assert is_ok(matched), matched
    assert matched["auto_matched"] == 2
    _insert_gl_debit(conn, banked["bank_account_id"])
    res = call_action(
        mod.ACTIONS["integration-bank-reconciliation-summary"], conn,
        ns(company_id=banked["company_id"],
           bank_account_id=banked["bank_account_id"], as_of="2026-01-31"))
    assert is_ok(res), res
    assert res["ledger_balance"] == "1500.00"
    assert res["statement_balance"] == "6249.50"
    assert res["reconciled_balance"] == "-1450.50"
    assert res["unmatched_total"] == "4700.00"
    assert res["difference"] == "4749.50"
    assert res["statement_balance_missing"] is False


def test_missing_closing_balance_is_not_zero(banked, tmp_path):
    mod = load_db_query()
    conn = banked["conn"]
    r = _import_text(mod, conn, banked["company_id"],
                     banked["bank_account_id"], BAI2_NO_CLOSE, tmp_path)
    assert is_ok(r), r
    res = call_action(
        mod.ACTIONS["integration-bank-reconciliation-summary"], conn,
        ns(company_id=banked["company_id"],
           bank_account_id=banked["bank_account_id"], as_of="2026-01-31"))
    assert is_ok(res), res
    assert res["statement_balance"] is None
    assert res["difference"] is None
    assert res["statement_balance_missing"] is True


def test_summary_writes_nothing(banked):
    mod = load_db_query()
    conn = banked["conn"]
    r = _import(mod, conn, banked["company_id"], banked["bank_account_id"],
                FORMAT_FILES["ofx"])
    assert is_ok(r), r
    _insert_gl_debit(conn, banked["bank_account_id"])

    def _snapshot():
        snap = {}
        for table in ("bank_statement", "bank_statement_line", "gl_entry"):
            tbl = Table(table)
            sql = (Q.from_(tbl).select(tbl.star).orderby(tbl.id)
                   .get_sql())
            snap[table] = [dict(row) for row in conn.execute(sql).fetchall()]
        return snap

    before = _snapshot()
    res = call_action(
        mod.ACTIONS["integration-bank-reconciliation-summary"], conn,
        ns(company_id=banked["company_id"],
           bank_account_id=banked["bank_account_id"], as_of="2026-01-31"))
    assert is_ok(res), res
    assert _snapshot() == before
