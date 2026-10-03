"""Company scope for the two bank lists."""
import json
import os
import uuid

from integration_helpers import (
    call_action, ns, is_error, is_ok, _uuid, seed_naming_series,
)
from erpclaw_lib.query import Q, Table

MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(MODULE_DIR)))
REPO_ROOT = os.path.dirname(SRC_DIR)
FIXTURES = os.path.join(REPO_ROOT, "testing", "fixtures", "bank")

MULTI_ERROR = "Multiple companies found. Please specify the company by name."
MULTI_SUGGESTION = "Pass the company name (e.g. --company \"Acme\"), or use --company-id with one of the IDs above."
ZERO_ERROR = "No company found. Create one first."
ZERO_SUGGESTION = "Run 'tutorial' to create a demo company, or 'setup company' to create your own."

_STATE_TABLES = ("bank_statement", "bank_statement_line", "bank_match_rule", "company", "audit_log")


def _rows(conn, table):
    t = Table(table)
    try:
        rows = conn.execute(Q.from_(t).select(t.star).get_sql()).fetchall()
    except Exception:
        return []
    return sorted(json.dumps(dict(r), sort_keys=True, default=str) for r in rows)


def _state(conn):
    return {t: _rows(conn, t) for t in _STATE_TABLES}


def _seed_company(conn, name, abbr):
    cid = _uuid()
    conn.execute(
        "INSERT INTO company (id, name, abbr, default_currency, country, fiscal_year_start_month)"
        " VALUES (?, ?, ?, 'USD', 'United States', 1)",
        (cid, name, abbr))
    conn.commit()
    seed_naming_series(conn, cid)
    return cid


def _seed_bank_account(conn, company_id, name):
    aid = _uuid()
    conn.execute(
        "INSERT INTO account (id, name, root_type, account_type, currency, is_group, disabled, company_id)"
        " VALUES (?, ?, 'asset', 'bank', 'USD', 0, 0, ?)",
        (aid, name, company_id))
    conn.commit()
    return aid


def _import_stmt(conn, mod, company_id, account_id):
    return call_action(mod.ACTIONS["integration-import-bank-statement"], conn,
                       ns(company_id=company_id, bank_account_id=account_id,
                          file=os.path.join(FIXTURES, "statement-jan-2026.ofx"), format="auto"))


def _add_rule(conn, mod, company_id, name):
    return call_action(mod.ACTIONS["integration-add-bank-match-rule"], conn,
                       ns(company_id=company_id, name=name, match_field="counterparty_name",
                          match_operator="contains", match_value="ACME",
                          target_action="ignore", target_id=None, priority=100))


def _two(conn, mod):
    acme = _seed_company(conn, "Acme Widgets", "ACME")
    wayne = _seed_company(conn, "Wayne Enterprises", "WAYNE")
    acme_acct = _seed_bank_account(conn, acme, "Acme Checking")
    wayne_acct = _seed_bank_account(conn, wayne, "Wayne Checking")
    imp_a = _import_stmt(conn, mod, acme, acme_acct)
    assert is_ok(imp_a), imp_a
    imp_w = _import_stmt(conn, mod, wayne, wayne_acct)
    assert is_ok(imp_w), imp_w
    rule_a = _add_rule(conn, mod, acme, "acme-rule")
    assert is_ok(rule_a), rule_a
    rule_w = _add_rule(conn, mod, wayne, "wayne-rule")
    assert is_ok(rule_w), rule_w
    return {
        "acme": acme, "wayne": wayne,
        "acme_acct": acme_acct, "wayne_acct": wayne_acct,
        "acme_statement": imp_a["statement_id"], "wayne_statement": imp_w["statement_id"],
        "acme_rule": rule_a["id"], "wayne_rule": rule_w["id"],
    }


def _multi_expected(acme_id, wayne_id):
    return {
        "status": "error",
        "error": MULTI_ERROR,
        "companies": [
            {"id": acme_id, "name": "Acme Widgets"},
            {"id": wayne_id, "name": "Wayne Enterprises"},
        ],
        "suggestion": MULTI_SUGGESTION,
        "message": MULTI_ERROR,
    }


_ZERO_EXPECTED = {
    "status": "error",
    "error": ZERO_ERROR,
    "suggestion": ZERO_SUGGESTION,
    "message": ZERO_ERROR,
}

_UNKNOWN_EXPECTED = {
    "status": "error",
    "error": "Company not found: no-such-company",
    "message": "Company not found: no-such-company",
}


def _load_mod():
    from integration_helpers import load_db_query
    return load_db_query()


def test_statements_two_companies_no_company_refuses(conn):
    mod = _load_mod()
    ids = _two(conn, mod)
    before = _state(conn)
    r = call_action(mod.ACTIONS["integration-list-bank-statements"], conn,
                    ns(company_id=None, company_name=None))
    assert r == _multi_expected(ids["acme"], ids["wayne"])
    assert _state(conn) == before


def test_statements_zero_companies_refuses(conn):
    mod = _load_mod()
    r = call_action(mod.ACTIONS["integration-list-bank-statements"], conn,
                    ns(company_id=None, company_name=None))
    assert r == dict(_ZERO_EXPECTED)


def test_statements_unknown_company_refuses(conn):
    mod = _load_mod()
    ids = _two(conn, mod)
    before = _state(conn)
    r = call_action(mod.ACTIONS["integration-list-bank-statements"], conn,
                    ns(company_id="no-such-company", company_name=None))
    assert r == dict(_UNKNOWN_EXPECTED)
    assert _state(conn) == before


def test_statements_explicit_company(conn):
    mod = _load_mod()
    ids = _two(conn, mod)
    r = call_action(mod.ACTIONS["integration-list-bank-statements"], conn,
                    ns(company_id=ids["wayne"], company_name=None))
    assert is_ok(r), r
    assert r["total_count"] == 1
    assert [row["id"] for row in r["rows"]] == [ids["wayne_statement"]]
    assert r["rows"][0]["company_id"] == ids["wayne"]


def test_statements_one_company_uses_it(conn):
    mod = _load_mod()
    acme = _seed_company(conn, "Acme Widgets", "ACME")
    acct = _seed_bank_account(conn, acme, "Acme Checking")
    imp = _import_stmt(conn, mod, acme, acct)
    assert is_ok(imp), imp
    r_none = call_action(mod.ACTIONS["integration-list-bank-statements"], conn,
                         ns(company_id=None, company_name=None))
    r_scoped = call_action(mod.ACTIONS["integration-list-bank-statements"], conn,
                           ns(company_id=acme, company_name=None))
    assert is_ok(r_none), r_none
    assert is_ok(r_scoped), r_scoped
    assert r_none == r_scoped
    assert r_none["total_count"] == 1


def test_statements_company_name(conn):
    mod = _load_mod()
    ids = _two(conn, mod)
    r = call_action(mod.ACTIONS["integration-list-bank-statements"], conn,
                    ns(company_id=None, company_name="Wayne Enterprises"))
    assert is_ok(r), r
    assert r["total_count"] == 1
    assert [row["id"] for row in r["rows"]] == [ids["wayne_statement"]]


def test_rules_two_companies_no_company_refuses(conn):
    mod = _load_mod()
    ids = _two(conn, mod)
    before = _state(conn)
    r = call_action(mod.ACTIONS["integration-list-bank-match-rules"], conn,
                    ns(company_id=None, company_name=None))
    assert r == _multi_expected(ids["acme"], ids["wayne"])
    assert _state(conn) == before


def test_rules_zero_companies_refuses(conn):
    mod = _load_mod()
    r = call_action(mod.ACTIONS["integration-list-bank-match-rules"], conn,
                    ns(company_id=None, company_name=None))
    assert r == dict(_ZERO_EXPECTED)


def test_rules_unknown_company_refuses(conn):
    mod = _load_mod()
    ids = _two(conn, mod)
    before = _state(conn)
    r = call_action(mod.ACTIONS["integration-list-bank-match-rules"], conn,
                    ns(company_id="no-such-company", company_name=None))
    assert r == dict(_UNKNOWN_EXPECTED)
    assert _state(conn) == before


def test_rules_explicit_company(conn):
    mod = _load_mod()
    ids = _two(conn, mod)
    r = call_action(mod.ACTIONS["integration-list-bank-match-rules"], conn,
                    ns(company_id=ids["wayne"], company_name=None))
    assert is_ok(r), r
    assert r["count"] == 1
    assert [row["id"] for row in r["rows"]] == [ids["wayne_rule"]]
    assert r["rows"][0]["company_id"] == ids["wayne"]


def test_rules_one_company_uses_it(conn):
    mod = _load_mod()
    acme = _seed_company(conn, "Acme Widgets", "ACME")
    rule = _add_rule(conn, mod, acme, "acme-rule")
    assert is_ok(rule), rule
    r_none = call_action(mod.ACTIONS["integration-list-bank-match-rules"], conn,
                         ns(company_id=None, company_name=None))
    r_scoped = call_action(mod.ACTIONS["integration-list-bank-match-rules"], conn,
                           ns(company_id=acme, company_name=None))
    assert is_ok(r_none), r_none
    assert is_ok(r_scoped), r_scoped
    assert r_none == r_scoped
    assert r_none["count"] == 1


def test_rules_company_name(conn):
    mod = _load_mod()
    ids = _two(conn, mod)
    r = call_action(mod.ACTIONS["integration-list-bank-match-rules"], conn,
                    ns(company_id=None, company_name="Wayne Enterprises"))
    assert is_ok(r), r
    assert r["count"] == 1
    assert [row["id"] for row in r["rows"]] == [ids["wayne_rule"]]
