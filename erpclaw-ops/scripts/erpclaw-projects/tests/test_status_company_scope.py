"""Company scope for the projects status dashboard (m789-nocompany-p6-ops-status).

With no company given the dashboard uses the only company when exactly one
exists and refuses when there are none or several; an unknown explicit id is
refused. The refusal happens before the action reads its own tables, so every
refusal also pins the database unchanged.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from projects_helpers import (  # noqa: E402
    call_action, ns, is_ok, load_db_query,
    seed_company, seed_naming_series,
)
from erpclaw_lib.query import P, Q, Table  # noqa: E402

mod = load_db_query()

ACME_NAME = "Acme Widgets"
ACME_ABBR = "ACME"
WAYNE_NAME = "Wayne Enterprises"
WAYNE_ABBR = "WAYNE"

ZERO_COMPANY_ERROR = "No company found. Create one first."
ZERO_COMPANY_SUGGESTION = "Run 'tutorial' to create a demo company, or 'setup company' to create your own."
MULTI_COMPANY_ERROR = "Multiple companies found. Please specify the company by name."
MULTI_COMPANY_SUGGESTION = "Pass the company name (e.g. --company \"Acme\"), or use --company-id with one of the IDs above."
NAME_MISS_SUGGESTION = "Use one of the available company names exactly, or run 'list-companies' to see them."


def _seed_exact_companies(conn):
    acme = seed_company(conn, ACME_NAME, ACME_ABBR)
    wayne = seed_company(conn, WAYNE_NAME, WAYNE_ABBR)
    t = Table("company")
    for cid, name, abbr in ((acme, ACME_NAME, ACME_ABBR),
                            (wayne, WAYNE_NAME, WAYNE_ABBR)):
        uq = (Q.update(t).set("name", P()).set("abbr", P()).where(t.id == P()))
        conn.execute(uq.get_sql(), (name, abbr, cid))
    conn.commit()
    return acme, wayne


def _seed_acme_only(conn):
    acme = seed_company(conn, ACME_NAME, ACME_ABBR)
    t = Table("company")
    uq = (Q.update(t).set("name", P()).set("abbr", P()).where(t.id == P()))
    conn.execute(uq.get_sql(), (ACME_NAME, ACME_ABBR, acme))
    conn.commit()
    return acme


def _seed_both_projects(conn, acme, wayne):
    seed_naming_series(conn, acme)
    seed_naming_series(conn, wayne)
    r1 = call_action(mod.add_project, conn, ns(company_id=acme, name="Acme Project Alpha"))
    assert is_ok(r1), r1
    r2 = call_action(mod.add_project, conn, ns(company_id=wayne, name="Wayne Project Alpha"))
    assert is_ok(r2), r2


def _seed_acme_project(conn, acme):
    seed_naming_series(conn, acme)
    r = call_action(mod.add_project, conn, ns(company_id=acme, name="Acme Project Alpha"))
    assert is_ok(r), r


def _state(conn):
    out = {}
    for name in ("project", "company", "audit_log"):
        t = Table(name)
        rows = conn.execute(Q.from_(t).select(t.star).get_sql()).fetchall()
        out[name] = sorted(json.dumps(dict(r), sort_keys=True, default=str) for r in rows)
    return out


def _zero_dict():
    return {"status": "error", "error": ZERO_COMPANY_ERROR,
            "message": ZERO_COMPANY_ERROR,
            "suggestion": ZERO_COMPANY_SUGGESTION}


def _multi_dict(acme, wayne):
    return {"status": "error", "error": MULTI_COMPANY_ERROR,
            "message": MULTI_COMPANY_ERROR,
            "companies": [{"id": acme, "name": ACME_NAME},
                          {"id": wayne, "name": WAYNE_NAME}],
            "suggestion": MULTI_COMPANY_SUGGESTION}


def test_status_two_companies_no_company_refuses(conn):
    acme, wayne = _seed_exact_companies(conn)
    _seed_both_projects(conn, acme, wayne)
    before = _state(conn)
    result = call_action(mod.status_action, conn, ns(company_id=None))
    assert result == _multi_dict(acme, wayne)
    assert _state(conn) == before


def test_status_zero_companies_refuses(conn):
    before = _state(conn)
    result = call_action(mod.status_action, conn, ns(company_id=None))
    assert result == _zero_dict()
    assert _state(conn) == before


def test_status_unknown_company_refuses(conn):
    acme, wayne = _seed_exact_companies(conn)
    _seed_both_projects(conn, acme, wayne)
    before = _state(conn)
    result = call_action(mod.status_action, conn, ns(company_id="no-such-company"))
    assert result == {"status": "error",
                      "error": "Company not found: no-such-company",
                      "message": "Company not found: no-such-company"}
    assert _state(conn) == before


def test_status_explicit_company(conn):
    acme, wayne = _seed_exact_companies(conn)
    _seed_both_projects(conn, acme, wayne)
    r = call_action(mod.status_action, conn, ns(company_id=wayne))
    assert is_ok(r), r
    assert r["company_id"] == wayne
    assert r["active_projects"] == 1
    assert r["projects_by_status"] == {"open": 1}
    assert r["overdue_tasks_count"] == 0
    assert r["overdue_tasks"] == []
    assert r["upcoming_milestones"] == []
    assert r["missed_milestones"] == []
    assert r["recent_timesheets"] == []
    assert r["hours_this_month"] == {"total": "0.00", "billable": "0.00"}


def test_status_one_company_uses_it(conn):
    acme = _seed_acme_only(conn)
    _seed_acme_project(conn, acme)
    implicit = call_action(mod.status_action, conn, ns(company_id=None))
    explicit = call_action(mod.status_action, conn, ns(company_id=acme))
    assert is_ok(implicit), implicit
    assert implicit == explicit


def test_company_flag_resolves_name(conn):
    acme, wayne = _seed_exact_companies(conn)
    _seed_both_projects(conn, acme, wayne)
    args = ns(company_id=None, company_name="wayne enterprises")
    mod._resolve_company_flag(conn, args)
    assert args.company_id == wayne
    args_id = ns(company_id=None, company_name=wayne)
    mod._resolve_company_flag(conn, args_id)
    assert args_id.company_id == wayne
    before = _state(conn)
    result = call_action(mod._resolve_company_flag, conn,
                         ns(company_id=None, company_name="Wayne"))
    assert result == {"status": "error",
                      "error": "Company 'Wayne' not found.",
                      "message": "Company 'Wayne' not found.",
                      "available_companies": [ACME_NAME, WAYNE_NAME],
                      "suggestion": NAME_MISS_SUGGESTION}
    assert _state(conn) == before
