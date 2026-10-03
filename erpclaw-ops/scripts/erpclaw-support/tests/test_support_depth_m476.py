"""Behavioural depth for ten erpclaw-support actions (task m476).

Each action below previously had only a shape test (asserts on the response
envelope, e.g. ``is_ok`` plus a key lookup) or a routability test (the
contract suite's ``"Unknown action" not in ...``). Neither observes the
database, so an action could return a perfect envelope while writing nothing
-- or the wrong thing -- and stay green. Every happy-path test here drives
the REAL action against a fresh DB, reads the stored rows back with
PyPika-built queries through ``erpclaw_lib.query`` on a connection from
``erpclaw_lib.db.get_connection``, and compares exact values; money is
compared as exact ``Decimal`` strings, never float. Every refusal test proves
the error message is truthful and the database is byte-identical afterwards.

Per-action depth (stored row vs ledger effect):

- add-maintenance-schedule: stored row (customer, item, frequency, dates,
  computed next_due_date, active status, audit). Reaches no ledger; the
  zero-count checks below pin that, so no debit/credit assertion can hold.
- list-issues: read-only. Pins the response against the stored rows under
  several filters and proves no table changed. No ledger assertion can hold
  for a pure read, so none is made.
- list-maintenance-schedules: read-only, same no-ledger note as list-issues.
- overdue-issues-report: read-only. Pins exactly which stored issues are
  reported (and which are not) and proves no table changed. No ledger
  assertion can hold for a pure read, so none is made.
- record-maintenance-visit: stored rows (the visit plus the schedule
  side-effect on completed visits, including the expired flip). Reaches no
  ledger; zero-count checks pin that.
- reopen-issue: stored row (open from resolved, resolved_at and notes
  cleared, breach flag kept). Reaches no ledger; zero-count checks pin that.
- resolve-issue: stored row (resolved from open, resolved_at stamped, notes
  kept). Reaches no ledger; zero-count checks pin that.
- sla-compliance-report: read-only. Pins every reported counter against the
  stored issues and proves no table changed. No ledger assertion can hold
  for a pure read, so none is made.
- update-issue: stored row (priority/status/description/assigned_to moved,
  everything else kept, audit carries the old row). Reaches no ledger;
  zero-count checks pin that.
- update-warranty-claim: stored row (status, resolution, resolution date and
  the exact money text) plus audit. Money is text: the cost is stored as the
  exact string given. Reaches no ledger; zero-count checks pin that.

No test in this file inspects catalog tables or sets connection options;
reads are PyPika-built and run on a connection from
``erpclaw_lib.db.get_connection``.
"""
import json
import os
import sys
import uuid
from decimal import Decimal

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from support_helpers import (  # noqa: E402
    build_env,
    call_action,
    is_error,
    is_ok,
    load_db_query,
    ns,
    seed_customer,
)
from erpclaw_lib.db import get_connection  # noqa: E402
from erpclaw_lib.query import (  # noqa: E402
    Field,
    P,
    Q,
    Table,
    fn,
    update_row,
)

M = load_db_query()


@pytest.fixture
def conn(db_path):
    connection = get_connection(db_path)
    yield connection
    connection.close()


@pytest.fixture
def env(conn):
    return build_env(conn)


SUPPORT_TABLES = (
    "issue",
    "issue_comment",
    "service_level_agreement",
    "warranty_claim",
    "maintenance_schedule",
    "maintenance_visit",
    "audit_log",
    "naming_series",
)

LEDGERS = ("gl_entry", "stock_ledger_entry", "payment_ledger_entry")


def _msg(result):
    return result.get("message", "")


def _row(conn, table, row_id):
    t = Table(table)
    q = Q.from_(t).select(t.star).where(t.id == P())
    found = conn.execute(q.get_sql(), (row_id,)).fetchone()
    assert found is not None, "%s %s not found" % (table, row_id)
    return dict(found)


def _where(conn, table, **filters):
    t = Table(table)
    q = Q.from_(t).select(t.star)
    params = []
    for column, value in filters.items():
        q = q.where(Field(column) == P())
        params.append(value)
    return [dict(r) for r in conn.execute(q.get_sql(), params).fetchall()]


def _all(conn, table):
    t = Table(table)
    q = Q.from_(t).select(t.star).orderby(t.id)
    return [dict(r) for r in conn.execute(q.get_sql()).fetchall()]


def _count(conn, table):
    t = Table(table)
    q = Q.from_(t).select(fn.Count("*").as_("n"))
    return conn.execute(q.get_sql()).fetchone()["n"]


def _snapshot(conn, tables):
    return {name: _all(conn, name) for name in tables}


def _audits(conn, action, entity_id):
    rows = _where(conn, "audit_log", action=action, entity_id=entity_id)
    return [r for r in rows if r["skill"] == "erpclaw-support"]


def _ledgers_empty(conn):
    return [_count(conn, t) for t in LEDGERS] == [0, 0, 0]


def _add_issue(conn, env, **over):
    kw = {"subject": "Depth issue %s" % uuid.uuid4().hex[:6],
          "customer_id": env["customer_id"]}
    kw.update(over)
    r = call_action(M.add_issue, conn, ns(**kw))
    assert is_ok(r), r
    return r["issue"]["id"]


def _add_sla(conn):
    r = call_action(M.add_sla, conn, ns(
        name="Depth SLA %s" % uuid.uuid4().hex[:6],
        priorities=json.dumps({
            "response_times": {"low": 24, "medium": 8, "high": 4,
                               "critical": 1},
            "resolution_times": {"low": 72, "medium": 24, "high": 12,
                                 "critical": 4},
        }),
    ))
    assert is_ok(r), r
    return r["sla"]["id"]


def _set_issue_dues(conn, issue_id, response_due, resolution_due):
    sql = update_row("issue", {"response_due": P(), "resolution_due": P()},
                     where={"id": P()})
    conn.execute(sql, (response_due, resolution_due, issue_id))
    conn.commit()


def _set_breached(conn, issue_id, value=1):
    sql = update_row("issue", {"sla_breached": P()}, where={"id": P()})
    conn.execute(sql, (value, issue_id))
    conn.commit()


# ---------------------------------------------------------------------------
# add-maintenance-schedule -- stored row (no ledger legs by design).
# ---------------------------------------------------------------------------

class TestAddMaintenanceScheduleDepth:
    def test_add_schedule_stores_exact_row_with_computed_next_due(
            self, conn, env):
        decoy = call_action(M.add_maintenance_schedule, conn, ns(
            customer_id=env["customer_id"],
            schedule_frequency="quarterly",
            start_date="2026-03-01",
            end_date="2026-12-31",
        ))
        assert is_ok(decoy), decoy
        decoy_row = _row(conn, "maintenance_schedule",
                         decoy["maintenance_schedule"]["id"])
        ledgers_before = _snapshot(conn, LEDGERS)

        r = call_action(M.add_maintenance_schedule, conn, ns(
            customer_id=env["customer_id"],
            item_id=env["item_id"],
            schedule_frequency="monthly",
            start_date="2026-01-01",
            end_date="2026-12-31",
            assigned_to="tech-1",
        ))
        assert is_ok(r), r
        sid = r["maintenance_schedule"]["id"]

        stored = _row(conn, "maintenance_schedule", sid)
        assert stored["customer_id"] == env["customer_id"]
        assert stored["item_id"] == env["item_id"]
        assert stored["schedule_frequency"] == "monthly"
        assert (stored["start_date"], stored["end_date"]) == (
            "2026-01-01", "2026-12-31")
        # 2026-01-01 plus 30 days, computed by hand.
        assert stored["next_due_date"] == "2026-01-31"
        assert stored["status"] == "active"
        assert stored["assigned_to"] == "tech-1"
        assert stored["last_completed_date"] is None
        assert stored["naming_series"]
        assert stored["naming_series"] == r["maintenance_schedule"][
            "naming_series"]
        assert r["maintenance_schedule"]["next_due_date"] == "2026-01-31"

        # The schedule that was not named is byte-identical.
        assert _row(conn, "maintenance_schedule",
                    decoy["maintenance_schedule"]["id"]) == decoy_row
        # A schedule creates no visits.
        assert _count(conn, "maintenance_visit") == 0
        # One audit row names the new schedule.
        audits = _audits(conn, "add-maintenance-schedule", sid)
        assert len(audits) == 1
        assert audits[0]["description"].startswith(
            "Created maintenance schedule")
        # Master-data write: no ledger legs may appear.
        assert _snapshot(conn, LEDGERS) == ledgers_before
        assert _ledgers_empty(conn)

    def test_add_schedule_refusals_write_nothing(self, conn, env):
        sid = call_action(M.add_maintenance_schedule, conn, ns(
            customer_id=env["customer_id"],
            start_date="2026-01-01",
            end_date="2026-12-31",
        ))
        assert is_ok(sid), sid
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)
        missing = str(uuid.uuid4())

        cases = [
            ({"start_date": "2026-01-01", "end_date": "2026-12-31"},
             "--customer-id is required"),
            ({"customer_id": env["customer_id"], "end_date": "2026-12-31"},
             "--start-date is required"),
            ({"customer_id": env["customer_id"], "start_date": "2026-01-01"},
             "--end-date is required"),
            ({"customer_id": env["customer_id"], "start_date": "2026-01-01",
              "end_date": "2026-12-31", "schedule_frequency": "weekly"},
             "--schedule-frequency must be one of ('monthly', 'quarterly', "
             "'semi_annual', 'annual')"),
            ({"customer_id": missing, "start_date": "2026-01-01",
              "end_date": "2026-12-31"},
             "Customer %s not found" % missing),
        ]
        for kwargs, message in cases:
            r = call_action(M.add_maintenance_schedule, conn, ns(**kwargs))
            assert is_error(r), (kwargs, r)
            assert _msg(r) == message, (kwargs, r)

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot


# ---------------------------------------------------------------------------
# list-issues -- read-only. The action validates nothing, so there is no
# refusal path in the code; the guard test pins the truthful empty result
# and the no-write proof instead.
# ---------------------------------------------------------------------------

class TestListIssuesDepth:
    def test_list_issues_matches_stored_rows_and_writes_nothing(
            self, conn, env):
        alpha = _add_issue(conn, env, subject="Depth Alpha", priority="high")
        beta = _add_issue(conn, env, subject="Depth Beta", priority="low")
        gamma = _add_issue(conn, env, subject="Depth Gamma", priority="high")
        assert is_ok(call_action(M.update_issue, conn, ns(
            issue_id=gamma, status="resolved")))
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)

        r = call_action(M.list_issues, conn, ns(
            company_id=env["company_id"]))
        assert is_ok(r), r
        assert r["total"] == 3
        by_id = {e["id"]: e for e in r["issues"]}
        assert set(by_id) == {alpha, beta, gamma}
        for issue_id in (alpha, beta, gamma):
            stored = _row(conn, "issue", issue_id)
            for column in ("subject", "priority", "status", "customer_id"):
                assert by_id[issue_id][column] == stored[column], column

        r = call_action(M.list_issues, conn, ns(
            company_id=env["company_id"], status="open"))
        assert is_ok(r), r
        assert r["total"] == 2
        assert {e["id"] for e in r["issues"]} == {alpha, beta}

        r = call_action(M.list_issues, conn, ns(
            company_id=env["company_id"], priority="high"))
        assert is_ok(r), r
        assert r["total"] == 2
        assert {e["id"] for e in r["issues"]} == {alpha, gamma}

        r = call_action(M.list_issues, conn, ns(
            customer_id=env["customer_id"]))
        assert is_ok(r), r
        assert r["total"] == 3

        # A pure read writes nothing, not even an audit row.
        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot

    def test_list_issues_empty_filter_is_truthful_and_writes_nothing(
            self, conn, env):
        _add_issue(conn, env, subject="Depth Solo", priority="low")
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)

        r = call_action(M.list_issues, conn, ns(
            company_id=env["company_id"], status="closed"))
        assert is_ok(r), r
        assert r["issues"] == []
        assert r["total"] == 0

        r = call_action(M.list_issues, conn, ns(
            customer_id=str(uuid.uuid4())))
        assert is_ok(r), r
        assert r["total"] == 0

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot


# ---------------------------------------------------------------------------
# list-maintenance-schedules -- read-only, same no-refusal note as
# list-issues: no err() path exists, so the guard pins the truthful empty
# result plus the no-write proof.
# ---------------------------------------------------------------------------

class TestListMaintenanceSchedulesDepth:
    def test_list_schedules_matches_stored_rows_and_writes_nothing(
            self, conn, env):
        other = seed_customer(conn, env["company_id"], "Second Client")
        s1 = call_action(M.add_maintenance_schedule, conn, ns(
            customer_id=env["customer_id"],
            item_id=env["item_id"],
            schedule_frequency="monthly",
            start_date="2026-01-01",
            end_date="2026-12-31",
        ))
        assert is_ok(s1), s1
        s2 = call_action(M.add_maintenance_schedule, conn, ns(
            customer_id=other,
            schedule_frequency="quarterly",
            start_date="2026-02-01",
            end_date="2026-11-30",
        ))
        assert is_ok(s2), s2
        id1 = s1["maintenance_schedule"]["id"]
        id2 = s2["maintenance_schedule"]["id"]
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)

        r = call_action(M.list_maintenance_schedules, conn, ns())
        assert is_ok(r), r
        assert r["total"] == 2
        by_id = {e["id"]: e for e in r["maintenance_schedules"]}
        assert set(by_id) == {id1, id2}
        for sid in (id1, id2):
            stored = _row(conn, "maintenance_schedule", sid)
            for column in ("customer_id", "schedule_frequency", "start_date",
                           "end_date", "next_due_date", "status"):
                assert by_id[sid][column] == stored[column], column
        assert by_id[id1]["next_due_date"] == "2026-01-31"
        assert by_id[id2]["next_due_date"] == "2026-05-02"

        r = call_action(M.list_maintenance_schedules, conn, ns(
            customer_id=env["customer_id"]))
        assert is_ok(r), r
        assert r["total"] == 1
        assert r["maintenance_schedules"][0]["id"] == id1

        r = call_action(M.list_maintenance_schedules, conn, ns(
            status="active"))
        assert is_ok(r), r
        assert r["total"] == 2

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot

    def test_list_schedules_empty_filter_is_truthful_and_writes_nothing(
            self, conn, env):
        s = call_action(M.add_maintenance_schedule, conn, ns(
            customer_id=env["customer_id"],
            start_date="2026-01-01",
            end_date="2026-12-31",
        ))
        assert is_ok(s), s
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)

        r = call_action(M.list_maintenance_schedules, conn, ns(
            status="cancelled"))
        assert is_ok(r), r
        assert r["maintenance_schedules"] == []
        assert r["total"] == 0

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot


# ---------------------------------------------------------------------------
# overdue-issues-report -- read-only, same no-refusal note as list-issues.
# ---------------------------------------------------------------------------

class TestOverdueIssuesReportDepth:
    def test_overdue_report_names_only_the_past_due_open_issue(
            self, conn, env):
        overdue = _add_issue(conn, env, subject="Depth Overdue")
        future = _add_issue(conn, env, subject="Depth Future")
        settled = _add_issue(conn, env, subject="Depth Settled")
        _set_issue_dues(conn, overdue, "2020-01-01 00:00:00",
                        "2020-01-02 00:00:00")
        _set_issue_dues(conn, future, "2099-01-01 00:00:00",
                        "2099-01-02 00:00:00")
        _set_issue_dues(conn, settled, "2020-01-01 00:00:00",
                        "2020-01-02 00:00:00")
        assert is_ok(call_action(M.resolve_issue, conn, ns(
            issue_id=settled, resolution_notes="Done")))
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)

        r = call_action(M.overdue_issues_report, conn, ns())
        assert is_ok(r), r
        assert r["total"] == 1
        assert len(r["overdue_issues"]) == 1
        entry = r["overdue_issues"][0]
        assert entry["id"] == overdue
        assert entry["subject"] == "Depth Overdue"
        assert entry["overdue_response"] is True
        assert entry["overdue_resolution"] is True
        assert r["as_of"]
        assert _msg(r) == "1 overdue issue(s) found"

        stored = _row(conn, "issue", overdue)
        assert entry["response_due"] == stored["response_due"]
        assert entry["resolution_due"] == stored["resolution_due"]

        # A future-due open issue and a past-due resolved issue stay out.
        r = call_action(M.overdue_issues_report, conn, ns(
            company_id=env["company_id"]))
        assert is_ok(r), r
        assert {e["id"] for e in r["overdue_issues"]} == {overdue}

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot

    def test_overdue_report_empty_company_is_truthful_and_writes_nothing(
            self, conn, env):
        _add_issue(conn, env, subject="Depth Current")
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)

        r = call_action(M.overdue_issues_report, conn, ns())
        assert is_ok(r), r
        assert r["total"] == 0
        assert r["overdue_issues"] == []

        r = call_action(M.overdue_issues_report, conn, ns(
            company_id=str(uuid.uuid4())))
        assert is_ok(r), r
        assert r["total"] == 0

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot


# ---------------------------------------------------------------------------
# record-maintenance-visit -- stored rows (no ledger legs by design).
# ---------------------------------------------------------------------------

class TestRecordMaintenanceVisitDepth:
    def test_scheduled_leaves_schedule_completed_advances_it(
            self, conn, env):
        s = call_action(M.add_maintenance_schedule, conn, ns(
            customer_id=env["customer_id"],
            schedule_frequency="quarterly",
            start_date="2026-01-01",
            end_date="2026-12-31",
        ))
        assert is_ok(s), s
        sid = s["maintenance_schedule"]["id"]
        assert _row(conn, "maintenance_schedule", sid)[
            "next_due_date"] == "2026-04-01"

        r = call_action(M.record_maintenance_visit, conn, ns(
            schedule_id=sid,
            visit_date="2026-01-10",
            status="scheduled",
            completed_by="amy",
        ))
        assert is_ok(r), r
        assert r["schedule_updated"] is False
        assert "schedule" not in r
        visit = _row(conn, "maintenance_visit", r["visit"]["id"])
        assert visit["maintenance_schedule_id"] == sid
        assert visit["customer_id"] == env["customer_id"]
        assert (visit["visit_date"], visit["completed_by"],
                visit["status"]) == ("2026-01-10", "amy", "scheduled")
        # A scheduled visit touches no schedule field.
        sched = _row(conn, "maintenance_schedule", sid)
        assert (sched["last_completed_date"], sched["next_due_date"],
                sched["status"]) == (None, "2026-04-01", "active")

        r = call_action(M.record_maintenance_visit, conn, ns(
            schedule_id=sid,
            visit_date="2026-02-15",
            status="completed",
            completed_by="amy",
            observations="All good",
            work_done="Oiled bearings",
        ))
        assert is_ok(r), r
        assert r["schedule_updated"] is True
        visit = _row(conn, "maintenance_visit", r["visit"]["id"])
        assert (visit["visit_date"], visit["completed_by"],
                visit["observations"], visit["work_done"],
                visit["status"]) == ("2026-02-15", "amy", "All good",
                                     "Oiled bearings", "completed")
        # 2026-02-15 plus 90 days, computed by hand; still inside the
        # schedule window, so the schedule stays active.
        sched = _row(conn, "maintenance_schedule", sid)
        assert (sched["last_completed_date"], sched["next_due_date"],
                sched["status"]) == ("2026-02-15", "2026-05-16", "active")
        assert r["schedule"]["next_due_date"] == "2026-05-16"
        assert r["schedule"]["last_completed_date"] == "2026-02-15"

        assert len(_audits(conn, "record-maintenance-visit",
                           r["visit"]["id"])) == 1
        assert _ledgers_empty(conn)

    def test_completed_visit_past_the_end_date_expires_the_schedule(
            self, conn, env):
        s = call_action(M.add_maintenance_schedule, conn, ns(
            customer_id=env["customer_id"],
            schedule_frequency="monthly",
            start_date="2026-01-01",
            end_date="2026-02-10",
        ))
        assert is_ok(s), s
        sid = s["maintenance_schedule"]["id"]

        r = call_action(M.record_maintenance_visit, conn, ns(
            schedule_id=sid,
            visit_date="2026-02-05",
            status="completed",
        ))
        assert is_ok(r), r
        assert r["schedule_updated"] is True
        # 2026-02-05 plus 30 days lands past the end date, so the
        # schedule flips to expired.
        sched = _row(conn, "maintenance_schedule", sid)
        assert (sched["last_completed_date"], sched["next_due_date"],
                sched["status"]) == ("2026-02-05", "2026-03-07", "expired")
        assert _ledgers_empty(conn)

    def test_record_visit_refusals_write_nothing(self, conn, env):
        s = call_action(M.add_maintenance_schedule, conn, ns(
            customer_id=env["customer_id"],
            start_date="2026-01-01",
            end_date="2026-12-31",
        ))
        assert is_ok(s), s
        sid = s["maintenance_schedule"]["id"]
        before = _row(conn, "maintenance_schedule", sid)
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)
        missing = str(uuid.uuid4())

        cases = [
            ({"visit_date": "2026-02-01"}, "--schedule-id is required"),
            ({"schedule_id": sid}, "--visit-date is required"),
            ({"schedule_id": sid, "visit_date": "2026-02-01",
              "status": "done"},
             "--status must be one of ('scheduled', 'completed', "
             "'cancelled')"),
            ({"schedule_id": missing, "visit_date": "2026-02-01"},
             "Maintenance schedule %s not found" % missing),
        ]
        for kwargs, message in cases:
            r = call_action(M.record_maintenance_visit, conn, ns(**kwargs))
            assert is_error(r), (kwargs, r)
            assert _msg(r) == message, (kwargs, r)
            assert _row(conn, "maintenance_schedule", sid) == before

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot


# ---------------------------------------------------------------------------
# reopen-issue -- stored row (no ledger legs by design).
# ---------------------------------------------------------------------------

class TestReopenIssueDepth:
    def test_reopen_clears_resolution_fields_and_keeps_the_breach(
            self, conn, env):
        iid = _add_issue(conn, env, subject="Depth Reopen")
        decoy = _add_issue(conn, env, subject="Depth Decoy")
        decoy_before = _row(conn, "issue", decoy)
        assert is_ok(call_action(M.resolve_issue, conn, ns(
            issue_id=iid, resolution_notes="Restarted service")))
        resolved = _row(conn, "issue", iid)
        assert resolved["status"] == "resolved"
        assert resolved["resolved_at"] is not None
        assert resolved["resolution_notes"] == "Restarted service"

        r = call_action(M.reopen_issue, conn, ns(
            issue_id=iid, reason="Issue recurred"))
        assert is_ok(r), r

        stored = _row(conn, "issue", iid)
        assert stored["status"] == "open"
        assert stored["resolved_at"] is None
        assert stored["resolution_notes"] is None
        # A breach, once set, stays set across a reopen.
        assert stored["sla_breached"] == resolved["sla_breached"]
        # Untouched columns survive the round trip.
        assert (stored["subject"], stored["priority"],
                stored["customer_id"]) == (
            resolved["subject"], resolved["priority"],
            resolved["customer_id"])
        assert r["issue"]["status"] == "open"

        # The reason lives in the audit trail, not on the row.
        audits = _audits(conn, "reopen-issue", iid)
        assert len(audits) == 1
        assert audits[0]["description"] == "Issue reopened: Issue recurred"
        assert json.loads(audits[0]["old_values"])["status"] == "resolved"
        assert json.loads(audits[0]["new_values"])["status"] == "open"

        # An issue that was not named is byte-identical.
        assert _row(conn, "issue", decoy) == decoy_before
        assert _ledgers_empty(conn)

    def test_reopen_refusals_write_nothing(self, conn, env):
        iid = _add_issue(conn, env, subject="Depth Reopen Guard")
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)
        missing = str(uuid.uuid4())

        r = call_action(M.reopen_issue, conn, ns(issue_id=None))
        assert is_error(r), r
        assert _msg(r) == "--issue-id is required"

        r = call_action(M.reopen_issue, conn, ns(issue_id=missing))
        assert is_error(r), r
        assert _msg(r) == "Issue %s not found" % missing

        # An open issue cannot be reopened; the message names the status.
        r = call_action(M.reopen_issue, conn, ns(issue_id=iid))
        assert is_error(r), r
        assert _msg(r) == ("Cannot reopen issue with status 'open'. Only "
                           "resolved or closed issues can be reopened.")
        assert _row(conn, "issue", iid)["status"] == "open"

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot


# ---------------------------------------------------------------------------
# resolve-issue -- stored row (no ledger legs by design).
# ---------------------------------------------------------------------------

class TestResolveIssueDepth:
    def test_resolve_stamps_status_date_and_notes(self, conn, env):
        iid = _add_issue(conn, env, subject="Depth Resolve", priority="high")
        decoy = _add_issue(conn, env, subject="Depth Resolve Decoy")
        decoy_before = _row(conn, "issue", decoy)
        before = _row(conn, "issue", iid)
        assert before["status"] == "open"
        assert before["resolved_at"] is None

        r = call_action(M.resolve_issue, conn, ns(
            issue_id=iid, resolution_notes="Replaced toner"))
        assert is_ok(r), r

        stored = _row(conn, "issue", iid)
        assert stored["status"] == "resolved"
        assert stored["resolved_at"] is not None
        assert stored["resolution_notes"] == "Replaced toner"
        # Untouched columns survive the resolve.
        assert (stored["subject"], stored["priority"],
                stored["customer_id"]) == (
            before["subject"], before["priority"], before["customer_id"])
        assert r["issue"]["status"] == "resolved"
        assert r["issue"]["resolution_notes"] == "Replaced toner"

        audits = _audits(conn, "resolve-issue", iid)
        assert len(audits) == 1
        assert json.loads(audits[0]["old_values"])["status"] == "open"
        assert json.loads(audits[0]["new_values"])["status"] == "resolved"

        assert _row(conn, "issue", decoy) == decoy_before
        assert _ledgers_empty(conn)

    def test_resolve_refusals_write_nothing(self, conn, env):
        iid = _add_issue(conn, env, subject="Depth Resolve Guard")
        assert is_ok(call_action(M.resolve_issue, conn, ns(issue_id=iid)))
        resolved = _row(conn, "issue", iid)
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)
        missing = str(uuid.uuid4())

        r = call_action(M.resolve_issue, conn, ns(issue_id=None))
        assert is_error(r), r
        assert _msg(r) == "--issue-id is required"

        r = call_action(M.resolve_issue, conn, ns(issue_id=missing))
        assert is_error(r), r
        assert _msg(r) == "Issue %s not found" % missing

        # Resolving twice is refused truthfully.
        r = call_action(M.resolve_issue, conn, ns(issue_id=iid))
        assert is_error(r), r
        assert _msg(r) == "Issue is already resolved."
        assert _row(conn, "issue", iid) == resolved

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot


# ---------------------------------------------------------------------------
# sla-compliance-report -- read-only. The action validates nothing, so there
# is no refusal path in the code; the guard test pins the truthful zeroes
# and the no-write proof instead.
# ---------------------------------------------------------------------------

class TestSlaComplianceReportDepth:
    def test_compliance_counts_are_pinned_to_stored_issues(
            self, conn, env):
        sla_id = _add_sla(conn)
        breached = _add_issue(conn, env, subject="Depth Breached",
                              priority="high", sla_id=sla_id)
        compliant = _add_issue(conn, env, subject="Depth Compliant",
                               priority="low", sla_id=sla_id)
        pending = _add_issue(conn, env, subject="Depth Pending",
                             priority="low", sla_id=sla_id)
        unslaed = _add_issue(conn, env, subject="Depth No SLA")
        _set_issue_dues(conn, breached, "2020-01-01 00:00:00",
                        "2020-01-02 00:00:00")
        assert is_ok(call_action(M.resolve_issue, conn, ns(
            issue_id=breached)))
        assert is_ok(call_action(M.resolve_issue, conn, ns(
            issue_id=compliant)))
        assert _row(conn, "issue", breached)["sla_breached"] == 1
        assert _row(conn, "issue", compliant)["sla_breached"] == 0
        assert _row(conn, "issue", unslaed)["sla_id"] is None
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)

        r = call_action(M.sla_compliance_report, conn, ns())
        assert is_ok(r), r
        report = r["report"]
        # Three issues carry an SLA; the unslaed one is out of scope.
        assert report["total_with_sla"] == 3
        assert report["breached"] == 1
        assert report["compliant"] == 1
        # Only the still-open SLA issue is in progress.
        assert report["in_progress"] == 1
        # 1 compliant of 2 decided, computed by hand.
        assert report["compliance_rate_pct"] == "50.00"
        assert Decimal(report["compliance_rate_pct"]) == Decimal("50.00")
        assert r["filters"] == {}
        assert _msg(r) == "SLA compliance: 50.00% (1/2)"

        r = call_action(M.sla_compliance_report, conn, ns(
            company_id=env["company_id"]))
        assert is_ok(r), r
        assert r["report"]["total_with_sla"] == 3

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot

    def test_compliance_empty_scope_is_truthful_and_writes_nothing(
            self, conn, env):
        _add_issue(conn, env, subject="Depth Unslaed")
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)

        r = call_action(M.sla_compliance_report, conn, ns())
        assert is_ok(r), r
        assert r["report"]["total_with_sla"] == 0
        assert r["report"]["compliance_rate_pct"] == "0.00"
        assert _msg(r) == "SLA compliance: 0.00% (0/0)"

        r = call_action(M.sla_compliance_report, conn, ns(
            company_id=str(uuid.uuid4())))
        assert is_ok(r), r
        assert r["report"]["total_with_sla"] == 0

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot


# ---------------------------------------------------------------------------
# update-issue -- stored row (no ledger legs by design).
# ---------------------------------------------------------------------------

class TestUpdateIssueDepth:
    def test_update_moves_fields_and_keeps_the_rest(self, conn, env):
        iid = _add_issue(conn, env, subject="Depth Update", priority="medium")
        decoy = _add_issue(conn, env, subject="Depth Update Decoy")
        decoy_before = _row(conn, "issue", decoy)
        before = _row(conn, "issue", iid)
        assert (before["status"], before["priority"],
                before["description"]) == ("open", "medium", None)

        r = call_action(M.update_issue, conn, ns(
            issue_id=iid,
            status="in_progress",
            priority="high",
            description="Investigating root cause",
            assigned_to="tech-1",
        ))
        assert is_ok(r), r

        stored = _row(conn, "issue", iid)
        assert stored["status"] == "in_progress"
        assert stored["priority"] == "high"
        assert stored["description"] == "Investigating root cause"
        assert stored["assigned_to"] == "tech-1"
        # Untouched columns survive the update.
        assert (stored["subject"], stored["customer_id"],
                stored["issue_type"]) == (
            before["subject"], before["customer_id"],
            before["issue_type"])
        assert r["issue"]["status"] == "in_progress"
        assert r["issue"]["priority"] == "high"

        audits = _audits(conn, "update-issue", iid)
        assert len(audits) == 1
        assert json.loads(audits[0]["old_values"])["priority"] == "medium"

        assert _row(conn, "issue", decoy) == decoy_before
        assert _ledgers_empty(conn)

    def test_update_refusals_write_nothing(self, conn, env):
        iid = _add_issue(conn, env, subject="Depth Update Guard")
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)
        missing = str(uuid.uuid4())

        r = call_action(M.update_issue, conn, ns(
            issue_id=None, priority="high"))
        assert is_error(r), r
        assert _msg(r) == "--issue-id is required"

        r = call_action(M.update_issue, conn, ns(
            issue_id=missing, priority="high"))
        assert is_error(r), r
        assert _msg(r) == "Issue %s not found" % missing

        r = call_action(M.update_issue, conn, ns(
            issue_id=iid, priority="extreme"))
        assert is_error(r), r
        assert _msg(r) == ("--priority must be one of ('low', 'medium', "
                           "'high', 'critical')")

        r = call_action(M.update_issue, conn, ns(
            issue_id=iid, status="bogus"))
        assert is_error(r), r
        assert _msg(r) == ("--status must be one of ('open', 'in_progress', "
                           "'waiting_on_customer', 'resolved', 'closed')")

        r = call_action(M.update_issue, conn, ns(issue_id=iid))
        assert is_error(r), r
        assert _msg(r) == ("No fields to update. Provide at least one "
                           "optional flag.")

        # A valid priority never reaches the row behind an invalid status.
        r = call_action(M.update_issue, conn, ns(
            issue_id=iid, priority="high", status="bogus"))
        assert is_error(r), r
        assert _row(conn, "issue", iid)["priority"] == "medium"

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot

    def test_update_closed_issue_is_refused_and_kept(self, conn, env):
        iid = _add_issue(conn, env, subject="Depth Closed Guard")
        assert is_ok(call_action(M.update_issue, conn, ns(
            issue_id=iid, status="closed")))
        closed = _row(conn, "issue", iid)
        assert closed["status"] == "closed"
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)

        r = call_action(M.update_issue, conn, ns(
            issue_id=iid, priority="high"))
        assert is_error(r), r
        assert _msg(r) == "Cannot update a closed issue. Reopen it first."
        assert _row(conn, "issue", iid) == closed

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot


# ---------------------------------------------------------------------------
# update-warranty-claim -- stored row plus exact money text (no ledger legs
# by design). Complements test_warranty_claim_update_behaviour.py, which
# reads the same rows back through raw SQL; these tests read them back
# through PyPika-built queries.
# ---------------------------------------------------------------------------

EXPIRY_DATE = "2027-01-31"
RESOLUTION_DATE = "2026-04-15"
COMPLAINT = "Motor stalls under load"


def _add_claim(conn, env):
    r = call_action(M.add_warranty_claim, conn, ns(
        customer_id=env["customer_id"],
        item_id=env["item_id"],
        warranty_expiry_date=EXPIRY_DATE,
        complaint_description=COMPLAINT,
    ))
    assert is_ok(r), r
    return r["warranty_claim"]["id"]


def _claim_tuple(conn, claim_id):
    row = _row(conn, "warranty_claim", claim_id)
    return (row["customer_id"], row["item_id"],
            row["warranty_expiry_date"], row["complaint_description"],
            row["status"], row["resolution"], row["resolution_date"],
            row["cost"])


class TestUpdateWarrantyClaimDepth:
    def test_cost_is_stored_as_exact_text_and_resolves_in_full(
            self, conn, env):
        claim_id = _add_claim(conn, env)
        assert _claim_tuple(conn, claim_id) == (
            env["customer_id"], env["item_id"], EXPIRY_DATE, COMPLAINT,
            "open", None, None, "0")

        r = call_action(M.update_warranty_claim, conn, ns(
            warranty_claim_id=claim_id, status="in_progress",
            cost="275.40"))
        assert is_ok(r), r
        assert _claim_tuple(conn, claim_id) == (
            env["customer_id"], env["item_id"], EXPIRY_DATE, COMPLAINT,
            "in_progress", None, None, "275.40")
        # Money is text: the stored string is exact, and Decimal sees the
        # same value. Never float, never approximate.
        assert _row(conn, "warranty_claim", claim_id)["cost"] == "275.40"
        assert Decimal(_row(conn, "warranty_claim", claim_id)[
            "cost"]) == Decimal("275.40")
        assert r["warranty_claim"]["cost"] == "275.40"

        r = call_action(M.update_warranty_claim, conn, ns(
            warranty_claim_id=claim_id, status="resolved",
            resolution="repair", resolution_date=RESOLUTION_DATE,
            cost="312.75"))
        assert is_ok(r), r
        assert _claim_tuple(conn, claim_id) == (
            env["customer_id"], env["item_id"], EXPIRY_DATE, COMPLAINT,
            "resolved", "repair", RESOLUTION_DATE, "312.75")
        assert (r["warranty_claim"]["status"],
                r["warranty_claim"]["cost"]) == ("resolved", "312.75")

        assert len(_audits(conn, "update-warranty-claim", claim_id)) == 2
        # A claim resolution writes no ledger of any kind.
        assert _ledgers_empty(conn)

    def test_closed_claim_refusals_write_nothing(self, conn, env):
        claim_id = _add_claim(conn, env)
        assert is_ok(call_action(M.update_warranty_claim, conn, ns(
            warranty_claim_id=claim_id, status="closed",
            resolution="rejected", resolution_date=RESOLUTION_DATE)))
        closed = _claim_tuple(conn, claim_id)
        assert closed[4:] == ("closed", "rejected", RESOLUTION_DATE, "0")
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)

        for kwargs in ({"status": "open"}, {"cost": "99.00"},
                       {"resolution": "refund",
                        "resolution_date": "2026-05-01"}):
            r = call_action(M.update_warranty_claim, conn, ns(
                warranty_claim_id=claim_id, **kwargs))
            assert is_error(r), (kwargs, r)
            assert _msg(r) == "Cannot update a closed warranty claim"
            assert _claim_tuple(conn, claim_id) == closed

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot

    def test_cost_and_enum_guards_write_nothing(self, conn, env):
        claim_id = _add_claim(conn, env)
        assert is_ok(call_action(M.update_warranty_claim, conn, ns(
            warranty_claim_id=claim_id, status="in_progress",
            cost="40.00")))
        before = _claim_tuple(conn, claim_id)
        assert before[4:] == ("in_progress", None, None, "40.00")
        snapshot = _snapshot(conn, SUPPORT_TABLES + LEDGERS)
        missing = str(uuid.uuid4())

        cases = [
            ({"warranty_claim_id": None, "status": "resolved"},
             "--warranty-claim-id is required"),
            ({"warranty_claim_id": missing, "status": "resolved"},
             "Warranty claim %s not found" % missing),
            ({"warranty_claim_id": claim_id, "status": "approved"},
             "--status must be one of ('open', 'in_progress', 'resolved', "
             "'closed')"),
            ({"warranty_claim_id": claim_id, "status": "resolved",
              "resolution": "bogus"},
             "--resolution must be one of ('repair', 'replace', 'refund', "
             "'rejected')"),
            ({"warranty_claim_id": claim_id, "status": "resolved",
              "cost": "abc"},
             "--cost must be a valid decimal value, got: abc"),
            ({"warranty_claim_id": claim_id, "status": "resolved",
              "cost": "-10.00"},
             "--cost cannot be negative"),
            ({"warranty_claim_id": claim_id},
             "No fields to update. Provide at least one optional flag."),
        ]
        for kwargs, message in cases:
            r = call_action(M.update_warranty_claim, conn, ns(**kwargs))
            assert is_error(r), (kwargs, r)
            assert _msg(r) == message, (kwargs, r)
            assert _claim_tuple(conn, claim_id) == before

        assert _snapshot(conn, SUPPORT_TABLES + LEDGERS) == snapshot
