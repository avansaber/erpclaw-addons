"""The billed-timesheet invoice is created in the same books as the billing.

create-billing-from-timesheets delegates the sales invoice to erpclaw-selling
through cross_skill. The child is given a database path only when the caller
gave --db-path; otherwise it inherits the environment and resolves the
database exactly as the parent did.
"""
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from projects_helpers import (  # noqa: E402
    load_db_query, call_action, ns, is_ok,
)

M = load_db_query()


def _billable_submitted_timesheet(conn, env):
    conn.execute(
        "UPDATE project SET billing_type = 'time_and_material', "
        "customer_id = ? WHERE id = ?",
        (env["customer_id"], env["project_id"]),
    )
    conn.commit()
    items_json = json.dumps([{
        "project_id": env["project_id"],
        "hours": "8",
        "billing_rate": "100.00",
        "billable": 1,
        "date": "2026-03-10",
        "activity_type": "development",
    }])
    add_r = call_action(M.add_timesheet, conn, ns(
        company_id=env["company_id"],
        employee_id=env["employee_id"],
        start_date="2026-03-10",
        end_date="2026-03-10",
        items=items_json,
    ))
    assert is_ok(add_r), add_r
    sub_r = call_action(M.submit_timesheet, conn, ns(
        timesheet_id=add_r["timesheet"]["id"],
    ))
    assert is_ok(sub_r), sub_r


def _recording_invoice(monkeypatch):
    from erpclaw_lib.cross_skill import CrossSkillError
    calls = []

    def recorder(**kwargs):
        calls.append(kwargs)
        raise CrossSkillError("recorded")

    monkeypatch.setattr(M, "create_invoice", recorder)
    return calls


def test_no_flag_forwards_no_path(conn, env, monkeypatch):
    _billable_submitted_timesheet(conn, env)
    calls = _recording_invoice(monkeypatch)
    r = call_action(M.create_billing_from_timesheets, conn, ns(
        company_id=env["company_id"],
        project_id=env["project_id"],
        db_path=None,
    ))
    assert r["status"] == "error", r
    assert r["message"].startswith("Failed to create invoice:"), r
    assert len(calls) == 1
    assert calls[0]["db_path"] is None


def test_explicit_flag_is_forwarded(conn, env, monkeypatch):
    _billable_submitted_timesheet(conn, env)
    calls = _recording_invoice(monkeypatch)
    r = call_action(M.create_billing_from_timesheets, conn, ns(
        company_id=env["company_id"],
        project_id=env["project_id"],
        db_path="/nonexistent/explicit.sqlite",
    ))
    assert r["status"] == "error", r
    assert len(calls) == 1
    assert calls[0]["db_path"] == "/nonexistent/explicit.sqlite"
