"""The projects dashboard's upcoming-milestone window is dialect-portable.

status_action lists pending milestones due in the next 30 days. The window's end
used to be written as SQLite's date(?, '+30 days'), which PostgreSQL has no
function for; it now comes from erpclaw_lib.query.date_add_days at call time.
"""
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from projects_helpers import call_action, is_ok, load_db_query, ns  # noqa: E402

M = load_db_query()


class _Row(dict):
    def __getitem__(self, key):
        return self.get(key, "0")


class _RecordingCursor:
    def __init__(self, sql=""):
        self._sql = sql

    def fetchone(self):
        return _Row(id="C1", cnt=0)

    def fetchall(self):
        if '"company"' in self._sql:
            return [_Row(id="C1", cnt=0)]
        return []


class _RecordingConn:
    """Records every SQL string status_action builds; returns empty results."""

    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        return _RecordingCursor(sql)


def _milestone_sql(statements):
    found = [s for s in statements if "FROM milestone m" in s]
    assert len(found) == 1, found
    return found[0]


def test_upcoming_window_has_no_sqlite_date_modifier_on_postgresql(monkeypatch):
    monkeypatch.setenv("ERPCLAW_DB_DIALECT", "postgresql")
    rec = _RecordingConn()
    call_action(M.status_action, rec, ns(company_id="C1"))
    sql = _milestone_sql(rec.statements)
    assert "date(?" not in sql
    assert "m.target_date <= (CAST(? AS date) + CAST(30 AS integer))::text" in sql


def test_upcoming_window_sql_on_sqlite(monkeypatch):
    monkeypatch.setenv("ERPCLAW_DB_DIALECT", "sqlite")
    rec = _RecordingConn()
    call_action(M.status_action, rec, ns(company_id="C1"))
    sql = _milestone_sql(rec.statements)
    assert "m.target_date <= date(?, '+' || 30 || ' days')" in sql


def test_upcoming_window_edges_on_sqlite(conn, env, monkeypatch):
    monkeypatch.setenv("ERPCLAW_DB_DIALECT", "sqlite")
    today = datetime.now(timezone.utc).date()
    for name, offset in (("due-now", 0), ("edge-30", 30), ("past-31", 31), ("missed", -1)):
        conn.execute(
            "INSERT INTO milestone (id, project_id, milestone_name, target_date, status) "
            "VALUES (?, ?, ?, ?, 'pending')",
            (str(uuid.uuid4()), env["project_id"], name,
             (today + timedelta(days=offset)).isoformat()))
    conn.commit()
    r = call_action(M.status_action, conn, ns(company_id=env["company_id"]))
    assert is_ok(r), r
    assert sorted(m["milestone_name"] for m in r["upcoming_milestones"]) == ["due-now", "edge-30"]
