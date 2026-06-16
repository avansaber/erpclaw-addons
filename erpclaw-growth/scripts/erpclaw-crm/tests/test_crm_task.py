"""L1 pytest tests for erpclaw-crm Wave 1B F2 (Tasks — first-class entity, 8 actions).

Covers: add/update/get/list/complete/cancel-crm-task, link/unlink-task-from-entity.
Plus the contract negative controls: complete-on-done reject (L0 idempotency),
add --link-to bad-entity reject (runtime FK existence), unlink-nonexistent reject,
and the link/unlink round-trip with linked_count denorm.
"""
import os
import sys
import uuid

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest
from crm_helpers import call_action, ns, is_ok, is_error, load_db_query

MOD = load_db_query()


# ---------------------------------------------------------------------------
# Namespace helper — defaults every flag F2 actions may read.
# ---------------------------------------------------------------------------

_DEFAULTS = dict(
    subject=None, description=None, priority=None, due_date=None,
    assigned_to=None, created_by=None, notes=None, reason=None,
    crm_task_id=None, link_to=None, linked_to=None, overdue=False,
    due_within_days=None, task=None, entity_type=None, entity_id=None,
    status=None, search=None, limit="20", offset="0",
    db_path=None, company_id=None,
)


def a(**kw):
    d = dict(_DEFAULTS)
    d.update(kw)
    return ns(**d)


def _add_task(conn, company_id, **kw):
    kw.setdefault("subject", "Follow up with Acme")
    r = call_action(MOD.add_crm_task, conn, a(company_id=company_id, **kw))
    assert is_ok(r), r
    return r["crm_task"]["id"]


def _seed_lead(conn, company_id, name="Acme Lead"):
    lid = str(uuid.uuid4())
    conn.execute("INSERT INTO lead (id, lead_name, status, company_id) VALUES (?,?,?,?)",
                 (lid, name, "new", company_id))
    conn.commit()
    return lid


def _seed_opportunity(conn, company_id, name="Acme expansion"):
    oid = str(uuid.uuid4())
    conn.execute("INSERT INTO opportunity (id, opportunity_name, company_id) VALUES (?,?,?)",
                 (oid, name, company_id))
    conn.commit()
    return oid


# ===========================================================================
# add-crm-task
# ===========================================================================

class TestAddCrmTask:
    def test_add_basic_defaults(self, conn, env):
        tid = _add_task(conn, env["company_id"], subject="Call Jane")
        row = conn.execute("SELECT * FROM crm_task WHERE id=?", (tid,)).fetchone()
        assert row["subject"] == "Call Jane"
        assert row["status"] == "open"        # default
        assert row["priority"] == "medium"    # default
        assert row["linked_count"] == 0

    def test_missing_subject_rejected(self, conn, env):
        r = call_action(MOD.add_crm_task, conn, a(company_id=env["company_id"], subject=None))
        assert is_error(r)

    def test_invalid_priority_rejected(self, conn, env):
        r = call_action(MOD.add_crm_task, conn,
                        a(company_id=env["company_id"], subject="X", priority="bogus"))
        assert is_error(r)

    def test_add_with_link_to_opportunity(self, conn, env):
        opp = _seed_opportunity(conn, env["company_id"])
        tid = _add_task(conn, env["company_id"], subject="Follow up",
                        link_to=[f"opportunity:{opp}"])
        link = conn.execute(
            "SELECT linked_entity_type, linked_entity_id FROM crm_task_link WHERE crm_task_id=?",
            (tid,)).fetchone()
        assert link["linked_entity_type"] == "opportunity"
        assert link["linked_entity_id"] == opp
        row = conn.execute("SELECT linked_count FROM crm_task WHERE id=?", (tid,)).fetchone()
        assert row["linked_count"] == 1

    def test_add_with_multiple_links(self, conn, env):
        opp = _seed_opportunity(conn, env["company_id"])
        lead = _seed_lead(conn, env["company_id"])
        tid = _add_task(conn, env["company_id"], subject="Multi",
                        link_to=[f"opportunity:{opp}", f"lead:{lead}"])
        n = conn.execute("SELECT COUNT(*) c FROM crm_task_link WHERE crm_task_id=?",
                         (tid,)).fetchone()["c"]
        assert n == 2

    # ── NEGATIVE CONTROL: --link-to to a non-existent entity rolls back the whole add ──
    def test_add_link_to_missing_entity_rejected_and_rolled_back(self, conn, env):
        r = call_action(MOD.add_crm_task, conn,
                        a(company_id=env["company_id"], subject="Bad link",
                          link_to=[f"opportunity:{uuid.uuid4()}"]))
        assert is_error(r)
        # No task row written (atomic create).
        n = conn.execute("SELECT COUNT(*) c FROM crm_task WHERE subject='Bad link'").fetchone()["c"]
        assert n == 0

    def test_add_link_to_malformed_token_rejected(self, conn, env):
        r = call_action(MOD.add_crm_task, conn,
                        a(company_id=env["company_id"], subject="X", link_to=["no-colon-here"]))
        assert is_error(r)

    def test_add_link_to_unknown_entity_type_rejected(self, conn, env):
        r = call_action(MOD.add_crm_task, conn,
                        a(company_id=env["company_id"], subject="X",
                          link_to=[f"invoice:{uuid.uuid4()}"]))
        assert is_error(r)

    # ── due_date may be in the past (backfill); flagged for audit ──
    def test_past_due_date_allowed_and_flagged(self, conn, env):
        r = call_action(MOD.add_crm_task, conn,
                        a(company_id=env["company_id"], subject="Overdue", due_date="2000-01-01"))
        assert is_ok(r)
        assert r["overdue_on_create"] is True


# ===========================================================================
# update-crm-task
# ===========================================================================

class TestUpdateCrmTask:
    def test_update_fields(self, conn, env):
        tid = _add_task(conn, env["company_id"])
        r = call_action(MOD.update_crm_task, conn,
                        a(crm_task_id=tid, priority="urgent", subject="Renamed"))
        assert is_ok(r)
        row = conn.execute("SELECT subject, priority FROM crm_task WHERE id=?", (tid,)).fetchone()
        assert row["subject"] == "Renamed"
        assert row["priority"] == "urgent"

    def test_update_terminal_task_rejected(self, conn, env):
        tid = _add_task(conn, env["company_id"])
        call_action(MOD.complete_crm_task, conn, a(crm_task_id=tid))
        r = call_action(MOD.update_crm_task, conn, a(crm_task_id=tid, subject="X"))
        assert is_error(r)

    def test_update_missing_id_rejected(self, conn, env):
        r = call_action(MOD.update_crm_task, conn, a(crm_task_id=None, subject="X"))
        assert is_error(r)

    def test_update_no_fields_rejected(self, conn, env):
        tid = _add_task(conn, env["company_id"])
        r = call_action(MOD.update_crm_task, conn, a(crm_task_id=tid))
        assert is_error(r)


# ===========================================================================
# get / list
# ===========================================================================

class TestGetListCrmTask:
    def test_get_with_links(self, conn, env):
        opp = _seed_opportunity(conn, env["company_id"])
        tid = _add_task(conn, env["company_id"], link_to=[f"opportunity:{opp}"])
        r = call_action(MOD.get_crm_task, conn, a(crm_task_id=tid))
        assert is_ok(r)
        assert len(r["crm_task"]["links"]) == 1
        assert r["crm_task"]["links"][0]["linked_entity_type"] == "opportunity"

    def test_list_filter_by_status(self, conn, env):
        t1 = _add_task(conn, env["company_id"], subject="Open one")
        t2 = _add_task(conn, env["company_id"], subject="Done one")
        call_action(MOD.complete_crm_task, conn, a(crm_task_id=t2))
        r = call_action(MOD.list_crm_tasks, conn, a(company_id=env["company_id"], status="open"))
        ids = [t["id"] for t in r["crm_tasks"]]
        assert t1 in ids and t2 not in ids

    def test_list_filter_by_priority(self, conn, env):
        _add_task(conn, env["company_id"], subject="Urgent", priority="urgent")
        _add_task(conn, env["company_id"], subject="Low", priority="low")
        r = call_action(MOD.list_crm_tasks, conn, a(company_id=env["company_id"], priority="urgent"))
        assert r["total"] == 1
        assert r["crm_tasks"][0]["subject"] == "Urgent"

    def test_list_overdue(self, conn, env):
        _add_task(conn, env["company_id"], subject="Past", due_date="2000-01-01")
        _add_task(conn, env["company_id"], subject="Future", due_date="2999-01-01")
        r = call_action(MOD.list_crm_tasks, conn, a(company_id=env["company_id"], overdue=True))
        subs = [t["subject"] for t in r["crm_tasks"]]
        assert "Past" in subs and "Future" not in subs

    def test_list_overdue_excludes_terminal(self, conn, env):
        tid = _add_task(conn, env["company_id"], subject="Past done", due_date="2000-01-01")
        call_action(MOD.complete_crm_task, conn, a(crm_task_id=tid))
        r = call_action(MOD.list_crm_tasks, conn, a(company_id=env["company_id"], overdue=True))
        assert "Past done" not in [t["subject"] for t in r["crm_tasks"]]

    def test_list_linked_to_filter(self, conn, env):
        opp = _seed_opportunity(conn, env["company_id"])
        tid = _add_task(conn, env["company_id"], subject="Linked", link_to=[f"opportunity:{opp}"])
        _add_task(conn, env["company_id"], subject="Unlinked")
        r = call_action(MOD.list_crm_tasks, conn,
                        a(company_id=env["company_id"], linked_to=f"opportunity:{opp}"))
        assert [t["id"] for t in r["crm_tasks"]] == [tid]


# ===========================================================================
# complete / cancel — L0 idempotency lives here
# ===========================================================================

class TestCompleteCancel:
    def test_complete_sets_done_and_completed_at(self, conn, env):
        tid = _add_task(conn, env["company_id"])
        r = call_action(MOD.complete_crm_task, conn, a(crm_task_id=tid, notes="called"))
        assert is_ok(r)
        row = conn.execute("SELECT status, completed_at FROM crm_task WHERE id=?",
                           (tid,)).fetchone()
        assert row["status"] == "done"
        assert row["completed_at"] is not None

    # ── NEGATIVE CONTROL / L0: complete-on-done is rejected (idempotent terminal) ──
    def test_complete_on_done_rejected(self, conn, env):
        tid = _add_task(conn, env["company_id"])
        r1 = call_action(MOD.complete_crm_task, conn, a(crm_task_id=tid))
        assert is_ok(r1)
        r2 = call_action(MOD.complete_crm_task, conn, a(crm_task_id=tid))
        assert is_error(r2)

    def test_complete_on_cancelled_rejected(self, conn, env):
        tid = _add_task(conn, env["company_id"])
        call_action(MOD.cancel_crm_task, conn, a(crm_task_id=tid, reason="dropped"))
        r = call_action(MOD.complete_crm_task, conn, a(crm_task_id=tid))
        assert is_error(r)

    def test_cancel_sets_cancelled_with_reason(self, conn, env):
        tid = _add_task(conn, env["company_id"])
        r = call_action(MOD.cancel_crm_task, conn, a(crm_task_id=tid, reason="not needed"))
        assert is_ok(r)
        row = conn.execute("SELECT status, cancel_reason FROM crm_task WHERE id=?",
                           (tid,)).fetchone()
        assert row["status"] == "cancelled"
        assert row["cancel_reason"] == "not needed"

    def test_cancel_already_terminal_rejected(self, conn, env):
        tid = _add_task(conn, env["company_id"])
        call_action(MOD.complete_crm_task, conn, a(crm_task_id=tid))
        r = call_action(MOD.cancel_crm_task, conn, a(crm_task_id=tid))
        assert is_error(r)


# ===========================================================================
# link / unlink round-trip + negative controls
# ===========================================================================

class TestLinkUnlink:
    def test_link_unlink_round_trip(self, conn, env):
        opp = _seed_opportunity(conn, env["company_id"])
        tid = _add_task(conn, env["company_id"])
        # link
        r1 = call_action(MOD.link_task_to_entity, conn,
                         a(task=tid, entity_type="opportunity", entity_id=opp))
        assert is_ok(r1)
        assert conn.execute("SELECT linked_count FROM crm_task WHERE id=?",
                            (tid,)).fetchone()["linked_count"] == 1
        # unlink
        r2 = call_action(MOD.unlink_task_from_entity, conn,
                         a(task=tid, entity_type="opportunity", entity_id=opp))
        assert is_ok(r2)
        assert conn.execute("SELECT linked_count FROM crm_task WHERE id=?",
                            (tid,)).fetchone()["linked_count"] == 0
        assert conn.execute("SELECT COUNT(*) c FROM crm_task_link WHERE crm_task_id=?",
                            (tid,)).fetchone()["c"] == 0

    # ── NEGATIVE CONTROL: link to a non-existent entity is rejected ──
    def test_link_to_missing_entity_rejected(self, conn, env):
        tid = _add_task(conn, env["company_id"])
        r = call_action(MOD.link_task_to_entity, conn,
                        a(task=tid, entity_type="opportunity", entity_id=str(uuid.uuid4())))
        assert is_error(r)

    def test_link_duplicate_rejected(self, conn, env):
        opp = _seed_opportunity(conn, env["company_id"])
        tid = _add_task(conn, env["company_id"])
        call_action(MOD.link_task_to_entity, conn,
                    a(task=tid, entity_type="opportunity", entity_id=opp))
        r = call_action(MOD.link_task_to_entity, conn,
                        a(task=tid, entity_type="opportunity", entity_id=opp))
        assert is_error(r)

    def test_link_unknown_entity_type_rejected(self, conn, env):
        tid = _add_task(conn, env["company_id"])
        r = call_action(MOD.link_task_to_entity, conn,
                        a(task=tid, entity_type="invoice", entity_id=str(uuid.uuid4())))
        assert is_error(r)

    # ── NEGATIVE CONTROL: unlink a link that does not exist is rejected ──
    def test_unlink_nonexistent_rejected(self, conn, env):
        opp = _seed_opportunity(conn, env["company_id"])
        tid = _add_task(conn, env["company_id"])
        r = call_action(MOD.unlink_task_from_entity, conn,
                        a(task=tid, entity_type="opportunity", entity_id=opp))
        assert is_error(r)
