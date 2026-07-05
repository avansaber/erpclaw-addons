"""L1 behavioral tests — M31 H1 B2 rider (2026-07-02).

One positive-path behavioral test each for the four actions the necessity audit
flagged as `untested` (register.json `never_exercised`): `update-crm-contact`,
`update-crm-company`, `update-crm-saved-view`, `link-task-to-entity`.

Audit note: the register flag is a scanner false-negative — each action already
has behavioral coverage elsewhere in this tree (TestUpdateCrmContact,
test_update_company, test_owner_only_update, TestLinkUnlink). The scanner searches
test files for the *hyphenated* action name but the tests invoke the handler by
its *underscore* function attribute (`MOD.update_crm_contact`), so real coverage
was invisible to it (the underscore/hyphen resolution is H2/G3's scanner fix).
These tests give each action an explicit, single-home B2 assertion regardless.

House style: crm_helpers.call_action against per-test fresh SQLite (conftest
`conn`/`env` fixtures), exact DB-row assertions.
"""
import json
import os
import sys
import uuid

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest
from crm_helpers import call_action, ns, is_ok, is_error, load_db_query

MOD = load_db_query()


# Superset of the flags the four B2 actions read (unioned from the F1/F2/F4 suites).
_DEFAULTS = dict(
    # crm-contact / crm-company
    name=None, email=None, phone=None, mobile=None, job_title=None,
    linkedin_url=None, lifecycle=None, domain=None, industry=None, revenue=None,
    employee_count=None, linked_customer_id=None, assigned_to=None, notes=None,
    crm_contact_id=None, crm_company_id=None, role_title=None, is_primary=False,
    # saved-view
    entity_type=None, owner_user_id=None, filter_json=None, sort_json=None,
    group_by_json=None, column_order_json=None, is_shared=False, set_shared=None,
    id=None, view=None, saved_view_id=None,
    # task link
    subject=None, priority=None, due_date=None, description=None,
    crm_task_id=None, link_to=None, task=None, entity_id=None,
    status=None, source=None,
    # plumbing
    search=None, limit="20", offset="0", db_path=None, company_id=None,
)


def a(**kw):
    d = dict(_DEFAULTS)
    d.update(kw)
    return ns(**d)


def test_update_crm_contact_persists_changed_fields(conn, env):
    """`update-crm-contact` writes the changed fields back to crm_contact."""
    add = call_action(MOD.add_crm_contact, conn,
                      a(company_id=env["company_id"], name="Jane Doe"))
    assert is_ok(add), add
    cid = add["crm_contact"]["id"]

    r = call_action(MOD.update_crm_contact, conn,
                    a(crm_contact_id=cid, job_title="VP Sales", phone="555-0100"))
    assert is_ok(r), r

    row = conn.execute(
        "SELECT job_title, phone FROM crm_contact WHERE id=?", (cid,)).fetchone()
    assert row["job_title"] == "VP Sales"
    assert row["phone"] == "555-0100"


def test_update_crm_company_persists_decimal_revenue(conn, env):
    """`update-crm-company` persists industry + annual_revenue (Decimal-as-TEXT)."""
    add = call_action(MOD.add_crm_company, conn,
                      a(company_id=env["company_id"], name="Acme Inc"))
    assert is_ok(add), add
    comp = add["crm_company"]["id"]

    r = call_action(MOD.update_crm_company, conn,
                    a(crm_company_id=comp, industry="SaaS", revenue="2500000.75"))
    assert is_ok(r), r

    row = conn.execute(
        "SELECT industry, annual_revenue FROM crm_company WHERE id=?",
        (comp,)).fetchone()
    assert row["industry"] == "SaaS"
    assert row["annual_revenue"] == "2500000.75"   # exact TEXT, no float drift


def test_update_crm_saved_view_renames_by_owner(conn, env):
    """`update-crm-saved-view` renames a view for its owner and persists it."""
    flt = json.dumps({
        "logic": "AND",
        "conditions": [{"field": "status", "op": "eq", "value": "new"}],
    })
    add = call_action(MOD.add_crm_saved_view, conn,
                      a(company_id=env["company_id"], name="My leads",
                        entity_type="lead", filter_json=flt, owner_user_id="alice"))
    assert is_ok(add), add
    vid = add["crm_saved_view"]["id"]

    r = call_action(MOD.update_crm_saved_view, conn,
                    a(company_id=env["company_id"], id=vid,
                      owner_user_id="alice", name="Hot leads"))
    assert is_ok(r), r
    assert r["crm_saved_view"]["name"] == "Hot leads"

    row = conn.execute(
        "SELECT name FROM crm_saved_view WHERE id=?", (vid,)).fetchone()
    assert row["name"] == "Hot leads"


def test_link_task_to_entity_creates_link_and_denorm(conn, env):
    """`link-task-to-entity` inserts a crm_task_link row and bumps linked_count."""
    opp_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO opportunity (id, opportunity_name, company_id) VALUES (?,?,?)",
        (opp_id, "Acme expansion", env["company_id"]))
    conn.commit()

    add = call_action(MOD.add_crm_task, conn,
                      a(company_id=env["company_id"], subject="Follow up with Acme"))
    assert is_ok(add), add
    tid = add["crm_task"]["id"]

    r = call_action(MOD.link_task_to_entity, conn,
                    a(task=tid, entity_type="opportunity", entity_id=opp_id))
    assert is_ok(r), r

    link = conn.execute(
        "SELECT COUNT(*) c FROM crm_task_link "
        "WHERE crm_task_id=? AND linked_entity_id=?",
        (tid, opp_id)).fetchone()
    assert link["c"] == 1
    denorm = conn.execute(
        "SELECT linked_count FROM crm_task WHERE id=?", (tid,)).fetchone()
    assert denorm["linked_count"] == 1
