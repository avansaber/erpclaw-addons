"""L1 pytest tests for erpclaw-crm Wave 1B F4 (Saved views — 6 actions).

Covers: add/update/get/list/delete-crm-saved-view + apply-saved-view, the
--saved-view-id flag on the 4 native list-* actions, shared-vs-private owner
rules, a UDF field used in a filter, and each filter operator. The
injection-resistance L0 guard lives in
testing/unit/constitution/test_crm_saved_view.py.
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


_DEFAULTS = dict(
    # Saved-view flags
    name=None, entity_type=None, owner_user_id=None, filter_json=None,
    sort_json=None, group_by_json=None, column_order_json=None,
    is_shared=False, set_shared=None, shared_only=False,
    id=None, view=None, saved_view_id=None,
    # List-* native flags the dispatched handlers read
    status=None, source=None, search=None, stage=None,
    lifecycle=None, crm_company_id=None,
    # plumbing
    limit="20", offset="0", db_path=None, company_id=None,
)


def a(**kw):
    d = dict(_DEFAULTS)
    d.update(kw)
    return ns(**d)


# ---------------------------------------------------------------------------
# Fixtures / seeds
# ---------------------------------------------------------------------------

def _seed_company(conn):
    cid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO company (id, name, abbr, default_currency, country, "
        "fiscal_year_start_month) VALUES (?,?,?,'USD','United States',1)",
        (cid, "Acme " + cid[:6], "A" + cid[:4]))
    conn.commit()
    conn.company_id = cid
    return cid


def _seed_lead(conn, company_id, name, source, status):
    lid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO lead (id, lead_name, source, status, company_id) "
        "VALUES (?,?,?,?,?)", (lid, name, source, status, company_id))
    conn.commit()
    return lid


_HOT_FILTER = json.dumps({
    "logic": "AND",
    "conditions": [
        {"field": "source", "op": "eq", "value": "referral"},
        {"field": "status", "op": "eq", "value": "qualified"},
    ],
})


def _add_view(conn, name="Hot leads", entity_type="lead", filter_json=_HOT_FILTER, **kw):
    r = call_action(MOD.add_crm_saved_view, conn,
                    a(name=name, entity_type=entity_type, filter_json=filter_json, **kw))
    assert is_ok(r), r
    return r["crm_saved_view"]["id"]


# ---------------------------------------------------------------------------
# add / get
# ---------------------------------------------------------------------------

def test_add_saved_view_happy(conn):
    cid = _seed_company(conn)
    vid = _add_view(conn, owner_user_id="alice")
    r = call_action(MOD.get_crm_saved_view, conn, a(id=vid))
    assert is_ok(r)
    v = r["crm_saved_view"]
    assert v["name"] == "Hot leads"
    assert v["entity_type"] == "lead"
    assert v["is_shared"] == 0
    assert json.loads(v["filter_json"])["logic"] == "AND"


def test_add_saved_view_requires_name(conn):
    _seed_company(conn)
    r = call_action(MOD.add_crm_saved_view, conn,
                    a(name=None, entity_type="lead", filter_json=_HOT_FILTER))
    assert is_error(r)


def test_add_saved_view_bad_entity_type(conn):
    _seed_company(conn)
    r = call_action(MOD.add_crm_saved_view, conn,
                    a(name="x", entity_type="invoice", filter_json=_HOT_FILTER))
    assert is_error(r)


def test_add_saved_view_unknown_field_rejected(conn):
    _seed_company(conn)
    bad = json.dumps({"field": "totally_made_up", "op": "eq", "value": "x"})
    r = call_action(MOD.add_crm_saved_view, conn,
                    a(name="x", entity_type="lead", filter_json=bad))
    assert is_error(r)


def test_add_saved_view_unknown_operator_rejected(conn):
    _seed_company(conn)
    bad = json.dumps({"field": "status", "op": "regex", "value": "x"})
    r = call_action(MOD.add_crm_saved_view, conn,
                    a(name="x", entity_type="lead", filter_json=bad))
    assert is_error(r)


def test_add_saved_view_duplicate_name_per_owner(conn):
    _seed_company(conn)
    _add_view(conn, name="Dup", owner_user_id="alice")
    r = call_action(MOD.add_crm_saved_view, conn,
                    a(name="Dup", entity_type="lead", filter_json=_HOT_FILTER,
                      owner_user_id="alice"))
    assert is_error(r)


# ---------------------------------------------------------------------------
# apply-saved-view + --saved-view-id flag
# ---------------------------------------------------------------------------

def test_apply_saved_view_filters_leads(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme expansion", "referral", "qualified")  # match
    _seed_lead(conn, cid, "Beta early", "referral", "new")            # no match
    _seed_lead(conn, cid, "Gamma inbound", "website", "qualified")    # no match
    vid = _add_view(conn, owner_user_id="alice")

    r = call_action(MOD.apply_saved_view, conn, a(view=vid))
    assert is_ok(r)
    names = [x["lead_name"] for x in r["leads"]]
    assert names == ["Acme expansion"]
    assert r["total"] == 1


def test_list_leads_saved_view_id_flag(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme expansion", "referral", "qualified")
    _seed_lead(conn, cid, "Beta early", "referral", "new")
    vid = _add_view(conn, owner_user_id="alice")

    r = call_action(MOD.list_leads, conn, a(saved_view_id=vid))
    assert is_ok(r)
    assert [x["lead_name"] for x in r["leads"]] == ["Acme expansion"]


def test_apply_saved_view_entity_mismatch_on_list(conn):
    """A lead view applied to list-opportunities is rejected."""
    cid = _seed_company(conn)
    vid = _add_view(conn, owner_user_id="alice")
    r = call_action(MOD.list_opportunities, conn, a(saved_view_id=vid))
    assert is_error(r)


def test_apply_saved_view_not_found(conn):
    _seed_company(conn)
    r = call_action(MOD.apply_saved_view, conn, a(view=str(uuid.uuid4())))
    assert is_error(r)


# ---------------------------------------------------------------------------
# Each operator
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("flt,expect_names", [
    ({"field": "status", "op": "eq", "value": "qualified"}, {"A", "C"}),
    ({"field": "status", "op": "neq", "value": "qualified"}, {"B"}),
    ({"field": "lead_name", "op": "contains", "value": "Acme"}, {"A"}),
    ({"field": "source", "op": "in", "value": ["referral", "website"]}, {"A", "B", "C"}),
])
def test_filter_operators(conn, flt, expect_names):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme A", "referral", "qualified")
    _seed_lead(conn, cid, "Beta B", "referral", "new")
    _seed_lead(conn, cid, "Gamma C", "website", "qualified")
    vid = _add_view(conn, name="op-" + flt["op"], filter_json=json.dumps(flt),
                    owner_user_id="alice")
    r = call_action(MOD.apply_saved_view, conn, a(view=vid))
    assert is_ok(r)
    got = {x["lead_name"].split()[-1] for x in r["leads"]}
    assert got == expect_names


def test_filter_between_operator(conn):
    """between on a TEXT-numeric column (probability), opportunity entity."""
    cid = _seed_company(conn)
    for nm, prob in [("low", "10"), ("mid", "50"), ("high", "90")]:
        oid = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO opportunity (id, opportunity_name, stage, probability, company_id) "
            "VALUES (?,?,?,?,?)", (oid, nm, "new", prob, cid))
    conn.commit()
    flt = json.dumps({"field": "probability", "op": "between", "value": ["10", "50"]})
    vid = _add_view(conn, name="prob-band", entity_type="opportunity",
                    filter_json=flt, owner_user_id="alice")
    r = call_action(MOD.apply_saved_view, conn, a(view=vid))
    assert is_ok(r)
    assert {x["opportunity_name"] for x in r["opportunities"]} == {"low", "mid"}


# ---------------------------------------------------------------------------
# UDF field in filter
# ---------------------------------------------------------------------------

def test_udf_field_in_filter(conn):
    cid = _seed_company(conn)
    la = _seed_lead(conn, cid, "Acme A", "referral", "qualified")
    lb = _seed_lead(conn, cid, "Beta B", "referral", "qualified")
    # Register a UDF on lead and set a value on lead A only.
    from erpclaw_lib.custom_fields import add_custom_field
    add_custom_field(conn, "lead", "deal_size", "text", "erpclaw-crm")
    conn.execute(
        "INSERT INTO custom_field_value (table_name, doc_id, field_name, value) "
        "VALUES (?,?,?,?)", ("lead", la, "deal_size", "large"))
    conn.commit()

    flt = json.dumps({"field": "deal_size", "op": "eq", "value": "large"})
    vid = _add_view(conn, name="big-deals", filter_json=flt, owner_user_id="alice")
    r = call_action(MOD.apply_saved_view, conn, a(view=vid))
    assert is_ok(r)
    assert [x["lead_name"] for x in r["leads"]] == ["Acme A"]


# ---------------------------------------------------------------------------
# Shared vs private + owner rules
# ---------------------------------------------------------------------------

def test_owner_only_delete(conn):
    _seed_company(conn)
    vid = _add_view(conn, owner_user_id="alice")
    # Non-owner cannot delete.
    r = call_action(MOD.delete_crm_saved_view, conn, a(id=vid, owner_user_id="bob"))
    assert is_error(r)
    # Owner can delete.
    r = call_action(MOD.delete_crm_saved_view, conn, a(id=vid, owner_user_id="alice"))
    assert is_ok(r)
    # Gone.
    r = call_action(MOD.get_crm_saved_view, conn, a(id=vid))
    assert is_error(r)


def test_owner_only_update(conn):
    _seed_company(conn)
    vid = _add_view(conn, owner_user_id="alice")
    r = call_action(MOD.update_crm_saved_view, conn,
                    a(id=vid, name="Renamed", owner_user_id="bob"))
    assert is_error(r)
    r = call_action(MOD.update_crm_saved_view, conn,
                    a(id=vid, name="Renamed", owner_user_id="alice"))
    assert is_ok(r)
    assert r["crm_saved_view"]["name"] == "Renamed"


def test_list_saved_views_shared_visibility(conn):
    _seed_company(conn)
    _add_view(conn, name="Alice private", owner_user_id="alice")
    _add_view(conn, name="Bob shared", owner_user_id="bob", is_shared=True)
    # Alice sees her own + the shared one, not Bob's private (he has none private here).
    r = call_action(MOD.list_crm_saved_views, conn, a(owner_user_id="alice"))
    assert is_ok(r)
    names = {v["name"] for v in r["crm_saved_views"]}
    assert "Alice private" in names
    assert "Bob shared" in names
    # shared-only filter.
    r = call_action(MOD.list_crm_saved_views, conn, a(shared_only=True))
    assert {v["name"] for v in r["crm_saved_views"]} == {"Bob shared"}


def test_update_set_and_clear_shared(conn):
    _seed_company(conn)
    vid = _add_view(conn, owner_user_id="alice")
    r = call_action(MOD.update_crm_saved_view, conn,
                    a(id=vid, owner_user_id="alice", set_shared=True))
    assert is_ok(r)
    assert r["crm_saved_view"]["is_shared"] == 1
    r = call_action(MOD.update_crm_saved_view, conn,
                    a(id=vid, owner_user_id="alice", set_shared=False))
    assert is_ok(r)
    assert r["crm_saved_view"]["is_shared"] == 0


def test_update_filter_revalidated(conn):
    """Updating with a bad filter is rejected; the stored view is unchanged."""
    _seed_company(conn)
    vid = _add_view(conn, owner_user_id="alice")
    bad = json.dumps({"field": "nope", "op": "eq", "value": "x"})
    r = call_action(MOD.update_crm_saved_view, conn,
                    a(id=vid, owner_user_id="alice", filter_json=bad))
    assert is_error(r)
    got = call_action(MOD.get_crm_saved_view, conn, a(id=vid))
    assert json.loads(got["crm_saved_view"]["filter_json"])["logic"] == "AND"
