"""L1 pytest tests for erpclaw-crm Wave 1B F5 (Global search — 1 action).

Covers global-crm-search:
  - min-length reject (<2 chars)
  - exact / prefix / contains rank tiers
  - multi-entity merge (one each of lead/opportunity/customer/crm_contact/
    crm_company named "Acme" → all returned with correct entity_type)
  - hard cap (200) enforcement
  - --entity-types filter (restricts the fan-out set)
  - unknown / absent entity type skips gracefully (no crash)
  - deterministic ordering (match_rank asc, updated_at desc)
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


_DEFAULTS = dict(
    # F5 flags
    query=None, entity_types=None,
    # plumbing
    limit="50", offset="0", db_path=None, company_id=None,
)


def a(**kw):
    d = dict(_DEFAULTS)
    d.update(kw)
    return ns(**d)


# ---------------------------------------------------------------------------
# Seeds
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


def _seed_lead(conn, cid, name, company_name=None, email=None, updated_at=None):
    lid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO lead (id, lead_name, company_name, email, status, "
        "company_id, updated_at) VALUES (?,?,?,?,?,?,COALESCE(?,CURRENT_TIMESTAMP))",
        (lid, name, company_name, email, "new", cid, updated_at))
    conn.commit()
    return lid


def _seed_opportunity(conn, cid, name, source=None, updated_at=None):
    oid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO opportunity (id, opportunity_name, source, stage, "
        "company_id, updated_at) VALUES (?,?,?,?,?,COALESCE(?,CURRENT_TIMESTAMP))",
        (oid, name, source, "new", cid, updated_at))
    conn.commit()
    return oid


def _seed_customer(conn, cid, name, email=None):
    custid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO customer (id, name, email, customer_type, status, "
        "company_id) VALUES (?,?,?,?,?,?)",
        (custid, name, email, "company", "active", cid))
    conn.commit()
    return custid


def _seed_crm_company(conn, cid, name, domain=None):
    ccid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO crm_company (id, name, domain, company_id) "
        "VALUES (?,?,?,?)", (ccid, name, domain, cid))
    conn.commit()
    return ccid


def _seed_crm_contact(conn, cid, name, email=None):
    cnid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO crm_contact (id, name, email, company_id) "
        "VALUES (?,?,?,?)", (cnid, name, email, cid))
    conn.commit()
    return cnid


# ---------------------------------------------------------------------------
# min-length reject
# ---------------------------------------------------------------------------

def test_query_too_short_rejected(conn):
    cid = _seed_company(conn)
    res = call_action(MOD.global_crm_search, conn, a(query="A", company_id=cid))
    assert is_error(res)
    assert "2 characters" in res["message"]


def test_empty_query_rejected(conn):
    cid = _seed_company(conn)
    res = call_action(MOD.global_crm_search, conn, a(query="", company_id=cid))
    assert is_error(res)


# ---------------------------------------------------------------------------
# rank tiers
# ---------------------------------------------------------------------------

def test_exact_match_is_rank_1(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid, entity_types="lead"))
    assert is_ok(res)
    assert len(res["results"]) == 1
    assert res["results"][0]["match_rank"] == 1
    assert res["results"][0]["entity_type"] == "lead"


def test_prefix_match_is_rank_2(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme Industries")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid, entity_types="lead"))
    assert is_ok(res)
    assert res["results"][0]["match_rank"] == 2


def test_contains_match_is_rank_3(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "The Acme Co")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid, entity_types="lead"))
    assert is_ok(res)
    assert res["results"][0]["match_rank"] == 3


def test_case_insensitive_match(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "ACME")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="acme", company_id=cid, entity_types="lead"))
    assert is_ok(res)
    assert len(res["results"]) == 1
    assert res["results"][0]["match_rank"] == 1


def test_rank_ordering_exact_before_contains(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Beta Acme Co")   # contains -> rank 3
    _seed_lead(conn, cid, "Acme")           # exact -> rank 1
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid, entity_types="lead"))
    assert is_ok(res)
    ranks = [r["match_rank"] for r in res["results"]]
    assert ranks == sorted(ranks)           # rank ascending
    assert res["results"][0]["match_rank"] == 1


# ---------------------------------------------------------------------------
# multi-entity merge
# ---------------------------------------------------------------------------

def test_multi_entity_merge_all_returned(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme")
    _seed_opportunity(conn, cid, "Acme")
    _seed_customer(conn, cid, "Acme")
    _seed_crm_contact(conn, cid, "Acme")
    _seed_crm_company(conn, cid, "Acme")
    res = call_action(MOD.global_crm_search, conn, a(query="Acme", company_id=cid))
    assert is_ok(res)
    got = {r["entity_type"] for r in res["results"]}
    assert got == {"lead", "opportunity", "customer", "crm_contact", "crm_company"}
    assert res["total"] == 5


def test_company_scoping(conn):
    cid = _seed_company(conn)
    other = _seed_company(conn)   # resets conn.company_id; restore below
    conn.company_id = cid
    _seed_lead(conn, cid, "Acme")
    _seed_lead(conn, other, "Acme")   # different company — must not appear
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid, entity_types="lead"))
    assert is_ok(res)
    assert res["total"] == 1


def test_no_match_returns_empty(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Globex")
    res = call_action(MOD.global_crm_search, conn, a(query="Acme", company_id=cid))
    assert is_ok(res)
    assert res["results"] == []
    assert res["total"] == 0


# ---------------------------------------------------------------------------
# hard cap
# ---------------------------------------------------------------------------

def test_hard_cap_enforced(conn):
    cid = _seed_company(conn)
    # 210 matching leads; cap is 200 regardless of a larger --limit.
    for i in range(210):
        _seed_lead(conn, cid, "Acme")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid, entity_types="lead",
                        limit="500"))
    assert is_ok(res)
    assert res["limit"] == 200          # clamped from 500
    assert res["returned"] == 200
    assert len(res["results"]) == 200
    assert res["total"] == 210          # total is the full match count


def test_limit_below_one_clamped(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid, entity_types="lead",
                        limit="0"))
    assert is_ok(res)
    assert res["limit"] == 1


def test_limit_applied(conn):
    cid = _seed_company(conn)
    for _ in range(5):
        _seed_lead(conn, cid, "Acme")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid, entity_types="lead",
                        limit="3"))
    assert is_ok(res)
    assert res["returned"] == 3
    assert res["total"] == 5


# ---------------------------------------------------------------------------
# --entity-types filter
# ---------------------------------------------------------------------------

def test_entity_types_filter_restricts_fanout(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme")
    _seed_customer(conn, cid, "Acme")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid, entity_types="lead"))
    assert is_ok(res)
    assert {r["entity_type"] for r in res["results"]} == {"lead"}
    assert res["total"] == 1


def test_entity_types_csv_multiple(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme")
    _seed_customer(conn, cid, "Acme")
    _seed_opportunity(conn, cid, "Acme")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid,
                        entity_types="lead,customer"))
    assert is_ok(res)
    assert {r["entity_type"] for r in res["results"]} == {"lead", "customer"}


# ---------------------------------------------------------------------------
# graceful skip — unknown / absent entity types
# ---------------------------------------------------------------------------

def test_unknown_entity_type_skipped_gracefully(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid,
                        entity_types="lead,not_a_real_entity"))
    assert is_ok(res)                       # no crash
    assert res["total"] == 1
    assert "not_a_real_entity" in res["skipped_entity_types"]


def test_absent_table_entity_type_skipped(conn):
    # crm_task is a valid V1-future type but is NOT in the V1 default fan-out
    # set and is not in _SEARCH_ENTITIES, so requesting it skips gracefully.
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid,
                        entity_types="lead,crm_task"))
    assert is_ok(res)
    assert res["total"] == 1
    assert "crm_task" in res["skipped_entity_types"]


def test_only_unknown_types_returns_empty_not_error(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid, entity_types="bogus"))
    assert is_ok(res)                       # graceful, not an error
    assert res["results"] == []
    assert res["skipped_entity_types"] == ["bogus"]


# ---------------------------------------------------------------------------
# searchable secondary columns + snippet / display
# ---------------------------------------------------------------------------

def test_match_on_email_column(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Some Lead", email="contact@acme.com")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="acme.com", company_id=cid, entity_types="lead"))
    assert is_ok(res)
    assert res["total"] == 1
    assert res["results"][0]["match_rank"] == 3


def test_display_name_and_snippet_populated(conn):
    cid = _seed_company(conn)
    _seed_lead(conn, cid, "Acme", company_name="Acme Holdings")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid, entity_types="lead"))
    assert is_ok(res)
    row = res["results"][0]
    assert row["display_name"] == "Acme"
    assert row["snippet"] == "Acme Holdings"
    assert "id" in row and "updated_at" in row


# ---------------------------------------------------------------------------
# updated_at desc tiebreak within same rank
# ---------------------------------------------------------------------------

def test_updated_at_desc_tiebreak_within_rank(conn):
    cid = _seed_company(conn)
    older = _seed_lead(conn, cid, "Acme", updated_at="2026-01-01T00:00:00")
    newer = _seed_lead(conn, cid, "Acme", updated_at="2026-06-01T00:00:00")
    res = call_action(MOD.global_crm_search, conn,
                      a(query="Acme", company_id=cid, entity_types="lead"))
    assert is_ok(res)
    assert len(res["results"]) == 2
    # both rank 1 (exact); newer updated_at first
    assert res["results"][0]["id"] == newer
    assert res["results"][1]["id"] == older
