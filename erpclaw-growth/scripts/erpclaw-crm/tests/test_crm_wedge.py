"""L1 pytest tests for the local CRM wedge v1 (`crm-account-brief`).

Read-only deterministic account brief: company + opportunity lookup paths,
exact Decimal money totals, stable ordering, company isolation, missing
target refusal, malformed money refusal without writes, and read-only
repeatability.
"""
import json
import os
import sys
import uuid

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from crm_helpers import call_action, ns, is_ok, is_error, load_db_query

MOD = load_db_query()


def _seed_company(conn, name="Wedge Co"):
    cid = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO company (id, name, abbr, default_currency, country,
           fiscal_year_start_month)
           VALUES (?, ?, ?, 'USD', 'United States', 1)""",
        (cid, "%s %s" % (name, cid[:6]), "W%s" % cid[:4]),
    )
    conn.commit()
    for entity_type, prefix in (("lead", "LEAD-"), ("opportunity", "OPP-"),
                                ("campaign", "CAMP-")):
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, 0, ?)""",
            (str(uuid.uuid4()), entity_type, prefix, cid),
        )
    conn.commit()
    return cid


def _add_crm_company(conn, company_id, name="Acme Industries"):
    r = call_action(MOD.add_crm_company, conn, ns(
        name=name, domain=None, industry=None, revenue=None,
        linked_customer_id=None, lifecycle=None, assigned_to=None,
        notes=None, company_id=company_id,
    ))
    assert is_ok(r), r
    return r["crm_company"]["id"]


def _add_contact(conn, company_id, crm_company_id, name, email):
    r = call_action(MOD.add_crm_contact, conn, ns(
        name=name, email=email, phone=None, lifecycle=None,
        assigned_to=None, notes=None, company_id=company_id,
        crm_company_id=crm_company_id,
    ))
    assert is_ok(r), r
    return r["crm_contact"]["id"]


def _add_opp(conn, company_id, crm_company_id, name, revenue, probability,
             follow_up=None):
    r = call_action(MOD.add_opportunity, conn, ns(
        opportunity_name=name, lead_id=None, customer_id=None,
        opportunity_type=None, expected_revenue=revenue,
        probability=probability, expected_closing_date=None,
        assigned_to=None, company_id=company_id,
    ))
    assert is_ok(r), r
    oid = r["opportunity"]["id"]
    conn.execute("UPDATE opportunity SET crm_company_id = ? WHERE id = ?",
                 (crm_company_id, oid))
    if follow_up:
        conn.execute("UPDATE opportunity SET next_follow_up_date = ? WHERE id = ?",
                     (follow_up, oid))
    conn.commit()
    return oid


def _seed_wedge(conn, company_id):
    """Seed one account: 2 contacts, 2 open opps (500.03), 1 won opp,
    2 activities, 2 open tasks. Returns ids dict."""
    ccid = _add_crm_company(conn, company_id)
    c1 = _add_contact(conn, company_id, ccid, "Zara Lane", "zara@example.com")
    c2 = _add_contact(conn, company_id, ccid, "Amy Pond", "amy@example.com")
    o1 = _add_opp(conn, company_id, ccid, "Beta Deal", "200.02", "25",
                  follow_up="2026-06-10")
    o2 = _add_opp(conn, company_id, ccid, "Alpha Deal", "300.01", "50",
                  follow_up="2026-05-01")
    won = _add_opp(conn, company_id, ccid, "Old Won", "1000.00", "100")
    r = call_action(MOD.mark_opportunity_won, conn, ns(opportunity_id=won))
    assert is_ok(r), r
    for opp_id, subject, date in (
        (o1, "Kickoff call", "2026-04-02"),
        (o2, "Demo meeting", "2026-04-05"),
    ):
        r = call_action(MOD.add_activity, conn, ns(
            activity_type="meeting", subject=subject, activity_date=date,
            lead_id=None, opportunity_id=opp_id, customer_id=None,
            description=None, created_by=None, next_action_date=None,
        ))
        assert is_ok(r), r
    t1 = call_action(MOD.add_crm_task, conn, ns(
        subject="Zebra follow-up", priority=None, description=None,
        due_date="2026-07-01", assigned_to=None, company_id=company_id,
        link_to=["crm_company:%s" % ccid],
    ))
    assert is_ok(t1), t1
    t2 = call_action(MOD.add_crm_task, conn, ns(
        subject="Alpha prep", priority=None, description=None,
        due_date="2026-05-20", assigned_to=None, company_id=company_id,
        link_to=["opportunity:%s" % o2],
    ))
    assert is_ok(t2), t2
    return {"crm_company_id": ccid, "contacts": (c1, c2),
            "opps": (o1, o2), "won": won}


def _brief_by_company(conn, company_id, crm_company_id):
    return call_action(MOD.crm_account_brief, conn, ns(
        company_id=company_id, crm_company_id=crm_company_id,
        opportunity_id=None,
    ))


class TestCompanyBrief:
    def test_company_brief_totals(self, conn):
        cid = _seed_company(conn)
        ids = _seed_wedge(conn, cid)
        r = _brief_by_company(conn, cid, ids["crm_company_id"])
        assert is_ok(r), r
        assert r["crm_company"]["id"] == ids["crm_company_id"]
        assert r["expected_revenue"] == "500.03"
        assert r["total_expected_revenue"] == "500.03"
        assert r["weighted_pipeline_value"] == "200.01"
        assert r["weighted_value"] == "200.01"
        assert r["total_weighted_revenue"] == "200.01"
        assert r["next_follow_up_date"] == "2026-05-01"
        assert r["positioning"] == "local_crm_wedge"
        limits = " ".join(r["limitations"]).lower()
        assert "hubspot" in limits
        assert "model" in limits
        assert "outbound" in limits
        assert len(r["contacts"]) == 2
        assert len(r["open_opportunities"]) == 2
        assert len(r["opportunities"]) == 2
        assert all(o["stage"] not in ("won", "lost") for o in r["opportunities"])
        assert len(r["recent_activities"]) == 2
        assert len(r["open_tasks"]) == 2

    def test_opportunity_lookup_matches(self, conn):
        cid = _seed_company(conn)
        ids = _seed_wedge(conn, cid)
        by_company = _brief_by_company(conn, cid, ids["crm_company_id"])
        assert is_ok(by_company), by_company
        by_opp = call_action(MOD.crm_account_brief, conn, ns(
            company_id=cid, crm_company_id=None, opportunity_id=ids["opps"][0],
        ))
        assert is_ok(by_opp), by_opp
        assert by_opp["crm_company"]["id"] == ids["crm_company_id"]
        assert by_opp["expected_revenue"] == by_company["expected_revenue"] == "500.03"
        assert by_opp["weighted_pipeline_value"] == "200.01"

    def test_stable_ordering(self, conn):
        cid = _seed_company(conn)
        ids = _seed_wedge(conn, cid)
        r = _brief_by_company(conn, cid, ids["crm_company_id"])
        assert is_ok(r), r
        assert [c["name"] for c in r["contacts"]] == ["Amy Pond", "Zara Lane"]
        assert [o["opportunity_name"] for o in r["opportunities"]] == [
            "Alpha Deal", "Beta Deal"]
        assert [a["activity_date"] for a in r["recent_activities"]] == [
            "2026-04-05", "2026-04-02"]
        assert [t["subject"] for t in r["open_tasks"]] == [
            "Alpha prep", "Zebra follow-up"]

    def test_read_only_twice_identical(self, conn):
        cid = _seed_company(conn)
        ids = _seed_wedge(conn, cid)
        before = conn.execute("SELECT COUNT(*) AS c FROM audit_log").fetchone()["c"]
        first = _brief_by_company(conn, cid, ids["crm_company_id"])
        second = _brief_by_company(conn, cid, ids["crm_company_id"])
        assert is_ok(first) and is_ok(second), (first, second)
        assert json.dumps(first, sort_keys=True, default=str) == \
            json.dumps(second, sort_keys=True, default=str)
        after = conn.execute("SELECT COUNT(*) AS c FROM audit_log").fetchone()["c"]
        assert after == before


class TestIsolationAndRefusal:
    def test_cross_company_refused(self, conn):
        cid_a = _seed_company(conn, "Wedge A")
        cid_b = _seed_company(conn, "Wedge B")
        ids = _seed_wedge(conn, cid_a)
        r = _brief_by_company(conn, cid_b, ids["crm_company_id"])
        assert is_error(r)
        r2 = call_action(MOD.crm_account_brief, conn, ns(
            company_id=cid_b, crm_company_id=None,
            opportunity_id=ids["opps"][0],
        ))
        assert is_error(r2)

    def test_missing_target_refused(self, conn):
        cid = _seed_company(conn)
        _seed_wedge(conn, cid)
        r = _brief_by_company(conn, cid, str(uuid.uuid4()))
        assert is_error(r)
        r2 = call_action(MOD.crm_account_brief, conn, ns(
            company_id=cid, crm_company_id=None,
            opportunity_id=str(uuid.uuid4()),
        ))
        assert is_error(r2)
        r3 = call_action(MOD.crm_account_brief, conn, ns(
            company_id=cid, crm_company_id=None, opportunity_id=None,
        ))
        assert is_error(r3)

    def test_malformed_money_refused_without_writes(self, conn):
        cid = _seed_company(conn)
        ids = _seed_wedge(conn, cid)
        counts_before = {
            t: conn.execute("SELECT COUNT(*) AS c FROM %s" % t).fetchone()["c"]
            for t in ("opportunity", "crm_contact", "crm_company",
                      "crm_activity", "crm_task", "audit_log")
        }
        conn.execute("UPDATE opportunity SET expected_revenue = 'not-money' "
                     "WHERE id = ?", (ids["opps"][0],))
        conn.commit()
        r = _brief_by_company(conn, cid, ids["crm_company_id"])
        assert is_error(r)
        counts_after = {
            t: conn.execute("SELECT COUNT(*) AS c FROM %s" % t).fetchone()["c"]
            for t in ("opportunity", "crm_contact", "crm_company",
                      "crm_activity", "crm_task", "audit_log")
        }
        assert counts_after == counts_before
