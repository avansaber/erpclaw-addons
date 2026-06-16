"""L1 pytest tests for erpclaw-crm Wave 1B F1 (Contact + Company model, 12 actions).

Covers: add/update/get/list/remove crm-contact, add/update/get/list crm-company,
link-contact-to-company, merge-crm-contacts (FK reassignment + atomicity),
promote-contact-to-customer (cross-skill success round-trip + rollback on failure).

The promote tests MOCK erpclaw_lib.cross_skill.call_skill_action — resolve_skill_script
returns None in the dev/test layout (the selling skill is not on a real box path), so an
in-process unit test cannot spawn the real subprocess. Mocking is the faithful unit-level
contract: we assert promote uses call_skill_action (Article 5), targets the top-level "erpclaw"
router (NOT the unresolvable "erpclaw-selling" sub-skill — the QA box bug), and rolls back on error.
"""
import os
import sys
import uuid

from unittest.mock import patch

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest
from crm_helpers import call_action, ns, is_ok, is_error, load_db_query

MOD = load_db_query()


# ---------------------------------------------------------------------------
# Namespace helper — defaults every flag F1 actions may read.
# ---------------------------------------------------------------------------

_DEFAULTS = dict(
    name=None, email=None, phone=None, mobile=None, job_title=None,
    linkedin_url=None, lifecycle=None, domain=None, industry=None, revenue=None,
    linked_customer_id=None, assigned_to=None, notes=None,
    crm_contact_id=None, crm_company_id=None, role_title=None, is_primary=False,
    primary_contact_id=None, duplicate_contact_id=None,
    search=None, limit="20", offset="0", db_path=None, company_id=None,
)


def a(**kw):
    d = dict(_DEFAULTS)
    d.update(kw)
    return ns(**d)


def _add_contact(conn, company_id, **kw):
    kw.setdefault("name", "Jane Doe")
    r = call_action(MOD.add_crm_contact, conn, a(company_id=company_id, **kw))
    assert is_ok(r), r
    return r["crm_contact"]["id"]


def _add_company(conn, company_id, **kw):
    kw.setdefault("name", "Acme")
    r = call_action(MOD.add_crm_company, conn, a(company_id=company_id, **kw))
    assert is_ok(r), r
    return r["crm_company"]["id"]


# ===========================================================================
# crm_contact
# ===========================================================================

class TestAddCrmContact:
    def test_add_basic(self, conn, env):
        cid = _add_contact(conn, env["company_id"], name="Jane Doe",
                           email="jane@acme.com", lifecycle="lead")
        row = conn.execute("SELECT * FROM crm_contact WHERE id=?", (cid,)).fetchone()
        assert row["name"] == "Jane Doe"
        assert row["email"] == "jane@acme.com"
        assert row["lifecycle"] == "lead"

    def test_default_lifecycle_is_lead(self, conn, env):
        cid = _add_contact(conn, env["company_id"], name="No Lifecycle")
        row = conn.execute("SELECT lifecycle FROM crm_contact WHERE id=?", (cid,)).fetchone()
        assert row["lifecycle"] == "lead"

    def test_missing_name_rejected(self, conn, env):
        r = call_action(MOD.add_crm_contact, conn, a(company_id=env["company_id"], name=None))
        assert is_error(r)

    def test_invalid_lifecycle_rejected(self, conn, env):
        r = call_action(MOD.add_crm_contact, conn,
                        a(company_id=env["company_id"], name="X", lifecycle="bogus"))
        assert is_error(r)

    def test_invalid_email_rejected(self, conn, env):
        r = call_action(MOD.add_crm_contact, conn,
                        a(company_id=env["company_id"], name="X", email="not-an-email"))
        assert is_error(r)

    def test_duplicate_email_case_insensitive_rejected(self, conn, env):
        _add_contact(conn, env["company_id"], name="A", email="dup@acme.com")
        r = call_action(MOD.add_crm_contact, conn,
                        a(company_id=env["company_id"], name="B", email="DUP@ACME.COM"))
        assert is_error(r)

    def test_link_to_company_on_create(self, conn, env):
        comp = _add_company(conn, env["company_id"], name="Acme Inc", domain="acme.com")
        cid = _add_contact(conn, env["company_id"], name="Jane", crm_company_id=comp)
        row = conn.execute("SELECT crm_company_id FROM crm_contact WHERE id=?", (cid,)).fetchone()
        assert row["crm_company_id"] == comp


class TestUpdateCrmContact:
    def test_update_fields(self, conn, env):
        cid = _add_contact(conn, env["company_id"], name="Jane")
        r = call_action(MOD.update_crm_contact, conn,
                        a(crm_contact_id=cid, job_title="VP Sales", phone="555-1"))
        assert is_ok(r)
        row = conn.execute("SELECT job_title, phone FROM crm_contact WHERE id=?", (cid,)).fetchone()
        assert row["job_title"] == "VP Sales"
        assert row["phone"] == "555-1"

    def test_update_duplicate_email_rejected(self, conn, env):
        _add_contact(conn, env["company_id"], name="A", email="taken@acme.com")
        cid = _add_contact(conn, env["company_id"], name="B", email="b@acme.com")
        r = call_action(MOD.update_crm_contact, conn,
                        a(crm_contact_id=cid, email="TAKEN@acme.com"))
        assert is_error(r)

    def test_update_missing_id_rejected(self, conn, env):
        r = call_action(MOD.update_crm_contact, conn, a(crm_contact_id=None, name="X"))
        assert is_error(r)


class TestGetListRemoveContact:
    def test_get_contact_with_roles(self, conn, env):
        comp = _add_company(conn, env["company_id"], name="Acme")
        cid = _add_contact(conn, env["company_id"], name="Jane")
        call_action(MOD.link_contact_to_company, conn,
                    a(crm_contact_id=cid, crm_company_id=comp, role_title="VP"))
        r = call_action(MOD.get_crm_contact, conn, a(crm_contact_id=cid))
        assert is_ok(r)
        assert len(r["crm_contact"]["roles"]) == 1
        assert r["crm_contact"]["roles"][0]["company_name"] == "Acme"

    def test_list_excludes_soft_deleted_by_default(self, conn, env):
        c1 = _add_contact(conn, env["company_id"], name="Keep", email="keep@x.com")
        c2 = _add_contact(conn, env["company_id"], name="Drop", email="drop@x.com")
        call_action(MOD.remove_crm_contact, conn, a(crm_contact_id=c2))
        r = call_action(MOD.list_crm_contacts, conn, a(company_id=env["company_id"]))
        ids = [c["id"] for c in r["crm_contacts"]]
        assert c1 in ids
        # removed contact's lifecycle is 'other' — default list (no lifecycle filter) shows it
        # only because there is no implicit exclude; assert it is reachable via lifecycle=other
        r_other = call_action(MOD.list_crm_contacts, conn,
                              a(company_id=env["company_id"], lifecycle="other"))
        assert c2 in [c["id"] for c in r_other["crm_contacts"]]

    def test_remove_cascades_roles(self, conn, env):
        comp = _add_company(conn, env["company_id"], name="Acme")
        cid = _add_contact(conn, env["company_id"], name="Jane")
        call_action(MOD.link_contact_to_company, conn,
                    a(crm_contact_id=cid, crm_company_id=comp))
        call_action(MOD.remove_crm_contact, conn, a(crm_contact_id=cid))
        n = conn.execute("SELECT COUNT(*) c FROM crm_contact_role WHERE crm_contact_id=?",
                         (cid,)).fetchone()["c"]
        assert n == 0


# ===========================================================================
# crm_company
# ===========================================================================

class TestCrmCompany:
    def test_add_basic_revenue_decimal(self, conn, env):
        cid = _add_company(conn, env["company_id"], name="Acme",
                           domain="acme.com", revenue="1234567.50")
        row = conn.execute("SELECT annual_revenue, domain FROM crm_company WHERE id=?",
                           (cid,)).fetchone()
        assert row["annual_revenue"] == "1234567.50"
        assert row["domain"] == "acme.com"

    def test_duplicate_domain_case_insensitive_rejected(self, conn, env):
        _add_company(conn, env["company_id"], name="A", domain="acme.com")
        r = call_action(MOD.add_crm_company, conn,
                        a(company_id=env["company_id"], name="B", domain="ACME.COM"))
        assert is_error(r)

    def test_invalid_lifecycle_rejected(self, conn, env):
        r = call_action(MOD.add_crm_company, conn,
                        a(company_id=env["company_id"], name="A", lifecycle="lead"))
        assert is_error(r)  # 'lead' is a contact lifecycle, not a company one

    def test_update_company(self, conn, env):
        cid = _add_company(conn, env["company_id"], name="Acme")
        r = call_action(MOD.update_crm_company, conn,
                        a(crm_company_id=cid, industry="SaaS"))
        assert is_ok(r)
        row = conn.execute("SELECT industry FROM crm_company WHERE id=?", (cid,)).fetchone()
        assert row["industry"] == "SaaS"

    def test_get_company_with_contacts(self, conn, env):
        comp = _add_company(conn, env["company_id"], name="Acme")
        cid = _add_contact(conn, env["company_id"], name="Jane")
        call_action(MOD.link_contact_to_company, conn,
                    a(crm_contact_id=cid, crm_company_id=comp))
        r = call_action(MOD.get_crm_company, conn, a(crm_company_id=comp))
        assert is_ok(r)
        assert r["crm_company"]["contacts"][0]["contact_name"] == "Jane"

    def test_list_companies(self, conn, env):
        _add_company(conn, env["company_id"], name="Alpha")
        _add_company(conn, env["company_id"], name="Beta")
        r = call_action(MOD.list_crm_companies, conn, a(company_id=env["company_id"]))
        assert r["total"] >= 2


# ===========================================================================
# link-contact-to-company
# ===========================================================================

class TestLink:
    def test_link_and_duplicate_rejected(self, conn, env):
        comp = _add_company(conn, env["company_id"], name="Acme")
        cid = _add_contact(conn, env["company_id"], name="Jane")
        r1 = call_action(MOD.link_contact_to_company, conn,
                         a(crm_contact_id=cid, crm_company_id=comp, role_title="VP"))
        assert is_ok(r1)
        r2 = call_action(MOD.link_contact_to_company, conn,
                         a(crm_contact_id=cid, crm_company_id=comp))
        assert is_error(r2)


# ===========================================================================
# merge-crm-contacts
# ===========================================================================

class TestMerge:
    def test_merge_fills_blank_fields(self, conn, env):
        primary = _add_contact(conn, env["company_id"], name="Jane P", email="jane@x.com")
        dup = _add_contact(conn, env["company_id"], name="Jane D", phone="555-9",
                           job_title="VP")
        r = call_action(MOD.merge_crm_contacts, conn,
                        a(primary_contact_id=primary, duplicate_contact_id=dup))
        assert is_ok(r)
        row = conn.execute("SELECT phone, job_title, email FROM crm_contact WHERE id=?",
                           (primary,)).fetchone()
        assert row["phone"] == "555-9"
        assert row["job_title"] == "VP"
        assert row["email"] == "jane@x.com"  # primary's email kept

    def test_merge_reassigns_foundation_fks(self, conn, env):
        """merge reassigns lead/opportunity/crm_activity.crm_contact_id to primary."""
        primary = _add_contact(conn, env["company_id"], name="Primary")
        dup = _add_contact(conn, env["company_id"], name="Dup")
        cid = env["company_id"]
        # Point a lead, an opportunity, and an activity at the DUPLICATE contact.
        lead_id, opp_id, act_id = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        conn.execute("INSERT INTO lead (id, lead_name, status, company_id, crm_contact_id) "
                     "VALUES (?,?,?,?,?)", (lead_id, "L", "new", cid, dup))
        conn.execute("INSERT INTO opportunity (id, opportunity_name, company_id, crm_contact_id) "
                     "VALUES (?,?,?,?)", (opp_id, "O", cid, dup))
        conn.execute("INSERT INTO crm_activity (id, activity_type, subject, activity_date, crm_contact_id) "
                     "VALUES (?,?,?,?,?)", (act_id, "note", "S", "2026-01-01", dup))
        conn.commit()

        r = call_action(MOD.merge_crm_contacts, conn,
                        a(primary_contact_id=primary, duplicate_contact_id=dup))
        assert is_ok(r), r
        for table, rid in (("lead", lead_id), ("opportunity", opp_id), ("crm_activity", act_id)):
            row = conn.execute(f"SELECT crm_contact_id FROM {table} WHERE id=?", (rid,)).fetchone()
            assert row["crm_contact_id"] == primary, f"{table} not reassigned"
        # Duplicate soft-deleted
        dup_row = conn.execute("SELECT lifecycle FROM crm_contact WHERE id=?", (dup,)).fetchone()
        assert dup_row["lifecycle"] == "other"

    def test_merge_same_contact_rejected(self, conn, env):
        c = _add_contact(conn, env["company_id"], name="Jane")
        r = call_action(MOD.merge_crm_contacts, conn,
                        a(primary_contact_id=c, duplicate_contact_id=c))
        assert is_error(r)

    def test_merge_reassigns_roles_without_collision(self, conn, env):
        comp = _add_company(conn, env["company_id"], name="Acme")
        primary = _add_contact(conn, env["company_id"], name="P")
        dup = _add_contact(conn, env["company_id"], name="D")
        call_action(MOD.link_contact_to_company, conn,
                    a(crm_contact_id=dup, crm_company_id=comp, role_title="VP"))
        r = call_action(MOD.merge_crm_contacts, conn,
                        a(primary_contact_id=primary, duplicate_contact_id=dup))
        assert is_ok(r)
        row = conn.execute("SELECT crm_contact_id FROM crm_contact_role WHERE crm_company_id=?",
                           (comp,)).fetchone()
        assert row["crm_contact_id"] == primary


# ===========================================================================
# promote-contact-to-customer (cross-skill, mocked)
# ===========================================================================

class TestPromote:
    def test_promote_success_roundtrip(self, conn, env):
        comp = _add_company(conn, env["company_id"], name="Acme", domain="acme.com")
        cid = _add_contact(conn, env["company_id"], name="Jane Doe",
                           email="jane@acme.com", crm_company_id=comp)
        fake_customer_id = str(uuid.uuid4())

        def fake_call(skill, action, args=None, db_path=None, timeout=30):
            assert skill == "erpclaw"  # top-level router, NOT the erpclaw-selling sub-skill (QA box bug)
            assert action == "add-customer"
            assert args["--name"] == "Jane Doe"
            # Materialize a real customer row so the back-reference is consistent.
            conn.execute("INSERT INTO customer (id, name, customer_type, status, company_id) "
                         "VALUES (?,?,?,?,?)",
                         (fake_customer_id, "Jane Doe", "individual", "active", env["company_id"]))
            conn.commit()
            return {"status": "ok", "customer_id": fake_customer_id, "name": "Jane Doe"}

        with patch("erpclaw_lib.cross_skill.call_skill_action", side_effect=fake_call):
            r = call_action(MOD.promote_contact_to_customer, conn, a(crm_contact_id=cid))
        assert is_ok(r), r
        assert r["customer_id"] == fake_customer_id

        contact = conn.execute("SELECT lifecycle FROM crm_contact WHERE id=?", (cid,)).fetchone()
        assert contact["lifecycle"] == "customer"
        company = conn.execute("SELECT linked_customer_id FROM crm_company WHERE id=?",
                               (comp,)).fetchone()
        assert company["linked_customer_id"] == fake_customer_id

    def test_promote_targets_resolvable_router_not_subskill(self):
        """Non-mocked regression guard for the Wave 1B F1 box bug: promote targeted the
        'erpclaw-selling' SUB-skill, which resolve_skill_script can never find (returns
        None) -> 'not installed' for every real user. promote must target the top-level
        'erpclaw' router (it dispatches add-customer -> erpclaw-selling internally). The
        mocked success test above asserts the target IS 'erpclaw'; this proves WHY the
        sub-skill is the wrong target."""
        from erpclaw_lib.dependencies import resolve_skill_script
        assert resolve_skill_script("erpclaw-selling") is None, \
            "erpclaw-selling is a sub-skill, never a call_skill_action target; use 'erpclaw'"

    def test_promote_rolls_back_on_cross_skill_failure(self, conn, env):
        from erpclaw_lib.cross_skill import CrossSkillError
        comp = _add_company(conn, env["company_id"], name="Acme")
        cid = _add_contact(conn, env["company_id"], name="Jane", crm_company_id=comp)

        def boom(skill, action, args=None, db_path=None, timeout=30):
            raise CrossSkillError("selling exploded", skill=skill, action=action)

        with patch("erpclaw_lib.cross_skill.call_skill_action", side_effect=boom):
            r = call_action(MOD.promote_contact_to_customer, conn, a(crm_contact_id=cid))
        assert is_error(r)
        # No growth-side mutation happened: contact lifecycle untouched, no back-link.
        contact = conn.execute("SELECT lifecycle FROM crm_contact WHERE id=?", (cid,)).fetchone()
        assert contact["lifecycle"] == "lead"
        company = conn.execute("SELECT linked_customer_id FROM crm_company WHERE id=?",
                               (comp,)).fetchone()
        assert company["linked_customer_id"] is None
        # No customer row created.
        n = conn.execute("SELECT COUNT(*) c FROM customer WHERE name='Jane'").fetchone()["c"]
        assert n == 0

    def test_promote_already_customer_rejected(self, conn, env):
        cid = _add_contact(conn, env["company_id"], name="Jane", lifecycle="customer")
        r = call_action(MOD.promote_contact_to_customer, conn, a(crm_contact_id=cid))
        assert is_error(r)


# ===========================================================================
# backfill script (dry-run + execute, audit-logged)
# ===========================================================================

class TestBackfill:
    def _backfill_mod(self):
        import importlib.util
        path = os.path.join(os.path.dirname(_TESTS_DIR), "backfill_crm_contact_fks.py")
        spec = importlib.util.spec_from_file_location("backfill_crm", path)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        return m

    def test_dry_run_reports_without_writing(self, conn, env, db_path):
        cid = env["company_id"]
        _add_contact(conn, cid, name="Jane", email="match@acme.com")
        # A lead with the same email but no FK set.
        lead_id = str(uuid.uuid4())
        conn.execute("INSERT INTO lead (id, lead_name, email, status, company_id) "
                     "VALUES (?,?,?,?,?)", (lead_id, "Jane", "match@acme.com", "new", cid))
        conn.commit()

        bf = self._backfill_mod()
        res = bf.run(db_path, company_id=cid, execute=False)
        assert res["dry_run"] is True
        assert res["lead_changes"] == 1
        # Nothing written.
        row = conn.execute("SELECT crm_contact_id FROM lead WHERE id=?", (lead_id,)).fetchone()
        assert row["crm_contact_id"] is None

    def test_execute_applies_and_audits(self, conn, env, db_path):
        cid = env["company_id"]
        contact = _add_contact(conn, cid, name="Jane", email="match@acme.com")
        company = _add_company(conn, cid, name="Acme Co")
        lead_id = str(uuid.uuid4())
        conn.execute("INSERT INTO lead (id, lead_name, company_name, email, status, company_id) "
                     "VALUES (?,?,?,?,?,?)",
                     (lead_id, "Jane", "Acme Co", "match@acme.com", "new", cid))
        # An opportunity off that lead inherits the FK.
        opp_id = str(uuid.uuid4())
        conn.execute("INSERT INTO opportunity (id, opportunity_name, lead_id, company_id) "
                     "VALUES (?,?,?,?)", (opp_id, "O", lead_id, cid))
        conn.commit()

        bf = self._backfill_mod()
        res = bf.run(db_path, company_id=cid, execute=True)
        assert res["dry_run"] is False
        assert res["total_changes"] >= 3  # lead contact+company + opp contact+company

        lead = conn.execute("SELECT crm_contact_id, crm_company_id FROM lead WHERE id=?",
                            (lead_id,)).fetchone()
        assert lead["crm_contact_id"] == contact
        assert lead["crm_company_id"] == company
        opp = conn.execute("SELECT crm_contact_id, crm_company_id FROM opportunity WHERE id=?",
                           (opp_id,)).fetchone()
        assert opp["crm_contact_id"] == contact
        assert opp["crm_company_id"] == company
        # Audit-logged.
        n = conn.execute("SELECT COUNT(*) c FROM audit_log WHERE action='backfill-crm-contact-fks'").fetchone()["c"]
        assert n >= 3
