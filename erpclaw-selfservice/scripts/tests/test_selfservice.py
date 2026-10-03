"""L1 tests for ERPClaw Self-Service -- Permissions, Portal, Sessions, Reports.

Covers all 25 actions across 4 domain modules:
  - permissions (8): add/list/get/update profile, add/list/remove permission,
    validate-permission
  - portal (6): add/list/get/update portal-config, activate/deactivate portal
  - sessions (5): create/list/get/expire session, list-active-sessions
  - reports (6): log-activity, usage-report, portal-analytics-report,
    permission-audit-report, active-sessions-report, status
"""
import json
import pytest
from selfservice_helpers import (
    call_action, ns, is_ok, is_error, load_db_query, _uuid,
    seed_company, seed_naming_series, seed_profile,
)
from erpclaw_lib.query import Q, P, Table, fn
from erpclaw_lib import seam


@pytest.fixture
def mod():
    return load_db_query()


SNAPSHOT_TABLES = (
    "selfservice_permission_profile",
    "selfservice_profile_assignment",
    "selfservice_portal_config",
    "selfservice_session",
    "selfservice_activity_log",
    "audit_log",
)


def _snapshot_tables(conn):
    """Ordered dump of every table a selfservice action could touch.

    Built with PyPika through ``erpclaw_lib.query`` so the tests never carry
    dialect-specific SQL. Two snapshots are byte-identical exactly when the
    action under test wrote nothing.
    """
    snap = {}
    for name in SNAPSHOT_TABLES:
        t = Table(name)
        q = Q.from_(t).select(t.star).orderby(t.id)
        rows = conn.execute(q.get_sql()).fetchall()
        snap[name] = [dict(r) for r in rows]
    return snap


# ============================================================================
# PERMISSIONS
# ============================================================================

class TestAddProfile:
    def test_add_profile_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-add-profile"], conn, ns(
            company_id=env["company_id"], name="Client Portal",
            target_role="client", description="Client access profile",
            allowed_actions='["view-invoice", "pay-invoice"]',
            denied_actions='[]', record_scope="own",
            field_visibility="{}",
        ))
        assert is_ok(r)
        assert r["id"]
        assert r["name"] == "Client Portal"
        assert r["target_role"] == "client"

    def test_add_profile_missing_name(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-add-profile"], conn, ns(
            company_id=env["company_id"], name=None,
            target_role="employee", description=None,
            allowed_actions=None, denied_actions=None,
            record_scope=None, field_visibility=None,
        ))
        assert is_error(r)

    def test_add_profile_missing_target_role(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-add-profile"], conn, ns(
            company_id=env["company_id"], name="No Role",
            target_role=None, description=None,
            allowed_actions=None, denied_actions=None,
            record_scope=None, field_visibility=None,
        ))
        assert is_error(r)

    def test_add_profile_invalid_target_role(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-add-profile"], conn, ns(
            company_id=env["company_id"], name="Bad Role",
            target_role="admin", description=None,
            allowed_actions=None, denied_actions=None,
            record_scope=None, field_visibility=None,
        ))
        assert is_error(r)


class TestListProfiles:
    def test_list_profiles(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-list-profiles"], conn, ns(
            company_id=env["company_id"], target_role=None,
            search=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1  # env seeds one profile

    def test_list_profiles_filter_role(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-list-profiles"], conn, ns(
            company_id=env["company_id"], target_role="employee",
            search=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestGetProfile:
    def test_get_profile_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-get-profile"], conn, ns(
            profile_id=env["profile_id"],
        ))
        assert is_ok(r)
        assert r["id"] == env["profile_id"]
        assert r["target_role"] == "employee"

    def test_get_profile_not_found(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-get-profile"], conn, ns(
            profile_id=_uuid(),
        ))
        assert is_error(r)


class TestUpdateProfile:
    def test_update_profile_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-update-profile"], conn, ns(
            profile_id=env["profile_id"], name="Updated Profile",
            description=None, target_role=None,
            allowed_actions=None, denied_actions=None,
            record_scope=None, field_visibility=None,
        ))
        assert is_ok(r)
        assert "name" in r["updated_fields"]

    def test_update_profile_no_fields(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-update-profile"], conn, ns(
            profile_id=env["profile_id"], name=None,
            description=None, target_role=None,
            allowed_actions=None, denied_actions=None,
            record_scope=None, field_visibility=None,
        ))
        assert is_error(r)


class TestAddPermission:
    def test_add_permission_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-add-permission"], conn, ns(
            company_id=env["company_id"],
            profile_id=env["profile_id"],
            user_id=env["user_id"],
            user_email="testuser@example.com",
            user_name="Test User",
            assigned_by="admin",
        ))
        assert is_ok(r)
        assert r["permission_status"] == "active"

    def test_add_permission_duplicate(self, conn, env, mod):
        call_action(mod.ACTIONS["selfservice-add-permission"], conn, ns(
            company_id=env["company_id"],
            profile_id=env["profile_id"],
            user_id=env["user_id"],
            user_email=None, user_name=None, assigned_by=None,
        ))
        r = call_action(mod.ACTIONS["selfservice-add-permission"], conn, ns(
            company_id=env["company_id"],
            profile_id=env["profile_id"],
            user_id=env["user_id"],
            user_email=None, user_name=None, assigned_by=None,
        ))
        assert is_error(r)

    def test_add_permission_missing_user(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-add-permission"], conn, ns(
            company_id=env["company_id"],
            profile_id=env["profile_id"],
            user_id=None,
            user_email=None, user_name=None, assigned_by=None,
        ))
        assert is_error(r)


class TestListPermissions:
    def test_list_permissions(self, conn, env, mod):
        call_action(mod.ACTIONS["selfservice-add-permission"], conn, ns(
            company_id=env["company_id"],
            profile_id=env["profile_id"],
            user_id=env["user_id"],
            user_email=None, user_name=None, assigned_by=None,
        ))
        r = call_action(mod.ACTIONS["selfservice-list-permissions"], conn, ns(
            company_id=env["company_id"], profile_id=None,
            user_id=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestRemovePermission:
    def test_remove_permission(self, conn, env, mod):
        add = call_action(mod.ACTIONS["selfservice-add-permission"], conn, ns(
            company_id=env["company_id"],
            profile_id=env["profile_id"],
            user_id=env["user_id"],
            user_email=None, user_name=None, assigned_by=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["selfservice-remove-permission"], conn, ns(
            permission_id=add["id"],
        ))
        assert is_ok(r)
        assert r["permission_status"] == "revoked"

    def test_remove_already_revoked(self, conn, env, mod):
        add = call_action(mod.ACTIONS["selfservice-add-permission"], conn, ns(
            company_id=env["company_id"],
            profile_id=env["profile_id"],
            user_id=_uuid(),
            user_email=None, user_name=None, assigned_by=None,
        ))
        call_action(mod.ACTIONS["selfservice-remove-permission"], conn, ns(
            permission_id=add["id"],
        ))
        r = call_action(mod.ACTIONS["selfservice-remove-permission"], conn, ns(
            permission_id=add["id"],
        ))
        assert is_error(r)


class TestValidatePermission:
    def test_validate_permitted(self, conn, env, mod):
        call_action(mod.ACTIONS["selfservice-add-permission"], conn, ns(
            company_id=env["company_id"],
            profile_id=env["profile_id"],
            user_id=env["user_id"],
            user_email=None, user_name=None, assigned_by=None,
        ))
        r = call_action(mod.ACTIONS["selfservice-validate-permission"], conn, ns(
            user_id=env["user_id"], action_name="view-payslip",
        ))
        assert is_ok(r)
        assert r["permitted"] is True

    def test_validate_denied(self, conn, env, mod):
        call_action(mod.ACTIONS["selfservice-add-permission"], conn, ns(
            company_id=env["company_id"],
            profile_id=env["profile_id"],
            user_id=env["user_id"],
            user_email=None, user_name=None, assigned_by=None,
        ))
        r = call_action(mod.ACTIONS["selfservice-validate-permission"], conn, ns(
            user_id=env["user_id"], action_name="delete-account",
        ))
        assert is_ok(r)
        assert r["permitted"] is False

    def test_validate_no_assignment(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-validate-permission"], conn, ns(
            user_id=_uuid(), action_name="anything",
        ))
        assert is_ok(r)
        assert r["permitted"] is False


# ============================================================================
# PORTAL
# ============================================================================

class TestAddPortalConfig:
    def test_add_portal_config_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-add-portal-config"], conn, ns(
            company_id=env["company_id"], name="Main Portal",
            branding_json='{"logo": "logo.png"}',
            welcome_message="Welcome!",
            enabled_modules='["payroll", "leave"]',
            enabled_actions='["view-payslip"]',
            require_mfa=0, session_timeout_minutes=60,
        ))
        assert is_ok(r)
        assert r["id"]
        assert r["name"] == "Main Portal"

    def test_add_portal_config_missing_name(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-add-portal-config"], conn, ns(
            company_id=env["company_id"], name=None,
            branding_json=None, welcome_message=None,
            enabled_modules=None, enabled_actions=None,
            require_mfa=None, session_timeout_minutes=None,
        ))
        assert is_error(r)


class TestListPortalConfigs:
    def test_list_portal_configs(self, conn, env, mod):
        call_action(mod.ACTIONS["selfservice-add-portal-config"], conn, ns(
            company_id=env["company_id"], name="Portal 1",
            branding_json=None, welcome_message=None,
            enabled_modules=None, enabled_actions=None,
            require_mfa=None, session_timeout_minutes=None,
        ))
        r = call_action(mod.ACTIONS["selfservice-list-portal-configs"], conn, ns(
            company_id=env["company_id"], search=None,
            limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestGetPortalConfig:
    def test_get_portal_config(self, conn, env, mod):
        add = call_action(mod.ACTIONS["selfservice-add-portal-config"], conn, ns(
            company_id=env["company_id"], name="Get Portal",
            branding_json=None, welcome_message=None,
            enabled_modules=None, enabled_actions=None,
            require_mfa=None, session_timeout_minutes=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["selfservice-get-portal-config"], conn, ns(
            portal_id=add["id"],
        ))
        assert is_ok(r)
        assert r["name"] == "Get Portal"


class TestUpdatePortalConfig:
    def test_update_portal_config(self, conn, env, mod):
        add = call_action(mod.ACTIONS["selfservice-add-portal-config"], conn, ns(
            company_id=env["company_id"], name="Update Portal",
            branding_json=None, welcome_message=None,
            enabled_modules=None, enabled_actions=None,
            require_mfa=None, session_timeout_minutes=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["selfservice-update-portal-config"], conn, ns(
            portal_id=add["id"], name="Updated Portal",
            branding_json=None, welcome_message="Hello!",
            enabled_modules=None, enabled_actions=None,
            require_mfa=None, session_timeout_minutes=None,
        ))
        assert is_ok(r)
        assert "name" in r["updated_fields"]
        assert "welcome_message" in r["updated_fields"]


class TestActivateDeactivatePortal:
    def _make_portal(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-add-portal-config"], conn, ns(
            company_id=env["company_id"], name="Toggle Portal",
            branding_json=None, welcome_message=None,
            enabled_modules=None, enabled_actions=None,
            require_mfa=None, session_timeout_minutes=None,
        ))
        assert is_ok(r)
        return r["id"]

    def test_deactivate_portal(self, conn, env, mod):
        pid = self._make_portal(conn, env, mod)
        r = call_action(mod.ACTIONS["selfservice-deactivate-portal"], conn, ns(
            portal_id=pid,
        ))
        assert is_ok(r)
        assert r["portal_status"] == "inactive"

    def test_activate_portal(self, conn, env, mod):
        pid = self._make_portal(conn, env, mod)
        call_action(mod.ACTIONS["selfservice-deactivate-portal"], conn, ns(portal_id=pid))
        r = call_action(mod.ACTIONS["selfservice-activate-portal"], conn, ns(
            portal_id=pid,
        ))
        assert is_ok(r)
        assert r["portal_status"] == "active"

    def test_activate_already_active(self, conn, env, mod):
        pid = self._make_portal(conn, env, mod)
        r = call_action(mod.ACTIONS["selfservice-activate-portal"], conn, ns(
            portal_id=pid,
        ))
        assert is_error(r)


# ============================================================================
# SESSIONS
# ============================================================================

class TestCreateSession:
    def test_create_session_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-create-session"], conn, ns(
            company_id=env["company_id"],
            user_id=env["user_id"],
            profile_id=env["profile_id"],
            portal_id=None,
            token="abc-token-123",
            expires_at="2026-12-31T23:59:59Z",
            ip_address="192.168.1.1",
            user_agent="TestBrowser/1.0",
        ))
        assert is_ok(r)
        assert r["session_status"] == "active"
        assert r["token"] == "abc-token-123"

    def test_create_session_missing_token(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-create-session"], conn, ns(
            company_id=env["company_id"],
            user_id=env["user_id"],
            profile_id=env["profile_id"],
            portal_id=None, token=None,
            expires_at="2026-12-31T23:59:59Z",
            ip_address=None, user_agent=None,
        ))
        assert is_error(r)

    def test_create_session_missing_expires(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-create-session"], conn, ns(
            company_id=env["company_id"],
            user_id=env["user_id"],
            profile_id=env["profile_id"],
            portal_id=None, token="token-123",
            expires_at=None,
            ip_address=None, user_agent=None,
        ))
        assert is_error(r)


class TestListSessions:
    def test_list_sessions(self, conn, env, mod):
        call_action(mod.ACTIONS["selfservice-create-session"], conn, ns(
            company_id=env["company_id"],
            user_id=env["user_id"],
            profile_id=env["profile_id"],
            portal_id=None, token="token-list",
            expires_at="2026-12-31T23:59:59Z",
            ip_address=None, user_agent=None,
        ))
        r = call_action(mod.ACTIONS["selfservice-list-sessions"], conn, ns(
            company_id=env["company_id"], user_id=None,
            profile_id=None, limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


class TestGetSession:
    def test_get_session_by_id(self, conn, env, mod):
        add = call_action(mod.ACTIONS["selfservice-create-session"], conn, ns(
            company_id=env["company_id"],
            user_id=env["user_id"],
            profile_id=env["profile_id"],
            portal_id=None, token="token-get",
            expires_at="2026-12-31T23:59:59Z",
            ip_address=None, user_agent=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["selfservice-get-session"], conn, ns(
            session_id=add["id"], token=None,
        ))
        assert is_ok(r)
        assert r["id"] == add["id"]


class TestExpireSession:
    def test_expire_session(self, conn, env, mod):
        add = call_action(mod.ACTIONS["selfservice-create-session"], conn, ns(
            company_id=env["company_id"],
            user_id=env["user_id"],
            profile_id=env["profile_id"],
            portal_id=None, token="token-expire",
            expires_at="2026-12-31T23:59:59Z",
            ip_address=None, user_agent=None,
        ))
        assert is_ok(add)
        r = call_action(mod.ACTIONS["selfservice-expire-session"], conn, ns(
            session_id=add["id"],
        ))
        assert is_ok(r)
        assert r["session_status"] == "expired"

    def test_expire_already_expired(self, conn, env, mod):
        add = call_action(mod.ACTIONS["selfservice-create-session"], conn, ns(
            company_id=env["company_id"],
            user_id=env["user_id"],
            profile_id=env["profile_id"],
            portal_id=None, token="token-double-expire",
            expires_at="2026-12-31T23:59:59Z",
            ip_address=None, user_agent=None,
        ))
        call_action(mod.ACTIONS["selfservice-expire-session"], conn, ns(
            session_id=add["id"],
        ))
        r = call_action(mod.ACTIONS["selfservice-expire-session"], conn, ns(
            session_id=add["id"],
        ))
        assert is_error(r)


class TestListActiveSessions:
    def test_list_active_sessions(self, conn, env, mod):
        call_action(mod.ACTIONS["selfservice-create-session"], conn, ns(
            company_id=env["company_id"],
            user_id=env["user_id"],
            profile_id=env["profile_id"],
            portal_id=None, token="token-active-list",
            expires_at="2026-12-31T23:59:59Z",
            ip_address=None, user_agent=None,
        ))
        r = call_action(mod.ACTIONS["selfservice-list-active-sessions"], conn, ns(
            company_id=env["company_id"], user_id=None,
            limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_count"] >= 1


# ============================================================================
# REPORTS
# ============================================================================

class TestLogActivity:
    def test_log_activity_success(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-log-activity"], conn, ns(
            company_id=env["company_id"],
            user_id=env["user_id"],
            action_name="view-payslip",
            entity_type="payslip", entity_id=_uuid(),
            result="allowed", session_id=None,
            ip_address="10.0.0.1",
        ))
        assert is_ok(r)
        assert r["action"] == "view-payslip"
        assert r["result"] == "allowed"

    def test_log_activity_denied(self, conn, env, mod):
        r = call_action(mod.ACTIONS["selfservice-log-activity"], conn, ns(
            company_id=env["company_id"],
            user_id=env["user_id"],
            action_name="delete-account",
            entity_type=None, entity_id=None,
            result="denied", session_id=None,
            ip_address=None,
        ))
        assert is_ok(r)
        assert r["result"] == "denied"


class TestUsageReport:
    def test_usage_report(self, conn, env, mod):
        # Log some activities first
        call_action(mod.ACTIONS["selfservice-log-activity"], conn, ns(
            company_id=env["company_id"],
            user_id=env["user_id"],
            action_name="view-payslip",
            entity_type=None, entity_id=None,
            result="allowed", session_id=None, ip_address=None,
        ))
        r = call_action(mod.ACTIONS["selfservice-usage-report"], conn, ns(
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["total_activities"] >= 1
        assert r["unique_users"] >= 1


class TestPortalAnalyticsReport:
    def test_portal_analytics_counts_match_stored_rows(self, conn, env, mod):
        # NOTE on depth: this report is read-only. It reaches no ledger, so
        # there are no debit/credit legs to assert and no balance to check;
        # the selfservice tables carry no amount columns, so no monetary
        # (Decimal-as-text) assertion can hold here either. What the test
        # proves instead is that every number in the response equals an
        # independent re-read of the stored session rows, and that the call
        # itself stored nothing.
        assert seam.table_exists("selfservice_portal_config")
        assert seam.table_exists("selfservice_session")

        alpha = call_action(mod.ACTIONS["selfservice-add-portal-config"], conn, ns(
            company_id=env["company_id"], name="Alpha Portal",
            branding_json=None, welcome_message="Alpha welcome",
            enabled_modules=None, enabled_actions=None,
            require_mfa=None, session_timeout_minutes=30,
        ))
        assert is_ok(alpha)
        beta = call_action(mod.ACTIONS["selfservice-add-portal-config"], conn, ns(
            company_id=env["company_id"], name="Beta Portal",
            branding_json=None, welcome_message="Beta welcome",
            enabled_modules=None, enabled_actions=None,
            require_mfa=None, session_timeout_minutes=45,
        ))
        assert is_ok(beta)

        def _session(user_id, portal_id, token):
            r = call_action(mod.ACTIONS["selfservice-create-session"], conn, ns(
                company_id=env["company_id"], user_id=user_id,
                profile_id=env["profile_id"], portal_id=portal_id,
                token=token, expires_at="2026-12-31T23:59:59Z",
                ip_address=None, user_agent=None,
            ))
            assert is_ok(r)
            return r["id"]

        user_a, user_b = _uuid(), _uuid()
        _session(user_a, alpha["id"], "tok-alpha-1")
        _session(user_b, alpha["id"], "tok-alpha-2")
        expiring = _session(_uuid(), alpha["id"], "tok-alpha-3")
        _session(_uuid(), beta["id"], "tok-beta-1")
        # Unscoped session: stored, but attached to no portal, so no
        # portal row may count it.
        _session(_uuid(), None, "tok-unscoped-1")
        call_action(mod.ACTIONS["selfservice-expire-session"], conn, ns(
            session_id=expiring,
        ))

        # Other-company control: must not leak into this company's report.
        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        other_portal = call_action(mod.ACTIONS["selfservice-add-portal-config"], conn, ns(
            company_id=other_company, name="Foreign Portal",
            branding_json=None, welcome_message=None,
            enabled_modules=None, enabled_actions=None,
            require_mfa=None, session_timeout_minutes=60,
        ))
        assert is_ok(other_portal)
        foreign = call_action(mod.ACTIONS["selfservice-create-session"], conn, ns(
            company_id=other_company, user_id=_uuid(),
            profile_id=env["profile_id"], portal_id=other_portal["id"],
            token="tok-foreign-1", expires_at="2026-12-31T23:59:59Z",
            ip_address=None, user_agent=None,
        ))
        assert is_ok(foreign)

        before = _snapshot_tables(conn)
        r = call_action(mod.ACTIONS["selfservice-portal-analytics-report"], conn, ns(
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["company_id"] == env["company_id"]
        assert r["total_portals"] == 2
        by_name = {portal["name"]: portal for portal in r["portals"]}
        assert set(by_name) == {"Alpha Portal", "Beta Portal"}
        assert by_name["Alpha Portal"]["total_sessions"] == 3
        assert by_name["Alpha Portal"]["active_sessions"] == 2
        assert by_name["Beta Portal"]["total_sessions"] == 1
        assert by_name["Beta Portal"]["active_sessions"] == 1
        # Stored portal attributes are echoed exactly, not just counted.
        assert by_name["Alpha Portal"]["id"] == alpha["id"]
        assert by_name["Alpha Portal"]["is_active"] == 1
        assert by_name["Alpha Portal"]["session_timeout_minutes"] == 30
        assert by_name["Beta Portal"]["id"] == beta["id"]
        assert by_name["Beta Portal"]["is_active"] == 1
        assert by_name["Beta Portal"]["session_timeout_minutes"] == 45
        # The unscoped and foreign sessions are stored rows that must NOT
        # be counted anywhere in this report.
        assert sum(portal["total_sessions"] for portal in r["portals"]) == 4

        # Independent re-read of the stored rows through erpclaw_lib.query;
        # every reported number must equal the database, not the envelope.
        t_ss = Table("selfservice_session")
        for portal_id, want_total, want_active in (
            (alpha["id"], 3, 2),
            (beta["id"], 1, 1),
        ):
            q_total = (Q.from_(t_ss).select(fn.Count("*"))
                       .where(t_ss.portal_id == P())
                       .where(t_ss.company_id == P()))
            have_total = conn.execute(
                q_total.get_sql(), (portal_id, env["company_id"])).fetchone()[0]
            q_active = (Q.from_(t_ss).select(fn.Count("*"))
                        .where(t_ss.portal_id == P())
                        .where(t_ss.session_status == P())
                        .where(t_ss.company_id == P()))
            have_active = conn.execute(
                q_active.get_sql(), (portal_id, "active", env["company_id"])).fetchone()[0]
            assert have_total == want_total
            assert have_active == want_active

        # Read-only proof: the report call stored nothing.
        assert _snapshot_tables(conn) == before

    def test_portal_analytics_refuses_without_company_without_writes(self, conn, env, mod):
        before = _snapshot_tables(conn)
        r = call_action(mod.ACTIONS["selfservice-portal-analytics-report"], conn, ns(
            company_id=None, limit=50, offset=0,
        ))
        assert is_error(r)
        assert r["message"] == "--company-id is required"
        assert _snapshot_tables(conn) == before


class TestPermissionAuditReport:
    def test_permission_audit(self, conn, env, mod):
        call_action(mod.ACTIONS["selfservice-log-activity"], conn, ns(
            company_id=env["company_id"],
            user_id=env["user_id"],
            action_name="delete-user",
            entity_type=None, entity_id=None,
            result="denied", session_id=None, ip_address=None,
        ))
        r = call_action(mod.ACTIONS["selfservice-permission-audit-report"], conn, ns(
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["denied_count"] >= 1


class TestActiveSessionsReport:
    def test_active_sessions_report_counts_and_breakdown(self, conn, env, mod):
        # NOTE on depth: this report is read-only. It reaches no ledger, so
        # there are no debit/credit legs to assert and no balance to check;
        # the selfservice tables carry no amount columns, so no monetary
        # (Decimal-as-text) assertion can hold here either. What the test
        # proves instead is that every number in the response equals an
        # independent re-read of the stored session rows, and that the call
        # itself stored nothing.
        assert seam.table_exists("selfservice_session")
        assert seam.table_exists("selfservice_permission_profile")

        prof = call_action(mod.ACTIONS["selfservice-get-profile"], conn, ns(
            profile_id=env["profile_id"],
        ))
        assert is_ok(prof)
        profile_a_name = prof["name"]
        profile_b = call_action(mod.ACTIONS["selfservice-add-profile"], conn, ns(
            company_id=env["company_id"], name="Vendor Portal",
            target_role="vendor", description=None,
            allowed_actions=None, denied_actions=None,
            record_scope=None, field_visibility=None,
        ))
        assert is_ok(profile_b)

        def _session(user_id, profile_id, token):
            r = call_action(mod.ACTIONS["selfservice-create-session"], conn, ns(
                company_id=env["company_id"], user_id=user_id,
                profile_id=profile_id, portal_id=None,
                token=token, expires_at="2026-12-31T23:59:59Z",
                ip_address=None, user_agent=None,
            ))
            assert is_ok(r)
            return r["id"]

        keep_a1 = _session(_uuid(), env["profile_id"], "tok-rep-a1")
        _session(_uuid(), env["profile_id"], "tok-rep-a2")
        _session(_uuid(), profile_b["id"], "tok-rep-b1")
        expiring = _session(_uuid(), env["profile_id"], "tok-rep-a3")
        call_action(mod.ACTIONS["selfservice-expire-session"], conn, ns(
            session_id=expiring,
        ))

        # Other-company control: must not leak into this company's report.
        other_company = seed_company(conn)
        seed_naming_series(conn, other_company)
        foreign = call_action(mod.ACTIONS["selfservice-create-session"], conn, ns(
            company_id=other_company, user_id=_uuid(),
            profile_id=env["profile_id"], portal_id=None,
            token="tok-rep-foreign-1", expires_at="2026-12-31T23:59:59Z",
            ip_address=None, user_agent=None,
        ))
        assert is_ok(foreign)

        before = _snapshot_tables(conn)
        r = call_action(mod.ACTIONS["selfservice-active-sessions-report"], conn, ns(
            company_id=env["company_id"], limit=50, offset=0,
        ))
        assert is_ok(r)
        assert r["company_id"] == env["company_id"]
        assert r["active"] == 3
        assert r["expired"] == 1
        # No owner action in this tree ever writes session_status "ended"
        # (create writes "active", expire writes "expired"), so the exact
        # zero below documents real behaviour rather than a guess.
        assert r["ended"] == 0
        assert r["by_profile"] == [
            {"profile_name": profile_a_name, "active_count": 2},
            {"profile_name": "Vendor Portal", "active_count": 1},
        ]

        # Independent re-read of the stored rows through erpclaw_lib.query.
        t_ss = Table("selfservice_session")
        for status, want in (("active", 3), ("expired", 1), ("ended", 0)):
            q = (Q.from_(t_ss).select(fn.Count("*"))
                 .where(t_ss.company_id == P())
                 .where(t_ss.session_status == P()))
            have = conn.execute(
                q.get_sql(), (env["company_id"], status)).fetchone()[0]
            assert have == want

        # What did NOT change: a sampled active row still reads back with
        # the exact values it was stored with.
        t_one = Table("selfservice_session")
        q_one = (Q.from_(t_one).select(t_one.star)
                 .where(t_one.id == P()))
        row = dict(conn.execute(q_one.get_sql(), (keep_a1,)).fetchone())
        assert row["token"] == "tok-rep-a1"
        assert row["session_status"] == "active"
        assert row["company_id"] == env["company_id"]

        # Read-only proof: the report call stored nothing.
        assert _snapshot_tables(conn) == before

    def test_active_sessions_report_refuses_unknown_company_without_writes(self, conn, env, mod):
        before = _snapshot_tables(conn)
        r = call_action(mod.ACTIONS["selfservice-active-sessions-report"], conn, ns(
            company_id="no-such-company", limit=50, offset=0,
        ))
        assert is_error(r)
        assert r["message"] == "Company no-such-company not found"
        assert _snapshot_tables(conn) == before


class TestSelfServiceStatus:
    def test_status(self, conn, env, mod):
        r = call_action(mod.ACTIONS["status"], conn, ns())
        assert is_ok(r)
        assert r["skill"] == "erpclaw-selfservice"
        assert r["healthy"] is True
