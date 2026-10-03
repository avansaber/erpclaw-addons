"""M479 depth, part B -- ledger effects and sync/dispatch writes.

  shopify-post-reserve-gl   ledger effect (two balanced legs + voucher + audit)
  shopify-reverse-order-gl  ledger effect (mirror legs, cancelled voucher,
                            order reset + audit)
  shopify-process-webhook   stored rows (sync_job + account timestamp + audit;
                            never posts GL directly, so no ledger effect)
  shopify-dispatch-command  stored rows (sync jobs for sync-now, disabled
                            account for disconnect; ack-only types write
                            nothing; never posts GL directly)

Prior state (read before writing): reserve and reversal already have deep
ledger tests in test_reserve_and_reversal_gl_values.py and refusal/rollback
tests in test_reserve_gl_failure_rollback.py. The tests below fill the gaps
those files leave: invalid reserve-type / zero-reserve / unknown-payout
refusals for post-reserve-gl, the unknown-order refusal and the reversal
audit row for reverse-order-gl. shopify-process-webhook had no test at all
(test_sync.py only names it in a docstring); shopify-dispatch-command had
routing-only tests in test_dispatcher.py with no database assertions.

Conventions: same as part A -- PyPika reads, exact Decimal strings, refusal
cases assert the truthful message and a byte-identical snapshot.
"""
import json
import os
import sys
from decimal import Decimal
from unittest.mock import patch

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from shopify_test_helpers import (
    call_action, ns, is_ok, is_error, init_all_tables,
    build_env, seed_shopify_order, seed_shopify_payout, _uuid,
)
from erpclaw_lib.query import Q, P, Table
from gl_posting import ACTIONS as GL_ACTIONS
from sync import ACTIONS as SYNC_ACTIONS, FULL_SYNC_ORDER
from dispatcher import dispatch_command, DISPATCHER_ACTIONS


SNAPSHOT_TABLES = (
    "shopify_account", "shopify_order", "shopify_order_line_item",
    "shopify_refund", "shopify_refund_line_item", "shopify_payout",
    "shopify_payout_transaction", "shopify_dispute", "shopify_gl_rule",
    "shopify_reconciliation_run", "shopify_sync_job",
    "company", "account", "journal_entry", "journal_entry_line",
    "gl_entry", "fiscal_year", "cost_center", "item", "customer",
    "audit_log", "action_call_log", "naming_series",
)


@pytest.fixture(scope="module")
def template_path(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("m479tpl_b") / "template.sqlite")
    init_all_tables(path)
    return path


@pytest.fixture
def mconn(template_path, tmp_path):
    from erpclaw_lib.db import get_connection
    dest = str(tmp_path / "case.sqlite")
    src = get_connection(template_path)
    try:
        dst = get_connection(dest)
        try:
            src._conn.backup(dst._conn)
        finally:
            dst.close()
    finally:
        src.close()
    conn = get_connection(dest)
    os.environ["ERPCLAW_DB_PATH"] = dest
    try:
        yield conn
    finally:
        conn.close()
        os.environ.pop("ERPCLAW_DB_PATH", None)


def _snapshot(conn):
    snap = {}
    for name in SNAPSHOT_TABLES:
        t = Table(name)
        rows = conn.execute(Q.from_(t).select("*").get_sql()).fetchall()
        snap[name] = sorted(repr(dict(r)) for r in rows)
    return snap


def _read(conn, table, row_id):
    t = Table(table)
    return conn.execute(
        Q.from_(t).select("*").where(t.id == P()).get_sql(), (row_id,)).fetchone()


def _audit_rows(conn, action):
    a = Table("audit_log")
    return conn.execute(
        Q.from_(a).select(
            a.skill, a.action, a.entity_type, a.entity_id, a.new_values)
        .where(a.action == P()).get_sql(), (action,)).fetchall()


def _legs(conn, voucher_id):
    g = Table("gl_entry")
    return conn.execute(
        Q.from_(g).select(
            g.id, g.account_id, g.debit, g.credit, g.voucher_type,
            g.voucher_id, g.posting_date, g.entry_set, g.is_cancelled,
            g.remarks)
        .where(g.voucher_id == P()).get_sql(), (voucher_id,)).fetchall()


# ===========================================================================
# shopify-post-reserve-gl -- ledger effect. Refusal gaps not covered by the
# existing rollback file: invalid reserve-type, zero reserve, unknown payout.
# ===========================================================================
class TestPostReserveGL:

    def test_hold_posts_balanced_legs_and_audit(self, mconn):
        env = build_env(mconn)
        acct = env["shopify_account"]
        pid = seed_shopify_payout(
            mconn, env["shopify_account_id"], env["company_id"],
            gross="1000.00", fee="29.00", reserved_funds_gross="250.00")
        expected_date = _read(mconn, "shopify_payout", pid)["issued_at"][:10]

        result = call_action(
            GL_ACTIONS["shopify-post-reserve-gl"], mconn, ns(
                shopify_payout_id=pid, reserve_type="hold"))
        assert is_ok(result), result
        assert result["amount"] == "250.00"
        assert result["reserve_type"] == "hold"
        assert result["gl_entry_count"] == 2
        je_id = result["journal_entry_id"]

        legs = _legs(mconn, je_id)
        assert len(legs) == 2
        by_acct = {r["account_id"]: r for r in legs}
        assert set(by_acct) == {
            acct["reserve_account_id"], acct["clearing_account_id"]}
        assert (by_acct[acct["reserve_account_id"]]["debit"],
                by_acct[acct["reserve_account_id"]]["credit"]) == (
            "250.00", "0.00")
        assert (by_acct[acct["clearing_account_id"]]["debit"],
                by_acct[acct["clearing_account_id"]]["credit"]) == (
            "0.00", "250.00")
        for leg in legs:
            assert leg["voucher_type"] == "journal_entry"
            assert leg["voucher_id"] == je_id
            assert leg["posting_date"] == expected_date
            assert leg["entry_set"] == "primary"
            assert leg["is_cancelled"] == 0
        assert sum(Decimal(r["debit"]) for r in legs) == Decimal("250.00")
        assert sum(Decimal(r["credit"]) for r in legs) == Decimal("250.00")

        j = Table("journal_entry")
        je = mconn.execute(
            Q.from_(j).select(
                j.posting_date, j.total_debit, j.total_credit, j.status)
            .where(j.id == P()).get_sql(), (je_id,)).fetchone()
        assert (je["posting_date"], je["total_debit"], je["total_credit"],
                je["status"]) == (
            expected_date, "250.00", "250.00", "submitted")

        payout = _read(mconn, "shopify_payout", pid)
        assert (payout["gl_status"], payout["gl_voucher_id"]) == (
            "pending", None)

        audits = _audit_rows(mconn, "shopify-post-reserve-gl")
        assert len(audits) == 1
        assert audits[0]["entity_id"] == pid

    def test_refuses_invalid_reserve_type(self, mconn):
        env = build_env(mconn)
        pid = seed_shopify_payout(
            mconn, env["shopify_account_id"], env["company_id"],
            reserved_funds_gross="250.00")
        snap_before = _snapshot(mconn)
        result = call_action(
            GL_ACTIONS["shopify-post-reserve-gl"], mconn, ns(
                shopify_payout_id=pid, reserve_type="freeze"))
        assert is_error(result), result
        assert result["message"] == (
            "--reserve-type must be 'hold' or 'release'")
        assert _snapshot(mconn) == snap_before

    def test_refuses_zero_reserve(self, mconn):
        env = build_env(mconn)
        pid = seed_shopify_payout(
            mconn, env["shopify_account_id"], env["company_id"],
            reserved_funds_gross="0")
        snap_before = _snapshot(mconn)
        result = call_action(
            GL_ACTIONS["shopify-post-reserve-gl"], mconn, ns(
                shopify_payout_id=pid, reserve_type="hold"))
        assert is_error(result), result
        assert result["message"] == "No reserved funds to post"
        assert _snapshot(mconn) == snap_before

    def test_refuses_unknown_payout(self, mconn):
        build_env(mconn)
        snap_before = _snapshot(mconn)
        missing = _uuid()
        result = call_action(
            GL_ACTIONS["shopify-post-reserve-gl"], mconn, ns(
                shopify_payout_id=missing, reserve_type="hold"))
        assert is_error(result), result
        assert result["message"] == f"Shopify payout {missing} not found"
        assert _snapshot(mconn) == snap_before


# ===========================================================================
# shopify-reverse-order-gl -- ledger effect. The mirror-leg assertions live
# in test_reserve_and_reversal_gl_values.py; the gaps filled here are the
# unknown-order refusal and the reversal audit row.
# ===========================================================================
class TestReverseOrderGL:

    def test_reversal_mirrors_legs_cancels_voucher_and_audits(self, mconn):
        env = build_env(mconn)
        acct = env["shopify_account"]
        oid = seed_shopify_order(
            mconn, env["shopify_account_id"], env["company_id"],
            shopify_order_id="M479-REV-1", subtotal="100.00",
            shipping="10.00", tax="8.00")
        posted = call_action(
            GL_ACTIONS["shopify-post-order-gl"], mconn,
            ns(shopify_order_id=oid))
        assert is_ok(posted), posted
        je_id = posted["journal_entry_id"]
        originals = {r["id"]: r for r in _legs(mconn, je_id)}
        assert len(originals) == 4

        result = call_action(
            GL_ACTIONS["shopify-reverse-order-gl"], mconn,
            ns(shopify_order_id=oid))
        assert is_ok(result), result
        assert result["reversed_voucher_id"] == je_id
        assert result["reversal_gl_entry_count"] == 4

        legs = _legs(mconn, je_id)
        assert len(legs) == 8
        reversals = [r for r in legs if r["id"] not in originals]
        assert len(reversals) == 4
        for rev in reversals:
            orig = originals[rev["remarks"][len("Reversal of "):]]
            assert rev["remarks"] == "Reversal of %s" % orig["id"]
            assert (rev["account_id"], rev["debit"], rev["credit"]) == (
                orig["account_id"], orig["credit"], orig["debit"])
        net = {}
        for leg in legs:
            net[leg["account_id"]] = net.get(
                leg["account_id"], Decimal("0")) + Decimal(
                    leg["debit"]) - Decimal(leg["credit"])
        assert set(net.values()) == {Decimal("0")}
        assert sum(Decimal(r["debit"]) for r in legs) == Decimal("236.00")
        assert sum(Decimal(r["credit"]) for r in legs) == Decimal("236.00")

        j = Table("journal_entry")
        je = mconn.execute(
            Q.from_(j).select(j.status).where(j.id == P()).get_sql(),
            (je_id,)).fetchone()
        assert je["status"] == "cancelled"
        order = _read(mconn, "shopify_order", oid)
        assert (order["gl_status"], order["gl_voucher_id"]) == (
            "pending", None)

        audits = _audit_rows(mconn, "shopify-reverse-order-gl")
        assert len(audits) == 1
        assert (audits[0]["entity_type"], audits[0]["entity_id"]) == (
            "shopify_order", oid)

    def test_refuses_unknown_order(self, mconn):
        build_env(mconn)
        snap_before = _snapshot(mconn)
        missing = _uuid()
        result = call_action(
            GL_ACTIONS["shopify-reverse-order-gl"], mconn,
            ns(shopify_order_id=missing))
        assert is_error(result), result
        assert result["message"] == f"Shopify order {missing} not found"
        assert _snapshot(mconn) == snap_before


# ===========================================================================
# shopify-process-webhook -- stored rows. A webhook triggers a sync job and
# stamps the account; it never posts GL directly, so there is no ledger
# effect to assert (stated so nobody adds one).
# ===========================================================================
class TestProcessWebhook:

    def test_orders_webhook_creates_completed_job_and_timestamp(self, mconn):
        import sync as sync_mod
        env = build_env(mconn)
        assert _read(mconn, "shopify_account", env["shopify_account_id"])[
            "last_orders_sync_at"] is None
        with patch.object(sync_mod, "graphql_request", return_value={}):
            result = call_action(
                SYNC_ACTIONS["shopify-process-webhook"], mconn, ns(
                    shopify_account_id=env["shopify_account_id"],
                    webhook_topic="orders/create",
                    webhook_data=json.dumps({"id": 1})))
        assert is_ok(result), result
        assert result["processed"] is True
        assert result["sync_type"] == "orders"
        assert result["records_processed"] == 0
        assert result["sync_job_id"] is not None

        job = _read(mconn, "shopify_sync_job", result["sync_job_id"])
        assert job["sync_type"] == "orders"
        assert job["status"] == "completed"
        assert job["records_processed"] == 0
        assert _read(mconn, "shopify_account", env["shopify_account_id"])[
            "last_orders_sync_at"] is not None
        # Empty Shopify response: the job ran, nothing was mirrored.
        s = Table("shopify_order")
        assert mconn.execute(
            Q.from_(s).select(s.id).get_sql()).fetchall() == []
        audits = _audit_rows(mconn, "shopify-process-webhook")
        assert len(audits) == 1
        assert audits[0]["entity_id"] == env["shopify_account_id"]

    def test_unknown_topic_processes_without_job(self, mconn):
        # Real behaviour, documented: unknown topics ack as processed with no
        # sync job, but still write one audit row.
        env = build_env(mconn)
        result = call_action(
            SYNC_ACTIONS["shopify-process-webhook"], mconn, ns(
                shopify_account_id=env["shopify_account_id"],
                webhook_topic="app/uninstalled",
                webhook_data=json.dumps({"id": 2})))
        assert is_ok(result), result
        assert result["processed"] is True
        assert result["sync_type"] is None
        assert result["sync_job_id"] is None
        j = Table("shopify_sync_job")
        assert mconn.execute(
            Q.from_(j).select(j.id).get_sql()).fetchall() == []
        assert len(_audit_rows(mconn, "shopify-process-webhook")) == 1

    def test_refuses_invalid_json_without_writing(self, mconn):
        env = build_env(mconn)
        snap_before = _snapshot(mconn)
        result = call_action(
            SYNC_ACTIONS["shopify-process-webhook"], mconn, ns(
                shopify_account_id=env["shopify_account_id"],
                webhook_topic="orders/create",
                webhook_data="{not-json"))
        assert is_error(result), result
        assert result["message"] == "--webhook-data must be valid JSON"
        # The refusal happens before any sync job is created.
        assert _snapshot(mconn) == snap_before


# ===========================================================================
# shopify-dispatch-command -- stored rows. sync-now fans out to full sync
# (sync_job rows + account timestamps); disconnect disables the account and
# clears the token; ack-only types write nothing. No ledger effect anywhere
# on this path: dispatch never posts GL directly.
# ===========================================================================
class TestDispatchCommand:

    def test_sync_now_creates_five_completed_jobs(self, mconn):
        import sync as sync_mod
        env = build_env(mconn)
        domain = env["shopify_account"]["shop_domain"]
        with patch.object(sync_mod, "graphql_request", return_value={}):
            outcome = dispatch_command(
                mconn, domain,
                {"id": "cmd-1", "type": "sync-now", "payload": {}})
        assert outcome["dispatched"] is True
        assert outcome["action"] == "shopify-start-full-sync"
        assert outcome.get("exit_code") == 0

        j = Table("shopify_sync_job")
        jobs = mconn.execute(
            Q.from_(j).select(
                j.sync_type, j.sync_mode, j.status,
                j.records_processed)
            .where(j.shopify_account_id == P()).get_sql(),
            (env["shopify_account_id"],)).fetchall()
        assert len(jobs) == len(FULL_SYNC_ORDER) == 5
        assert {r["sync_type"] for r in jobs} == set(FULL_SYNC_ORDER)
        for row in jobs:
            assert row["sync_mode"] == "full"
            assert row["status"] == "completed"
            assert row["records_processed"] == 0
        acct = _read(mconn, "shopify_account", env["shopify_account_id"])
        for field in ("last_orders_sync_at", "last_products_sync_at",
                      "last_customers_sync_at", "last_payouts_sync_at",
                      "last_disputes_sync_at"):
            assert acct[field] is not None, field

    def test_disconnect_disables_account_and_clears_token(self, mconn):
        import disconnect as disconnect_mod
        env = build_env(mconn)
        domain = env["shopify_account"]["shop_domain"]
        with patch.object(
                disconnect_mod, "_revoke_access_token",
                return_value=(True, "revoked in test")), patch.object(
                disconnect_mod, "_uninstall_daemon_best_effort",
                return_value={"uninstalled": False}):
            outcome = dispatch_command(
                mconn, domain,
                {"id": "cmd-2", "type": "disconnect", "payload": {}})
        assert outcome["dispatched"] is True
        assert outcome["action"] == "shopify-disconnect"
        row = _read(mconn, "shopify_account", env["shopify_account_id"])
        assert row["status"] == "disabled"
        assert row["access_token_enc"] == ""

    def test_refresh_token_ack_writes_nothing(self, mconn):
        # Real behaviour, documented: refresh-token is a v1.1 ack stub. It
        # reports dispatched so the Worker drops the command, and changes
        # nothing, reaching no ledger.
        env = build_env(mconn)
        domain = env["shopify_account"]["shop_domain"]
        snap_before = _snapshot(mconn)
        outcome = dispatch_command(
            mconn, domain,
            {"id": "cmd-3", "type": "refresh-token", "payload": {}})
        assert outcome["dispatched"] is True
        assert outcome["action"] == "refresh-token"
        assert _snapshot(mconn) == snap_before

    def test_unknown_type_not_dispatched_and_writes_nothing(self, mconn):
        env = build_env(mconn)
        domain = env["shopify_account"]["shop_domain"]
        snap_before = _snapshot(mconn)
        outcome = dispatch_command(
            mconn, domain,
            {"id": "cmd-4", "type": "totally-fake", "payload": {}})
        assert outcome["dispatched"] is False
        assert _snapshot(mconn) == snap_before

    def test_gdpr_dispatch_without_module_writes_nothing(self, mconn):
        env = build_env(mconn)
        domain = env["shopify_account"]["shop_domain"]
        snap_before = _snapshot(mconn)
        with patch.dict(sys.modules, {"gdpr": None}):
            outcome = dispatch_command(
                mconn, domain,
                {"id": "cmd-5", "type": "gdpr-dispatch",
                 "payload": {"topic": "shop/redact"}})
        assert outcome["dispatched"] is False
        assert _snapshot(mconn) == snap_before

    def test_wrapper_refuses_missing_command(self, mconn):
        build_env(mconn)
        snap_before = _snapshot(mconn)
        result = call_action(
            DISPATCHER_ACTIONS["shopify-dispatch-command"], mconn, ns(
                shop_domain=None, command_json=None))
        assert is_error(result), result
        assert result["message"] == (
            "--shop-domain and --command-json are both required")
        assert _snapshot(mconn) == snap_before

    def test_wrapper_refuses_invalid_json(self, mconn):
        build_env(mconn)
        snap_before = _snapshot(mconn)
        result = call_action(
            DISPATCHER_ACTIONS["shopify-dispatch-command"], mconn, ns(
                shop_domain="x.myshopify.com", command_json="{bad"))
        assert is_error(result), result
        assert result["message"].startswith(
            "--command-json is not valid JSON")
        assert _snapshot(mconn) == snap_before
