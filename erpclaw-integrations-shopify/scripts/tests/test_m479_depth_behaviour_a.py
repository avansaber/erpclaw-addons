"""M479 depth, part A -- stored-row and filesystem effects (no ledger).

Covers 6 of the 10 named actions with tests that read the database back
instead of only checking the response envelope:

  shopify-configure-gl              stored row (shopify_account mapping + audit)
  shopify-test-connection           stored row (shopify_account shop_name/status)
  shopify-get-dispute               stored row (read-only; payload mirrors row)
  shopify-list-payout-transactions  stored row (read-only; payload mirrors rows)
  shopify-install-daemon            filesystem effect; DB byte-identical
  shopify-uninstall-daemon          filesystem effect; DB byte-identical

Prior state (read before writing): shopify-configure-gl, shopify-test-connection,
shopify-get-dispute and shopify-list-payout-transactions had NO test at all
(test_accounts.py only names configure-gl/test-connection in a docstring).
shopify-install-daemon / shopify-uninstall-daemon had mechanism tests in
test_daemon.py that call install_daemon()/uninstall_daemon() directly and never
touch the DB; the tests below drive the router actions and prove the DB is
untouched.

Conventions:
  - Reads go back through PyPika (erpclaw_lib.query). No catalog-table
    probes, no pragma statements, and no schema-dictionary queries below.
  - Money compares exact Decimal strings; never binary float, never
    approximate.
  - Every refusal case asserts the truthful message AND a byte-identical DB
    snapshot. A refusal that half-writes is worse than no refusal.
  - Where an action never reaches the ledger it says so explicitly, so a later
    reader does not add a balance assertion that cannot hold.
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
    build_env, seed_gl_account, seed_shopify_order,
    seed_shopify_payout, seed_shopify_dispute, _uuid,
)
from erpclaw_lib.query import Q, P, Table, insert_row
from accounts import ACTIONS as ACCOUNTS_ACTIONS
from browse import ACTIONS as BROWSE_ACTIONS


GL_MAPPING_COLUMNS = (
    "clearing_account_id", "revenue_account_id",
    "shipping_revenue_account_id", "tax_payable_account_id",
    "cogs_account_id", "inventory_account_id", "fee_account_id",
    "discount_account_id", "refund_account_id", "chargeback_account_id",
    "chargeback_fee_account_id", "gift_card_liability_account_id",
    "reserve_account_id", "bank_account_id",
)

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
    """Build the full schema once per module; per-test DBs are page copies."""
    path = str(tmp_path_factory.mktemp("m479tpl_a") / "template.sqlite")
    init_all_tables(path)
    return path


@pytest.fixture
def mconn(template_path, tmp_path):
    """Fresh isolated DB per test without re-running ~800 DDL statements."""
    from erpclaw_lib.db import get_connection
    dest = str(tmp_path / "case.sqlite")
    src = get_connection(template_path)
    try:
        dst = get_connection(dest)
        try:
            # Page-level copy of the template. backup() needs raw
            # DB-API connections; both still originate from get_connection().
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


# ===========================================================================
# shopify-configure-gl -- stored row. No ledger effect: remapping accounts
# posts nothing; it only changes which account later postings will use.
# ===========================================================================
class TestConfigureGL:

    def test_updates_mapping_and_writes_audit(self, mconn):
        env = build_env(mconn)
        acct = env["shopify_account"]
        before = {col: acct[col] for col in GL_MAPPING_COLUMNS}
        new_clearing = seed_gl_account(
            mconn, env["company_id"], "Alt Clearing", "asset", "bank")
        new_revenue = seed_gl_account(
            mconn, env["company_id"], "Alt Revenue", "income", "revenue")
        snap_before = _snapshot(mconn)

        result = call_action(
            ACCOUNTS_ACTIONS["shopify-configure-gl"], mconn, ns(
                shopify_account_id=env["shopify_account_id"],
                clearing_account_id=new_clearing,
                revenue_account_id=new_revenue,
            ))
        assert is_ok(result), result
        assert sorted(result["updated_mappings"]) == [
            "clearing_account_id", "revenue_account_id"]

        row = _read(mconn, "shopify_account", env["shopify_account_id"])
        assert row["clearing_account_id"] == new_clearing
        assert row["revenue_account_id"] == new_revenue
        for col in GL_MAPPING_COLUMNS:
            if col not in ("clearing_account_id", "revenue_account_id"):
                assert row[col] == before[col], col

        audits = _audit_rows(mconn, "shopify-configure-gl")
        assert len(audits) == 1
        assert (audits[0]["skill"], audits[0]["entity_type"],
                audits[0]["entity_id"]) == (
            "erpclaw-integrations-shopify", "shopify_account",
            env["shopify_account_id"])

        # No ledger effect: no journal_entry, no gl_entry.
        snap_after = _snapshot(mconn)
        assert snap_after["gl_entry"] == snap_before["gl_entry"]
        assert snap_after["journal_entry"] == snap_before["journal_entry"]

    def test_refuses_unknown_gl_account(self, mconn):
        env = build_env(mconn)
        snap_before = _snapshot(mconn)
        result = call_action(
            ACCOUNTS_ACTIONS["shopify-configure-gl"], mconn, ns(
                shopify_account_id=env["shopify_account_id"],
                clearing_account_id="gl-does-not-exist",
            ))
        assert is_error(result), result
        assert result["message"] == (
            "GL account (clearing_account_id) gl-does-not-exist "
            "not found in chart of accounts")
        assert _snapshot(mconn) == snap_before

    def test_refuses_missing_account_id(self, mconn):
        env = build_env(mconn)
        snap_before = _snapshot(mconn)
        result = call_action(
            ACCOUNTS_ACTIONS["shopify-configure-gl"], mconn, ns(
                shopify_account_id=None,
                clearing_account_id=env["shopify_account"]["fee_account_id"],
            ))
        assert is_error(result), result
        assert result["message"] == "--shopify-account-id is required"
        assert _snapshot(mconn) == snap_before


# ===========================================================================
# shopify-test-connection -- stored row. No ledger effect: success rewrites
# shop_name, failure flips status to error; neither posts anything.
# ===========================================================================
class TestTestConnection:

    def test_success_updates_shop_name(self, mconn):
        import accounts as accounts_mod
        env = build_env(mconn)
        assert _read(
            mconn, "shopify_account",
            env["shopify_account_id"])["shop_name"] == "Test Shop"
        remote = {"shop": {
            "name": "Remote Shop",
            "url": "https://remote.myshopify.com",
            "myshopifyDomain": "remote.myshopify.com",
            "currencyCode": "USD",
        }}
        with patch.object(
                accounts_mod, "graphql_request", return_value=remote):
            result = call_action(
                ACCOUNTS_ACTIONS["shopify-test-connection"], mconn, ns(
                    shopify_account_id=env["shopify_account_id"]))
        assert is_ok(result), result
        assert result["connection"] == "success"
        assert result["shop_name"] == "Remote Shop"
        row = _read(mconn, "shopify_account", env["shopify_account_id"])
        assert row["shop_name"] == "Remote Shop"
        assert row["status"] == "active"
        # No ledger effect: connection test posts nothing.

    def test_failure_marks_account_error(self, mconn):
        import accounts as accounts_mod
        env = build_env(mconn)
        with patch.object(
                accounts_mod, "graphql_request",
                side_effect=RuntimeError("denied")):
            result = call_action(
                ACCOUNTS_ACTIONS["shopify-test-connection"], mconn, ns(
                    shopify_account_id=env["shopify_account_id"]))
        assert is_error(result), result
        assert result["message"] == "Shopify connection test failed: denied"
        row = _read(mconn, "shopify_account", env["shopify_account_id"])
        assert row["status"] == "error"
        assert row["shop_name"] == "Test Shop"

    def test_refuses_missing_account_id(self, mconn):
        build_env(mconn)
        snap_before = _snapshot(mconn)
        result = call_action(
            ACCOUNTS_ACTIONS["shopify-test-connection"], mconn,
            ns(shopify_account_id=None))
        assert is_error(result), result
        assert result["message"] == "--shopify-account-id is required"
        assert _snapshot(mconn) == snap_before


# ===========================================================================
# shopify-get-dispute -- stored row, read-only by design. No ledger effect:
# the success path writes nothing, not even an audit row.
# ===========================================================================
class TestGetDispute:

    def test_returns_exact_stored_values_with_linked_order(self, mconn):
        env = build_env(mconn)
        oid = seed_shopify_order(
            mconn, env["shopify_account_id"], env["company_id"],
            shopify_order_id="9001", subtotal="200.00", shipping="20.00",
            tax="16.00", discount="0")
        did = seed_shopify_dispute(
            mconn, env["shopify_account_id"], env["company_id"],
            amount="65.50", fee_amount="15.00", status="needs_response",
            order_id=oid)
        snap_before = _snapshot(mconn)

        result = call_action(
            BROWSE_ACTIONS["shopify-get-dispute"], mconn,
            ns(shopify_dispute_id_local=did))
        assert is_ok(result), result
        assert result["id"] == did
        assert result["amount"] == "65.50"
        assert Decimal(result["amount"]) == Decimal("65.50")
        assert result["fee_amount"] == "15.00"
        assert Decimal(result["fee_amount"]) == Decimal("15.00")
        # Envelope collision, documented: ok() moves the dispute row's own
        # "status" field to document_status so the envelope status stays "ok".
        assert result["status"] == "ok"
        assert result["document_status"] == "needs_response"
        assert result["linked_order"]["shopify_order_number"] == "#9001"
        assert Decimal(result["linked_order"]["total_amount"]) == Decimal("236.00")
        # Read-only proof: the success path changed nothing.
        assert _snapshot(mconn) == snap_before

    def test_refuses_unknown_dispute(self, mconn):
        build_env(mconn)
        snap_before = _snapshot(mconn)
        missing = _uuid()
        result = call_action(
            BROWSE_ACTIONS["shopify-get-dispute"], mconn,
            ns(shopify_dispute_id_local=missing))
        assert is_error(result), result
        assert result["message"] == f"Shopify dispute {missing} not found"
        assert _snapshot(mconn) == snap_before

    def test_refuses_missing_dispute_id(self, mconn):
        build_env(mconn)
        snap_before = _snapshot(mconn)
        result = call_action(
            BROWSE_ACTIONS["shopify-get-dispute"], mconn,
            ns(shopify_dispute_id_local=None))
        assert is_error(result), result
        assert result["message"] == (
            "--shopify-dispute-id-local is required (internal UUID)")
        assert _snapshot(mconn) == snap_before


# ===========================================================================
# shopify-list-payout-transactions -- stored rows, read-only by design.
# No ledger effect.
# ===========================================================================

def _seed_txn(conn, payout_id, company_id, txn_type, gross, fee, net):
    sql, _ = insert_row("shopify_payout_transaction", {
        "id": P(), "shopify_payout_id_local": P(),
        "shopify_balance_txn_id": P(), "transaction_type": P(),
        "gross_amount": P(), "fee_amount": P(), "net_amount": P(),
        "processed_at": P(), "company_id": P(),
    })
    conn.execute(sql, (
        _uuid(), payout_id, _uuid()[:12], txn_type, gross, fee, net,
        "2026-03-14T12:00:00Z", company_id,
    ))
    conn.commit()


class TestListPayoutTransactions:

    def test_returns_exact_transactions_scoped_to_payout(self, mconn):
        env = build_env(mconn)
        pay_a = seed_shopify_payout(
            mconn, env["shopify_account_id"], env["company_id"])
        _seed_txn(mconn, pay_a, env["company_id"], "charge",
                  "194.00", "5.62", "188.38")
        _seed_txn(mconn, pay_a, env["company_id"], "refund",
                  "-50.00", "0", "-50.00")
        pay_b = seed_shopify_payout(
            mconn, env["shopify_account_id"], env["company_id"],
            gross="500.00", fee="14.50")
        _seed_txn(mconn, pay_b, env["company_id"], "charge",
                  "500.00", "14.50", "485.50")
        snap_before = _snapshot(mconn)

        result = call_action(
            BROWSE_ACTIONS["shopify-list-payout-transactions"], mconn,
            ns(shopify_payout_id=pay_a))
        assert is_ok(result), result
        assert result["count"] == 2
        assert len(result["transactions"]) == 2
        assert {t["shopify_payout_id_local"]
                for t in result["transactions"]} == {pay_a}
        by_type = {t["transaction_type"]: t
                   for t in result["transactions"]}
        assert set(by_type) == {"charge", "refund"}
        charge = by_type["charge"]
        assert (charge["gross_amount"], charge["fee_amount"],
                charge["net_amount"]) == ("194.00", "5.62", "188.38")
        assert (Decimal(charge["gross_amount"])
                - Decimal(charge["fee_amount"])) == Decimal(
                    charge["net_amount"])
        refund = by_type["refund"]
        assert (refund["gross_amount"], refund["fee_amount"],
                refund["net_amount"]) == ("-50.00", "0", "-50.00")
        # Read-only proof.
        assert _snapshot(mconn) == snap_before

    def test_refuses_missing_payout_id(self, mconn):
        build_env(mconn)
        snap_before = _snapshot(mconn)
        result = call_action(
            BROWSE_ACTIONS["shopify-list-payout-transactions"], mconn,
            ns(shopify_payout_id=None))
        assert is_error(result), result
        assert result["message"] == (
            "--shopify-payout-id is required (local UUID)")
        assert _snapshot(mconn) == snap_before


# ===========================================================================
# shopify-install-daemon / shopify-uninstall-daemon -- filesystem effects.
# Neither action touches the database at all (conn is accepted only because
# the router passes it); every test below proves the DB is byte-identical.
# No ledger effect.
# ===========================================================================

def _fake_run(returncode=0, stdout="", stderr=""):
    from unittest.mock import MagicMock
    result = MagicMock()
    result.returncode = returncode
    result.stdout = stdout
    result.stderr = stderr
    return result


class TestInstallDaemon:

    def test_install_launchd_writes_plist_via_action(
            self, mconn, tmp_path, monkeypatch):
        import daemon as daemon_mod
        monkeypatch.setattr(daemon_mod, "HOME", str(tmp_path))
        plist = str(tmp_path / "Library" / "LaunchAgents"
                    / "com.avansaber.erpclaw.shopify-push.plist")
        monkeypatch.setattr(daemon_mod, "LAUNCHD_PLIST", plist)
        monkeypatch.setattr(
            daemon_mod.platform, "system", lambda: "Darwin")
        calls = []

        def fake_run(cmd, **kw):
            calls.append(cmd)
            return _fake_run(returncode=0)

        monkeypatch.setattr(daemon_mod.subprocess, "run", fake_run)
        snap_before = _snapshot(mconn)

        result = call_action(
            daemon_mod.shopify_install_daemon, mconn, ns())
        assert is_ok(result), result
        assert result["mechanism"] == "launchd"
        assert result["loaded"] is True
        with open(plist, encoding="utf-8") as handle:
            content = handle.read()
        assert "--action" in content
        assert "shopify-push-status" in content
        assert "com.avansaber.erpclaw.shopify-push" in content
        assert any("load" in cmd for cmd in calls)
        assert _snapshot(mconn) == snap_before

    def test_install_refuses_unsupported_os(self, mconn, tmp_path, monkeypatch):
        import daemon as daemon_mod
        monkeypatch.setattr(daemon_mod, "HOME", str(tmp_path))
        plist = str(tmp_path / "not-installed.plist")
        monkeypatch.setattr(daemon_mod, "LAUNCHD_PLIST", plist)
        monkeypatch.setattr(
            daemon_mod.platform, "system", lambda: "Windows")
        snap_before = _snapshot(mconn)

        result = call_action(
            daemon_mod.shopify_install_daemon, mconn, ns())
        assert is_error(result), result
        assert result["message"] == "install failed: unsupported OS: Windows"
        assert not os.path.exists(plist)
        assert _snapshot(mconn) == snap_before


class TestUninstallDaemon:

    def test_uninstall_removes_plist_via_action(
            self, mconn, tmp_path, monkeypatch):
        import daemon as daemon_mod
        monkeypatch.setattr(daemon_mod, "HOME", str(tmp_path))
        plist_dir = tmp_path / "Library" / "LaunchAgents"
        plist_dir.mkdir(parents=True)
        plist = str(plist_dir / "com.avansaber.erpclaw.shopify-push.plist")
        with open(plist, "w", encoding="utf-8") as handle:
            handle.write("<plist/>")
        monkeypatch.setattr(daemon_mod, "LAUNCHD_PLIST", plist)
        monkeypatch.setattr(
            daemon_mod.platform, "system", lambda: "Darwin")
        monkeypatch.setattr(
            daemon_mod.subprocess, "run",
            lambda cmd, **kw: _fake_run(returncode=0))
        snap_before = _snapshot(mconn)

        result = call_action(
            daemon_mod.shopify_uninstall_daemon, mconn, ns())
        assert is_ok(result), result
        assert result["uninstalled"] is True
        assert not os.path.exists(plist)
        assert _snapshot(mconn) == snap_before

    def test_uninstall_is_noop_when_absent(self, mconn, tmp_path, monkeypatch):
        # The action defines no required input, so there is no input
        # validation to refuse; the honest no-op with a truthful reason is
        # the refusal analogue, and it must still write nothing.
        import daemon as daemon_mod
        monkeypatch.setattr(daemon_mod, "HOME", str(tmp_path))
        monkeypatch.setattr(
            daemon_mod, "LAUNCHD_PLIST",
            str(tmp_path / "Library" / "LaunchAgents" / "absent.plist"))
        monkeypatch.setattr(
            daemon_mod.platform, "system", lambda: "Darwin")
        snap_before = _snapshot(mconn)

        result = call_action(
            daemon_mod.shopify_uninstall_daemon, mconn, ns())
        assert is_ok(result), result
        assert result["uninstalled"] is False
        assert _snapshot(mconn) == snap_before
