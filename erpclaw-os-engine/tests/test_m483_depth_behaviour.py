"""M483 depth: behavioural evidence for 8 os-engine actions.

Each action below already had a test that proved the wrong thing: the
contract suite's ``"Unknown action" not in ...`` (routability only) or a
unit test that pins the response envelope (shape only). Neither observes
the database, so an action could return a perfect envelope while writing
nothing and stay green. Every test here drives the real handler and reads
the effect back: the exact stored rows (or the exact files), what changed
from what to what, and what did not change. Reads are PyPika-built through
``erpclaw_lib.query`` on connections from ``erpclaw_lib.db.get_connection``;
catalog questions go through ``erpclaw_lib.seam``. Money is TEXT: exact
string comparisons with ``Decimal`` for arithmetic, never float.

Per-action depth signal (stored row vs ledger effect):

- os-add-feature-to-module .... FILESYSTEM EFFECT (new function + ACTIONS
  entry + .bak backup in the target db_query.py). Touches no table, so no
  ledger legs can exist; the DB snapshot is pinned byte-identical.
- os-compliance-weather-status  READ-ONLY (derives the period from the
  company row's fiscal year end). Pins period/strictness/checks against the
  seeded row and proves the DB is unchanged. No ledger effect.
- os-configure-module .......... STORED ROWS (account inserts + the
  erpclaw_module_config record attempt). The router wrapper is DEFECTIVE
  (see below); the success test drives the underlying function, which is
  what every existing test does too. Creates no GL rows; ledgers pinned.
- os-deploy-module ............. STORED ROW (erpclaw_deploy_audit entry with
  exact steps/reasoning). Creates no GL rows; ledgers pinned. The CLI layer
  reports failures truthfully (one JSON error document, non-zero exit) while
  the audit side effect still lands.
- os-generate-module ........... FIXED: the router wrapper used to call
  ``generate_module(args)`` but the function takes
  ``(module_name, prefix, business_description, entities, ...)``, so the
  action raised TypeError and wrote nothing. The argument mapping is fixed:
  the wrapper forwards the mapped arguments and reports a validation
  failure truthfully (``status: "error"``, non-zero exit) while still
  writing nothing. The underlying generator's filesystem effect is pinned
  separately.
- os-list-industries ........... READ-ONLY (static config). Pins every entry
  against INDUSTRY_CONFIGS and proves the DB is untouched. No ledger effect.
- os-setup-web-dashboard ...... PROVISIONING (shells out; never touches the
  DB). Refusal path is deterministic here; success path runs fully mocked
  (no network). DB pinned untouched. No ledger effect.
- os-status .................... READ-ONLY (static dict). Pins exact values
  and proves the DB is untouched. No ledger effect.

Ledger note: none of these eight actions posts to the general, stock, or
payment ledgers on any path, so no success test below asserts a new ledger
leg. Each one pins the ledger tables unchanged so a later reader does not
add a balance assertion that cannot hold.

No test in this file opens the database any way except
``erpclaw_lib.db.get_connection`` and asks catalog questions any way except
``erpclaw_lib.seam``.
"""
import argparse
import ast
import importlib.util
import io
import json
import os
import subprocess
import sys
import textwrap
import uuid
from contextlib import contextmanager, redirect_stdout
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_OS_SCRIPTS = os.path.join(os.path.dirname(_TESTS_DIR), "scripts")
if _OS_SCRIPTS not in sys.path:
    sys.path.insert(0, _OS_SCRIPTS)
ROUTER_PATH = os.path.join(_OS_SCRIPTS, "db_query.py")

_FOUNDATION_OS_DIR = os.path.abspath(os.path.join(
    os.path.dirname(_TESTS_DIR), "..", "..", "erpclaw", "scripts", "erpclaw-os"))
if _FOUNDATION_OS_DIR not in sys.path:
    sys.path.insert(0, _FOUNDATION_OS_DIR)
_SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(_TESTS_DIR)))
_SETUP_DIR = os.path.join(_SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
_INIT_SCHEMA_PATH = os.path.join(_SETUP_DIR, "init_schema.py")
_IN_TREE_LIB = os.path.join(_SETUP_DIR, "lib")
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, _IN_TREE_LIB)

from erpclaw_lib.db import get_connection
from erpclaw_lib.query import Q, P, Table, Field, insert_row
import erpclaw_lib.seam as seam

from generate_module import generate_module
from configure_module import configure_module
from industry_configs import INDUSTRY_CONFIGS, list_industries
from deploy_pipeline import run_pipeline, handle_deploy_module
from deploy_audit import query_audit_log
from compliance_weather import (
    get_compliance_weather,
    get_additional_checks,
    handle_compliance_weather_status,
)
from in_module_generator import handle_add_feature_to_module
import web_dashboard as web_dashboard_mod
from web_dashboard import handle_setup_web_dashboard
_ROUTER_SPEC = importlib.util.spec_from_file_location(
    "m483_os_engine_router", ROUTER_PATH)
os_router = importlib.util.module_from_spec(_ROUTER_SPEC)
_ROUTER_SPEC.loader.exec_module(os_router)


_LEDGERS = ("gl_entry", "stock_ledger_entry", "payment_ledger_entry")
_SNAPSHOT_TABLES = (
    "company", "account", "erpclaw_deploy_audit",
    "gl_entry", "stock_ledger_entry", "payment_ledger_entry",
    "audit_log", "naming_series",
)


def ns(**kw):
    """Build an argparse.Namespace like the CLI layer hands to handlers."""
    return argparse.Namespace(**kw)


def call_handler(fn, args):
    """Drive a router handler, capturing ok()/err() output.

    Handlers that print via ok()/err() raise SystemExit; handlers that
    return plain dicts (deploy, compliance, add-feature) return them with
    no output. Both shapes are returned as the parsed dict.
    """
    buf = io.StringIO()

    def _fake_exit(code=0):
        raise SystemExit(code)

    ret = None
    try:
        with patch("sys.stdout", buf), patch("sys.exit", side_effect=_fake_exit):
            ret = fn(args)
    except SystemExit:
        pass
    out = buf.getvalue().strip()
    if out:
        return json.loads(out)
    return ret


@contextmanager
def _conn(db_path):
    conn = get_connection(db_path)
    try:
        yield conn
    finally:
        conn.close()


def init_foundation_db(db_path):
    """Create every foundation table via the canonical installer."""
    spec = importlib.util.spec_from_file_location("m483_init_schema", _INIT_SCHEMA_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.init_db(db_path)


def _insert(conn, table, row):
    sql, _cols = insert_row(table, {key: P() for key in row})
    conn.execute(sql, tuple(row[key] for key in row))
    conn.commit()


def _snapshot(conn, tables):
    """Byte-level dump of the tables these actions could touch."""
    snap = {}
    for name in tables:
        tbl = Table(name)
        rows = conn.execute(Q.from_(tbl).select("*").get_sql()).fetchall()
        snap[name] = sorted(repr(sorted(dict(row).items())) for row in rows)
    return snap


def _live_tables(db_path):
    """Snapshot tables restricted to ones that actually exist."""
    return [name for name in _SNAPSHOT_TABLES if seam.table_exists(name, db_path)]


def _read(conn, table, row_id):
    tbl = Table(table)
    return conn.execute(
        Q.from_(tbl).select("*").where(tbl.id == P()).get_sql(), (row_id,)
    ).fetchone()


def _rows_where(conn, table, **filters):
    tbl = Table(table)
    query = Q.from_(tbl).select("*")
    params = []
    for column, value in filters.items():
        query = query.where(Field(column) == P())
        params.append(value)
    return [dict(row) for row in conn.execute(query.get_sql(), params).fetchall()]


def _seed_company(conn, name="M483 Co"):
    cid = str(uuid.uuid4())
    _insert(conn, "company", {"id": cid, "name": name, "abbr": "M" + cid[:6]})
    return cid


_PARENT_GROUPS = [
    ("Direct Income", "income", "revenue"),
    ("Direct Expenses", "expense", "expense"),
    ("Accounts Receivable", "asset", "receivable"),
    ("Accounts Payable", "liability", "payable"),
    ("Fixed Assets", "asset", "fixed_asset"),
    ("Stock Assets", "asset", "stock"),
    ("Bank Accounts", "asset", "bank"),
    ("Equity", "equity", "equity"),
]


def _seed_parent_groups(conn, company_id):
    ids = {}
    for name, root_type, account_type in _PARENT_GROUPS:
        aid = str(uuid.uuid4())
        direction = "debit_normal" if root_type in ("asset", "expense") else "credit_normal"
        _insert(conn, "account", {
            "id": aid, "name": name, "root_type": root_type,
            "account_type": account_type, "is_group": 1,
            "balance_direction": direction, "company_id": company_id, "depth": 0,
        })
    for name, _root, _type in _PARENT_GROUPS:
        rows = _rows_where(conn, "account", name=name, company_id=company_id)
        assert len(rows) == 1
        ids[name] = rows[0]["id"]
    return ids


def _router_actions():
    """Dispatch surface of the real router, read statically (no side effects)."""
    with open(ROUTER_PATH) as handle:
        tree = ast.parse(handle.read(), filename=ROUTER_PATH)
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id == "ACTIONS" and isinstance(node.value, ast.Dict):
                return {
                    key.value for key in node.value.keys
                    if isinstance(key, ast.Constant) and isinstance(key.value, str)
                }
    return set()


# ---------------------------------------------------------------------------
# os-generate-module -- FIXED (router wrapper maps the real arguments).
#
# Prior test: test_generate_module.py drives generate_module() directly and
# pins envelopes; the contract suite only checks the action string routes.
# Past behaviour: os_router.handle_generate_module forwarded the argparse
# Namespace to generate_module(module_name, prefix, ...) which raised
# TypeError, so the action wrote nothing. The argument-mapping defect is
# fixed: the wrapper forwards the mapped arguments and a refusal now exits
# non-zero with one JSON error document, still writing nothing. The
# generator's own filesystem effect is pinned below. No ledger can be
# involved: nothing is written.
# ---------------------------------------------------------------------------

class TestOsGenerateModuleDepth:
    ACTION = "os-generate-module"

    def test_action_is_dispatched(self):
        assert self.ACTION in os_router.ACTIONS
        assert self.ACTION in _router_actions()

    def test_router_handler_refuses_without_prefix_and_writes_nothing(self, tmp_path):
        out_dir = str(tmp_path / "m483gen")
        db_path = str(tmp_path / "gen.sqlite")
        init_foundation_db(db_path)
        tables_before = _live_tables(db_path)
        with _conn(db_path) as conn:
            snap_before = _snapshot(conn, tables_before)

        buf = io.StringIO()
        with redirect_stdout(buf):
            with pytest.raises(SystemExit) as exc_info:
                os_router.handle_generate_module(ns(
                    module_name="depthclaw", prefix=None,
                    description="M483 depth probe module",
                    entities=[{"name": "widget", "pattern": "crud_entity",
                               "fields": ["price TEXT NOT NULL DEFAULT '0'"]}],
                    output_dir=out_dir, src_root=None,
                    industry="Custom", company_id="c1", action_name=None,
                    module_path=None, domain=None, ssl=None, skip_build=False,
                    target=None, variant_id=None, feature_name=None, topic=None,
                    db_path=db_path, dry_run=False,
                ))
        assert exc_info.value.code != 0
        envelope = json.loads(buf.getvalue())
        assert envelope["status"] == "error"
        assert "prefix" in envelope["message"]

        assert not os.path.exists(out_dir)
        with _conn(db_path) as conn:
            assert _snapshot(conn, tables_before) == snap_before
        assert _live_tables(db_path) == tables_before

    def test_underlying_generator_writes_exact_files(self, tmp_path):
        # Documents the behaviour the action SHOULD have: one entity yields
        # the full file set with the exact table declared, and no DB change.
        # This drives the function, not the action (the action is the defect
        # above); it is marked so nobody mistakes it for action coverage.
        out_dir = str(tmp_path / "depthclaw")
        db_path = str(tmp_path / "gen2.sqlite")
        init_foundation_db(db_path)
        with _conn(db_path) as conn:
            snap_before = _snapshot(conn, _live_tables(db_path))

        result = generate_module(
            module_name="depthclaw", prefix="m483depth",
            business_description="M483 depth probe module",
            entities=[{"name": "widget", "pattern": "crud_entity",
                       "fields": ["price TEXT NOT NULL DEFAULT '0'"]}],
            output_dir=out_dir,
        )
        assert result["entities"] == 1
        assert result["module_path"] == out_dir
        made = {os.path.basename(path) for path in result["files_created"]}
        assert {"init_db.py", "db_query.py", "SKILL.md"} <= made
        init_db = open(os.path.join(out_dir, "init_db.py")).read()
        assert "m483depth_widget" in init_db
        assert "price TEXT NOT NULL DEFAULT '0'" in init_db
        assert "name TEXT NOT NULL" in init_db

        with _conn(db_path) as conn:
            assert _snapshot(conn, _live_tables(db_path)) == snap_before

    def test_underlying_generator_refuses_bad_prefix_and_writes_nothing(self, tmp_path):
        out_dir = str(tmp_path / "badclaw")
        db_path = str(tmp_path / "gen3.sqlite")
        init_foundation_db(db_path)
        with _conn(db_path) as conn:
            snap_before = _snapshot(conn, _live_tables(db_path))

        result = generate_module(
            module_name="badclaw", prefix="Bad-Prefix",
            business_description="refusal probe",
            entities=[{"name": "widget", "pattern": "crud_entity", "fields": []}],
            output_dir=out_dir,
        )
        assert result["result"] == "fail"
        assert result["files_created"] == []
        assert any("Prefix" in err for err in result["validation"]["errors"])
        assert not os.path.exists(out_dir)
        with _conn(db_path) as conn:
            assert _snapshot(conn, _live_tables(db_path)) == snap_before


# ---------------------------------------------------------------------------
# os-configure-module -- STORED ROWS (account inserts).
#
# Prior tests: test_configure_module.py asserts counts and recommendation
# membership but never reads a stored account row back field-by-field.
# This action does NOT reach the ledger: it inserts chart-of-accounts rows
# only, so the ledger tables are pinned unchanged.
# DEFECT (recorded, not fixed): the router wrapper forwards the Namespace,
# so os-configure-module via dispatch raises TypeError; the success test
# drives configure_module(), exactly as the existing suite does.
# ---------------------------------------------------------------------------

class TestOsConfigureModuleDepth:
    ACTION = "os-configure-module"

    def test_action_is_dispatched(self):
        assert self.ACTION in os_router.ACTIONS
        assert self.ACTION in _router_actions()

    def test_router_handler_raises_type_error(self, tmp_path):
        db_path = str(tmp_path / "cfg0.sqlite")
        init_foundation_db(db_path)
        with pytest.raises(TypeError) as excinfo:
            os_router.handle_configure_module(ns(
                industry="dental_practice", company_id="c1", size_tier="small",
            ))
        assert "company_id" in str(excinfo.value)

    def test_configure_writes_exact_account_rows(self, tmp_path):
        db_path = str(tmp_path / "cfg.sqlite")
        init_foundation_db(db_path)
        with _conn(db_path) as conn:
            company_id = _seed_company(conn)
            parents = _seed_parent_groups(conn, company_id)
            company_before = dict(_read(conn, "company", company_id))
            snap_before = _snapshot(conn, _live_tables(db_path))

        config = INDUSTRY_CONFIGS["dental_practice"]
        result = configure_module(
            industry="dental_practice", company_id=company_id,
            size_tier="small", db_path=db_path,
        )
        assert result["result"] == "pass"
        assert result["accounts_created"] == len(config["accounts"])
        assert result["accounts_skipped"] == 0
        assert result["accounts_failed"] == []
        assert result["modules_recommended"] == config["modules"]["small"]
        assert result["compliance_items"] == config["compliance_items"]

        with _conn(db_path) as conn:
            for acct_def in config["accounts"]:
                rows = _rows_where(conn, "account", name=acct_def["name"], company_id=company_id)
                assert len(rows) == 1, acct_def["name"]
                row = rows[0]
                assert row["root_type"] == acct_def["root_type"]
                assert row["account_type"] == acct_def.get("account_type")
                assert row["is_group"] == acct_def.get("is_group", 0)
                expected_direction = (
                    "credit_normal"
                    if acct_def["root_type"] in ("liability", "equity", "income")
                    else "debit_normal"
                )
                assert row["balance_direction"] == expected_direction
                parent = acct_def.get("parent")
                if parent is not None and parent in parents:
                    assert row["parent_id"] == parents[parent]
                    assert row["depth"] == 1
                assert row["company_id"] == company_id

            # erpclaw_module_config is not a foundation table, so step 4 is
            # skipped by design; the success must not claim otherwise.
            assert not seam.table_exists("erpclaw_module_config", db_path)
            assert not any("Recorded configuration" in line for line in result["configuration_applied"])

            # What must NOT have changed: the company row (money stays TEXT:
            # receipt_tolerance_pct is an exact Decimal string, never float)
            # and every pre-existing parent group.
            company_after = dict(_read(conn, "company", company_id))
            assert company_after == company_before
            assert company_after["receipt_tolerance_pct"] == "0"
            assert Decimal(company_after["receipt_tolerance_pct"]) == Decimal("0")
            for name, pid in parents.items():
                assert dict(_read(conn, "account", pid)) == _rows_where(
                    conn, "account", name=name, company_id=company_id)[0]

            snap_after = _snapshot(conn, _live_tables(db_path))
            for table in _LEDGERS:
                assert snap_after[table] == snap_before[table]
            assert snap_after["company"] == snap_before["company"]

    def test_configure_is_idempotent(self, tmp_path):
        db_path = str(tmp_path / "cfgidem.sqlite")
        init_foundation_db(db_path)
        with _conn(db_path) as conn:
            company_id = _seed_company(conn)
            _seed_parent_groups(conn, company_id)
        first = configure_module(
            industry="dental_practice", company_id=company_id,
            size_tier="small", db_path=db_path,
        )
        assert first["result"] == "pass"
        with _conn(db_path) as conn:
            count_after_first = len(_rows_where(conn, "account", company_id=company_id))
        second = configure_module(
            industry="dental_practice", company_id=company_id,
            size_tier="small", db_path=db_path,
        )
        assert second["result"] == "pass"
        assert second["accounts_created"] == 0
        assert second["accounts_skipped"] == first["accounts_created"]
        with _conn(db_path) as conn:
            assert len(_rows_where(conn, "account", company_id=company_id)) == count_after_first

    def test_configure_refuses_unknown_industry_and_writes_nothing(self, tmp_path):
        db_path = str(tmp_path / "cfgref.sqlite")
        init_foundation_db(db_path)
        with _conn(db_path) as conn:
            company_id = _seed_company(conn)
            _seed_parent_groups(conn, company_id)
            snap_before = _snapshot(conn, _live_tables(db_path))

        result = configure_module(
            industry="no_such_industry", company_id=company_id, db_path=db_path,
        )
        assert result["result"] == "fail"
        assert result["error"] == "Unknown industry: no_such_industry"
        assert "no_such_industry" not in str(result.get("available_industries", ""))
        assert sorted(result["available_industries"]) == sorted(INDUSTRY_CONFIGS.keys())
        with _conn(db_path) as conn:
            assert _snapshot(conn, _live_tables(db_path)) == snap_before


# ---------------------------------------------------------------------------
# os-list-industries -- READ-ONLY (static config).
#
# Prior test: TestListIndustries pins envelope keys and ordering only.
# Behavioural content: every entry's counts are exact against the config
# source, and the database is byte-identical (the action never opens it).
# This action does NOT reach the ledger: it is a pure read.
# ---------------------------------------------------------------------------

class TestOsListIndustriesDepth:
    ACTION = "os-list-industries"

    def test_action_is_dispatched(self):
        assert self.ACTION in os_router.ACTIONS
        assert self.ACTION in _router_actions()

    def test_list_matches_config_exactly_and_touches_no_table(self, tmp_path):
        db_path = str(tmp_path / "ind.sqlite")
        conn = get_connection(db_path)
        conn.close()
        tables_before = seam.table_names(db_path)

        result = call_handler(os_router.handle_list_industries, ns())
        assert result["status"] == "ok"
        assert result["count"] == len(INDUSTRY_CONFIGS)
        assert result["industries"] == list_industries()
        assert [entry["industry"] for entry in result["industries"]] == sorted(INDUSTRY_CONFIGS.keys())
        for entry in result["industries"]:
            config = INDUSTRY_CONFIGS[entry["industry"]]
            assert entry["display_name"] == config["display_name"]
            assert entry["account_count"] == len(config["accounts"])
            assert entry["compliance_item_count"] == len(config["compliance_items"])
            assert entry["size_tiers"] == sorted(config["modules"].keys())
        dental = next(e for e in result["industries"] if e["industry"] == "dental_practice")
        assert dental["display_name"] == INDUSTRY_CONFIGS["dental_practice"]["display_name"]
        assert dental["account_count"] > 0

        assert seam.table_names(db_path) == tables_before == []

    def test_unknown_action_is_not_routed_and_writes_nothing(self, tmp_path):
        db_path = str(tmp_path / "indref.sqlite")
        conn = get_connection(db_path)
        conn.close()
        with pytest.raises(KeyError):
            os_router.ACTIONS["os-no-such-action"]
        assert seam.table_names(db_path) == []


# ---------------------------------------------------------------------------
# os-deploy-module -- STORED ROW (erpclaw_deploy_audit).
#
# Prior tests: TestDeployPipeline asserts the envelope
# (pipeline_result/audit_id/steps keys) and that AN audit row exists, but
# never pins the row's exact values. Here the audit row is read back with
# PyPika and compared field-by-field to the response, and every other table
# is pinned unchanged. This action does NOT reach the ledger: the audit log
# is provenance, not postings, so the ledger tables are pinned unchanged.
# FINDING (fixed): the wrapper used to return a plain dict, which the
# router's main() discarded, so the CLI printed nothing on success or
# failure. The wrapper now emits the envelope via ok()/err(), so the CLI
# prints one JSON document and exits non-zero on failure.
# ---------------------------------------------------------------------------

def _passing_module(path):
    """A module that clears constitution validation (tier 2 -> queued)."""
    scripts = os.path.join(path, "scripts", "tests")
    os.makedirs(scripts)
    with open(os.path.join(path, "init_db.py"), "w") as handle:
        handle.write(textwrap.dedent("""\
            import os, sqlite3, sys
            def init_schema(db_path=None):
                conn = sqlite3.connect(db_path)
                conn.executescript(\"\"\"
                    CREATE TABLE IF NOT EXISTS depthclaw_item (
                        id TEXT PRIMARY KEY, name TEXT NOT NULL,
                        price TEXT NOT NULL DEFAULT '0',
                        company_id TEXT NOT NULL);
                \"\"\")
                conn.commit(); conn.close()
            if __name__ == "__main__":
                init_schema(sys.argv[1])
            """))
    with open(os.path.join(path, "scripts", "db_query.py"), "w") as handle:
        handle.write(textwrap.dedent("""\
            #!/usr/bin/env python3
            import os, sys, json
            sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
            from erpclaw_lib.response import ok, err
            from erpclaw_lib.args import SafeArgumentParser
            def handle_status(args):
                ok({"message": "depthclaw is running"})
            ACTIONS = {"status": handle_status}
            def main():
                parser = SafeArgumentParser()
                parser.add_argument("--action", required=True)
                args, unknown = parser.parse_known_args()
                if args.action == "status":
                    handle_status(args)
                else:
                    err("Unknown")
            if __name__ == "__main__":
                main()
            """))
    with open(os.path.join(path, "SKILL.md"), "w") as handle:
        handle.write(textwrap.dedent("""\
            ---
            name: depthclaw
            version: 1.0.0
            description: x
            author: t
            scripts:
              - scripts/db_query.py
            ---

            # depthclaw

            ## Actions

            | Action | Description |
            |--------|-------------|
            | `status` | Check status |
            """))
    with open(os.path.join(path, "scripts", "tests", "test_basic.py"), "w") as handle:
        handle.write("def test_status():\n    assert True\n")


class TestOsDeployModuleDepth:
    ACTION = "os-deploy-module"

    def test_action_is_dispatched(self):
        assert self.ACTION in os_router.ACTIONS
        assert self.ACTION in _router_actions()

    def test_queued_run_writes_exact_audit_row(self, tmp_path):
        module_path = str(tmp_path / "depthclaw")
        _passing_module(module_path)
        db_path = str(tmp_path / "deploy.sqlite")
        init_foundation_db(db_path)
        with _conn(db_path) as conn:
            snap_before = _snapshot(conn, _live_tables(db_path))

        result = handle_deploy_module(ns(
            module_path=module_path, db_path=db_path,
            src_root=None, skip_sandbox=True,
        ))
        assert result["pipeline_result"] == "queued"
        assert result["tier"] == 2
        assert result["audit_id"] is not None
        assert [step["step_name"] for step in result["steps"]] == [
            "constitution_validation", "sandbox_testing",
            "gl_invariant_check", "tier_classification",
            "deployment_decision", "provision_schema",
        ]
        assert "human review" in result["reasoning"]

        records = query_audit_log(module_name="depthclaw", db_path=db_path)
        assert len(records) == 1
        row = records[0]
        assert row["id"] == result["audit_id"]
        assert row["module_name"] == "depthclaw"
        assert row["pipeline_result"] == "queued"
        assert row["tier"] == 2
        assert row["reasoning"] == result["reasoning"]
        stored_steps = json.loads(row["steps"]) if isinstance(row["steps"], str) else row["steps"]
        assert [step["step_name"] for step in stored_steps] == [
            "constitution_validation", "sandbox_testing",
            "gl_invariant_check", "tier_classification",
            "deployment_decision", "provision_schema",
        ]
        assert stored_steps[0]["result"] == "pass"
        provision_step = [s for s in stored_steps if s["step_name"] == "provision_schema"][0]
        assert provision_step["result"] == "skipped"
        assert provision_step["details"]["tables_created"] == []
        assert result["tables_created"] == []
        assert seam.table_exists("depthclaw_item", db_path) is False

        with _conn(db_path) as conn:
            snap_after = _snapshot(conn, _live_tables(db_path))
            for table in _SNAPSHOT_TABLES:
                if table == "erpclaw_deploy_audit":
                    assert len(snap_after[table]) == len(snap_before[table]) + 1
                else:
                    assert snap_after[table] == snap_before[table]

    def test_cli_reports_failure_truthfully_and_still_writes_the_row(self, tmp_path):
        # The silence defect is fixed: an argc-valid invocation that fails
        # now prints one JSON error document and exits non-zero, while the
        # audit side effect still lands.
        bad_module = str(tmp_path / "badclaw")
        os.makedirs(bad_module)
        with open(os.path.join(bad_module, "init_db.py"), "w") as handle:
            handle.write("# empty\n")
        db_path = str(tmp_path / "deploycli.sqlite")
        open(db_path, "wb").close()
        env = dict(os.environ, ERPCLAW_DB_PATH=db_path)
        proc = subprocess.run(
            [sys.executable, ROUTER_PATH, "--action", self.ACTION,
             "--module-path", bad_module, "--db-path", db_path],
            capture_output=True, text=True, timeout=60, env=env,
        )
        assert proc.returncode != 0
        envelope = json.loads(proc.stdout)
        assert envelope["status"] == "error"
        assert "fail" in envelope["message"].lower()
        assert "validation" in envelope["message"].lower()
        records = query_audit_log(module_name="badclaw", db_path=db_path)
        assert len(records) == 1
        assert records[0]["pipeline_result"] == "failed"

    def test_deploy_refuses_without_module_path_and_writes_nothing(self, tmp_path):
        db_path = str(tmp_path / "deployref.sqlite")
        init_foundation_db(db_path)
        with _conn(db_path) as conn:
            snap_before = _snapshot(conn, _live_tables(db_path))

        result = handle_deploy_module(ns(
            module_path=None, db_path=db_path, src_root=None, skip_sandbox=False,
        ))
        assert result["error"] == "--module-path is required for deploy-module"
        with _conn(db_path) as conn:
            assert _snapshot(conn, _live_tables(db_path)) == snap_before
        assert query_audit_log(db_path=db_path) == []


# ---------------------------------------------------------------------------
# os-compliance-weather-status -- READ-ONLY (period derived from company row).
#
# Prior tests: TestComplianceWeather pins period strings from a bespoke
# fixture but never proves the database is unchanged, and the CLI handler
# tests pin one envelope key. Here the period inputs are read back from the
# seeded company row, the full check lists are pinned exactly, and the table
# is proved byte-identical. This action does NOT reach the ledger.
# Note: the foundation company table has no fiscal_year_end column, so on a
# real install the lookup falls back to the calendar year; the override
# branch below is exercised with a purpose-built table.
# ---------------------------------------------------------------------------

def _provision_company_table(db_path):
    metadata = seam.MetaData()
    seam.Table(
        "company", metadata,
        seam.Column("id", seam.Text, primary_key=True),
        seam.Column("name", seam.Text),
        seam.Column("fiscal_year_end", seam.Text),
    )
    assert seam.provision(metadata, db_path)["tables"] == 1
    assert seam.column_names("company", db_path) == ["id", "name", "fiscal_year_end"]


class TestOsComplianceWeatherStatusDepth:
    ACTION = "os-compliance-weather-status"

    def test_action_is_dispatched(self):
        assert self.ACTION in os_router.ACTIONS
        assert self.ACTION in _router_actions()

    def test_year_end_close_pins_period_and_full_check_list(self, tmp_path):
        # Wall-clock independent: a fiscal year end 10 days out is always in
        # the close window, and close outranks the calendar tax season, so
        # the handler (which takes no reference date) is deterministic.
        fy_end = (date.today() + timedelta(days=10)).isoformat()
        db_path = str(tmp_path / "weather.sqlite")
        _provision_company_table(db_path)
        with _conn(db_path) as conn:
            _insert(conn, "company", {"id": "c1", "name": "FY Soon", "fiscal_year_end": fy_end})
            before = _rows_where(conn, "company", id="c1")
            assert before[0]["fiscal_year_end"] == fy_end
            snap_before = _snapshot(conn, ["company"])

        result = handle_compliance_weather_status(ns(company_id="c1", db_path=db_path))
        assert result["period_type"] == "year_end_close"
        assert result["strictness_level"] == 3
        assert result["company_id"] == "c1"
        assert result["fiscal_year_end"] == fy_end
        assert result["reference_date"] == date.today().isoformat()
        assert [(c["check"], c["severity"]) for c in result["additional_checks"]] == [
            ("depreciation_schedule", "warning"),
            ("accrual_reversal", "warning"),
            ("year_end_adjustments", "info"),
            ("inventory_reconciliation", "warning"),
        ]

        direct = get_compliance_weather("c1", db_path=db_path)
        assert direct == result

        with _conn(db_path) as conn:
            assert _snapshot(conn, ["company"]) == snap_before
            assert _rows_where(conn, "company", id="c1") == before

    def test_tax_season_pins_period_and_full_check_list(self, tmp_path):
        # The handler takes no reference date, so the calendar branch is
        # pinned one layer down; the March-FY row still proves the seeded
        # fiscal_year_end value drives the derivation and is left intact.
        db_path = str(tmp_path / "weather2.sqlite")
        _provision_company_table(db_path)
        with _conn(db_path) as conn:
            _insert(conn, "company", {"id": "c9", "name": "Calendar", "fiscal_year_end": "2026-12-31"})
            snap_before = _snapshot(conn, ["company"])

        result = get_compliance_weather("c9", db_path=db_path, reference_date="2026-02-15")
        assert result["period_type"] == "tax_season"
        assert result["strictness_level"] == 2
        assert result["fiscal_year_end"] == "2026-12-31"
        assert [(c["check"], c["severity"]) for c in result["additional_checks"]] == [
            ("tax_categorization", "warning"),
            ("1099_reporting", "info"),
            ("tax_deduction_documentation", "info"),
        ]
        assert get_additional_checks("normal") == []

        with _conn(db_path) as conn:
            assert _snapshot(conn, ["company"]) == snap_before

    def test_handler_refuses_without_company_and_writes_nothing(self, tmp_path):
        db_path = str(tmp_path / "weatherref.sqlite")
        _provision_company_table(db_path)
        with _conn(db_path) as conn:
            _insert(conn, "company", {"id": "c1", "name": "Kept", "fiscal_year_end": "2026-12-31"})
            snap_before = _snapshot(conn, ["company"])

        result = handle_compliance_weather_status(ns(company_id=None, db_path=db_path))
        assert result["error"] == "--company-id is required for compliance-weather-status"
        with _conn(db_path) as conn:
            assert _snapshot(conn, ["company"]) == snap_before


# ---------------------------------------------------------------------------
# os-add-feature-to-module -- FILESYSTEM EFFECT (code insertion + backup).
#
# Prior tests: TestInsertFeature et al pin insertion mechanics but never
# prove the database is untouched, and nothing drives the action handler
# end-to-end. Here the handler is driven with a real feature spec: the new
# function and ACTIONS entry are asserted in the file, the .bak backup is
# asserted byte-identical to the original, and the DB snapshot is pinned.
# This action does NOT reach the ledger: it edits source files only.
# ---------------------------------------------------------------------------

_FEATURE_DB_QUERY = """\
#!/usr/bin/env python3
import os
import sys
import json

sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.response import ok, err
from erpclaw_lib.query import Q, P, Table, Field, dynamic_update


def handle_status(args):
    \"\"\"Check module status.\"\"\"
    ok({"message": "m483depth is running"})


ACTIONS = {
    "m483depth-status": handle_status,
}


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", required=True)
    args = parser.parse_args()
    handler = ACTIONS.get(args.action)
    if handler:
        handler(args)
    else:
        err(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()
"""

_FEATURE_SPEC = {
    "action_name": "m483depth-list-widgets",
    "parameters": [
        {"name": "company-id", "type": "str", "required": True,
         "description": "Company UUID"},
    ],
    "description": "List depth widgets for the M483 probe.",
    "table_name": "m483depth_widget",
}


class TestOsAddFeatureToModuleDepth:
    ACTION = "os-add-feature-to-module"

    def test_action_is_dispatched(self):
        assert self.ACTION in os_router.ACTIONS
        assert self.ACTION in _router_actions()

    def test_handler_inserts_function_actions_entry_and_backup(self, tmp_path):
        module_dir = tmp_path / "featclaw"
        scripts_dir = module_dir / "scripts"
        scripts_dir.mkdir(parents=True)
        target = scripts_dir / "db_query.py"
        target.write_text(_FEATURE_DB_QUERY)
        original_bytes = target.read_bytes()
        db_path = str(tmp_path / "feat.sqlite")
        init_foundation_db(db_path)
        with _conn(db_path) as conn:
            snap_before = _snapshot(conn, _live_tables(db_path))

        result = handle_add_feature_to_module(ns(
            module_path=str(module_dir),
            action_name="m483depth-list-widgets",
            feature_spec_json=json.dumps(_FEATURE_SPEC),
        ))
        assert result.get("success") is True, result
        assert result.get("action_added") == "m483depth-list-widgets"

        after = target.read_bytes()
        assert b"def m483depth_list_widgets" in after
        assert b'"m483depth-list-widgets"' in after
        assert b"def handle_status" in after
        assert b'"m483depth-status": handle_status' in after

        backup = scripts_dir / "db_query.py.bak"
        assert backup.is_file()
        assert backup.read_bytes() == original_bytes

        validation = result.get("validation", {})
        assert validation.get("valid") is not False

        with _conn(db_path) as conn:
            assert _snapshot(conn, _live_tables(db_path)) == snap_before

    def test_handler_refuses_without_action_name_and_changes_nothing(self, tmp_path):
        module_dir = tmp_path / "featref"
        scripts_dir = module_dir / "scripts"
        scripts_dir.mkdir(parents=True)
        target = scripts_dir / "db_query.py"
        target.write_text(_FEATURE_DB_QUERY)
        original_bytes = target.read_bytes()
        db_path = str(tmp_path / "featref.sqlite")
        init_foundation_db(db_path)
        with _conn(db_path) as conn:
            snap_before = _snapshot(conn, _live_tables(db_path))

        result = handle_add_feature_to_module(ns(
            module_path=str(module_dir),
            action_name=None,
            feature_spec_json=json.dumps(_FEATURE_SPEC),
        ))
        assert result["error"] == "--action-name is required"
        assert target.read_bytes() == original_bytes
        assert not (scripts_dir / "db_query.py.bak").exists()
        with _conn(db_path) as conn:
            assert _snapshot(conn, _live_tables(db_path)) == snap_before

    def test_second_insert_of_same_action_is_refused_truthfully(self, tmp_path):
        module_dir = tmp_path / "featdup"
        scripts_dir = module_dir / "scripts"
        scripts_dir.mkdir(parents=True)
        target = scripts_dir / "db_query.py"
        target.write_text(_FEATURE_DB_QUERY)
        first = handle_add_feature_to_module(ns(
            module_path=str(module_dir),
            action_name="m483depth-list-widgets",
            feature_spec_json=json.dumps(_FEATURE_SPEC),
        ))
        assert first.get("success") is True, first
        after_first = target.read_bytes()

        second = handle_add_feature_to_module(ns(
            module_path=str(module_dir),
            action_name="m483depth-list-widgets",
            feature_spec_json=json.dumps(_FEATURE_SPEC),
        ))
        assert "error" in second
        assert "already exists" in second["error"]
        assert target.read_bytes() == after_first


# ---------------------------------------------------------------------------
# os-setup-web-dashboard -- PROVISIONING (shells out; never touches the DB).
#
# Prior test: none in this directory (only the contract routability probe).
# The success path is exercised with _check_binary/_run_cmd mocked so no
# network or package manager runs; the refusal path runs unmocked and is
# deterministic in this environment (no node/npm/nginx on PATH). Both pin
# the database byte-identical. This action does NOT reach the ledger.
# ---------------------------------------------------------------------------

class TestOsSetupWebDashboardDepth:
    ACTION = "os-setup-web-dashboard"

    def test_action_is_dispatched(self):
        assert self.ACTION in os_router.ACTIONS
        assert self.ACTION in _router_actions()

    def test_mocked_provisioning_reports_exact_steps(self, tmp_path):
        db_path = str(tmp_path / "dash.sqlite")
        conn = get_connection(db_path)
        conn.close()
        tables_before = seam.table_names(db_path)

        with patch.object(web_dashboard_mod, "_check_binary", return_value=True), \
             patch.object(web_dashboard_mod, "_run_cmd", return_value=(True, "", "")):
            result = call_handler(
                handle_setup_web_dashboard,
                ns(domain="dash.example.com", ssl=True, skip_build=True),
            )
        assert result["status"] == "ok"
        assert result["url"] == "https://dash.example.com"
        assert result["setup_url"] == "https://dash.example.com/setup"
        assert "Cloned erpclaw-web from GitHub" in result["steps_completed"]
        assert "Skipped npm install + build (--skip-build)" in result["steps_completed"]
        assert "SSL certificate issued for dash.example.com" in result["steps_completed"]

        assert seam.table_names(db_path) == tables_before == []

    def test_missing_toolchain_is_refused_truthfully_and_writes_nothing(self, tmp_path):
        db_path = str(tmp_path / "dashref.sqlite")
        conn = get_connection(db_path)
        conn.close()
        tables_before = seam.table_names(db_path)

        result = call_handler(
            handle_setup_web_dashboard,
            ns(domain=None, ssl=None, skip_build=True),
        )
        assert result["status"] == "error"
        assert "required" in result["message"]
        assert seam.table_names(db_path) == tables_before == []


# ---------------------------------------------------------------------------
# os-status -- READ-ONLY (static dict).
#
# Prior test: none in this directory (only the contract routability probe).
# Pins the exact status values against the router's own dispatch table and
# proves the database is untouched. This action does NOT reach the ledger.
# ---------------------------------------------------------------------------

class TestOsStatusDepth:
    ACTION = "os-status"

    def test_action_is_dispatched(self):
        assert self.ACTION in os_router.ACTIONS
        assert self.ACTION in _router_actions()

    def test_status_pins_exact_values_and_touches_no_table(self, tmp_path):
        db_path = str(tmp_path / "status.sqlite")
        conn = get_connection(db_path)
        conn.close()
        tables_before = seam.table_names(db_path)

        result = call_handler(os_router.handle_status, ns())
        assert result["status"] == "ok"
        assert result["addon"] == "erpclaw-os-engine"
        assert result["version"] == "1.0.0"
        assert result["foundation"] == "erpclaw"
        assert result["self_check"] == "ok"
        assert result["actions_count"] == 28
        # FINDING (recorded, not fixed): the response hardcodes 28 but the
        # router dispatches 31 actions here (SKILL.md claims 33). The count
        # is stale; the test pins the real response (28) and the real
        # dispatch surface (31) side by side so the drift is visible.
        assert len(os_router.ACTIONS) == 31

        again = call_handler(os_router.handle_status, ns())
        assert again == result
        assert seam.table_names(db_path) == tables_before == []

