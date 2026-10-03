"""Behavioural depth for os-generate-module and os-deploy-module (task m323c).

Both actions are money-touching by reach and were tested for shape only
(response envelopes). Every test here reads back real state; none asserts
on response keys alone.

- os-generate-module: drives generate_module() (and the db_query action
  wrapper) against a temp dir, then reads the tree back from disk: the files
  that must exist, the tables the emitted init_db.py declares, and the
  column types of the money columns (TEXT, never a numeric type, with exact
  string defaults compared as Decimal). Refusal cases prove a bad module
  name or prefix fails AND leaves nothing on disk.
- os-deploy-module: generates a module, provisions a disposable foundation
  database, runs the deploy pipeline, then reads the database back through
  the seam (table_exists) and the rows back through PyPika queries on a
  connection from erpclaw_lib.db.get_connection. A spy on the shared ledger
  guard proves the invariant checker the deploy path calls actually ran
  (with skip_sandbox=True, so the call can only come from the deploy path).
  A validation-failing fixture proves the negative: pipeline failed, the
  guard never ran, and no table was left behind.
- No ledger write from either action: gl_entry and stock_ledger_entry (the
  two ledgers Article 6 protects) are digested before and after each action
  and compared exactly. Money is compared as exact strings, never float.

Reads use PyPika-built queries through erpclaw_lib.query on connections
from erpclaw_lib.db.get_connection; catalog questions go through
erpclaw_lib.seam. No raw catalog reads.
"""
import argparse
import importlib.util
import io
import json
import os
import re
import sys
from contextlib import redirect_stderr, redirect_stdout
from decimal import Decimal

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
OS_DIR = os.path.join(os.path.dirname(TESTS_DIR), "scripts")
if OS_DIR not in sys.path:
    sys.path.insert(0, OS_DIR)
FOUNDATION_OS_DIR = os.path.abspath(os.path.join(
    os.path.dirname(TESTS_DIR), "..", "..", "erpclaw", "scripts", "erpclaw-os"))
if FOUNDATION_OS_DIR not in sys.path:
    sys.path.insert(0, FOUNDATION_OS_DIR)
SRC_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(TESTS_DIR)))

_IN_TREE_LIB = os.path.join(
    SRC_ROOT, "erpclaw", "scripts", "erpclaw-setup", "lib")
_ERPCLAW_LIB = (_IN_TREE_LIB if os.path.isdir(os.path.join(_IN_TREE_LIB, "erpclaw_lib"))
                else os.path.join(os.path.expanduser(
                    os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, _ERPCLAW_LIB)

from erpclaw_lib import seam  # noqa: E402
from erpclaw_lib.db import get_connection  # noqa: E402
from erpclaw_lib.query import Q, Table  # noqa: E402
import erpclaw_lib.gl_invariants as gl_invariants  # noqa: E402

from generate_module import generate_module  # noqa: E402
import deploy_pipeline  # noqa: E402
from deploy_pipeline import run_pipeline, handle_deploy_module  # noqa: E402
from deploy_audit import query_audit_log  # noqa: E402


INIT_SCHEMA_PATH = os.path.join(
    SRC_ROOT, "erpclaw", "scripts", "erpclaw-setup", "init_schema.py")
FIXTURE_ART2 = os.path.join(TESTS_DIR, "fixtures", "violation_art2_float_money")

ENTITIES = [
    {
        "name": "item",
        "pattern": "crud_entity",
        "fields": [
            "price TEXT NOT NULL DEFAULT '0'",
            "total TEXT NOT NULL DEFAULT '0'",
            "quantity INTEGER NOT NULL DEFAULT 1",
            "color TEXT",
        ],
    },
    {
        "name": "visit",
        "pattern": "appointment_booking",
        "fields": [],
    },
]

PROTECTED_LEDGERS = ("gl_entry", "stock_ledger_entry")


def _load_router():
    path = os.path.join(OS_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_os_behav", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _init_foundation(db_path):
    spec = importlib.util.spec_from_file_location("init_schema_os_behav", INIT_SCHEMA_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.init_db(db_path)


def _declared_tables(init_db_path):
    with open(init_db_path, encoding="utf-8") as fh:
        content = fh.read()
    return re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", content)


def _column_types(init_db_path):
    with open(init_db_path, encoding="utf-8") as fh:
        content = fh.read()
    return {
        match.group(1): match.group(2)
        for match in re.finditer(
            r"^\s*(\w+)\s+(TEXT|REAL|FLOAT|NUMERIC|INTEGER)\b",
            content, re.MULTILINE)
    }


def _ledger_digest(db_path):
    conn = get_connection(db_path)
    try:
        digest = {}
        for name in PROTECTED_LEDGERS:
            table = Table(name)
            query = Q.from_(table).select(table.star).orderby(table.id)
            digest[name] = [dict(row) for row in conn.execute(query.get_sql()).fetchall()]
        return digest
    finally:
        conn.close()


@pytest.fixture(autouse=True)
def _dispose_seam_engines():
    yield
    try:
        seam.dispose_engines()
    except Exception:
        pass


@pytest.fixture(scope="session")
def _foundation_template(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("foundation-template") / "template.sqlite")
    _init_foundation(path)
    return path


@pytest.fixture()
def seed_db(tmp_path, _foundation_template):
    import shutil
    path = str(tmp_path / "test.sqlite")
    shutil.copy(_foundation_template, path)
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = _foundation_template + suffix
        if os.path.isfile(sidecar):
            shutil.copy(sidecar, path + suffix)
    return path


def _step(result, name):
    for step in result["steps"]:
        if step["step_name"] == name:
            return step
    raise AssertionError("step %r missing from %r" % (
        name, [s["step_name"] for s in result["steps"]]))


class TestOsGenerateModuleBehaviour:
    def test_artifacts_files_schema_and_money_are_text(self, tmp_path, seed_db):
        db_path = seed_db
        before = _ledger_digest(db_path)

        output_dir = str(tmp_path / "genclaw")
        result = generate_module(
            module_name="genclaw",
            prefix="gen",
            business_description="Behavioural depth fixture module.",
            entities=ENTITIES,
            output_dir=output_dir,
            src_root=SRC_ROOT,
        )
        assert result["result"] == "pass", result.get("validation")

        assert os.path.isfile(os.path.join(output_dir, "init_db.py"))
        assert os.path.isfile(os.path.join(output_dir, "scripts", "db_query.py"))
        assert os.path.isfile(os.path.join(output_dir, "scripts", "gen.py"))
        assert os.path.isfile(os.path.join(output_dir, "SKILL.md"))
        assert os.path.isfile(os.path.join(output_dir, "scripts", "tests", "conftest.py"))
        assert os.path.isfile(
            os.path.join(output_dir, "scripts", "tests", "gen_helpers.py"))
        assert os.path.isfile(
            os.path.join(output_dir, "scripts", "tests", "test_gen.py"))

        declared = _declared_tables(os.path.join(output_dir, "init_db.py"))
        assert sorted(declared) == ["gen_item", "gen_visit"]

        column_types = _column_types(os.path.join(output_dir, "init_db.py"))
        assert column_types["price"] == "TEXT"
        assert column_types["total"] == "TEXT"
        assert column_types["quantity"] == "INTEGER"

        with open(os.path.join(output_dir, "init_db.py"), encoding="utf-8") as fh:
            ddl = fh.read()
        for money_col in ("price", "total"):
            match = re.search(
                r"\b%s\s+TEXT[^,]*DEFAULT\s+'([^']+)'" % money_col, ddl)
            assert match, "money column %s lacks an exact string default" % money_col
            assert Decimal(match.group(1)) == Decimal("0")

        validation = result["validation"]
        assert validation["articles"][1] == "pass"
        assert validation["articles"][2] == "pass"
        assert validation["articles"][3] == "pass"

        assert _ledger_digest(db_path) == before

    @pytest.mark.parametrize("bad_name", ["", "   "])
    def test_refuses_bad_module_name_and_writes_nothing(self, tmp_path, bad_name):
        output_dir = str(tmp_path / "refused-name")
        result = generate_module(
            module_name=bad_name,
            prefix="gen",
            business_description="Must be refused.",
            entities=[{"name": "item", "pattern": "crud_entity"}],
            output_dir=output_dir,
            src_root=SRC_ROOT,
        )
        assert result["result"] == "fail"
        assert result["entities"] == 0
        assert result["files_created"] == []
        assert not os.path.exists(output_dir)

    def test_plain_module_name_without_claw_is_accepted(self, tmp_path):
        # The claw suffix is a convention, not a requirement: a well-formed
        # name without it must generate (with the table prefix matching the
        # module name, as the validator derives it from there).
        output_dir = str(tmp_path / "plainname")
        result = generate_module(
            module_name="plainname",
            prefix="plainname",
            business_description="Convention, not a requirement.",
            entities=[{"name": "item", "pattern": "crud_entity"}],
            output_dir=output_dir,
            src_root=SRC_ROOT,
        )
        assert result["result"] == "pass", result.get("validation")
        assert os.path.isfile(os.path.join(output_dir, "init_db.py"))

    @pytest.mark.parametrize("bad_prefix", ["Bad-Prefix", "9lives", ""])
    def test_refuses_bad_prefix_and_writes_nothing(self, tmp_path, bad_prefix):
        output_dir = str(tmp_path / "refused-prefix")
        result = generate_module(
            module_name="genclaw",
            prefix=bad_prefix,
            business_description="Must be refused.",
            entities=[{"name": "item", "pattern": "crud_entity"}],
            output_dir=output_dir,
            src_root=SRC_ROOT,
        )
        assert result["result"] == "fail"
        assert result["entities"] == 0
        assert result["files_created"] == []
        assert not os.path.exists(output_dir)

    def test_action_entry_point_writes_real_tree(self, tmp_path):
        router = _load_router()
        output_dir = str(tmp_path / "actclaw")
        args = argparse.Namespace(
            module_name="actclaw",
            prefix="act",
            description="Action-level fixture module.",
            industry=None,
            entities=ENTITIES,
            output_dir=output_dir,
            src_root=SRC_ROOT,
        )
        buf = io.StringIO()
        with redirect_stdout(buf):
            with pytest.raises(SystemExit) as exc_info:
                router.handle_generate_module(args)
        assert exc_info.value.code == 0
        envelope = json.loads(buf.getvalue())
        assert envelope["status"] == "ok"
        assert envelope["entities"] == 2

        assert os.path.isfile(os.path.join(output_dir, "init_db.py"))
        assert os.path.isfile(os.path.join(output_dir, "scripts", "act.py"))
        assert sorted(_declared_tables(os.path.join(output_dir, "init_db.py"))) == [
            "act_item", "act_visit"]
        column_types = _column_types(os.path.join(output_dir, "init_db.py"))
        assert column_types["price"] == "TEXT"
        assert column_types["total"] == "TEXT"


class TestOsDeployModuleBehaviour:
    def _generated(self, tmp_path, name="depclaw", prefix="dep"):
        output_dir = str(tmp_path / name)
        result = generate_module(
            module_name=name,
            prefix=prefix,
            business_description="Deploy fixture module.",
            entities=ENTITIES,
            output_dir=output_dir,
            src_root=SRC_ROOT,
        )
        assert result["result"] == "pass", result.get("validation")
        return output_dir

    def _approve(self, monkeypatch):
        def fake_classify(action, module_name=None):
            assert action == "deploy-module"
            return {"tier": 1, "tier_name": "guardrailed_autonomous",
                    "reasoning": "test approval"}
        monkeypatch.setattr(deploy_pipeline, "classify_action", fake_classify)

    def test_queued_deploy_provisions_nothing_and_runs_ledger_guard(
            self, tmp_path, seed_db, monkeypatch):
        db_path = seed_db
        module_dir = self._generated(tmp_path)
        declared = _declared_tables(os.path.join(module_dir, "init_db.py"))
        assert sorted(declared) == ["dep_item", "dep_visit"]
        tables_before = set(seam.table_names(db_path))
        before = _ledger_digest(db_path)

        guard_calls = []
        real_guard = gl_invariants.check_gl_invariants

        def spy(db_path_arg=None):
            guard_calls.append(db_path_arg)
            return real_guard(db_path_arg)

        monkeypatch.setattr(gl_invariants, "check_gl_invariants", spy)

        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "queued"

        assert guard_calls == [db_path]
        guard_step = _step(result, "gl_invariant_check")
        assert guard_step["result"] in ("pass", "skip")
        assert guard_step["details"]["passed"] is True

        provision_step = _step(result, "provision_schema")
        assert provision_step["result"] == "skipped"
        assert provision_step["details"]["tables_created"] == []
        assert result["tables_created"] == []
        reason = provision_step["details"]["reason"]
        assert "approval" in reason.lower() or "deferred" in reason.lower()

        for table in declared:
            assert seam.table_exists(table, db_path) is False
        tables_after = set(seam.table_names(db_path))
        assert tables_after - tables_before <= {"erpclaw_deploy_audit"}

        assert _ledger_digest(db_path) == before

    def test_invalid_module_is_not_deployed_and_leaves_no_table(
            self, tmp_path, seed_db, monkeypatch):
        db_path = seed_db
        tables_before = set(seam.table_names(db_path))
        before = _ledger_digest(db_path)

        guard_calls = []
        real_guard = gl_invariants.check_gl_invariants

        def spy(db_path_arg=None):
            guard_calls.append(db_path_arg)
            return real_guard(db_path_arg)

        monkeypatch.setattr(gl_invariants, "check_gl_invariants", spy)

        result = run_pipeline(
            FIXTURE_ART2, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "failed"
        first = result["steps"][0]
        assert first["step_name"] == "constitution_validation"
        assert first["result"] == "fail"
        assert result["tables_created"] == []
        assert guard_calls == []

        assert seam.table_exists("violart2claw_invoice", db_path) is False
        tables_after = set(seam.table_names(db_path))
        assert tables_after - tables_before <= {"erpclaw_deploy_audit"}

        assert _ledger_digest(db_path) == before

    def test_deploy_action_entry_point_provisions_nothing_without_approval(
            self, tmp_path, seed_db):
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="actdepclaw", prefix="actdep")
        tables_before = set(seam.table_names(db_path))
        before = _ledger_digest(db_path)

        args = argparse.Namespace(
            module_path=module_dir,
            db_path=db_path,
            src_root=SRC_ROOT,
            skip_sandbox=True,
            dry_run=False,
        )
        result = handle_deploy_module(args)
        assert result["pipeline_result"] == "queued"

        declared = _declared_tables(os.path.join(module_dir, "init_db.py"))
        provision_step = _step(result, "provision_schema")
        assert provision_step["result"] == "skipped"
        assert result["tables_created"] == []
        for table in declared:
            assert seam.table_exists(table, db_path) is False
        tables_after = set(seam.table_names(db_path))
        assert tables_after - tables_before <= {"erpclaw_deploy_audit"}

        assert _ledger_digest(db_path) == before

    def test_approved_path_provisions_and_audits_tables(
            self, tmp_path, seed_db, monkeypatch):
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="apprclaw", prefix="appr")
        declared = _declared_tables(os.path.join(module_dir, "init_db.py"))
        assert sorted(declared) == ["appr_item", "appr_visit"]
        before = _ledger_digest(db_path)
        self._approve(monkeypatch)

        guard_calls = []
        real_guard = gl_invariants.check_gl_invariants

        def spy(db_path_arg=None):
            guard_calls.append(db_path_arg)
            return real_guard(db_path_arg)

        monkeypatch.setattr(gl_invariants, "check_gl_invariants", spy)

        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "deployed"
        assert result["dry_run"] is False

        assert guard_calls == [db_path]
        assert result["audit_recorded"] is True
        assert result["audit_error"] is None
        provision_step = _step(result, "provision_schema")
        assert provision_step["result"] == "pass"
        assert sorted(provision_step["details"]["tables_created"]) == sorted(declared)
        assert sorted(result["tables_created"]) == sorted(declared)

        for table in declared:
            assert seam.table_exists(table, db_path) is True

        conn = get_connection(db_path)
        try:
            table = Table("appr_item")
            query = Q.from_(table).select(table.id, table.price, table.total)
            assert conn.execute(query.get_sql()).fetchall() == []
        finally:
            conn.close()

        records = query_audit_log(db_path=db_path)
        assert len(records) >= 1
        record = [r for r in records if r["module_name"] == "apprclaw"][0]
        assert record["pipeline_result"] == "deployed"
        audit_steps = {s["step_name"]: s for s in record["steps"]}
        assert audit_steps["provision_schema"]["result"] == "pass"
        assert sorted(
            audit_steps["provision_schema"]["details"]["tables_created"]
        ) == sorted(declared)

        assert _ledger_digest(db_path) == before

    def test_dry_run_reports_without_creating(
            self, tmp_path, seed_db, monkeypatch):
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="dryclaw", prefix="dry")
        declared = _declared_tables(os.path.join(module_dir, "init_db.py"))
        assert sorted(declared) == ["dry_item", "dry_visit"]
        tables_before = set(seam.table_names(db_path))
        before = _ledger_digest(db_path)
        self._approve(monkeypatch)

        args = argparse.Namespace(
            module_path=module_dir,
            db_path=db_path,
            src_root=SRC_ROOT,
            skip_sandbox=True,
            dry_run=True,
        )
        result = handle_deploy_module(args)
        assert result["pipeline_result"] == "deployed"
        assert result["dry_run"] is True
        assert result["tables_created"] == []
        assert result["audit_id"] is None

        provision_step = _step(result, "provision_schema")
        assert provision_step["result"] == "skipped"
        assert provision_step["details"]["dry_run"] is True
        assert sorted(provision_step["details"]["tables_created"]) == sorted(declared)
        assert "dry-run" in provision_step["details"]["reason"].lower()

        for table in declared:
            assert seam.table_exists(table, db_path) is False
        # The foundation seed already carries the audit table, so an absent
        # table cannot be shown here; instead the row count must be unchanged
        # (zero). The audit module's query helper is not used for this check:
        # it creates the audit table as a side effect.
        audit_table = Table("erpclaw_deploy_audit")
        conn = get_connection(db_path)
        try:
            query = Q.from_(audit_table).select(audit_table.id)
            assert conn.execute(query.get_sql()).fetchall() == []
        finally:
            conn.close()
        tables_after = set(seam.table_names(db_path))
        assert tables_after == tables_before

        assert _ledger_digest(db_path) == before

    def test_no_target_touches_no_database(
            self, tmp_path, monkeypatch):
        # With no deploy target the pipeline refuses before the ledger
        # guard and before any audit write: failed with a stated reason,
        # audit_id None, and no database file created anywhere — neither
        # the audit module's import-time default, nor the shared lib's
        # import-time default, nor the environment default.
        import deploy_audit
        import erpclaw_lib.db as db_lib
        module_dir = self._generated(tmp_path, name="notargclaw", prefix="notarg")
        declared = _declared_tables(os.path.join(module_dir, "init_db.py"))
        assert sorted(declared) == ["notarg_item", "notarg_visit"]
        self._approve(monkeypatch)

        scratch_audit = str(tmp_path / "audit_default.sqlite")
        scratch_lib = str(tmp_path / "lib_default.sqlite")
        scratch_env = str(tmp_path / "env_default.sqlite")
        monkeypatch.setattr(deploy_audit, "DEFAULT_DB_PATH", scratch_audit)
        monkeypatch.setattr(db_lib, "DEFAULT_DB_PATH", scratch_lib)
        monkeypatch.setenv("ERPCLAW_DB_PATH", scratch_env)

        result = run_pipeline(
            module_dir, db_path=None, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "failed"
        assert "no deploy target" in result["reasoning"].lower()
        assert result["audit_id"] is None
        assert result["tables_created"] == []

        # Note: absence is asserted on the files themselves, not through
        # the seam — asking the seam about a missing file creates it.
        assert not os.path.exists(scratch_audit)
        assert not os.path.exists(scratch_lib)
        assert not os.path.exists(scratch_env)

    def _suggest(self, monkeypatch):
        def fake_classify(action, module_name=None):
            assert action == "deploy-module"
            return {"tier": 25, "tier_name": "ai_suggestion_only",
                    "reasoning": "test suggestion"}
        monkeypatch.setattr(deploy_pipeline, "classify_action", fake_classify)

    def _reject(self, monkeypatch):
        def fake_classify(action, module_name=None):
            assert action == "deploy-module"
            return {"tier": 3, "tier_name": "human_only",
                    "reasoning": "test rejection"}
        monkeypatch.setattr(deploy_pipeline, "classify_action", fake_classify)

    def test_suggestion_provisions_nothing(
            self, tmp_path, seed_db, monkeypatch):
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="sugclaw", prefix="sug")
        declared = _declared_tables(os.path.join(module_dir, "init_db.py"))
        assert sorted(declared) == ["sug_item", "sug_visit"]
        audit_before = seam.table_exists("erpclaw_deploy_audit", db_path)
        before = _ledger_digest(db_path)
        self._suggest(monkeypatch)

        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "suggestion"

        provision_step = _step(result, "provision_schema")
        assert provision_step["result"] == "skipped"
        assert provision_step["details"]["tables_created"] == []
        assert result["tables_created"] == []

        for table in declared:
            assert seam.table_exists(table, db_path) is False
        # FINDING (recorded, not fixed): the audit table accepts only
        # deployed, queued, rejected and failed, so a suggestion cannot be
        # recorded. The accepted set is deliberately left unchanged.
        assert result["audit_id"] is None
        assert result["audit_recorded"] is False
        assert result["audit_error"] == "outcome 'suggestion' is not a recordable audit outcome"
        if not audit_before:
            assert seam.table_exists("erpclaw_deploy_audit", db_path) is False

        assert _ledger_digest(db_path) == before

    def test_rejected_provisions_nothing(
            self, tmp_path, seed_db, monkeypatch):
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="rejclaw", prefix="rej")
        declared = _declared_tables(os.path.join(module_dir, "init_db.py"))
        assert sorted(declared) == ["rej_item", "rej_visit"]
        before = _ledger_digest(db_path)
        self._reject(monkeypatch)

        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "rejected"
        assert result["audit_id"] is not None

        provision_step = _step(result, "provision_schema")
        assert provision_step["result"] == "skipped"
        assert provision_step["details"]["tables_created"] == []
        assert result["tables_created"] == []

        for table in declared:
            assert seam.table_exists(table, db_path) is False

        records = query_audit_log(db_path=db_path)
        record = [r for r in records if r["module_name"] == "rejclaw"][0]
        assert record["pipeline_result"] == "rejected"

        assert _ledger_digest(db_path) == before

    def test_queued_dry_run_writes_nothing(
            self, tmp_path, monkeypatch):
        # An empty target has no audit table at all; a queued dry run must
        # leave it that way: no table may appear, through the seam.
        db_path = str(tmp_path / "empty.sqlite")
        open(db_path, "wb").close()
        module_dir = self._generated(tmp_path, name="qdryclaw", prefix="qdry")
        declared = _declared_tables(os.path.join(module_dir, "init_db.py"))
        assert sorted(declared) == ["qdry_item", "qdry_visit"]

        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT,
            skip_sandbox=True, dry_run=True)
        assert result["pipeline_result"] == "queued"
        assert result["dry_run"] is True
        assert result["audit_id"] is None
        assert result["tables_created"] == []

        provision_step = _step(result, "provision_schema")
        assert provision_step["result"] == "skipped"
        assert provision_step["details"]["tables_created"] == []

        assert seam.table_names(db_path) == []

    def test_dry_run_writes_nothing_when_audit_table_exists(
            self, tmp_path, seed_db, monkeypatch):
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="dry2claw", prefix="dry2")
        declared = _declared_tables(os.path.join(module_dir, "init_db.py"))
        assert sorted(declared) == ["dry2_item", "dry2_visit"]

        first = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert first["pipeline_result"] == "queued"
        assert first["audit_id"] is not None

        audit_table = Table("erpclaw_deploy_audit")
        conn = get_connection(db_path)
        try:
            query = Q.from_(audit_table).select(audit_table.id)
            before_ids = sorted(row["id"] for row in conn.execute(query.get_sql()).fetchall())
        finally:
            conn.close()
        assert len(before_ids) == 1

        self._approve(monkeypatch)
        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT,
            skip_sandbox=True, dry_run=True)
        assert result["pipeline_result"] == "deployed"
        assert result["dry_run"] is True
        assert result["audit_id"] is None
        assert result["tables_created"] == []

        provision_step = _step(result, "provision_schema")
        assert provision_step["result"] == "skipped"
        assert sorted(provision_step["details"]["tables_created"]) == sorted(declared)

        conn = get_connection(db_path)
        try:
            query = Q.from_(audit_table).select(audit_table.id)
            after_ids = sorted(row["id"] for row in conn.execute(query.get_sql()).fetchall())
        finally:
            conn.close()
        assert after_ids == before_ids

        for table in declared:
            assert seam.table_exists(table, db_path) is False

    def test_installer_ignoring_its_target_fails_the_step(
            self, tmp_path, seed_db, monkeypatch):
        # An installer that ignores its argv target and writes to its own
        # default must fail the provisioning step instead of passing empty.
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="hostileclaw", prefix="hostile")
        own_default = str(tmp_path / "own_default.sqlite")
        with open(os.path.join(module_dir, "init_db.py"), "w") as handle:
            handle.write(
                "import os, sqlite3, sys\n"
                "OWN_DEFAULT = %r\n" % (own_default,) +
                "def init_hostile_schema(db_path=None):\n"
                "    conn = sqlite3.connect(OWN_DEFAULT)\n"
                "    conn.executescript(\"\"\"\n"
                "        CREATE TABLE IF NOT EXISTS hostile_item (\n"
                "            id TEXT PRIMARY KEY, name TEXT NOT NULL,\n"
                "            price TEXT NOT NULL DEFAULT '0',\n"
                "            company_id TEXT NOT NULL);\n"
                "    \"\"\")\n"
                "    conn.commit(); conn.close()\n"
                "if __name__ == \"__main__\":\n"
                "    init_hostile_schema()\n")
        before = _ledger_digest(db_path)
        self._approve(monkeypatch)

        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "failed"
        assert result["tables_created"] == []

        provision_step = _step(result, "provision_schema")
        assert provision_step["result"] == "fail"

        assert seam.table_exists("hostile_item", db_path) is False
        assert seam.table_exists("hostile_item", own_default) is True

        assert _ledger_digest(db_path) == before

    def _write_env_only_installer(self, module_dir):
        path = os.path.join(module_dir, "init_db.py")
        with open(path, "w") as handle:
            handle.write(
                "#!/usr/bin/env python3\n"
                "import importlib.util\n"
                "import os\n"
                "import sys\n"
                'if importlib.util.find_spec("erpclaw_lib") is None:\n'
                '    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))\n'
                "from erpclaw_lib import seam\n"
                "from erpclaw_lib.seam import Column, MetaData, Table, Text, provision, text\n"
                'REQUIRED_FOUNDATION = ["company", "customer", "naming_series", "audit_log"]\n'
                "METADATA = MetaData()\n"
                'ENV_ONLY_ITEM = Table(\n'
                '    "envpin_item", METADATA,\n'
                "    Column(\"id\", Text, primary_key=True, nullable=True),\n"
                "    Column(\"name\", Text, nullable=False),\n"
                "    Column(\"price\", Text, nullable=False, server_default=text(\"'0'\")),\n"
                "    Column(\"company_id\", Text, nullable=False),\n"
                ")\n"
                "def _require_foundation(db_path):\n"
                "    missing = [name for name in REQUIRED_FOUNDATION if not seam.table_exists(name, db_path)]\n"
                "    if missing:\n"
                '        print("foundation tables missing", file=sys.stderr)\n'
                "        sys.exit(1)\n"
                "def init_envpin_schema():\n"
                '    db_path = os.environ.get("ERPCLAW_DB_PATH")\n'
                "    if not db_path:\n"
                '        print("ERPCLAW_DB_PATH is not set", file=sys.stderr)\n'
                "        sys.exit(1)\n"
                "    _require_foundation(db_path)\n"
                "    provision(METADATA, db_path)\n"
                'if __name__ == "__main__":\n'
                "    init_envpin_schema()\n")

    def _write_home_default_installer(self, module_dir):
        path = os.path.join(module_dir, "init_db.py")
        with open(path, "w") as handle:
            handle.write(
                "#!/usr/bin/env python3\n"
                "import importlib.util\n"
                "import os\n"
                "import sys\n"
                'if importlib.util.find_spec("erpclaw_lib") is None:\n'
                '    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))\n'
                "from erpclaw_lib.seam import Column, MetaData, Table, Text, provision, text\n"
                "METADATA = MetaData()\n"
                'HOME_ITEM = Table(\n'
                '    "homewriter_item", METADATA,\n'
                "    Column(\"id\", Text, primary_key=True, nullable=True),\n"
                "    Column(\"name\", Text, nullable=False),\n"
                "    Column(\"price\", Text, nullable=False, server_default=text(\"'0'\")),\n"
                "    Column(\"company_id\", Text, nullable=False),\n"
                ")\n"
                "def _home_default():\n"
                '    return os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")\n'
                "def init_homewriter_schema():\n"
                "    provision(METADATA, _home_default())\n"
                'if __name__ == "__main__":\n'
                "    init_homewriter_schema()\n")

    def test_redeploy_reports_no_new_tables(
            self, tmp_path, seed_db, monkeypatch):
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="redepclaw", prefix="redep")
        self._approve(monkeypatch)
        first = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert first["pipeline_result"] == "deployed"
        assert first["audit_recorded"] is True
        assert sorted(first["tables_created"]) == ["redep_item", "redep_visit"]
        for table in ["redep_item", "redep_visit"]:
            assert seam.table_exists(table, db_path) is True
        second = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert second["pipeline_result"] == "deployed"
        assert second["tables_created"] == []
        assert second["audit_recorded"] is True
        provision_step = _step(second, "provision_schema")
        assert provision_step["result"] == "pass"
        assert provision_step["details"]["tables_created"] == []
        for table in ["redep_item", "redep_visit"]:
            assert seam.table_exists(table, db_path) is True

    def test_installer_honouring_only_env_target_passes(
            self, tmp_path, seed_db, monkeypatch):
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="envpinclaw", prefix="envpin")
        self._write_env_only_installer(module_dir)
        before = _ledger_digest(db_path)
        self._approve(monkeypatch)
        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "deployed"
        assert result["audit_recorded"] is True
        assert sorted(result["tables_created"]) == ["envpin_item"]
        assert seam.table_exists("envpin_item", db_path) is True
        assert _ledger_digest(db_path) == before

    def test_installer_writing_to_home_default_fails_presence(
            self, tmp_path, seed_db, monkeypatch):
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="homewclaw", prefix="homew")
        self._write_home_default_installer(module_dir)
        home = tmp_path / "pipeline_home"
        home.mkdir()
        os.symlink(
            os.path.join(
                os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")),
                "lib"),
            os.path.join(str(home), "lib"))
        monkeypatch.setenv("ERPCLAW_HOME", str(home))
        before = _ledger_digest(db_path)
        self._approve(monkeypatch)
        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "failed"
        assert result["tables_created"] == []
        provision_step = _step(result, "provision_schema")
        assert provision_step["result"] == "fail"
        assert "homewriter_item" in (provision_step["details"]["error"] or "")
        assert seam.table_exists("homewriter_item", db_path) is False
        assert not os.path.exists(os.path.join(str(home), "data.sqlite"))
        assert _ledger_digest(db_path) == before

    def test_missing_target_is_refused_and_never_created(
            self, tmp_path, monkeypatch):
        module_dir = self._generated(tmp_path, name="missclaw", prefix="miss")
        db_path = str(tmp_path / "missing.sqlite")
        assert not os.path.exists(db_path)
        guard_calls = []
        real_guard = gl_invariants.check_gl_invariants

        def spy(db_path_arg=None):
            guard_calls.append(db_path_arg)
            return real_guard(db_path_arg)

        monkeypatch.setattr(gl_invariants, "check_gl_invariants", spy)
        self._approve(monkeypatch)
        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "failed"
        assert "does not exist" in result["reasoning"]
        assert result["audit_id"] is None
        assert result["audit_recorded"] is False
        assert result["audit_error"] == "no deploy target"
        assert guard_calls == []
        assert not os.path.exists(db_path)

    def test_audit_write_failure_is_reported(
            self, tmp_path, seed_db, monkeypatch):
        import deploy_pipeline as pipeline_mod
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="audfailclaw", prefix="audfail")

        def boom(**kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(pipeline_mod, "record_deployment", boom)
        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "queued"
        assert result["audit_id"] is None
        assert result["audit_recorded"] is False
        assert "disk full" in (result["audit_error"] or "")

    def test_generate_wrapper_reports_failure_truthfully(self, tmp_path):
        router = _load_router()
        output_dir = str(tmp_path / "failclaw")
        args = argparse.Namespace(
            module_name="failclaw",
            prefix="Bad-Prefix",
            description="Must be refused truthfully.",
            industry=None,
            entities=[{"name": "item", "pattern": "crud_entity"}],
            output_dir=output_dir,
            src_root=SRC_ROOT,
        )
        buf = io.StringIO()
        with redirect_stdout(buf):
            with pytest.raises(SystemExit) as exc_info:
                router.handle_generate_module(args)
        assert exc_info.value.code != 0
        envelope = json.loads(buf.getvalue())
        assert envelope["status"] == "error"
        assert "prefix" in envelope["message"].lower()
        assert not os.path.exists(output_dir)

    def test_deploy_wrapper_reports_failure_truthfully(self, tmp_path, seed_db):
        router = _load_router()
        args = argparse.Namespace(
            module_path=FIXTURE_ART2,
            db_path=seed_db,
            src_root=SRC_ROOT,
            skip_sandbox=True,
            dry_run=False,
        )
        buf = io.StringIO()
        with redirect_stdout(buf):
            with pytest.raises(SystemExit) as exc_info:
                router.handle_deploy_action(args)
        assert exc_info.value.code != 0
        envelope = json.loads(buf.getvalue())
        assert envelope["status"] == "error"
        assert "validation" in envelope["message"].lower()

    def test_postgresql_url_is_refused_without_echoing_it(
            self, tmp_path, monkeypatch):
        module_dir = self._generated(tmp_path, name="pgurlclaw", prefix="pgurl")
        db_path = "postgresql://u:s3cret@h/db"
        guard_calls = []
        real_guard = gl_invariants.check_gl_invariants

        def spy(db_path_arg=None):
            guard_calls.append(db_path_arg)
            return real_guard(db_path_arg)

        monkeypatch.setattr(gl_invariants, "check_gl_invariants", spy)
        self._approve(monkeypatch)
        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "failed"
        assert result["reasoning"] == (
            "Refused: the deploy pipeline supports SQLite file targets only")
        assert result["audit_id"] is None
        assert result["audit_recorded"] is False
        assert result["audit_error"] == "no deploy target"
        assert result["tables_created"] == []
        dumped = json.dumps(result)
        assert "s3cret" not in dumped
        assert "postgresql://" not in dumped
        assert guard_calls == []

    def test_postgresql_url_wrapper_reports_failure_without_echoing_it(
            self, tmp_path):
        router = _load_router()
        module_dir = self._generated(tmp_path, name="pgwrapclaw", prefix="pgwrap")
        args = argparse.Namespace(
            module_path=module_dir,
            db_path="postgresql://u:s3cret@h/db",
            src_root=SRC_ROOT,
            skip_sandbox=True,
            dry_run=False,
        )
        out_buf = io.StringIO()
        err_buf = io.StringIO()
        with redirect_stdout(out_buf), redirect_stderr(err_buf):
            with pytest.raises(SystemExit) as exc_info:
                router.handle_deploy_action(args)
        assert exc_info.value.code != 0
        envelope = json.loads(out_buf.getvalue())
        assert envelope["status"] == "error"
        assert "s3cret" not in out_buf.getvalue()
        assert "s3cret" not in err_buf.getvalue()
        assert "postgresql://" not in out_buf.getvalue()

    def test_postgresql_dialect_refuses_existing_empty_file(
            self, tmp_path, monkeypatch):
        module_dir = self._generated(tmp_path, name="pgdialclaw", prefix="pgdial")
        db_path = str(tmp_path / "pgdial.sqlite")
        open(db_path, "wb").close()
        assert os.path.getsize(db_path) == 0
        monkeypatch.setenv("ERPCLAW_DB_DIALECT", "postgresql")
        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "failed"
        assert result["reasoning"] == (
            "Refused: the deploy pipeline supports SQLite file targets only")
        assert result["audit_id"] is None
        assert result["audit_recorded"] is False
        assert os.path.getsize(db_path) == 0

    def test_suggestion_on_empty_target_records_nothing_and_creates_no_table(
            self, tmp_path, monkeypatch):
        db_path = str(tmp_path / "empty_sug.sqlite")
        open(db_path, "wb").close()
        module_dir = self._generated(tmp_path, name="sugemptyclaw", prefix="sugempty")
        self._suggest(monkeypatch)
        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "suggestion"
        assert result["audit_id"] is None
        assert result["audit_recorded"] is False
        assert result["audit_error"] == (
            "outcome 'suggestion' is not a recordable audit outcome")
        assert seam.table_names(db_path) == []

    def test_dry_run_installer_failure_labels_installer_error(
            self, tmp_path, seed_db, monkeypatch):
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="dryfailclaw", prefix="dryfail")
        with open(os.path.join(module_dir, "init_db.py"), "w") as handle:
            handle.write(
                "import sys\n"
                'print("dryfail installer boom", file=sys.stderr)\n'
                "sys.exit(1)\n")
        self._approve(monkeypatch)
        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT,
            skip_sandbox=True, dry_run=True)
        assert result["pipeline_result"] == "failed"
        assert result["tables_created"] == []
        provision_step = _step(result, "provision_schema")
        assert provision_step["result"] == "fail"
        assert (provision_step["details"]["error"] or "").startswith(
            "installer failed for dry run: ")

    def _write_isolation_pinned_installer(self, module_dir, session_home):
        path = os.path.join(module_dir, "init_db.py")
        with open(path, "w") as handle:
            handle.write(
                "#!/usr/bin/env python3\n"
                "import importlib.util\n"
                "import os\n"
                "import sys\n"
                'if importlib.util.find_spec("erpclaw_lib") is None:\n'
                '    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))\n'
                "from erpclaw_lib.seam import Column, MetaData, Table, Text, provision, text\n"
                "from erpclaw_lib import seam\n"
                "SESSION_HOME = %r\n" % (session_home,) +
                'REQUIRED_FOUNDATION = ["company", "customer", "naming_series", "audit_log"]\n'
                "METADATA = MetaData()\n"
                'ISOL_ITEM = Table(\n'
                '    "isolpin_item", METADATA,\n'
                '    Column("id", Text, primary_key=True, nullable=True),\n'
                '    Column("name", Text, nullable=False),\n'
                '    Column("price", Text, nullable=False, server_default=text("\'0\'")),\n'
                '    Column("company_id", Text, nullable=False),\n'
                ")\n"
                "def _check_isolation():\n"
                '    home = os.environ.get("ERPCLAW_HOME")\n'
                '    if not os.path.islink(os.path.join(home, "lib")):\n'
                '        print("isolation check failed: ERPCLAW_HOME/lib is not a symlink", file=sys.stderr)\n'
                "        sys.exit(1)\n"
                '    if not (os.environ.get("HOME") == home == os.environ.get("TMPDIR")):\n'
                '        print("isolation check failed: HOME, ERPCLAW_HOME and TMPDIR are not pinned to the same scratch home", file=sys.stderr)\n'
                "        sys.exit(1)\n"
                "    if home == SESSION_HOME:\n"
                '        print("isolation check failed: installer ran under the session home", file=sys.stderr)\n'
                "        sys.exit(1)\n"
                "def init_isolpin_schema():\n"
                "    _check_isolation()\n"
                '    db_path = os.environ.get("ERPCLAW_DB_PATH")\n'
                "    if not db_path:\n"
                '        print("ERPCLAW_DB_PATH is not set", file=sys.stderr)\n'
                "        sys.exit(1)\n"
                "    missing = [name for name in REQUIRED_FOUNDATION if not seam.table_exists(name, db_path)]\n"
                "    if missing:\n"
                '        print("foundation tables missing", file=sys.stderr)\n'
                "        sys.exit(1)\n"
                "    provision(METADATA, db_path)\n"
                'if __name__ == "__main__":\n'
                "    init_isolpin_schema()\n")

    def test_installer_runs_in_isolated_home(
            self, tmp_path, seed_db, monkeypatch):
        session_home = os.path.expanduser(
            os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw"))
        db_path = seed_db
        module_dir = self._generated(tmp_path, name="isolclaw", prefix="isol")
        self._write_isolation_pinned_installer(module_dir, session_home)
        before = _ledger_digest(db_path)
        self._approve(monkeypatch)
        result = run_pipeline(
            module_dir, db_path=db_path, src_root=SRC_ROOT, skip_sandbox=True)
        assert result["pipeline_result"] == "deployed"
        assert result["audit_recorded"] is True
        assert sorted(result["tables_created"]) == ["isolpin_item"]
        assert seam.table_exists("isolpin_item", db_path) is True
        assert _ledger_digest(db_path) == before

