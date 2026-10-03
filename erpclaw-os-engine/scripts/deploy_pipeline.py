#!/usr/bin/env python3
"""ERPClaw OS — Auto-Deploy Pipeline

End-to-end deployment orchestrator. Full flow:
  1. Validate module (constitution check)
  2. Sandbox test execution
  3. GL invariant check on the deploy target (ledger guard)
  4. Tier classification
  5. Decision: auto-deploy (T0-1), queue for human (T2), suggestion only (T2.5), reject (T3)
  6. Provision schema into the deploy target (the module's own init_db.py)
     on an approved-for-target outcome only (deployed). Every other
     outcome provisions nothing.
  7. Record audit trail

Only the owning module writes its tables: provisioning executes the
module's own init_db.py as a subprocess (the same mechanism module_manager
and sandbox use). Catalog questions go through erpclaw_lib.seam.

Produces deployment report (JSON) with all step results.
"""
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from validate_module import validate_module_static
from sandbox import run_in_sandbox
from tier_classifier import classify_action
from deploy_audit import record_deployment, ensure_deploy_audit_table
from regression_gate import run_regression

# Tier thresholds for deployment decisions
TIER_AUTO_DEPLOY = 1     # Tier 0-1: auto-deploy
TIER_HUMAN_REVIEW = 2    # Tier 2: queue for human approval
TIER_SUGGESTION_ONLY = 25  # Tier 2.5: advisory suggestion, never deployed
TIER_REJECT = 3          # Tier 3: reject (human-only modification)


def run_pipeline(module_path, db_path=None, src_root=None, skip_sandbox=False,
               dry_run=False):
    """Run the full deployment pipeline for a module.

    Args:
        module_path: Path to the module directory
        db_path: Path to SQLite database (required to provision; a missing
            target refuses the provisioning step instead of falling back)
        src_root: Path to source/ directory (for validation)
        skip_sandbox: Skip sandbox testing (for pre-tested modules)
        dry_run: Report what provisioning would create without creating it

    Returns:
        dict with pipeline_result, steps, tier, reasoning, audit_id
    """
    pipeline_start = time.time()
    dry_run = bool(dry_run)
    module_name = os.path.basename(module_path)
    steps = []

    # A missing target refuses before anything can resolve a default
    # database: no ledger guard, no installer side effects, no audit write.
    if not db_path:
        return _pipeline_result(
            "failed", steps, None, None, pipeline_start,
            "Refused: no deploy target (db_path is None); refusing to "
            "provision into the default database",
            tables_created=[], dry_run=dry_run,
            audit_recorded=False, audit_error="no deploy target")
    # A non-file target is refused without repeating it: a PostgreSQL
    # URL can carry a password, and the existence check below would echo
    # the target into the result (which the action wrapper prints). This
    # refusal carries no target text at all: no ledger guard, no installer
    # side effects, no audit write.
    try:
        from erpclaw_lib.db import get_dialect
        _dialect = get_dialect()
    except Exception:
        _dialect = "sqlite"
    if _dialect == "postgresql" or (isinstance(db_path, str) and "://" in db_path):
        return _pipeline_result(
            "failed", steps, None, None, pipeline_start,
            "Refused: the deploy pipeline supports SQLite file targets only",
            tables_created=[], dry_run=dry_run,
            audit_recorded=False, audit_error="no deploy target")
    # A target path that does not exist is refused at entry, before the
    # ledger guard and before any audit write; it is never created.
    if not os.path.exists(db_path):
        return _pipeline_result(
            "failed", steps, None, None, pipeline_start,
            "Refused: deploy target does not exist: %s; refusing to "
            "create it" % (db_path,),
            tables_created=[], dry_run=dry_run,
            audit_recorded=False, audit_error="no deploy target")

    # -----------------------------------------------------------------------
    # Step 1: Constitution Validation
    # -----------------------------------------------------------------------
    step_start = time.time()
    try:
        validation = validate_module_static(module_path, src_root)
        validation_passed = validation.get("result") != "fail"
        step_result = "pass" if validation_passed else "fail"
        violations = validation.get("violations", [])
    except Exception as e:
        step_result = "error"
        validation_passed = False
        violations = [str(e)]

    steps.append({
        "step_name": "constitution_validation",
        "result": step_result,
        "duration_ms": int((time.time() - step_start) * 1000),
        "details": {
            "passed": validation_passed,
            "violation_count": len(violations),
        },
    })

    if not validation_passed:
        audit_id, audit_recorded, audit_error = _record_and_return(
            module_name, "failed", None, steps,
            "Constitution validation failed", db_path, dry_run=dry_run,
        )
        return _pipeline_result("failed", steps, None, audit_id, pipeline_start,
                                "Blocked at step 1: constitution validation failed",
                                tables_created=[], dry_run=dry_run, audit_recorded=audit_recorded, audit_error=audit_error)

    # -----------------------------------------------------------------------
    # Step 2: Sandbox Test Execution
    # -----------------------------------------------------------------------
    if not skip_sandbox:
        step_start = time.time()
        try:
            sandbox_result = run_in_sandbox(module_path)
            sandbox_passed = sandbox_result.get("result") == "pass"
            step_result = "pass" if sandbox_passed else "fail"
        except Exception as e:
            step_result = "error"
            sandbox_passed = False
            sandbox_result = {"error": str(e)}

        steps.append({
            "step_name": "sandbox_testing",
            "result": step_result,
            "duration_ms": int((time.time() - step_start) * 1000),
            "details": {
                "passed": sandbox_passed,
                "tests_run": sandbox_result.get("tests_run", 0),
                "tests_passed": sandbox_result.get("tests_passed", 0),
                "tests_failed": sandbox_result.get("tests_failed", 0),
            },
        })

        if not sandbox_passed:
            audit_id, audit_recorded, audit_error = _record_and_return(
                module_name, "failed", None, steps,
                "Sandbox testing failed", db_path, dry_run=dry_run,
            )
            return _pipeline_result("failed", steps, None, audit_id, pipeline_start,
                                    "Blocked at step 2: sandbox tests failed",
                                    tables_created=[], dry_run=dry_run, audit_recorded=audit_recorded, audit_error=audit_error)
    else:
        steps.append({
            "step_name": "sandbox_testing",
            "result": "skipped",
            "duration_ms": 0,
            "details": {"reason": "skip_sandbox=True"},
        })

    # -----------------------------------------------------------------------
    # Step 3: GL Invariant Check on the deploy target
    # -----------------------------------------------------------------------
    # The deploy path calls the shared ledger guard directly so a deploy can
    # never land on top of a broken book, even with skip_sandbox=True. A
    # "skip" (no ledger rows yet) is not a failure; fail/error blocks.
    step_start = time.time()
    try:
        from erpclaw_lib.gl_invariants import check_gl_invariants
        gl_result = check_gl_invariants(db_path)
        gl_state = gl_result.get("result", "error")
        gl_violations = gl_result.get("violations", [])
        if gl_state in ("pass", "skip"):
            step_result = gl_state
            gl_passed = True
        else:
            step_result = "fail" if gl_state == "fail" else "error"
            gl_passed = False
    except Exception as e:
        step_result = "error"
        gl_passed = False
        gl_result = {"error": str(e)}
        gl_violations = [str(e)]

    steps.append({
        "step_name": "gl_invariant_check",
        "result": step_result,
        "duration_ms": int((time.time() - step_start) * 1000),
        "details": {
            "passed": gl_passed,
            "state": gl_result.get("result", "error"),
            "violation_count": len(gl_violations),
        },
    })

    if not gl_passed:
        audit_id, audit_recorded, audit_error = _record_and_return(
            module_name, "failed", None, steps,
            "GL invariant check failed", db_path, dry_run=dry_run,
        )
        return _pipeline_result("failed", steps, None, audit_id, pipeline_start,
                                "Blocked at step 3: GL invariant check failed",
                                tables_created=[], dry_run=dry_run, audit_recorded=audit_recorded, audit_error=audit_error)

    # -----------------------------------------------------------------------
    # Step 4: Tier Classification
    # -----------------------------------------------------------------------
    step_start = time.time()
    tier_result = classify_action("deploy-module", module_name=module_name)
    tier = tier_result.get("tier", 2)

    steps.append({
        "step_name": "tier_classification",
        "result": "pass",
        "duration_ms": int((time.time() - step_start) * 1000),
        "details": {
            "tier": tier,
            "tier_name": tier_result.get("tier_name", "unknown"),
            "reasoning": tier_result.get("reasoning", ""),
        },
    })

    # -----------------------------------------------------------------------
    # Step 5: Deployment Decision
    # -----------------------------------------------------------------------
    if tier <= TIER_AUTO_DEPLOY:
        pipeline_result = "deployed"
        reasoning = f"Tier {tier} — auto-deployed (autonomous deployment allowed)"
    elif tier == TIER_HUMAN_REVIEW:
        pipeline_result = "queued"
        reasoning = f"Tier {tier} — queued for human review (module lifecycle operation)"
    elif tier == TIER_SUGGESTION_ONLY:
        pipeline_result = "suggestion"
        reasoning = f"Tier 2.5 — logged as advisory suggestion only. NEVER auto-deployed. Human must manually review and apply."
    else:
        pipeline_result = "rejected"
        reasoning = f"Tier {tier} — rejected (human-only operation, requires manual intervention)"

    steps.append({
        "step_name": "deployment_decision",
        "result": pipeline_result,
        "duration_ms": 0,
        "details": {
            "tier": tier,
            "decision": pipeline_result,
            "reasoning": reasoning,
        },
    })

    # -----------------------------------------------------------------------
    # Step 6: Provision Schema into the deploy target
    # -----------------------------------------------------------------------
    # Only an approved-for-target outcome (deployed) provisions. A module
    # failing validation, sandbox, or the ledger guard never reaches this
    # step, so it leaves no table behind. Queued (waiting for a human),
    # suggestion, and rejected outcomes skip: nothing is provisioned without
    # an approved-for-target decision. A dry run reports what it would
    # create and creates nothing. A missing target refuses instead of
    # falling back to any default.
    tables_created = []
    if pipeline_result == "deployed":
        if dry_run:
            step_start = time.time()
            try:
                would_create = _would_create_tables(module_path, db_path)
                dry_error = None
            except RuntimeError as e:
                would_create = []
                dry_error = "installer failed for dry run: %s" % (e,)
            except Exception as e:
                would_create = []
                dry_error = "catalog read failed for dry run: %s" % (e,)
            if dry_error is not None:
                steps.append({
                    "step_name": "provision_schema",
                    "result": "fail",
                    "duration_ms": int((time.time() - step_start) * 1000),
                    "details": {
                        "tables_created": [],
                        "error": dry_error,
                        "dry_run": True,
                    },
                })
                audit_id, audit_recorded, audit_error = _record_and_return(
                    module_name, "failed", tier, steps,
                    "Schema dry run failed: %s" % (dry_error,), db_path, dry_run=dry_run,
                )
                return _pipeline_result("failed", steps, tier, audit_id,
                                        pipeline_start,
                                        "Blocked at step 6: schema dry run failed",
                                        tables_created=[],
                                        dry_run=True, audit_recorded=audit_recorded, audit_error=audit_error)
            steps.append({
                "step_name": "provision_schema",
                "result": "skipped",
                "duration_ms": int((time.time() - step_start) * 1000),
                "details": {
                    "reason": ("dry-run: would create %s; created nothing"
                               % (would_create,)),
                    "tables_created": would_create,
                    "error": None,
                    "dry_run": True,
                },
            })
            tables_created = []
        else:
            step_start = time.time()
            provision_ok, tables_created, provision_error = _provision_module_schema(
                module_path, db_path
            )
            steps.append({
                "step_name": "provision_schema",
                "result": "pass" if provision_ok else "fail",
                "duration_ms": int((time.time() - step_start) * 1000),
                "details": {
                    "tables_created": tables_created,
                    "error": provision_error,
                    "dry_run": False,
                },
            })
            if not provision_ok:
                audit_id, audit_recorded, audit_error = _record_and_return(
                    module_name, "failed", tier, steps,
                    "Schema provisioning failed: %s" % (provision_error,), db_path, dry_run=dry_run,
                )
                return _pipeline_result("failed", steps, tier, audit_id,
                                        pipeline_start,
                                        "Blocked at step 6: schema provisioning failed",
                                        tables_created=tables_created,
                                        dry_run=False, audit_recorded=audit_recorded, audit_error=audit_error)
    else:
        if pipeline_result == "queued":
            reason = ("decision=queued: awaiting human approval; "
                      "provisioning deferred until approval")
        elif pipeline_result == "suggestion":
            reason = ("decision=suggestion: advisory only; "
                      "provisioning deferred")
        elif pipeline_result == "rejected":
            reason = ("decision=rejected: human-only operation; "
                      "nothing provisioned")
        else:
            reason = "decision=%s" % (pipeline_result,)
        if dry_run:
            reason = reason + " (dry-run: no changes made)"
        steps.append({
            "step_name": "provision_schema",
            "result": "skipped",
            "duration_ms": 0,
            "details": {
                "reason": reason,
                "tables_created": [],
                "error": None,
                "dry_run": dry_run,
            },
        })

    # -----------------------------------------------------------------------
    # Record Audit
    # -----------------------------------------------------------------------
    if pipeline_result == "deployed" and dry_run:
        reasoning = reasoning + " (dry-run: no changes made)"
    audit_id, audit_recorded, audit_error = _record_and_return(
        module_name, pipeline_result, tier, steps, reasoning, db_path, dry_run=dry_run,
    )

    return _pipeline_result(pipeline_result, steps, tier, audit_id,
                            pipeline_start, reasoning,
                            tables_created=tables_created,
                            dry_run=dry_run, audit_recorded=audit_recorded, audit_error=audit_error)


def _pipeline_home_lib():
    # The shared library dir of the ERPCLAW_HOME the pipeline itself runs
    # under. Scratch homes link to it so installers can still import it.
    return os.path.join(
        os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")),
        "lib")


def _make_isolated_home():
    # A fresh scratch home for one installer run, with a lib entry that
    # links to the pipeline home's lib. Caller removes it afterwards.
    import tempfile
    scratch = tempfile.mkdtemp(prefix="erpclaw_install_")
    real_lib = _pipeline_home_lib()
    try:
        if os.path.isdir(real_lib):
            os.symlink(real_lib, os.path.join(scratch, "lib"))
    except OSError:
        pass
    return scratch


def _cleanup_scratch_home(scratch):
    import shutil
    try:
        shutil.rmtree(scratch, ignore_errors=True)
    finally:
        try:
            from erpclaw_lib import seam
            seam.dispose_engines()
        except Exception:
            pass


def _run_installer_in_home(module_path, db_path, home):
    # Run the module installer once with HOME, ERPCLAW_HOME and TMPDIR
    # pointed at home. The target travels as both the argument and
    # ERPCLAW_DB_PATH. Returns (ok, error); caller owns home cleanup.
    init_db_path = os.path.join(module_path, "init_db.py")
    if not os.path.isfile(init_db_path):
        return True, None
    env = dict(os.environ)
    env["HOME"] = home
    env["ERPCLAW_HOME"] = home
    env["TMPDIR"] = home
    env["ERPCLAW_DB_PATH"] = db_path
    cmd = [sys.executable, init_db_path, db_path]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=os.path.dirname(init_db_path),
            env=env,
        )
    except Exception as e:
        return False, "init_db.py execution failed: %s" % (e,)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "exit %s" % (proc.returncode,)).strip()
        return False, detail
    return True, None


def _run_installer(module_path, db_path):
    # Run the module's own installer as an isolated subprocess with the
    # target db path as its argument and ERPCLAW_DB_PATH pinned to the
    # same target, so an installer that ignores its argument still lands
    # on the target instead of its own default. Each run gets a fresh
    # scratch home, removed afterwards, also on failure.
    # Returns (ok, error).
    init_db_path = os.path.join(module_path, "init_db.py")
    if not os.path.isfile(init_db_path):
        return True, None
    scratch = _make_isolated_home()
    try:
        return _run_installer_in_home(module_path, db_path, scratch)
    finally:
        _cleanup_scratch_home(scratch)


def _learn_declared_tables(module_path):
    # Learn the module's declared tables by running its installer against
    # a scratch database that holds the foundation schema and nothing of
    # this module. The scratch database lives at <scratch home>/data.sqlite
    # so an installer that falls back to its home default still lands on
    # it. Catalog answers come through the seam, never from source.
    # Returns (declared, error); error is not None when the installer
    # added nothing visible.
    from erpclaw_lib import seam
    init_db_path = os.path.join(module_path, "init_db.py")
    if not os.path.isfile(init_db_path):
        return [], None
    try:
        from sandbox import _find_init_schema
    except Exception as e:
        return None, "foundation installer lookup failed: %s" % (e,)
    try:
        init_schema_path = _find_init_schema()
    except Exception as e:
        return None, "foundation installer lookup failed: %s" % (e,)
    scratch = _make_isolated_home()
    scratch_db = os.path.join(scratch, "data.sqlite")
    try:
        proc = subprocess.run(
            [sys.executable, init_schema_path, "--db-path", scratch_db],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=os.path.dirname(init_schema_path),
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "exit %s" % (proc.returncode,)).strip()
            return None, "foundation installer failed for scratch database: %s" % (detail,)
        try:
            before = set(seam.table_names(scratch_db))
        except Exception as e:
            return None, "catalog read failed before scratch provisioning: %s" % (e,)
        ok, error = _run_installer_in_home(module_path, scratch_db, scratch)
        if not ok:
            return None, error
        try:
            after = set(seam.table_names(scratch_db))
        except Exception as e:
            return None, "catalog read failed after scratch provisioning: %s" % (e,)
        declared = sorted(after - before)
        if not declared:
            return None, ("installer added no tables to the scratch database; "
                          "it wrote somewhere the pipeline cannot see")
        return declared, None
    finally:
        _cleanup_scratch_home(scratch)


def _would_create_tables(module_path, db_path):
    # Dry-run answer, computed by running instead of reading: copy the
    # target to a throwaway temporary database (so installers that require
    # foundation tables can run), run the module's installer against the
    # copy isolated, list the tables it created through the seam, then
    # delete the copy; installer failure raises.
    import shutil
    import tempfile
    from erpclaw_lib import seam
    init_db_path = os.path.join(module_path, "init_db.py")
    if not os.path.isfile(init_db_path):
        return []
    tmp_dir = tempfile.mkdtemp(prefix="erpclaw_dryrun_")
    tmp_db = os.path.join(tmp_dir, "dryrun.sqlite")
    try:
        shutil.copy(db_path, tmp_db)
        for suffix in ("-wal", "-shm", "-journal"):
            sidecar = db_path + suffix
            if os.path.isfile(sidecar):
                shutil.copy(sidecar, tmp_db + suffix)
        before = set(seam.table_names(tmp_db))
        ok, error = _run_installer(module_path, tmp_db)
        if not ok:
            raise RuntimeError(error)
        after = set(seam.table_names(tmp_db))
        return sorted(after - before)
    finally:
        for tmp_path in [tmp_db] + [tmp_db + s for s in ("-wal", "-shm", "-journal")]:
            try:
                if os.path.isfile(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
        try:
            os.rmdir(tmp_dir)
        except OSError:
            pass
        try:
            seam.dispose_engines()
        except Exception:
            pass


def _provision_module_schema(module_path, db_path):
    # Provision the module's tables into the deploy target database.
    # Learns the declared tables from a scratch run, runs the module's
    # own installer isolated and pinned to the target, then requires every
    # declared table to be PRESENT in the target. Reports tables_created
    # truthfully as present-after minus present-before, which may be empty
    # on a re-deploy. Returns (ok, tables_created, error).
    init_db_path = os.path.join(module_path, "init_db.py")
    if not os.path.isfile(init_db_path):
        return True, [], None
    try:
        declared, decl_error = _learn_declared_tables(module_path)
    except Exception as e:
        return False, [], "declared tables discovery failed: %s" % (e,)
    if decl_error is not None:
        return False, [], decl_error
    try:
        from erpclaw_lib import seam
        before = set(seam.table_names(db_path))
    except Exception as e:
        return False, [], "catalog read failed before provisioning: %s" % (e,)
    ok, error = _run_installer(module_path, db_path)
    if not ok:
        return False, [], error
    try:
        from erpclaw_lib import seam
        after = set(seam.table_names(db_path))
    except Exception as e:
        return False, [], "catalog read failed after provisioning: %s" % (e,)
    tables_created = sorted(after - before)
    missing = [name for name in declared if name not in after]
    if missing:
        return (False, tables_created,
                "declared tables missing from deploy target: %s" % (", ".join(missing),))
    return True, tables_created, None


def _record_and_return(module_name, pipeline_result, tier, steps, reasoning, db_path,
                       dry_run=False):
    """Record deployment in audit log, return audit status.

    A dry run writes nothing and a missing target writes nothing: both
    return (None, False, <reason>) without touching any database, for
    every outcome. An outcome the audit table does not accept is refused
    before any table is created. Returns (audit_id, audit_recorded,
    audit_error).
    """
    if dry_run:
        return None, False, "dry run: nothing recorded"
    if not db_path:
        return None, False, "no deploy target"
    try:
        audit_id = record_deployment(
            module_name=module_name,
            pipeline_result=pipeline_result,
            tier=tier,
            steps=steps,
            reasoning=reasoning,
            db_path=db_path,
        )
        return audit_id, True, None
    except Exception as e:
        return None, False, str(e)


def _pipeline_result(pipeline_result, steps, tier, audit_id, start_time, reasoning,
                   tables_created=None, dry_run=False,
                   audit_recorded=False, audit_error=None):
    """Build the pipeline result dict."""
    return {
        "result": pipeline_result,
        "pipeline_result": pipeline_result,
        "steps": steps,
        "tier": tier,
        "audit_id": audit_id,
        "audit_recorded": bool(audit_recorded),
        "audit_error": audit_error,
        "reasoning": reasoning,
        "tables_created": tables_created or [],
        "dry_run": bool(dry_run),
        "duration_ms": int((time.time() - start_time) * 1000),
    }


# ---------------------------------------------------------------------------
# CLI Handler
# ---------------------------------------------------------------------------

def handle_deploy_module(args):
    """CLI handler for deploy-module action."""
    module_path = getattr(args, "module_path", None)
    db_path = getattr(args, "db_path", None)
    src_root = getattr(args, "src_root", None)
    skip_sandbox = getattr(args, "skip_sandbox", False)
    dry_run = getattr(args, "dry_run", False)

    if not module_path:
        return {"error": "--module-path is required for deploy-module"}

    if not os.path.isdir(module_path):
        return {"error": f"Module path does not exist: {module_path}"}

    result = run_pipeline(
        module_path,
        db_path=db_path,
        src_root=src_root,
        skip_sandbox=skip_sandbox,
        dry_run=bool(dry_run),
    )
    return result
