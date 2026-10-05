"""Generated module validation v1 (floor-o085).

Covers the read-only os-validate-generated-module action: one valid
fixture, missing SKILL row, missing route, prohibited secret-like
filename, symlink refusal, unsafe path refusal, deterministic sorted
findings, two identical calls, and no writes. The candidate is parsed as
data (never imported or executed) through the db_query.py router.
"""
import argparse
import importlib.util
import json
import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
OS_SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
DB_QUERY_PATH = os.path.join(OS_SCRIPTS_DIR, "db_query.py")

ACTION = "os-validate-generated-module"

VALID_SKILL = """\
---
name: democlaw
version: 1.0.0
description: Demo candidate module
author: TestAuthor
scripts:
  - scripts/db_query.py
---

# democlaw

## Actions

| Action | Description |
|--------|-------------|
| `demo-add-item` | Add an item |
| `status` | Check status |
"""

VALID_ROUTER = """\
ACTIONS = {
    "demo-add-item": None,
    "status": None,
}
"""


def _load_router():
    spec = importlib.util.spec_from_file_location("db_query_gmv_under_test", DB_QUERY_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def router():
    return _load_router()


def _write_candidate(root, skill_text=VALID_SKILL, router_text=VALID_ROUTER, extra_files=None):
    candidate = os.path.join(str(root), "democlaw")
    os.makedirs(os.path.join(candidate, "scripts"), exist_ok=True)
    with open(os.path.join(candidate, "SKILL.md"), "w", encoding="utf-8") as handle:
        handle.write(skill_text)
    with open(os.path.join(candidate, "scripts", "db_query.py"), "w", encoding="utf-8") as handle:
        handle.write(router_text)
    for relative, content in (extra_files or {}).items():
        absolute = os.path.join(candidate, relative)
        os.makedirs(os.path.dirname(absolute), exist_ok=True)
        mode = "wb" if isinstance(content, bytes) else "w"
        kwargs = {} if isinstance(content, bytes) else {"encoding": "utf-8"}
        with open(absolute, mode, **kwargs) as handle:
            handle.write(content)
    return candidate


def _rules(result):
    return [finding["rule"] for finding in result["findings"]]


def test_valid_fixture_passes(router, tmp_path):
    candidate = _write_candidate(tmp_path)
    result = router.validate_generated_module(candidate)
    assert result["valid"] is True
    assert result["findings"] == []
    assert result["declared_actions"] == ["demo-add-item", "status"]
    assert result["routed_actions"] == ["demo-add-item", "status"]
    assert result["missing_skill_rows"] == []
    assert result["missing_routes"] == []
    assert result["unexpected_routes"] == []
    assert result["prohibited_files"] == []
    assert result["metadata"]["name"] == "democlaw"
    assert result["metadata"]["version"] == "1.0.0"


def test_missing_skill_row(router, tmp_path):
    candidate = _write_candidate(
        tmp_path,
        router_text="ACTIONS = {\n    \"demo-add-item\": None,\n    \"status\": None,\n"
        "    \"demo-secret-thing\": None,\n}\n",
    )
    result = router.validate_generated_module(candidate)
    assert result["valid"] is False
    assert result["missing_skill_rows"] == ["demo-secret-thing"]
    assert result["unexpected_routes"] == ["demo-secret-thing"]
    assert "GMV_SKILL_ROW_MISSING" in _rules(result)


def test_missing_route(router, tmp_path):
    skill = VALID_SKILL.replace(
        "| `status` | Check status |",
        "| `status` | Check status |\n| `demo-ghost-thing` | Missing route |",
    )
    candidate = _write_candidate(tmp_path, skill_text=skill)
    result = router.validate_generated_module(candidate)
    assert result["valid"] is False
    assert result["missing_routes"] == ["demo-ghost-thing"]
    assert "GMV_ROUTE_MISSING" in _rules(result)


def test_prohibited_secret_filename(router, tmp_path):
    candidate = _write_candidate(
        tmp_path, extra_files={".env": "SECRET=topsecret\n", "keys/deploy.pem": "x"}
    )
    result = router.validate_generated_module(candidate)
    assert result["valid"] is False
    assert ".env" in result["prohibited_files"]
    assert "keys/deploy.pem" in result["prohibited_files"]
    assert "GMV_PROHIBITED_FILE" in _rules(result)


def test_symlink_refused(router, tmp_path):
    candidate = _write_candidate(tmp_path)
    real_file = os.path.join(candidate, "scripts", "db_query.py")
    os.symlink(real_file, os.path.join(candidate, "scripts", "link.py"))
    result = router.validate_generated_module(candidate)
    assert result["valid"] is False
    assert "GMV_SYMLINK_REFUSED" in _rules(result)

    alias = str(tmp_path) + "-alias"
    os.symlink(candidate, alias)
    aliased = router.validate_generated_module(alias)
    assert aliased["valid"] is False
    assert "GMV_SYMLINK_REFUSED" in [finding["rule"] for finding in aliased["findings"]]


def test_unsafe_path_refused(router, tmp_path):
    for bad in ("", None, os.path.join(str(tmp_path), "does-not-exist"), str(tmp_path / ".." / "nope")):
        result = router.validate_generated_module(bad)
        assert result["valid"] is False
        assert "GMV_UNSAFE_PATH" in _rules(result)
    as_file = os.path.join(str(tmp_path), "plain.txt")
    with open(as_file, "w", encoding="utf-8") as handle:
        handle.write("x")
    result = router.validate_generated_module(as_file)
    assert result["valid"] is False
    assert "GMV_UNSAFE_PATH" in _rules(result)


def test_findings_deterministic_and_sorted(router, tmp_path):
    candidate = _write_candidate(
        tmp_path,
        router_text="ACTIONS = {\n    \"demo-add-item\": None,\n    \"zzz-extra\": None,\n}\n",
        extra_files={"id_rsa": "x"},
    )
    result = router.validate_generated_module(candidate)
    assert result["valid"] is False
    keys = [(finding["rule"], finding["path"], finding["message"]) for finding in result["findings"]]
    assert keys == sorted(keys)
    assert result["declared_actions"] == sorted(result["declared_actions"])
    assert result["routed_actions"] == sorted(result["routed_actions"])


def test_two_identical_calls_match(router, tmp_path):
    candidate = _write_candidate(tmp_path, extra_files={".env": "x"})
    first = router.validate_generated_module(candidate)
    second = router.validate_generated_module(candidate)
    assert first == second


def _snapshot_tree(root):
    snapshot = {}
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames.sort()
        for name in sorted(filenames):
            absolute = os.path.join(dirpath, name)
            if os.path.islink(absolute):
                continue
            with open(absolute, "rb") as handle:
                snapshot[os.path.relpath(absolute, root)] = handle.read()
    return snapshot


def test_no_writes_to_candidate(router, tmp_path):
    candidate = _write_candidate(tmp_path, extra_files={".env": "x"})
    before = _snapshot_tree(candidate)
    router.validate_generated_module(candidate)
    router.validate_generated_module(candidate)
    assert _snapshot_tree(candidate) == before


def test_candidate_never_executed(router, tmp_path):
    candidate = _write_candidate(
        tmp_path,
        router_text=VALID_ROUTER + '\nraise RuntimeError("candidate must never be executed")\n',
        extra_files={"trap.py": 'raise RuntimeError("candidate must never be imported")\n'},
    )
    result = router.validate_generated_module(candidate)
    assert result["valid"] is True


def test_routing_requires_module_path(router):
    assert ACTION in router.ACTIONS
    assert router.GMV_ACTION_NAME == ACTION
    with pytest.raises(SystemExit):
        router.handle_validate_generated_module(argparse.Namespace(module_path=None))


def test_cli_end_to_end(router, tmp_path, monkeypatch, capsys):
    candidate = _write_candidate(tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["db_query.py", "--action", ACTION, "--module-path", candidate]
    )
    with pytest.raises(SystemExit) as excinfo:
        router.main()
    assert excinfo.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["valid"] is True
    assert payload["declared_actions"] == ["demo-add-item", "status"]
