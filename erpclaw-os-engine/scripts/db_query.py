#!/usr/bin/env python3
"""ERPClaw OS Engine — db_query.py (addon)

Action router for the optional ERPClaw OS Engine. Provides 28
os-prefixed actions for module generation, deploy pipeline, DGM
evolution, semantic checks, compliance, and the web-dashboard
provisioner.

The addon depends on foundation skill `erpclaw >= 4.0.0`. On every
invocation, this router does a runtime self-check that the
foundation's shared library `erpclaw_lib` is importable. If the
foundation isn't installed, the router emits a structured error JSON
with installation guidance.

Usage: python3 db_query.py --action <os-action-name> [--flags ...]
Output: JSON to stdout, exit 0 on success, exit 1 on error.
"""
import ast
import json
import os
import re
import sys

# ---------------------------------------------------------------------------
# Runtime self-check: foundation skill must be present
# ---------------------------------------------------------------------------

def _self_check_foundation():
    """Verify foundation skill `erpclaw` is installed; locate erpclaw_lib.

    Prepends candidate lib paths to sys.path. Tries installed first
    (production), then source-relative (dev / not-yet-published).
    """
    candidates = []
    env_home = os.environ.get("OPENCLAW_HOME")
    if env_home:
        candidates.append(os.path.join(env_home, "scripts", "erpclaw-setup", "lib"))
    # Production install path (foundation lib)
    candidates.append(os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    # Dev / source-relative fallback (5 levels up: scripts -> erpclaw-os-engine -> erpclaw-addons -> source -> repo, then into foundation source)
    here = os.path.abspath(os.path.dirname(__file__))
    repo_lib = os.path.normpath(os.path.join(here, "..", "..", "..", "erpclaw", "scripts", "erpclaw-setup", "lib"))
    candidates.append(repo_lib)

    # Insert all existing candidates into sys.path in iteration order, so the LAST
    # candidate ends up at sys.path[0] and wins for the package resolution.
    # Candidate order: env_home, production install, repo-relative (dev). We want
    # repo-relative to win during local dev so newly-added modules (gl_invariants)
    # are resolvable before the foundation is republished.
    located = None
    matched = []
    for c in candidates:
        if c and os.path.isdir(os.path.join(c, "erpclaw_lib")):
            matched.append(c)
            located = c
    for c in matched:
        if c not in sys.path:
            sys.path.insert(0, c)

    if located is None:
        print(json.dumps({
            "status": "error",
            "error": "erpclaw-os-engine requires foundation skill 'erpclaw' (>= v4.0.0) to be installed",
            "missing_dependency": "erpclaw",
            "tried_paths": candidates,
            "install_command": "clawhub install erpclaw  # foundation",
            "addon_skill": "erpclaw-os-engine",
        }, indent=2))
        sys.exit(1)

    try:
        import erpclaw_lib  # noqa: F401
        return located
    except ImportError as e:
        print(json.dumps({
            "status": "error",
            "error": f"erpclaw_lib import failed: {e}",
            "missing_dependency": "erpclaw_lib",
        }, indent=2))
        sys.exit(1)


_self_check_foundation()

# Now safe to import shared lib
from erpclaw_lib.response import ok, err
from erpclaw_lib.args import SafeArgumentParser, check_unknown_args

# ---------------------------------------------------------------------------
# Foundation-locator: addon needs to import a few foundation runtime modules
# (validate_module, constitution, schema_*, dependency_resolver) from
# foundation's scripts/erpclaw-os/ subdir. Search in this order:
#   1. $OPENCLAW_HOME/scripts/erpclaw-os/  (env override)
#   2. ~/.openclaw/workspace/skills/erpclaw/scripts/erpclaw-os/  (production install)
#   3. <repo>/source/erpclaw/scripts/erpclaw-os/  (local dev / repo-relative)
# ---------------------------------------------------------------------------

def _add_foundation_os_to_sys_path():
    """Locate foundation's scripts/erpclaw-os/ and prepend to sys.path."""
    candidates = []
    env_home = os.environ.get("OPENCLAW_HOME")
    if env_home:
        candidates.append(os.path.join(env_home, "scripts", "erpclaw-os"))
    candidates.append(os.path.expanduser("~/.openclaw/workspace/skills/erpclaw/scripts/erpclaw-os"))
    # Repo-relative fallback (5 levels up: scripts -> erpclaw-os-engine -> erpclaw-addons -> source -> repo)
    here = os.path.abspath(os.path.dirname(__file__))
    repo_candidate = os.path.normpath(os.path.join(here, "..", "..", "..", "erpclaw", "scripts", "erpclaw-os"))
    candidates.append(repo_candidate)

    for c in candidates:
        if os.path.isdir(c) and os.path.isfile(os.path.join(c, "validate_module.py")):
            if c not in sys.path:
                sys.path.insert(0, c)
            return c
    return None


_foundation_os_path = _add_foundation_os_to_sys_path()
if _foundation_os_path is None:
    print(json.dumps({
        "status": "error",
        "error": "erpclaw-os-engine cannot locate foundation's scripts/erpclaw-os/ directory",
        "tried_paths": [
            os.environ.get("OPENCLAW_HOME"),
            os.path.expanduser("~/.openclaw/workspace/skills/erpclaw/scripts/erpclaw-os"),
            "<repo>/source/erpclaw/scripts/erpclaw-os",
        ],
        "missing_dependency": "erpclaw",
    }, indent=2))
    sys.exit(1)


# ---------------------------------------------------------------------------
# Sibling-package imports (all moved files live next to this one)
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from generate_module import generate_module
from configure_module import configure_module
from industry_configs import list_industries
from tier_classifier import handle_classify_operation
from deploy_pipeline import handle_deploy_module
from deploy_audit import handle_deploy_audit_log
from install_suite import handle_install_suite
from adversarial_audit import handle_run_audit
from compliance_weather import handle_compliance_weather_status
from improvement_log import (
    handle_log_improvement,
    handle_list_improvements,
    handle_review_improvement,
)
from semantic_engine import handle_semantic_check, handle_semantic_rules_list
from dgm_engine import (
    handle_dgm_run_variant,
    handle_dgm_list_variants,
    handle_dgm_select_best,
)
from gap_detector import (
    handle_detect_gaps,
    handle_suggest_modules,
    handle_detect_schema_divergence,
    handle_detect_stubs,
)
from heartbeat_analysis import (
    handle_heartbeat_analyze,
    handle_heartbeat_report,
    handle_heartbeat_suggest,
)
from in_module_generator import handle_add_feature_to_module
from research_engine import handle_research_rule, handle_get_implementation_guide
from feature_matrix import handle_check_feature_completeness, handle_list_feature_matrix
from web_dashboard import handle_setup_web_dashboard


# ---------------------------------------------------------------------------
# Generated module validation v1, read-only candidate checker
# ---------------------------------------------------------------------------
# Validates one local candidate ERPClaw module directory against the shipped
# module conventions: SKILL.md action table plus scripts/db_query.py router.
# Read-only by construction: candidate files are parsed as data (UTF-8 text
# plus ast), never imported, executed, installed, or followed through
# symlinks. No subprocess, no network, no database, no model call, and no
# writes to the candidate directory. Findings carry stable GMV_* rule
# identifiers so an external module author can correct a candidate without
# private plan context.

GMV_ACTION_NAME = "os-validate-generated-module"

GMV_UNSAFE_PATH = "GMV_UNSAFE_PATH"
GMV_SYMLINK_REFUSED = "GMV_SYMLINK_REFUSED"
GMV_SKILL_MD_REQUIRED = "GMV_SKILL_MD_REQUIRED"
GMV_SKILL_MD_PARSE = "GMV_SKILL_MD_PARSE"
GMV_ROUTER_REQUIRED = "GMV_ROUTER_REQUIRED"
GMV_ROUTER_PARSE = "GMV_ROUTER_PARSE"
GMV_SKILL_ROW_MISSING = "GMV_SKILL_ROW_MISSING"
GMV_ROUTE_MISSING = "GMV_ROUTE_MISSING"
GMV_PROHIBITED_FILE = "GMV_PROHIBITED_FILE"
GMV_METADATA_REQUIRED = "GMV_METADATA_REQUIRED"

_GMV_REQUIRED_METADATA_KEYS = ("name", "version", "description")

_GMV_PROHIBITED_BASENAME_RES = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\.env(\..+)?",
        r".*\.pem",
        r".*\.key",
        r"id_rsa.*",
        r"id_dsa.*",
        r".*secret.*",
        r".*credential.*",
        r".*\.p12",
        r".*\.pfx",
        r".*\.jks",
        r"token\.json",
        r"auth\.json",
    )
)

_GMV_MAX_READ_BYTES = 1000000

_GMV_TABLE_ROW_RE = re.compile(r"^\s*\|.*\|\s*$")
_GMV_BACKTICK_RE = re.compile(r"`([^`\s][^`]*)`")


def _gmv_finding(rule, message, path=""):
    return {"rule": rule, "path": path, "message": message}


def _gmv_sort_findings(findings):
    findings.sort(key=lambda item: (item["rule"], item["path"], item["message"]))
    return findings


def _gmv_empty_result(module_path):
    return {
        "action": GMV_ACTION_NAME,
        "module_path": module_path if isinstance(module_path, str) else "",
        "valid": False,
        "declared_actions": [],
        "routed_actions": [],
        "missing_skill_rows": [],
        "missing_routes": [],
        "unexpected_routes": [],
        "prohibited_files": [],
        "metadata": {"name": None, "version": None, "description": None, "author": None},
        "findings": [],
    }


def _gmv_check_module_path(module_path):
    """Return (root, refusal) with exactly one side set.

    root is the canonical candidate directory. refusal is a finding when the
    supplied path fails safe-path bounds: empty, NUL byte, '..' segments, a
    symlink anywhere on the path, or not an existing directory.
    """
    if not isinstance(module_path, str) or not module_path or "\x00" in module_path:
        return None, _gmv_finding(GMV_UNSAFE_PATH, "module path must be a non-empty local path")
    if ".." in module_path.replace("\\", "/").split("/"):
        return None, _gmv_finding(GMV_UNSAFE_PATH, "module path must not contain '..' segments")
    absolute = os.path.abspath(module_path)
    probe = absolute
    while True:
        if os.path.islink(probe):
            return None, _gmv_finding(
                GMV_SYMLINK_REFUSED,
                "module path resolves through a symlink; pass the real directory",
            )
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    if not os.path.isdir(absolute):
        return None, _gmv_finding(GMV_UNSAFE_PATH, "module path does not exist or is not a directory")
    return os.path.realpath(absolute), None


def _gmv_walk(root):
    """List candidate files without following symlinks.

    Returns (files, symlinks): files holds (relative_posix, absolute) pairs
    for regular files; symlinks holds relative paths of symlinks met while
    walking, which are recorded but never followed.
    """
    files = []
    symlinks = []
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                batch = sorted(entries, key=lambda entry: entry.name)
        except OSError:
            continue
        for entry in batch:
            try:
                if entry.is_symlink():
                    symlinks.append(os.path.relpath(entry.path, root).replace(os.sep, "/"))
                elif entry.is_dir(follow_symlinks=False):
                    stack.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    relative = os.path.relpath(entry.path, root).replace(os.sep, "/")
                    if relative != ".." and not relative.startswith("../"):
                        files.append((relative, entry.path))
            except OSError:
                continue
    files.sort(key=lambda item: item[0])
    symlinks.sort()
    return files, symlinks


def _gmv_read_text(absolute_path):
    """Read a candidate file as UTF-8 text, or None when unreadable."""
    try:
        if os.path.getsize(absolute_path) > _GMV_MAX_READ_BYTES:
            return None
        with open(absolute_path, "r", encoding="utf-8", errors="strict") as handle:
            return handle.read()
    except (OSError, UnicodeDecodeError, ValueError):
        return None


def _gmv_parse_frontmatter(text):
    """Parse simple key: value YAML frontmatter; return dict or None.

    Only the scalar keys the validator reports are needed, plus the scripts
    list. Anything richer is out of scope and yields None.
    """
    lines = text.splitlines()
    if len(lines) < 2 or lines[0].strip() != "---":
        return None
    end = None
    for index in range(1, len(lines)):
        if lines[index].strip() == "---":
            end = index
            break
    if end is None:
        return None
    data = {}
    current_list = None
    for line in lines[1:end]:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("- ") and current_list is not None:
            current_list.append(stripped[2:].strip().strip("'\""))
            continue
        if ":" in stripped:
            key, _, value = stripped.partition(":")
            key = key.strip()
            value = value.strip().strip("'\"")
            if value == "" and key == "scripts":
                data[key] = []
                current_list = data[key]
            else:
                current_list = None
                data[key] = value
        else:
            current_list = None
    return data


def _gmv_declared_actions(skill_text):
    """Collect backtick-quoted action names from SKILL.md pipe-table rows."""
    declared = set()
    for line in skill_text.splitlines():
        if _GMV_TABLE_ROW_RE.match(line):
            for match in _GMV_BACKTICK_RE.finditer(line):
                declared.add(match.group(1).strip())
    return sorted(declared)


def _gmv_const_strings(node):
    """String literals held directly by a List, Tuple, or Constant node."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, (ast.List, ast.Tuple)):
        return [
            element.value
            for element in node.elts
            if isinstance(element, ast.Constant) and isinstance(element.value, str)
        ]
    return []


def _gmv_collect_action_dicts(tree):
    """Map dict variable name to its string keys for ACTIONS-style dicts."""
    found = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict):
            for target in node.targets:
                if isinstance(target, ast.Name) and (
                    target.id == "ACTIONS" or target.id.endswith("_ACTIONS")
                ):
                    keys = {
                        key.value
                        for key in node.value.keys
                        if isinstance(key, ast.Constant) and isinstance(key.value, str)
                    }
                    found.setdefault(target.id, set()).update(keys)
    return found


def _gmv_is_action_attr(node):
    return isinstance(node, ast.Attribute) and node.attr == "action"


def _gmv_routed_from_tree(tree):
    """Collect routed action names from a router AST without executing it."""
    routed = set()
    for keys in _gmv_collect_action_dicts(tree).values():
        routed.update(keys)
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            for operator, comparator in zip(node.ops, node.comparators):
                if isinstance(operator, (ast.Eq, ast.NotEq, ast.In, ast.NotIn)):
                    if _gmv_is_action_attr(node.left):
                        routed.update(_gmv_const_strings(comparator))
                    if _gmv_is_action_attr(comparator):
                        routed.update(_gmv_const_strings(node.left))
        elif isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr == "add_argument":
                if node.args:
                    first = node.args[0]
                    if isinstance(first, ast.Constant) and first.value == "--action":
                        for keyword in node.keywords:
                            if keyword.arg == "choices":
                                routed.update(_gmv_const_strings(keyword.value))
    return routed


def _gmv_referenced_update_names(tree):
    """Local names passed to *.update(...) calls in the router."""
    referenced = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr == "update":
                for argument in node.args:
                    if isinstance(argument, ast.Name):
                        referenced.add((None, argument.id))
                    elif isinstance(argument, ast.Attribute) and isinstance(
                        argument.value, ast.Name
                    ):
                        referenced.add((argument.value.id, argument.attr))
    return referenced


def _gmv_sibling_actions(root, router_text):
    """Resolve same-directory sibling *_ACTIONS dicts wired into the router.

    Generated routers merge domain dicts (ACTIONS.update(DOMAIN_ACTIONS))
    imported from sibling files. Those files are parsed as data with ast and
    only when reached through a same-directory, non-symlink .py file, so the
    candidate is still never imported or executed.
    """
    try:
        tree = ast.parse(router_text)
    except (SyntaxError, ValueError):
        return set()
    imports = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            if "." in node.module or "/" in node.module or "\\" in node.module:
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                imports[alias.asname or alias.name] = (node.module, alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if "." in alias.name:
                    continue
                imports[alias.asname or alias.name] = (alias.name, None)
    if not imports:
        return set()
    routed = set()
    for qualifier, wanted in _gmv_referenced_update_names(tree):
        target_module = None
        target_dict = None
        if qualifier is None:
            hit = imports.get(wanted)
            if hit is None:
                continue
            target_module, original = hit
            target_dict = original or wanted
        else:
            hit = imports.get(qualifier)
            if hit is None:
                continue
            target_module = hit[0]
            target_dict = wanted
        sibling_path = os.path.join(root, "scripts", target_module + ".py")
        if not sibling_path.startswith(root + os.sep):
            continue
        if not os.path.isfile(sibling_path) or os.path.islink(sibling_path):
            continue
        sibling_text = _gmv_read_text(sibling_path)
        if sibling_text is None:
            continue
        try:
            sibling_tree = ast.parse(sibling_text)
        except (SyntaxError, ValueError):
            continue
        sibling_dicts = _gmv_collect_action_dicts(sibling_tree)
        if target_dict in sibling_dicts:
            routed.update(sibling_dicts[target_dict])
    return routed


def _gmv_prohibited_files(files):
    """Relative paths whose basename looks like a secret or key file."""
    bad = []
    for relative, _absolute in files:
        basename = relative.rsplit("/", 1)[-1]
        for pattern in _GMV_PROHIBITED_BASENAME_RES:
            if pattern.fullmatch(basename):
                bad.append(relative)
                break
    return sorted(bad)


def validate_generated_module(module_path):
    """Validate one local candidate module directory (read-only).

    Returns a JSON-serializable dict reporting declared actions (SKILL.md),
    routed actions (scripts/db_query.py parsed as data), missing SKILL rows,
    unexpected routes, missing routes, prohibited files, package metadata,
    and deterministically sorted findings with stable GMV_* rule identifiers.
    valid is True only when every required check passes. Never imports,
    executes, installs, writes, or follows symlinks from the candidate.
    """
    result = _gmv_empty_result(module_path)
    root, refusal = _gmv_check_module_path(module_path)
    if refusal is not None:
        result["findings"] = _gmv_sort_findings([refusal])
        return result
    result["module_path"] = root
    findings = result["findings"]

    files, symlinks = _gmv_walk(root)
    for link in symlinks:
        findings.append(
            _gmv_finding(
                GMV_SYMLINK_REFUSED,
                "candidate contains a symlink; symlinks are never followed",
                link,
            )
        )

    by_relative = dict(files)
    skill_text = None
    if "SKILL.md" in by_relative:
        skill_text = _gmv_read_text(by_relative["SKILL.md"])
        if skill_text is None:
            findings.append(
                _gmv_finding(GMV_SKILL_MD_PARSE, "SKILL.md is not readable UTF-8 text", "SKILL.md")
            )
    else:
        findings.append(_gmv_finding(GMV_SKILL_MD_REQUIRED, "candidate must ship SKILL.md"))

    router_relative = "scripts/db_query.py"
    router_text = None
    if router_relative in by_relative:
        router_text = _gmv_read_text(by_relative[router_relative])
        if router_text is None:
            findings.append(
                _gmv_finding(
                    GMV_ROUTER_PARSE,
                    "scripts/db_query.py is not readable UTF-8 text",
                    router_relative,
                )
            )
    else:
        findings.append(
            _gmv_finding(GMV_ROUTER_REQUIRED, "candidate must ship scripts/db_query.py")
        )

    declared = []
    if skill_text is not None:
        frontmatter = _gmv_parse_frontmatter(skill_text)
        if frontmatter is None:
            findings.append(
                _gmv_finding(
                    GMV_SKILL_MD_PARSE,
                    "SKILL.md frontmatter is missing or unparseable",
                    "SKILL.md",
                )
            )
        else:
            for key in _GMV_REQUIRED_METADATA_KEYS:
                value = frontmatter.get(key)
                if not isinstance(value, str) or not value.strip():
                    findings.append(
                        _gmv_finding(
                            GMV_METADATA_REQUIRED,
                            "package metadata key '%s' is missing or empty" % key,
                            "SKILL.md",
                        )
                    )
            result["metadata"] = {
                "name": frontmatter.get("name") if isinstance(frontmatter.get("name"), str) else None,
                "version": frontmatter.get("version")
                if isinstance(frontmatter.get("version"), str)
                else None,
                "description": frontmatter.get("description")
                if isinstance(frontmatter.get("description"), str)
                else None,
                "author": frontmatter.get("author") if isinstance(frontmatter.get("author"), str) else None,
            }
        declared = _gmv_declared_actions(skill_text)
        result["declared_actions"] = declared

    routed = []
    if router_text is not None:
        try:
            router_tree = ast.parse(router_text)
        except (SyntaxError, ValueError):
            findings.append(
                _gmv_finding(
                    GMV_ROUTER_PARSE, "scripts/db_query.py is not parseable Python", router_relative
                )
            )
        else:
            routed = sorted(_gmv_routed_from_tree(router_tree) | _gmv_sibling_actions(root, router_text))
            result["routed_actions"] = routed

    declared_set = set(declared)
    routed_set = set(routed)
    missing_rows = sorted(routed_set - declared_set)
    missing_routes = sorted(declared_set - routed_set)
    result["missing_skill_rows"] = missing_rows
    result["unexpected_routes"] = list(missing_rows)
    result["missing_routes"] = missing_routes
    for action in missing_rows:
        findings.append(
            _gmv_finding(
                GMV_SKILL_ROW_MISSING,
                "routed action '%s' has no SKILL.md row" % action,
                "SKILL.md",
            )
        )
    for action in missing_routes:
        findings.append(
            _gmv_finding(
                GMV_ROUTE_MISSING,
                "declared action '%s' has no route in scripts/db_query.py" % action,
                router_relative,
            )
        )

    prohibited = _gmv_prohibited_files(files)
    result["prohibited_files"] = prohibited
    for relative in prohibited:
        findings.append(_gmv_finding(GMV_PROHIBITED_FILE, "prohibited secret-like filename", relative))

    result["findings"] = _gmv_sort_findings(findings)
    result["valid"] = not result["findings"]
    return result


def handle_validate_generated_module(args):
    """Read-only validation of one local candidate module directory."""
    module_path = getattr(args, "module_path", None)
    if not module_path:
        err("--module-path is required for os-validate-generated-module")
    ok(validate_generated_module(module_path))


# ---------------------------------------------------------------------------
# Wrappers for actions that don't follow the handle_* convention
# ---------------------------------------------------------------------------

def handle_generate_module(args):
    """Wrap generate_module() function for action dispatch."""
    entities = getattr(args, "entities", None)
    if isinstance(entities, str):
        try:
            entities = json.loads(entities)
        except json.JSONDecodeError as exc:
            err("--entities must be a JSON list of entity definitions: %s" % (exc,))
    result = generate_module(
        module_name=getattr(args, "module_name", None),
        prefix=getattr(args, "prefix", None),
        business_description=(
            getattr(args, "description", None)
            or getattr(args, "industry", None)
            or ""
        ),
        entities=entities,
        output_dir=getattr(args, "output_dir", None),
        src_root=getattr(args, "src_root", None),
    )
    if isinstance(result, dict) and "error" in result:
        err(result["error"])
    if isinstance(result, dict) and result.get("result") == "fail":
        validation = result.get("validation") or {}
        errors = validation.get("errors") or validation.get("violations") or []
        if not errors and validation.get("message"):
            errors = [validation.get("message")]
        if not errors:
            errors = ["unknown validation failure"]
        err("module generation failed: %s"
            % ("; ".join(str(item) for item in errors),))
    ok(result if isinstance(result, dict) else {"result": result})


def handle_deploy_action(args):
    """Dispatch wrapper for os-deploy-module: emit the result as JSON."""
    result = handle_deploy_module(args)
    if isinstance(result, dict) and "error" in result:
        err(result["error"])
    if isinstance(result, dict) and result.get("pipeline_result") == "failed":
        err("module deployment failed: %s"
            % (result.get("reasoning") or "unknown failure",))
    ok(result if isinstance(result, dict) else {"result": result})


def handle_configure_module(args):
    """Wrap configure_module() function for action dispatch."""
    result = configure_module(args)
    if isinstance(result, dict) and "error" in result:
        err(result["error"])
    ok(result if isinstance(result, dict) else {"result": result})


def handle_list_industries(args):
    """Wrap list_industries() function for action dispatch."""
    industries = list_industries()
    ok({
        "industries": industries,
        "count": len(industries),
        "hint": "Use --action os-configure-module --industry <name> --company-id <id> to apply",
    })


def handle_status(args):
    """Report addon status."""
    ok({
        "addon": "erpclaw-os-engine",
        "version": "1.0.0",
        "foundation": "erpclaw",
        "actions_count": 28,
        "self_check": "ok",
    })


# ---------------------------------------------------------------------------
# Action dispatch table — all os-prefixed
# ---------------------------------------------------------------------------

ACTIONS = {
    # Generation + config
    "os-generate-module": handle_generate_module,
    "os-configure-module": handle_configure_module,
    "os-list-industries": handle_list_industries,
    "os-classify-operation": handle_classify_operation,
    # Deploy pipeline
    "os-deploy-module": handle_deploy_action,
    "os-deploy-audit-log": handle_deploy_audit_log,
    "os-install-suite": handle_install_suite,
    # Audit
    "os-run-audit": handle_run_audit,
    # Generated module validation (read-only candidate checker)
    "os-validate-generated-module": handle_validate_generated_module,
    "os-compliance-weather-status": handle_compliance_weather_status,
    # Semantic engine
    "os-semantic-check": handle_semantic_check,
    "os-semantic-rules-list": handle_semantic_rules_list,
    # Improvement log
    "os-log-improvement": handle_log_improvement,
    "os-list-improvements": handle_list_improvements,
    "os-review-improvement": handle_review_improvement,
    # DGM evolution
    "os-dgm-run-variant": handle_dgm_run_variant,
    "os-dgm-list-variants": handle_dgm_list_variants,
    "os-dgm-select-best": handle_dgm_select_best,
    # Gap detection
    "os-detect-gaps": handle_detect_gaps,
    "os-detect-schema-divergence": handle_detect_schema_divergence,
    "os-detect-stubs": handle_detect_stubs,
    "os-suggest-modules": handle_suggest_modules,
    # Heartbeat
    "os-heartbeat-analyze": handle_heartbeat_analyze,
    "os-heartbeat-report": handle_heartbeat_report,
    "os-heartbeat-suggest": handle_heartbeat_suggest,
    # In-module feature injection
    "os-add-feature-to-module": handle_add_feature_to_module,
    # Feature matrix
    "os-check-feature-completeness": handle_check_feature_completeness,
    "os-list-feature-matrix": handle_list_feature_matrix,
    # Research
    "os-research-business-rule": handle_research_rule,
    "os-get-implementation-guide": handle_get_implementation_guide,
    # Web dashboard provisioning (moved from erpclaw-meta on 2026-05-04)
    "os-setup-web-dashboard": handle_setup_web_dashboard,
    # Status
    "os-status": handle_status,
}


def main():
    parser = SafeArgumentParser(description="ERPClaw OS Engine — module generation, deploy, DGM, semantic, heartbeat")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--module-name", help="Module name")
    parser.add_argument("--module-path", help="Module path")
    parser.add_argument("--domain", help="Domain for web dashboard (os-setup-web-dashboard)")
    parser.add_argument("--ssl", action="store_true", default=None, help="Enable SSL via certbot")
    parser.add_argument("--no-ssl", dest="ssl", action="store_false")
    parser.add_argument("--skip-build", action="store_true", help="Skip npm install + build")
    parser.add_argument("--industry", help="Industry preset")
    parser.add_argument("--company-id", help="Company ID")
    parser.add_argument("--action-name", help="Action name (for in-module-feature-add)")
    parser.add_argument("--src-root", help="Source root")
    parser.add_argument("--target", help="Target environment")
    parser.add_argument("--variant-id", help="DGM variant ID")
    parser.add_argument("--feature-name", help="Feature name (for get-implementation-guide)")
    parser.add_argument("--topic", help="Topic (for research-business-rule)")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--prefix", default=None,
                        help="Table/action namespace for os-generate-module")
    parser.add_argument("--description", default=None,
                        help="Business description for os-generate-module")
    parser.add_argument("--entities", default=None,
                        help="JSON list of entity definitions for os-generate-module")
    parser.add_argument("--output-dir", default=None,
                        help="Output directory for os-generate-module")

    args, unknown = parser.parse_known_args()
    check_unknown_args(parser, unknown)

    handler = ACTIONS.get(args.action)
    if handler is None:
        err(f"Unknown action: {args.action}")
    handler(args)


if __name__ == "__main__":
    main()
