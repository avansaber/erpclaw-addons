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
import json
import os
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
    candidates.append(os.path.expanduser("~/.openclaw/erpclaw/lib"))
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
# Wrappers for actions that don't follow the handle_* convention
# ---------------------------------------------------------------------------

def handle_generate_module(args):
    """Wrap generate_module() function for action dispatch."""
    result = generate_module(args)
    if isinstance(result, dict) and "error" in result:
        err(result["error"])
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
    "os-deploy-module": handle_deploy_module,
    "os-deploy-audit-log": handle_deploy_audit_log,
    "os-install-suite": handle_install_suite,
    # Audit
    "os-run-audit": handle_run_audit,
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

    args, unknown = parser.parse_known_args()
    check_unknown_args(parser, unknown)

    handler = ACTIONS.get(args.action)
    if handler is None:
        err(f"Unknown action: {args.action}")
    handler(args)


if __name__ == "__main__":
    main()
