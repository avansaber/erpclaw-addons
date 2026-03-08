#!/usr/bin/env python3
"""ERPClaw Compliance -- db_query.py (unified router)

Compliance, audit, risk, and policy management for ERPClaw.
Routes all actions across 4 domain modules: audit, risk, controls, policy.

Usage: python3 db_query.py --action <action-name> [--flags ...]
Output: JSON to stdout, exit 0 on success, exit 1 on error.
"""
import argparse
import json
import os
import sys

# Add shared lib to path
try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection, ensure_db_exists, DEFAULT_DB_PATH
    from erpclaw_lib.validation import check_input_lengths
    from erpclaw_lib.response import ok, err
    from erpclaw_lib.dependencies import check_required_tables
except ImportError:
    import json as _json
    print(_json.dumps({
        "status": "error",
        "error": "ERPClaw foundation not installed. Install erpclaw-setup first: clawhub install erpclaw-setup",
        "suggestion": "clawhub install erpclaw-setup"
    }))
    sys.exit(1)

# Add this script's directory so domain modules can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from audit import ACTIONS as AUDIT_ACTIONS
from risk import ACTIONS as RISK_ACTIONS
from controls import ACTIONS as CONTROLS_ACTIONS
from policy import ACTIONS as POLICY_ACTIONS

# ---------------------------------------------------------------------------
# Merge all domain actions into one router
# ---------------------------------------------------------------------------
SKILL = "erpclaw-compliance"
REQUIRED_TABLES = ["company", "audit_plan"]

ACTIONS = {}
ACTIONS.update(AUDIT_ACTIONS)
ACTIONS.update(RISK_ACTIONS)
ACTIONS.update(CONTROLS_ACTIONS)
ACTIONS.update(POLICY_ACTIONS)


def main():
    parser = argparse.ArgumentParser(description="erpclaw-compliance")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # -- Shared IDs --
    parser.add_argument("--company-id")

    # -- Shared --
    parser.add_argument("--search")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--notes")
    parser.add_argument("--status")
    parser.add_argument("--description")

    # -- Audit domain --
    parser.add_argument("--audit-plan-id")
    parser.add_argument("--name")
    parser.add_argument("--audit-type")
    parser.add_argument("--scope")
    parser.add_argument("--lead-auditor")
    parser.add_argument("--planned-start")
    parser.add_argument("--planned-end")
    parser.add_argument("--title")
    parser.add_argument("--finding-type")
    parser.add_argument("--area")
    parser.add_argument("--root-cause")
    parser.add_argument("--recommendation")
    parser.add_argument("--remediation-due")
    parser.add_argument("--remediation-status")
    parser.add_argument("--assigned-to")

    # -- Risk domain --
    parser.add_argument("--risk-id")
    parser.add_argument("--category")
    parser.add_argument("--likelihood", type=int)
    parser.add_argument("--impact", type=int)
    parser.add_argument("--owner")
    parser.add_argument("--mitigation-plan")
    parser.add_argument("--residual-likelihood", type=int)
    parser.add_argument("--residual-impact", type=int)
    parser.add_argument("--review-date")
    parser.add_argument("--risk-level")
    parser.add_argument("--assessor")

    # -- Controls domain --
    parser.add_argument("--control-test-id")
    parser.add_argument("--control-name")
    parser.add_argument("--control-description")
    parser.add_argument("--control-type")
    parser.add_argument("--frequency")
    parser.add_argument("--test-date")
    parser.add_argument("--tester")
    parser.add_argument("--test-procedure")
    parser.add_argument("--test-result")
    parser.add_argument("--evidence")
    parser.add_argument("--deficiency-type")
    parser.add_argument("--next-test-date")

    # -- Calendar domain --
    parser.add_argument("--calendar-item-id")
    parser.add_argument("--compliance-type")
    parser.add_argument("--due-date")
    parser.add_argument("--reminder-days", type=int)
    parser.add_argument("--responsible")
    parser.add_argument("--recurrence")

    # -- Policy domain --
    parser.add_argument("--policy-id")
    parser.add_argument("--policy-type")
    parser.add_argument("--version")
    parser.add_argument("--content")
    parser.add_argument("--effective-date")
    parser.add_argument("--requires-acknowledgment")
    parser.add_argument("--employee-name")
    parser.add_argument("--employee-id")
    parser.add_argument("--ip-address")

    args, _unknown = parser.parse_known_args()
    check_input_lengths(args)

    db_path = args.db_path or DEFAULT_DB_PATH
    ensure_db_exists(db_path)
    conn = get_connection(db_path)

    _dep = check_required_tables(conn, REQUIRED_TABLES)
    if _dep:
        _dep["suggestion"] = "clawhub install erpclaw-setup && python3 init_db.py"
        print(json.dumps(_dep, indent=2))
        conn.close()
        sys.exit(1)

    try:
        ACTIONS[args.action](conn, args)
    except Exception as e:
        conn.rollback()
        sys.stderr.write(f"[{SKILL}] {e}\n")
        err(str(e))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
