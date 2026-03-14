#!/usr/bin/env python3
"""ERPClaw Approvals -- db_query.py (unified router)

Multi-step approval workflows: rules, steps, and requests.
All 13 actions are routed through this single entry point.

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
    from erpclaw_lib.response import ok, err
    from erpclaw_lib.args import SafeArgumentParser, check_unknown_args
except ImportError:
    import json as _json
    print(_json.dumps({
        "status": "error",
        "error": "ERPClaw foundation not installed. Install erpclaw-setup first: clawhub install erpclaw-setup",
        "suggestion": "clawhub install erpclaw-setup"
    }))
    sys.exit(1)

# Add this script's directory so domain modules can be imported
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from approvals import ACTIONS  # noqa: E402

SKILL = "erpclaw-approvals"
REQUIRED_TABLES = ["company", "approval_rule", "approval_step", "approval_request"]


def main():
    parser = SafeArgumentParser(description=SKILL)
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # Entity IDs
    parser.add_argument("--id")
    parser.add_argument("--company-id")
    parser.add_argument("--rule-id")

    # Rule fields
    parser.add_argument("--name")
    parser.add_argument("--entity-type")
    parser.add_argument("--conditions")
    parser.add_argument("--is-active", type=int)

    # Step fields
    parser.add_argument("--step-order", type=int)
    parser.add_argument("--approver")
    parser.add_argument("--approval-type")
    parser.add_argument("--is-required", type=int)

    # Request fields
    parser.add_argument("--entity-id")
    parser.add_argument("--requested-by")
    parser.add_argument("--notes")

    # Filters
    parser.add_argument("--status")
    parser.add_argument("--search")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--offset", type=int, default=0)

    args, unknown = parser.parse_known_args()
    check_unknown_args(parser, unknown)

    # DB setup
    db_path = args.db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    ensure_db_exists(db_path)
    conn = get_connection(db_path)

    # Check required tables exist
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    missing = [t for t in REQUIRED_TABLES if t not in tables]
    if missing:
        conn.close()
        err(f"Missing tables: {', '.join(missing)}. Run init_db.py first.",
            suggestion="python3 init_db.py")

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
