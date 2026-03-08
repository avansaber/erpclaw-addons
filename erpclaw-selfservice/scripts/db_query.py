#!/usr/bin/env python3
"""ERPClaw Self-Service -- db_query.py (unified router)

Generic self-service permission layer. Routes all 25 actions
across 4 domain modules: permissions, portal, sessions, reports.

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

from permissions import ACTIONS as PERM_ACTIONS
from portal import ACTIONS as PORTAL_ACTIONS
from sessions import ACTIONS as SESS_ACTIONS
from reports import ACTIONS as RPT_ACTIONS

# ---------------------------------------------------------------------------
# Merge all domain actions into one router
# ---------------------------------------------------------------------------
SKILL = "erpclaw-selfservice"
REQUIRED_TABLES = ["company", "selfservice_permission_profile"]

ACTIONS = {}
ACTIONS.update(PERM_ACTIONS)
ACTIONS.update(PORTAL_ACTIONS)
ACTIONS.update(SESS_ACTIONS)
ACTIONS.update(RPT_ACTIONS)


def main():
    parser = argparse.ArgumentParser(description="erpclaw-selfservice")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # -- Shared IDs --
    parser.add_argument("--company-id")
    parser.add_argument("--profile-id")
    parser.add_argument("--permission-id")
    parser.add_argument("--portal-id")
    parser.add_argument("--session-id")
    parser.add_argument("--user-id")

    # -- Profile fields --
    parser.add_argument("--name")
    parser.add_argument("--description")
    parser.add_argument("--target-role")
    parser.add_argument("--allowed-actions")
    parser.add_argument("--denied-actions")
    parser.add_argument("--record-scope")
    parser.add_argument("--field-visibility")

    # -- Permission fields --
    parser.add_argument("--user-email")
    parser.add_argument("--user-name")
    parser.add_argument("--assigned-by")

    # -- Portal config fields --
    parser.add_argument("--branding-json")
    parser.add_argument("--welcome-message")
    parser.add_argument("--enabled-modules")
    parser.add_argument("--enabled-actions")
    parser.add_argument("--require-mfa", type=int)
    parser.add_argument("--session-timeout-minutes", type=int)

    # -- Session fields --
    parser.add_argument("--token")
    parser.add_argument("--expires-at")
    parser.add_argument("--ip-address")
    parser.add_argument("--user-agent")

    # -- Activity log fields --
    parser.add_argument("--action-name")
    parser.add_argument("--entity-type")
    parser.add_argument("--entity-id")
    parser.add_argument("--result")

    # -- Shared --
    parser.add_argument("--search")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)

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
