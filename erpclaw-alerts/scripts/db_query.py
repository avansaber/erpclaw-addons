#!/usr/bin/env python3
"""ERPClaw Alerts -- db_query.py (unified router)

Configurable notification triggers: low stock, overdue invoices, expiring contracts, custom rules.
Routes all actions to the alerts domain module.

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
    from erpclaw_lib.dependencies import check_required_tables
    from erpclaw_lib.args import SafeArgumentParser
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

from alerts import ACTIONS as ALERTS_ACTIONS

# ---------------------------------------------------------------------------
# Merge all domain actions into one router
# ---------------------------------------------------------------------------
SKILL = "erpclaw-alerts"
REQUIRED_TABLES = ["company", "alert_rule"]

ACTIONS = {}
ACTIONS.update(ALERTS_ACTIONS)


def main():
    parser = SafeArgumentParser(description="erpclaw-alerts")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # -- Shared --
    parser.add_argument("--company-id")
    parser.add_argument("--search")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")

    # -- Alert Rules --
    parser.add_argument("--rule-id")
    parser.add_argument("--name")
    parser.add_argument("--description")
    parser.add_argument("--entity-type")
    parser.add_argument("--condition-json")
    parser.add_argument("--severity")
    parser.add_argument("--channel-ids")
    parser.add_argument("--cooldown-minutes", type=int)
    parser.add_argument("--is-active")

    # -- Notification Channels --
    parser.add_argument("--channel-id")
    parser.add_argument("--channel-type")
    parser.add_argument("--config-json")

    # -- Alert Logs --
    parser.add_argument("--alert-log-id")
    parser.add_argument("--entity-id")
    parser.add_argument("--message")
    parser.add_argument("--alert-status")
    parser.add_argument("--acknowledged-by")
    parser.add_argument("--channel-results")

    args = parser.parse_args()
    action = args.action

    # DB setup
    db_path = args.db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    ensure_db_exists(db_path)

    conn = get_connection(db_path) if args.db_path else get_connection()

    # Check required tables exist
    check_required_tables(conn, REQUIRED_TABLES)

    # Dispatch
    handler = ACTIONS[action]
    handler(conn, args)


if __name__ == "__main__":
    main()
