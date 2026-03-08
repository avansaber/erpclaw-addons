#!/usr/bin/env python3
"""erpclaw-crm-adv -- db_query.py (unified router)

Advanced CRM & marketing automation. Routes all 47 actions
across 5 domain modules: campaigns, territories, contracts, automation, reports.

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

from campaigns import ACTIONS as CAMP_ACTIONS
from territories import ACTIONS as TER_ACTIONS
from contracts import ACTIONS as CTR_ACTIONS
from automation import ACTIONS as AUTO_ACTIONS
from reports import ACTIONS as RPT_ACTIONS

# ---------------------------------------------------------------------------
# Merge all domain actions into one router
# ---------------------------------------------------------------------------
SKILL = "erpclaw-crm-adv"
REQUIRED_TABLES = ["company", "crmadv_email_campaign"]

ACTIONS = {}
ACTIONS.update(CAMP_ACTIONS)
ACTIONS.update(TER_ACTIONS)
ACTIONS.update(CTR_ACTIONS)
ACTIONS.update(AUTO_ACTIONS)
ACTIONS.update(RPT_ACTIONS)


def main():
    parser = argparse.ArgumentParser(description="erpclaw-crm-adv")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # -- Shared IDs --
    parser.add_argument("--company-id")
    parser.add_argument("--campaign-id")
    parser.add_argument("--template-id")
    parser.add_argument("--recipient-list-id")
    parser.add_argument("--territory-id")
    parser.add_argument("--contract-id")
    parser.add_argument("--workflow-id")
    parser.add_argument("--obligation-id")

    # -- Campaign fields --
    parser.add_argument("--name")
    parser.add_argument("--subject")
    parser.add_argument("--subject-template")
    parser.add_argument("--body-html")
    parser.add_argument("--body-text")
    parser.add_argument("--template-type")
    parser.add_argument("--scheduled-date")
    parser.add_argument("--campaign-status-filter")
    parser.add_argument("--recipient-email")
    parser.add_argument("--event-type")
    parser.add_argument("--event-timestamp")
    parser.add_argument("--metadata")
    parser.add_argument("--list-type")
    parser.add_argument("--filter-criteria")

    # -- Territory fields --
    parser.add_argument("--region")
    parser.add_argument("--parent-territory-id")
    parser.add_argument("--territory-type")
    parser.add_argument("--salesperson")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--period")
    parser.add_argument("--quota-amount")

    # -- Contract fields --
    parser.add_argument("--customer-name")
    parser.add_argument("--contract-type")
    parser.add_argument("--total-value")
    parser.add_argument("--annual-value")
    parser.add_argument("--auto-renew")
    parser.add_argument("--renewal-terms")
    parser.add_argument("--contract-status-filter")
    parser.add_argument("--description")
    parser.add_argument("--due-date")
    parser.add_argument("--obligee")
    parser.add_argument("--obligation-status-filter")

    # -- Automation fields --
    parser.add_argument("--trigger-event")
    parser.add_argument("--conditions-json")
    parser.add_argument("--actions-json")
    parser.add_argument("--workflow-status-filter")
    parser.add_argument("--criteria-json")
    parser.add_argument("--points", type=int)
    parser.add_argument("--steps-json")

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
        _dep["suggestion"] = "clawhub install erpclaw-setup && clawhub install erpclaw-crm-adv"
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
