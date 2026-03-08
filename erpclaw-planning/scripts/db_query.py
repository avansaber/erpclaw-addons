#!/usr/bin/env python3
"""ERPClaw Planning -- unified action router.

Usage: python3 db_query.py --action <action-name> [flags]

Routes all 30 planning actions to scenario, forecast, and budget domain modules.
"""
import argparse
import os
import sys

# Shared library
sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
# Domain modules (same directory)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))

from erpclaw_lib.db import get_connection
from erpclaw_lib.response import ok, err

from scenarios import ACTIONS as SCENARIO_ACTIONS
from forecasts import ACTIONS as FORECAST_ACTIONS
from budgets import ACTIONS as BUDGET_ACTIONS

# Merge all actions
ACTIONS = {}
ACTIONS.update(SCENARIO_ACTIONS)
ACTIONS.update(FORECAST_ACTIONS)
ACTIONS.update(BUDGET_ACTIONS)


def build_parser():
    parser = argparse.ArgumentParser(description="ERPClaw Planning -- budgets, scenarios, and forecasts")
    parser.add_argument("--action", required=True, help="Action to execute")

    # Common flags
    parser.add_argument("--company-id")
    parser.add_argument("--name")
    parser.add_argument("--description")

    # Scenario flags
    parser.add_argument("--scenario-id")
    parser.add_argument("--scenario-type")
    parser.add_argument("--base-scenario-id")
    parser.add_argument("--fiscal-year")
    parser.add_argument("--assumptions")

    # Scenario line flags
    parser.add_argument("--scenario-line-id")
    parser.add_argument("--account-name")
    parser.add_argument("--account-type")
    parser.add_argument("--period")
    parser.add_argument("--amount")
    parser.add_argument("--notes")

    # Scenario comparison flags
    parser.add_argument("--scenario-id-1")
    parser.add_argument("--scenario-id-2")

    # Forecast flags
    parser.add_argument("--forecast-id")
    parser.add_argument("--forecast-type")
    parser.add_argument("--period-type")
    parser.add_argument("--start-period")
    parser.add_argument("--end-period")

    # Forecast line flags
    parser.add_argument("--forecast-line-id")
    parser.add_argument("--forecast-amount")
    parser.add_argument("--actual-amount")

    # Budget flags
    parser.add_argument("--budget-id")
    parser.add_argument("--budget-id-1")
    parser.add_argument("--budget-id-2")

    # Common optional
    parser.add_argument("--status")
    parser.add_argument("--search")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    action = args.action

    if action == "status":
        ok({
            "skill": "erpclaw-planning",
            "version": "1.0.0",
            "actions_available": len(ACTIONS),
            "tables": [
                "planning_scenario",
                "planning_scenario_line",
                "forecast",
                "forecast_line",
            ],
            "status": "ok",
        })
        return

    if action not in ACTIONS:
        err(f"Unknown action: {action}. Available: {', '.join(sorted(ACTIONS.keys()))}")

    conn = get_connection()
    try:
        ACTIONS[action](conn, args)
    except SystemExit:
        raise
    except Exception as e:
        err(f"Internal error in {action}: {str(e)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
