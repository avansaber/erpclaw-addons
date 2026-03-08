#!/usr/bin/env python3
"""ERPClaw Maintenance — unified action router.

Usage: python3 db_query.py --action <action-name> [flags]

Routes all 39 maintenance actions to domain modules:
  equipment.py (10), plans.py (6), work_orders.py (12),
  checklists.py (4), reports.py (7).
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

from equipment import ACTIONS as EQUIPMENT_ACTIONS
from plans import ACTIONS as PLANS_ACTIONS
from work_orders import ACTIONS as WORK_ORDERS_ACTIONS
from checklists import ACTIONS as CHECKLISTS_ACTIONS
from reports import ACTIONS as REPORTS_ACTIONS

# Merge all actions
ACTIONS = {}
ACTIONS.update(EQUIPMENT_ACTIONS)
ACTIONS.update(PLANS_ACTIONS)
ACTIONS.update(WORK_ORDERS_ACTIONS)
ACTIONS.update(CHECKLISTS_ACTIONS)
ACTIONS.update(REPORTS_ACTIONS)


def build_parser():
    parser = argparse.ArgumentParser(description="ERPClaw Maintenance — equipment & maintenance management")
    parser.add_argument("--action", required=True, help="Action to execute")

    # Common flags
    parser.add_argument("--company-id")
    parser.add_argument("--equipment-id")
    parser.add_argument("--name")

    # Equipment flags
    parser.add_argument("--equipment-type")
    parser.add_argument("--model")
    parser.add_argument("--manufacturer")
    parser.add_argument("--serial-number")
    parser.add_argument("--location")
    parser.add_argument("--parent-equipment-id")
    parser.add_argument("--asset-id")
    parser.add_argument("--item-id")
    parser.add_argument("--purchase-date")
    parser.add_argument("--warranty-expiry")
    parser.add_argument("--criticality")
    parser.add_argument("--equipment-status")

    # Reading flags
    parser.add_argument("--reading-type")
    parser.add_argument("--reading-value")
    parser.add_argument("--reading-unit")
    parser.add_argument("--reading-date")
    parser.add_argument("--recorded-by")

    # Plan flags
    parser.add_argument("--plan-id")
    parser.add_argument("--plan-name")
    parser.add_argument("--plan-type")
    parser.add_argument("--frequency")
    parser.add_argument("--frequency-days", type=int)
    parser.add_argument("--last-performed")
    parser.add_argument("--next-due")
    parser.add_argument("--estimated-duration")
    parser.add_argument("--estimated-cost")
    parser.add_argument("--assigned-to")
    parser.add_argument("--instructions")
    parser.add_argument("--is-active", type=int)
    parser.add_argument("--item-name")
    parser.add_argument("--quantity")

    # Work order flags
    parser.add_argument("--work-order-id")
    parser.add_argument("--work-order-type")
    parser.add_argument("--priority")
    parser.add_argument("--description")
    parser.add_argument("--scheduled-date")
    parser.add_argument("--failure-mode")
    parser.add_argument("--root-cause")
    parser.add_argument("--resolution")
    parser.add_argument("--actual-duration")
    parser.add_argument("--actual-cost")
    parser.add_argument("--wo-status")
    parser.add_argument("--unit-cost")

    # Checklist flags
    parser.add_argument("--checklist-id")
    parser.add_argument("--checklist-name")
    parser.add_argument("--checklist-item-id")
    parser.add_argument("--sort-order", type=int, default=0)
    parser.add_argument("--completed-by")

    # Downtime flags
    parser.add_argument("--start-time")
    parser.add_argument("--end-time")
    parser.add_argument("--duration-hours")
    parser.add_argument("--reason")
    parser.add_argument("--impact")

    # Report flags
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--as-of-date")

    # Common optional
    parser.add_argument("--notes")
    parser.add_argument("--search")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    action = args.action

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
