#!/usr/bin/env python3
"""ERPClaw Advanced Manufacturing -- db_query.py (unified router)

Shop Floor Control, Tool Management, ECOs, Process Recipes.
Routes all 35 actions across 4 domain modules: shop_floor, tools, eco, recipes.

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
    from erpclaw_lib.naming import register_prefix
    from erpclaw_lib.args import SafeArgumentParser, check_unknown_args
except ImportError:
    import json as _json
    print(_json.dumps({
        "status": "error",
        "error": "ERPClaw foundation not installed. Install erpclaw-setup first: clawhub install erpclaw-setup",
        "suggestion": "clawhub install erpclaw-setup"
    }))
    sys.exit(1)

# Register naming prefixes
register_prefix("shop_floor_entry", "SFE-")
register_prefix("tool", "TOOL-")
register_prefix("engineering_change_order", "ECO-")
register_prefix("process_recipe", "RCPE-")

# Add this script's directory so domain modules can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shop_floor import ACTIONS as SF_ACTIONS
from tools import ACTIONS as TOOL_ACTIONS
from eco import ACTIONS as ECO_ACTIONS
from recipes import ACTIONS as RECIPE_ACTIONS

# ---------------------------------------------------------------------------
# Merge all domain actions into one router
# ---------------------------------------------------------------------------
SKILL = "erpclaw-advmfg"
REQUIRED_TABLES = ["company", "shop_floor_entry"]

ACTIONS = {}
ACTIONS.update(SF_ACTIONS)
ACTIONS.update(TOOL_ACTIONS)
ACTIONS.update(ECO_ACTIONS)
ACTIONS.update(RECIPE_ACTIONS)


def main():
    parser = SafeArgumentParser(description="erpclaw-advmfg")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # -- Shop Floor --
    parser.add_argument("--entry-id")
    parser.add_argument("--company-id")
    parser.add_argument("--equipment-id")
    parser.add_argument("--work-order-id")
    parser.add_argument("--operator")
    parser.add_argument("--entry-type")
    parser.add_argument("--start-time")
    parser.add_argument("--machine-status")
    parser.add_argument("--batch-number")
    parser.add_argument("--serial-number")
    parser.add_argument("--quantity-produced")
    parser.add_argument("--quantity-rejected")

    # -- Tools --
    parser.add_argument("--tool-id")
    parser.add_argument("--name")
    parser.add_argument("--tool-type")
    parser.add_argument("--tool-code")
    parser.add_argument("--manufacturer")
    parser.add_argument("--model")
    parser.add_argument("--location")
    parser.add_argument("--purchase-date")
    parser.add_argument("--purchase-cost")
    parser.add_argument("--max-usage-count")
    parser.add_argument("--calibration-due")
    parser.add_argument("--condition")
    parser.add_argument("--tool-status")
    parser.add_argument("--usage-count")
    parser.add_argument("--usage-duration-minutes")
    parser.add_argument("--condition-after")

    # -- ECO --
    parser.add_argument("--eco-id")
    parser.add_argument("--title")
    parser.add_argument("--eco-type")
    parser.add_argument("--description")
    parser.add_argument("--reason")
    parser.add_argument("--affected-items")
    parser.add_argument("--affected-boms")
    parser.add_argument("--impact-analysis")
    parser.add_argument("--requested-by")
    parser.add_argument("--approved-by")
    parser.add_argument("--priority")
    parser.add_argument("--implementation-date")
    parser.add_argument("--eco-status")

    # -- Recipes --
    parser.add_argument("--recipe-id")
    parser.add_argument("--product-name")
    parser.add_argument("--recipe-type")
    parser.add_argument("--version")
    parser.add_argument("--batch-size")
    parser.add_argument("--batch-unit")
    parser.add_argument("--expected-yield")
    parser.add_argument("--instructions")
    parser.add_argument("--is-active")
    parser.add_argument("--ingredient-id")
    parser.add_argument("--ingredient-name")
    parser.add_argument("--item-id")
    parser.add_argument("--quantity")
    parser.add_argument("--unit")
    parser.add_argument("--sequence")
    parser.add_argument("--is-optional")

    # -- Shared --
    parser.add_argument("--notes")
    parser.add_argument("--search")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)

    args, unknown = parser.parse_known_args()
    check_unknown_args(parser, unknown)
    check_input_lengths(args)

    db_path = args.db_path or DEFAULT_DB_PATH
    ensure_db_exists(db_path)
    conn = get_connection(db_path)

    _dep = check_required_tables(conn, REQUIRED_TABLES)
    if _dep:
        _dep["suggestion"] = "clawhub install erpclaw-setup && clawhub install erpclaw-advmfg"
        print(json.dumps(_dep, indent=2))
        conn.close()
        sys.exit(1)

    try:
        ACTIONS[args.action](conn, args)
    except SystemExit:
        raise
    except Exception as e:
        conn.rollback()
        sys.stderr.write(f"[{SKILL}] {e}\n")
        err(str(e))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
