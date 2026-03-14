#!/usr/bin/env python3
"""ERPClaw Treasury -- db_query.py (unified router)

Cash management, investments, and inter-company transfers.
Routes all 35 actions across 3 domain modules: cash, investments, intercompany.

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
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cash import ACTIONS as CASH_ACTIONS
from investments import ACTIONS as INV_ACTIONS
from intercompany import ACTIONS as IC_ACTIONS

# ---------------------------------------------------------------------------
# Merge all domain actions into one router
# ---------------------------------------------------------------------------
SKILL = "erpclaw-treasury"
REQUIRED_TABLES = ["company", "bank_account_extended"]

ACTIONS = {}
ACTIONS.update(CASH_ACTIONS)
ACTIONS.update(INV_ACTIONS)
ACTIONS.update(IC_ACTIONS)


def main():
    parser = SafeArgumentParser(description="erpclaw-treasury")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # -- Bank Accounts --
    parser.add_argument("--account-id")
    parser.add_argument("--bank-name")
    parser.add_argument("--account-name")
    parser.add_argument("--account-number")
    parser.add_argument("--routing-number")
    parser.add_argument("--account-type")
    parser.add_argument("--currency")
    parser.add_argument("--current-balance")
    parser.add_argument("--gl-account-id")
    parser.add_argument("--is-active")

    # -- Cash Position --
    parser.add_argument("--position-id")
    parser.add_argument("--position-date")
    parser.add_argument("--total-cash")
    parser.add_argument("--total-receivables")
    parser.add_argument("--total-payables")

    # -- Cash Forecast --
    parser.add_argument("--forecast-id")
    parser.add_argument("--forecast-name")
    parser.add_argument("--forecast-type")
    parser.add_argument("--period-start")
    parser.add_argument("--period-end")
    parser.add_argument("--expected-inflows")
    parser.add_argument("--expected-outflows")
    parser.add_argument("--assumptions")

    # -- Investments --
    parser.add_argument("--investment-id")
    parser.add_argument("--name")
    parser.add_argument("--investment-type")
    parser.add_argument("--institution")
    parser.add_argument("--principal")
    parser.add_argument("--current-value")
    parser.add_argument("--interest-rate")
    parser.add_argument("--purchase-date")
    parser.add_argument("--maturity-date")
    parser.add_argument("--investment-status")

    # -- Investment Transactions --
    parser.add_argument("--transaction-type")
    parser.add_argument("--transaction-date")
    parser.add_argument("--amount")
    parser.add_argument("--reference")

    # -- Inter-Company Transfers --
    parser.add_argument("--transfer-id")
    parser.add_argument("--from-company-id")
    parser.add_argument("--to-company-id")
    parser.add_argument("--transfer-date")
    parser.add_argument("--reason")
    parser.add_argument("--transfer-status")
    parser.add_argument("--from-account-id")
    parser.add_argument("--to-account-id")

    # -- Maturity Alerts --
    parser.add_argument("--days")

    # -- Shared --
    parser.add_argument("--company-id")
    parser.add_argument("--notes")
    parser.add_argument("--search")
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
        _dep["suggestion"] = "clawhub install erpclaw-setup && clawhub install erpclaw-treasury"
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
