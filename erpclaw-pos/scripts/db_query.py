#!/usr/bin/env python3
"""erpclaw-pos — db_query.py (unified router)

Point of Sale skill for ERPClaw. Routes all 29 actions across 4 domain
modules: profiles, sessions, transactions, reports.

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

from profiles import ACTIONS as PROFILES_ACTIONS
from sessions import ACTIONS as SESSIONS_ACTIONS
from transactions import ACTIONS as TRANSACTIONS_ACTIONS
from reports import ACTIONS as REPORTS_ACTIONS

# ---------------------------------------------------------------------------
# Merge all domain actions into one router
# ---------------------------------------------------------------------------
SKILL = "erpclaw-pos"
REQUIRED_TABLES = ["company", "pos_profile", "pos_session"]

ACTIONS = {}
ACTIONS.update(PROFILES_ACTIONS)
ACTIONS.update(SESSIONS_ACTIONS)
ACTIONS.update(TRANSACTIONS_ACTIONS)
ACTIONS.update(REPORTS_ACTIONS)


def main():
    parser = argparse.ArgumentParser(description="erpclaw-pos")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # -- Identity --
    parser.add_argument("--id")
    parser.add_argument("--company-id")

    # -- Profiles --
    parser.add_argument("--name")
    parser.add_argument("--warehouse-id")
    parser.add_argument("--price-list-id")
    parser.add_argument("--default-payment-method")
    parser.add_argument("--allow-discount")
    parser.add_argument("--max-discount-pct")
    parser.add_argument("--auto-print-receipt")
    parser.add_argument("--is-active")

    # -- Sessions --
    parser.add_argument("--pos-profile-id")
    parser.add_argument("--cashier-name")
    parser.add_argument("--opening-amount")
    parser.add_argument("--closing-amount")

    # -- Transactions --
    parser.add_argument("--pos-session-id")
    parser.add_argument("--pos-transaction-id")
    parser.add_argument("--pos-transaction-item-id")
    parser.add_argument("--customer-id")
    parser.add_argument("--customer-name")

    # -- Items --
    parser.add_argument("--item-id")
    parser.add_argument("--item-name")
    parser.add_argument("--qty")
    parser.add_argument("--rate")
    parser.add_argument("--uom")
    parser.add_argument("--barcode")

    # -- Discounts --
    parser.add_argument("--discount-pct")
    parser.add_argument("--discount-amount")

    # -- Payments --
    parser.add_argument("--payment-method",
                        choices=["cash", "card", "mobile", "check", "gift_card", "other"])
    parser.add_argument("--amount")
    parser.add_argument("--reference")

    # -- Reports --
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--date")

    # -- Shared --
    parser.add_argument("--search")
    parser.add_argument("--status")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)

    args, _unknown = parser.parse_known_args()
    check_input_lengths(args)

    db_path = args.db_path or DEFAULT_DB_PATH
    ensure_db_exists(db_path)
    conn = get_connection(db_path)

    _dep = check_required_tables(conn, REQUIRED_TABLES)
    if _dep:
        _dep["suggestion"] = "clawhub install erpclaw-setup && clawhub install erpclaw-pos"
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
