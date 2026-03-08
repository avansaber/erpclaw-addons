#!/usr/bin/env python3
"""ERPClaw Fleet -- db_query.py (unified router)

Fleet management: vehicles, assignments, fuel logs, maintenance.
All 15 actions are routed through this single entry point.

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

from fleet import ACTIONS  # noqa: E402

SKILL = "erpclaw-fleet"
REQUIRED_TABLES = ["company", "fleet_vehicle", "fleet_vehicle_assignment",
                   "fleet_fuel_log", "fleet_vehicle_maintenance"]


def main():
    parser = argparse.ArgumentParser(description=SKILL)
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # Entity IDs
    parser.add_argument("--company-id")
    parser.add_argument("--vehicle-id")
    parser.add_argument("--assignment-id")
    parser.add_argument("--maintenance-id")

    # Vehicle fields
    parser.add_argument("--make")
    parser.add_argument("--model")
    parser.add_argument("--year")
    parser.add_argument("--vin")
    parser.add_argument("--license-plate")
    parser.add_argument("--vehicle-type")
    parser.add_argument("--color")
    parser.add_argument("--purchase-date")
    parser.add_argument("--purchase-cost")
    parser.add_argument("--current-odometer")
    parser.add_argument("--fuel-type")
    parser.add_argument("--insurance-provider")
    parser.add_argument("--insurance-policy")
    parser.add_argument("--insurance-expiry")
    parser.add_argument("--vehicle-status")
    parser.add_argument("--notes")

    # Assignment fields
    parser.add_argument("--driver-name")
    parser.add_argument("--driver-id")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--assignment-status")

    # Fuel log fields
    parser.add_argument("--log-date")
    parser.add_argument("--gallons")
    parser.add_argument("--cost")
    parser.add_argument("--odometer-reading")
    parser.add_argument("--station")

    # Maintenance fields
    parser.add_argument("--maintenance-type")
    parser.add_argument("--scheduled-date")
    parser.add_argument("--completed-date")
    parser.add_argument("--vendor")
    parser.add_argument("--odometer-at-service")
    parser.add_argument("--maintenance-status")

    # Filters
    parser.add_argument("--search")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--offset", type=int, default=0)

    args, _unknown = parser.parse_known_args()

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
