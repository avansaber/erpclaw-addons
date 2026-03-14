#!/usr/bin/env python3
"""ERPClaw Logistics -- db_query.py (unified router)

Transportation & logistics management: shipments, carriers, routes, freight.
All 33 actions are routed through this single entry point.

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

from shipments import ACTIONS as SHIPMENT_ACTIONS  # noqa: E402
from carriers import ACTIONS as CARRIER_ACTIONS  # noqa: E402
from routes import ACTIONS as ROUTE_ACTIONS  # noqa: E402
from freight import ACTIONS as FREIGHT_ACTIONS  # noqa: E402
from reports import ACTIONS as REPORT_ACTIONS  # noqa: E402

# Merge all actions
ACTIONS = {}
ACTIONS.update(SHIPMENT_ACTIONS)
ACTIONS.update(CARRIER_ACTIONS)
ACTIONS.update(ROUTE_ACTIONS)
ACTIONS.update(FREIGHT_ACTIONS)
ACTIONS.update(REPORT_ACTIONS)

SKILL = "erpclaw-logistics"
REQUIRED_TABLES = [
    "company", "logistics_shipment", "logistics_tracking_event",
    "logistics_carrier", "logistics_carrier_rate",
    "logistics_route", "logistics_route_stop",
    "logistics_freight_charge", "logistics_carrier_invoice",
]


def main():
    parser = SafeArgumentParser(description=SKILL)
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # Entity IDs
    parser.add_argument("--id")
    parser.add_argument("--company-id")
    parser.add_argument("--carrier-id")
    parser.add_argument("--shipment-id")
    parser.add_argument("--route-id")
    parser.add_argument("--supplier-id")

    # Shipment fields
    parser.add_argument("--origin-address")
    parser.add_argument("--origin-city")
    parser.add_argument("--origin-state")
    parser.add_argument("--origin-zip")
    parser.add_argument("--destination-address")
    parser.add_argument("--destination-city")
    parser.add_argument("--destination-state")
    parser.add_argument("--destination-zip")
    parser.add_argument("--service-level")
    parser.add_argument("--weight")
    parser.add_argument("--dimensions")
    parser.add_argument("--package-count", type=int)
    parser.add_argument("--declared-value")
    parser.add_argument("--reference-number")
    parser.add_argument("--shipment-status")
    parser.add_argument("--estimated-delivery")
    parser.add_argument("--shipping-cost")
    parser.add_argument("--tracking-number")
    parser.add_argument("--pod-signature")
    parser.add_argument("--pod-timestamp")
    parser.add_argument("--notes")

    # Carrier fields
    parser.add_argument("--name")
    parser.add_argument("--carrier-code")
    parser.add_argument("--contact-name")
    parser.add_argument("--contact-email")
    parser.add_argument("--contact-phone")
    parser.add_argument("--dot-number")
    parser.add_argument("--mc-number")
    parser.add_argument("--carrier-type")
    parser.add_argument("--insurance-expiry")
    parser.add_argument("--carrier-status")
    parser.add_argument("--on-time-pct")

    # Carrier rate fields
    parser.add_argument("--origin-zone")
    parser.add_argument("--destination-zone")
    parser.add_argument("--weight-min")
    parser.add_argument("--weight-max")
    parser.add_argument("--rate-per-unit")
    parser.add_argument("--flat-rate")
    parser.add_argument("--effective-date")
    parser.add_argument("--expiry-date")

    # Route fields
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--distance")
    parser.add_argument("--estimated-hours")
    parser.add_argument("--route-status")

    # Route stop fields
    parser.add_argument("--stop-order", type=int)
    parser.add_argument("--address")
    parser.add_argument("--city")
    parser.add_argument("--state")
    parser.add_argument("--zip-code")
    parser.add_argument("--estimated-arrival")
    parser.add_argument("--stop-type")

    # Freight fields
    parser.add_argument("--charge-type")
    parser.add_argument("--description")
    parser.add_argument("--amount")

    # Carrier invoice fields
    parser.add_argument("--invoice-number")
    parser.add_argument("--invoice-date")
    parser.add_argument("--total-amount")
    parser.add_argument("--invoice-status")
    parser.add_argument("--shipment-count", type=int)

    # Tracking event fields
    parser.add_argument("--event-type")
    parser.add_argument("--event-timestamp")
    parser.add_argument("--location")

    # Filters
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
