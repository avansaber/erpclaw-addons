#!/usr/bin/env python3
"""ERPClaw Integrations -- db_query.py (unified router)

Integration connectors: manage connector configs, field mappings, sync logs,
and webhook registrations for external platforms.
Routes all actions across 9 domain modules (3 core + 6 connectors-v2).

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

from connectors import ACTIONS as CONNECTOR_ACTIONS
from sync import ACTIONS as SYNC_ACTIONS
from mappings import ACTIONS as MAPPING_ACTIONS
from booking import ACTIONS as BOOKING_ACTIONS
from delivery import ACTIONS as DELIVERY_ACTIONS
from realestate import ACTIONS as REALESTATE_ACTIONS
from financial import ACTIONS as FINANCIAL_ACTIONS
from productivity import ACTIONS as PRODUCTIVITY_ACTIONS
from connv2_reports import ACTIONS as CONNV2_REPORTS_ACTIONS

# ---------------------------------------------------------------------------
# Merge all domain actions into one router
# ---------------------------------------------------------------------------
SKILL = "erpclaw-integrations"
REQUIRED_TABLES = ["company", "integration_connector", "connv2_booking_connector"]

ACTIONS = {}
ACTIONS.update(CONNECTOR_ACTIONS)
ACTIONS.update(SYNC_ACTIONS)
ACTIONS.update(MAPPING_ACTIONS)
ACTIONS.update(BOOKING_ACTIONS)
ACTIONS.update(DELIVERY_ACTIONS)
ACTIONS.update(REALESTATE_ACTIONS)
ACTIONS.update(FINANCIAL_ACTIONS)
ACTIONS.update(PRODUCTIVITY_ACTIONS)
# Remove "status" from connv2_reports before merging to avoid overwriting existing status
connv2_reports_filtered = {k: v for k, v in CONNV2_REPORTS_ACTIONS.items() if k != "status"}
ACTIONS.update(connv2_reports_filtered)


# ---------------------------------------------------------------------------
# Status action (skill health check)
# ---------------------------------------------------------------------------
def status_action(conn, args):
    connector_count = conn.execute("SELECT COUNT(*) FROM integration_connector").fetchone()[0]
    active_connectors = conn.execute(
        "SELECT COUNT(*) FROM integration_connector WHERE connector_status = 'active'"
    ).fetchone()[0]
    sync_count = conn.execute("SELECT COUNT(*) FROM integration_sync").fetchone()[0]
    mapping_count = conn.execute("SELECT COUNT(*) FROM integration_field_mapping").fetchone()[0]
    entity_map_count = conn.execute("SELECT COUNT(*) FROM integration_entity_map").fetchone()[0]

    # Connectors V2 table counts
    connv2_counts = {}
    for tbl in [
        "connv2_booking_connector", "connv2_booking_sync_log",
        "connv2_delivery_connector", "connv2_delivery_order",
        "connv2_realestate_connector", "connv2_realestate_lead",
        "connv2_financial_connector",
        "connv2_productivity_connector",
    ]:
        try:
            connv2_counts[tbl] = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        except Exception:
            connv2_counts[tbl] = -1

    ok({
        "skill": SKILL,
        "version": "2.0.0",
        "tables": 17,
        "actions": len(ACTIONS) + 1,
        "connectors": connector_count,
        "active_connectors": active_connectors,
        "syncs": sync_count,
        "field_mappings": mapping_count,
        "entity_maps": entity_map_count,
        "connv2_record_counts": connv2_counts,
    })


ACTIONS["status"] = status_action


def main():
    parser = argparse.ArgumentParser(description="erpclaw-integrations")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # -- Shared IDs --
    parser.add_argument("--company-id")
    parser.add_argument("--connector-id")
    parser.add_argument("--order-id")

    # -- Shared --
    parser.add_argument("--search")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)

    # -- Connector domain --
    parser.add_argument("--name")
    parser.add_argument("--platform")
    parser.add_argument("--connector-type")
    parser.add_argument("--connector-status")
    parser.add_argument("--base-url")
    parser.add_argument("--config-json")

    # -- Credential domain --
    parser.add_argument("--credential-id")
    parser.add_argument("--credential-type")
    parser.add_argument("--credential-key")
    parser.add_argument("--credential-value")
    parser.add_argument("--expires-at")

    # -- Webhook domain --
    parser.add_argument("--webhook-id")
    parser.add_argument("--event-type")
    parser.add_argument("--webhook-url")
    parser.add_argument("--webhook-secret")

    # -- Sync domain --
    parser.add_argument("--sync-id")
    parser.add_argument("--sync-type")
    parser.add_argument("--sync-status")
    parser.add_argument("--direction")
    parser.add_argument("--entity-type")
    parser.add_argument("--entity-id")
    parser.add_argument("--error-message")
    parser.add_argument("--error-id")
    parser.add_argument("--resolution-notes")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")

    # -- Schedule domain --
    parser.add_argument("--schedule-id")
    parser.add_argument("--frequency")
    parser.add_argument("--next-run-at")
    parser.add_argument("--is-active")

    # -- Mapping domain --
    parser.add_argument("--field-mapping-id")
    parser.add_argument("--source-field")
    parser.add_argument("--target-field")
    parser.add_argument("--transform-rule")
    parser.add_argument("--is-required")
    parser.add_argument("--default-value")

    # -- Entity map domain --
    parser.add_argument("--entity-map-id")
    parser.add_argument("--local-id")
    parser.add_argument("--remote-id")

    # -- Transform rule domain --
    parser.add_argument("--transform-rule-id")
    parser.add_argument("--rule-name")
    parser.add_argument("--rule-json")

    # -- Booking domain (connectors-v2) --
    parser.add_argument("--property-id")
    parser.add_argument("--api-credentials-ref")
    parser.add_argument("--sync-reservations")
    parser.add_argument("--sync-rates")
    parser.add_argument("--sync-availability")
    parser.add_argument("--records-synced")
    parser.add_argument("--errors")

    # -- Delivery domain (connectors-v2) --
    parser.add_argument("--store-id")
    parser.add_argument("--auto-accept")
    parser.add_argument("--sync-menu")
    parser.add_argument("--external-order-id")
    parser.add_argument("--order-data")
    parser.add_argument("--total-amount")
    parser.add_argument("--commission")
    parser.add_argument("--net-amount")
    parser.add_argument("--order-status")

    # -- Real estate domain (connectors-v2) --
    parser.add_argument("--agent-id")
    parser.add_argument("--sync-listings")
    parser.add_argument("--capture-leads")
    parser.add_argument("--lead-source")
    parser.add_argument("--contact-name")
    parser.add_argument("--contact-email")
    parser.add_argument("--contact-phone")
    parser.add_argument("--property-ref")
    parser.add_argument("--inquiry")

    # -- Financial domain (connectors-v2) --
    parser.add_argument("--account-ref")
    parser.add_argument("--sync-enabled")
    parser.add_argument("--recipient")
    parser.add_argument("--message-body")
    parser.add_argument("--subject")

    # -- Productivity domain (connectors-v2) --
    parser.add_argument("--workspace-id")
    parser.add_argument("--sync-calendar")
    parser.add_argument("--sync-contacts")
    parser.add_argument("--sync-files")

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
