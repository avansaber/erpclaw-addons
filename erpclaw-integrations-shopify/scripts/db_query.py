#!/usr/bin/env python3
"""ERPClaw Integrations Shopify -- db_query.py (unified router)

Deep Shopify integration for full-cycle e-commerce order sync and
payout reconciliation.
Routes 66 actions across 15 domain modules:
  accounts (6): add, update, get, list, configure-gl, test-connection
  sync (10): sync-orders, sync-products, sync-customers, sync-payouts,
             sync-disputes, start-full-sync, get-sync-job, list-sync-jobs,
             cancel-sync-job, process-webhook
  mapping (6): map-product, auto-map-products, list-product-maps,
               map-customer, auto-map-customers, list-customer-maps
  gl_rules (5): add-gl-rule, update-gl-rule, list-gl-rules, delete-gl-rule,
                preview-gl
  gl_posting (8): post-order-gl, post-refund-gl, post-payout-gl,
                  post-dispute-gl, post-gift-card-gl, bulk-post-gl,
                  reverse-order-gl, post-reserve-gl
  reconciliation (6): run-reconciliation, verify-payout, clearing-balance,
                      match-bank-transaction, list-reconciliations,
                      get-reconciliation
  browse (10): list-orders, get-order, list-refunds, get-refund,
               list-payouts, get-payout, list-payout-transactions,
               list-disputes, get-dispute, order-gl-detail
  reports (7): revenue-summary, fee-summary, refund-summary,
               payout-detail-report, product-revenue-report,
               customer-revenue-report, status
  connect (1): shopify-connect (redeem pairing code)
  disconnect (1): shopify-disconnect
  status_push (1): shopify-push-status (HMAC to Worker)
  dispatcher (1): shopify-dispatch-command
  daemon (2): install-daemon, uninstall-daemon
  gdpr (1): shopify-handle-gdpr (customers/data_request, customers/redact, shop/redact)
  flush (1): shopify-flush-pending-events

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
        "error": "ERPClaw foundation not installed. Install erpclaw first: clawhub install erpclaw",
        "suggestion": "clawhub install erpclaw"
    }))
    sys.exit(1)

# Add this script's directory so domain modules can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from accounts import ACTIONS as ACCOUNTS_ACTIONS  # noqa: E402
from sync import ACTIONS as SYNC_ACTIONS  # noqa: E402
from mapping import ACTIONS as MAPPING_ACTIONS  # noqa: E402
from gl_rules import ACTIONS as GL_RULES_ACTIONS  # noqa: E402
from gl_posting import ACTIONS as GL_POSTING_ACTIONS  # noqa: E402
from reconciliation import ACTIONS as RECONCILIATION_ACTIONS  # noqa: E402
from browse import ACTIONS as BROWSE_ACTIONS  # noqa: E402
from reports import ACTIONS as REPORTS_ACTIONS  # noqa: E402
from connect import CONNECT_ACTIONS  # noqa: E402
from disconnect import DISCONNECT_ACTIONS  # noqa: E402
from status_push import STATUS_PUSH_ACTIONS  # noqa: E402
from dispatcher import DISPATCHER_ACTIONS  # noqa: E402
from daemon import DAEMON_ACTIONS  # noqa: E402
from gdpr import GDPR_ACTIONS  # noqa: E402
from flush import FLUSH_ACTIONS  # noqa: E402

# Merge all domain actions into one router
SKILL = "erpclaw-integrations-shopify"
REQUIRED_TABLES = ["company", "account", "shopify_account"]

ACTIONS = {}
ACTIONS.update(ACCOUNTS_ACTIONS)
ACTIONS.update(SYNC_ACTIONS)
ACTIONS.update(MAPPING_ACTIONS)
ACTIONS.update(GL_RULES_ACTIONS)
ACTIONS.update(GL_POSTING_ACTIONS)
ACTIONS.update(RECONCILIATION_ACTIONS)
ACTIONS.update(BROWSE_ACTIONS)
ACTIONS.update(REPORTS_ACTIONS)
ACTIONS.update(CONNECT_ACTIONS)
ACTIONS.update(DISCONNECT_ACTIONS)
ACTIONS.update(STATUS_PUSH_ACTIONS)
ACTIONS.update(DISPATCHER_ACTIONS)
ACTIONS.update(DAEMON_ACTIONS)
ACTIONS.update(GDPR_ACTIONS)
ACTIONS.update(FLUSH_ACTIONS)

ACTIONS["status"] = lambda conn, args: ok({
    "skill": SKILL,
    "version": "1.1.0",
    "actions_available": len([k for k in ACTIONS if k != "status"]),
    "domains": ["accounts", "sync", "mapping", "gl_rules", "gl_posting",
                "reconciliation", "browse", "reports", "connect", "disconnect",
                "status_push", "dispatcher", "daemon", "gdpr", "flush"],
    "database": DEFAULT_DB_PATH,
})


def main():
    parser = SafeArgumentParser(description="erpclaw-integrations-shopify")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # Shared IDs
    parser.add_argument("--company-id")

    # Shared
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--status")

    # ACCOUNTS domain
    parser.add_argument("--shopify-account-id")
    parser.add_argument("--shop-domain")
    parser.add_argument("--shop-name")

    # CONNECT / DISCONNECT / STATUS_PUSH domain
    parser.add_argument("--pairing-code")
    parser.add_argument("--worker-url")
    parser.add_argument("--erpclaw-url")
    parser.add_argument("--command-json")

    # GDPR domain
    parser.add_argument("--topic")
    parser.add_argument("--payload")
    parser.add_argument("--access-token")
    parser.add_argument("--api-version")
    parser.add_argument("--currency")
    parser.add_argument("--discount-method")
    parser.add_argument("--auto-post-gl", type=int)
    parser.add_argument("--track-cogs", type=int)

    # GL mapping
    parser.add_argument("--clearing-account-id")
    parser.add_argument("--revenue-account-id")
    parser.add_argument("--shipping-revenue-account-id")
    parser.add_argument("--tax-payable-account-id")
    parser.add_argument("--cogs-account-id")
    parser.add_argument("--inventory-account-id")
    parser.add_argument("--fee-account-id")
    parser.add_argument("--discount-account-id")
    parser.add_argument("--refund-account-id")
    parser.add_argument("--chargeback-account-id")
    parser.add_argument("--chargeback-fee-account-id")
    parser.add_argument("--gift-card-liability-account-id")
    parser.add_argument("--reserve-account-id")
    parser.add_argument("--bank-account-id")

    # SYNC domain
    parser.add_argument("--sync-job-id")
    parser.add_argument("--sync-type")
    parser.add_argument("--sync-mode")

    # Webhook
    parser.add_argument("--webhook-topic")
    parser.add_argument("--webhook-data")

    # MAPPING domain
    parser.add_argument("--shopify-product-id")
    parser.add_argument("--shopify-customer-id")
    parser.add_argument("--item-id")
    parser.add_argument("--customer-id")

    # GL RULES domain
    parser.add_argument("--gl-rule-id")
    parser.add_argument("--rule-name")
    parser.add_argument("--transaction-type")
    parser.add_argument("--debit-account-id")
    parser.add_argument("--credit-account-id")
    parser.add_argument("--priority", type=int)

    # GL POSTING domain
    parser.add_argument("--shopify-order-id")
    parser.add_argument("--shopify-refund-id")
    parser.add_argument("--shopify-payout-id")
    parser.add_argument("--shopify-dispute-id")
    parser.add_argument("--gift-card-type")
    parser.add_argument("--reserve-type")
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")

    # RECONCILIATION domain
    parser.add_argument("--period-start")
    parser.add_argument("--period-end")
    parser.add_argument("--bank-reference")
    parser.add_argument("--reconciliation-id")

    # BROWSE domain
    parser.add_argument("--shopify-order-id-local")
    parser.add_argument("--shopify-refund-id-local")
    parser.add_argument("--shopify-dispute-id-local")
    parser.add_argument("--financial-status")
    parser.add_argument("--gl-status")
    parser.add_argument("--payout-status")
    parser.add_argument("--dispute-status")

    # REPORTS domain
    parser.add_argument("--period")

    args, unknown = parser.parse_known_args()
    check_unknown_args(parser, unknown)
    check_input_lengths(args)

    db_path = args.db_path or DEFAULT_DB_PATH
    ensure_db_exists(db_path)
    conn = get_connection(db_path)

    _dep = check_required_tables(conn, REQUIRED_TABLES)
    if _dep:
        _dep["suggestion"] = "clawhub install erpclaw && run shopify init_db.py"
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
