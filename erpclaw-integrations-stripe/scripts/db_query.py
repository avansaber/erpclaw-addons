#!/usr/bin/env python3
"""ERPClaw Integrations Stripe — db_query.py (unified router)

Deep Stripe integration for full-cycle payment reconciliation.
Routes all actions across domain modules:
  accounts (6): add, update, get, list, configure-gl-mapping, test-connection
  sync (8): start-sync, start-full-sync, get-sync-status, list-sync-jobs,
            cancel-sync, process-webhook, replay-webhook, list-webhook-events
  customers (5): map-customer, auto-map-customers, list-customer-maps,
                 unmap-customer, get-customer-detail
  gl_rules (5): add-gl-rule, update-gl-rule, list-gl-rules,
                delete-gl-rule, preview-gl-posting
  reconciliation (8): run-reconciliation, reconcile-payout, match-charge,
                      unmatch-charge, list-unreconciled, get-reconciliation-run,
                      list-reconciliation-runs, reconciliation-summary
  rev_rec (4): stripe-create-rev-rec-schedule, stripe-recognize-subscription-revenue,
               stripe-rev-rec-status, stripe-handle-subscription-change

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

from accounts import ACTIONS as ACCOUNTS_ACTIONS
from sync import ACTIONS as SYNC_ACTIONS
from customers import ACTIONS as CUSTOMERS_ACTIONS
from gl_rules import ACTIONS as GL_RULES_ACTIONS
from reconciliation import ACTIONS as RECONCILIATION_ACTIONS
from gl_posting import ACTIONS as GL_POSTING_ACTIONS
from browse import ACTIONS as BROWSE_ACTIONS
from connect import ACTIONS as CONNECT_ACTIONS
from reports import ACTIONS as REPORTS_ACTIONS
from rev_rec import ACTIONS as REV_REC_ACTIONS
from utils import ACTIONS as UTILS_ACTIONS

# ---------------------------------------------------------------------------
# Merge all domain actions into one router
# ---------------------------------------------------------------------------
SKILL = "erpclaw-integrations-stripe"
REQUIRED_TABLES = ["company", "account", "stripe_account"]

ACTIONS = {}
ACTIONS.update(ACCOUNTS_ACTIONS)
ACTIONS.update(SYNC_ACTIONS)
ACTIONS.update(CUSTOMERS_ACTIONS)
ACTIONS.update(GL_RULES_ACTIONS)
ACTIONS.update(RECONCILIATION_ACTIONS)
ACTIONS.update(GL_POSTING_ACTIONS)
ACTIONS.update(BROWSE_ACTIONS)
ACTIONS.update(CONNECT_ACTIONS)
ACTIONS.update(REPORTS_ACTIONS)
ACTIONS.update(REV_REC_ACTIONS)
ACTIONS.update(UTILS_ACTIONS)

ACTIONS["status"] = lambda conn, args: ok({
    "skill": SKILL,
    "version": "1.0.0",
    "actions_available": len([k for k in ACTIONS if k != "status"]),
    "domains": ["accounts", "sync", "customers", "gl_rules", "reconciliation",
                "gl_posting", "rev_rec", "browse", "connect", "reports", "utils"],
    "database": DEFAULT_DB_PATH,
})


def main():
    parser = SafeArgumentParser(description="erpclaw-integrations-stripe")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # -- Shared IDs --
    parser.add_argument("--company-id")

    # -- Shared --
    parser.add_argument("--search")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--notes")
    parser.add_argument("--status")
    parser.add_argument("--description")

    # ── ACCOUNTS domain ──────────────────────────────────────────
    parser.add_argument("--stripe-account-id")
    parser.add_argument("--account-name")
    parser.add_argument("--api-key")
    parser.add_argument("--mode")
    parser.add_argument("--webhook-secret")
    parser.add_argument("--is-connect-platform", type=int)

    # -- GL mapping --
    parser.add_argument("--clearing-account-id")
    parser.add_argument("--fees-account-id")
    parser.add_argument("--payout-account-id")
    parser.add_argument("--dispute-account-id")
    parser.add_argument("--unearned-revenue-account-id")
    parser.add_argument("--platform-revenue-account-id")

    # ── SYNC domain ───────────────────────────────────────────────
    parser.add_argument("--sync-job-id")
    parser.add_argument("--object-type")
    parser.add_argument("--sync-from")
    parser.add_argument("--sync-to")
    parser.add_argument("--event-data")
    parser.add_argument("--webhook-event-id")
    parser.add_argument("--event-type")
    parser.add_argument("--processed", type=int)

    # ── CUSTOMERS domain ──────────────────────────────────────────
    parser.add_argument("--stripe-customer-id")
    parser.add_argument("--erpclaw-customer-id")
    parser.add_argument("--customer-map-id")
    parser.add_argument("--match-method")

    # ── GL RULES domain ───────────────────────────────────────────
    parser.add_argument("--gl-rule-id")
    parser.add_argument("--transaction-type")
    parser.add_argument("--debit-account-id")
    parser.add_argument("--credit-account-id")
    parser.add_argument("--fee-account-id")
    parser.add_argument("--match-field")
    parser.add_argument("--match-value")
    parser.add_argument("--cost-center-id")
    parser.add_argument("--priority", type=int)

    # ── RECONCILIATION domain ─────────────────────────────────────
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")
    parser.add_argument("--payout-stripe-id")
    parser.add_argument("--charge-stripe-id")
    parser.add_argument("--erpclaw-invoice-id")
    parser.add_argument("--reconciliation-run-id")
    parser.add_argument("--type")

    # ── GL POSTING domain ──────────────────────────────────────────
    parser.add_argument("--refund-stripe-id")
    parser.add_argument("--dispute-stripe-id")
    parser.add_argument("--app-fee-stripe-id")

    # ── REV REC domain (ASC 606) ──────────────────────────────────
    parser.add_argument("--subscription-stripe-id")
    parser.add_argument("--revenue-account-id")
    parser.add_argument("--period-date")
    parser.add_argument("--change-type")
    parser.add_argument("--new-plan-amount")

    # ── BROWSE domain ──────────────────────────────────────────────
    parser.add_argument("--customer-stripe-id")

    args, unknown = parser.parse_known_args()
    check_unknown_args(parser, unknown)
    check_input_lengths(args)

    db_path = args.db_path or DEFAULT_DB_PATH
    ensure_db_exists(db_path)
    conn = get_connection(db_path)

    _dep = check_required_tables(conn, REQUIRED_TABLES)
    if _dep:
        _dep["suggestion"] = "clawhub install erpclaw && run stripe init_db.py"
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
