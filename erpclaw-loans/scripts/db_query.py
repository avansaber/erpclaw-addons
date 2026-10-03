#!/usr/bin/env python3
"""ERPClaw Loans — db_query.py (unified router)

Routes all actions across 3 domain modules: loans, repayments, reports.

Usage: python3 db_query.py --action <action-name> [--flags ...]
Output: JSON to stdout, exit 0 on success, exit 1 on error.
"""
import argparse
import json
import os
import sys

# Add shared lib to path
try:
    import importlib.util
    if importlib.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.dependencies import table_exists
    from erpclaw_lib.response import ok, err
    from erpclaw_lib.args import SafeArgumentParser, check_unknown_args
except ImportError:
    print(json.dumps({
        "status": "error",
        "error": "ERPClaw foundation not installed. Install erpclaw-setup first.",
        "suggestion": "clawhub install erpclaw-setup"
    }))
    sys.exit(1)

# Add this script's directory so domain modules can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from loans import ACTIONS as LOANS_ACTIONS
from repayments import ACTIONS as REPAYMENTS_ACTIONS
from reports import ACTIONS as REPORTS_ACTIONS

# ---------------------------------------------------------------------------
# Merge all domain actions into one router
# ---------------------------------------------------------------------------
SKILL = "erpclaw-loans"
REQUIRED_TABLES = ["company", "loan"]

ACTIONS = {}
ACTIONS.update(LOANS_ACTIONS)
ACTIONS.update(REPAYMENTS_ACTIONS)
ACTIONS.update(REPORTS_ACTIONS)


def _resolve_company_flag(conn, args):
    from erpclaw_lib.query import Q as _Q, Table as _T, P as _P
    from erpclaw_lib.query_helpers import resolve_company_id
    company_name = getattr(args, "company_name", None)
    company_id = getattr(args, "company_id", None)
    if company_name and not company_id:
        c = _T("company")
        q = _Q.from_(c).select(c.id).where(c.id == _P())
        rows = conn.execute(q.get_sql(), [company_name]).fetchall()
        if rows:
            args.company_id = company_name
            return
        args.company_id = resolve_company_id(conn, None, company_name)


def main():
    parser = SafeArgumentParser(description="erpclaw-loans")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # -- Shared IDs --
    parser.add_argument("--id")
    parser.add_argument("--company-id")
    parser.add_argument("--company", dest="company_name", default=None)
    parser.add_argument("--loan-id")
    parser.add_argument("--loan-application-id")

    # -- Loan application fields --
    parser.add_argument("--applicant-type",
                        choices=["customer", "employee", "supplier"])
    parser.add_argument("--applicant-id")
    parser.add_argument("--applicant-name")
    parser.add_argument("--loan-type",
                        choices=["term_loan", "demand_loan", "staff_loan", "credit_line"])
    parser.add_argument("--requested-amount")
    parser.add_argument("--approved-amount")
    parser.add_argument("--interest-rate")
    parser.add_argument("--repayment-method",
                        choices=["equal_installment", "equal_principal", "bullet", "custom"])
    parser.add_argument("--repayment-periods", type=int)
    parser.add_argument("--application-date")
    parser.add_argument("--purpose")
    parser.add_argument("--collateral-description")
    parser.add_argument("--collateral-value")
    parser.add_argument("--rejection-reason")

    # -- Loan fields --
    parser.add_argument("--loan-account-id")
    parser.add_argument("--interest-income-account-id")
    parser.add_argument("--disbursement-account-id")
    parser.add_argument("--disbursement-date")

    # -- Repayment fields --
    parser.add_argument("--principal-amount")
    parser.add_argument("--interest-amount")
    parser.add_argument("--penalty-amount")
    parser.add_argument("--payment-method",
                        choices=["cash", "bank_transfer", "check", "auto_debit"])
    parser.add_argument("--repayment-date")
    parser.add_argument("--reference-number")
    parser.add_argument("--remarks")

    # -- Interest / write-off fields --
    parser.add_argument("--as-of-date")
    parser.add_argument("--bad-debt-account-id")
    parser.add_argument("--write-off-date")
    parser.add_argument("--reason")

    # -- Restructure fields --
    parser.add_argument("--new-interest-rate")
    parser.add_argument("--new-repayment-periods", type=int)

    # -- List filters --
    parser.add_argument("--status")
    parser.add_argument("--search")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)

    args, unknown = parser.parse_known_args()
    check_unknown_args(parser, unknown)

    db_path = getattr(args, "db_path", None)   # None unless --db-path was given
    conn = get_connection(db_path)

    # Verify required tables exist
    for t in REQUIRED_TABLES:
        if not table_exists(conn, t):
            print(json.dumps({
                "status": "error",
                "error": f"Required table '{t}' not found. Run erpclaw-loans init_db.py first.",
                "suggestion": "python3 init_db.py"
            }))
            conn.close()
            sys.exit(1)

    _resolve_company_flag(conn, args)

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
