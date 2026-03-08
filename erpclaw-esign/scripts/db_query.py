#!/usr/bin/env python3
"""ERPClaw E-Sign -- db_query.py (unified router)

Electronic signature workflows: request, sign, decline, track, audit.
Routes all 13 actions to the esign domain module.

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
except ImportError:
    import json as _json
    print(_json.dumps({
        "status": "error",
        "error": "ERPClaw foundation not installed. Install erpclaw-setup first: clawhub install erpclaw-setup",
        "suggestion": "clawhub install erpclaw-setup"
    }))
    sys.exit(1)

# Register naming prefixes
register_prefix("signature_request", "ESIG-")

# Add this script's directory so domain modules can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from esign import ACTIONS as ESIGN_ACTIONS

# ---------------------------------------------------------------------------
# Merge all domain actions into one router
# ---------------------------------------------------------------------------
SKILL = "erpclaw-esign"
REQUIRED_TABLES = ["company"]

ACTIONS = {}
ACTIONS.update(ESIGN_ACTIONS)


def main():
    parser = argparse.ArgumentParser(description="erpclaw-esign")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # -- Signature Request --
    parser.add_argument("--request-id")
    parser.add_argument("--company-id")
    parser.add_argument("--document-type")
    parser.add_argument("--document-id")
    parser.add_argument("--document-name")
    parser.add_argument("--signers")
    parser.add_argument("--requested-by")
    parser.add_argument("--message")
    parser.add_argument("--expires-at")

    # -- Signing --
    parser.add_argument("--signer-email")
    parser.add_argument("--signer-name")
    parser.add_argument("--signature-data")
    parser.add_argument("--ip-address")
    parser.add_argument("--user-agent")

    # -- Shared --
    parser.add_argument("--request-status")
    parser.add_argument("--notes")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)

    args, _unknown = parser.parse_known_args()
    check_input_lengths(args)

    db_path = args.db_path or DEFAULT_DB_PATH
    ensure_db_exists(db_path)
    conn = get_connection(db_path)

    _dep = check_required_tables(conn, REQUIRED_TABLES)
    if _dep:
        _dep["suggestion"] = "clawhub install erpclaw-setup && clawhub install erpclaw-esign"
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
