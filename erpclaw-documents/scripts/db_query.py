#!/usr/bin/env python3
"""ERPClaw Documents -- db_query.py (unified router)

Document management: CRUD, versioning, tagging, linking, templates, search.
Routes all 25 actions across 2 domain modules: documents, templates.

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
register_prefix("document", "DOC-")
register_prefix("document_template", "DTPL-")

# Add this script's directory so domain modules can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from documents import ACTIONS as DOC_ACTIONS
from templates import ACTIONS as TPL_ACTIONS

# ---------------------------------------------------------------------------
# Merge all domain actions into one router
# ---------------------------------------------------------------------------
SKILL = "erpclaw-documents"
REQUIRED_TABLES = ["company", "document"]

ACTIONS = {}
ACTIONS.update(DOC_ACTIONS)
ACTIONS.update(TPL_ACTIONS)


def main():
    parser = SafeArgumentParser(description="erpclaw-documents")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # -- Documents --
    parser.add_argument("--document-id")
    parser.add_argument("--company-id")
    parser.add_argument("--title")
    parser.add_argument("--document-type")
    parser.add_argument("--file-name")
    parser.add_argument("--file-path")
    parser.add_argument("--file-size")
    parser.add_argument("--mime-type")
    parser.add_argument("--content")
    parser.add_argument("--tags")
    parser.add_argument("--tag")
    parser.add_argument("--linked-entity-type")
    parser.add_argument("--linked-entity-id")
    parser.add_argument("--owner")
    parser.add_argument("--retention-date")

    # -- Versioning --
    parser.add_argument("--version-number")
    parser.add_argument("--change-notes")
    parser.add_argument("--created-by")

    # -- Linking --
    parser.add_argument("--link-id")
    parser.add_argument("--link-type")

    # -- Templates --
    parser.add_argument("--template-id")
    parser.add_argument("--name")
    parser.add_argument("--template-type")
    parser.add_argument("--merge-fields")
    parser.add_argument("--description")
    parser.add_argument("--is-active")
    parser.add_argument("--merge-data")

    # -- Shared --
    parser.add_argument("--status")
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
        _dep["suggestion"] = "clawhub install erpclaw-setup && clawhub install erpclaw-documents"
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
