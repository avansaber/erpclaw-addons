#!/usr/bin/env python3
"""ERPClaw Approvals schema extension -- adds approval workflow tables to the shared database.

3 tables: approval_rule, approval_step, approval_request.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Conversion rules are the pilot's (`erpclaw-esign`).
"""
import importlib.util
import os
import sys

# Bootstrap the shared lib only when it is not already reachable — an
# unconditional insert at position 0 overrides a caller that deliberately bound a
# different tree (ADR-0034 phase 2 step 2d).
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(
        os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))

from erpclaw_lib.seam import (  # noqa: E402
    CheckConstraint, Column, ForeignKey, Index, Integer, MetaData, Table, Text,
    provision, reference_table, text,
)

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")
DISPLAY_NAME = "ERPClaw Approvals"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log",
]

METADATA = MetaData()

# Foundation table this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)

# ---------------------------------------------------------------------------
# 1. approval_rule
# ---------------------------------------------------------------------------
RULE = Table(
    "approval_rule", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("name", Text, nullable=False),
    Column("entity_type", Text),
    Column("conditions", Text),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_appr_rule_company", RULE.c.company_id)
Index("idx_appr_rule_entity", RULE.c.entity_type)
Index("idx_appr_rule_active", RULE.c.is_active)

# ---------------------------------------------------------------------------
# 2. approval_step
# ---------------------------------------------------------------------------
STEP = Table(
    "approval_step", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("rule_id", Text,
           ForeignKey("approval_rule.id", ondelete="CASCADE"), nullable=False),
    Column("step_order", Integer, nullable=False, server_default=text("1")),
    Column("approver", Text, nullable=False),
    Column("approval_type", Text, nullable=False,
           server_default=text("'sequential'")),
    Column("is_required", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "approval_type IN ('sequential','parallel','conditional')",
        name="ck_approval_step_type"),
)

Index("idx_appr_step_rule", STEP.c.rule_id)
Index("idx_appr_step_order", STEP.c.step_order)
Index("idx_appr_step_company", STEP.c.company_id)

# ---------------------------------------------------------------------------
# 3. approval_request
# ---------------------------------------------------------------------------
REQUEST = Table(
    "approval_request", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("rule_id", Text, ForeignKey("approval_rule.id"), nullable=False),
    Column("entity_type", Text),
    Column("entity_id", Text),
    Column("requested_by", Text),
    Column("current_step", Integer, nullable=False, server_default=text("1")),
    Column("request_status", Text, nullable=False, server_default=text("'pending'")),
    Column("notes", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "request_status IN ('pending','in_progress','approved','rejected','cancelled')",
        name="ck_approval_request_status"),
)

Index("idx_appr_req_rule", REQUEST.c.rule_id)
Index("idx_appr_req_entity", REQUEST.c.entity_type, REQUEST.c.entity_id)
Index("idx_appr_req_status", REQUEST.c.request_status)
Index("idx_appr_req_company", REQUEST.c.company_id)



def _require_foundation(db_path):
    """The pre-conversion installer's foundation probe, asked through the seam.

    The original read ``sqlite_master`` directly, so the guard that exists to
    produce a friendly error was itself SQLite-only — on PostgreSQL it would have
    raised before it could explain anything. ``seam.table_exists`` answers on both
    backends (ADR-0034 bulk-39).
    """
    from erpclaw_lib import seam

    missing = [t for t in REQUIRED_FOUNDATION if not seam.table_exists(t, db_path)]
    if missing:
        print(f"ERROR: Foundation tables missing: {', '.join(missing)}")
        print("Run erpclaw-setup first: clawhub install erpclaw-setup")
        sys.exit(1)

def create_approvals_tables(db_path=None):
    """Create approval tables and indexes on whichever backend is configured.

    Same contract as before the ADR-0034 conversion: idempotent, and the returned
    counts are what was ACTUALLY created rather than what was declared.
    """
    db_path = db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    _require_foundation(db_path)
    result = provision(METADATA, db_path)
    return {
        "database": db_path,
        "tables": result["tables"],
        "indexes": result["indexes"],
    }


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else None
    result = create_approvals_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
