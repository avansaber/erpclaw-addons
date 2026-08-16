#!/usr/bin/env python3
"""ERPClaw Self-Service schema extension -- adds portal tables to the shared database.

5 tables: selfservice_permission_profile, selfservice_profile_assignment,
selfservice_portal_config, selfservice_session, selfservice_activity_log.

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
DISPLAY_NAME = "ERPClaw Self-Service"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log",
]

METADATA = MetaData()

# Foundation table this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)

# ---------------------------------------------------------------------------
# 1. selfservice_permission_profile
# ---------------------------------------------------------------------------
PERMISSION_PROFILE = Table(
    "selfservice_permission_profile", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("description", Text),
    Column("target_role", Text, nullable=False, server_default=text("'employee'")),
    Column("allowed_actions", Text, server_default=text("'[]'")),
    Column("denied_actions", Text, server_default=text("'[]'")),
    Column("record_scope", Text, server_default=text("'own'")),
    Column("field_visibility", Text, server_default=text("'{}'")),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "target_role IN ('employee','client','tenant','patient','vendor','other')",
        name="ck_selfservice_profile_target_role"),
    CheckConstraint("record_scope IN ('own','department','company')",
                    name="ck_selfservice_profile_record_scope"),
)

Index("idx_ss_profile_company", PERMISSION_PROFILE.c.company_id)
Index("idx_ss_profile_role", PERMISSION_PROFILE.c.target_role)
Index("idx_ss_profile_active", PERMISSION_PROFILE.c.is_active)

# ---------------------------------------------------------------------------
# 2. selfservice_profile_assignment
# ---------------------------------------------------------------------------
PROFILE_ASSIGNMENT = Table(
    "selfservice_profile_assignment", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("profile_id", Text,
           ForeignKey("selfservice_permission_profile.id"), nullable=False),
    Column("user_id", Text, nullable=False),
    Column("user_email", Text),
    Column("user_name", Text),
    Column("assigned_by", Text),
    Column("assignment_status", Text, nullable=False,
           server_default=text("'active'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("assignment_status IN ('active','revoked')",
                    name="ck_selfservice_assignment_status"),
)

Index("idx_ss_assign_profile", PROFILE_ASSIGNMENT.c.profile_id)
Index("idx_ss_assign_user", PROFILE_ASSIGNMENT.c.user_id)
Index("idx_ss_assign_company", PROFILE_ASSIGNMENT.c.company_id)
Index("idx_ss_assign_status", PROFILE_ASSIGNMENT.c.assignment_status)

# ---------------------------------------------------------------------------
# 3. selfservice_portal_config
# ---------------------------------------------------------------------------
PORTAL_CONFIG = Table(
    "selfservice_portal_config", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("branding_json", Text, server_default=text("'{}'")),
    Column("welcome_message", Text),
    Column("enabled_modules", Text, server_default=text("'[]'")),
    Column("enabled_actions", Text, server_default=text("'[]'")),
    Column("require_mfa", Integer, nullable=False, server_default=text("0")),
    Column("session_timeout_minutes", Integer, nullable=False,
           server_default=text("60")),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_ss_portal_company", PORTAL_CONFIG.c.company_id)
Index("idx_ss_portal_active", PORTAL_CONFIG.c.is_active)

# ---------------------------------------------------------------------------
# 4. selfservice_session
# ---------------------------------------------------------------------------
SESSION = Table(
    "selfservice_session", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("user_id", Text, nullable=False),
    Column("profile_id", Text,
           ForeignKey("selfservice_permission_profile.id"), nullable=False),
    Column("portal_id", Text, ForeignKey("selfservice_portal_config.id")),
    Column("token", Text, nullable=False),
    Column("ip_address", Text),
    Column("user_agent", Text),
    Column("session_status", Text, nullable=False, server_default=text("'active'")),
    Column("expires_at", Text, nullable=False),
    Column("last_activity_at", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("session_status IN ('active','expired','ended')",
                    name="ck_selfservice_session_status"),
)

Index("idx_ss_session_user", SESSION.c.user_id)
Index("idx_ss_session_token", SESSION.c.token)
Index("idx_ss_session_company", SESSION.c.company_id)
Index("idx_ss_session_status", SESSION.c.session_status)
Index("idx_ss_session_profile", SESSION.c.profile_id)

# ---------------------------------------------------------------------------
# 5. selfservice_activity_log
# ---------------------------------------------------------------------------
ACTIVITY_LOG = Table(
    "selfservice_activity_log", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("session_id", Text, ForeignKey("selfservice_session.id")),
    Column("user_id", Text, nullable=False),
    Column("action", Text, nullable=False),
    Column("entity_type", Text),
    Column("entity_id", Text),
    Column("result", Text, nullable=False, server_default=text("'allowed'")),
    Column("ip_address", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("result IN ('allowed','denied','error')",
                    name="ck_selfservice_activity_result"),
)

Index("idx_ss_actlog_user", ACTIVITY_LOG.c.user_id)
Index("idx_ss_actlog_company", ACTIVITY_LOG.c.company_id)
Index("idx_ss_actlog_session", ACTIVITY_LOG.c.session_id)
Index("idx_ss_actlog_result", ACTIVITY_LOG.c.result)
Index("idx_ss_actlog_action", ACTIVITY_LOG.c.action)



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

def create_selfservice_tables(db_path=None):
    """Create self-service tables and indexes on whichever backend is configured.

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
    result = create_selfservice_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
