#!/usr/bin/env python3
"""ERPClaw Integrations schema extension -- adds integration tables to the shared database.

Operator-facing connectors for syncing data with external platforms.
17 tables: 9 core integration tables + 8 connectors-v2 tables
(booking, delivery, realestate, financial, productivity).

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Conversion rules are the pilot's (`erpclaw-esign`): seam
vocabulary only, IDs and amounts stay TEXT on every backend, and
``primary_key=True, nullable=True`` reproduces SQLite's ``id TEXT PRIMARY KEY``
without adding a NOT NULL that never shipped.

Two asymmetries in the shipped DDL are transcribed rather than tidied: the nine
``integration_*`` tables carry ``company_id`` as a bare column while the eight
``connv2_*`` tables declare a foreign key to ``company(id)``, and
``integration_sync_schedule`` gives ``sync_type`` / ``direction`` defaults
without the CHECK lists its sibling ``integration_sync`` puts on the same two
column names.
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
    UniqueConstraint, provision, reference_table, text,
)

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")
DISPLAY_NAME = "ERPClaw Integrations"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log",
]

METADATA = MetaData()

# Foundation tables this module points at but does not own. Declared for foreign
# key resolution only and never created here — see `seam.reference_table`.
reference_table("company", METADATA)

# ---------------------------------------------------------------------------
# 1. integration_connector
# ---------------------------------------------------------------------------
CONNECTOR = Table(
    "integration_connector", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("platform", Text, nullable=False),
    Column("connector_type", Text, nullable=False,
           server_default=text("'bidirectional'")),
    Column("base_url", Text),
    Column("connector_status", Text, nullable=False,
           server_default=text("'inactive'")),
    Column("config_json", Text, nullable=False, server_default=text("'{}'")),
    Column("last_sync_at", Text),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "platform IN ('shopify','woocommerce','amazon','quickbooks','stripe',"
        "'square','xero','custom')",
        name="ck_integration_connector_platform"),
    CheckConstraint(
        "connector_type IN ('inbound','outbound','bidirectional')",
        name="ck_integration_connector_connector_type"),
    CheckConstraint(
        "connector_status IN ('active','inactive','error')",
        name="ck_integration_connector_connector_status"),
)

Index("idx_intg_connector_company", CONNECTOR.c.company_id)
Index("idx_intg_connector_platform", CONNECTOR.c.platform)
Index("idx_intg_connector_status", CONNECTOR.c.connector_status)

# ---------------------------------------------------------------------------
# 2. integration_credential
# ---------------------------------------------------------------------------
CREDENTIAL = Table(
    "integration_credential", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("connector_id", Text, ForeignKey("integration_connector.id"),
           nullable=False),
    Column("credential_type", Text, nullable=False),
    Column("credential_key", Text, nullable=False),
    Column("credential_value", Text, nullable=False),
    Column("expires_at", Text),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "credential_type IN ('api_key','oauth2','basic_auth','webhook_secret')",
        name="ck_integration_credential_credential_type"),
)

Index("idx_intg_credential_connector", CREDENTIAL.c.connector_id)
Index("idx_intg_credential_company", CREDENTIAL.c.company_id)

# ---------------------------------------------------------------------------
# 3. integration_webhook
# ---------------------------------------------------------------------------
WEBHOOK = Table(
    "integration_webhook", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("connector_id", Text, ForeignKey("integration_connector.id"),
           nullable=False),
    Column("event_type", Text, nullable=False),
    Column("webhook_url", Text, nullable=False),
    Column("webhook_secret", Text),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_intg_webhook_connector", WEBHOOK.c.connector_id)
Index("idx_intg_webhook_company", WEBHOOK.c.company_id)

# ---------------------------------------------------------------------------
# 4. integration_sync
# ---------------------------------------------------------------------------
SYNC = Table(
    "integration_sync", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("connector_id", Text, ForeignKey("integration_connector.id"),
           nullable=False),
    Column("sync_type", Text, nullable=False),
    Column("direction", Text, nullable=False),
    Column("entity_type", Text),
    Column("sync_status", Text, nullable=False,
           server_default=text("'pending'")),
    Column("records_processed", Integer, nullable=False,
           server_default=text("0")),
    Column("records_failed", Integer, nullable=False, server_default=text("0")),
    Column("started_at", Text),
    Column("completed_at", Text),
    Column("error_message", Text),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("sync_type IN ('full','incremental','manual')",
                    name="ck_integration_sync_sync_type"),
    CheckConstraint("direction IN ('inbound','outbound','bidirectional')",
                    name="ck_integration_sync_direction"),
    CheckConstraint(
        "sync_status IN ('pending','running','completed','failed','cancelled')",
        name="ck_integration_sync_sync_status"),
)

Index("idx_intg_sync_connector", SYNC.c.connector_id)
Index("idx_intg_sync_status", SYNC.c.sync_status)
Index("idx_intg_sync_company", SYNC.c.company_id)

# ---------------------------------------------------------------------------
# 5. integration_sync_schedule
#
# `sync_type` and `direction` carry defaults here but NOT the CHECK lists that
# `integration_sync` puts on the same two column names. Transcribed as shipped.
# ---------------------------------------------------------------------------
SYNC_SCHEDULE = Table(
    "integration_sync_schedule", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("connector_id", Text, ForeignKey("integration_connector.id"),
           nullable=False),
    Column("entity_type", Text, nullable=False),
    Column("frequency", Text, nullable=False),
    Column("sync_type", Text, nullable=False,
           server_default=text("'incremental'")),
    Column("direction", Text, nullable=False,
           server_default=text("'bidirectional'")),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("last_run_at", Text),
    Column("next_run_at", Text),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "frequency IN ('hourly','daily','weekly','monthly','manual')",
        name="ck_integration_sync_schedule_frequency"),
)

Index("idx_intg_schedule_connector", SYNC_SCHEDULE.c.connector_id)
Index("idx_intg_schedule_company", SYNC_SCHEDULE.c.company_id)

# ---------------------------------------------------------------------------
# 6. integration_field_mapping
# ---------------------------------------------------------------------------
FIELD_MAPPING = Table(
    "integration_field_mapping", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("connector_id", Text, ForeignKey("integration_connector.id"),
           nullable=False),
    Column("entity_type", Text, nullable=False),
    Column("source_field", Text, nullable=False),
    Column("target_field", Text, nullable=False),
    Column("transform_rule", Text),
    Column("is_required", Integer, nullable=False, server_default=text("0")),
    Column("default_value", Text),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_intg_field_map_connector", FIELD_MAPPING.c.connector_id)
Index("idx_intg_field_map_entity", FIELD_MAPPING.c.entity_type)
Index("idx_intg_field_map_company", FIELD_MAPPING.c.company_id)

# ---------------------------------------------------------------------------
# 7. integration_entity_map
# ---------------------------------------------------------------------------
ENTITY_MAP = Table(
    "integration_entity_map", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("connector_id", Text, ForeignKey("integration_connector.id"),
           nullable=False),
    Column("entity_type", Text, nullable=False),
    Column("local_id", Text, nullable=False),
    Column("remote_id", Text, nullable=False),
    Column("last_synced_at", Text),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    # Unnamed, as shipped. SQLite backs this with an implicit `sqlite_autoindex`
    # that the parity oracle deliberately filters out, so it is carried here by
    # transcription rather than by the diff catching its absence.
    UniqueConstraint("connector_id", "entity_type", "local_id"),
)

Index("idx_intg_entity_map_connector", ENTITY_MAP.c.connector_id)
Index("idx_intg_entity_map_entity", ENTITY_MAP.c.entity_type)
Index("idx_intg_entity_map_local", ENTITY_MAP.c.local_id)
Index("idx_intg_entity_map_remote", ENTITY_MAP.c.remote_id)
Index("idx_intg_entity_map_company", ENTITY_MAP.c.company_id)

# ---------------------------------------------------------------------------
# 8. integration_transform_rule (supplementary)
# ---------------------------------------------------------------------------
TRANSFORM_RULE = Table(
    "integration_transform_rule", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("connector_id", Text, ForeignKey("integration_connector.id"),
           nullable=False),
    Column("entity_type", Text, nullable=False),
    Column("rule_name", Text, nullable=False),
    Column("rule_json", Text, nullable=False, server_default=text("'{}'")),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_intg_transform_connector", TRANSFORM_RULE.c.connector_id)
Index("idx_intg_transform_company", TRANSFORM_RULE.c.company_id)

# ---------------------------------------------------------------------------
# 9. integration_sync_error (child of sync)
#
# The only table in this module with no `company_id` at all. As shipped.
# ---------------------------------------------------------------------------
SYNC_ERROR = Table(
    "integration_sync_error", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("sync_id", Text, ForeignKey("integration_sync.id"), nullable=False),
    Column("entity_type", Text),
    Column("entity_id", Text),
    Column("error_message", Text, nullable=False),
    Column("is_resolved", Integer, nullable=False, server_default=text("0")),
    Column("resolution_notes", Text),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("resolved_at", Text),
)

Index("idx_intg_sync_error_sync", SYNC_ERROR.c.sync_id)
Index("idx_intg_sync_error_resolved", SYNC_ERROR.c.is_resolved)

# ===========================================================================
# CONNECTORS V2 -- BOOKING DOMAIN
# ===========================================================================

# ---------------------------------------------------------------------------
# 10. connv2_booking_connector
# ---------------------------------------------------------------------------
BOOKING_CONNECTOR = Table(
    "connv2_booking_connector", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("platform", Text, nullable=False),
    Column("property_id", Text),
    Column("api_credentials_ref", Text),
    Column("sync_reservations", Integer, nullable=False,
           server_default=text("1")),
    Column("sync_rates", Integer, nullable=False, server_default=text("1")),
    Column("sync_availability", Integer, nullable=False,
           server_default=text("1")),
    Column("last_sync_at", Text),
    Column("connector_status", Text, nullable=False,
           server_default=text("'inactive'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "platform IN ('booking_com','expedia','airbnb','vrbo')",
        name="ck_connv2_booking_connector_platform"),
    CheckConstraint(
        "connector_status IN ('active','inactive','error')",
        name="ck_connv2_booking_connector_connector_status"),
)

Index("idx_cv2_bkc_company", BOOKING_CONNECTOR.c.company_id)
Index("idx_cv2_bkc_platform", BOOKING_CONNECTOR.c.platform)
Index("idx_cv2_bkc_status", BOOKING_CONNECTOR.c.connector_status)

# ---------------------------------------------------------------------------
# 11. connv2_booking_sync_log
# ---------------------------------------------------------------------------
BOOKING_SYNC_LOG = Table(
    "connv2_booking_sync_log", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("connector_id", Text, ForeignKey("connv2_booking_connector.id"),
           nullable=False),
    Column("sync_type", Text, nullable=False),
    Column("direction", Text, nullable=False),
    Column("records_synced", Integer, nullable=False, server_default=text("0")),
    Column("errors", Integer, nullable=False, server_default=text("0")),
    Column("sync_status", Text, nullable=False,
           server_default=text("'completed'")),
    Column("started_at", Text),
    Column("completed_at", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "sync_type IN ('reservations','rates','availability')",
        name="ck_connv2_booking_sync_log_sync_type"),
    CheckConstraint("direction IN ('inbound','outbound')",
                    name="ck_connv2_booking_sync_log_direction"),
    CheckConstraint(
        "sync_status IN ('pending','running','completed','failed')",
        name="ck_connv2_booking_sync_log_sync_status"),
)

Index("idx_cv2_bsl_connector", BOOKING_SYNC_LOG.c.connector_id)
Index("idx_cv2_bsl_company", BOOKING_SYNC_LOG.c.company_id)
Index("idx_cv2_bsl_status", BOOKING_SYNC_LOG.c.sync_status)

# ===========================================================================
# CONNECTORS V2 -- DELIVERY DOMAIN
# ===========================================================================

# ---------------------------------------------------------------------------
# 12. connv2_delivery_connector
# ---------------------------------------------------------------------------
DELIVERY_CONNECTOR = Table(
    "connv2_delivery_connector", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("platform", Text, nullable=False),
    Column("store_id", Text),
    Column("api_credentials_ref", Text),
    Column("auto_accept", Integer, nullable=False, server_default=text("0")),
    Column("sync_menu", Integer, nullable=False, server_default=text("1")),
    Column("last_sync_at", Text),
    Column("connector_status", Text, nullable=False,
           server_default=text("'inactive'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "platform IN ('doordash','ubereats','grubhub','postmates')",
        name="ck_connv2_delivery_connector_platform"),
    CheckConstraint(
        "connector_status IN ('active','inactive','error')",
        name="ck_connv2_delivery_connector_connector_status"),
)

Index("idx_cv2_dlc_company", DELIVERY_CONNECTOR.c.company_id)
Index("idx_cv2_dlc_platform", DELIVERY_CONNECTOR.c.platform)
Index("idx_cv2_dlc_status", DELIVERY_CONNECTOR.c.connector_status)

# ---------------------------------------------------------------------------
# 13. connv2_delivery_order
#
# `total_amount` / `commission` / `net_amount` are money and stay TEXT
# (ADR-0034 dec. 1), exactly as the shipped DDL declared them.
# ---------------------------------------------------------------------------
DELIVERY_ORDER = Table(
    "connv2_delivery_order", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("connector_id", Text, ForeignKey("connv2_delivery_connector.id"),
           nullable=False),
    Column("external_order_id", Text),
    Column("order_data", Text),
    Column("total_amount", Text),
    Column("commission", Text),
    Column("net_amount", Text),
    Column("order_status", Text, nullable=False,
           server_default=text("'received'")),
    Column("received_at", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "order_status IN ('received','confirmed','preparing','ready',"
        "'picked_up','delivered','cancelled')",
        name="ck_connv2_delivery_order_order_status"),
)

Index("idx_cv2_dlo_connector", DELIVERY_ORDER.c.connector_id)
Index("idx_cv2_dlo_company", DELIVERY_ORDER.c.company_id)
Index("idx_cv2_dlo_status", DELIVERY_ORDER.c.order_status)
Index("idx_cv2_dlo_ext_id", DELIVERY_ORDER.c.external_order_id)

# ===========================================================================
# CONNECTORS V2 -- REAL ESTATE DOMAIN
# ===========================================================================

# ---------------------------------------------------------------------------
# 14. connv2_realestate_connector
# ---------------------------------------------------------------------------
REALESTATE_CONNECTOR = Table(
    "connv2_realestate_connector", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("platform", Text, nullable=False),
    Column("agent_id", Text),
    Column("api_credentials_ref", Text),
    Column("sync_listings", Integer, nullable=False, server_default=text("1")),
    Column("capture_leads", Integer, nullable=False, server_default=text("1")),
    Column("last_sync_at", Text),
    Column("connector_status", Text, nullable=False,
           server_default=text("'inactive'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "platform IN ('zillow','realtor_com','mls','trulia')",
        name="ck_connv2_realestate_connector_platform"),
    CheckConstraint(
        "connector_status IN ('active','inactive','error')",
        name="ck_connv2_realestate_connector_connector_status"),
)

Index("idx_cv2_rec_company", REALESTATE_CONNECTOR.c.company_id)
Index("idx_cv2_rec_platform", REALESTATE_CONNECTOR.c.platform)
Index("idx_cv2_rec_status", REALESTATE_CONNECTOR.c.connector_status)

# ---------------------------------------------------------------------------
# 15. connv2_realestate_lead
# ---------------------------------------------------------------------------
REALESTATE_LEAD = Table(
    "connv2_realestate_lead", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("connector_id", Text, ForeignKey("connv2_realestate_connector.id"),
           nullable=False),
    Column("lead_source", Text),
    Column("contact_name", Text),
    Column("contact_email", Text),
    Column("contact_phone", Text),
    Column("property_ref", Text),
    Column("inquiry", Text),
    Column("lead_status", Text, nullable=False, server_default=text("'new'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "lead_status IN ('new','contacted','qualified','converted','lost')",
        name="ck_connv2_realestate_lead_lead_status"),
)

Index("idx_cv2_rel_connector", REALESTATE_LEAD.c.connector_id)
Index("idx_cv2_rel_company", REALESTATE_LEAD.c.company_id)
Index("idx_cv2_rel_status", REALESTATE_LEAD.c.lead_status)

# ===========================================================================
# CONNECTORS V2 -- FINANCIAL DOMAIN
# ===========================================================================

# ---------------------------------------------------------------------------
# 16. connv2_financial_connector
# ---------------------------------------------------------------------------
FINANCIAL_CONNECTOR = Table(
    "connv2_financial_connector", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("platform", Text, nullable=False),
    Column("account_ref", Text),
    Column("api_credentials_ref", Text),
    Column("sync_enabled", Integer, nullable=False, server_default=text("1")),
    Column("last_sync_at", Text),
    Column("connector_status", Text, nullable=False,
           server_default=text("'inactive'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "platform IN ('plaid','twilio','sendgrid','mailchimp')",
        name="ck_connv2_financial_connector_platform"),
    CheckConstraint(
        "connector_status IN ('active','inactive','error')",
        name="ck_connv2_financial_connector_connector_status"),
)

Index("idx_cv2_fnc_company", FINANCIAL_CONNECTOR.c.company_id)
Index("idx_cv2_fnc_platform", FINANCIAL_CONNECTOR.c.platform)
Index("idx_cv2_fnc_status", FINANCIAL_CONNECTOR.c.connector_status)

# ===========================================================================
# CONNECTORS V2 -- PRODUCTIVITY DOMAIN
# ===========================================================================

# ---------------------------------------------------------------------------
# 17. connv2_productivity_connector
# ---------------------------------------------------------------------------
PRODUCTIVITY_CONNECTOR = Table(
    "connv2_productivity_connector", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("platform", Text, nullable=False),
    Column("workspace_id", Text),
    Column("api_credentials_ref", Text),
    Column("sync_calendar", Integer, nullable=False, server_default=text("1")),
    Column("sync_contacts", Integer, nullable=False, server_default=text("1")),
    Column("sync_files", Integer, nullable=False, server_default=text("0")),
    Column("last_sync_at", Text),
    Column("connector_status", Text, nullable=False,
           server_default=text("'inactive'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "platform IN ('google_workspace','microsoft_365','slack','zoom')",
        name="ck_connv2_productivity_connector_platform"),
    CheckConstraint(
        "connector_status IN ('active','inactive','error')",
        name="ck_connv2_productivity_connector_connector_status"),
)

Index("idx_cv2_pdc_company", PRODUCTIVITY_CONNECTOR.c.company_id)
Index("idx_cv2_pdc_platform", PRODUCTIVITY_CONNECTOR.c.platform)
Index("idx_cv2_pdc_status", PRODUCTIVITY_CONNECTOR.c.connector_status)

# ===========================================================================
# PLAID / STRIPE / S3 -- config + scaffolding tables (all removed)
# ===========================================================================
#
# plaid_config / stripe_config / s3_config removed 2026-07-02 (M31 H2 / audit
# B7): the "KEPT (referenced)" rationale from migration 001 is overturned. The
# register (writers=[], readers=[]) confirms zero writers/readers ever; the
# sole reference was the erpclaw-meta ownership map (SKILL_TABLES), a runtime-
# computed doc-map, not a persistence path. All three also normalized plaintext
# secrets, contradicting the typed-credential mechanism (crypto.encrypt_field /
# integration_credential) that the live connectors actually use. Dropped from
# existing DBs by this module's migration 002.
#
# Earlier siblings removed 2026-06-01 (audit P2) by migration 001:
#   plaid_linked_account / plaid_transaction (unbuilt Plaid connector),
#   stripe_payment_intent / stripe_webhook_event (superseded by the dedicated
#   erpclaw-integrations-stripe addon), s3_backup_record (unbuilt S3 backup).



def _require_foundation(db_path):
    """The pre-conversion installer's foundation probe, asked through the seam.

    The original read ``sqlite_master`` directly, so the guard that exists to
    produce a friendly error was itself SQLite-only. ``seam.table_exists`` answers
    on both backends (ADR-0034 bulk-39).
    """
    from erpclaw_lib import seam

    missing = [t for t in REQUIRED_FOUNDATION if not seam.table_exists(t, db_path)]
    if missing:
        print(f"ERROR: Foundation tables missing: {', '.join(missing)}")
        print("Run erpclaw-setup first: clawhub install erpclaw-setup")
        sys.exit(1)

def create_integration_tables(db_path=None):
    """Create integration tables and indexes on whichever backend is configured.

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
    result = create_integration_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
