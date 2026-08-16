#!/usr/bin/env python3
"""ERPClaw Alerts schema extension -- adds alerting and email tables to the shared database.

7 tables: alert_rule, alert_log, notification_channel, email_account,
email_template, email_outbox, email_log.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Conversion rules are the pilot's (`erpclaw-esign`).

Note on `alert_rule`, `alert_log` and `notification_channel`: their `company_id`
carries NO foreign key in the shipped DDL, unlike the four email tables. That
asymmetry is preserved rather than tidied — adding a constraint during a
conversion is a schema change wearing a refactor's clothes.
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
DISPLAY_NAME = "ERPClaw Alerts"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log",
]

METADATA = MetaData()

# Foundation table this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)

# ---------------------------------------------------------------------------
# 1. alert_rule
# ---------------------------------------------------------------------------
ALERT_RULE = Table(
    "alert_rule", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("description", Text),
    Column("entity_type", Text, nullable=False),
    Column("condition_json", Text, nullable=False, server_default=text("'{}'")),
    Column("severity", Text, server_default=text("'medium'")),
    Column("channel_ids", Text),
    Column("cooldown_minutes", Integer, server_default=text("60")),
    Column("is_active", Integer, server_default=text("1")),
    Column("last_triggered_at", Text),
    Column("trigger_count", Integer, server_default=text("0")),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("severity IN ('low','medium','high','critical')",
                    name="ck_alert_rule_severity"),
)

Index("idx_alert_rule_company", ALERT_RULE.c.company_id)
Index("idx_alert_rule_entity", ALERT_RULE.c.entity_type)
Index("idx_alert_rule_severity", ALERT_RULE.c.severity)
Index("idx_alert_rule_active", ALERT_RULE.c.is_active)

# ---------------------------------------------------------------------------
# 2. alert_log
# ---------------------------------------------------------------------------
ALERT_LOG = Table(
    "alert_log", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("rule_id", Text, ForeignKey("alert_rule.id")),
    Column("rule_name", Text),
    Column("entity_type", Text),
    Column("entity_id", Text),
    Column("severity", Text),
    Column("message", Text, nullable=False),
    Column("alert_status", Text, server_default=text("'triggered'")),
    Column("acknowledged_by", Text),
    Column("acknowledged_at", Text),
    Column("resolved_at", Text),
    Column("channel_results", Text),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "alert_status IN ('triggered','acknowledged','resolved','expired')",
        name="ck_alert_log_status"),
)

Index("idx_alert_log_rule", ALERT_LOG.c.rule_id)
Index("idx_alert_log_company", ALERT_LOG.c.company_id)
Index("idx_alert_log_severity", ALERT_LOG.c.severity)
Index("idx_alert_log_status", ALERT_LOG.c.alert_status)
Index("idx_alert_log_entity", ALERT_LOG.c.entity_type, ALERT_LOG.c.entity_id)

# ---------------------------------------------------------------------------
# 3. notification_channel
# ---------------------------------------------------------------------------
NOTIFICATION_CHANNEL = Table(
    "notification_channel", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("channel_type", Text, nullable=False),
    Column("config_json", Text, nullable=False, server_default=text("'{}'")),
    Column("is_active", Integer, server_default=text("1")),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("channel_type IN ('email','webhook','telegram','sms')",
                    name="ck_notification_channel_type"),
)

Index("idx_notif_channel_company", NOTIFICATION_CHANNEL.c.company_id)
Index("idx_notif_channel_type", NOTIFICATION_CHANNEL.c.channel_type)
Index("idx_notif_channel_active", NOTIFICATION_CHANNEL.c.is_active)

# ---------------------------------------------------------------------------
# 4. email_account
# ---------------------------------------------------------------------------
EMAIL_ACCOUNT = Table(
    "email_account", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("name", Text, nullable=False),
    Column("provider", Text, nullable=False, server_default=text("'smtp'")),
    Column("from_address", Text, nullable=False),
    Column("reply_to_address", Text),
    Column("is_default", Integer, nullable=False, server_default=text("0")),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("config_json", Text, nullable=False, server_default=text("'{}'")),
    Column("last_health_check_at", Text),
    Column("last_health_status", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("provider IN ('smtp','ses','mailgun')",
                    name="ck_email_account_provider"),
    CheckConstraint("is_default IN (0,1)", name="ck_email_account_is_default"),
    CheckConstraint("is_active IN (0,1)", name="ck_email_account_is_active"),
)

Index("idx_email_account_company", EMAIL_ACCOUNT.c.company_id)
Index("idx_email_account_default", EMAIL_ACCOUNT.c.is_default)

# ---------------------------------------------------------------------------
# 5. email_template
# ---------------------------------------------------------------------------
EMAIL_TEMPLATE = Table(
    "email_template", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("name", Text, nullable=False),
    Column("subject", Text, nullable=False, server_default=text("''")),
    Column("body_html", Text, nullable=False, server_default=text("''")),
    Column("body_text", Text, nullable=False, server_default=text("''")),
    Column("merge_field_list_json", Text, nullable=False,
           server_default=text("'[]'")),
    Column("language", Text, nullable=False, server_default=text("'en'")),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text, ForeignKey("company.id", ondelete="RESTRICT")),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("is_active IN (0,1)", name="ck_email_template_is_active"),
)

Index("idx_email_template_name", EMAIL_TEMPLATE.c.name)

# ---------------------------------------------------------------------------
# 6. email_outbox
# ---------------------------------------------------------------------------
EMAIL_OUTBOX = Table(
    "email_outbox", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("to_address", Text, nullable=False),
    Column("from_account_id", Text,
           ForeignKey("email_account.id", ondelete="RESTRICT")),
    Column("subject", Text, nullable=False, server_default=text("''")),
    Column("body_html", Text, nullable=False, server_default=text("''")),
    Column("body_text", Text, nullable=False, server_default=text("''")),
    Column("template_id", Text,
           ForeignKey("email_template.id", ondelete="SET NULL")),
    Column("merge_vars_json", Text, nullable=False, server_default=text("'{}'")),
    Column("status", Text, nullable=False, server_default=text("'queued'")),
    Column("attempt_count", Integer, nullable=False, server_default=text("0")),
    Column("next_attempt_at", Text),
    Column("provider_message_id", Text),
    Column("sent_at", Text),
    Column("error_message", Text),
    Column("company_id", Text, ForeignKey("company.id", ondelete="RESTRICT")),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ('queued','sending','sent','bounced','failed','retry')",
        name="ck_email_outbox_status"),
)

Index("idx_email_outbox_status", EMAIL_OUTBOX.c.status)
Index("idx_email_outbox_next_attempt", EMAIL_OUTBOX.c.next_attempt_at)

# ---------------------------------------------------------------------------
# 7. email_log
# ---------------------------------------------------------------------------
EMAIL_LOG = Table(
    "email_log", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("email_outbox_id", Text,
           ForeignKey("email_outbox.id", ondelete="CASCADE")),
    Column("event_type", Text, nullable=False),
    Column("event_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("payload_json", Text),
    # The space after 'complaint', is deliberate: the shipped DDL wraps this
    # predicate across two lines, so the stored constraint text carries a space
    # there. Matching it keeps the parity proof exact rather than teaching the
    # comparison to ignore whitespace — which it must not do, since whitespace
    # inside a quoted value is significant.
    CheckConstraint(
        "event_type IN ('queued','sending','sent','bounced','complaint', "
        "'delivered','opened','clicked','failed','retry')",
        name="ck_email_log_event_type"),
)

Index("idx_email_log_outbox", EMAIL_LOG.c.email_outbox_id)



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

def create_alerts_tables(db_path=None):
    """Create alert and email tables on whichever backend is configured.

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
    result = create_alerts_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
