#!/usr/bin/env python3
"""erpclaw-growth schema extension -- adds advanced CRM/marketing tables to the shared database.

32 tables: 15 CRM advanced (campaigns, territories, contracts, automation, drip
sequences) + 8 CRM core entities (contact/company model, tasks, pipelines, saved
views) + 9 AI engine / analytics tables (moved from core init_schema.py):
  anomaly, scenario, correlation, categorization_rule, business_rule,
  pending_decision, audit_conversation, conversation_context,
  relationship_score.
(elimination_rule / elimination_entry were retired 2026-08-12 -- M63-C.)
Part of the erpclaw-growth super-package (CRM + Analytics + AI Engine).

The docstring said "25 tables" and listed `usage_event` among the AI-engine
group. Both were stale: the count predates the Wave 1B F1-F4 tables and the M8
drip tables, and `usage_event` moved to the foundation in the 2026-05-31
migration audit (BUG-007). Corrected here against a provisioned count.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. The four case-insensitive uniqueness guarantees on CRM
identity (`uq_crm_company_domain`, `uq_crm_contact_email`, `uq_crm_pipeline_name`,
`uq_crm_pipeline_stage_name`) plus `uq_crm_saved_view_name` are EXPRESSION
indexes over ``lower(...)``; SQLAlchemy refuses to reflect those, so they are
declared inline in their ``Table`` blocks (the form the seam's static reader can
attribute) and were verified against the raw catalog rather than reflection.
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

from erpclaw_lib.db import get_connection  # noqa: E402
from erpclaw_lib.seam import (  # noqa: E402
    CheckConstraint, Column, ForeignKey, Index, Integer, MetaData, Table, Text,
    provision, reference_table, table_exists, text,
)

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")
DISPLAY_NAME = "ERPClaw Growth (CRM Advanced tables)"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log",
]

# Wave 1B F3 — default "Standard Sales" pipeline seed. Kept in sync with
# migrations/003_crm_pipelines.py and foundation migration 024.
# (stage_order, name, is_terminal_won, is_terminal_lost, default_probability)
DEFAULT_PIPELINE_NAME = "Standard Sales"
DEFAULT_PIPELINE_STAGES = [
    (1, "new", 0, 0, "0"),
    (2, "contacted", 0, 0, "10"),
    (3, "qualified", 0, 0, "25"),
    (4, "proposal_sent", 0, 0, "50"),
    (5, "negotiation", 0, 0, "75"),
    (6, "won", 1, 0, "100"),
    (7, "lost", 0, 1, "0"),
]

METADATA = MetaData()

# Foundation tables this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)
reference_table("customer", METADATA)


def _require_foundation(db_path):
    """Refuse to provision without the foundation tables this module points at.

    The pre-conversion probe queried SQLite's own catalog table, so on PostgreSQL
    it raised rather than answering; ``seam.table_exists`` answers on both
    backends. The message wording is the original's, deliberately.
    """
    missing = [t for t in REQUIRED_FOUNDATION if not table_exists(t, db_path)]
    if missing:
        print(f"ERROR: Foundation tables missing: {', '.join(missing)}", file=sys.stderr)
        print("Run erpclaw-setup first: clawhub install erpclaw-setup", file=sys.stderr)
        sys.exit(1)


def _seed_default_pipeline(conn):
    """Seed the default 7-stage 'Standard Sales' pipeline if no default exists.

    Idempotent. Used by both create_crmadv_tables() (fresh installs) and growth
    migration 003 (existing installs) so they converge. TEXT uuid4 ids;
    default_probability is TEXT-Decimal.
    """
    import uuid
    existing = conn.execute(
        "SELECT id FROM crm_pipeline WHERE is_default = 1 LIMIT 1"
    ).fetchone()
    if existing:
        return existing[0]
    named = conn.execute(
        "SELECT id FROM crm_pipeline WHERE name = ? LIMIT 1", (DEFAULT_PIPELINE_NAME,)
    ).fetchone()
    if named:
        return named[0]

    pipeline_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO crm_pipeline (id, name, description, is_default, is_active) "
        "VALUES (?, ?, ?, 1, 1)",
        (pipeline_id, DEFAULT_PIPELINE_NAME,
         "Default sales pipeline (the original 7 hardcoded opportunity stages)"),
    )
    for order_no, name, won, lost, prob in DEFAULT_PIPELINE_STAGES:
        conn.execute(
            "INSERT INTO crm_pipeline_stage "
            "(id, crm_pipeline_id, stage_order, name, is_terminal_won, "
            " is_terminal_lost, default_probability, is_active) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
            (str(uuid.uuid4()), pipeline_id, order_no, name, won, lost, prob),
        )
    return pipeline_id


# ==================================================================
# CAMPAIGNS DOMAIN
# ==================================================================

# 1. crmadv_campaign_template
CAMPAIGN_TEMPLATE = Table(
    "crmadv_campaign_template", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("name", Text, nullable=False),
    Column("subject_template", Text),
    Column("body_html", Text),
    Column("body_text", Text),
    Column("template_type", Text, server_default=text("'newsletter'")),
    Column("is_active", Integer, server_default=text("1")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "template_type IN ('newsletter','promotional','transactional','drip','welcome')",
        name="ck_crmadv_campaign_template_template_type"),
)

Index("idx_crmadv_tmpl_company", CAMPAIGN_TEMPLATE.c.company_id)
Index("idx_crmadv_tmpl_type", CAMPAIGN_TEMPLATE.c.template_type)

# 2. crmadv_recipient_list
RECIPIENT_LIST = Table(
    "crmadv_recipient_list", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("name", Text, nullable=False),
    Column("description", Text),
    Column("list_type", Text, server_default=text("'static'")),
    Column("filter_criteria", Text),
    Column("recipient_count", Integer, server_default=text("0")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("list_type IN ('static','dynamic','segment')",
                    name="ck_crmadv_recipient_list_list_type"),
)

Index("idx_crmadv_rlist_company", RECIPIENT_LIST.c.company_id)

# 3. crmadv_email_campaign
EMAIL_CAMPAIGN = Table(
    "crmadv_email_campaign", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("subject", Text),
    Column("template_id", Text, ForeignKey("crmadv_campaign_template.id")),
    Column("recipient_list_id", Text, ForeignKey("crmadv_recipient_list.id")),
    Column("campaign_status", Text, server_default=text("'draft'")),
    Column("scheduled_date", Text),
    Column("sent_date", Text),
    Column("total_sent", Integer, server_default=text("0")),
    Column("total_opened", Integer, server_default=text("0")),
    Column("total_clicked", Integer, server_default=text("0")),
    Column("total_bounced", Integer, server_default=text("0")),
    Column("total_unsubscribed", Integer, server_default=text("0")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "campaign_status IN ('draft','scheduled','sending','sent','paused','cancelled')",
        name="ck_crmadv_email_campaign_campaign_status"),
)

Index("idx_crmadv_camp_company", EMAIL_CAMPAIGN.c.company_id)
Index("idx_crmadv_camp_status", EMAIL_CAMPAIGN.c.campaign_status)
Index("idx_crmadv_camp_template", EMAIL_CAMPAIGN.c.template_id)

# 4. crmadv_campaign_event
CAMPAIGN_EVENT = Table(
    "crmadv_campaign_event", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("campaign_id", Text, ForeignKey("crmadv_email_campaign.id"), nullable=False),
    Column("event_type", Text, nullable=False),
    Column("recipient_email", Text),
    Column("event_timestamp", Text),
    Column("metadata", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "event_type IN ('sent','opened','clicked','bounced','unsubscribed','converted')",
        name="ck_crmadv_campaign_event_event_type"),
)

Index("idx_crmadv_evt_campaign", CAMPAIGN_EVENT.c.campaign_id)
Index("idx_crmadv_evt_type", CAMPAIGN_EVENT.c.event_type)
Index("idx_crmadv_evt_company", CAMPAIGN_EVENT.c.company_id)

# ==================================================================
# TERRITORIES DOMAIN
# ==================================================================

# 5. crmadv_territory
TERRITORY = Table(
    "crmadv_territory", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("region", Text),
    # Self-referential: a territory may nest under a parent territory.
    Column("parent_territory_id", Text, ForeignKey("crmadv_territory.id")),
    Column("territory_type", Text, server_default=text("'geographic'")),
    Column("territory_status", Text, server_default=text("'active'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "territory_type IN ('geographic','industry','named_account','product')",
        name="ck_crmadv_territory_territory_type"),
    CheckConstraint("territory_status IN ('active','inactive')",
                    name="ck_crmadv_territory_territory_status"),
)

Index("idx_crmadv_terr_company", TERRITORY.c.company_id)
Index("idx_crmadv_terr_parent", TERRITORY.c.parent_territory_id)
Index("idx_crmadv_terr_type", TERRITORY.c.territory_type)

# 6. crmadv_territory_assignment
TERRITORY_ASSIGNMENT = Table(
    "crmadv_territory_assignment", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("territory_id", Text, ForeignKey("crmadv_territory.id"), nullable=False),
    Column("salesperson", Text, nullable=False),
    Column("start_date", Text),
    Column("end_date", Text),
    Column("assignment_status", Text, server_default=text("'active'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("assignment_status IN ('active','ended')",
                    name="ck_crmadv_territory_assignment_assignment_status"),
)

Index("idx_crmadv_tassign_terr", TERRITORY_ASSIGNMENT.c.territory_id)
Index("idx_crmadv_tassign_company", TERRITORY_ASSIGNMENT.c.company_id)

# 7. crmadv_territory_quota
# quota_amount / actual_amount / attainment_pct are money and stay TEXT-Decimal.
TERRITORY_QUOTA = Table(
    "crmadv_territory_quota", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("territory_id", Text, ForeignKey("crmadv_territory.id"), nullable=False),
    Column("period", Text, nullable=False),
    Column("quota_amount", Text, nullable=False),
    Column("actual_amount", Text, server_default=text("'0'")),
    Column("attainment_pct", Text, server_default=text("'0'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_crmadv_tquota_terr", TERRITORY_QUOTA.c.territory_id)
Index("idx_crmadv_tquota_company", TERRITORY_QUOTA.c.company_id)

# ==================================================================
# CONTRACTS DOMAIN
# ==================================================================

# 8. crmadv_contract
# total_value / annual_value are money and stay TEXT-Decimal.
CONTRACT = Table(
    "crmadv_contract", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("customer_name", Text, nullable=False),
    Column("contract_type", Text, server_default=text("'service'")),
    Column("contract_status", Text, server_default=text("'draft'")),
    Column("start_date", Text),
    Column("end_date", Text),
    Column("total_value", Text),
    Column("annual_value", Text),
    Column("auto_renew", Integer, server_default=text("0")),
    Column("renewal_terms", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    # 'licensing' and 'license' are both accepted — a historical duplication that
    # is preserved rather than tidied (a conversion transcribes, it does not fix).
    CheckConstraint(
        "contract_type IN ('service','subscription','licensing','license','maintenance','consulting')",
        name="ck_crmadv_contract_contract_type"),
    CheckConstraint(
        "contract_status IN ('draft','active','expired','renewed','terminated')",
        name="ck_crmadv_contract_contract_status"),
)

Index("idx_crmadv_ctr_company", CONTRACT.c.company_id)
Index("idx_crmadv_ctr_status", CONTRACT.c.contract_status)
Index("idx_crmadv_ctr_type", CONTRACT.c.contract_type)

# 9. crmadv_contract_obligation
CONTRACT_OBLIGATION = Table(
    "crmadv_contract_obligation", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("contract_id", Text, ForeignKey("crmadv_contract.id"), nullable=False),
    Column("description", Text, nullable=False),
    Column("due_date", Text),
    Column("obligee", Text),
    Column("obligation_status", Text, server_default=text("'pending'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "obligation_status IN ('pending','in_progress','completed','overdue')",
        name="ck_crmadv_contract_obligation_obligation_status"),
)

Index("idx_crmadv_obl_contract", CONTRACT_OBLIGATION.c.contract_id)
Index("idx_crmadv_obl_status", CONTRACT_OBLIGATION.c.obligation_status)
Index("idx_crmadv_obl_company", CONTRACT_OBLIGATION.c.company_id)

# ==================================================================
# AUTOMATION DOMAIN
# ==================================================================

# 10. crmadv_automation_workflow
AUTOMATION_WORKFLOW = Table(
    "crmadv_automation_workflow", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("trigger_event", Text),
    Column("conditions_json", Text, server_default=text("'{}'")),
    Column("actions_json", Text, server_default=text("'[]'")),
    Column("workflow_status", Text, server_default=text("'inactive'")),
    Column("execution_count", Integer, server_default=text("0")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("workflow_status IN ('active','inactive','paused')",
                    name="ck_crmadv_automation_workflow_workflow_status"),
)

Index("idx_crmadv_wf_company", AUTOMATION_WORKFLOW.c.company_id)
Index("idx_crmadv_wf_status", AUTOMATION_WORKFLOW.c.workflow_status)

# 11. crmadv_lead_score_rule
LEAD_SCORE_RULE = Table(
    "crmadv_lead_score_rule", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("name", Text, nullable=False),
    Column("criteria_json", Text, nullable=False),
    Column("points", Integer, nullable=False, server_default=text("0")),
    Column("is_active", Integer, server_default=text("1")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_crmadv_lsr_company", LEAD_SCORE_RULE.c.company_id)

# 12. crmadv_nurture_sequence
NURTURE_SEQUENCE = Table(
    "crmadv_nurture_sequence", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("description", Text),
    Column("steps_json", Text, server_default=text("'[]'")),
    Column("total_steps", Integer, server_default=text("0")),
    Column("sequence_status", Text, server_default=text("'draft'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("sequence_status IN ('draft','active','paused','completed')",
                    name="ck_crmadv_nurture_sequence_sequence_status"),
)

Index("idx_crmadv_ns_company", NURTURE_SEQUENCE.c.company_id)
Index("idx_crmadv_ns_status", NURTURE_SEQUENCE.c.sequence_status)

# 13. crmadv_drip_sequence (M8 phase B -- drip campaign sequences)
DRIP_SEQUENCE = Table(
    "crmadv_drip_sequence", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("name", Text, nullable=False),
    Column("description", Text),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    # The M8 tables space their IN-lists ("(0, 1)") where the older tables do not
    # ("(0,1)"). Both spellings are kept exactly as shipped.
    CheckConstraint("is_active IN (0, 1)",
                    name="ck_crmadv_drip_sequence_is_active"),
)

Index("idx_crmadv_drip_company", DRIP_SEQUENCE.c.company_id)
Index("idx_crmadv_drip_active", DRIP_SEQUENCE.c.is_active)

# 14. crmadv_drip_sequence_step (M8 phase B -- steps within a drip sequence)
# email_template_id deliberately carries NO foreign key, unlike
# crmadv_email_campaign.template_id which does.
DRIP_SEQUENCE_STEP = Table(
    "crmadv_drip_sequence_step", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("sequence_id", Text, ForeignKey("crmadv_drip_sequence.id"), nullable=False),
    Column("step_order", Integer, nullable=False),
    Column("delay_hours", Integer, nullable=False, server_default=text("0")),
    Column("email_template_id", Text),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("is_active IN (0, 1)",
                    name="ck_crmadv_drip_sequence_step_is_active"),
)

Index("idx_crmadv_drip_step_seq", DRIP_SEQUENCE_STEP.c.sequence_id)
Index("idx_crmadv_drip_step_seq_order",
      DRIP_SEQUENCE_STEP.c.sequence_id, DRIP_SEQUENCE_STEP.c.step_order)

# 15. crmadv_drip_enrollment (M8 phase B -- contacts enrolled in a drip sequence)
# contact_id is opaque (no foreign key) and there is no company_id at all here,
# unlike every sibling in this domain. Both preserved.
DRIP_ENROLLMENT = Table(
    "crmadv_drip_enrollment", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("sequence_id", Text, ForeignKey("crmadv_drip_sequence.id"), nullable=False),
    Column("contact_id", Text, nullable=False),
    Column("current_step", Integer, nullable=False, server_default=text("0")),
    Column("status", Text, nullable=False, server_default=text("'active'")),
    Column("next_send_at", Text),
    Column("enrolled_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("status IN ('active', 'completed', 'cancelled')",
                    name="ck_crmadv_drip_enrollment_status"),
)

Index("idx_crmadv_drip_enr_seq", DRIP_ENROLLMENT.c.sequence_id)
Index("idx_crmadv_drip_enr_contact", DRIP_ENROLLMENT.c.contact_id)
Index("idx_crmadv_drip_enr_status_send",
      DRIP_ENROLLMENT.c.status, DRIP_ENROLLMENT.c.next_send_at)

# ==================================================================
# CONTACT + COMPANY MODEL (Wave 1B F1)
# crm_contact / crm_company / crm_contact_role. Person + Org entities
# that the foundation lead/opportunity/customer/crm_activity tables point
# at via the nullable FK columns added in foundation migration 023
# (ADR-0023). Growth is the sole writer of both these tables and those
# foundation FK columns.
# ==================================================================

# crm_company — Org entity (defined before crm_contact: contact FKs company)
# annual_revenue is money and stays TEXT-Decimal; employee_count is a count.
CRM_COMPANY = Table(
    "crm_company", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("name", Text, nullable=False),
    Column("domain", Text),
    Column("industry", Text),
    Column("employee_count", Integer),
    Column("annual_revenue", Text),
    Column("address_line1", Text),
    Column("address_line2", Text),
    Column("city", Text),
    Column("state", Text),
    Column("postal_code", Text),
    Column("country", Text),
    Column("linkedin_url", Text),
    Column("lifecycle", Text, nullable=False, server_default=text("'prospect'")),
    Column("linked_customer_id", Text,
           ForeignKey("customer.id", ondelete="SET NULL")),
    Column("assigned_to_user_id", Text),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "lifecycle IN ('prospect','customer','partner','vendor','other')",
        name="ck_crm_company_lifecycle"),
    # domain UNIQUE where not NULL (case-insensitive): partial unique index on
    # lower(domain). Declared inline because SQLAlchemy cannot reflect an
    # expression index, so this is the form the seam's static reader attributes
    # to the right table (ADR-0034 step 2f).
    Index("uq_crm_company_domain", "company_id", text("lower(domain)"),
          unique=True,
          sqlite_where=text("domain IS NOT NULL"),
          postgresql_where=text("domain IS NOT NULL")),
)

Index("idx_crm_company_company", CRM_COMPANY.c.company_id)
Index("idx_crm_company_lifecycle", CRM_COMPANY.c.lifecycle)
Index("idx_crm_company_linked_customer", CRM_COMPANY.c.linked_customer_id)

# crm_contact — Person entity
CRM_CONTACT = Table(
    "crm_contact", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("name", Text, nullable=False),
    Column("email", Text),
    Column("phone", Text),
    Column("mobile", Text),
    Column("job_title", Text),
    Column("linkedin_url", Text),
    Column("address_line1", Text),
    Column("address_line2", Text),
    Column("city", Text),
    Column("state", Text),
    Column("postal_code", Text),
    Column("country", Text),
    Column("lifecycle", Text, nullable=False, server_default=text("'lead'")),
    Column("crm_company_id", Text,
           ForeignKey("crm_company.id", ondelete="SET NULL")),
    Column("assigned_to_user_id", Text),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("lifecycle IN ('lead','mql','sql','customer','other')",
                    name="ck_crm_contact_lifecycle"),
    # email UNIQUE where not NULL, case-insensitive: partial unique index on
    # lower(email). Inline for the same reason as uq_crm_company_domain.
    Index("uq_crm_contact_email", "company_id", text("lower(email)"),
          unique=True,
          sqlite_where=text("email IS NOT NULL"),
          postgresql_where=text("email IS NOT NULL")),
)

Index("idx_crm_contact_company", CRM_CONTACT.c.company_id)
Index("idx_crm_contact_crm_company", CRM_CONTACT.c.crm_company_id)
Index("idx_crm_contact_lifecycle", CRM_CONTACT.c.lifecycle)

# crm_contact_role — many-to-many: a person can work at multiple companies
CRM_CONTACT_ROLE = Table(
    "crm_contact_role", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("crm_contact_id", Text,
           ForeignKey("crm_contact.id", ondelete="CASCADE"), nullable=False),
    Column("crm_company_id", Text,
           ForeignKey("crm_company.id", ondelete="CASCADE"), nullable=False),
    Column("role_title", Text),
    Column("is_primary", Integer, nullable=False, server_default=text("0")),
    Column("started_at", Text),
    Column("ended_at", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("is_primary IN (0,1)", name="ck_crm_contact_role_is_primary"),
)

Index("idx_crm_contact_role_contact", CRM_CONTACT_ROLE.c.crm_contact_id)
Index("idx_crm_contact_role_company", CRM_CONTACT_ROLE.c.crm_company_id)
Index("uq_crm_contact_role",
      CRM_CONTACT_ROLE.c.crm_contact_id, CRM_CONTACT_ROLE.c.crm_company_id,
      unique=True)

# ==================================================================
# TASKS — FIRST-CLASS ENTITY (Wave 1B F2)
# crm_task / crm_task_link. A richer task row than crm_activity
# (status / priority / due_date lifecycle); crm_task_link is the
# many-to-many tie to any CRM entity (lead / opportunity / customer /
# crm_contact / crm_company). crm_activity is NOT replaced — legacy
# activity_type='task' rows stay valid. Growth-owned.
# ==================================================================

# crm_task — first-class task entity
CRM_TASK = Table(
    "crm_task", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("subject", Text, nullable=False),
    Column("description", Text),
    Column("status", Text, nullable=False, server_default=text("'open'")),
    Column("priority", Text, nullable=False, server_default=text("'medium'")),
    Column("due_date", Text),
    Column("assigned_to_user_id", Text),
    Column("created_by_user_id", Text),
    Column("completed_at", Text),
    Column("cancel_reason", Text),
    Column("linked_count", Integer, nullable=False, server_default=text("0")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("status IN ('open','in_progress','done','cancelled')",
                    name="ck_crm_task_status"),
    CheckConstraint("priority IN ('low','medium','high','urgent')",
                    name="ck_crm_task_priority"),
)

Index("idx_crm_task_company", CRM_TASK.c.company_id)
Index("idx_crm_task_status", CRM_TASK.c.status)
Index("idx_crm_task_assigned", CRM_TASK.c.assigned_to_user_id)
Index("idx_crm_task_due", CRM_TASK.c.due_date)

# crm_task_link — many-to-many: a task can attach to any CRM entity
# linked_entity_id is opaque (the entity type decides the table), so no FK.
CRM_TASK_LINK = Table(
    "crm_task_link", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("crm_task_id", Text,
           ForeignKey("crm_task.id", ondelete="CASCADE"), nullable=False),
    Column("linked_entity_type", Text, nullable=False),
    Column("linked_entity_id", Text, nullable=False),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "linked_entity_type IN ('lead','opportunity','customer','crm_contact','crm_company')",
        name="ck_crm_task_link_linked_entity_type"),
)

Index("idx_crm_task_link_task", CRM_TASK_LINK.c.crm_task_id)
Index("idx_crm_task_link_entity",
      CRM_TASK_LINK.c.linked_entity_type, CRM_TASK_LINK.c.linked_entity_id)
Index("uq_crm_task_link",
      CRM_TASK_LINK.c.crm_task_id, CRM_TASK_LINK.c.linked_entity_type,
      CRM_TASK_LINK.c.linked_entity_id, unique=True)

# ==================================================================
# Wave 1B F3 — Pipeline stages (customizable). crm_pipeline /
# crm_pipeline_stage (growth-owned). Foundation opportunity carries a
# nullable opaque FK column pipeline_stage_id -> crm_pipeline_stage (ADR-0023;
# growth is the SOLE writer of that column). The hardcoded opportunity.stage
# CHECK is dropped in foundation migration 024; the legacy `stage` text column
# stays for backward-compat (dual-path pipeline-report). A default
# "Standard Sales" 7-stage pipeline is seeded below so existing opportunity
# rows have somewhere to point. Pipelines are catalog rows (no company_id) —
# shared across the install, like a chart-of-accounts template.
# ==================================================================

# crm_pipeline — pipeline definition
CRM_PIPELINE = Table(
    "crm_pipeline", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("name", Text, nullable=False),
    Column("description", Text),
    Column("is_default", Integer, nullable=False, server_default=text("0")),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("is_default IN (0,1)", name="ck_crm_pipeline_is_default"),
    CheckConstraint("is_active IN (0,1)", name="ck_crm_pipeline_is_active"),
    # Pipeline names are unique case-insensitively across the install. Purely an
    # expression index — there is no plain column in it at all — so it must be
    # declared where the table is known.
    Index("uq_crm_pipeline_name", text("lower(name)"), unique=True),
)

# crm_pipeline_stage — ordered stage within a pipeline
# default_probability is a Decimal percentage and stays TEXT.
CRM_PIPELINE_STAGE = Table(
    "crm_pipeline_stage", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("crm_pipeline_id", Text,
           ForeignKey("crm_pipeline.id", ondelete="CASCADE"), nullable=False),
    Column("stage_order", Integer, nullable=False),
    Column("name", Text, nullable=False),
    Column("is_terminal_won", Integer, nullable=False, server_default=text("0")),
    Column("is_terminal_lost", Integer, nullable=False, server_default=text("0")),
    Column("default_probability", Text, nullable=False, server_default=text("'0'")),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("is_terminal_won IN (0,1)",
                    name="ck_crm_pipeline_stage_is_terminal_won"),
    CheckConstraint("is_terminal_lost IN (0,1)",
                    name="ck_crm_pipeline_stage_is_terminal_lost"),
    CheckConstraint("is_active IN (0,1)", name="ck_crm_pipeline_stage_is_active"),
    # Stage names are unique case-insensitively within one pipeline.
    Index("uq_crm_pipeline_stage_name", "crm_pipeline_id", text("lower(name)"),
          unique=True),
)

Index("idx_crm_pipeline_stage_pipeline", CRM_PIPELINE_STAGE.c.crm_pipeline_id)
Index("uq_crm_pipeline_stage_order",
      CRM_PIPELINE_STAGE.c.crm_pipeline_id, CRM_PIPELINE_STAGE.c.stage_order,
      unique=True)

# ==================================================================
# Wave 1B F4 — Saved views (filter-JSON DSL + persistence). crm_saved_view
# (growth-owned). A persisted, named view over one CRM entity: a bounded
# filter-JSON (operator + column whitelist, validated at SAVE-time, never
# interpolated into SQL) plus optional sort / group-by / column-order JSON.
# company_id is NOT NULL (multi-company-safe; matches every other company-scoped
# growth table — DECISION #2, Wave 1B plan). is_shared 0/1: a shared view is
# readable by every user in the company; only the owner may update or delete it.
# entity_type is CHECK-bounded over the 6 supported CRM entities. No FK on the
# opaque list-side (the view simply filters whatever list-<entity> returns).
# ==================================================================
CRM_SAVED_VIEW = Table(
    "crm_saved_view", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("name", Text, nullable=False),
    Column("entity_type", Text, nullable=False),
    Column("owner_user_id", Text),
    Column("is_shared", Integer, nullable=False, server_default=text("0")),
    Column("filter_json", Text),
    Column("sort_json", Text),
    Column("group_by_json", Text),
    Column("column_order_json", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    # The shipped DDL wraps this IN-list across two lines, which leaves a space
    # after 'customer',. It is part of the constraint body and is reproduced.
    CheckConstraint(
        "entity_type IN ('lead','opportunity','customer', "
        "'crm_contact','crm_company','crm_task')",
        name="ck_crm_saved_view_entity_type"),
    CheckConstraint("is_shared IN (0,1)", name="ck_crm_saved_view_is_shared"),
    # A fifth expression index, easy to miss: the shipped statement is split
    # across two Python string literals, and the third key is lower(name).
    Index("uq_crm_saved_view_name", "company_id", "owner_user_id",
          text("lower(name)"), unique=True),
)

Index("idx_crm_saved_view_company", CRM_SAVED_VIEW.c.company_id)
Index("idx_crm_saved_view_entity", CRM_SAVED_VIEW.c.entity_type)
Index("idx_crm_saved_view_owner", CRM_SAVED_VIEW.c.owner_user_id)

# ==================================================================
# AI ENGINE / ANALYTICS TABLES (moved from core init_schema.py)
# ==================================================================

# 16. anomaly
ANOMALY = Table(
    "anomaly", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("detected_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("anomaly_type", Text, nullable=False),
    Column("severity", Text, nullable=False, server_default=text("'info'")),
    Column("entity_type", Text),
    Column("entity_id", Text),
    Column("description", Text, nullable=False),
    Column("evidence", Text),
    Column("baseline", Text),
    Column("actual", Text),
    Column("deviation_pct", Text),
    Column("status", Text, nullable=False, server_default=text("'new'")),
    Column("resolution_notes", Text),
    Column("assigned_to", Text),
    Column("expires_at", Text),
    # Wrapped across seven lines in the shipped DDL; each wrap is a space inside
    # the constraint body, including right after the opening paren and before
    # the closing one.
    CheckConstraint(
        "anomaly_type IN ( "
        "'price_spike','volume_change','duplicate_possible', "
        "'margin_erosion','unusual_vendor','pattern_break', "
        "'consumption_spike','late_pattern','round_number', "
        "'ghost_employee','vendor_concentration', "
        "'sequence_violation','benford_deviation','budget_overrun', "
        "'inventory_shrinkage','payment_pattern_shift', "
        "'asset_book_value_drift','dimension_tag_drift', "
        "'reservation_over_available','subcontract_receipt_mismatch', "
        "'rate_plan_mismatch' )",
        name="ck_anomaly_anomaly_type"),
    CheckConstraint("severity IN ('info','warning','critical')",
                    name="ck_anomaly_severity"),
    CheckConstraint(
        "status IN ('new','acknowledged','investigated','dismissed','resolved')",
        name="ck_anomaly_status"),
)

Index("idx_anomaly_status", ANOMALY.c.status)
Index("idx_anomaly_type", ANOMALY.c.anomaly_type)
Index("idx_anomaly_severity", ANOMALY.c.severity)
Index("idx_anomaly_entity", ANOMALY.c.entity_type, ANOMALY.c.entity_id)

# 17. scenario
SCENARIO = Table(
    "scenario", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("question", Text, nullable=False),
    Column("scenario_type", Text, nullable=False),
    Column("assumptions", Text),
    Column("baseline", Text),
    Column("projected", Text),
    Column("impact_summary", Text),
    Column("confidence", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("expires_at", Text),
    CheckConstraint(
        "scenario_type IN ( "
        "'price_change','supplier_loss','demand_shift','cost_change', "
        "'hiring_impact','expansion','contraction' )",
        name="ck_scenario_scenario_type"),
)

Index("idx_scenario_type", SCENARIO.c.scenario_type)

# 18. correlation
CORRELATION = Table(
    "correlation", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("discovered_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("module_a", Text, nullable=False),
    Column("module_b", Text, nullable=False),
    Column("description", Text, nullable=False),
    Column("evidence", Text),
    Column("strength", Text, nullable=False, server_default=text("'moderate'")),
    Column("statistical_confidence", Text),
    Column("actionable", Integer, nullable=False, server_default=text("0")),
    Column("suggested_action", Text),
    Column("status", Text, nullable=False, server_default=text("'new'")),
    Column("expires_at", Text),
    CheckConstraint("strength IN ('weak','moderate','strong')",
                    name="ck_correlation_strength"),
    CheckConstraint("actionable IN (0,1)", name="ck_correlation_actionable"),
    CheckConstraint("status IN ('new','validated','dismissed')",
                    name="ck_correlation_status"),
)

Index("idx_correlation_status", CORRELATION.c.status)

# 19. categorization_rule
# target_account_id / target_cost_center_id are opaque here (no foreign keys in
# the shipped DDL, unlike most account references elsewhere). Preserved.
CATEGORIZATION_RULE = Table(
    "categorization_rule", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("pattern", Text, nullable=False),
    Column("source", Text, nullable=False),
    Column("target_account_id", Text),
    Column("target_cost_center_id", Text),
    Column("confidence", Text, nullable=False, server_default=text("'0'")),
    Column("times_applied", Integer, nullable=False, server_default=text("0")),
    Column("times_overridden", Integer, nullable=False, server_default=text("0")),
    Column("last_applied_at", Text),
    Column("created_by", Text, nullable=False, server_default=text("'ai'")),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("source IN ('bank_feed','ocr_vendor','email_subject')",
                    name="ck_categorization_rule_source"),
    CheckConstraint("created_by IN ('user','ai')",
                    name="ck_categorization_rule_created_by"),
)

Index("idx_categorization_source", CATEGORIZATION_RULE.c.source)

# 20. business_rule
BUSINESS_RULE = Table(
    "business_rule", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("rule_text", Text, nullable=False),
    Column("parsed_condition", Text),
    Column("applies_to", Text),
    Column("action", Text, nullable=False, server_default=text("'warn'")),
    Column("active", Integer, nullable=False, server_default=text("1")),
    Column("times_triggered", Integer, nullable=False, server_default=text("0")),
    Column("last_triggered_at", Text),
    Column("created_by", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "action IN ('block','warn','notify','auto_execute','suggest')",
        name="ck_business_rule_action"),
    CheckConstraint("active IN (0,1)", name="ck_business_rule_active"),
)

Index("idx_business_rule_active", BUSINESS_RULE.c.active)

# 21. pending_decision
PENDING_DECISION = Table(
    "pending_decision", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("context_id", Text),
    Column("question", Text, nullable=False),
    Column("options", Text),
    Column("deadline", Text),
    Column("impact", Text),
    Column("status", Text, nullable=False, server_default=text("'pending'")),
    Column("decision_made", Text),
    Column("decided_at", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("status IN ('pending','decided','expired')",
                    name="ck_pending_decision_status"),
)

Index("idx_pending_decision_status", PENDING_DECISION.c.status)
Index("idx_pending_decision_context", PENDING_DECISION.c.context_id)

# 22. usage_event — OWNED BY FOUNDATION (erpclaw-setup/init_schema.py) as of
# the 2026-05-31 migration audit (BUG-007). erpclaw-billing (foundation) also
# uses it, so a foundation module can't depend on an addon-owned table. growth
# reads/writes it as a foundation table; the definition + indexes live there.

# 23. audit_conversation
AUDIT_CONVERSATION = Table(
    "audit_conversation", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("timestamp", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("voucher_type", Text),
    Column("voucher_id", Text),
    Column("user_message", Text),
    Column("ai_interpretation", Text),
    Column("actions_taken", Text),
    Column("confidence_score", Text),
    Column("user_confirmed", Integer),
    Column("entity_changes", Text),
    CheckConstraint("user_confirmed IN (0,1)",
                    name="ck_audit_conversation_user_confirmed"),
)

Index("idx_audit_conv_voucher",
      AUDIT_CONVERSATION.c.voucher_type, AUDIT_CONVERSATION.c.voucher_id)
Index("idx_audit_conv_timestamp", AUDIT_CONVERSATION.c.timestamp)

# 24. conversation_context
CONVERSATION_CONTEXT = Table(
    "conversation_context", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("user_id", Text),
    Column("context_type", Text, nullable=False),
    Column("summary", Text),
    Column("related_entities", Text),
    Column("state", Text),
    Column("last_active", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("priority", Integer, nullable=False, server_default=text("0")),
    Column("expires_at", Text),
    CheckConstraint(
        "context_type IN ( "
        "'active_workflow','pending_decision','in_progress_analysis' )",
        name="ck_conversation_context_context_type"),
)

Index("idx_conv_ctx_user", CONVERSATION_CONTEXT.c.user_id)
Index("idx_conv_ctx_type", CONVERSATION_CONTEXT.c.context_type)

# 25. relationship_score
# Every score and lifetime_value is a Decimal and stays TEXT.
RELATIONSHIP_SCORE = Table(
    "relationship_score", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("party_type", Text, nullable=False),
    Column("party_id", Text, nullable=False),
    Column("score_date", Text, nullable=False),
    Column("overall_score", Text, nullable=False, server_default=text("'0'")),
    Column("payment_score", Text, nullable=False, server_default=text("'0'")),
    Column("volume_trend", Text),
    Column("profitability_score", Text, nullable=False, server_default=text("'0'")),
    Column("risk_score", Text, nullable=False, server_default=text("'0'")),
    Column("lifetime_value", Text, nullable=False, server_default=text("'0'")),
    Column("factors", Text),
    Column("ai_summary", Text),
    Column("expires_at", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("party_type IN ('customer','supplier')",
                    name="ck_relationship_score_party_type"),
    CheckConstraint("volume_trend IN ('growing','stable','declining')",
                    name="ck_relationship_score_volume_trend"),
)

Index("idx_rel_score_party",
      RELATIONSHIP_SCORE.c.party_type, RELATIONSHIP_SCORE.c.party_id)

# elimination_rule / elimination_entry were RETIRED 2026-08-12 (M63-C):
# a legacy pair no growth code ever used, whose only writer was a foundation
# action posting group eliminations straight into the live ledger. The real
# system is the foundation consolidation layer (ADR-0010); migration 007
# archives any rows an existing install holds and then drops both tables.
# Removed from this metadata at the phase-2 merge, where the conversion (which
# still declared them) met the retirement (which had removed them from the DDL).
# SIM: planning/simlogs/m63c_SIM_2026-08-12.md


def create_crmadv_tables(db_path=None):
    """Create growth tables and indexes on whichever backend is configured.

    Same contract as before the ADR-0034 conversion: idempotent, the returned
    counts are what was ACTUALLY created rather than what was declared, and the
    default "Standard Sales" pipeline is seeded afterwards.

    Provisioning and seeding are deliberately separate steps on separate
    connections. The seam's engine opens its own connection, so its DDL is not
    inside any transaction this function could hold; provision first, then open
    the DML connection to seed.
    """
    db_path = db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)

    # -- Verify ERPClaw foundation --
    _require_foundation(db_path)

    result = provision(METADATA, db_path)

    # Seed the default "Standard Sales" 7-stage pipeline (matches migration 024's
    # DEFAULT_PIPELINE_STAGES). Idempotent: only seed when no default pipeline exists.
    conn = get_connection(db_path)
    try:
        _seed_default_pipeline(conn)
        conn.commit()
    finally:
        conn.close()

    return {
        "database": db_path,
        "tables": result["tables"],
        "indexes": result["indexes"],
    }


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else None
    result = create_crmadv_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}", file=sys.stderr)
    print(f"  Tables: {result['tables']}", file=sys.stderr)
    print(f"  Indexes: {result['indexes']}", file=sys.stderr)
