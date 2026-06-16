#!/usr/bin/env python3
"""erpclaw-growth schema extension -- adds advanced CRM/marketing tables to the shared database.

25 tables: 13 CRM advanced (campaigns, territories, contracts, automation, drip sequences)
+ 12 AI engine / analytics tables (moved from core init_schema.py):
  anomaly, scenario, correlation, categorization_rule, business_rule,
  pending_decision, usage_event, audit_conversation, conversation_context,
  relationship_score, elimination_rule, elimination_entry.
Part of the erpclaw-growth super-package (CRM + Analytics + AI Engine).

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys

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


def create_crmadv_tables(db_path=None):
    db_path = db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    conn = sqlite3.connect(db_path)
    from erpclaw_lib.db import setup_pragmas
    setup_pragmas(conn)

    # -- Verify ERPClaw foundation --
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    missing = [t for t in REQUIRED_FOUNDATION if t not in tables]
    if missing:
        print(f"ERROR: Foundation tables missing: {', '.join(missing)}", file=sys.stderr)
        print("Run erpclaw-setup first: clawhub install erpclaw-setup", file=sys.stderr)
        conn.close()
        sys.exit(1)

    tables_created = 0
    indexes_created = 0

    # ==================================================================
    # CAMPAIGNS DOMAIN
    # ==================================================================

    # 1. crmadv_campaign_template
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_campaign_template (
            id              TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            subject_template TEXT,
            body_html       TEXT,
            body_text       TEXT,
            template_type   TEXT DEFAULT 'newsletter'
                            CHECK(template_type IN ('newsletter','promotional','transactional','drip','welcome')),
            is_active       INTEGER DEFAULT 1,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_tmpl_company ON crmadv_campaign_template(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_tmpl_type ON crmadv_campaign_template(template_type)")
    indexes_created += 2

    # 2. crmadv_recipient_list
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_recipient_list (
            id              TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            description     TEXT,
            list_type       TEXT DEFAULT 'static'
                            CHECK(list_type IN ('static','dynamic','segment')),
            filter_criteria TEXT,
            recipient_count INTEGER DEFAULT 0,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_rlist_company ON crmadv_recipient_list(company_id)")
    indexes_created += 1

    # 3. crmadv_email_campaign
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_email_campaign (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            subject         TEXT,
            template_id     TEXT REFERENCES crmadv_campaign_template(id),
            recipient_list_id TEXT REFERENCES crmadv_recipient_list(id),
            campaign_status TEXT DEFAULT 'draft'
                            CHECK(campaign_status IN ('draft','scheduled','sending','sent','paused','cancelled')),
            scheduled_date  TEXT,
            sent_date       TEXT,
            total_sent      INTEGER DEFAULT 0,
            total_opened    INTEGER DEFAULT 0,
            total_clicked   INTEGER DEFAULT 0,
            total_bounced   INTEGER DEFAULT 0,
            total_unsubscribed INTEGER DEFAULT 0,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_camp_company ON crmadv_email_campaign(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_camp_status ON crmadv_email_campaign(campaign_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_camp_template ON crmadv_email_campaign(template_id)")
    indexes_created += 3

    # 4. crmadv_campaign_event
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_campaign_event (
            id              TEXT PRIMARY KEY,
            campaign_id     TEXT NOT NULL REFERENCES crmadv_email_campaign(id),
            event_type      TEXT NOT NULL
                            CHECK(event_type IN ('sent','opened','clicked','bounced','unsubscribed','converted')),
            recipient_email TEXT,
            event_timestamp TEXT,
            metadata        TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_evt_campaign ON crmadv_campaign_event(campaign_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_evt_type ON crmadv_campaign_event(event_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_evt_company ON crmadv_campaign_event(company_id)")
    indexes_created += 3

    # ==================================================================
    # TERRITORIES DOMAIN
    # ==================================================================

    # 5. crmadv_territory
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_territory (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            region          TEXT,
            parent_territory_id TEXT REFERENCES crmadv_territory(id),
            territory_type  TEXT DEFAULT 'geographic'
                            CHECK(territory_type IN ('geographic','industry','named_account','product')),
            territory_status TEXT DEFAULT 'active'
                            CHECK(territory_status IN ('active','inactive')),
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_terr_company ON crmadv_territory(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_terr_parent ON crmadv_territory(parent_territory_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_terr_type ON crmadv_territory(territory_type)")
    indexes_created += 3

    # 6. crmadv_territory_assignment
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_territory_assignment (
            id              TEXT PRIMARY KEY,
            territory_id    TEXT NOT NULL REFERENCES crmadv_territory(id),
            salesperson     TEXT NOT NULL,
            start_date      TEXT,
            end_date        TEXT,
            assignment_status TEXT DEFAULT 'active'
                            CHECK(assignment_status IN ('active','ended')),
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_tassign_terr ON crmadv_territory_assignment(territory_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_tassign_company ON crmadv_territory_assignment(company_id)")
    indexes_created += 2

    # 7. crmadv_territory_quota
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_territory_quota (
            id              TEXT PRIMARY KEY,
            territory_id    TEXT NOT NULL REFERENCES crmadv_territory(id),
            period          TEXT NOT NULL,
            quota_amount    TEXT NOT NULL,
            actual_amount   TEXT DEFAULT '0',
            attainment_pct  TEXT DEFAULT '0',
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_tquota_terr ON crmadv_territory_quota(territory_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_tquota_company ON crmadv_territory_quota(company_id)")
    indexes_created += 2

    # ==================================================================
    # CONTRACTS DOMAIN
    # ==================================================================

    # 8. crmadv_contract
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_contract (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            customer_name   TEXT NOT NULL,
            contract_type   TEXT DEFAULT 'service'
                            CHECK(contract_type IN ('service','subscription','licensing','license','maintenance','consulting')),
            contract_status TEXT DEFAULT 'draft'
                            CHECK(contract_status IN ('draft','active','expired','renewed','terminated')),
            start_date      TEXT,
            end_date        TEXT,
            total_value     TEXT,
            annual_value    TEXT,
            auto_renew      INTEGER DEFAULT 0,
            renewal_terms   TEXT,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_ctr_company ON crmadv_contract(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_ctr_status ON crmadv_contract(contract_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_ctr_type ON crmadv_contract(contract_type)")
    indexes_created += 3

    # 9. crmadv_contract_obligation
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_contract_obligation (
            id              TEXT PRIMARY KEY,
            contract_id     TEXT NOT NULL REFERENCES crmadv_contract(id),
            description     TEXT NOT NULL,
            due_date        TEXT,
            obligee         TEXT,
            obligation_status TEXT DEFAULT 'pending'
                            CHECK(obligation_status IN ('pending','in_progress','completed','overdue')),
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_obl_contract ON crmadv_contract_obligation(contract_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_obl_status ON crmadv_contract_obligation(obligation_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_obl_company ON crmadv_contract_obligation(company_id)")
    indexes_created += 3

    # ==================================================================
    # AUTOMATION DOMAIN
    # ==================================================================

    # 10. crmadv_automation_workflow
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_automation_workflow (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            trigger_event   TEXT,
            conditions_json TEXT DEFAULT '{}',
            actions_json    TEXT DEFAULT '[]',
            workflow_status TEXT DEFAULT 'inactive'
                            CHECK(workflow_status IN ('active','inactive','paused')),
            execution_count INTEGER DEFAULT 0,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_wf_company ON crmadv_automation_workflow(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_wf_status ON crmadv_automation_workflow(workflow_status)")
    indexes_created += 2

    # 11. crmadv_lead_score_rule
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_lead_score_rule (
            id              TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            criteria_json   TEXT NOT NULL,
            points          INTEGER NOT NULL DEFAULT 0,
            is_active       INTEGER DEFAULT 1,
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_lsr_company ON crmadv_lead_score_rule(company_id)")
    indexes_created += 1

    # 12. crmadv_nurture_sequence
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_nurture_sequence (
            id              TEXT PRIMARY KEY,
            naming_series   TEXT,
            name            TEXT NOT NULL,
            description     TEXT,
            steps_json      TEXT DEFAULT '[]',
            total_steps     INTEGER DEFAULT 0,
            sequence_status TEXT DEFAULT 'draft'
                            CHECK(sequence_status IN ('draft','active','paused','completed')),
            company_id      TEXT NOT NULL REFERENCES company(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_ns_company ON crmadv_nurture_sequence(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_ns_status ON crmadv_nurture_sequence(sequence_status)")
    indexes_created += 2

    # 13. crmadv_drip_sequence (M8 phase B -- drip campaign sequences)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_drip_sequence (
            id            TEXT PRIMARY KEY,
            company_id    TEXT NOT NULL REFERENCES company(id),
            name          TEXT NOT NULL,
            description   TEXT,
            is_active     INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0, 1)),
            created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_drip_company ON crmadv_drip_sequence(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_drip_active ON crmadv_drip_sequence(is_active)")
    indexes_created += 2

    # 14. crmadv_drip_sequence_step (M8 phase B -- steps within a drip sequence)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_drip_sequence_step (
            id                TEXT PRIMARY KEY,
            sequence_id       TEXT NOT NULL REFERENCES crmadv_drip_sequence(id),
            step_order        INTEGER NOT NULL,
            delay_hours       INTEGER NOT NULL DEFAULT 0,
            email_template_id TEXT,
            is_active         INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0, 1)),
            created_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_drip_step_seq ON crmadv_drip_sequence_step(sequence_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_drip_step_seq_order ON crmadv_drip_sequence_step(sequence_id, step_order)")
    indexes_created += 2

    # 15. crmadv_drip_enrollment (M8 phase B -- contacts enrolled in a drip sequence)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crmadv_drip_enrollment (
            id            TEXT PRIMARY KEY,
            sequence_id   TEXT NOT NULL REFERENCES crmadv_drip_sequence(id),
            contact_id    TEXT NOT NULL,
            current_step  INTEGER NOT NULL DEFAULT 0,
            status        TEXT NOT NULL DEFAULT 'active'
                          CHECK(status IN ('active', 'completed', 'cancelled')),
            next_send_at  TEXT,
            enrolled_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_drip_enr_seq ON crmadv_drip_enrollment(sequence_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_drip_enr_contact ON crmadv_drip_enrollment(contact_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_drip_enr_status_send ON crmadv_drip_enrollment(status, next_send_at)")
    indexes_created += 3

    # ==================================================================
    # CONTACT + COMPANY MODEL (Wave 1B F1)
    # crm_contact / crm_company / crm_contact_role. Person + Org entities
    # that the foundation lead/opportunity/customer/crm_activity tables point
    # at via the nullable FK columns added in foundation migration 023
    # (ADR-0023). Growth is the sole writer of both these tables and those
    # foundation FK columns.
    # ==================================================================

    # crm_company — Org entity (defined before crm_contact: contact FKs company)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_company (
            id                 TEXT PRIMARY KEY,
            name               TEXT NOT NULL,
            domain             TEXT,
            industry           TEXT,
            employee_count     INTEGER,
            annual_revenue     TEXT,
            address_line1      TEXT,
            address_line2      TEXT,
            city               TEXT,
            state              TEXT,
            postal_code        TEXT,
            country            TEXT,
            linkedin_url       TEXT,
            lifecycle          TEXT NOT NULL DEFAULT 'prospect'
                               CHECK(lifecycle IN ('prospect','customer','partner','vendor','other')),
            linked_customer_id TEXT REFERENCES customer(id) ON DELETE SET NULL,
            assigned_to_user_id TEXT,
            notes              TEXT,
            company_id         TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_company_company ON crm_company(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_company_lifecycle ON crm_company(lifecycle)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_company_linked_customer ON crm_company(linked_customer_id)")
    # domain UNIQUE where not NULL (case-insensitive): partial unique index on lower(domain)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_company_domain ON crm_company(company_id, lower(domain)) WHERE domain IS NOT NULL")
    indexes_created += 4

    # crm_contact — Person entity
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_contact (
            id                 TEXT PRIMARY KEY,
            name               TEXT NOT NULL,
            email              TEXT,
            phone              TEXT,
            mobile             TEXT,
            job_title          TEXT,
            linkedin_url       TEXT,
            address_line1      TEXT,
            address_line2      TEXT,
            city               TEXT,
            state              TEXT,
            postal_code        TEXT,
            country            TEXT,
            lifecycle          TEXT NOT NULL DEFAULT 'lead'
                               CHECK(lifecycle IN ('lead','mql','sql','customer','other')),
            crm_company_id     TEXT REFERENCES crm_company(id) ON DELETE SET NULL,
            assigned_to_user_id TEXT,
            notes              TEXT,
            company_id         TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_contact_company ON crm_contact(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_contact_crm_company ON crm_contact(crm_company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_contact_lifecycle ON crm_contact(lifecycle)")
    # email UNIQUE where not NULL, case-insensitive: partial unique index on lower(email)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_contact_email ON crm_contact(company_id, lower(email)) WHERE email IS NOT NULL")
    indexes_created += 4

    # crm_contact_role — many-to-many: a person can work at multiple companies
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_contact_role (
            id                 TEXT PRIMARY KEY,
            crm_contact_id     TEXT NOT NULL REFERENCES crm_contact(id) ON DELETE CASCADE,
            crm_company_id     TEXT NOT NULL REFERENCES crm_company(id) ON DELETE CASCADE,
            role_title         TEXT,
            is_primary         INTEGER NOT NULL DEFAULT 0 CHECK(is_primary IN (0,1)),
            started_at         TEXT,
            ended_at           TEXT,
            company_id         TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_contact_role_contact ON crm_contact_role(crm_contact_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_contact_role_company ON crm_contact_role(crm_company_id)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_contact_role ON crm_contact_role(crm_contact_id, crm_company_id)")
    indexes_created += 3

    # ==================================================================
    # TASKS — FIRST-CLASS ENTITY (Wave 1B F2)
    # crm_task / crm_task_link. A richer task row than crm_activity
    # (status / priority / due_date lifecycle); crm_task_link is the
    # many-to-many tie to any CRM entity (lead / opportunity / customer /
    # crm_contact / crm_company). crm_activity is NOT replaced — legacy
    # activity_type='task' rows stay valid. Growth-owned.
    # ==================================================================

    # crm_task — first-class task entity
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_task (
            id                  TEXT PRIMARY KEY,
            subject             TEXT NOT NULL,
            description         TEXT,
            status              TEXT NOT NULL DEFAULT 'open'
                                CHECK(status IN ('open','in_progress','done','cancelled')),
            priority            TEXT NOT NULL DEFAULT 'medium'
                                CHECK(priority IN ('low','medium','high','urgent')),
            due_date            TEXT,
            assigned_to_user_id TEXT,
            created_by_user_id  TEXT,
            completed_at        TEXT,
            cancel_reason       TEXT,
            linked_count        INTEGER NOT NULL DEFAULT 0,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_task_company ON crm_task(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_task_status ON crm_task(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_task_assigned ON crm_task(assigned_to_user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_task_due ON crm_task(due_date)")
    indexes_created += 4

    # crm_task_link — many-to-many: a task can attach to any CRM entity
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_task_link (
            id                 TEXT PRIMARY KEY,
            crm_task_id        TEXT NOT NULL REFERENCES crm_task(id) ON DELETE CASCADE,
            linked_entity_type TEXT NOT NULL
                               CHECK(linked_entity_type IN ('lead','opportunity','customer','crm_contact','crm_company')),
            linked_entity_id   TEXT NOT NULL,
            company_id         TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_task_link_task ON crm_task_link(crm_task_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_task_link_entity ON crm_task_link(linked_entity_type, linked_entity_id)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_task_link ON crm_task_link(crm_task_id, linked_entity_type, linked_entity_id)")
    indexes_created += 3

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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_pipeline (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            description TEXT,
            is_default  INTEGER NOT NULL DEFAULT 0 CHECK(is_default IN (0,1)),
            is_active   INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_pipeline_name ON crm_pipeline(lower(name))")
    indexes_created += 1

    # crm_pipeline_stage — ordered stage within a pipeline
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_pipeline_stage (
            id                  TEXT PRIMARY KEY,
            crm_pipeline_id     TEXT NOT NULL REFERENCES crm_pipeline(id) ON DELETE CASCADE,
            stage_order         INTEGER NOT NULL,
            name                TEXT NOT NULL,
            is_terminal_won     INTEGER NOT NULL DEFAULT 0 CHECK(is_terminal_won IN (0,1)),
            is_terminal_lost    INTEGER NOT NULL DEFAULT 0 CHECK(is_terminal_lost IN (0,1)),
            default_probability TEXT NOT NULL DEFAULT '0',
            is_active           INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_pipeline_stage_pipeline ON crm_pipeline_stage(crm_pipeline_id)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_pipeline_stage_order ON crm_pipeline_stage(crm_pipeline_id, stage_order)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_pipeline_stage_name ON crm_pipeline_stage(crm_pipeline_id, lower(name))")
    indexes_created += 3

    # Seed the default "Standard Sales" 7-stage pipeline (matches migration 024's
    # DEFAULT_PIPELINE_STAGES). Idempotent: only seed when no default pipeline exists.
    _seed_default_pipeline(conn)

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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_saved_view (
            id                TEXT PRIMARY KEY,
            name              TEXT NOT NULL,
            entity_type       TEXT NOT NULL
                              CHECK(entity_type IN ('lead','opportunity','customer',
                                                    'crm_contact','crm_company','crm_task')),
            owner_user_id     TEXT,
            is_shared         INTEGER NOT NULL DEFAULT 0 CHECK(is_shared IN (0,1)),
            filter_json       TEXT,
            sort_json         TEXT,
            group_by_json     TEXT,
            column_order_json TEXT,
            company_id        TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_saved_view_company ON crm_saved_view(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_saved_view_entity ON crm_saved_view(entity_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crm_saved_view_owner ON crm_saved_view(owner_user_id)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_saved_view_name "
                 "ON crm_saved_view(company_id, owner_user_id, lower(name))")
    indexes_created += 4

    # ==================================================================
    # AI ENGINE / ANALYTICS TABLES (moved from core init_schema.py)
    # ==================================================================

    # 13. anomaly
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anomaly (
            id              TEXT PRIMARY KEY,
            detected_at     TEXT DEFAULT CURRENT_TIMESTAMP,
            anomaly_type    TEXT NOT NULL CHECK(anomaly_type IN (
                                'price_spike','volume_change','duplicate_possible',
                                'margin_erosion','unusual_vendor','pattern_break',
                                'consumption_spike','late_pattern','round_number',
                                'ghost_employee','vendor_concentration',
                                'sequence_violation','benford_deviation','budget_overrun',
                                'inventory_shrinkage','payment_pattern_shift',
                                'asset_book_value_drift','dimension_tag_drift'
                            )),
            severity        TEXT NOT NULL DEFAULT 'info'
                            CHECK(severity IN ('info','warning','critical')),
            entity_type     TEXT,
            entity_id       TEXT,
            description     TEXT NOT NULL,
            evidence        TEXT,
            baseline        TEXT,
            actual          TEXT,
            deviation_pct   TEXT,
            status          TEXT NOT NULL DEFAULT 'new'
                            CHECK(status IN ('new','acknowledged','investigated','dismissed','resolved')),
            resolution_notes TEXT,
            assigned_to     TEXT,
            expires_at      TEXT
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_anomaly_status ON anomaly(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_anomaly_type ON anomaly(anomaly_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_anomaly_severity ON anomaly(severity)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_anomaly_entity ON anomaly(entity_type, entity_id)")
    indexes_created += 4

    # 14. scenario
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scenario (
            id              TEXT PRIMARY KEY,
            question        TEXT NOT NULL,
            scenario_type   TEXT NOT NULL CHECK(scenario_type IN (
                                'price_change','supplier_loss','demand_shift','cost_change',
                                'hiring_impact','expansion','contraction'
                            )),
            assumptions     TEXT,
            baseline        TEXT,
            projected       TEXT,
            impact_summary  TEXT,
            confidence      TEXT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            expires_at      TEXT
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_scenario_type ON scenario(scenario_type)")
    indexes_created += 1

    # 15. correlation
    conn.execute("""
        CREATE TABLE IF NOT EXISTS correlation (
            id              TEXT PRIMARY KEY,
            discovered_at   TEXT DEFAULT CURRENT_TIMESTAMP,
            module_a        TEXT NOT NULL,
            module_b        TEXT NOT NULL,
            description     TEXT NOT NULL,
            evidence        TEXT,
            strength        TEXT NOT NULL DEFAULT 'moderate'
                            CHECK(strength IN ('weak','moderate','strong')),
            statistical_confidence TEXT,
            actionable      INTEGER NOT NULL DEFAULT 0 CHECK(actionable IN (0,1)),
            suggested_action TEXT,
            status          TEXT NOT NULL DEFAULT 'new'
                            CHECK(status IN ('new','validated','dismissed')),
            expires_at      TEXT
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_correlation_status ON correlation(status)")
    indexes_created += 1

    # 16. categorization_rule
    conn.execute("""
        CREATE TABLE IF NOT EXISTS categorization_rule (
            id              TEXT PRIMARY KEY,
            pattern         TEXT NOT NULL,
            source          TEXT NOT NULL CHECK(source IN ('bank_feed','ocr_vendor','email_subject')),
            target_account_id TEXT,
            target_cost_center_id TEXT,
            confidence      TEXT NOT NULL DEFAULT '0',
            times_applied   INTEGER NOT NULL DEFAULT 0,
            times_overridden INTEGER NOT NULL DEFAULT 0,
            last_applied_at TEXT,
            created_by      TEXT NOT NULL DEFAULT 'ai'
                            CHECK(created_by IN ('user','ai')),
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_categorization_source ON categorization_rule(source)")
    indexes_created += 1

    # 17. business_rule
    conn.execute("""
        CREATE TABLE IF NOT EXISTS business_rule (
            id              TEXT PRIMARY KEY,
            rule_text       TEXT NOT NULL,
            parsed_condition TEXT,
            applies_to      TEXT,
            action          TEXT NOT NULL DEFAULT 'warn'
                            CHECK(action IN ('block','warn','notify','auto_execute','suggest')),
            active          INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
            times_triggered INTEGER NOT NULL DEFAULT 0,
            last_triggered_at TEXT,
            created_by      TEXT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_business_rule_active ON business_rule(active)")
    indexes_created += 1

    # 18. pending_decision
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pending_decision (
            id              TEXT PRIMARY KEY,
            context_id      TEXT,
            question        TEXT NOT NULL,
            options         TEXT,
            deadline        TEXT,
            impact          TEXT,
            status          TEXT NOT NULL DEFAULT 'pending'
                            CHECK(status IN ('pending','decided','expired')),
            decision_made   TEXT,
            decided_at      TEXT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_decision_status ON pending_decision(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_decision_context ON pending_decision(context_id)")
    indexes_created += 2

    # 19. usage_event — OWNED BY FOUNDATION (erpclaw-setup/init_schema.py) as of
    # the 2026-05-31 migration audit (BUG-007). erpclaw-billing (foundation) also
    # uses it, so a foundation module can't depend on an addon-owned table. growth
    # reads/writes it as a foundation table; the definition + indexes live there.

    # 20. audit_conversation
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_conversation (
            id              TEXT PRIMARY KEY,
            timestamp       TEXT DEFAULT CURRENT_TIMESTAMP,
            voucher_type    TEXT,
            voucher_id      TEXT,
            user_message    TEXT,
            ai_interpretation TEXT,
            actions_taken   TEXT,
            confidence_score TEXT,
            user_confirmed  INTEGER CHECK(user_confirmed IN (0,1)),
            entity_changes  TEXT
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_conv_voucher ON audit_conversation(voucher_type, voucher_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_conv_timestamp ON audit_conversation(timestamp)")
    indexes_created += 2

    # 21. conversation_context
    conn.execute("""
        CREATE TABLE IF NOT EXISTS conversation_context (
            id              TEXT PRIMARY KEY,
            user_id         TEXT,
            context_type    TEXT NOT NULL CHECK(context_type IN (
                                'active_workflow','pending_decision','in_progress_analysis'
                            )),
            summary         TEXT,
            related_entities TEXT,
            state           TEXT,
            last_active     TEXT DEFAULT CURRENT_TIMESTAMP,
            priority        INTEGER NOT NULL DEFAULT 0,
            expires_at      TEXT
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_conv_ctx_user ON conversation_context(user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_conv_ctx_type ON conversation_context(context_type)")
    indexes_created += 2

    # 22. relationship_score
    conn.execute("""
        CREATE TABLE IF NOT EXISTS relationship_score (
            id              TEXT PRIMARY KEY,
            party_type      TEXT NOT NULL CHECK(party_type IN ('customer','supplier')),
            party_id        TEXT NOT NULL,
            score_date      TEXT NOT NULL,
            overall_score   TEXT NOT NULL DEFAULT '0',
            payment_score   TEXT NOT NULL DEFAULT '0',
            volume_trend    TEXT CHECK(volume_trend IN ('growing','stable','declining')),
            profitability_score TEXT NOT NULL DEFAULT '0',
            risk_score      TEXT NOT NULL DEFAULT '0',
            lifetime_value  TEXT NOT NULL DEFAULT '0',
            factors         TEXT,
            ai_summary      TEXT,
            expires_at      TEXT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rel_score_party ON relationship_score(party_type, party_id)")
    indexes_created += 1

    # 23. elimination_rule (intercompany elimination, moved from GL)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS elimination_rule (
            id                  TEXT PRIMARY KEY,
            name                TEXT NOT NULL,
            source_company_id   TEXT NOT NULL,
            target_company_id   TEXT NOT NULL,
            source_account_id   TEXT NOT NULL,
            target_account_id   TEXT NOT NULL,
            status              TEXT NOT NULL DEFAULT 'active'
                                CHECK(status IN ('active','disabled')),
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_elim_rule_source ON elimination_rule(source_company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_elim_rule_target ON elimination_rule(target_company_id)")
    indexes_created += 2

    # 24. elimination_entry (intercompany elimination, moved from GL)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS elimination_entry (
            id                      TEXT PRIMARY KEY,
            elimination_rule_id     TEXT NOT NULL REFERENCES elimination_rule(id) ON DELETE RESTRICT,
            fiscal_year_id          TEXT,
            posting_date            TEXT NOT NULL,
            amount                  TEXT NOT NULL DEFAULT '0',
            source_gl_entry_id      TEXT,
            target_gl_entry_id      TEXT,
            status                  TEXT NOT NULL DEFAULT 'posted'
                                    CHECK(status IN ('posted','reversed')),
            created_at              TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_elim_entry_rule ON elimination_entry(elimination_rule_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_elim_entry_fy ON elimination_entry(fiscal_year_id)")
    indexes_created += 2

    conn.commit()
    conn.close()

    return {
        "database": db_path,
        "tables": tables_created,
        "indexes": indexes_created,
    }


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else None
    result = create_crmadv_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}", file=sys.stderr)
    print(f"  Tables: {result['tables']}", file=sys.stderr)
    print(f"  Indexes: {result['indexes']}", file=sys.stderr)
