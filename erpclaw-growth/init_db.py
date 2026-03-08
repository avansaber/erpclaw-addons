#!/usr/bin/env python3
"""erpclaw-growth schema extension -- adds advanced CRM/marketing tables to the shared database.

12 tables across 5 domains: campaigns, territories, contracts, automation, analytics.
Part of the erpclaw-growth super-package (CRM Advanced domain).

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys

DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
DISPLAY_NAME = "ERPClaw Growth (CRM Advanced tables)"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log",
]


def create_crmadv_tables(db_path=None):
    db_path = db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")

    # -- Verify ERPClaw foundation --
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    missing = [t for t in REQUIRED_FOUNDATION if t not in tables]
    if missing:
        print(f"ERROR: Foundation tables missing: {', '.join(missing)}")
        print("Run erpclaw-setup first: clawhub install erpclaw-setup")
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
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
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
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
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
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
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
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
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
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
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
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
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
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
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
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
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
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
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
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
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
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
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
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_ns_company ON crmadv_nurture_sequence(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crmadv_ns_status ON crmadv_nurture_sequence(sequence_status)")
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
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
