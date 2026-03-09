#!/usr/bin/env python3
"""erpclaw-growth schema extension -- adds advanced CRM/marketing tables to the shared database.

24 tables: 12 CRM advanced (campaigns, territories, contracts, automation, analytics)
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

    # ==================================================================
    # AI ENGINE / ANALYTICS TABLES (moved from core init_schema.py)
    # ==================================================================

    # 13. anomaly
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anomaly (
            id              TEXT PRIMARY KEY,
            detected_at     TEXT DEFAULT (datetime('now')),
            anomaly_type    TEXT NOT NULL CHECK(anomaly_type IN (
                                'price_spike','volume_change','duplicate_possible',
                                'margin_erosion','unusual_vendor','pattern_break',
                                'consumption_spike','late_pattern','round_number',
                                'ghost_employee','vendor_concentration',
                                'sequence_violation','benford_deviation','budget_overrun',
                                'inventory_shrinkage','payment_pattern_shift'
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
            created_at      TEXT DEFAULT (datetime('now')),
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
            discovered_at   TEXT DEFAULT (datetime('now')),
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
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
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
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
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
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_decision_status ON pending_decision(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_decision_context ON pending_decision(context_id)")
    indexes_created += 2

    # 19. usage_event
    conn.execute("""
        CREATE TABLE IF NOT EXISTS usage_event (
            id              TEXT PRIMARY KEY,
            customer_id     TEXT,
            meter_id        TEXT,
            event_type      TEXT NOT NULL,
            quantity        TEXT NOT NULL DEFAULT '0',
            timestamp       TEXT NOT NULL,
            metadata        TEXT,
            idempotency_key TEXT UNIQUE,
            billing_period_id TEXT,
            processed       INTEGER NOT NULL DEFAULT 0 CHECK(processed IN (0,1)),
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_usage_event_customer ON usage_event(customer_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_usage_event_meter ON usage_event(meter_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_usage_event_processed ON usage_event(processed)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_usage_event_idempotency ON usage_event(idempotency_key)")
    indexes_created += 4

    # 20. audit_conversation
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_conversation (
            id              TEXT PRIMARY KEY,
            timestamp       TEXT DEFAULT (datetime('now')),
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
            last_active     TEXT DEFAULT (datetime('now')),
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
            created_at      TEXT DEFAULT (datetime('now'))
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
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
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
            created_at              TEXT DEFAULT (datetime('now'))
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
