#!/usr/bin/env python3
"""ERPClaw Compliance schema extension -- adds compliance tables to the shared database.

8 tables: audit_plan, audit_finding, risk_register, risk_assessment,
control_test, compliance_calendar, policy, policy_acknowledgment.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]
"""
import os
import sqlite3
import sys


DEFAULT_DB_PATH = os.path.expanduser("~/.openclaw/erpclaw/data.sqlite")
DISPLAY_NAME = "ERPClaw Compliance"

REQUIRED_FOUNDATION = ["company", "naming_series", "audit_log"]


def create_compliance_tables(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")

    # Verify ERPClaw foundation
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    missing = [t for t in REQUIRED_FOUNDATION if t not in tables]
    if missing:
        print(f"ERROR: Foundation tables missing: {', '.join(missing)}")
        print("Run erpclaw-setup first: clawhub install erpclaw-setup")
        conn.close()
        sys.exit(1)

    conn.executescript("""
        -- ==========================================================
        -- ERPClaw Compliance Domain Tables
        -- 8 tables, 4 domains
        -- Convention: TEXT for IDs (UUID4), TEXT for dates (ISO 8601)
        -- ==========================================================

        -- ── Audit Domain ─────────────────────────────────────────

        CREATE TABLE IF NOT EXISTS audit_plan (
            id TEXT PRIMARY KEY,
            naming_series TEXT,
            name TEXT NOT NULL,
            audit_type TEXT NOT NULL DEFAULT 'internal'
                CHECK(audit_type IN ('internal','external','regulatory','special')),
            scope TEXT,
            lead_auditor TEXT,
            planned_start TEXT,
            planned_end TEXT,
            actual_start TEXT,
            actual_end TEXT,
            status TEXT NOT NULL DEFAULT 'draft'
                CHECK(status IN ('draft','scheduled','in_progress','completed','cancelled')),
            notes TEXT,
            company_id TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_audit_plan_company ON audit_plan(company_id);
        CREATE INDEX IF NOT EXISTS idx_audit_plan_status ON audit_plan(status);
        CREATE INDEX IF NOT EXISTS idx_audit_plan_type ON audit_plan(audit_type);

        CREATE TABLE IF NOT EXISTS audit_finding (
            id TEXT PRIMARY KEY,
            audit_plan_id TEXT NOT NULL REFERENCES audit_plan(id) ON DELETE RESTRICT,
            finding_type TEXT NOT NULL DEFAULT 'observation'
                CHECK(finding_type IN ('critical','major','minor','observation','improvement')),
            title TEXT NOT NULL,
            description TEXT,
            area TEXT,
            root_cause TEXT,
            recommendation TEXT,
            management_response TEXT,
            remediation_due TEXT,
            remediation_status TEXT NOT NULL DEFAULT 'open'
                CHECK(remediation_status IN ('open','in_progress','remediated','verified','overdue','accepted')),
            assigned_to TEXT,
            company_id TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_audit_finding_plan ON audit_finding(audit_plan_id);
        CREATE INDEX IF NOT EXISTS idx_audit_finding_company ON audit_finding(company_id);
        CREATE INDEX IF NOT EXISTS idx_audit_finding_status ON audit_finding(remediation_status);

        -- ── Risk Domain ──────────────────────────────────────────

        CREATE TABLE IF NOT EXISTS risk_register (
            id TEXT PRIMARY KEY,
            naming_series TEXT,
            name TEXT NOT NULL,
            category TEXT NOT NULL DEFAULT 'operational'
                CHECK(category IN ('operational','financial','compliance','strategic','reputational','technology','other')),
            description TEXT,
            likelihood INTEGER NOT NULL DEFAULT 3 CHECK(likelihood BETWEEN 1 AND 5),
            impact INTEGER NOT NULL DEFAULT 3 CHECK(impact BETWEEN 1 AND 5),
            risk_score INTEGER,
            risk_level TEXT,
            owner TEXT,
            mitigation_plan TEXT,
            residual_likelihood INTEGER CHECK(residual_likelihood BETWEEN 1 AND 5),
            residual_impact INTEGER CHECK(residual_impact BETWEEN 1 AND 5),
            residual_score INTEGER,
            status TEXT NOT NULL DEFAULT 'identified'
                CHECK(status IN ('identified','assessed','mitigating','monitoring','closed','accepted')),
            review_date TEXT,
            company_id TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_risk_register_company ON risk_register(company_id);
        CREATE INDEX IF NOT EXISTS idx_risk_register_status ON risk_register(status);
        CREATE INDEX IF NOT EXISTS idx_risk_register_category ON risk_register(category);
        CREATE INDEX IF NOT EXISTS idx_risk_register_level ON risk_register(risk_level);

        CREATE TABLE IF NOT EXISTS risk_assessment (
            id TEXT PRIMARY KEY,
            risk_id TEXT NOT NULL REFERENCES risk_register(id) ON DELETE RESTRICT,
            assessment_date TEXT NOT NULL DEFAULT (date('now')),
            assessor TEXT,
            likelihood INTEGER NOT NULL CHECK(likelihood BETWEEN 1 AND 5),
            impact INTEGER NOT NULL CHECK(impact BETWEEN 1 AND 5),
            score INTEGER,
            notes TEXT,
            company_id TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_risk_assessment_risk ON risk_assessment(risk_id);
        CREATE INDEX IF NOT EXISTS idx_risk_assessment_company ON risk_assessment(company_id);

        -- ── Controls Domain ──────────────────────────────────────

        CREATE TABLE IF NOT EXISTS control_test (
            id TEXT PRIMARY KEY,
            naming_series TEXT,
            control_name TEXT NOT NULL,
            control_description TEXT,
            control_type TEXT NOT NULL DEFAULT 'preventive'
                CHECK(control_type IN ('preventive','detective','corrective','compensating')),
            frequency TEXT NOT NULL DEFAULT 'quarterly'
                CHECK(frequency IN ('continuous','daily','weekly','monthly','quarterly','semi_annual','annual')),
            test_date TEXT NOT NULL DEFAULT (date('now')),
            tester TEXT,
            test_procedure TEXT,
            test_result TEXT NOT NULL DEFAULT 'not_tested'
                CHECK(test_result IN ('not_tested','effective','ineffective','partially_effective','not_applicable')),
            evidence TEXT,
            deficiency_type TEXT CHECK(deficiency_type IN ('significant','material_weakness','control_deficiency',NULL)),
            remediation_plan TEXT,
            next_test_date TEXT,
            company_id TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_control_test_company ON control_test(company_id);
        CREATE INDEX IF NOT EXISTS idx_control_test_type ON control_test(control_type);
        CREATE INDEX IF NOT EXISTS idx_control_test_result ON control_test(test_result);

        CREATE TABLE IF NOT EXISTS compliance_calendar (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            compliance_type TEXT NOT NULL DEFAULT 'filing'
                CHECK(compliance_type IN ('filing','certification','renewal','inspection','report','training','other')),
            due_date TEXT NOT NULL,
            reminder_days INTEGER DEFAULT 30,
            responsible TEXT,
            description TEXT,
            recurrence TEXT CHECK(recurrence IN ('none','monthly','quarterly','semi_annual','annual',NULL)),
            status TEXT NOT NULL DEFAULT 'upcoming'
                CHECK(status IN ('upcoming','in_progress','completed','overdue','waived')),
            completed_date TEXT,
            notes TEXT,
            company_id TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_compliance_calendar_company ON compliance_calendar(company_id);
        CREATE INDEX IF NOT EXISTS idx_compliance_calendar_status ON compliance_calendar(status);
        CREATE INDEX IF NOT EXISTS idx_compliance_calendar_due ON compliance_calendar(due_date);

        -- ── Policy Domain ────────────────────────────────────────

        CREATE TABLE IF NOT EXISTS policy (
            id TEXT PRIMARY KEY,
            naming_series TEXT,
            title TEXT NOT NULL,
            policy_type TEXT NOT NULL DEFAULT 'general'
                CHECK(policy_type IN ('general','hr','financial','it','safety','compliance','operational','other')),
            version TEXT NOT NULL DEFAULT '1.0',
            content TEXT,
            effective_date TEXT,
            review_date TEXT,
            owner TEXT,
            status TEXT NOT NULL DEFAULT 'draft'
                CHECK(status IN ('draft','review','approved','published','retired')),
            requires_acknowledgment INTEGER NOT NULL DEFAULT 0 CHECK(requires_acknowledgment IN (0,1)),
            company_id TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_policy_company ON policy(company_id);
        CREATE INDEX IF NOT EXISTS idx_policy_status ON policy(status);
        CREATE INDEX IF NOT EXISTS idx_policy_type ON policy(policy_type);

        CREATE TABLE IF NOT EXISTS policy_acknowledgment (
            id TEXT PRIMARY KEY,
            policy_id TEXT NOT NULL REFERENCES policy(id) ON DELETE RESTRICT,
            employee_name TEXT NOT NULL,
            employee_id TEXT,
            acknowledged_date TEXT NOT NULL DEFAULT (date('now')),
            ip_address TEXT,
            notes TEXT,
            company_id TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_policy_ack_policy ON policy_acknowledgment(policy_id);
        CREATE INDEX IF NOT EXISTS idx_policy_ack_company ON policy_acknowledgment(company_id);
        CREATE INDEX IF NOT EXISTS idx_policy_ack_employee ON policy_acknowledgment(employee_id);
    """)

    conn.commit()
    conn.close()
    print(f"[{DISPLAY_NAME}] Schema created successfully at {db_path}")


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_PATH
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    create_compliance_tables(db_path)
