#!/usr/bin/env python3
"""ERPClaw Compliance schema extension -- adds compliance tables to the shared database.

8 tables: audit_plan, audit_finding, risk_register, risk_assessment,
control_test, compliance_calendar, policy, policy_acknowledgment.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Conversion rules are the pilot's (`erpclaw-esign`): seam
vocabulary only, IDs stay TEXT, ``Integer`` only for the 1-5 risk scores and the
0/1 acknowledgment flag, and ``primary_key=True, nullable=True`` reproduces
SQLite's ``id TEXT PRIMARY KEY`` without adding a NOT NULL that never shipped.
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
DISPLAY_NAME = "ERPClaw Compliance"

REQUIRED_FOUNDATION = ["company", "naming_series", "audit_log"]

METADATA = MetaData()

# Foundation table this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)

# ---------------------------------------------------------------------------
# Audit domain
# 1. audit_plan
# ---------------------------------------------------------------------------
AUDIT_PLAN = Table(
    "audit_plan", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("audit_type", Text, nullable=False, server_default=text("'internal'")),
    Column("scope", Text),
    Column("lead_auditor", Text),
    Column("planned_start", Text),
    Column("planned_end", Text),
    Column("actual_start", Text),
    Column("actual_end", Text),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "audit_type IN ('internal','external','regulatory','special')",
        name="ck_audit_plan_audit_type"),
    CheckConstraint(
        "status IN ('draft','scheduled','in_progress','completed','cancelled')",
        name="ck_audit_plan_status"),
)

Index("idx_audit_plan_company", AUDIT_PLAN.c.company_id)
Index("idx_audit_plan_status", AUDIT_PLAN.c.status)
Index("idx_audit_plan_type", AUDIT_PLAN.c.audit_type)

# ---------------------------------------------------------------------------
# 2. audit_finding
# ---------------------------------------------------------------------------
AUDIT_FINDING = Table(
    "audit_finding", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("audit_plan_id", Text,
           ForeignKey("audit_plan.id", ondelete="RESTRICT"), nullable=False),
    Column("finding_type", Text, nullable=False,
           server_default=text("'observation'")),
    Column("title", Text, nullable=False),
    Column("description", Text),
    Column("area", Text),
    Column("root_cause", Text),
    Column("recommendation", Text),
    Column("management_response", Text),
    Column("remediation_due", Text),
    Column("remediation_status", Text, nullable=False,
           server_default=text("'open'")),
    Column("assigned_to", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "finding_type IN ('critical','major','minor','observation','improvement')",
        name="ck_audit_finding_finding_type"),
    CheckConstraint(
        "remediation_status IN ('open','in_progress','remediated','verified',"
        "'overdue','accepted')",
        name="ck_audit_finding_remediation_status"),
)

Index("idx_audit_finding_plan", AUDIT_FINDING.c.audit_plan_id)
Index("idx_audit_finding_company", AUDIT_FINDING.c.company_id)
Index("idx_audit_finding_status", AUDIT_FINDING.c.remediation_status)

# ---------------------------------------------------------------------------
# Risk domain
# 3. risk_register
# ---------------------------------------------------------------------------
RISK_REGISTER = Table(
    "risk_register", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("category", Text, nullable=False, server_default=text("'operational'")),
    Column("description", Text),
    Column("likelihood", Integer, nullable=False, server_default=text("3")),
    Column("impact", Integer, nullable=False, server_default=text("3")),
    Column("risk_score", Integer),
    Column("risk_level", Text),
    Column("owner", Text),
    Column("mitigation_plan", Text),
    Column("residual_likelihood", Integer),
    Column("residual_impact", Integer),
    Column("residual_score", Integer),
    Column("status", Text, nullable=False, server_default=text("'identified'")),
    Column("review_date", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "category IN ('operational','financial','compliance','strategic',"
        "'reputational','technology','other')",
        name="ck_risk_register_category"),
    CheckConstraint("likelihood BETWEEN 1 AND 5",
                    name="ck_risk_register_likelihood"),
    CheckConstraint("impact BETWEEN 1 AND 5", name="ck_risk_register_impact"),
    CheckConstraint("residual_likelihood BETWEEN 1 AND 5",
                    name="ck_risk_register_residual_likelihood"),
    CheckConstraint("residual_impact BETWEEN 1 AND 5",
                    name="ck_risk_register_residual_impact"),
    CheckConstraint(
        "status IN ('identified','assessed','mitigating','monitoring','closed',"
        "'accepted')",
        name="ck_risk_register_status"),
)

Index("idx_risk_register_company", RISK_REGISTER.c.company_id)
Index("idx_risk_register_status", RISK_REGISTER.c.status)
Index("idx_risk_register_category", RISK_REGISTER.c.category)
Index("idx_risk_register_level", RISK_REGISTER.c.risk_level)

# ---------------------------------------------------------------------------
# 4. risk_assessment
# ---------------------------------------------------------------------------
RISK_ASSESSMENT = Table(
    "risk_assessment", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("risk_id", Text,
           ForeignKey("risk_register.id", ondelete="RESTRICT"), nullable=False),
    Column("assessment_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("assessor", Text),
    Column("likelihood", Integer, nullable=False),
    Column("impact", Integer, nullable=False),
    Column("score", Integer),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("likelihood BETWEEN 1 AND 5",
                    name="ck_risk_assessment_likelihood"),
    CheckConstraint("impact BETWEEN 1 AND 5", name="ck_risk_assessment_impact"),
)

Index("idx_risk_assessment_risk", RISK_ASSESSMENT.c.risk_id)
Index("idx_risk_assessment_company", RISK_ASSESSMENT.c.company_id)

# ---------------------------------------------------------------------------
# Controls domain
# 5. control_test
# ---------------------------------------------------------------------------
CONTROL_TEST = Table(
    "control_test", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("control_name", Text, nullable=False),
    Column("control_description", Text),
    Column("control_type", Text, nullable=False,
           server_default=text("'preventive'")),
    Column("frequency", Text, nullable=False, server_default=text("'quarterly'")),
    Column("test_date", Text, nullable=False, server_default=text("CURRENT_DATE")),
    Column("tester", Text),
    Column("test_procedure", Text),
    Column("test_result", Text, nullable=False,
           server_default=text("'not_tested'")),
    Column("evidence", Text),
    Column("deficiency_type", Text),
    Column("remediation_plan", Text),
    Column("next_test_date", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "control_type IN ('preventive','detective','corrective','compensating')",
        name="ck_control_test_control_type"),
    CheckConstraint(
        "frequency IN ('continuous','daily','weekly','monthly','quarterly',"
        "'semi_annual','annual')",
        name="ck_control_test_frequency"),
    CheckConstraint(
        "test_result IN ('not_tested','effective','ineffective',"
        "'partially_effective','not_applicable')",
        name="ck_control_test_test_result"),
    # The trailing NULL is in the shipped predicate and is transcribed as-is: it
    # makes the whole IN comparison NULL rather than false, so the constraint
    # passes for any value. Removing it would tighten what shipped.
    CheckConstraint(
        "deficiency_type IN ('significant','material_weakness',"
        "'control_deficiency',NULL)",
        name="ck_control_test_deficiency_type"),
)

Index("idx_control_test_company", CONTROL_TEST.c.company_id)
Index("idx_control_test_type", CONTROL_TEST.c.control_type)
Index("idx_control_test_result", CONTROL_TEST.c.test_result)

# ---------------------------------------------------------------------------
# 6. compliance_calendar
# ---------------------------------------------------------------------------
COMPLIANCE_CALENDAR = Table(
    "compliance_calendar", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("title", Text, nullable=False),
    Column("compliance_type", Text, nullable=False,
           server_default=text("'filing'")),
    Column("due_date", Text, nullable=False),
    Column("reminder_days", Integer, server_default=text("30")),
    Column("responsible", Text),
    Column("description", Text),
    Column("recurrence", Text),
    Column("status", Text, nullable=False, server_default=text("'upcoming'")),
    Column("completed_date", Text),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "compliance_type IN ('filing','certification','renewal','inspection',"
        "'report','training','other')",
        name="ck_compliance_calendar_compliance_type"),
    # Same NULL-in-list shape as control_test.deficiency_type; transcribed, not
    # repaired.
    CheckConstraint(
        "recurrence IN ('none','monthly','quarterly','semi_annual','annual',NULL)",
        name="ck_compliance_calendar_recurrence"),
    CheckConstraint(
        "status IN ('upcoming','in_progress','completed','overdue','waived')",
        name="ck_compliance_calendar_status"),
)

Index("idx_compliance_calendar_company", COMPLIANCE_CALENDAR.c.company_id)
Index("idx_compliance_calendar_status", COMPLIANCE_CALENDAR.c.status)
Index("idx_compliance_calendar_due", COMPLIANCE_CALENDAR.c.due_date)

# ---------------------------------------------------------------------------
# Policy domain
# 7. policy
# ---------------------------------------------------------------------------
POLICY = Table(
    "policy", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("title", Text, nullable=False),
    Column("policy_type", Text, nullable=False, server_default=text("'general'")),
    Column("version", Text, nullable=False, server_default=text("'1.0'")),
    Column("content", Text),
    Column("effective_date", Text),
    Column("review_date", Text),
    Column("owner", Text),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("requires_acknowledgment", Integer, nullable=False,
           server_default=text("0")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "policy_type IN ('general','hr','financial','it','safety','compliance',"
        "'operational','other')",
        name="ck_policy_policy_type"),
    CheckConstraint(
        "status IN ('draft','review','approved','published','retired')",
        name="ck_policy_status"),
    CheckConstraint("requires_acknowledgment IN (0,1)",
                    name="ck_policy_requires_acknowledgment"),
)

Index("idx_policy_company", POLICY.c.company_id)
Index("idx_policy_status", POLICY.c.status)
Index("idx_policy_type", POLICY.c.policy_type)

# ---------------------------------------------------------------------------
# 8. policy_acknowledgment
# ---------------------------------------------------------------------------
# `employee_id` carries no foreign key here while every `company_id` in this
# module does. That asymmetry is what shipped and is preserved deliberately.
POLICY_ACKNOWLEDGMENT = Table(
    "policy_acknowledgment", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("policy_id", Text,
           ForeignKey("policy.id", ondelete="RESTRICT"), nullable=False),
    Column("employee_name", Text, nullable=False),
    Column("employee_id", Text),
    Column("acknowledged_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("ip_address", Text),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_policy_ack_policy", POLICY_ACKNOWLEDGMENT.c.policy_id)
Index("idx_policy_ack_company", POLICY_ACKNOWLEDGMENT.c.company_id)
Index("idx_policy_ack_employee", POLICY_ACKNOWLEDGMENT.c.employee_id)



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

def create_compliance_tables(db_path=None):
    """Create compliance tables and indexes on whichever backend is configured.

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
    result = create_compliance_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
