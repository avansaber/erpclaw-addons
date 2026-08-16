#!/usr/bin/env python3
"""ERPClaw Loans schema extension -- adds lending tables to the shared database.

5 tables: loan_application, loan, loan_repayment_schedule, loan_repayment,
loan_write_off.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Conversion rules are the pilot's (`erpclaw-esign`); money
stays TEXT throughout, which matters more here than anywhere else in the addons.
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
DISPLAY_NAME = "ERPClaw Loans"

# What the shipped installer actually refused to run without. The conversion
# introduced a three-table constant this module never had and then never read it
# (Mac merge-QA, 2026-08-13); widening the probe while restoring it would be a
# behaviour change smuggled inside a regression fix, so the list is the
# pre-conversion contract — `company` — and it is now read.
REQUIRED_FOUNDATION = [
    "company",
]

METADATA = MetaData()

# Foundation tables this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)
reference_table("account", METADATA)

# ---------------------------------------------------------------------------
# 1. loan_application
# ---------------------------------------------------------------------------
LOAN_APPLICATION = Table(
    "loan_application", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("applicant_type", Text, nullable=False, server_default=text("'customer'")),
    Column("applicant_id", Text, nullable=False),
    Column("applicant_name", Text),
    Column("loan_type", Text, nullable=False, server_default=text("'term_loan'")),
    Column("requested_amount", Text, nullable=False, server_default=text("'0'")),
    Column("approved_amount", Text, nullable=False, server_default=text("'0'")),
    Column("interest_rate", Text, nullable=False, server_default=text("'0'")),
    Column("repayment_method", Text, nullable=False,
           server_default=text("'equal_installment'")),
    Column("repayment_periods", Integer, nullable=False, server_default=text("12")),
    Column("application_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("purpose", Text),
    Column("collateral_description", Text),
    Column("collateral_value", Text, nullable=False, server_default=text("'0'")),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("rejection_reason", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("applicant_type IN ('customer','employee','supplier')",
                    name="ck_loan_application_applicant_type"),
    CheckConstraint(
        "loan_type IN ('term_loan','demand_loan','staff_loan','credit_line')",
        name="ck_loan_application_loan_type"),
    CheckConstraint(
        "repayment_method IN ('equal_installment','equal_principal','bullet','custom')",
        name="ck_loan_application_repayment_method"),
    CheckConstraint(
        "status IN ('draft','applied','approved','rejected','cancelled')",
        name="ck_loan_application_status"),
)

Index("idx_loan_app_status", LOAN_APPLICATION.c.status)
Index("idx_loan_app_company", LOAN_APPLICATION.c.company_id)
Index("idx_loan_app_applicant",
      LOAN_APPLICATION.c.applicant_type, LOAN_APPLICATION.c.applicant_id)

# ---------------------------------------------------------------------------
# 2. loan
# ---------------------------------------------------------------------------
LOAN = Table(
    "loan", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("loan_application_id", Text,
           ForeignKey("loan_application.id", ondelete="RESTRICT")),
    Column("applicant_type", Text, nullable=False, server_default=text("'customer'")),
    Column("applicant_id", Text, nullable=False),
    Column("applicant_name", Text),
    Column("loan_type", Text, nullable=False, server_default=text("'term_loan'")),
    Column("loan_amount", Text, nullable=False, server_default=text("'0'")),
    Column("disbursed_amount", Text, nullable=False, server_default=text("'0'")),
    Column("total_interest", Text, nullable=False, server_default=text("'0'")),
    Column("total_repaid", Text, nullable=False, server_default=text("'0'")),
    Column("outstanding_amount", Text, nullable=False, server_default=text("'0'")),
    Column("interest_rate", Text, nullable=False, server_default=text("'0'")),
    Column("repayment_method", Text, nullable=False,
           server_default=text("'equal_installment'")),
    Column("repayment_periods", Integer, nullable=False, server_default=text("12")),
    Column("disbursement_date", Text),
    Column("maturity_date", Text),
    Column("loan_account_id", Text, ForeignKey("account.id", ondelete="RESTRICT")),
    Column("interest_income_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("disbursement_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("applicant_type IN ('customer','employee','supplier')",
                    name="ck_loan_applicant_type"),
    CheckConstraint(
        "loan_type IN ('term_loan','demand_loan','staff_loan','credit_line')",
        name="ck_loan_loan_type"),
    CheckConstraint(
        "repayment_method IN ('equal_installment','equal_principal','bullet','custom')",
        name="ck_loan_repayment_method"),
    CheckConstraint(
        "status IN ('draft','disbursed','partially_repaid','repaid',"
        "'written_off','closed')",
        name="ck_loan_status"),
)

Index("idx_loan_status", LOAN.c.status)
Index("idx_loan_company", LOAN.c.company_id)
Index("idx_loan_applicant", LOAN.c.applicant_type, LOAN.c.applicant_id)
Index("idx_loan_app_ref", LOAN.c.loan_application_id)

# ---------------------------------------------------------------------------
# 3. loan_repayment_schedule
# ---------------------------------------------------------------------------
LOAN_REPAYMENT_SCHEDULE = Table(
    "loan_repayment_schedule", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("loan_id", Text,
           ForeignKey("loan.id", ondelete="RESTRICT"), nullable=False),
    Column("installment_no", Integer, nullable=False),
    Column("due_date", Text, nullable=False),
    Column("principal_amount", Text, nullable=False, server_default=text("'0'")),
    Column("interest_amount", Text, nullable=False, server_default=text("'0'")),
    Column("total_amount", Text, nullable=False, server_default=text("'0'")),
    Column("paid_amount", Text, nullable=False, server_default=text("'0'")),
    Column("outstanding", Text, nullable=False, server_default=text("'0'")),
    Column("status", Text, nullable=False, server_default=text("'pending'")),
    Column("payment_date", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ('pending','partially_paid','paid','overdue','waived')",
        name="ck_loan_repayment_schedule_status"),
)

Index("idx_lrs_loan", LOAN_REPAYMENT_SCHEDULE.c.loan_id)
Index("idx_lrs_due_date", LOAN_REPAYMENT_SCHEDULE.c.due_date)
Index("idx_lrs_status", LOAN_REPAYMENT_SCHEDULE.c.status)

# ---------------------------------------------------------------------------
# 4. loan_repayment
# ---------------------------------------------------------------------------
LOAN_REPAYMENT = Table(
    "loan_repayment", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("loan_id", Text,
           ForeignKey("loan.id", ondelete="RESTRICT"), nullable=False),
    Column("repayment_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("principal_amount", Text, nullable=False, server_default=text("'0'")),
    Column("interest_amount", Text, nullable=False, server_default=text("'0'")),
    Column("penalty_amount", Text, nullable=False, server_default=text("'0'")),
    Column("total_amount", Text, nullable=False, server_default=text("'0'")),
    Column("payment_entry_id", Text),
    Column("payment_method", Text, nullable=False,
           server_default=text("'bank_transfer'")),
    Column("reference_number", Text),
    Column("remarks", Text),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "payment_method IN ('cash','bank_transfer','check','auto_debit')",
        name="ck_loan_repayment_payment_method"),
    CheckConstraint("status IN ('draft','submitted','cancelled')",
                    name="ck_loan_repayment_status"),
)

Index("idx_lr_loan", LOAN_REPAYMENT.c.loan_id)
Index("idx_lr_status", LOAN_REPAYMENT.c.status)
Index("idx_lr_company", LOAN_REPAYMENT.c.company_id)

# ---------------------------------------------------------------------------
# 5. loan_write_off
# ---------------------------------------------------------------------------
LOAN_WRITE_OFF = Table(
    "loan_write_off", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("loan_id", Text,
           ForeignKey("loan.id", ondelete="RESTRICT"), nullable=False),
    Column("write_off_date", Text, nullable=False,
           server_default=text("CURRENT_DATE")),
    Column("write_off_amount", Text, nullable=False, server_default=text("'0'")),
    Column("outstanding_at_write_off", Text, nullable=False,
           server_default=text("'0'")),
    Column("reason", Text),
    Column("bad_debt_account_id", Text,
           ForeignKey("account.id", ondelete="RESTRICT")),
    Column("journal_entry_id", Text),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("status IN ('draft','submitted','cancelled')",
                    name="ck_loan_write_off_status"),
)

Index("idx_lwo_loan", LOAN_WRITE_OFF.c.loan_id)
Index("idx_lwo_company", LOAN_WRITE_OFF.c.company_id)


def _require_foundation(db_path):
    """The pre-conversion installer's foundation probe, asked through the seam.

    The probed table and the wording are this module's own, unchanged; only the
    mechanism changed. The original read SQLite's own catalog table directly,
    so the guard that exists to produce a friendly error was itself
    SQLite-only — on PostgreSQL it would have raised before it could explain
    anything, and ``seam.table_exists`` answers on both backends (ADR-0034
    bulk-39). This note names that catalog table in prose rather than as the
    identifier, because the seam-bypass ratchet counts string literals and an
    installers bucket that is only allowed to fall should not rise for a
    docstring.
    """
    from erpclaw_lib import seam

    missing = [t for t in REQUIRED_FOUNDATION if not seam.table_exists(t, db_path)]
    if missing:
        print("ERROR: Foundation tables not found. Run erpclaw-setup first.")
        sys.exit(1)


def create_loans_tables(db_path=None):
    """Create loan tables and indexes on whichever backend is configured.

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
    result = create_loans_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
