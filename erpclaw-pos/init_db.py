#!/usr/bin/env python3
"""ERPClaw POS schema extension -- adds point-of-sale tables to the shared database.

5 tables: pos_profile, pos_session, pos_transaction, pos_transaction_item,
pos_payment.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Conversion rules are the pilot's (`erpclaw-esign`); every
money column stays TEXT.
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
DISPLAY_NAME = "ERPClaw POS"

# What the shipped installer actually refused to run without. The conversion
# introduced a three-table constant this module never had and then never read it
# (Mac merge-QA, 2026-08-13); widening the probe while restoring it would be a
# behaviour change smuggled inside a regression fix, so the list is the
# pre-conversion contract — `company` — and it is now read.
REQUIRED_FOUNDATION = [
    "company",
]

METADATA = MetaData()

# Foundation table this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)

# ---------------------------------------------------------------------------
# 1. pos_profile
# ---------------------------------------------------------------------------
POS_PROFILE = Table(
    "pos_profile", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("warehouse_id", Text),
    Column("price_list_id", Text),
    Column("default_payment_method", Text, nullable=False,
           server_default=text("'cash'")),
    Column("allow_discount", Integer, nullable=False, server_default=text("1")),
    Column("max_discount_pct", Text, nullable=False, server_default=text("'100'")),
    Column("auto_print_receipt", Integer, nullable=False, server_default=text("0")),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "default_payment_method IN ('cash','card','mobile','split')",
        name="ck_pos_profile_default_payment_method"),
    CheckConstraint("allow_discount IN (0,1)",
                    name="ck_pos_profile_allow_discount"),
    CheckConstraint("auto_print_receipt IN (0,1)",
                    name="ck_pos_profile_auto_print_receipt"),
    CheckConstraint("is_active IN (0,1)", name="ck_pos_profile_is_active"),
)

Index("idx_pos_profile_company", POS_PROFILE.c.company_id)

# ---------------------------------------------------------------------------
# 2. pos_session
# ---------------------------------------------------------------------------
POS_SESSION = Table(
    "pos_session", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("pos_profile_id", Text,
           ForeignKey("pos_profile.id", ondelete="RESTRICT"), nullable=False),
    Column("cashier_name", Text),
    Column("opening_amount", Text, nullable=False, server_default=text("'0'")),
    Column("closing_amount", Text),
    Column("expected_amount", Text),
    Column("difference", Text),
    Column("total_sales", Text, nullable=False, server_default=text("'0'")),
    Column("total_returns", Text, nullable=False, server_default=text("'0'")),
    Column("transaction_count", Integer, nullable=False, server_default=text("0")),
    Column("opened_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("closed_at", Text),
    Column("status", Text, nullable=False, server_default=text("'open'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("status IN ('open','closing','closed','reconciled')",
                    name="ck_pos_session_status"),
)

Index("idx_pos_session_status", POS_SESSION.c.status)
Index("idx_pos_session_company", POS_SESSION.c.company_id)
Index("idx_pos_session_profile", POS_SESSION.c.pos_profile_id)

# ---------------------------------------------------------------------------
# 3. pos_transaction
# ---------------------------------------------------------------------------
POS_TRANSACTION = Table(
    "pos_transaction", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("pos_session_id", Text,
           ForeignKey("pos_session.id", ondelete="RESTRICT"), nullable=False),
    Column("customer_id", Text),
    Column("customer_name", Text),
    Column("subtotal", Text, nullable=False, server_default=text("'0'")),
    Column("discount_amount", Text, nullable=False, server_default=text("'0'")),
    Column("discount_pct", Text, nullable=False, server_default=text("'0'")),
    Column("tax_amount", Text, nullable=False, server_default=text("'0'")),
    Column("grand_total", Text, nullable=False, server_default=text("'0'")),
    Column("paid_amount", Text, nullable=False, server_default=text("'0'")),
    Column("change_amount", Text, nullable=False, server_default=text("'0'")),
    Column("sales_invoice_id", Text),
    Column("receipt_number", Text),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "status IN ('draft','held','submitted','voided','returned')",
        name="ck_pos_transaction_status"),
)

Index("idx_pos_txn_session", POS_TRANSACTION.c.pos_session_id)
Index("idx_pos_txn_status", POS_TRANSACTION.c.status)
Index("idx_pos_txn_company", POS_TRANSACTION.c.company_id)

# ---------------------------------------------------------------------------
# 4. pos_transaction_item
# ---------------------------------------------------------------------------
POS_TRANSACTION_ITEM = Table(
    "pos_transaction_item", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("pos_transaction_id", Text,
           ForeignKey("pos_transaction.id", ondelete="CASCADE"), nullable=False),
    Column("item_id", Text, nullable=False),
    Column("item_name", Text, nullable=False),
    Column("item_code", Text),
    Column("barcode", Text),
    Column("qty", Text, nullable=False, server_default=text("'1'")),
    Column("rate", Text, nullable=False, server_default=text("'0'")),
    Column("discount_pct", Text, nullable=False, server_default=text("'0'")),
    Column("discount_amount", Text, nullable=False, server_default=text("'0'")),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("uom", Text, nullable=False, server_default=text("'Nos'")),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_pos_txn_item_txn", POS_TRANSACTION_ITEM.c.pos_transaction_id)
Index("idx_pos_txn_item_item", POS_TRANSACTION_ITEM.c.item_id)

# ---------------------------------------------------------------------------
# 5. pos_payment
# ---------------------------------------------------------------------------
POS_PAYMENT = Table(
    "pos_payment", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("pos_transaction_id", Text,
           ForeignKey("pos_transaction.id", ondelete="CASCADE"), nullable=False),
    Column("payment_method", Text, nullable=False, server_default=text("'cash'")),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("reference", Text),
    Column("payment_entry_id", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "payment_method IN ('cash','card','mobile','check','gift_card','other')",
        name="ck_pos_payment_method"),
)

Index("idx_pos_payment_txn", POS_PAYMENT.c.pos_transaction_id)


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


def create_pos_tables(db_path=None):
    """Create POS tables and indexes on whichever backend is configured.

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
    result = create_pos_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
