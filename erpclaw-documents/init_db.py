"""ERPClaw Documents -- schema initialization.

Creates 5 document-management tables (document, document_version, document_tag,
document_link, document_template) in the shared ERPClaw database.

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Conversion rules are the pilot's (`erpclaw-esign`).
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

DB_PATH = os.environ.get(
    "ERPCLAW_DB_PATH",
    os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite"),
)

METADATA = MetaData()

# Foundation table this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)

# ---------------------------------------------------------------------------
# 1. document
# ---------------------------------------------------------------------------
DOCUMENT = Table(
    "document", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("title", Text, nullable=False),
    Column("document_type", Text, nullable=False, server_default=text("'general'")),
    Column("file_name", Text),
    Column("file_path", Text),
    Column("pdf_path", Text),
    Column("file_size", Integer),
    Column("mime_type", Text),
    Column("content", Text),
    Column("current_version", Text, nullable=False, server_default=text("'1'")),
    Column("tags", Text),
    Column("linked_entity_type", Text),
    Column("linked_entity_id", Text),
    Column("owner", Text),
    Column("retention_date", Text),
    Column("is_archived", Integer, nullable=False, server_default=text("0")),
    Column("status", Text, nullable=False, server_default=text("'draft'")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "document_type IN ('general','contract','policy','report','invoice',"
        "'receipt','certificate','specification','manual','other')",
        name="ck_document_type"),
    CheckConstraint("is_archived IN (0,1)", name="ck_document_is_archived"),
    CheckConstraint(
        "status IN ('draft','review','approved','published','archived','on_hold')",
        name="ck_document_status"),
)

Index("idx_document_company", DOCUMENT.c.company_id)
Index("idx_document_status", DOCUMENT.c.status)
Index("idx_document_type", DOCUMENT.c.document_type)
Index("idx_document_owner", DOCUMENT.c.owner)
Index("idx_document_archived", DOCUMENT.c.is_archived)

# ---------------------------------------------------------------------------
# 2. document_version
# ---------------------------------------------------------------------------
DOCUMENT_VERSION = Table(
    "document_version", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("document_id", Text,
           ForeignKey("document.id", ondelete="CASCADE"), nullable=False),
    Column("version_number", Text, nullable=False),
    Column("file_name", Text),
    Column("file_path", Text),
    Column("content", Text),
    Column("change_notes", Text),
    Column("created_by", Text),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_document_version_doc", DOCUMENT_VERSION.c.document_id)

# ---------------------------------------------------------------------------
# 3. document_tag
# ---------------------------------------------------------------------------
DOCUMENT_TAG = Table(
    "document_tag", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("document_id", Text,
           ForeignKey("document.id", ondelete="CASCADE"), nullable=False),
    Column("tag", Text, nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
)

Index("idx_document_tag_doc", DOCUMENT_TAG.c.document_id)
Index("idx_document_tag_tag", DOCUMENT_TAG.c.tag)

# ---------------------------------------------------------------------------
# 4. document_link
# ---------------------------------------------------------------------------
DOCUMENT_LINK = Table(
    "document_link", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("document_id", Text,
           ForeignKey("document.id", ondelete="CASCADE"), nullable=False),
    Column("linked_entity_type", Text, nullable=False),
    Column("linked_entity_id", Text, nullable=False),
    Column("link_type", Text, nullable=False, server_default=text("'attachment'")),
    Column("notes", Text),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "link_type IN ('attachment','reference','supporting','supersedes')",
        name="ck_document_link_type"),
)

Index("idx_document_link_doc", DOCUMENT_LINK.c.document_id)
Index("idx_document_link_entity",
      DOCUMENT_LINK.c.linked_entity_type, DOCUMENT_LINK.c.linked_entity_id)
Index("idx_document_link_company", DOCUMENT_LINK.c.company_id)

# ---------------------------------------------------------------------------
# 5. document_template
# ---------------------------------------------------------------------------
DOCUMENT_TEMPLATE = Table(
    "document_template", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("template_type", Text, nullable=False, server_default=text("'general'")),
    Column("content", Text, nullable=False),
    Column("format", Text, nullable=False, server_default=text("'text'")),
    Column("engine", Text, nullable=False, server_default=text("'legacy_replace'")),
    Column("merge_fields", Text),
    Column("description", Text),
    Column("is_active", Integer, nullable=False, server_default=text("1")),
    Column("company_id", Text,
           ForeignKey("company.id", ondelete="RESTRICT"), nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "template_type IN ('general','contract','invoice','letter','report',"
        "'certificate','other')",
        name="ck_document_template_type"),
    CheckConstraint("format IN ('text','markdown','html')",
                    name="ck_document_template_format"),
    CheckConstraint("engine IN ('legacy_replace','jinja2')",
                    name="ck_document_template_engine"),
    CheckConstraint("is_active IN (0,1)", name="ck_document_template_is_active"),
)

Index("idx_document_template_company", DOCUMENT_TEMPLATE.c.company_id)
Index("idx_document_template_type", DOCUMENT_TEMPLATE.c.template_type)
Index("idx_document_template_active", DOCUMENT_TEMPLATE.c.is_active)


def init_documents_schema(db_path: str = DB_PATH) -> dict:
    """Create document tables and indexes on whichever backend is configured.

    Same contract as before the ADR-0034 conversion: idempotent, and the returned
    counts are what was ACTUALLY created rather than what was declared.
    """
    result = provision(METADATA, db_path)
    return {
        "database": db_path,
        "tables": result["tables"],
        "indexes": result["indexes"],
    }


if __name__ == "__main__":
    # The positional db path the shipped installer accepted. The conversion
    # dropped it (Mac merge-QA sweep, 2026-08-13 — the same defect as
    # erpclaw-ops, in the same conversion family), so
    # `init_db.py /some/where.sqlite` provisioned the default home and said
    # nothing — a silently ignored destination, which is worse than an error.
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    result = init_documents_schema(path)
    print(f"ERPClaw Documents schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
