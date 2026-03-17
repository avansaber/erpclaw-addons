"""ERPClaw Documents -- schema initialization.

Creates 5 tables for document management in the shared ERPClaw database.
Requires company table to exist (erpclaw-setup).
"""
import os
import sqlite3
import sys

DB_PATH = os.environ.get(
    "ERPCLAW_DB_PATH",
    os.path.expanduser("~/.openclaw/erpclaw/data.sqlite"),
)


def init_documents_schema(db_path: str = DB_PATH) -> dict:
    """Create document management tables and indexes."""
    conn = sqlite3.connect(db_path)
    from erpclaw_lib.db import setup_pragmas
    setup_pragmas(conn)

    tables_created = 0
    indexes_created = 0

    # -------------------------------------------------------------------
    # 1. document -- core document records
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS document (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            title               TEXT NOT NULL,
            document_type       TEXT NOT NULL DEFAULT 'general'
                                CHECK(document_type IN ('general','contract','policy','report','invoice','receipt','certificate','specification','manual','other')),
            file_name           TEXT,
            file_path           TEXT,
            file_size           INTEGER,
            mime_type           TEXT,
            content             TEXT,
            current_version     TEXT NOT NULL DEFAULT '1',
            tags                TEXT,
            linked_entity_type  TEXT,
            linked_entity_id    TEXT,
            owner               TEXT,
            retention_date      TEXT,
            is_archived         INTEGER NOT NULL DEFAULT 0 CHECK(is_archived IN (0,1)),
            status              TEXT NOT NULL DEFAULT 'draft'
                                CHECK(status IN ('draft','review','approved','published','archived','on_hold')),
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_company ON document(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_status ON document(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_type ON document(document_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_owner ON document(owner)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_archived ON document(is_archived)")
    indexes_created += 5

    # -------------------------------------------------------------------
    # 2. document_version -- version history for documents
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS document_version (
            id                  TEXT PRIMARY KEY,
            document_id         TEXT NOT NULL REFERENCES document(id) ON DELETE CASCADE,
            version_number      TEXT NOT NULL,
            file_name           TEXT,
            file_path           TEXT,
            content             TEXT,
            change_notes        TEXT,
            created_by          TEXT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_version_doc ON document_version(document_id)")
    indexes_created += 1

    # -------------------------------------------------------------------
    # 3. document_tag -- tags associated with documents
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS document_tag (
            id                  TEXT PRIMARY KEY,
            document_id         TEXT NOT NULL REFERENCES document(id) ON DELETE CASCADE,
            tag                 TEXT NOT NULL,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_tag_doc ON document_tag(document_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_tag_tag ON document_tag(tag)")
    indexes_created += 2

    # -------------------------------------------------------------------
    # 4. document_link -- links between documents and other entities
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS document_link (
            id                  TEXT PRIMARY KEY,
            document_id         TEXT NOT NULL REFERENCES document(id) ON DELETE CASCADE,
            linked_entity_type  TEXT NOT NULL,
            linked_entity_id    TEXT NOT NULL,
            link_type           TEXT NOT NULL DEFAULT 'attachment'
                                CHECK(link_type IN ('attachment','reference','supporting','supersedes')),
            notes               TEXT,
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_link_doc ON document_link(document_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_link_entity ON document_link(linked_entity_type, linked_entity_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_link_company ON document_link(company_id)")
    indexes_created += 3

    # -------------------------------------------------------------------
    # 5. document_template -- reusable document templates
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS document_template (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            name                TEXT NOT NULL,
            template_type       TEXT NOT NULL DEFAULT 'general'
                                CHECK(template_type IN ('general','contract','invoice','letter','report','certificate','other')),
            content             TEXT NOT NULL,
            merge_fields        TEXT,
            description         TEXT,
            is_active           INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
            company_id          TEXT NOT NULL REFERENCES company(id) ON DELETE RESTRICT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_template_company ON document_template(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_template_type ON document_template(template_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_document_template_active ON document_template(is_active)")
    indexes_created += 3

    conn.commit()
    conn.close()

    return {
        "database": db_path,
        "tables": tables_created,
        "indexes": indexes_created,
    }


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    result = init_documents_schema(path)
    print(f"ERPClaw Documents schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
