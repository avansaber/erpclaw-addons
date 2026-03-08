"""ERPClaw E-Sign -- schema initialization.

Creates 2 tables for electronic signature management in the shared ERPClaw database.
Requires company table to exist (erpclaw-setup).
"""
import os
import sqlite3
import sys

DB_PATH = os.environ.get(
    "ERPCLAW_DB_PATH",
    os.path.expanduser("~/.openclaw/erpclaw/data.sqlite"),
)


def init_esign_schema(db_path: str = DB_PATH) -> dict:
    """Create e-sign tables and indexes."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")

    tables_created = 0
    indexes_created = 0

    # -------------------------------------------------------------------
    # 1. esign_signature_request -- signature request records
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS esign_signature_request (
            id                  TEXT PRIMARY KEY,
            naming_series       TEXT,
            document_type       TEXT NOT NULL,
            document_id         TEXT,
            document_name       TEXT NOT NULL,
            signers             TEXT NOT NULL,
            requested_by        TEXT NOT NULL,
            request_status      TEXT DEFAULT 'draft'
                                CHECK(request_status IN ('draft','sent','partially_signed','completed','declined','cancelled','voided','expired')),
            total_signers       INTEGER DEFAULT 0,
            signed_count        INTEGER DEFAULT 0,
            message             TEXT,
            expires_at          TEXT,
            completed_at        TEXT,
            company_id          TEXT NOT NULL,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_esign_req_company ON esign_signature_request(company_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_esign_req_status ON esign_signature_request(request_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_esign_req_requested_by ON esign_signature_request(requested_by)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_esign_req_doc_type ON esign_signature_request(document_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_esign_req_doc_id ON esign_signature_request(document_id)")
    indexes_created += 5

    # -------------------------------------------------------------------
    # 2. esign_signature_event -- audit trail of all signing events
    # -------------------------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS esign_signature_event (
            id                  TEXT PRIMARY KEY,
            request_id          TEXT NOT NULL REFERENCES esign_signature_request(id),
            event_type          TEXT NOT NULL
                                CHECK(event_type IN ('created','sent','viewed','signed','declined','cancelled','voided','reminded','expired')),
            signer_email        TEXT,
            signer_name         TEXT,
            ip_address          TEXT,
            user_agent          TEXT,
            signature_data      TEXT,
            notes               TEXT,
            company_id          TEXT NOT NULL,
            created_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    tables_created += 1
    conn.execute("CREATE INDEX IF NOT EXISTS idx_esign_event_request ON esign_signature_event(request_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_esign_event_type ON esign_signature_event(event_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_esign_event_signer ON esign_signature_event(signer_email)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_esign_event_company ON esign_signature_event(company_id)")
    indexes_created += 4

    conn.commit()
    conn.close()

    return {
        "database": db_path,
        "tables": tables_created,
        "indexes": indexes_created,
    }


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    result = init_esign_schema(path)
    print(f"ERPClaw E-Sign schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
