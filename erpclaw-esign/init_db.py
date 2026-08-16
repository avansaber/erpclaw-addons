"""ERPClaw E-Sign -- schema initialization.

Creates 2 tables for electronic signature management in the shared ERPClaw database.
Requires company table to exist (erpclaw-setup).

ADR-0034 phase 2 pilot. The schema is declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, instead of a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect``. That old shape could not
run on PostgreSQL at all -- it failed with "unable to open database file" when
handed a Postgres URL -- which is what kept every addon and vertical SQLite-only
(PG-8).

Declaration notes for the 39 installers that follow this one:

* Import the vocabulary from ``erpclaw_lib.seam``, never from SQLAlchemy. The
  seam is the only sanctioned import site and the L0 bypass gate is written
  against that fact.
* Money and IDs stay TEXT on every backend (ADR-0034 dec. 1). ``Integer`` is for
  counts only.
* ``primary_key=True, nullable=True`` reproduces SQLite's ``id TEXT PRIMARY KEY``
  exactly. SQLAlchemy would otherwise add ``NOT NULL``, which is stricter than
  what shipped and would show up as a structural difference against the existing
  installed schema.
"""
import importlib.util
import os
import sys

# Bootstrap the shared lib, but only when it is not already reachable. An
# unconditional insert at position 0 overrides a caller that deliberately put a
# different tree first — which is how this file's first draft imported the
# DEPLOYED lib instead of the one under test and failed to find `seam` at all.
# module_manager carried the identical defect (ADR-0034 phase 2 step 1). The 39
# installers that follow this pilot inherit this shape, so it is guarded here
# rather than repeated 39 times and fixed 39 times.
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(
        os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))

from erpclaw_lib.seam import (  # noqa: E402
    CheckConstraint, Column, ForeignKey, Index, Integer, MetaData, Table, Text,
    provision, text,
)

DB_PATH = os.environ.get(
    "ERPCLAW_DB_PATH",
    os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite"),
)

METADATA = MetaData()

# ---------------------------------------------------------------------------
# 1. esign_signature_request -- signature request records
# ---------------------------------------------------------------------------
SIGNATURE_REQUEST = Table(
    "esign_signature_request", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("document_type", Text, nullable=False),
    Column("document_id", Text),
    Column("document_name", Text, nullable=False),
    Column("signers", Text, nullable=False),
    Column("requested_by", Text, nullable=False),
    Column("request_status", Text, server_default=text("'draft'")),
    Column("total_signers", Integer, server_default=text("0")),
    Column("signed_count", Integer, server_default=text("0")),
    Column("message", Text),
    Column("expires_at", Text),
    Column("completed_at", Text),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "request_status IN ('draft','sent','partially_signed','completed',"
        "'declined','cancelled','voided','expired')",
        name="ck_esign_request_status"),
)

Index("idx_esign_req_company", SIGNATURE_REQUEST.c.company_id)
Index("idx_esign_req_status", SIGNATURE_REQUEST.c.request_status)
Index("idx_esign_req_requested_by", SIGNATURE_REQUEST.c.requested_by)
Index("idx_esign_req_doc_type", SIGNATURE_REQUEST.c.document_type)
Index("idx_esign_req_doc_id", SIGNATURE_REQUEST.c.document_id)

# ---------------------------------------------------------------------------
# 2. esign_signature_event -- audit trail of all signing events
# ---------------------------------------------------------------------------
SIGNATURE_EVENT = Table(
    "esign_signature_event", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("request_id", Text, ForeignKey("esign_signature_request.id"),
           nullable=False),
    Column("event_type", Text, nullable=False),
    Column("signer_email", Text),
    Column("signer_name", Text),
    Column("ip_address", Text),
    Column("user_agent", Text),
    Column("signature_data", Text),
    Column("notes", Text),
    Column("company_id", Text, nullable=False),
    Column("created_at", Text, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "event_type IN ('created','sent','viewed','signed','declined',"
        "'cancelled','voided','reminded','expired')",
        name="ck_esign_event_type"),
)

Index("idx_esign_event_request", SIGNATURE_EVENT.c.request_id)
Index("idx_esign_event_type", SIGNATURE_EVENT.c.event_type)
Index("idx_esign_event_signer", SIGNATURE_EVENT.c.signer_email)
Index("idx_esign_event_company", SIGNATURE_EVENT.c.company_id)


def init_esign_schema(db_path: str = DB_PATH) -> dict:
    """Create e-sign tables and indexes on whichever backend is configured.

    Same contract as before the ADR-0034 conversion: idempotent (a re-run creates
    nothing), and the returned counts are what was ACTUALLY created rather than
    what was declared.
    """
    result = provision(METADATA, db_path)
    return {
        "database": db_path,
        "tables": result["tables"],
        "indexes": result["indexes"],
    }


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    result = init_esign_schema(path)
    print(f"ERPClaw E-Sign schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
