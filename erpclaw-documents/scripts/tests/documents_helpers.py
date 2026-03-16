"""Shared helper functions for ERPClaw Documents L1 unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + init_documents_schema()
  - load_db_query() for explicit module loading (avoids sys.path collisions)
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming_series
  - build_env() for a complete documents test environment
"""
import argparse
import importlib.util
import io
import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_DIR = os.path.dirname(TESTS_DIR)               # scripts/
ROOT_DIR = os.path.dirname(MODULE_DIR)                 # erpclaw-documents/
ADDONS_DIR = os.path.dirname(ROOT_DIR)                 # erpclaw-addons/
SRC_DIR = os.path.dirname(ADDONS_DIR)                  # src/

# Foundation schema init
SETUP_DIR = os.path.join(SRC_DIR, "erpclaw", "scripts", "erpclaw-setup")
INIT_SCHEMA_PATH = os.path.join(SETUP_DIR, "init_schema.py")

# Vertical schema init
VERTICAL_INIT_PATH = os.path.join(ROOT_DIR, "init_db.py")

# Make erpclaw_lib importable
ERPCLAW_LIB = os.path.expanduser("~/.openclaw/erpclaw/lib")
if ERPCLAW_LIB not in sys.path:
    sys.path.insert(0, ERPCLAW_LIB)

# Make scripts dir importable so domain modules resolve
if MODULE_DIR not in sys.path:
    sys.path.insert(0, MODULE_DIR)


def load_db_query():
    """Load erpclaw-documents db_query.py explicitly to avoid sys.path collisions."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_documents", db_query_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # Attach action functions as underscore-named attributes for convenience
    for action_name, fn in mod.ACTIONS.items():
        setattr(mod, action_name.replace("-", "_"), fn)
    return mod


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def init_all_tables(db_path: str):
    """Create foundation tables + documents extension tables."""
    # Step 1: Foundation schema
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    schema_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_mod)
    schema_mod.init_db(db_path)

    # Step 2: Documents extension tables
    spec2 = importlib.util.spec_from_file_location("documents_init_db", VERTICAL_INIT_PATH)
    doc_mod = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(doc_mod)
    doc_mod.init_documents_schema(db_path)


class _ConnWrapper:
    """Thin wrapper so conn.company_id works (some actions set it)."""
    def __init__(self, real_conn):
        self._conn = real_conn
        self.company_id = None

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def execute(self, *a, **kw):
        return self._conn.execute(*a, **kw)

    def executemany(self, *a, **kw):
        return self._conn.executemany(*a, **kw)

    def executescript(self, *a, **kw):
        return self._conn.executescript(*a, **kw)

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return self._conn.close()

    @property
    def row_factory(self):
        return self._conn.row_factory

    @row_factory.setter
    def row_factory(self, value):
        self._conn.row_factory = value


class _DecimalSum:
    """Custom SQLite aggregate: SUM using Python Decimal for precision."""
    def __init__(self):
        self.total = Decimal("0")

    def step(self, value):
        if value is not None:
            self.total += Decimal(str(value))

    def finalize(self):
        return str(self.total)


def get_conn(db_path: str):
    """Return a wrapped sqlite3.Connection with FK enabled and Row factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.create_aggregate("decimal_sum", 1, _DecimalSum)
    return _ConnWrapper(conn)


# ---------------------------------------------------------------------------
# Action invocation helpers
# ---------------------------------------------------------------------------

def call_action(fn, conn, args) -> dict:
    """Invoke a domain function, capture stdout JSON, return parsed dict."""
    buf = io.StringIO()

    def _fake_exit(code=0):
        raise SystemExit(code)

    try:
        with patch("sys.stdout", buf), patch("sys.exit", side_effect=_fake_exit):
            fn(conn, args)
    except SystemExit:
        pass

    output = buf.getvalue().strip()
    if not output:
        return {"status": "error", "message": "no output captured"}
    return json.loads(output)


def ns(**kwargs) -> argparse.Namespace:
    """Build an argparse.Namespace from keyword args (mimics CLI flags)."""
    defaults = {
        "limit": 50,
        "offset": 0,
        "company_id": None,
        "search": None,
        "notes": None,
        "status": None,
        "description": None,
        # Document fields
        "document_id": None,
        "title": None,
        "document_type": None,
        "file_name": None,
        "file_path": None,
        "file_size": None,
        "mime_type": None,
        "content": None,
        "tags": None,
        "tag": None,
        "linked_entity_type": None,
        "linked_entity_id": None,
        "owner": None,
        "retention_date": None,
        # Versioning
        "version_number": None,
        "change_notes": None,
        "created_by": None,
        # Linking
        "link_id": None,
        "link_type": None,
        # Templates
        "template_id": None,
        "name": None,
        "template_type": None,
        "merge_fields": None,
        "is_active": None,
        "merge_data": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def is_error(result: dict) -> bool:
    return result.get("status") == "error"


def is_ok(result: dict) -> bool:
    return result.get("status") == "ok"


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def seed_company(conn, name="Documents Test Co", abbr="DTC") -> str:
    """Insert a test company and return its ID."""
    cid = _uuid()
    conn.execute(
        """INSERT INTO company (id, name, abbr, default_currency, country,
           fiscal_year_start_month)
           VALUES (?, ?, ?, 'USD', 'United States', 1)""",
        (cid, f"{name} {cid[:6]}", f"{abbr}{cid[:4]}")
    )
    conn.commit()
    return cid


def seed_naming_series(conn, company_id: str):
    """Seed naming series for document entity types."""
    series = [
        ("document", "DOC-", 0),
        ("document_template", "DTPL-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_document(conn, company_id: str, title="Test Document",
                  doc_type="general") -> str:
    """Insert a document and return its ID."""
    from erpclaw_lib.naming import get_next_name
    doc_id = _uuid()
    naming = get_next_name(conn, "document", company_id=company_id)
    conn.execute(
        """INSERT INTO document
           (id, naming_series, title, document_type, content,
            current_version, is_archived, status, company_id)
           VALUES (?,?,?,?,?,?,0,?,?)""",
        (doc_id, naming, title, doc_type, "Sample content",
         "1", "draft", company_id)
    )
    # Create initial version
    ver_id = _uuid()
    conn.execute(
        """INSERT INTO document_version
           (id, document_id, version_number, content, change_notes)
           VALUES (?,?,?,?,?)""",
        (ver_id, doc_id, "1", "Sample content", "Initial version")
    )
    conn.commit()
    return doc_id


def seed_template(conn, company_id: str, name="Test Template",
                  template_type="general") -> str:
    """Insert a document template and return its ID."""
    from erpclaw_lib.naming import get_next_name
    tpl_id = _uuid()
    naming = get_next_name(conn, "document_template", company_id=company_id)
    conn.execute(
        """INSERT INTO document_template
           (id, naming_series, name, template_type, content,
            merge_fields, is_active, company_id)
           VALUES (?,?,?,?,?,?,1,?)""",
        (tpl_id, naming, name, template_type,
         "Hello {{name}}, your order {{order_id}} is ready.",
         "name,order_id", company_id)
    )
    conn.commit()
    return tpl_id


def build_env(conn) -> dict:
    """Create a complete documents test environment.

    Returns dict with all IDs needed for document domain tests.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    doc_id = seed_document(conn, cid)
    tpl_id = seed_template(conn, cid)

    return {
        "company_id": cid,
        "document_id": doc_id,
        "template_id": tpl_id,
    }
