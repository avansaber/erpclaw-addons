"""Shared helper functions for ERPClaw Compliance L1 unit tests.

Provides:
  - DB bootstrap via init_schema.init_db() + create_compliance_tables()
  - load_db_query() for explicit module loading (avoids sys.path collisions)
  - call_action() / ns() / is_error() / is_ok()
  - Seed functions for company, naming_series, and domain entities
  - build_env() for a complete compliance test environment
"""
import argparse
import importlib.util
import io
import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime, timezone, date
from decimal import Decimal
from unittest.mock import patch

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_DIR = os.path.dirname(TESTS_DIR)               # scripts/
ROOT_DIR = os.path.dirname(MODULE_DIR)                 # erpclaw-compliance/
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
    """Load erpclaw-compliance db_query.py explicitly to avoid sys.path collisions."""
    db_query_path = os.path.join(MODULE_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("db_query_compliance", db_query_path)
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
    """Create foundation tables + compliance extension tables."""
    # Step 1: Foundation schema
    spec = importlib.util.spec_from_file_location("init_schema", INIT_SCHEMA_PATH)
    schema_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_mod)
    schema_mod.init_db(db_path)

    # Step 2: Compliance extension tables
    spec2 = importlib.util.spec_from_file_location("compliance_init_db", VERTICAL_INIT_PATH)
    comp_mod = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(comp_mod)
    comp_mod.create_compliance_tables(db_path)


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
        "name": None,
        # Audit domain
        "audit_plan_id": None,
        "audit_type": None,
        "scope": None,
        "lead_auditor": None,
        "planned_start": None,
        "planned_end": None,
        "title": None,
        "finding_type": None,
        "area": None,
        "root_cause": None,
        "recommendation": None,
        "remediation_due": None,
        "remediation_status": None,
        "assigned_to": None,
        # Risk domain
        "risk_id": None,
        "category": None,
        "likelihood": None,
        "impact": None,
        "owner": None,
        "mitigation_plan": None,
        "residual_likelihood": None,
        "residual_impact": None,
        "review_date": None,
        "risk_level": None,
        "assessor": None,
        # Controls domain
        "control_test_id": None,
        "control_name": None,
        "control_description": None,
        "control_type": None,
        "frequency": None,
        "test_date": None,
        "tester": None,
        "test_procedure": None,
        "test_result": None,
        "evidence": None,
        "deficiency_type": None,
        "next_test_date": None,
        # Calendar domain
        "calendar_item_id": None,
        "compliance_type": None,
        "due_date": None,
        "reminder_days": None,
        "responsible": None,
        "recurrence": None,
        # Policy domain
        "policy_id": None,
        "policy_type": None,
        "version": None,
        "content": None,
        "effective_date": None,
        "requires_acknowledgment": None,
        "employee_name": None,
        "employee_id": None,
        "ip_address": None,
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


def _today() -> str:
    return date.today().isoformat()


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def seed_company(conn, name="Compliance Test Co", abbr="CTC") -> str:
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
    """Seed naming series for compliance entity types."""
    series = [
        ("audit_plan", "AUD-", 0),
        ("risk_register", "RISK-", 0),
        ("control_test", "CTRL-", 0),
        ("compliance_calendar", "CCAL-", 0),
        ("policy", "POL-", 0),
    ]
    for entity_type, prefix, current in series:
        conn.execute(
            """INSERT OR IGNORE INTO naming_series
               (id, entity_type, prefix, current_value, company_id)
               VALUES (?, ?, ?, ?, ?)""",
            (_uuid(), entity_type, prefix, current, company_id)
        )
    conn.commit()


def seed_employee(conn, company_id: str, first_name="John",
                  last_name="Doe") -> str:
    """Insert a core employee and return its ID."""
    eid = _uuid()
    now = _now()
    conn.execute(
        """INSERT INTO employee (id, first_name, last_name, full_name,
           company_id, status, date_of_joining, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, 'active', '2026-01-15', ?, ?)""",
        (eid, first_name, last_name, f"{first_name} {last_name}",
         company_id, now, now)
    )
    conn.commit()
    return eid


def seed_audit_plan(conn, company_id: str, name="Test Audit",
                    audit_type="internal", status="draft") -> str:
    """Insert an audit plan and return its ID."""
    from erpclaw_lib.naming import get_next_name
    plan_id = _uuid()
    naming = get_next_name(conn, "audit_plan", company_id=company_id)
    now = _now()
    conn.execute("""
        INSERT INTO audit_plan (
            id, naming_series, name, audit_type, scope, lead_auditor,
            planned_start, planned_end, status, notes,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        plan_id, naming, name, audit_type,
        "Test scope", "Lead Auditor",
        "2026-04-01", "2026-04-30", status,
        None, company_id, now, now,
    ))
    conn.commit()
    return plan_id


def seed_risk(conn, company_id: str, name="Test Risk",
              likelihood=3, impact=3) -> str:
    """Insert a risk register entry and return its ID."""
    from erpclaw_lib.naming import get_next_name
    risk_id = _uuid()
    naming = get_next_name(conn, "risk_register", company_id=company_id)
    now = _now()
    score = likelihood * impact
    # Determine level
    if score <= 4:
        level = "low"
    elif score <= 9:
        level = "medium"
    elif score <= 15:
        level = "high"
    else:
        level = "critical"
    conn.execute("""
        INSERT INTO risk_register (
            id, naming_series, name, category, description,
            likelihood, impact, risk_score, risk_level,
            owner, status, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        risk_id, naming, name, "operational", "Test risk description",
        likelihood, impact, score, level,
        "Risk Owner", "identified",
        company_id, now, now,
    ))
    conn.commit()
    return risk_id


def seed_policy(conn, company_id: str, title="Test Policy",
                policy_type="general", status="draft") -> str:
    """Insert a policy and return its ID."""
    from erpclaw_lib.naming import get_next_name
    policy_id = _uuid()
    naming = get_next_name(conn, "policy", company_id=company_id)
    now = _now()
    conn.execute("""
        INSERT INTO policy (
            id, naming_series, title, policy_type, version,
            content, status, requires_acknowledgment,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        policy_id, naming, title, policy_type, "1.0",
        "This is a test policy.", status, 0,
        company_id, now, now,
    ))
    conn.commit()
    return policy_id


def build_env(conn) -> dict:
    """Create a complete compliance test environment.

    Returns dict with all IDs needed for compliance domain tests.
    """
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    audit_plan_id = seed_audit_plan(conn, cid)
    risk_id = seed_risk(conn, cid)
    policy_id = seed_policy(conn, cid)
    emp_id = seed_employee(conn, cid, "Jane", "Auditor")

    return {
        "company_id": cid,
        "audit_plan_id": audit_plan_id,
        "risk_id": risk_id,
        "policy_id": policy_id,
        "employee_id": emp_id,
    }
