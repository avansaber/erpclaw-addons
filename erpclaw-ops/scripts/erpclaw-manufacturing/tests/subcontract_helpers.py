"""Shared scaffolding for Wave 2 S5 subcontracting-lifecycle tests.

The subcontracting lifecycle drives real cross-skill subprocess calls:
  - transfer-materials-to-subcontractor  -> erpclaw add/submit-stock-entry
  - receive-subcontracted-items          -> erpclaw create-purchase-invoice
  - cancel-subcontract-transfer          -> erpclaw cancel-stock-entry

Those resolve a *deployed* skill layout (erpclaw_lib.dependencies.resolve_skill_script),
so the tests stand up a throwaway OPENCLAW_SKILLS_DIR with two symlinked skills:

  <skills>/erpclaw/scripts                  -> source/erpclaw/scripts        (foundation router)
  <skills>/erpclaw-manufacturing/scripts    -> .../erpclaw-manufacturing     (this module)

The foundation router (source/erpclaw/scripts/db_query.py) os.execvp's its sibling
domain scripts relative to its own dir, so the whole scripts/ tree is symlinked (a
per-file symlink would orphan the siblings). With that in place add/submit-stock-entry,
create-purchase-invoice, and cancel-stock-entry all run as real subprocesses against
the test DB — the same path production uses.

Money assertions are exact Decimal-as-text; the DB is the constitution/manufacturing
fresh full-schema SQLite.
"""
import argparse
import io
import json
import os
import sys
import uuid
from contextlib import contextmanager
from decimal import Decimal

_HERE = os.path.dirname(os.path.abspath(__file__))
_MFG_DIR = os.path.dirname(_HERE)                                  # erpclaw-manufacturing/
_OPS_SCRIPTS = os.path.dirname(_MFG_DIR)                           # erpclaw-ops/scripts/
_OPS = os.path.dirname(_OPS_SCRIPTS)                               # erpclaw-ops/
_ADDONS = os.path.dirname(_OPS)                                    # erpclaw-addons/
_SRC = os.path.dirname(_ADDONS)                                    # source/
_FOUND_SCRIPTS = os.path.join(_SRC, "erpclaw", "scripts")          # foundation router dir
_FOUND_ROUTER = os.path.join(_FOUND_SCRIPTS, "db_query.py")

# Every flag any subcontracting action reads, defaulted so a Namespace built from a
# few kwargs never AttributeErrors.
_ARG_DEFAULTS = dict(
    supplier_id=None, bom_id=None, quantity=None, company_id=None,
    service_item_id=None, supplier_warehouse_id=None, id=None, order=None,
    received_qty=None, qty_override=None, bin=None, stock_entry=None,
    subcontract_charge_rate=None, reason=None, posting_date=None,
    status=None, from_date=None, to_date=None, limit="20", offset="0",
    db_path=None,
)


def deploy_skills(tmp_path, db_path):
    """Create a throwaway OPENCLAW_SKILLS_DIR so cross-skill subprocesses resolve.

    Returns the skills dir; sets OPENCLAW_SKILLS_DIR + ERPCLAW_DB_PATH in os.environ.
    """
    skills = os.path.join(str(tmp_path), "skills")
    os.makedirs(os.path.join(skills, "erpclaw"), exist_ok=True)
    os.makedirs(os.path.join(skills, "erpclaw-manufacturing"), exist_ok=True)
    erpclaw_scripts = os.path.join(skills, "erpclaw", "scripts")
    mfg_scripts = os.path.join(skills, "erpclaw-manufacturing", "scripts")
    if not os.path.lexists(erpclaw_scripts):
        os.symlink(_FOUND_SCRIPTS, erpclaw_scripts)
    if not os.path.lexists(mfg_scripts):
        os.symlink(_MFG_DIR, mfg_scripts)
    os.environ["OPENCLAW_SKILLS_DIR"] = skills
    os.environ["ERPCLAW_DB_PATH"] = db_path
    return skills


def load_mfg():
    """Load erpclaw-manufacturing db_query.py as a module."""
    import importlib.util
    path = os.path.join(_MFG_DIR, "db_query.py")
    spec = importlib.util.spec_from_file_location("mfg_dq_subcontract", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def call(fn, conn, db_path, **kw):
    """Invoke a subcontracting action in-process; return its parsed JSON dict.

    db_path is threaded through so the action's own cross-skill subprocesses hit
    the same DB.
    """
    buf = io.StringIO()
    ns = dict(_ARG_DEFAULTS)
    ns["db_path"] = db_path
    ns.update(kw)
    args = argparse.Namespace(**ns)
    from unittest.mock import patch

    def _exit(code=0):
        raise SystemExit(code)

    try:
        with patch("sys.stdout", buf), patch("sys.exit", side_effect=_exit):
            fn(conn, args)
    except SystemExit:
        pass
    out = buf.getvalue().strip()
    return json.loads(out) if out else {"status": "error", "message": "no output"}


def _u():
    return str(uuid.uuid4())


def seed_subcontract_env(conn, raw_rate="20.00", raw_per_fg="2", order_qty="100"):
    """Seed a complete subcontracting environment and return the id dict.

    BOM: 1 finished good consumes `raw_per_fg` units of one raw material (rate
    `raw_rate`) plus a non-stock service line. Opening stock of the raw material is
    seeded at the supplier sub-store so receive-time valuation resolves to
    `raw_rate` (matching how the materials were transferred there). All money is
    Decimal-as-text.
    """
    from erpclaw_lib.stock_posting import insert_sle_entries
    ids = {}
    cid = _u(); ids["company"] = cid
    conn.execute(
        "INSERT INTO company (id, name, abbr, default_currency, country, "
        "fiscal_year_start_month) VALUES (?,?,?,?,?,?)",
        (cid, "SubCo", "SC", "USD", "United States", 1))
    conn.execute(
        "INSERT INTO fiscal_year (id, name, start_date, end_date, company_id) "
        "VALUES (?,?,?,?,?)",
        (_u(), "FY2026", "2026-01-01", "2026-12-31", cid))
    ids["cost_center"] = _u()
    conn.execute("INSERT INTO cost_center (id, name, company_id, is_group) "
                 "VALUES (?,?,?,0)", (ids["cost_center"], "Main", cid))
    for et, pre in (("subcontracting_order", "SCO-"), ("stock_entry", "SE-"),
                    ("purchase_invoice", "PINV-"), ("purchase_receipt", "PR-")):
        conn.execute(
            "INSERT OR IGNORE INTO naming_series "
            "(id, entity_type, prefix, current_value, company_id) VALUES (?,?,?,0,?)",
            (_u(), et, pre, cid))

    def acct(name, atype, rtype):
        aid = _u()
        conn.execute(
            "INSERT INTO account (id, name, account_type, root_type, is_group, "
            "company_id) VALUES (?,?,?,?,0,?)", (aid, name, atype, rtype, cid))
        return aid

    ids["stock_acct"] = acct("Stock In Hand", "stock", "asset")
    acct("Stock Received Not Billed", "stock_received_not_billed", "liability")
    acct("COGS", "cost_of_goods_sold", "expense")
    acct("Creditors", "payable", "liability")
    acct("Subcontract Expense", "expense", "expense")

    sup = _u(); ids["supplier"] = sup
    conn.execute("INSERT INTO supplier (id, name, company_id) VALUES (?,?,?)",
                 (sup, "SubVendor", cid))

    fg = _u(); ids["fg_item"] = fg
    conn.execute(
        "INSERT INTO item (id, item_name, item_code, stock_uom, standard_rate, "
        "is_stock_item) VALUES (?,?,?,?,?,1)",
        (fg, "Assembled Widget", f"FG-{fg[:8]}", "Each", "0"))
    rm = _u(); ids["raw_item"] = rm
    conn.execute(
        "INSERT INTO item (id, item_name, item_code, stock_uom, standard_rate, "
        "is_stock_item) VALUES (?,?,?,?,?,1)",
        (rm, "Raw Bracket", f"RM-{rm[:8]}", "Each", raw_rate))
    svc = _u(); ids["service_item"] = svc
    conn.execute(
        "INSERT INTO item (id, item_name, item_code, stock_uom, standard_rate, "
        "is_stock_item) VALUES (?,?,?,?,?,0)",
        (svc, "Assembly Service", f"SVC-{svc[:8]}", "Each", "0"))

    src = _u(); ids["src_wh"] = src
    conn.execute(
        "INSERT INTO warehouse (id, name, company_id, warehouse_type, account_id) "
        "VALUES (?,?,?,?,?)", (src, "Raw Store", cid, "stores", ids["stock_acct"]))
    sub = _u(); ids["sub_wh"] = sub
    conn.execute(
        "INSERT INTO warehouse (id, name, company_id, warehouse_type, account_id) "
        "VALUES (?,?,?,?,?)",
        (sub, "Subcontractor Store", cid, "transit", ids["stock_acct"]))

    bom = _u(); ids["bom"] = bom
    conn.execute(
        "INSERT INTO bom (id, item_id, quantity, is_active, is_default, company_id) "
        "VALUES (?,?,?,1,1,?)", (bom, fg, "1", cid))
    raw_amount = str(Decimal(raw_per_fg) * Decimal(raw_rate))
    conn.execute(
        "INSERT INTO bom_item (id, bom_id, item_id, quantity, rate, amount, "
        "source_warehouse_id) VALUES (?,?,?,?,?,?,?)",
        (_u(), bom, rm, raw_per_fg, raw_rate, raw_amount, src))
    conn.execute(
        "INSERT INTO bom_item (id, bom_id, item_id, quantity, rate, amount, "
        "source_warehouse_id) VALUES (?,?,?,?,?,?,?)",
        (_u(), bom, svc, "1", "0", "0", None))

    # Opening raw stock at BOTH source (for transfer) and supplier sub-store
    # (so receive-time valuation resolves to raw_rate). Plenty of headroom.
    insert_sle_entries(conn, [{
        "item_id": rm, "warehouse_id": src, "actual_qty": "100000",
        "incoming_rate": raw_rate, "require_rate": True,
    }], voucher_type="stock_entry", voucher_id=f"opening-src-{rm[:8]}",
        posting_date="2026-01-05", company_id=cid)
    insert_sle_entries(conn, [{
        "item_id": rm, "warehouse_id": sub, "actual_qty": "100000",
        "incoming_rate": raw_rate, "require_rate": True,
    }], voucher_type="stock_entry", voucher_id=f"opening-sub-{rm[:8]}",
        posting_date="2026-01-05", company_id=cid)
    conn.commit()
    ids["order_qty"] = order_qty
    ids["raw_rate"] = raw_rate
    ids["raw_per_fg"] = raw_per_fg
    return ids


def gl_count(conn):
    return conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0]


def active_sle_count(conn):
    return conn.execute(
        "SELECT COUNT(*) FROM stock_ledger_entry WHERE is_cancelled = 0").fetchone()[0]
