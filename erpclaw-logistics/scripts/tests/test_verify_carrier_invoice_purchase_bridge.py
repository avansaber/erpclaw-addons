"""In-process bridge tests for logistics-verify-carrier-invoice.

Verifying a carrier invoice creates a real draft purchase invoice through
the buying module (via erpclaw_lib.cross_skill.create_purchase_invoice),
or refuses with the buying cause. No subprocess is spawned: the shared
library's call_skill_action is redirected to the genuine inventory/buying
functions in-process, while recording exactly which action and flags the
vertical sent.
"""
import pytest

from logistics_helpers import call_action, ns, is_ok, is_error
from test_logistics import _mk_carrier, _mk_invoice
from erpclaw_lib import cross_skill as _cs
from erpclaw_lib.cross_skill import CrossSkillError
from erpclaw_lib.query import Q, P, Table, Field, fn


def _delegate_buying_in_process(conn, monkeypatch):
    """Redirect cross_skill.call_skill_action to the REAL foundation functions.

    call_skill_action shells out to the INSTALLED skill tree, which is neither
    this worktree's code nor this test's database. Running the genuine
    inventory/buying functions in-process instead keeps every assertion real
    (item resolution, totals) while still recording exactly which action and
    flags the vertical sent through the shared library.
    """
    import argparse
    import importlib.util
    import io
    import json as _json
    import os as _os
    from unittest.mock import patch as _patch
    from logistics_helpers import SRC_DIR as _SRC

    def _load(domain):
        path = _os.path.join(_SRC, "erpclaw", "scripts", domain, "db_query.py")
        spec = importlib.util.spec_from_file_location(f"_fnd_{domain}", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    buying = _load("erpclaw-buying")
    inventory = _load("erpclaw-inventory")
    captured = {}

    def _run(fn, args_ns):
        buf = io.StringIO()

        def _fake_exit(code=0):
            raise SystemExit(code)

        try:
            with _patch("sys.stdout", buf), _patch("sys.exit", side_effect=_fake_exit):
                fn(conn, args_ns)
        except SystemExit:
            pass
        return _json.loads(buf.getvalue().strip())

    def _in_process(skill_name, action, args=None, db_path=None, timeout=30):
        flags = dict(args or {})
        captured.setdefault("calls", []).append(
            {"skill": skill_name, "action": action, "args": flags})
        if action == "add-item":
            result = _run(inventory.add_item, argparse.Namespace(
                item_code=flags.get("--item-code"),
                item_name=flags.get("--item-name"),
                item_type=flags.get("--item-type"),
                valuation_method=None, item_group=None, stock_uom=None,
                has_batch=None, has_serial=None, standard_rate=None,
                custom_fields=None))
        elif action == "list-items":
            result = _run(inventory.list_items, argparse.Namespace(
                item_group=None, item_type=None, search=flags.get("--search"),
                limit="20", offset="0", warehouse_id=None, company_id=None))
        elif action == "create-purchase-invoice":
            result = _run(buying.create_purchase_invoice, argparse.Namespace(
                company_id=flags.get("--company-id"),
                supplier_id=flags.get("--supplier-id"),
                items=flags.get("--items"),
                posting_date=flags.get("--posting-date"),
                due_date=flags.get("--due-date"),
                tax_template_id=None,
                purchase_order_id=None,
                purchase_receipt_id=None,
                cwip_asset_id=None))
        else:
            raise AssertionError(f"unexpected cross-skill action {action}")
        if result.get("status") == "error":
            raise _cs.CrossSkillError(
                result.get("message", f"{action} failed"))
        return result

    monkeypatch.setattr(_cs, "call_skill_action", _in_process)
    return captured


def test_verify_creates_draft_purchase_invoice(conn, env, mod, monkeypatch):
    _delegate_buying_in_process(conn, monkeypatch)
    carrier_id = _mk_carrier(conn, mod, env["company_id"],
                             supplier_id=env["supplier_id"])
    invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id,
                             "1250.00", "CI-PI")
    r = call_action(mod.logistics_verify_carrier_invoice, conn, ns(id=invoice_id))
    assert is_ok(r), r
    pi_id = r["purchase_invoice_id"]
    assert pi_id, r
    t = Table("logistics_carrier_invoice")
    stored = conn.execute(
        Q.from_(t).select(t.star).where(t.id == P()).get_sql(),
        (invoice_id,)).fetchone()
    assert stored["invoice_status"] == "verified"
    assert stored["purchase_invoice_id"] == pi_id
    pt = Table("purchase_invoice")
    pi = conn.execute(
        Q.from_(pt).select(pt.star).where(pt.id == P()).get_sql(),
        (pi_id,)).fetchone()
    assert pi["status"] == "draft"
    assert pi["total_amount"] == "1250.00"
    assert pi["grand_total"] == "1250.00"
    assert pi["posting_date"] == "2026-03-01"
    it = Table("purchase_invoice_item")
    lines = conn.execute(
        Q.from_(it).select(it.star).where(it.purchase_invoice_id == P()).get_sql(),
        (pi_id,)).fetchall()
    assert len(lines) == 1
    assert lines[0]["quantity"] == "1.00"
    assert lines[0]["rate"] == "1250.00"
    assert lines[0]["amount"] == "1250.00"
    mt = Table("item")
    item = conn.execute(
        Q.from_(mt).select(mt.star).where(mt.id == P()).get_sql(),
        (lines[0]["item_id"],)).fetchone()
    assert item["item_code"] == f"SVC-{env['company_id']}"
    gt = Table("gl_entry")
    gl_count = conn.execute(
        Q.from_(gt).select(fn.Count(gt.star)).where(gt.voucher_id == P()).get_sql(),
        (pi_id,)).fetchone()[0]
    assert gl_count == 0


def test_verify_refuses_when_buying_refuses(conn, env, mod, monkeypatch):
    _delegate_buying_in_process(conn, monkeypatch)
    carrier_id = _mk_carrier(conn, mod, env["company_id"],
                             supplier_id=env["supplier_id"])
    invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id,
                             "1250.00", "CI-PI-REF")
    pt = Table("purchase_invoice")
    before = conn.execute(
        Q.from_(pt).select(fn.Count(pt.star)).get_sql(), ()).fetchone()[0]
    real = _cs.call_skill_action

    def _refuse_purchase(skill_name, action, args=None, db_path=None, timeout=30):
        if action == "create-purchase-invoice":
            raise CrossSkillError("simulated purchase refusal")
        return real(skill_name, action, args=args, db_path=db_path, timeout=timeout)

    monkeypatch.setattr(_cs, "call_skill_action", _refuse_purchase)
    r = call_action(mod.logistics_verify_carrier_invoice, conn, ns(id=invoice_id))
    assert is_error(r), r
    assert r["message"] == "Failed to create purchase invoice: simulated purchase refusal"
    t = Table("logistics_carrier_invoice")
    stored = conn.execute(
        Q.from_(t).select(t.star).where(t.id == P()).get_sql(),
        (invoice_id,)).fetchone()
    assert stored["invoice_status"] == "pending"
    assert stored["purchase_invoice_id"] is None
    after = conn.execute(
        Q.from_(pt).select(fn.Count(pt.star)).get_sql(), ()).fetchone()[0]
    assert after == before
