"""Idempotent verify-carrier-invoice: one draft purchase invoice, retries reuse it.

Verifying a pending carrier invoice creates exactly one draft, non-stock
purchase invoice for the carrier's supplier through the buying module, links
it and marks the carrier invoice verified in one logistics write. A carrier
invoice that already carries a purchase_invoice_id never causes a second
purchase invoice: verification reuses the linked one when usable and refuses
when it is not. Verification writes no ledger row.

Harness: a copy of _delegate_selling_in_process from
constructclaw/scripts/tests/test_progress_bill_g703.py, retargeted at buying.
It monkeypatches erpclaw_lib.cross_skill.call_skill_action, records every
call, and routes add-item / list-items / create-purchase-invoice to the real
inventory/buying functions in-process (loaded from SRC_DIR), so item
resolution, totals and statuses are all real while the argv the vertical sent
stays observable. Not imported across modules.
"""
import argparse
import importlib.util
import io
import json
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from logistics_helpers import (
    SRC_DIR, call_action, ns, is_ok, is_error, seed_supplier,
)
from test_logistics import _mk_carrier, _mk_invoice
from erpclaw_lib.query import Q, P, Table, Field, fn, update_row


def _delegate_buying_in_process(conn, monkeypatch):
    """Redirect cross_skill.call_skill_action to the REAL buying functions.

    call_skill_action shells out to the INSTALLED skill tree, which is neither
    this worktree's code nor this test's database. Running the genuine
    inventory/buying functions in-process instead keeps every assertion real
    (item resolution, totals, draft status) while still recording exactly
    which action and flags the vertical sent through the shared library.
    """
    import sys as _sys
    from unittest.mock import patch as _patch
    from logistics_helpers import SRC_DIR as _SRC
    from erpclaw_lib import cross_skill as _cs
    _cs._SERVICE_ITEM_CACHE.clear()

    def _load(domain):
        path = os.path.join(_SRC, "erpclaw", "scripts", domain, "db_query.py")
        spec = importlib.util.spec_from_file_location(f"_fnd_{domain}", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    buying = _load("erpclaw-buying")
    inventory = _load("erpclaw-inventory")
    captured = {"calls": [], "fail_create": None}

    def _run(fn, args_ns):
        buf = io.StringIO()

        def _fake_exit(code=0):
            raise SystemExit(code)

        try:
            with _patch("sys.stdout", buf), _patch("sys.exit", side_effect=_fake_exit):
                fn(conn, args_ns)
        except SystemExit:
            pass
        return json.loads(buf.getvalue().strip())

    def _in_process(skill_name, action, args=None, db_path=None, timeout=30):
        flags = dict(args or {})
        captured["calls"].append(
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
            if captured["fail_create"] is not None:
                raise _cs.CrossSkillError(captured["fail_create"])
            result = _run(buying.create_purchase_invoice, argparse.Namespace(
                company_id=flags.get("--company-id"),
                supplier_id=flags.get("--supplier-id"),
                items=flags.get("--items"),
                posting_date=flags.get("--posting-date"),
                due_date=flags.get("--due-date"),
                tax_template_id=None, purchase_order_id=None,
                purchase_receipt_id=None, cwip_asset_id=None))
        else:
            raise AssertionError(f"unexpected cross-skill action {action}")
        if result.get("status") == "error":
            raise _cs.CrossSkillError(
                result.get("message", f"{action} failed"))
        return result

    monkeypatch.setattr(_cs, "call_skill_action", _in_process)
    return captured


def _create_calls(captured):
    return [c for c in captured["calls"] if c["action"] == "create-purchase-invoice"]


def _count(conn, table):
    t = Table(table)
    return conn.execute(
        Q.from_(t).select(fn.Count(t.star)).get_sql()
    ).fetchone()[0]


def _verify_audit_count(conn):
    t = Table("audit_log")
    return conn.execute(
        Q.from_(t).select(fn.Count(t.star)).where(Field("action") == P()).get_sql(),
        ("logistics-verify-carrier-invoice",),
    ).fetchone()[0]


def _carrier_row(conn, invoice_id):
    t = Table("logistics_carrier_invoice")
    row = conn.execute(
        Q.from_(t).select(t.star).where(t.id == P()).get_sql(),
        (invoice_id,),
    ).fetchone()
    assert row is not None
    return {k: row[k] for k in row.keys()}


def _pi_row(conn, pi_id):
    t = Table("purchase_invoice")
    row = conn.execute(
        Q.from_(t).select(t.star).where(t.id == P()).get_sql(),
        (pi_id,),
    ).fetchone()
    assert row is not None, f"purchase invoice {pi_id} missing"
    return {k: row[k] for k in row.keys()}


def _snap(conn, invoice_id):
    return (
        _carrier_row(conn, invoice_id),
        _count(conn, "purchase_invoice"),
        _count(conn, "purchase_invoice_item"),
        _count(conn, "gl_entry"),
        _verify_audit_count(conn),
    )


def _link_setup_pi(conn, invoice_id, pi_id):
    conn.execute(
        update_row("logistics_carrier_invoice",
                   data={"purchase_invoice_id": P()}, where={"id": P()}),
        (pi_id, invoice_id),
    )
    conn.commit()


def _direct_draft_pi(company_id, supplier_id):
    from erpclaw_lib import cross_skill as _cs
    return _cs.create_purchase_invoice(
        supplier_id=supplier_id,
        items=[{"description": "Setup freight", "qty": "1", "rate": "1250.00"}],
        company_id=company_id,
        posting_date="2026-03-01",
    )["purchase_invoice_id"]


class TestVerifyCarrierInvoiceIdempotent:
    def test_verify_creates_one_draft_non_stock_purchase_invoice(
            self, conn, env, mod, monkeypatch):
        captured = _delegate_buying_in_process(conn, monkeypatch)
        company_id = env["company_id"]
        carrier_id = _mk_carrier(conn, mod, company_id,
                                 supplier_id=env["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, company_id, carrier_id,
                                 "1250.00", "CI-673")
        before = _snap(conn, invoice_id)
        assert before[0]["invoice_status"] == "pending"
        assert before[0]["purchase_invoice_id"] is None

        r = call_action(mod.logistics_verify_carrier_invoice, conn,
                        ns(id=invoice_id))
        assert is_ok(r), r
        pi_id = r.get("purchase_invoice_id")
        assert pi_id, r

        after = _snap(conn, invoice_id)
        stored = after[0]
        assert stored["invoice_status"] == "verified"
        assert stored["purchase_invoice_id"] == pi_id

        pi = _pi_row(conn, pi_id)
        assert pi["status"] == "draft"
        assert pi["supplier_id"] == env["supplier_id"]
        assert pi["total_amount"] == "1250.00"
        assert pi["grand_total"] == "1250.00"
        assert pi["outstanding_amount"] == "1250.00"
        assert pi["posting_date"] == "2026-03-01"

        t = Table("purchase_invoice_item")
        lines = conn.execute(
            Q.from_(t).select(t.star).where(t.purchase_invoice_id == P()).get_sql(),
            (pi_id,),
        ).fetchall()
        assert len(lines) == 1
        line = {k: lines[0][k] for k in lines[0].keys()}
        assert line["quantity"] == "1.00"
        assert line["rate"] == "1250.00"
        assert line["amount"] == "1250.00"

        ti = Table("item")
        irow = conn.execute(
            Q.from_(ti).select(ti.star).where(ti.id == P()).get_sql(),
            (line["item_id"],),
        ).fetchone()
        assert irow["item_code"] == f"SVC-{company_id}"
        assert irow["is_stock_item"] == 0

        assert after[1] - before[1] == 1
        assert after[2] - before[2] == 1
        assert after[3] - before[3] == 0
        assert after[4] - before[4] == 1

        creates = _create_calls(captured)
        assert len(creates) == 1
        assert creates[0]["skill"] == "erpclaw"
        assert "--remarks" not in creates[0]["args"]
        assert "--project-id" not in creates[0]["args"]

    def test_linked_usable_invoice_is_reused_without_a_second(
            self, conn, env, mod, monkeypatch):
        captured = _delegate_buying_in_process(conn, monkeypatch)
        setup_pi = _direct_draft_pi(env["company_id"], env["supplier_id"])
        carrier_id = _mk_carrier(conn, mod, env["company_id"],
                                 supplier_id=env["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id,
                                 "1250.00", "CI-673-B")
        _link_setup_pi(conn, invoice_id, setup_pi)
        captured["calls"].clear()

        before = _snap(conn, invoice_id)
        r = call_action(mod.logistics_verify_carrier_invoice, conn,
                        ns(id=invoice_id))
        assert is_ok(r), r
        assert r["purchase_invoice_id"] == setup_pi

        after = _snap(conn, invoice_id)
        assert after[0]["invoice_status"] == "verified"
        assert after[0]["purchase_invoice_id"] == setup_pi
        assert _create_calls(captured) == []
        assert after[1] == before[1]
        assert after[4] - before[4] == 1

    def test_linked_cancelled_or_missing_invoice_refuses(
            self, conn, env, mod, monkeypatch):
        captured = _delegate_buying_in_process(conn, monkeypatch)
        setup_pi = _direct_draft_pi(env["company_id"], env["supplier_id"])
        carrier_id = _mk_carrier(conn, mod, env["company_id"],
                                 supplier_id=env["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id,
                                 "1250.00", "CI-673-C")
        _link_setup_pi(conn, invoice_id, setup_pi)
        conn.execute(
            update_row("purchase_invoice",
                       data={"status": P()}, where={"id": P()}),
            ("cancelled", setup_pi),
        )
        conn.commit()
        captured["calls"].clear()

        before = _snap(conn, invoice_id)
        r = call_action(mod.logistics_verify_carrier_invoice, conn,
                        ns(id=invoice_id))
        assert is_error(r), r
        assert r["message"] == (
            f"Carrier invoice CI-673-C is linked to purchase invoice {setup_pi} "
            "in status 'cancelled'; it cannot be verified against that invoice."
        )
        assert _snap(conn, invoice_id) == before
        assert captured["calls"] == []

        conn.execute(
            update_row("logistics_carrier_invoice",
                       data={"purchase_invoice_id": P()}, where={"id": P()}),
            ("no-such-pi", invoice_id),
        )
        conn.commit()
        before = _snap(conn, invoice_id)
        r = call_action(mod.logistics_verify_carrier_invoice, conn,
                        ns(id=invoice_id))
        assert is_error(r), r
        assert r["message"] == (
            "Carrier invoice CI-673-C is linked to purchase invoice no-such-pi "
            "in status 'None'; it cannot be verified against that invoice."
        )
        assert _snap(conn, invoice_id) == before
        assert captured["calls"] == []

    def test_linked_invoice_of_another_supplier_refuses(
            self, conn, env, mod, monkeypatch):
        captured = _delegate_buying_in_process(conn, monkeypatch)
        other_supplier = seed_supplier(conn, env["company_id"],
                                       "Other Carrier Supplier")
        other_pi = _direct_draft_pi(env["company_id"], other_supplier)
        carrier_id = _mk_carrier(conn, mod, env["company_id"],
                                 supplier_id=env["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id,
                                 "1250.00", "CI-673-D")
        _link_setup_pi(conn, invoice_id, other_pi)
        captured["calls"].clear()

        before = _snap(conn, invoice_id)
        r = call_action(mod.logistics_verify_carrier_invoice, conn,
                        ns(id=invoice_id))
        assert is_error(r), r
        assert r["message"] == (
            f"Carrier invoice CI-673-D is linked to purchase invoice {other_pi} "
            f"of supplier {other_supplier}, not the carrier's supplier "
            f"{env['supplier_id']}."
        )
        assert _snap(conn, invoice_id) == before
        assert captured["calls"] == []

    def test_buying_refusal_leaves_carrier_invoice_pending(
            self, conn, env, mod, monkeypatch):
        captured = _delegate_buying_in_process(conn, monkeypatch)
        captured["fail_create"] = "simulated purchase refusal"
        carrier_id = _mk_carrier(conn, mod, env["company_id"],
                                 supplier_id=env["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id,
                                 "1250.00", "CI-673-E")

        before = _snap(conn, invoice_id)
        r = call_action(mod.logistics_verify_carrier_invoice, conn,
                        ns(id=invoice_id))
        assert is_error(r), r
        assert r["message"] == (
            "Failed to create purchase invoice: simulated purchase refusal"
        )
        after = _snap(conn, invoice_id)
        assert after == before
        assert after[0]["invoice_status"] == "pending"
        assert after[0]["purchase_invoice_id"] is None
        assert len(_create_calls(captured)) == 1

    def test_second_verify_after_success_refuses_and_creates_nothing(
            self, conn, env, mod, monkeypatch):
        captured = _delegate_buying_in_process(conn, monkeypatch)
        carrier_id = _mk_carrier(conn, mod, env["company_id"],
                                 supplier_id=env["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id,
                                 "1250.00", "CI-673-F")
        r = call_action(mod.logistics_verify_carrier_invoice, conn,
                        ns(id=invoice_id))
        assert is_ok(r), r
        captured["calls"].clear()

        before = _snap(conn, invoice_id)
        r = call_action(mod.logistics_verify_carrier_invoice, conn,
                        ns(id=invoice_id))
        assert is_error(r), r
        assert r["message"] == (
            "Cannot verify carrier invoice: status is 'verified' (must be 'pending')"
        )
        assert _snap(conn, invoice_id) == before
        assert captured["calls"] == []
