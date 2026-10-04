"""Carrier invoice amount and company checks (m854).

Add refuses non-positive / non-currency amounts and foreign carriers;
verify re-checks the stored amount and companies before the buying module.
"""
import pytest

from logistics_helpers import (
    call_action, ns, is_ok, is_error,
    seed_company, seed_supplier, seed_naming_series,
)
from test_logistics import _mk_carrier, _mk_invoice
from test_verify_carrier_invoice_purchase_bridge import _delegate_buying_in_process
from erpclaw_lib.query import Q, P, Table, Field, fn, insert_row, update_row


def _count(conn, table):
    t = Table(table)
    return conn.execute(Q.from_(t).select(fn.Count(t.star)).get_sql()).fetchone()[0]


def _audit_count(conn, action):
    t = Table("audit_log")
    return conn.execute(
        Q.from_(t).select(fn.Count(t.star)).where(Field("action") == P()).get_sql(),
        (action,),
    ).fetchone()[0]


def _invoice_row(conn, invoice_id):
    t = Table("logistics_carrier_invoice")
    row = conn.execute(
        Q.from_(t).select(t.star).where(t.id == P()).get_sql(),
        (invoice_id,),
    ).fetchone()
    assert row is not None
    return {k: row[k] for k in row.keys()}


def _snap(conn, invoice_id):
    return (
        _invoice_row(conn, invoice_id),
        _count(conn, "purchase_invoice"),
        _count(conn, "purchase_invoice_item"),
        _audit_count(conn, "logistics-add-carrier-invoice"),
        _audit_count(conn, "logistics-verify-carrier-invoice"),
    )


def _other_env(conn):
    company_id = seed_company(conn, name="Other Freight Co", abbr="OFC")
    seed_naming_series(conn, company_id)
    supplier_id = seed_supplier(conn, company_id, "Other Carrier Supplier")
    return {"company_id": company_id, "supplier_id": supplier_id}


def _set_amount(conn, invoice_id, value):
    conn.execute(
        update_row("logistics_carrier_invoice",
                   data={"total_amount": P()}, where={"id": P()}),
        (value, invoice_id),
    )
    conn.commit()


def _set_carrier(conn, invoice_id, carrier_id):
    conn.execute(
        update_row("logistics_carrier_invoice",
                   data={"carrier_id": P()}, where={"id": P()}),
        (carrier_id, invoice_id),
    )
    conn.commit()


def _link_pi(conn, invoice_id, pi_id):
    conn.execute(
        update_row("logistics_carrier_invoice",
                   data={"purchase_invoice_id": P()}, where={"id": P()}),
        (pi_id, invoice_id),
    )
    conn.commit()


def _add_invoice(conn, mod, company_id, carrier_id, total_amount=None, number="CI-T"):
    kwargs = dict(carrier_id=carrier_id, company_id=company_id,
                  invoice_number=number, invoice_date="2026-03-01",
                  shipment_count=1)
    if total_amount is not None:
        kwargs["total_amount"] = total_amount
    return call_action(mod.logistics_add_carrier_invoice, conn, ns(**kwargs))


class TestAddRefusesBadAmounts:
    @pytest.mark.parametrize("value", ["NaN", "Infinity", "-50.00", "0", "0.00", "1e3", "12.345", "+5"])
    def test_bad_amount_refused(self, conn, env, mod, value):
        carrier_id = _mk_carrier(conn, mod, env["company_id"], supplier_id=env["supplier_id"])
        before = _count(conn, "logistics_carrier_invoice")
        before_audit = _audit_count(conn, "logistics-add-carrier-invoice")
        r = _add_invoice(conn, mod, env["company_id"], carrier_id, value, number="CI-BAD")
        assert is_error(r), r
        assert r["message"] == f"total-amount must be a positive amount with at most two decimal places: {value}"
        assert _count(conn, "logistics_carrier_invoice") == before
        assert _audit_count(conn, "logistics-add-carrier-invoice") == before_audit

    def test_unparsable_keeps_invalid_text(self, conn, env, mod):
        carrier_id = _mk_carrier(conn, mod, env["company_id"], supplier_id=env["supplier_id"])
        before = _count(conn, "logistics_carrier_invoice")
        r = _add_invoice(conn, mod, env["company_id"], carrier_id, "12.3.4", number="CI-BAD2")
        assert is_error(r), r
        assert r["message"] == "Invalid total-amount: 12.3.4"
        assert _count(conn, "logistics_carrier_invoice") == before

    def test_missing_amount_required(self, conn, env, mod):
        carrier_id = _mk_carrier(conn, mod, env["company_id"], supplier_id=env["supplier_id"])
        before = _count(conn, "logistics_carrier_invoice")
        r = call_action(mod.logistics_add_carrier_invoice, conn, ns(
            carrier_id=carrier_id, company_id=env["company_id"],
            invoice_number="CI-MISS", invoice_date="2026-03-01", shipment_count=1,
        ))
        assert is_error(r), r
        assert r["message"] == "--total-amount is required"
        assert _count(conn, "logistics_carrier_invoice") == before
        r2 = _add_invoice(conn, mod, env["company_id"], carrier_id, "", number="CI-EMPTY")
        assert is_error(r2), r2
        assert r2["message"] == "--total-amount is required"
        assert _count(conn, "logistics_carrier_invoice") == before


class TestAddAcceptsValidAmounts:
    @pytest.mark.parametrize("value", ["0.10", "5", "1250.00"])
    def test_valid_stored_exactly(self, conn, env, mod, value):
        carrier_id = _mk_carrier(conn, mod, env["company_id"], supplier_id=env["supplier_id"])
        r = _add_invoice(conn, mod, env["company_id"], carrier_id, value, number="CI-OK")
        assert is_ok(r), r
        assert r["total_amount"] == value
        stored = _invoice_row(conn, r["id"])
        assert stored["total_amount"] == value
        assert stored["invoice_status"] == "pending"


class TestAddRefusesForeignCarrier:
    def test_other_company_carrier(self, conn, env, mod):
        other = _other_env(conn)
        foreign_carrier = _mk_carrier(conn, mod, other["company_id"], supplier_id=other["supplier_id"])
        before = _count(conn, "logistics_carrier_invoice")
        before_audit = _audit_count(conn, "logistics-add-carrier-invoice")
        r = _add_invoice(conn, mod, env["company_id"], foreign_carrier, "1250.00", number="CI-FOR")
        assert is_error(r), r
        assert r["message"] == f"Carrier {foreign_carrier} belongs to another company"
        assert _count(conn, "logistics_carrier_invoice") == before
        assert _audit_count(conn, "logistics-add-carrier-invoice") == before_audit


class TestVerifyRefusesBadStoredAmounts:
    @pytest.mark.parametrize("value", ["NaN", "Infinity", "12.345", "0"])
    def test_planted_amount_refused(self, conn, env, mod, monkeypatch, value):
        captured = _delegate_buying_in_process(conn, monkeypatch)
        carrier_id = _mk_carrier(conn, mod, env["company_id"], supplier_id=env["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id, "1250.00", "CI-VAMT")
        inv_number = _invoice_row(conn, invoice_id)["invoice_number"]
        _set_amount(conn, invoice_id, value)
        captured.get("calls", []).clear()
        before = _snap(conn, invoice_id)
        r = call_action(mod.logistics_verify_carrier_invoice, conn, ns(id=invoice_id))
        assert is_error(r), r
        assert r["message"] == (
            f"Carrier invoice {inv_number} has total amount '{value}'; "
            "only a positive amount with at most two decimal places can be verified."
        )
        assert captured.get("calls", []) == []
        assert _snap(conn, invoice_id) == before


class TestVerifyRefusesCompanyMismatch:
    def test_carrier_of_another_company(self, conn, env, mod, monkeypatch):
        captured = _delegate_buying_in_process(conn, monkeypatch)
        other = _other_env(conn)
        foreign_carrier = _mk_carrier(conn, mod, other["company_id"], supplier_id=other["supplier_id"])
        own_carrier = _mk_carrier(conn, mod, env["company_id"], supplier_id=env["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, env["company_id"], own_carrier, "1250.00", "CI-VCAR")
        _set_carrier(conn, invoice_id, foreign_carrier)
        inv = _invoice_row(conn, invoice_id)
        inv_number = inv["invoice_number"]
        inv_company = inv["company_id"]
        carrier_row = conn.execute(
            Q.from_(Table("logistics_carrier")).select(Table("logistics_carrier").star).where(Field("id") == P()).get_sql(),
            (foreign_carrier,),
        ).fetchone()
        carrier_company = carrier_row["company_id"]
        assert carrier_company != inv_company
        captured.get("calls", []).clear()
        before = _snap(conn, invoice_id)
        r = call_action(mod.logistics_verify_carrier_invoice, conn, ns(id=invoice_id))
        assert is_error(r), r
        assert r["message"] == (
            f"Carrier invoice {inv_number} belongs to company {inv_company}, "
            f"but carrier {foreign_carrier} belongs to company {carrier_company}."
        )
        assert captured.get("calls", []) == []
        assert _snap(conn, invoice_id) == before

    def test_supplier_of_another_company(self, conn, env, mod, monkeypatch):
        captured = _delegate_buying_in_process(conn, monkeypatch)
        other = _other_env(conn)
        carrier_id = _mk_carrier(conn, mod, env["company_id"], supplier_id=other["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id, "1250.00", "CI-VSUP")
        inv_number = _invoice_row(conn, invoice_id)["invoice_number"]
        captured.get("calls", []).clear()
        before = _snap(conn, invoice_id)
        r = call_action(mod.logistics_verify_carrier_invoice, conn, ns(id=invoice_id))
        assert is_error(r), r
        assert r["message"] == (
            f"Supplier {other['supplier_id']} linked to carrier {carrier_id} "
            f"belongs to another company than carrier invoice {inv_number}."
        )
        assert captured.get("calls", []) == []
        assert _snap(conn, invoice_id) == before


class TestVerifyReuseRefusesForeignPurchaseInvoice:
    def test_linked_pi_of_another_company(self, conn, env, mod, monkeypatch):
        from erpclaw_lib import cross_skill as _cs
        captured = _delegate_buying_in_process(conn, monkeypatch)
        other = _other_env(conn)
        carrier_id = _mk_carrier(conn, mod, env["company_id"], supplier_id=env["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id, "1250.00", "CI-VLINK")
        inv_number = _invoice_row(conn, invoice_id)["invoice_number"]
        pi_id = _cs.create_purchase_invoice(
            supplier_id=env["supplier_id"],
            items=[{"description": "Setup freight", "qty": "1", "rate": "1250.00"}],
            company_id=env["company_id"],
            posting_date="2026-03-01",
        )["purchase_invoice_id"]
        conn.execute(
            update_row("purchase_invoice", data={"company_id": P()}, where={"id": P()}),
            (other["company_id"], pi_id),
        )
        conn.commit()
        _link_pi(conn, invoice_id, pi_id)
        captured.get("calls", []).clear()
        before = _snap(conn, invoice_id)
        r = call_action(mod.logistics_verify_carrier_invoice, conn, ns(id=invoice_id))
        assert is_error(r), r
        assert r["message"] == (
            f"Carrier invoice {inv_number} is linked to purchase invoice {pi_id} of another company."
        )
        assert captured.get("calls", []) == []
        assert _snap(conn, invoice_id) == before


class TestVerifyHappyPath:
    def test_happy_path(self, conn, env, mod, monkeypatch):
        _delegate_buying_in_process(conn, monkeypatch)
        carrier_id = _mk_carrier(conn, mod, env["company_id"], supplier_id=env["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id, "1250.00", "CI-HAPPY")
        r = call_action(mod.logistics_verify_carrier_invoice, conn, ns(id=invoice_id))
        assert is_ok(r), r
        pi_id = r["purchase_invoice_id"]
        assert pi_id
        t = Table("purchase_invoice")
        pi = conn.execute(
            Q.from_(t).select(t.star).where(t.id == P()).get_sql(), (pi_id,)).fetchone()
        assert pi["status"] == "draft"
        assert pi["grand_total"] == "1250.00"
        stored = _invoice_row(conn, invoice_id)
        assert stored["invoice_status"] == "verified"
        assert stored["purchase_invoice_id"] == pi_id
