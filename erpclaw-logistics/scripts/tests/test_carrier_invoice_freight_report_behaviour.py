"""Part A: behaviour of logistics-add-carrier-invoice, the refusals of
logistics-verify-carrier-invoice, and logistics-freight-cost-analysis-report,
read back from the database.

add-carrier-invoice is pinned by the row it writes (amount as an exact string,
status 'pending', no purchase invoice linked) and by its audit row, and its
refusals by exact message with nothing written.

verify-carrier-invoice is pinned only on its guards that run before it calls
the buying module: missing id, unknown invoice, an invoice that is no longer
pending, and a carrier with no supplier linked. Each refusal must leave the
carrier invoice pending and unlinked, create no purchase invoice and write no
verify audit row. Its success path is not pinned here.

freight-cost-analysis-report is pinned by exact totals over rows these tests
create: freight charges grouped by charge type, carrier invoices of every
status, and shipments that carry a shipping cost (one of them set by
allocate-freight from its charges). Rows of a second company must not leak
into the first company's figures.

All document dates are fixed.
"""
import json

from logistics_helpers import (call_action, is_error, is_ok, load_db_query, ns,
                               seed_company, seed_supplier)

mod = load_db_query()

INVOICE_DATE = "2026-03-01"


# ── helpers ────────────────────────────────────────────────────────────────

def _carrier(conn, company_id, name="FastFreight", supplier_id=None):
    r = call_action(mod.logistics_add_carrier, conn, ns(
        company_id=company_id, name=name, carrier_type="ltl",
        supplier_id=supplier_id, carrier_code=None, contact_name=None,
        contact_email=None, contact_phone=None, dot_number=None,
        mc_number=None, insurance_expiry=None))
    assert is_ok(r), r
    return r["id"]


def _shipment(conn, company_id, shipping_cost=None, carrier_id=None):
    r = call_action(mod.logistics_add_shipment, conn, ns(
        company_id=company_id, carrier_id=carrier_id,
        origin_address="1 Dock Rd", origin_city="Portland", origin_state="OR",
        origin_zip="97201", destination_address="9 Pier St",
        destination_city="Seattle", destination_state="WA",
        destination_zip="98101", service_level="ground", weight="120.0",
        dimensions="48x40x40", package_count=2, declared_value="900.00",
        reference_number="PO-338", estimated_delivery="2026-03-10",
        shipping_cost=shipping_cost, tracking_number=None, notes=None))
    assert is_ok(r), r
    return r["id"]


def _charge(conn, company_id, shipment_id, charge_type, amount):
    r = call_action(mod.logistics_add_freight_charge, conn, ns(
        shipment_id=shipment_id, company_id=company_id,
        charge_type=charge_type, description=None, amount=amount))
    assert is_ok(r), r
    return r["id"]


def _invoice(conn, company_id, carrier_id, total_amount, number="CI-338"):
    r = call_action(mod.logistics_add_carrier_invoice, conn, ns(
        carrier_id=carrier_id, company_id=company_id, invoice_number=number,
        invoice_date=INVOICE_DATE, total_amount=total_amount, shipment_count=3))
    assert is_ok(r), r
    return r["id"]


def _invoice_row(conn, invoice_id):
    row = conn.execute(
        "SELECT carrier_id, company_id, invoice_number, invoice_date, "
        "total_amount, invoice_status, purchase_invoice_id, shipment_count "
        "FROM logistics_carrier_invoice WHERE id = ?", (invoice_id,)).fetchone()
    return tuple(row) if row else None


def _count(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()[0]


def _audit_count(conn, action, entity_id=None):
    if entity_id is None:
        return _count(conn, "SELECT COUNT(*) FROM audit_log WHERE action = ?",
                      (action,))
    return _count(conn, "SELECT COUNT(*) FROM audit_log WHERE action = ? "
                        "AND entity_id = ?", (action, entity_id))


def _report(conn, company_id):
    return call_action(mod.logistics_freight_cost_analysis_report, conn,
                       ns(company_id=company_id))


# ── add-carrier-invoice ────────────────────────────────────────────────────

def test_add_carrier_invoice_writes_a_pending_unlinked_row(conn, env):
    carrier_id = _carrier(conn, env["company_id"], supplier_id=env["supplier_id"])
    invoice_id = _invoice(conn, env["company_id"], carrier_id, "1250.00")

    assert _invoice_row(conn, invoice_id) == (
        carrier_id, env["company_id"], "CI-338", INVOICE_DATE,
        "1250.00", "pending", None, 3)
    audit = conn.execute(
        "SELECT skill, entity_type, new_values FROM audit_log "
        "WHERE action = 'logistics-add-carrier-invoice' AND entity_id = ?",
        (invoice_id,)).fetchall()
    assert len(audit) == 1
    assert (audit[0]["skill"], audit[0]["entity_type"]) == (
        "erpclaw-logistics", "logistics_carrier_invoice")
    assert json.loads(audit[0]["new_values"]) == {
        "carrier_id": carrier_id, "total_amount": "1250.00"}
    assert _count(conn, "SELECT COUNT(*) FROM purchase_invoice") == 0


def test_add_carrier_invoice_refusals_write_nothing(conn, env):
    carrier_id = _carrier(conn, env["company_id"])

    def attempt(**over):
        args = dict(carrier_id=carrier_id, company_id=env["company_id"],
                    invoice_number="CI-X", invoice_date=INVOICE_DATE,
                    total_amount="10.00", shipment_count=1)
        args.update(over)
        return call_action(mod.logistics_add_carrier_invoice, conn, ns(**args))

    cases = [
        (dict(carrier_id=None), "--carrier-id is required"),
        (dict(carrier_id="no-such-carrier"), "Carrier no-such-carrier not found"),
        (dict(company_id=None), "--company-id is required"),
        (dict(company_id="no-such-company"), "Company no-such-company not found"),
        (dict(total_amount="12.3.4"), "Invalid total-amount: 12.3.4"),
    ]
    for over, message in cases:
        r = attempt(**over)
        assert is_error(r), (over, r)
        assert r["message"] == message
    assert _count(conn, "SELECT COUNT(*) FROM logistics_carrier_invoice") == 0
    assert _audit_count(conn, "logistics-add-carrier-invoice") == 0


# ── verify-carrier-invoice: guards before the purchase invoice call ────────

def _assert_untouched(conn, invoice_id, carrier_id, env, status="pending"):
    assert _invoice_row(conn, invoice_id) == (
        carrier_id, env["company_id"], "CI-338", INVOICE_DATE,
        "1250.00", status, None, 3)
    assert _count(conn, "SELECT COUNT(*) FROM purchase_invoice") == 0
    assert _count(conn, "SELECT COUNT(*) FROM purchase_invoice_item") == 0
    assert _audit_count(conn, "logistics-verify-carrier-invoice") == 0


def test_verify_carrier_invoice_refuses_missing_and_unknown_invoice(conn, env):
    carrier_id = _carrier(conn, env["company_id"], supplier_id=env["supplier_id"])
    invoice_id = _invoice(conn, env["company_id"], carrier_id, "1250.00")

    r = call_action(mod.logistics_verify_carrier_invoice, conn, ns(id=None))
    assert is_error(r)
    assert r["message"] == "--id is required (carrier invoice ID)"

    r = call_action(mod.logistics_verify_carrier_invoice, conn,
                    ns(id="no-such-invoice"))
    assert is_error(r)
    assert r["message"] == "Carrier invoice no-such-invoice not found"

    _assert_untouched(conn, invoice_id, carrier_id, env)


def test_verify_carrier_invoice_refuses_an_invoice_that_is_not_pending(conn, env):
    carrier_id = _carrier(conn, env["company_id"], supplier_id=env["supplier_id"])
    invoice_id = _invoice(conn, env["company_id"], carrier_id, "1250.00")
    for status in ("disputed", "verified", "paid"):
        conn.execute("UPDATE logistics_carrier_invoice SET invoice_status = ? "
                     "WHERE id = ?", (status, invoice_id))
        conn.commit()
        r = call_action(mod.logistics_verify_carrier_invoice, conn,
                        ns(id=invoice_id))
        assert is_error(r), (status, r)
        assert r["message"] == (
            f"Cannot verify carrier invoice: status is '{status}' "
            "(must be 'pending')")
        _assert_untouched(conn, invoice_id, carrier_id, env, status=status)


def test_verify_carrier_invoice_refuses_a_carrier_with_no_supplier(conn, env):
    carrier_id = _carrier(conn, env["company_id"], name="NoSupplier Haulage")
    invoice_id = _invoice(conn, env["company_id"], carrier_id, "1250.00")

    r = call_action(mod.logistics_verify_carrier_invoice, conn, ns(id=invoice_id))
    assert is_error(r)
    assert r["message"] == (
        "Carrier 'NoSupplier Haulage' has no supplier_id. Link a supplier first "
        "via logistics-update-carrier --id {carrier_id} --supplier-id {supplier_id}")
    _assert_untouched(conn, invoice_id, carrier_id, env)


# ── freight-cost-analysis-report ───────────────────────────────────────────

def test_freight_cost_analysis_report_exact_totals_scoped_to_company(conn, env):
    company = env["company_id"]
    carrier_id = _carrier(conn, company, supplier_id=env["supplier_id"])

    _shipment(conn, company, shipping_cost="45.00", carrier_id=carrier_id)
    allocated = _shipment(conn, company, carrier_id=carrier_id)
    _shipment(conn, company, carrier_id=carrier_id)  # no shipping cost: not counted
    for charge_type, amount in [("base", "19.99"), ("base", "333.33"),
                                ("fuel_surcharge", "0.10"),
                                ("fuel_surcharge", "0.20"),
                                ("accessorial", "0.01")]:
        _charge(conn, company, allocated, charge_type, amount)
    alloc = call_action(mod.logistics_allocate_freight, conn,
                        ns(shipment_id=allocated))
    assert is_ok(alloc), alloc
    assert conn.execute("SELECT shipping_cost FROM logistics_shipment WHERE id = ?",
                        (allocated,)).fetchone()[0] == "353.63"

    _invoice(conn, company, carrier_id, "1250.00", number="CI-1")
    _invoice(conn, company, carrier_id, "0.10", number="CI-2")
    disputed = _invoice(conn, company, carrier_id, "0.20", number="CI-3")
    conn.execute("UPDATE logistics_carrier_invoice SET invoice_status = 'disputed' "
                 "WHERE id = ?", (disputed,))
    conn.commit()

    other = seed_company(conn, name="Other Freight Co", abbr="OFC")
    other_supplier = seed_supplier(conn, other, "Other Carrier Supplier")
    other_carrier = _carrier(conn, other, name="Other Carrier",
                             supplier_id=other_supplier)
    other_ship = _shipment(conn, other, shipping_cost="77.00",
                           carrier_id=other_carrier)
    _charge(conn, other, other_ship, "base", "999.99")
    _charge(conn, other, other_ship, "customs", "40.00")
    _invoice(conn, other, other_carrier, "500.00", number="CI-OTHER")

    r = _report(conn, company)
    assert is_ok(r), r
    assert r["report"] == "freight-cost-analysis"
    assert r["company_id"] == company
    assert r["charges_by_type"] == {
        "base": {"count": 2, "total": "353.32"},
        "fuel_surcharge": {"count": 2, "total": "0.30"},
        "accessorial": {"count": 1, "total": "0.01"},
    }
    assert r["total_carrier_invoices"] == 3
    assert r["total_invoice_amount"] == "1250.30"
    assert r["total_shipments_with_cost"] == 2
    assert r["total_shipping_cost"] == "398.63"

    o = _report(conn, other)
    assert is_ok(o), o
    assert o["charges_by_type"] == {
        "base": {"count": 1, "total": "999.99"},
        "customs": {"count": 1, "total": "40.00"},
    }
    assert (o["total_carrier_invoices"], o["total_invoice_amount"]) == (1, "500.00")
    assert (o["total_shipments_with_cost"], o["total_shipping_cost"]) == (1, "77.00")


def test_freight_cost_analysis_report_empty_company_and_refusals(conn, env):
    r = _report(conn, env["company_id"])
    assert is_ok(r), r
    assert r["charges_by_type"] == {}
    assert (r["total_carrier_invoices"], r["total_invoice_amount"]) == (0, "0.00")
    assert (r["total_shipments_with_cost"], r["total_shipping_cost"]) == (0, "0.00")

    r = _report(conn, None)
    assert is_error(r)
    assert r["message"] == "--company-id is required"
    r = _report(conn, "no-such-company")
    assert is_error(r)
    assert r["message"] == "Company no-such-company not found"
