"""L1 pytest tests for erpclaw-logistics (33 actions across 5 domain modules).

Tests cover:
  carriers.py (8): add/update/get/list carrier, add/list carrier-rate,
    carrier-performance-report, carrier-cost-comparison
  shipments.py (10): add/update/get/list shipment, update-shipment-status,
    add/list tracking-event, add-proof-of-delivery, generate-bill-of-lading,
    shipment-summary-report
  routes.py (6): add/update/list route, add/list route-stop, optimize-route-report
  freight.py (7): add/list freight-charge, allocate-freight, add/list carrier-invoice,
    verify-carrier-invoice, freight-cost-analysis-report
  reports.py (3): on-time-delivery-report, delivery-exception-report, status

Depth (m495): logistics-on-time-delivery-report,
logistics-delivery-exception-report, logistics-freight-cost-analysis-report and
logistics-verify-carrier-invoice are pinned behaviourally below -- exact rows
read back from the database, exact Decimal money strings, refusal cases that
leave the database byte-identical -- not just response shape or routability.
"""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import json
from decimal import Decimal

import pytest

from logistics_helpers import (
    call_action, ns, is_ok, is_error, _uuid,
    seed_company, seed_naming_series, seed_supplier,
)
from erpclaw_lib.query import Q, P, Table, Field, fn, update_row


# ===========================================================================
# Carrier helpers
# ===========================================================================

def _add_carrier(conn, env, mod, name="FastFreight", carrier_type="parcel"):
    """Add a carrier and return result."""
    return call_action(mod.logistics_add_carrier, conn, ns(
        company_id=env["company_id"],
        name=name,
        carrier_type=carrier_type,
        supplier_id=None,
        carrier_code=None,
        contact_name=None,
        contact_email=None,
        contact_phone=None,
        dot_number=None,
        mc_number=None,
        insurance_expiry=None,
    ))


def _add_shipment(conn, env, mod, carrier_id=None):
    """Add a shipment and return result."""
    return call_action(mod.logistics_add_shipment, conn, ns(
        company_id=env["company_id"],
        carrier_id=carrier_id,
        origin_address="123 Main St",
        origin_city="Portland",
        origin_state="OR",
        origin_zip="97201",
        destination_address="456 Oak Ave",
        destination_city="Seattle",
        destination_state="WA",
        destination_zip="98101",
        service_level="ground",
        weight="50.0",
        dimensions="24x18x12",
        package_count=1,
        declared_value="500.00",
        reference_number="PO-001",
        estimated_delivery="2025-07-15",
        shipping_cost="45.00",
        tracking_number=None,
        notes="Handle with care",
    ))


# ===========================================================================
# Carrier Actions
# ===========================================================================


class TestAddCarrier:
    def test_add_carrier_ok(self, conn, env, mod):
        r = _add_carrier(conn, env, mod)
        assert is_ok(r), r
        assert r["carrier_status"] == "active"
        assert r["name"] == "FastFreight"

    def test_add_carrier_missing_name(self, conn, env, mod):
        r = call_action(mod.logistics_add_carrier, conn, ns(
            company_id=env["company_id"],
            name=None,
            carrier_type=None,
            supplier_id=None,
            carrier_code=None,
            contact_name=None,
            contact_email=None,
            contact_phone=None,
            dot_number=None,
            mc_number=None,
            insurance_expiry=None,
        ))
        assert is_error(r)

    def test_add_carrier_with_supplier(self, conn, env, mod):
        r = call_action(mod.logistics_add_carrier, conn, ns(
            company_id=env["company_id"],
            name="Linked Carrier",
            carrier_type="ltl",
            supplier_id=env["supplier_id"],
            carrier_code="LC001",
            contact_name="John",
            contact_email="john@carrier.com",
            contact_phone="555-0100",
            dot_number="DOT123",
            mc_number="MC456",
            insurance_expiry="2026-12-31",
        ))
        assert is_ok(r), r
        assert r["supplier_id"] == env["supplier_id"]


class TestUpdateCarrier:
    def test_update_carrier_ok(self, conn, env, mod):
        r1 = _add_carrier(conn, env, mod)
        carrier_id = r1["id"]

        r2 = call_action(mod.logistics_update_carrier, conn, ns(
            id=carrier_id,
            name="FastFreight Express",
            carrier_code=None,
            contact_name=None,
            contact_email=None,
            contact_phone=None,
            dot_number=None,
            mc_number=None,
            carrier_type=None,
            insurance_expiry=None,
            carrier_status=None,
            on_time_pct=None,
            supplier_id=None,
        ))
        assert is_ok(r2), r2
        assert "name" in r2["updated_fields"]

    def test_update_carrier_no_fields(self, conn, env, mod):
        r1 = _add_carrier(conn, env, mod)
        carrier_id = r1["id"]

        r2 = call_action(mod.logistics_update_carrier, conn, ns(
            id=carrier_id,
            name=None, carrier_code=None, contact_name=None,
            contact_email=None, contact_phone=None, dot_number=None,
            mc_number=None, carrier_type=None, insurance_expiry=None,
            carrier_status=None, on_time_pct=None, supplier_id=None,
        ))
        assert is_error(r2)


class TestGetCarrier:
    def test_get_carrier_ok(self, conn, env, mod):
        r1 = _add_carrier(conn, env, mod)
        carrier_id = r1["id"]

        r2 = call_action(mod.logistics_get_carrier, conn, ns(id=carrier_id))
        assert is_ok(r2), r2
        assert r2["id"] == carrier_id
        assert "rates" in r2

    def test_get_carrier_not_found(self, conn, env, mod):
        r = call_action(mod.logistics_get_carrier, conn, ns(id=_uuid()))
        assert is_error(r)


class TestListCarriers:
    def test_list_carriers(self, conn, env, mod):
        _add_carrier(conn, env, mod, "Carrier A")
        _add_carrier(conn, env, mod, "Carrier B")

        r = call_action(mod.logistics_list_carriers, conn, ns(
            company_id=env["company_id"],
            carrier_status=None,
            carrier_type=None,
            search=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 2


class TestAddCarrierRate:
    def test_add_carrier_rate_ok(self, conn, env, mod):
        r1 = _add_carrier(conn, env, mod)
        carrier_id = r1["id"]

        r2 = call_action(mod.logistics_add_carrier_rate, conn, ns(
            carrier_id=carrier_id,
            company_id=env["company_id"],
            service_level="express",
            origin_zone="West",
            destination_zone="Northwest",
            weight_min="0",
            weight_max="100",
            rate_per_unit="2.50",
            flat_rate="15.00",
            effective_date="2025-01-01",
            expiry_date="2025-12-31",
        ))
        assert is_ok(r2), r2
        assert r2["service_level"] == "express"


class TestListCarrierRates:
    def test_list_carrier_rates(self, conn, env, mod):
        r1 = _add_carrier(conn, env, mod)
        carrier_id = r1["id"]

        # Add two rates
        for svc in ["ground", "express"]:
            call_action(mod.logistics_add_carrier_rate, conn, ns(
                carrier_id=carrier_id,
                company_id=env["company_id"],
                service_level=svc,
                origin_zone=None, destination_zone=None,
                weight_min=None, weight_max=None,
                rate_per_unit="1.00", flat_rate=None,
                effective_date=None, expiry_date=None,
            ))

        r = call_action(mod.logistics_list_carrier_rates, conn, ns(
            carrier_id=carrier_id,
            company_id=None,
            service_level=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 2


# ===========================================================================
# Shipment Actions
# ===========================================================================


class TestAddShipment:
    def test_add_shipment_ok(self, conn, env, mod):
        r = _add_shipment(conn, env, mod)
        assert is_ok(r), r
        assert r["shipment_status"] == "created"

    def test_add_shipment_with_carrier(self, conn, env, mod):
        cr = _add_carrier(conn, env, mod)
        carrier_id = cr["id"]
        r = _add_shipment(conn, env, mod, carrier_id=carrier_id)
        assert is_ok(r), r

    def test_add_shipment_missing_company(self, conn, env, mod):
        r = call_action(mod.logistics_add_shipment, conn, ns(
            company_id=None,
            carrier_id=None,
            origin_address=None, origin_city=None, origin_state=None, origin_zip=None,
            destination_address=None, destination_city=None, destination_state=None, destination_zip=None,
            service_level=None, weight=None, dimensions=None, package_count=None,
            declared_value=None, reference_number=None,
            estimated_delivery=None, shipping_cost=None, tracking_number=None, notes=None,
        ))
        assert is_error(r)


class TestUpdateShipment:
    def test_update_shipment_ok(self, conn, env, mod):
        r1 = _add_shipment(conn, env, mod)
        ship_id = r1["id"]

        r2 = call_action(mod.logistics_update_shipment, conn, ns(
            id=ship_id,
            tracking_number="1Z999AA10123456784",
            origin_address=None, origin_city=None, origin_state=None, origin_zip=None,
            destination_address=None, destination_city=None, destination_state=None, destination_zip=None,
            weight=None, dimensions=None, declared_value=None, reference_number=None,
            estimated_delivery=None, shipping_cost=None, notes=None,
            service_level=None, carrier_id=None, package_count=None,
        ))
        assert is_ok(r2), r2
        assert "tracking_number" in r2["updated_fields"]


class TestGetShipment:
    def test_get_shipment_ok(self, conn, env, mod):
        r1 = _add_shipment(conn, env, mod)
        ship_id = r1["id"]

        r2 = call_action(mod.logistics_get_shipment, conn, ns(id=ship_id))
        assert is_ok(r2), r2
        assert r2["id"] == ship_id
        assert "tracking_events" in r2
        assert "freight_charges" in r2


class TestListShipments:
    def test_list_shipments(self, conn, env, mod):
        _add_shipment(conn, env, mod)
        _add_shipment(conn, env, mod)

        r = call_action(mod.logistics_list_shipments, conn, ns(
            company_id=env["company_id"],
            shipment_status=None,
            carrier_id=None,
            service_level=None,
            search=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 2


class TestUpdateShipmentStatus:
    def test_update_shipment_status_ok(self, conn, env, mod):
        r1 = _add_shipment(conn, env, mod)
        ship_id = r1["id"]

        r2 = call_action(mod.logistics_update_shipment_status, conn, ns(
            id=ship_id,
            shipment_status="in_transit",
        ))
        assert is_ok(r2), r2
        assert r2["shipment_status"] == "in_transit"
        assert r2["old_status"] == "created"

    def test_update_shipment_status_missing(self, conn, env, mod):
        r1 = _add_shipment(conn, env, mod)
        ship_id = r1["id"]

        r2 = call_action(mod.logistics_update_shipment_status, conn, ns(
            id=ship_id,
            shipment_status=None,
        ))
        assert is_error(r2)


class TestAddTrackingEvent:
    def test_add_tracking_event_ok(self, conn, env, mod):
        r1 = _add_shipment(conn, env, mod)
        ship_id = r1["id"]

        r2 = call_action(mod.logistics_add_tracking_event, conn, ns(
            shipment_id=ship_id,
            event_type="picked_up",
            company_id=env["company_id"],
            event_timestamp=None,
            location="Portland, OR",
            description="Package picked up from shipper",
        ))
        assert is_ok(r2), r2
        assert r2["event_type"] == "picked_up"


class TestListTrackingEvents:
    def test_list_tracking_events(self, conn, env, mod):
        r1 = _add_shipment(conn, env, mod)
        ship_id = r1["id"]

        # Add two events
        for etype in ["created", "picked_up"]:
            call_action(mod.logistics_add_tracking_event, conn, ns(
                shipment_id=ship_id,
                event_type=etype,
                company_id=env["company_id"],
                event_timestamp=None,
                location="Portland, OR",
                description=None,
            ))

        r = call_action(mod.logistics_list_tracking_events, conn, ns(
            shipment_id=ship_id,
            company_id=None,
            event_type=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 2


class TestAddProofOfDelivery:
    def test_add_proof_of_delivery_ok(self, conn, env, mod):
        r1 = _add_shipment(conn, env, mod)
        ship_id = r1["id"]

        r2 = call_action(mod.logistics_add_proof_of_delivery, conn, ns(
            id=ship_id,
            pod_signature="J. Smith",
            pod_timestamp=None,
        ))
        assert is_ok(r2), r2
        assert r2["shipment_status"] == "delivered"
        assert r2["pod_signature"] == "J. Smith"


class TestGenerateBillOfLading:
    def test_generate_bill_of_lading_ok(self, conn, env, mod):
        r1 = _add_shipment(conn, env, mod)
        ship_id = r1["id"]

        r2 = call_action(mod.logistics_generate_bill_of_lading, conn, ns(id=ship_id))
        assert is_ok(r2), r2
        assert r2["document_type"] == "Bill of Lading"


# ===========================================================================
# Route Actions
# ===========================================================================


class TestAddRoute:
    def test_add_route_ok(self, conn, env, mod):
        r = call_action(mod.logistics_add_route, conn, ns(
            company_id=env["company_id"],
            name="Portland to Seattle",
            origin="Portland, OR",
            destination="Seattle, WA",
            distance="174",
            estimated_hours="3.0",
        ))
        assert is_ok(r), r
        assert r["route_status"] == "active"

    def test_add_route_missing_name(self, conn, env, mod):
        r = call_action(mod.logistics_add_route, conn, ns(
            company_id=env["company_id"],
            name=None,
            origin=None,
            destination=None,
            distance=None,
            estimated_hours=None,
        ))
        assert is_error(r)


class TestUpdateRoute:
    def test_update_route_ok(self, conn, env, mod):
        r1 = call_action(mod.logistics_add_route, conn, ns(
            company_id=env["company_id"],
            name="Portland to Seattle",
            origin="Portland, OR",
            destination="Seattle, WA",
            distance="174",
            estimated_hours="3.0",
        ))
        route_id = r1["id"]

        r2 = call_action(mod.logistics_update_route, conn, ns(
            id=route_id,
            name=None,
            origin=None,
            destination=None,
            distance="180",
            estimated_hours=None,
            route_status=None,
        ))
        assert is_ok(r2), r2
        assert "distance" in r2["updated_fields"]


class TestListRoutes:
    def test_list_routes(self, conn, env, mod):
        for name in ["Route A", "Route B"]:
            call_action(mod.logistics_add_route, conn, ns(
                company_id=env["company_id"],
                name=name,
                origin=None, destination=None,
                distance=None, estimated_hours=None,
            ))
        r = call_action(mod.logistics_list_routes, conn, ns(
            company_id=env["company_id"],
            route_status=None,
            search=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 2


class TestAddRouteStop:
    def test_add_route_stop_ok(self, conn, env, mod):
        r1 = call_action(mod.logistics_add_route, conn, ns(
            company_id=env["company_id"],
            name="Portland to Seattle",
            origin="Portland, OR",
            destination="Seattle, WA",
            distance="174",
            estimated_hours="3.0",
        ))
        route_id = r1["id"]

        r2 = call_action(mod.logistics_add_route_stop, conn, ns(
            route_id=route_id,
            company_id=env["company_id"],
            stop_order=1,
            address="789 Highway 5",
            city="Olympia",
            state="WA",
            zip_code="98501",
            estimated_arrival="2025-07-10T14:00:00Z",
            stop_type="delivery",
        ))
        assert is_ok(r2), r2
        assert r2["stop_order"] == 1


class TestListRouteStops:
    def test_list_route_stops(self, conn, env, mod):
        r1 = call_action(mod.logistics_add_route, conn, ns(
            company_id=env["company_id"],
            name="Test Route",
            origin=None, destination=None,
            distance=None, estimated_hours=None,
        ))
        route_id = r1["id"]

        for i in range(3):
            call_action(mod.logistics_add_route_stop, conn, ns(
                route_id=route_id,
                company_id=env["company_id"],
                stop_order=i + 1,
                address=None, city=f"City{i}", state="WA",
                zip_code=None, estimated_arrival=None,
                stop_type="delivery",
            ))

        r = call_action(mod.logistics_list_route_stops, conn, ns(
            route_id=route_id,
            company_id=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 3


# ===========================================================================
# Freight Actions
# ===========================================================================


class TestAddFreightCharge:
    def test_add_freight_charge_ok(self, conn, env, mod):
        sr = _add_shipment(conn, env, mod)
        ship_id = sr["id"]

        r = call_action(mod.logistics_add_freight_charge, conn, ns(
            shipment_id=ship_id,
            company_id=env["company_id"],
            charge_type="base",
            description="Base shipping charge",
            amount="45.00",
        ))
        assert is_ok(r), r
        assert r["charge_type"] == "base"
        assert r["amount"] == "45.00"


class TestListFreightCharges:
    def test_list_freight_charges(self, conn, env, mod):
        sr = _add_shipment(conn, env, mod)
        ship_id = sr["id"]

        for ct, amt in [("base", "40.00"), ("fuel_surcharge", "5.00")]:
            call_action(mod.logistics_add_freight_charge, conn, ns(
                shipment_id=ship_id,
                company_id=env["company_id"],
                charge_type=ct,
                description=None,
                amount=amt,
            ))

        r = call_action(mod.logistics_list_freight_charges, conn, ns(
            shipment_id=ship_id,
            company_id=None,
            charge_type=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 2


class TestAllocateFreight:
    def test_allocate_freight_ok(self, conn, env, mod):
        sr = _add_shipment(conn, env, mod)
        ship_id = sr["id"]

        call_action(mod.logistics_add_freight_charge, conn, ns(
            shipment_id=ship_id,
            company_id=env["company_id"],
            charge_type="base",
            description=None,
            amount="40.00",
        ))
        call_action(mod.logistics_add_freight_charge, conn, ns(
            shipment_id=ship_id,
            company_id=env["company_id"],
            charge_type="fuel_surcharge",
            description=None,
            amount="5.50",
        ))

        r = call_action(mod.logistics_allocate_freight, conn, ns(
            shipment_id=ship_id,
        ))
        assert is_ok(r), r
        assert r["total_freight"] == "45.50"
        assert r["charge_count"] == 2


class TestAddCarrierInvoice:
    def test_add_carrier_invoice_ok(self, conn, env, mod):
        cr = _add_carrier(conn, env, mod)
        carrier_id = cr["id"]

        r = call_action(mod.logistics_add_carrier_invoice, conn, ns(
            carrier_id=carrier_id,
            company_id=env["company_id"],
            invoice_number="INV-001",
            invoice_date="2025-07-01",
            total_amount="1250.00",
            shipment_count=5,
        ))
        assert is_ok(r), r
        assert r["invoice_status"] == "pending"
        assert r["total_amount"] == "1250.00"


class TestListCarrierInvoices:
    def test_list_carrier_invoices(self, conn, env, mod):
        cr = _add_carrier(conn, env, mod)
        carrier_id = cr["id"]

        for num in ["INV-001", "INV-002"]:
            call_action(mod.logistics_add_carrier_invoice, conn, ns(
                carrier_id=carrier_id,
                company_id=env["company_id"],
                invoice_number=num,
                invoice_date="2025-07-01",
                total_amount="500.00",
                shipment_count=2,
            ))

        r = call_action(mod.logistics_list_carrier_invoices, conn, ns(
            carrier_id=carrier_id,
            company_id=None,
            invoice_status=None,
            limit=20,
            offset=0,
        ))
        assert is_ok(r), r
        assert r["total_count"] == 2


# ===========================================================================
# Reports
# ===========================================================================


class TestShipmentSummaryReport:
    def test_shipment_summary_report(self, conn, env, mod):
        _add_shipment(conn, env, mod)
        r = call_action(mod.logistics_shipment_summary_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert r["total_shipments"] >= 1
        assert "by_status" in r


class TestCarrierPerformanceReport:
    def test_carrier_performance_report(self, conn, env, mod):
        _add_carrier(conn, env, mod)
        r = call_action(mod.logistics_carrier_performance_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert r["total_carriers"] >= 1


class TestCarrierCostComparison:
    def test_carrier_cost_comparison(self, conn, env, mod):
        _add_carrier(conn, env, mod)
        r = call_action(mod.logistics_carrier_cost_comparison, conn, ns(
            company_id=env["company_id"],
            service_level=None,
        ))
        assert is_ok(r), r
        assert len(r["carriers"]) >= 1


class TestOptimizeRouteReport:
    def test_optimize_route_report(self, conn, env, mod):
        call_action(mod.logistics_add_route, conn, ns(
            company_id=env["company_id"],
            name="Route X",
            origin="Portland", destination="Seattle",
            distance="174", estimated_hours="3",
        ))
        r = call_action(mod.logistics_optimize_route_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert r["active_routes"] >= 1


# ===========================================================================
# Behavioural depth for the four routed-but-shallow actions (m495).
# Helpers read every row back through PyPika-built queries (never raw catalog
# access); money is compared as exact Decimal strings, never float.
# ===========================================================================

_SNAPSHOT_TABLES = [
    "logistics_shipment", "logistics_tracking_event",
    "logistics_carrier", "logistics_carrier_rate",
    "logistics_route", "logistics_route_stop",
    "logistics_freight_charge", "logistics_carrier_invoice",
    "audit_log",
]


def _snapshot(conn, tables=_SNAPSHOT_TABLES):
    """Full row dump per table, as sorted JSON strings (order-free compare)."""
    snap = {}
    for name in tables:
        t = Table(name)
        rows = conn.execute(
            Q.from_(t).select(t.star).orderby(Field("id")).get_sql()
        ).fetchall()
        snap[name] = sorted(
            json.dumps({k: r[k] for k in r.keys()}, sort_keys=True, default=str)
            for r in rows
        )
    return snap


def _audit_count(conn, action):
    t = Table("audit_log")
    return conn.execute(
        Q.from_(t).select(fn.Count(t.star)).where(Field("action") == P()).get_sql(),
        (action,),
    ).fetchone()[0]


def _mk_carrier(conn, mod, company_id, name="FastFreight", supplier_id=None):
    r = call_action(mod.logistics_add_carrier, conn, ns(
        company_id=company_id, name=name, carrier_type="ltl",
        supplier_id=supplier_id, carrier_code=None, contact_name=None,
        contact_email=None, contact_phone=None, dot_number=None,
        mc_number=None, insurance_expiry=None,
    ))
    assert is_ok(r), r
    return r["id"]


def _mk_shipment(conn, mod, company_id, carrier_id=None, reference_number="PO-001",
                 tracking_number=None, estimated_delivery="2026-03-10",
                 shipping_cost=None):
    r = call_action(mod.logistics_add_shipment, conn, ns(
        company_id=company_id, carrier_id=carrier_id,
        origin_address="1 Dock Rd", origin_city="Portland", origin_state="OR",
        origin_zip="97201", destination_address="9 Pier St",
        destination_city="Seattle", destination_state="WA",
        destination_zip="98101", service_level="ground", weight="120.0",
        dimensions="48x40x40", package_count=1, declared_value="900.00",
        reference_number=reference_number,
        estimated_delivery=estimated_delivery, shipping_cost=shipping_cost,
        tracking_number=tracking_number, notes=None,
    ))
    assert is_ok(r), r
    return r["id"]


def _mk_charge(conn, mod, company_id, shipment_id, charge_type, amount):
    r = call_action(mod.logistics_add_freight_charge, conn, ns(
        shipment_id=shipment_id, company_id=company_id,
        charge_type=charge_type, description=None, amount=amount,
    ))
    assert is_ok(r), r
    return r["id"]


def _mk_invoice(conn, mod, company_id, carrier_id, total_amount, number):
    r = call_action(mod.logistics_add_carrier_invoice, conn, ns(
        carrier_id=carrier_id, company_id=company_id, invoice_number=number,
        invoice_date="2026-03-01", total_amount=total_amount, shipment_count=1,
    ))
    assert is_ok(r), r
    return r["id"]


def _other_env(conn):
    """A second company with its own carrier supplier and naming series."""
    company_id = seed_company(conn, name="Other Freight Co", abbr="OFC")
    seed_naming_series(conn, company_id)
    supplier_id = seed_supplier(conn, company_id, "Other Carrier Supplier")
    return {"company_id": company_id, "supplier_id": supplier_id}


def _set_status(conn, mod, shipment_id, shipment_status):
    r = call_action(mod.logistics_update_shipment_status, conn, ns(
        id=shipment_id, shipment_status=shipment_status,
    ))
    assert is_ok(r), r


def _deliver(conn, mod, shipment_id, pod_timestamp, pod_signature="J. Smith"):
    r = call_action(mod.logistics_add_proof_of_delivery, conn, ns(
        id=shipment_id, pod_signature=pod_signature, pod_timestamp=pod_timestamp,
    ))
    assert is_ok(r), r


def _track(conn, mod, company_id, shipment_id, event_type, event_timestamp,
           location="Seattle, WA", description="note"):
    r = call_action(mod.logistics_add_tracking_event, conn, ns(
        shipment_id=shipment_id, event_type=event_type, company_id=company_id,
        location=location, description=description,
        event_timestamp=event_timestamp,
    ))
    assert is_ok(r), r
    return r["id"]


class TestOnTimeDeliveryReport:
    """Behavioural: figures are recomputed from the shipment rows the test wrote.

    The report is read-only -- it stores no row and reaches no ledger (so no
    balance assertion can hold here) -- therefore the test also proves the
    report wrote nothing: the full snapshot is identical afterwards and no
    audit row exists for the action.
    """

    def test_on_time_delivery_report_exact_counts_scoped_to_company(
            self, conn, env, mod):
        company = env["company_id"]
        carrier_id = _mk_carrier(conn, mod, company, supplier_id=env["supplier_id"])
        on_time = _mk_shipment(conn, mod, company, carrier_id,
                               reference_number="OT-ON", tracking_number="TRK-OT-ON")
        late = _mk_shipment(conn, mod, company, carrier_id,
                            reference_number="OT-LATE", tracking_number="TRK-OT-LATE")
        no_est = _mk_shipment(conn, mod, company, carrier_id,
                              reference_number="OT-NOEST",
                              tracking_number="TRK-OT-NOEST",
                              estimated_delivery=None)
        open_ship = _mk_shipment(conn, mod, company, carrier_id,
                                 reference_number="OT-OPEN",
                                 tracking_number="TRK-OT-OPEN")
        _set_status(conn, mod, open_ship, "in_transit")
        _deliver(conn, mod, on_time, "2026-03-09")
        _deliver(conn, mod, late, "2026-03-12")
        _deliver(conn, mod, no_est, "2026-03-11")

        other = _other_env(conn)
        other_carrier = _mk_carrier(conn, mod, other["company_id"],
                                    name="Other Carrier",
                                    supplier_id=other["supplier_id"])
        other_ship = _mk_shipment(conn, mod, other["company_id"], other_carrier,
                                  reference_number="OT-OTHER",
                                  tracking_number="TRK-OT-OTHER")
        _deliver(conn, mod, other_ship, "2026-03-09")

        before = _snapshot(conn)
        r = call_action(mod.logistics_on_time_delivery_report, conn,
                        ns(company_id=company))
        assert is_ok(r), r
        assert r["report"] == "on-time-delivery"
        assert r["company_id"] == company
        assert (r["total_delivered"], r["on_time"], r["late"],
                r["no_estimate"]) == (3, 1, 1, 1)
        assert r["on_time_pct"] == "33.3"
        assert r["by_carrier"] == [{
            "carrier_id": carrier_id, "carrier_name": "FastFreight",
            "delivered": 3, "on_time": 1, "on_time_pct": "33.3",
        }]
        assert _snapshot(conn) == before
        assert _audit_count(conn, "logistics-on-time-delivery-report") == 0

        o = call_action(mod.logistics_on_time_delivery_report, conn,
                        ns(company_id=other["company_id"]))
        assert is_ok(o), o
        assert (o["total_delivered"], o["on_time"], o["late"],
                o["no_estimate"]) == (1, 1, 0, 0)

    def test_on_time_delivery_report_refusals_leave_database_unchanged(
            self, conn, env, mod):
        carrier_id = _mk_carrier(conn, mod, env["company_id"])
        _mk_shipment(conn, mod, env["company_id"], carrier_id,
                     reference_number="OT-R", tracking_number="TRK-OT-R")
        before = _snapshot(conn)

        r = call_action(mod.logistics_on_time_delivery_report, conn,
                        ns(company_id=None))
        assert is_error(r)
        assert r["message"] == "--company-id is required"
        r = call_action(mod.logistics_on_time_delivery_report, conn,
                        ns(company_id="no-such-company"))
        assert is_error(r)
        assert r["message"] == "Company no-such-company not found"
        assert _snapshot(conn) == before


class TestDeliveryExceptionReport:
    """Behavioural: exception/returned rows and exception events read back exactly.

    Read-only like the other reports: no stored row, no ledger legs (commented
    so no balance assertion is added later). The snapshot comparison proves the
    report wrote nothing; the second company proves scoping of both the
    shipment rows and the tracking-event rows.
    """

    def test_delivery_exception_report_exact_rows_scoped_to_company(
            self, conn, env, mod):
        company = env["company_id"]
        carrier_id = _mk_carrier(conn, mod, company)
        e1 = _mk_shipment(conn, mod, company, carrier_id,
                          reference_number="REF-EX-1", tracking_number="TRK-EX-1")
        e2 = _mk_shipment(conn, mod, company, carrier_id,
                          reference_number="REF-EX-2", tracking_number="TRK-EX-2")
        returned = _mk_shipment(conn, mod, company, carrier_id,
                                reference_number="REF-RT-1",
                                tracking_number="TRK-RT-1")
        delivered = _mk_shipment(conn, mod, company, carrier_id,
                                 reference_number="REF-OK-1",
                                 tracking_number="TRK-OK-1")
        _set_status(conn, mod, e1, "exception")
        _set_status(conn, mod, e2, "exception")
        _set_status(conn, mod, returned, "returned")
        _deliver(conn, mod, delivered, "2026-03-09")
        _track(conn, mod, company, e1, "exception", "2026-03-05T10:00:00Z",
               description="Refused at dock")
        _track(conn, mod, company, e1, "picked_up", "2026-03-01T10:00:00Z",
               location="Portland, OR", description="Picked up")
        _track(conn, mod, company, delivered, "delivered",
               "2026-03-09T10:00:00Z", description="Signed")

        other = _other_env(conn)
        other_carrier = _mk_carrier(conn, mod, other["company_id"],
                                    name="Other Carrier")
        other_ship = _mk_shipment(conn, mod, other["company_id"], other_carrier,
                                  reference_number="REF-EX-O",
                                  tracking_number="TRK-EX-O")
        _set_status(conn, mod, other_ship, "exception")
        _track(conn, mod, other["company_id"], other_ship, "exception",
               "2026-03-06T10:00:00Z", description="Other exception")

        before = _snapshot(conn)
        r = call_action(mod.logistics_delivery_exception_report, conn,
                        ns(company_id=company))
        assert is_ok(r), r
        assert r["report"] == "delivery-exception"
        assert r["company_id"] == company
        assert (r["total_exceptions"], r["total_returned"]) == (2, 1)
        by_id = {s["id"]: s for s in r["exception_shipments"]}
        assert set(by_id) == {e1, e2}
        assert by_id[e1] == {
            "id": e1, "tracking_number": "TRK-EX-1",
            "reference_number": "REF-EX-1", "destination_city": "Seattle",
            "destination_state": "WA", "carrier_id": carrier_id,
        }
        assert by_id[e2]["tracking_number"] == "TRK-EX-2"
        assert r["exception_events"] == [{
            "shipment_id": e1, "event_timestamp": "2026-03-05T10:00:00Z",
            "location": "Seattle, WA", "description": "Refused at dock",
        }]
        assert _snapshot(conn) == before
        assert _audit_count(conn, "logistics-delivery-exception-report") == 0

        o = call_action(mod.logistics_delivery_exception_report, conn,
                        ns(company_id=other["company_id"]))
        assert is_ok(o), o
        assert (o["total_exceptions"], o["total_returned"]) == (1, 0)
        assert [s["id"] for s in o["exception_shipments"]] == [other_ship]

    def test_delivery_exception_report_refusals_leave_database_unchanged(
            self, conn, env, mod):
        carrier_id = _mk_carrier(conn, mod, env["company_id"])
        ship_id = _mk_shipment(conn, mod, env["company_id"], carrier_id,
                               reference_number="REF-EX-R",
                               tracking_number="TRK-EX-R")
        _set_status(conn, mod, ship_id, "exception")
        before = _snapshot(conn)

        r = call_action(mod.logistics_delivery_exception_report, conn,
                        ns(company_id=None))
        assert is_error(r)
        assert r["message"] == "--company-id is required"
        r = call_action(mod.logistics_delivery_exception_report, conn,
                        ns(company_id="no-such-company"))
        assert is_error(r)
        assert r["message"] == "Company no-such-company not found"
        assert _snapshot(conn) == before


class TestFreightCostAnalysisReport:
    """Behavioural: exact money totals (Decimal strings) over seeded rows.

    Read-only: no stored row, no ledger legs. Invoices of every status count
    toward the invoice totals (a disputed invoice is still invoiced money), and
    shipments without a shipping cost are excluded from the shipment totals.
    """

    def test_freight_cost_analysis_report_exact_money_scoped_to_company(
            self, conn, env, mod):
        company = env["company_id"]
        carrier_id = _mk_carrier(conn, mod, company,
                                 supplier_id=env["supplier_id"])
        s1 = _mk_shipment(conn, mod, company, carrier_id,
                          reference_number="FR-1", tracking_number="TRK-FR-1",
                          shipping_cost="45.00")
        s2 = _mk_shipment(conn, mod, company, carrier_id,
                          reference_number="FR-2", tracking_number="TRK-FR-2",
                          shipping_cost="10.55")
        s3 = _mk_shipment(conn, mod, company, carrier_id,
                          reference_number="FR-3", tracking_number="TRK-FR-3")
        _mk_charge(conn, mod, company, s1, "base", "19.99")
        _mk_charge(conn, mod, company, s1, "base", "0.01")
        _mk_charge(conn, mod, company, s1, "fuel_surcharge", "0.30")
        _mk_charge(conn, mod, company, s3, "customs", "5.50")
        _mk_invoice(conn, mod, company, carrier_id, "1250.00", "CI-A")
        disputed = _mk_invoice(conn, mod, company, carrier_id, "0.10", "CI-B")
        conn.execute(
            update_row("logistics_carrier_invoice",
                       data={"invoice_status": P()}, where={"id": P()}),
            ("disputed", disputed),
        )
        conn.commit()

        other = _other_env(conn)
        other_carrier = _mk_carrier(conn, mod, other["company_id"],
                                    name="Other Carrier",
                                    supplier_id=other["supplier_id"])
        other_ship = _mk_shipment(conn, mod, other["company_id"], other_carrier,
                                  reference_number="FR-O",
                                  tracking_number="TRK-FR-O",
                                  shipping_cost="77.00")
        _mk_charge(conn, mod, other["company_id"], other_ship, "base", "999.99")
        _mk_invoice(conn, mod, other["company_id"], other_carrier, "500.00",
                    "CI-OTHER")

        before = _snapshot(conn)
        r = call_action(mod.logistics_freight_cost_analysis_report, conn,
                        ns(company_id=company))
        assert is_ok(r), r
        assert r["report"] == "freight-cost-analysis"
        assert r["company_id"] == company
        assert r["charges_by_type"] == {
            "base": {"count": 2, "total": "20.00"},
            "fuel_surcharge": {"count": 1, "total": "0.30"},
            "customs": {"count": 1, "total": "5.50"},
        }
        assert r["total_carrier_invoices"] == 2
        assert r["total_invoice_amount"] == "1250.10"
        assert Decimal(r["total_invoice_amount"]) == Decimal("1250.10")
        assert r["total_shipments_with_cost"] == 2
        assert r["total_shipping_cost"] == "55.55"
        assert Decimal(r["total_shipping_cost"]) == Decimal("55.55")
        assert _snapshot(conn) == before
        assert _audit_count(conn, "logistics-freight-cost-analysis-report") == 0

        o = call_action(mod.logistics_freight_cost_analysis_report, conn,
                        ns(company_id=other["company_id"]))
        assert is_ok(o), o
        assert o["charges_by_type"] == {
            "base": {"count": 1, "total": "999.99"},
        }
        assert (o["total_carrier_invoices"],
                o["total_invoice_amount"]) == (1, "500.00")
        assert (o["total_shipments_with_cost"],
                o["total_shipping_cost"]) == (1, "77.00")

    def test_freight_cost_analysis_report_refusals_leave_database_unchanged(
            self, conn, env, mod):
        carrier_id = _mk_carrier(conn, mod, env["company_id"])
        ship_id = _mk_shipment(conn, mod, env["company_id"], carrier_id,
                               reference_number="FR-R", tracking_number="TRK-FR-R",
                               shipping_cost="45.00")
        _mk_charge(conn, mod, env["company_id"], ship_id, "base", "19.99")
        _mk_invoice(conn, mod, env["company_id"], carrier_id, "1250.00", "CI-R")
        before = _snapshot(conn)

        r = call_action(mod.logistics_freight_cost_analysis_report, conn,
                        ns(company_id=None))
        assert is_error(r)
        assert r["message"] == "--company-id is required"
        r = call_action(mod.logistics_freight_cost_analysis_report, conn,
                        ns(company_id="no-such-company"))
        assert is_error(r)
        assert r["message"] == "Company no-such-company not found"
        assert _snapshot(conn) == before


class TestVerifyCarrierInvoice:
    """Behavioural: every refusal leaves the stored invoice row byte-identical.

    The refusal path stores no row and reaches no ledger, so there are no legs
    to balance here -- the test pins the full stored row (amount as an exact
    Decimal string, still 'pending', still unlinked), the absence of any
    purchase-invoice rows, and the absence of a verify audit row. The success
    path is deliberately not pinned: it shells out to erpclaw-buying through a
    subprocess, which cannot resolve inside this harness.
    """

    _VERIFY_TABLES = _SNAPSHOT_TABLES + [
        "purchase_invoice", "purchase_invoice_item",
    ]

    def _stored_row(self, conn, invoice_id):
        t = Table("logistics_carrier_invoice")
        row = conn.execute(
            Q.from_(t).select(t.star).where(Field("id") == P()).get_sql(),
            (invoice_id,),
        ).fetchone()
        assert row is not None
        return {k: row[k] for k in row.keys()}

    def test_verify_carrier_invoice_refusals_leave_stored_row_untouched(
            self, conn, env, mod):
        carrier_id = _mk_carrier(conn, mod, env["company_id"],
                                 supplier_id=env["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id,
                                 "1250.00", "CI-VRF")

        before = _snapshot(conn, self._VERIFY_TABLES)
        r = call_action(mod.logistics_verify_carrier_invoice, conn, ns(id=None))
        assert is_error(r)
        assert r["message"] == "--id is required (carrier invoice ID)"
        r = call_action(mod.logistics_verify_carrier_invoice, conn,
                        ns(id="no-such-invoice"))
        assert is_error(r)
        assert r["message"] == "Carrier invoice no-such-invoice not found"
        assert _snapshot(conn, self._VERIFY_TABLES) == before

        for status in ("disputed", "verified", "paid"):
            conn.execute(
                update_row("logistics_carrier_invoice",
                           data={"invoice_status": P()}, where={"id": P()}),
                (status, invoice_id),
            )
            conn.commit()
            before = _snapshot(conn, self._VERIFY_TABLES)
            r = call_action(mod.logistics_verify_carrier_invoice, conn,
                            ns(id=invoice_id))
            assert is_error(r), (status, r)
            assert r["message"] == (
                f"Cannot verify carrier invoice: status is '{status}' "
                "(must be 'pending')")
            assert _snapshot(conn, self._VERIFY_TABLES) == before
            stored = self._stored_row(conn, invoice_id)
            assert stored["invoice_status"] == status
            assert stored["total_amount"] == "1250.00"
            assert Decimal(stored["total_amount"]) == Decimal("1250.00")
            assert stored["purchase_invoice_id"] is None

        plain_carrier = _mk_carrier(conn, mod, env["company_id"],
                                    name="NoSupplier Haulage")
        plain_invoice = _mk_invoice(conn, mod, env["company_id"], plain_carrier,
                                    "1250.00", "CI-VRF-PLAIN")
        before = _snapshot(conn, self._VERIFY_TABLES)
        r = call_action(mod.logistics_verify_carrier_invoice, conn,
                        ns(id=plain_invoice))
        assert is_error(r)
        assert r["message"] == (
            "Carrier 'NoSupplier Haulage' has no supplier_id. Link a supplier "
            "first via logistics-update-carrier --id {carrier_id} "
            "--supplier-id {supplier_id}")
        assert _snapshot(conn, self._VERIFY_TABLES) == before
        assert _audit_count(conn, "logistics-verify-carrier-invoice") == 0

    def test_verify_carrier_invoice_missing_supplier_guard_unreachable_finding(
            self, conn, env, mod):
        """FINDING (documented, deliberately not fixed): the
        "Supplier ... linked to carrier no longer exists" guard in
        verify-carrier-invoice cannot trigger through the seam, because
        logistics_carrier.supplier_id is a enforced foreign key to
        supplier.id: orphaning the carrier raises IntegrityError before the
        action ever runs. The action is correct to keep the guard as
        defence-in-depth; this test pins the real behaviour.
        """
        import sqlite3

        carrier_id = _mk_carrier(conn, mod, env["company_id"],
                                 name="Guarded Haulage",
                                 supplier_id=env["supplier_id"])
        invoice_id = _mk_invoice(conn, mod, env["company_id"], carrier_id,
                                 "1250.00", "CI-VRF-GUARD")
        before = _snapshot(conn, self._VERIFY_TABLES)

        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            conn.execute(
                update_row("logistics_carrier",
                           data={"supplier_id": P()}, where={"id": P()}),
                ("no-such-supplier", carrier_id),
            )
        conn.rollback()

        stored = self._stored_row(conn, invoice_id)
        assert stored["invoice_status"] == "pending"
        assert stored["total_amount"] == "1250.00"
        assert stored["purchase_invoice_id"] is None
        assert _snapshot(conn, self._VERIFY_TABLES) == before
        assert _audit_count(conn, "logistics-verify-carrier-invoice") == 0


class TestStatus:
    def test_status(self, conn, env, mod):
        r = call_action(mod.status, conn, ns())
        assert is_ok(r), r
        assert r["skill"] == "erpclaw-logistics"
        assert r["total_tables"] == 8
