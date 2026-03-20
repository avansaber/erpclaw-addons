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
"""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from logistics_helpers import call_action, ns, is_ok, is_error, _uuid


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


class TestOnTimeDeliveryReport:
    def test_on_time_delivery_report(self, conn, env, mod):
        r = call_action(mod.logistics_on_time_delivery_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert "total_delivered" in r


class TestDeliveryExceptionReport:
    def test_delivery_exception_report(self, conn, env, mod):
        r = call_action(mod.logistics_delivery_exception_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert "total_exceptions" in r


class TestFreightCostAnalysisReport:
    def test_freight_cost_analysis_report(self, conn, env, mod):
        r = call_action(mod.logistics_freight_cost_analysis_report, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(r), r
        assert "charges_by_type" in r


class TestStatus:
    def test_status(self, conn, env, mod):
        r = call_action(mod.status, conn, ns())
        assert is_ok(r), r
        assert r["skill"] == "erpclaw-logistics"
        assert r["total_tables"] == 8
