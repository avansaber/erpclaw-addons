#!/usr/bin/env python3
"""ERPClaw Logistics schema extension -- adds logistics tables to the shared database.

8 tables: logistics_carrier, logistics_carrier_rate, logistics_shipment,
logistics_tracking_event, logistics_route, logistics_route_stop,
logistics_freight_charge, logistics_carrier_invoice.

Prerequisite: ERPClaw init_db.py must have run first (creates foundation tables).
Run: python3 init_db.py [db_path]

ADR-0034 phase 2 bulk-39. Schema declared as metadata and provisioned through
`erpclaw_lib.seam`, which emits dialect-correct DDL, replacing a hand-written
``CREATE TABLE`` block opened with ``sqlite3.connect`` that could not run on
PostgreSQL at all. Conversion rules are the pilot's (`erpclaw-esign`); the
shipment/rate/charge amount columns stay TEXT, as money does on every backend.
"""
import importlib.util
import os
import sys

# Bootstrap the shared lib only when it is not already reachable — an
# unconditional insert at position 0 overrides a caller that deliberately bound a
# different tree (ADR-0034 phase 2 step 2d).
if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, os.path.join(os.path.expanduser(
        os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))

from erpclaw_lib.seam import (  # noqa: E402
    CheckConstraint, Column, ForeignKey, Index, Integer, MetaData, Table, Text,
    provision, reference_table, text,
)

DEFAULT_DB_PATH = os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "data.sqlite")
DISPLAY_NAME = "ERPClaw Logistics"

REQUIRED_FOUNDATION = [
    "company", "naming_series", "audit_log", "supplier",
]

METADATA = MetaData()

# Foundation tables this module points at but does not own — declared so the
# foreign keys resolve, never created here.
reference_table("company", METADATA)
reference_table("supplier", METADATA)

# ---------------------------------------------------------------------------
# 1. logistics_carrier
# ---------------------------------------------------------------------------
CARRIER = Table(
    "logistics_carrier", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("carrier_code", Text),
    Column("supplier_id", Text, ForeignKey("supplier.id")),
    Column("contact_name", Text),
    Column("contact_email", Text),
    Column("contact_phone", Text),
    Column("dot_number", Text),
    Column("mc_number", Text),
    Column("carrier_type", Text, nullable=False, server_default=text("'parcel'")),
    Column("insurance_expiry", Text),
    Column("carrier_status", Text, nullable=False, server_default=text("'active'")),
    Column("on_time_pct", Text, nullable=False, server_default=text("'100'")),
    Column("total_shipments", Integer, nullable=False, server_default=text("0")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "carrier_type IN ('ltl','ftl','parcel','freight_forwarder','courier')",
        name="ck_logistics_carrier_carrier_type"),
    CheckConstraint("carrier_status IN ('active','inactive','suspended')",
                    name="ck_logistics_carrier_carrier_status"),
)

Index("idx_log_carrier_company", CARRIER.c.company_id)
Index("idx_log_carrier_status", CARRIER.c.carrier_status)
Index("idx_log_carrier_type", CARRIER.c.carrier_type)
Index("idx_log_carrier_supplier", CARRIER.c.supplier_id)

# ---------------------------------------------------------------------------
# 2. logistics_carrier_rate
# ---------------------------------------------------------------------------
CARRIER_RATE = Table(
    "logistics_carrier_rate", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("carrier_id", Text,
           ForeignKey("logistics_carrier.id", ondelete="CASCADE"), nullable=False),
    Column("origin_zone", Text),
    Column("destination_zone", Text),
    Column("service_level", Text, nullable=False, server_default=text("'ground'")),
    Column("weight_min", Text),
    Column("weight_max", Text),
    Column("rate_per_unit", Text),
    Column("flat_rate", Text),
    Column("effective_date", Text),
    Column("expiry_date", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "service_level IN ('ground','express','overnight','freight','ltl')",
        name="ck_logistics_carrier_rate_service_level"),
)

Index("idx_log_crate_carrier", CARRIER_RATE.c.carrier_id)
Index("idx_log_crate_company", CARRIER_RATE.c.company_id)
Index("idx_log_crate_service", CARRIER_RATE.c.service_level)

# ---------------------------------------------------------------------------
# 3. logistics_shipment
# ---------------------------------------------------------------------------
SHIPMENT = Table(
    "logistics_shipment", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("origin_address", Text),
    Column("origin_city", Text),
    Column("origin_state", Text),
    Column("origin_zip", Text),
    Column("destination_address", Text),
    Column("destination_city", Text),
    Column("destination_state", Text),
    Column("destination_zip", Text),
    Column("carrier_id", Text, ForeignKey("logistics_carrier.id")),
    Column("service_level", Text, nullable=False, server_default=text("'ground'")),
    Column("weight", Text),
    Column("dimensions", Text),
    Column("package_count", Integer, nullable=False, server_default=text("1")),
    Column("declared_value", Text),
    Column("reference_number", Text),
    Column("shipment_status", Text, nullable=False, server_default=text("'created'")),
    Column("estimated_delivery", Text),
    Column("actual_delivery", Text),
    Column("shipping_cost", Text),
    Column("tracking_number", Text),
    Column("pod_signature", Text),
    Column("pod_timestamp", Text),
    Column("notes", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "service_level IN ('ground','express','overnight','freight','ltl')",
        name="ck_logistics_shipment_service_level"),
    CheckConstraint(
        "shipment_status IN ('created','picked_up','in_transit','out_for_delivery',"
        "'delivered','exception','returned')",
        name="ck_logistics_shipment_shipment_status"),
)

Index("idx_log_ship_company", SHIPMENT.c.company_id)
Index("idx_log_ship_status", SHIPMENT.c.shipment_status)
Index("idx_log_ship_carrier", SHIPMENT.c.carrier_id)
Index("idx_log_ship_tracking", SHIPMENT.c.tracking_number)

# ---------------------------------------------------------------------------
# 4. logistics_tracking_event
# ---------------------------------------------------------------------------
TRACKING_EVENT = Table(
    "logistics_tracking_event", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("shipment_id", Text,
           ForeignKey("logistics_shipment.id", ondelete="CASCADE"), nullable=False),
    Column("event_timestamp", Text, nullable=False),
    Column("event_type", Text, nullable=False),
    Column("location", Text),
    Column("description", Text),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "event_type IN ('created','picked_up','departed','arrived','out_for_delivery',"
        "'delivered','exception','returned')",
        name="ck_logistics_tracking_event_event_type"),
)

Index("idx_log_track_shipment", TRACKING_EVENT.c.shipment_id)
Index("idx_log_track_type", TRACKING_EVENT.c.event_type)
Index("idx_log_track_company", TRACKING_EVENT.c.company_id)

# ---------------------------------------------------------------------------
# 5. logistics_route
# ---------------------------------------------------------------------------
ROUTE = Table(
    "logistics_route", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("name", Text, nullable=False),
    Column("origin", Text),
    Column("destination", Text),
    Column("distance", Text),
    Column("estimated_hours", Text),
    Column("route_status", Text, nullable=False, server_default=text("'active'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("route_status IN ('active','inactive')",
                    name="ck_logistics_route_route_status"),
)

Index("idx_log_route_company", ROUTE.c.company_id)
Index("idx_log_route_status", ROUTE.c.route_status)

# ---------------------------------------------------------------------------
# 6. logistics_route_stop
# ---------------------------------------------------------------------------
ROUTE_STOP = Table(
    "logistics_route_stop", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("route_id", Text,
           ForeignKey("logistics_route.id", ondelete="CASCADE"), nullable=False),
    Column("stop_order", Integer, nullable=False, server_default=text("1")),
    Column("address", Text),
    Column("city", Text),
    Column("state", Text),
    Column("zip_code", Text),
    Column("estimated_arrival", Text),
    Column("stop_type", Text, nullable=False, server_default=text("'delivery'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("stop_type IN ('pickup','delivery','transfer')",
                    name="ck_logistics_route_stop_stop_type"),
)

Index("idx_log_rstop_route", ROUTE_STOP.c.route_id)
Index("idx_log_rstop_company", ROUTE_STOP.c.company_id)

# ---------------------------------------------------------------------------
# 7. logistics_freight_charge
# ---------------------------------------------------------------------------
FREIGHT_CHARGE = Table(
    "logistics_freight_charge", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("shipment_id", Text,
           ForeignKey("logistics_shipment.id", ondelete="CASCADE"), nullable=False),
    Column("charge_type", Text, nullable=False, server_default=text("'base'")),
    Column("description", Text),
    Column("amount", Text, nullable=False, server_default=text("'0'")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "charge_type IN ('base','fuel_surcharge','accessorial','insurance',"
        "'handling','customs')",
        name="ck_logistics_freight_charge_charge_type"),
)

Index("idx_log_fcharge_shipment", FREIGHT_CHARGE.c.shipment_id)
Index("idx_log_fcharge_company", FREIGHT_CHARGE.c.company_id)

# ---------------------------------------------------------------------------
# 8. logistics_carrier_invoice
# ---------------------------------------------------------------------------
CARRIER_INVOICE = Table(
    "logistics_carrier_invoice", METADATA,
    Column("id", Text, primary_key=True, nullable=True),
    Column("naming_series", Text),
    Column("carrier_id", Text, ForeignKey("logistics_carrier.id"), nullable=False),
    Column("invoice_number", Text),
    Column("invoice_date", Text),
    Column("total_amount", Text, nullable=False, server_default=text("'0'")),
    Column("invoice_status", Text, nullable=False, server_default=text("'pending'")),
    # No foreign key on purchase_invoice_id in the shipped DDL — preserved as-is.
    Column("purchase_invoice_id", Text),
    Column("shipment_count", Integer, nullable=False, server_default=text("0")),
    Column("company_id", Text, ForeignKey("company.id"), nullable=False),
    Column("created_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", Text, nullable=False,
           server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint("invoice_status IN ('pending','verified','paid','disputed')",
                    name="ck_logistics_carrier_invoice_invoice_status"),
)

Index("idx_log_cinv_carrier", CARRIER_INVOICE.c.carrier_id)
Index("idx_log_cinv_company", CARRIER_INVOICE.c.company_id)
Index("idx_log_cinv_status", CARRIER_INVOICE.c.invoice_status)


def _require_foundation(db_path):
    """The pre-conversion installer's foundation probe, asked through the seam.

    The probed tables and the wording are this module's own, unchanged; only the
    mechanism changed. The original read SQLite's own catalog table directly,
    so the guard that exists to produce a friendly error was itself
    SQLite-only — on PostgreSQL it would have raised before it could explain
    anything, and ``seam.table_exists`` answers on both backends (ADR-0034
    bulk-39). This note names that catalog table in prose rather than as the
    identifier, because the seam-bypass ratchet counts string literals and an
    installers bucket that is only allowed to fall should not rise for a
    docstring.
    """
    from erpclaw_lib import seam

    missing = [t for t in REQUIRED_FOUNDATION if not seam.table_exists(t, db_path)]
    if missing:
        print(f"ERROR: Foundation tables missing: {', '.join(missing)}")
        print("Run erpclaw-setup first: clawhub install erpclaw-setup")
        sys.exit(1)


def create_logistics_tables(db_path=None):
    """Create logistics tables and indexes on whichever backend is configured.

    Same contract as before the ADR-0034 conversion: idempotent, and the returned
    counts are what was ACTUALLY created rather than what was declared.
    """
    db_path = db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    _require_foundation(db_path)
    result = provision(METADATA, db_path)
    return {
        "database": db_path,
        "tables": result["tables"],
        "indexes": result["indexes"],
    }


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else None
    result = create_logistics_tables(db)
    print(f"{DISPLAY_NAME} schema created in {result['database']}")
    print(f"  Tables: {result['tables']}")
    print(f"  Indexes: {result['indexes']}")
