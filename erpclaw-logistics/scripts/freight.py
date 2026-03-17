"""ERPClaw Logistics -- freight domain module

Actions for freight charges, carrier invoices, and freight allocation.
Imported by db_query.py (unified router).
"""
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.cross_skill import create_purchase_invoice, CrossSkillError
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row, dynamic_update, now
    from erpclaw_lib.vendor.pypika.terms import LiteralValue

    ENTITY_PREFIXES.setdefault("logistics_carrier_invoice", "CINV-")
except ImportError:
    pass

SKILL = "erpclaw-logistics"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_CHARGE_TYPES = ("base", "fuel_surcharge", "accessorial", "insurance", "handling", "customs")
VALID_INVOICE_STATUSES = ("pending", "verified", "paid", "disputed")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _validate_enum(value, valid_values, field_name):
    if value and value not in valid_values:
        err(f"Invalid {field_name}: {value}. Must be one of: {', '.join(valid_values)}")


# ===========================================================================
# 1. add-freight-charge
# ===========================================================================
def add_freight_charge(conn, args):
    shipment_id = getattr(args, "shipment_id", None)
    if not shipment_id:
        err("--shipment-id is required")
    if not conn.execute(Q.from_(Table("logistics_shipment")).select(Field('id')).where(Field("id") == P()).get_sql(), (shipment_id,)).fetchone():
        err(f"Shipment {shipment_id} not found")

    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    charge_type = getattr(args, "charge_type", None) or "base"
    _validate_enum(charge_type, VALID_CHARGE_TYPES, "charge-type")

    amount = getattr(args, "amount", None) or "0"
    # Validate Decimal
    try:
        Decimal(amount)
    except Exception:
        err(f"Invalid amount: {amount}")

    charge_id = str(uuid.uuid4())

    sql, _ = insert_row("logistics_freight_charge", {
        "id": P(), "shipment_id": P(), "charge_type": P(), "description": P(),
        "amount": P(), "company_id": P(), "created_at": P(),
    })
    conn.execute(sql, (
        charge_id, shipment_id, charge_type,
        getattr(args, "description", None),
        amount, company_id, _now_iso(),
    ))
    audit(conn, SKILL, "logistics-add-freight-charge", "logistics_freight_charge", charge_id,
          new_values={"shipment_id": shipment_id, "charge_type": charge_type, "amount": amount})
    conn.commit()
    ok({
        "id": charge_id, "shipment_id": shipment_id,
        "charge_type": charge_type, "amount": amount,
    })


# ===========================================================================
# 2. list-freight-charges
# ===========================================================================
def list_freight_charges(conn, args):
    t = Table("logistics_freight_charge")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star).as_("cnt"))
    params = []

    shipment_id = getattr(args, "shipment_id", None)
    if shipment_id:
        q = q.where(t.shipment_id == P())
        q_cnt = q_cnt.where(t.shipment_id == P())
        params.append(shipment_id)
    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "charge_type", None):
        q = q.where(t.charge_type == P())
        q_cnt = q_cnt.where(t.charge_type == P())
        params.append(args.charge_type)

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]
    q = q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [args.limit, args.offset]).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 3. allocate-freight
# ===========================================================================
def allocate_freight(conn, args):
    shipment_id = getattr(args, "shipment_id", None)
    if not shipment_id:
        err("--shipment-id is required")
    ship = conn.execute(Q.from_(Table("logistics_shipment")).select(Table("logistics_shipment").star).where(Field("id") == P()).get_sql(), (shipment_id,)).fetchone()
    if not ship:
        err(f"Shipment {shipment_id} not found")

    # Sum all freight charges for this shipment
    charges = conn.execute(Q.from_(Table("logistics_freight_charge")).select(Field('charge_type'), Field('amount')).where(Field("shipment_id") == P()).get_sql(), (shipment_id,)).fetchall()
    total = sum(Decimal(c[1] or "0") for c in charges)

    # Update shipment shipping_cost with total freight
    sql, upd_params = dynamic_update("logistics_shipment",
        {"shipping_cost": str(total), "updated_at": now()},
        {"id": shipment_id})
    conn.execute(sql, upd_params)
    audit(conn, SKILL, "logistics-allocate-freight", "logistics_shipment", shipment_id,
          new_values={"shipping_cost": str(total), "charge_count": len(charges)})
    conn.commit()
    ok({
        "shipment_id": shipment_id,
        "total_freight": str(total),
        "charge_count": len(charges),
        "charges": [{"charge_type": c[0], "amount": c[1]} for c in charges],
    })


# ===========================================================================
# 4. add-carrier-invoice
# ===========================================================================
def add_carrier_invoice(conn, args):
    carrier_id = getattr(args, "carrier_id", None)
    if not carrier_id:
        err("--carrier-id is required")
    if not conn.execute(Q.from_(Table("logistics_carrier")).select(Field('id')).where(Field("id") == P()).get_sql(), (carrier_id,)).fetchone():
        err(f"Carrier {carrier_id} not found")

    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    total_amount = getattr(args, "total_amount", None) or "0"
    try:
        Decimal(total_amount)
    except Exception:
        err(f"Invalid total-amount: {total_amount}")

    invoice_id = str(uuid.uuid4())
    conn.company_id = company_id
    naming = get_next_name(conn, "logistics_carrier_invoice", company_id=company_id)
    _ts = _now_iso()

    sql, _ = insert_row("logistics_carrier_invoice", {
        "id": P(), "naming_series": P(), "carrier_id": P(), "invoice_number": P(),
        "invoice_date": P(), "total_amount": P(), "invoice_status": P(),
        "shipment_count": P(), "company_id": P(), "created_at": P(), "updated_at": P(),
    })
    conn.execute(sql, (
        invoice_id, naming, carrier_id,
        getattr(args, "invoice_number", None),
        getattr(args, "invoice_date", None),
        total_amount, "pending",
        int(getattr(args, "shipment_count", None) or 0),
        company_id, _ts, _ts,
    ))
    audit(conn, SKILL, "logistics-add-carrier-invoice", "logistics_carrier_invoice", invoice_id,
          new_values={"carrier_id": carrier_id, "total_amount": total_amount})
    conn.commit()
    ok({
        "id": invoice_id, "naming_series": naming,
        "carrier_id": carrier_id, "invoice_status": "pending",
        "total_amount": total_amount,
    })


# ===========================================================================
# 5. list-carrier-invoices
# ===========================================================================
def list_carrier_invoices(conn, args):
    t = Table("logistics_carrier_invoice")
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count(t.star).as_("cnt"))
    params = []

    if getattr(args, "carrier_id", None):
        q = q.where(t.carrier_id == P())
        q_cnt = q_cnt.where(t.carrier_id == P())
        params.append(args.carrier_id)
    if getattr(args, "company_id", None):
        q = q.where(t.company_id == P())
        q_cnt = q_cnt.where(t.company_id == P())
        params.append(args.company_id)
    if getattr(args, "invoice_status", None):
        q = q.where(t.invoice_status == P())
        q_cnt = q_cnt.where(t.invoice_status == P())
        params.append(args.invoice_status)

    total = conn.execute(q_cnt.get_sql(), params).fetchone()[0]
    q = q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [args.limit, args.offset]).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 6. verify-carrier-invoice
# ===========================================================================
def verify_carrier_invoice(conn, args):
    """Verify a carrier invoice and create a purchase invoice via cross_skill.

    Transitions invoice_status from 'pending' to 'verified' and calls
    erpclaw-buying to create a real purchase_invoice (carriers are suppliers).
    The carrier must have a supplier_id linked for the PI to be created.
    """
    invoice_id = getattr(args, "id", None)
    if not invoice_id:
        err("--id is required (carrier invoice ID)")

    row = conn.execute(Q.from_(Table("logistics_carrier_invoice")).select(Table("logistics_carrier_invoice").star).where(Field("id") == P()).get_sql(), (invoice_id,)).fetchone()
    if not row:
        err(f"Carrier invoice {invoice_id} not found")

    inv = row_to_dict(row)

    if inv["invoice_status"] != "pending":
        err(f"Cannot verify carrier invoice: status is '{inv['invoice_status']}' (must be 'pending')")

    # Look up carrier to get supplier_id
    carrier = conn.execute(Q.from_(Table("logistics_carrier")).select(Table("logistics_carrier").star).where(Field("id") == P()).get_sql(), (inv["carrier_id"],)).fetchone()
    if not carrier:
        err(f"Carrier {inv['carrier_id']} not found")

    carrier_data = row_to_dict(carrier)
    supplier_id = carrier_data.get("supplier_id")
    if not supplier_id:
        err(
            f"Carrier '{carrier_data['name']}' has no supplier_id. "
            "Link a supplier first via logistics-update-carrier --id {carrier_id} --supplier-id {supplier_id}"
        )

    # Validate supplier still exists
    if not conn.execute(Q.from_(Table("supplier")).select(Field('id')).where(Field("id") == P()).get_sql(), (supplier_id,)).fetchone():
        err(f"Supplier {supplier_id} linked to carrier no longer exists")

    # Build description for the PI line item
    inv_number = inv.get("invoice_number") or inv["id"][:8]
    total_amount = inv["total_amount"]
    company_id = inv["company_id"]

    # Get db_path from connection (for cross_skill subprocess)
    db_path = getattr(args, "db_path", None)

    # Create purchase invoice via cross_skill
    try:
        pi_result = create_purchase_invoice(
            supplier_id=supplier_id,
            items=[{
                "description": f"Carrier Invoice {inv_number} - {carrier_data['name']}",
                "qty": "1",
                "rate": str(total_amount),
            }],
            company_id=company_id,
            posting_date=inv.get("invoice_date"),
            remarks=f"Auto-created from logistics carrier invoice {inv_number}",
            db_path=db_path,
        )
    except CrossSkillError as e:
        err(f"Failed to create purchase invoice: {e}")

    # Extract purchase_invoice_id from the buying skill response
    # The response structure depends on erpclaw-buying's add-purchase-invoice action
    pi_data = pi_result.get("purchase_invoice") or pi_result.get("data") or pi_result
    purchase_invoice_id = (
        pi_data.get("id")
        if isinstance(pi_data, dict)
        else pi_result.get("id")
    )

    if not purchase_invoice_id:
        err(f"Purchase invoice created but could not extract ID from response: {pi_result}")

    # Update carrier invoice: set status to verified and store PI link
    _ts = _now_iso()
    sql = update_row("logistics_carrier_invoice",
        data={"invoice_status": P(), "purchase_invoice_id": P(), "updated_at": P()},
        where={"id": P()})
    conn.execute(sql, ("verified", purchase_invoice_id, _ts, invoice_id))
    audit(conn, SKILL, "logistics-verify-carrier-invoice", "logistics_carrier_invoice", invoice_id,
          new_values={
              "invoice_status": "verified",
              "purchase_invoice_id": purchase_invoice_id,
              "supplier_id": supplier_id,
          })
    conn.commit()

    ok({
        "id": invoice_id,
        "invoice_status": "verified",
        "purchase_invoice_id": purchase_invoice_id,
        "supplier_id": supplier_id,
        "carrier_id": inv["carrier_id"],
        "total_amount": total_amount,
    })


# ===========================================================================
# 7. freight-cost-analysis-report
# ===========================================================================
def freight_cost_analysis_report(conn, args):
    company_id = getattr(args, "company_id", None)
    _validate_company(conn, company_id)

    # Total freight charges by type
    # PyPika: skipped — CAST(amount AS NUMERIC) aggregate
    by_type = {}
    t_fc = Table("logistics_freight_charge")
    rows = conn.execute(
        Q.from_(t_fc).select(
            t_fc.charge_type, fn.Count(t_fc.star).as_("cnt"),
            LiteralValue("SUM(CAST(amount AS NUMERIC))").as_("total")
        ).where(t_fc.company_id == P()).groupby(t_fc.charge_type).get_sql(),
        (company_id,)
    ).fetchall()
    for r in rows:
        by_type[r[0]] = {"count": r[1], "total": str(round(Decimal(str(r[2] or 0)), 2))}

    # Total carrier invoices
    t_ci = Table("logistics_carrier_invoice")
    invoice_total = conn.execute(
        Q.from_(t_ci).select(
            fn.Count(t_ci.star),
            LiteralValue("SUM(CAST(total_amount AS NUMERIC))")
        ).where(t_ci.company_id == P()).get_sql(),
        (company_id,)
    ).fetchone()

    # Total shipment shipping costs
    t_ship = Table("logistics_shipment")
    ship_total = conn.execute(
        Q.from_(t_ship).select(
            fn.Count(t_ship.star),
            LiteralValue("SUM(CAST(shipping_cost AS NUMERIC))")
        ).where(t_ship.company_id == P()).where(t_ship.shipping_cost.isnotnull()).get_sql(),
        (company_id,)
    ).fetchone()

    ok({
        "report": "freight-cost-analysis",
        "company_id": company_id,
        "charges_by_type": by_type,
        "total_carrier_invoices": invoice_total[0] or 0,
        "total_invoice_amount": str(round(Decimal(str(invoice_total[1] or 0)), 2)),
        "total_shipments_with_cost": ship_total[0] or 0,
        "total_shipping_cost": str(round(Decimal(str(ship_total[1] or 0)), 2)),
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "logistics-add-freight-charge": add_freight_charge,
    "logistics-list-freight-charges": list_freight_charges,
    "logistics-allocate-freight": allocate_freight,
    "logistics-add-carrier-invoice": add_carrier_invoice,
    "logistics-list-carrier-invoices": list_carrier_invoices,
    "logistics-verify-carrier-invoice": verify_carrier_invoice,
    "logistics-freight-cost-analysis-report": freight_cost_analysis_report,
}
