"""ERPClaw Documents -- Print convenience wrappers (S8 chunk 4).

Three actions that load a bundled Jinja2 template, build merge data from a
*submitted* parent document, and delegate to ``document-print-document``.

  document-print-invoice         --invoice-id I [--template-id T] [--output-path P]
  document-print-purchase-order  --po-id PO    [--template-id T] [--output-path P]
  document-print-packing-slip    --slip-id S   [--template-id T] [--output-path P]

All three:
  - Reject ``draft`` parents — the plan spec mandates ``submitted`` status only.
  - Fall back to the appropriate bundled seed template when ``--template-id`` is
    omitted (looked up from ``document_template`` by ``template_type`` + name).
  - Build a merge-data dict from the parent document + child rows, then call
    ``print_document()`` from the sibling ``print_docs`` module.

Bundled templates live at  ../templates/default_{invoice,purchase_order,packing_slip}.html.j2
The shared CSS is at       ../templates/default.css

The templates reference ``{% include_raw_css %}`` which is NOT a real Jinja2 tag.
We replace that literal string with the inline CSS content before handing the
body to Jinja2, keeping the sandbox intact (no filesystem access at render time).

Imported by db_query.py (unified router) as WRAPPER_ACTIONS.
"""
import json
import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.response import ok, err
from erpclaw_lib.query import Q, P, Table, Field

# Reuse the composite print action from print_docs.
import print_docs as _print_docs

SKILL = "erpclaw-documents"

# ---------------------------------------------------------------------------
# Bundled template assets
# ---------------------------------------------------------------------------
_TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
_CSS_PATH = os.path.join(_TEMPLATES_DIR, "default.css")
_BUNDLED = {
    "invoice":        os.path.join(_TEMPLATES_DIR, "default_invoice.html.j2"),
    "purchase_order": os.path.join(_TEMPLATES_DIR, "default_purchase_order.html.j2"),
    "packing_slip":   os.path.join(_TEMPLATES_DIR, "default_packing_slip.html.j2"),
}
# Map template_type to the user-visible name used as seed key.
_SEED_NAMES = {
    "invoice":        "Default Invoice",
    "purchase_order": "Default Purchase Order",
    "packing_slip":   "Default Packing Slip",
}
# The document_template.template_type enum is constrained to the values below.
# purchase_order and packing_slip don't have dedicated slots; we store them
# under 'general' and use the name as the discriminator.
_SEED_TEMPLATE_TYPES = {
    "invoice":        "invoice",
    "purchase_order": "general",
    "packing_slip":   "general",
}


def _load_bundled_template(key):
    """Load the raw Jinja2 body for a bundled template and inline the CSS.

    ``{% include_raw_css %}`` is a pseudo-tag we embed in the .html.j2 files
    to keep the template files readable. We replace it with the literal CSS
    content before handing the body to Jinja2, so the sandbox never touches the
    filesystem at render time. Returns the template body string or err()s."""
    path = _BUNDLED.get(key)
    if not path or not os.path.isfile(path):
        err(f"Bundled template not found for type '{key}'. Reinstall erpclaw-documents.")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            body = fh.read()
    except OSError as e:
        err(f"Could not read bundled template: {e}")
    # Inline the shared CSS.
    css = ""
    if os.path.isfile(_CSS_PATH):
        try:
            with open(_CSS_PATH, "r", encoding="utf-8") as fh:
                css = fh.read()
        except OSError:
            pass
    return body.replace("{%  include_raw_css  %}", css).replace("{% include_raw_css %}", css)


def _resolve_template(conn, args, doc_type, company_id):
    """Return a (template_id, body) pair.

    If ``--template-id`` is supplied, fetch it and return its stored body. If
    omitted, look for the seed template (name = ``_SEED_NAMES[doc_type]``) in
    ``document_template`` and use its content; fall back to the bundled file
    body if no DB row is found (graceful for fresh installs where seed has not
    run yet). Errors if the resolved template is inactive."""
    explicit = getattr(args, "template_id", None)
    if explicit:
        row = conn.execute(
            Q.from_(Table("document_template")).select(Table("document_template").star)
            .where(Field("id") == P()).get_sql(),
            (explicit,),
        ).fetchone()
        if not row:
            err(f"Template {explicit} not found")
        if not row["is_active"]:
            err(f"Template {explicit} is inactive")
        return row["id"], row["content"]

    # Try the seed row first (look up by name, not template_type, because PO/slip
    # are stored as 'general' template_type — the name is the discriminator).
    seed_name = _SEED_NAMES[doc_type]
    row = conn.execute(
        "SELECT id, content, is_active FROM document_template "
        "WHERE name = ? AND company_id = ? LIMIT 1",
        (seed_name, company_id),
    ).fetchone()
    if row:
        if not row["is_active"]:
            err(f"Default {doc_type} template is inactive. Pass --template-id to override.")
        return row["id"], row["content"]

    # No DB row yet — use the bundled file (fresh install before seed-defaults ran).
    body = _load_bundled_template(doc_type)
    return None, body


def _format_decimal(val):
    """Format a TEXT decimal for display (strip trailing zeros, keep 2dp min)."""
    if val is None:
        return "0.00"
    try:
        from decimal import Decimal
        d = Decimal(str(val))
        formatted = f"{d:,.2f}"
        return formatted
    except Exception:
        return str(val)


# ---------------------------------------------------------------------------
# print-invoice
# ---------------------------------------------------------------------------
def print_invoice(conn, args):
    """Build merge data from ``sales_invoice`` + line items, then print to PDF."""
    inv_id = getattr(args, "invoice_id", None)
    if not inv_id:
        err("--invoice-id is required")

    # Fetch the invoice; only submitted invoices may be printed.
    inv = conn.execute(
        "SELECT si.*, c.name AS customer_name, co.name AS company_name, co.tax_id AS company_tax_id "
        "FROM sales_invoice si "
        "JOIN customer c ON c.id = si.customer_id "
        "JOIN company co ON co.id = si.company_id "
        "WHERE si.id = ?", (inv_id,)
    ).fetchone()
    if not inv:
        err(f"Invoice {inv_id} not found")
    if inv["status"] == "draft":
        err(f"Invoice {inv_id} is still in draft — submit it first before printing")

    # Line items
    lines = conn.execute(
        "SELECT sii.*, i.item_code AS item_code FROM sales_invoice_item sii "
        "LEFT JOIN item i ON i.id = sii.item_id "
        "WHERE sii.sales_invoice_id = ? ORDER BY sii.id",
        (inv_id,),
    ).fetchall()

    merge = {
        "naming_series": inv["naming_series"] or "",
        "company_name": inv["company_name"],
        "company_tax_id": inv["company_tax_id"] or "",
        "customer_name": inv["customer_name"],
        "customer_address": "",
        "posting_date": inv["posting_date"],
        "due_date": inv["due_date"] or "",
        "currency": inv["currency"],
        "status": inv["status"],
        "total_amount": _format_decimal(inv["total_amount"]),
        "tax_amount": _format_decimal(inv["tax_amount"]),
        "grand_total": _format_decimal(inv["grand_total"]),
        "rounding_adjustment": _format_decimal(inv["rounding_adjustment"]),
        "notes": "",
        "items": [
            {
                "item_id": r["item_id"],
                "item_code": r["item_code"] or r["item_id"],
                "description": "",
                "quantity": _format_decimal(r["quantity"]),
                "uom": r["uom"] or "",
                "rate": _format_decimal(r["rate"]),
                "amount": _format_decimal(r["amount"]),
            }
            for r in lines
        ],
    }

    company_id = inv["company_id"]
    tpl_id, tpl_body = _resolve_template(conn, args, "invoice", company_id)

    title = getattr(args, "title", None) or f"Invoice {inv['naming_series'] or inv_id}"
    _call_print(conn, args, tpl_id, tpl_body, title, company_id, merge, "invoice")


# ---------------------------------------------------------------------------
# print-purchase-order
# ---------------------------------------------------------------------------
def print_purchase_order(conn, args):
    """Build merge data from ``purchase_order`` + line items, then print to PDF."""
    po_id = getattr(args, "po_id", None)
    if not po_id:
        err("--po-id is required")

    po = conn.execute(
        "SELECT po.*, s.name AS supplier_name, co.name AS company_name, co.tax_id AS company_tax_id "
        "FROM purchase_order po "
        "JOIN supplier s ON s.id = po.supplier_id "
        "JOIN company co ON co.id = po.company_id "
        "WHERE po.id = ?", (po_id,)
    ).fetchone()
    if not po:
        err(f"Purchase order {po_id} not found")
    if po["status"] == "draft":
        err(f"Purchase order {po_id} is still in draft — confirm it first before printing")

    lines = conn.execute(
        "SELECT poi.*, i.item_code AS item_code FROM purchase_order_item poi "
        "LEFT JOIN item i ON i.id = poi.item_id "
        "WHERE poi.purchase_order_id = ? ORDER BY poi.id",
        (po_id,),
    ).fetchall()

    merge = {
        "naming_series": po["naming_series"] or "",
        "company_name": po["company_name"],
        "company_tax_id": po["company_tax_id"] or "",
        "supplier_name": po["supplier_name"],
        "supplier_address": po["primary_address"] if "primary_address" in po.keys() else "",
        "delivery_address": po["delivery_address"] or "",
        "order_date": po["order_date"],
        "required_date": po["required_date"] or "",
        "currency": po["currency"],
        "status": po["status"],
        "total_amount": _format_decimal(po["total_amount"]),
        "tax_amount": _format_decimal(po["tax_amount"]),
        "grand_total": _format_decimal(po["grand_total"]),
        "notes": "",
        "items": [
            {
                "item_id": r["item_id"],
                "item_code": r["item_code"] or r["item_id"],
                "quantity": _format_decimal(r["quantity"]),
                "uom": r["uom"] or "",
                "rate": _format_decimal(r["rate"]),
                "amount": _format_decimal(r["amount"]),
                "required_date": r["required_date"] or "",
            }
            for r in lines
        ],
    }

    company_id = po["company_id"]
    tpl_id, tpl_body = _resolve_template(conn, args, "purchase_order", company_id)
    title = getattr(args, "title", None) or f"PO {po['naming_series'] or po_id}"
    _call_print(conn, args, tpl_id, tpl_body, title, company_id, merge, "purchase_order")


# ---------------------------------------------------------------------------
# print-packing-slip
# ---------------------------------------------------------------------------
def print_packing_slip(conn, args):
    """Build merge data from ``packing_slip`` + items, then print to PDF."""
    slip_id = getattr(args, "slip_id", None)
    if not slip_id:
        err("--slip-id is required")

    slip = conn.execute(
        "SELECT ps.*, dn.status AS dn_status, c.name AS customer_name, "
        "co.name AS company_name "
        "FROM packing_slip ps "
        "JOIN delivery_note dn ON dn.id = ps.delivery_note_id "
        "LEFT JOIN customer c ON c.id = dn.customer_id "
        "JOIN company co ON co.id = ps.company_id "
        "WHERE ps.id = ?", (slip_id,)
    ).fetchone()
    if not slip:
        err(f"Packing slip {slip_id} not found")
    if slip["dn_status"] == "draft":
        err(f"Delivery note for slip {slip_id} is still in draft — submit it first")

    items = conn.execute(
        "SELECT psi.*, i.item_code AS item_code FROM packing_slip_item psi "
        "LEFT JOIN item i ON i.id = psi.item_id "
        "WHERE psi.packing_slip_id = ? ORDER BY psi.id",
        (slip_id,),
    ).fetchall()

    merge = {
        "slip_id": slip_id,
        "company_name": slip["company_name"],
        "customer_name": slip["customer_name"] or "",
        "shipping_address": "",
        "delivery_note_id": slip["delivery_note_id"],
        "posting_date": slip["posting_date"],
        "notes": slip["notes"] or "",
        "items": [
            {
                "item_id": r["item_id"],
                "item_code": r["item_code"] or r["item_id"],
                "qty_packed": _format_decimal(r["qty_packed"]),
                "uom": r["uom"] or "",
                "notes": r["notes"] or "",
            }
            for r in items
        ],
    }

    company_id = slip["company_id"]
    tpl_id, tpl_body = _resolve_template(conn, args, "packing_slip", company_id)
    title = getattr(args, "title", None) or f"Packing Slip {slip_id}"
    _call_print(conn, args, tpl_id, tpl_body, title, company_id, merge, "packing_slip")


# ---------------------------------------------------------------------------
# Shared call-through to print_document
# ---------------------------------------------------------------------------
def _call_print(conn, args, tpl_id, tpl_body, title, company_id, merge, doc_type):
    """Upsert a scratch template row if needed and delegate to print_document.

    When the wrappers resolved a DB template row, ``tpl_id`` is already set and
    we pass it directly. When they fell back to the bundled file body (fresh
    install), we insert a transient in-memory template row, run print_document,
    and then remove the scratch row — keeping the DB clean while still reusing
    the entire render + persist pipeline."""
    import argparse

    scratch_id = None
    if tpl_id is None:
        # No DB row yet: insert a scratch template so print_document can fetch it.
        scratch_id = str(uuid.uuid4())
        db_type = _SEED_TEMPLATE_TYPES.get(doc_type, "general")
        conn.execute(
            """INSERT INTO document_template
               (id, name, template_type, content, format, engine, is_active, company_id)
               VALUES (?,?,?,?,?,?,1,?)""",
            (scratch_id, f"_scratch_{doc_type}", db_type, tpl_body, "html", "jinja2", company_id),
        )
        conn.commit()
        tpl_id = scratch_id

    print_args = argparse.Namespace(
        template_id=tpl_id,
        title=title,
        company_id=company_id,
        merge_data=json.dumps(merge),
        format=None,  # use the template's stored format (html)
        owner=getattr(args, "owner", None),
        output_path=getattr(args, "output_path", None),
        max_html_bytes=getattr(args, "max_html_bytes", None),
    )

    try:
        _print_docs.print_document(conn, print_args)
    finally:
        if scratch_id:
            conn.execute("DELETE FROM document_template WHERE id = ?", (scratch_id,))
            conn.commit()


# ---------------------------------------------------------------------------
# seed-defaults (idempotent install hook)
# ---------------------------------------------------------------------------
def seed_default_templates(conn, args):
    """Seed the 3 bundled templates into ``document_template`` for a company.

    Idempotent: skips any template whose (company_id, name, template_type)
    already exists. Requires ``--company-id``. Used by the install hook and
    the ``erpclaw-documents`` init flow.
    """
    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required for seed-document-defaults")
    if not conn.execute(
        Q.from_(Table("company")).select(Field("id")).where(Field("id") == P()).get_sql(),
        (company_id,),
    ).fetchone():
        err(f"Company {company_id} not found")

    seeded = []
    for doc_type, tpl_name in _SEED_NAMES.items():
        db_type = _SEED_TEMPLATE_TYPES[doc_type]
        exists = conn.execute(
            "SELECT id FROM document_template WHERE company_id=? AND name=? AND template_type=? LIMIT 1",
            (company_id, tpl_name, db_type),
        ).fetchone()
        if exists:
            seeded.append({"name": tpl_name, "action": "skipped", "template_id": exists["id"]})
            continue
        body = _load_bundled_template(doc_type)
        tpl_id = str(uuid.uuid4())
        conn.execute(
            """INSERT INTO document_template
               (id, name, template_type, content, format, engine, is_active, company_id)
               VALUES (?,?,?,?,?,?,1,?)""",
            (tpl_id, tpl_name, db_type, body, "html", "jinja2", company_id),
        )
        seeded.append({"name": tpl_name, "action": "seeded", "template_id": tpl_id})
    conn.commit()
    ok({"seeded": seeded})


# ---------------------------------------------------------------------------
# ACTIONS registry
# ---------------------------------------------------------------------------
WRAPPER_ACTIONS = {
    "document-print-invoice":        print_invoice,
    "document-print-purchase-order": print_purchase_order,
    "document-print-packing-slip":   print_packing_slip,
    "document-seed-defaults":        seed_default_templates,
}
