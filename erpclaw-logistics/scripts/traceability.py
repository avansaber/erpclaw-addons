"""ERPClaw Logistics: traceability domain module

Read-only serial and batch lineage over foundation inventory records.
Imported by db_query.py (unified router).
"""
import os
import sys
from decimal import Decimal, InvalidOperation

try:
    import importlib.util
    if importlib.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.response import ok, err
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order
except ImportError:
    pass

SKILL = "erpclaw-logistics"

VALID_DIRECTIONS = ("forward", "backward")

_VOUCHER_TABLES = {
    "purchase_receipt": "purchase_receipt",
    "delivery_note": "delivery_note",
    "stock_entry": "stock_entry",
}


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field("id")).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _clean(value):
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    text = str(value).strip()
    return text if text else None


def _serial_matches(stored, wanted):
    if stored is None:
        return False
    text = str(stored)
    if text == wanted:
        return True
    parts = []
    for chunk in text.replace(",", "\n").replace(";", "\n").split("\n"):
        piece = chunk.strip()
        if piece:
            parts.append(piece)
    return wanted in parts


def _fetch_one(conn, table_name, column, value):
    t = Table(table_name)
    return conn.execute(
        Q.from_(t).select(t.star).where(Field(column) == P()).get_sql(),
        (value,),
    ).fetchone()


def _row_get(row, key):
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return None


def _qty_string(raw):
    text = "0" if raw is None else str(raw)
    try:
        Decimal(text)
    except (InvalidOperation, ValueError, TypeError):
        err(f"Stored quantity is not a Decimal string: {raw!r}")
    return text


def trace_inventory(conn, args):
    company_id = _clean(getattr(args, "company_id", None))
    _validate_company(conn, company_id)
    direction = _clean(getattr(args, "direction", None))
    if direction not in VALID_DIRECTIONS:
        err(f"Invalid --direction: {getattr(args, 'direction', None)}. Must be one of: forward, backward")
    serial_number = _clean(getattr(args, "serial_number", None))
    batch_number = _clean(getattr(args, "batch_number", None))
    has_serial = serial_number is not None
    has_batch = batch_number is not None
    if has_serial and has_batch:
        err("Provide exactly one of --serial-number or --batch-number, not both")
    if not has_serial and not has_batch:
        err("Exactly one of --serial-number or --batch-number is required")

    sle = Table("stock_ledger_entry")
    candidate_rows = []

    if has_serial:
        exact_q = (
            Q.from_(sle).select(sle.star)
            .where(Field("is_cancelled") == 0)
            .where(Field("serial_number") == P())
            .orderby(sle.posting_date, order=Order.asc)
            .orderby(sle.created_at, order=Order.asc)
            .orderby(sle.id, order=Order.asc)
        )
        exact_rows = conn.execute(exact_q.get_sql(), (serial_number,)).fetchall()
        like_q = (
            Q.from_(sle).select(sle.star)
            .where(Field("is_cancelled") == 0)
            .where(Field("serial_number").like(P()))
            .orderby(sle.posting_date, order=Order.asc)
            .orderby(sle.created_at, order=Order.asc)
            .orderby(sle.id, order=Order.asc)
        )
        like_rows = conn.execute(like_q.get_sql(), ("%" + serial_number + "%",)).fetchall()
        seen = set()
        for row in list(exact_rows) + list(like_rows):
            rid = _row_get(row, "id")
            if rid in seen:
                continue
            seen.add(rid)
            if _serial_matches(_row_get(row, "serial_number"), serial_number):
                candidate_rows.append(row)
    else:
        batch_ids = set()
        batch_ids.add(batch_number)
        btab = Table("batch")
        brow_q = (
            Q.from_(btab).select(btab.id, btab.batch_name)
            .where((Field("batch_name") == P()) | (Field("id") == P()))
        )
        for brow in conn.execute(brow_q.get_sql(), (batch_number, batch_number)).fetchall():
            bid = _row_get(brow, "id")
            if bid:
                batch_ids.add(str(bid))
        ordered_ids = sorted(batch_ids)
        crit = None
        for _bid in ordered_ids:
            piece = Field("batch_id") == P()
            crit = piece if crit is None else (crit | piece)
        batch_q = (
            Q.from_(sle).select(sle.star)
            .where(Field("is_cancelled") == 0)
            .where(crit)
            .orderby(sle.posting_date, order=Order.asc)
            .orderby(sle.created_at, order=Order.asc)
            .orderby(sle.id, order=Order.asc)
        )
        candidate_rows = list(conn.execute(batch_q.get_sql(), tuple(ordered_ids)).fetchall())

    nodes = []
    gaps = []

    for row in candidate_rows:
        voucher_type = _row_get(row, "voucher_type")
        voucher_id = _row_get(row, "voucher_id")
        item_id = _row_get(row, "item_id")
        warehouse_id = _row_get(row, "warehouse_id")
        posting_date = _row_get(row, "posting_date")
        created_at = _row_get(row, "created_at")
        sle_id = _row_get(row, "id")
        stored_serial = _row_get(row, "serial_number")
        stored_batch = _row_get(row, "batch_id")
        qty_raw = _row_get(row, "actual_qty")

        warehouse_row = _fetch_one(conn, "warehouse", "id", warehouse_id) if warehouse_id else None
        if warehouse_row is None:
            gaps.append({
                "reason": "missing_warehouse",
                "document_type": voucher_type,
                "document_id": voucher_id,
                "sle_id": sle_id,
                "warehouse_id": warehouse_id,
                "detail": f"Warehouse {warehouse_id} not found for {voucher_type} {voucher_id}",
            })
            continue
        if str(_row_get(warehouse_row, "company_id")) != str(company_id):
            continue

        parent_table = _VOUCHER_TABLES.get(voucher_type)
        parent_row = None
        if parent_table is not None:
            parent_row = _fetch_one(conn, parent_table, "id", voucher_id)
            if parent_row is None:
                gaps.append({
                    "reason": "missing_document",
                    "document_type": voucher_type,
                    "document_id": voucher_id,
                    "sle_id": sle_id,
                    "detail": f"{voucher_type} {voucher_id} not found",
                })
            elif str(_row_get(parent_row, "company_id")) != str(company_id):
                continue
        else:
            maybe_row = None
            try:
                maybe_row = _fetch_one(conn, voucher_type, "id", voucher_id)
            except Exception:
                maybe_row = None
            if maybe_row is not None:
                try:
                    parent_company = _row_get(maybe_row, "company_id")
                except Exception:
                    parent_company = None
                if parent_company is not None and str(parent_company) != str(company_id):
                    continue

        item_row = _fetch_one(conn, "item", "id", item_id) if item_id else None
        if item_row is None:
            gaps.append({
                "reason": "missing_item",
                "document_type": voucher_type,
                "document_id": voucher_id,
                "sle_id": sle_id,
                "item_id": item_id,
                "detail": f"Item {item_id} not found for {voucher_type} {voucher_id}",
            })
            continue

        qty_text = _qty_string(qty_raw)
        try:
            Decimal(qty_text)
        except (InvalidOperation, ValueError):
            err(f"Stored quantity is not a Decimal string: {qty_raw!r}")

        nodes.append({
            "document_type": voucher_type,
            "document_id": voucher_id,
            "posting_date": posting_date,
            "created_at": created_at,
            "item_id": item_id,
            "warehouse_id": warehouse_id,
            "quantity": qty_text,
            "serial_number": stored_serial,
            "batch_number": stored_batch,
            "batch_id": stored_batch,
            "sle_id": sle_id,
        })

    nodes.sort(key=lambda n: (
        n["posting_date"] or "",
        n["created_at"] or "",
        n["sle_id"] or "",
        n["document_id"] or "",
    ))
    if direction == "backward":
        nodes = list(reversed(nodes))

    edges = []
    for idx in range(len(nodes) - 1):
        src = nodes[idx]
        dst = nodes[idx + 1]
        edges.append({
            "sequence": idx + 1,
            "from_index": idx,
            "to_index": idx + 1,
            "from_document_type": src["document_type"],
            "from_document_id": src["document_id"],
            "to_document_type": dst["document_type"],
            "to_document_id": dst["document_id"],
        })

    gaps.sort(key=lambda g: (
        str(g.get("document_type") or ""),
        str(g.get("document_id") or ""),
        str(g.get("reason") or ""),
    ))

    ok({
        "report": "inventory-trace",
        "company_id": company_id,
        "direction": direction,
        "serial_number": serial_number,
        "batch_number": batch_number,
        "nodes": nodes,
        "edges": edges,
        "gaps": gaps,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "gap_count": len(gaps),
    })


ACTIONS = {
    "logistics-trace-inventory": trace_inventory,
}
