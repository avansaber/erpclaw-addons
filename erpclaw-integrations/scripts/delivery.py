"""ERPClaw Connectors V2 -- delivery domain module

Actions for delivery platform connectors (2 tables, 8 actions).
Supports DoorDash, UberEats, Grubhub, Postmates integrations.
Imported by db_query.py (unified router).
"""
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row

    ENTITY_PREFIXES.setdefault("connv2_delivery_connector", "DLC-")
except ImportError:
    pass

SKILL = "erpclaw-connectors-v2"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_PLATFORMS = ("doordash", "ubereats", "grubhub", "postmates")
VALID_ORDER_STATUSES = ("received", "confirmed", "preparing", "ready", "picked_up", "delivered", "cancelled")
VALID_CONNECTOR_STATUSES = ("active", "inactive", "error")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field("id")).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _get_connector(conn, connector_id):
    if not connector_id:
        err("--connector-id is required")
    row = conn.execute(Q.from_(Table("connv2_delivery_connector")).select(Table("connv2_delivery_connector").star).where(Field("id") == P()).get_sql(), (connector_id,)).fetchone()
    if not row:
        err(f"Delivery connector {connector_id} not found")
    return row


# ===========================================================================
# 1. add-delivery-connector
# ===========================================================================
def add_delivery_connector(conn, args):
    _validate_company(conn, args.company_id)
    platform = getattr(args, "platform", None)
    if not platform:
        err("--platform is required")
    if platform not in VALID_PLATFORMS:
        err(f"Invalid platform: {platform}. Must be one of: {', '.join(VALID_PLATFORMS)}")

    conn_id = str(uuid.uuid4())
    now = _now_iso()
    conn.company_id = args.company_id
    naming = get_next_name(conn, "connv2_delivery_connector")

    sql, _ = insert_row("connv2_delivery_connector", {"id": P(), "naming_series": P(), "platform": P(), "store_id": P(), "api_credentials_ref": P(), "auto_accept": P(), "sync_menu": P(), "connector_status": P(), "company_id": P(), "created_at": P(), "updated_at": P()})
    conn.execute(sql, (
        conn_id, naming, platform,
        getattr(args, "store_id", None),
        getattr(args, "api_credentials_ref", None),
        1 if getattr(args, "auto_accept", None) == "1" else 0,
        1 if getattr(args, "sync_menu", None) != "0" else 0,
        "inactive",
        args.company_id, now, now,
    ))
    audit(conn, SKILL, "integration-add-delivery-connector", "connv2_delivery_connector", conn_id,
          new_values={"platform": platform})
    conn.commit()
    ok({"id": conn_id, "naming_series": naming, "platform": platform,
        "connector_status": "inactive"})


# ===========================================================================
# 2. configure-delivery-sync
# ===========================================================================
def configure_delivery_sync(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "auto_accept": "auto_accept",
        "sync_menu": "sync_menu",
        "connector_status": "connector_status",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            if arg_name == "connector_status":
                if val not in VALID_CONNECTOR_STATUSES:
                    err(f"Invalid connector_status: {val}")
                updates.append(f"{col_name} = ?")
                params.append(val)
            else:
                updates.append(f"{col_name} = ?")
                params.append(int(val))
            changed.append(col_name)

    if not updates:
        err("No fields to update. Provide --auto-accept, --sync-menu, or --connector-status")

    updates.append("updated_at = ?")
    params.append(_now_iso())
    params.append(connector_id)
    conn.execute(
        # PyPika: skipped — dynamic UPDATE with variable columns
        f"UPDATE connv2_delivery_connector SET {', '.join(updates)} WHERE id = ?", params
    )
    audit(conn, SKILL, "integration-configure-delivery-sync", "connv2_delivery_connector", connector_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": connector_id, "updated_fields": changed})


# ===========================================================================
# 3. ingest-orders
# ===========================================================================
def ingest_orders(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    order_id = str(uuid.uuid4())
    now = _now_iso()
    total_amount = getattr(args, "total_amount", None) or "0.00"
    commission = getattr(args, "commission", None) or "0.00"

    total_dec = to_decimal(total_amount)
    commission_dec = to_decimal(commission)
    net_dec = total_dec - commission_dec
    net_amount = str(round_currency(net_dec))

    company_id = row["company_id"]
    sql, _ = insert_row("connv2_delivery_order", {"id": P(), "connector_id": P(), "external_order_id": P(), "order_data": P(), "total_amount": P(), "commission": P(), "net_amount": P(), "order_status": P(), "received_at": P(), "company_id": P(), "created_at": P(), "updated_at": P()})
    conn.execute(sql, (
        order_id, connector_id,
        getattr(args, "external_order_id", None),
        getattr(args, "order_data", None),
        str(round_currency(total_dec)),
        str(round_currency(commission_dec)),
        net_amount,
        "received",
        now, company_id, now, now,
    ))
    audit(conn, SKILL, "integration-ingest-orders", "connv2_delivery_order", order_id,
          new_values={"total_amount": total_amount, "external_order_id": getattr(args, "external_order_id", None)})
    conn.commit()
    ok({"id": order_id, "connector_id": connector_id,
        "total_amount": str(round_currency(total_dec)),
        "net_amount": net_amount, "order_status": "received"})


# ===========================================================================
# 4. sync-menu
# ===========================================================================
def sync_menu(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    now = _now_iso()
    conn.execute(
        update_row("connv2_delivery_connector",
                   data={"last_sync_at": P(), "updated_at": P()},
                   where={"id": P()}),
        (now, now, connector_id)
    )
    audit(conn, SKILL, "integration-sync-menu", "connv2_delivery_connector", connector_id,
          new_values={"sync_type": "menu"})
    conn.commit()
    ok({"connector_id": connector_id, "sync_type": "menu", "synced_at": now})


# ===========================================================================
# 5. update-order-status
# ===========================================================================
def update_order_status(conn, args):
    order_id = getattr(args, "order_id", None)
    if not order_id:
        err("--order-id is required")
    row = conn.execute(Q.from_(Table("connv2_delivery_order")).select(Table("connv2_delivery_order").star).where(Field("id") == P()).get_sql(), (order_id,)).fetchone()
    if not row:
        err(f"Delivery order {order_id} not found")

    new_status = getattr(args, "order_status", None)
    if not new_status:
        err("--order-status is required")
    if new_status not in VALID_ORDER_STATUSES:
        err(f"Invalid order_status: {new_status}. Must be one of: {', '.join(VALID_ORDER_STATUSES)}")

    now = _now_iso()
    conn.execute(
        update_row("connv2_delivery_order",
                   data={"order_status": P(), "updated_at": P()},
                   where={"id": P()}),
        (new_status, now, order_id)
    )
    audit(conn, SKILL, "integration-update-order-status", "connv2_delivery_order", order_id,
          old_values={"order_status": row["order_status"]},
          new_values={"order_status": new_status})
    conn.commit()
    ok({"id": order_id, "order_status": new_status})


# ===========================================================================
# 6. list-delivery-syncs
# ===========================================================================
def list_delivery_syncs(conn, args):
    # PyPika: skipped — dynamic WHERE with LEFT JOIN
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("o.company_id = ?")
        params.append(args.company_id)
    if getattr(args, "connector_id", None):
        where.append("o.connector_id = ?")
        params.append(args.connector_id)
    if getattr(args, "order_status", None):
        where.append("o.order_status = ?")
        params.append(args.order_status)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM connv2_delivery_order o WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"""SELECT o.*, c.platform FROM connv2_delivery_order o
            LEFT JOIN connv2_delivery_connector c ON o.connector_id = c.id
            WHERE {where_sql} ORDER BY o.created_at DESC LIMIT ? OFFSET ?""",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 7. delivery-revenue-report
# ===========================================================================
def delivery_revenue_report(conn, args):
    # PyPika: skipped — complex aggregate JOIN with CAST report
    _validate_company(conn, args.company_id)
    rows = conn.execute("""
        SELECT c.platform,
               COUNT(o.id) as total_orders,
               COALESCE(SUM(CAST(o.total_amount AS NUMERIC)), 0) as gross_revenue,
               COALESCE(SUM(CAST(o.commission AS NUMERIC)), 0) as total_commission,
               COALESCE(SUM(CAST(o.net_amount AS NUMERIC)), 0) as net_revenue
        FROM connv2_delivery_connector c
        LEFT JOIN connv2_delivery_order o ON c.id = o.connector_id
        WHERE c.company_id = ?
        GROUP BY c.platform
        ORDER BY net_revenue DESC
    """, (args.company_id,)).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "count": len(rows),
    })


# ===========================================================================
# 8. delivery-platform-comparison
# ===========================================================================
def delivery_platform_comparison(conn, args):
    # PyPika: skipped — complex aggregate JOIN with CASE report
    _validate_company(conn, args.company_id)
    rows = conn.execute("""
        SELECT c.platform,
               COUNT(DISTINCT c.id) as connector_count,
               COUNT(o.id) as total_orders,
               SUM(CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END) as delivered_orders,
               SUM(CASE WHEN o.order_status = 'cancelled' THEN 1 ELSE 0 END) as cancelled_orders,
               COALESCE(SUM(CAST(o.net_amount AS NUMERIC)), 0) as total_net
        FROM connv2_delivery_connector c
        LEFT JOIN connv2_delivery_order o ON c.id = o.connector_id
        WHERE c.company_id = ?
        GROUP BY c.platform
        ORDER BY total_orders DESC
    """, (args.company_id,)).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "count": len(rows),
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "integration-add-delivery-connector": add_delivery_connector,
    "integration-configure-delivery-sync": configure_delivery_sync,
    "integration-ingest-orders": ingest_orders,
    "integration-sync-menu": sync_menu,
    "integration-update-order-status": update_order_status,
    "integration-list-delivery-syncs": list_delivery_syncs,
    "integration-delivery-revenue-report": delivery_revenue_report,
    "integration-delivery-platform-comparison": delivery_platform_comparison,
}
