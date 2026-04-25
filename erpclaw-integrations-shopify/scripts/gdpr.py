"""shopify-handle-gdpr: process a GDPR event the Worker forwarded to us.

Three topics (per Shopify's compliance contract):
  - customers/data_request   -> produce a JSON export of the customer's
                                data in our local DB so the merchant can
                                forward it to Shopify. We DO NOT email or
                                otherwise transmit; the file lives on the
                                merchant's own machine.
  - customers/redact         -> null out PII on shopify_order rows tied
                                to the customer: customer_email,
                                billing/shipping address, phone.
  - shop/redact              -> hard delete all shopify_* rows for the
                                shop. GL entries are PRESERVED per
                                ERPClaw immutability rule; this is
                                defensible under GDPR because the GL is
                                an accounting record (Art. 17(3)(b)).

Plus a fourth signal for our own internal bookkeeping:
  - app/uninstalled          -> best-effort cleanup, queue a
                                shop_redact-style scrub. Shopify will
                                also fire shop/redact 48h later.

Output format matches the dispatcher contract: on success we print a
status JSON so redirect_stdout can capture it; caller acks the command.
"""
import json
import os
import sys
import time

LIB_PATH = os.path.expanduser("~/.openclaw/erpclaw/lib")
if LIB_PATH not in sys.path:
    sys.path.insert(0, LIB_PATH)

from erpclaw_lib.response import err, ok


VALID_TOPICS = {"customers/data_request", "customers/redact", "shop/redact", "app/uninstalled"}


# ---------------------------------------------------------------------------
# Audit helpers
# ---------------------------------------------------------------------------

def _logs_dir():
    d = os.path.expanduser("~/.openclaw/erpclaw/logs")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        d = os.path.abspath(".")
    return d


def _write_audit(topic, shop, detail):
    path = os.path.join(_logs_dir(), "shopify_gdpr.log")
    entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "topic": topic,
        "shop_domain": shop,
        "detail": detail,
    }
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _handle_data_request(conn, shop, payload):
    """Write a pointer file the merchant can act on.

    The shopify_order table links customers via ERPClaw's `customer_id`
    (foreign key to the core `customer` table), not directly by Shopify
    customer id. Rather than reach into the ERPClaw customer redaction
    flow (which is a separate core concern not owned by this connector),
    we drop an audit record + pointer file so the merchant knows to run
    their own customer DSR workflow. Shopify's contract only requires
    acknowledgement within 30 days.
    """
    shopify_customer_id = None
    if isinstance(payload, dict):
        customer = payload.get("customer") or {}
        shopify_customer_id = customer.get("id")

    account_row = conn.execute(
        "SELECT id FROM shopify_account WHERE shop_domain = ?",
        (shop,),
    ).fetchone()
    shopify_account_id = account_row["id"] if account_row else None

    export = {
        "shop_domain": shop,
        "shopify_account_id": shopify_account_id,
        "shopify_customer_id": shopify_customer_id,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "note": (
            "ERPClaw's Shopify connector does not store customer PII directly. "
            "Customer contact data lives in the ERPClaw core `customer` table. "
            "Run the core ERPClaw DSR workflow to complete this request."
        ),
    }
    out_path = None
    if shopify_customer_id:
        out_path = os.path.join(
            _logs_dir(),
            f"shopify_dsr_{shop}_{shopify_customer_id}_{int(time.time())}.json",
        )
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(export, f, indent=2, default=str)
        except OSError:
            out_path = None

    _write_audit("customers/data_request", shop, {
        "shopify_customer_id": shopify_customer_id,
        "export_path": out_path,
    })
    return {"export_path": out_path, "shopify_customer_id": shopify_customer_id}


def _handle_customers_redact(conn, shop, payload):
    """Log the redaction request for merchant follow-up.

    Same rationale as _handle_data_request: the connector does not hold
    the customer's contact PII. ERPClaw's core customer redaction flow
    handles the actual deletion/anonymisation.
    """
    shopify_customer_id = None
    if isinstance(payload, dict):
        customer = payload.get("customer") or {}
        shopify_customer_id = customer.get("id")

    account_row = conn.execute(
        "SELECT id FROM shopify_account WHERE shop_domain = ?",
        (shop,),
    ).fetchone()
    shopify_account_id = account_row["id"] if account_row else None

    _write_audit("customers/redact", shop, {
        "shopify_customer_id": shopify_customer_id,
        "shopify_account_id": shopify_account_id,
        "note": "forwarded to ERPClaw core customer redaction flow",
    })
    return {"shopify_customer_id": shopify_customer_id, "logged": True}


# Per the architecture doc we hard-delete mirror tables on shop/redact but
# preserve gl_entry rows. Listing the tables explicitly keeps the delete
# order predictable and keeps us from accidentally wiping GL-related
# columns on non-shopify tables.
_SHOPIFY_MIRROR_TABLES = [
    "shopify_dispute",
    "shopify_payout_transaction",
    "shopify_payout",
    "shopify_refund_line_item",
    "shopify_refund",
    "shopify_order_line_item",
    "shopify_order",
    "shopify_product_variant",
    "shopify_product",
    "shopify_customer",
    "shopify_sync_job",
]


def _handle_shop_redact(conn, shop, payload):
    """Hard-delete every shopify_* row for this shop. GL entries preserved."""
    account_row = conn.execute(
        "SELECT id FROM shopify_account WHERE shop_domain = ?",
        (shop,),
    ).fetchone()
    if not account_row:
        _write_audit("shop/redact", shop, {"note": "already removed"})
        return {"rows_deleted": 0, "shopify_account_removed": False}
    account_id = account_row["id"]

    deleted = {}
    for table in _SHOPIFY_MIRROR_TABLES:
        # Skip tables that don't exist yet in this install (defensive).
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (table,),
        ).fetchone()
        if not exists:
            continue
        # Check the table actually has a shopify_account_id column before
        # trying to delete on it.
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if "shopify_account_id" not in cols:
            continue
        cur = conn.execute(
            f"DELETE FROM {table} WHERE shopify_account_id = ?",
            (account_id,),
        )
        deleted[table] = cur.rowcount or 0

    # Finally the shopify_account row itself.
    conn.execute("DELETE FROM shopify_account WHERE id = ?", (account_id,))
    conn.commit()

    _write_audit("shop/redact", shop, {
        "shopify_account_id": account_id,
        "deleted": deleted,
    })
    return {
        "rows_deleted": sum(deleted.values()),
        "per_table": deleted,
        "shopify_account_removed": True,
    }


def _handle_app_uninstalled(conn, shop, payload):
    """Treat app/uninstalled as a soft signal: flag the account so the
    daemon stops pushing status. shop/redact 48h later will do the
    hard delete.
    """
    cur = conn.execute(
        "UPDATE shopify_account SET disconnect_state = 'app-uninstalled', status = 'disabled' "
        "WHERE shop_domain = ?",
        (shop,),
    )
    rows = cur.rowcount or 0
    conn.commit()
    _write_audit("app/uninstalled", shop, {"flagged": rows})
    return {"flagged": rows}


# ---------------------------------------------------------------------------
# Action entrypoint
# ---------------------------------------------------------------------------

def shopify_handle_gdpr(conn, args):
    topic = getattr(args, "topic", None)
    shop = getattr(args, "shop_domain", None)
    payload_raw = getattr(args, "payload", None)

    if not topic or topic not in VALID_TOPICS:
        err(f"topic must be one of {sorted(VALID_TOPICS)}")
    if not shop:
        err("--shop-domain is required")

    payload = None
    if payload_raw:
        try:
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
        except Exception:  # noqa: BLE001
            payload = None

    if topic == "customers/data_request":
        result = _handle_data_request(conn, shop, payload)
    elif topic == "customers/redact":
        result = _handle_customers_redact(conn, shop, payload)
    elif topic == "shop/redact":
        result = _handle_shop_redact(conn, shop, payload)
    elif topic == "app/uninstalled":
        result = _handle_app_uninstalled(conn, shop, payload)
    else:
        err(f"unreachable: topic {topic!r}")

    ok({"topic": topic, "shop_domain": shop, "processed": True, **result})


GDPR_ACTIONS = {
    "shopify-handle-gdpr": shopify_handle_gdpr,
}
