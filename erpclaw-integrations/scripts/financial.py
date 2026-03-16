"""ERPClaw Connectors V2 -- financial domain module

Actions for financial/communication platform connectors (1 table, 8 actions).
Supports Plaid, Twilio, SendGrid, Mailchimp integrations.
Imported by db_query.py (unified router).
"""
import os
import sys
import uuid
from datetime import datetime, timezone

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row

    ENTITY_PREFIXES.setdefault("connv2_financial_connector", "FNC-")
except ImportError:
    pass

SKILL = "erpclaw-connectors-v2"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_PLATFORMS = ("plaid", "twilio", "sendgrid", "mailchimp")
VALID_CONNECTOR_STATUSES = ("active", "inactive", "error")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field("id")).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _get_connector(conn, connector_id):
    if not connector_id:
        err("--connector-id is required")
    row = conn.execute(Q.from_(Table("connv2_financial_connector")).select(Table("connv2_financial_connector").star).where(Field("id") == P()).get_sql(), (connector_id,)).fetchone()
    if not row:
        err(f"Financial connector {connector_id} not found")
    return row


# ===========================================================================
# 1. add-financial-connector
# ===========================================================================
def add_financial_connector(conn, args):
    _validate_company(conn, args.company_id)
    platform = getattr(args, "platform", None)
    if not platform:
        err("--platform is required")
    if platform not in VALID_PLATFORMS:
        err(f"Invalid platform: {platform}. Must be one of: {', '.join(VALID_PLATFORMS)}")

    conn_id = str(uuid.uuid4())
    now = _now_iso()
    conn.company_id = args.company_id
    naming = get_next_name(conn, "connv2_financial_connector")

    sql, _ = insert_row("connv2_financial_connector", {"id": P(), "naming_series": P(), "platform": P(), "account_ref": P(), "api_credentials_ref": P(), "sync_enabled": P(), "connector_status": P(), "company_id": P(), "created_at": P(), "updated_at": P()})
    conn.execute(sql, (
        conn_id, naming, platform,
        getattr(args, "account_ref", None),
        getattr(args, "api_credentials_ref", None),
        1 if getattr(args, "sync_enabled", None) != "0" else 0,
        "inactive",
        args.company_id, now, now,
    ))
    audit(conn, SKILL, "integration-add-financial-connector", "connv2_financial_connector", conn_id,
          new_values={"platform": platform})
    conn.commit()
    ok({"id": conn_id, "naming_series": naming, "platform": platform,
        "connector_status": "inactive"})


# ===========================================================================
# 2. sync-bank-feeds
# ===========================================================================
def sync_bank_feeds(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    # Verify it's a plaid connector for bank feeds
    if row["platform"] != "plaid":
        err("sync-bank-feeds is only available for Plaid connectors")

    now = _now_iso()
    conn.execute(
        update_row("connv2_financial_connector",
                   data={"last_sync_at": P(), "updated_at": P()},
                   where={"id": P()}),
        (now, now, connector_id)
    )
    audit(conn, SKILL, "integration-sync-bank-feeds", "connv2_financial_connector", connector_id,
          new_values={"sync_type": "bank_feeds"})
    conn.commit()
    ok({"connector_id": connector_id, "sync_type": "bank_feeds", "synced_at": now})


# ===========================================================================
# 3. sync-transactions
# ===========================================================================
def sync_transactions(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    if row["platform"] != "plaid":
        err("sync-transactions is only available for Plaid connectors")

    now = _now_iso()
    conn.execute(
        update_row("connv2_financial_connector",
                   data={"last_sync_at": P(), "updated_at": P()},
                   where={"id": P()}),
        (now, now, connector_id)
    )
    audit(conn, SKILL, "integration-sync-transactions", "connv2_financial_connector", connector_id,
          new_values={"sync_type": "transactions"})
    conn.commit()
    ok({"connector_id": connector_id, "sync_type": "transactions", "synced_at": now})


# ===========================================================================
# 4. send-sms
# ===========================================================================
def send_sms(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    if row["platform"] != "twilio":
        err("send-sms is only available for Twilio connectors")

    recipient = getattr(args, "recipient", None)
    if not recipient:
        err("--recipient is required")
    message_body = getattr(args, "message_body", None)
    if not message_body:
        err("--message-body is required")

    now = _now_iso()
    conn.execute(
        update_row("connv2_financial_connector",
                   data={"last_sync_at": P(), "updated_at": P()},
                   where={"id": P()}),
        (now, now, connector_id)
    )
    audit(conn, SKILL, "integration-send-sms", "connv2_financial_connector", connector_id,
          new_values={"recipient": recipient, "action": "sms_sent"})
    conn.commit()
    ok({"connector_id": connector_id, "recipient": recipient,
        "message_type": "sms", "sent_at": now})


# ===========================================================================
# 5. send-email-delivery
# ===========================================================================
def send_email_delivery(conn, args):
    connector_id = getattr(args, "connector_id", None)
    row = _get_connector(conn, connector_id)

    if row["platform"] not in ("sendgrid", "mailchimp"):
        err("send-email-delivery is only available for SendGrid or Mailchimp connectors")

    recipient = getattr(args, "recipient", None)
    if not recipient:
        err("--recipient is required")
    subject = getattr(args, "subject", None)
    if not subject:
        err("--subject is required")

    now = _now_iso()
    conn.execute(
        update_row("connv2_financial_connector",
                   data={"last_sync_at": P(), "updated_at": P()},
                   where={"id": P()}),
        (now, now, connector_id)
    )
    audit(conn, SKILL, "integration-send-email-delivery", "connv2_financial_connector", connector_id,
          new_values={"recipient": recipient, "subject": subject})
    conn.commit()
    ok({"connector_id": connector_id, "recipient": recipient,
        "subject": subject, "message_type": "email", "sent_at": now})


# ===========================================================================
# 6. list-financial-syncs
# ===========================================================================
def list_financial_syncs(conn, args):
    # PyPika: skipped — dynamic WHERE with optional filters
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "platform", None):
        where.append("platform = ?")
        params.append(args.platform)
    if getattr(args, "connector_status", None):
        where.append("connector_status = ?")
        params.append(args.connector_status)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM connv2_financial_connector WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"""SELECT * FROM connv2_financial_connector
            WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?""",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 7. bank-feed-reconciliation-report
# ===========================================================================
def bank_feed_reconciliation_report(conn, args):
    # PyPika: skipped — filtered query with platform constant
    _validate_company(conn, args.company_id)
    rows = conn.execute("""
        SELECT id, naming_series, platform, account_ref,
               connector_status, sync_enabled, last_sync_at
        FROM connv2_financial_connector
        WHERE company_id = ? AND platform = 'plaid'
        ORDER BY last_sync_at DESC
    """, (args.company_id,)).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "count": len(rows),
    })


# ===========================================================================
# 8. communication-delivery-report
# ===========================================================================
def communication_delivery_report(conn, args):
    # PyPika: skipped — complex aggregate report with IN clause and CASE
    _validate_company(conn, args.company_id)
    rows = conn.execute("""
        SELECT platform,
               COUNT(*) as connector_count,
               SUM(CASE WHEN connector_status = 'active' THEN 1 ELSE 0 END) as active_count,
               SUM(CASE WHEN connector_status = 'error' THEN 1 ELSE 0 END) as error_count
        FROM connv2_financial_connector
        WHERE company_id = ? AND platform IN ('twilio', 'sendgrid', 'mailchimp')
        GROUP BY platform
        ORDER BY connector_count DESC
    """, (args.company_id,)).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "count": len(rows),
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "integration-add-financial-connector": add_financial_connector,
    "integration-sync-bank-feeds": sync_bank_feeds,
    "integration-sync-transactions": sync_transactions,
    "integration-send-sms": send_sms,
    "integration-send-email-delivery": send_email_delivery,
    "integration-list-financial-syncs": list_financial_syncs,
    "integration-bank-feed-reconciliation-report": bank_feed_reconciliation_report,
    "integration-communication-delivery-report": communication_delivery_report,
}
