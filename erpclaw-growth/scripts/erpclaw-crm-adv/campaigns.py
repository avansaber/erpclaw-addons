"""erpclaw-crm-adv -- campaigns domain module

Actions for email campaign management, templates, recipient lists, and event tracking (4 tables, 12 actions).
Imported by db_query.py (unified router).
"""
import json
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

    ENTITY_PREFIXES.setdefault("crmadv_email_campaign", "EMCAMP-")
except ImportError:
    pass

SKILL = "erpclaw-crm-adv"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_TEMPLATE_TYPES = ("newsletter", "promotional", "transactional", "drip", "welcome")
VALID_LIST_TYPES = ("static", "dynamic", "segment")
VALID_CAMPAIGN_STATUSES = ("draft", "scheduled", "sending", "sent", "paused", "cancelled")
VALID_EVENT_TYPES = ("sent", "opened", "clicked", "bounced", "unsubscribed", "converted")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


# ===========================================================================
# 1. add-email-campaign
# ===========================================================================
def add_email_campaign(conn, args):
    _validate_company(conn, args.company_id)
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    camp_id = str(uuid.uuid4())
    now = _now_iso()
    conn.company_id = args.company_id
    naming = get_next_name(conn, "crmadv_email_campaign")

    template_id = getattr(args, "template_id", None)
    if template_id:
        if not conn.execute(Q.from_(Table("crmadv_campaign_template")).select(Field('id')).where(Field("id") == P()).get_sql(), (template_id,)).fetchone():
            err(f"Campaign template {template_id} not found")

    recipient_list_id = getattr(args, "recipient_list_id", None)
    if recipient_list_id:
        if not conn.execute(Q.from_(Table("crmadv_recipient_list")).select(Field('id')).where(Field("id") == P()).get_sql(), (recipient_list_id,)).fetchone():
            err(f"Recipient list {recipient_list_id} not found")

    conn.execute("""
        INSERT INTO crmadv_email_campaign (
            id, naming_series, name, subject, template_id, recipient_list_id,
            campaign_status, scheduled_date,
            company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        camp_id, naming, name,
        getattr(args, "subject", None),
        template_id, recipient_list_id,
        "draft",
        getattr(args, "scheduled_date", None),
        args.company_id, now, now,
    ))
    audit(conn, SKILL, "add-email-campaign", "crmadv_email_campaign", camp_id,
          new_values={"name": name})
    conn.commit()
    ok({"id": camp_id, "naming_series": naming, "name": name, "campaign_status": "draft"})


# ===========================================================================
# 2. update-email-campaign
# ===========================================================================
def update_email_campaign(conn, args):
    camp_id = getattr(args, "campaign_id", None)
    if not camp_id:
        err("--campaign-id is required")
    row = conn.execute(Q.from_(Table("crmadv_email_campaign")).select(Table("crmadv_email_campaign").star).where(Field("id") == P()).get_sql(), (camp_id,)).fetchone()
    if not row:
        err(f"Email campaign {camp_id} not found")

    d = row_to_dict(row)
    if d["campaign_status"] in ("sent", "sending"):
        err("Cannot update a campaign that has been sent or is sending")

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "name": "name", "subject": "subject",
        "template_id": "template_id", "recipient_list_id": "recipient_list_id",
        "scheduled_date": "scheduled_date",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    if not updates:
        err("No fields to update")

    updates.append("updated_at = ?")
    params.append(_now_iso())
    params.append(camp_id)
    conn.execute(f"UPDATE crmadv_email_campaign SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, SKILL, "update-email-campaign", "crmadv_email_campaign", camp_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": camp_id, "updated_fields": changed})


# ===========================================================================
# 3. get-email-campaign
# ===========================================================================
def get_email_campaign(conn, args):
    camp_id = getattr(args, "campaign_id", None)
    if not camp_id:
        err("--campaign-id is required")
    row = conn.execute(Q.from_(Table("crmadv_email_campaign")).select(Table("crmadv_email_campaign").star).where(Field("id") == P()).get_sql(), (camp_id,)).fetchone()
    if not row:
        err(f"Email campaign {camp_id} not found")
    ok(row_to_dict(row))


# ===========================================================================
# 4. list-email-campaigns
# ===========================================================================
def list_email_campaigns(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "campaign_status_filter", None):
        where.append("campaign_status = ?")
        params.append(args.campaign_status_filter)
    if getattr(args, "search", None):
        where.append("(name LIKE ? OR subject LIKE ?)")
        params.extend([f"%{args.search}%"] * 2)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM crmadv_email_campaign WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM crmadv_email_campaign WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 5. add-campaign-template
# ===========================================================================
def add_campaign_template(conn, args):
    _validate_company(conn, args.company_id)
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    template_type = getattr(args, "template_type", None) or "newsletter"
    if template_type not in VALID_TEMPLATE_TYPES:
        err(f"Invalid template_type: {template_type}. Must be one of: {', '.join(VALID_TEMPLATE_TYPES)}")

    tmpl_id = str(uuid.uuid4())
    now = _now_iso()

    conn.execute("""
        INSERT INTO crmadv_campaign_template (
            id, name, subject_template, body_html, body_text,
            template_type, is_active, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        tmpl_id, name,
        getattr(args, "subject_template", None),
        getattr(args, "body_html", None),
        getattr(args, "body_text", None),
        template_type, 1,
        args.company_id, now, now,
    ))
    audit(conn, SKILL, "add-campaign-template", "crmadv_campaign_template", tmpl_id,
          new_values={"name": name, "template_type": template_type})
    conn.commit()
    ok({"id": tmpl_id, "name": name, "template_type": template_type})


# ===========================================================================
# 6. list-campaign-templates
# ===========================================================================
def list_campaign_templates(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "template_type", None):
        where.append("template_type = ?")
        params.append(args.template_type)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM crmadv_campaign_template WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM crmadv_campaign_template WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 7. add-recipient-list
# ===========================================================================
def add_recipient_list(conn, args):
    _validate_company(conn, args.company_id)
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")

    list_type = getattr(args, "list_type", None) or "static"
    if list_type not in VALID_LIST_TYPES:
        err(f"Invalid list_type: {list_type}. Must be one of: {', '.join(VALID_LIST_TYPES)}")

    rl_id = str(uuid.uuid4())
    now = _now_iso()

    conn.execute("""
        INSERT INTO crmadv_recipient_list (
            id, name, description, list_type, filter_criteria,
            recipient_count, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        rl_id, name,
        getattr(args, "description", None),
        list_type,
        getattr(args, "filter_criteria", None),
        0, args.company_id, now, now,
    ))
    audit(conn, SKILL, "add-recipient-list", "crmadv_recipient_list", rl_id,
          new_values={"name": name, "list_type": list_type})
    conn.commit()
    ok({"id": rl_id, "name": name, "list_type": list_type})


# ===========================================================================
# 8. list-recipient-lists
# ===========================================================================
def list_recipient_lists(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM crmadv_recipient_list WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM crmadv_recipient_list WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 9. schedule-campaign
# ===========================================================================
def schedule_campaign(conn, args):
    camp_id = getattr(args, "campaign_id", None)
    if not camp_id:
        err("--campaign-id is required")
    scheduled_date = getattr(args, "scheduled_date", None)
    if not scheduled_date:
        err("--scheduled-date is required")

    row = conn.execute(Q.from_(Table("crmadv_email_campaign")).select(Table("crmadv_email_campaign").star).where(Field("id") == P()).get_sql(), (camp_id,)).fetchone()
    if not row:
        err(f"Email campaign {camp_id} not found")

    d = row_to_dict(row)
    if d["campaign_status"] not in ("draft", "paused"):
        err(f"Cannot schedule campaign in status '{d['campaign_status']}'. Must be draft or paused.")

    now = _now_iso()
    conn.execute("""
        UPDATE crmadv_email_campaign
        SET campaign_status = 'scheduled', scheduled_date = ?, updated_at = ?
        WHERE id = ?
    """, (scheduled_date, now, camp_id))
    audit(conn, SKILL, "schedule-campaign", "crmadv_email_campaign", camp_id,
          new_values={"campaign_status": "scheduled", "scheduled_date": scheduled_date})
    conn.commit()
    ok({"id": camp_id, "campaign_status": "scheduled", "scheduled_date": scheduled_date})


# ===========================================================================
# 10. send-campaign
# ===========================================================================
def send_campaign(conn, args):
    camp_id = getattr(args, "campaign_id", None)
    if not camp_id:
        err("--campaign-id is required")

    row = conn.execute(Q.from_(Table("crmadv_email_campaign")).select(Table("crmadv_email_campaign").star).where(Field("id") == P()).get_sql(), (camp_id,)).fetchone()
    if not row:
        err(f"Email campaign {camp_id} not found")

    d = row_to_dict(row)
    if d["campaign_status"] not in ("draft", "scheduled"):
        err(f"Cannot send campaign in status '{d['campaign_status']}'. Must be draft or scheduled.")

    now = _now_iso()
    conn.execute("""
        UPDATE crmadv_email_campaign
        SET campaign_status = 'sent', sent_date = ?, updated_at = ?
        WHERE id = ?
    """, (now, now, camp_id))
    audit(conn, SKILL, "send-campaign", "crmadv_email_campaign", camp_id,
          new_values={"campaign_status": "sent"})
    conn.commit()
    ok({"id": camp_id, "campaign_status": "sent", "sent_date": now})


# ===========================================================================
# 11. track-campaign-event
# ===========================================================================
def track_campaign_event(conn, args):
    camp_id = getattr(args, "campaign_id", None)
    if not camp_id:
        err("--campaign-id is required")
    _validate_company(conn, args.company_id)

    if not conn.execute(Q.from_(Table("crmadv_email_campaign")).select(Field('id')).where(Field("id") == P()).get_sql(), (camp_id,)).fetchone():
        err(f"Email campaign {camp_id} not found")

    event_type = getattr(args, "event_type", None)
    if not event_type:
        err("--event-type is required")
    if event_type not in VALID_EVENT_TYPES:
        err(f"Invalid event_type: {event_type}. Must be one of: {', '.join(VALID_EVENT_TYPES)}")

    evt_id = str(uuid.uuid4())
    now = _now_iso()

    conn.execute("""
        INSERT INTO crmadv_campaign_event (
            id, campaign_id, event_type, recipient_email, event_timestamp,
            metadata, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?)
    """, (
        evt_id, camp_id, event_type,
        getattr(args, "recipient_email", None),
        getattr(args, "event_timestamp", None) or now,
        getattr(args, "metadata", None),
        args.company_id, now,
    ))

    # Update campaign counters
    counter_map = {
        "opened": "total_opened",
        "clicked": "total_clicked",
        "bounced": "total_bounced",
        "unsubscribed": "total_unsubscribed",
        "sent": "total_sent",
    }
    if event_type in counter_map:
        col = counter_map[event_type]
        conn.execute(
            f"UPDATE crmadv_email_campaign SET {col} = {col} + 1, updated_at = ? WHERE id = ?",
            (now, camp_id)
        )

    audit(conn, SKILL, "track-campaign-event", "crmadv_campaign_event", evt_id,
          new_values={"campaign_id": camp_id, "event_type": event_type})
    conn.commit()
    ok({"id": evt_id, "campaign_id": camp_id, "event_type": event_type})


# ===========================================================================
# 12. campaign-roi-report
# ===========================================================================
def campaign_roi_report(conn, args):
    _validate_company(conn, args.company_id)

    rows = conn.execute("""
        SELECT id, name, campaign_status, total_sent, total_opened,
               total_clicked, total_bounced, total_unsubscribed,
               sent_date, created_at
        FROM crmadv_email_campaign
        WHERE company_id = ?
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    """, (args.company_id, args.limit, args.offset)).fetchall()

    result_rows = []
    for r in rows:
        d = row_to_dict(r)
        sent = d.get("total_sent", 0) or 0
        opened = d.get("total_opened", 0) or 0
        clicked = d.get("total_clicked", 0) or 0
        d["open_rate_pct"] = round(opened / sent * 100, 1) if sent > 0 else 0.0
        d["click_rate_pct"] = round(clicked / sent * 100, 1) if sent > 0 else 0.0
        result_rows.append(d)

    total = conn.execute(
        "SELECT COUNT(*) FROM crmadv_email_campaign WHERE company_id = ?",
        (args.company_id,)
    ).fetchone()[0]

    ok({
        "rows": result_rows,
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "add-email-campaign": add_email_campaign,
    "update-email-campaign": update_email_campaign,
    "get-email-campaign": get_email_campaign,
    "list-email-campaigns": list_email_campaigns,
    "add-campaign-template": add_campaign_template,
    "list-campaign-templates": list_campaign_templates,
    "add-recipient-list": add_recipient_list,
    "list-recipient-lists": list_recipient_lists,
    "schedule-campaign": schedule_campaign,
    "send-campaign": send_campaign,
    "track-campaign-event": track_campaign_event,
    "campaign-roi-report": campaign_roi_report,
}
