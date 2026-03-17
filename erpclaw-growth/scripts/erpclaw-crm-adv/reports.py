"""erpclaw-crm-adv -- reports domain module

Cross-domain reports and status action (6 actions).
Imported by db_query.py (unified router).
"""
import os
import sys

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row
except ImportError:
    pass

SKILL = "erpclaw-crm-adv"


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


# ===========================================================================
# 1. funnel-analysis
# ===========================================================================
def funnel_analysis(conn, args):
    _validate_company(conn, args.company_id)

    # Campaign funnel: sent -> opened -> clicked -> converted
    total_sent = conn.execute(
        "SELECT COALESCE(SUM(total_sent), 0) FROM crmadv_email_campaign WHERE company_id = ?",
        (args.company_id,)
    ).fetchone()[0]
    total_opened = conn.execute(
        "SELECT COALESCE(SUM(total_opened), 0) FROM crmadv_email_campaign WHERE company_id = ?",
        (args.company_id,)
    ).fetchone()[0]
    total_clicked = conn.execute(
        "SELECT COALESCE(SUM(total_clicked), 0) FROM crmadv_email_campaign WHERE company_id = ?",
        (args.company_id,)
    ).fetchone()[0]
    total_converted = conn.execute(
        "SELECT COUNT(*) FROM crmadv_campaign_event WHERE company_id = ? AND event_type = 'converted'",
        (args.company_id,)
    ).fetchone()[0]

    open_rate = round(total_opened / total_sent * 100, 1) if total_sent > 0 else 0.0
    click_rate = round(total_clicked / total_sent * 100, 1) if total_sent > 0 else 0.0
    conversion_rate = round(total_converted / total_sent * 100, 1) if total_sent > 0 else 0.0

    ok({
        "total_sent": total_sent,
        "total_opened": total_opened,
        "total_clicked": total_clicked,
        "total_converted": total_converted,
        "open_rate_pct": open_rate,
        "click_rate_pct": click_rate,
        "conversion_rate_pct": conversion_rate,
    })


# ===========================================================================
# 2. pipeline-velocity
# ===========================================================================
def pipeline_velocity(conn, args):
    _validate_company(conn, args.company_id)

    # Contract pipeline
    draft_contracts = conn.execute(
        "SELECT COUNT(*) FROM crmadv_contract WHERE company_id = ? AND contract_status = 'draft'",
        (args.company_id,)
    ).fetchone()[0]
    active_contracts = conn.execute(
        "SELECT COUNT(*) FROM crmadv_contract WHERE company_id = ? AND contract_status = 'active'",
        (args.company_id,)
    ).fetchone()[0]
    total_pipeline_value = conn.execute(
        "SELECT COALESCE(SUM(CAST(total_value AS NUMERIC)), 0) FROM crmadv_contract WHERE company_id = ? AND contract_status IN ('draft','active')",
        (args.company_id,)
    ).fetchone()[0]

    # Territory performance
    territories_with_quota = conn.execute(
        "SELECT COUNT(DISTINCT territory_id) FROM crmadv_territory_quota WHERE company_id = ?",
        (args.company_id,)
    ).fetchone()[0]

    ok({
        "draft_contracts": draft_contracts,
        "active_contracts": active_contracts,
        "total_pipeline_value": str(round_currency(to_decimal(str(total_pipeline_value)))),
        "territories_with_quota": territories_with_quota,
    })


# ===========================================================================
# 3. win-loss-analysis
# ===========================================================================
def win_loss_analysis(conn, args):
    _validate_company(conn, args.company_id)

    active = conn.execute(
        "SELECT COUNT(*) FROM crmadv_contract WHERE company_id = ? AND contract_status = 'active'",
        (args.company_id,)
    ).fetchone()[0]
    renewed = conn.execute(
        "SELECT COUNT(*) FROM crmadv_contract WHERE company_id = ? AND contract_status = 'renewed'",
        (args.company_id,)
    ).fetchone()[0]
    terminated = conn.execute(
        "SELECT COUNT(*) FROM crmadv_contract WHERE company_id = ? AND contract_status = 'terminated'",
        (args.company_id,)
    ).fetchone()[0]
    expired = conn.execute(
        "SELECT COUNT(*) FROM crmadv_contract WHERE company_id = ? AND contract_status = 'expired'",
        (args.company_id,)
    ).fetchone()[0]

    total = active + renewed + terminated + expired
    win_rate = round((active + renewed) / total * 100, 1) if total > 0 else 0.0

    ok({
        "active_contracts": active,
        "renewed_contracts": renewed,
        "terminated_contracts": terminated,
        "expired_contracts": expired,
        "total_decided": total,
        "win_rate_pct": win_rate,
    })


# ===========================================================================
# 4. marketing-dashboard
# ===========================================================================
def marketing_dashboard(conn, args):
    _validate_company(conn, args.company_id)

    # Campaigns
    total_campaigns = conn.execute(
        "SELECT COUNT(*) FROM crmadv_email_campaign WHERE company_id = ?",
        (args.company_id,)
    ).fetchone()[0]
    sent_campaigns = conn.execute(
        "SELECT COUNT(*) FROM crmadv_email_campaign WHERE company_id = ? AND campaign_status = 'sent'",
        (args.company_id,)
    ).fetchone()[0]

    # Automation
    active_workflows = conn.execute(
        "SELECT COUNT(*) FROM crmadv_automation_workflow WHERE company_id = ? AND workflow_status = 'active'",
        (args.company_id,)
    ).fetchone()[0]
    active_sequences = conn.execute(
        "SELECT COUNT(*) FROM crmadv_nurture_sequence WHERE company_id = ? AND sequence_status = 'active'",
        (args.company_id,)
    ).fetchone()[0]

    # Territories
    active_territories = conn.execute(
        "SELECT COUNT(*) FROM crmadv_territory WHERE company_id = ? AND territory_status = 'active'",
        (args.company_id,)
    ).fetchone()[0]

    # Contracts
    active_contracts = conn.execute(
        "SELECT COUNT(*) FROM crmadv_contract WHERE company_id = ? AND contract_status IN ('active','renewed')",
        (args.company_id,)
    ).fetchone()[0]

    ok({
        "total_campaigns": total_campaigns,
        "sent_campaigns": sent_campaigns,
        "active_workflows": active_workflows,
        "active_nurture_sequences": active_sequences,
        "active_territories": active_territories,
        "active_contracts": active_contracts,
    })


# ===========================================================================
# 5. status
# ===========================================================================
def status_action(conn, args):
    tables = [
        "crmadv_email_campaign", "crmadv_campaign_template",
        "crmadv_recipient_list", "crmadv_campaign_event",
        "crmadv_territory", "crmadv_territory_assignment",
        "crmadv_territory_quota",
        "crmadv_contract", "crmadv_contract_obligation",
        "crmadv_automation_workflow", "crmadv_lead_score_rule",
        "crmadv_nurture_sequence",
    ]
    counts = {}
    for tbl in tables:
        try:
            counts[tbl] = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        except Exception:
            counts[tbl] = -1
    ok({
        "skill": "erpclaw-crm-adv",
        "version": "1.0.0",
        "total_tables": len(tables),
        "record_counts": counts,
        "domains": ["campaigns", "territories", "contracts", "automation", "reports"],
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "funnel-analysis": funnel_analysis,
    "pipeline-velocity": pipeline_velocity,
    "win-loss-analysis": win_loss_analysis,
    "marketing-dashboard": marketing_dashboard,
    "status": status_action,
}
