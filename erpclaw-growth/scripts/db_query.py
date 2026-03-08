#!/usr/bin/env python3
"""ERPClaw-Growth v2 — Unified router for 113 actions across 4 domains.

Routes --action to the correct domain script via os.execvp().
Domains: crm, analytics, ai-engine, crm-adv.

Usage: python3 db_query.py --action <action-name> [--flags ...]
Output: JSON to stdout (passed through from domain script)
"""
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Action → domain mapping (109 unique actions + 4 status aliases)
# Only collision: `status` in all 4 domains → routes to analytics.
ACTION_MAP = {
    # === CRM (18 actions) ===
    "add-lead": "erpclaw-crm",
    "update-lead": "erpclaw-crm",
    "get-lead": "erpclaw-crm",
    "list-leads": "erpclaw-crm",
    "convert-lead-to-opportunity": "erpclaw-crm",
    "add-opportunity": "erpclaw-crm",
    "update-opportunity": "erpclaw-crm",
    "get-opportunity": "erpclaw-crm",
    "list-opportunities": "erpclaw-crm",
    "convert-opportunity-to-quotation": "erpclaw-crm",
    "mark-opportunity-won": "erpclaw-crm",
    "mark-opportunity-lost": "erpclaw-crm",
    "add-campaign": "erpclaw-crm",
    "list-campaigns": "erpclaw-crm",
    "add-activity": "erpclaw-crm",
    "list-activities": "erpclaw-crm",
    "pipeline-report": "erpclaw-crm",

    # === Analytics (26 actions) ===
    "status": "erpclaw-analytics",
    "available-metrics": "erpclaw-analytics",
    "liquidity-ratios": "erpclaw-analytics",
    "profitability-ratios": "erpclaw-analytics",
    "efficiency-ratios": "erpclaw-analytics",
    "revenue-by-customer": "erpclaw-analytics",
    "revenue-by-item": "erpclaw-analytics",
    "revenue-trend": "erpclaw-analytics",
    "customer-concentration": "erpclaw-analytics",
    "expense-breakdown": "erpclaw-analytics",
    "cost-trend": "erpclaw-analytics",
    "opex-vs-capex": "erpclaw-analytics",
    "abc-analysis": "erpclaw-analytics",
    "inventory-turnover": "erpclaw-analytics",
    "aging-inventory": "erpclaw-analytics",
    "headcount-analytics": "erpclaw-analytics",
    "payroll-analytics": "erpclaw-analytics",
    "leave-utilization": "erpclaw-analytics",
    "project-profitability": "erpclaw-analytics",
    "quality-dashboard": "erpclaw-analytics",
    "support-metrics": "erpclaw-analytics",
    "executive-dashboard": "erpclaw-analytics",
    "company-scorecard": "erpclaw-analytics",
    "metric-trend": "erpclaw-analytics",
    "period-comparison": "erpclaw-analytics",

    # === AI Engine (22 actions) ===
    "detect-anomalies": "erpclaw-ai-engine",
    "list-anomalies": "erpclaw-ai-engine",
    "acknowledge-anomaly": "erpclaw-ai-engine",
    "dismiss-anomaly": "erpclaw-ai-engine",
    "forecast-cash-flow": "erpclaw-ai-engine",
    "get-forecast": "erpclaw-ai-engine",
    "discover-correlations": "erpclaw-ai-engine",
    "list-correlations": "erpclaw-ai-engine",
    "create-scenario": "erpclaw-ai-engine",
    "list-scenarios": "erpclaw-ai-engine",
    "add-business-rule": "erpclaw-ai-engine",
    "list-business-rules": "erpclaw-ai-engine",
    "evaluate-business-rules": "erpclaw-ai-engine",
    "add-categorization-rule": "erpclaw-ai-engine",
    "categorize-transaction": "erpclaw-ai-engine",
    "score-relationship": "erpclaw-ai-engine",
    "list-relationship-scores": "erpclaw-ai-engine",
    "save-conversation-context": "erpclaw-ai-engine",
    "get-conversation-context": "erpclaw-ai-engine",
    "add-pending-decision": "erpclaw-ai-engine",
    "log-audit-conversation": "erpclaw-ai-engine",

    # === CRM Advanced — Campaigns (12 actions) ===
    "add-email-campaign": "erpclaw-crm-adv",
    "update-email-campaign": "erpclaw-crm-adv",
    "get-email-campaign": "erpclaw-crm-adv",
    "list-email-campaigns": "erpclaw-crm-adv",
    "add-campaign-template": "erpclaw-crm-adv",
    "list-campaign-templates": "erpclaw-crm-adv",
    "add-recipient-list": "erpclaw-crm-adv",
    "list-recipient-lists": "erpclaw-crm-adv",
    "schedule-campaign": "erpclaw-crm-adv",
    "send-campaign": "erpclaw-crm-adv",
    "track-campaign-event": "erpclaw-crm-adv",
    "campaign-roi-report": "erpclaw-crm-adv",

    # === CRM Advanced — Territories (10 actions) ===
    "add-territory": "erpclaw-crm-adv",
    "update-territory": "erpclaw-crm-adv",
    "get-territory": "erpclaw-crm-adv",
    "list-territories": "erpclaw-crm-adv",
    "add-territory-assignment": "erpclaw-crm-adv",
    "list-territory-assignments": "erpclaw-crm-adv",
    "set-territory-quota": "erpclaw-crm-adv",
    "list-territory-quotas": "erpclaw-crm-adv",
    "territory-performance-report": "erpclaw-crm-adv",
    "territory-comparison-report": "erpclaw-crm-adv",

    # === CRM Advanced — Contracts (10 actions) ===
    "add-contract": "erpclaw-crm-adv",
    "update-contract": "erpclaw-crm-adv",
    "get-contract": "erpclaw-crm-adv",
    "list-contracts": "erpclaw-crm-adv",
    "add-contract-obligation": "erpclaw-crm-adv",
    "list-contract-obligations": "erpclaw-crm-adv",
    "renew-contract": "erpclaw-crm-adv",
    "terminate-contract": "erpclaw-crm-adv",
    "contract-expiry-report": "erpclaw-crm-adv",
    "contract-value-report": "erpclaw-crm-adv",

    # === CRM Advanced — Automation (10 actions) ===
    "add-automation-workflow": "erpclaw-crm-adv",
    "update-automation-workflow": "erpclaw-crm-adv",
    "list-automation-workflows": "erpclaw-crm-adv",
    "activate-workflow": "erpclaw-crm-adv",
    "deactivate-workflow": "erpclaw-crm-adv",
    "add-lead-score-rule": "erpclaw-crm-adv",
    "list-lead-score-rules": "erpclaw-crm-adv",
    "add-nurture-sequence": "erpclaw-crm-adv",
    "list-nurture-sequences": "erpclaw-crm-adv",
    "automation-performance-report": "erpclaw-crm-adv",

    # === CRM Advanced — Reports (4 actions) ===
    "funnel-analysis": "erpclaw-crm-adv",
    "pipeline-velocity": "erpclaw-crm-adv",
    "win-loss-analysis": "erpclaw-crm-adv",
    "marketing-dashboard": "erpclaw-crm-adv",
}

# Aliases: domain-specific status commands
ALIASES = {
    "crm-status": ("erpclaw-crm", "status"),
    "analytics-status": ("erpclaw-analytics", "status"),
    "ai-status": ("erpclaw-ai-engine", "status"),
    "crm-adv-status": ("erpclaw-crm-adv", "status"),
}


def find_action():
    """Extract --action value from sys.argv."""
    for i, arg in enumerate(sys.argv):
        if arg == "--action" and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return None


def forward(domain, action_override=None):
    """Forward execution to the domain script via os.execvp."""
    script = os.path.join(BASE_DIR, domain, "db_query.py")
    if not os.path.isfile(script):
        print(json.dumps({
            "status": "error",
            "error": f"Domain script not found: {domain}/db_query.py"
        }))
        sys.exit(1)

    args = list(sys.argv[1:])

    if action_override:
        for i, arg in enumerate(args):
            if arg == "--action" and i + 1 < len(args):
                args[i + 1] = action_override
                break

    os.execvp(sys.executable, [sys.executable, script] + args)


def main():
    action = find_action()
    if not action:
        print(json.dumps({
            "status": "error",
            "error": "Missing --action flag. Usage: python3 db_query.py --action <action-name> [flags]"
        }))
        sys.exit(1)

    if action in ALIASES:
        domain, original_action = ALIASES[action]
        forward(domain, action_override=original_action)
        return

    domain = ACTION_MAP.get(action)
    if not domain:
        print(json.dumps({
            "status": "error",
            "error": f"Unknown action: {action}",
            "hint": "Run --action status for system overview"
        }))
        sys.exit(1)

    forward(domain)


if __name__ == "__main__":
    main()
