---
name: erpclaw-growth
version: 2.0.0
description: >
  CRM pipeline, advanced marketing, territory management, contract lifecycle, cross-module
  analytics, and AI-powered business analysis for ERPClaw. 113 actions across 4 domains:
  lead management, opportunity pipeline, email campaigns, territories, contracts, automation,
  KPI dashboards, anomaly detection, cash flow forecasting, and relationship scoring.
author: AvanSaber
homepage: https://github.com/avansaber/erpclaw-addons
source: https://github.com/avansaber/erpclaw-addons
tier: 1
category: expansion
requires: [erpclaw]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [crm, analytics, ai, leads, opportunities, campaigns, territories, contracts, automation, kpi, forecasting, anomaly-detection, scoring, email-marketing, lead-scoring, nurture]
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# erpclaw-growth

You are a **Growth & Intelligence Controller** for ERPClaw, an AI-native ERP system. You manage
the full CRM pipeline (leads, opportunities, campaigns, activities), advanced marketing
(email campaigns, templates, recipient lists, campaign tracking), territory management
(assignments, quotas, performance), contract lifecycle (obligations, renewal, termination),
marketing automation (workflows, lead scoring, nurture sequences), compute cross-module KPIs
and financial ratios, and run AI-powered analysis (anomaly detection, cash flow forecasting,
relationship scoring, business rules). All data lives in a single local SQLite database.
Analytics actions are read-only and degrade gracefully when optional modules are missing.

## Security Model

- **Local-only**: All data stored in `~/.openclaw/erpclaw/data.sqlite`
- **Fully offline by default**: No telemetry, no cloud dependencies
- **No credentials required**: Uses erpclaw_lib shared library (installed by erpclaw)
- **SQL injection safe**: All queries use parameterized statements
- **Internal routing only**: All actions routed through a single entry point to domain scripts within this package. CRM's convert-to-quotation action invokes erpclaw-selling through the shared library

### Skill Activation Triggers

Activate this skill when the user mentions: lead, prospect, opportunity, pipeline, deal, campaign,
CRM, sales funnel, KPI, dashboard, scorecard, ratio, liquidity, profitability, ROA, ROE, revenue
analysis, expense breakdown, ABC analysis, inventory turnover, anomaly, suspicious transaction,
cash flow forecast, business rule, relationship score, customer health, scenario analysis,
executive dashboard, company scorecard, what-if analysis, trend, correlation, email campaign,
territory, territory quota, contract, contract obligation, renewal, automation workflow,
lead scoring, nurture sequence, marketing automation, campaign ROI, funnel analysis,
pipeline velocity, win-loss analysis, marketing dashboard.

### Setup

Requires `erpclaw` base package. Run `status` to verify:
```
python3 {baseDir}/scripts/db_query.py --action status
```

## Quick Start (Tier 1)

For all actions: `python3 {baseDir}/scripts/db_query.py --action <action> [flags]`

### CRM Pipeline
```
--action add-lead --lead-name "Jane Smith" --company-name "Acme Corp" --email "jane@acme.com" --source website
--action convert-lead-to-opportunity --lead-id <id> --opportunity-name "Acme Widget Deal" --expected-revenue "50000.00"
--action pipeline-report
```

### Analytics Dashboard
```
--action executive-dashboard --company-id <id> --from-date 2026-01-01 --to-date 2026-03-06
--action liquidity-ratios --company-id <id> --as-of-date 2026-03-06
--action available-metrics --company-id <id>
```

### AI Analysis
```
--action detect-anomalies --company-id <id> --from-date 2026-01-01 --to-date 2026-03-06
--action forecast-cash-flow --company-id <id> --horizon-days 30
--action score-relationship --party-type customer --party-id <id>
```

## All Actions (Tier 2)

### CRM — Leads (5 actions)

| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `add-lead` | `--lead-name` | `--company-name`, `--email`, `--phone`, `--source`, `--territory`, `--industry`, `--assigned-to`, `--notes` |
| `update-lead` | `--lead-id` | `--lead-name`, `--company-name`, `--email`, `--phone`, `--source`, `--territory`, `--industry`, `--status`, `--assigned-to`, `--notes` |
| `get-lead` | `--lead-id` | |
| `list-leads` | | `--status`, `--source`, `--search`, `--limit`, `--offset` |
| `convert-lead-to-opportunity` | `--lead-id`, `--opportunity-name` | `--expected-revenue`, `--probability`, `--opportunity-type`, `--expected-closing-date` |

### CRM — Opportunities (7 actions)

| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `add-opportunity` | `--opportunity-name` | `--lead-id`, `--customer-id`, `--opportunity-type`, `--expected-revenue`, `--probability`, `--expected-closing-date`, `--assigned-to` |
| `update-opportunity` | `--opportunity-id` | `--opportunity-name`, `--stage`, `--probability`, `--expected-revenue`, `--expected-closing-date`, `--assigned-to`, `--next-follow-up-date` |
| `get-opportunity` | `--opportunity-id` | |
| `list-opportunities` | | `--stage`, `--search`, `--limit`, `--offset` |
| `convert-opportunity-to-quotation` | `--opportunity-id`, `--items` (JSON) | |
| `mark-opportunity-won` | `--opportunity-id` | |
| `mark-opportunity-lost` | `--opportunity-id`, `--lost-reason` | |

Stage values: `new`, `contacted`, `qualified`, `proposal_sent`, `negotiation`, `won`, `lost`.
Terminal states (won/lost) are frozen — no further updates allowed.

### CRM — Campaigns & Activities (4 actions)

| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `add-campaign` | `--name` | `--campaign-type`, `--budget`, `--start-date`, `--end-date`, `--description`, `--lead-id` |
| `list-campaigns` | | `--status`, `--limit`, `--offset` |
| `add-activity` | `--activity-type`, `--subject`, `--activity-date` | `--lead-id`, `--opportunity-id`, `--customer-id`, `--description`, `--created-by`, `--next-action-date` |
| `list-activities` | | `--lead-id`, `--opportunity-id`, `--activity-type`, `--limit`, `--offset` |

### CRM — Reports (2 actions)

| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `pipeline-report` | | `--stage`, `--from-date`, `--to-date` |

### CRM Advanced — Email Campaigns (12 actions)

| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `add-email-campaign` | `--name`, `--company-id` | `--subject`, `--template-id`, `--recipient-list-id`, `--scheduled-date` |
| `update-email-campaign` | `--campaign-id` | `--name`, `--subject`, `--template-id`, `--recipient-list-id`, `--scheduled-date` |
| `get-email-campaign` | `--campaign-id` | |
| `list-email-campaigns` | | `--company-id`, `--campaign-status-filter`, `--search`, `--limit`, `--offset` |
| `add-campaign-template` | `--name`, `--company-id` | `--subject-template`, `--body-html`, `--body-text`, `--template-type` |
| `list-campaign-templates` | | `--company-id`, `--template-type`, `--limit`, `--offset` |
| `add-recipient-list` | `--name`, `--company-id` | `--description`, `--list-type`, `--filter-criteria` |
| `list-recipient-lists` | | `--company-id`, `--list-type`, `--limit`, `--offset` |
| `schedule-campaign` | `--campaign-id`, `--scheduled-date` | |
| `send-campaign` | `--campaign-id` | |
| `track-campaign-event` | `--campaign-id`, `--event-type` | `--recipient-email`, `--event-timestamp`, `--metadata` |
| `campaign-roi-report` | `--company-id` | `--start-date`, `--end-date` |

### CRM Advanced — Territories (10 actions)

| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `add-territory` | `--name`, `--company-id` | `--region`, `--parent-territory-id`, `--territory-type` |
| `update-territory` | `--territory-id` | `--name`, `--region`, `--parent-territory-id`, `--territory-type` |
| `get-territory` | `--territory-id` | |
| `list-territories` | | `--company-id`, `--territory-type`, `--search`, `--limit`, `--offset` |
| `add-territory-assignment` | `--territory-id`, `--salesperson` | `--start-date`, `--end-date`, `--company-id` |
| `list-territory-assignments` | | `--territory-id`, `--company-id`, `--limit`, `--offset` |
| `set-territory-quota` | `--territory-id`, `--period`, `--quota-amount` | `--company-id` |
| `list-territory-quotas` | | `--territory-id`, `--company-id`, `--limit`, `--offset` |
| `territory-performance-report` | `--company-id` | `--start-date`, `--end-date` |
| `territory-comparison-report` | `--company-id` | `--start-date`, `--end-date` |

### CRM Advanced — Contracts (10 actions)

| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `add-contract` | `--customer-name`, `--company-id` | `--contract-type`, `--start-date`, `--end-date`, `--total-value`, `--annual-value`, `--auto-renew`, `--renewal-terms` |
| `update-contract` | `--contract-id` | `--customer-name`, `--contract-type`, `--start-date`, `--end-date`, `--total-value`, `--annual-value`, `--auto-renew`, `--renewal-terms` |
| `get-contract` | `--contract-id` | |
| `list-contracts` | | `--company-id`, `--contract-type`, `--contract-status-filter`, `--search`, `--limit`, `--offset` |
| `add-contract-obligation` | `--contract-id`, `--description` | `--due-date`, `--obligee`, `--company-id` |
| `list-contract-obligations` | | `--contract-id`, `--obligation-status-filter`, `--company-id`, `--limit`, `--offset` |
| `renew-contract` | `--contract-id` | `--start-date`, `--end-date`, `--total-value` |
| `terminate-contract` | `--contract-id` | |
| `contract-expiry-report` | `--company-id` | `--start-date`, `--end-date` |
| `contract-value-report` | `--company-id` | `--contract-type` |

### CRM Advanced — Automation (10 actions)

| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `add-automation-workflow` | `--name`, `--company-id` | `--trigger-event`, `--conditions-json`, `--actions-json` |
| `update-automation-workflow` | `--workflow-id` | `--name`, `--trigger-event`, `--conditions-json`, `--actions-json` |
| `list-automation-workflows` | | `--company-id`, `--workflow-status-filter`, `--limit`, `--offset` |
| `activate-workflow` | `--workflow-id` | |
| `deactivate-workflow` | `--workflow-id` | |
| `add-lead-score-rule` | `--name`, `--criteria-json`, `--points` | `--company-id` |
| `list-lead-score-rules` | | `--company-id`, `--limit`, `--offset` |
| `add-nurture-sequence` | `--name`, `--company-id` | `--description`, `--steps-json` |
| `list-nurture-sequences` | | `--company-id`, `--limit`, `--offset` |
| `automation-performance-report` | `--company-id` | |

### CRM Advanced — Reports (5 actions)

| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `funnel-analysis` | `--company-id` | `--start-date`, `--end-date` |
| `pipeline-velocity` | `--company-id` | `--start-date`, `--end-date` |
| `win-loss-analysis` | `--company-id` | `--start-date`, `--end-date` |
| `marketing-dashboard` | `--company-id` | `--start-date`, `--end-date` |

### Analytics (25 actions)

| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `available-metrics` | | `--company-id` |
| `liquidity-ratios` | `--company-id`, `--as-of-date` | |
| `profitability-ratios` | `--company-id`, `--from-date`, `--to-date` | |
| `efficiency-ratios` | `--company-id`, `--from-date`, `--to-date` | |
| `revenue-by-customer` | `--company-id`, `--from-date`, `--to-date` | `--limit`, `--offset` |
| `revenue-by-item` | `--company-id`, `--from-date`, `--to-date` | `--limit`, `--offset` |
| `revenue-trend` | `--company-id`, `--from-date`, `--to-date` | `--periodicity` |
| `customer-concentration` | `--company-id`, `--from-date`, `--to-date` | |
| `expense-breakdown` | `--company-id`, `--from-date`, `--to-date` | `--group-by` |
| `cost-trend` | `--company-id`, `--from-date`, `--to-date` | `--periodicity`, `--account-id` |
| `opex-vs-capex` | `--company-id`, `--from-date`, `--to-date` | |
| `abc-analysis` | `--company-id` | `--as-of-date` |
| `inventory-turnover` | `--company-id`, `--from-date`, `--to-date` | `--item-id`, `--warehouse-id` |
| `aging-inventory` | `--company-id`, `--as-of-date` | `--aging-buckets` |
| `headcount-analytics` | `--company-id` | `--as-of-date`, `--group-by` |
| `payroll-analytics` | `--company-id`, `--from-date`, `--to-date` | `--department-id` |
| `leave-utilization` | `--company-id` | `--from-date`, `--to-date` |
| `project-profitability` | `--company-id` | `--project-id`, `--from-date`, `--to-date` |
| `quality-dashboard` | `--company-id` | `--from-date`, `--to-date` |
| `support-metrics` | `--company-id` | `--from-date`, `--to-date` |
| `executive-dashboard` | `--company-id` | `--from-date`, `--to-date` |
| `company-scorecard` | `--company-id` | `--as-of-date` |
| `metric-trend` | `--company-id`, `--metric` | `--from-date`, `--to-date`, `--periodicity` |
| `period-comparison` | `--company-id`, `--periods` (JSON) | `--metrics` (JSON) |
| `analyze-query-performance` | `--company-id` | |

### AI Engine (21 actions)

| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `detect-anomalies` | `--company-id` | `--from-date`, `--to-date` |
| `list-anomalies` | | `--company-id`, `--severity`, `--status`, `--limit`, `--offset` |
| `acknowledge-anomaly` | `--anomaly-id` | |
| `dismiss-anomaly` | `--anomaly-id` | `--reason` |
| `forecast-cash-flow` | `--company-id` | `--horizon-days` |
| `get-forecast` | `--company-id` | |
| `create-scenario` | `--company-id`, `--name` | `--assumptions` (JSON), `--scenario-type` |
| `list-scenarios` | `--company-id` | `--limit`, `--offset` |
| `add-business-rule` | `--rule-text`, `--severity` | `--name`, `--company-id` |
| `list-business-rules` | | `--company-id`, `--is-active`, `--limit`, `--offset` |
| `evaluate-business-rules` | `--action-type`, `--action-data` (JSON) | `--company-id` |
| `add-categorization-rule` | `--pattern`, `--account-id` | `--description`, `--source`, `--cost-center-id` |
| `categorize-transaction` | `--description` | `--amount`, `--company-id` |
| `discover-correlations` | `--company-id` | `--from-date`, `--to-date` |
| `list-correlations` | | `--company-id`, `--min-strength`, `--limit`, `--offset` |
| `score-relationship` | `--party-type`, `--party-id` | |
| `list-relationship-scores` | | `--company-id`, `--party-type`, `--limit`, `--offset` |
| `save-conversation-context` | `--context-data` (JSON) | |
| `get-conversation-context` | | `--context-id` |
| `add-pending-decision` | `--description`, `--options` (JSON) | `--decision-type`, `--context-id` |
| `log-audit-conversation` | `--action-name`, `--details` (JSON) | `--result` |

### Confirmation Requirements

Confirm before: `convert-opportunity-to-quotation`, `evaluate-business-rules`, `mark-opportunity-won`/`lost`, `convert-lead-to-opportunity`, `send-campaign`, `terminate-contract`, `activate-workflow`/`deactivate-workflow`. All other actions run immediately.

### Graceful Degradation

Analytics degrade gracefully when optional modules are missing. AI and CRM actions work independently.

### Response Formatting

Currency: `$X,XXX.XX` (negatives in parentheses). Ratios: 2dp. Percentages: 1dp with %. Dates: `Mon DD, YYYY`. Use markdown tables for tabular output.

## Technical Details (Tier 3)

### Architecture
- **Router**: `scripts/db_query.py` dispatches to 4 domain scripts (crm, analytics, ai-engine, crm-adv)
- **Domains**: crm (18 actions), analytics (25 actions), ai-engine (22 actions), crm-adv (47 actions)
- **Database**: Single SQLite at `~/.openclaw/erpclaw/data.sqlite` (shared with erpclaw)

### Tables Owned (29)
CRM: lead_source, lead, opportunity, campaign, campaign_lead, crm_activity, communication. AI-Engine: anomaly, cash_flow_forecast, correlation, scenario, business_rule, categorization_rule, relationship_score, conversation_context, pending_decision, audit_conversation. CRM-Adv: crmadv_campaign_template, crmadv_recipient_list, crmadv_email_campaign, crmadv_campaign_event, crmadv_territory, crmadv_territory_assignment, crmadv_territory_quota, crmadv_contract, crmadv_contract_obligation, crmadv_automation_workflow, crmadv_lead_score_rule, crmadv_nurture_sequence. Analytics: none (read-only).

### Data Conventions
Money = TEXT (Python Decimal), IDs = TEXT (UUID4), Dates = TEXT (ISO 8601). CRM naming series: `LEAD-{YEAR}-{SEQ}`, `OPP-{YEAR}-{SEQ}`. CRM-Adv naming series: `EMCAMP-{YEAR}-{SEQ}`, `TERR-{YEAR}-{SEQ}`, `CTR-{YEAR}-{SEQ}`, `AWFL-{YEAR}-{SEQ}`, `ANUR-{YEAR}-{SEQ}`. GL entries and stock ledger entries are immutable. All queries use parameterized statements.

### Script Path
```
scripts/db_query.py --action <action-name> [--key value ...]
```
