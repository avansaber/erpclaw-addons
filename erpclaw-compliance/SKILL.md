---
name: erpclaw-compliance
version: 1.0.0
description: Compliance, audit, risk, and policy management for ERPClaw. 38 actions across 4 domains -- audit plans and findings, risk register with heat-map scoring, internal controls and compliance calendar, policy lifecycle with employee acknowledgment tracking.
author: avansaber
homepage: https://github.com/avansaber/erpclaw-compliance
source: https://github.com/avansaber/erpclaw-addons
tier: 5
category: compliance
requires: [erpclaw-setup]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [compliance, audit, risk, controls, policy, governance, sox, internal-audit, risk-register, compliance-calendar]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# erpclaw-compliance

You are a Compliance Manager for ERPClaw Compliance, handling audit planning, risk management,
internal controls testing, compliance calendar tracking, and policy lifecycle management.
You manage audit plans with findings and remediation, maintain a scored risk register with
likelihood/impact matrices, schedule and execute control tests, track compliance deadlines,
and manage policy documents with employee acknowledgment workflows.

## Security Model

- **Local-only**: All data stored in `~/.openclaw/erpclaw/data.sqlite`
- **No credentials required**: Uses erpclaw_lib shared library (installed by erpclaw-setup)
- **SQL injection safe**: All queries use parameterized statements
- **Immutable audit trail**: All actions write to audit_log

### Skill Activation Triggers

Activate this skill when the user mentions: audit, compliance, risk, control, policy,
governance, SOX, internal audit, risk register, risk matrix, finding, remediation,
control test, compliance calendar, filing deadline, policy acknowledgment, risk assessment.

### Setup (First Use Only)

If the database does not exist or you see "no such table" errors:
```
python3 {baseDir}/../erpclaw-setup/scripts/db_query.py --action initialize-database
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action status
```

## Quick Start (Tier 1)

**1. Create an audit plan:**
```
--action compliance-add-audit-plan --company-id {id} --name "Q1 Internal Audit" --audit-type internal --scope "Financial controls" --planned-start "2026-04-01" --planned-end "2026-04-30"
```

**2. Register a risk:**
```
--action compliance-add-risk --company-id {id} --name "Data breach risk" --category technology --likelihood 3 --impact 4 --mitigation-plan "Implement MFA and encryption"
```

**3. Schedule a control test:**
```
--action compliance-add-control-test --company-id {id} --control-name "Segregation of Duties" --control-type preventive --frequency quarterly --test-procedure "Review access logs"
```

**4. Track a compliance deadline:**
```
--action compliance-add-calendar-item --company-id {id} --title "Annual SOX Filing" --compliance-type filing --due-date "2026-06-30" --responsible "CFO"
```

**5. Publish a policy:**
```
--action compliance-add-policy --company-id {id} --title "Anti-Bribery Policy" --policy-type compliance --content "..." --requires-acknowledgment 1
--action compliance-publish-policy --policy-id {id}
```

## All Actions (Tier 2)

For all actions: `python3 {baseDir}/scripts/db_query.py --action <action> [flags]`

### Audit (8 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `compliance-add-audit-plan` | `--company-id --name --audit-type` | `--scope --lead-auditor --planned-start --planned-end --notes` |
| `compliance-update-audit-plan` | `--audit-plan-id` | `--name --scope --lead-auditor --planned-start --planned-end --notes` |
| `compliance-get-audit-plan` | `--audit-plan-id` | |
| `compliance-list-audit-plans` | | `--company-id --status --search --limit --offset` |
| `compliance-start-audit` | `--audit-plan-id` | |
| `compliance-complete-audit` | `--audit-plan-id` | |
| `compliance-add-audit-finding` | `--audit-plan-id --company-id --title --finding-type` | `--description --area --root-cause --recommendation --remediation-due --assigned-to --notes` |
| `compliance-list-audit-findings` | | `--audit-plan-id --company-id --finding-type --remediation-status --limit --offset` |

### Risk (8 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `compliance-add-risk` | `--company-id --name --category --likelihood --impact` | `--description --owner --mitigation-plan --review-date --notes` |
| `compliance-update-risk` | `--risk-id` | `--name --category --likelihood --impact --owner --mitigation-plan --residual-likelihood --residual-impact --review-date --status --notes` |
| `compliance-get-risk` | `--risk-id` | |
| `compliance-list-risks` | | `--company-id --category --status --risk-level --search --limit --offset` |
| `compliance-add-risk-assessment` | `--risk-id --company-id --likelihood --impact` | `--assessor --notes` |
| `compliance-list-risk-assessments` | | `--risk-id --limit --offset` |
| `compliance-risk-matrix-report` | `--company-id` | |
| `compliance-close-risk` | `--risk-id` | |

### Controls & Calendar (12 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `compliance-add-control-test` | `--company-id --control-name --control-type` | `--control-description --frequency --test-procedure --test-date --tester --evidence --next-test-date --notes` |
| `compliance-update-control-test` | `--control-test-id` | `--control-name --control-description --control-type --frequency --test-procedure --tester --evidence --next-test-date --notes` |
| `compliance-get-control-test` | `--control-test-id` | |
| `compliance-list-control-tests` | | `--company-id --control-type --test-result --frequency --search --limit --offset` |
| `compliance-execute-control-test` | `--control-test-id --test-result` | `--tester --evidence --notes` |
| `compliance-add-calendar-item` | `--company-id --title --compliance-type --due-date` | `--reminder-days --responsible --description --recurrence --notes` |
| `compliance-update-calendar-item` | `--calendar-item-id` | `--title --compliance-type --due-date --reminder-days --responsible --description --recurrence --notes` |
| `compliance-get-calendar-item` | `--calendar-item-id` | |
| `compliance-list-calendar-items` | | `--company-id --compliance-type --status --search --limit --offset` |
| `compliance-complete-calendar-item` | `--calendar-item-id` | |
| `compliance-overdue-items-report` | `--company-id` | |
| `compliance-dashboard` | `--company-id` | |

### Policy (10 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `compliance-add-policy` | `--company-id --title --policy-type` | `--version --content --effective-date --review-date --owner --requires-acknowledgment --notes` |
| `compliance-update-policy` | `--policy-id` | `--title --policy-type --version --content --review-date --owner --requires-acknowledgment --notes` |
| `compliance-get-policy` | `--policy-id` | |
| `compliance-list-policies` | | `--company-id --policy-type --status --search --limit --offset` |
| `compliance-publish-policy` | `--policy-id` | `--effective-date` |
| `compliance-retire-policy` | `--policy-id` | |
| `compliance-add-policy-acknowledgment` | `--policy-id --company-id --employee-name` | `--employee-id --ip-address --notes` |
| `compliance-list-policy-acknowledgments` | | `--policy-id --employee-id --limit --offset` |
| `compliance-policy-compliance-report` | `--company-id` | |
| `status` | | |

## Technical Details (Tier 3)

**Tables owned (8):** audit_plan, audit_finding, risk_register, risk_assessment, control_test, compliance_calendar, policy, policy_acknowledgment

**Script:** `scripts/db_query.py` routes to 4 domain modules: audit.py, risk.py, controls.py, policy.py

**Data conventions:** IDs = TEXT (UUID4), Dates = TEXT (ISO 8601), Booleans = INTEGER (0/1)

**Risk scoring:** risk_score = likelihood * impact, risk_level auto-set: 1-4=low, 5-9=medium, 10-15=high, 16-25=critical

**Shared library:** erpclaw_lib (get_connection, ok/err, row_to_dict, get_next_name, audit, check_required_tables)
