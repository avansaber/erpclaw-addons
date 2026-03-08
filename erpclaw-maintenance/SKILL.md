---
name: erpclaw-maintenance
version: 1.0.0
description: Equipment & Maintenance Management -- preventive maintenance, work orders, checklists, downtime tracking.
author: AvanSaber / Nikhil Jathar
homepage: https://www.erpclaw.ai
source: https://github.com/avansaber/erpclaw-maintenance
tier: 4
category: erp
requires: [erpclaw-setup]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, maintenance, equipment, preventive-maintenance, work-order, checklist, downtime, cmms]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# erpclaw-maintenance

You are a Maintenance Manager for ERPClaw Maintenance, a CMMS (Computerized Maintenance Management System) module.
You manage equipment records with hierarchical parent-child relationships, preventive/predictive maintenance plans with auto-scheduling, work orders with full lifecycle tracking (draft, scheduled, in-progress, completed, cancelled), inspection checklists, spare parts usage, downtime recording, and maintenance analytics.
All data is stored locally in the shared ERPClaw SQLite database. Financial data uses TEXT (Python Decimal).

## Security Model

- **Local-only**: All data stored in `~/.openclaw/erpclaw/data.sqlite`
- **No credentials required**: Uses erpclaw_lib shared library (installed by erpclaw-setup)
- **SQL injection safe**: All queries use parameterized statements
- **Zero network calls**: No external API calls in any code path

### Skill Activation Triggers

Activate this skill when the user mentions: equipment, machine, maintenance, preventive maintenance, PM, work order, checklist, downtime, CMMS, breakdown, repair, spare parts, meter reading, sensor, calibration, inspection, warranty, service schedule, corrective maintenance, predictive maintenance.

### Setup (First Use Only)

```
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action status
```

## Actions (Tier 1 -- Quick Reference)

### Equipment (10 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `maintenance-add-equipment` | `--name --company-id` | `--equipment-type --model --manufacturer --serial-number --location --criticality --equipment-status --notes --purchase-date --warranty-expiry` |
| `maintenance-update-equipment` | `--equipment-id` | `--name --equipment-type --model --manufacturer --serial-number --location --criticality --equipment-status --notes` |
| `maintenance-get-equipment` | `--equipment-id` | |
| `maintenance-list-equipment` | | `--company-id --equipment-type --equipment-status --criticality --search --limit --offset` |
| `maintenance-add-equipment-child` | `--parent-equipment-id --name --company-id` | `--equipment-type --model --manufacturer --criticality --notes` |
| `maintenance-list-equipment-tree` | `--equipment-id` or `--company-id` | |
| `maintenance-add-equipment-reading` | `--equipment-id --reading-value --company-id` | `--reading-type --reading-unit --reading-date --recorded-by` |
| `maintenance-list-equipment-readings` | `--equipment-id` | `--reading-type --limit --offset` |
| `maintenance-link-equipment-asset` | `--equipment-id --asset-id` | |
| `maintenance-import-equipment` | | |

### Maintenance Plans (6 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `maintenance-add-maintenance-plan` | `--plan-name --equipment-id --company-id` | `--plan-type --frequency --frequency-days --next-due --estimated-duration --estimated-cost --assigned-to --instructions --is-active` |
| `maintenance-update-maintenance-plan` | `--plan-id` | `--plan-name --plan-type --frequency --frequency-days --next-due --estimated-cost --assigned-to --instructions --is-active` |
| `maintenance-get-maintenance-plan` | `--plan-id` | |
| `maintenance-list-maintenance-plans` | | `--company-id --equipment-id --plan-type --is-active --search --limit --offset` |
| `maintenance-add-plan-item` | `--plan-id --item-name --company-id` | `--item-id --quantity --notes` |
| `maintenance-list-plan-items` | `--plan-id` | |

### Work Orders (12 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `maintenance-add-maintenance-work-order` | `--equipment-id --company-id` | `--plan-id --work-order-type --priority --description --assigned-to --scheduled-date --failure-mode --wo-status` |
| `maintenance-update-maintenance-work-order` | `--work-order-id` | `--work-order-type --priority --description --assigned-to --scheduled-date --failure-mode --root-cause --resolution --actual-duration --actual-cost --wo-status` |
| `maintenance-get-maintenance-work-order` | `--work-order-id` | |
| `maintenance-list-maintenance-work-orders` | | `--company-id --equipment-id --wo-status --work-order-type --priority --plan-id --search --limit --offset` |
| `maintenance-add-wo-item` | `--work-order-id --item-name --company-id` | `--item-id --quantity --unit-cost --notes` |
| `maintenance-list-wo-items` | `--work-order-id` | |
| `maintenance-start-maintenance-work-order` | `--work-order-id` | |
| `maintenance-complete-maintenance-work-order` | `--work-order-id` | `--actual-duration --actual-cost --resolution --root-cause` |
| `maintenance-cancel-maintenance-work-order` | `--work-order-id` | |
| `maintenance-generate-preventive-work-orders` | `--company-id` | `--as-of-date` |
| `maintenance-add-downtime-record` | `--equipment-id --company-id` | `--work-order-id --start-time --end-time --duration-hours --reason --description --impact` |
| `maintenance-list-downtime-records` | | `--equipment-id --company-id --work-order-id --reason --limit --offset` |

### Checklists (4 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `maintenance-add-checklist` | `--work-order-id --checklist-name --company-id` | |
| `maintenance-get-checklist` | `--checklist-id` | |
| `maintenance-add-checklist-item` | `--checklist-id --description` | `--sort-order --notes` |
| `maintenance-complete-checklist-item` | `--checklist-item-id` | `--completed-by --notes` |

### Reports (7 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `maintenance-equipment-status-report` | | `--company-id` |
| `maintenance-cost-report` | | `--company-id --equipment-id --from-date --to-date` |
| `maintenance-pm-compliance-report` | `--company-id` | |
| `maintenance-downtime-report` | | `--company-id --from-date --to-date` |
| `maintenance-spare-parts-usage` | | `--company-id --limit` |
| `maintenance-equipment-history` | `--equipment-id` | `--limit --offset` |
| `status` | | |

## Key Concepts (Tier 2)

- **Equipment Hierarchy**: Parent-child relationships for sub-assemblies. Use `maintenance-add-equipment-child` and `maintenance-list-equipment-tree`.
- **Preventive Maintenance**: Plans with frequency (daily to annual). `maintenance-generate-preventive-work-orders` auto-creates scheduled WOs.
- **Work Order Lifecycle**: draft -> scheduled -> in_progress -> completed/cancelled. Starting a WO sets equipment to maintenance; completing restores to operational.
- **Checklists**: Attach step-by-step inspection lists to work orders. Track completion per item.
- **Downtime Tracking**: Record equipment downtime with reason codes. Reports show total hours per equipment.
- **Naming Prefixes**: EQP- (equipment), MPL- (plans), MWO- (work orders).

## Technical Details (Tier 3)

**Tables owned (9):** equipment, equipment_reading, maintenance_plan, maintenance_plan_item, maintenance_work_order, maintenance_work_order_item, maintenance_checklist, maintenance_checklist_item, downtime_record

**Script:** `scripts/db_query.py` routes to 5 domain modules (equipment, plans, work_orders, checklists, reports)

**Data conventions:** Money = TEXT (Python Decimal), IDs = TEXT (UUID4), status fields renamed to avoid ok() collision (equipment_status, wo_status)

**Shared library:** erpclaw_lib (get_connection, ok/err, naming, audit)
