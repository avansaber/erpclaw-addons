---
name: erpclaw-alerts
version: 1.0.0
description: >
  Configurable notification triggers -- low stock alerts, overdue invoice alerts, expiring contract alerts, custom rules. Channels: email, webhook, Telegram, SMS. 14 actions across alert rules, notification channels, and alert logs.
author: AvanSaber
homepage: https://github.com/avansaber/erpclaw-addons
source: https://github.com/avansaber/erpclaw-addons
tier: 5
category: infrastructure
requires: [erpclaw]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, alerts, notifications, triggers, low-stock, overdue, contract, webhook, telegram, email, monitoring]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# erpclaw-alerts

You are an Alert & Notification Manager for ERPClaw, an AI-native ERP system.
You manage configurable alert rules that trigger on business events (low stock, overdue invoices,
expiring contracts), notification channels (email, webhook, Telegram, SMS), and alert logs
with acknowledgment tracking. All data stored locally in SQLite with full audit trail.

## Security Model

- **Local-only**: All data stored in `~/.openclaw/erpclaw/data.sqlite`
- **No credentials required**: Uses erpclaw_lib shared library (installed by erpclaw)
- **SQL injection safe**: All queries use parameterized statements
- **Zero network calls**: No external API calls, no telemetry, no cloud dependencies
- **Immutable audit trail**: All actions write to audit_log

### Skill Activation Triggers

Activate this skill when the user mentions: alert, notification, trigger, low stock alert,
overdue invoice, expiring contract, webhook, alert rule, notification channel, alert log,
acknowledge alert, alert summary, monitoring, warning, critical alert.

### Setup (First Use Only)

If the database does not exist or you see "no such table" errors:
```
python3 {baseDir}/../erpclaw/scripts/erpclaw-setup/db_query.py --action initialize-database
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action status
```

## Quick Start (Tier 1)

**1. Create a notification channel:**
```
--action alert-add-notification-channel --company-id {id} --name "Admin Email" --channel-type email --config-json '{"email": "admin@company.com"}'
```

**2. Create an alert rule:**
```
--action alert-add-alert-rule --company-id {id} --name "Low Stock Alert" --entity-type stock_entry --condition-json '{"field": "qty", "operator": "lt", "value": 10}' --severity high --channel-ids '["channel-uuid"]'
```

**3. Trigger an alert:**
```
--action alert-trigger-alert --rule-id {id} --entity-id {entity-id} --message "Widget A stock below threshold"
```

**4. Acknowledge an alert:**
```
--action alert-acknowledge-alert --alert-log-id {id} --acknowledged-by "admin"
```

## All Actions (Tier 2)

For all actions: `python3 {baseDir}/scripts/db_query.py --action <action> [flags]`

### Alert Rules (6 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `alert-add-alert-rule` | `--company-id --name --entity-type --condition-json` | `--description --severity --channel-ids --cooldown-minutes --is-active` |
| `alert-update-alert-rule` | `--rule-id` | `--name --description --entity-type --condition-json --severity --channel-ids --cooldown-minutes` |
| `alert-get-alert-rule` | `--rule-id` | |
| `alert-list-alert-rules` | | `--company-id --entity-type --severity --is-active --search --limit --offset` |
| `alert-activate-alert-rule` | `--rule-id` | |
| `alert-deactivate-alert-rule` | `--rule-id` | |

### Notification Channels (3 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `alert-add-notification-channel` | `--company-id --name --channel-type --config-json` | `--is-active` |
| `alert-list-notification-channels` | | `--company-id --channel-type --is-active --search --limit --offset` |
| `alert-delete-notification-channel` | `--channel-id` | |

### Alert Logs (4 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `alert-trigger-alert` | `--rule-id --message` | `--entity-id --channel-results` |
| `alert-list-alert-logs` | | `--company-id --rule-id --severity --alert-status --entity-type --search --limit --offset` |
| `alert-acknowledge-alert` | `--alert-log-id --acknowledged-by` | |
| `alert-summary-report` | | `--company-id --start-date --end-date` |

### System (1 action)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `status` | | |

### Quick Command Reference
| User Says | Action |
|-----------|--------|
| "Set up an alert" | `alert-add-alert-rule` |
| "Add email channel" | `alert-add-notification-channel` |
| "Fire an alert" | `alert-trigger-alert` |
| "Show alert history" | `alert-list-alert-logs` |
| "I handled that alert" | `alert-acknowledge-alert` |
| "Alert stats" | `alert-summary-report` |

## Technical Details (Tier 3)

**Tables owned (3):** alert_rule, alert_log, notification_channel

**Script:** `scripts/db_query.py` routes to domain module: alerts.py

**Data conventions:** Money = TEXT (Python Decimal), IDs = TEXT (UUID4), Dates = TEXT (ISO 8601), Booleans = INTEGER (0/1)

**Shared library:** erpclaw_lib (get_connection, ok/err, row_to_dict, get_next_name, audit)
