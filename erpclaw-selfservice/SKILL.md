---
name: erpclaw-selfservice
version: 1.0.0
description: Generic self-service permission layer for ERPClaw. 25 actions across permission profiles, portal configuration, session management, and activity logging. Enables scoped access for employees, clients, tenants, patients, and vendors across all verticals.
author: AvanSaber
homepage: https://github.com/avansaber/erpclaw-addons
source: https://github.com/avansaber/erpclaw-addons
tier: 2
category: infrastructure
requires: [erpclaw]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, self-service, permissions, portal, session, access-control, rbac, employee-portal, client-portal, tenant-portal]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# erpclaw-selfservice

You are a Self-Service Portal Manager for ERPClaw. You manage permission profiles that control what actions
external users (employees, clients, tenants, patients, vendors) can perform through self-service portals.
You configure portal branding and settings, manage scoped sessions with expiration, enforce permission
checks on every action, and maintain a complete activity audit trail.

## Security Model

- **Local-only**: All data stored in `~/.openclaw/erpclaw/data.sqlite`
- **Zero network calls**: No external API calls, no telemetry, no cloud dependencies
- **No credentials required**: Uses erpclaw_lib shared library (installed by erpclaw)
- **SQL injection safe**: All queries use parameterized statements

### Skill Activation Triggers

Activate this skill when the user mentions: self-service, portal, permission profile, access control,
employee portal, client portal, tenant portal, patient portal, vendor portal, session management,
permission check, activity log, portal config, field visibility, record scope.

### Setup (First Use Only)

```
python3 {baseDir}/../erpclaw/scripts/erpclaw-setup/db_query.py --action initialize-database
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action status
```

## Quick Start (Tier 1)

**1. Create a permission profile:**
```
--action selfservice-add-profile --company-id {id} --name "Employee Self-Service" --target-role employee --allowed-actions '["list-leave","add-leave","get-payslip"]' --record-scope own
```

**2. Assign permission and create a portal:**
```
--action selfservice-add-permission --company-id {id} --profile-id {id} --user-id {uid} --user-email "jane@co.com"
--action selfservice-add-portal-config --company-id {id} --name "Employee Portal" --welcome-message "Welcome!"
```

**3. Session and permission check:**
```
--action selfservice-create-session --company-id {id} --user-id {uid} --profile-id {id} --token "abc123" --expires-at "2026-12-31T23:59:59Z"
--action selfservice-validate-permission --user-id {uid} --action-name "list-leave"
--action selfservice-get-session --token "abc123"
```

## All Actions (Tier 2)

For all actions: `python3 {baseDir}/scripts/db_query.py --action <action> [flags]`

### Profiles (4 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `selfservice-add-profile` | `--company-id --name --target-role` | `--description --allowed-actions --denied-actions --record-scope --field-visibility` |
| `selfservice-update-profile` | `--profile-id` | `--name --description --target-role --allowed-actions --denied-actions --record-scope --field-visibility` |
| `selfservice-get-profile` | `--profile-id` | |
| `selfservice-list-profiles` | | `--company-id --target-role --search --limit --offset` |

### Permissions (4 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `selfservice-add-permission` | `--company-id --profile-id --user-id` | `--user-email --user-name --assigned-by` |
| `selfservice-list-permissions` | | `--company-id --profile-id --user-id --limit --offset` |
| `selfservice-remove-permission` | `--permission-id` | |
| `selfservice-validate-permission` | `--user-id --action-name` | |

### Portal Configuration (6 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `selfservice-add-portal-config` | `--company-id --name` | `--branding-json --welcome-message --enabled-modules --enabled-actions --require-mfa --session-timeout-minutes` |
| `selfservice-update-portal-config` | `--portal-id` | `--name --branding-json --welcome-message --enabled-modules --enabled-actions --require-mfa --session-timeout-minutes` |
| `selfservice-get-portal-config` | `--portal-id` | |
| `selfservice-list-portal-configs` | | `--company-id --search --limit --offset` |
| `selfservice-activate-portal` | `--portal-id` | |
| `selfservice-deactivate-portal` | `--portal-id` | |

### Sessions (5 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `selfservice-create-session` | `--company-id --user-id --profile-id --token --expires-at` | `--portal-id --ip-address --user-agent` |
| `selfservice-get-session` | `--session-id` or `--token` | |
| `selfservice-list-sessions` | | `--company-id --user-id --profile-id --limit --offset` |
| `selfservice-expire-session` | `--session-id` | |
| `selfservice-list-active-sessions` | | `--company-id --user-id --limit --offset` |

### Reports + Status (6 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `selfservice-log-activity` | `--company-id --user-id --action-name` | `--session-id --entity-type --entity-id --result --ip-address` |
| `selfservice-usage-report` | `--company-id` | `--limit --offset` |
| `selfservice-portal-analytics-report` | `--company-id` | |
| `selfservice-permission-audit-report` | `--company-id` | `--limit --offset` |
| `selfservice-active-sessions-report` | `--company-id` | |
| `status` | | |

## Technical Details (Tier 3)

**Tables owned (5):** selfservice_permission_profile, selfservice_profile_assignment, selfservice_portal_config, selfservice_session, selfservice_activity_log

**Script:** `scripts/db_query.py` routes to 4 domain modules: `permissions.py`, `portal.py`, `sessions.py`, `reports.py`

**Data conventions:** IDs = TEXT (UUID4), Dates = TEXT (ISO 8601), Booleans = INTEGER (0/1), JSON = TEXT

**Shared library:** erpclaw_lib (get_connection, ok/err, row_to_dict, get_next_name, audit)
