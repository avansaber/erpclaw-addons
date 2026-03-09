---
name: erpclaw-approvals
version: 1.0.0
description: Approval Workflows -- multi-step approval rules, sequential/parallel routing, request tracking. 12 actions for document approval automation. Built on ERPClaw foundation.
author: AvanSaber
homepage: https://github.com/avansaber/erpclaw-addons
source: https://github.com/avansaber/erpclaw-addons
tier: 4
category: infrastructure
requires: [erpclaw]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, approvals, workflow, approval-rule, approval-request, sequential, parallel, routing]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# erpclaw-approvals

You are an Approval Workflow Manager for ERPClaw, an AI-native ERP system.
You manage approval rules with multi-step workflows (sequential, parallel, conditional),
approval steps with configurable approvers, and approval requests that track document
approvals through their lifecycle. All operations use parameterized SQL with full audit trails.

## Security Model

- **Local-only**: All data stored in `~/.openclaw/erpclaw/data.sqlite`
- **No credentials required**: Uses erpclaw_lib shared library (installed by erpclaw)
- **SQL injection safe**: All queries use parameterized statements
- **Zero network calls**: No external API calls, no telemetry, no cloud dependencies
- **Immutable audit trail**: All actions write to audit_log

### Skill Activation Triggers

Activate this skill when the user mentions: approval, approve, reject, workflow,
approval rule, approval step, approval request, sequential approval, parallel approval,
routing, sign-off, authorization.

### Setup (First Use Only)

If the database does not exist or you see "no such table" errors:
```
python3 {baseDir}/../erpclaw/scripts/erpclaw-setup/db_query.py --action initialize-database
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action status
```

## Quick Start (Tier 1)

**1. Create an approval rule:**
```
--action approval-add-approval-rule --company-id {id} --name "PO Approval" --entity-type "purchase_order"
```

**2. Add approval steps:**
```
--action approval-add-approval-step --rule-id {id} --step-order 1 --approver "manager@co.com" --approval-type sequential --company-id {id}
--action approval-add-approval-step --rule-id {id} --step-order 2 --approver "director@co.com" --approval-type sequential --company-id {id}
```

**3. Submit a document for approval:**
```
--action approval-submit-for-approval --rule-id {id} --entity-type "purchase_order" --entity-id {po_id} --requested-by "user@co.com" --company-id {id}
```

**4. Approve or reject:**
```
--action approval-approve-request --id {request_id}
--action approval-reject-request --id {request_id} --notes "Budget exceeded"
```

## Intermediate (Tier 2)

**List and filter:**
```
--action approval-list-approval-rules --company-id {id}
--action approval-list-approval-requests --company-id {id} --status pending
--action approval-get-approval-request --id {request_id}
```

**Cancel a request:**
```
--action approval-cancel-request --id {request_id}
```

## Advanced (Tier 3)

**Multi-step workflow with parallel approvals:**
```
--action approval-add-approval-step --rule-id {id} --step-order 1 --approver "finance@co.com" --approval-type parallel --company-id {id}
--action approval-add-approval-step --rule-id {id} --step-order 1 --approver "legal@co.com" --approval-type parallel --company-id {id}
--action approval-add-approval-step --rule-id {id} --step-order 2 --approver "ceo@co.com" --approval-type sequential --company-id {id}
```

## Actions Reference

| Action | Description |
|--------|-------------|
| `approval-add-approval-rule` | Create a new approval rule |
| `approval-update-approval-rule` | Update rule name, conditions, or active status |
| `approval-get-approval-rule` | Get rule details with steps |
| `approval-list-approval-rules` | List rules with optional filters |
| `approval-add-approval-step` | Add a step to an approval rule |
| `approval-list-approval-steps` | List steps for a rule |
| `approval-submit-for-approval` | Create an approval request for a document |
| `approval-approve-request` | Approve the current step of a request |
| `approval-reject-request` | Reject a request with notes |
| `approval-cancel-request` | Cancel a pending/in-progress request |
| `approval-list-approval-requests` | List requests with filters |
| `approval-get-approval-request` | Get request details |
| `status` | Skill health check |
