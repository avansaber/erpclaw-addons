---
name: erpclaw-integrations-stripe
version: 1.0.0
description: Deep Stripe integration -- 67 actions across 10 domains. Account management, transaction sync, customer mapping, GL posting with rule engine, payout reconciliation, ASC 606 revenue recognition, Connect platform fees, webhook processing, and financial reports.
author: avansaber
homepage: https://github.com/avansaber/erpclaw-addons
source: https://github.com/avansaber/erpclaw-addons
tier: 4
category: integrations
requires: [erpclaw]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, stripe, payments, gateway, charges, refunds, disputes, payouts, subscriptions, webhooks, connect, reconciliation, gl-posting, mrr, asc606, revenue-recognition]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action stripe-status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH","STRIPE_API_KEY"]},"os":["darwin","linux"]}}
---

# ERPClaw Integrations -- Stripe

Deep Stripe integration that syncs every transaction into your ERP general ledger.
Manages Stripe accounts, syncs charges/refunds/disputes/payouts/subscriptions,
maps Stripe customers to ERP customers, posts GL entries with configurable rules,
reconciles payouts, tracks Connect platform fees, processes webhooks, and generates
financial reports (revenue, MRR, fees, disputes).

### Skill Activation Triggers

Activate when user mentions: Stripe, payment gateway, payment processor, charges,
refunds, disputes, chargebacks, payouts, subscriptions, MRR, webhooks, Connect,
platform fees, payment reconciliation, Stripe sync.

### Setup

The ERPClaw Accounting app uses **restricted API keys** (starting with `rk_test_` or `rk_live_`), NOT standard secret keys.
Users get their restricted key from the Stripe Dashboard: **Installed Apps > ERPClaw Accounting > View API Keys**.
When a user says "connect Stripe" or "set up Stripe", ask them for their restricted API key (starts with `rk_test_` or `rk_live_`).
Do NOT ask for publishable keys (pk_) or standard secret keys (sk_). Only restricted keys (rk_) are accepted.

```
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action stripe-add-account --company-id {id} --account-name "Main" --api-key "rk_test_..."
python3 {baseDir}/scripts/db_query.py --action stripe-test-connection --stripe-account-id {id}
```

## Quick Start
```
--action stripe-add-account --company-id {id} --account-name "Main Stripe" --api-key "rk_test_..." --mode test
--action stripe-configure-gl-mapping --stripe-account-id {id} --clearing-account-id {id}
--action stripe-start-sync --stripe-account-id {id}
--action stripe-run-reconciliation --stripe-account-id {id}
--action stripe-revenue-report --stripe-account-id {id}
```

## All 67 Actions

### Account Management (6 actions)
| Action | Description |
|--------|-------------|
| `stripe-add-account` | Create Stripe account config (auto-creates GL accounts) |
| `stripe-update-account` | Update account name/key/mode |
| `stripe-get-account` | View account with masked key |
| `stripe-list-accounts` | List Stripe accounts |
| `stripe-configure-gl-mapping` | Update GL account mapping |
| `stripe-test-connection` | Verify API key works |

### Sync & Jobs (6 actions)
| Action | Description |
|--------|-------------|
| `stripe-start-sync` | Start incremental sync |
| `stripe-start-full-sync` | Start full historical sync |
| `stripe-get-sync-status` | Get sync job status |
| `stripe-list-sync-jobs` | List sync jobs |
| `stripe-cancel-sync` | Cancel running sync |
| `stripe-status` | Overall Stripe status |

### Transaction Browse (10 actions)
| Action | Description |
|--------|-------------|
| `stripe-list-charges` | List synced charges |
| `stripe-get-charge` | Get charge details |
| `stripe-list-refunds` | List refunds |
| `stripe-list-disputes` | List disputes |
| `stripe-list-payouts` | List payouts |
| `stripe-get-payout` | Get payout with transactions |
| `stripe-list-invoices` | List Stripe invoices |
| `stripe-list-subscriptions` | List subscriptions |
| `stripe-list-webhook-events` | List webhook events |
| `stripe-health-check` | Stripe health check |

### Customer Mapping (4 actions)
| Action | Description |
|--------|-------------|
| `stripe-map-customer` | Map Stripe customer to ERP |
| `stripe-unmap-customer` | Remove customer mapping |
| `stripe-list-customer-maps` | List customer mappings |
| `stripe-auto-map-customers` | Auto-map by email match |

### GL Posting (8 actions)
| Action | Description |
|--------|-------------|
| `stripe-post-charge-gl` | Post charge to GL |
| `stripe-post-refund-gl` | Post refund to GL |
| `stripe-post-dispute-gl` | Post dispute to GL |
| `stripe-post-payout-gl` | Post payout to GL |
| `stripe-post-connect-fee-gl` | Post Connect fee to GL |
| `stripe-preview-gl-posting` | Preview GL posting |
| `stripe-bulk-post-gl` | Bulk post unposted items |
| `stripe-verify-gl-balance` | Verify GL balance |

### GL Rules (4 actions)
| Action | Description |
|--------|-------------|
| `stripe-add-gl-rule` | Create custom GL posting rule |
| `stripe-update-gl-rule` | Update GL rule |
| `stripe-delete-gl-rule` | Delete GL rule |
| `stripe-list-gl-rules` | List GL rules |

### Reconciliation (7 actions)
| Action | Description |
|--------|-------------|
| `stripe-run-reconciliation` | Run payout reconciliation |
| `stripe-get-reconciliation-run` | Get reconciliation run |
| `stripe-list-reconciliation-runs` | List reconciliation runs |
| `stripe-reconcile-payout` | Reconcile specific payout |
| `stripe-match-charge` | Match charge to ERP invoice |
| `stripe-unmatch-charge` | Remove charge match |
| `stripe-list-unreconciled` | List unreconciled items |

### Webhooks (3 actions)
| Action | Description |
|--------|-------------|
| `stripe-process-webhook` | Process incoming webhook |
| `stripe-replay-webhook` | Replay webhook event |
| `stripe-get-customer-detail` | Get mapped customer detail |

### Connect Platform (4 actions)
| Action | Description |
|--------|-------------|
| `stripe-list-connected-accounts` | List connected accounts |
| `stripe-list-application-fees` | List application fees |
| `stripe-list-transfers` | List transfers |
| `stripe-connect-fee-summary` | Connect fee summary |

### Revenue Recognition / ASC 606 (5 actions)
| Action | Description |
|--------|-------------|
| `stripe-create-rev-rec-schedule` | Create ASC 606 revenue contract from Stripe subscription |
| `stripe-recognize-subscription-revenue` | Batch recognize deferred revenue for period |
| `stripe-rev-rec-status` | Revenue recognition status across subscriptions |
| `stripe-handle-subscription-change` | Handle subscription upgrade/downgrade/cancel |
| `stripe-rev-rec-summary` | Revenue recognition summary report |

### Reports (10 actions)
| Action | Description |
|--------|-------------|
| `stripe-revenue-report` | Revenue summary report |
| `stripe-fee-report` | Fee breakdown report |
| `stripe-dispute-report` | Dispute report |
| `stripe-payout-detail-report` | Payout detail report |
| `stripe-reconciliation-report` | Reconciliation report |
| `stripe-reconciliation-summary` | Reconciliation summary |
| `stripe-customer-revenue-report` | Revenue by customer |
| `stripe-mrr-report` | Monthly recurring revenue |
| `stripe-connect-payout-report` | Connect payout report |
| `stripe-connect-revenue-report` | Connect revenue report |

## Tables (17)
stripe_account, stripe_sync_job, stripe_balance_transaction, stripe_charge, stripe_refund, stripe_dispute, stripe_payout, stripe_invoice, stripe_subscription, stripe_customer_map, stripe_deep_webhook_event, stripe_credit_note, stripe_application_fee, stripe_transfer, stripe_gl_rule, stripe_fee_detail, stripe_reconciliation_run.
