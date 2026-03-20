---
name: erpclaw-integrations-stripe
version: 1.0.0
description: Deep Stripe integration for full-cycle payment reconciliation — charges, refunds, disputes, payouts, subscriptions, GL posting.
author: avansaber
scripts:
  - scripts/db_query.py
actions:
  # ── Account Management (6) ────────────────────────────────────
  - stripe-add-account
  - stripe-update-account
  - stripe-get-account
  - stripe-list-accounts
  - stripe-configure-gl-mapping
  - stripe-test-connection
  - status
---

# ERPClaw Integrations — Stripe

Deep Stripe integration that syncs every transaction into your ERP's general ledger automatically.

## Tier 1 — Account Setup

### stripe-add-account
Create a Stripe account configuration. Auto-creates 5 GL accounts.
```
--company-id (required) --account-name (required) --api-key (required) --mode test|live
```

### stripe-get-account
View account details with masked API key.
```
--stripe-account-id (required)
```

### stripe-list-accounts
List all Stripe accounts for a company.
```
--company-id (required)
```

### stripe-update-account
Update account name, API key, mode, or status.
```
--stripe-account-id (required) [--account-name] [--api-key] [--mode] [--status]
```

### stripe-configure-gl-mapping
Update which GL accounts are used for Stripe transactions.
```
--stripe-account-id (required) [--clearing-account-id] [--fees-account-id] [--payout-account-id] [--dispute-account-id] [--unearned-revenue-account-id] [--platform-revenue-account-id]
```

### stripe-test-connection
Verify API key works by calling Stripe's Account API.
```
--stripe-account-id (required)
```

## Tier 2 — Sync & Reconciliation (planned)

Sync charges, refunds, disputes, payouts from Stripe. Auto-match to ERP invoices and customers. Run reconciliation reports.

## Tier 3 — Advanced (planned)

Subscription revenue recognition, Connect platform fee tracking, webhook-driven real-time sync, GL rule engine for custom posting logic.

## Tables (17)

| Table | Purpose |
|-------|---------|
| stripe_account | Account config + GL mapping |
| stripe_sync_job | Sync operation tracking |
| stripe_balance_transaction | Core reconciliation entity |
| stripe_charge | Payment charges |
| stripe_refund | Refunds |
| stripe_dispute | Chargebacks |
| stripe_payout | Bank payouts |
| stripe_invoice | Stripe invoices |
| stripe_subscription | Subscriptions |
| stripe_customer_map | Customer ID mapping |
| stripe_deep_webhook_event | Webhook events |
| stripe_credit_note | Credit notes |
| stripe_application_fee | Connect fees |
| stripe_transfer | Connect transfers |
| stripe_gl_rule | GL posting rules |
| stripe_fee_detail | Fee breakdown |
| stripe_reconciliation_run | Reconciliation runs |
