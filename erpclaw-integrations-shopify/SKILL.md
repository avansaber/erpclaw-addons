---
name: erpclaw-integrations-shopify
version: 1.1.2
description: Deep Shopify integration -- 66 actions across 15 domains. Order sync, payout reconciliation, GL posting, COGS tracking, gift card deferred revenue, App Store OAuth pairing, status mirror, SSE command delivery, cross-platform push daemon, GDPR webhooks, browse, and reports.
author: avansaber
homepage: https://github.com/avansaber/erpclaw-addons
source: https://github.com/avansaber/erpclaw-addons
tier: 4
category: integrations
requires: [erpclaw]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, shopify, ecommerce, orders, refunds, payouts, disputes, products, customers, sync, webhooks, reconciliation, gl-mapping, reports, browse, oauth, app-store, gdpr]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# ERPClaw Integrations -- Shopify

Deep Shopify integration that syncs every order, refund, payout, and dispute into your
ERP general ledger. Manages Shopify shop accounts, syncs orders with line items and
refunds via GraphQL Admin API, maps products by SKU, matches customers by name, tracks
Shopify Payments payouts with fee breakdowns, processes webhooks, supports 14 GL account
mappings, configurable GL routing rules, full reconciliation, browse/search, and reports.

## v1.1: two ways to connect a Shopify store

**OAuth pairing (recommended, App Store flow)** — merchant installs the "ERPClaw
Accounting & ERP" app from the Shopify App Store. Cloudflare Worker at
`shopify.erpclaw.ai` handles OAuth + issues a 6-character pairing code. User runs
`erpclaw shopify-connect --pairing-code ABC-XYZ` on their own ERPClaw instance and
the connection is wired. After pairing, ERPClaw pushes a status blob to the Worker
every 15 min so the Shopify admin UI can show sync health; commands queued from
the admin UI (Sync now, Disconnect) are delivered either via SSE (active mode) or
piggybacked on the status push (scheduled / on-demand).

**Custom-app (power-user flow)** — merchant creates their own Shopify Custom App
in the Partners dashboard, grabs a `shpat_` access token, and runs
`shopify-add-account --company-id <id> --shop-domain x.myshopify.com --access-token
shpat_...`. Skips the Worker entirely. No background daemon, no Worker dependency.
Useful for air-gapped installs or people who prefer end-to-end control.

Both flows populate the same 11 tables + 14 GL accounts and work identically after
setup.

### Skill Activation Triggers

Activate when user mentions: Shopify, e-commerce, online store, orders sync, Shopify
payments, payouts, gift cards, product sync, Shopify disputes, Shopify webhooks,
myshopify.com, Shopify GraphQL, Shopify revenue, Shopify fees, Shopify reports.

### Setup

**OAuth pairing (recommended)** — after installing the app in the Shopify admin, a 6-character pairing code is shown. Run:
```
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action shopify-connect --pairing-code ABC-XYZ [--company-id {id}]
python3 {baseDir}/scripts/db_query.py --action shopify-install-daemon
```

**Custom-app (power-user)** — skip the Worker, use your own `shpat_` token:
```
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action shopify-add-account --company-id {id} --shop-domain "my-store.myshopify.com" --access-token "shpat_..."
python3 {baseDir}/scripts/db_query.py --action shopify-test-connection --shopify-account-id {id}
```

## Quick Start
```
# OAuth pairing flow
--action shopify-connect --pairing-code ABC-XYZ
--action shopify-push-status --shopify-account-id {id}
--action shopify-flush-pending-events

# Custom-app flow
--action shopify-add-account --company-id {id} --shop-domain "my-store.myshopify.com" --access-token "shpat_..."
--action shopify-test-connection --shopify-account-id {id}

# Common
--action shopify-start-full-sync --shopify-account-id {id}
--action shopify-status --shopify-account-id {id}
--action shopify-disconnect --shopify-account-id {id}
```

## All 66 Actions

### Account Management (6 actions)
| Action | Description |
|--------|-------------|
| `shopify-add-account` | Create Shopify shop config (auto-creates 14 GL accounts) |
| `shopify-update-account` | Update shop name/token/settings |
| `shopify-get-account` | View account with masked token |
| `shopify-list-accounts` | List Shopify accounts |
| `shopify-configure-gl` | Update GL account mapping (14 accounts) |
| `shopify-test-connection` | Verify access token works via GraphQL |

### Sync & Jobs (10 actions)
| Action | Description |
|--------|-------------|
| `shopify-sync-orders` | Sync orders with line items and refunds |
| `shopify-sync-products` | Sync products, auto-map by SKU |
| `shopify-sync-customers` | Sync customers, auto-match by name |
| `shopify-sync-payouts` | Sync payouts with fee breakdown |
| `shopify-sync-disputes` | Sync disputes/chargebacks |
| `shopify-start-full-sync` | Full sync all types in order |
| `shopify-get-sync-job` | Get sync job details |
| `shopify-list-sync-jobs` | List sync jobs with filters |
| `shopify-cancel-sync-job` | Cancel running sync |
| `shopify-process-webhook` | Process incoming webhook |

### Product & Customer Mapping (6 actions)
| Action | Description |
|--------|-------------|
| `shopify-map-product` | Manually map Shopify product to ERP item |
| `shopify-auto-map-products` | Auto-match products by SKU |
| `shopify-list-product-maps` | List product mappings |
| `shopify-map-customer` | Manually map Shopify customer to ERP customer |
| `shopify-auto-map-customers` | Auto-match customers by name |
| `shopify-list-customer-maps` | List customer mappings |

### GL Routing Rules (5 actions)
| Action | Description |
|--------|-------------|
| `shopify-add-gl-rule` | Add GL routing rule for transaction type |
| `shopify-update-gl-rule` | Update GL routing rule |
| `shopify-list-gl-rules` | List GL routing rules |
| `shopify-delete-gl-rule` | Delete GL routing rule |
| `shopify-preview-gl` | Dry-run GL preview for order |

### GL Posting (8 actions)
| Action | Description |
|--------|-------------|
| `shopify-post-order-gl` | Post order to GL (revenue, tax, clearing) |
| `shopify-post-refund-gl` | Post refund to GL (reverse revenue) |
| `shopify-post-payout-gl` | Post payout to GL (clearing to bank, fees) |
| `shopify-post-dispute-gl` | Post dispute to GL (chargeback) |
| `shopify-post-gift-card-gl` | Post gift card deferred revenue |
| `shopify-bulk-post-gl` | Bulk-post pending orders/refunds/payouts |
| `shopify-reverse-order-gl` | Reverse GL entries for an order |
| `shopify-post-reserve-gl` | Post Shopify reserve hold/release |

### Reconciliation (6 actions)
| Action | Description |
|--------|-------------|
| `shopify-run-reconciliation` | Run 3-layer payout reconciliation |
| `shopify-verify-payout` | Verify payout transaction sums |
| `shopify-clearing-balance` | Check Shopify Clearing GL balance |
| `shopify-match-bank-transaction` | Manually match payout to bank ref |
| `shopify-list-reconciliations` | List reconciliation runs |
| `shopify-get-reconciliation` | Get reconciliation run detail |

### Browse & Search (10 actions)
| Action | Description |
|--------|-------------|
| `shopify-list-orders` | List orders (filter by date, status, GL) |
| `shopify-get-order` | Get order with line items, refunds, GL |
| `shopify-list-refunds` | List refunds (filter by date, GL status) |
| `shopify-get-refund` | Get refund with line items |
| `shopify-list-payouts` | List payouts (filter by status, date) |
| `shopify-get-payout` | Get payout with transactions |
| `shopify-list-payout-transactions` | List transactions for a payout |
| `shopify-list-disputes` | List disputes (filter by status) |
| `shopify-get-dispute` | Get dispute detail with linked order |
| `shopify-order-gl-detail` | Show GL entries for an order |

### Reports (7 actions)
| Action | Description |
|--------|-------------|
| `shopify-revenue-summary` | Revenue by period (products, shipping, tax) |
| `shopify-fee-summary` | Processing fees by period with rate |
| `shopify-refund-summary` | Refunds by period and type |
| `shopify-payout-detail-report` | Detailed payout breakdown for date range |
| `shopify-product-revenue-report` | Revenue by product/SKU |
| `shopify-customer-revenue-report` | Revenue by customer |
| `shopify-status` | Overall health: sync, GL, clearing, reconciliation |

### Connect & Disconnect (2 actions)
| Action | Description |
|--------|-------------|
| `shopify-connect` | Redeem 6-char pairing code from Worker, wire account + GL + daemon |
| `shopify-disconnect` | Revoke token, clear pairing state, reference-count daemon cleanup |

### Status Push & Command Delivery (3 actions)
| Action | Description |
|--------|-------------|
| `shopify-push-status` | HMAC-signed status blob to Worker (includes pending ack_ids) |
| `shopify-dispatch-command` | Route inbound command (sync-now, disconnect, refresh-token, gdpr-dispatch) |
| `shopify-flush-pending-events` | Two-cycle push_all to flush queued commands and acks |

### Daemon (2 actions)
| Action | Description |
|--------|-------------|
| `shopify-install-daemon` | Install launchd (macOS) / systemd-user timer (Linux) / cron fallback |
| `shopify-uninstall-daemon` | Uninstall platform daemon and timers |

### GDPR Compliance (1 action)
| Action | Description |
|--------|-------------|
| `shopify-handle-gdpr` | Process customers/data_request, customers/redact, shop/redact webhooks |

## Tables (11)
shopify_account, shopify_order, shopify_order_line_item, shopify_refund,
shopify_refund_line_item, shopify_payout, shopify_payout_transaction,
shopify_dispute, shopify_gl_rule, shopify_reconciliation_run, shopify_sync_job.

## GL Account Mapping (14 accounts)
clearing, revenue, shipping_revenue, tax_payable, cogs, inventory, fee,
discount, refund, chargeback, chargeback_fee, gift_card_liability, reserve, bank.
