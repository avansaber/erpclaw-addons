---
name: erpclaw-integrations
version: 2.0.0
description: Integration Connectors -- manage connector configs, field mappings, sync logs, webhook registrations, and platform-specific connectors for booking, delivery, real estate, financial, and productivity platforms. 80 actions across 9 domains. Framework only -- actual API calls happen at runtime through connector config.
author: AvanSaber / Nikhil Jathar
homepage: https://www.erpclaw.ai
source: https://github.com/avansaber/erpclaw-addons
tier: 5
category: integrations
requires: [erpclaw-setup]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, integrations, connector, shopify, woocommerce, amazon, quickbooks, stripe, square, xero, booking, airbnb, expedia, doordash, ubereats, zillow, plaid, twilio, google-workspace, sync, webhook, mapping, field-mapping, entity-map, etl]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# erpclaw-integrations

You are an Integration Manager for ERPClaw, managing connector configurations, field mappings,
sync operations, and webhook registrations for external platforms. You do NOT make actual API calls --
you manage the framework that stores connector configs, credentials, sync logs, field mappings,
and entity ID maps. Actual platform communication happens at runtime through the stored configs.

Supported platforms: Shopify, WooCommerce, Amazon Seller, QuickBooks Online, Stripe, Square, Xero, Custom,
Booking.com, Expedia, Airbnb, VRBO, DoorDash, UberEats, Grubhub, Postmates, Zillow, Realtor.com, MLS,
Trulia, Plaid, Twilio, SendGrid, Mailchimp, Google Workspace, Microsoft 365, Slack, Zoom.

## Security Model

- **Local-only**: All data stored in `~/.openclaw/erpclaw/data.sqlite`
- **No credentials required**: Uses erpclaw_lib shared library (installed by erpclaw-setup)
- **SQL injection safe**: All queries use parameterized statements
- **Zero network calls**: No external API calls, no telemetry, no cloud dependencies
- **Immutable audit trail**: All actions write to audit_log

### Skill Activation Triggers

Activate this skill when the user mentions: integration, connector, sync, webhook, field mapping,
entity map, Shopify, WooCommerce, Amazon, QuickBooks, Stripe, Square, Xero, import, export,
ETL, data sync, API connector, platform, external system.

### Setup (First Use Only)

If the database does not exist or you see "no such table" errors:
```
python3 {baseDir}/../erpclaw-setup/scripts/db_query.py --action initialize-database
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action status
```

## Quick Start (Tier 1)

**1. Add a connector:**
```
--action integration-add-connector --company-id {id} --name "Shopify Store" --platform shopify --base-url "https://mystore.myshopify.com"
```

**2. Add credentials:**
```
--action integration-add-connector-credential --connector-id {id} --credential-type api_key --credential-key "X-Shopify-Access-Token" --credential-value "shpat_xxx"
```

**3. Set up field mappings:**
```
--action integration-add-field-mapping --connector-id {id} --entity-type customer --source-field "email" --target-field "customer_email"
```

**4. Start a sync:**
```
--action integration-start-sync --connector-id {id} --sync-type full --direction inbound --entity-type customer
```

## All Actions (Tier 2)

For all actions: `python3 {baseDir}/scripts/db_query.py --action <action> [flags]`

### Connectors (14 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `integration-add-connector` | `--company-id --name --platform` | `--connector-type --base-url --config-json` |
| `integration-update-connector` | `--connector-id` | `--name --platform --connector-type --base-url --config-json` |
| `integration-get-connector` | `--connector-id` | |
| `integration-list-connectors` | | `--company-id --platform --connector-status --search --limit --offset` |
| `integration-activate-connector` | `--connector-id` | |
| `integration-deactivate-connector` | `--connector-id` | |
| `integration-test-connector` | `--connector-id` | |
| `integration-add-connector-credential` | `--connector-id --credential-type --credential-key --credential-value` | `--expires-at` |
| `integration-list-connector-credentials` | `--connector-id` | `--limit --offset` |
| `integration-delete-connector-credential` | `--credential-id` | |
| `integration-add-webhook` | `--connector-id --event-type --webhook-url` | `--webhook-secret` |
| `integration-list-webhooks` | `--connector-id` | `--limit --offset` |
| `integration-delete-webhook` | `--webhook-id` | |
| `integration-connector-health-report` | | `--company-id` |

### Sync (14 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `integration-start-sync` | `--connector-id --sync-type --direction` | `--entity-type` |
| `integration-get-sync` | `--sync-id` | |
| `integration-list-syncs` | | `--connector-id --sync-status --company-id --limit --offset` |
| `integration-cancel-sync` | `--sync-id` | |
| `integration-add-sync-schedule` | `--connector-id --entity-type --frequency` | `--sync-type --direction --next-run-at` |
| `integration-update-sync-schedule` | `--schedule-id` | `--entity-type --frequency --sync-type --direction --is-active --next-run-at` |
| `integration-list-sync-schedules` | | `--connector-id --company-id --limit --offset` |
| `integration-delete-sync-schedule` | `--schedule-id` | |
| `integration-add-sync-error` | `--sync-id --error-message` | `--entity-type --entity-id` |
| `integration-list-sync-errors` | | `--sync-id --is-resolved --limit --offset` |
| `integration-resolve-sync-error` | `--error-id` | `--resolution-notes` |
| `integration-retry-sync` | `--sync-id` | |
| `integration-sync-summary-report` | | `--company-id --connector-id --start-date --end-date` |
| `integration-get-sync-log` | `--sync-id` | `--limit --offset` |

### Mappings (12 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `integration-add-field-mapping` | `--connector-id --entity-type --source-field --target-field` | `--transform-rule --is-required --default-value` |
| `integration-update-field-mapping` | `--field-mapping-id` | `--source-field --target-field --transform-rule --is-required --default-value` |
| `integration-get-field-mapping` | `--field-mapping-id` | |
| `integration-list-field-mappings` | | `--connector-id --entity-type --limit --offset` |
| `integration-delete-field-mapping` | `--field-mapping-id` | |
| `integration-add-entity-map` | `--connector-id --entity-type --local-id --remote-id` | |
| `integration-get-entity-map` | `--entity-map-id` | |
| `integration-list-entity-maps` | | `--connector-id --entity-type --local-id --remote-id --limit --offset` |
| `integration-delete-entity-map` | `--entity-map-id` | |
| `integration-add-transform-rule` | `--connector-id --entity-type --rule-name --rule-json` | |
| `integration-list-transform-rules` | | `--connector-id --entity-type --limit --offset` |
| `integration-delete-transform-rule` | `--transform-rule-id` | |

### Booking Connectors (8 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `integration-add-booking-connector` | `--company-id --platform` | `--property-id --api-credentials-ref --sync-reservations --sync-rates --sync-availability` |
| `integration-configure-booking-sync` | `--connector-id` | `--sync-reservations --sync-rates --sync-availability --connector-status` |
| `integration-sync-reservations` | `--connector-id` | `--records-synced --errors` |
| `integration-push-rates` | `--connector-id` | `--records-synced --errors` |
| `integration-push-availability` | `--connector-id` | `--records-synced --errors` |
| `integration-list-booking-syncs` | | `--company-id --connector-id --sync-type --limit --offset` |
| `integration-booking-revenue-report` | `--company-id` | |
| `integration-booking-channel-report` | `--company-id` | |

### Delivery Connectors (8 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `integration-add-delivery-connector` | `--company-id --platform` | `--store-id --api-credentials-ref --auto-accept --sync-menu` |
| `integration-configure-delivery-sync` | `--connector-id` | `--auto-accept --sync-menu --connector-status` |
| `integration-ingest-orders` | `--connector-id` | `--external-order-id --order-data --total-amount --commission` |
| `integration-sync-menu` | `--connector-id` | |
| `integration-update-order-status` | `--order-id --order-status` | |
| `integration-list-delivery-syncs` | | `--company-id --connector-id --order-status --limit --offset` |
| `integration-delivery-revenue-report` | `--company-id` | |
| `integration-delivery-platform-comparison` | `--company-id` | |

### Real Estate Connectors (6 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `integration-add-realestate-connector` | `--company-id --platform` | `--agent-id --api-credentials-ref --sync-listings --capture-leads` |
| `integration-sync-listings` | `--connector-id` | |
| `integration-capture-leads` | `--connector-id --contact-name` | `--lead-source --contact-email --contact-phone --property-ref --inquiry` |
| `integration-list-realestate-syncs` | | `--company-id --connector-id --platform --limit --offset` |
| `integration-listing-performance-report` | `--company-id` | |
| `integration-lead-source-report` | `--company-id` | |

### Financial Connectors (8 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `integration-add-financial-connector` | `--company-id --platform` | `--account-ref --api-credentials-ref --sync-enabled` |
| `integration-sync-bank-feeds` | `--connector-id` | |
| `integration-sync-transactions` | `--connector-id` | |
| `integration-send-sms` | `--connector-id --recipient --message-body` | |
| `integration-send-email-delivery` | `--connector-id --recipient --subject` | |
| `integration-list-financial-syncs` | | `--company-id --platform --connector-status --limit --offset` |
| `integration-bank-feed-reconciliation-report` | `--company-id` | |
| `integration-communication-delivery-report` | `--company-id` | |

### Productivity Connectors (6 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `integration-add-productivity-connector` | `--company-id --platform` | `--workspace-id --api-credentials-ref --sync-calendar --sync-contacts --sync-files` |
| `integration-sync-calendar` | `--connector-id` | |
| `integration-sync-contacts` | `--connector-id` | |
| `integration-sync-files` | `--connector-id` | |
| `integration-list-productivity-syncs` | | `--company-id --platform --connector-status --limit --offset` |
| `integration-sync-status-report` | `--company-id` | |

### Cross-Domain Reports (3 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `integration-connector-usage-report` | `--company-id` | |
| `integration-sync-volume-report` | `--company-id` | |
| `integration-error-rate-report` | `--company-id` | |

### Quick Command Reference
| User Says | Action |
|-----------|--------|
| "Connect to Shopify" | `integration-add-connector` with `--platform shopify` |
| "Set up API key" | `integration-add-connector-credential` |
| "Map fields" | `integration-add-field-mapping` |
| "Sync customers" | `integration-start-sync` with `--entity-type customer` |
| "Schedule daily sync" | `integration-add-sync-schedule` with `--frequency daily` |
| "Check sync status" | `integration-get-sync` or `integration-list-syncs` |
| "See connector health" | `integration-connector-health-report` |
| "Connect Airbnb" | `integration-add-booking-connector` with `--platform airbnb` |
| "Sync DoorDash orders" | `integration-ingest-orders` with delivery connector |
| "Capture Zillow lead" | `integration-capture-leads` with realestate connector |
| "Connect Plaid" | `integration-add-financial-connector` with `--platform plaid` |
| "Sync Google Calendar" | `integration-sync-calendar` with productivity connector |

## Technical Details (Tier 3)

**Tables owned (17):** integration_connector, integration_credential, integration_webhook, integration_sync, integration_sync_schedule, integration_field_mapping, integration_entity_map, integration_transform_rule, integration_sync_error, connv2_booking_connector, connv2_booking_sync_log, connv2_delivery_connector, connv2_delivery_order, connv2_realestate_connector, connv2_realestate_lead, connv2_financial_connector, connv2_productivity_connector

**Script:** `scripts/db_query.py` routes to 9 domain modules: connectors.py, sync.py, mappings.py, booking.py, delivery.py, realestate.py, financial.py, productivity.py, connv2_reports.py

**Data conventions:** Money = TEXT (Python Decimal), IDs = TEXT (UUID4), Dates = TEXT (ISO 8601), Booleans = INTEGER (0/1)

**Shared library:** erpclaw_lib (get_connection, ok/err, row_to_dict, get_next_name, audit)
