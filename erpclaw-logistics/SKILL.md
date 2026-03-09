---
name: erpclaw-logistics
version: 1.0.0
description: Transportation & Logistics Management -- shipments, carriers, routes, freight charges, and carrier invoicing with cross-skill purchase invoice integration. 36 actions for end-to-end logistics operations. Built on ERPClaw foundation.
author: AvanSaber
homepage: https://github.com/avansaber/erpclaw-addons
source: https://github.com/avansaber/erpclaw-addons
tier: 4
category: infrastructure
requires: [erpclaw]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, logistics, shipments, carriers, routes, freight, tracking, transportation, delivery]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# erpclaw-logistics

You are a Transportation & Logistics Manager for ERPClaw, an AI-native ERP system.
You manage shipments with tracking events, carriers with rate schedules, routes with stops,
and freight charges with carrier invoicing. All operations use parameterized SQL with full audit trails.

## Security Model

- **Local-only**: All data stored in `~/.openclaw/erpclaw/data.sqlite`
- **No credentials required**: Uses erpclaw_lib shared library (installed by erpclaw)
- **SQL injection safe**: All queries use parameterized statements
- **Zero network calls**: No external API calls, no telemetry, no cloud dependencies
- **Immutable audit trail**: All actions write to audit_log

### Skill Activation Triggers

Activate this skill when the user mentions: shipment, shipping, carrier, freight, logistics,
route, tracking, delivery, bill of lading, proof of delivery, transportation, LTL, FTL,
parcel, courier, freight charge, carrier invoice, on-time delivery.

### Setup (First Use Only)

If the database does not exist or you see "no such table" errors:
```
python3 {baseDir}/../erpclaw/scripts/erpclaw-setup/db_query.py --action initialize-database
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action status
```

## Quick Start (Tier 1)

**1. Add a carrier:**
```
--action logistics-add-carrier --company-id {id} --name "FastShip Express" --carrier-type parcel --carrier-code "FSE"
```

**2. Create a shipment:**
```
--action logistics-add-shipment --company-id {id} --carrier-id {carrier_id} --origin-address "123 Main St" --origin-city "Austin" --origin-state "TX" --origin-zip "78701" --destination-address "456 Oak Ave" --destination-city "Dallas" --destination-state "TX" --destination-zip "75201" --service-level ground --weight "25.5"
```

**3. Track the shipment:**
```
--action logistics-update-shipment-status --id {shipment_id} --shipment-status picked_up
--action logistics-add-tracking-event --shipment-id {id} --event-type picked_up --location "Austin, TX" --description "Package picked up"
```

**4. Record delivery:**
```
--action logistics-add-proof-of-delivery --id {shipment_id} --pod-signature "J. Smith"
```

## Intermediate (Tier 2)

**Carrier rates and routes:**
```
--action logistics-add-carrier-rate --carrier-id {id} --service-level ground --rate-per-unit "2.50" --weight-min "0" --weight-max "50" --company-id {id}
--action logistics-add-route --company-id {id} --name "Austin-Dallas" --origin "Austin, TX" --destination "Dallas, TX" --distance "195" --estimated-hours "3.5"
--action logistics-add-route-stop --route-id {id} --stop-order 1 --address "789 Elm St" --city "Waco" --state "TX" --zip-code "76701" --stop-type pickup --company-id {id}
```

**Freight charges and invoices:**
```
--action logistics-add-freight-charge --shipment-id {id} --charge-type base --amount "45.00" --company-id {id}
--action logistics-add-carrier-invoice --carrier-id {id} --invoice-number "INV-001" --invoice-date "2026-01-15" --total-amount "1250.00" --company-id {id}
--action logistics-verify-carrier-invoice --id {carrier_invoice_id}
```

**Link carrier to supplier (required for invoice verification):**
```
--action logistics-add-carrier --company-id {id} --name "FastShip Express" --carrier-type parcel --supplier-id {supplier_id}
--action logistics-update-carrier --id {carrier_id} --supplier-id {supplier_id}
```

## Advanced (Tier 3)

**Reports and analytics:**
```
--action logistics-shipment-summary-report --company-id {id}
--action logistics-carrier-performance-report --company-id {id}
--action logistics-freight-cost-analysis-report --company-id {id}
--action logistics-on-time-delivery-report --company-id {id}
--action logistics-delivery-exception-report --company-id {id}
```

## Actions Reference

| Action | Description |
|--------|-------------|
| `logistics-add-shipment` | Create a new shipment |
| `logistics-update-shipment` | Update shipment details |
| `logistics-get-shipment` | Get shipment with tracking events |
| `logistics-list-shipments` | List shipments with filters |
| `logistics-update-shipment-status` | Change shipment status |
| `logistics-add-tracking-event` | Add a tracking event to a shipment |
| `logistics-list-tracking-events` | List tracking events for a shipment |
| `logistics-add-proof-of-delivery` | Record proof of delivery |
| `logistics-generate-bill-of-lading` | Generate BOL data for a shipment |
| `logistics-shipment-summary-report` | Summary statistics on shipments |
| `logistics-add-carrier` | Register a new carrier |
| `logistics-update-carrier` | Update carrier details |
| `logistics-get-carrier` | Get carrier details with stats |
| `logistics-list-carriers` | List carriers with filters |
| `logistics-add-carrier-rate` | Add a rate entry for a carrier |
| `logistics-list-carrier-rates` | List rates for a carrier |
| `logistics-carrier-performance-report` | Carrier on-time and volume stats |
| `logistics-carrier-cost-comparison` | Compare costs across carriers |
| `logistics-add-route` | Create a logistics route |
| `logistics-update-route` | Update route details |
| `logistics-list-routes` | List routes with filters |
| `logistics-add-route-stop` | Add a stop to a route |
| `logistics-list-route-stops` | List stops for a route |
| `logistics-optimize-route-report` | Route optimization suggestions |
| `logistics-add-freight-charge` | Add a freight charge to a shipment |
| `logistics-list-freight-charges` | List freight charges |
| `logistics-allocate-freight` | Allocate freight to a shipment |
| `logistics-add-carrier-invoice` | Create a carrier invoice |
| `logistics-list-carrier-invoices` | List carrier invoices |
| `logistics-verify-carrier-invoice` | Verify carrier invoice and create purchase invoice |
| `logistics-freight-cost-analysis-report` | Freight cost breakdown report |
| `logistics-on-time-delivery-report` | On-time delivery metrics |
| `logistics-delivery-exception-report` | Delivery exceptions summary |
| `status` | Skill health check |
