---
name: erpclaw-pos
version: "1.0.0"
description: "Point of Sale — register sessions, transactions, payments, receipts"
author: "avansaber"
source: https://github.com/avansaber/erpclaw-addons
scripts:
  - scripts/db_query.py
actions:
  # Profile management
  - pos-add-pos-profile
  - pos-update-pos-profile
  - pos-list-pos-profiles
  - pos-get-pos-profile
  # Session management
  - pos-open-session
  - pos-get-session
  - pos-list-sessions
  - pos-close-session
  # Transaction lifecycle
  - pos-add-transaction
  - pos-void-transaction
  - pos-get-transaction
  - pos-list-transactions
  # Cart operations
  - pos-add-transaction-item
  - pos-remove-transaction-item
  - pos-apply-discount
  # Hold/resume
  - pos-hold-transaction
  - pos-resume-transaction
  # Payment & finalization
  - pos-add-payment
  - pos-submit-transaction
  - pos-return-transaction
  # Lookup
  - pos-lookup-item
  # Reporting
  - pos-cash-reconciliation
  - pos-session-summary
  - pos-daily-report
  - pos-hourly-sales
  - pos-top-items
  - pos-cashier-performance
  - pos-generate-receipt
  - pos-status
---

# ERPClaw POS

Point of Sale module for in-store and counter sales. Manages register sessions,
cart-based transactions, split payments, and end-of-day reconciliation.

## Tier 1 — Basic POS

Set up POS profiles (terminal configurations), open/close register sessions,
ring up sales with item lookup, process payments, and generate receipts.

**Quick start:**
```
add-pos-profile --name "Main Register" --company-id <id>
open-session --pos-profile-id <id> --opening-amount 200 --cashier-name "Jane"
add-transaction --pos-session-id <id>
add-transaction-item --pos-transaction-id <id> --item-id <id> --qty 2
add-payment --pos-transaction-id <id> --payment-method cash --amount 50
submit-transaction --id <id>
close-session --id <id> --closing-amount 250
```

## Tier 2 — Advanced Features

Hold/resume transactions, process returns (creates credit notes), apply discounts,
void transactions, and run reports (daily sales, hourly breakdown, top items).

## Tier 3 — Analytics & Integration

Cashier performance metrics, cash reconciliation with variance tracking.
submit-transaction auto-creates sales invoice + payment entry + stock ledger updates.
return-transaction auto-creates credit note + reverse stock entries.

## Cross-Skill Integration

- **erpclaw-selling:** submit-transaction → create-sales-invoice
- **erpclaw-payments:** submit-transaction → add-payment-entry
- **erpclaw-inventory:** lookup-item reads item/item_barcode tables; submit updates stock
- **item_barcode table:** Fast barcode scanning (added by expansion prerequisites)
