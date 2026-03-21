---
name: erpclaw-pos
version: 1.0.0
description: Point of Sale -- 28 actions across 4 domains. POS profiles, register sessions, cart-based transactions, split payments, receipts, hold/resume, returns, discounts, and end-of-day reporting with cash reconciliation.
author: AvanSaber
homepage: https://github.com/avansaber/erpclaw-addons
source: https://github.com/avansaber/erpclaw-addons
tier: 4
category: infrastructure
requires: [erpclaw]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, pos, point-of-sale, register, transactions, payments, receipts, cash-reconciliation, retail, cart, barcode]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action pos-status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# ERPClaw POS

Point of Sale module for in-store and counter sales. Manages register sessions,
cart-based transactions, split payments, hold/resume, returns, discounts,
and end-of-day reconciliation. submit-transaction auto-creates sales invoice,
payment entry, and stock ledger updates via cross-skill integration.

### Skill Activation Triggers

Activate when user mentions: POS, point of sale, register, cash register, checkout,
ring up, transaction, receipt, cashier, terminal, barcode scan, hold transaction.

### Setup
```
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action pos-add-pos-profile --name "Main Register" --company-id {id}
```

## Quick Start
```
--action pos-add-pos-profile --name "Main Register" --company-id {id}
--action pos-open-session --pos-profile-id {id} --opening-amount 200 --cashier-name "Jane"
--action pos-add-transaction --pos-session-id {id}
--action pos-add-transaction-item --pos-transaction-id {id} --item-id {id} --qty 2
--action pos-add-payment --pos-transaction-id {id} --payment-method cash --amount 50
--action pos-submit-transaction --id {id}
--action pos-close-session --id {id} --closing-amount 250
```

## All 28 Actions

### POS Profiles (4 actions)
| Action | Description |
|--------|-------------|
| `pos-add-pos-profile` | Create POS terminal profile |
| `pos-get-pos-profile` | Get profile details |
| `pos-update-pos-profile` | Update profile settings |
| `pos-list-pos-profiles` | List POS profiles |

### Sessions (5 actions)
| Action | Description |
|--------|-------------|
| `pos-open-session` | Open register session |
| `pos-close-session` | Close session with reconciliation |
| `pos-get-session` | Get session details |
| `pos-list-sessions` | List sessions |
| `pos-session-summary` | Session summary with totals |

### Transactions (11 actions)
| Action | Description |
|--------|-------------|
| `pos-add-transaction` | Start new transaction |
| `pos-get-transaction` | Get transaction details |
| `pos-list-transactions` | List transactions |
| `pos-add-transaction-item` | Add item to cart |
| `pos-remove-transaction-item` | Remove item from cart |
| `pos-add-payment` | Add payment to transaction |
| `pos-apply-discount` | Apply discount |
| `pos-submit-transaction` | Submit (creates invoice + payment + stock) |
| `pos-void-transaction` | Void transaction |
| `pos-hold-transaction` | Hold transaction for later |
| `pos-resume-transaction` | Resume held transaction |

### Returns & Reports (8 actions)
| Action | Description |
|--------|-------------|
| `pos-return-transaction` | Process return (creates credit note) |
| `pos-lookup-item` | Lookup item by barcode/name |
| `pos-generate-receipt` | Generate receipt |
| `pos-daily-report` | Daily sales report |
| `pos-hourly-sales` | Hourly sales breakdown |
| `pos-top-items` | Top selling items |
| `pos-cashier-performance` | Cashier performance metrics |
| `pos-cash-reconciliation` | Cash reconciliation with variance |

## Cross-Skill Integration
- **erpclaw-selling:** submit-transaction creates sales_invoice
- **erpclaw-payments:** submit-transaction creates payment_entry
- **erpclaw-inventory:** lookup-item reads item/item_barcode; submit updates stock
- **item_barcode table:** Fast barcode scanning

## Technical Details (Tier 3)
**Tables:** pos_profile, pos_session, pos_transaction, pos_transaction_item, pos_payment. **Data:** Money=TEXT(Decimal), IDs=TEXT(UUID4).
