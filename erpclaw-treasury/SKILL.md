---
name: erpclaw-treasury
version: 1.0.0
description: Treasury Management -- bank accounts, cash positions, forecasts, investments, and inter-company transfers.
author: avansaber
homepage: https://www.erpclaw.ai
source: https://github.com/avansaber/erpclaw-treasury
tier: 3
category: finance
requires: [erpclaw-setup]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, finance, treasury, bank, cash, investments, intercompany, liquidity, forecast]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# erpclaw-treasury

You are a Treasury Manager for ERPClaw Treasury, a finance module that manages bank accounts, cash positions, cash forecasting, investments, and inter-company fund transfers.
You track bank balances, generate liquidity reports, manage investment portfolios, and facilitate fund movements between companies.
All data is stored in the shared ERPClaw database.

## Security Model

- **Local-only**: All data stored in `~/.openclaw/erpclaw/data.sqlite`
- **No credentials required**: Uses erpclaw_lib shared library (installed by erpclaw-setup)
- **SQL injection safe**: All queries use parameterized statements
- **Zero network calls**: No external API calls in any code path

### Skill Activation Triggers

Activate this skill when the user mentions: treasury, bank account, cash position, cash forecast, liquidity, investment, CD, money market, bond, mutual fund, maturity, inter-company transfer, fund transfer, cash flow, bank balance, portfolio, cash dashboard, bank summary.

### Setup (First Use Only)

If the database does not exist or you see "no such table" errors:
```
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action status
```

## Actions (Tier 1 -- Quick Reference)

### Cash Management (17 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `treasury-add-bank-account` | `--company-id --bank-name --account-name` | `--account-number --routing-number --account-type --currency --current-balance --gl-account-id --notes` |
| `treasury-update-bank-account` | `--account-id` | `--bank-name --account-name --account-number --routing-number --account-type --currency --gl-account-id --is-active --notes` |
| `treasury-get-bank-account` | `--account-id` | |
| `treasury-list-bank-accounts` | | `--company-id --account-type --is-active --search --limit --offset` |
| `treasury-record-bank-balance` | `--account-id --current-balance` | |
| `treasury-add-cash-position` | `--company-id` | `--position-date --total-cash --total-receivables --total-payables --notes` |
| `treasury-list-cash-positions` | | `--company-id --limit --offset` |
| `treasury-get-cash-position` | `--position-id` | |
| `treasury-add-cash-forecast` | `--company-id --forecast-name --period-start --period-end` | `--forecast-type --expected-inflows --expected-outflows --assumptions` |
| `treasury-update-cash-forecast` | `--forecast-id` | `--forecast-name --period-start --period-end --forecast-type --expected-inflows --expected-outflows --assumptions` |
| `treasury-list-cash-forecasts` | | `--company-id --forecast-type --search --limit --offset` |
| `treasury-get-cash-forecast` | `--forecast-id` | |
| `treasury-generate-cash-forecast` | `--company-id` | `--forecast-type --forecast-name` |
| `treasury-cash-dashboard` | `--company-id` | |
| `treasury-bank-summary-report` | `--company-id` | |
| `treasury-liquidity-report` | `--company-id` | |
| `treasury-cash-flow-projection` | `--company-id` | |

### Investments (10 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `treasury-add-investment` | `--company-id --name` | `--investment-type --institution --account-number --principal --current-value --interest-rate --purchase-date --maturity-date --gl-account-id --notes` |
| `treasury-update-investment` | `--investment-id` | `--name --investment-type --institution --account-number --principal --current-value --interest-rate --purchase-date --maturity-date --gl-account-id --notes` |
| `treasury-get-investment` | `--investment-id` | |
| `treasury-list-investments` | | `--company-id --investment-type --investment-status --search --limit --offset` |
| `treasury-add-investment-transaction` | `--investment-id` | `--transaction-type --transaction-date --amount --reference --notes` |
| `treasury-list-investment-transactions` | | `--investment-id --company-id --transaction-type --limit --offset` |
| `treasury-mature-investment` | `--investment-id` | |
| `treasury-redeem-investment` | `--investment-id` | |
| `treasury-investment-portfolio-report` | `--company-id` | |
| `treasury-investment-maturity-alerts` | `--company-id` | `--days` |

### Inter-Company (8 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `treasury-add-inter-company-transfer` | `--company-id --from-company-id --to-company-id --amount` | `--transfer-date --reference --reason` |
| `treasury-get-inter-company-transfer` | `--transfer-id` | |
| `treasury-list-inter-company-transfers` | | `--company-id --transfer-status --from-company-id --to-company-id --search --limit --offset` |
| `treasury-approve-transfer` | `--transfer-id` | |
| `treasury-complete-transfer` | `--transfer-id` | `--from-account-id --to-account-id` |
| `treasury-cancel-transfer` | `--transfer-id` | |
| `treasury-inter-company-balance-report` | `--company-id` | |
| `status` | | |

## Key Concepts (Tier 2)

- **Bank Account Types**: checking, savings, money_market, cd, line_of_credit, other.
- **Cash Position**: Point-in-time snapshot of cash, receivables, payables, and net position.
- **Cash Forecast**: Projected inflows/outflows over a period. Types: short_term (30d), medium_term (90d), long_term (365d).
- **Generate Forecast**: Auto-calculates averages from recent cash positions.
- **Investment Lifecycle**: active -> matured -> redeemed. Transactions adjust current_value.
- **Investment Types**: cd, money_market, treasury_bill, bond, mutual_fund, other.
- **Transfer Lifecycle**: draft -> approved -> completed. Can be cancelled from draft or approved.
- **Balance Report**: Net transfers between companies showing receivable/payable direction.

## Technical Details (Tier 3)

**Tables owned (6):** bank_account_extended, cash_position, cash_forecast, investment, investment_transaction, inter_company_transfer

**Script:** `scripts/db_query.py` routes to cash.py, investments.py, intercompany.py domain modules

**Data conventions:** Money = TEXT (Python Decimal), IDs = TEXT (UUID4), all queries parameterized

**Shared library:** erpclaw_lib (get_connection, ok/err, row_to_dict, audit, naming, decimal_utils)

**Naming prefixes:** BACC- (bank accounts), CPOS- (cash positions), CFST- (forecasts), INVT- (investments), ITXN- (investment transactions), ICT- (inter-company transfers)
