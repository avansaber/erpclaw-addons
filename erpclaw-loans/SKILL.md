---
name: erpclaw-loans
version: 1.0.0
description: Loan application, disbursement, repayment scheduling, and portfolio management. 20 actions across loan applications, disbursement, repayments, and reporting.
author: AvanSaber
homepage: https://github.com/avansaber/erpclaw-addons
source: https://github.com/avansaber/erpclaw-addons
tier: 4
category: infrastructure
requires: [erpclaw]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, loans, lending, repayment, disbursement, interest, amortization, portfolio]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action loan-status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# ERPClaw Loans

Loan management for term loans, demand loans, staff loans, and credit lines.

## Tier 1 — Basic Operations

### Loan Applications

| Action | Description |
|--------|-------------|
| `loan-add-loan-application` | Create loan application (customer/employee/supplier) |
| `loan-update-loan-application` | Update application details |
| `loan-list-loan-applications` | List applications with status filter |
| `loan-get-loan-application` | Get application details |
| `loan-approve-loan` | Approve application, set approved amount |
| `loan-reject-loan` | Reject application with reason |

### Loan Lifecycle

| Action | Description |
|--------|-------------|
| `loan-disburse-loan` | Disburse approved loan — creates GL entries |
| `loan-list-loans` | List loans with status/applicant filter |
| `loan-get-loan` | Get loan details with schedule |
| `loan-generate-repayment-schedule` | Generate installment schedule |
| `loan-get-repayment-schedule` | View repayment schedule |

### Repayments

| Action | Description |
|--------|-------------|
| `loan-record-repayment` | Record loan repayment — updates GL |
| `loan-list-repayments` | List repayments for a loan |
| `loan-calculate-interest` | Calculate accrued interest to date |

## Tier 2 — Advanced

| Action | Description |
|--------|-------------|
| `loan-write-off-loan` | Write off bad debt — GL: debit bad debt, credit receivable |
| `loan-restructure-loan` | Modify terms, regenerate schedule |
| `loan-close-loan` | Close fully repaid loan |

## Tier 3 — Reports

| Action | Description |
|--------|-------------|
| `loan-statement` | Full statement for a loan |
| `loan-overdue-loans` | List overdue installments |
| `status` | Module status summary |

## GL Integration

- **Disbursement:** Debit Loan Receivable, Credit Bank/Cash
- **Repayment:** Debit Bank/Cash, Credit Loan Receivable (principal) + Interest Income (interest)
- **Write-off:** Debit Bad Debt Expense, Credit Loan Receivable
- All GL postings use `erpclaw_lib.gl_posting.insert_gl_entries()` and pass the full
  12-step GL validation. A GL failure rolls the whole action back and returns an
  error; no loans action records a document the ledger has not accepted.
- The receivable leg carries `party_type` / `party_id` from the loan applicant
  (GL validation step 5); the interest-income leg carries a cost center (step 6).
- Loan vouchers post under the registered `journal_entry` voucher type; identity
  rides on `voucher_id` (loan / repayment / write-off row) plus `remarks`.
