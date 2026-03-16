# ERPClaw Addons

16 addon modules for [ERPClaw](https://github.com/avansaber/erpclaw). Business growth tools, operations management, infrastructure utilities, and integration connectors.

## Modules

### Growth Suite (`erpclaw-growth`)
CRM pipeline, advanced marketing, territory management, contract lifecycle, cross-module analytics, and AI-powered business analysis. 113 actions across 4 sub-modules: CRM, CRM Advanced, Analytics, AI Engine.

### Operations Suite (`erpclaw-ops`)
Manufacturing (BOMs, work orders, MRP), advanced manufacturing (shop floor, tools, ECOs), projects (tasks, milestones, timesheets), fixed assets (depreciation, disposal), quality (inspections, NCRs), and support (issues, SLAs, warranty). 126 actions across 6 sub-modules.

### Integration Connectors (`erpclaw-integrations`)
Connector configs, field mappings, sync logs, and webhook registrations for booking, delivery, real estate, financial, and productivity platforms. 80 actions across 9 domains.

### Business Modules

| Module | Description |
|--------|-------------|
| `erpclaw-alerts` | Configurable notification triggers -- low stock, overdue invoices, expiring contracts. Email, webhook, Telegram, SMS channels. |
| `erpclaw-approvals` | Multi-step approval workflows -- sequential/parallel routing, request tracking. |
| `erpclaw-compliance` | Audit plans, risk register, internal controls, compliance calendar, policy lifecycle. 38 actions. |
| `erpclaw-documents` | Document management -- versioning, tagging, linking, templates, retention, compliance holds. |
| `erpclaw-esign` | Electronic signatures -- signing workflows, audit trails, legally binding e-signatures. |
| `erpclaw-fleet` | Fleet management -- vehicles, driver assignments, fuel tracking, maintenance scheduling. |
| `erpclaw-loans` | Loan applications, disbursements, repayment scheduling, portfolio management. 20 actions. |
| `erpclaw-logistics` | Transportation and logistics -- shipments, carriers, routes, freight charges, carrier invoicing. 36 actions. |
| `erpclaw-maintenance` | Equipment maintenance -- preventive schedules, work orders, checklists, downtime tracking. |
| `erpclaw-planning` | Financial planning -- budgets, scenario modeling, forecasting, variance analysis. |
| `erpclaw-pos` | Point of sale -- register sessions, transactions, cart operations, payments, receipts. 29 actions. |
| `erpclaw-selfservice` | Self-service permission layer -- scoped portal access for employees, clients, tenants, patients, vendors. 25 actions. |
| `erpclaw-treasury` | Treasury management -- bank accounts, cash positions, forecasts, investments, inter-company transfers. |

## Installation

Requires [ERPClaw](https://github.com/avansaber/erpclaw) core. Install individual modules by name:

```
install-module erpclaw-growth
install-module erpclaw-ops
install-module erpclaw-pos
```

Or ask naturally:

```
"I need CRM and manufacturing"
"Set up point of sale"
"I need document management"
```

## Links

- **Source**: [github.com/avansaber/erpclaw-addons](https://github.com/avansaber/erpclaw-addons)
- **ERPClaw Core**: [github.com/avansaber/erpclaw](https://github.com/avansaber/erpclaw)
- **Website**: [erpclaw.ai](https://www.erpclaw.ai)

## License

MIT License -- Copyright (c) 2026 AvanSaber / Nikhil Jathar
