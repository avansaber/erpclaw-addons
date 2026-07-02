# ERPClaw Addons

<!-- SYNC:value:group.erpclaw-addons.module_count -->19<!-- /SYNC --> addon modules for [ERPClaw](https://github.com/avansaber/erpclaw). Business growth tools, operations management, infrastructure utilities, integration connectors, deep platform integrations for Stripe and Shopify, and developer tooling for module authoring.

## Deep Integrations

Free, open-source, self-hosted connectors that sync every transaction directly into your ERPClaw general ledger. Your data stays on your own ERPClaw instance.

### Stripe (`erpclaw-integrations-stripe`)
Deep Stripe integration. <!-- SYNC:value:module.erpclaw-integrations-stripe.actions -->66<!-- /SYNC --> actions across 10 domains: account management, charges/refunds/disputes/payouts/subscriptions sync, customer mapping, GL posting with rule engine, payout reconciliation, ASC 606 revenue recognition, Connect platform fees, webhook processing, and financial reports (revenue, MRR, fees, disputes).

```
install-module erpclaw-integrations-stripe
```

### Shopify (`erpclaw-integrations-shopify`)
Deep Shopify integration. <!-- SYNC:value:module.erpclaw-integrations-shopify.actions -->66<!-- /SYNC --> actions across 15 domains: order/refund/payout/dispute sync, product + customer mapping, 14 GL account mappings, configurable GL routing rules, three-layer payout reconciliation, COGS tracking, gift card deferred revenue, GDPR webhooks, App Store OAuth pairing with status mirror, and revenue/fee/refund reports.

```
install-module erpclaw-integrations-shopify
```

- **Shopify App (OAuth pairing):** [ERPClaw Accounting & ERP on the Shopify App Store](https://apps.shopify.com/TBD-APP-SLUG)
- **Docs:** [erpclaw.ai/docs/shopify](https://www.erpclaw.ai/docs/shopify)

OAuth tokens are forwarded once to your ERPClaw during pairing and deleted from the pairing Worker within 60 seconds. A custom-app flow is also available for air-gapped installs.

## Modules

### Growth Suite (`erpclaw-growth`)
CRM pipeline, advanced marketing, territory management, contract lifecycle, cross-module analytics, and AI-powered business analysis. <!-- SYNC:value:module.erpclaw-growth.actions -->159<!-- /SYNC --> actions across 4 sub-modules: CRM, CRM Advanced, Analytics, AI Engine.

### Operations Suite (`erpclaw-ops`)
Manufacturing (BOMs, work orders, MRP), advanced manufacturing (shop floor, tools, ECOs), projects (tasks, milestones, timesheets), fixed assets (depreciation, disposal), quality (inspections, NCRs), and support (issues, SLAs, warranty). <!-- SYNC:value:module.erpclaw-ops.actions -->142<!-- /SYNC --> actions across 6 sub-modules.

### Integration Connectors (`erpclaw-integrations`)
Connector configs, field mappings, sync logs, and webhook registrations for booking, delivery, real estate, financial, and productivity platforms. <!-- SYNC:value:module.erpclaw-integrations.actions -->91<!-- /SYNC --> actions across 9 domains. (For Stripe and Shopify, use the dedicated deep-integration modules above.)

### OS Engine (developer tooling) (`erpclaw-os-engine`)
Optional addon for developers authoring new ERPClaw vertical modules and inspecting OS-level health. Sandbox-first module generation; user approval required before any deploy. Not installed by default. Required only if you are authoring modules or running ERPClaw in power-user mode where the OS proposes patterns over time. See [erpclaw-os-engine/README.md](erpclaw-os-engine/README.md) for the full action catalog and install path.

### Business Modules

| Module | Description |
|--------|-------------|
| `erpclaw-alerts` | Configurable notification triggers -- low stock, overdue invoices, expiring contracts. Email, webhook, Telegram, SMS channels. |
| `erpclaw-approvals` | Multi-step approval workflows -- sequential/parallel routing, request tracking. |
| `erpclaw-compliance` | Audit plans, risk register, internal controls, compliance calendar, policy lifecycle. <!-- SYNC:value:module.erpclaw-compliance.actions -->38<!-- /SYNC --> actions. |
| `erpclaw-documents` | Document management -- versioning, tagging, linking, templates, retention, compliance holds. |
| `erpclaw-esign` | Electronic signatures -- signing workflows, audit trails, legally binding e-signatures. |
| `erpclaw-fleet` | Fleet management -- vehicles, driver assignments, fuel tracking, maintenance scheduling. |
| `erpclaw-loans` | Loan applications, disbursements, repayment scheduling, portfolio management. <!-- SYNC:value:module.erpclaw-loans.actions -->20<!-- /SYNC --> actions. |
| `erpclaw-logistics` | Transportation and logistics -- shipments, carriers, routes, freight charges, carrier invoicing. <!-- SYNC:value:module.erpclaw-logistics.actions -->34<!-- /SYNC --> actions. |
| `erpclaw-maintenance` | Equipment maintenance -- preventive schedules, work orders, checklists, downtime tracking. |
| `erpclaw-planning` | Financial planning -- budgets, scenario modeling, forecasting, variance analysis. |
| `erpclaw-pos` | Point of sale -- register sessions, transactions, cart operations, payments, receipts. <!-- SYNC:value:module.erpclaw-pos.actions -->29<!-- /SYNC --> actions. |
| `erpclaw-selfservice` | Self-service permission layer -- scoped portal access for employees, clients, tenants, patients, vendors. <!-- SYNC:value:module.erpclaw-selfservice.actions -->25<!-- /SYNC --> actions. |
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

GNU General Public License v3 -- Copyright (c) 2026 AvanSaber / Nikhil Jathar
