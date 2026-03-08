---
name: erpclaw-planning
version: 1.0.0
description: Financial planning module for ERPClaw -- budgets, scenario modeling, and forecasting with variance analysis and budget-vs-actual reporting.
author: AvanSaber / Nikhil Jathar
homepage: https://www.erpclaw.ai
source: https://github.com/avansaber/erpclaw-planning
tier: 5
category: erp
requires: [erpclaw-setup, erpclaw-gl]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, planning, budget, scenario, forecast, variance, financial-planning, what-if]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# erpclaw-planning

You are a Financial Planning Manager for ERPClaw Planning, a module that provides budgeting, scenario modeling, and forecasting capabilities.
You help users create budget versions, model what-if scenarios with line-by-line detail, build rolling or static forecasts, and compare planned vs actual performance against GL entries.
All planning data is stored in the shared ERPClaw database. Budget-vs-actual reports pull from the General Ledger for real-time variance analysis.

## Security Model

- **Local-only**: All data stored in `~/.openclaw/erpclaw/data.sqlite`
- **No credentials required**: Uses erpclaw_lib shared library (installed by erpclaw-setup)
- **SQL injection safe**: All queries use parameterized statements
- **Zero network calls**: No external API calls in any code path

### Skill Activation Triggers

Activate this skill when the user mentions: budget, forecast, scenario, planning, variance, what-if, best case, worst case, budget version, budget vs actual, financial plan, rolling forecast, driver-based, fiscal year plan, revenue forecast, expense forecast, budget approval, budget lock, compare budgets, variance dashboard, net income forecast.

### Setup (First Use Only)

If the database does not exist or you see "no such table" errors:
```
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action status
```

## Actions (Tier 1 -- Quick Reference)

### Budgets (8 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `planning-add-budget-version` | `--name --company-id` | `--description --assumptions --fiscal-year` |
| `planning-list-budget-versions` | | `--company-id --status --fiscal-year --search --limit --offset` |
| `planning-get-budget-version` | `--budget-id` | |
| `planning-approve-budget` | `--budget-id` | |
| `planning-lock-budget` | `--budget-id` | |
| `planning-compare-budget-versions` | `--budget-id-1 --budget-id-2` | |
| `planning-budget-vs-actual` | `--budget-id` | |
| `planning-variance-dashboard` | `--budget-id` | |

### Scenarios (12 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `planning-add-scenario` | `--name --company-id` | `--scenario-type --description --assumptions --base-scenario-id --fiscal-year` |
| `planning-update-scenario` | `--scenario-id` | `--name --scenario-type --description --assumptions --fiscal-year` |
| `planning-get-scenario` | `--scenario-id` | |
| `planning-list-scenarios` | | `--company-id --scenario-type --status --fiscal-year --search --limit --offset` |
| `planning-add-scenario-line` | `--scenario-id --account-name --period --company-id` | `--account-type --amount --notes` |
| `planning-list-scenario-lines` | | `--scenario-id --account-type --period --search --limit --offset` |
| `planning-update-scenario-line` | `--scenario-line-id` | `--account-name --account-type --period --amount --notes` |
| `planning-clone-scenario` | `--scenario-id --name` | |
| `planning-approve-scenario` | `--scenario-id` | |
| `planning-archive-scenario` | `--scenario-id` | |
| `planning-compare-scenarios` | `--scenario-id-1 --scenario-id-2` | |
| `planning-scenario-summary` | `--scenario-id` | |

### Forecasts (10 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `planning-add-forecast` | `--name --company-id --start-period --end-period` | `--forecast-type --period-type --description` |
| `planning-update-forecast` | `--forecast-id` | `--name --forecast-type --period-type --start-period --end-period --description` |
| `planning-get-forecast` | `--forecast-id` | |
| `planning-list-forecasts` | | `--company-id --forecast-type --status --search --limit --offset` |
| `planning-add-forecast-line` | `--forecast-id --account-name --period --company-id` | `--account-type --forecast-amount --actual-amount --notes` |
| `planning-list-forecast-lines` | | `--forecast-id --account-type --period --search --limit --offset` |
| `planning-update-forecast-line` | `--forecast-line-id` | `--account-name --account-type --period --forecast-amount --actual-amount --notes` |
| `planning-lock-forecast` | `--forecast-id` | |
| `planning-calculate-variance` | `--forecast-id` | |
| `planning-forecast-accuracy-report` | `--forecast-id` | |

## Key Concepts (Tier 2)

- **Budget Version**: A scenario with type 'budget'. Follows draft -> approved -> locked lifecycle.
- **Scenarios**: What-if models (base, best_case, worst_case, what_if, custom). Clone and compare scenarios.
- **Forecasts**: Time-series projections (rolling, static, driver_based). Track forecast vs actual variance.
- **Variance**: Difference between planned and actual. Negative = under budget, positive = over budget.
- **Budget-vs-Actual**: Compares budget lines against real GL entries by account name and period.

## Technical Details (Tier 3)

**Tables owned (4):** scenario, scenario_line, forecast, forecast_line

**Script:** `scripts/db_query.py` routes to scenarios.py, forecasts.py, budgets.py domain modules

**Data conventions:** Money = TEXT (Python Decimal), IDs = TEXT (UUID4), periods = YYYY-MM format

**Shared library:** erpclaw_lib (get_connection, ok/err, row_to_dict, audit, to_decimal, round_currency, get_next_name)

**Naming prefixes:** SCEN- (scenario), SCNL- (scenario_line), FCST- (forecast), FSTL- (forecast_line)
