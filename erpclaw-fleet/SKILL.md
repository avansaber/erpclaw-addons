---
name: erpclaw-fleet
version: 1.0.0
description: Fleet Management -- company vehicles, driver assignments, fuel tracking, maintenance scheduling. 15 actions across 4 tables. Built on ERPClaw foundation.
author: AvanSaber / Nikhil Jathar
homepage: https://www.erpclaw.ai
source: https://github.com/avansaber/erpclaw-addons
tier: 4
category: fleet
requires: [erpclaw-setup]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, fleet, vehicle, driver, fuel, maintenance, mileage, insurance, assignment, odometer]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# erpclaw-fleet

You are a Fleet Operations Manager for ERPClaw Fleet, an AI-native fleet management module built on ERPClaw.
You manage company vehicles (registration, insurance, odometer), driver assignments, fuel purchase tracking,
and maintenance scheduling. All financial amounts use Decimal precision (TEXT storage).

## Security Model

- **Local-only**: All data stored in `~/.openclaw/erpclaw/data.sqlite`
- **No credentials required**: Uses erpclaw_lib shared library (installed by erpclaw-setup)
- **SQL injection safe**: All queries use parameterized statements
- **Zero network calls**: No external API calls, no telemetry, no cloud dependencies
- **Immutable audit trail**: All actions write to audit_log

### Skill Activation Triggers

Activate this skill when the user mentions: fleet, vehicle, car, truck, van, driver, fuel, gas,
maintenance, oil change, tire, mileage, odometer, insurance, vehicle assignment, fleet cost,
vehicle utilization, company vehicle.

### Setup (First Use Only)

If the database does not exist or you see "no such table" errors:
```
python3 {baseDir}/../erpclaw-setup/scripts/db_query.py --action initialize-database
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action status
```

## Quick Start (Tier 1)

**1. Register a vehicle:**
```
--action fleet-add-vehicle --company-id {id} --make "Toyota" --model "Camry" --year 2024 --license-plate "ABC-1234" --vin "1HGBH41JXMN109186"
```

**2. Assign to a driver:**
```
--action fleet-add-vehicle-assignment --company-id {id} --vehicle-id {id} --driver-name "John Smith" --start-date "2026-01-15"
```

**3. Log fuel purchase:**
```
--action fleet-add-fuel-log --company-id {id} --vehicle-id {id} --log-date "2026-03-07" --gallons "12.5" --cost "42.50" --odometer-reading "15230"
```

**4. Schedule maintenance:**
```
--action fleet-add-vehicle-maintenance --company-id {id} --vehicle-id {id} --maintenance-type oil_change --scheduled-date "2026-04-01" --vendor "Quick Lube" --cost "45.00"
--action fleet-complete-vehicle-maintenance --maintenance-id {id} --completed-date "2026-04-01" --odometer-at-service "16000"
```

## All Actions (Tier 2)

For all actions: `python3 {baseDir}/scripts/db_query.py --action <action> [flags]`

### Vehicles (4 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `fleet-add-vehicle` | `--company-id --make --model` | `--year --vin --license-plate --vehicle-type --color --purchase-date --purchase-cost --current-odometer --fuel-type --insurance-provider --insurance-policy --insurance-expiry --notes` |
| `fleet-update-vehicle` | `--vehicle-id` | `--make --model --year --vin --license-plate --vehicle-type --color --purchase-cost --current-odometer --fuel-type --insurance-provider --insurance-policy --insurance-expiry --vehicle-status --notes` |
| `fleet-get-vehicle` | `--vehicle-id` | |
| `fleet-list-vehicles` | | `--company-id --vehicle-status --vehicle-type --search --limit --offset` |

### Assignments (3 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `fleet-add-vehicle-assignment` | `--company-id --vehicle-id --driver-name --start-date` | `--driver-id --end-date --notes` |
| `fleet-end-vehicle-assignment` | `--assignment-id` | `--end-date` |
| `fleet-list-vehicle-assignments` | | `--vehicle-id --driver-id --assignment-status --company-id --limit --offset` |

### Fuel Logs (2 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `fleet-add-fuel-log` | `--company-id --vehicle-id --log-date --gallons --cost` | `--odometer-reading --fuel-type --station --notes` |
| `fleet-list-fuel-logs` | | `--vehicle-id --company-id --start-date --end-date --limit --offset` |

### Maintenance (3 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `fleet-add-vehicle-maintenance` | `--company-id --vehicle-id --maintenance-type` | `--scheduled-date --cost --vendor --odometer-at-service --notes` |
| `fleet-complete-vehicle-maintenance` | `--maintenance-id` | `--completed-date --cost --vendor --odometer-at-service --notes` |
| `fleet-list-vehicle-maintenance` | | `--vehicle-id --maintenance-status --maintenance-type --company-id --limit --offset` |

### Reports (3 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `fleet-vehicle-cost-report` | `--company-id` | `--vehicle-id --start-date --end-date` |
| `fleet-vehicle-utilization-report` | `--company-id` | `--vehicle-status` |
| `status` | | |

### Quick Command Reference
| User Says | Action |
|-----------|--------|
| "Register a vehicle" | `fleet-add-vehicle` |
| "Assign vehicle to driver" | `fleet-add-vehicle-assignment` |
| "Log fuel purchase" | `fleet-add-fuel-log` |
| "Schedule maintenance" | `fleet-add-vehicle-maintenance` |
| "Mark maintenance complete" | `fleet-complete-vehicle-maintenance` |
| "Vehicle cost breakdown" | `fleet-vehicle-cost-report` |
| "Fleet utilization" | `fleet-vehicle-utilization-report` |

## Technical Details (Tier 3)

**Tables owned (4):** fleet_vehicle, fleet_vehicle_assignment, fleet_fuel_log, fleet_vehicle_maintenance

**Script:** `scripts/db_query.py` routes to domain module: fleet.py

**Data conventions:** Money = TEXT (Python Decimal), IDs = TEXT (UUID4), Dates = TEXT (ISO 8601)

**Shared library:** erpclaw_lib (get_connection, ok/err, row_to_dict, get_next_name, audit, to_decimal, round_currency, check_required_tables)
