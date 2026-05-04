---
name: erpclaw-os-engine
version: 1.0.0
description: ERPClaw OS Engine -- the optional self-improvement layer for ERPClaw. Module generation from natural language, in-module feature injection, sandboxed execution, deploy pipeline, DGM evolution, gap detection, semantic checks, compliance weather, heartbeat analysis. 31 actions, all `os-` prefixed. Dev-time / power-user tooling. Foundation skill `erpclaw` is required (>= v4.0.0).
author: AvanSaber
homepage: https://github.com/avansaber/erpclaw-addons
source: https://github.com/avansaber/erpclaw-addons
tier: 4
category: expansion
requires: [erpclaw]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, ai-native, os, self-improving, module-generation, code-generation, sandbox, dgm, deploy, compliance, heartbeat, gap-detection, semantic-check]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action os-status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# ERPClaw OS Engine

The optional self-improvement layer for ERPClaw. Module generation, deploy pipeline, evolutionary improvement (Darwin-Gödel Machine), and the meta-tooling that lets ERPClaw extend itself.

## When to install

You want this addon if you are:

- A developer authoring new ERPClaw vertical modules (`os-generate-module`)
- Adding a feature to an existing module (`os-add-feature-to-module`)
- Operating ERPClaw in a power-user or evolution mode where the OS proposes new patterns over time

You **don't** need this addon for normal day-to-day ERP use (invoicing, accounting, inventory, reports). The foundation `erpclaw` skill ships everything for that path.

## What ERPClaw foundation keeps

The foundation skill (`erpclaw`) keeps the runtime-essential parts of the OS:

- **12-step GL invariant validation** (`erpclaw_lib.gl_invariants.check_gl_invariants`) — runs on every submit
- Constitutional articles and `validate-module` action
- Schema migration (`schema-plan`, `schema-apply`, `schema-rollback`, `schema-drift`)
- `dependency_resolver.py` for cross-module install ordering

## What this addon provides

| Domain | Actions |
|---|---|
| Module generation | `os-generate-module`, `os-configure-module`, `os-list-industries`, `os-classify-operation` |
| In-module features | `os-add-feature-to-module`, `os-check-feature-completeness`, `os-list-feature-matrix` |
| Deploy pipeline | `os-deploy-module`, `os-deploy-audit-log`, `os-install-suite` |
| Evolutionary improvement | `os-dgm-run-variant`, `os-dgm-list-variants`, `os-dgm-select-best` |
| Compliance + health | `os-compliance-weather-status`, `os-heartbeat-analyze`, `os-heartbeat-report`, `os-heartbeat-suggest` |
| Improvement loop | `os-log-improvement`, `os-list-improvements`, `os-review-improvement` |
| Gap analysis + research | `os-detect-gaps`, `os-suggest-modules`, `os-research-business-rule`, `os-get-implementation-guide` |
| Adversarial audit | `os-run-audit` |
| Semantic checks | `os-semantic-check`, `os-semantic-rules-list` |
| Web dashboard provisioning | `os-setup-web-dashboard` |
| Status | `os-status` |

All 31 actions use the `os-` prefix to avoid namespace collision with foundation actions and to clearly signal the dev-time nature of the work.

## Install

```bash
# From the foundation skill's module manager
python3 ~/.openclaw/workspace/skills/erpclaw/scripts/module_manager.py \
  --action install-module --module-name erpclaw-os-engine
```

The addon installs from `github.com/avansaber/erpclaw-addons` subdir `erpclaw-os-engine`.

This addon is **not published to ClawHub directly**. ERPClaw's distribution model is: foundation on ClawHub, addons via `module_manager.py` from GitHub. The reason is that this addon contains the dynamic-code-generation patterns flagged by ClawHub's static-analysis scanner (`suspicious.dynamic_code_execution`). The patterns are intentional (it's the module-generation engine) and isolated to this addon, so foundation users who don't install it stay scan-clean.

## Runtime self-check

On any action invocation, the addon's `db_query.py` verifies that `erpclaw_lib` is importable (foundation present) before doing real work. If foundation isn't installed, you'll get:

```json
{"error": "erpclaw-os-engine requires foundation skill 'erpclaw' (v4.0.0+) to be installed", "missing_dependency": "erpclaw"}
```

## Safety gates

The OS engine includes the following safety mechanisms (preserved from when this code lived in the foundation skill):

- **Constitution validation** — every generated module is validated against 18 constitutional articles before deploy
- **Sandboxed execution** — newly-generated modules run inside `sandbox.py` first, with full GL invariant checks on the sandbox DB
- **`.bak` backups** — `os-add-feature-to-module` always creates a `.bak` backup before mutating an existing file, validates syntax via `ast.parse` after, and refuses to modify files in `SAFETY_EXCLUDED_FILES`
- **Adversarial audit** — `os-run-audit` runs adversarial test cases against new modules

These gates are not weaker for being in an addon. The addon is opt-in to bound the security surface for foundation users; if you install it, the same constitutional validation applies.

## Tier 1: Quickstart

Generate your first module:

```bash
python3 scripts/db_query.py --action os-generate-module \
  --module-name myindustry \
  --industry "Custom Workflow"
```

Add a feature to an existing module:

```bash
python3 scripts/db_query.py --action os-add-feature-to-module \
  --module-path source/erpclaw-addons/myindustry \
  --action-name add-widget
```

## Tier 2: Common workflows

### Run the deploy pipeline

```bash
python3 scripts/db_query.py --action os-deploy-module \
  --module-name myindustry --target sandbox
```

### Check feature completeness

```bash
python3 scripts/db_query.py --action os-check-feature-completeness \
  --src-root source/erpclaw-addons/myindustry
```

### Run adversarial audit on a generated module

```bash
python3 scripts/db_query.py --action os-run-audit \
  --module-name myindustry
```

## Tier 3: Advanced

### DGM (Darwin-Gödel Machine) evolution

The DGM engine runs variants of generated modules against a fitness landscape and selects the best. See `dgm_engine.py` and `variant_manager.py`.

```bash
python3 scripts/db_query.py --action os-dgm-run-variant \
  --module-name myindustry --variant-id v1
```

### Compliance weather + heartbeat

Periodic system health snapshots:

```bash
python3 scripts/db_query.py --action os-compliance-weather-status
python3 scripts/db_query.py --action os-heartbeat-report
```

### Setup the web dashboard

Provisions the ERPClaw Web frontend (`erpclaw-web` repo) end-to-end including domain + SSL:

```bash
python3 scripts/db_query.py --action os-setup-web-dashboard \
  --domain dashboard.example.com --ssl auto
```

## Source

- Code: https://github.com/avansaber/erpclaw-addons (subdir `erpclaw-os-engine`)
- Foundation skill: https://github.com/avansaber/erpclaw
- Documentation: https://www.erpclaw.ai/docs/
- License: MIT
