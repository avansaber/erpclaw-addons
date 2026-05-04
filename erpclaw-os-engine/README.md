# erpclaw-os-engine

Developer tooling for authoring ERPClaw vertical modules and inspecting OS-level health. Sandbox-first generation; user approval required before deploy. Optional addon, not installed by default.

## When to install

You want this addon if you are:

- A developer authoring new ERPClaw vertical modules (`os-generate-module`)
- Adding a feature to an existing module (`os-add-feature-to-module`)
- Operating ERPClaw in a power-user mode where the OS proposes patterns over time

You don't need this addon for normal day-to-day ERP use (invoicing, accounting, inventory, reports). The foundation `erpclaw` skill ships everything for that path.

Requires foundation skill `erpclaw >= 4.0.0`.

## Install

```bash
python3 ~/.openclaw/workspace/skills/erpclaw/scripts/module_manager.py \
  --action install-module --module-name erpclaw-os-engine
```

Installs from `github.com/avansaber/erpclaw-addons` subdir `erpclaw-os-engine`. The addon is not published to ClawHub directly — ERPClaw's distribution model is foundation on ClawHub, addons via `module_manager.py` from GitHub.

## Safety gates

The OS engine includes the following safety mechanisms:

1. **Constitution validator (18 articles).** Every generated module is checked against 18 financial-correctness laws covering naming conventions, data types, GL immutability, transaction atomicity, and audit trail requirements. A module that fails any article is rejected at generation time and never proceeds to the sandbox.
2. **Sandboxed execution.** Modules that pass the constitution run inside `sandbox.py` against an isolated test database, with full GL invariant checks on the sandbox DB.
3. **AST + `.bak` backup on in-place edits.** `os-add-feature-to-module` always creates a `.bak` backup before mutating an existing file, validates syntax via `ast.parse` after, and refuses to modify files in `SAFETY_EXCLUDED_FILES`.
4. **Adversarial audit.** `os-run-audit` runs a dedicated set of adversarial test cases against new modules.
5. **Explicit user approval before deploy.** `os-deploy-module` requires the user-approved flag and is on the foundation's "always confirm" list. A module that passes all mechanical checks still does not deploy automatically — the user reviews the diff and approves the deploy.

The mechanical checks are designed to fail fast (rejecting non-compliant modules without consuming reviewer time). They are not a substitute for human review of business logic, domain edge cases, or UX decisions; those still require manual sign-off.

## What this addon provides

| Domain | Actions |
|---|---|
| Module generation | `os-generate-module`, `os-configure-module`, `os-list-industries`, `os-classify-operation` |
| In-module features | `os-add-feature-to-module`, `os-check-feature-completeness`, `os-list-feature-matrix` |
| Deploy pipeline | `os-deploy-module`, `os-deploy-audit-log`, `os-install-suite` |
| Variant analysis | `os-dgm-run-variant`, `os-dgm-list-variants`, `os-dgm-select-best` |
| Compliance + health | `os-compliance-weather-status`, `os-heartbeat-analyze`, `os-heartbeat-report`, `os-heartbeat-suggest` |
| Improvement loop | `os-log-improvement`, `os-list-improvements`, `os-review-improvement` |
| Gap analysis + research | `os-detect-gaps`, `os-suggest-modules`, `os-research-business-rule`, `os-get-implementation-guide` |
| Adversarial audit | `os-run-audit` |
| Semantic checks | `os-semantic-check`, `os-semantic-rules-list` |
| Web dashboard provisioning | `os-setup-web-dashboard` |
| Status | `os-status` |

All actions use the `os-` prefix to avoid namespace collision with foundation actions and to make the dev-time scope explicit.

## What ERPClaw foundation keeps

Foundation `erpclaw` keeps the runtime-essential parts of the OS:

- **12-step GL invariant validation** (`erpclaw_lib.gl_invariants.check_gl_invariants`) — runs on every submit
- Constitutional articles + `validate-module` action (read-only check, available without the addon)
- Schema migration (`schema-plan`, `schema-apply`, `schema-rollback`, `schema-drift`)
- `dependency_resolver.py` for cross-module install ordering

## Runtime self-check

On any action invocation, the addon's `db_query.py` verifies that `erpclaw_lib` is importable (foundation present) before doing real work. If the foundation is not installed, you get:

```json
{"error": "erpclaw-os-engine requires foundation skill 'erpclaw' (v4.0.0+) to be installed", "missing_dependency": "erpclaw"}
```

## Quickstart

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

Run the deploy pipeline (sandbox first, requires user approval to advance):

```bash
python3 scripts/db_query.py --action os-deploy-module \
  --module-name myindustry --target sandbox
```

Run the adversarial audit:

```bash
python3 scripts/db_query.py --action os-run-audit \
  --module-name myindustry
```

## License

MIT — see `LICENSE.txt`.

See `SKILL.md` for the full action catalog with parameters and Tier 2/3 workflows.
