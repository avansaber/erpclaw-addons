# CRM Pipeline Stages (Wave 1B F3)

Customizable sales pipelines. The hardcoded 7-value `opportunity.stage` CHECK was
displaced (foundation migration 024); stages now live in growth-owned tables and a
nullable opaque FK `opportunity.pipeline_stage_id` (ADR-0023) points at the chosen
stage. The legacy `stage` text column stays for backward-compat (dual-write +
dual-path report). A default "Standard Sales" 7-stage pipeline (`new`, `contacted`,
`qualified`, `proposal_sent`, `negotiation`, `won`, `lost`) is seeded on install so
existing opportunities have somewhere to point.

## Tables (growth-owned)

- `crm_pipeline` — catalog row (no company scope; shared across the install like a
  CoA template). `name` UNIQUE case-insensitive. `is_default` (one default),
  `is_active`.
- `crm_pipeline_stage` — `crm_pipeline_id`, `stage_order` (UNIQUE per pipeline),
  `name` (UNIQUE case-insensitive per pipeline), `is_terminal_won`,
  `is_terminal_lost` (each at most one per pipeline), `default_probability`
  (TEXT Decimal), `is_active`.

## Actions

| Action | Required | Optional |
|--------|----------|----------|
| `add-crm-pipeline` | `--name` | `--description`, `--set-as-default` |
| `add-crm-pipeline-stage` | `--pipeline`, `--name` | `--order` (default last+1), `--terminal won\|lost`, `--probability`, `--shift-existing` |
| `update-crm-pipeline-stage` | `--id` | `--name`, `--order`, `--probability`, `--terminal won\|lost\|none`, `--is-active 0\|1` |
| `list-crm-pipelines` | | `--limit`, `--offset` (returns `stage_count` each) |
| `list-crm-pipeline-stages` | | `--pipeline` (ordered by `stage_order`) |
| `set-opportunity-pipeline-stage` | `--opportunity`, `--stage` (a `crm_pipeline_stage` id) | |

## Rules

- **stage_order collision:** `add-crm-pipeline-stage` rejects a duplicate order
  unless `--shift-existing` (then every stage at >= the requested order bumps up
  by one, highest-first to avoid transient UNIQUE collisions).
- **Terminal uniqueness:** exactly one `is_terminal_won` and one `is_terminal_lost`
  per pipeline (app-layer; `--terminal none` clears both on update).
- **Cross-pipeline block:** `set-opportunity-pipeline-stage` refuses to move an
  opportunity to a stage in a different pipeline than its current one.
- **Dual-write:** `set-opportunity-pipeline-stage`, `update-opportunity --stage X`,
  `mark-opportunity-won`, and `mark-opportunity-lost` write BOTH the legacy `stage`
  text and the resolved `pipeline_stage_id` (best-effort; NULL on a zero-pipeline
  install, where the text path remains authoritative). Terminal stages set
  probability (won=100, lost=0).
- **`pipeline-report` dual-path:** joins `crm_pipeline_stage` when
  `pipeline_stage_id` is set (groups by pipeline + `stage_order`), else falls back
  to the `stage` text with the original 7-stage CASE ordering. Legacy/text rows
  surface under pipeline name `(none)`. Both kinds coexist in one report.
- `VALID_OPP_STAGES` (app-side) replaces the dropped CHECK as the text-path
  enforcement for `update-opportunity --stage`.

## Backward-compat

- `update-opportunity --stage X` still works.
- Zero-pipeline opportunities still appear in `pipeline-report` (text path).
- Foundation-only installs (growth absent) keep `pipeline_stage_id` NULL; reads
  treat it as an opaque TEXT reference.
