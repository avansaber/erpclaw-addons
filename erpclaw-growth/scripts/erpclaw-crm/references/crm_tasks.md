# CRM Tasks (Wave 1B F2)

First-class task entity with a status / priority / due-date lifecycle, plus a
many-to-many tie to any CRM entity. Tables are growth-owned (`crm_task`,
`crm_task_link`). `crm_activity` is NOT replaced — legacy `activity_type='task'`
rows stay valid. Naming collision: `add-task` routes to `erpclaw-projects`, so
the CRM task creator is `add-crm-task`.

## Actions

| Action | Required | Optional |
|--------|----------|----------|
| `add-crm-task` | `--subject` | `--description`, `--priority`, `--due-date`, `--assigned-to`, `--created-by`, `--link-to "<type>:<id>"` (repeatable), `--company-id` |
| `update-crm-task` | `--crm-task-id` | `--subject`, `--description`, `--priority`, `--due-date`, `--assigned-to` |
| `get-crm-task` | `--crm-task-id` | |
| `list-crm-tasks` | | `--status`, `--priority`, `--assigned-to`, `--linked-to "<type>:<id>"`, `--overdue`, `--due-within-days N`, `--limit`, `--offset`, `--company-id` |
| `complete-crm-task` | `--crm-task-id` | `--notes` |
| `cancel-crm-task` | `--crm-task-id` | `--reason` |
| `link-task-to-entity` | `--task`, `--entity-type`, `--entity-id` | |
| `unlink-task-from-entity` | `--task`, `--entity-type`, `--entity-id` | |

## Status + priority

- `crm_task.status`: `open` (default), `in_progress`, `done`, `cancelled`.
  Status is NOT set via `update-crm-task`; use `complete-crm-task` /
  `cancel-crm-task` for the terminal transitions.
- `crm_task.priority`: `low`, `medium` (default), `high`, `urgent`.

## Link types + runtime existence check

`linked_entity_type` is one of `lead`, `opportunity`, `customer`,
`crm_contact`, `crm_company`. Every link target (on `add-crm-task --link-to`
and on `link-task-to-entity`) is existence-checked at runtime against the
resolved table, scoped to the same `company_id`. A bad target on
`add-crm-task` rolls back the entire create (single transaction). Re-linking
an existing `(task, type, id)` tuple is rejected (already linked); unlinking a
link that does not exist is rejected (no silent no-op). `linked_count` is a
denormalized count maintained on every link/unlink.

## Validation rules

- `complete-crm-task` is rejected if the task is already `done` (idempotent
  terminal) or `cancelled`. `cancel-crm-task` is rejected if already terminal.
- `--due-date` may be in the past (for backfill); the create is allowed and an
  audit note flags `overdue_on_create`.
- `list-crm-tasks --overdue` returns non-terminal tasks whose `due_date` is
  strictly before today; `--due-within-days N` returns tasks due between today
  and today+N inclusive.
