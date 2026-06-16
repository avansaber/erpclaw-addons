# CRM Global Search (Wave 1B F5)

A single search box across the CRM. `global-crm-search` fans one query out over
several entities, runs the same ranked match against each, and merges the hits
into one flat, deterministically-ordered list. There is no new table and no
FTS5; it reuses the per-entity name columns the existing `--search` flags
already match on. `customer` is foundation-owned, so the fan-out reads it via a
plain SELECT (a READ, never a cross-module write).

## Action

`global-crm-search --query Q [--limit N] [--entity-types "lead,opportunity,customer,crm_contact,crm_company"]`

- `--query` (required): the search term. Minimum **2 characters**; a shorter
  query is rejected with an error.
- `--limit` (optional, default 50): max results returned after merge. **Hard
  capped at 200** across all entity types (a larger `--limit` is clamped to 200;
  `< 1` is clamped to 1).
- `--entity-types` (optional): CSV restricting the fan-out set. Defaults to the
  V1 set `lead,opportunity,customer,crm_contact,crm_company`. An **unknown**
  entity type (e.g. a typo, or `crm_task` before F2 ships) or one whose **table
  is absent** is **skipped gracefully** (reported in `skipped_entity_types`),
  never a crash — existence is checked with `table_exists` (PG-strict; no
  `sqlite_master`).

## Searched columns + display per entity

| entity_type | name columns (matched) | display_name | snippet |
|---|---|---|---|
| `lead` | `lead_name`, `company_name`, `email` | `lead_name` | `company_name` |
| `opportunity` | `opportunity_name`, `source` | `opportunity_name` | `source` |
| `customer` | `name`, `email`, `phone` | `name` | `email` |
| `crm_contact` | `name`, `email`, `job_title` | `name` | `job_title` |
| `crm_company` | `name`, `domain`, `industry` | `name` | `domain` |

Every entity is scoped to the caller's `--company-id`.

## Ranking + merge

Per entity, three ranked passes (each row keeps its best/lowest rank):

1. **rank 1** — exact match (case-insensitive) on any name column.
2. **rank 2** — prefix match `q%`.
3. **rank 3** — contains match `%q%`.

All LIKE values are `?`-bound (never f-string-interpolated); `lower()` is applied
to both sides so prefix/contains matching is case-insensitive on SQLite **and**
PostgreSQL (PG `LIKE` is case-sensitive). The merged list is sorted by
`match_rank` asc, then `updated_at` desc, then the entity-type order (the order
given in `--entity-types`, else the V1 default order), then `id` — a fully
deterministic ordering on both backends.

## Result shape

```json
{
  "status": "ok",
  "query": "Acme",
  "results": [
    {"entity_type": "lead", "id": "...", "display_name": "Acme", "snippet": "Acme Inc", "updated_at": "...", "match_rank": 1}
  ],
  "total": 5,
  "returned": 5,
  "limit": 50,
  "skipped_entity_types": ["crm_task"]
}
```

`total` is the full merged count before the limit; `returned` is `min(total, limit)`.
`skipped_entity_types` is present only when at least one requested type was skipped.
