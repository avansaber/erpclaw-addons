# CRM Saved Views (Wave 1B F4)

Persisted, named views over a single CRM entity. A view stores a bounded
filter-JSON (plus optional sort / group-by / column-order JSON, which are opaque
renderer hints in v1) and can be replayed by any of the entity's `list-*` actions
or by `apply-saved-view`. The whole point of the DSL is safety: the user (or an
agent) never writes SQL, and no value or field name is ever interpolated into a
query string.

## Table (growth-owned)

- `crm_saved_view` — `id`, `name`, `entity_type` (CHECK over
  `lead`/`opportunity`/`customer`/`crm_contact`/`crm_company`/`crm_task`),
  `owner_user_id` (nullable), `is_shared` (0/1 CHECK), `filter_json`, `sort_json`,
  `group_by_json`, `column_order_json`, `company_id` (NOT NULL REFERENCES
  `company(id)` — multi-company-safe, DECISION #2), `created_at`, `updated_at`.
  UNIQUE `(company_id, owner_user_id, lower(name))`.

## Actions

| Action | Required | Optional |
|--------|----------|----------|
| `add-crm-saved-view` | `--name`, `--entity-type`, `--filter-json` | `--sort-json`, `--group-by-json`, `--column-order-json`, `--is-shared`, `--owner-user-id` |
| `update-crm-saved-view` | `--id` | `--name`, `--filter-json`, `--sort-json`, `--group-by-json`, `--column-order-json`, `--is-shared`/`--not-shared`, `--owner-user-id` (proof of owner) |
| `get-crm-saved-view` | `--id` | |
| `list-crm-saved-views` | | `--entity-type`, `--owner-user-id`, `--shared-only`, `--limit`, `--offset` |
| `delete-crm-saved-view` | `--id` | `--owner-user-id` (proof of owner) — hard delete |
| `apply-saved-view` | `--view` (the saved-view id) | `--limit`, `--offset` |

`entity_type` is immutable after creation (a view's column whitelist is bound to
its entity). `update`/`delete` are owner-only when the view has a non-null
`owner_user_id`: the caller must pass `--owner-user-id` matching the stored owner.
A view with a NULL owner is mutable by anyone (system/shared default).

## Replaying a view on a list action

`list-leads`, `list-opportunities`, `list-crm-contacts`, and `list-crm-companies`
take `--saved-view-id`; the view's `entity_type` must match the list action (a
lead view cannot be applied to opportunities). The `customer` entity is
foundation-owned (`erpclaw-selling`), so growth does NOT add a flag to
`list-customers`; instead `apply-saved-view` (Option A, DECISION #1) calls
`list-customers` and post-filters the result in Python.

`apply-saved-view --view <id>` is the uniform entry point: it dispatches the 4
native entities to their `list-*` handler with the filter applied, and routes
`customer` through the Option-A wrapper. `crm_task` has no list-side wiring in v1
and is rejected with guidance.

## Filter-JSON DSL

A filter is a tree of GROUP and LEAF nodes:

```json
{
  "logic": "AND",
  "conditions": [
    {"field": "source", "op": "eq", "value": "referral"},
    {"logic": "OR", "conditions": [
      {"field": "status", "op": "eq", "value": "qualified"},
      {"field": "status", "op": "eq", "value": "contacted"}
    ]}
  ]
}
```

- **GROUP** `{"logic": "AND"|"OR", "conditions": [ ...nodes ]}` — non-empty list.
- **LEAF** `{"field": <name>, "op": <operator>, "value": <scalar|list>}`.

### Operators (whitelist)

| Op | SQL | Value shape |
|----|-----|-------------|
| `eq` | `col = ?` | scalar |
| `neq` | `col <> ?` | scalar |
| `contains` | `col LIKE ?` (`%value%`) | scalar |
| `gt` | `col > ?` | scalar |
| `lt` | `col < ?` | scalar |
| `in` | `col IN (?, ?, …)` | non-empty list of scalars |
| `between` | `col BETWEEN ? AND ?` | `[low, high]` |

### Column whitelist

`field` must be one of the entity's native filterable columns (curated per entity
in `ENTITY_NATIVE_COLUMNS`) **or** a registered UDF field name for that entity
(`custom_fields.get_custom_fields`). A UDF condition compiles to a fully
parameterized `EXISTS` sub-select over `custom_field_value`. An unknown or
malicious `field` (e.g. `'; DROP TABLE lead; --`) is rejected at validate time,
**before any SQL is built**.

### Validation + limits

The filter is validated at **save** time (`add`/`update`) AND again at **apply**
time (defence in depth — a UDF could be dropped between save and apply). Limits:
nesting depth ≤ 5, total leaf conditions ≤ 50 (DoS guards). Values are always
bound parameters; only whitelisted column names and fixed operator fragments are
ever placed into the SQL string.

## Dialect notes (PostgreSQL-strict)

All emitted SQL is standard (`=`, `<>`, `LIKE`, `>`, `<`, `IN`, `BETWEEN`) — no
SQLite-only constructs. No `GROUP BY` is emitted by the filter builder. The UDF
`EXISTS` correlation uses the unaliased base table name (`<entity_type>.id`),
which resolves identically on SQLite and PostgreSQL.
