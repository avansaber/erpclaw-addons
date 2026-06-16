# CRM CSV Import / Export (Wave 1B F6)

Bulk load and extract the four main CRM entities — lead, opportunity,
crm_contact, crm_company — as CSV files. No new tables: import writes the same
columns the per-entity `add-*` actions write; export reads a curated set of
business columns. The shared library lives in the foundation
(`erpclaw_lib/csv_import.py` + `erpclaw_lib/csv_export.py`); the actions here are
the CRM-side wrappers. XLSX is deferred (M11 / Wave 4).

## Import actions

`import-leads`, `import-opportunities`, `import-crm-contacts`,
`import-crm-companies`.

```
import-leads --file <path.csv> --on-duplicate {skip|update|fail} [--company-id C]
```

- `--file` (required): the source CSV. The path is `realpath`-resolved (symlinks
  followed), must end in `.csv`, and must be a regular file. A traversal attempt
  like `../../etc/passwd` is rejected (no `.csv` extension).
- `--on-duplicate` (**required — no default**): forces explicit intent.
  - `skip` — leave the existing row, count it under `skipped`.
  - `update` — overwrite the existing row in place from the CSV row's values
    (only columns present + non-blank in the file are written; a sparse row
    never blanks an existing column). No second row is inserted.
  - `fail` — abort on the first duplicate; the **entire** import rolls back.
- The whole import is one transaction. Any validation error or a `fail`-mode
  duplicate rolls everything back (no partial import).

### Required + optional CSV columns

| entity | required | dedup key | money cols (Decimal) |
|---|---|---|---|
| lead | `lead_name` | `email` (CI, per company) | — |
| opportunity | `opportunity_name` | none (every row inserts) | `expected_revenue` |
| crm_contact | `name` | `email` (CI, per company) | — |
| crm_company | `name` | `domain` (CI, per company) | `annual_revenue` |

Optional columns mirror the entity's `add-*` action flags (e.g. lead:
`company_name, email, phone, source, territory, industry, status, notes`;
opportunity: `opportunity_type, probability, source, expected_closing_date,
stage, notes`; contact: `phone, mobile, job_title, linkedin_url, lifecycle,
notes`; company: `domain, industry, employee_count, linkedin_url, lifecycle,
linked_customer_id, notes`).

### Validation (before any write)

- A missing **required column** is rejected up front (no insert attempted).
- A present-but-blank required **value** in a row is rejected.
- Money columns must parse as `Decimal` — a non-numeric value (e.g.
  `not-a-number`) is rejected; a blank optional money cell falls back to the
  default (`0`) / NULL.
- Enum columns (`source`, `status`, `opportunity_type`, `stage`, `lifecycle`)
  must be one of the entity's valid values.
- `email` columns must match the email format.
- Leads and opportunities get a fresh `naming_series` per imported row; every
  row is scoped to `--company-id`.

### Result

```json
{"status": "ok", "entity_type": "lead", "on_duplicate": "skip",
 "imported": 5, "updated": 0, "skipped": 0, "total_rows": 5,
 "message": "Imported 5 lead row(s); updated 0, skipped 0."}
```

## Export actions

`export-leads`, `export-opportunities`, `export-crm-contacts`,
`export-crm-companies`.

```
export-leads --output <path.csv> [--status S] [--include-udfs] [--company-id C]
```

- `--output` (required): destination `.csv`. The parent directory must exist;
  the file is **overwritten** if present (mode `w`, never appended). Encoding is
  UTF-8 with BOM (`utf-8-sig`) so Excel opens it cleanly.
- Simple filters (pre-F4; the F4 `--filter-json` DSL supersedes these when a
  saved view is used):
  - lead: `--status`
  - opportunity: `--stage` (or `--status` as an alias for `stage`)
  - crm_contact / crm_company: `--lifecycle`
- `--include-udfs`: append `cf_<field_name>` columns for any M1 custom field
  registered on the table that has a value on at least one exported row. Columns
  with no data anywhere are omitted.
- Output is scoped to `--company-id` and ordered `created_at DESC`.

### Round-trip guarantee

Export then re-import the same file with `--on-duplicate skip` is a no-op:
every exported row already exists (matched on its dedup key), so `imported`
is 0 and `skipped` equals the row count. UDF columns are export-only in v1
(UDF *import* is out of scope).

## Library notes

- `csv_import.bulk_insert` gained a keyword-only `on_duplicate_mode` plus an
  optional `dup_check(conn, row)` callback and `update_columns`. With no
  `dup_check` it keeps its legacy `int` return (the 4 pre-F6 foundation callers
  — item/customer/supplier/account/opening-balance imports — are unaffected);
  with a `dup_check` it returns `{inserted, updated, skipped}`.
- All SQL is `?`-bound and dialect-portable (no `sqlite_master`, no SQLite-only
  constructs) so the same code runs on PostgreSQL.
