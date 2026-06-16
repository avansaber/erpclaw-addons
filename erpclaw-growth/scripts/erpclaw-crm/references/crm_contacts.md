# CRM Contacts & Companies (Wave 1B F1)

Separate Contact (person) and Company (organization) entities, plus the
association and promotion flows. Tables are growth-owned (`crm_contact`,
`crm_company`, `crm_contact_role`); foundation `lead` / `opportunity` /
`customer` / `crm_activity` carry nullable opaque FK columns to them
(ADR-0023). Growth is the sole writer of those FK columns.

## Actions

| Action | Required | Optional |
|--------|----------|----------|
| `add-crm-contact` | `--name` | `--email`, `--phone`, `--mobile`, `--job-title`, `--linkedin-url`, `--crm-company-id`, `--lifecycle`, `--assigned-to`, `--notes`, `--company-id` |
| `update-crm-contact` | `--crm-contact-id` | any of the add fields |
| `get-crm-contact` | `--crm-contact-id` | |
| `list-crm-contacts` | | `--crm-company-id`, `--lifecycle`, `--search`, `--limit`, `--offset`, `--company-id` |
| `remove-crm-contact` | `--crm-contact-id` | (soft delete; lifecycle→other; cascades roles) |
| `add-crm-company` | `--name` | `--domain`, `--industry`, `--revenue`, `--linkedin-url`, `--linked-customer-id`, `--lifecycle`, `--assigned-to`, `--notes`, `--company-id` |
| `update-crm-company` | `--crm-company-id` | any of the add fields |
| `get-crm-company` | `--crm-company-id` | |
| `list-crm-companies` | | `--lifecycle`, `--search`, `--limit`, `--offset`, `--company-id` |
| `link-contact-to-company` | `--crm-contact-id`, `--crm-company-id` | `--role-title`, `--is-primary` |
| `merge-crm-contacts` | `--primary-contact-id`, `--duplicate-contact-id` | |
| `promote-contact-to-customer` | `--crm-contact-id` | |

## Lifecycles

- `crm_contact.lifecycle`: `lead` (default), `mql`, `sql`, `customer`, `other`.
- `crm_company.lifecycle`: `prospect` (default), `customer`, `partner`, `vendor`, `other`.

## Validation rules

- `crm_contact.email` and `crm_company.domain` are UNIQUE case-insensitively
  within a company (partial unique index on `lower(...)`). `add-*` / `update-*`
  reject a duplicate with a friendly message; the user consolidates duplicates
  with `merge-crm-contacts`.
- `crm_company.annual_revenue` is TEXT (Python `Decimal`), never float.

## merge-crm-contacts (single transaction)

Copies non-null fields from the duplicate onto the primary where the primary
is blank, reassigns every FK reference — `crm_contact_role` plus the foundation
`lead` / `opportunity` / `crm_activity.crm_contact_id` columns — to the primary,
then soft-deletes the duplicate (`lifecycle='other'`). All in one
`BEGIN…COMMIT`; any failure rolls back fully.

## promote-contact-to-customer (cross-skill)

Creates a `customer` row in `erpclaw-selling` via
`cross_skill.call_skill_action("erpclaw-selling", "add-customer", …)` (Article 5,
not a raw subprocess), sets the contact lifecycle to `customer`, and back-links
the contact's company (`crm_company.linked_customer_id`) to the new customer.
A cross-skill failure rolls back the entire growth-side transaction (no contact
lifecycle change, no back-link, no orphan).

## Backfill (run-once, operator tool)

`scripts/erpclaw-crm/backfill_crm_contact_fks.py` links EXISTING leads /
opportunities to contacts + companies by exact email / name match. `--dry-run`
by default (reports only); `--execute` applies in one audit-logged transaction.
`customer.crm_company_id` backfill is deferred (selling-owned column; needs an
erpclaw-selling-side action — out of F1 scope).
