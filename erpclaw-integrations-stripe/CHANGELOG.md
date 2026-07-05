# Changelog

All notable changes to the erpclaw-integrations-stripe addon.

## [2.2.0] — 2026-07-05 — M33 / B8 (integrations deep-sync completion)

### Added
- **Connect transfer sync (`transfer` object type).** `stripe-start-sync
  --object-type transfer` and full sync now pull Stripe Connect transfers into
  `stripe_transfer` (previously a read-only table with no writer — `stripe-list-transfers`
  and the Connect payout report returned nothing after a sync). Amounts stored
  as exact TEXT Decimal; idempotent via `INSERT OR REPLACE`.
- **Credit-note sync (`credit_note` object type) + reader.** Full sync now pulls
  Stripe credit notes into `stripe_credit_note`, and a new **`stripe-list-credit-notes`**
  action reads them back (the table previously had neither a writer nor a reader).
- **Fee-detail expansion.** Balance-transaction sync now expands each transaction's
  `fee_details[]` into `stripe_fee_detail` rows (keyed by the local balance-transaction
  id), so `stripe-fee-report` reflects the granular fee breakdown instead of falling
  back to the aggregate. Graceful when the API omits the expansion; idempotent on re-sync.

### Audit-only (no change)
- **`stripe_application_fee` already has a writer.** The register lists
  `erpclaw-integrations-stripe` as a writer (the GL-posting path
  `stripe-post-connect-fee-gl` sets `erpclaw_journal_entry_id` via
  `gl_posting.py`). Per B8 this resolves to verify-only — no `_sync_application_fees`
  handler was added.

### Schema (DEVIATION from the M33 "zero migrations" plan — see PR notes)
- **`stripe_sync_job.object_type` CHECK widened** to include `transfer` and
  `credit_note`. The plan's design routes the two new types through the shared
  sync-job machinery, which writes a `stripe_sync_job` row whose `object_type` was
  CHECK-constrained to the old 8 values; without this the new syncs fail with an
  IntegrityError on job creation. `init_db.py` carries the widened CHECK for fresh
  installs; **new `migrations/001_widen_sync_job_object_types.py`** widens it on
  existing (marketplace-live) installs (rebuild idiom of foundation migration 007,
  row-preserving, idempotent, dialect-aware). This is the stripe addon's first
  migration. Purely permissive (superset enum) — no existing row can be invalidated.

## [2.1.0] — 2026-07-05 — M31 H6 (foundation-hygiene mini-wave)

### Changed
- **`stripe-delete-gl-rule` response shape converged (user-visible).** The
  successful-delete response is now `{"gl_rule_id": <id>, "is_active": 0}`
  (the envelope still adds `status: "ok"`). Previously the handler emitted
  `{"gl_rule_id": <id>, "status": "deleted"}`, but `response.ok()` always
  overwrites `status` with `"ok"`, so the `"deleted"` value never reached the
  caller. The addon now reports the real persisted state (`is_active: 0`)
  instead of a dead field, and the shape matches `shopify-delete-gl-rule`. The
  already-deleted error message is unchanged (`GL rule <id> is already deleted`).

### Internal
- **API-key encryption upgraded to AES-256-GCM.** `restricted_key_enc` /
  `webhook_secret_enc` are now encrypted with the machine master key
  (`enc:v2:` ciphertext) via the shared `erpclaw_lib.integration_secrets`,
  replacing the previous home-salted XOR obfuscation. Existing installs are
  read transparently (legacy XOR values decrypt unchanged); values upgrade to
  GCM on the next account write. No user action required.
- Shared, single-sourced helpers (validators, sync-job lifecycle,
  reconciliation-run fetch, GL-rule soft-delete) hoisted to `erpclaw_lib`;
  behavior unchanged except the converged delete response above.
