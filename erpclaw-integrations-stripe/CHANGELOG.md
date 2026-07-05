# Changelog

All notable changes to the erpclaw-integrations-stripe addon.

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
