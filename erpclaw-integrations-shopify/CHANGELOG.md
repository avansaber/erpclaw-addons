# Changelog

All notable changes to the erpclaw-integrations-shopify addon.

## [1.2.0] — 2026-07-05 — M31 H6 (foundation-hygiene mini-wave)

### Changed
- **`shopify-delete-gl-rule` response shape converged (user-visible).** The
  successful-delete response is now `{"gl_rule_id": <id>, "is_active": 0}`
  (the envelope still adds `status: "ok"`). Previously the handler returned
  `{"id": <id>, "is_active": 0}`; the identifier key is now the explicit
  `gl_rule_id` and the shape matches `stripe-delete-gl-rule`. The
  already-inactive error message changed from `GL rule <id> is already
  inactive` to `GL rule <id> is already deleted`.

### Internal
- **Access-token / HMAC-secret encryption upgraded to AES-256-GCM.**
  `access_token_enc` / `hmac_secret_enc` are now encrypted with the machine
  master key (`enc:v2:` ciphertext) via the shared
  `erpclaw_lib.integration_secrets`, replacing the previous home-salted XOR
  obfuscation. Existing installs are read transparently (legacy XOR values
  decrypt unchanged); values upgrade to GCM on the next account write. No user
  action required.
- Shared, single-sourced helpers (validators, sync-job lifecycle,
  reconciliation-run fetch, GL-rule soft-delete) hoisted to `erpclaw_lib`;
  behavior unchanged except the converged delete response above.
