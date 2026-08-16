# Changelog

All notable changes to the erpclaw-integrations-shopify addon.

## [Unreleased]

### Fixed
- **GDPR webhook text now matches shipped behavior (`shopify-handle-gdpr`).**
  The shipped, merchant-facing text in `scripts/gdpr.py` promised a "core
  ERPClaw DSR workflow" / "core customer redaction flow" that does not exist
  in the codebase, and described `customers/redact` as nulling out PII on
  `shopify_order` rows when the handler only records the request. Corrected
  the module docstring, both handler docstrings, the `customers/data_request`
  receipt-file `note`, and the `customers/redact` audit note to state what the
  code actually does: the two customer-scoped topics are acknowledgement-only
  today (record the request; `data_request` also writes a receipt/pointer file
  that contains no customer data), and automated per-customer fulfilment is a
  later-wave capability. Text only — no behavior change; `shop/redact` (real
  hard-delete, GL preserved) and `app/uninstalled` were already accurate.

## [1.3.0] — 2026-07-05 — M33 / B8 (integrations deep-sync completion)

### Added
- **Payout transaction sync.** Payout sync now fetches each payout's individual
  ShopifyPayments balance transactions and writes them to
  `shopify_payout_transaction` (previously a read-only table with no writer, so
  the shipped reconciliation feature had nothing to reconcile against). Amounts
  stored as exact TEXT Decimal; transaction types mapped onto the table's CHECK
  enum; the associated order is resolved to its local id so reconciliation Layer 2
  (order coverage) works. Idempotent per (payout, balance-transaction id).

### Fixed
- **Reconciliation no longer vacuously matches zero-transaction payouts.**
  `shopify-run-reconciliation` Layer 1 previously counted a payout with zero
  synced transactions as `matched` (a clean pass that hid missing data). It now
  reports such payouts distinctly as `payouts_no_data` with a
  `no_transaction_data_synced` reason, and the run reports `discrepancy` rather
  than a false `completed` clean match. A reconciliation that finds no data must
  not claim a clean match.
- **Money is Decimal, not float, in reconciliation.** The now-load-bearing
  transaction-net sum switched from `SUM(CAST(net_amount AS REAL))` (a float read
  on a money column) to the module's `decimal_sum` aggregate, so payout matching
  is exact.

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
