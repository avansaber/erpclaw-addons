# Changelog

All notable changes to the erpclaw-growth addon.

## [Unreleased] — Wave G F7 / M40a (anomaly-sweep NL steer)

### Removed
- **Two tables this module never used are gone (M63-C).** `elimination_rule` and
  `elimination_entry` arrived here from the GL domain during an earlier split. No
  growth code has ever read or written either one: their only reader and writer was
  a foundation action (`run-elimination` in erpclaw-reports), which has been retired
  because it posted group eliminations into the operating companies' live books.
  Intercompany elimination belongs to the consolidation layer
  (`advacct_elimination_entry`), per ADR-0010. `init_db.py` no longer creates the
  pair (23 tables here now, was 25), and new **migration 007** removes them from
  existing installs. Before dropping anything it **archives every row of both tables**
  to `<ERPCLAW_HOME>/archive/m63c_elimination_legacy_<database>_<UTC>.json` with full
  column fidelity (money stays the exact TEXT it was stored as), reads the file back
  to confirm it landed, and prints the path; `--report-only` writes nothing and states
  what the real run would do. **An archive is never overwritten:** the name carries a
  per-database tag, the file is created exclusively rather than truncated, and a name
  already in use yields the next one, so two databases migrated in the same second and
  a re-run after a failure each get their own sidecar. Nothing is dropped that could
  not be archived first — an unwritable archive directory leaves every row in place and
  fails loudly. It **never
  touches `gl_entry`** — entries already posted stay in the books, and the archive
  keeps each one's `source_gl_entry_id` / `target_gl_entry_id` so a controller can
  reverse them deliberately with a journal entry. Empty or absent tables produce no
  archive file, and a second run is a clean no-op. No growth action changes.
  SIM: `planning/simlogs/m63c_SIM_2026-08-12.md`.

### Changed
- **`detect-anomalies` is now reachable in plain business English.** The anomaly sweep
  was steer-invisible: a non-technical owner who asks *"anything unusual in the books?"*
  ("off", "irregular", "doesn't look right") did not route to `detect-anomalies` (M40a).
  `SKILL.md` widens the Skill Activation Triggers sentence with that paraphrase vocabulary
  and adds a worked Wrong/Right steer inline on the `detect-anomalies` catalog row: route
  those phrasings to a fresh 21-type sweep, **not** a ledger keyword-search or `list-anomalies`
  (which only re-reads prior finds). **Doc-only** — no action, schema, or GL change; the two
  edits are net-zero on line count and `SKILL.md` stays ≤ 300 lines. Version unchanged
  (2.10.0): registry re-sign is the wave release gate, not this row.

### Testing (NL suite)
- Authored `planning/nl_test_suite/scenarios_growth_basics.yaml` scenario
  `growth-b01-anomaly-sweep` (phrasing contains neither "anomaly" nor "suspicious"): seeds
  two round-number GL legs and asserts a routed+executed sweep records a `round_number`
  find. Status `known-gap` — the cross-model box RED-run is QA's step (WAVE_G_PLAN §8); the
  routing claim stays a hypothesis until the box reproduces it.
- Added an `anomaly` binding to `planning/nl_test_suite/column_map.yaml` so the scenario's
  oracle asserts a real end state (the table the sweep writes to had no logical binding).
