"""erpclaw-integrations-shopify migration 001: the v1.1 App Store pairing columns.

Six columns on `shopify_account` carry the v1.1 OAuth pairing flow:

  pairing_method         'oauth' | 'custom_app'
  hmac_secret_enc        per-shop HMAC secret, encrypted at rest
  last_status_push_at    ISO-8601; when the daemon last pushed status
  disconnect_state       nullable; set to 'pending' during disconnect
  status_mode            'active' | 'scheduled' | 'on-demand'
  erpclaw_url_override   user-provided public URL for the embedded UI deep-link

**Why this file exists.** Until ADR-0034 phase 2 they arrived through an
``ALTER TABLE ADD COLUMN`` loop that `init_db.py` ran after its own table
declarations — the one module of the 40 whose installer carried migration logic
(step 2f drift audit, 2026-08-12). The converted installer declares all six as
metadata, so a fresh provision creates the table complete. That fixes fresh
installs and nothing else: ``provision()`` creates missing TABLES, it does not
add missing COLUMNS to a table that already exists, so an install predating the
v1.1 release still needs the ALTER. This is that path, in the place the house
keeps them. An installer PROVISIONS, a migration ALTERS; a converted installer
carries no migration logic (founder ruling, 2026-08-12).

Applied under `erpclaw-integrations-shopify:001_shopify_account_v11_pairing_columns`
in the shared `erpclaw_schema_migration` ledger by
`module_manager._run_module_migrations` -> the foundation runner. Nothing
registers it; the runner discovers `migrations/NNN_*.py`.

**Idempotent**, and by column rather than by file: each column is added only when
the catalog does not already have it, so a re-run adds nothing, a fresh install
(where the installer already declared all six) is a no-op, and an interrupted run
finishes on the next one. That last case is real on SQLite, where a DDL statement
issued outside a transaction self-commits, so a crash halfway through leaves the
columns added so far — all of them inert, nullable and unread until something
writes one.

**Dialect-aware without a dialect branch.** ``ALTER TABLE <t> ADD COLUMN <c> TEXT``
is the same statement on SQLite and PostgreSQL, so there is nothing to branch on;
what differs between the backends is how you ASK whether a column is already
there, and that question goes to ``erpclaw_lib.seam.column_names``, which answers
on both (ADR-0034). ``ADD COLUMN IF NOT EXISTS`` would have removed the need for
the question on PostgreSQL alone and is not a statement SQLite has, which is how
a migration ends up with two spellings of one idea.

Every statement is a FIXED string literal — no table name, column name or value
is ever formatted into SQL (migration 031's rule), so the Article-10 static
scanner has nothing to read as an injection site.

money: `shopify_account` holds GL account mappings and sync bookkeeping, no
amounts. None of the six columns is a money column; the module's amounts live in
`shopify_order`, `shopify_refund`, `shopify_payout` and `shopify_dispute` and are
untouched here.

Usage:
    python3 001_shopify_account_v11_pairing_columns.py [--db-path PATH]
"""
import argparse
import importlib.util
import os
import sys

# M102: adds six columns and nothing else. No row is read, rewritten, inserted or
# deleted; before this run every one of these columns held nothing on every
# install. That is the "a new column" case of the definition, not a rewrite.
MIGRATION_DATA_CLASS = "none"

# Deployed-lib bootstrap, guarded: production has nothing pre-imported so this
# resolves the installed lib, while a caller that already bound a tree (tests,
# the module runner inside a worktree) keeps its binding (ADR-0034 step 2d).
if importlib.util.find_spec("erpclaw_lib") is None:  # pragma: no cover - env-dependent
    sys.path.insert(0, os.path.join(
        os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))

from erpclaw_lib import seam  # noqa: E402
from erpclaw_lib.db import get_connection, get_dialect  # noqa: E402
from erpclaw_lib.paths import db_default  # noqa: E402

DEFAULT_DB_PATH = db_default()

TABLE = "shopify_account"

# (column, statement). Spelled out in full rather than assembled from the column
# name, so no name is ever formatted INTO SQL. The type is TEXT and the column is
# nullable with no default on both backends — exactly what the retired in-installer
# loop produced, and what the converted installer now declares.
ADD_COLUMNS = (
    ("pairing_method",
     "ALTER TABLE shopify_account ADD COLUMN pairing_method TEXT"),
    ("hmac_secret_enc",
     "ALTER TABLE shopify_account ADD COLUMN hmac_secret_enc TEXT"),
    ("last_status_push_at",
     "ALTER TABLE shopify_account ADD COLUMN last_status_push_at TEXT"),
    ("disconnect_state",
     "ALTER TABLE shopify_account ADD COLUMN disconnect_state TEXT"),
    ("status_mode",
     "ALTER TABLE shopify_account ADD COLUMN status_mode TEXT"),
    ("erpclaw_url_override",
     "ALTER TABLE shopify_account ADD COLUMN erpclaw_url_override TEXT"),
)


def _target(db_path):
    """The database to act on, resolved once and by the seam's own env chain.

    On PostgreSQL the runner hands `run_migration` the SQLite default path and
    reads the real target from ``ERPCLAW_DB_URL`` itself, so using the argument
    verbatim would point the seam at a filesystem path and call it a connection
    URL. ``None`` makes the seam resolve it through the same chain the DML path
    uses, which is the whole reason that chain lives in one place.
    """
    if get_dialect() == "postgresql":
        return None
    return db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)


def run_migration(db_path=None):
    """Add whichever of the six v1.1 columns this install is missing.

    Returns ``{"added": [...], "already_present": [...]}`` so a caller can tell a
    real upgrade from a no-op. The runner discards it; the module's lazy-upgrade
    path in `scripts/connect.py` does not.
    """
    target = _target(db_path)

    if not seam.table_exists(TABLE, target):
        print(f"  {TABLE} absent on this install (shopify not installed). "
              f"Nothing to migrate.")
        return {"added": [], "already_present": [], "reason": "table absent"}

    existing = set(seam.column_names(TABLE, target))
    pending = [(column, statement) for column, statement in ADD_COLUMNS
               if column not in existing]
    already = [column for column, _ in ADD_COLUMNS if column in existing]

    if not pending:
        print(f"  {TABLE}: all {len(ADD_COLUMNS)} v1.1 pairing columns already "
              f"present (idempotent no-op).")
        return {"added": [], "already_present": already}

    conn = get_connection(target)
    try:
        for column, statement in pending:
            conn.execute(statement)
            print(f"  {TABLE}.{column}: added.")
        conn.commit()
    finally:
        conn.close()

    if already:
        print(f"  {TABLE}: {len(already)} column(s) were already present "
              f"({', '.join(already)}).")
    print(f"  {TABLE}: {len(pending)} v1.1 pairing column(s) added; no row was "
          f"read or written.")
    return {"added": [column for column, _ in pending], "already_present": already}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="erpclaw-integrations-shopify migration 001: add the v1.1 "
                    "App Store pairing columns to shopify_account")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("erpclaw-integrations-shopify migration 001 complete.")
