"""erpclaw-integrations-shopify migration 002: the payout reserve voucher columns.

Two columns on `shopify_payout` carry the reserve posting links:

  reserve_hold_voucher_id      the journal_entry id of the reserve hold posting
  reserve_release_voucher_id   the journal_entry id of the reserve release posting

**Why this file exists.** Until this release the reserve posting recorded
nothing on the payout, so every call posted another voucher. The converted
installer declares both columns as metadata, so a fresh provision creates the
table complete. That fixes fresh installs and nothing else: ``provision()``
creates missing TABLES, it does not add missing COLUMNS to a table that
already exists, so an install predating this release still needs the ALTER.
This is that path, in the place the house keeps them. An installer PROVISIONS,
a migration ALTERS; a converted installer carries no migration logic (founder
ruling, 2026-08-12).

Applied under `erpclaw-integrations-shopify:002_shopify_payout_reserve_vouchers`
in the shared `erpclaw_schema_migration` ledger by
`module_manager._run_module_migrations` -> the foundation runner. Nothing
registers it; the runner discovers `migrations/NNN_*.py`.

**Idempotent**, and by column rather than by file: each column is added only when
the catalog does not already have it, so a re-run adds nothing, a fresh install
(where the installer already declared both) is a no-op, and an interrupted run
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

money: `shopify_payout` holds gross, fee, net and reserved-funds amounts as
TEXT (Decimal strings) on every backend. Neither of the two columns is a money
column; the module's amounts are untouched here.

Usage:
    python3 002_shopify_payout_reserve_vouchers.py [--db-path PATH]
"""
import argparse
import importlib.util
import os
import sys

# M102: two new columns, no row read or written.
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

TABLE = "shopify_payout"

# (column, statement). Spelled out in full rather than assembled from the column
# name, so no name is ever formatted INTO SQL. The type is TEXT and the column is
# nullable with no default on both backends — exactly what the retired in-installer
# loop produced, and what the converted installer now declares.
ADD_COLUMNS = (
    ("reserve_hold_voucher_id",
     "ALTER TABLE shopify_payout ADD COLUMN reserve_hold_voucher_id TEXT"),
    ("reserve_release_voucher_id",
     "ALTER TABLE shopify_payout ADD COLUMN reserve_release_voucher_id TEXT"),
)


def _target(db_path):
    """The database to act on.

    On PostgreSQL the runner passes ``ERPCLAW_DB_URL`` when set, else the
    location it was given (from the module manager, the SQLite default file
    path); ``connect.py`` passes ``None`` or the action's ``--db-path``; a URL
    argument is used as given, anything else yields ``None``.
    """
    if get_dialect() == "postgresql":
        if isinstance(db_path, str) and (
                db_path.startswith("postgresql://")
                or db_path.startswith("postgres://")):
            return db_path
        return None
    return db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)


def run_migration(db_path=None):
    """Add whichever of the two reserve voucher columns this install is missing.

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
        print(f"  {TABLE}: all {len(ADD_COLUMNS)} reserve voucher columns already "
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
    print(f"  {TABLE}: {len(pending)} reserve voucher column(s) added; no row was "
          f"read or written.")
    return {"added": [column for column, _ in pending], "already_present": already}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="erpclaw-integrations-shopify migration 002: add the reserve "
                    "voucher columns to shopify_payout")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("erpclaw-integrations-shopify migration 002 complete.")
