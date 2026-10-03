"""erpclaw-pos migration 001: the return-link columns.

Two nullable TEXT pointers carry a return document back to the sale it
corrects:

  pos_transaction.return_against_id        the original sale's id
  pos_transaction_item.return_against_item_id  the original line's id

plus the ``idx_pos_txn_return_against`` index on the first, which is how the
void pre-check finds whether a sale already has returns. Both are bare TEXT
pointers with no foreign key and no default — exactly what the converted
installer now declares as each table's last column. An install predating this
release acquires them here; a fresh install already has them.

**Why this file exists.** Until this release the link from a return to its
sale existed only as text in ``pos_payment.reference`` (``Return of {id}``);
there was no column. Fresh provisions declare the columns through the
installer's metadata, so a fresh provision creates the tables complete. That
fixes fresh installs and nothing else: ``provision()`` creates missing TABLES,
it does not add missing COLUMNS to a table that already exists, so an install
predating this release still needs the ALTER. This is that path, in the place
the house keeps them. An installer PROVISIONS, a migration ALTERS; a converted
installer carries no migration logic (founder ruling, 2026-08-12).

Applied under `erpclaw-pos:001_pos_return_links` in the shared
`erpclaw_schema_migration` ledger by `module_manager._run_module_migrations`
-> the foundation runner. Nothing registers it; the runner discovers
`migrations/NNN_*.py`.

**Idempotent**, and by column rather than by file: each column is added only
when the catalog does not already have it, and the index only when it is not
already there, so a re-run adds nothing, a fresh install (where the installer
already declared both columns and the index) is a no-op, and an interrupted
run finishes on the next one. That last case is real on SQLite, where a DDL
statement issued outside a transaction self-commits, so a crash halfway
through leaves the columns added so far — all of them inert, nullable and
unread until something writes one.

**Dialect-aware without a dialect branch.** ``ALTER TABLE <t> ADD COLUMN <c>
TEXT`` is the same statement on SQLite and PostgreSQL, so there is nothing to
branch on; what differs between the backends is how you ASK whether a column
or index is already there, and those questions go to
``erpclaw_lib.seam.column_names`` and ``erpclaw_lib.seam.index_names``, which
answer on both (ADR-0034). ``ADD COLUMN IF NOT EXISTS`` would have removed the
need for the question on PostgreSQL alone and is not a statement SQLite has,
which is how a migration ends up with two spellings of one idea.

Every statement is a FIXED string literal — no table name, column name or value
is ever formatted into SQL (migration 031's rule), so the Article-10 static
scanner has nothing to read as an injection site.

money: `pos_transaction` holds TEXT money columns and `pos_transaction_item`
holds TEXT amount columns; neither of the two new columns is a money column
and the module's amounts are untouched here.

Usage:
    python3 001_pos_return_links.py [--db-path PATH]
"""
import argparse
import importlib.util
import os
import sys

# M102: adds two columns and one index, nothing else. No row is read,
# rewritten, inserted or deleted; before this run each of these columns held
# nothing on every install. That is the "a new column" case of the definition,
# not a rewrite.
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

TABLE = "pos_transaction"
ITEM_TABLE = "pos_transaction_item"
INDEX_NAME = "idx_pos_txn_return_against"

# (table, column, statement). Spelled out in full rather than assembled from
# the column name, so no name is ever formatted INTO SQL. The type is TEXT and
# the column is nullable with no default on both backends — exactly what the
# converted installer now declares as each table's last column.
ADD_COLUMNS = (
    ("pos_transaction",
     "return_against_id",
     "ALTER TABLE pos_transaction ADD COLUMN return_against_id TEXT"),
    ("pos_transaction_item",
     "return_against_item_id",
     "ALTER TABLE pos_transaction_item ADD COLUMN return_against_item_id TEXT"),
)

ADD_INDEX = ("CREATE INDEX IF NOT EXISTS idx_pos_txn_return_against "
             "ON pos_transaction (return_against_id)")


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
    """Add whichever of the two return-link columns this install is missing.

    Returns ``{"added": [...], "already_present": [...]}`` so a caller can tell a
    real upgrade from a no-op. The runner discards it; the module's lazy-upgrade
    path in `scripts/connect.py` does not.
    """
    target = _target(db_path)

    if not seam.table_exists(TABLE, target):
        print(f"  {TABLE} absent on this install (POS not installed). "
              f"Nothing to migrate.")
        return {"added": [], "already_present": [], "reason": "table absent"}

    if not seam.table_exists(ITEM_TABLE, target):
        print(f"  {ITEM_TABLE} absent on this install (POS not installed). "
              f"Nothing to migrate.")
        return {"added": [], "already_present": [], "reason": "table absent"}

    existing = set(seam.column_names(TABLE, target)) | set(
        seam.column_names(ITEM_TABLE, target))
    pending = [(table, column, statement)
               for table, column, statement in ADD_COLUMNS
               if column not in existing]
    already = [column for _, column, _ in ADD_COLUMNS if column in existing]

    indexes = set(seam.index_names(TABLE, target))
    index_pending = INDEX_NAME not in indexes
    if not index_pending:
        already.append(INDEX_NAME)

    if not pending and not index_pending:
        print(f"  {TABLE}: both return-link columns and {INDEX_NAME} already "
              f"present (idempotent no-op).")
        return {"added": [], "already_present": already}

    conn = get_connection(target)
    try:
        for table, column, statement in pending:
            conn.execute(statement)
            print(f"  {table}.{column}: added.")
        if index_pending:
            conn.execute(ADD_INDEX)
            print(f"  {TABLE}.{INDEX_NAME}: added.")
        conn.commit()
    finally:
        conn.close()

    added = [column for _, column, _ in pending]
    if index_pending:
        added.append(INDEX_NAME)
    if already:
        print(f"  {TABLE}: {len(already)} name(s) were already present "
              f"({', '.join(already)}).")
    print(f"  {TABLE}: {len(added)} return-link name(s) added; no row was "
          f"read or written.")
    return {"added": added, "already_present": already}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="erpclaw-pos migration 001: add the return-link columns "
                    "to pos_transaction and pos_transaction_item")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_migration(args.db_path)
    print("erpclaw-pos migration 001 complete.")
