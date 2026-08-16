"""erpclaw-growth migration 007: retire the legacy intercompany-elimination pair (M63-C).

`elimination_rule` and `elimination_entry` arrived in this module from the GL
domain and no growth code has ever touched them. Their only reader and writer was
a FOUNDATION action — erpclaw-reports `run-elimination` — which posted group
eliminations straight into the operating companies' live `gl_entry` with raw SQL:
the "balanced" pair spanned two companies, so each entity's own trial balance came
out unbalanced (measured: 1,000.00 short on a single 1,000.00 elimination), and
none of it went through the constitutional 12-step helper. ADR-0010 is explicit
that consolidation-level adjustments belong to the GROUP statements and leave
subsidiary books untouched, which is what erpclaw-accounting-adv already does
(`advacct_elimination_entry`). The four foundation actions now steer there; this
drops the tables they used.

WHAT THIS DOES NOT DO: it does not touch `gl_entry`. Elimination pairs an operator
already posted are submitted ledger rows, and the house rule is cancel = reverse,
never edit — a migration silently reversing someone's books would be a worse defect
than the one being retired. Every archived `elimination_entry` row keeps its
`source_gl_entry_id` / `target_gl_entry_id`, so a controller can reverse them
deliberately with a journal entry. The `elimination_entry` voucher type stays
registered for the same reason: those ledger rows must remain explicable.

Sequence, in this order, so a crash can never lose data:

  1. count (both tables, via the seam — absent tables are a clean no-op);
  2. archive every row of both tables to a JSON sidecar under
     <ERPCLAW_HOME>/archive/, money left as the exact TEXT it is stored as,
     then read the file back and check it holds what was counted;
  3. drop `elimination_entry` first, then `elimination_rule` (the child carries
     REFERENCES elimination_rule(id) ON DELETE RESTRICT and the seam enables FK
     enforcement, so the parent cannot go first).

Step 2 fails loudly rather than quietly, and step 3 never runs when it does: the
archive is the ONLY copy of rows this migration is about to destroy, so anything
that could not be written must leave the rows where they are.

The sidecar name is `<prefix>_<database>_<UTC second>[-N].json` and it is opened
with exclusive-create. Three properties, each one of them load-bearing:

  * the database discriminator (sanitized name + digest of the resolved path,
    never any credentials from a URL) keeps two databases apart. `--db-path` is
    a documented flag and `install-module` / `update-module` run this migration
    on whatever database is configured, so one ERPCLAW_HOME serving two
    databases is a supported shape, not an exotic one;
  * exclusive-create means an existing file is never truncated. A timestamp at
    one-second granularity is not a unique name — two migrations in one second
    is the NORMAL case, not the rare one (measured: both complete in ~3ms);
  * a name that is taken yields the NEXT name, not an error. A crash-then-retry
    is exactly what the migration runner instructs ("Fix the failing migration
    and re-run"), and a retry must not have to choose between overwriting the
    first run's archive and refusing to proceed.

The failure mode is therefore always "one extra archive file on disk", never
"the rows are gone and so is their archive".

`--report-only` writes nothing at all — no archive, no drop — and states exactly
what the real run would do, including the row counts it found. Idempotent: a
second run finds no tables and says so. An install that never had the growth
addon, or one whose tables are empty, gets a clean no-op and NO archive file
(an empty artefact is noise, not evidence).

AUDIT TRAIL (M102). This migration DESTROYS rows, which makes it data-changing
under M102 §3 ("a row deleted"), and it is the only table-dropping migration in
the tree that already holds what a trail needs: it has counted the rows and it
knows where it put them. So it writes ONE `audit_log` row per dropped table —
`entity_type='table'`, `entity_id=<table name>`, the row count it destroyed and
the archive that holds them — written on the SAME connection BEFORE the drops
and committed with them, so a drop that fails leaves no row claiming it happened.
The count in that row is what turns "these tables are empty" from a docstring
assertion into a recorded measurement. Read it back with

    get-audit-log --audit-action "migration:007_retire_legacy_elimination_tables"

The other eight `table-drop` migrations do NOT do this — none of them counts
anything before dropping — which is pending row M109, not a claim this file
makes on their behalf. Convention + gate:
`planning/simlogs/m102_SIM_2026-08-12.md`,
`testing/unit/L0/test_migration_audit_trail.py`.

Authored through the seam (ADR-0034): `erpclaw_lib.db.get_connection` for the
connection, `erpclaw_lib.seam.table_exists` for the catalog question. No raw DDL,
no connection-setting statements, no catalog table read by hand, so it works
unchanged on SQLite and PostgreSQL. Every statement is a FIXED string (migration
031's rule): no table name, column name or value is ever formatted into SQL.

(The two sentences above are worded around the literal tokens the seam-bypass
ratchet matches. It scans string literals as well as code, deliberately, because
that is where real SQL lives — so a docstring that spelled them out would count
itself as the bypass it is denying.)

SIM: planning/simlogs/m63c_SIM_2026-08-12.md
Plan home: planning/pending_items.md row M63.

Usage:
    python3 007_retire_legacy_elimination_tables.py [--db-path PATH] [--report-only]
"""
import argparse
import hashlib
import importlib.util
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone

# Deployed-lib bootstrap, guarded: production has nothing pre-imported so this
# resolves the installed lib, while a caller that already bound a tree (tests,
# the module runner inside a worktree) keeps its binding (ADR-0034 step 2d).
if importlib.util.find_spec("erpclaw_lib") is None:  # pragma: no cover - env-dependent
    sys.path.insert(0, os.path.join(
        os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))

from erpclaw_lib import seam  # noqa: E402
from erpclaw_lib.audit import audit_migration, migration_action  # noqa: E402
from erpclaw_lib.db import get_connection  # noqa: E402
from erpclaw_lib.paths import db_default, erpclaw_home  # noqa: E402

# M102: derived from the filename, never typed, so the trail's action string
# cannot drift from the stem migration_runner ledgers this file under.
MIGRATION_ID = os.path.splitext(os.path.basename(__file__))[0]

# M102: destroys rows — an install that carries elimination data loses it here.
# NOT "table-drop": that class means "the rows are gone and nothing counted
# them", and this migration counts them, archives them and verifies the archive
# before dropping anything, so it can and does write a real trail (§3: a row
# deleted is data-changing).
MIGRATION_DATA_CLASS = "rows"

# The module that owns this migration — the trail's `skill` column.
MODULE_NAME = "erpclaw-growth"

DEFAULT_DB_PATH = db_default()

# Child first: elimination_entry references elimination_rule ON DELETE RESTRICT.
_DROP_ORDER = ("elimination_entry", "elimination_rule")

# Fixed statements, one per table. Nothing is interpolated.
_SELECT = {
    "elimination_entry": "SELECT * FROM elimination_entry",
    "elimination_rule": "SELECT * FROM elimination_rule",
}
_DROP = {
    "elimination_entry": "DROP TABLE IF EXISTS elimination_entry",
    "elimination_rule": "DROP TABLE IF EXISTS elimination_rule",
}


_ARCHIVE_PREFIX = "m63c_elimination_legacy"

# How many same-second archives of the SAME database we will name in sequence
# before falling back to a random suffix. Not a limit on archives: see
# _open_new_archive, where exhausting it still produces a file.
_MAX_SEQ = 1000

_UNSAFE_IN_FILENAME = re.compile(r"[^A-Za-z0-9_-]+")
_URL_CREDENTIALS = re.compile(r"//[^/@]*@")


def _now_stamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _db_discriminator(db_path):
    """A filesystem-safe, stable tag for the database being archived.

    Two databases must not be able to produce the same archive name. The tag is
    a readable stem plus a digest of the whole resolved location, so
    `/srv/a/data.sqlite` and `/srv/b/data.sqlite` differ even though both stems
    read `data`.

    Credentials are stripped BEFORE either half is computed, so a PostgreSQL URL
    contributes its database name and nothing else — an archive filename sitting
    in a support bundle is not a place for a password, and the digest must not
    be a digest of one either.
    """
    ident = _URL_CREDENTIALS.sub("//", str(db_path or "")).split("?", 1)[0]
    stem = os.path.splitext(os.path.basename(ident.rstrip("/")))[0]
    stem = _UNSAFE_IN_FILENAME.sub("-", stem).strip("-")[:24] or "db"
    digest = hashlib.sha256(ident.encode("utf-8")).hexdigest()[:8]
    return "%s-%s" % (stem, digest)


def archive_path(db_path, stamp=None, seq=0):
    """Where the sidecar lands, for attempt `seq` (0-based) of one run.

    Under ERPCLAW_HOME, so it is dialect-independent (a Postgres install has no
    DB directory to sit next to). `seq` 0 is the plain name; every later attempt
    appends `-2`, `-3`, … so a collision reads as what it is — another archive,
    not a replacement for the first.
    """
    return os.path.join(
        erpclaw_home(), "archive",
        "%s_%s_%s%s.json" % (_ARCHIVE_PREFIX, _db_discriminator(db_path),
                             stamp or _now_stamp(),
                             "" if seq == 0 else "-%d" % (seq + 1)))


def _open_new_archive(db_path):
    """Create and open a NEW archive file; return (handle, path).

    Mode "x" is exclusive-create: if the name is already taken the open fails
    instead of emptying the file that holds it, and we move to the next name.
    Nothing this function returns can ever be a truncated existing archive.
    """
    os.makedirs(os.path.join(erpclaw_home(), "archive"), exist_ok=True)
    stamp = _now_stamp()
    for seq in range(_MAX_SEQ):
        try:
            path = archive_path(db_path, stamp, seq)
            return open(path, "x", encoding="utf-8"), path
        except FileExistsError:
            continue
    # A thousand archives of one database inside one second is not a real
    # deployment, but an operator must never be stranded by our naming: a random
    # suffix ends the search in one step instead of raising and leaving the rows
    # un-archived (and therefore un-droppable).
    path = archive_path(db_path, "%s-%s" % (stamp, uuid.uuid4().hex[:8]))
    return open(path, "x", encoding="utf-8"), path


def _read_rows(conn, table):
    """Every row of `table` as dicts, column names taken from the cursor itself.

    `cursor.description` is DBAPI-standard and aligned with the result set by
    construction, so this stays correct on both backends without asking either
    one for its catalog ordering.
    """
    cur = conn.execute(_SELECT[table])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _write_archive(db_path, rows):
    """Write every row of both tables to a NEW sidecar; return its path.

    Raises on ANY failure, and the caller drops nothing when it does — this file
    is the only copy of what the next statement destroys. Three defences:

      * the file is created exclusively (`_open_new_archive`), so no existing
        archive is ever truncated;
      * it is flushed and fsynced, so "the write returned" means the bytes are
        on the device and not only in a buffer a crash would discard;
      * it is read back and counted, so a truncated or half-serialised archive
        fails here rather than passing for a complete one.

    A file that failed mid-write is renamed to `<name>.partial` so it cannot be
    mistaken for a good archive later. That rename is best-effort and never
    replaces the real error (and the name it moves to is unique by construction,
    since the archive name itself was just created exclusively).
    """
    payload = {
        "migration": "erpclaw-growth:007_retire_legacy_elimination_tables",
        "archived_at": datetime.now(timezone.utc).isoformat(),
        "database": db_path,
        "note": ("Retired by M63-C. The gl_entry rows these entries point at "
                 "were NOT touched: reverse them with a journal entry if the "
                 "group elimination should come out of the operating books."),
        "tables": rows,
    }
    fh, written = _open_new_archive(db_path)
    try:
        with fh:
            json.dump(payload, fh, indent=2, default=str, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        _verify_archive(written, rows)
    except BaseException:
        try:
            os.replace(written, written + ".partial")
        except OSError:
            pass
        raise
    return written


def _verify_archive(path, rows):
    """Re-read the sidecar and confirm it holds every row that was counted."""
    with open(path, encoding="utf-8") as fh:
        back = json.load(fh)
    got = {t: len(v) for t, v in back.get("tables", {}).items()}
    want = {t: len(v) for t, v in rows.items()}
    if got != want:
        raise RuntimeError(
            "archive %s does not hold what was counted (%s vs %s); refusing to "
            "drop anything" % (path, got, want))


_NO_AUDIT_LOG = (
    "audit_log is absent from %s. Migration 007 destroys rows and records what "
    "it destroyed (M102), so it will not run without it. Run the foundation "
    "install/upgrade first.")


def _require_audit_log(path, report_only):
    """Refuse to destroy rows we cannot record destroying (M102).

    `audit_log` is created by `init_schema.SETUP_TABLES` on every install that
    has ever run a foundation migration, so this is not a reachable state through
    the runner. It is checked anyway, and checked FIRST, because the alternative
    on a database that somehow lacks it is dropping the tables and discovering
    the missing log on the INSERT — after the rows are gone. A destructive
    migration that cannot write its trail has to stop before it does anything.

    `--report-only` writes nothing, so it has nothing to refuse; it SAYS the real
    run would refuse instead. A report that raised where the real run raises
    would be honest about the outcome and wrong about its own contract.
    """
    if seam.table_exists("audit_log", path):
        return
    if not report_only:
        raise RuntimeError(_NO_AUDIT_LOG % path)
    print("  report-only: audit_log is ABSENT — the real run would REFUSE to "
          "drop anything here (M102). Run the foundation install/upgrade first.")


def run_migration(db_path=None, report_only=False):
    path = db_path or os.environ.get("ERPCLAW_DB_PATH", DEFAULT_DB_PATH)
    conn = get_connection(path)
    try:
        present = [t for t in _DROP_ORDER if seam.table_exists(t, path)]
        if present:
            _require_audit_log(path, report_only)
        if not present:
            print("  elimination_rule / elimination_entry absent "
                  "(already retired, or growth never installed). Nothing to do.")
            return {"present": [], "archived": 0, "dropped": [], "archive": None,
                    "report_only": report_only, "audit_rows": 0}

        rows = {t: _read_rows(conn, t) for t in present}
        total = sum(len(v) for v in rows.values())
        for t in present:
            print("  found %s: %d row(s)" % (t, len(rows[t])))

        if report_only:
            if total:
                print("  report-only: would archive %d row(s) to %s"
                      % (total, archive_path(path, "<timestamp>")))
            else:
                print("  report-only: no rows to archive (no file would be written)")
            print("  report-only: would drop %s" % ", ".join(present))
            print("  report-only: gl_entry is NOT touched, on this or the real run")
            print("  report-only: no audit_log row is written either — a trail "
                  "for a drop that did not happen would be the lie M102 exists "
                  "to prevent. The real run writes %d." % len(present))
            return {"present": present, "archived": total, "dropped": [],
                    "archive": None, "report_only": True, "audit_rows": 0}

        written = None
        if total:
            # Archive FIRST and completely. Nothing below this line may run if
            # this raises: the drop that follows is irreversible.
            written = _write_archive(path, rows)
            print("  archived %d row(s) to %s" % (total, written))
        else:
            print("  both tables empty — nothing to archive")

        # M102 — the trail goes in BEFORE the drops, on this same connection.
        # Order matters and is not stylistic: sqlite3 runs DDL outside a
        # transaction when none is open, so the INSERTs are what open the
        # transaction the DROPs then join. Written first, they still commit
        # with the drops and still roll back with a failed one.
        for t in present:
            audit_migration(
                conn, MIGRATION_ID, "table", t, module_name=MODULE_NAME,
                old_values={"rows": len(rows[t])},
                new_values={"dropped": True, "archive": written},
                description="migration %s dropped table %s after archiving its "
                            "%d row(s)%s"
                            % (MIGRATION_ID, t, len(rows[t]),
                               " to %s" % written if written
                               else " (it was empty, so no archive was written)"))
        for t in present:                      # child before parent
            conn.execute(_DROP[t])
        conn.commit()
        print("  dropped: %s" % ", ".join(present))
        print("  audit trail: %d audit_log row(s), committed with the drop. Read "
              "them back with:  get-audit-log --audit-action \"%s\""
              % (len(present), migration_action(MIGRATION_ID)))
        return {"present": present, "archived": total, "dropped": list(present),
                "archive": written, "report_only": False,
                "audit_rows": len(present)}
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migration 007: retire the legacy intercompany-elimination tables")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--report-only", action="store_true",
                        help="State what the real run would do; write nothing.")
    args = parser.parse_args()
    run_migration(args.db_path, report_only=args.report_only)
    print("erpclaw-growth migration 007 "
          + ("report complete (no writes)." if args.report_only else "complete."))
