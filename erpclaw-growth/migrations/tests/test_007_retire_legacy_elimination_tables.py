"""Part A — erpclaw-growth migration 007 (M63-C): archive-then-drop, report-only first.

The migration drops two tables that a real deployment may hold rows in, so the
properties that matter are the ones a careless drop gets wrong:

  * `--report-only` writes NOTHING — not the archive, not the drop;
  * every row of both tables reaches the archive with every column and the money
    string byte-identical (money is TEXT and stays TEXT);
  * `gl_entry` is not touched, including the rows the legacy engine posted —
    submitted ledger entries are immutable, and reversing them is a controller's
    decision, not a migration's;
  * a second run is a clean no-op that writes no second archive;
  * an install that never had the tables, and one whose tables are empty, both
    end without an archive file (an empty artefact is noise, not evidence);
  * **no archive can ever be overwritten** — not by a second database migrated
    from the same ERPCLAW_HOME in the same second, not by the retry the runner
    itself instructs after a crash (QA round 1, B1);
  * **the drop never runs unless the archive is on disk** — the ordering is a
    property, not an accident of statement order (QA round 1, B2: this was the
    one mutation of eight that survived);
  * **what it destroyed is recorded** — one `audit_log` row per dropped table
    carrying the row count and the archive path, committed with the drop and
    absent when the drop does not happen (M102).

Every fixture table is declared as SQLAlchemy metadata and provisioned through
the seam (ADR-0034) — a test is not a licence to hand-write DDL for a table the
tree just retired.
"""
import importlib.util
import json
import os
import re
import sys

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_MIGRATIONS_DIR = os.path.dirname(_TESTS_DIR)
_MODULE_DIR = os.path.dirname(_MIGRATIONS_DIR)                       # erpclaw-growth/
_REPO = os.path.abspath(os.path.join(_MODULE_DIR, "..", "..", ".."))  # repo root
_LIB = os.path.join(_REPO, "source", "erpclaw", "scripts", "erpclaw-setup", "lib")

if importlib.util.find_spec("erpclaw_lib") is None:
    sys.path.insert(0, _LIB)

from erpclaw_lib import seam  # noqa: E402
from erpclaw_lib.db import get_connection  # noqa: E402

MONEY = "1234.56"          # exact TEXT, never a float
RULE_ID = "rule-0001"
ENTRY_ID = "entry-0001"
SRC_GL = "gl-src-0001"
TGT_GL = "gl-tgt-0001"


def _load_migration():
    path = os.path.join(_MIGRATIONS_DIR, "007_retire_legacy_elimination_tables.py")
    spec = importlib.util.spec_from_file_location("growth_migration_007", path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


mig = _load_migration()


def _audit_log_table(sa, md):
    """The foundation's `audit_log`, as `init_schema.SETUP_TABLES` declares it.

    Present in every fixture because it is present on every real install (the
    growth addon cannot exist without the foundation), and because M102 makes the
    migration refuse to destroy rows it cannot record destroying — a fixture
    without it would be testing a database that cannot happen.
    """
    return sa.Table(
        "audit_log", md,
        sa.Column("id", sa.Text, primary_key=True),
        sa.Column("timestamp", sa.Text, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("user_id", sa.Text),
        sa.Column("skill", sa.Text),
        sa.Column("action", sa.Text),
        sa.Column("entity_type", sa.Text),
        sa.Column("entity_id", sa.Text),
        sa.Column("old_values", sa.Text),
        sa.Column("new_values", sa.Text),
        sa.Column("description", sa.Text),
    )


def _trail(path):
    """Every audit_log row migration 007 wrote, oldest first, JSON parsed."""
    conn = get_connection(path)
    try:
        cur = conn.execute(
            "SELECT skill, entity_type, entity_id, old_values, new_values, "
            "description FROM audit_log WHERE action = ? ORDER BY timestamp, id",
            ("migration:" + mig.MIGRATION_ID,))
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    seam.dispose_engines()
    for row in rows:
        for key in ("old_values", "new_values"):
            row[key] = json.loads(row[key]) if row[key] else None
    return rows


def _legacy_metadata(with_ledger=True, with_audit_log=True):
    """The retired pair as growth's init_db declared it, plus a minimal gl_entry
    stand-in so "the ledger is untouched" can be DRIVEN rather than asserted."""
    sa = seam._sqlalchemy()
    md = sa.MetaData()
    if with_audit_log:
        _audit_log_table(sa, md)
    sa.Table(
        "elimination_rule", md,
        sa.Column("id", sa.Text, primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("source_company_id", sa.Text, nullable=False),
        sa.Column("target_company_id", sa.Text, nullable=False),
        sa.Column("source_account_id", sa.Text, nullable=False),
        sa.Column("target_account_id", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False, server_default="active"),
        sa.Column("created_at", sa.Text),
        sa.Column("updated_at", sa.Text),
    )
    sa.Table(
        "elimination_entry", md,
        sa.Column("id", sa.Text, primary_key=True),
        sa.Column("elimination_rule_id", sa.Text,
                  sa.ForeignKey("elimination_rule.id", ondelete="RESTRICT"),
                  nullable=False),
        sa.Column("fiscal_year_id", sa.Text),
        sa.Column("posting_date", sa.Text, nullable=False),
        sa.Column("amount", sa.Text, nullable=False, server_default="0"),
        sa.Column("source_gl_entry_id", sa.Text),
        sa.Column("target_gl_entry_id", sa.Text),
        sa.Column("status", sa.Text, nullable=False, server_default="posted"),
        sa.Column("created_at", sa.Text),
    )
    if with_ledger:
        sa.Table(
            "gl_entry", md,
            sa.Column("id", sa.Text, primary_key=True),
            sa.Column("voucher_type", sa.Text),
            sa.Column("debit", sa.Text),
            sa.Column("credit", sa.Text),
        )
    return md


@pytest.fixture
def home(tmp_path, monkeypatch):
    """Isolate ERPCLAW_HOME so the archive can never land in a live install."""
    monkeypatch.setenv("ERPCLAW_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("ERPCLAW_DB_PATH", raising=False)
    monkeypatch.delenv("ERPCLAW_DB_DIALECT", raising=False)
    return str(tmp_path / "home")


@pytest.fixture
def empty_db(tmp_path, home):
    """A DB with the ledger stand-in but WITHOUT the retired pair."""
    path = str(tmp_path / "empty.sqlite")
    sa = seam._sqlalchemy()
    md = sa.MetaData()
    _audit_log_table(sa, md)
    sa.Table("gl_entry", md,
             sa.Column("id", sa.Text, primary_key=True),
             sa.Column("voucher_type", sa.Text))
    seam.provision(md, path)
    seam.dispose_engines()
    return path


@pytest.fixture
def seeded_db(tmp_path, home):
    """A pre-migration install: both tables present, one rule + one entry, and two
    ledger rows carrying the voucher type the legacy engine posted."""
    path = str(tmp_path / "seeded.sqlite")
    seam.provision(_legacy_metadata(), path)
    conn = get_connection(path)
    conn.execute(
        "INSERT INTO elimination_rule (id, name, source_company_id, target_company_id, "
        "source_account_id, target_account_id, status, created_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (RULE_ID, "IC sales elimination", "co-a", "co-b", "acct-inc", "acct-exp",
         "active", "2026-06-01T00:00:00Z", "2026-06-01T00:00:00Z"))
    conn.execute(
        "INSERT INTO elimination_entry (id, elimination_rule_id, fiscal_year_id, "
        "posting_date, amount, source_gl_entry_id, target_gl_entry_id, status, created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (ENTRY_ID, RULE_ID, "fy-2026", "2026-12-31", MONEY, SRC_GL, TGT_GL,
         "posted", "2026-12-31T00:00:00Z"))
    for gl_id, dr, cr in ((SRC_GL, MONEY, "0"), (TGT_GL, "0", MONEY)):
        conn.execute(
            "INSERT INTO gl_entry (id, voucher_type, debit, credit) VALUES (?,?,?,?)",
            (gl_id, "elimination_entry", dr, cr))
    conn.commit()
    conn.close()
    seam.dispose_engines()
    return path


@pytest.fixture
def empty_tables_db(tmp_path, home):
    """Both tables present and EMPTY (an install that never ran an elimination)."""
    path = str(tmp_path / "empty_tables.sqlite")
    seam.provision(_legacy_metadata(), path)
    seam.dispose_engines()
    return path


def _archives(home_dir):
    d = os.path.join(home_dir, "archive")
    return sorted(f for f in os.listdir(d) if f.endswith(".json")) \
        if os.path.isdir(d) else []


def _all_archived_ids(home_dir):
    """Every row id recoverable from EVERY archive file, as {table: {ids}}.

    Spread across files on purpose: after a crash-and-retry the rows live in two
    sidecars, and "can the operator get their rows back" is a question about the
    archive directory, not about any single file in it.
    """
    found = {}
    for fname in _archives(home_dir):
        payload = json.load(open(os.path.join(home_dir, "archive", fname)))
        for table, rows in payload["tables"].items():
            found.setdefault(table, set()).update(r["id"] for r in rows)
    return found


def _seed_db(path, tag, n_rules, n_entries_per_rule):
    """A pre-migration install with tag-prefixed ids, so rows from two different
    databases are told apart by inspection rather than by counting."""
    seam.provision(_legacy_metadata(with_ledger=False), path)
    conn = get_connection(path)
    ids = {"elimination_rule": set(), "elimination_entry": set()}
    for r in range(n_rules):
        rid = "%s-rule-%d" % (tag, r)
        ids["elimination_rule"].add(rid)
        conn.execute(
            "INSERT INTO elimination_rule (id, name, source_company_id, "
            "target_company_id, source_account_id, target_account_id, status, "
            "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (rid, "%s rule %d" % (tag, r), "co-a", "co-b", "acct-inc", "acct-exp",
             "active", "2026-06-01T00:00:00Z", "2026-06-01T00:00:00Z"))
        for e in range(n_entries_per_rule):
            eid = "%s-entry-%d-%d" % (tag, r, e)
            ids["elimination_entry"].add(eid)
            conn.execute(
                "INSERT INTO elimination_entry (id, elimination_rule_id, "
                "fiscal_year_id, posting_date, amount, source_gl_entry_id, "
                "target_gl_entry_id, status, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (eid, rid, "fy-2026", "2026-12-31", MONEY, SRC_GL, TGT_GL,
                 "posted", "2026-12-31T00:00:00Z"))
    conn.commit()
    conn.close()
    seam.dispose_engines()
    return ids


def _table_rows(path, table):
    conn = get_connection(path)
    try:
        cur = conn.execute("SELECT * FROM " + table)          # fixed names only
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# report-only
# ──────────────────────────────────────────────────────────────────────────────

def test_report_only_writes_nothing(seeded_db, home):
    res = mig.run_migration(seeded_db, report_only=True)

    assert res["report_only"] is True
    assert res["archived"] == 2, "it must still COUNT what a real run would archive"
    assert res["dropped"] == []
    assert res["archive"] is None
    assert _archives(home) == [], "report-only wrote an archive file"
    seam.dispose_engines()
    assert seam.table_exists("elimination_rule", seeded_db)
    assert seam.table_exists("elimination_entry", seeded_db)
    assert len(_table_rows(seeded_db, "elimination_entry")) == 1


# ──────────────────────────────────────────────────────────────────────────────
# archive-then-drop
# ──────────────────────────────────────────────────────────────────────────────

def test_real_run_archives_then_drops(seeded_db, home):
    res = mig.run_migration(seeded_db)

    assert res["dropped"] == ["elimination_entry", "elimination_rule"], (
        "child before parent — the FK is ON DELETE RESTRICT")
    assert res["archived"] == 2
    seam.dispose_engines()
    assert not seam.table_exists("elimination_entry", seeded_db)
    assert not seam.table_exists("elimination_rule", seeded_db)

    assert os.path.isfile(res["archive"])
    assert _archives(home) == [os.path.basename(res["archive"])]


def test_archive_keeps_every_column_and_the_exact_money_string(seeded_db):
    res = mig.run_migration(seeded_db)
    payload = json.load(open(res["archive"]))

    entry = payload["tables"]["elimination_entry"][0]
    rule = payload["tables"]["elimination_rule"][0]

    # completeness: every column of the shipped shape, not a hand-picked subset
    assert set(entry) == {"id", "elimination_rule_id", "fiscal_year_id", "posting_date",
                          "amount", "source_gl_entry_id", "target_gl_entry_id",
                          "status", "created_at"}
    assert set(rule) == {"id", "name", "source_company_id", "target_company_id",
                         "source_account_id", "target_account_id", "status",
                         "created_at", "updated_at"}
    # money survives as the exact TEXT it was stored as
    assert entry["amount"] == MONEY
    assert isinstance(entry["amount"], str)
    # the ledger link survives, which is what makes a deliberate reversal possible
    assert entry["source_gl_entry_id"] == SRC_GL
    assert entry["target_gl_entry_id"] == TGT_GL
    assert "gl_entry" in payload["note"]


def test_gl_entry_is_never_touched(seeded_db):
    """Immutable GL: the pairs the legacy engine posted stay exactly where they are."""
    before = _table_rows(seeded_db, "gl_entry")
    mig.run_migration(seeded_db)
    seam.dispose_engines()
    after = _table_rows(seeded_db, "gl_entry")

    assert after == before
    assert len(after) == 2
    assert {r["voucher_type"] for r in after} == {"elimination_entry"}


def test_migration_source_contains_no_ledger_write():
    """Static companion to the driven check: this file must never grow one."""
    src = open(os.path.join(_MIGRATIONS_DIR,
                            "007_retire_legacy_elimination_tables.py")).read()
    for forbidden in ("INSERT INTO gl_entry", "UPDATE gl_entry", "DELETE FROM gl_entry",
                      "DROP TABLE IF EXISTS gl_entry"):
        assert forbidden not in src, f"migration 007 touches the ledger: {forbidden}"


# ──────────────────────────────────────────────────────────────────────────────
# idempotence + the empty cases
# ──────────────────────────────────────────────────────────────────────────────

def test_second_run_is_a_clean_no_op(seeded_db, home):
    first = mig.run_migration(seeded_db)
    seam.dispose_engines()
    second = mig.run_migration(seeded_db)

    assert second == {"present": [], "archived": 0, "dropped": [], "archive": None,
                      "report_only": False, "audit_rows": 0}
    assert _archives(home) == [os.path.basename(first["archive"])], (
        "a re-run wrote a second archive of nothing")


def test_absent_tables_are_a_no_op(empty_db, home):
    res = mig.run_migration(empty_db)
    assert res["present"] == [] and res["dropped"] == []
    assert _archives(home) == []
    seam.dispose_engines()
    assert seam.table_exists("gl_entry", empty_db), "an unrelated table was dropped"


def test_empty_tables_drop_without_an_archive_file(empty_tables_db, home):
    res = mig.run_migration(empty_tables_db)

    assert res["archived"] == 0
    assert res["archive"] is None
    assert res["dropped"] == ["elimination_entry", "elimination_rule"]
    assert _archives(home) == [], "an empty archive file is noise, not evidence"
    seam.dispose_engines()
    assert not seam.table_exists("elimination_rule", empty_tables_db)


def test_report_only_then_real_run_agree(seeded_db):
    """The report must describe the run that follows it, or it is decoration."""
    report = mig.run_migration(seeded_db, report_only=True)
    seam.dispose_engines()
    real = mig.run_migration(seeded_db)

    assert report["present"] == real["present"]
    assert report["archived"] == real["archived"]


# ──────────────────────────────────────────────────────────────────────────────
# The archive is never overwritten (QA round 1, B1)
#
# The sidecar is the only copy of rows the next statement destroys, so the one
# thing its name must guarantee is that writing a new archive can never empty an
# old one. Both scenarios below are reachable from documented usage, and both
# were driven end-to-end before the fix: scenario 1 lost 3 of 7 rows, scenario 2
# lost 4 of 6.
#
# `_now_stamp` is frozen in these tests so "the same second" is a fact rather
# than a race. It is not an exotic assumption: a full migration of a seeded
# database measures ~3ms, so two of them landing in one second is the normal
# case, not the corner.
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def frozen_second(monkeypatch):
    monkeypatch.setattr(mig, "_now_stamp", lambda: "20260812T101112Z")


def test_two_databases_in_one_second_get_separate_archives(
        tmp_path, home, frozen_second):
    """`--db-path` is documented and install-module runs 007 on whichever
    database is configured, so one ERPCLAW_HOME can serve two of them."""
    db_a = str(tmp_path / "a" / "data.sqlite")
    db_b = str(tmp_path / "b" / "data.sqlite")
    ids_a = _seed_db(db_a, "A", n_rules=1, n_entries_per_rule=2)   # 3 rows
    ids_b = _seed_db(db_b, "B", n_rules=1, n_entries_per_rule=3)   # 4 rows

    res_a = mig.run_migration(db_a)
    seam.dispose_engines()
    res_b = mig.run_migration(db_b)
    seam.dispose_engines()

    assert res_a["archived"] == 3 and res_b["archived"] == 4
    assert res_a["archive"] != res_b["archive"], (
        "both databases archived to one filename; the first one's rows are gone")
    assert len(_archives(home)) == 2

    recoverable = _all_archived_ids(home)
    for table, expected in ids_a.items():
        assert expected <= recoverable[table], "database A's rows are unrecoverable"
    for table, expected in ids_b.items():
        assert expected <= recoverable[table], "database B's rows are unrecoverable"
    assert sum(len(v) for v in recoverable.values()) == 7


def test_a_retry_after_a_crash_archives_again_instead_of_overwriting(
        tmp_path, home, frozen_second, monkeypatch):
    """The runner's own instruction on failure is "Fix the failing migration and
    re-run". The re-run must not destroy what the first run archived."""
    db = str(tmp_path / "c" / "data.sqlite")
    ids = _seed_db(db, "C", n_rules=2, n_entries_per_rule=2)       # 6 rows

    real_get_connection = mig.get_connection

    class _CrashBetweenTheDrops:
        """Dies between the two DROPs, exactly as a killed process would."""

        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, *a, **k):
            if sql == mig._DROP["elimination_rule"]:
                raise RuntimeError("simulated crash between the two drops")
            return self._inner.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    # A scoped context, never monkeypatch.undo(): undo() would also revert the
    # `home` fixture's ERPCLAW_HOME and send the retry's archive into the real
    # install (observed while writing this test).
    with monkeypatch.context() as crash:
        crash.setattr(mig, "get_connection",
                      lambda p: _CrashBetweenTheDrops(real_get_connection(p)))
        with pytest.raises(RuntimeError):
            mig.run_migration(db)
    seam.dispose_engines()

    # BOTH tables survive, which they did not before M102. This assertion used to
    # read `not table_exists("elimination_entry")` and was right at the time:
    # sqlite3 runs DDL in autocommit when no transaction is open, so the child
    # drop was durable the moment it executed and a crash left the pair
    # half-retired. The M102 trail rows are INSERTs and they go in first, so the
    # transaction is already open when the drops run and the whole migration is
    # now all-or-nothing. Recorded here rather than quietly updated, because it is
    # a real behaviour change to a shipped migration.
    assert seam.table_exists("elimination_entry", db)
    assert seam.table_exists("elimination_rule", db)
    first = _archives(home)
    assert len(first) == 1

    retry = mig.run_migration(db)                                  # the same second
    seam.dispose_engines()

    assert retry["dropped"] == ["elimination_entry", "elimination_rule"]
    assert len(_archives(home)) == 2, (
        "the retry reused the first run's filename")
    recoverable = _all_archived_ids(home)
    for table, expected in ids.items():
        assert expected <= recoverable[table], (
            "rows archived by run 1 were destroyed by the retry")


def test_an_existing_file_is_never_truncated(seeded_db, home, frozen_second):
    """Whatever already holds the name we would have used keeps its bytes."""
    planted = mig.archive_path(seeded_db, "20260812T101112Z")
    os.makedirs(os.path.dirname(planted), exist_ok=True)
    with open(planted, "w", encoding="utf-8") as fh:
        fh.write("SOMEBODY ELSE'S EVIDENCE")

    res = mig.run_migration(seeded_db)

    assert res["archive"] != planted
    assert open(planted, encoding="utf-8").read() == "SOMEBODY ELSE'S EVIDENCE"
    assert os.path.basename(res["archive"]).endswith("-2.json"), (
        "a collision should read as another archive, not as a replacement")


def test_the_archive_name_carries_a_database_discriminator():
    """Same stamp, different databases, different names — including two
    databases whose file names are identical."""
    stamp = "20260812T101112Z"
    one = mig.archive_path("/srv/one/data.sqlite", stamp)
    two = mig.archive_path("/srv/two/data.sqlite", stamp)
    other = mig.archive_path("/srv/one/reporting.sqlite", stamp)

    assert one != two, "two databases with the same file name collide"
    assert one != other
    for path in (one, two, other):
        name = os.path.basename(path)
        assert os.sep not in name and re.fullmatch(r"[A-Za-z0-9_.-]+", name), name


def test_the_archive_name_never_carries_url_credentials():
    """A Postgres URL contributes its database name and nothing else — an
    archive filename in a support bundle is not a place for a password, and
    neither is a digest computed over one.

    The URL is assembled at runtime rather than written as a literal: this file
    ships to the public erpclaw-growth repo, and a standing credential-shaped
    string there trips our own push scanner on every push and any third-party
    secret scanner on arrival. Same reason the publish-guard tests concatenate
    their planted patterns — a fixture must not look like the thing it guards
    against. Nothing about the assertion changes.
    """
    stamp = "20260812T101112Z"
    user, secret, host = "erpuser", "s3" + "kret", "db.internal:5432"
    with_creds = mig.archive_path(
        "postgresql://%s:%s@%s/erpclaw_prod" % (user, secret, host), stamp)
    without = mig.archive_path(
        "postgresql://%s/erpclaw_prod" % host, stamp)

    assert secret not in with_creds and user not in with_creds
    assert "erpclaw_prod" in with_creds
    assert with_creds == without, (
        "the digest is being taken over the credentials as well")


# ──────────────────────────────────────────────────────────────────────────────
# The drop never runs unless the archive is on disk (QA round 1, B2)
#
# QA planted 8 mutations of this migration; 7 died against the suite above and
# one survived — moving the drop BEFORE the archive write. Driven against an
# unwritable archive directory that mutation destroys 6 rows, writes no archive,
# and leaves the suite green. The ordering was correct and unpinned; these three
# pin it, one per way the archive can fail.
# ──────────────────────────────────────────────────────────────────────────────

def _surviving(db):
    seam.dispose_engines()
    assert seam.table_exists("elimination_rule", db)
    assert seam.table_exists("elimination_entry", db)
    return (_table_rows(db, "elimination_rule"),
            _table_rows(db, "elimination_entry"))


@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0,
                    reason="root ignores directory permissions")
def test_an_unwritable_archive_directory_drops_nothing(seeded_db, home):
    """QA's driven consequence, verbatim: the archive cannot be written."""
    before = _surviving(seeded_db)
    archive_dir = os.path.join(home, "archive")
    os.makedirs(archive_dir, exist_ok=True)
    os.chmod(archive_dir, 0o500)
    try:
        with pytest.raises(OSError):
            mig.run_migration(seeded_db)
    finally:
        os.chmod(archive_dir, 0o700)

    assert _surviving(seeded_db) == before, "rows were dropped without an archive"
    assert _archives(home) == []


def test_a_failed_archive_write_drops_nothing(seeded_db, home, monkeypatch):
    """The file is created, then the write dies (full disk, killed process)."""
    before = _surviving(seeded_db)

    def _boom(*a, **k):
        raise OSError("No space left on device")

    with monkeypatch.context() as mp:
        mp.setattr(mig.json, "dump", _boom)
        with pytest.raises(OSError):
            mig.run_migration(seeded_db)

    assert _surviving(seeded_db) == before, "rows were dropped without an archive"
    assert _archives(home) == [], "a half-written file is passing as an archive"
    partials = [f for f in os.listdir(os.path.join(home, "archive"))
                if f.endswith(".partial")]
    assert len(partials) == 1, (
        "the failed attempt must be marked, not left looking complete")


def test_an_incomplete_archive_is_caught_before_the_drop(seeded_db, home, monkeypatch):
    """The write "succeeds" but one table never reached the file. Read-back is
    what makes archive-then-drop mean archived-then-drop."""
    before = _surviving(seeded_db)
    real_dump = mig.json.dump

    def _drop_a_table(payload, fh, **k):
        payload = dict(payload)
        payload["tables"] = {"elimination_rule": payload["tables"]["elimination_rule"]}
        return real_dump(payload, fh, **k)

    with monkeypatch.context() as mp:
        mp.setattr(mig.json, "dump", _drop_a_table)
        with pytest.raises(RuntimeError, match="does not hold what was counted"):
            mig.run_migration(seeded_db)

    assert _surviving(seeded_db) == before
    assert _archives(home) == []


# ──────────────────────────────────────────────────────────────────────────────
# What it destroyed is recorded (M102)
#
# This is the one table-dropping migration in the tree that can write a real
# trail, because it is the only one that COUNTS before it drops and knows where
# it put the rows. The other eight assert emptiness and never check (row M109),
# which is why they stay `MIGRATION_DATA_CLASS = "table-drop"` and this one does
# not. SIM: planning/simlogs/m102_SIM_2026-08-12.md.
# ──────────────────────────────────────────────────────────────────────────────

def test_the_trail_records_each_dropped_table_with_its_row_count(seeded_db, home):
    """The counts in the trail are compared against the rows that were really
    there, read before the run — not against what the migration reported."""
    before = {t: len(_table_rows(seeded_db, t))
              for t in ("elimination_rule", "elimination_entry")}
    assert before == {"elimination_rule": 1, "elimination_entry": 1}

    res = mig.run_migration(seeded_db)
    seam.dispose_engines()

    rows = {r["entity_id"]: r for r in _trail(seeded_db)}
    assert set(rows) == set(before), rows
    for table, count in before.items():
        row = rows[table]
        assert row["skill"] == "erpclaw-growth"
        assert row["entity_type"] == "table"
        assert row["old_values"] == {"rows": count}
        assert row["new_values"] == {"dropped": True, "archive": res["archive"]}
        assert str(count) in row["description"] and table in row["description"]
    assert res["audit_rows"] == 2
    # the archive named in the row is the one that exists, so the trail leads a
    # reader to the rows themselves rather than only to their number
    assert os.path.isfile(rows["elimination_entry"]["new_values"]["archive"])


def test_the_trail_records_an_empty_table_as_empty(empty_tables_db, home):
    """Zero is the interesting number: it is the measurement that turns "these
    tables are empty" from a docstring claim into a recorded fact."""
    res = mig.run_migration(empty_tables_db)
    seam.dispose_engines()

    rows = {r["entity_id"]: r for r in _trail(empty_tables_db)}
    assert set(rows) == {"elimination_rule", "elimination_entry"}
    assert all(r["old_values"] == {"rows": 0} for r in rows.values())
    assert all(r["new_values"]["archive"] is None for r in rows.values())
    assert "no archive was written" in rows["elimination_rule"]["description"]
    assert res["audit_rows"] == 2


def test_report_only_writes_no_trail(seeded_db, home, capsys):
    res = mig.run_migration(seeded_db, report_only=True)
    seam.dispose_engines()

    assert _trail(seeded_db) == []
    assert res["audit_rows"] == 0
    assert "no audit_log row is written" in capsys.readouterr().out


def test_a_second_run_does_not_duplicate_the_trail(seeded_db, home):
    mig.run_migration(seeded_db)
    seam.dispose_engines()
    first = _trail(seeded_db)
    assert len(first) == 2

    second = mig.run_migration(seeded_db)
    seam.dispose_engines()

    assert second["audit_rows"] == 0
    assert _trail(seeded_db) == first


def test_a_drop_that_fails_leaves_no_trail(seeded_db, home, monkeypatch):
    """Same transaction, proven by breaking it: the trail rows are written BEFORE
    the drops (they are what opens the transaction the drops then join), so a
    drop that raises must take them with it. Otherwise the install keeps a record
    of a destruction that never happened."""
    before = _surviving(seeded_db)
    real_get_connection = mig.get_connection

    class _FailTheSecondDrop:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, *a, **k):
            if sql == mig._DROP["elimination_rule"]:
                raise RuntimeError("simulated failure on the second drop")
            return self._inner.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    with monkeypatch.context() as mp:
        mp.setattr(mig, "get_connection",
                   lambda p: _FailTheSecondDrop(real_get_connection(p)))
        with pytest.raises(RuntimeError):
            mig.run_migration(seeded_db)
    seam.dispose_engines()

    assert _trail(seeded_db) == [], (
        "a failed run left audit rows claiming tables were dropped")
    # The child DDL that DID execute rolls back with them: one transaction.
    assert _surviving(seeded_db) == before


def test_it_refuses_to_destroy_rows_it_cannot_record(tmp_path, home):
    """A database with no `audit_log` is not a database this migration will drop
    tables in. Checked FIRST, before the archive, so the refusal costs nothing
    and cannot half-happen."""
    path = str(tmp_path / "no_audit.sqlite")
    seam.provision(_legacy_metadata(with_audit_log=False), path)
    seam.dispose_engines()

    with pytest.raises(RuntimeError, match="audit_log is absent"):
        mig.run_migration(path)
    seam.dispose_engines()

    assert seam.table_exists("elimination_rule", path)
    assert seam.table_exists("elimination_entry", path)
    assert _archives(home) == [], "it wrote an archive before refusing"


def test_report_only_says_the_real_run_would_refuse_rather_than_raising(
        tmp_path, home, capsys):
    """`--report-only` writes nothing, so it has nothing to refuse — its job is to
    say what the real run would do, and "it would refuse" is that. A report that
    raised would be right about the outcome and wrong about its own contract."""
    path = str(tmp_path / "no_audit_report.sqlite")
    seam.provision(_legacy_metadata(with_audit_log=False), path)
    seam.dispose_engines()

    res = mig.run_migration(path, report_only=True)
    seam.dispose_engines()

    out = capsys.readouterr().out
    assert "audit_log is ABSENT" in out and "would REFUSE" in out
    assert res["dropped"] == [] and res["audit_rows"] == 0
    assert seam.table_exists("elimination_rule", path)
    assert _archives(home) == []


def test_the_migration_id_is_the_stem_the_runner_ledgers_it_under():
    """`migration_runner.discover` ledgers the file under `fn[:-3]`, and the trail
    is retrieved by `migration:<that stem>`. Pinned BY VALUE: deriving the
    expectation from `mig.MIGRATION_ID` would agree with any drifted value."""
    assert mig.MIGRATION_ID == "007_retire_legacy_elimination_tables"
    assert mig.MIGRATION_DATA_CLASS == "rows"
