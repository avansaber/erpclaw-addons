"""L1 unit tests for offline bank feed import v1 (offline_bank_feed.py).

Covers: exact 500.03 Decimal-as-TEXT money, both provider mappings
(NetSuite Transaction ID vs Xero Reference), idempotent replay, partial
overlap, company isolation, unsafe path refusal, malformed input rollback
(no partial writes), deterministic file-order storage, and no ledger writes.
"""
import os
import sys

import pytest

from integration_helpers import (
    load_db_query, call_action, ns, is_error, is_ok,
    seed_company, seed_naming_series,
)

MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, MODULE_DIR)
import offline_bank_feed as obf  # noqa: E402

ACTION = "integration-import-offline-bank-feed"

NETSUITE_ROWS = [
    ("2026-03-20", "Acme invoice 1042", "NS-001", "1500.00"),
    ("2026-03-05", "Office rent March", "NS-002", "-999.97"),
    ("2026-03-12", "Refund from vendor", "NS-003", "500.03"),
]

XERO_ROWS = [
    ("2026-04-02", "Stripe payout", "X-101", "750.25"),
    ("2026-04-09", "Supplier payment", "X-102", "-250.22"),
]


def _write_csv(path, headers, rows):
    with open(path, "w", newline="", encoding="utf-8") as handle:
        handle.write(",".join(headers) + "\n")
        for row in rows:
            handle.write(",".join(f'"{cell}"' for cell in row) + "\n")
    return str(path)


def _netsuite_csv(path):
    return _write_csv(path, ["Date", "Description", "Transaction ID", "Amount"],
                      NETSUITE_ROWS)


def _xero_csv(path):
    return _write_csv(path, ["Date", "Description", "Reference", "Amount"],
                      XERO_ROWS)


def _seed_company(conn):
    cid = seed_company(conn)
    seed_naming_series(conn, cid)
    return cid


def _counts(conn):
    batch = conn.execute(
        "SELECT COUNT(*) FROM integration_offline_bank_batch").fetchone()[0]
    lines = conn.execute(
        "SELECT COUNT(*) FROM integration_offline_bank_line").fetchone()[0]
    audits = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
    ledger = conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0]
    return (batch, lines, audits, ledger)


def _import(mod, conn, company_id, provider, account_ref, csv_path):
    return call_action(mod.ACTIONS[ACTION], conn, ns(
        company_id=company_id, provider=provider,
        account_ref=account_ref, file=csv_path))


# ---------------------------------------------------------------------------
# happy paths: exact money + both provider mappings
# ---------------------------------------------------------------------------
def test_netsuite_import_exact_money(conn, tmp_path):
    mod = load_db_query()
    cid = _seed_company(conn)
    csv_path = _netsuite_csv(tmp_path / "netsuite.csv")
    result = _import(mod, conn, cid, "netsuite", "CHK-001", csv_path)
    assert is_ok(result), result
    assert result["batch_id"]
    assert result["row_count"] == 3
    assert result["line_count"] == 3
    assert result["debit_total"] == "999.97"
    assert result["credit_total"] == "2000.03"
    assert result["skipped_duplicate_count"] == 0
    assert result["posted"] is False

    batch = conn.execute(
        "SELECT company_id, provider, account_ref, file_path, row_count,"
        " debit_total, credit_total FROM integration_offline_bank_batch"
        " WHERE id = ?", (result["batch_id"],)).fetchone()
    assert tuple(batch) == (cid, "netsuite", "CHK-001", os.path.realpath(csv_path),
                            3, "999.97", "2000.03")

    stored = [tuple(row) for row in conn.execute(
        "SELECT txn_date, description, external_id, amount, sequence"
        " FROM integration_offline_bank_line WHERE batch_id = ?"
        " ORDER BY sequence", (result["batch_id"],)).fetchall()]
    assert stored == [
        ("2026-03-20", "Acme invoice 1042", "NS-001", "1500.00", 1),
        ("2026-03-05", "Office rent March", "NS-002", "-999.97", 2),
        ("2026-03-12", "Refund from vendor", "NS-003", "500.03", 3),
    ]
    assert conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0] == 0


def test_xero_import_mapping(conn, tmp_path):
    mod = load_db_query()
    cid = _seed_company(conn)
    csv_path = _xero_csv(tmp_path / "xero.csv")
    result = _import(mod, conn, cid, "xero", "SAV-002", csv_path)
    assert is_ok(result), result
    assert result["provider"] == "xero"
    assert result["row_count"] == 2
    assert result["debit_total"] == "250.22"
    assert result["credit_total"] == "750.25"
    assert result["skipped_duplicate_count"] == 0
    stored = [tuple(row) for row in conn.execute(
        "SELECT external_id, amount, sequence"
        " FROM integration_offline_bank_line WHERE batch_id = ?"
        " ORDER BY sequence", (result["batch_id"],)).fetchall()]
    assert stored == [("X-101", "750.25", 1), ("X-102", "-250.22", 2)]


def test_provider_mapping_is_documented():
    assert set(obf.PROVIDER_COLUMNS) == {"netsuite", "xero"}
    for provider, mapping in obf.PROVIDER_COLUMNS.items():
        assert set(mapping) == {"date", "description", "external_id", "amount"}, provider
    assert obf.PROVIDER_COLUMNS["netsuite"]["external_id"] == "Transaction ID"
    assert obf.PROVIDER_COLUMNS["xero"]["external_id"] == "Reference"


# ---------------------------------------------------------------------------
# idempotency + company isolation
# ---------------------------------------------------------------------------
def test_idempotent_replay_returns_existing_batch(conn, tmp_path):
    mod = load_db_query()
    cid = _seed_company(conn)
    csv_path = _netsuite_csv(tmp_path / "netsuite.csv")
    first = _import(mod, conn, cid, "netsuite", "CHK-001", csv_path)
    assert is_ok(first), first
    before = _counts(conn)
    second = _import(mod, conn, cid, "netsuite", "CHK-001", csv_path)
    assert is_ok(second), second
    assert second["batch_id"] == first["batch_id"]
    assert second["row_count"] == 3
    assert second["skipped_duplicate_count"] == 3
    assert second["debit_total"] == "999.97"
    assert second["credit_total"] == "2000.03"
    assert _counts(conn) == before


def test_partial_overlap_imports_only_new_lines(conn, tmp_path):
    mod = load_db_query()
    cid = _seed_company(conn)
    first = _import(mod, conn, cid, "netsuite", "CHK-001",
                    _netsuite_csv(tmp_path / "a.csv"))
    assert is_ok(first), first
    overlap = _write_csv(tmp_path / "b.csv",
                         ["Date", "Description", "Transaction ID", "Amount"],
                         [NETSUITE_ROWS[0],
                          ("2026-03-25", "New deposit", "NS-009", "100.00")])
    second = _import(mod, conn, cid, "netsuite", "CHK-001", overlap)
    assert is_ok(second), second
    assert second["batch_id"] != first["batch_id"]
    assert second["row_count"] == 1
    assert second["skipped_duplicate_count"] == 1
    assert second["debit_total"] == "0.00"
    assert second["credit_total"] == "100.00"
    total = conn.execute(
        "SELECT COUNT(*) FROM integration_offline_bank_line"
        " WHERE company_id = ?", (cid,)).fetchone()[0]
    assert total == 4


def test_company_isolation(conn, tmp_path):
    mod = load_db_query()
    cid_a = _seed_company(conn)
    cid_b = _seed_company(conn)
    csv_path = _netsuite_csv(tmp_path / "netsuite.csv")
    result_a = _import(mod, conn, cid_a, "netsuite", "CHK-001", csv_path)
    assert is_ok(result_a), result_a
    result_b = _import(mod, conn, cid_b, "netsuite", "CHK-001", csv_path)
    assert is_ok(result_b), result_b
    assert result_b["batch_id"] != result_a["batch_id"]
    assert result_b["row_count"] == 3
    assert result_b["skipped_duplicate_count"] == 0
    count_a = conn.execute(
        "SELECT COUNT(*) FROM integration_offline_bank_line"
        " WHERE company_id = ?", (cid_a,)).fetchone()[0]
    count_b = conn.execute(
        "SELECT COUNT(*) FROM integration_offline_bank_line"
        " WHERE company_id = ?", (cid_b,)).fetchone()[0]
    assert (count_a, count_b) == (3, 3)


# ---------------------------------------------------------------------------
# refusals: unsafe paths, malformed input, no partial writes
# ---------------------------------------------------------------------------
def test_unsafe_path_refusal(conn, tmp_path):
    mod = load_db_query()
    cid = _seed_company(conn)
    before = _counts(conn)
    not_csv = tmp_path / "notes.txt"
    not_csv.write_text("Date,Description,Reference,Amount\n")
    bad_paths = [
        str(tmp_path / "missing.csv"),
        str(not_csv),
        str(tmp_path / ".." / "outside.csv"),
        "",
    ]
    for bad in bad_paths:
        result = _import(mod, conn, cid, "xero", "SAV-002", bad)
        assert is_error(result), (bad, result)
    assert _counts(conn) == before


def test_malformed_input_rollback(conn, tmp_path):
    mod = load_db_query()
    cid = _seed_company(conn)
    headers = ["Date", "Description", "Reference", "Amount"]
    cases = {
        "bad-date": [("not-a-date", "Stripe payout", "X-201", "10.00")],
        "bad-amount": [("2026-04-02", "Stripe payout", "X-202", "ten")],
        "blank-amount": [("2026-04-02", "Stripe payout", "X-203", "")],
        "intra-file-dup": [("2026-04-02", "One", "X-204", "10.00"),
                           ("2026-04-03", "Two", "X-204", "20.00")],
        "missing-external": [("2026-04-02", "Stripe payout", "", "10.00")],
    }
    for name, rows in cases.items():
        before = _counts(conn)
        csv_path = _write_csv(tmp_path / f"{name}.csv", headers, rows)
        result = _import(mod, conn, cid, "xero", "SAV-002", csv_path)
        assert is_error(result), (name, result)
        assert _counts(conn) == before, name

    before = _counts(conn)
    no_amount = _write_csv(tmp_path / "no-amount.csv",
                           ["Date", "Description", "Reference"],
                           [("2026-04-02", "Stripe payout", "X-205")])
    assert is_error(_import(mod, conn, cid, "xero", "SAV-002", no_amount))
    assert _counts(conn) == before

    before = _counts(conn)
    csv_path = _netsuite_csv(tmp_path / "ok.csv")
    assert is_error(_import(mod, conn, cid, "quickbooks", "CHK-001", csv_path))
    assert is_error(_import(mod, conn, "no-such-company", "netsuite",
                            "CHK-001", csv_path))
    assert is_error(call_action(mod.ACTIONS[ACTION], conn, ns(
        company_id=None, provider="netsuite",
        account_ref="CHK-001", file=csv_path)))
    assert is_error(call_action(mod.ACTIONS[ACTION], conn, ns(
        company_id=cid, provider="netsuite",
        account_ref="   ", file=csv_path)))
    assert _counts(conn) == before


# ---------------------------------------------------------------------------
# deterministic ordering + no ledger writes
# ---------------------------------------------------------------------------
def test_deterministic_file_order(conn, tmp_path):
    mod = load_db_query()
    cid = _seed_company(conn)
    rows = [
        ("2026-05-30", "Last", "D-3", "3.00"),
        ("2026-05-01", "First", "D-1", "1.00"),
        ("2026-05-15", "Middle", "D-2", "2.00"),
    ]
    csv_path = _write_csv(tmp_path / "shuffled.csv",
                          ["Date", "Description", "Reference", "Amount"], rows)
    result = _import(mod, conn, cid, "xero", "SAV-009", csv_path)
    assert is_ok(result), result
    assert [line["external_id"] for line in result["lines"]] == ["D-3", "D-1", "D-2"]
    stored = [row["external_id"] for row in conn.execute(
        "SELECT external_id FROM integration_offline_bank_line"
        " WHERE batch_id = ? ORDER BY sequence",
        (result["batch_id"],)).fetchall()]
    assert stored == ["D-3", "D-1", "D-2"]


def test_no_ledger_or_foreign_table_writes(conn, tmp_path):
    mod = load_db_query()
    cid = _seed_company(conn)
    _import(mod, conn, cid, "netsuite", "CHK-001",
            _netsuite_csv(tmp_path / "n.csv"))
    _import(mod, conn, cid, "xero", "SAV-002", _xero_csv(tmp_path / "x.csv"))
    assert conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM bank_statement").fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM bank_statement_line").fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM integration_quickbooks_batch").fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM integration_entity_map").fetchone()[0] == 0
