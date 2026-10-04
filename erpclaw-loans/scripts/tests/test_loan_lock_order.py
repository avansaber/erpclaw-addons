"""Repaying and writing off a loan take the company ledger chain head first (m834a).

Every action that posts to the ledger and changes a document's figures or
state takes the company's ledger chain head before its first write, then
decides on state re-read under the head; the document write is a
compare-and-set on what the decision was made from. The two loan postings in
``repayments.py`` did not, so overlapping repayments lost one repayment from
the loan while the schedule rows and the ledger recorded two.

Every loan here is ``12000`` at rate ``0`` over 12 periods, disbursed, so all
totals are literal. SQLite legs use the ``conn``/``env``/``mod`` fixtures from
``tests/conftest.py`` (SQLite-only; not edited). PostgreSQL legs use the
module-local ``pg_book`` fixture below.
"""
import importlib.util
import json
import os
import subprocess
import sys
import time
import uuid
from decimal import Decimal

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest  # noqa: E402

from loans_helpers import call_action, get_conn, is_error, is_ok, ns  # noqa: E402
import loans_helpers as _loans_helpers  # noqa: E402

_LGV_PATH = os.path.join(_TESTS_DIR, "test_loan_gl_values.py")
_lgv_spec = importlib.util.spec_from_file_location(
    "test_loan_gl_values_book", _LGV_PATH)
_lgv = importlib.util.module_from_spec(_lgv_spec)
_lgv_spec.loader.exec_module(_lgv)

_PAY_TESTS = os.path.normpath(os.path.join(
    _TESTS_DIR, "..", "..", "..", "..", "erpclaw", "scripts",
    "erpclaw-payments", "tests"))
_pay_helpers_spec = importlib.util.spec_from_file_location(
    "payments_helpers", os.path.join(_PAY_TESTS, "payments_helpers.py"))
_pay_helpers = importlib.util.module_from_spec(_pay_helpers_spec)
sys.modules["payments_helpers"] = _pay_helpers
_pay_helpers_spec.loader.exec_module(_pay_helpers)
_proofs_spec = importlib.util.spec_from_file_location(
    "loan_lock_proofs_src",
    os.path.join(_PAY_TESTS, "test_chain_lock_proofs.py"))
_proofs = importlib.util.module_from_spec(_proofs_spec)
_proofs_spec.loader.exec_module(_proofs)
_cas_spec = importlib.util.spec_from_file_location(
    "loan_lock_cas_src", os.path.join(
        _PAY_TESTS, "test_payment_edit_and_allocation_compare_and_set.py"))
_cas = importlib.util.module_from_spec(_cas_spec)
_cas_spec.loader.exec_module(_cas)
if _PAY_TESTS in sys.path:
    sys.path.remove(_PAY_TESTS)

_LOANS_INIT_PATH = os.path.normpath(
    os.path.join(_TESTS_DIR, "..", "..", "init_db.py"))
_loans_init_spec = importlib.util.spec_from_file_location(
    "loans_init_pg", _LOANS_INIT_PATH)
_loans_init = importlib.util.module_from_spec(_loans_init_spec)
_loans_init_spec.loader.exec_module(_loans_init)

_LOANS_SCRIPT = os.path.normpath(os.path.join(_TESTS_DIR, "..", "db_query.py"))

_pg_only = _proofs._pg_only
_proc_env = _proofs._proc_env
_assert_chain_intact = _proofs._assert_chain_intact
_assert_contiguous = _proofs._assert_contiguous
_RecordingProxy = _cas._RecordingProxy

D = Decimal

_WATCH_TABLES = ("naming_series", "loan", "loan_repayment",
                 "loan_repayment_schedule", "loan_write_off", "gl_entry")


def _msg(result):
    return result.get("message", "") + result.get("error", "")


def _new_loan(conn, env, mod):
    app_id = _lgv._approved_application(
        conn, env, mod, amount="12000", rate="0", periods=12)
    return _lgv._disburse(conn, env, mod, app_id)["loan_id"]


def _paid_sum(conn, loan_id):
    return sum((D(r[0]) for r in conn.execute(
        "SELECT paid_amount FROM loan_repayment_schedule WHERE loan_id = ?",
        (loan_id,)).fetchall()), D("0"))


def _repayments_module():
    return sys.modules["repayments"]


def _build_env_pg(conn):
    """Same environment as ``loans_helpers.build_env`` but without the
    SQLite-only ``INSERT OR IGNORE`` naming-series seed (``get_next_name``
    upserts its own row). Recorded per the task packet."""
    cid = _loans_helpers.seed_company(conn)
    _loans_helpers.seed_cost_center(conn, cid)
    _loans_helpers.seed_fiscal_year(conn, cid)
    cust = _loans_helpers.seed_customer(conn, cid, "Acme Corp")
    emp = _loans_helpers.seed_employee(conn, cid, "John Doe")
    sup = _loans_helpers.seed_supplier(conn, cid, "Parts Inc")
    loan_acct = _loans_helpers.seed_account(
        conn, cid, "Loan Receivable", "receivable", "asset")
    interest_acct = _loans_helpers.seed_account(
        conn, cid, "Interest Income", "revenue", "income")
    disbursement_acct = _loans_helpers.seed_account(
        conn, cid, "Bank Account", "bank", "asset")
    bad_debt_acct = _loans_helpers.seed_account(
        conn, cid, "Bad Debt Expense", "expense", "expense")
    return {
        "company_id": cid,
        "customer_id": cust,
        "employee_id": emp,
        "supplier_id": sup,
        "loan_account_id": loan_acct,
        "interest_income_account_id": interest_acct,
        "disbursement_account_id": disbursement_acct,
        "bad_debt_account_id": bad_debt_acct,
    }


@pytest.fixture
def pg_book():
    """PostgreSQL book: fresh schema, loans tables, one company environment.

    Same environment steps as ``erpclaw-payments/tests/conftest.py``
    ``db_path``, then the loans tables, then a ``get_connection()``.
    Skips (stopping the PostgreSQL legs) when the book cannot be built.
    """
    _pg_only()
    pg_url = os.environ.get("ERPCLAW_PG_TEST_URL")
    if not pg_url:
        pytest.skip("PostgreSQL legs need ERPCLAW_PG_TEST_URL")
    from erpclaw_lib.db import get_connection as _get_connection  # noqa: E402
    old_url = os.environ.get("ERPCLAW_DB_URL")
    old_path = os.environ.get("ERPCLAW_DB_PATH")
    os.environ["ERPCLAW_DB_URL"] = pg_url
    os.environ.pop("ERPCLAW_DB_PATH", None)
    conn = None
    redacted = "<pg-url-redacted>"

    def _fail(step, exc):
        detail = str(exc).replace(pg_url, redacted)
        pytest.fail(f"{step} failed: {type(exc).__name__}: {detail}")

    try:
        try:
            _pay_helpers.init_all_tables(None)
        except Exception as exc:
            _fail("PostgreSQL foundation build", exc)
        try:
            _loans_init.create_loans_tables(
                os.environ["ERPCLAW_PG_TEST_URL"])
        except Exception as exc:
            _fail("PostgreSQL loans-table build", exc)
        conn = _get_connection()
        try:
            env = _build_env_pg(conn)
        except Exception as exc:
            _fail("PostgreSQL book build", exc)
        mod = _loans_helpers.load_db_query()
        yield (conn, env, mod)
    finally:
        try:
            if conn is not None:
                conn.close()
        finally:
            if old_url is None:
                os.environ.pop("ERPCLAW_DB_URL", None)
            else:
                os.environ["ERPCLAW_DB_URL"] = old_url
            if old_path is None:
                os.environ.pop("ERPCLAW_DB_PATH", None)
            else:
                os.environ["ERPCLAW_DB_PATH"] = old_path


def _head_take_wrapper(monkeypatch, repayments, loan_id, plant):
    real = getattr(repayments, "take_chain_heads", None)
    if real is None:
        def real(conn, company_ids):  # base has no head take: no-op
            return None

    def _wrap(conn, company_ids):
        real(conn, company_ids)
        plant(conn, loan_id)
        conn.commit()
        real(conn, company_ids)

    monkeypatch.setattr(repayments, "take_chain_heads", _wrap, raising=False)


def _gl_counter(monkeypatch, repayments):
    calls = []
    real = repayments.insert_gl_entries

    def _count(*args, **kwargs):
        calls.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(repayments, "insert_gl_entries", _count)
    return calls


def test_1a_stale_repayment_sees_head_state_sqlite(conn, env, mod, monkeypatch):
    repayments = _repayments_module()
    loan_id = _new_loan(conn, env, mod)
    calls = _gl_counter(monkeypatch, repayments)

    def _plant(c, lid):
        c.execute("UPDATE loan SET total_repaid = '500.00', "
                  "outstanding_amount = '11500.00' WHERE id = ?", (lid,))

    _head_take_wrapper(monkeypatch, repayments, loan_id, _plant)
    res = _lgv._repay(conn, mod, loan_id, principal="1000.00")
    assert is_ok(res), res
    row = conn.execute(
        "SELECT total_repaid, outstanding_amount, status FROM loan "
        "WHERE id = ?", (loan_id,)).fetchone()
    assert D(row["total_repaid"]) == D("1500.00")
    assert D(row["outstanding_amount"]) == D("10500.00")
    assert row["status"] == "partially_repaid"
    assert len(calls) == 1


def test_1a_stale_repayment_sees_head_state_pg(pg_book, monkeypatch):
    conn, env, mod = pg_book
    repayments = _repayments_module()
    loan_id = _new_loan(conn, env, mod)
    calls = _gl_counter(monkeypatch, repayments)

    def _plant(c, lid):
        c.execute("UPDATE loan SET total_repaid = '500.00', "
                  "outstanding_amount = '11500.00' WHERE id = ?", (lid,))

    _head_take_wrapper(monkeypatch, repayments, loan_id, _plant)
    res = _lgv._repay(conn, mod, loan_id, principal="1000.00")
    assert is_ok(res), res
    row = conn.execute(
        "SELECT total_repaid, outstanding_amount, status FROM loan "
        "WHERE id = ?", (loan_id,)).fetchone()
    assert D(row["total_repaid"]) == D("1500.00")
    assert D(row["outstanding_amount"]) == D("10500.00")
    assert row["status"] == "partially_repaid"
    assert len(calls) == 1


def test_1b_stale_writeoff_is_refused_sqlite(conn, env, mod, monkeypatch):
    repayments = _repayments_module()
    loan_id = _new_loan(conn, env, mod)
    calls = _gl_counter(monkeypatch, repayments)
    gl_before = conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0]

    def _plant(c, lid):
        c.execute("UPDATE loan SET status = 'written_off' WHERE id = ?",
                  (lid,))

    _head_take_wrapper(monkeypatch, repayments, loan_id, _plant)
    res = call_action(mod.loan_write_off_loan, conn, ns(
        loan_id=loan_id, bad_debt_account_id=env["bad_debt_account_id"],
        reason="stale", write_off_date="2026-09-30"))
    assert is_error(res), res
    assert _msg(res) == "Cannot write off loan in status 'written_off'"
    assert len(calls) == 0
    assert conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0] == gl_before
    assert conn.execute(
        "SELECT COUNT(*) FROM loan_write_off").fetchone()[0] == 0


def test_1b_stale_writeoff_is_refused_pg(pg_book, monkeypatch):
    conn, env, mod = pg_book
    repayments = _repayments_module()
    loan_id = _new_loan(conn, env, mod)
    calls = _gl_counter(monkeypatch, repayments)
    gl_before = conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0]

    def _plant(c, lid):
        c.execute("UPDATE loan SET status = 'written_off' WHERE id = ?",
                  (lid,))

    _head_take_wrapper(monkeypatch, repayments, loan_id, _plant)
    res = call_action(mod.loan_write_off_loan, conn, ns(
        loan_id=loan_id, bad_debt_account_id=env["bad_debt_account_id"],
        reason="stale", write_off_date="2026-09-30"))
    assert is_error(res), res
    assert _msg(res) == "Cannot write off loan in status 'written_off'"
    assert len(calls) == 0
    assert conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0] == gl_before
    assert conn.execute(
        "SELECT COUNT(*) FROM loan_write_off").fetchone()[0] == 0


def _assert_head_before_first_write(proxy):
    writes = [s for s in proxy.statements
              if s.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))]
    assert writes, "expected writes, got none"
    assert writes[0].lstrip().upper().startswith("INSERT"), writes[0]
    assert "gl_chain_head" in writes[0], writes[0]
    watched = [w for w in writes
               if any(t in w for t in _WATCH_TABLES)]
    assert watched, "expected document/ledger writes after the head"
    first_watched = next(
        i for i, w in enumerate(writes)
        if any(t in w for t in _WATCH_TABLES))
    assert first_watched > 0, writes[:3]


def test_2_head_before_first_write_repayment(conn, env, mod):
    loan_id = _new_loan(conn, env, mod)
    proxy = _RecordingProxy(conn)
    res = call_action(mod.loan_record_repayment, proxy, ns(
        loan_id=loan_id, principal_amount="1000.00", interest_amount="0",
        penalty_amount=None, payment_method="bank_transfer",
        repayment_date="2026-07-01", reference_number="CHK-001",
        remarks="head order"))
    assert is_ok(res), res
    _assert_head_before_first_write(proxy)


def test_2_head_before_first_write_writeoff(conn, env, mod):
    loan_id = _new_loan(conn, env, mod)
    proxy = _RecordingProxy(conn)
    res = call_action(mod.loan_write_off_loan, proxy, ns(
        loan_id=loan_id, bad_debt_account_id=env["bad_debt_account_id"],
        reason="head order", write_off_date="2026-09-30"))
    assert is_ok(res), res
    _assert_head_before_first_write(proxy)


def test_3_happy_path_unchanged(conn, env, mod):
    app_id = _lgv._approved_application(
        conn, env, mod, amount="12000", rate="0", periods=12)
    loan_id = _lgv._disburse(conn, env, mod, app_id)["loan_id"]

    res = _lgv._repay(conn, mod, loan_id, principal="1000", interest="50")
    assert is_ok(res), res

    gl = _lgv._gl_by_account(conn, res["id"])
    assert len(gl) == 3, "DR bank / CR loan receivable / CR interest income"
    bank = gl[env["disbursement_account_id"]]
    assert D(bank["debit"]) == D("1050.00")
    assert D(bank["credit"]) == D("0")
    receivable = gl[env["loan_account_id"]]
    assert D(receivable["credit"]) == D("1000.00")
    assert D(receivable["debit"]) == D("0")
    assert receivable["party_type"] == "customer"
    assert receivable["party_id"] == env["customer_id"]
    income = gl[env["interest_income_account_id"]]
    assert D(income["credit"]) == D("50.00")
    assert D(income["debit"]) == D("0")

    loan = _lgv._loan(conn, loan_id)
    assert D(loan["outstanding_amount"]) == D("11000.00")
    assert loan["status"] == "partially_repaid"

    wo = call_action(mod.loan_write_off_loan, conn, ns(
        loan_id=loan_id, bad_debt_account_id=env["bad_debt_account_id"],
        reason="Debtor insolvent", write_off_date="2026-09-30"))
    assert is_ok(wo), wo
    assert D(wo["write_off_amount"]) == D("11000.00")

    loan2 = _lgv._loan(conn, loan_id)
    assert loan2["status"] == "written_off"
    assert D(loan2["outstanding_amount"]) == D("0")

    row = conn.execute(
        "SELECT write_off_amount, outstanding_at_write_off, reason, status "
        "FROM loan_write_off WHERE id = ?", (wo["id"],)).fetchone()
    assert D(row["write_off_amount"]) == D("11000.00")
    assert D(row["outstanding_at_write_off"]) == D("11000.00")
    assert row["reason"] == "Debtor insolvent"
    assert row["status"] == "submitted"

    pair = _lgv._gl(conn, wo["id"])
    assert len(pair) == 2
    assert pair[0]["account_id"] == env["bad_debt_account_id"]
    assert D(pair[0]["debit"]) == D("11000.00")
    assert pair[1]["account_id"] == env["loan_account_id"]
    assert D(pair[1]["credit"]) == D("11000.00")
    assert pair[1]["party_type"] == "customer"
    assert pair[1]["party_id"] == env["customer_id"]
    assert "Debtor insolvent" in pair[1]["remarks"]


def test_4_head_first_pg(pg_book):
    from erpclaw_lib.db import get_connection as _get_connection  # noqa: E402
    from erpclaw_lib.gl_posting import take_chain_heads as _take  # noqa: E402
    conn, env, mod = pg_book
    loan_id = _new_loan(conn, env, mod)
    holder = _get_connection()
    proc = None
    try:
        try:
            _take(holder, [env["company_id"]])
            penv = _proc_env(ERPCLAW_PG_LOCK_TIMEOUT="10s")
            proc = subprocess.Popen(
                [sys.executable, _LOANS_SCRIPT,
                 "--action", "loan-record-repayment",
                 "--loan-id", loan_id,
                 "--principal-amount", "1000.00",
                 "--repayment-date", "2026-07-01"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                env=penv)
            time.sleep(1)
            assert proc.poll() is None, "repayment must wait at the head"
            third = _get_connection()
            try:
                third.execute("SET LOCAL lock_timeout = '500ms'")
                cur = third.execute(
                    "UPDATE loan SET updated_at = updated_at WHERE id = ?",
                    (loan_id,))
                assert cur.rowcount == 1
            finally:
                third.rollback()
                third.close()
        finally:
            try:
                holder.rollback()
            except Exception:  # noqa: BLE001, S110 - best effort
                pass
        assert proc is not None
        try:
            out, err = proc.communicate(timeout=12)
        finally:
            if proc.poll() is None:
                proc.kill()
        assert proc.returncode == 0, (out, err)
    finally:
        try:
            if proc is not None and proc.poll() is None:
                proc.kill()
        except Exception:  # noqa: BLE001, S110 - best effort
            pass
        try:
            holder.close()
        except Exception:  # noqa: BLE001, S110 - best effort
            pass


def test_5_no_lost_repayment_pg(pg_book):
    conn, _env0, mod = pg_book
    spawn_gaps = []
    for _ in range(5):
        env = _build_env_pg(conn)
        loan_id = _new_loan(conn, env, mod)
        company_id = env["company_id"]
        penv = _proc_env()
        args = [sys.executable, _LOANS_SCRIPT,
                "--action", "loan-record-repayment",
                "--loan-id", loan_id,
                "--principal-amount", "1000.00",
                "--repayment-date", "2026-07-01"]
        t0 = time.time()
        p1 = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=penv)
        p2 = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=penv)
        t2 = time.time()
        spawn_gaps.append(t2 - t0)
        try:
            out1, err1 = p1.communicate(timeout=15)
            out2, err2 = p2.communicate(timeout=15)
        finally:
            for p in (p1, p2):
                if p.poll() is None:
                    p.kill()
        assert p1.returncode == 0, (out1, err1)
        assert p2.returncode == 0, (out2, err2)
        assert "deadlock" not in (out1 + err1).lower(), (out1, err1)
        assert "deadlock" not in (out2 + err2).lower(), (out2, err2)
        after = conn.execute(
            "SELECT total_repaid, outstanding_amount FROM loan WHERE id = ?",
            (loan_id,)).fetchone()
        assert D(after["total_repaid"]) == D("2000.00")
        assert D(after["outstanding_amount"]) == D("10000.00")
        assert _paid_sum(conn, loan_id) == D("2000.00")
        rep_ids = [r[0] for r in conn.execute(
            "SELECT id FROM loan_repayment WHERE loan_id = ?",
            (loan_id,)).fetchall()]
        assert len(rep_ids) == 2
        for rid in rep_ids:
            grows = conn.execute(
                "SELECT debit, credit FROM gl_entry WHERE voucher_id = ? "
                "AND is_cancelled = 0", (rid,)).fetchall()
            assert len(grows) == 2
            assert D(grows[0]["debit"]) + D(grows[1]["debit"]) == D("1000.00")
            assert D(grows[0]["credit"]) + D(grows[1]["credit"]) == D("1000.00")
            assert max(D(grows[0]["debit"]), D(grows[1]["debit"])) == D("1000.00")
            assert max(D(grows[0]["credit"]), D(grows[1]["credit"])) == D("1000.00")
        _assert_chain_intact(conn, company_id)
        _assert_contiguous(conn, company_id)
    assert len(spawn_gaps) == 5, f"spawn_gaps={spawn_gaps}"


def test_6_final_compare_and_set_pg(pg_book, monkeypatch):
    from erpclaw_lib.db import get_connection as _get_connection  # noqa: E402
    conn, env, mod = pg_book
    repayments = _repayments_module()
    loan_id = _new_loan(conn, env, mod)
    gl_before = conn.execute("SELECT COUNT(*) FROM gl_entry").fetchone()[0]
    real = repayments.get_next_name
    state = {"once": True}

    def _wrap(c, *args, **kwargs):
        result = real(c, *args, **kwargs)
        if state["once"]:
            state["once"] = False
            other = _get_connection()
            try:
                other.execute(
                    "UPDATE loan SET status = 'written_off' WHERE id = ?",
                    (loan_id,))
                other.commit()
            finally:
                other.close()
        return result

    monkeypatch.setattr(repayments, "get_next_name", _wrap)
    res = _lgv._repay(conn, mod, loan_id, principal="1000.00")
    assert is_error(res), res
    assert _msg(res) == "Cannot record repayment for loan in status 'written_off'"
    fresh = _get_connection()
    try:
        assert fresh.execute(
            "SELECT COUNT(*) FROM loan_repayment WHERE loan_id = ?",
            (loan_id,)).fetchone()[0] == 0
        assert fresh.execute(
            "SELECT COUNT(*) FROM gl_entry").fetchone()[0] == gl_before
    finally:
        fresh.close()


def test_7_rollback_on_refusal_after_head(conn, env, mod, monkeypatch):
    repayments = _repayments_module()
    loan_id = _new_loan(conn, env, mod)
    before = conn.execute(
        "SELECT total_repaid, outstanding_amount FROM loan WHERE id = ?",
        (loan_id,)).fetchone()
    sched_before = _paid_sum(conn, loan_id)

    def _boom(*args, **kwargs):
        return repayments.err("planted")

    monkeypatch.setattr(repayments, "get_default_cost_center", _boom)
    res = _lgv._repay(conn, mod, loan_id, principal="1000.00")
    assert is_error(res), res
    assert _msg(res) == "planted"
    assert conn.execute(
        "SELECT COUNT(*) FROM loan_repayment WHERE loan_id = ?",
        (loan_id,)).fetchone()[0] == 0
    after = conn.execute(
        "SELECT total_repaid, outstanding_amount FROM loan WHERE id = ?",
        (loan_id,)).fetchone()
    assert D(after["total_repaid"]) == D(before["total_repaid"])
    assert D(after["outstanding_amount"]) == D(before["outstanding_amount"])
    assert _paid_sum(conn, loan_id) == sched_before


def _plant_outstanding_once(monkeypatch, repayments, conn, loan_id):
    real = repayments.dynamic_update
    state = {"n": 0}

    def _wrap(table, data, where):
        if state["n"] == 0:
            state["n"] += 1
            conn.execute(
                "UPDATE loan SET outstanding_amount = '11999.00' "
                "WHERE id = ?", (loan_id,))
        return real(table, data, where)

    monkeypatch.setattr(repayments, "dynamic_update", _wrap)


def test_8a_changed_while_repayment(conn, env, mod, db_path, monkeypatch):
    repayments = _repayments_module()
    loan_id = _new_loan(conn, env, mod)
    before_repaid = conn.execute(
        "SELECT total_repaid FROM loan WHERE id = ?",
        (loan_id,)).fetchone()["total_repaid"]
    calls = _gl_counter(monkeypatch, repayments)
    _plant_outstanding_once(monkeypatch, repayments, conn, loan_id)
    res = _lgv._repay(conn, mod, loan_id, principal="1000.00")
    assert is_error(res), res
    assert _msg(res) == (
        f"Loan {loan_id} changed while this repayment was being recorded; "
        "nothing was written. Retry the action.")
    assert calls == []
    fresh = get_conn(db_path)
    try:
        row = fresh.execute(
            "SELECT total_repaid, outstanding_amount FROM loan WHERE id = ?",
            (loan_id,)).fetchone()
        assert D(row["outstanding_amount"]) == D("12000.00")
        assert D(row["total_repaid"]) == D(before_repaid)
        assert fresh.execute(
            "SELECT COUNT(*) FROM loan_repayment WHERE loan_id = ?",
            (loan_id,)).fetchone()[0] == 0
    finally:
        fresh.close()


def test_8b_changed_while_writeoff(conn, env, mod, db_path, monkeypatch):
    repayments = _repayments_module()
    loan_id = _new_loan(conn, env, mod)
    _plant_outstanding_once(monkeypatch, repayments, conn, loan_id)
    res = call_action(mod.loan_write_off_loan, conn, ns(
        loan_id=loan_id, bad_debt_account_id=env["bad_debt_account_id"],
        reason="race", write_off_date="2026-09-30"))
    assert is_error(res), res
    assert _msg(res) == (
        f"Loan {loan_id} changed while this write-off was being recorded; "
        "nothing was written. Retry the action.")
    fresh = get_conn(db_path)
    try:
        row = fresh.execute(
            "SELECT status FROM loan WHERE id = ?", (loan_id,)).fetchone()
        assert row["status"] == "disbursed"
        assert fresh.execute(
            "SELECT COUNT(*) FROM loan_write_off").fetchone()[0] == 0
    finally:
        fresh.close()


# ── m834b: disbursing a loan takes the company ledger chain head first ──
#
# `handle_disburse_loan` checked "already disbursed" with a plain read, then
# took the `loan` naming row and posted. Two disbursements of one approved
# application that both read before either commits each create a loan and each
# post: the money goes out twice. With the head first, the second waits, then
# re-reads under the head and is refused with the existing message.


def _loans_module():
    return sys.modules["loans"]


def _disburse_head_wrapper(monkeypatch, loans_mod, app_id, planted_id, env):
    real = getattr(loans_mod, "take_chain_heads", None)
    if real is None:
        def real(conn, company_ids):  # base has no head take: no-op
            return None

    def _wrap(conn, company_ids):
        real(conn, company_ids)
        app_row = dict(conn.execute(
            "SELECT * FROM loan_application WHERE id = ?",
            (app_id,)).fetchone())
        conn.execute(
            "INSERT INTO loan (id, naming_series, loan_application_id, "
            "applicant_type, applicant_id, applicant_name, loan_type, "
            "loan_amount, disbursed_amount, total_interest, total_repaid, "
            "outstanding_amount, interest_rate, repayment_method, "
            "repayment_periods, disbursement_date, maturity_date, "
            "loan_account_id, interest_income_account_id, "
            "disbursement_account_id, status, company_id, created_at, "
            "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (planted_id, "PLANTED-001", app_id,
             app_row["applicant_type"], app_row["applicant_id"],
             app_row["applicant_name"], app_row["loan_type"],
             "12000.00", "12000.00", "0.00", "0.00", "12000.00", "0.00",
             app_row["repayment_method"], app_row["repayment_periods"],
             "2026-06-01", "2027-06-01",
             env["loan_account_id"], env["interest_income_account_id"],
             env["disbursement_account_id"], "disbursed",
             app_row["company_id"],
             "2026-06-01T00:00:00Z", "2026-06-01T00:00:00Z"),
        )
        conn.commit()
        real(conn, company_ids)

    monkeypatch.setattr(loans_mod, "take_chain_heads", _wrap, raising=False)


def test_9a_disburse_sees_planted_loan_sqlite(conn, env, mod, monkeypatch):
    loans_mod = _loans_module()
    app_id = _lgv._approved_application(
        conn, env, mod, amount="12000", rate="0", periods=12)
    planted = "11111111-2222-3333-4444-555555555555"
    calls = _gl_counter(monkeypatch, loans_mod)
    _disburse_head_wrapper(monkeypatch, loans_mod, app_id, planted, env)
    res = _lgv._disburse(conn, env, mod, app_id)
    assert is_error(res), res
    assert _msg(res) == (
        f"Loan already disbursed for application {app_id} (loan: {planted})")
    assert calls == []
    assert conn.execute(
        "SELECT COUNT(*) FROM loan WHERE loan_application_id = ?",
        (app_id,)).fetchone()[0] == 1


def test_9a_disburse_sees_planted_loan_pg(pg_book, monkeypatch):
    conn, env, mod = pg_book
    loans_mod = _loans_module()
    app_id = _lgv._approved_application(
        conn, env, mod, amount="12000", rate="0", periods=12)
    planted = "11111111-2222-3333-4444-555555555555"
    calls = _gl_counter(monkeypatch, loans_mod)
    _disburse_head_wrapper(monkeypatch, loans_mod, app_id, planted, env)
    res = _lgv._disburse(conn, env, mod, app_id)
    assert is_error(res), res
    assert _msg(res) == (
        f"Loan already disbursed for application {app_id} (loan: {planted})")
    assert calls == []
    assert conn.execute(
        "SELECT COUNT(*) FROM loan WHERE loan_application_id = ?",
        (app_id,)).fetchone()[0] == 1


def test_9b_disburse_head_before_first_write(conn, env, mod):
    app_id = _lgv._approved_application(
        conn, env, mod, amount="12000", rate="0", periods=12)
    proxy = _RecordingProxy(conn)
    res = _lgv._disburse(proxy, env, mod, app_id)
    assert is_ok(res), res
    writes = [s for s in proxy.statements
              if s.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))]
    assert writes, "expected writes, got none"
    assert writes[0].lstrip().upper().startswith("INSERT"), writes[0]
    assert "gl_chain_head" in writes[0], writes[0]
    for i, w in enumerate(writes):
        if any(t in w for t in ("naming_series", "loan",
                                "loan_repayment_schedule", "gl_entry")):
            assert i > 0, (i, w)


def test_9c_disburse_happy_path_unchanged(conn, env, mod):
    app_id = _lgv._approved_application(conn, env, mod, amount="50000",
                                        periods=12)
    res = _lgv._disburse(conn, env, mod, app_id)
    assert is_ok(res), res
    assert res["loan_application_id"] == app_id
    assert res["loan_amount"] == "50000.00"
    assert res["disbursement_date"] == _lgv.DISBURSE_DATE
    assert res["installments"] == 12

    loan = _lgv._loan(conn, res["loan_id"])
    assert D(loan["loan_amount"]) == D("50000.00")
    assert D(loan["disbursed_amount"]) == D("50000.00")
    assert D(loan["outstanding_amount"]) == D("50000.00")
    assert loan["status"] == "disbursed"

    rows = conn.execute(
        "SELECT principal_amount, interest_amount, total_amount, status "
        "FROM loan_repayment_schedule WHERE loan_id = ?",
        (res["loan_id"],)).fetchall()
    assert len(rows) == 12
    assert sum((D(r["principal_amount"]) for r in rows), D("0")) == D("50000.00")
    assert sum((D(r["total_amount"]) for r in rows), D("0")) == \
        D("50000.00") + D(loan["total_interest"])
    assert all(r["status"] == "pending" for r in rows)

    gl = _lgv._gl(conn, res["loan_id"])
    assert len(gl) == 2, "DR loan receivable / CR bank"
    assert gl[0]["account_id"] == env["loan_account_id"]
    assert D(gl[0]["debit"]) == D("50000.00")
    assert D(gl[0]["credit"]) == D("0")
    assert gl[0]["party_type"] == "customer"
    assert gl[0]["party_id"] == env["customer_id"]
    assert gl[1]["account_id"] == env["disbursement_account_id"]
    assert D(gl[1]["credit"]) == D("50000.00")
    assert D(gl[1]["debit"]) == D("0")

    assert _lgv._count(conn, "gl_entry") == 2
    assert _lgv._sum(conn, "SELECT debit FROM gl_entry WHERE is_cancelled = 0") == \
        D("50000.00")
    assert _lgv._sum(conn, "SELECT credit FROM gl_entry WHERE is_cancelled = 0") == \
        D("50000.00")


def test_9d_disbursed_once_pg(pg_book):
    conn, env, mod = pg_book
    company_id = env["company_id"]
    for _ in range(5):
        app_id = _lgv._approved_application(
            conn, env, mod, amount="12000", rate="0", periods=12)
        args = [sys.executable, _LOANS_SCRIPT,
                "--action", "loan-disburse-loan",
                "--loan-application-id", app_id,
                "--loan-account-id", env["loan_account_id"],
                "--interest-income-account-id",
                env["interest_income_account_id"],
                "--disbursement-account-id", env["disbursement_account_id"],
                "--disbursement-date", _lgv.DISBURSE_DATE]
        penv = _proc_env()
        t0 = time.time()
        p1 = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=penv)
        t1 = time.time()
        p2 = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=penv)
        t2 = time.time()
        assert (t2 - t1) <= 0.05, "both disbursements start within 50 ms"
        try:
            out1, err1 = p1.communicate(timeout=15)
            out2, err2 = p2.communicate(timeout=15)
        finally:
            for p in (p1, p2):
                if p.poll() is None:
                    p.kill()
        assert "deadlock" not in (out1 + err1).lower(), (out1, err1)
        assert "deadlock" not in (out2 + err2).lower(), (out2, err2)
        assert sorted([p1.returncode, p2.returncode]) == [0, 1], (out1, err1,
                                                                  out2, err2)
        if p1.returncode == 0:
            winner_out, loser_out = out1, out2
        else:
            winner_out, loser_out = out2, out1
        winner = json.loads(winner_out.strip())
        assert winner.get("status") == "ok", winner
        winner_loan = winner["loan_id"]
        loser = json.loads(loser_out.strip())
        assert loser.get("status") == "error", loser
        assert _msg(loser) == (
            f"Loan already disbursed for application {app_id} "
            f"(loan: {winner_loan})")
        assert conn.execute(
            "SELECT COUNT(*) FROM loan WHERE loan_application_id = ?",
            (app_id,)).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM gl_entry WHERE voucher_id = ?",
            (winner_loan,)).fetchone()[0] == 2
        _assert_chain_intact(conn, company_id)
        _assert_contiguous(conn, company_id)


def test_9e_disburse_refusal_rolls_back_head(conn, env, mod):
    first_app = _lgv._approved_application(
        conn, env, mod, amount="12000", rate="0", periods=12)
    assert is_ok(_lgv._disburse(conn, env, mod, first_app))
    conn.execute("UPDATE gl_chain_head SET updated_at = ? WHERE company_id = ?",
                 ("2000-01-01 00:00:00", env["company_id"]))
    conn.commit()
    second_app = _lgv._approved_application(
        conn, env, mod, amount="12000", rate="0", periods=12)
    bogus = str(uuid.uuid4())
    res = call_action(mod.loan_disburse_loan, conn, ns(
        loan_application_id=second_app,
        loan_account_id=env["loan_account_id"],
        interest_income_account_id=bogus,
        disbursement_account_id=env["disbursement_account_id"],
        disbursement_date=_lgv.DISBURSE_DATE))
    assert is_error(res), res
    assert _msg(res) == f"Account {bogus} not found (--interest-income-account-id)"
    assert conn.execute(
        "SELECT updated_at FROM gl_chain_head WHERE company_id = ?",
        (env["company_id"],)).fetchone()["updated_at"] == "2000-01-01 00:00:00"
    assert conn.execute(
        "SELECT COUNT(*) FROM loan WHERE loan_application_id = ?",
        (second_app,)).fetchone()[0] == 0


# ---------------------------------------------------------------------------
# m834c: generate / restructure / close take the chain head first (appended).
#
# Every writer of an existing loan or its schedule takes the company's chain
# head before its first write and compare-and-sets the loan row. Money is
# Decimal over TEXT; all totals below are literal for a rate-0 12000/12 loan.
# ---------------------------------------------------------------------------
# NOTE: _loans_module() is defined above (m834b disbursement section) and is
# reused by the tests below; it is not redefined here.


def _m834c_snap_loan(conn, loan_id):
    return dict(conn.execute(
        "SELECT * FROM loan WHERE id = ?", (loan_id,)).fetchone())


def _m834c_snap_sched(conn, loan_id):
    return [tuple(r) for r in conn.execute(
        "SELECT installment_no, principal_amount, interest_amount, "
        "total_amount, paid_amount, outstanding, status "
        "FROM loan_repayment_schedule WHERE loan_id = ? "
        "ORDER BY installment_no", (loan_id,)).fetchall()]


def _m834c_wrap_head_with_plant(monkeypatch, loans_mod, plant):
    real = getattr(loans_mod, "take_chain_heads", None)
    if real is None:
        def real(conn, company_ids):  # base has no head take: no-op
            return None

    def _wrap(conn, company_ids):
        real(conn, company_ids)
        plant(conn)
        conn.commit()
        real(conn, company_ids)

    monkeypatch.setattr(loans_mod, "take_chain_heads", _wrap, raising=False)


def _m834c_pypika_set_loan_status(conn, loan_id, status):
    from erpclaw_lib.query import Field, P, Q, Table  # noqa: E402
    t = Table("loan")
    sql = Q.update(t).set(Field("status"), P()).where(
        Field("id") == P()).get_sql()
    conn.execute(sql, (status, loan_id))


def _m834c_repay_all(conn, mod, loan_id):
    res = _lgv._repay(conn, mod, loan_id, principal="12000.00")
    assert is_ok(res), res
    return res


# --- 1. stale under the head, per action -----------------------------------

def test_m834c_1a_generate_stale_sqlite(conn, env, mod, db_path, monkeypatch):
    loans_mod = _loans_module()
    loan_id = _new_loan(conn, env, mod)
    pre_loan = _m834c_snap_loan(conn, loan_id)
    pre_sched = _m834c_snap_sched(conn, loan_id)

    def _plant(c):
        _m834c_pypika_set_loan_status(c, loan_id, "written_off")

    _m834c_wrap_head_with_plant(monkeypatch, loans_mod, _plant)
    res = call_action(
        mod.loan_generate_repayment_schedule, conn, ns(loan_id=loan_id))
    assert is_error(res), res
    assert _msg(res) == (
        "Cannot generate schedule for loan in status 'written_off'")
    fresh = get_conn(db_path)
    try:
        post_loan = _m834c_snap_loan(fresh, loan_id)
        assert post_loan["status"] == "written_off"
        for key, val in pre_loan.items():
            if key == "status":
                continue
            assert post_loan[key] == val, key
        assert _m834c_snap_sched(fresh, loan_id) == pre_sched
    finally:
        fresh.close()


def test_m834c_1a_generate_stale_pg(pg_book, monkeypatch):
    from erpclaw_lib.db import get_connection as _get_connection  # noqa: E402
    conn, env, mod = pg_book
    loans_mod = _loans_module()
    loan_id = _new_loan(conn, env, mod)
    pre_loan = _m834c_snap_loan(conn, loan_id)
    pre_sched = _m834c_snap_sched(conn, loan_id)

    def _plant(c):
        _m834c_pypika_set_loan_status(c, loan_id, "written_off")

    _m834c_wrap_head_with_plant(monkeypatch, loans_mod, _plant)
    res = call_action(
        mod.loan_generate_repayment_schedule, conn, ns(loan_id=loan_id))
    assert is_error(res), res
    assert _msg(res) == (
        "Cannot generate schedule for loan in status 'written_off'")
    fresh = _get_connection()
    try:
        post_loan = _m834c_snap_loan(fresh, loan_id)
        assert post_loan["status"] == "written_off"
        for key, val in pre_loan.items():
            if key == "status":
                continue
            assert post_loan[key] == val, key
        assert _m834c_snap_sched(fresh, loan_id) == pre_sched
    finally:
        fresh.close()


def test_m834c_1b_restructure_sees_repayment_sqlite(
        conn, env, mod, monkeypatch):
    from erpclaw_lib.query import Field, P, Q, Table  # noqa: E402
    loans_mod = _loans_module()
    loan_id = _new_loan(conn, env, mod)

    def _plant(c):
        rs = Table("loan_repayment_schedule")
        sql = Q.update(rs).set(Field("paid_amount"), P()).set(
            Field("outstanding"), P()).set(Field("status"), P()).where(
            Field("loan_id") == P()).where(
            Field("installment_no") == P()).get_sql()
        c.execute(sql, ("1000.00", "0.00", "paid", loan_id, 1))
        t = Table("loan")
        sql2 = Q.update(t).set(Field("total_repaid"), P()).set(
            Field("outstanding_amount"), P()).set(Field("status"), P()).where(
            Field("id") == P()).get_sql()
        c.execute(sql2, ("1000.00", "11000.00", "partially_repaid", loan_id))

    _m834c_wrap_head_with_plant(monkeypatch, loans_mod, _plant)
    res = call_action(mod.loan_restructure_loan, conn, ns(
        loan_id=loan_id, new_interest_rate=None, new_repayment_periods=24))
    assert is_ok(res), res
    assert res["remaining_principal"] == "11000.00"
    assert res["installments_regenerated"] == 23
    row = conn.execute(
        "SELECT outstanding_amount FROM loan WHERE id = ?",
        (loan_id,)).fetchone()
    assert row["outstanding_amount"] == "11000.00"


def test_m834c_1b_restructure_sees_repayment_pg(pg_book, monkeypatch):
    from erpclaw_lib.query import Field, P, Q, Table  # noqa: E402
    conn, env, mod = pg_book
    loans_mod = _loans_module()
    loan_id = _new_loan(conn, env, mod)

    def _plant(c):
        rs = Table("loan_repayment_schedule")
        sql = Q.update(rs).set(Field("paid_amount"), P()).set(
            Field("outstanding"), P()).set(Field("status"), P()).where(
            Field("loan_id") == P()).where(
            Field("installment_no") == P()).get_sql()
        c.execute(sql, ("1000.00", "0.00", "paid", loan_id, 1))
        t = Table("loan")
        sql2 = Q.update(t).set(Field("total_repaid"), P()).set(
            Field("outstanding_amount"), P()).set(Field("status"), P()).where(
            Field("id") == P()).get_sql()
        c.execute(sql2, ("1000.00", "11000.00", "partially_repaid", loan_id))

    _m834c_wrap_head_with_plant(monkeypatch, loans_mod, _plant)
    res = call_action(mod.loan_restructure_loan, conn, ns(
        loan_id=loan_id, new_interest_rate=None, new_repayment_periods=24))
    assert is_ok(res), res
    assert res["remaining_principal"] == "11000.00"
    assert res["installments_regenerated"] == 23
    row = conn.execute(
        "SELECT outstanding_amount FROM loan WHERE id = ?",
        (loan_id,)).fetchone()
    assert row["outstanding_amount"] == "11000.00"


def test_m834c_1c_close_stale_sqlite(conn, env, mod, db_path, monkeypatch):
    loans_mod = _loans_module()
    loan_id = _new_loan(conn, env, mod)
    pre_loan = _m834c_snap_loan(conn, loan_id)
    pre_sched = _m834c_snap_sched(conn, loan_id)

    def _plant(c):
        _m834c_pypika_set_loan_status(c, loan_id, "written_off")

    _m834c_wrap_head_with_plant(monkeypatch, loans_mod, _plant)
    res = call_action(mod.loan_close_loan, conn, ns(loan_id=loan_id))
    assert is_error(res), res
    assert _msg(res) == (
        "Cannot close loan in status 'written_off'. "
        "Must be disbursed, partially_repaid, or repaid.")
    fresh = get_conn(db_path)
    try:
        post_loan = _m834c_snap_loan(fresh, loan_id)
        assert post_loan["status"] == "written_off"
        for key, val in pre_loan.items():
            if key == "status":
                continue
            assert post_loan[key] == val, key
        assert _m834c_snap_sched(fresh, loan_id) == pre_sched
    finally:
        fresh.close()


def test_m834c_1c_close_stale_pg(pg_book, monkeypatch):
    from erpclaw_lib.db import get_connection as _get_connection  # noqa: E402
    conn, env, mod = pg_book
    loans_mod = _loans_module()
    loan_id = _new_loan(conn, env, mod)
    pre_loan = _m834c_snap_loan(conn, loan_id)
    pre_sched = _m834c_snap_sched(conn, loan_id)

    def _plant(c):
        _m834c_pypika_set_loan_status(c, loan_id, "written_off")

    _m834c_wrap_head_with_plant(monkeypatch, loans_mod, _plant)
    res = call_action(mod.loan_close_loan, conn, ns(loan_id=loan_id))
    assert is_error(res), res
    assert _msg(res) == (
        "Cannot close loan in status 'written_off'. "
        "Must be disbursed, partially_repaid, or repaid.")
    fresh = _get_connection()
    try:
        post_loan = _m834c_snap_loan(fresh, loan_id)
        assert post_loan["status"] == "written_off"
        for key, val in pre_loan.items():
            if key == "status":
                continue
            assert post_loan[key] == val, key
        assert _m834c_snap_sched(fresh, loan_id) == pre_sched
    finally:
        fresh.close()


# --- 2. head before the first write, per action ----------------------------

def test_m834c_2a_generate_head_first(conn, env, mod):
    loan_id = _new_loan(conn, env, mod)
    proxy = _RecordingProxy(conn)
    res = call_action(
        mod.loan_generate_repayment_schedule, proxy, ns(loan_id=loan_id))
    assert is_ok(res), res
    _assert_head_before_first_write(proxy)


def test_m834c_2b_restructure_head_first(conn, env, mod):
    loan_id = _new_loan(conn, env, mod)
    proxy = _RecordingProxy(conn)
    res = call_action(mod.loan_restructure_loan, proxy, ns(
        loan_id=loan_id, new_interest_rate=None, new_repayment_periods=24))
    assert is_ok(res), res
    _assert_head_before_first_write(proxy)


def test_m834c_2c_close_head_first(conn, env, mod):
    loan_id = _new_loan(conn, env, mod)
    _m834c_repay_all(conn, mod, loan_id)
    proxy = _RecordingProxy(conn)
    res = call_action(mod.loan_close_loan, proxy, ns(loan_id=loan_id))
    assert is_ok(res), res
    _assert_head_before_first_write(proxy)


# --- 3. compare-and-set miss, per action -----------------------------------

def test_m834c_3a_generate_cas_miss(conn, env, mod, db_path, monkeypatch):
    loans_mod = _loans_module()
    loan_id = _new_loan(conn, env, mod)
    pre_loan = _m834c_snap_loan(conn, loan_id)
    pre_sched = _m834c_snap_sched(conn, loan_id)
    real = loans_mod.update_row
    state = {"n": 0}

    def _wrap(*args, **kwargs):
        if state["n"] == 0:
            state["n"] += 1
            conn.execute(
                "UPDATE loan SET outstanding_amount = '11999.00' "
                "WHERE id = ?", (loan_id,))
        return real(*args, **kwargs)

    monkeypatch.setattr(loans_mod, "update_row", _wrap)
    res = call_action(
        mod.loan_generate_repayment_schedule, conn, ns(loan_id=loan_id))
    assert is_error(res), res
    assert _msg(res) == (
        f"Loan {loan_id} changed while this action was running; "
        "nothing was written. Retry the action.")
    fresh = get_conn(db_path)
    try:
        assert _m834c_snap_loan(fresh, loan_id) == pre_loan
        assert _m834c_snap_sched(fresh, loan_id) == pre_sched
    finally:
        fresh.close()


def test_m834c_3b_restructure_cas_miss(conn, env, mod, db_path, monkeypatch):
    loans_mod = _loans_module()
    loan_id = _new_loan(conn, env, mod)
    pre_loan = _m834c_snap_loan(conn, loan_id)
    pre_sched = _m834c_snap_sched(conn, loan_id)
    real = loans_mod.dynamic_update
    state = {"n": 0}

    def _wrap(table, data, where):
        if state["n"] == 0:
            state["n"] += 1
            conn.execute(
                "UPDATE loan SET outstanding_amount = '11999.00' "
                "WHERE id = ?", (loan_id,))
        return real(table, data, where)

    monkeypatch.setattr(loans_mod, "dynamic_update", _wrap)
    res = call_action(mod.loan_restructure_loan, conn, ns(
        loan_id=loan_id, new_interest_rate=None, new_repayment_periods=24))
    assert is_error(res), res
    assert _msg(res) == (
        f"Loan {loan_id} changed while this action was running; "
        "nothing was written. Retry the action.")
    fresh = get_conn(db_path)
    try:
        assert _m834c_snap_loan(fresh, loan_id) == pre_loan
        assert _m834c_snap_sched(fresh, loan_id) == pre_sched
    finally:
        fresh.close()


def test_m834c_3c_close_cas_miss(conn, env, mod, db_path, monkeypatch):
    loans_mod = _loans_module()
    loan_id = _new_loan(conn, env, mod)
    _m834c_repay_all(conn, mod, loan_id)
    pre_loan = _m834c_snap_loan(conn, loan_id)
    pre_sched = _m834c_snap_sched(conn, loan_id)
    real = loans_mod.update_row
    state = {"n": 0}

    def _wrap(*args, **kwargs):
        if state["n"] == 0:
            state["n"] += 1
            conn.execute(
                "UPDATE loan SET outstanding_amount = '11999.00' "
                "WHERE id = ?", (loan_id,))
        return real(*args, **kwargs)

    monkeypatch.setattr(loans_mod, "update_row", _wrap)
    res = call_action(mod.loan_close_loan, conn, ns(loan_id=loan_id))
    assert is_error(res), res
    assert _msg(res) == (
        f"Loan {loan_id} changed while this action was running; "
        "nothing was written. Retry the action.")
    fresh = get_conn(db_path)
    try:
        assert _m834c_snap_loan(fresh, loan_id) == pre_loan
        assert _m834c_snap_sched(fresh, loan_id) == pre_sched
    finally:
        fresh.close()


# --- 4. repayment miss rule (PostgreSQL leg) --------------------------------
# SQLite note: this case is unreachable through head-taking writers on SQLite,
# where the plant would run on the action's own connection and the miss path's
# conn.rollback() would undo it, so only the PostgreSQL leg below bites.

def test_m834c_4_repayment_changed_while_pg(pg_book, monkeypatch):
    """Accepted-status CAS miss reports changed-while, not the status message.

    SQLite note: unreachable through head-taking writers on SQLite (the plant
    runs on the action's own connection and the miss path's rollback undoes
    it); PostgreSQL-only by design.
    """
    from erpclaw_lib.db import get_connection as _get_connection  # noqa: E402
    conn, env, mod = pg_book
    repayments = _repayments_module()
    loan_id = _new_loan(conn, env, mod)
    real = repayments.dynamic_update
    state = {"n": 0}

    def _wrap(table, data, where):
        if state["n"] == 0:
            state["n"] += 1
            other = _get_connection()
            try:
                other.execute(
                    "UPDATE loan SET status = 'partially_repaid' WHERE id = ?",
                    (loan_id,))
                other.commit()
            finally:
                other.close()
        return real(table, data, where)

    monkeypatch.setattr(repayments, "dynamic_update", _wrap)
    res = _lgv._repay(conn, mod, loan_id, principal="1000.00")
    assert is_error(res), res
    assert _msg(res) == (
        f"Loan {loan_id} changed while this repayment was being recorded; "
        "nothing was written. Retry the action.")


# --- 5. write-off with no loan account --------------------------------------

def test_m834c_5_writeoff_without_loan_account(conn, env, mod, db_path):
    from erpclaw_lib.query import dynamic_update as _dyn  # noqa: E402
    loan_id = _new_loan(conn, env, mod)
    conn.execute(
        "UPDATE gl_chain_head SET updated_at = ? WHERE company_id = ?",
        ("2000-01-01 00:00:00", env["company_id"]))
    conn.commit()
    pre_heads = [dict(r) for r in conn.execute(
        "SELECT * FROM gl_chain_head ORDER BY company_id").fetchall()]
    sql, params = _dyn("loan", {"loan_account_id": None}, {"id": loan_id})
    conn.execute(sql, params)
    conn.commit()
    res = call_action(mod.loan_write_off_loan, conn, ns(
        loan_id=loan_id, bad_debt_account_id=env["bad_debt_account_id"],
        reason="no account", write_off_date="2026-09-30"))
    assert is_error(res), res
    assert _msg(res) == (
        f"Cannot write off loan {loan_id}: it has no loan account "
        "to post the write-off to.")
    row = conn.execute(
        "SELECT status FROM loan WHERE id = ?", (loan_id,)).fetchone()
    assert row["status"] == "disbursed"
    assert conn.execute(
        "SELECT COUNT(*) FROM loan_write_off").fetchone()[0] == 0
    fresh = get_conn(db_path)
    try:
        post_heads = [dict(r) for r in fresh.execute(
            "SELECT * FROM gl_chain_head ORDER BY company_id").fetchall()]
        assert post_heads == pre_heads
        head = fresh.execute(
            "SELECT updated_at FROM gl_chain_head WHERE company_id = ?",
            (env["company_id"],)).fetchone()
        assert head["updated_at"] == "2000-01-01 00:00:00"
    finally:
        fresh.close()


# --- 6. no lost repayment under restructure (PostgreSQL only) ---------------

def test_m834c_6_no_lost_repayment_under_restructure_pg(pg_book):
    conn, _env0, mod = pg_book
    spawn_gaps = []
    for _ in range(5):
        env = _build_env_pg(conn)
        app_id = _lgv._approved_application(
            conn, env, mod, amount="12000", rate="0", periods=12)
        loan_id = _lgv._disburse(conn, env, mod, app_id)["loan_id"]
        company_id = env["company_id"]
        penv = _proc_env()
        repay_args = [sys.executable, _LOANS_SCRIPT,
                      "--action", "loan-record-repayment",
                      "--loan-id", loan_id,
                      "--principal-amount", "1000.00",
                      "--repayment-date", "2026-07-01"]
        restructure_args = [sys.executable, _LOANS_SCRIPT,
                            "--action", "loan-restructure-loan",
                            "--loan-id", loan_id,
                            "--new-repayment-periods", "24"]
        t0 = time.time()
        p1 = subprocess.Popen(
            repay_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=penv)
        p2 = subprocess.Popen(
            restructure_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=penv)
        t2 = time.time()
        spawn_gaps.append(t2 - t0)
        try:
            out1, err1 = p1.communicate(timeout=15)
            out2, err2 = p2.communicate(timeout=15)
        finally:
            for p in (p1, p2):
                if p.poll() is None:
                    p.kill()
        assert p1.returncode == 0, (out1, err1)
        assert p2.returncode == 0, (out2, err2)
        assert "deadlock" not in (out1 + err1).lower(), (out1, err1)
        assert "deadlock" not in (out2 + err2).lower(), (out2, err2)
        loan = conn.execute(
            "SELECT outstanding_amount, total_repaid FROM loan WHERE id = ?",
            (loan_id,)).fetchone()
        assert loan["outstanding_amount"] == "11000.00"
        assert D(loan["total_repaid"]) == D("1000.00")
        assert _paid_sum(conn, loan_id) == D("1000.00")
        open_sum = sum((D(r[0]) for r in conn.execute(
            "SELECT outstanding FROM loan_repayment_schedule WHERE loan_id = ? "
            "AND status IN ('pending', 'overdue', 'partially_paid')",
            (loan_id,)).fetchall()), D("0"))
        assert open_sum == D("11000.00")
        rep_ids = [r[0] for r in conn.execute(
            "SELECT id FROM loan_repayment WHERE loan_id = ?",
            (loan_id,)).fetchall()]
        assert len(rep_ids) == 1
        grows = conn.execute(
            "SELECT debit, credit FROM gl_entry WHERE voucher_id = ? "
            "AND is_cancelled = 0", (rep_ids[0],)).fetchall()
        assert len(grows) == 2
        assert D(grows[0]["debit"]) + D(grows[1]["debit"]) == D("1000.00")
        assert D(grows[0]["credit"]) + D(grows[1]["credit"]) == D("1000.00")
        assert max(D(grows[0]["debit"]), D(grows[1]["debit"])) == D("1000.00")
        assert max(D(grows[0]["credit"]), D(grows[1]["credit"])) == D("1000.00")
        _assert_chain_intact(conn, company_id)
        _assert_contiguous(conn, company_id)
    assert len(spawn_gaps) == 5, f"spawn_gaps={spawn_gaps}"


# --- 8. write-off GL failure -------------------------------------------------

def test_m834c_8_writeoff_gl_failure_rolls_back(conn, env, mod, monkeypatch):
    repayments = _repayments_module()
    loan_id = _new_loan(conn, env, mod)

    def _boom(*args, **kwargs):
        raise ValueError("planted")

    monkeypatch.setattr(repayments, "insert_gl_entries", _boom)
    res = call_action(mod.loan_write_off_loan, conn, ns(
        loan_id=loan_id, bad_debt_account_id=env["bad_debt_account_id"],
        reason="gl boom", write_off_date="2026-09-30"))
    assert is_error(res), res
    assert _msg(res) == "GL posting failed, write-off rolled back: planted"
    assert conn.execute(
        "SELECT COUNT(*) FROM loan_write_off").fetchone()[0] == 0
    row = conn.execute(
        "SELECT status, outstanding_amount FROM loan WHERE id = ?",
        (loan_id,)).fetchone()
    assert row["status"] == "disbursed"
    assert D(row["outstanding_amount"]) == D("12000.00")
