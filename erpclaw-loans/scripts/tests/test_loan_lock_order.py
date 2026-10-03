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
import os
import subprocess
import sys
import time
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
if _PAY_TESTS not in sys.path:
    sys.path.insert(0, _PAY_TESTS)
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
_pay_helpers_spec = importlib.util.spec_from_file_location(
    "payments_helpers_pg", os.path.join(_PAY_TESTS, "payments_helpers.py"))
_pay_helpers = importlib.util.module_from_spec(_pay_helpers_spec)
_pay_helpers_spec.loader.exec_module(_pay_helpers)

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
    try:
        try:
            _pay_helpers.init_all_tables(None)
        except Exception:
            pytest.skip("PostgreSQL foundation build failed")
        try:
            _loans_init.create_loans_tables(
                os.environ["ERPCLAW_PG_TEST_URL"])
        except Exception:
            pytest.skip("PostgreSQL loans-table build failed")
        conn = _get_connection()
        try:
            env = _build_env_pg(conn)
        except Exception:
            pytest.skip("PostgreSQL book build failed")
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
    holder.close()


def test_5_no_lost_repayment_pg(pg_book):
    conn, env, mod = pg_book
    loan_id = _new_loan(conn, env, mod)
    company_id = env["company_id"]
    for _ in range(5):
        before = conn.execute(
            "SELECT total_repaid, outstanding_amount FROM loan WHERE id = ?",
            (loan_id,)).fetchone()
        sched_before = _paid_sum(conn, loan_id)
        rep_before = conn.execute(
            "SELECT COUNT(*) FROM loan_repayment WHERE loan_id = ?",
            (loan_id,)).fetchone()[0]
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
        t1 = time.time()
        p2 = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=penv)
        t2 = time.time()
        assert (t2 - t1) <= 0.05, "both repayments start within 50 ms"
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
        assert D(after["total_repaid"]) == D(before["total_repaid"]) + D("2000.00")
        assert D(after["outstanding_amount"]) == D(before["outstanding_amount"]) - D("2000.00")
        assert _paid_sum(conn, loan_id) == sched_before + D("2000.00")
        assert conn.execute(
            "SELECT COUNT(*) FROM loan_repayment WHERE loan_id = ?",
            (loan_id,)).fetchone()[0] == rep_before + 2
        _assert_chain_intact(conn, company_id)
        _assert_contiguous(conn, company_id)


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
