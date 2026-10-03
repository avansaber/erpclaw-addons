"""pos-abandon-posting: abandon a sale whose posting cannot finish."""
import json
import os
import sys
from datetime import datetime

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest

from pos_helpers import (
    SRC_DIR, build_env, call_action, is_error, is_ok, load_db_query, ns,
    seed_item, seed_till_accounts,
)
from erpclaw_lib import cross_skill
from erpclaw_lib.query import Q, P, Table, Field

MOD = load_db_query()
A = MOD.ACTIONS


class _Clock(datetime):
    frozen = datetime(2026, 3, 10, 12, 0, 0)

    @classmethod
    def now(cls, tz=None):
        return cls.frozen


@pytest.fixture(autouse=True)
def _frozen_clock(monkeypatch):
    monkeypatch.setitem(
        A["pos-submit-transaction"].__globals__, "datetime", _Clock)


class _Recorder:
    def __init__(self, monkeypatch, fail_on=None):
        self.calls = []
        self.successes = []
        self.fail_on = fail_on
        self._failed = set()
        real = cross_skill.call_skill_action

        def _wrap(skill, action, args=None, db_path=None, timeout=30):
            self.calls.append(action)
            if action == self.fail_on and action not in self._failed:
                self._failed.add(action)
                raise cross_skill.CrossSkillError("injected")
            result = real(skill, action, args=args, db_path=db_path,
                          timeout=timeout)
            self.successes.append(action)
            return result

        monkeypatch.setattr(cross_skill, "call_skill_action", _wrap)


def _new_txn(conn, session_id, customer_id):
    r = call_action(A["pos-add-transaction"], conn, ns(
        pos_session_id=session_id, customer_id=customer_id,
        customer_name="Walk-in"))
    assert is_ok(r), r
    return r["id"]


def _add_line(conn, txn_id, item_id, qty="2", rate="10.00"):
    r = call_action(A["pos-add-transaction-item"], conn, ns(
        pos_transaction_id=txn_id, item_id=item_id, item_name=None,
        qty=qty, rate=rate, uom=None, barcode=None, discount_pct=None))
    assert is_ok(r), r
    return r


def _pay(conn, txn_id, method="cash", amount="20.00"):
    r = call_action(A["pos-add-payment"], conn, ns(
        pos_transaction_id=txn_id, payment_method=method, amount=amount,
        reference=None))
    assert is_ok(r), r
    return r


def _submit(conn, txn_id):
    return call_action(A["pos-submit-transaction"], conn, ns(
        pos_transaction_id=txn_id))


def _abandon(conn, txn_id):
    return call_action(A["pos-abandon-posting"], conn, ns(
        pos_transaction_id=txn_id))


def _txn_row(conn, txn_id):
    return conn.execute(
        Q.from_(Table("pos_transaction")).select(Table("pos_transaction").star)
        .where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()


def _pos_payments(conn, txn_id):
    return conn.execute(
        Q.from_(Table("pos_payment")).select(Table("pos_payment").star)
        .where(Field("pos_transaction_id") == P()).get_sql(), (txn_id,)).fetchall()


def _invoice(conn, inv_id):
    return conn.execute(
        Q.from_(Table("sales_invoice")).select(Table("sales_invoice").star)
        .where(Field("id") == P()).get_sql(), (inv_id,)).fetchone()


def _payment(conn, pe_id):
    return conn.execute(
        Q.from_(Table("payment_entry")).select(Table("payment_entry").star)
        .where(Field("id") == P()).get_sql(), (pe_id,)).fetchone()


def _pos_audits(conn, txn_id, action=None):
    if action is None:
        return conn.execute(
            "SELECT * FROM audit_log WHERE skill=? AND entity_type=? "
            "AND entity_id=? ORDER BY rowid",
            ("erpclaw-pos", "pos_transaction", txn_id)).fetchall()
    return conn.execute(
        "SELECT * FROM audit_log WHERE skill=? AND entity_type=? "
        "AND entity_id=? AND action=? ORDER BY rowid",
        ("erpclaw-pos", "pos_transaction", txn_id, action)).fetchall()


def _ar_account(conn, company_id):
    return conn.execute(
        Q.from_(Table("company")).select(Field("default_receivable_account_id"))
        .where(Field("id") == P()).get_sql(), (company_id,)).fetchone()["default_receivable_account_id"]


def _cash_account(conn, company_id):
    return conn.execute(
        Q.from_(Table("company")).select(Field("default_cash_account_id"))
        .where(Field("id") == P()).get_sql(), (company_id,)).fetchone()["default_cash_account_id"]


def _snapshot(conn, txn_id, inv_id=None, pe_ids=()):
    snap = {}
    snap["txn"] = [tuple(r) for r in conn.execute(
        Q.from_(Table("pos_transaction")).select(Table("pos_transaction").star)
        .where(Field("id") == P()).get_sql(), (txn_id,)).fetchall()]
    snap["pay"] = sorted(tuple(r) for r in _pos_payments(conn, txn_id))
    if inv_id:
        snap["inv"] = [tuple(r) for r in conn.execute(
            Q.from_(Table("sales_invoice")).select(Table("sales_invoice").star)
            .where(Field("id") == P()).get_sql(), (inv_id,)).fetchall()]
    for pe in pe_ids:
        snap[f"pe:{pe}"] = [tuple(r) for r in conn.execute(
            Q.from_(Table("payment_entry")).select(Table("payment_entry").star)
            .where(Field("id") == P()).get_sql(), (pe,)).fetchall()]
    snap["gl"] = conn.execute(Q.from_(Table("gl_entry")).select(Field("id")).get_sql()).fetchall().__len__()
    snap["audit"] = [tuple(r) for r in conn.execute(
        Q.from_(Table("audit_log")).select(Table("audit_log").star).get_sql()).fetchall()]
    return snap


def _stop_after_draft_invoice(conn, env, widget, monkeypatch):
    txn = _new_txn(conn, env["session_id"], env["customer_id"])
    _add_line(conn, txn, widget)
    _pay(conn, txn, "cash", "20.00")
    conn.commit()
    rec = _Recorder(monkeypatch, fail_on="submit-sales-invoice")
    r = _submit(conn, txn)
    assert is_error(r), r
    inv_id = _txn_row(conn, txn)["sales_invoice_id"]
    assert inv_id
    assert _invoice(conn, inv_id)["status"] == "draft"
    return txn, inv_id, rec


def test_abandon_after_draft_invoice(conn, env, mod, selling_bridge, monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn, inv_id, _rec = _stop_after_draft_invoice(conn, env, widget, monkeypatch)
    r = _abandon(conn, txn)
    assert is_ok(r), r
    assert r["id"] == txn
    assert r["transaction_status"] == "draft"
    assert r["deleted_sales_invoice_id"] == inv_id
    assert r["deleted_payment_entry_ids"] == []
    assert _invoice(conn, inv_id) is None
    row = _txn_row(conn, txn)
    assert row["sales_invoice_id"] is None
    assert row["status"] == "draft"
    audits = _pos_audits(conn, txn, "pos-abandon-posting")
    assert len(audits) == 1
    assert json.loads(audits[0]["old_values"]) == {"sales_invoice_id": inv_id}
    v = call_action(A["pos-void-transaction"], conn, ns(pos_transaction_id=txn))
    assert is_ok(v), v
    c = call_action(A["pos-close-session"], conn, ns(
        id=env["session_id"], closing_amount="100.00"))
    assert is_ok(c), c


def test_abandon_with_draft_payment(conn, env, mod, selling_bridge, monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn, inv_id, _rec = _stop_after_draft_invoice(conn, env, widget, monkeypatch)
    till = {"cash": _cash_account(conn, env["company_id"])}
    made = cross_skill.call_skill_action(
        "erpclaw", "add-payment",
        {"--payment-type": "receive", "--party-type": "customer",
         "--party-id": env["customer_id"], "--company-id": env["company_id"],
         "--posting-date": "2026-03-10",
         "--paid-from-account": _ar_account(conn, env["company_id"]),
         "--paid-to-account": till["cash"],
         "--paid-amount": "20.00",
         "--allocations": json.dumps([{"voucher_type": "sales_invoice",
                                       "voucher_id": inv_id,
                                       "allocated_amount": "20.00"}])},
        db_path=None)
    pe_id = made["payment_entry_id"]
    pays = _pos_payments(conn, txn)
    assert len(pays) == 1
    conn.execute(
        Q.update(Table("pos_payment")).set("payment_entry_id", P())
        .where(Field("id") == P()).get_sql(), (pe_id, pays[0]["id"]))
    conn.commit()
    r = _abandon(conn, txn)
    assert is_ok(r), r
    assert r["deleted_sales_invoice_id"] == inv_id
    assert r["deleted_payment_entry_ids"] == [pe_id]
    assert _invoice(conn, inv_id) is None
    assert _payment(conn, pe_id) is None
    assert _txn_row(conn, txn)["sales_invoice_id"] is None
    assert [p["payment_entry_id"] for p in _pos_payments(conn, txn)] == [None]
    audits = _pos_audits(conn, txn, "pos-abandon-posting")
    assert len(audits) == 2
    olds = [json.loads(a["old_values"]) for a in audits]
    assert {"payment_entry_id": pe_id} in olds
    assert {"sales_invoice_id": inv_id} in olds
    r2 = _abandon(conn, txn)
    assert is_error(r2)
    assert r2["message"] == f"Transaction {txn} has no posting to abandon"


def test_abandon_refuses_submitted_invoice(conn, env, mod, selling_bridge, monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn = _new_txn(conn, env["session_id"], env["customer_id"])
    _add_line(conn, txn, widget)
    _pay(conn, txn, "cash", "20.00")
    conn.commit()
    rec = _Recorder(monkeypatch, fail_on="add-payment")
    r1 = _submit(conn, txn)
    assert is_error(r1), r1
    inv_id = _txn_row(conn, txn)["sales_invoice_id"]
    assert _invoice(conn, inv_id)["status"] == "submitted"
    pe_ids = [p["payment_entry_id"] for p in _pos_payments(conn, txn) if p["payment_entry_id"]]
    before = _snapshot(conn, txn, inv_id, pe_ids)
    r = _abandon(conn, txn)
    assert is_error(r)
    inv = _invoice(conn, inv_id)
    assert r["message"] == (
        f"Transaction {txn} cannot be abandoned: sales invoice {inv_id} "
        f"is '{inv['status']}'; finish with pos-submit-transaction")
    assert _snapshot(conn, txn, inv_id, pe_ids) == before


def test_abandon_refuses_submitted_payment(conn, env, mod, selling_bridge, monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn, inv_id, _rec = _stop_after_draft_invoice(conn, env, widget, monkeypatch)
    made = cross_skill.call_skill_action(
        "erpclaw", "add-payment",
        {"--payment-type": "receive", "--party-type": "customer",
         "--party-id": env["customer_id"], "--company-id": env["company_id"],
         "--posting-date": "2026-03-10",
         "--paid-from-account": conn.execute(
             Q.from_(Table("company")).select(Field("default_receivable_account_id"))
             .where(Field("id") == P()).get_sql(), (env["company_id"],)).fetchone()["default_receivable_account_id"],
         "--paid-to-account": conn.execute(
             Q.from_(Table("company")).select(Field("default_cash_account_id"))
             .where(Field("id") == P()).get_sql(), (env["company_id"],)).fetchone()["default_cash_account_id"],
         "--paid-amount": "5.00"},
        db_path=None)
    pe_id = made["payment_entry_id"]
    cross_skill.call_skill_action(
        "erpclaw", "submit-payment",
        {"--payment-entry-id": pe_id, "--user-confirmed": None}, db_path=None)
    conn.execute(
        "INSERT INTO payment_allocation (id, payment_entry_id, voucher_type, voucher_id, allocated_amount) "
        "VALUES (?, ?, 'sales_invoice', ?, '5.00')",
        (f"alloc-{pe_id[:8]}", pe_id, inv_id))
    conn.commit()
    pays = _pos_payments(conn, txn)
    conn.execute(
        Q.update(Table("pos_payment")).set("payment_entry_id", P())
        .where(Field("id") == P()).get_sql(), (pe_id, pays[0]["id"]))
    conn.commit()
    before = _snapshot(conn, txn, inv_id, [pe_id])
    r = _abandon(conn, txn)
    assert is_error(r)
    assert r["message"] == (
        f"Transaction {txn} cannot be abandoned: payment {pe_id} "
        f"is 'submitted'; finish with pos-submit-transaction")
    assert _snapshot(conn, txn, inv_id, [pe_id]) == before


def test_abandon_refuses_bad_states(conn, env, mod, selling_bridge, monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn = _new_txn(conn, env["session_id"], env["customer_id"])
    _add_line(conn, txn, widget)
    _pay(conn, txn, "cash", "20.00")
    conn.commit()
    r = _submit(conn, txn)
    assert is_ok(r), r
    r2 = _abandon(conn, txn)
    assert is_error(r2)
    assert r2["message"] == f"Transaction {txn} has no posting to abandon"
    txn2 = _new_txn(conn, env["session_id"], env["customer_id"])
    _add_line(conn, txn2, widget)
    conn.commit()
    r3 = _abandon(conn, txn2)
    assert is_error(r3)
    assert r3["message"] == f"Transaction {txn2} has no posting to abandon"
    r4 = call_action(A["pos-abandon-posting"], conn, ns(pos_transaction_id=None))
    assert is_error(r4)
    assert r4["message"] == "--pos-transaction-id is required"
    r5 = call_action(A["pos-abandon-posting"], conn, ns(pos_transaction_id="no-such"))
    assert is_error(r5)
    assert r5["message"] == "Transaction no-such not found"


def test_submit_chain_writes_intermediate_audits(conn, env, mod, selling_bridge):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn = _new_txn(conn, env["session_id"], env["customer_id"])
    _add_line(conn, txn, widget)
    _pay(conn, txn, "cash", "10.00")
    _pay(conn, txn, "card", "10.00")
    conn.commit()
    r = _submit(conn, txn)
    assert is_ok(r), r
    inv_id = r["sales_invoice_id"]
    audits = _pos_audits(conn, txn, "pos-submit-transaction")
    assert len(audits) == 4, [json.loads(a["new_values"] or "{}") for a in audits]
    first = json.loads(audits[0]["new_values"])
    assert first == {"sales_invoice_id": inv_id}
    pays = {p["payment_method"]: p for p in _pos_payments(conn, txn)}
    second = json.loads(audits[1]["new_values"])
    third = json.loads(audits[2]["new_values"])
    assert second == {"payment_entry_id": pays["cash"]["payment_entry_id"],
                      "payment_method": "cash"}
    assert third == {"payment_entry_id": pays["card"]["payment_entry_id"],
                     "payment_method": "card"}
    last = json.loads(audits[3]["new_values"])
    assert last["sales_invoice_id"] == inv_id
    assert last["payment_entry_ids"] == [pays["cash"]["payment_entry_id"],
                                         pays["card"]["payment_entry_id"]]


def test_abandon_stopped_then_retried(conn, env, mod, selling_bridge, monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn, inv_id, _rec = _stop_after_draft_invoice(conn, env, widget, monkeypatch)
    till_cash = _cash_account(conn, env["company_id"])
    made = cross_skill.call_skill_action(
        "erpclaw", "add-payment",
        {"--payment-type": "receive", "--party-type": "customer",
         "--party-id": env["customer_id"], "--company-id": env["company_id"],
         "--posting-date": "2026-03-10",
         "--paid-from-account": _ar_account(conn, env["company_id"]),
         "--paid-to-account": till_cash,
         "--paid-amount": "20.00",
         "--allocations": json.dumps([{"voucher_type": "sales_invoice",
                                       "voucher_id": inv_id,
                                       "allocated_amount": "20.00"}])},
        db_path=None)
    pe_id = made["payment_entry_id"]
    pays = _pos_payments(conn, txn)
    conn.execute(
        Q.update(Table("pos_payment")).set("payment_entry_id", P())
        .where(Field("id") == P()).get_sql(), (pe_id, pays[0]["id"]))
    conn.commit()

    real = cross_skill.call_skill_action
    calls = []

    def _flaky(skill, action, args=None, db_path=None, timeout=30):
        calls.append(action)
        if action == "delete-sales-invoice":
            raise cross_skill.CrossSkillError("injected sell failure")
        return real(skill, action, args=args, db_path=db_path, timeout=timeout)

    monkeypatch.setattr(cross_skill, "call_skill_action", _flaky)
    r = _abandon(conn, txn)
    assert is_error(r)
    assert r["message"] == (
        "POS abandon stopped at delete-sales-invoice: injected sell failure. "
        "Retry the same action to finish")
    assert _payment(conn, pe_id) is None
    assert [p["payment_entry_id"] for p in _pos_payments(conn, txn)] == [None]
    assert _txn_row(conn, txn)["sales_invoice_id"] == inv_id
    assert _invoice(conn, inv_id) is not None
    audits = _pos_audits(conn, txn, "pos-abandon-posting")
    assert len(audits) == 1
    assert json.loads(audits[0]["old_values"]) == {"payment_entry_id": pe_id}
    monkeypatch.setattr(cross_skill, "call_skill_action", real)
    r2 = _abandon(conn, txn)
    assert is_ok(r2), r2
    assert _invoice(conn, inv_id) is None
    assert _txn_row(conn, txn)["sales_invoice_id"] is None


def test_abandon_retry_after_delete_without_clear(conn, env, mod, selling_bridge, monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn, inv_id, _rec = _stop_after_draft_invoice(conn, env, widget, monkeypatch)
    cross_skill.call_skill_action(
        "erpclaw", "delete-sales-invoice",
        {"--sales-invoice-id": inv_id, "--user-confirmed": None}, db_path=None)
    assert _invoice(conn, inv_id) is None
    assert _txn_row(conn, txn)["sales_invoice_id"] == inv_id
    real = cross_skill.call_skill_action
    calls = []

    def _watch(skill, action, args=None, db_path=None, timeout=30):
        calls.append(action)
        return real(skill, action, args=args, db_path=db_path, timeout=timeout)

    monkeypatch.setattr(cross_skill, "call_skill_action", _watch)
    r = _abandon(conn, txn)
    assert is_ok(r), r
    assert "delete-sales-invoice" not in calls
    assert _txn_row(conn, txn)["sales_invoice_id"] is None
    audits = _pos_audits(conn, txn, "pos-abandon-posting")
    assert len(audits) == 1
    assert json.loads(audits[0]["old_values"]) == {"sales_invoice_id": inv_id}

    txn2, inv2, _r2 = _stop_after_draft_invoice(conn, env, widget, monkeypatch)
    till_cash = _cash_account(conn, env["company_id"])
    made = cross_skill.call_skill_action(
        "erpclaw", "add-payment",
        {"--payment-type": "receive", "--party-type": "customer",
         "--party-id": env["customer_id"], "--company-id": env["company_id"],
         "--posting-date": "2026-03-10",
         "--paid-from-account": _ar_account(conn, env["company_id"]),
         "--paid-to-account": till_cash,
         "--paid-amount": "20.00",
         "--allocations": json.dumps([{"voucher_type": "sales_invoice",
                                       "voucher_id": inv2,
                                       "allocated_amount": "20.00"}])},
        db_path=None)
    pe2 = made["payment_entry_id"]
    pays = _pos_payments(conn, txn2)
    conn.execute(
        Q.update(Table("pos_payment")).set("payment_entry_id", P())
        .where(Field("id") == P()).get_sql(), (pe2, pays[0]["id"]))
    conn.commit()
    cross_skill.call_skill_action(
        "erpclaw", "delete-payment",
        {"--payment-entry-id": pe2, "--user-confirmed": None}, db_path=None)
    assert _payment(conn, pe2) is None
    calls2 = []

    def _watch2(skill, action, args=None, db_path=None, timeout=30):
        calls2.append(action)
        return real(skill, action, args=args, db_path=db_path, timeout=timeout)

    monkeypatch.setattr(cross_skill, "call_skill_action", _watch2)
    r = _abandon(conn, txn2)
    assert is_ok(r), r
    assert "delete-payment" not in calls2
    cleared = [p["payment_entry_id"] for p in _pos_payments(conn, txn2)]
    assert cleared == [None]


def test_abandon_stops_on_unrecorded_draft_payment(conn, env, mod, selling_bridge, monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn, inv_id, _rec = _stop_after_draft_invoice(conn, env, widget, monkeypatch)
    till_cash = _cash_account(conn, env["company_id"])
    made = cross_skill.call_skill_action(
        "erpclaw", "add-payment",
        {"--payment-type": "receive", "--party-type": "customer",
         "--party-id": env["customer_id"], "--company-id": env["company_id"],
         "--posting-date": "2026-03-10",
         "--paid-from-account": _ar_account(conn, env["company_id"]),
         "--paid-to-account": till_cash,
         "--paid-amount": "20.00",
         "--allocations": json.dumps([{"voucher_type": "sales_invoice",
                                       "voucher_id": inv_id,
                                       "allocated_amount": "20.00"}])},
        db_path=None)
    stray = made["payment_entry_id"]
    r = _abandon(conn, txn)
    assert is_error(r)
    assert "POS abandon stopped at delete-sales-invoice: " in r["message"]
    assert stray in r["message"]
    assert "Retry the same action to finish" in r["message"]
    assert _invoice(conn, inv_id) is not None
    cross_skill.call_skill_action(
        "erpclaw", "delete-payment",
        {"--payment-entry-id": stray, "--user-confirmed": None}, db_path=None)
    r2 = _abandon(conn, txn)
    assert is_ok(r2), r2
    assert _invoice(conn, inv_id) is None
