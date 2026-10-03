"""POS sale discount and settlement ledger chain.

Proves the product rule: a submitted sale's invoice carries the
transaction discount as one negative line (invoice total == POS total),
and each tender method becomes one submitted receipt allocated to that
invoice (invoice ends paid, cash net of change). Steps are resumable and
idempotent: a retry reads recorded document ids, skips finished steps and
never creates a second invoice or payment.
"""
import importlib.util
import io
import json
import os
import sys
import uuid
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from unittest.mock import patch

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import pytest  # noqa: E402

from pos_helpers import (  # noqa: E402
    SRC_DIR, build_env, call_action, is_error, is_ok, load_db_query, ns,
    seed_company, seed_cost_center, seed_customer, seed_fiscal_year,
    seed_item, seed_naming_series, seed_open_session, seed_pos_profile,
    seed_return_document, seed_selling_accounts, seed_till_accounts,
)
from erpclaw_lib import cross_skill  # noqa: E402
from erpclaw_lib.party_ledger import is_live_row  # noqa: E402
from erpclaw_lib.query import Q, P, Table, Field  # noqa: E402

MOD = load_db_query()
A = MOD.ACTIONS


class _Clock(datetime):
    """Stands in for ``datetime`` inside the transactions module."""
    frozen = datetime(2026, 3, 10, 12, 0, 0)

    @classmethod
    def now(cls, tz=None):
        return cls.frozen


@pytest.fixture(autouse=True)
def _frozen_clock(monkeypatch):
    monkeypatch.setitem(
        A["pos-submit-transaction"].__globals__, "datetime", _Clock)


@pytest.fixture(autouse=True)
def _scrubbed_naming(env, conn):
    """Drop the dead legacy naming_series seeds ``build_env`` plants.

    ``get_next_name()`` manages its own ``{PREFIX}{YEAR}-`` rows; the four
    short-prefix rows are never read, and INV-10 flags their format, so the
    ledger check would fail on seed data rather than on anything the sale
    posted. Runs after ``build_env`` through the ``env`` dependency.
    """
    conn.execute(
        Q.from_(Table("naming_series")).delete()
        .where(Field("prefix").isin(["POS-", "POSS-", "PTXN-",
                                     "SINV-"])).get_sql())
    conn.commit()


def _dec(val):
    if val is None:
        return Decimal("0")
    return Decimal(str(val))


def _q2(val):
    return _dec(val).quantize(Decimal("0.01"), ROUND_HALF_UP)


def _net(conn, account_id):
    """Σdebit − Σcredit over live gl_entry rows, quantized, as text."""
    g = Table("gl_entry")
    rows = conn.execute(
        Q.from_(g).select(g.debit, g.credit)
        .where(g.account_id == P())
        .where(g.is_cancelled == 0).get_sql(),
        (account_id,)).fetchall()
    return str(_q2(sum((_dec(r["debit"]) - _dec(r["credit"])
                        for r in rows), Decimal("0"))))


def _legs(conn, voucher_type, voucher_id):
    """Sorted (account_id, debit, credit, is_cancelled) legs of a voucher."""
    g = Table("gl_entry")
    rows = conn.execute(
        Q.from_(g).select(g.account_id, g.debit, g.credit, g.is_cancelled)
        .where(g.voucher_type == P())
        .where(g.voucher_id == P()).get_sql(),
        (voucher_type, voucher_id)).fetchall()
    return sorted((r["account_id"], r["debit"], r["credit"],
                   r["is_cancelled"]) for r in rows)


def _assert_invariants(conn):
    path = os.path.join(os.path.dirname(SRC_DIR), "testing",
                        "invariant_engine.py")
    assert os.path.exists(path), f"invariant engine absent: {path}"
    spec = importlib.util.spec_from_file_location(
        "invariant_engine_pos_chain", path)
    engine = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(engine)
    bad = [o for o in engine.evaluate_invariants(conn) if o.is_failure]
    assert not bad, [(o.inv_id, o.status, o.detail) for o in bad]


class _Recorder:
    """Wraps cross_skill.call_skill_action: counts actions, optionally fails one."""

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


def _company_accounts(conn, company_id):
    row = conn.execute(
        Q.from_(Table("company"))
        .select(Field("default_receivable_account_id"),
                Field("default_income_account_id"))
        .where(Field("id") == P()).get_sql(),
        (company_id,)).fetchone()
    return row["default_receivable_account_id"], row["default_income_account_id"]


def _new_txn(conn, session_id, customer_id):
    r = call_action(A["pos-add-transaction"], conn, ns(
        pos_session_id=session_id, customer_id=customer_id,
        customer_name="Walk-in"))
    assert is_ok(r), f"pos-add-transaction failed: {r}"
    return r["id"]


def _add_line(conn, txn_id, item_id, qty, rate, pct=None):
    r = call_action(A["pos-add-transaction-item"], conn, ns(
        pos_transaction_id=txn_id, item_id=item_id, item_name=None,
        qty=qty, rate=rate, uom=None, barcode=None, discount_pct=pct))
    assert is_ok(r), f"pos-add-transaction-item failed: {r}"
    return r


def _amount_discount(conn, txn_id, amount):
    r = call_action(A["pos-apply-discount"], conn, ns(
        pos_transaction_id=txn_id, discount_pct=None,
        discount_amount=amount))
    assert is_ok(r), f"pos-apply-discount failed: {r}"
    return r


def _pay(conn, txn_id, method, amount):
    r = call_action(A["pos-add-payment"], conn, ns(
        pos_transaction_id=txn_id, payment_method=method, amount=amount,
        reference=None))
    assert is_ok(r), f"pos-add-payment failed: {r}"
    return r


def _submit(conn, txn_id):
    return call_action(A["pos-submit-transaction"], conn, ns(
        pos_transaction_id=txn_id))


def _txn_row(conn, txn_id):
    return conn.execute(
        Q.from_(Table("pos_transaction")).select(Table("pos_transaction").star)
        .where(Field("id") == P()).get_sql(), (txn_id,)).fetchone()


def _sale_a(conn, env, widget):
    """2 x Widget with a 1.50 transaction discount, cash 20.00."""
    txn = _new_txn(conn, env["session_id"], env["customer_id"])
    _add_line(conn, txn, widget, "2", "10.00")
    _amount_discount(conn, txn, "1.50")
    _pay(conn, txn, "cash", "20.00")
    return txn


def _table_count(conn, table):
    t = Table(table)
    return conn.execute(
        Q.from_(t).select(Field("id")).get_sql()).fetchall().__len__()


def _invoice(conn, inv_id):
    return conn.execute(
        Q.from_(Table("sales_invoice")).select(Table("sales_invoice").star)
        .where(Field("id") == P()).get_sql(), (inv_id,)).fetchone()


def _invoice_lines(conn, inv_id):
    sii = Table("sales_invoice_item")
    return conn.execute(
        Q.from_(sii)
        .select(sii.item_id, sii.quantity, sii.rate, sii.net_amount)
        .where(sii.sales_invoice_id == P()).get_sql(),
        (inv_id,)).fetchall()


def _discount_item(conn, company_id):
    return conn.execute(
        Q.from_(Table("item")).select(Table("item").star)
        .where(Field("item_code") == P()).get_sql(),
        (f"POS-DISC-{company_id}",)).fetchone()


def _payment_of(conn, pe_id):
    return conn.execute(
        Q.from_(Table("payment_entry")).select(Table("payment_entry").star)
        .where(Field("id") == P()).get_sql(), (pe_id,)).fetchone()


def _pos_payments(conn, txn_id):
    return conn.execute(
        Q.from_(Table("pos_payment")).select(Table("pos_payment").star)
        .where(Field("pos_transaction_id") == P()).get_sql(),
        (txn_id,)).fetchall()


def _allocations(conn, pe_id):
    pa = Table("payment_allocation")
    return conn.execute(
        Q.from_(pa)
        .select(pa.voucher_type, pa.voucher_id, pa.allocated_amount)
        .where(pa.payment_entry_id == P()).get_sql(),
        (pe_id,)).fetchall()


# ---------------------------------------------------------------------------
# 1. discount reaches the invoice, cash settles it
# ---------------------------------------------------------------------------

def test_sale_discount_reaches_invoice_and_cash_settles(conn, env, mod,
                                                        selling_bridge):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    till = seed_till_accounts(conn, env["company_id"])
    ar, revenue = _company_accounts(conn, env["company_id"])
    txn = _sale_a(conn, env, widget)
    conn.commit()

    before = _txn_row(conn, txn)
    assert (before["subtotal"], before["discount_amount"],
            before["grand_total"]) == ("20.00", "1.50", "18.50")

    r = _submit(conn, txn)
    assert is_ok(r), f"submit failed: {r}"
    assert (r["transaction_status"], r["grand_total"],
            r["change_amount"]) == ("submitted", "18.50", "1.50")
    inv_id = r["sales_invoice_id"]
    assert r["sales_invoice_status"] == "paid"
    assert r["sales_invoice_outstanding_amount"] == "0"

    inv = _invoice(conn, inv_id)
    assert (inv["total_amount"], inv["grand_total"],
            inv["outstanding_amount"], inv["status"]) == (
        "18.50", "18.50", "0", "paid")

    disc = _discount_item(conn, env["company_id"])
    assert disc is not None
    assert disc["is_stock_item"] == 0
    assert disc["item_code"] == f"POS-DISC-{env['company_id']}"
    assert sorted(tuple(x) for x in _invoice_lines(conn, inv_id)) == sorted([
        (widget, "2.00", "10.00", "20.00"),
        (disc["id"], "1.00", "-1.50", "-1.50"),
    ])

    pays = _pos_payments(conn, txn)
    assert len(pays) == 1
    pe_id = pays[0]["payment_entry_id"]
    assert pe_id
    assert r["payment_entry_ids"] == [pe_id]
    pe = _payment_of(conn, pe_id)
    assert (pe["payment_type"], pe["paid_amount"], pe["unallocated_amount"],
            pe["status"], pe["posting_date"], pe["paid_from_account"],
            pe["paid_to_account"]) == (
        "receive", "18.50", "0.00", "submitted", "2026-03-10", ar,
        till["cash"])
    assert [tuple(x) for x in _allocations(conn, pe_id)] == [
        ("sales_invoice", inv_id, "18.50")]

    assert _net(conn, ar) == "0.00"
    assert _net(conn, revenue) == "-18.50"
    assert _net(conn, till["cash"]) == "18.50"
    assert _table_count(conn, "stock_ledger_entry") == 0
    _assert_invariants(conn)


# ---------------------------------------------------------------------------
# 2. split tender posts cash net of change
# ---------------------------------------------------------------------------

def test_split_tender_posts_cash_net_of_change(conn, env, mod, selling_bridge):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    till = seed_till_accounts(conn, env["company_id"])
    ar, _revenue = _company_accounts(conn, env["company_id"])
    txn = _new_txn(conn, env["session_id"], env["customer_id"])
    _add_line(conn, txn, widget, "2", "10.00")
    _amount_discount(conn, txn, "1.50")
    _pay(conn, txn, "cash", "10.00")
    _pay(conn, txn, "card", "10.00")
    conn.commit()

    r = _submit(conn, txn)
    assert is_ok(r), f"submit failed: {r}"
    assert r["sales_invoice_status"] == "paid"
    inv_id = r["sales_invoice_id"]
    assert _invoice(conn, inv_id)["status"] == "paid"

    pays = {p["payment_method"]: p for p in _pos_payments(conn, txn)}
    assert set(pays) == {"cash", "card"}
    cash_pe = _payment_of(conn, pays["cash"]["payment_entry_id"])
    card_pe = _payment_of(conn, pays["card"]["payment_entry_id"])
    assert (cash_pe["paid_amount"], cash_pe["paid_to_account"]) == (
        "8.50", till["cash"])
    assert (card_pe["paid_amount"], card_pe["paid_to_account"]) == (
        "10.00", till["bank"])
    assert r["payment_entry_ids"] == [pays["cash"]["payment_entry_id"],
                                      pays["card"]["payment_entry_id"]]

    assert _net(conn, till["cash"]) == "8.50"
    assert _net(conn, till["bank"]) == "10.00"
    assert _net(conn, ar) == "0.00"
    _assert_invariants(conn)


# ---------------------------------------------------------------------------
# 3. zero cash after change gets no payment
# ---------------------------------------------------------------------------

def test_zero_cash_after_change_gets_no_payment(conn, env, mod, selling_bridge):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    till = seed_till_accounts(conn, env["company_id"])
    txn = _new_txn(conn, env["session_id"], env["customer_id"])
    _add_line(conn, txn, widget, "2", "10.00")
    _amount_discount(conn, txn, "1.50")
    _pay(conn, txn, "cash", "1.50")
    _pay(conn, txn, "card", "18.50")
    conn.commit()

    r = _submit(conn, txn)
    assert is_ok(r), f"submit failed: {r}"
    assert r["sales_invoice_status"] == "paid"

    pays = {p["payment_method"]: p for p in _pos_payments(conn, txn)}
    assert set(pays) == {"cash", "card"}
    assert pays["cash"]["payment_entry_id"] is None
    card_pe = _payment_of(conn, pays["card"]["payment_entry_id"])
    assert (card_pe["paid_amount"], card_pe["paid_to_account"],
            card_pe["status"]) == ("18.50", till["bank"], "submitted")
    assert _table_count(conn, "payment_entry") == 1
    assert _net(conn, till["cash"]) == "0.00"
    _assert_invariants(conn)


# ---------------------------------------------------------------------------
# 4. line formula matches selling
# ---------------------------------------------------------------------------

def test_line_formula_matches_selling(conn, env, mod, selling_bridge):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn = _new_txn(conn, env["session_id"], env["customer_id"])
    added = _add_line(conn, txn, widget, "3", "1.15", "10")
    line = conn.execute(
        Q.from_(Table("pos_transaction_item"))
        .select(Field("amount"), Field("discount_amount"))
        .where(Field("id") == P()).get_sql(),
        (added["id"],)).fetchone()
    assert (line["amount"], line["discount_amount"]) == ("3.12", "0.33")
    _pay(conn, txn, "cash", "3.12")
    conn.commit()

    r = _submit(conn, txn)
    assert is_ok(r), f"submit failed: {r}"
    assert _invoice(conn, r["sales_invoice_id"])["grand_total"] == "3.12"
    _assert_invariants(conn)


# ---------------------------------------------------------------------------
# 5. refusals write nothing
# ---------------------------------------------------------------------------

def _pos_snapshot(conn, txn_id):
    row = _txn_row(conn, txn_id)
    return {k: row[k] for k in (
        "status", "subtotal", "discount_pct", "discount_amount",
        "tax_amount", "grand_total", "paid_amount", "change_amount",
        "sales_invoice_id")}


def _second_company_no_till(conn):
    cid = seed_company(conn, name="Second Co", abbr="SCO")
    seed_naming_series(conn, cid)
    seed_selling_accounts(conn, cid)
    seed_fiscal_year(conn, cid)
    seed_cost_center(conn, cid)
    customer_id = seed_customer(conn, cid)
    item_id = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    profile_id = seed_pos_profile(conn, cid)
    session_id = seed_open_session(conn, profile_id)
    return {"company_id": cid, "session_id": session_id,
            "item_id": item_id, "customer_id": customer_id}


@pytest.mark.parametrize("case", [
    "gift_card", "no_till", "card_overpay", "tax", "line_mismatch",
    "discount_above", "remove_under_discount",
])
def test_refusals_write_nothing(conn, env, mod, selling_bridge, monkeypatch,
                                case):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    rec = _Recorder(monkeypatch)

    if case == "no_till":
        second = _second_company_no_till(conn)
        txn = _new_txn(conn, second["session_id"], second["customer_id"])
        _add_line(conn, txn, second["item_id"], "2", "10.00")
        _pay(conn, txn, "cash", "20.00")
        conn.commit()
        snap = _pos_snapshot(conn, txn)
        r = _submit(conn, txn)
        expected = ("POS submit refused: company has no default cash "
                    "account for cash payments")
    elif case == "gift_card":
        txn = _new_txn(conn, env["session_id"], env["customer_id"])
        _add_line(conn, txn, widget, "2", "10.00")
        _pay(conn, txn, "gift_card", "20.00")
        conn.commit()
        snap = _pos_snapshot(conn, txn)
        r = _submit(conn, txn)
        expected = ("POS submit refused: no ledger account is mapped "
                    "for payment method gift_card")
    elif case == "card_overpay":
        txn = _new_txn(conn, env["session_id"], env["customer_id"])
        _add_line(conn, txn, widget, "2", "10.00")
        _amount_discount(conn, txn, "1.50")
        _pay(conn, txn, "card", "20.00")
        conn.commit()
        snap = _pos_snapshot(conn, txn)
        r = _submit(conn, txn)
        expected = ("POS submit refused: change 1.50 exceeds "
                    "cash tendered 0.00")
    elif case == "tax":
        txn = _sale_a(conn, env, widget)
        conn.commit()
        conn.execute(
            Q.update(Table("pos_transaction"))
            .set(Field("tax_amount"), P())
            .where(Field("id") == P()).get_sql(), ("0.50", txn))
        conn.commit()
        snap = _pos_snapshot(conn, txn)
        r = _submit(conn, txn)
        expected = ("POS submit refused: transaction carries tax 0.50 "
                    "but POS posts no tax")
    elif case == "line_mismatch":
        txn = _new_txn(conn, env["session_id"], env["customer_id"])
        added = _add_line(conn, txn, widget, "3", "1.15", "10")
        conn.execute(
            Q.update(Table("pos_transaction_item"))
            .set(Field("amount"), P())
            .where(Field("id") == P()).get_sql(), ("3.10", added["id"]))
        conn.commit()
        _pay(conn, txn, "cash", "3.12")
        conn.commit()
        snap = _pos_snapshot(conn, txn)
        r = _submit(conn, txn)
        expected = (f"POS submit refused: line {added['id']} amount 3.10 "
                    f"does not match 3.12; remove and re-add line {added['id']}")
    elif case == "discount_above":
        txn = _new_txn(conn, env["session_id"], env["customer_id"])
        _add_line(conn, txn, widget, "2", "10.00")
        conn.commit()
        snap = _pos_snapshot(conn, txn)
        r = call_action(A["pos-apply-discount"], conn, ns(
            pos_transaction_id=txn, discount_pct=None,
            discount_amount="25.00"))
        expected = "--discount-amount 25.00 exceeds subtotal 20.00"
    elif case == "remove_under_discount":
        txn = _new_txn(conn, env["session_id"], env["customer_id"])
        _add_line(conn, txn, widget, "1", "10.00")
        _add_line(conn, txn, widget, "1", "10.00")
        _amount_discount(conn, txn, "15.00")
        conn.commit()
        lines = conn.execute(
            Q.from_(Table("pos_transaction_item")).select(Field("id"))
            .where(Field("pos_transaction_id") == P()).get_sql(),
            (txn,)).fetchall()
        assert len(lines) == 2
        snap = _pos_snapshot(conn, txn)
        victim = lines[0]["id"]
        r = call_action(A["pos-remove-transaction-item"], conn, ns(
            pos_transaction_item_id=victim))
        expected = (f"Removing line {victim} leaves discount 15.00 above "
                    f"subtotal 10.00; lower the discount first")
        assert len(conn.execute(
            Q.from_(Table("pos_transaction_item")).select(Field("id"))
            .where(Field("pos_transaction_id") == P()).get_sql(),
            (txn,)).fetchall()) == 2
    else:
        raise AssertionError(f"unknown case {case}")

    assert is_error(r), f"{case}: expected refusal, got {r}"
    assert r["message"] == expected, f"{case}: {r['message']!r}"
    assert _pos_snapshot(conn, txn) == snap, case
    assert _table_count(conn, "sales_invoice") == 0, case
    assert _table_count(conn, "payment_entry") == 0, case
    assert _table_count(conn, "gl_entry") == 0, case
    assert rec.calls == [], case


# ---------------------------------------------------------------------------
# 6. retry finishes without duplicates
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("step", [
    "create-sales-invoice", "submit-sales-invoice", "add-payment",
    "submit-payment",
])
def test_retry_finishes_without_duplicates(conn, env, mod, selling_bridge,
                                           monkeypatch, step):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn = _sale_a(conn, env, widget)
    conn.commit()
    rec = _Recorder(monkeypatch, fail_on=step)

    r1 = _submit(conn, txn)
    assert is_error(r1), f"{step}: expected stop, got {r1}"
    inv_id = _txn_row(conn, txn)["sales_invoice_id"]
    pe_ids = [p["payment_entry_id"] for p in sorted(
        _pos_payments(conn, txn), key=lambda r: r["id"])
        if p["payment_entry_id"]]
    done = ([inv_id] if inv_id else []) + pe_ids
    done_text = ", ".join(done) if done else "none"
    assert r1["message"] == (
        f"POS submit stopped at {step}: injected. "
        f"Done so far: {done_text}. Retry the same action to finish"), step
    assert _txn_row(conn, txn)["status"] == "draft", step

    def _full_snapshot():
        rows = {"txn": _pos_snapshot(conn, txn)}
        rows["items"] = [{k: r[k] for k in r.keys()} for r in conn.execute(
            Q.from_(Table("pos_transaction_item"))
            .select(Table("pos_transaction_item").star)
            .where(Field("pos_transaction_id") == P()).get_sql(),
            (txn,)).fetchall()]
        rows["payments"] = [{k: r[k] for k in r.keys()}
                            for r in _pos_payments(conn, txn)]
        return rows

    if step == "create-sales-invoice":
        assert inv_id is None, step
        assert _table_count(conn, "sales_invoice") == 0, step
    else:
        assert inv_id, step
        snap = _full_snapshot()
        r_add = call_action(A["pos-add-transaction-item"], conn, ns(
            pos_transaction_id=txn, item_id=widget, item_name=None,
            qty="1", rate="10.00", uom=None, barcode=None,
            discount_pct=None))
        assert is_error(r_add), step
        assert r_add["message"] == (
            f"Transaction {txn} is being posted (sales invoice {inv_id}); "
            f"finish with pos-submit-transaction"), step
        r_close = call_action(A["pos-close-session"], conn, ns(
            id=env["session_id"], closing_amount="118.50"))
        assert is_error(r_close), step
        assert r_close["message"] == (
            f"Session {env['session_id']} has a POS action in progress "
            f"({txn}); finish it before closing"), step
        assert _full_snapshot() == snap, step

    r2 = _submit(conn, txn)
    assert is_ok(r2), f"{step}: retry failed: {r2}"
    assert _table_count(conn, "sales_invoice") == 1, step
    assert _table_count(conn, "payment_entry") == 1, step
    for action in ("create-sales-invoice", "submit-sales-invoice",
                   "add-payment", "submit-payment"):
        assert rec.successes.count(action) == 1, (step, action, rec.successes)
    assert _invoice(conn, r2["sales_invoice_id"])["status"] == "paid", step

    r_close = call_action(A["pos-close-session"], conn, ns(
        id=env["session_id"], closing_amount="118.50"))
    assert is_ok(r_close), f"{step}: close failed: {r_close}"
    assert r_close["difference"] == "0.00", step
    _assert_invariants(conn)


# ---------------------------------------------------------------------------
# m352b: voiding a submitted sale cancels its receipts and invoice by reversal
# ---------------------------------------------------------------------------

def _sale_b(conn, env, widget):
    """1 x Widget at 5.00, card 5.00 (grand 5.00)."""
    txn = _new_txn(conn, env["session_id"], env["customer_id"])
    _add_line(conn, txn, widget, "1", "5.00")
    _pay(conn, txn, "card", "5.00")
    return txn


def _book_two_sales(conn, env, widget):
    """Sale A (2 x 10.00, discount 1.50, cash 20.00) and Sale B, both submitted."""
    a = _sale_a(conn, env, widget)
    ra = _submit(conn, a)
    assert is_ok(ra), f"submit A failed: {ra}"
    b = _sale_b(conn, env, widget)
    rb = _submit(conn, b)
    assert is_ok(rb), f"submit B failed: {rb}"
    return a, ra, b, rb


def _snap(conn, voucher_type, voucher_id):
    """Sorted (id, account_id, debit, credit, posting_date, voucher_type,
    voucher_id) of a voucher's gl_entry rows."""
    g = Table("gl_entry")
    rows = conn.execute(
        Q.from_(g).select(g.id, g.account_id, g.debit, g.credit,
                          g.posting_date, g.voucher_type, g.voucher_id)
        .where(g.voucher_type == P())
        .where(g.voucher_id == P()).get_sql(),
        (voucher_type, voucher_id)).fetchall()
    return sorted((r["id"], r["account_id"], r["debit"], r["credit"],
                   r["posting_date"], r["voucher_type"], r["voucher_id"])
                  for r in rows)


def _gl_count(conn):
    return _table_count(conn, "gl_entry")


def _void(conn, txn_id):
    return call_action(A["pos-void-transaction"], conn, ns(
        pos_transaction_id=txn_id))


def _close(conn, session_id, amount):
    return call_action(A["pos-close-session"], conn, ns(
        id=session_id, closing_amount=amount))


def _void_audit(conn, txn_id):
    return conn.execute(
        Q.from_(Table("audit_log")).select(Table("audit_log").star)
        .where(Field("action") == P())
        .where(Field("entity_id") == P()).get_sql(),
        ("pos-void-transaction", txn_id)).fetchall()


def test_void_reverses_settlement_and_invoice(conn, env, mod,
                                              selling_bridge):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    till = seed_till_accounts(conn, env["company_id"])
    ar, revenue = _company_accounts(conn, env["company_id"])
    a, ra, b, rb = _book_two_sales(conn, env, widget)
    inv_a = ra["sales_invoice_id"]
    inv_b = rb["sales_invoice_id"]
    assert rb["grand_total"] == "5.00"
    pays_b = _pos_payments(conn, b)
    assert len(pays_b) == 1 and pays_b[0]["payment_entry_id"]
    pe_b = pays_b[0]["payment_entry_id"]
    assert rb["payment_entry_ids"] == [pe_b]
    snap_inv = _snap(conn, "sales_invoice", inv_b)
    snap_pe = _snap(conn, "payment_entry", pe_b)
    assert len(snap_inv) == 2
    assert len(snap_pe) == 2
    conn.commit()

    r = _void(conn, b)
    assert is_ok(r), f"void failed: {r}"
    assert r["id"] == b
    assert r["transaction_status"] == "voided"
    assert r["cancelled_sales_invoice_id"] == inv_b
    assert r["cancelled_payment_entry_ids"] == [pe_b]

    assert _payment_of(conn, pe_b)["status"] == "cancelled"
    inv = _invoice(conn, inv_b)
    assert (inv["status"], inv["outstanding_amount"]) == ("cancelled", "0")

    for voucher_type, voucher_id, snap in (
            ("sales_invoice", inv_b, snap_inv),
            ("payment_entry", pe_b, snap_pe)):
        g = Table("gl_entry")
        rows = conn.execute(
            Q.from_(g).select(Table("gl_entry").star)
            .where(g.voucher_type == P())
            .where(g.voucher_id == P()).get_sql(),
            (voucher_type, voucher_id)).fetchall()
        assert len(rows) == 4, (voucher_type, len(rows))
        assert all(row["is_cancelled"] == 1 for row in rows), voucher_type
        by_account = {}
        for row in rows:
            by_account.setdefault(row["account_id"], Decimal("0"))
            by_account[row["account_id"]] += (
                _dec(row["debit"]) - _dec(row["credit"]))
        for account_id, net in by_account.items():
            assert str(_q2(net)) == "0.00", (voucher_type, account_id, net)
        after = {(row["id"]): (row["id"], row["account_id"], row["debit"],
                               row["credit"], row["posting_date"],
                               row["voucher_type"], row["voucher_id"])
                 for row in rows}
        for original in snap:
            assert after[original[0]] == original, (voucher_type, original)

    assert _net(conn, ar) == "0.00"
    assert _net(conn, revenue) == "-18.50"
    assert _net(conn, till["cash"]) == "18.50"
    assert _net(conn, till["bank"]) == "0.00"
    assert _invoice(conn, inv_a)["status"] == "paid"

    audit_rows = _void_audit(conn, b)
    assert len(audit_rows) == 1
    assert json.loads(audit_rows[0]["old_values"]) == {"status": "submitted"}
    assert json.loads(audit_rows[0]["new_values"]) == {
        "status": "voided", "cancelled_sales_invoice_id": inv_b,
        "cancelled_payment_entry_ids": [pe_b]}
    _assert_invariants(conn)


@pytest.mark.parametrize("case", [
    "closed_session", "has_returns", "no_invoice", "foreign_allocation",
])
def test_void_refusals_write_nothing(conn, env, mod, selling_bridge,
                                     monkeypatch, case):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    till = seed_till_accounts(conn, env["company_id"])
    ar, _revenue = _company_accounts(conn, env["company_id"])
    a, ra, b, rb = _book_two_sales(conn, env, widget)
    inv_b = rb["sales_invoice_id"]
    pe_b = rb["payment_entry_ids"][0]
    rec = _Recorder(monkeypatch)
    target = b

    if case == "closed_session":
        rc = _close(conn, env["session_id"], "118.50")
        assert is_ok(rc), f"close failed: {rc}"
        assert rc["difference"] == "0.00"
        expected = (f"Cannot void: session {env['session_id']} is closed; "
                    f"use pos-return-transaction")
    elif case == "has_returns":
        ret_id = str(uuid.uuid4())
        t = Table("pos_transaction")
        conn.execute(
            Q.into(t).columns("id", "pos_session_id", "company_id", "status",
                              "return_against_id", "grand_total")
            .insert(P(), P(), P(), P(), P(), P()).get_sql(),
            (ret_id, env["session_id"], env["company_id"], "returned", b,
             "-5.00"))
        conn.commit()
        expected = (f"Cannot void: transaction {b} has returns; return the "
                    f"remaining items instead")
    elif case == "no_invoice":
        target = _new_txn(conn, env["session_id"], env["customer_id"])
        conn.execute(
            Q.update(Table("pos_transaction"))
            .set(Field("status"), P())
            .where(Field("id") == P()).get_sql(), ("submitted", target))
        conn.commit()
        expected = (f"Transaction {target} has no sales invoice (submitted "
                    f"before POS posted to the ledger); correct it in selling")
    elif case == "foreign_allocation":
        cross_skill.call_skill_action(
            "erpclaw", "cancel-payment",
            {"--payment-entry-id": pe_b, "--user-confirmed": None})
        made = cross_skill.call_skill_action(
            "erpclaw", "add-payment",
            {"--payment-type": "receive", "--party-type": "customer",
             "--party-id": env["customer_id"],
             "--company-id": env["company_id"],
             "--posting-date": "2026-03-10",
             "--paid-from-account": ar,
             "--paid-to-account": till["cash"],
             "--paid-amount": "5.00",
             "--allocations": json.dumps([{"voucher_type": "sales_invoice",
                                           "voucher_id": inv_b,
                                           "allocated_amount": "5.00"}])})
        foreign_id = made["payment_entry_id"]
        cross_skill.call_skill_action(
            "erpclaw", "submit-payment",
            {"--payment-entry-id": foreign_id, "--user-confirmed": None})
        conn.commit()
        expected = (f"Cannot void: sales invoice {inv_b} carries allocations "
                    f"from payments outside this sale ({foreign_id})")
    else:
        raise AssertionError(f"unknown case {case}")

    def _receipt_statuses(txn_id):
        states = []
        for pay in _pos_payments(conn, txn_id):
            if pay["payment_entry_id"]:
                states.append(_payment_of(
                    conn, pay["payment_entry_id"])["status"])
            else:
                states.append(None)
        return sorted(states, key=str)

    def _status_snapshot(txn_id):
        txn = _txn_row(conn, txn_id)
        if txn["sales_invoice_id"]:
            inv = _invoice(conn, txn["sales_invoice_id"])
            inv_state = (inv["status"], inv["outstanding_amount"])
        else:
            inv_state = None
        return (
            txn["status"], inv_state,
            _receipt_statuses(txn_id),
            _gl_count(conn),
        )

    before = _status_snapshot(target)
    calls_before = len(rec.calls)
    r = _void(conn, target)
    assert is_error(r), f"{case}: expected refusal, got {r}"
    assert r["message"] == expected, f"{case}: {r['message']!r}"
    assert _status_snapshot(target) == before, case
    assert len(rec.calls) == calls_before, case


def test_void_retry_after_invoice_cancel_failure(conn, env, mod,
                                                 selling_bridge,
                                                 monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    a, ra, b, rb = _book_two_sales(conn, env, widget)
    inv_b = rb["sales_invoice_id"]
    pe_b = rb["payment_entry_ids"][0]
    rec = _Recorder(monkeypatch, fail_on="cancel-sales-invoice")

    r1 = _void(conn, b)
    assert is_error(r1), f"expected stop, got {r1}"
    assert r1["message"] == (
        f"POS void stopped at cancel-sales-invoice: injected. "
        f"Done so far: {pe_b}. Retry the same action to finish"), r1
    assert _payment_of(conn, pe_b)["status"] == "cancelled"
    inv = _invoice(conn, inv_b)
    assert (inv["status"], inv["outstanding_amount"]) == ("submitted", "5.00")
    assert _txn_row(conn, b)["status"] == "submitted"

    rc = _close(conn, env["session_id"], "118.50")
    assert is_error(rc), f"expected close refusal, got {rc}"
    assert b in rc["message"], rc

    r2 = _void(conn, b)
    assert is_ok(r2), f"retry failed: {r2}"
    assert r2["cancelled_sales_invoice_id"] == inv_b
    assert r2["cancelled_payment_entry_ids"] == [pe_b]
    assert rec.successes.count("cancel-payment") == 1, rec.successes
    assert _invoice(conn, inv_b)["status"] == "cancelled"
    assert _txn_row(conn, b)["status"] == "voided"

    rc2 = _close(conn, env["session_id"], "118.50")
    assert is_ok(rc2), f"close failed: {rc2}"
    assert rc2["difference"] == "0.00"

    r3 = _void(conn, b)
    assert is_error(r3), f"expected already-voided, got {r3}"
    assert r3["message"] == "Transaction is already voided", r3


def test_void_is_a_confirmed_action(conn, env, mod, selling_bridge,
                                    monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    seen = []
    real = cross_skill.call_skill_action

    def _wrap(skill, action, args=None, db_path=None, timeout=30):
        seen.append((skill, action, dict(args or {})))
        return real(skill, action, args=args, db_path=db_path,
                    timeout=timeout)

    monkeypatch.setattr(cross_skill, "call_skill_action", _wrap)
    b = _sale_b(conn, env, widget)
    rb = _submit(conn, b)
    assert is_ok(rb), f"submit B failed: {rb}"
    pe_b = rb["payment_entry_ids"][0]

    spec = importlib.util.spec_from_file_location(
        "_void_router",
        os.path.join(SRC_DIR, "erpclaw", "scripts", "db_query.py"))
    router = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(router)
    assert "pos-void-transaction" in router.DANGEROUS_ACTIONS

    r = _void(conn, b)
    assert is_ok(r), f"void failed: {r}"
    cancels = [s for s in seen if s[1] in ("cancel-payment",
                                           "cancel-sales-invoice")]
    assert sorted(s[1] for s in cancels) == ["cancel-payment",
                                             "cancel-sales-invoice"]
    for _skill, _action, _args in cancels:
        assert "--user-confirmed" in _args, (_action, _args)

    captured = next(s for s in seen if s[1] == "cancel-payment")
    argv = ["db_query.py", "--action", captured[1]]
    for key, value in captured[2].items():
        argv.append(key)
        if value is not None:
            argv.append(str(value))

    # As sent: the gate lets it through.
    with patch.object(sys, "argv", argv):
        router._gate_dangerous_action(captured[1])

    # With the confirmation stripped: refused, exactly as a real box would.
    stripped = [a for a in argv if a != "--user-confirmed"]
    buf = io.StringIO()
    with patch.object(sys, "argv", stripped), \
            patch("sys.stdout", buf), pytest.raises(SystemExit) as exc:
        router._gate_dangerous_action(captured[1])
    assert exc.value.code == 2
    assert json.loads(buf.getvalue())["error"] == "user_confirmation_required"

# ---------------------------------------------------------------------------
# m352c: a return issues a credit note and a refund that clears it
# ---------------------------------------------------------------------------

def _ret_call(conn, txn_id, **kw):
    args = {"pos_transaction_id": txn_id}
    args.update(kw)
    return call_action(A["pos-return-transaction"], conn, ns(**args))


def _sale_line(conn, txn_id):
    rows = conn.execute(
        Q.from_(Table("pos_transaction_item")).select(Field("id"))
        .where(Field("pos_transaction_id") == P()).get_sql(),
        (txn_id,)).fetchall()
    assert len(rows) == 1
    return rows[0]["id"]


def _voucher_net(conn, voucher_type, voucher_id):
    g = Table("gl_entry")
    rows = conn.execute(
        Q.from_(g).select(g.account_id, g.debit, g.credit, g.is_cancelled)
        .where(g.voucher_type == P())
        .where(g.voucher_id == P()).get_sql(),
        (voucher_type, voucher_id)).fetchall()
    nets = {}
    for r in rows:
        if r["is_cancelled"]:
            continue
        nets[r["account_id"]] = _q2(
            _dec(nets.get(r["account_id"], "0"))
            + _dec(r["debit"]) - _dec(r["credit"]))
    return {k: str(v) for k, v in nets.items()}


def _party_net(conn, party_id):
    ple = Table("payment_ledger_entry")
    rows = conn.execute(
        Q.from_(ple).select(ple.voucher_type, ple.amount, ple.delinked)
        .where(ple.party_type == P())
        .where(ple.party_id == P()).get_sql(),
        ("customer", party_id)).fetchall()
    total = sum((_dec(r["amount"]) for r in rows
                 if is_live_row(r["voucher_type"], r["delinked"])),
                Decimal("0"))
    return str(_q2(total))


def _invariant_failures(conn):
    path = os.path.join(os.path.dirname(SRC_DIR), "testing",
                        "invariant_engine.py")
    spec = importlib.util.spec_from_file_location(
        "invariant_engine_pos_return", path)
    engine = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(engine)
    return [o for o in engine.evaluate_invariants(conn) if o.is_failure]


def _return_tables(conn):
    return {t: _table_count(conn, t) for t in (
        "pos_transaction", "pos_transaction_item", "pos_payment",
        "sales_invoice", "sales_invoice_item", "payment_entry",
        "payment_allocation", "gl_entry", "payment_ledger_entry",
        "audit_log", "naming_series")}


def _draft_return_of(conn, sale_id):
    t = Table("pos_transaction")
    return conn.execute(
        Q.from_(t).select(t.star)
        .where(t.return_against_id == P())
        .where(t.status == P()).get_sql(), (sale_id, "draft")).fetchone()


def _open_session(conn, profile_id, opening="100.00"):
    r = call_action(A["pos-open-session"], conn, ns(
        pos_profile_id=profile_id, cashier_name="Return Cashier",
        opening_amount=opening))
    assert is_ok(r), f"pos-open-session failed: {r}"
    return r["id"]


def _close(conn, sid, amount):
    return call_action(A["pos-close-session"], conn, ns(
        id=sid, closing_amount=amount))


def test_partial_return_credit_note_and_refund(conn, env, mod,
                                               selling_bridge):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    till = seed_till_accounts(conn, env["company_id"])
    ar, revenue = _company_accounts(conn, env["company_id"])
    a = _sale_a(conn, env, widget)
    conn.commit()
    r = _submit(conn, a)
    assert is_ok(r), f"submit A failed: {r}"
    inv_a = r["sales_invoice_id"]
    s1 = env["session_id"]
    line = _sale_line(conn, a)
    assert _table_count(conn, "sales_invoice") == 1

    ret = _ret_call(conn, a, items=json.dumps(
        [{"pos_transaction_item_id": line, "qty": "1"}]))
    assert is_ok(ret), f"return failed: {ret}"
    cn = ret["credit_note_id"]
    pe = ret["refund_payment_entry_id"]
    assert (ret["original_transaction_id"],
            ret["transaction_status"]) == (a, "submitted")
    assert ret["return_grand_total"] == "-9.25"

    inv = _invoice(conn, cn)
    assert (inv["is_return"], inv["return_against"], inv["total_amount"],
            inv["grand_total"], inv["outstanding_amount"], inv["status"],
            inv["posting_date"]) == (
        1, inv_a, "-9.25", "-9.25", "0", "paid", "2026-03-10")
    disc = _discount_item(conn, env["company_id"])
    assert sorted(tuple(x) for x in _invoice_lines(conn, cn)) == sorted([
        (widget, "-1.00", "10.00", "-10.00"),
        (disc["id"], "-1.00", "-0.75", "0.75"),
    ])
    assert _voucher_net(conn, "credit_note", cn) == {revenue: "9.25",
                                                     ar: "-9.25"}

    entry = _payment_of(conn, pe)
    assert (entry["payment_type"], entry["party_type"],
            entry["paid_amount"], entry["unallocated_amount"],
            entry["status"], entry["paid_from_account"],
            entry["paid_to_account"]) == (
        "pay", "customer", "9.25", "0.00", "submitted", till["cash"], ar)
    assert [tuple(x) for x in _allocations(conn, pe)] == [
        ("credit_note", cn, "9.25")]

    doc = _txn_row(conn, ret["return_transaction_id"])
    assert (doc["status"], doc["subtotal"], doc["discount_amount"],
            doc["grand_total"], doc["paid_amount"],
            doc["sales_invoice_id"], doc["return_against_id"],
            doc["pos_session_id"]) == (
        "returned", "-10.00", "-0.75", "-9.25", "-9.25", cn, a, s1)
    dlines = conn.execute(
        Q.from_(Table("pos_transaction_item"))
        .select(Field("qty"), Field("rate"), Field("amount"),
                Field("return_against_item_id"))
        .where(Field("pos_transaction_id") == P()).get_sql(),
        (ret["return_transaction_id"],)).fetchall()
    assert [tuple(x) for x in dlines] == [
        ("-1.00", "10.00", "-10.00", line)]
    assert [(p["payment_method"], p["amount"], p["payment_entry_id"],
             p["reference"]) for p in _pos_payments(
                 conn, ret["return_transaction_id"])] == [
        ("cash", "-9.25", pe, f"Return of {a}")]

    assert _txn_row(conn, a)["status"] == "submitted"
    assert _invoice(conn, inv_a)["status"] == "paid"
    assert _net(conn, ar) == "0.00"
    assert _net(conn, revenue) == "-9.25"
    assert _net(conn, till["cash"]) == "9.25"
    assert _party_net(conn, env["customer_id"]) == "0.00"
    assert _table_count(conn, "stock_ledger_entry") == 0
    # The credit note's payment-ledger row counts on the credit note (INV-22 after m774), so a paid invoice with a refunded partial return is consistent.
    assert [(f.inv_id, f.detail) for f in _invariant_failures(conn)] == []

    ret2 = _ret_call(conn, a)
    assert is_ok(ret2), f"second return failed: {ret2}"
    assert ret2["transaction_status"] == "returned"
    assert ret2["return_grand_total"] == "-9.25"
    assert _txn_row(conn, ret2["return_transaction_id"])[
        "discount_amount"] == "-0.75"
    assert _invoice(conn, ret2["credit_note_id"])["grand_total"] == "-9.25"
    assert _txn_row(conn, a)["status"] == "returned"
    assert _net(conn, ar) == "0.00"
    assert _net(conn, revenue) == "0.00"
    assert _net(conn, till["cash"]) == "0.00"
    assert _party_net(conn, env["customer_id"]) == "0.00"
    assert [(f.inv_id, f.detail) for f in _invariant_failures(conn)] == []


def _refusal_sale(conn, env, kind, widget_stock=None):
    """Submitted Sale A variant per refusal case; returns (sale_id, line_id)."""
    if kind == "fractional":
        txn = _new_txn(conn, env["session_id"], env["customer_id"])
        _add_line(conn, txn, widget_stock, "1.5", "10.00")
        _pay(conn, txn, "cash", "15.00")
    elif kind == "multi":
        txn = _new_txn(conn, env["session_id"], env["customer_id"])
        _add_line(conn, txn, widget_stock, "2", "10.00")
        _amount_discount(conn, txn, "1.50")
        _pay(conn, txn, "cash", "10.00")
        _pay(conn, txn, "card", "10.00")
    elif kind == "stock":
        txn = _new_txn(conn, env["session_id"], env["customer_id"])
        _add_line(conn, txn, widget_stock, "2", "10.00")
        _amount_discount(conn, txn, "1.50")
        _pay(conn, txn, "cash", "20.00")
    else:
        txn = _sale_a(conn, env, widget_stock)
    conn.commit()
    r = _submit(conn, txn)
    assert is_ok(r), f"refusal setup submit failed: {r}"
    return txn, _sale_line(conn, txn)


def test_return_refusals_write_nothing(conn, env, mod, selling_bridge,
                                       monkeypatch):
    plaintext = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    stock = seed_item(conn, "Stock Widget", "STK", is_stock_item=1)
    seed_till_accounts(conn, env["company_id"])

    rec = _Recorder(monkeypatch)
    cases = []
    a, line = _refusal_sale(conn, env, "plain", plaintext)
    over_kw = {"items": json.dumps(
        [{"pos_transaction_item_id": line, "qty": 3}])}
    over_expected = (f"Return qty 3 for line {line} exceeds returnable 2 "
                     f"(sold 2, already returned 0)")
    before = (_return_tables(conn), _txn_row(conn, a)["status"])
    calls_before = len(rec.calls)
    r = _ret_call(conn, a, **over_kw)
    assert is_error(r), f"over: expected refusal, got {r}"
    assert r["message"] == over_expected, f"over: {r['message']!r}"
    assert _return_tables(conn) == before[0], "over"
    assert _txn_row(conn, a)["status"] == before[1], "over"
    assert len(rec.calls) == calls_before, "over"
    conn.commit()

    dup_kw = {"items": json.dumps(
        [{"pos_transaction_item_id": line, "qty": "1"},
         {"pos_transaction_item_id": line, "qty": "1"}])}
    dup_expected = (f"Return refused: line {line} appears more than "
                    f"once in --items")
    before = (_return_tables(conn), _txn_row(conn, a)["status"])
    calls_before = len(rec.calls)
    r = _ret_call(conn, a, **dup_kw)
    assert is_error(r), f"duplicate: expected refusal, got {r}"
    assert r["message"] == dup_expected, f"duplicate: {r['message']!r}"
    assert _return_tables(conn) == before[0], "duplicate"
    assert _txn_row(conn, a)["status"] == before[1], "duplicate"
    assert len(rec.calls) == calls_before, "duplicate"
    conn.commit()

    r = _ret_call(conn, a, items=json.dumps(
        [{"pos_transaction_item_id": line, "qty": "1"}]))
    assert is_ok(r), f"partial setup return failed: {r}"
    cases.append(("over-after-partial", a,
                  {"items": json.dumps(
                      [{"pos_transaction_item_id": line, "qty": "2"}])},
                  f"Return qty 2 for line {line} exceeds returnable 1 "
                  f"(sold 2, already returned 1)"))

    b, b_line = _refusal_sale(conn, env, "plain", plaintext)
    cases.append(("foreign", a,
                  {"items": json.dumps(
                      [{"pos_transaction_item_id": b_line, "qty": "1"}])},
                  f"Line {b_line} does not belong to transaction {a}"))

    c, _c_line = _refusal_sale(conn, env, "stock", stock)
    cases.append(("stock", c, {},
                  "Return refused: Stock Widget is a stock item; stock "
                  "returns wait for the selling credit-note valuation fix"))

    d, d_line = _refusal_sale(conn, env, "fractional", plaintext)
    cases.append(("fractional", d,
                  {"items": json.dumps(
                      [{"pos_transaction_item_id": d_line, "qty": "1"}])},
                  f"Return refused: line {d_line} has a fractional "
                  f"quantity; return the whole line"))

    e, _e_line = _refusal_sale(conn, env, "plain", plaintext)
    pp = Table("pos_payment")
    conn.execute(
        Q.update(pp).set(pp.payment_entry_id, P()).where(
            pp.pos_transaction_id == P()).get_sql(), (None, e))
    conn.commit()
    cases.append(("unsettled", e, {},
                  f"Return refused: transaction {e} was never settled "
                  f"(no payment entry)"))

    f = _new_txn(conn, env["session_id"], env["customer_id"])
    _add_line(conn, f, plaintext, "1", "10.00")
    t = Table("pos_transaction")
    conn.execute(
        Q.update(t).set(t.status, P()).where(t.id == P()).get_sql(),
        ("submitted", f))
    conn.commit()
    cases.append(("no-invoice", f, {},
                  f"Transaction {f} has no sales invoice (submitted before "
                  f"POS posted to the ledger); correct it in selling"))

    g, _g_line = _refusal_sale(conn, env, "multi", plaintext)
    cases.append(("multi-method", g, {},
                  f"Return refused: transaction {g} was paid by more than "
                  f"one method (card, cash); pass --refund-method"))
    cases.append(("gift-card", g, {"refund_method": "gift_card"},
                  "POS return refused: no ledger account is mapped for "
                  "payment method gift_card"))

    h, h_line = _refusal_sale(conn, env, "plain", plaintext)
    rec_fail = _Recorder(monkeypatch, fail_on="create-credit-note")
    r1 = _ret_call(conn, h, items=json.dumps(
        [{"pos_transaction_item_id": h_line, "qty": "1"}]))
    assert is_error(r1), f"expected stop, got {r1}"
    rid = _draft_return_of(conn, h)["id"]
    cases.append(("in-progress", h,
                  {"items": json.dumps(
                      [{"pos_transaction_item_id": h_line, "qty": "1"}])},
                  f"Return {rid} is in progress for this sale; call "
                  f"pos-return-transaction without --items to finish it"))

    for case, target, kw, expected in cases:
        before = (_return_tables(conn), _txn_row(conn, target)["status"])
        calls_before = len(rec.calls)
        r = _ret_call(conn, target, **kw)
        assert is_error(r), f"{case}: expected refusal, got {r}"
        assert r["message"] == expected, f"{case}: {r['message']!r}"
        assert _return_tables(conn) == before[0], case
        assert _txn_row(conn, target)["status"] == before[1], case
        assert len(rec.calls) == calls_before, case


def test_return_nothing_to_refund_zero_total(conn, env, mod,
                                             selling_bridge):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    txn = _new_txn(conn, env["session_id"], env["customer_id"])
    _add_line(conn, txn, widget, "1", "0.00")
    _pay(conn, txn, "cash", "5.00")
    conn.commit()
    r = _submit(conn, txn)
    if not is_ok(r):
        pytest.skip(f"selling refuses to submit a zero-total sale: {r}")
    ret = _ret_call(conn, txn)
    assert is_error(ret), f"expected Nothing to refund, got {ret}"
    assert ret["message"] == "Nothing to refund: the returned lines total 0.00"


def test_return_into_open_session(conn, env, mod, selling_bridge):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    a = _sale_a(conn, env, widget)
    conn.commit()
    assert is_ok(_submit(conn, a)), "submit A failed"
    s1 = env["session_id"]
    t = Table("pos_transaction")
    s = Table("pos_session")
    line = _sale_line(conn, a)

    rc = _close(conn, s1, "118.50")
    assert is_ok(rc), f"close S1 failed: {rc}"
    s1_row = conn.execute(
        Q.from_(s).select(s.star).where(s.id == P()).get_sql(),
        (s1,)).fetchone()
    s1_before = dict(s1_row)

    r = _ret_call(conn, a)
    assert is_error(r), f"expected closed-session refusal, got {r}"
    assert r["message"] == (
        f"Return needs an open session: session {s1} is closed; pass "
        f"--pos-session-id of an open session"), r

    s2 = _open_session(conn, s1_before["pos_profile_id"])
    r2 = _ret_call(conn, a, pos_session_id=s2, items=json.dumps(
        [{"pos_transaction_item_id": line, "qty": "1"}]))
    assert is_ok(r2), f"return into S2 failed: {r2}"
    assert _txn_row(conn, r2["return_transaction_id"])[
        "pos_session_id"] == s2

    rc2 = _close(conn, s2, "90.75")
    assert is_ok(rc2), f"close S2 failed: {rc2}"
    assert (rc2["total_sales"], rc2["total_returns"],
            rc2["expected_amount"], rc2["difference"],
            rc2["transaction_count"]) == (
        "0.00", "9.25", "90.75", "0.00", 1)
    s1_after = conn.execute(
        Q.from_(s).select(s.star).where(s.id == P()).get_sql(),
        (s1,)).fetchone()
    assert dict(s1_after) == s1_before


@pytest.mark.parametrize("step", ["create-credit-note",
                                  "submit-sales-invoice",
                                  "add-payment", "submit-payment"])
def test_return_retry_finishes_without_duplicates(conn, env, mod,
                                                  selling_bridge,
                                                  monkeypatch, step):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    till = seed_till_accounts(conn, env["company_id"])
    ar, revenue = _company_accounts(conn, env["company_id"])
    a = _sale_a(conn, env, widget)
    conn.commit()
    assert is_ok(_submit(conn, a)), "submit A failed"
    inv_a = _txn_row(conn, a)["sales_invoice_id"]
    line = _sale_line(conn, a)
    rec = _Recorder(monkeypatch, fail_on=step)

    r1 = _ret_call(conn, a, items=json.dumps(
        [{"pos_transaction_item_id": line, "qty": "1"}]))
    assert is_error(r1), f"{step}: expected stop, got {r1}"
    doc = _draft_return_of(conn, a)
    assert doc is not None, f"{step}: no draft return document"
    rid = doc["id"]
    cn = doc["sales_invoice_id"]
    linked_pe = [p["payment_entry_id"] for p in _pos_payments(conn, rid)
                 if p["payment_entry_id"]]
    if step == "create-credit-note":
        assert cn is None and linked_pe == [], (step, cn, linked_pe)
        done = "none"
    elif step == "submit-payment":
        assert cn is not None and len(linked_pe) == 1, (
            step, cn, linked_pe)
        done = f"{cn}, {linked_pe[0]}"
    else:
        assert cn is not None and linked_pe == [], (step, cn, linked_pe)
        done = cn
    assert r1["message"] == (
        f"POS return stopped at {step}: injected. Done so far: {done}. "
        f"Retry the same action to finish"), r1
    assert _txn_row(conn, a)["status"] == "submitted"

    rc = _close(conn, env["session_id"], "118.50")
    assert is_error(rc), f"{step}: expected close refusal, got {rc}"
    assert rid in rc["message"], rc

    guard = (f"Transaction {rid} is a return document; finish with "
             f"pos-return-transaction")
    before = _return_tables(conn)
    rg = call_action(A["pos-add-transaction-item"], conn, ns(
        pos_transaction_id=rid, item_id=widget, item_name=None, qty="1",
        rate="10.00", uom=None, barcode=None, discount_pct=None))
    assert is_error(rg) and rg["message"] == guard, rg
    rs = call_action(A["pos-submit-transaction"], conn, ns(
        pos_transaction_id=rid))
    assert is_error(rs) and rs["message"] == guard, rs
    assert _return_tables(conn) == before, step

    r2 = _ret_call(conn, a)
    assert is_ok(r2), f"{step}: retry failed: {r2}"
    assert r2["return_transaction_id"] == rid
    si = Table("sales_invoice")
    cn_count = conn.execute(
        Q.from_(si).select(Field("id"))
        .where(si.return_against == P()).get_sql(), (inv_a,)).fetchall()
    assert len(cn_count) == 1
    pa = Table("payment_allocation")
    refunds = conn.execute(
        Q.from_(pa).select(Field("payment_entry_id"))
        .where(pa.voucher_type == P())
        .where(pa.voucher_id == P()).get_sql(),
        ("credit_note", cn_count[0]["id"])).fetchall()
    assert len(refunds) == 1
    for action in ("create-credit-note", "submit-sales-invoice",
                   "add-payment", "submit-payment"):
        assert rec.successes.count(action) == 1, (step, rec.successes)
    assert r2["credit_note_id"] == cn_count[0]["id"]
    assert r2["refund_payment_entry_id"] == refunds[0]["payment_entry_id"]
    assert _invoice(conn, r2["credit_note_id"])["grand_total"] == "-9.25"
    assert _invoice(conn, r2["credit_note_id"])["status"] == "paid"
    assert _payment_of(conn, r2["refund_payment_entry_id"])[
        "status"] == "submitted"
    assert _txn_row(conn, rid)["status"] == "returned"
    assert _txn_row(conn, rid)["grand_total"] == "-9.25"
    assert _txn_row(conn, a)["status"] == "submitted"
    assert _net(conn, revenue) == "-9.25"
    assert _net(conn, till["cash"]) == "9.25"


def test_return_resume_refuses_a_cancelled_credit_note(
        conn, env, mod, selling_bridge, monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    a = _sale_a(conn, env, widget)
    conn.commit()
    assert is_ok(_submit(conn, a)), "submit A failed"
    line = _sale_line(conn, a)
    rec = _Recorder(monkeypatch, fail_on="add-payment")

    r1 = _ret_call(conn, a, items=json.dumps(
        [{"pos_transaction_item_id": line, "qty": "1"}]))
    assert is_error(r1), f"expected stop, got {r1}"
    doc = _draft_return_of(conn, a)
    assert doc is not None, "no draft return document"
    rid = doc["id"]
    cn = doc["sales_invoice_id"]
    assert cn, "expected a linked credit note"

    si = Table("sales_invoice")
    conn.execute(
        Q.update(si).set(si.status, P()).where(si.id == P()).get_sql(),
        ("cancelled", cn))
    conn.commit()

    before = _return_tables(conn)
    calls_before = len(rec.calls)
    r2 = _ret_call(conn, a)
    assert is_error(r2), f"expected refusal, got {r2}"
    assert r2["message"] == (
        f"POS return refused: credit note {cn} linked to return {rid} "
        f"is cancelled; correct it in selling"), r2
    assert _return_tables(conn) == before
    assert len(rec.calls) == calls_before


def test_return_resume_refuses_a_missing_refund(
        conn, env, mod, selling_bridge, monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    a = _sale_a(conn, env, widget)
    conn.commit()
    assert is_ok(_submit(conn, a)), "submit A failed"
    line = _sale_line(conn, a)
    rec = _Recorder(monkeypatch, fail_on="submit-payment")

    r1 = _ret_call(conn, a, items=json.dumps(
        [{"pos_transaction_item_id": line, "qty": "1"}]))
    assert is_error(r1), f"expected stop, got {r1}"
    doc = _draft_return_of(conn, a)
    assert doc is not None, "no draft return document"
    rid = doc["id"]
    linked = [p["payment_entry_id"] for p in _pos_payments(conn, rid)
              if p["payment_entry_id"]]
    assert len(linked) == 1, linked
    pe = linked[0]
    assert _payment_of(conn, pe)["status"] == "draft"

    pa = Table("payment_allocation")
    conn.execute(
        Q.from_(pa).delete().where(pa.payment_entry_id == P()).get_sql(),
        (pe,))
    conn.execute(
        Q.from_(Table("payment_entry")).delete()
        .where(Field("id") == P()).get_sql(), (pe,))
    conn.commit()

    before = _return_tables(conn)
    calls_before = len(rec.calls)
    r2 = _ret_call(conn, a)
    assert is_error(r2), f"expected refusal, got {r2}"
    assert r2["message"] == (
        f"POS return refused: refund {pe} linked to return {rid} "
        f"is missing; correct it in payments"), r2
    assert _return_tables(conn) == before
    assert len(rec.calls) == calls_before
    assert _draft_return_of(conn, a)["status"] == "draft"


def test_return_is_a_confirmed_action(conn, env, mod, selling_bridge,
                                      monkeypatch):
    widget = seed_item(conn, "Widget", "WDG", is_stock_item=0)
    seed_till_accounts(conn, env["company_id"])
    seen = []
    real = cross_skill.call_skill_action

    def _wrap(skill, action, args=None, db_path=None, timeout=30):
        seen.append((skill, action, dict(args or {})))
        return real(skill, action, args=args, db_path=db_path,
                    timeout=timeout)

    monkeypatch.setattr(cross_skill, "call_skill_action", _wrap)
    a = _sale_a(conn, env, widget)
    conn.commit()
    assert is_ok(_submit(conn, a)), "submit A failed"
    line = _sale_line(conn, a)
    del seen[:]

    spec = importlib.util.spec_from_file_location(
        "_return_router",
        os.path.join(SRC_DIR, "erpclaw", "scripts", "db_query.py"))
    router = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(router)
    assert "pos-return-transaction" in router.DANGEROUS_ACTIONS

    r = _ret_call(conn, a, items=json.dumps(
        [{"pos_transaction_item_id": line, "qty": "1"}]))
    assert is_ok(r), f"return failed: {r}"
    hops = [s for s in seen if s[1] in ("submit-sales-invoice",
                                        "submit-payment")]
    assert sorted(s[1] for s in hops) == ["submit-payment",
                                          "submit-sales-invoice"]
    for _skill, _action, _args in hops:
        assert "--user-confirmed" in _args, (_action, _args)

    for _skill, action, args in hops:
        argv = ["db_query.py", "--action", action]
        for key, value in args.items():
            argv.append(key)
            if value is not None:
                argv.append(str(value))
        with patch.object(sys, "argv", argv):
            router._gate_dangerous_action(action)
        stripped = [x for x in argv if x != "--user-confirmed"]
        buf = io.StringIO()
        with patch.object(sys, "argv", stripped), \
                patch("sys.stdout", buf), pytest.raises(SystemExit) as exc:
            router._gate_dangerous_action(action)
        assert exc.value.code == 2
        assert json.loads(buf.getvalue())["error"] == \
            "user_confirmation_required"
