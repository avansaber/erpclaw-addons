"""A subcontracting child document lands in the same books as its parent.

The transfer, receive and cancel actions delegate to inventory / purchasing
through cross-skill subprocesses. The child is given a database path only when
the caller gave --db-path; otherwise it inherits the environment and resolves
the database exactly as the parent did. Each test points the built-in default
at a decoy directory and calls the action with no --db-path, so any forwarded
default either fails the child or materialises the decoy.
"""
import os
import sys
from decimal import Decimal

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import subcontract_helpers as sc  # noqa: E402
from mfg_helpers import init_all_tables, get_conn  # noqa: E402
from erpclaw_lib.query import Q, P, Table  # noqa: E402

# Every test here drives real cross-skill subprocesses against the foundation
# router (transfer/receive/cancel resolve source/erpclaw/scripts/db_query.py via
# a symlinked deployed-skills layout). In a standalone erpclaw-ops/addons
# checkout the foundation tree is absent, so the whole module skips instead of
# failing with "erpclaw is not installed"; in the monorepo the router resolves
# and every test runs.
pytestmark = pytest.mark.skipif(
    not os.path.exists(sc._FOUND_ROUTER),
    reason="erpclaw foundation router not present — subcontracting lifecycle "
           "needs the foundation tree (monorepo-only cross-skill subprocesses)")


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "subcontract.sqlite")
    init_all_tables(path)
    os.environ["ERPCLAW_DB_PATH"] = path
    yield path
    os.environ.pop("ERPCLAW_DB_PATH", None)
    os.environ.pop("OPENCLAW_SKILLS_DIR", None)


@pytest.fixture
def conn(db_path):
    c = get_conn(db_path)
    yield c
    c.close()


@pytest.fixture
def mfg():
    return sc.load_mfg()


@pytest.fixture
def env(conn, db_path, tmp_path, mfg):
    sc.deploy_skills(tmp_path, db_path)
    ids = sc.seed_subcontract_env(conn, raw_rate="20.00", raw_per_fg="2",
                                  order_qty="100")
    return ids


def _add(conn, db_path, mfg, env, qty="100"):
    r = sc.call(mfg.add_subcontracting_order, conn, db_path,
                supplier_id=env["supplier"], bom_id=env["bom"], quantity=qty,
                company_id=env["company"], service_item_id=env["service_item"],
                supplier_warehouse_id=env["sub_wh"])
    assert r["status"] == "ok", r
    return r["subcontracting_order_id"]


def _decoy(monkeypatch, mfg, tmp_path):
    decoy_dir = str(tmp_path / "decoy")
    monkeypatch.setattr(mfg, "DEFAULT_DB_PATH",
                        str(tmp_path / "decoy" / "decoy.sqlite"),
                        raising=False)
    return decoy_dir


def test_transfer_without_flag_posts_in_the_same_books(
        conn, db_path, mfg, env, tmp_path, monkeypatch):
    decoy_dir = _decoy(monkeypatch, mfg, tmp_path)
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    r = sc.call(mfg.transfer_materials_to_subcontractor, conn, None, order=oid,
                posting_date="2026-02-01")
    assert r["status"] == "ok", r
    assert r["materials_transferred"] == "100.00"
    assert r["stock_entry_id"]
    se_t = Table("stock_entry")
    se_q = Q.from_(se_t).select(se_t.id).where(se_t.id == P())
    assert conn.execute(se_q.get_sql(), (r["stock_entry_id"],)).fetchone() is not None
    sei_t = Table("stock_entry_item")
    sei_q = (Q.from_(sei_t)
             .select(sei_t.item_id, sei_t.quantity)
             .where(sei_t.stock_entry_id == P()))
    se_items = conn.execute(sei_q.get_sql(), (r["stock_entry_id"],)).fetchall()
    assert len(se_items) == 1
    assert se_items[0]["item_id"] == env["raw_item"]
    assert Decimal(se_items[0]["quantity"]) == Decimal("200.00")
    assert not os.path.exists(decoy_dir)


def test_receive_without_flag_bills_in_the_same_books(
        conn, db_path, mfg, env, tmp_path, monkeypatch):
    decoy_dir = _decoy(monkeypatch, mfg, tmp_path)
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path, order=oid,
            posting_date="2026-02-01")
    r = sc.call(mfg.receive_subcontracted_items, conn, None, order=oid,
                received_qty="60", subcontract_charge_rate="5.00",
                posting_date="2026-02-05")
    assert r["status"] == "ok", r
    assert r["fg_total_cost"] == "2700.00"
    assert r["purchase_invoice_id"]
    pi_t = Table("purchase_invoice")
    pi_q = Q.from_(pi_t).select(pi_t.id).where(pi_t.id == P())
    assert conn.execute(pi_q.get_sql(), (r["purchase_invoice_id"],)).fetchone() is not None
    assert not os.path.exists(decoy_dir)


def test_cancel_transfer_without_flag_reverses_in_the_same_books(
        conn, db_path, mfg, env, tmp_path, monkeypatch):
    decoy_dir = _decoy(monkeypatch, mfg, tmp_path)
    oid = _add(conn, db_path, mfg, env)
    sc.call(mfg.submit_subcontracting_order, conn, db_path, id=oid)
    t = sc.call(mfg.transfer_materials_to_subcontractor, conn, db_path, order=oid)
    se_id = t["stock_entry_id"]
    sle_t = Table("stock_ledger_entry")
    count_q = (Q.from_(sle_t).select(sle_t.id)
               .where(sle_t.voucher_id == P()))
    assert conn.execute(count_q.get_sql(), (se_id,)).fetchall()
    r = sc.call(mfg.cancel_subcontract_transfer, conn, None, stock_entry=se_id,
                order=oid, reason="wrong items shipped")
    assert r["status"] == "ok", r
    orig_q = (Q.from_(sle_t).select(sle_t.is_cancelled)
              .where(sle_t.voucher_id == P()))
    orig = conn.execute(orig_q.get_sql(), (se_id,)).fetchall()
    assert any(row["is_cancelled"] == 1 for row in orig), \
        "original SLE must be cancelled, not deleted"
    sco_t = Table("subcontracting_order")
    o_q = (Q.from_(sco_t)
           .select(sco_t.materials_transferred, sco_t.status)
           .where(sco_t.id == P()))
    o = conn.execute(o_q.get_sql(), (oid,)).fetchone()
    assert Decimal(o["materials_transferred"]) == Decimal("0")
    assert o["status"] == "submitted"
    assert not os.path.exists(decoy_dir)
