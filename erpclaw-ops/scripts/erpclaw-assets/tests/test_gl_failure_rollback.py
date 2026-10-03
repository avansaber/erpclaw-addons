"""GL-failure rollback in the assets domain."""
import ast
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from assets_helpers import (build_gl_env, call_action, is_error,  # noqa: E402
                           load_db_query, ns, seed_account)

M = load_db_query()


def _handlers_missing_rollback():
    with open(M.__file__, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    missing = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler) or not isinstance(node.type, ast.Tuple):
            continue
        names = {e.id for e in node.type.elts if isinstance(e, ast.Name)}
        if names != {"ValueError", "NotImplementedError"}:
            continue
        calls_err = any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                        and n.func.id == "err" for n in ast.walk(node))
        if not calls_err:
            continue
        first = node.body[0]
        ok = (isinstance(first, ast.Expr) and isinstance(first.value, ast.Call)
              and isinstance(first.value.func, ast.Attribute)
              and first.value.func.attr == "rollback"
              and isinstance(first.value.func.value, ast.Name)
              and first.value.func.value.id == "conn")
        if not ok:
            missing.append(node.lineno)
    return missing


def test_every_gl_failure_refusal_rolls_back_first():
    assert _handlers_missing_rollback() == []


def _boom(*a, **k):
    raise ValueError("planted GL failure")


def test_failed_impairment_leaves_no_row(conn, monkeypatch):
    env = build_gl_env(conn)
    conn.commit()
    monkeypatch.setattr(M, "insert_gl_entries", _boom)
    r = call_action(M.impair_asset, conn, ns(
        asset_id=env["asset_id"], impairment_amount="1000.00",
        recoverable_amount="3000.00", impairment_date="2026-03-01"))
    assert is_error(r)
    assert "GL posting failed" in r.get("message", "")
    assert conn.execute("SELECT COUNT(*) FROM asset_impairment").fetchone()[0] == 0
    row = conn.execute("SELECT status, current_book_value FROM asset WHERE id = ?",
                       (env["asset_id"],)).fetchone()
    assert (row["status"], row["current_book_value"]) == ("in_use", "5000.00")


def test_failed_capitalization_leaves_no_asset_and_no_row(conn, monkeypatch):
    env = build_gl_env(conn)
    src = seed_account(conn, env["company_id"], "CWIP Clearing", "asset", "asset")
    conn.commit()
    monkeypatch.setattr(M, "insert_gl_entries", _boom)
    r = call_action(M.capitalize_asset, conn, ns(
        company_id=env["company_id"], name="New Machine",
        asset_category_id=env["category_id"], capitalized_amount="12000.00",
        source_account_id=src, purchase_invoice_id="PI-001",
        capitalization_date="2026-02-01"))
    assert is_error(r)
    assert conn.execute("SELECT COUNT(*) FROM asset_capitalization").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM asset WHERE asset_name = ?",
                        ("New Machine",)).fetchone()[0] == 0
