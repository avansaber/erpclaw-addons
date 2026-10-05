"""Serial and batch traceability v1 (floor-i05).

One disposable chain: purchase receipt -> material transfer -> delivery note,
all carrying the same serial and batch, with one stock-ledger row per leg
(receipt in, transfer out, transfer in, delivery out). Proves forward and
backward order, exact Decimal quantities, serial and batch filters, company
isolation, exclusive-selector refusal, unknown-identifier empty result, and
two identical read-only calls.
"""
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from decimal import Decimal

import pytest

from logistics_helpers import call_action, ns, is_ok, is_error, _uuid
from erpclaw_lib.query import Q, P, Table, Field, fn


SERIAL = "SN-TRACE-001"
BATCH_NAME = "BATCH-TRACE-001"

EXPECTED_QTYS = ["10.00", "-10.00", "10.00", "-10.00"]


def _insert_chain(conn, company_id, serial=SERIAL, batch_name=BATCH_NAME):
    item_id = _uuid()
    conn.execute(
        "INSERT INTO item (id, item_code, item_name, stock_uom, is_stock_item,"
        " item_type, standard_rate, status, has_batch, has_serial)"
        " VALUES (?, ?, ?, 'Each', 1, 'stock', '10.00', 'active', 1, 1)",
        (item_id, f"TR-{item_id[:6]}", "Trace Item"),
    )
    wid1 = _uuid()
    wid2 = _uuid()
    conn.execute(
        "INSERT INTO warehouse (id, name, warehouse_type, company_id)"
        " VALUES (?, ?, 'stores', ?)",
        (wid1, f"Trace WH1 {wid1[:6]}", company_id),
    )
    conn.execute(
        "INSERT INTO warehouse (id, name, warehouse_type, company_id)"
        " VALUES (?, ?, 'stores', ?)",
        (wid2, f"Trace WH2 {wid2[:6]}", company_id),
    )
    batch_id = _uuid()
    conn.execute(
        "INSERT INTO batch (id, batch_name, item_id) VALUES (?, ?, ?)",
        (batch_id, f"{batch_name}-{item_id[:4]}", item_id),
    )
    batch_name_stored = f"{batch_name}-{item_id[:4]}"
    sn_id = _uuid()
    conn.execute(
        "INSERT INTO serial_number (id, serial_no, item_id, warehouse_id, batch_id, status)"
        " VALUES (?, ?, ?, ?, ?, 'active')",
        (sn_id, serial, item_id, wid2, batch_id),
    )
    sup_id = _uuid()
    conn.execute(
        "INSERT INTO supplier (id, name, company_id) VALUES (?, ?, ?)",
        (sup_id, f"Trace Supplier {sup_id[:6]}", company_id),
    )
    cust_id = _uuid()
    conn.execute(
        "INSERT INTO customer (id, name, company_id) VALUES (?, ?, ?)",
        (cust_id, f"Trace Customer {cust_id[:6]}", company_id),
    )
    pr_id = _uuid()
    conn.execute(
        "INSERT INTO purchase_receipt (id, supplier_id, posting_date, company_id, status, total_qty)"
        " VALUES (?, ?, '2026-01-10', ?, 'submitted', '10.00')",
        (pr_id, sup_id, company_id),
    )
    conn.execute(
        "INSERT INTO purchase_receipt_item (id, purchase_receipt_id, item_id, quantity,"
        " warehouse_id, batch_id, serial_numbers) VALUES (?, ?, ?, '10.00', ?, ?, ?)",
        (_uuid(), pr_id, item_id, wid1, batch_id, serial),
    )
    se_id = _uuid()
    conn.execute(
        "INSERT INTO stock_entry (id, stock_entry_type, posting_date,"
        " from_warehouse_id, to_warehouse_id, company_id, status)"
        " VALUES (?, 'material_transfer', '2026-01-12', ?, ?, ?, 'submitted')",
        (se_id, wid1, wid2, company_id),
    )
    conn.execute(
        "INSERT INTO stock_entry_item (id, stock_entry_id, item_id, quantity,"
        " from_warehouse_id, to_warehouse_id, batch_id, serial_numbers)"
        " VALUES (?, ?, ?, '10.00', ?, ?, ?, ?)",
        (_uuid(), se_id, item_id, wid1, wid2, batch_id, serial),
    )
    dn_id = _uuid()
    conn.execute(
        "INSERT INTO delivery_note (id, customer_id, posting_date, company_id, status, total_qty)"
        " VALUES (?, ?, '2026-01-15', ?, 'submitted', '10.00')",
        (dn_id, cust_id, company_id),
    )
    conn.execute(
        "INSERT INTO delivery_note_item (id, delivery_note_id, item_id, quantity,"
        " warehouse_id, batch_id, serial_numbers) VALUES (?, ?, ?, '10.00', ?, ?, ?)",
        (_uuid(), dn_id, item_id, wid2, batch_id, serial),
    )
    legs = [
        ("2026-01-10", "2026-01-10T10:00:00Z", wid1, "10.00", "purchase_receipt", pr_id),
        ("2026-01-12", "2026-01-12T10:00:00Z", wid1, "-10.00", "stock_entry", se_id),
        ("2026-01-12", "2026-01-12T11:00:00Z", wid2, "10.00", "stock_entry", se_id),
        ("2026-01-15", "2026-01-15T10:00:00Z", wid2, "-10.00", "delivery_note", dn_id),
    ]
    for posting_date, created_at, wid, qty, vtype, vid in legs:
        conn.execute(
            "INSERT INTO stock_ledger_entry (id, posting_date, item_id, warehouse_id,"
            " actual_qty, qty_after_transaction, valuation_rate, stock_value,"
            " stock_value_difference, voucher_type, voucher_id,"
            " batch_id, serial_number, is_cancelled, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, '10.00', '0', '0', ?, ?, ?, ?, 0, ?)",
            (_uuid(), posting_date, item_id, wid, qty, qty, vtype, vid,
             batch_id, serial, created_at),
        )
    conn.commit()
    return {
        "company_id": company_id,
        "item_id": item_id,
        "wid1": wid1,
        "wid2": wid2,
        "batch_id": batch_id,
        "batch_name": batch_name_stored,
        "serial": serial,
        "pr_id": pr_id,
        "se_id": se_id,
        "dn_id": dn_id,
    }


def _trace(conn, mod, **kwargs):
    return call_action(mod.logistics_trace_inventory, conn, ns(**kwargs))


def _counts(conn):
    out = {}
    for table in ("stock_ledger_entry", "purchase_receipt", "stock_entry",
                  "delivery_note", "audit_log", "item", "warehouse",
                  "batch", "serial_number"):
        t = Table(table)
        out[table] = conn.execute(
            Q.from_(t).select(fn.Count(t.star)).get_sql()
        ).fetchone()[0]
    return out


REQUIRED_NODE_KEYS = {"document_type", "document_id", "posting_date", "item_id",
                      "warehouse_id", "quantity", "serial_number", "batch_number"}


class TestForwardBackwardOrder:
    def test_logistics_trace_inventory_forward_order_and_quantities(
            self, conn, env, mod):
        chain = _insert_chain(conn, env["company_id"])
        r = _trace(conn, mod, company_id=env["company_id"],
                   direction="forward", serial_number=chain["serial"])
        assert is_ok(r), r
        nodes = r["nodes"]
        assert len(nodes) == 4, r
        for node in nodes:
            assert REQUIRED_NODE_KEYS <= set(node.keys()), node
        assert [n["document_type"] for n in nodes] == [
            "purchase_receipt", "stock_entry", "stock_entry", "delivery_note"]
        assert [n["document_id"] for n in nodes] == [
            chain["pr_id"], chain["se_id"], chain["se_id"], chain["dn_id"]]
        assert [n["posting_date"] for n in nodes] == [
            "2026-01-10", "2026-01-12", "2026-01-12", "2026-01-15"]
        assert [n["quantity"] for n in nodes] == EXPECTED_QTYS
        for node, expected in zip(nodes, EXPECTED_QTYS):
            assert Decimal(node["quantity"]) == Decimal(expected)
            assert node["quantity"] == expected
            assert node["serial_number"] == chain["serial"]
            assert node["batch_number"] == chain["batch_id"]
        assert r["gaps"] == []
        edges = r["edges"]
        assert len(edges) == 3
        for idx, edge in enumerate(edges):
            assert edge["from_document_id"] == nodes[idx]["document_id"]
            assert edge["to_document_id"] == nodes[idx + 1]["document_id"]
            assert edge["from_document_type"] == nodes[idx]["document_type"]
            assert edge["to_document_type"] == nodes[idx + 1]["document_type"]

    def test_backward_is_reverse_of_forward(self, conn, env, mod):
        chain = _insert_chain(conn, env["company_id"])
        fwd = _trace(conn, mod, company_id=env["company_id"],
                     direction="forward", serial_number=chain["serial"])
        bwd = _trace(conn, mod, company_id=env["company_id"],
                     direction="backward", serial_number=chain["serial"])
        assert is_ok(fwd) and is_ok(bwd)
        assert [n["document_id"] for n in bwd["nodes"]] == list(
            reversed([n["document_id"] for n in fwd["nodes"]]))
        assert [n["posting_date"] for n in bwd["nodes"]] == list(
            reversed([n["posting_date"] for n in fwd["nodes"]]))
        assert [n["quantity"] for n in bwd["nodes"]] == list(
            reversed([n["quantity"] for n in fwd["nodes"]]))
        assert len(bwd["edges"]) == 3
        assert bwd["gaps"] == []


class TestSerialBatchFilters:
    def test_batch_filter_matches_serial_chain(self, conn, env, mod):
        chain = _insert_chain(conn, env["company_id"])
        by_serial = _trace(conn, mod, company_id=env["company_id"],
                           direction="forward", serial_number=chain["serial"])
        by_batch_id = _trace(conn, mod, company_id=env["company_id"],
                             direction="forward", batch_number=chain["batch_id"])
        by_batch_name = _trace(conn, mod, company_id=env["company_id"],
                               direction="forward", batch_number=chain["batch_name"])
        assert is_ok(by_serial) and is_ok(by_batch_id) and is_ok(by_batch_name)
        assert [n["document_id"] for n in by_batch_id["nodes"]] == [
            n["document_id"] for n in by_serial["nodes"]]
        assert [n["document_id"] for n in by_batch_name["nodes"]] == [
            n["document_id"] for n in by_serial["nodes"]]
        assert [n["quantity"] for n in by_batch_id["nodes"]] == EXPECTED_QTYS

    def test_company_isolation(self, conn, env, mod):
        chain = _insert_chain(conn, env["company_id"])
        from logistics_helpers import seed_company
        other_company = seed_company(conn, name="Other Trace Co", abbr="OTC")
        other_wid = _uuid()
        conn.execute(
            "INSERT INTO warehouse (id, name, warehouse_type, company_id)"
            " VALUES (?, ?, 'stores', ?)",
            (other_wid, f"Other WH {other_wid[:6]}", other_company),
        )
        other_item = _uuid()
        conn.execute(
            "INSERT INTO item (id, item_code, item_name, stock_uom, is_stock_item,"
            " item_type, standard_rate, status) VALUES (?, ?, ?, 'Each', 1, 'stock', '5.00', 'active')",
            (other_item, f"OT-{other_item[:6]}", "Other Item"),
        )
        other_pr = _uuid()
        other_sup = _uuid()
        conn.execute(
            "INSERT INTO supplier (id, name, company_id) VALUES (?, ?, ?)",
            (other_sup, f"Other Sup {other_sup[:6]}", other_company),
        )
        conn.execute(
            "INSERT INTO purchase_receipt (id, supplier_id, posting_date, company_id, status, total_qty)"
            " VALUES (?, ?, '2026-01-11', ?, 'submitted', '2.00')",
            (other_pr, other_sup, other_company),
        )
        conn.execute(
            "INSERT INTO stock_ledger_entry (id, posting_date, item_id, warehouse_id,"
            " actual_qty, qty_after_transaction, valuation_rate, stock_value,"
            " stock_value_difference, voucher_type, voucher_id,"
            " batch_id, serial_number, is_cancelled, created_at)"
            " VALUES (?, '2026-01-11', ?, ?, '2.00', '2.00', '5.00', '0', '0',"
            " 'purchase_receipt', ?, NULL, ?, 0, '2026-01-11T10:00:00Z')",
            (_uuid(), other_item, other_wid, other_pr, chain["serial"]),
        )
        conn.commit()
        own = _trace(conn, mod, company_id=env["company_id"],
                     direction="forward", serial_number=chain["serial"])
        assert is_ok(own), own
        assert len(own["nodes"]) == 4
        assert all(n["warehouse_id"] in (chain["wid1"], chain["wid2"]) for n in own["nodes"])
        assert other_pr not in [n["document_id"] for n in own["nodes"]]
        other = _trace(conn, mod, company_id=other_company,
                       direction="forward", serial_number=chain["serial"])
        assert is_ok(other), other
        assert len(other["nodes"]) == 1
        assert other["nodes"][0]["document_id"] == other_pr


class TestRefusalsAndEmpty:
    def test_exclusive_selector_refusal(self, conn, env, mod):
        chain = _insert_chain(conn, env["company_id"])
        before = _counts(conn)
        both = _trace(conn, mod, company_id=env["company_id"], direction="forward",
                       serial_number=chain["serial"], batch_number=chain["batch_id"])
        assert is_error(both), both
        neither = _trace(conn, mod, company_id=env["company_id"], direction="forward")
        assert is_error(neither), neither
        bad_dir = _trace(conn, mod, company_id=env["company_id"], direction="sideways",
                          serial_number=chain["serial"])
        assert is_error(bad_dir), bad_dir
        missing_company = _trace(conn, mod, direction="forward",
                                 serial_number=chain["serial"])
        assert is_error(missing_company), missing_company
        assert _counts(conn) == before

    def test_unknown_identifier_empty(self, conn, env, mod):
        _insert_chain(conn, env["company_id"])
        r = _trace(conn, mod, company_id=env["company_id"],
                   direction="forward", serial_number="NOPE-999")
        assert is_ok(r), r
        assert r["nodes"] == []
        assert r["edges"] == []
        assert r["gaps"] == []
        r2 = _trace(conn, mod, company_id=env["company_id"],
                    direction="backward", batch_number="NOPE-BATCH")
        assert is_ok(r2), r2
        assert r2["nodes"] == []
        assert r2["edges"] == []


class TestReadOnly:
    def test_two_identical_calls_change_nothing(self, conn, env, mod):
        chain = _insert_chain(conn, env["company_id"])
        db_path = os.environ.get("ERPCLAW_DB_PATH")
        size_before = os.path.getsize(db_path) if db_path and os.path.exists(db_path) else None
        counts_before = _counts(conn)
        first = _trace(conn, mod, company_id=env["company_id"],
                        direction="forward", serial_number=chain["serial"])
        counts_mid = _counts(conn)
        second = _trace(conn, mod, company_id=env["company_id"],
                         direction="forward", serial_number=chain["serial"])
        counts_after = _counts(conn)
        assert is_ok(first) and is_ok(second)
        assert first == second
        assert counts_before == counts_mid == counts_after
        if size_before is not None:
            assert os.path.getsize(db_path) == size_before
        for node in first["nodes"]:
            Decimal(node["quantity"])
