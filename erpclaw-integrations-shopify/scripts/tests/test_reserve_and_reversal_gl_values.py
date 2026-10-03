"""Ledger effects of shopify-post-reserve-gl and shopify-reverse-order-gl.

Every amount is compared as an exact string read back from gl_entry /
journal_entry. Dates are fixed: the fiscal year and every posting date below
are pinned to 2025, so nothing here depends on the day the suite runs.
"""
import uuid
from decimal import Decimal

from shopify_test_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_shopify_order, seed_shopify_order_line_item, seed_shopify_payout,
    seed_item,
)

mod = load_db_query()

FY_START = "2025-01-01"
FY_END = "2025-12-31"
ORDER_DATE = "2025-03-15"
PAYOUT_ISSUED_AT = "2025-06-30T12:00:00Z"
PAYOUT_POSTING_DATE = "2025-06-30"


def _seed_fixed_fiscal_year(conn, company_id):
    fy_id = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO fiscal_year (id, name, start_date, end_date, is_closed, company_id)
           VALUES (?, ?, ?, ?, 0, ?)""",
        (fy_id, f"FY-2025-{fy_id[:6]}", FY_START, FY_END, company_id),
    )
    conn.commit()
    return fy_id


def _legs(conn, voucher_id):
    """Every gl_entry row of a voucher, sorted by a stable key."""
    rows = conn.execute(
        """SELECT id, account_id, debit, credit, voucher_type, voucher_id,
                  posting_date, entry_set, is_cancelled, remarks
           FROM gl_entry WHERE voucher_id = ?""",
        (voucher_id,),
    ).fetchall()
    return sorted((dict(r) for r in rows),
                  key=lambda r: (r["account_id"], r["remarks"] or "", r["id"]))


def _row_counts(conn):
    gl = conn.execute("SELECT COUNT(*) AS n FROM gl_entry").fetchone()["n"]
    je = conn.execute("SELECT COUNT(*) AS n FROM journal_entry").fetchone()["n"]
    return gl, je


def _payout(conn, env, reserved):
    _seed_fixed_fiscal_year(conn, env["company_id"])
    payout_id = seed_shopify_payout(
        conn, env["shopify_account_id"], env["company_id"],
        gross="1000.00", fee="29.00", reserved_funds_gross=reserved,
    )
    conn.execute("UPDATE shopify_payout SET issued_at = ? WHERE id = ?",
                 (PAYOUT_ISSUED_AT, payout_id))
    conn.commit()
    return payout_id


# ---------------------------------------------------------------------------
# shopify-post-reserve-gl
# ---------------------------------------------------------------------------

class TestPostReserveGLValues:

    def test_hold_debits_reserve_and_credits_clearing(self, conn, env):
        acct = env["shopify_account"]
        payout_id = _payout(conn, env, reserved="250.00")

        result = call_action(mod.shopify_post_reserve_gl, conn, ns(
            shopify_payout_id=payout_id, reserve_type="hold",
        ))
        assert is_ok(result), result
        je_id = result["journal_entry_id"]
        assert result["amount"] == "250.00"
        assert result["reserve_type"] == "hold"
        assert result["gl_entry_count"] == 2

        legs = _legs(conn, je_id)
        by_account = {r["account_id"]: r for r in legs}
        assert len(legs) == 2
        assert set(by_account) == {acct["reserve_account_id"],
                                   acct["clearing_account_id"]}
        reserve = by_account[acct["reserve_account_id"]]
        clearing = by_account[acct["clearing_account_id"]]
        assert (reserve["debit"], reserve["credit"]) == ("250.00", "0.00")
        assert (clearing["debit"], clearing["credit"]) == ("0.00", "250.00")
        for r in legs:
            assert r["voucher_type"] == "journal_entry"
            assert r["voucher_id"] == je_id
            assert r["posting_date"] == PAYOUT_POSTING_DATE
            assert r["entry_set"] == "primary"
            assert r["is_cancelled"] == 0
        assert sum(Decimal(r["debit"]) for r in legs) == Decimal("250.00")
        assert sum(Decimal(r["credit"]) for r in legs) == Decimal("250.00")

        je = conn.execute(
            """SELECT posting_date, total_debit, total_credit, status, company_id
               FROM journal_entry WHERE id = ?""", (je_id,)).fetchone()
        assert dict(je) == {
            "posting_date": PAYOUT_POSTING_DATE, "total_debit": "250.00",
            "total_credit": "250.00", "status": "submitted",
            "company_id": env["company_id"],
        }

        # The reserve posting is separate from the settlement posting: the
        # payout's own gl_status is left for shopify-post-payout-gl.
        payout = conn.execute(
            "SELECT gl_status, gl_voucher_id FROM shopify_payout WHERE id = ?",
            (payout_id,)).fetchone()
        assert (payout["gl_status"], payout["gl_voucher_id"]) == ("pending", None)

    def test_release_debits_clearing_and_credits_reserve(self, conn, env):
        acct = env["shopify_account"]
        payout_id = _payout(conn, env, reserved="87.35")

        result = call_action(mod.shopify_post_reserve_gl, conn, ns(
            shopify_payout_id=payout_id, reserve_type="release",
        ))
        assert is_ok(result), result
        assert result["amount"] == "87.35"
        assert result["reserve_type"] == "release"

        by_account = {r["account_id"]: r for r in _legs(conn, result["journal_entry_id"])}
        assert set(by_account) == {acct["reserve_account_id"],
                                   acct["clearing_account_id"]}
        clearing = by_account[acct["clearing_account_id"]]
        reserve = by_account[acct["reserve_account_id"]]
        assert (clearing["debit"], clearing["credit"]) == ("87.35", "0.00")
        assert (reserve["debit"], reserve["credit"]) == ("0.00", "87.35")

    def test_hold_then_release_nets_each_account_to_zero(self, conn, env):
        acct = env["shopify_account"]
        payout_id = _payout(conn, env, reserved="400.10")

        hold = call_action(mod.shopify_post_reserve_gl, conn, ns(
            shopify_payout_id=payout_id, reserve_type="hold"))
        release = call_action(mod.shopify_post_reserve_gl, conn, ns(
            shopify_payout_id=payout_id, reserve_type="release"))
        assert is_ok(hold), hold
        assert is_ok(release), release
        assert hold["journal_entry_id"] != release["journal_entry_id"]

        net = {}
        for voucher in (hold["journal_entry_id"], release["journal_entry_id"]):
            for r in _legs(conn, voucher):
                net[r["account_id"]] = (net.get(r["account_id"], Decimal("0"))
                                        + Decimal(r["debit"]) - Decimal(r["credit"]))
        assert net == {acct["reserve_account_id"]: Decimal("0.00"),
                       acct["clearing_account_id"]: Decimal("0.00")}

    def test_refuses_payout_without_reserved_funds(self, conn, env):
        payout_id = _payout(conn, env, reserved="0")
        before = _row_counts(conn)

        result = call_action(mod.shopify_post_reserve_gl, conn, ns(
            shopify_payout_id=payout_id, reserve_type="hold"))
        assert is_error(result)
        assert result["message"] == "No reserved funds to post"
        assert _row_counts(conn) == before

    def test_refuses_unknown_reserve_type(self, conn, env):
        payout_id = _payout(conn, env, reserved="250.00")
        before = _row_counts(conn)

        result = call_action(mod.shopify_post_reserve_gl, conn, ns(
            shopify_payout_id=payout_id, reserve_type="partial"))
        assert is_error(result)
        assert result["message"] == "--reserve-type must be 'hold' or 'release'"
        assert _row_counts(conn) == before


# ---------------------------------------------------------------------------
# shopify-reverse-order-gl
# ---------------------------------------------------------------------------

def _posted_order(conn, env, shopify_order_id="REV-1001"):
    _seed_fixed_fiscal_year(conn, env["company_id"])
    order_id = seed_shopify_order(
        conn, env["shopify_account_id"], env["company_id"],
        shopify_order_id=shopify_order_id,
        subtotal="100.00", shipping="10.00", tax="8.00",
    )
    conn.execute("UPDATE shopify_order SET order_date = ? WHERE id = ?",
                 (ORDER_DATE, order_id))
    conn.commit()
    return order_id


class TestReverseOrderGLValues:

    def test_reversal_mirrors_every_leg_and_cancels_the_voucher(self, conn, env):
        acct = env["shopify_account"]
        order_id = _posted_order(conn, env)
        posted = call_action(mod.shopify_post_order_gl, conn, ns(shopify_order_id=order_id))
        assert is_ok(posted), posted
        je_id = posted["journal_entry_id"]
        originals = {r["id"]: r for r in _legs(conn, je_id)}
        assert len(originals) == 4

        result = call_action(mod.shopify_reverse_order_gl, conn, ns(shopify_order_id=order_id))
        assert is_ok(result), result
        assert result["reversed_voucher_id"] == je_id
        assert result["reversal_gl_entry_count"] == 4

        legs = _legs(conn, je_id)
        assert len(legs) == 8
        reversals = [r for r in legs if r["id"] not in originals]
        kept = [r for r in legs if r["id"] in originals]

        # Originals: the order posting as it was, now marked cancelled.
        assert sorted((r["account_id"], r["debit"], r["credit"]) for r in kept) == sorted([
            (acct["clearing_account_id"], "118.00", "0.00"),
            (acct["revenue_account_id"], "0.00", "100.00"),
            (acct["shipping_revenue_account_id"], "0.00", "10.00"),
            (acct["tax_payable_account_id"], "0.00", "8.00"),
        ])
        # Reversals: debit and credit swapped, one per original, same voucher.
        assert sorted((r["account_id"], r["debit"], r["credit"]) for r in reversals) == sorted([
            (acct["clearing_account_id"], "0.00", "118.00"),
            (acct["revenue_account_id"], "100.00", "0.00"),
            (acct["shipping_revenue_account_id"], "10.00", "0.00"),
            (acct["tax_payable_account_id"], "8.00", "0.00"),
        ])
        for rev in reversals:
            orig = originals[rev["remarks"][len("Reversal of "):]]
            assert rev["remarks"] == f"Reversal of {orig['id']}"
            assert (rev["account_id"], rev["debit"], rev["credit"]) == (
                orig["account_id"], orig["credit"], orig["debit"])
            assert rev["entry_set"] == orig["entry_set"] == "primary"
        for r in legs:
            assert r["voucher_type"] == "journal_entry"
            assert r["voucher_id"] == je_id
            assert r["posting_date"] == ORDER_DATE
            assert r["is_cancelled"] == 1

        net = {}
        for r in legs:
            net[r["account_id"]] = (net.get(r["account_id"], Decimal("0"))
                                    + Decimal(r["debit"]) - Decimal(r["credit"]))
        assert set(net.values()) == {Decimal("0.00")}
        assert sum(Decimal(r["debit"]) for r in legs) == Decimal("236.00")
        assert sum(Decimal(r["credit"]) for r in legs) == Decimal("236.00")

        je = conn.execute("SELECT status, total_debit FROM journal_entry WHERE id = ?",
                          (je_id,)).fetchone()
        assert (je["status"], je["total_debit"]) == ("cancelled", "118.00")
        order = conn.execute(
            "SELECT gl_status, gl_voucher_id FROM shopify_order WHERE id = ?",
            (order_id,)).fetchone()
        assert (order["gl_status"], order["gl_voucher_id"]) == ("pending", None)

    def test_reversed_order_can_be_posted_again(self, conn, env):
        order_id = _posted_order(conn, env, shopify_order_id="REV-1002")
        first = call_action(mod.shopify_post_order_gl, conn, ns(shopify_order_id=order_id))
        assert is_ok(first), first
        reversed_ = call_action(mod.shopify_reverse_order_gl, conn,
                                ns(shopify_order_id=order_id))
        assert is_ok(reversed_), reversed_

        second = call_action(mod.shopify_post_order_gl, conn, ns(shopify_order_id=order_id))
        assert is_ok(second), second
        assert second["journal_entry_id"] != first["journal_entry_id"]
        active = [r for r in _legs(conn, second["journal_entry_id"]) if r["is_cancelled"] == 0]
        assert sum(Decimal(r["debit"]) for r in active) == Decimal("118.00")
        assert sum(Decimal(r["credit"]) for r in active) == Decimal("118.00")
        order = conn.execute(
            "SELECT gl_status, gl_voucher_id FROM shopify_order WHERE id = ?",
            (order_id,)).fetchone()
        assert (order["gl_status"], order["gl_voucher_id"]) == (
            "posted", second["journal_entry_id"])

    def test_reversal_includes_the_cogs_entry_set(self, conn, env):
        acct = env["shopify_account"]
        call_action(mod.shopify_update_account, conn, ns(
            shopify_account_id=env["shopify_account_id"], track_cogs=1))
        item_id = seed_item(conn, env["company_id"], "REV-COGS-ITEM")
        conn.execute("UPDATE item SET last_purchase_rate = '25.00' WHERE id = ?", (item_id,))
        conn.commit()
        order_id = _posted_order(conn, env, shopify_order_id="REV-COGS-1")
        seed_shopify_order_line_item(conn, order_id, env["company_id"],
                                     sku="REV-COGS-ITEM", quantity=2,
                                     unit_price="50.00", item_id=item_id)
        posted = call_action(mod.shopify_post_order_gl, conn, ns(shopify_order_id=order_id))
        assert is_ok(posted), posted
        assert posted["cogs_amount"] == "50.00"

        result = call_action(mod.shopify_reverse_order_gl, conn, ns(shopify_order_id=order_id))
        assert is_ok(result), result
        assert result["reversal_gl_entry_count"] == 6

        cogs = [r for r in _legs(conn, posted["journal_entry_id"]) if r["entry_set"] == "cogs"]
        assert sorted((r["account_id"], r["debit"], r["credit"], r["is_cancelled"])
                      for r in cogs) == sorted([
            (acct["cogs_account_id"], "50.00", "0.00", 1),
            (acct["inventory_account_id"], "0.00", "50.00", 1),
            (acct["cogs_account_id"], "0.00", "50.00", 1),
            (acct["inventory_account_id"], "50.00", "0.00", 1),
        ])

    def test_refuses_order_whose_gl_is_not_posted(self, conn, env):
        order_id = _posted_order(conn, env, shopify_order_id="REV-1003")
        before = _row_counts(conn)

        result = call_action(mod.shopify_reverse_order_gl, conn, ns(shopify_order_id=order_id))
        assert is_error(result)
        assert result["message"] == "Cannot reverse: order GL has not been posted"
        assert _row_counts(conn) == before
        order = conn.execute("SELECT gl_status FROM shopify_order WHERE id = ?",
                             (order_id,)).fetchone()
        assert order["gl_status"] == "pending"

    def test_refuses_a_second_reversal(self, conn, env):
        order_id = _posted_order(conn, env, shopify_order_id="REV-1004")
        posted = call_action(mod.shopify_post_order_gl, conn, ns(shopify_order_id=order_id))
        assert is_ok(posted), posted
        first = call_action(mod.shopify_reverse_order_gl, conn, ns(shopify_order_id=order_id))
        assert is_ok(first), first
        before = _row_counts(conn)

        again = call_action(mod.shopify_reverse_order_gl, conn, ns(shopify_order_id=order_id))
        assert is_error(again)
        assert again["message"] == "Cannot reverse: order GL has not been posted"
        assert _row_counts(conn) == before

    def test_refuses_posted_order_without_voucher(self, conn, env):
        _seed_fixed_fiscal_year(conn, env["company_id"])
        order_id = seed_shopify_order(
            conn, env["shopify_account_id"], env["company_id"],
            shopify_order_id="REV-1005", gl_status="posted")
        before = _row_counts(conn)

        result = call_action(mod.shopify_reverse_order_gl, conn, ns(shopify_order_id=order_id))
        assert is_error(result)
        assert result["message"] == "Cannot reverse: no gl_voucher_id on order"
        assert _row_counts(conn) == before
