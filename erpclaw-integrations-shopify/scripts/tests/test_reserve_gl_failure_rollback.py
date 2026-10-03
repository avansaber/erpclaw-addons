"""shopify-post-reserve-gl rolls back its journal_entry when GL posting fails.

The handler inserts the journal_entry voucher before it calls the shared GL
library. When that call raises, the refusal must discard the voucher, so a
caller holding the connection in-process never commits a submitted
journal_entry with no gl_entry rows behind it.

Dates are fixed. The environment's own fiscal year is the current calendar
year; the postings below use 2025 (seeded here) and 2019 (deliberately left
without a fiscal year), so nothing depends on the day the suite runs.
"""
import sys
import uuid
from decimal import Decimal

from shopify_test_helpers import (
    call_action, ns, is_error, is_ok, load_db_query, seed_shopify_payout,
)

mod = load_db_query()
# The module whose globals post_reserve_gl resolves insert_gl_entries from.
GL = sys.modules[mod.shopify_post_reserve_gl.__module__]

ISSUED_2025 = "2025-06-30T12:00:00Z"
ISSUED_2019 = "2019-06-30T12:00:00Z"


def _seed_fiscal_year(conn, company_id, year):
    fy_id = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO fiscal_year (id, name, start_date, end_date, is_closed, company_id)
           VALUES (?, ?, ?, ?, 0, ?)""",
        (fy_id, f"FY-{year}-{fy_id[:6]}", f"{year}-01-01", f"{year}-12-31", company_id),
    )
    conn.commit()


def _payout(conn, env, issued_at, reserved="250.00"):
    payout_id = seed_shopify_payout(
        conn, env["shopify_account_id"], env["company_id"],
        gross="1000.00", fee="29.00", reserved_funds_gross=reserved,
    )
    conn.execute("UPDATE shopify_payout SET issued_at = ? WHERE id = ?",
                 (issued_at, payout_id))
    conn.commit()
    return payout_id


def _row_counts(conn):
    return tuple(
        conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
        for table in ("journal_entry", "gl_entry", "audit_log")
    )


def _post_hold(conn, payout_id):
    """Drive the action; an escaping ValueError is recorded, not raised, so the
    row-count assertions are what report it."""
    try:
        return call_action(mod.shopify_post_reserve_gl, conn, ns(
            shopify_payout_id=payout_id, reserve_type="hold"))
    except ValueError as exc:
        return {"status": "raised", "message": str(exc)}


def _boom(*args, **kwargs):
    raise ValueError("planted GL failure")


class TestPostReserveGLFailureRollback:

    def test_planted_gl_failure_leaves_no_journal_entry(self, conn, env, monkeypatch):
        _seed_fiscal_year(conn, env["company_id"], 2025)
        payout_id = _payout(conn, env, ISSUED_2025)
        before = _row_counts(conn)
        monkeypatch.setattr(GL, "insert_gl_entries", _boom)

        result = _post_hold(conn, payout_id)

        assert _row_counts(conn) == before
        conn.commit()
        assert _row_counts(conn) == before
        assert is_error(result), result
        assert result["message"] == "GL posting failed: planted GL failure"
        payout = conn.execute(
            "SELECT gl_status, gl_voucher_id FROM shopify_payout WHERE id = ?",
            (payout_id,)).fetchone()
        assert (payout["gl_status"], payout["gl_voucher_id"]) == ("pending", None)

    def test_no_open_fiscal_year_refusal_writes_nothing(self, conn, env):
        payout_id = _payout(conn, env, ISSUED_2019)
        before = _row_counts(conn)

        result = _post_hold(conn, payout_id)

        assert _row_counts(conn) == before
        conn.commit()
        assert _row_counts(conn) == before
        assert is_error(result), result
        assert result["message"] == (
            "GL posting failed: GL Validation Step 9 Failed: "
            "No open fiscal year found for posting date 2019-06-30")

    def test_retry_after_refusal_posts_exactly_one_voucher(self, conn, env):
        acct = env["shopify_account"]
        payout_id = _payout(conn, env, ISSUED_2019, reserved="312.45")
        before = _row_counts(conn)
        refused = _post_hold(conn, payout_id)
        assert refused["status"] != "ok", refused

        _seed_fiscal_year(conn, env["company_id"], 2019)
        result = _post_hold(conn, payout_id)
        assert is_ok(result), result
        je_id = result["journal_entry_id"]

        # One voucher, two legs, one audit row: nothing left over from the refusal.
        assert _row_counts(conn) == (before[0] + 1, before[1] + 2, before[2] + 1)
        vouchers = conn.execute(
            "SELECT id, total_debit, total_credit, status, posting_date FROM journal_entry"
        ).fetchall()
        assert [dict(v) for v in vouchers] == [{
            "id": je_id, "total_debit": "312.45", "total_credit": "312.45",
            "status": "submitted", "posting_date": "2019-06-30",
        }]
        legs = conn.execute(
            """SELECT account_id, debit, credit, voucher_type, voucher_id, is_cancelled
               FROM gl_entry"""
        ).fetchall()
        assert sorted(tuple(r) for r in legs) == sorted([
            (acct["reserve_account_id"], "312.45", "0.00", "journal_entry", je_id, 0),
            (acct["clearing_account_id"], "0.00", "312.45", "journal_entry", je_id, 0),
        ])
        assert sum(Decimal(r["debit"]) for r in legs) == Decimal("312.45")
        assert sum(Decimal(r["credit"]) for r in legs) == Decimal("312.45")
