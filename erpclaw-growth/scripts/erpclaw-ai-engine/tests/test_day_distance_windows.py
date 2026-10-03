"""Day-distance windows in anomaly detection and payment-timing correlation.

Possible duplicates are GL entries on one account with the same amount posted
at most seven days apart, inclusive. Payment timing is the average number of
days from an invoice to the payment against it. Both are computed in SQL from
TEXT dates through the erpclaw_lib.query day-distance helpers, so these pins
fix the window edges and the average the helpers must reproduce.
"""
import uuid

from erpclaw_lib.query import P, Q, Table, insert_row

from ai_helpers import call_action, is_ok, load_db_query, ns, seed_customer

MOD = load_db_query()


def _gl(conn, account_id, posting_date, debit):
    entry_id = str(uuid.uuid4())
    sql, _ = insert_row("gl_entry", {
        "id": P(), "posting_date": P(), "account_id": P(), "debit": P(),
        "credit": P(), "voucher_type": P(), "voucher_id": P(), "is_cancelled": P(),
    })
    conn.execute(sql, (entry_id, posting_date, account_id, debit, "0",
                       "journal_entry", str(uuid.uuid4()), 0))
    return entry_id


def test_duplicates_are_flagged_within_seven_days_inclusive(conn, env):
    account_id = env["accounts"]["expense"]
    # The anomaly is recorded against whichever entry of a pair has the lower
    # id, and ids are random, so each pair is asserted as a set.
    seven_apart = {_gl(conn, account_id, "2026-03-01", "55.55"),
                   _gl(conn, account_id, "2026-03-08", "55.55")}
    eight_apart = {_gl(conn, account_id, "2026-03-01", "44.44"),
                   _gl(conn, account_id, "2026-03-09", "44.44")}
    conn.commit()

    result = call_action(MOD.detect_anomalies, conn, ns(
        company_id=env["company_id"], from_date="2026-03-01", to_date="2026-03-31"))

    assert is_ok(result), result
    anomaly = Table("anomaly")
    flagged = {row["entity_id"] for row in conn.execute(
        Q.from_(anomaly).select(anomaly.entity_id)
        .where(anomaly.anomaly_type == P()).get_sql(),
        ("duplicate_possible",)).fetchall()}
    assert len(flagged & seven_apart) == 1
    assert not flagged & eight_apart


def test_payment_timing_averages_days_from_invoice(conn, env):
    customer_id = seed_customer(conn, env["company_id"], name="Timing Cust")
    sql, _ = insert_row("sales_invoice", {
        "id": P(), "customer_id": P(), "posting_date": P(), "grand_total": P(),
        "outstanding_amount": P(), "status": P(), "company_id": P(),
    })
    conn.execute(sql, (str(uuid.uuid4()), customer_id, "2026-02-01", "250.00",
                       "0.00", "paid", env["company_id"]))
    sql, _ = insert_row("payment_entry", {
        "id": P(), "payment_type": P(), "posting_date": P(), "party_type": P(),
        "party_id": P(), "paid_from_account": P(), "paid_to_account": P(),
        "paid_amount": P(), "status": P(), "company_id": P(),
    })
    conn.execute(sql, (str(uuid.uuid4()), "receive", "2026-02-11", "customer",
                       customer_id, env["accounts"]["receivable"], env["accounts"]["cash"],
                       "250.00", "submitted", env["company_id"]))
    conn.commit()

    result = call_action(MOD.discover_correlations, conn, ns(
        company_id=env["company_id"], from_date="2026-01-01", to_date="2026-03-31"))

    assert is_ok(result), result
    correlation = Table("correlation")
    descriptions = [row["description"] for row in conn.execute(
        Q.from_(correlation).select(correlation.description)
        .where(correlation.module_b == P()).get_sql(), ("customer",)).fetchall()]
    assert descriptions == ["Average customer payment timing: 10.0 days from invoice"]
