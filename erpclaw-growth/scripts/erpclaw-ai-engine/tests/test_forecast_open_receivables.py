"""The cash-flow forecast counts only receivables that are still open.

outstanding_amount is TEXT. SQLite orders every TEXT value above every number,
so a bare "outstanding_amount > 0" keeps a fully paid "0.00" invoice; the
forecast has to compare the numeric value.
"""
import json
import uuid

from erpclaw_lib.query import P, Q, Table, insert_row

from ai_helpers import call_action, is_ok, load_db_query, ns, seed_customer

MOD = load_db_query()


def test_forecast_skips_fully_paid_invoices(conn, env):
    customer_id = seed_customer(conn, env["company_id"], name="Forecast Cust")
    sql, _ = insert_row("sales_invoice", {
        "id": P(), "customer_id": P(), "posting_date": P(), "due_date": P(),
        "grand_total": P(), "outstanding_amount": P(), "status": P(), "company_id": P(),
    })
    for outstanding in ("120.00", "0.00"):
        conn.execute(sql, (str(uuid.uuid4()), customer_id, "2026-01-01", "2026-01-31",
                           "120.00", outstanding, "submitted", env["company_id"]))
    conn.commit()

    result = call_action(MOD.forecast_cash_flow, conn, ns(
        company_id=env["company_id"], horizon_days="30", from_date=None, to_date=None))

    assert is_ok(result), result
    assert result["total_ar"] == "120.00"
    forecast = Table("cash_flow_forecast")
    row = conn.execute(
        Q.from_(forecast).select(forecast.projected_inflows)
        .where(forecast.id == P()).get_sql(),
        (result["forecast_ids"][0],)).fetchone()
    assert json.loads(row["projected_inflows"]) == [
        {"date": "2026-01-31", "amount": "120.00"}]
