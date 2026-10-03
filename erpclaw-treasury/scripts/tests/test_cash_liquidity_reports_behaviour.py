"""Part A: behaviour of the treasury read reports, driven through the module's
own actions and read back from the database.

Actions covered: treasury-cash-dashboard, treasury-liquidity-report and
treasury-cash-flow-projection (cash.py), treasury-investment-maturity-alerts
(investments.py) and treasury-inter-company-balance-report (intercompany.py).

Every bank account, cash position, forecast, investment and transfer is
created through its real action with fixed dates and exact money strings.
Every figure the reports return is pinned as an exact string, together with
the rows each report must leave out: another company's rows, a deactivated
bank account, a CD account (not liquid), matured and redeemed investments, an
investment with no maturity date, forecasts whose period has ended, and
transfers that are not completed or do not involve the company.

Calendar. None of these reports takes an as-of date. liquidity-report selects
investments maturing on or before today + 90 days, cash-flow-projection
selects forecasts whose period ends on or after today, and
investment-maturity-alerts selects investments maturing on or before today +
--days and computes days_until_maturity from today. Every date below is
therefore either on or before 2026-03-31 (always in the past from now on) or
in 2199 (outside every window these tests use), and days_until_maturity is
compared with today's date read immediately around the call.

Company names carry a random suffix from the seed helper, so they are read
back from the company table rather than typed.
"""
from datetime import date

from treasury_helpers import (call_action, is_error, is_ok, ns, seed_company,
                              seed_naming_series)

TREASURY_TABLES = ("bank_account_extended", "cash_position", "cash_forecast",
                   "investment", "investment_transaction",
                   "inter_company_transfer", "audit_log")


# -- helpers -----------------------------------------------------------------

def _company(conn, name, abbr):
    cid = seed_company(conn, name, abbr)
    seed_naming_series(conn, cid)
    return cid


def _company_name(conn, cid):
    return conn.execute("SELECT name FROM company WHERE id = ?", (cid,)).fetchone()[0]


def _counts(conn):
    return {t: conn.execute("SELECT COUNT(*) FROM " + t).fetchone()[0]
            for t in TREASURY_TABLES}


def _ok(r):
    assert is_ok(r), r
    return r


def _bank(conn, mod, cid, bank, account, account_type, balance):
    r = _ok(call_action(mod.ACTIONS["treasury-add-bank-account"], conn, ns(
        company_id=cid, bank_name=bank, account_name=account,
        account_type=account_type, current_balance=balance)))
    return r["account_id"]


def _position(conn, mod, cid, position_date, cash, receivables, payables):
    _ok(call_action(mod.ACTIONS["treasury-add-cash-position"], conn, ns(
        company_id=cid, position_date=position_date, total_cash=cash,
        total_receivables=receivables, total_payables=payables)))


def _investment(conn, mod, cid, name, investment_type, principal,
                current_value=None, maturity_date=None):
    r = _ok(call_action(mod.ACTIONS["treasury-add-investment"], conn, ns(
        company_id=cid, name=name, investment_type=investment_type,
        principal=principal, current_value=current_value,
        purchase_date="2025-01-02", maturity_date=maturity_date)))
    return r["investment_id"]


def _forecast(conn, mod, cid, name, start, end, inflows, outflows):
    r = _ok(call_action(mod.ACTIONS["treasury-add-cash-forecast"], conn, ns(
        company_id=cid, forecast_name=name, period_start=start,
        period_end=end, expected_inflows=inflows, expected_outflows=outflows)))
    return r["forecast_id"]


def _completed_transfer(conn, mod, recorded_by, sender, receiver, amount, on):
    xfer = _transfer(conn, mod, recorded_by, sender, receiver, amount, on)
    _ok(call_action(mod.ACTIONS["treasury-approve-transfer"], conn,
                    ns(transfer_id=xfer)))
    _ok(call_action(mod.ACTIONS["treasury-complete-transfer"], conn,
                    ns(transfer_id=xfer)))
    return xfer


def _transfer(conn, mod, recorded_by, sender, receiver, amount, on):
    r = _ok(call_action(mod.ACTIONS["treasury-add-inter-company-transfer"], conn, ns(
        company_id=recorded_by, from_company_id=sender, to_company_id=receiver,
        amount=amount, transfer_date=on)))
    return r["transfer_id"]


def _book(conn, mod):
    """Bank accounts, cash positions and investments for two companies."""
    a = _company(conn, "Northwind Holdings", "NWH")
    b = _company(conn, "Lakeside Traders", "LKT")

    accts = {
        "operating": _bank(conn, mod, a, "First National", "Operating", "checking", "12500.50"),
        "reserve": _bank(conn, mod, a, "First National", "Reserve", "savings", "900.00"),
        "sweep": _bank(conn, mod, a, "Harbor Bank", "Sweep", "money_market", "4321.09"),
        "cd": _bank(conn, mod, a, "Harbor Bank", "12-Month CD", "cd", "10000.00"),
        "old_payroll": _bank(conn, mod, a, "First National", "Old Payroll", "checking", "7777.77"),
        "b_operating": _bank(conn, mod, b, "Lakeside Bank", "Operating", "checking", "99999.99"),
    }
    _ok(call_action(mod.ACTIONS["treasury-update-bank-account"], conn, ns(
        account_id=accts["old_payroll"], is_active="0")))

    # Company A's latest position (2026-02-28) is added between two older ones.
    _position(conn, mod, a, "2026-01-31", "15000.00", "4000.00", "1500.00")
    _position(conn, mod, a, "2026-02-28", "27721.59", "3200.40", "2100.15")
    _position(conn, mod, a, "2025-12-31", "9999.00", "9999.00", "1.00")
    _position(conn, mod, b, "2026-03-31", "99999.99", "50000.00", "10.00")

    inv = {
        "tbill": _investment(conn, mod, a, "T-Bill Dec 2025", "treasury_bill",
                             "8000.00", maturity_date="2025-12-31"),
        "cd": _investment(conn, mod, a, "CD Mar 2026", "cd", "5000.00",
                          current_value="5050.25", maturity_date="2026-03-31"),
        "bond": _investment(conn, mod, a, "Bond 2199", "bond", "20000.00",
                            maturity_date="2199-06-30"),
        "fund": _investment(conn, mod, a, "Open Fund", "mutual_fund", "3500.00"),
        "matured": _investment(conn, mod, a, "CD Jun 2025", "cd", "3000.00",
                               maturity_date="2025-06-30"),
        "redeemed": _investment(conn, mod, a, "MM Sep 2025", "money_market",
                                "2000.00", maturity_date="2025-09-30"),
        "b_cd": _investment(conn, mod, b, "Lakeside CD", "cd", "7000.00",
                            maturity_date="2025-11-30"),
    }
    r = _ok(call_action(mod.ACTIONS["treasury-add-investment-transaction"], conn, ns(
        investment_id=inv["tbill"], transaction_type="interest", amount="125.50",
        transaction_date="2025-06-30")))
    assert r["new_current_value"] == "8125.50"
    _ok(call_action(mod.ACTIONS["treasury-mature-investment"], conn,
                    ns(investment_id=inv["matured"])))
    _ok(call_action(mod.ACTIONS["treasury-redeem-investment"], conn,
                    ns(investment_id=inv["redeemed"])))
    return {"a": a, "b": b, "accts": accts, "inv": inv}


# -- treasury-cash-dashboard ---------------------------------------------------

def test_cash_dashboard_sums_active_accounts_and_latest_position(conn, mod):
    bk = _book(conn, mod)
    rows = conn.execute(
        "SELECT account_name, account_type, current_balance, is_active "
        "FROM bank_account_extended WHERE company_id = ? ORDER BY account_name",
        (bk["a"],)).fetchall()
    assert [tuple(r) for r in rows] == [
        ("12-Month CD", "cd", "10000.00", 1),
        ("Old Payroll", "checking", "7777.77", 0),
        ("Operating", "checking", "12500.50", 1),
        ("Reserve", "savings", "900.00", 1),
        ("Sweep", "money_market", "4321.09", 1),
    ]
    statuses = conn.execute(
        "SELECT name, status, current_value FROM investment WHERE company_id = ? "
        "ORDER BY name", (bk["a"],)).fetchall()
    assert [tuple(r) for r in statuses] == [
        ("Bond 2199", "active", "20000.00"),
        ("CD Jun 2025", "matured", "3000.00"),
        ("CD Mar 2026", "active", "5050.25"),
        ("MM Sep 2025", "redeemed", "0"),
        ("Open Fund", "active", "3500.00"),
        ("T-Bill Dec 2025", "active", "8125.50"),
    ]

    r = _ok(call_action(mod.ACTIONS["treasury-cash-dashboard"], conn,
                        ns(company_id=bk["a"])))
    # 12500.50 + 900.00 + 4321.09 + 10000.00; Old Payroll is inactive.
    assert r["total_cash"] == "27721.59"
    # From the 2026-02-28 position, the latest by date, not the last added.
    assert (r["total_receivables"], r["total_payables"]) == ("3200.40", "2100.15")
    assert r["net_position"] == "28821.84"
    assert r["active_bank_accounts"] == 4
    assert r["active_investments"] == 4

    rb = _ok(call_action(mod.ACTIONS["treasury-cash-dashboard"], conn,
                         ns(company_id=bk["b"])))
    assert (rb["total_cash"], rb["total_receivables"], rb["total_payables"],
            rb["net_position"], rb["active_bank_accounts"],
            rb["active_investments"]) == (
        "99999.99", "50000.00", "10.00", "149989.99", 1, 1)


def test_cash_dashboard_empty_company_and_refusal(conn, mod):
    _book(conn, mod)
    empty = _company(conn, "Summit Ventures", "SMV")
    r = _ok(call_action(mod.ACTIONS["treasury-cash-dashboard"], conn,
                        ns(company_id=empty)))
    assert (r["total_cash"], r["total_receivables"], r["total_payables"],
            r["net_position"], r["active_bank_accounts"],
            r["active_investments"]) == ("0", "0", "0", "0", 0, 0)

    before = _counts(conn)
    refused = call_action(mod.ACTIONS["treasury-cash-dashboard"], conn,
                          ns(company_id=None))
    assert is_error(refused)
    assert refused["message"] == "--company-id is required"
    assert _counts(conn) == before


# -- treasury-liquidity-report -------------------------------------------------

def test_liquidity_report_liquid_accounts_and_short_term_investments(conn, mod):
    bk = _book(conn, mod)
    accts, inv = bk["accts"], bk["inv"]
    r = _ok(call_action(mod.ACTIONS["treasury-liquidity-report"], conn,
                        ns(company_id=bk["a"])))

    # Checking, savings and money market only; the CD account, the inactive
    # account and company B are left out. Largest balance first, by amount.
    assert r["liquid_bank_accounts"] == [
        {"id": accts["operating"], "bank_name": "First National",
         "account_name": "Operating", "account_type": "checking",
         "current_balance": "12500.50"},
        {"id": accts["sweep"], "bank_name": "Harbor Bank",
         "account_name": "Sweep", "account_type": "money_market",
         "current_balance": "4321.09"},
        {"id": accts["reserve"], "bank_name": "First National",
         "account_name": "Reserve", "account_type": "savings",
         "current_balance": "900.00"},
    ]
    assert r["liquid_bank_total"] == "17721.59"

    # Active investments with a maturity date on or before today + 90 days,
    # earliest first. The 2199 bond, the undated fund, the matured and the
    # redeemed investments and company B's CD are left out.
    assert r["short_term_investments"] == [
        {"id": inv["tbill"], "name": "T-Bill Dec 2025",
         "investment_type": "treasury_bill", "current_value": "8125.50",
         "maturity_date": "2025-12-31"},
        {"id": inv["cd"], "name": "CD Mar 2026", "investment_type": "cd",
         "current_value": "5050.25", "maturity_date": "2026-03-31"},
    ]
    assert r["short_term_investment_total"] == "13175.75"
    assert r["total_liquidity"] == "30897.34"

    rb = _ok(call_action(mod.ACTIONS["treasury-liquidity-report"], conn,
                         ns(company_id=bk["b"])))
    assert [a["id"] for a in rb["liquid_bank_accounts"]] == [accts["b_operating"]]
    assert [i["id"] for i in rb["short_term_investments"]] == [inv["b_cd"]]
    assert (rb["liquid_bank_total"], rb["short_term_investment_total"],
            rb["total_liquidity"]) == ("99999.99", "7000.00", "106999.99")


def test_liquidity_report_refusal_writes_nothing(conn, mod):
    _book(conn, mod)
    before = _counts(conn)
    r = call_action(mod.ACTIONS["treasury-liquidity-report"], conn,
                    ns(company_id=None))
    assert is_error(r)
    assert r["message"] == "--company-id is required"
    assert _counts(conn) == before


# -- treasury-cash-flow-projection ---------------------------------------------

def test_cash_flow_projection_open_forecasts_and_projected_balance(conn, mod):
    bk = _book(conn, mod)
    a, b = bk["a"], bk["b"]
    q1 = _forecast(conn, mod, a, "Q1 2199", "2199-01-01", "2199-03-31",
                   "15000.00", "9500.25")
    _forecast(conn, mod, a, "Q1 2025", "2025-01-01", "2025-03-31",
              "99999.00", "1.00")
    q2 = _forecast(conn, mod, a, "Q2 2199", "2199-04-01", "2199-06-30",
                   "1000.00", "1200.00")
    span = _forecast(conn, mod, a, "Long span", "2025-10-01", "2199-12-31",
                     "2500.10", "4000.00")
    _forecast(conn, mod, b, "Lakeside 2199", "2199-01-01", "2199-12-31",
              "70000.00", "0")
    _ok(call_action(mod.ACTIONS["treasury-update-cash-forecast"], conn, ns(
        forecast_id=q2, expected_inflows="3000.00")))
    stored = conn.execute("SELECT expected_inflows, expected_outflows, net_forecast "
                          "FROM cash_forecast WHERE id = ?", (q2,)).fetchone()
    assert tuple(stored) == ("3000.00", "1200.00", "1800.00")

    r = _ok(call_action(mod.ACTIONS["treasury-cash-flow-projection"], conn,
                        ns(company_id=a)))
    # Forecasts still open, by period start; "Q1 2025" ended and is left out.
    assert r["forecasts"] == [
        {"forecast_id": span, "forecast_name": "Long span",
         "period_start": "2025-10-01", "period_end": "2199-12-31",
         "expected_inflows": "2500.10", "expected_outflows": "4000.00",
         "net_forecast": "-1499.90"},
        {"forecast_id": q1, "forecast_name": "Q1 2199",
         "period_start": "2199-01-01", "period_end": "2199-03-31",
         "expected_inflows": "15000.00", "expected_outflows": "9500.25",
         "net_forecast": "5499.75"},
        {"forecast_id": q2, "forecast_name": "Q2 2199",
         "period_start": "2199-04-01", "period_end": "2199-06-30",
         "expected_inflows": "3000.00", "expected_outflows": "1200.00",
         "net_forecast": "1800.00"},
    ]
    # Active accounts only: 12500.50 + 900.00 + 4321.09 + 10000.00.
    assert r["current_cash"] == "27721.59"
    assert r["total_projected_inflows"] == "20500.10"
    assert r["total_projected_outflows"] == "14700.25"
    assert r["projected_end_balance"] == "33521.44"

    rb = _ok(call_action(mod.ACTIONS["treasury-cash-flow-projection"], conn,
                         ns(company_id=b)))
    assert [f["forecast_name"] for f in rb["forecasts"]] == ["Lakeside 2199"]
    assert (rb["current_cash"], rb["total_projected_inflows"],
            rb["total_projected_outflows"], rb["projected_end_balance"]) == (
        "99999.99", "70000.00", "0", "169999.99")

    before = _counts(conn)
    refused = call_action(mod.ACTIONS["treasury-cash-flow-projection"], conn,
                          ns(company_id=None))
    assert is_error(refused)
    assert refused["message"] == "--company-id is required"
    assert _counts(conn) == before


# -- treasury-investment-maturity-alerts ---------------------------------------

def _alert_call(conn, mod, cid, days):
    first = date.today()
    r = _ok(call_action(mod.ACTIONS["treasury-investment-maturity-alerts"], conn,
                        ns(company_id=cid, days=days)))
    last = date.today()
    for alert in r["alerts"]:
        maturity = date.fromisoformat(alert["maturity_date"])
        assert alert["days_until_maturity"] in {(maturity - first).days,
                                                (maturity - last).days}
        assert alert["is_overdue"] is (alert["days_until_maturity"] < 0)
    return r


def test_maturity_alerts_select_active_investments_inside_the_window(conn, mod):
    bk = _book(conn, mod)
    inv = bk["inv"]

    r = _alert_call(conn, mod, bk["a"], "30")
    assert r["days_window"] == 30
    assert r["total_alerts"] == 2
    assert [(x["id"], x["name"], x["status"], x["principal"], x["current_value"],
             x["maturity_date"], x["is_overdue"]) for x in r["alerts"]] == [
        (inv["tbill"], "T-Bill Dec 2025", "active", "8000.00", "8125.50",
         "2025-12-31", True),
        (inv["cd"], "CD Mar 2026", "active", "5000.00", "5050.25",
         "2026-03-31", True),
    ]
    assert (r["alerts"][1]["days_until_maturity"]
            - r["alerts"][0]["days_until_maturity"]) == 90

    # Without --days the window is 30 days: the same two alerts.
    default = _alert_call(conn, mod, bk["a"], None)
    assert default["days_window"] == 30
    assert [x["id"] for x in default["alerts"]] == [inv["tbill"], inv["cd"]]

    # A window reaching past 2199-06-30 adds the bond, not yet due.
    wide = _alert_call(conn, mod, bk["a"], "100000")
    assert wide["days_window"] == 100000
    assert [(x["id"], x["current_value"], x["is_overdue"]) for x in wide["alerts"]] == [
        (inv["tbill"], "8125.50", True),
        (inv["cd"], "5050.25", True),
        (inv["bond"], "20000.00", False),
    ]
    assert wide["alerts"][2]["days_until_maturity"] > 0
    assert (wide["alerts"][2]["days_until_maturity"]
            - wide["alerts"][0]["days_until_maturity"]) == (
        date(2199, 6, 30) - date(2025, 12, 31)).days

    rb = _alert_call(conn, mod, bk["b"], "30")
    assert [(x["id"], x["principal"]) for x in rb["alerts"]] == [
        (inv["b_cd"], "7000.00")]


def test_maturity_alerts_refusal_writes_nothing(conn, mod):
    _book(conn, mod)
    before = _counts(conn)
    r = call_action(mod.ACTIONS["treasury-investment-maturity-alerts"], conn,
                    ns(company_id=None, days="30"))
    assert is_error(r)
    assert r["message"] == "--company-id is required"
    assert _counts(conn) == before


# -- treasury-inter-company-balance-report -------------------------------------

def test_inter_company_balance_report_nets_completed_transfers_per_pair(conn, mod):
    a = _company(conn, "Northwind Holdings", "NWH")
    b = _company(conn, "Lakeside Traders", "LKT")
    c = _company(conn, "Summit Ventures", "SMV")
    d = _company(conn, "Delta Supply", "DLS")

    _completed_transfer(conn, mod, a, a, b, "25000.00", "2026-01-10")
    _completed_transfer(conn, mod, b, b, a, "7500.50", "2026-02-05")
    _completed_transfer(conn, mod, a, c, a, "1200.00", "2026-02-20")
    _completed_transfer(conn, mod, a, a, c, "1200.00", "2026-03-01")
    _completed_transfer(conn, mod, b, b, c, "4444.44", "2026-03-07")
    _completed_transfer(conn, mod, d, d, a, "300.25", "2025-12-15")
    approved = _transfer(conn, mod, a, a, b, "999.99", "2026-03-05")
    _ok(call_action(mod.ACTIONS["treasury-approve-transfer"], conn,
                    ns(transfer_id=approved)))
    cancelled = _transfer(conn, mod, b, b, a, "5000.00", "2026-03-06")
    _ok(call_action(mod.ACTIONS["treasury-cancel-transfer"], conn,
                    ns(transfer_id=cancelled)))
    _transfer(conn, mod, a, d, a, "640.00", "2026-03-08")  # stays draft

    rows = conn.execute(
        "SELECT from_company_id, to_company_id, amount, status "
        "FROM inter_company_transfer").fetchall()
    assert sorted(tuple(r) for r in rows) == sorted([
        (a, b, "25000.00", "completed"), (b, a, "7500.50", "completed"),
        (c, a, "1200.00", "completed"), (a, c, "1200.00", "completed"),
        (b, c, "4444.44", "completed"), (d, a, "300.25", "completed"),
        (a, b, "999.99", "approved"), (b, a, "5000.00", "cancelled"),
        (d, a, "640.00", "draft"),
    ])

    # A net balance is what the company received from the counterparty minus
    # what it sent to it. Net funds received are owed back: payable. Net funds
    # sent are owed to the company: receivable. Counterparties are listed in
    # the order of their first completed transfer.
    r = _ok(call_action(mod.ACTIONS["treasury-inter-company-balance-report"], conn,
                        ns(company_id=a)))
    assert r["balances"] == [
        {"company_id": d, "company_name": _company_name(conn, d),
         "net_balance": "300.25", "direction": "payable"},
        {"company_id": b, "company_name": _company_name(conn, b),
         "net_balance": "-17499.50", "direction": "receivable"},
        {"company_id": c, "company_name": _company_name(conn, c),
         "net_balance": "0.00", "direction": "settled"},
    ]
    assert (r["total_sent"], r["total_received"], r["net_position"]) == (
        "26200.00", "9000.75", "-17199.25")

    rb = _ok(call_action(mod.ACTIONS["treasury-inter-company-balance-report"], conn,
                         ns(company_id=b)))
    assert rb["balances"] == [
        {"company_id": a, "company_name": _company_name(conn, a),
         "net_balance": "17499.50", "direction": "payable"},
        {"company_id": c, "company_name": _company_name(conn, c),
         "net_balance": "-4444.44", "direction": "receivable"},
    ]
    assert (rb["total_sent"], rb["total_received"], rb["net_position"]) == (
        "11944.94", "25000.00", "13055.06")

    # Each pair reads as the mirror of the other side.
    a_with_b = next(x for x in r["balances"] if x["company_id"] == b)
    b_with_a = next(x for x in rb["balances"] if x["company_id"] == a)
    assert a_with_b["net_balance"] == "-" + b_with_a["net_balance"]


def test_inter_company_balance_report_no_transfers_and_refusal(conn, mod):
    a = _company(conn, "Northwind Holdings", "NWH")
    b = _company(conn, "Lakeside Traders", "LKT")
    _transfer(conn, mod, a, a, b, "800.00", "2026-01-15")  # draft only

    r = _ok(call_action(mod.ACTIONS["treasury-inter-company-balance-report"], conn,
                        ns(company_id=a)))
    assert (r["balances"], r["total_sent"], r["total_received"],
            r["net_position"]) == ([], "0", "0", "0")

    before = _counts(conn)
    refused = call_action(mod.ACTIONS["treasury-inter-company-balance-report"],
                          conn, ns(company_id=None))
    assert is_error(refused)
    assert refused["message"] == "--company-id is required"
    assert _counts(conn) == before
