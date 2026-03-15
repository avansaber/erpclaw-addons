"""ERPClaw Treasury -- Inter-Company Transfers domain module.

Fund transfers between companies.
8 actions exported via ACTIONS dict.
"""
import os
import sys
import uuid
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name, register_prefix
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit
from erpclaw_lib.db import DEFAULT_DB_PATH
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row

register_prefix("inter_company_transfer", "ICT-")

SKILL = "erpclaw-treasury"

VALID_TRANSFER_STATUSES = ("draft", "approved", "completed", "cancelled")


# ---------------------------------------------------------------------------
# add-inter-company-transfer
# ---------------------------------------------------------------------------
def add_inter_company_transfer(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "from_company_id", None):
        err("--from-company-id is required")
    if not getattr(args, "to_company_id", None):
        err("--to-company-id is required")

    amount = getattr(args, "amount", None)
    if not amount:
        err("--amount is required")
    try:
        amt = Decimal(amount)
    except Exception:
        err(f"Invalid amount: {amount}")
    if amt <= 0:
        err("Amount must be positive")

    # Validate all three company IDs
    for label, cid in [("company", args.company_id),
                       ("from-company", args.from_company_id),
                       ("to-company", args.to_company_id)]:
        if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (cid,)).fetchone():
            err(f"{label} {cid} not found")

    if args.from_company_id == args.to_company_id:
        err("From and To company must be different")

    xfer_id = str(uuid.uuid4())
    ns = get_next_name(conn, "inter_company_transfer", company_id=args.company_id)

    conn.execute(
        """INSERT INTO inter_company_transfer
           (id, naming_series, from_company_id, to_company_id, amount,
            transfer_date, reference, reason, status, company_id)
           VALUES (?,?,?,?,?,COALESCE(?,date('now')),?,?,?,?)""",
        (
            xfer_id, ns, args.from_company_id, args.to_company_id, amount,
            getattr(args, "transfer_date", None),
            getattr(args, "reference", None),
            getattr(args, "reason", None),
            "draft",
            args.company_id,
        ),
    )
    audit(conn, SKILL, "treasury-add-inter-company-transfer", "inter_company_transfer", xfer_id,
          new_values={"naming_series": ns, "amount": amount})
    conn.commit()
    ok({"transfer_id": xfer_id, "naming_series": ns, "transfer_status": "draft"})


# ---------------------------------------------------------------------------
# get-inter-company-transfer
# ---------------------------------------------------------------------------
def get_inter_company_transfer(conn, args):
    xfer_id = getattr(args, "transfer_id", None)
    if not xfer_id:
        err("--transfer-id is required")
    row = conn.execute(Q.from_(Table("inter_company_transfer")).select(Table("inter_company_transfer").star).where(Field("id") == P()).get_sql(), (xfer_id,)).fetchone()
    if not row:
        err(f"Transfer {xfer_id} not found")

    data = row_to_dict(row)
    # Enrich with company names
    for field in ("from_company_id", "to_company_id"):
        c = conn.execute(
            "SELECT name FROM company WHERE id = ?", (data[field],)
        ).fetchone()
        data[field.replace("_id", "_name")] = c[0] if c else None

    ok(data)


# ---------------------------------------------------------------------------
# list-inter-company-transfers
# ---------------------------------------------------------------------------
def list_inter_company_transfers(conn, args):
    query = "SELECT * FROM inter_company_transfer WHERE 1=1"
    params = []

    company_id = getattr(args, "company_id", None)
    if company_id:
        query += " AND company_id = ?"
        params.append(company_id)

    transfer_status = getattr(args, "transfer_status", None)
    if transfer_status:
        query += " AND status = ?"
        params.append(transfer_status)

    from_id = getattr(args, "from_company_id", None)
    if from_id:
        query += " AND from_company_id = ?"
        params.append(from_id)

    to_id = getattr(args, "to_company_id", None)
    if to_id:
        query += " AND to_company_id = ?"
        params.append(to_id)

    search = getattr(args, "search", None)
    if search:
        query += " AND (reference LIKE ? OR reason LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])

    count_q = query.replace("SELECT *", "SELECT COUNT(*)", 1)
    total = conn.execute(count_q, params).fetchone()[0]

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0
    query += " ORDER BY transfer_date DESC, created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = conn.execute(query, params).fetchall()
    ok({"transfers": [row_to_dict(r) for r in rows], "total_count": total})


# ---------------------------------------------------------------------------
# approve-transfer
# ---------------------------------------------------------------------------
def approve_transfer(conn, args):
    xfer_id = getattr(args, "transfer_id", None)
    if not xfer_id:
        err("--transfer-id is required")
    row = conn.execute(Q.from_(Table("inter_company_transfer")).select(Table("inter_company_transfer").star).where(Field("id") == P()).get_sql(), (xfer_id,)).fetchone()
    if not row:
        err(f"Transfer {xfer_id} not found")

    data = row_to_dict(row)
    if data["status"] != "draft":
        err(f"Can only approve draft transfers, current status: {data['status']}")

    conn.execute(
        "UPDATE inter_company_transfer SET status = 'approved', updated_at = datetime('now') WHERE id = ?",
        (xfer_id,),
    )
    audit(conn, SKILL, "treasury-approve-transfer", "inter_company_transfer", xfer_id,
          old_values={"transfer_status": "draft"}, new_values={"transfer_status": "approved"})
    conn.commit()
    ok({"transfer_id": xfer_id, "transfer_status": "approved"})


# ---------------------------------------------------------------------------
# complete-transfer
# ---------------------------------------------------------------------------
def complete_transfer(conn, args):
    """Complete the transfer. Adjusts bank account balances if accounts are specified."""
    xfer_id = getattr(args, "transfer_id", None)
    if not xfer_id:
        err("--transfer-id is required")
    row = conn.execute(Q.from_(Table("inter_company_transfer")).select(Table("inter_company_transfer").star).where(Field("id") == P()).get_sql(), (xfer_id,)).fetchone()
    if not row:
        err(f"Transfer {xfer_id} not found")

    data = row_to_dict(row)
    if data["status"] != "approved":
        err(f"Can only complete approved transfers, current status: {data['status']}")

    amount = Decimal(data["amount"])

    # Optionally adjust bank account balances if from/to account IDs are provided
    from_acct = getattr(args, "from_account_id", None)
    to_acct = getattr(args, "to_account_id", None)

    if from_acct:
        fa = conn.execute(
            "SELECT current_balance FROM bank_account_extended WHERE id = ?",
            (from_acct,),
        ).fetchone()
        if fa:
            new_bal = Decimal(fa[0] or "0") - amount
            conn.execute(
                "UPDATE bank_account_extended SET current_balance = ?, updated_at = datetime('now') WHERE id = ?",
                (str(new_bal), from_acct),
            )

    if to_acct:
        ta = conn.execute(
            "SELECT current_balance FROM bank_account_extended WHERE id = ?",
            (to_acct,),
        ).fetchone()
        if ta:
            new_bal = Decimal(ta[0] or "0") + amount
            conn.execute(
                "UPDATE bank_account_extended SET current_balance = ?, updated_at = datetime('now') WHERE id = ?",
                (str(new_bal), to_acct),
            )

    conn.execute(
        "UPDATE inter_company_transfer SET status = 'completed', updated_at = datetime('now') WHERE id = ?",
        (xfer_id,),
    )
    audit(conn, SKILL, "treasury-complete-transfer", "inter_company_transfer", xfer_id,
          old_values={"transfer_status": "approved"}, new_values={"transfer_status": "completed"})
    conn.commit()
    ok({"transfer_id": xfer_id, "transfer_status": "completed", "amount": str(amount)})


# ---------------------------------------------------------------------------
# cancel-transfer
# ---------------------------------------------------------------------------
def cancel_transfer(conn, args):
    xfer_id = getattr(args, "transfer_id", None)
    if not xfer_id:
        err("--transfer-id is required")
    row = conn.execute(Q.from_(Table("inter_company_transfer")).select(Table("inter_company_transfer").star).where(Field("id") == P()).get_sql(), (xfer_id,)).fetchone()
    if not row:
        err(f"Transfer {xfer_id} not found")

    data = row_to_dict(row)
    if data["status"] == "completed":
        err("Cannot cancel a completed transfer")
    if data["status"] == "cancelled":
        err("Transfer is already cancelled")

    conn.execute(
        "UPDATE inter_company_transfer SET status = 'cancelled', updated_at = datetime('now') WHERE id = ?",
        (xfer_id,),
    )
    audit(conn, SKILL, "treasury-cancel-transfer", "inter_company_transfer", xfer_id,
          old_values={"transfer_status": data["status"]}, new_values={"transfer_status": "cancelled"})
    conn.commit()
    ok({"transfer_id": xfer_id, "transfer_status": "cancelled"})


# ---------------------------------------------------------------------------
# inter-company-balance-report
# ---------------------------------------------------------------------------
def inter_company_balance_report(conn, args):
    """Report on net transfers between companies."""
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    # Get completed transfers involving this company
    rows = conn.execute(
        """SELECT from_company_id, to_company_id, amount
           FROM inter_company_transfer
           WHERE status = 'completed'
             AND (from_company_id = ? OR to_company_id = ?)
           ORDER BY transfer_date""",
        (args.company_id, args.company_id),
    ).fetchall()

    # Net balances with each counterparty
    balances = {}
    total_sent = Decimal("0")
    total_received = Decimal("0")

    for r in rows:
        d = row_to_dict(r)
        amt = Decimal(d["amount"] or "0")
        if d["from_company_id"] == args.company_id:
            # Outgoing
            counterparty = d["to_company_id"]
            total_sent += amt
            balances[counterparty] = balances.get(counterparty, Decimal("0")) - amt
        else:
            # Incoming
            counterparty = d["from_company_id"]
            total_received += amt
            balances[counterparty] = balances.get(counterparty, Decimal("0")) + amt

    # Enrich with company names
    balance_list = []
    for cid, net in balances.items():
        c = conn.execute(Q.from_(Table("company")).select(Field('name')).where(Field("id") == P()).get_sql(), (cid,)).fetchone()
        balance_list.append({
            "company_id": cid,
            "company_name": c[0] if c else None,
            "net_balance": str(net),
            "direction": "receivable" if net > 0 else "payable" if net < 0 else "settled",
        })

    ok({
        "balances": balance_list,
        "total_sent": str(total_sent),
        "total_received": str(total_received),
        "net_position": str(total_received - total_sent),
    })


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
def _status(conn, args):
    ok({
        "skill": SKILL,
        "version": "1.0.0",
        "actions_available": len(ACTIONS) - 1,
        "domains": ["cash", "investments", "intercompany"],
        "tables": [
            "bank_account_extended", "cash_position", "cash_forecast",
            "investment", "investment_transaction", "inter_company_transfer",
        ],
        "database": DEFAULT_DB_PATH,
    })


# ---------------------------------------------------------------------------
# ACTIONS export
# ---------------------------------------------------------------------------
ACTIONS = {
    "treasury-add-inter-company-transfer": add_inter_company_transfer,
    "treasury-get-inter-company-transfer": get_inter_company_transfer,
    "treasury-list-inter-company-transfers": list_inter_company_transfers,
    "treasury-approve-transfer": approve_transfer,
    "treasury-complete-transfer": complete_transfer,
    "treasury-cancel-transfer": cancel_transfer,
    "treasury-inter-company-balance-report": inter_company_balance_report,
    "status": _status,
}
