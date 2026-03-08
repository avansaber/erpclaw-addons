"""erpclaw-crm-adv -- contracts domain module

Actions for contract lifecycle management and obligations (2 tables, 10 actions).
Imported by db_query.py (unified router).
"""
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.naming import get_next_name, ENTITY_PREFIXES
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.decimal_utils import to_decimal, round_currency

    ENTITY_PREFIXES.setdefault("crmadv_contract", "CTR-")
except ImportError:
    pass

SKILL = "erpclaw-crm-adv"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_CONTRACT_TYPES = ("service", "subscription", "licensing", "license", "maintenance", "consulting")
VALID_CONTRACT_STATUSES = ("draft", "active", "renewed", "terminated", "expired")
VALID_OBLIGEES = ("us", "customer")
VALID_OBLIGATION_STATUSES = ("pending", "completed", "overdue", "waived")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute("SELECT id FROM company WHERE id = ?", (company_id,)).fetchone():
        err(f"Company {company_id} not found")


# ===========================================================================
# 1. add-contract
# ===========================================================================
def add_contract(conn, args):
    _validate_company(conn, args.company_id)
    customer_name = getattr(args, "customer_name", None)
    if not customer_name:
        err("--customer-name is required")

    contract_type = getattr(args, "contract_type", None) or "service"
    if contract_type not in VALID_CONTRACT_TYPES:
        err(f"Invalid contract_type: {contract_type}. Must be one of: {', '.join(VALID_CONTRACT_TYPES)}")

    ctr_id = str(uuid.uuid4())
    now = _now_iso()
    conn.company_id = args.company_id
    naming = get_next_name(conn, "crmadv_contract")

    conn.execute("""
        INSERT INTO crmadv_contract (
            id, naming_series, customer_name, contract_type, start_date, end_date,
            total_value, annual_value, auto_renew, renewal_terms,
            contract_status, company_id, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        ctr_id, naming, customer_name, contract_type,
        getattr(args, "start_date", None),
        getattr(args, "end_date", None),
        getattr(args, "total_value", None),
        getattr(args, "annual_value", None),
        1 if getattr(args, "auto_renew", None) == "1" else 0,
        getattr(args, "renewal_terms", None),
        "draft", args.company_id, now, now,
    ))
    audit(conn, SKILL, "add-contract", "crmadv_contract", ctr_id,
          new_values={"customer_name": customer_name, "contract_type": contract_type})
    conn.commit()
    ok({"id": ctr_id, "naming_series": naming, "customer_name": customer_name,
        "contract_type": contract_type, "contract_status": "draft"})


# ===========================================================================
# 2. update-contract
# ===========================================================================
def update_contract(conn, args):
    ctr_id = getattr(args, "contract_id", None)
    if not ctr_id:
        err("--contract-id is required")
    row = conn.execute("SELECT * FROM crmadv_contract WHERE id = ?", (ctr_id,)).fetchone()
    if not row:
        err(f"Contract {ctr_id} not found")

    d = row_to_dict(row)
    if d["contract_status"] == "terminated":
        err("Cannot update a terminated contract")

    updates, params, changed = [], [], []
    for arg_name, col_name in {
        "customer_name": "customer_name", "contract_type": "contract_type",
        "start_date": "start_date", "end_date": "end_date",
        "total_value": "total_value", "annual_value": "annual_value",
        "renewal_terms": "renewal_terms",
    }.items():
        val = getattr(args, arg_name, None)
        if val is not None:
            updates.append(f"{col_name} = ?")
            params.append(val)
            changed.append(col_name)

    if not updates:
        err("No fields to update")

    updates.append("updated_at = ?")
    params.append(_now_iso())
    params.append(ctr_id)
    conn.execute(f"UPDATE crmadv_contract SET {', '.join(updates)} WHERE id = ?", params)
    audit(conn, SKILL, "update-contract", "crmadv_contract", ctr_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"id": ctr_id, "updated_fields": changed})


# ===========================================================================
# 3. get-contract
# ===========================================================================
def get_contract(conn, args):
    ctr_id = getattr(args, "contract_id", None)
    if not ctr_id:
        err("--contract-id is required")
    row = conn.execute("SELECT * FROM crmadv_contract WHERE id = ?", (ctr_id,)).fetchone()
    if not row:
        err(f"Contract {ctr_id} not found")
    ok(row_to_dict(row))


# ===========================================================================
# 4. list-contracts
# ===========================================================================
def list_contracts(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "contract_type", None):
        where.append("contract_type = ?")
        params.append(args.contract_type)
    if getattr(args, "contract_status_filter", None):
        where.append("contract_status = ?")
        params.append(args.contract_status_filter)
    if getattr(args, "search", None):
        where.append("customer_name LIKE ?")
        params.append(f"%{args.search}%")

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM crmadv_contract WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM crmadv_contract WHERE {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 5. add-contract-obligation
# ===========================================================================
def add_contract_obligation(conn, args):
    contract_id = getattr(args, "contract_id", None)
    if not contract_id:
        err("--contract-id is required")
    if not conn.execute("SELECT id FROM crmadv_contract WHERE id = ?", (contract_id,)).fetchone():
        err(f"Contract {contract_id} not found")

    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")

    description = getattr(args, "description", None)
    if not description:
        err("--description is required")

    obligee = getattr(args, "obligee", None) or "us"
    if obligee not in VALID_OBLIGEES:
        err(f"Invalid obligee: {obligee}. Must be one of: {', '.join(VALID_OBLIGEES)}")

    ob_id = str(uuid.uuid4())
    now = _now_iso()

    conn.execute("""
        INSERT INTO crmadv_contract_obligation (
            id, contract_id, description, due_date,
            obligee, obligation_status, company_id, created_at
        ) VALUES (?,?,?,?,?,?,?,?)
    """, (
        ob_id, contract_id, description,
        getattr(args, "due_date", None),
        obligee, "pending", company_id, now,
    ))
    audit(conn, SKILL, "add-contract-obligation", "crmadv_contract_obligation", ob_id,
          new_values={"contract_id": contract_id, "description": description})
    conn.commit()
    ok({"id": ob_id, "contract_id": contract_id, "description": description,
        "obligee": obligee, "obligation_status": "pending"})


# ===========================================================================
# 6. list-contract-obligations
# ===========================================================================
def list_contract_obligations(conn, args):
    where, params = ["1=1"], []
    if getattr(args, "contract_id", None):
        where.append("contract_id = ?")
        params.append(args.contract_id)
    if getattr(args, "company_id", None):
        where.append("company_id = ?")
        params.append(args.company_id)
    if getattr(args, "obligation_status_filter", None):
        where.append("obligation_status = ?")
        params.append(args.obligation_status_filter)

    where_sql = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) FROM crmadv_contract_obligation WHERE {where_sql}", params
    ).fetchone()[0]
    params.extend([args.limit, args.offset])
    rows = conn.execute(
        f"SELECT * FROM crmadv_contract_obligation WHERE {where_sql} ORDER BY due_date ASC LIMIT ? OFFSET ?",
        params
    ).fetchall()
    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 7. renew-contract
# ===========================================================================
def renew_contract(conn, args):
    ctr_id = getattr(args, "contract_id", None)
    if not ctr_id:
        err("--contract-id is required")
    row = conn.execute("SELECT * FROM crmadv_contract WHERE id = ?", (ctr_id,)).fetchone()
    if not row:
        err(f"Contract {ctr_id} not found")

    d = row_to_dict(row)
    if d["contract_status"] not in ("draft", "active", "expired"):
        err(f"Cannot renew contract in '{d['contract_status']}' status. Must be draft, active, or expired.")

    now = _now_iso()
    new_end_date = getattr(args, "end_date", None)

    conn.execute("""
        UPDATE crmadv_contract
        SET contract_status = 'renewed', end_date = COALESCE(?, end_date), updated_at = ?
        WHERE id = ?
    """, (new_end_date, now, ctr_id))
    audit(conn, SKILL, "renew-contract", "crmadv_contract", ctr_id,
          new_values={"contract_status": "renewed"})
    conn.commit()
    ok({"id": ctr_id, "contract_status": "renewed"})


# ===========================================================================
# 8. terminate-contract
# ===========================================================================
def terminate_contract(conn, args):
    ctr_id = getattr(args, "contract_id", None)
    if not ctr_id:
        err("--contract-id is required")
    row = conn.execute("SELECT * FROM crmadv_contract WHERE id = ?", (ctr_id,)).fetchone()
    if not row:
        err(f"Contract {ctr_id} not found")

    d = row_to_dict(row)
    if d["contract_status"] in ("terminated",):
        err("Contract is already terminated")

    now = _now_iso()
    conn.execute("""
        UPDATE crmadv_contract
        SET contract_status = 'terminated', updated_at = ?
        WHERE id = ?
    """, (now, ctr_id))
    audit(conn, SKILL, "terminate-contract", "crmadv_contract", ctr_id,
          new_values={"contract_status": "terminated"})
    conn.commit()
    ok({"id": ctr_id, "contract_status": "terminated"})


# ===========================================================================
# 9. contract-expiry-report
# ===========================================================================
def contract_expiry_report(conn, args):
    _validate_company(conn, args.company_id)

    rows = conn.execute("""
        SELECT id, naming_series, customer_name, contract_type, contract_status,
               start_date, end_date, total_value, annual_value, auto_renew
        FROM crmadv_contract
        WHERE company_id = ? AND contract_status NOT IN ('terminated')
        ORDER BY end_date ASC
        LIMIT ? OFFSET ?
    """, (args.company_id, args.limit, args.offset)).fetchall()

    total = conn.execute(
        "SELECT COUNT(*) FROM crmadv_contract WHERE company_id = ? AND contract_status NOT IN ('terminated')",
        (args.company_id,)
    ).fetchone()[0]

    ok({
        "rows": [row_to_dict(r) for r in rows],
        "total_count": total, "limit": args.limit, "offset": args.offset,
        "has_more": (args.offset + args.limit) < total,
    })


# ===========================================================================
# 10. contract-value-report
# ===========================================================================
def contract_value_report(conn, args):
    _validate_company(conn, args.company_id)

    total_value_row = conn.execute("""
        SELECT COALESCE(SUM(CAST(total_value AS REAL)), 0),
               COALESCE(SUM(CAST(annual_value AS REAL)), 0),
               COUNT(*)
        FROM crmadv_contract
        WHERE company_id = ? AND contract_status NOT IN ('terminated')
    """, (args.company_id,)).fetchone()

    by_type = conn.execute("""
        SELECT contract_type, COUNT(*) as count,
               COALESCE(SUM(CAST(total_value AS REAL)), 0) as type_total_value
        FROM crmadv_contract
        WHERE company_id = ? AND contract_status NOT IN ('terminated')
        GROUP BY contract_type
        ORDER BY type_total_value DESC
    """, (args.company_id,)).fetchall()

    ok({
        "total_contract_value": str(round_currency(to_decimal(str(total_value_row[0])))),
        "total_annual_value": str(round_currency(to_decimal(str(total_value_row[1])))),
        "total_contracts": total_value_row[2],
        "by_type": [row_to_dict(r) for r in by_type],
    })


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "add-contract": add_contract,
    "update-contract": update_contract,
    "get-contract": get_contract,
    "list-contracts": list_contracts,
    "add-contract-obligation": add_contract_obligation,
    "list-contract-obligations": list_contract_obligations,
    "renew-contract": renew_contract,
    "terminate-contract": terminate_contract,
    "contract-expiry-report": contract_expiry_report,
    "contract-value-report": contract_value_report,
}
