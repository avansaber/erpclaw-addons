"""ERPClaw E-Sign -- Electronic Signature domain module.

Signature request CRUD, signing workflows, audit trail, summary reporting.
13 actions exported via ACTIONS dict.
"""
import json
import os
import sys
import uuid

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit

SKILL = "erpclaw-esign"

VALID_REQUEST_STATUSES = (
    "draft", "sent", "partially_signed", "completed",
    "declined", "cancelled", "voided", "expired",
)

VALID_EVENT_TYPES = (
    "created", "sent", "viewed", "signed", "declined",
    "cancelled", "voided", "reminded", "expired",
)


def _parse_signers(signers_str):
    """Parse and validate signers JSON string."""
    try:
        signers = json.loads(signers_str)
    except (json.JSONDecodeError, TypeError):
        err("--signers must be a valid JSON array")
    if not isinstance(signers, list) or len(signers) == 0:
        err("--signers must be a non-empty JSON array")
    for i, s in enumerate(signers):
        if not isinstance(s, dict):
            err(f"Signer at index {i} must be an object")
        if "email" not in s:
            err(f"Signer at index {i} missing 'email' field")
        # Ensure defaults
        s.setdefault("name", s["email"])
        s.setdefault("order", i + 1)
        s.setdefault("signed", False)
        s.setdefault("signed_at", None)
    return signers


def _add_event(conn, request_id, event_type, company_id,
               signer_email=None, signer_name=None, ip_address=None,
               user_agent=None, signature_data=None, notes=None):
    """Insert a signature event record."""
    event_id = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO esign_signature_event
           (id, request_id, event_type, signer_email, signer_name,
            ip_address, user_agent, signature_data, notes, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (event_id, request_id, event_type, signer_email, signer_name,
         ip_address, user_agent, signature_data, notes, company_id),
    )
    return event_id


# ---------------------------------------------------------------------------
# add-signature-request
# ---------------------------------------------------------------------------
def add_signature_request(conn, args):
    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")
    document_name = getattr(args, "document_name", None)
    if not document_name:
        err("--document-name is required")
    signers_str = getattr(args, "signers", None)
    if not signers_str:
        err("--signers is required (JSON array)")
    requested_by = getattr(args, "requested_by", None)
    if not requested_by:
        err("--requested-by is required")

    if not conn.execute(
        "SELECT id FROM company WHERE id = ?", (company_id,)
    ).fetchone():
        err(f"Company {company_id} not found")

    signers = _parse_signers(signers_str)
    document_type = getattr(args, "document_type", None) or "general"

    req_id = str(uuid.uuid4())
    ns = get_next_name(conn, "signature_request", company_id=company_id)

    conn.execute(
        """INSERT INTO esign_signature_request
           (id, naming_series, document_type, document_id, document_name,
            signers, requested_by, request_status, total_signers, signed_count,
            message, expires_at, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            req_id, ns, document_type,
            getattr(args, "document_id", None),
            document_name,
            json.dumps(signers),
            requested_by,
            "draft",
            len(signers),
            0,
            getattr(args, "message", None),
            getattr(args, "expires_at", None),
            company_id,
        ),
    )

    # Create audit event
    _add_event(conn, req_id, "created", company_id, notes=f"Request created by {requested_by}")
    audit(conn, SKILL, "esign-add-signature-request", "esign_signature_request", req_id,
          new_values={"document_name": document_name, "naming_series": ns, "total_signers": len(signers)})
    conn.commit()
    ok({"request_id": req_id, "naming_series": ns, "request_status": "draft",
        "total_signers": len(signers)})


# ---------------------------------------------------------------------------
# update-signature-request
# ---------------------------------------------------------------------------
def update_signature_request(conn, args):
    req_id = getattr(args, "request_id", None)
    if not req_id:
        err("--request-id is required")

    row = conn.execute(
        "SELECT * FROM esign_signature_request WHERE id = ?", (req_id,)
    ).fetchone()
    if not row:
        err(f"Signature request {req_id} not found")
    if row["request_status"] != "draft":
        err(f"Cannot update: request is '{row['request_status']}'. Must be draft")

    updates, params, changed = [], [], []

    for field, attr in [
        ("document_name", "document_name"),
        ("document_type", "document_type"),
        ("document_id", "document_id"),
        ("message", "message"),
        ("expires_at", "expires_at"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            updates.append(f"{field} = ?")
            params.append(val)
            changed.append(field)

    signers_str = getattr(args, "signers", None)
    if signers_str is not None:
        signers = _parse_signers(signers_str)
        updates.append("signers = ?")
        params.append(json.dumps(signers))
        updates.append("total_signers = ?")
        params.append(len(signers))
        updates.append("signed_count = 0")
        changed.append("signers")

    if not changed:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(req_id)
    conn.execute(
        f"UPDATE esign_signature_request SET {', '.join(updates)} WHERE id = ?", params
    )
    audit(conn, SKILL, "esign-update-signature-request", "esign_signature_request", req_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"request_id": req_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# get-signature-request
# ---------------------------------------------------------------------------
def get_signature_request(conn, args):
    req_id = getattr(args, "request_id", None)
    if not req_id:
        err("--request-id is required")

    row = conn.execute(
        "SELECT * FROM esign_signature_request WHERE id = ?", (req_id,)
    ).fetchone()
    if not row:
        err(f"Signature request {req_id} not found")

    data = row_to_dict(row)
    # Parse signers JSON for the response
    try:
        data["signers"] = json.loads(data["signers"]) if isinstance(data["signers"], str) else data["signers"]
    except (json.JSONDecodeError, TypeError):
        pass

    # Get events
    events = conn.execute(
        "SELECT * FROM esign_signature_event WHERE request_id = ? ORDER BY created_at ASC",
        (req_id,),
    ).fetchall()
    data["events"] = [row_to_dict(e) for e in events]
    data["event_count"] = len(events)

    ok(data)


# ---------------------------------------------------------------------------
# list-signature-requests
# ---------------------------------------------------------------------------
def list_signature_requests(conn, args):
    conditions, params = [], []

    company_id = getattr(args, "company_id", None)
    if company_id:
        conditions.append("company_id = ?")
        params.append(company_id)
    request_status = getattr(args, "request_status", None)
    if request_status:
        conditions.append("request_status = ?")
        params.append(request_status)
    requested_by = getattr(args, "requested_by", None)
    if requested_by:
        conditions.append("requested_by = ?")
        params.append(requested_by)
    document_type = getattr(args, "document_type", None)
    if document_type:
        conditions.append("document_type = ?")
        params.append(document_type)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    total = conn.execute(
        f"SELECT COUNT(*) as cnt FROM esign_signature_request {where}", params
    ).fetchone()["cnt"]

    rows = conn.execute(
        f"SELECT * FROM esign_signature_request {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    requests = []
    for r in rows:
        d = row_to_dict(r)
        try:
            d["signers"] = json.loads(d["signers"]) if isinstance(d["signers"], str) else d["signers"]
        except (json.JSONDecodeError, TypeError):
            pass
        requests.append(d)

    ok({"requests": requests, "total_count": total, "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# send-signature-request
# ---------------------------------------------------------------------------
def send_signature_request(conn, args):
    req_id = getattr(args, "request_id", None)
    if not req_id:
        err("--request-id is required")

    row = conn.execute(
        "SELECT * FROM esign_signature_request WHERE id = ?", (req_id,)
    ).fetchone()
    if not row:
        err(f"Signature request {req_id} not found")
    if row["request_status"] != "draft":
        err(f"Cannot send: request is '{row['request_status']}'. Must be draft")

    conn.execute(
        "UPDATE esign_signature_request SET request_status = 'sent', updated_at = datetime('now') WHERE id = ?",
        (req_id,),
    )
    _add_event(conn, req_id, "sent", row["company_id"],
               notes=f"Request sent to {row['total_signers']} signer(s)")
    audit(conn, SKILL, "esign-send-signature-request", "esign_signature_request", req_id,
          new_values={"request_status": "sent"})
    conn.commit()
    ok({"request_id": req_id, "request_status": "sent", "total_signers": row["total_signers"]})


# ---------------------------------------------------------------------------
# sign-document
# ---------------------------------------------------------------------------
def sign_document(conn, args):
    req_id = getattr(args, "request_id", None)
    if not req_id:
        err("--request-id is required")
    signer_email = getattr(args, "signer_email", None)
    if not signer_email:
        err("--signer-email is required")
    signature_data = getattr(args, "signature_data", None)
    if not signature_data:
        err("--signature-data is required")

    row = conn.execute(
        "SELECT * FROM esign_signature_request WHERE id = ?", (req_id,)
    ).fetchone()
    if not row:
        err(f"Signature request {req_id} not found")
    if row["request_status"] not in ("sent", "partially_signed"):
        err(f"Cannot sign: request is '{row['request_status']}'. Must be sent or partially_signed")

    # Parse signers and find the matching one
    try:
        signers = json.loads(row["signers"])
    except (json.JSONDecodeError, TypeError):
        err("Invalid signers data in request")

    found = False
    signer_name = None
    for s in signers:
        if s["email"] == signer_email:
            if s.get("signed"):
                err(f"Signer {signer_email} has already signed")
            s["signed"] = True
            s["signed_at"] = "now"
            signer_name = s.get("name", signer_email)
            found = True
            break

    if not found:
        err(f"Signer {signer_email} not found in request signers")

    signed_count = row["signed_count"] + 1
    total_signers = row["total_signers"]

    # Determine new status
    if signed_count >= total_signers:
        new_status = "completed"
    else:
        new_status = "partially_signed"

    completed_at_clause = ", completed_at = datetime('now')" if new_status == "completed" else ""

    conn.execute(
        f"""UPDATE esign_signature_request
            SET signers = ?, signed_count = ?, request_status = ?,
                updated_at = datetime('now'){completed_at_clause}
            WHERE id = ?""",
        (json.dumps(signers), signed_count, new_status, req_id),
    )

    ip_address = getattr(args, "ip_address", None)
    user_agent = getattr(args, "user_agent", None)

    _add_event(conn, req_id, "signed", row["company_id"],
               signer_email=signer_email, signer_name=signer_name,
               ip_address=ip_address, user_agent=user_agent,
               signature_data=signature_data)
    audit(conn, SKILL, "esign-sign-document", "esign_signature_request", req_id,
          new_values={"signer_email": signer_email, "signed_count": signed_count,
                      "request_status": new_status})
    conn.commit()
    ok({"request_id": req_id, "signer_email": signer_email,
        "signed_count": signed_count, "total_signers": total_signers,
        "request_status": new_status})


# ---------------------------------------------------------------------------
# decline-signature
# ---------------------------------------------------------------------------
def decline_signature(conn, args):
    req_id = getattr(args, "request_id", None)
    if not req_id:
        err("--request-id is required")
    signer_email = getattr(args, "signer_email", None)
    if not signer_email:
        err("--signer-email is required")

    row = conn.execute(
        "SELECT * FROM esign_signature_request WHERE id = ?", (req_id,)
    ).fetchone()
    if not row:
        err(f"Signature request {req_id} not found")
    if row["request_status"] not in ("sent", "partially_signed"):
        err(f"Cannot decline: request is '{row['request_status']}'. Must be sent or partially_signed")

    # Verify signer exists in the request
    try:
        signers = json.loads(row["signers"])
    except (json.JSONDecodeError, TypeError):
        err("Invalid signers data in request")

    found = False
    signer_name = None
    for s in signers:
        if s["email"] == signer_email:
            if s.get("signed"):
                err(f"Signer {signer_email} has already signed and cannot decline")
            signer_name = s.get("name", signer_email)
            found = True
            break

    if not found:
        err(f"Signer {signer_email} not found in request signers")

    conn.execute(
        "UPDATE esign_signature_request SET request_status = 'declined', updated_at = datetime('now') WHERE id = ?",
        (req_id,),
    )

    notes = getattr(args, "notes", None)
    ip_address = getattr(args, "ip_address", None)
    user_agent = getattr(args, "user_agent", None)

    _add_event(conn, req_id, "declined", row["company_id"],
               signer_email=signer_email, signer_name=signer_name,
               ip_address=ip_address, user_agent=user_agent, notes=notes)
    audit(conn, SKILL, "esign-decline-signature", "esign_signature_request", req_id,
          new_values={"signer_email": signer_email, "request_status": "declined"})
    conn.commit()
    ok({"request_id": req_id, "signer_email": signer_email, "request_status": "declined"})


# ---------------------------------------------------------------------------
# cancel-signature-request
# ---------------------------------------------------------------------------
def cancel_signature_request(conn, args):
    req_id = getattr(args, "request_id", None)
    if not req_id:
        err("--request-id is required")

    row = conn.execute(
        "SELECT * FROM esign_signature_request WHERE id = ?", (req_id,)
    ).fetchone()
    if not row:
        err(f"Signature request {req_id} not found")
    if row["request_status"] in ("completed", "cancelled", "voided"):
        err(f"Cannot cancel: request is '{row['request_status']}'")

    conn.execute(
        "UPDATE esign_signature_request SET request_status = 'cancelled', updated_at = datetime('now') WHERE id = ?",
        (req_id,),
    )

    notes = getattr(args, "notes", None)
    _add_event(conn, req_id, "cancelled", row["company_id"], notes=notes)
    audit(conn, SKILL, "esign-cancel-signature-request", "esign_signature_request", req_id,
          new_values={"request_status": "cancelled"})
    conn.commit()
    ok({"request_id": req_id, "request_status": "cancelled"})


# ---------------------------------------------------------------------------
# void-signature-request
# ---------------------------------------------------------------------------
def void_signature_request(conn, args):
    req_id = getattr(args, "request_id", None)
    if not req_id:
        err("--request-id is required")

    row = conn.execute(
        "SELECT * FROM esign_signature_request WHERE id = ?", (req_id,)
    ).fetchone()
    if not row:
        err(f"Signature request {req_id} not found")
    if row["request_status"] not in ("completed", "sent", "partially_signed"):
        err(f"Cannot void: request is '{row['request_status']}'. Must be completed, sent, or partially_signed")

    conn.execute(
        "UPDATE esign_signature_request SET request_status = 'voided', updated_at = datetime('now') WHERE id = ?",
        (req_id,),
    )

    notes = getattr(args, "notes", None)
    _add_event(conn, req_id, "voided", row["company_id"], notes=notes)
    audit(conn, SKILL, "esign-void-signature-request", "esign_signature_request", req_id,
          new_values={"request_status": "voided"})
    conn.commit()
    ok({"request_id": req_id, "request_status": "voided"})


# ---------------------------------------------------------------------------
# add-reminder
# ---------------------------------------------------------------------------
def add_reminder(conn, args):
    req_id = getattr(args, "request_id", None)
    if not req_id:
        err("--request-id is required")

    row = conn.execute(
        "SELECT * FROM esign_signature_request WHERE id = ?", (req_id,)
    ).fetchone()
    if not row:
        err(f"Signature request {req_id} not found")
    if row["request_status"] not in ("sent", "partially_signed"):
        err(f"Cannot remind: request is '{row['request_status']}'. Must be sent or partially_signed")

    signer_email = getattr(args, "signer_email", None)
    notes = getattr(args, "notes", None) or "Reminder sent for pending signature"

    _add_event(conn, req_id, "reminded", row["company_id"],
               signer_email=signer_email, notes=notes)
    audit(conn, SKILL, "esign-add-reminder", "esign_signature_request", req_id,
          new_values={"signer_email": signer_email, "reminder": True})
    conn.commit()
    ok({"request_id": req_id, "reminder_sent": True, "signer_email": signer_email})


# ---------------------------------------------------------------------------
# get-signature-audit-trail
# ---------------------------------------------------------------------------
def get_signature_audit_trail(conn, args):
    req_id = getattr(args, "request_id", None)
    if not req_id:
        err("--request-id is required")

    row = conn.execute(
        "SELECT id, naming_series, document_name, request_status FROM esign_signature_request WHERE id = ?",
        (req_id,),
    ).fetchone()
    if not row:
        err(f"Signature request {req_id} not found")

    events = conn.execute(
        "SELECT * FROM esign_signature_event WHERE request_id = ? ORDER BY created_at ASC",
        (req_id,),
    ).fetchall()

    event_list = [row_to_dict(e) for e in events]

    ok({
        "request_id": req_id,
        "naming_series": row["naming_series"],
        "document_name": row["document_name"],
        "request_status": row["request_status"],
        "events": event_list,
        "event_count": len(event_list),
    })


# ---------------------------------------------------------------------------
# signature-summary-report
# ---------------------------------------------------------------------------
def signature_summary_report(conn, args):
    company_id = getattr(args, "company_id", None)
    if not company_id:
        err("--company-id is required")

    # Status breakdown
    rows = conn.execute(
        """SELECT request_status, COUNT(*) as cnt
           FROM esign_signature_request
           WHERE company_id = ?
           GROUP BY request_status""",
        (company_id,),
    ).fetchall()

    by_status = {}
    total_requests = 0
    for r in rows:
        by_status[r["request_status"]] = r["cnt"]
        total_requests += r["cnt"]

    # Average completion time (completed requests only)
    avg_row = conn.execute(
        """SELECT AVG(
               CAST((julianday(completed_at) - julianday(created_at)) * 24 AS REAL)
           ) as avg_hours
           FROM esign_signature_request
           WHERE company_id = ? AND request_status = 'completed' AND completed_at IS NOT NULL""",
        (company_id,),
    ).fetchone()
    avg_completion_hours = round(avg_row["avg_hours"], 2) if avg_row and avg_row["avg_hours"] is not None else None

    # Total signatures
    sig_count = conn.execute(
        """SELECT COUNT(*) as cnt FROM esign_signature_event
           WHERE company_id = ? AND event_type = 'signed'""",
        (company_id,),
    ).fetchone()["cnt"]

    ok({
        "company_id": company_id,
        "total_requests": total_requests,
        "by_status": by_status,
        "total_signatures": sig_count,
        "avg_completion_hours": avg_completion_hours,
    })


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
def module_status(conn, args):
    tables = []
    for tbl in ["esign_signature_request", "esign_signature_event"]:
        row = conn.execute(
            f"SELECT COUNT(*) as cnt FROM {tbl}"
        ).fetchone()
        tables.append(tbl)

    ok({
        "skill": SKILL,
        "version": "1.0.0",
        "tables": tables,
        "actions_available": len(ACTIONS),
    })


# ---------------------------------------------------------------------------
# ACTIONS registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "esign-add-signature-request": add_signature_request,
    "esign-update-signature-request": update_signature_request,
    "esign-get-signature-request": get_signature_request,
    "esign-list-signature-requests": list_signature_requests,
    "esign-send-signature-request": send_signature_request,
    "esign-sign-document": sign_document,
    "esign-decline-signature": decline_signature,
    "esign-cancel-signature-request": cancel_signature_request,
    "esign-void-signature-request": void_signature_request,
    "esign-add-reminder": add_reminder,
    "esign-get-signature-audit-trail": get_signature_audit_trail,
    "esign-signature-summary-report": signature_summary_report,
    "status": module_status,
}
