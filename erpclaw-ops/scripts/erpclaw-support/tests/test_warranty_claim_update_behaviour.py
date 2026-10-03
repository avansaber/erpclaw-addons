"""Behaviour of update-warranty-claim, read back from the database.

The action writes the claim row (status, resolution, resolution date, cost)
and one audit row carrying the previous row, and no ledger row of any kind.
The cost is money: it is stored as the exact text given, and a value that is
not a finite, non-negative amount is refused. A closed claim refuses every
update. Every refusal leaves the claim row and the audit trail as they were.

All document dates are fixed.
"""
import json
import os
import sys
import uuid

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from support_helpers import call_action, is_error, is_ok, load_db_query, ns  # noqa: E402

M = load_db_query()

RESOLUTION_DATE = "2026-04-15"
EXPIRY_DATE = "2027-01-31"
COMPLAINT = "Motor stalls under load"


def _claim(conn, env):
    r = call_action(M.add_warranty_claim, conn, ns(
        customer_id=env["customer_id"], item_id=env["item_id"],
        warranty_expiry_date=EXPIRY_DATE, complaint_description=COMPLAINT))
    assert is_ok(r), r
    return r["warranty_claim"]["id"]


def _claim_row(conn, claim_id):
    row = conn.execute(
        "SELECT customer_id, item_id, warranty_expiry_date, complaint_description, "
        "status, resolution, resolution_date, cost FROM warranty_claim WHERE id = ?",
        (claim_id,)).fetchone()
    return tuple(row) if row else None


def _update_audits(conn, claim_id):
    return conn.execute(
        "SELECT skill, entity_type, old_values, new_values, description FROM audit_log "
        "WHERE action = 'update-warranty-claim' AND entity_id = ?",
        (claim_id,)).fetchall()


def _audit_count(conn):
    return conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]


def _ledger_counts(conn):
    return tuple(
        conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        for t in ("gl_entry", "stock_ledger_entry", "payment_ledger_entry"))


def test_update_warranty_claim_progress_resolve_close_read_back(conn, env):
    claim_id = _claim(conn, env)
    cust, item = env["customer_id"], env["item_id"]
    assert _claim_row(conn, claim_id) == (
        cust, item, EXPIRY_DATE, COMPLAINT, "open", None, None, "0")

    # In progress with a first cost estimate.
    r = call_action(M.update_warranty_claim, conn, ns(
        warranty_claim_id=claim_id, status="in_progress", cost="275.40"))
    assert is_ok(r), r
    assert _claim_row(conn, claim_id) == (
        cust, item, EXPIRY_DATE, COMPLAINT, "in_progress", None, None, "275.40")
    audits = _update_audits(conn, claim_id)
    assert len(audits) == 1
    skill, entity_type, old_values, new_values, description = tuple(audits[0])
    assert (skill, entity_type, new_values, description) == (
        "erpclaw-support", "warranty_claim", None, "Updated warranty claim")
    old = json.loads(old_values)
    assert (old["id"], old["status"], old["cost"], old["resolution"]) == (
        claim_id, "open", "0", None)

    # Resolved by repair: resolution, date and the final cost in one update.
    r = call_action(M.update_warranty_claim, conn, ns(
        warranty_claim_id=claim_id, status="resolved", resolution="repair",
        resolution_date=RESOLUTION_DATE, cost="312.75"))
    assert is_ok(r), r
    assert _claim_row(conn, claim_id) == (
        cust, item, EXPIRY_DATE, COMPLAINT, "resolved", "repair", RESOLUTION_DATE,
        "312.75")
    assert (r["warranty_claim"]["status"], r["warranty_claim"]["cost"]) == (
        "resolved", "312.75")
    audits = _update_audits(conn, claim_id)
    assert len(audits) == 2
    assert sorted((json.loads(a["old_values"])["status"], json.loads(a["old_values"])["cost"])
                  for a in audits) == [("in_progress", "275.40"), ("open", "0")]

    # A status-only update leaves the money and the resolution alone.
    r = call_action(M.update_warranty_claim, conn, ns(
        warranty_claim_id=claim_id, status="closed"))
    assert is_ok(r), r
    assert _claim_row(conn, claim_id) == (
        cust, item, EXPIRY_DATE, COMPLAINT, "closed", "repair", RESOLUTION_DATE, "312.75")
    assert len(_update_audits(conn, claim_id)) == 3

    # Resolving a claim writes no ledger of any kind.
    assert _ledger_counts(conn) == (0, 0, 0)


def test_update_warranty_claim_refuses_a_closed_claim_and_writes_nothing(conn, env):
    claim_id = _claim(conn, env)
    r = call_action(M.update_warranty_claim, conn, ns(
        warranty_claim_id=claim_id, status="closed", resolution="rejected",
        resolution_date=RESOLUTION_DATE))
    assert is_ok(r), r
    closed = _claim_row(conn, claim_id)
    assert closed[4:] == ("closed", "rejected", RESOLUTION_DATE, "0")
    audit_before = _audit_count(conn)

    for kwargs in ({"status": "open"}, {"cost": "99.00"},
                   {"resolution": "refund", "resolution_date": "2026-05-01"}):
        r = call_action(M.update_warranty_claim, conn, ns(
            warranty_claim_id=claim_id, **kwargs))
        assert is_error(r), (kwargs, r)
        assert r["message"] == "Cannot update a closed warranty claim"
        assert _claim_row(conn, claim_id) == closed

    assert _audit_count(conn) == audit_before


def test_update_warranty_claim_guards_write_nothing(conn, env):
    claim_id = _claim(conn, env)
    r = call_action(M.update_warranty_claim, conn, ns(
        warranty_claim_id=claim_id, status="in_progress", cost="40.00"))
    assert is_ok(r), r
    before = _claim_row(conn, claim_id)
    assert before[4:] == ("in_progress", None, None, "40.00")
    audit_before = _audit_count(conn)
    missing = str(uuid.uuid4())

    cases = [
        ({"warranty_claim_id": None, "status": "resolved"},
         "--warranty-claim-id is required"),
        ({"warranty_claim_id": missing, "status": "resolved"},
         f"Warranty claim {missing} not found"),
        ({"warranty_claim_id": claim_id, "status": "approved"},
         "--status must be one of ('open', 'in_progress', 'resolved', 'closed')"),
        # A valid status does not reach the row when the resolution is invalid.
        ({"warranty_claim_id": claim_id, "status": "resolved", "resolution": "bogus"},
         "--resolution must be one of ('repair', 'replace', 'refund', 'rejected')"),
        ({"warranty_claim_id": claim_id, "status": "resolved", "cost": "abc"},
         "--cost must be a valid decimal value, got: abc"),
        ({"warranty_claim_id": claim_id, "status": "resolved", "cost": "NaN"},
         "--cost must be a valid decimal value, got: NaN"),
        ({"warranty_claim_id": claim_id, "cost": "Infinity"},
         "--cost must be a valid decimal value, got: Infinity"),
        ({"warranty_claim_id": claim_id, "status": "resolved", "cost": "-10.00"},
         "--cost cannot be negative"),
        ({"warranty_claim_id": claim_id},
         "No fields to update. Provide at least one optional flag."),
    ]
    for kwargs, message in cases:
        r = call_action(M.update_warranty_claim, conn, ns(**kwargs))
        assert is_error(r), (kwargs, r)
        assert r["message"] == message
        assert _claim_row(conn, claim_id) == before

    assert _audit_count(conn) == audit_before

    # Zero is a valid cost.
    r = call_action(M.update_warranty_claim, conn, ns(
        warranty_claim_id=claim_id, cost="0.00"))
    assert is_ok(r), r
    assert _claim_row(conn, claim_id)[7] == "0.00"
    assert _audit_count(conn) == audit_before + 1
