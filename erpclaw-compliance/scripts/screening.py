"""ERPClaw Compliance: exclusion screening domain module

Deterministic exact-identifier exclusion/debarment screening (v1).
Imported by db_query.py (unified router).
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone

try:
    import importlib.util
    if importlib.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.response import ok, err
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Field, P, Q, Table, insert_row
except ImportError:
    pass

SKILL = "erpclaw-compliance"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_PARTY_TYPES = ("supplier", "employee", "subrecipient", "provider")
VALID_MATCH_STATUSES = ("clear", "matched")


def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _normalize(identifier):
    return identifier.strip().upper()


def screen_exclusion(conn, args):
    _validate_company(conn, getattr(args, "company_id", None))
    company_id = args.company_id

    party_type = getattr(args, "party_type", None)
    if not party_type:
        err("--party-type is required")
    if party_type not in VALID_PARTY_TYPES:
        err(f"Invalid party-type: {party_type}. Must be one of: {', '.join(VALID_PARTY_TYPES)}")

    party_id = getattr(args, "party_id", None)
    if not party_id:
        err("--party-id is required")

    raw_candidate = getattr(args, "candidate_identifier", None)
    if raw_candidate is None or (isinstance(raw_candidate, str) and raw_candidate.strip() == ""):
        err("--candidate-identifier is required")
    if not isinstance(raw_candidate, str):
        err("--candidate-identifier is required")
    candidate = _normalize(raw_candidate)
    if not candidate:
        err("--candidate-identifier is required")

    list_source = getattr(args, "list_source", None)
    if not list_source:
        err("--list-source is required")

    list_version = getattr(args, "list_version", None)
    if not list_version:
        err("--list-version is required")

    evidence_reference = getattr(args, "evidence_reference", None)
    if not evidence_reference:
        err("--evidence-reference is required")

    raw_excluded = getattr(args, "excluded_identifiers", None)
    if raw_excluded is None:
        err("--excluded-identifiers is required")
    if isinstance(raw_excluded, list):
        members = raw_excluded
    elif isinstance(raw_excluded, str):
        if raw_excluded.strip() == "":
            err("--excluded-identifiers is required")
        try:
            members = json.loads(raw_excluded)
        except (json.JSONDecodeError, ValueError, TypeError):
            err("Invalid --excluded-identifiers: must be a JSON array of strings")
    else:
        err("Invalid --excluded-identifiers: must be a JSON array of strings")
    if not isinstance(members, list):
        err("Invalid --excluded-identifiers: must be a JSON array of strings")
    if len(members) == 0:
        err("--excluded-identifiers must not be empty")
    for member in members:
        if not isinstance(member, str):
            err("Invalid --excluded-identifiers: every member must be a string")
    normalized_set = set(_normalize(m) for m in members)

    if candidate in normalized_set:
        match_status = "matched"
        matched_identifier = candidate
        payment_blocked = True
    else:
        match_status = "clear"
        matched_identifier = None
        payment_blocked = False

    screening_id = str(uuid.uuid4())
    now = _now_iso()
    try:
        sql, _ = insert_row("compliance_exclusion_screening", {
            "id": P(), "company_id": P(), "party_type": P(),
            "party_id": P(), "candidate_identifier": P(),
            "list_source": P(), "list_version": P(),
            "screened_at": P(), "match_status": P(),
            "matched_identifier": P(), "evidence_reference": P(),
            "created_at": P(),
        })
        conn.execute(sql, (
            screening_id, company_id, party_type,
            party_id, candidate,
            list_source, list_version,
            now, match_status,
            matched_identifier, evidence_reference,
            now,
        ))
        audit(conn, SKILL, "compliance-screen-exclusion", "compliance_exclusion_screening", screening_id)
        conn.commit()
    except SystemExit:
        raise
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        err(str(e))

    result = {
        "id": screening_id,
        "screening_id": screening_id,
        "company_id": company_id,
        "party_type": party_type,
        "party_id": party_id,
        "candidate_identifier": candidate,
        "list_source": list_source,
        "list_version": list_version,
        "screened_at": now,
        "match_status": match_status,
        "evidence_reference": evidence_reference,
        "payment_blocked": payment_blocked,
    }
    if matched_identifier is not None:
        result["matched_identifier"] = matched_identifier
    ok(result)


ACTIONS = {
    "compliance-screen-exclusion": screen_exclusion,
}
