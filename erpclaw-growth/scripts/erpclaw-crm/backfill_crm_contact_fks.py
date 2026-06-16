#!/usr/bin/env python3
"""Wave 1B F1 backfill: populate foundation lead/opportunity FK columns from CRM entities.

Run-once heuristic backfill that links EXISTING lead / opportunity rows to the
contact + company entities created post-F1. Growth is the legitimate writer of
lead.crm_contact_id / crm_company_id and opportunity.crm_contact_id / crm_company_id
(lead + opportunity are erpclaw-crm-owned tables).

Heuristic (exact match only — fuzzy is explicitly out of scope, open decision #1):
  - lead.crm_contact_id  <- crm_contact.email  == lower(lead.email)   (per company)
  - lead.crm_company_id  <- crm_company.name   == lead.company_name   (per company)
  - opportunity FKs       <- inherited from the opportunity's source lead (lead_id chain)

NOT in scope (deferred): customer.crm_company_id. That column is on the
erpclaw-selling-owned `customer` table; per Article 5 + ADR-0023 growth may not
write it directly. Backfilling it needs an erpclaw-selling-side action/migration
(tracked for a later item), so this script leaves customer.crm_company_id alone.

Safety:
  - `--dry-run` (default behavior unless `--execute`) reports what WOULD change,
    writes nothing.
  - Every applied change is written inside a single transaction and audit-logged
    (audit_log) so the backfill is reversible/inspectable.

Usage:
  python3 backfill_crm_contact_fks.py [--db-path PATH] [--company-id ID] [--execute]
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.db import get_connection, DEFAULT_DB_PATH
from erpclaw_lib.audit import audit


def _plan(conn, company_id=None):
    """Compute the set of (table, id, column, value) changes the backfill would make."""
    changes = []
    where_company = " AND l.company_id = ?" if company_id else ""
    cparams = [company_id] if company_id else []

    # lead.crm_contact_id <- crm_contact by case-insensitive email (same company)
    rows = conn.execute(
        "SELECT l.id AS lead_id, ct.id AS contact_id "
        "FROM lead l JOIN crm_contact ct "
        "  ON ct.company_id = l.company_id "
        " AND l.email IS NOT NULL AND ct.email IS NOT NULL "
        " AND LOWER(l.email) = LOWER(ct.email) "
        "WHERE l.crm_contact_id IS NULL" + where_company,
        cparams).fetchall()
    for r in rows:
        changes.append(("lead", r["lead_id"], "crm_contact_id", r["contact_id"]))

    # lead.crm_company_id <- crm_company by exact name (same company)
    rows = conn.execute(
        "SELECT l.id AS lead_id, co.id AS crm_company_id "
        "FROM lead l JOIN crm_company co "
        "  ON co.company_id = l.company_id "
        " AND l.company_name IS NOT NULL AND co.name = l.company_name "
        "WHERE l.crm_company_id IS NULL" + where_company,
        cparams).fetchall()
    for r in rows:
        changes.append(("lead", r["lead_id"], "crm_company_id", r["crm_company_id"]))

    return changes


def _plan_opportunity(conn, lead_changes, company_id=None):
    """opportunity FKs inherit from the source lead (post lead-backfill state)."""
    changes = []
    # Build the resolved lead FK map (existing + newly-planned).
    lead_fk = {}  # lead_id -> {col: value}
    for table, rid, col, val in lead_changes:
        lead_fk.setdefault(rid, {})[col] = val
    where_company = " AND o.company_id = ?" if company_id else ""
    cparams = [company_id] if company_id else []
    rows = conn.execute(
        "SELECT o.id AS opp_id, o.lead_id, o.crm_contact_id, o.crm_company_id, "
        "       l.crm_contact_id AS l_contact, l.crm_company_id AS l_company "
        "FROM opportunity o JOIN lead l ON l.id = o.lead_id "
        "WHERE o.lead_id IS NOT NULL" + where_company,
        cparams).fetchall()
    for r in rows:
        planned = lead_fk.get(r["lead_id"], {})
        contact = r["l_contact"] or planned.get("crm_contact_id")
        company = r["l_company"] or planned.get("crm_company_id")
        if contact and r["crm_contact_id"] is None:
            changes.append(("opportunity", r["opp_id"], "crm_contact_id", contact))
        if company and r["crm_company_id"] is None:
            changes.append(("opportunity", r["opp_id"], "crm_company_id", company))
    return changes


# Fixed dispatch of the only (table, column) pairs this backfill writes — no
# f-string SQL assembly (Article 10). Every value is a bound parameter.
_APPLY_SQL = {
    ("lead", "crm_contact_id"): "UPDATE lead SET crm_contact_id = ? WHERE id = ?",
    ("lead", "crm_company_id"): "UPDATE lead SET crm_company_id = ? WHERE id = ?",
    ("opportunity", "crm_contact_id"): "UPDATE opportunity SET crm_contact_id = ? WHERE id = ?",
    ("opportunity", "crm_company_id"): "UPDATE opportunity SET crm_company_id = ? WHERE id = ?",
}


def _apply_one(conn, table, col, rid, val):
    sql = _APPLY_SQL.get((table, col))
    if sql is None:
        raise ValueError(f"backfill refuses to write {table}.{col} (not in whitelist)")
    conn.execute(sql, (val, rid))


def run(db_path, company_id=None, execute=False):
    conn = get_connection(db_path)
    try:
        lead_changes = _plan(conn, company_id)
        opp_changes = _plan_opportunity(conn, lead_changes, company_id)
        all_changes = lead_changes + opp_changes

        summary = {
            "dry_run": not execute,
            "total_changes": len(all_changes),
            "lead_changes": len(lead_changes),
            "opportunity_changes": len(opp_changes),
            "changes": [
                {"table": t, "id": i, "column": c, "value": v}
                for (t, i, c, v) in all_changes
            ],
        }

        if not execute:
            summary["message"] = (
                f"DRY RUN — {len(all_changes)} change(s) would be applied. "
                "Re-run with --execute to write them.")
            print(json.dumps(summary, indent=2, default=str))
            return summary

        # Apply in one transaction; audit every change. Literal SQL per
        # (table, column) — the pair is from a fixed internal whitelist, never
        # user input, but we avoid f-string SQL assembly entirely.
        for table, rid, col, val in all_changes:
            _apply_one(conn, table, col, rid, val)
            audit(conn, "erpclaw-crm", "backfill-crm-contact-fks", table, rid,
                  new_values={col: val},
                  description=f"Backfilled {table}.{col}")
        conn.commit()
        summary["message"] = f"Applied {len(all_changes)} change(s)."
        print(json.dumps(summary, indent=2, default=str))
        return summary
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Wave 1B F1 backfill: lead/opportunity CRM FK columns")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--company-id", default=None,
                        help="Limit the backfill to a single company.")
    parser.add_argument("--execute", action="store_true",
                        help="Apply changes (default is a dry run).")
    args = parser.parse_args()
    run(args.db_path, company_id=args.company_id, execute=args.execute)
