#!/usr/bin/env python3
"""ERPClaw CRM Skill -- db_query.py

Lead management, opportunity pipeline, campaigns, and activity tracking.
All 18 actions are routed through this single entry point.

Usage: python3 db_query.py --action <action-name> [--flags ...]
Output: JSON to stdout, exit 0 on success, exit 1 on error.
"""
import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

# Add shared lib to path
try:
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    from erpclaw_lib.db import get_connection, ensure_db_exists, DEFAULT_DB_PATH
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.naming import get_next_name
    from erpclaw_lib.validation import check_input_lengths
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.dependencies import check_required_tables, table_exists
    from erpclaw_lib.query import Q, P, Table, Field, fn, Case, Order, Criterion, Not, NULL, insert_row, update_row, dynamic_update, now
    from erpclaw_lib.args import SafeArgumentParser, check_unknown_args
    from erpclaw_lib.vendor.pypika.terms import LiteralValue, ValueWrapper
except ImportError:
    import json as _json
    print(_json.dumps({"status": "error", "error": "ERPClaw foundation not installed. Install erpclaw first: clawhub install erpclaw", "suggestion": "clawhub install erpclaw"}))
    sys.exit(1)

REQUIRED_TABLES = ["company"]

VALID_LEAD_SOURCES = ("website", "referral", "campaign", "cold_call",
                      "social_media", "trade_show", "other")
VALID_LEAD_STATUSES = ("new", "contacted", "qualified", "converted",
                       "unresponsive", "lost")
VALID_OPP_STAGES = ("new", "contacted", "qualified", "proposal_sent",
                    "negotiation", "won", "lost")
VALID_OPP_TYPES = ("sales", "support", "maintenance")
VALID_CAMPAIGN_TYPES = ("email", "social", "event", "referral", "content")
VALID_CAMPAIGN_STATUSES = ("planned", "active", "completed")
VALID_ACTIVITY_TYPES = ("call", "email", "meeting", "note", "task")

# Wave 1B F1 — Contact + Company model
VALID_CONTACT_LIFECYCLES = ("lead", "mql", "sql", "customer", "other")
VALID_COMPANY_LIFECYCLES = ("prospect", "customer", "partner", "vendor", "other")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Wave 1B F2 — Tasks (first-class entity)
VALID_TASK_STATUSES = ("open", "in_progress", "done", "cancelled")
VALID_TASK_PRIORITIES = ("low", "medium", "high", "urgent")
# linked_entity_type values + the foundation/growth table each resolves to.
VALID_TASK_LINK_TYPES = ("lead", "opportunity", "customer", "crm_contact", "crm_company")
_TASK_LINK_TABLE = {
    "lead": "lead",
    "opportunity": "opportunity",
    "customer": "customer",
    "crm_contact": "crm_contact",
    "crm_company": "crm_company",
}

# ---------------------------------------------------------------------------
# PyPika table references
# ---------------------------------------------------------------------------
_t_company = Table("company")
_t_lead = Table("lead")
_t_opportunity = Table("opportunity")
_t_customer = Table("customer")
_t_campaign = Table("campaign")
_t_campaign_lead = Table("campaign_lead")
_t_activity = Table("crm_activity")
_t_crm_contact = Table("crm_contact")
_t_crm_company = Table("crm_company")
_t_crm_contact_role = Table("crm_contact_role")
_t_crm_task = Table("crm_task")
_t_crm_task_link = Table("crm_task_link")
_t_crm_pipeline = Table("crm_pipeline")
_t_crm_pipeline_stage = Table("crm_pipeline_stage")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_json_arg(value, name):
    if value is None:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        err(f"Invalid JSON for --{name}: {value}")


def _calc_weighted_revenue(expected_revenue: str, probability: str) -> str:
    """Calculate weighted_revenue = expected_revenue * (probability / 100)."""
    rev = to_decimal(expected_revenue or "0")
    prob = to_decimal(probability or "0")
    weighted = (rev * prob / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return str(weighted)


# ===========================================================================
# Wave 1B F4 — Saved views: filter-JSON DSL
# ===========================================================================
#
# A saved view persists a BOUNDED filter expression that any list-<entity>
# action can replay. Security model (the whole point of the DSL):
#
#   * COLUMN NAMES are whitelisted. The whitelist is the entity's native
#     filterable columns (frozen per entity below) plus the entity's UDF field
#     names (custom_fields.get_custom_fields). A column name is only ever
#     f-string-injected into SQL AFTER it passes the whitelist membership test;
#     it is never taken from user input verbatim.
#   * OPERATORS are whitelisted. Each maps to a fixed SQL fragment template
#     (=, <>, LIKE, >, <, IN (...), BETWEEN ? AND ?). The operator string is
#     never interpolated — only its hardcoded fragment is.
#   * VALUES are ALWAYS bound parameters (?). No value ever reaches the SQL
#     string. UDF-targeted conditions are resolved via an EXISTS sub-select on
#     custom_field_value, again fully parameterized.
#   * NESTING is depth-capped (DoS / runaway-recursion guard).
#
# Validation runs at SAVE time (add/update-crm-saved-view) and again at APPLY
# time (defence in depth — a UDF could be dropped between save and apply, or a
# row could be hand-edited in the DB). build_filter_where() only ever runs on a
# tree that already passed validate_filter_tree().

# Native filterable columns per entity. Deliberately curated (not every column):
# excludes free-text blobs (notes/description) and the company_id scope column
# (the list action always scopes company_id itself). Matches the live schema as
# of Wave 1B (lead/opportunity/customer foundation; crm_* growth-owned).
ENTITY_NATIVE_COLUMNS = {
    "lead": {
        "id", "naming_series", "lead_name", "company_name", "email", "phone",
        "source", "territory", "industry", "status", "assigned_to",
        "crm_contact_id", "crm_company_id", "created_at", "updated_at",
    },
    "opportunity": {
        "id", "naming_series", "opportunity_name", "lead_id", "customer_id",
        "opportunity_type", "source", "expected_closing_date", "probability",
        "expected_revenue", "weighted_revenue", "stage", "assigned_to",
        "next_follow_up_date", "crm_contact_id", "crm_company_id",
        "pipeline_stage_id", "created_at", "updated_at",
    },
    "customer": {
        "id", "naming_series", "name", "customer_type", "customer_group",
        "territory", "default_currency", "credit_status", "tax_id",
        "email", "phone", "status", "crm_company_id", "created_at", "updated_at",
    },
    "crm_contact": {
        "id", "name", "email", "phone", "mobile", "job_title", "linkedin_url",
        "city", "state", "country", "lifecycle", "crm_company_id",
        "assigned_to_user_id", "created_at", "updated_at",
    },
    "crm_company": {
        "id", "name", "domain", "industry", "employee_count", "annual_revenue",
        "city", "state", "country", "linkedin_url", "lifecycle",
        "linked_customer_id", "assigned_to_user_id", "created_at", "updated_at",
    },
    "crm_task": {
        "id", "subject", "status", "priority", "due_date",
        "assigned_to_user_id", "created_by_user_id", "completed_at",
        "linked_count", "created_at", "updated_at",
    },
}

VALID_SAVED_VIEW_ENTITIES = tuple(ENTITY_NATIVE_COLUMNS.keys())

# Operator -> SQL fragment builder. STRUCTURE is fixed; the only variable piece is
# the column name (whitelisted upstream) and the ? placeholders. Each builder
# returns (sql_fragment, params_list). `col` is already validated + safe.
_FILTER_MAX_DEPTH = 5          # nesting cap (DoS guard)
_FILTER_MAX_CONDITIONS = 50    # total leaf-condition cap per view (DoS guard)
VALID_FILTER_OPERATORS = ("eq", "neq", "contains", "gt", "lt", "in", "between")
VALID_FILTER_LOGIC = ("AND", "OR")


class FilterValidationError(ValueError):
    """Raised when a filter-JSON tree fails validation (unknown field/op, bad
    shape, or depth/condition cap). Carries a user-facing message."""


def _udf_field_names(conn, entity_type):
    """UDF field names registered for the entity's table (M1 custom fields)."""
    try:
        from erpclaw_lib.custom_fields import get_custom_fields
    except ImportError:
        return set()
    try:
        return {f["field_name"] for f in get_custom_fields(conn, entity_type)}
    except Exception:
        return set()


def allowed_columns_for(conn, entity_type):
    """Whitelist = native filterable columns + registered UDF field names."""
    native = ENTITY_NATIVE_COLUMNS.get(entity_type)
    if native is None:
        raise FilterValidationError(
            f"Unknown entity_type '{entity_type}'. "
            f"Must be one of {VALID_SAVED_VIEW_ENTITIES}.")
    return set(native) | _udf_field_names(conn, entity_type)


def validate_filter_tree(node, allowed_columns, udf_columns, _depth=1, _counter=None):
    """Recursively validate a filter-JSON node against the column whitelist.

    A node is either a GROUP {logic, conditions:[...]} or a LEAF
    {field, op, value}. Raises FilterValidationError on any violation. Returns
    the number of leaf conditions seen (for the cap). Pure validation — touches
    no SQL. `udf_columns` is the subset of `allowed_columns` that are UDFs (so
    leaf validation can flag them for the EXISTS sub-select path at build time).
    """
    if _counter is None:
        _counter = [0]
    if _depth > _FILTER_MAX_DEPTH:
        raise FilterValidationError(
            f"Filter nesting too deep (max {_FILTER_MAX_DEPTH}).")
    if not isinstance(node, dict):
        raise FilterValidationError("Each filter node must be an object.")

    if "conditions" in node or "logic" in node:
        logic = node.get("logic", "AND")
        if logic not in VALID_FILTER_LOGIC:
            raise FilterValidationError(
                f"Invalid logic '{logic}'. Must be one of {VALID_FILTER_LOGIC}.")
        conditions = node.get("conditions")
        if not isinstance(conditions, list) or not conditions:
            raise FilterValidationError(
                "A filter group needs a non-empty 'conditions' list.")
        for child in conditions:
            validate_filter_tree(child, allowed_columns, udf_columns,
                                 _depth + 1, _counter)
        return _counter[0]

    # Leaf condition.
    field = node.get("field")
    op = node.get("op")
    if field is None or op is None:
        raise FilterValidationError(
            "Each condition needs a 'field' and an 'op'.")
    if not isinstance(field, str) or field not in allowed_columns:
        # The injection guard: an unknown / malicious field is rejected here,
        # BEFORE any SQL is constructed.
        raise FilterValidationError(
            f"Unknown or disallowed field '{field}'.")
    if op not in VALID_FILTER_OPERATORS:
        raise FilterValidationError(
            f"Unknown operator '{op}'. Must be one of {VALID_FILTER_OPERATORS}.")

    value = node.get("value")
    if op in ("in",):
        if not isinstance(value, list) or not value:
            raise FilterValidationError(
                f"Operator 'in' on '{field}' needs a non-empty list value.")
        if any(isinstance(v, (list, dict)) for v in value):
            raise FilterValidationError(
                f"Operator 'in' on '{field}' takes a list of scalars.")
    elif op == "between":
        if not isinstance(value, list) or len(value) != 2:
            raise FilterValidationError(
                f"Operator 'between' on '{field}' needs a [low, high] list.")
        if any(isinstance(v, (list, dict)) for v in value):
            raise FilterValidationError(
                f"Operator 'between' on '{field}' takes two scalars.")
    else:
        if isinstance(value, (list, dict)):
            raise FilterValidationError(
                f"Operator '{op}' on '{field}' takes a scalar value.")

    _counter[0] += 1
    if _counter[0] > _FILTER_MAX_CONDITIONS:
        raise FilterValidationError(
            f"Too many filter conditions (max {_FILTER_MAX_CONDITIONS}).")
    return _counter[0]


def _build_leaf_sql(node, udf_columns, entity_type):
    """Build (sql_fragment, params) for one validated leaf condition.

    Native column -> `<col> <op-sql> <?>`. UDF column -> an EXISTS sub-select on
    custom_field_value (the EAV store) so the value still compares against a ?.
    `node` has already passed validate_filter_tree(); the field is known-safe.
    """
    field = node["field"]
    op = node["op"]
    value = node.get("value")
    is_udf = field in udf_columns

    # Column expression: a bare native column, or the EAV value column inside an
    # EXISTS sub-select. The sub-select column/field-name binds are parameterized.
    if is_udf:
        # cfv.value holds the UDF value as TEXT; correlate on the parent row id.
        col_expr = "cfv.value"
    else:
        col_expr = field   # whitelisted native column name only

    if op == "eq":
        frag, params = f"{col_expr} = ?", [value]
    elif op == "neq":
        frag, params = f"{col_expr} <> ?", [value]
    elif op == "contains":
        frag, params = f"{col_expr} LIKE ?", [f"%{value}%"]
    elif op == "gt":
        frag, params = f"{col_expr} > ?", [value]
    elif op == "lt":
        frag, params = f"{col_expr} < ?", [value]
    elif op == "in":
        placeholders = ", ".join("?" for _ in value)
        frag, params = f"{col_expr} IN ({placeholders})", list(value)
    elif op == "between":
        frag, params = f"{col_expr} BETWEEN ? AND ?", [value[0], value[1]]
    else:  # unreachable — validate_filter_tree already gated the operator.
        raise FilterValidationError(f"Unsupported operator '{op}'.")

    if is_udf:
        # Wrap in an EXISTS over the EAV store. table_name + field_name are bound;
        # the parent-row correlation uses the entity's own id column.
        sub = (f"EXISTS (SELECT 1 FROM custom_field_value cfv "
               f"WHERE cfv.table_name = ? AND cfv.doc_id = {entity_type}.id "
               f"AND cfv.field_name = ? AND {frag})")
        return sub, [entity_type, field] + params
    return frag, params


def build_filter_where(node, entity_type, udf_columns, _depth=1):
    """Compile a validated filter-JSON tree into (sql_fragment, params).

    The fragment is a parenthesized boolean expression with ? placeholders only;
    no value or raw field name is ever interpolated. Must only be called on a
    tree that has passed validate_filter_tree().
    """
    if "conditions" in node or "logic" in node:
        logic = node.get("logic", "AND")
        joiner = f" {logic} "
        parts = []
        params = []
        for child in node["conditions"]:
            frag, p = build_filter_where(child, entity_type, udf_columns, _depth + 1)
            parts.append(frag)
            params.extend(p)
        return "(" + joiner.join(parts) + ")", params
    return _build_leaf_sql(node, udf_columns, entity_type)


def _saved_view_clause(conn, args, entity_type):
    """If --saved-view-id is set, load + validate the view and compile its filter.

    Returns (where_fragment, params) to AND-append to a list query, or (None, []).
    Enforces the view's entity_type matches the list action (a lead view cannot be
    applied to opportunities). Re-validates the stored filter at apply time
    (defence in depth — a UDF could have been dropped since save).
    """
    view_id = getattr(args, "saved_view_id", None)
    if not view_id:
        return None, []
    view = conn.execute(
        "SELECT * FROM crm_saved_view WHERE id = ?", (view_id,)).fetchone()
    if not view:
        err(f"Saved view {view_id} not found")
    if view["entity_type"] != entity_type:
        err(f"Saved view {view_id} is for '{view['entity_type']}', "
            f"not '{entity_type}'.")
    if not view["filter_json"]:
        return None, []
    tree = json.loads(view["filter_json"])
    allowed = allowed_columns_for(conn, entity_type)
    udf_cols = _udf_field_names(conn, entity_type)
    try:
        validate_filter_tree(tree, allowed, udf_cols)
    except FilterValidationError as e:
        err(str(e))
    return build_filter_where(tree, entity_type, udf_cols)


def _exec_list_raw(conn, args, entity_type, table, select_cols, where_clauses,
                   where_params, result_key, extra_payload=None):
    """Run a list-* query built as raw parameterized SQL, AND-appending a
    saved-view filter when --saved-view-id is set.

    `where_clauses` is a list of SQL fragments (each already parameterized via ?)
    and `where_params` the matching values, in order. The query is assembled
    deterministically: WHERE (native clauses [+ saved-view fragment]) ORDER BY
    created_at DESC LIMIT ? OFFSET ?. All fragments use ? placeholders and bound
    params only — PG-strict-safe (no aliases in any GROUP BY; there is none here).
    `table` is the unaliased FROM table (so the UDF EXISTS correlation
    `{entity_type}.id` resolves).
    """
    clauses = list(where_clauses)
    params = list(where_params)

    sv_frag, sv_params = _saved_view_clause(conn, args, entity_type)
    if sv_frag:
        clauses.append(sv_frag)
        params.extend(sv_params)

    where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    limit = int(args.limit or 20)
    offset = int(args.offset or 0)

    sel_sql = (f"SELECT {select_cols} FROM {table}{where_sql} "
               f"ORDER BY created_at DESC LIMIT ? OFFSET ?")
    cnt_sql = f"SELECT COUNT(*) AS cnt FROM {table}{where_sql}"

    rows = conn.execute(sel_sql, params + [limit, offset]).fetchall()
    total = conn.execute(cnt_sql, params).fetchone()["cnt"]

    payload = {
        result_key: [row_to_dict(r) for r in rows],
        "total": total, "limit": limit, "offset": offset,
        "has_more": offset + limit < total,
    }
    if extra_payload:
        payload.update(extra_payload)
    ok(payload)


# ---------------------------------------------------------------------------
# Company resolution
# ---------------------------------------------------------------------------

def _resolve_company_id(conn, args):
    """Resolve company_id from args or conn, set on conn for get_next_name()."""
    company_id = getattr(args, "company_id", None) or getattr(conn, "company_id", None)
    if not company_id:
        err("--company-id is required")
    # Validate company exists
    q = Q.from_(_t_company).select(_t_company.id).where(_t_company.id == P())
    comp = conn.execute(q.get_sql(), (company_id,)).fetchone()
    if not comp:
        err(f"Company {company_id} not found")
    # Set on conn so get_next_name() can find it
    conn.company_id = company_id
    return company_id


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_lead_exists(conn, lead_id: str):
    q = Q.from_(_t_lead).select(_t_lead.star).where(_t_lead.id == P())
    lead = conn.execute(q.get_sql(), (lead_id,)).fetchone()
    if not lead:
        err(f"Lead {lead_id} not found",
             suggestion="Use 'list leads' to see available leads.")
    return lead


def _validate_opportunity_exists(conn, opp_id: str):
    q = Q.from_(_t_opportunity).select(_t_opportunity.star).where(_t_opportunity.id == P())
    opp = conn.execute(q.get_sql(), (opp_id,)).fetchone()
    if not opp:
        err(f"Opportunity {opp_id} not found",
             suggestion="Use 'list opportunities' to see available opportunities.")
    return opp


def _validate_customer_exists(conn, customer_id: str):
    q = Q.from_(_t_customer).select(_t_customer.star).where(_t_customer.id == P())
    cust = conn.execute(q.get_sql(), (customer_id,)).fetchone()
    if not cust:
        err(f"Customer {customer_id} not found")
    return cust


def _validate_campaign_exists(conn, campaign_id: str):
    q = Q.from_(_t_campaign).select(_t_campaign.star).where(_t_campaign.id == P())
    camp = conn.execute(q.get_sql(), (campaign_id,)).fetchone()
    if not camp:
        err(f"Campaign {campaign_id} not found")
    return camp


# ---------------------------------------------------------------------------
# 1. add-lead
# ---------------------------------------------------------------------------

def add_lead(conn, args):
    """Add a new lead.

    Required: --lead-name
    Optional: --company-name, --email, --phone, --source, --territory,
              --industry, --assigned-to, --notes
    """
    if not args.lead_name:
        err("--lead-name is required")

    if args.source and args.source not in VALID_LEAD_SOURCES:
        err(f"--source must be one of {VALID_LEAD_SOURCES}")

    # Validate email format if provided
    _EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    if args.email and not _EMAIL_RE.match(args.email):
        err(f"Invalid email format for --email: '{args.email}'")

    company_id = _resolve_company_id(conn, args)
    lead_id = str(uuid.uuid4())
    naming = get_next_name(conn, "lead")

    sql, _ = insert_row("lead", {
        "id": P(), "naming_series": P(), "lead_name": P(), "company_name": P(),
        "email": P(), "phone": P(), "source": P(), "territory": P(),
        "industry": P(), "status": ValueWrapper("new"), "assigned_to": P(),
        "notes": P(), "company_id": P(),
    })
    conn.execute(sql,
        (lead_id, naming, args.lead_name, args.company_name, args.email,
         args.phone, args.source, args.territory, args.industry,
         args.assigned_to, args.notes, company_id),
    )

    audit(conn, "erpclaw-crm", "add-lead", "lead", lead_id,
           new_values={"lead_name": args.lead_name, "source": args.source},
           description=f"Created lead: {args.lead_name}")
    conn.commit()

    ok({
        "lead": {
            "id": lead_id,
            "naming_series": naming,
            "lead_name": args.lead_name,
            "company_name": args.company_name,
            "email": args.email,
            "phone": args.phone,
            "source": args.source,
            "status": "new",
        },
        "message": f"Lead '{args.lead_name}' created ({naming})",
    })


# ---------------------------------------------------------------------------
# 2. update-lead
# ---------------------------------------------------------------------------

def update_lead(conn, args):
    """Update an existing lead.

    Required: --lead-id
    Optional: --lead-name, --company-name, --email, --phone, --source,
              --territory, --industry, --status, --assigned-to, --notes
    """
    if not args.lead_id:
        err("--lead-id is required")

    lead = _validate_lead_exists(conn, args.lead_id)
    old_values = row_to_dict(lead)

    # Frozen after conversion
    if lead["status"] == "converted":
        err("Cannot update a converted lead. Work with the opportunity instead.",
             suggestion="Use 'list opportunities' to find the opportunity created from this lead.")

    if args.source and args.source not in VALID_LEAD_SOURCES:
        err(f"--source must be one of {VALID_LEAD_SOURCES}")
    if args.status and args.status not in VALID_LEAD_STATUSES:
        err(f"--status must be one of {VALID_LEAD_STATUSES}")

    field_map = {
        "lead_name": args.lead_name,
        "company_name": args.company_name,
        "email": args.email,
        "phone": args.phone,
        "source": args.source,
        "territory": args.territory,
        "industry": args.industry,
        "status": args.status,
        "assigned_to": args.assigned_to,
        "notes": args.notes,
    }

    data = {k: P() for k, v in field_map.items() if v is not None}
    values = [v for v in field_map.values() if v is not None]

    if not data:
        err("No fields to update. Provide at least one optional flag.")

    data["updated_at"] = now()
    sql = update_row("lead", data, {"id": P()})
    values.append(args.lead_id)
    conn.execute(sql, values)

    audit(conn, "erpclaw-crm", "update-lead", "lead", args.lead_id,
           old_values=old_values,
           description="Updated lead")
    conn.commit()

    q = Q.from_(_t_lead).select(_t_lead.star).where(_t_lead.id == P())
    updated = conn.execute(q.get_sql(), (args.lead_id,)).fetchone()

    ok({
        "lead": row_to_dict(updated),
        "message": f"Lead {updated['naming_series']} updated",
    })


# ---------------------------------------------------------------------------
# 3. get-lead
# ---------------------------------------------------------------------------

def get_lead(conn, args):
    """Get lead details with activities and campaigns.

    Required: --lead-id
    """
    if not args.lead_id:
        err("--lead-id is required")

    lead = _validate_lead_exists(conn, args.lead_id)
    lead_dict = row_to_dict(lead)

    # Fetch activities for this lead
    q = (Q.from_(_t_activity).select(_t_activity.star)
         .where(_t_activity.lead_id == P())
         .orderby(_t_activity.activity_date, order=Order.desc))
    activities = conn.execute(q.get_sql(), (args.lead_id,)).fetchall()
    lead_dict["activities"] = [row_to_dict(a) for a in activities]

    # Fetch campaigns this lead is linked to
    c = _t_campaign
    cl = _t_campaign_lead
    q = (Q.from_(c).join(cl).on(cl.campaign_id == c.id)
         .select(c.star, cl.added_date, cl.converted)
         .where(cl.lead_id == P())
         .orderby(cl.added_date, order=Order.desc))
    campaigns = conn.execute(q.get_sql(), (args.lead_id,)).fetchall()
    lead_dict["campaigns"] = [row_to_dict(c) for c in campaigns]

    ok({"lead": lead_dict})


# ---------------------------------------------------------------------------
# 4. list-leads
# ---------------------------------------------------------------------------

def list_leads(conn, args):
    """List leads with optional filters.

    Optional: --status, --source, --search, --saved-view-id, --limit, --offset
    --saved-view-id replays a saved view's filter-JSON (Wave 1B F4).
    """
    clauses = []
    params = []
    if args.status:
        clauses.append("status = ?")
        params.append(args.status)
    if args.source:
        clauses.append("source = ?")
        params.append(args.source)
    if args.search:
        clauses.append("(lead_name LIKE ? OR company_name LIKE ? OR email LIKE ?)")
        params.extend([f"%{args.search}%"] * 3)

    _exec_list_raw(conn, args, "lead", "lead", "lead.*", clauses, params, "leads")


# ---------------------------------------------------------------------------
# 5. convert-lead-to-opportunity
# ---------------------------------------------------------------------------

def convert_lead_to_opportunity(conn, args):
    """Convert a lead to an opportunity (single transaction).

    Required: --lead-id, --opportunity-name
    Optional: --expected-revenue, --probability, --opportunity-type,
              --expected-closing-date
    """
    if not args.lead_id:
        err("--lead-id is required")
    if not args.opportunity_name:
        err("--opportunity-name is required")

    lead = _validate_lead_exists(conn, args.lead_id)

    if lead["status"] == "converted":
        err(f"Lead {lead['naming_series']} is already converted to opportunity {lead['converted_to_opportunity']}")

    opp_type = args.opportunity_type or "sales"
    if opp_type not in VALID_OPP_TYPES:
        err(f"--opportunity-type must be one of {VALID_OPP_TYPES}")

    probability = args.probability or "50"
    expected_revenue = args.expected_revenue or "0"
    weighted = _calc_weighted_revenue(expected_revenue, probability)

    company_id = _resolve_company_id(conn, args)
    opp_id = str(uuid.uuid4())
    opp_naming = get_next_name(conn, "opportunity")

    # Single transaction: create opportunity + update lead
    sql, _ = insert_row("opportunity", {
        "id": P(), "naming_series": P(), "opportunity_name": P(), "lead_id": P(),
        "opportunity_type": P(), "source": P(), "probability": P(),
        "expected_revenue": P(), "weighted_revenue": P(),
        "stage": ValueWrapper("new"), "expected_closing_date": P(), "company_id": P(),
    })
    conn.execute(sql,
        (opp_id, opp_naming, args.opportunity_name, args.lead_id,
         opp_type, lead["source"], probability, expected_revenue, weighted,
         args.expected_closing_date, company_id),
    )

    sql = update_row("lead", {
        "status": ValueWrapper("converted"),
        "converted_to_opportunity": P(),
        "updated_at": now(),
    }, {"id": P()})
    conn.execute(sql, (opp_id, args.lead_id))

    # Mark campaign_lead as converted if applicable
    sql = update_row("campaign_lead", {
        "converted": ValueWrapper(1),
    }, {"lead_id": P()})
    conn.execute(sql, (args.lead_id,))

    audit(conn, "erpclaw-crm", "convert-lead-to-opportunity", "lead", args.lead_id,
           new_values={"opportunity_id": opp_id},
           description=f"Converted lead to opportunity {opp_naming}")
    audit(conn, "erpclaw-crm", "convert-lead-to-opportunity", "opportunity", opp_id,
           new_values={"opportunity_name": args.opportunity_name, "lead_id": args.lead_id},
           description=f"Opportunity created from lead conversion")
    conn.commit()

    ok({
        "opportunity": {
            "id": opp_id,
            "naming_series": opp_naming,
            "opportunity_name": args.opportunity_name,
            "lead_id": args.lead_id,
            "stage": "new",
            "probability": probability,
            "expected_revenue": expected_revenue,
            "weighted_revenue": weighted,
        },
        "lead_status": "converted",
        "message": f"Lead converted to opportunity {opp_naming}",
    })


# ---------------------------------------------------------------------------
# 6. add-opportunity
# ---------------------------------------------------------------------------

def add_opportunity(conn, args):
    """Add a new opportunity.

    Required: --opportunity-name
    Optional: --lead-id, --customer-id, --opportunity-type, --expected-revenue,
              --probability, --expected-closing-date, --assigned-to
    """
    if not args.opportunity_name:
        err("--opportunity-name is required")

    opp_type = args.opportunity_type or "sales"
    if opp_type not in VALID_OPP_TYPES:
        err(f"--opportunity-type must be one of {VALID_OPP_TYPES}")

    if args.lead_id:
        _validate_lead_exists(conn, args.lead_id)
    if args.customer_id:
        _validate_customer_exists(conn, args.customer_id)

    probability = args.probability or "0"
    expected_revenue = args.expected_revenue or "0"
    weighted = _calc_weighted_revenue(expected_revenue, probability)

    company_id = _resolve_company_id(conn, args)
    opp_id = str(uuid.uuid4())
    naming = get_next_name(conn, "opportunity")

    source = None
    if args.lead_id:
        q = Q.from_(_t_lead).select(_t_lead.source).where(_t_lead.id == P())
        lead = conn.execute(q.get_sql(), (args.lead_id,)).fetchone()
        if lead:
            source = lead["source"]

    sql, _ = insert_row("opportunity", {
        "id": P(), "naming_series": P(), "opportunity_name": P(), "lead_id": P(),
        "customer_id": P(), "opportunity_type": P(), "source": P(),
        "probability": P(), "expected_revenue": P(), "weighted_revenue": P(),
        "stage": ValueWrapper("new"), "expected_closing_date": P(),
        "assigned_to": P(), "company_id": P(),
    })
    conn.execute(sql,
        (opp_id, naming, args.opportunity_name, args.lead_id,
         args.customer_id, opp_type, source, probability, expected_revenue,
         weighted, args.expected_closing_date, args.assigned_to, company_id),
    )

    audit(conn, "erpclaw-crm", "add-opportunity", "opportunity", opp_id,
           new_values={"opportunity_name": args.opportunity_name},
           description=f"Created opportunity: {args.opportunity_name}")
    conn.commit()

    ok({
        "opportunity": {
            "id": opp_id,
            "naming_series": naming,
            "opportunity_name": args.opportunity_name,
            "lead_id": args.lead_id,
            "customer_id": args.customer_id,
            "stage": "new",
            "probability": probability,
            "expected_revenue": expected_revenue,
            "weighted_revenue": weighted,
        },
        "message": f"Opportunity '{args.opportunity_name}' created ({naming})",
    })


# ---------------------------------------------------------------------------
# 7. update-opportunity
# ---------------------------------------------------------------------------

def update_opportunity(conn, args):
    """Update an existing opportunity.

    Required: --opportunity-id
    Optional: --opportunity-name, --stage, --probability, --expected-revenue,
              --expected-closing-date, --assigned-to, --next-follow-up-date
    """
    if not args.opportunity_id:
        err("--opportunity-id is required")

    opp = _validate_opportunity_exists(conn, args.opportunity_id)
    old_values = row_to_dict(opp)

    # Terminal states are frozen
    if opp["stage"] in ("won", "lost"):
        err(f"Opportunity is {opp['stage']}. Terminal states cannot be updated.")

    if args.stage and args.stage not in VALID_OPP_STAGES:
        err(f"--stage must be one of {VALID_OPP_STAGES}")

    # Don't allow setting won/lost via update-opportunity; use mark- actions
    if args.stage in ("won", "lost"):
        err(f"Use mark-opportunity-{args.stage} to set terminal state")

    field_map = {
        "opportunity_name": args.opportunity_name,
        "stage": args.stage,
        "probability": args.probability,
        "expected_revenue": args.expected_revenue,
        "expected_closing_date": args.expected_closing_date,
        "assigned_to": args.assigned_to,
        "next_follow_up_date": args.next_follow_up_date,
        "customer_id": getattr(args, "customer_id", None),
    }

    data = {k: P() for k, v in field_map.items() if v is not None}
    values = [v for v in field_map.values() if v is not None]

    if not data:
        err("No fields to update. Provide at least one optional flag.")

    # Wave 1B F3 dual-write: when --stage changes the legacy text, also mirror it
    # onto pipeline_stage_id (resolved within the opportunity's current pipeline,
    # else the default). Best-effort: a zero-pipeline install resolves to None and
    # leaves the FK untouched, keeping backward-compat (the text write is authoritative).
    if args.stage is not None:
        resolved_stage_id = _resolve_stage_id_for_opportunity(conn, opp, args.stage)
        if resolved_stage_id is not None:
            data["pipeline_stage_id"] = P()
            values.append(resolved_stage_id)

    # Recalculate weighted revenue if probability or expected_revenue changed
    new_prob = args.probability or opp["probability"]
    new_rev = args.expected_revenue or opp["expected_revenue"]
    new_weighted = _calc_weighted_revenue(new_rev, new_prob)
    data["weighted_revenue"] = P()
    values.append(new_weighted)

    data["updated_at"] = now()
    sql = update_row("opportunity", data, {"id": P()})
    values.append(args.opportunity_id)
    conn.execute(sql, values)

    audit(conn, "erpclaw-crm", "update-opportunity", "opportunity", args.opportunity_id,
           old_values=old_values,
           description="Updated opportunity")
    conn.commit()

    q = Q.from_(_t_opportunity).select(_t_opportunity.star).where(_t_opportunity.id == P())
    updated = conn.execute(q.get_sql(), (args.opportunity_id,)).fetchone()

    ok({
        "opportunity": row_to_dict(updated),
        "message": f"Opportunity {updated['naming_series']} updated",
    })


# ---------------------------------------------------------------------------
# 8. get-opportunity
# ---------------------------------------------------------------------------

def get_opportunity(conn, args):
    """Get opportunity details with activities, lead, and customer info.

    Required: --opportunity-id
    """
    if not args.opportunity_id:
        err("--opportunity-id is required")

    opp = _validate_opportunity_exists(conn, args.opportunity_id)
    opp_dict = row_to_dict(opp)

    # Fetch activities
    q = (Q.from_(_t_activity).select(_t_activity.star)
         .where(_t_activity.opportunity_id == P())
         .orderby(_t_activity.activity_date, order=Order.desc))
    activities = conn.execute(q.get_sql(), (args.opportunity_id,)).fetchall()
    opp_dict["activities"] = [row_to_dict(a) for a in activities]

    # Fetch lead info if linked
    if opp["lead_id"]:
        t = _t_lead
        q = (Q.from_(t).select(
            t.id, t.naming_series, t.lead_name, t.company_name,
            t.email, t.phone, t.source, t.status)
            .where(t.id == P()))
        lead = conn.execute(q.get_sql(), (opp["lead_id"],)).fetchone()
        opp_dict["lead"] = row_to_dict(lead) if lead else None

    # Fetch customer info if linked
    if opp["customer_id"]:
        t = _t_customer
        q = (Q.from_(t).select(
            t.id, t.name, t.customer_type, t.territory, t.status)
            .where(t.id == P()))
        customer = conn.execute(q.get_sql(), (opp["customer_id"],)).fetchone()
        opp_dict["customer"] = row_to_dict(customer) if customer else None

    ok({"opportunity": opp_dict})


# ---------------------------------------------------------------------------
# 9. list-opportunities
# ---------------------------------------------------------------------------

def list_opportunities(conn, args):
    """List opportunities with optional filters.

    Optional: --stage, --search, --saved-view-id, --limit, --offset
    --saved-view-id replays a saved view's filter-JSON (Wave 1B F4).
    """
    clauses = []
    params = []
    if args.stage:
        clauses.append("stage = ?")
        params.append(args.stage)
    if args.search:
        clauses.append("(opportunity_name LIKE ? OR source LIKE ?)")
        params.extend([f"%{args.search}%"] * 2)

    _exec_list_raw(conn, args, "opportunity", "opportunity", "opportunity.*",
                   clauses, params, "opportunities")


# ---------------------------------------------------------------------------
# 10. convert-opportunity-to-quotation (cross-skill subprocess)
# ---------------------------------------------------------------------------

def convert_opportunity_to_quotation(conn, args):
    """Convert a won opportunity to a quotation via erpclaw-selling subprocess.

    Required: --opportunity-id, --items (JSON array)
    """
    if not args.opportunity_id:
        err("--opportunity-id is required")
    if not args.items:
        err("--items is required (JSON array of {item_id, qty, rate})")

    opp = _validate_opportunity_exists(conn, args.opportunity_id)

    items_data = _parse_json_arg(args.items, "items")
    if not items_data or not isinstance(items_data, list):
        err("--items must be a non-empty JSON array")

    # Require customer_id for quotation
    if not opp["customer_id"]:
        err("Opportunity must have a customer_id to create a quotation. "
             "Update the opportunity with --customer-id first.")

    # Pre-flight: check erpclaw base package (contains selling domain) is installed
    from erpclaw_lib.dependencies import check_subprocess_target, resolve_skill_script
    from erpclaw_lib.args import SafeArgumentParser, check_unknown_args
    dep_err = check_subprocess_target(conn, "erpclaw", "quotation")
    if dep_err:
        err(dep_err["error"])
    selling_script = resolve_skill_script("erpclaw")

    # Build subprocess command
    cmd = [
        "python3", selling_script,
        "--action", "add-quotation",
        "--customer-id", opp["customer_id"],
        "--posting-date", datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "--items", json.dumps(items_data),
    ]

    # Pass db-path if using non-default
    db_path = getattr(args, "db_path", None)
    if db_path:
        cmd.extend(["--db-path", db_path])

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30,
        )
    except subprocess.TimeoutExpired:
        err("Quotation creation timed out (30s)")

    if result.returncode != 0:
        err_msg = result.stdout.strip() or result.stderr.strip()
        err(f"Failed to create quotation: {err_msg}")

    try:
        qtn_result = json.loads(result.stdout)
    except json.JSONDecodeError:
        err(f"Invalid response from selling skill: {result.stdout[:200]}")

    if qtn_result.get("status") != "ok":
        err(f"Quotation creation failed: {qtn_result.get('message', 'unknown error')}")

    # Extract quotation ID from response
    quotation_id = None
    if "quotation" in qtn_result:
        quotation_id = qtn_result["quotation"].get("id")

    # Update opportunity with quotation reference
    if quotation_id:
        sql = update_row("opportunity", {
            "quotation_id": P(),
            "updated_at": now(),
        }, {"id": P()})
        conn.execute(sql, (quotation_id, args.opportunity_id))
        audit(conn, "erpclaw-crm", "convert-opportunity-to-quotation", "opportunity", args.opportunity_id,
               new_values={"quotation_id": quotation_id},
               description=f"Created quotation from opportunity")
        conn.commit()

    ok({
        "quotation": qtn_result.get("quotation", {}),
        "opportunity_id": args.opportunity_id,
        "message": f"Quotation created from opportunity {opp['naming_series']}",
    })


# ---------------------------------------------------------------------------
# 11. mark-opportunity-won
# ---------------------------------------------------------------------------

def mark_opportunity_won(conn, args):
    """Mark an opportunity as won (terminal state).

    Required: --opportunity-id
    Sets probability to 100, weighted_revenue = expected_revenue.
    """
    if not args.opportunity_id:
        err("--opportunity-id is required")

    opp = _validate_opportunity_exists(conn, args.opportunity_id)

    if opp["stage"] in ("won", "lost"):
        err(f"Opportunity is already {opp['stage']}. Terminal states cannot be changed.")

    new_weighted = opp["expected_revenue"]  # 100% probability

    # Wave 1B F3 dual-write: resolve the terminal-won stage of the opportunity's
    # pipeline (or the default) so pipeline_stage_id mirrors the legacy 'won' text.
    won_stage_id = _resolve_stage_id_for_opportunity(conn, opp, "won")
    if won_stage_id is None:
        # No name='won' stage; fall back to the pipeline's flagged terminal-won.
        pipeline_id = None
        if opp["pipeline_stage_id"] if "pipeline_stage_id" in opp.keys() else None:
            r = conn.execute("SELECT crm_pipeline_id FROM crm_pipeline_stage WHERE id=?",
                             (opp["pipeline_stage_id"],)).fetchone()
            pipeline_id = r["crm_pipeline_id"] if r else None
        if not pipeline_id:
            pipeline_id = _default_pipeline_id(conn)
        if pipeline_id:
            won_stage_id = _terminal_stage_id(conn, pipeline_id, won=True)

    set_data = {
        "stage": ValueWrapper("won"),
        "probability": ValueWrapper("100"),
        "weighted_revenue": P(),
        "updated_at": now(),
    }
    exec_values = [new_weighted]
    if won_stage_id is not None:
        set_data["pipeline_stage_id"] = P()
        exec_values.append(won_stage_id)
    sql = update_row("opportunity", set_data, {"id": P()})
    exec_values.append(args.opportunity_id)
    conn.execute(sql, exec_values)

    audit(conn, "erpclaw-crm", "mark-opportunity-won", "opportunity", args.opportunity_id,
           old_values={"stage": opp["stage"], "probability": opp["probability"]},
           new_values={"stage": "won", "probability": "100"},
           description=f"Opportunity marked as won")
    conn.commit()

    ok({
        "opportunity": {
            "id": args.opportunity_id,
            "naming_series": opp["naming_series"],
            "stage": "won",
            "probability": "100",
            "expected_revenue": opp["expected_revenue"],
            "weighted_revenue": new_weighted,
        },
        "message": f"Opportunity {opp['naming_series']} marked as WON",
    })


# ---------------------------------------------------------------------------
# 12. mark-opportunity-lost
# ---------------------------------------------------------------------------

def mark_opportunity_lost(conn, args):
    """Mark an opportunity as lost (terminal state).

    Required: --opportunity-id, --lost-reason
    Sets probability to 0, weighted_revenue = 0.
    """
    if not args.opportunity_id:
        err("--opportunity-id is required")
    if not args.lost_reason:
        err("--lost-reason is required when marking as lost")

    opp = _validate_opportunity_exists(conn, args.opportunity_id)

    if opp["stage"] in ("won", "lost"):
        err(f"Opportunity is already {opp['stage']}. Terminal states cannot be changed.")

    # Wave 1B F3 dual-write: resolve the terminal-lost stage (by name 'lost' or the
    # pipeline's flagged terminal-lost) so pipeline_stage_id mirrors the text.
    lost_stage_id = _resolve_stage_id_for_opportunity(conn, opp, "lost")
    if lost_stage_id is None:
        pipeline_id = None
        if opp["pipeline_stage_id"] if "pipeline_stage_id" in opp.keys() else None:
            r = conn.execute("SELECT crm_pipeline_id FROM crm_pipeline_stage WHERE id=?",
                             (opp["pipeline_stage_id"],)).fetchone()
            pipeline_id = r["crm_pipeline_id"] if r else None
        if not pipeline_id:
            pipeline_id = _default_pipeline_id(conn)
        if pipeline_id:
            lost_stage_id = _terminal_stage_id(conn, pipeline_id, won=False)

    set_data = {
        "stage": ValueWrapper("lost"),
        "probability": ValueWrapper("0"),
        "weighted_revenue": ValueWrapper("0"),
        "lost_reason": P(),
        "updated_at": now(),
    }
    exec_values = [args.lost_reason]
    if lost_stage_id is not None:
        set_data["pipeline_stage_id"] = P()
        exec_values.append(lost_stage_id)
    sql = update_row("opportunity", set_data, {"id": P()})
    exec_values.append(args.opportunity_id)
    conn.execute(sql, exec_values)

    audit(conn, "erpclaw-crm", "mark-opportunity-lost", "opportunity", args.opportunity_id,
           old_values={"stage": opp["stage"]},
           new_values={"stage": "lost", "lost_reason": args.lost_reason},
           description=f"Opportunity marked as lost: {args.lost_reason}")
    conn.commit()

    ok({
        "opportunity": {
            "id": args.opportunity_id,
            "naming_series": opp["naming_series"],
            "stage": "lost",
            "probability": "0",
            "expected_revenue": opp["expected_revenue"],
            "weighted_revenue": "0",
            "lost_reason": args.lost_reason,
        },
        "message": f"Opportunity {opp['naming_series']} marked as LOST",
    })


# ---------------------------------------------------------------------------
# 13. add-campaign
# ---------------------------------------------------------------------------

def add_campaign(conn, args):
    """Add a new campaign.

    Required: --name
    Optional: --campaign-type, --budget, --start-date, --end-date,
              --description, --lead-id (auto-links lead)
    """
    if not args.name:
        err("--name is required")

    if args.campaign_type and args.campaign_type not in VALID_CAMPAIGN_TYPES:
        err(f"--campaign-type must be one of {VALID_CAMPAIGN_TYPES}")

    campaign_id = str(uuid.uuid4())
    budget = args.budget or "0"

    sql, _ = insert_row("campaign", {
        "id": P(), "name": P(), "campaign_type": P(), "start_date": P(),
        "end_date": P(), "budget": P(), "status": ValueWrapper("planned"),
        "description": P(),
    })
    conn.execute(sql,
        (campaign_id, args.name, args.campaign_type, args.start_date,
         args.end_date, budget, args.description),
    )

    # Auto-link lead if provided
    lead_linked = False
    if args.lead_id:
        _validate_lead_exists(conn, args.lead_id)
        cl_id = str(uuid.uuid4())
        sql, _ = insert_row("campaign_lead", {
            "id": P(), "campaign_id": P(), "lead_id": P(),
        })
        conn.execute(sql, (cl_id, campaign_id, args.lead_id))
        lead_linked = True

    audit(conn, "erpclaw-crm", "add-campaign", "campaign", campaign_id,
           new_values={"name": args.name, "campaign_type": args.campaign_type},
           description=f"Created campaign: {args.name}")
    conn.commit()

    resp = {
        "campaign": {
            "id": campaign_id,
            "name": args.name,
            "campaign_type": args.campaign_type,
            "budget": budget,
            "status": "planned",
            "start_date": args.start_date,
            "end_date": args.end_date,
        },
        "message": f"Campaign '{args.name}' created",
    }
    if lead_linked:
        resp["lead_linked"] = args.lead_id

    ok(resp)


# ---------------------------------------------------------------------------
# 14. list-campaigns
# ---------------------------------------------------------------------------

def list_campaigns(conn, args):
    """List campaigns with lead counts.

    Optional: --status, --limit, --offset
    """
    # PyPika: skipped — complex LEFT JOIN + CASE SUM + GROUP BY with aliased table
    conditions = ["1=1"]
    params = []

    if args.status:
        conditions.append("c.status = ?")
        params.append(args.status)

    where = " AND ".join(conditions)
    limit = int(args.limit or 20)
    offset = int(args.offset or 0)

    rows = conn.execute(
        f"""SELECT c.*,
               COUNT(cl.id) AS total_leads,
               SUM(CASE WHEN cl.converted = 1 THEN 1 ELSE 0 END) AS converted_leads
           FROM campaign c
           LEFT JOIN campaign_lead cl ON cl.campaign_id = c.id
           WHERE {where}
           GROUP BY c.id
           ORDER BY c.created_at DESC
           LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()

    q_cnt = Q.from_(_t_campaign).select(fn.Count("*").as_("cnt"))
    if args.status:
        q_cnt = q_cnt.where(_t_campaign.status == P())
    total = conn.execute(q_cnt.get_sql(), [p for p in params]).fetchone()["cnt"]

    ok({
        "campaigns": [row_to_dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + limit < total,
    })


# ---------------------------------------------------------------------------
# 15. add-activity
# ---------------------------------------------------------------------------

def add_activity(conn, args):
    """Add a CRM activity.

    Required: --activity-type, --subject, --activity-date
    Optional: --lead-id, --opportunity-id, --customer-id, --description,
              --created-by, --next-action-date
    """
    if not args.activity_type:
        err("--activity-type is required")
    if not args.subject:
        err("--subject is required")
    if not args.activity_date:
        err("--activity-date is required")

    if args.activity_type not in VALID_ACTIVITY_TYPES:
        err(f"--activity-type must be one of {VALID_ACTIVITY_TYPES}")

    # At least one reference should be provided
    if not any([args.lead_id, args.opportunity_id, args.customer_id]):
        err("At least one of --lead-id, --opportunity-id, or --customer-id is required")

    if args.lead_id:
        _validate_lead_exists(conn, args.lead_id)
    if args.opportunity_id:
        _validate_opportunity_exists(conn, args.opportunity_id)
    if args.customer_id:
        _validate_customer_exists(conn, args.customer_id)

    activity_id = str(uuid.uuid4())

    sql, _ = insert_row("crm_activity", {
        "id": P(), "activity_type": P(), "subject": P(), "description": P(),
        "activity_date": P(), "lead_id": P(), "opportunity_id": P(),
        "customer_id": P(), "created_by": P(), "next_action_date": P(),
    })
    conn.execute(sql,
        (activity_id, args.activity_type, args.subject, args.description,
         args.activity_date, args.lead_id, args.opportunity_id,
         args.customer_id, args.created_by, args.next_action_date),
    )

    audit(conn, "erpclaw-crm", "add-activity", "crm_activity", activity_id,
           new_values={"activity_type": args.activity_type, "subject": args.subject},
           description=f"Logged {args.activity_type}: {args.subject}")
    conn.commit()

    ok({
        "activity": {
            "id": activity_id,
            "activity_type": args.activity_type,
            "subject": args.subject,
            "activity_date": args.activity_date,
            "lead_id": args.lead_id,
            "opportunity_id": args.opportunity_id,
            "customer_id": args.customer_id,
        },
        "message": f"Activity '{args.subject}' logged",
    })


# ---------------------------------------------------------------------------
# 16. list-activities
# ---------------------------------------------------------------------------

def list_activities(conn, args):
    """List CRM activities with optional filters.

    Optional: --lead-id, --opportunity-id, --activity-type, --limit, --offset
    """
    t = _t_activity
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count("*").as_("cnt"))
    params = []

    if args.lead_id:
        q = q.where(t.lead_id == P())
        q_cnt = q_cnt.where(t.lead_id == P())
        params.append(args.lead_id)
    if args.opportunity_id:
        q = q.where(t.opportunity_id == P())
        q_cnt = q_cnt.where(t.opportunity_id == P())
        params.append(args.opportunity_id)
    if args.activity_type:
        q = q.where(t.activity_type == P())
        q_cnt = q_cnt.where(t.activity_type == P())
        params.append(args.activity_type)

    limit = int(args.limit or 20)
    offset = int(args.offset or 0)

    q = q.orderby(t.activity_date, order=Order.desc).limit(P()).offset(P())

    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()
    total = conn.execute(q_cnt.get_sql(), params).fetchone()["cnt"]

    ok({
        "activities": [row_to_dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + limit < total,
    })


# ---------------------------------------------------------------------------
# 17. pipeline-report
# ---------------------------------------------------------------------------

def pipeline_report(conn, args):
    """Pipeline report with stage-wise aggregation (Wave 1B F3 dual-path).

    Optional: --stage, --from-date, --to-date

    Dual-path (backward-compatible): each opportunity is grouped by its resolved
    stage. When pipeline_stage_id is set, the report joins crm_pipeline_stage and
    groups by the customizable pipeline + stage (ordered by stage_order, the
    pipeline's own ordering). When pipeline_stage_id is NULL (zero-pipeline /
    legacy install), it falls back to the `stage` text column with the original
    7-stage CASE ordering. Both kinds of rows can appear in one report; legacy
    text rows surface under pipeline name '(none)'.
    """
    # PyPika: skipped — CASE-based ORDER BY + decimal_sum aggregate + LEFT JOIN.
    conditions = ["1=1"]
    params = []

    if args.stage:
        conditions.append("o.stage = ?")
        params.append(args.stage)
    if args.from_date:
        conditions.append("o.created_at >= ?")
        params.append(args.from_date)
    if args.to_date:
        conditions.append("o.created_at <= ?")
        params.append(args.to_date + " 23:59:59")

    where = " AND ".join(conditions)

    has_pipeline_tables = table_exists(conn, "crm_pipeline_stage")

    if has_pipeline_tables:
        # Resolve each opportunity's stage via its pipeline stage when set, else
        # the legacy stage text. Group by (pipeline, resolved stage), preserving
        # pipeline-defined order for FK rows and the legacy CASE order for text rows.
        stage_rows = conn.execute(
            f"""SELECT
                   COALESCE(p.name, '(none)') AS pipeline,
                   COALESCE(ps.name, o.stage) AS stage,
                   COALESCE(ps.stage_order,
                            CASE o.stage
                                WHEN 'new' THEN 1 WHEN 'contacted' THEN 2
                                WHEN 'qualified' THEN 3 WHEN 'proposal_sent' THEN 4
                                WHEN 'negotiation' THEN 5 WHEN 'won' THEN 6
                                WHEN 'lost' THEN 7 ELSE 99
                            END) AS sort_order,
                   COALESCE(ps.is_terminal_won, CASE o.stage WHEN 'won' THEN 1 ELSE 0 END) AS won_flag,
                   COALESCE(ps.is_terminal_lost, CASE o.stage WHEN 'lost' THEN 1 ELSE 0 END) AS lost_flag,
                   COUNT(*) AS count,
                   COALESCE(decimal_sum(o.expected_revenue), '0') AS total_expected_revenue,
                   COALESCE(decimal_sum(o.weighted_revenue), '0') AS total_weighted_revenue
               FROM opportunity o
               LEFT JOIN crm_pipeline_stage ps ON ps.id = o.pipeline_stage_id
               LEFT JOIN crm_pipeline p ON p.id = ps.crm_pipeline_id
               WHERE {where}
               -- GROUP BY by output-column ordinal: PG strict-GROUP-BY-safe (groups by the
               -- SELECT expressions, not bare aliases) and identical semantics on SQLite.
               GROUP BY 1, 2, 3, 4, 5
               ORDER BY pipeline, sort_order""",
            params,
        ).fetchall()
    else:
        # Foundation-only / pre-F3: legacy single-pipeline text path (unchanged).
        stage_rows = conn.execute(
            f"""SELECT '(none)' AS pipeline, o.stage AS stage,
                   CASE o.stage
                       WHEN 'new' THEN 1 WHEN 'contacted' THEN 2 WHEN 'qualified' THEN 3
                       WHEN 'proposal_sent' THEN 4 WHEN 'negotiation' THEN 5
                       WHEN 'won' THEN 6 WHEN 'lost' THEN 7 ELSE 99
                   END AS sort_order,
                   CASE o.stage WHEN 'won' THEN 1 ELSE 0 END AS won_flag,
                   CASE o.stage WHEN 'lost' THEN 1 ELSE 0 END AS lost_flag,
                   COUNT(*) AS count,
                   COALESCE(decimal_sum(o.expected_revenue), '0') AS total_expected_revenue,
                   COALESCE(decimal_sum(o.weighted_revenue), '0') AS total_weighted_revenue
               FROM opportunity o
               WHERE {where}
               GROUP BY o.stage
               ORDER BY sort_order""",
            params,
        ).fetchall()

    stages = []
    total_won = 0
    total_lost = 0
    total_all = 0
    for row in stage_rows:
        cnt = row["count"]
        total_all += cnt
        if row["won_flag"]:
            total_won += cnt
        elif row["lost_flag"]:
            total_lost += cnt
        stages.append({
            "pipeline": row["pipeline"],
            "stage": row["stage"],
            "count": cnt,
            "total_expected_revenue": str(Decimal(str(row["total_expected_revenue"])).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP)),
            "total_weighted_revenue": str(Decimal(str(row["total_weighted_revenue"])).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP)),
        })

    total_closed = total_won + total_lost
    conversion_rate = "0.00"
    if total_closed > 0:
        rate = (Decimal(str(total_won)) / Decimal(str(total_closed))) * Decimal("100")
        conversion_rate = str(rate.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    ok({
        "pipeline": {
            "stages": stages,
            "total_opportunities": total_all,
            "total_won": total_won,
            "total_lost": total_lost,
            "conversion_rate_pct": conversion_rate,
        },
    })


# ---------------------------------------------------------------------------
# 18. status
# ---------------------------------------------------------------------------

def status_action(conn, args):
    """CRM status summary."""
    q = Q.from_(_t_lead).select(fn.Count("*").as_("cnt"))
    lead_count = conn.execute(q.get_sql()).fetchone()["cnt"]

    q = (Q.from_(_t_lead).select(fn.Count("*").as_("cnt"))
         .where(_t_lead.status.notin([ValueWrapper("converted"), ValueWrapper("lost")])))
    active_leads = conn.execute(q.get_sql()).fetchone()["cnt"]

    q = Q.from_(_t_opportunity).select(fn.Count("*").as_("cnt"))
    opp_count = conn.execute(q.get_sql()).fetchone()["cnt"]

    q = (Q.from_(_t_opportunity).select(fn.Count("*").as_("cnt"))
         .where(_t_opportunity.stage.notin([ValueWrapper("won"), ValueWrapper("lost")])))
    open_opps = conn.execute(q.get_sql()).fetchone()["cnt"]

    q = Q.from_(_t_campaign).select(fn.Count("*").as_("cnt"))
    campaign_count = conn.execute(q.get_sql()).fetchone()["cnt"]

    q = Q.from_(_t_activity).select(fn.Count("*").as_("cnt"))
    activity_count = conn.execute(q.get_sql()).fetchone()["cnt"]

    ok({
        "crm_status": {
            "leads": {"total": lead_count, "active": active_leads},
            "opportunities": {"total": opp_count, "open": open_opps},
            "campaigns": {"total": campaign_count},
            "activities": {"total": activity_count},
        },
        "message": f"CRM: {active_leads} active leads, {open_opps} open opportunities",
    })


# ===========================================================================
# Wave 1B F1 — Contact + Company model
# crm_contact / crm_company / crm_contact_role (growth-owned). Foundation
# lead/opportunity/customer/crm_activity carry nullable opaque FK columns to
# these tables (ADR-0023); growth is the SOLE writer of those columns.
# ===========================================================================

def _validate_crm_contact_exists(conn, contact_id, company_id=None):
    t = _t_crm_contact
    q = Q.from_(t).select(t.star).where(t.id == P())
    params = [contact_id]
    if company_id is not None:
        q = q.where(t.company_id == P())
        params.append(company_id)
    row = conn.execute(q.get_sql(), params).fetchone()
    if not row:
        err(f"Contact {contact_id} not found",
            suggestion="Use 'list crm contacts' to see available contacts.")
    return row


def _validate_crm_company_exists(conn, crm_company_id, company_id=None):
    t = _t_crm_company
    q = Q.from_(t).select(t.star).where(t.id == P())
    params = [crm_company_id]
    if company_id is not None:
        q = q.where(t.company_id == P())
        params.append(company_id)
    row = conn.execute(q.get_sql(), params).fetchone()
    if not row:
        err(f"CRM company {crm_company_id} not found",
            suggestion="Use 'list crm companies' to see available companies.")
    return row


def _contact_email_taken(conn, company_id, email, exclude_id=None):
    """Case-insensitive email duplicate check within a company."""
    sql = ("SELECT id FROM crm_contact "
           "WHERE company_id = ? AND email IS NOT NULL AND LOWER(email) = LOWER(?)")
    params = [company_id, email]
    if exclude_id:
        sql += " AND id != ?"
        params.append(exclude_id)
    return conn.execute(sql, params).fetchone()


def _company_domain_taken(conn, company_id, domain, exclude_id=None):
    """Case-insensitive domain duplicate check within a company."""
    sql = ("SELECT id FROM crm_company "
           "WHERE company_id = ? AND domain IS NOT NULL AND LOWER(domain) = LOWER(?)")
    params = [company_id, domain]
    if exclude_id:
        sql += " AND id != ?"
        params.append(exclude_id)
    return conn.execute(sql, params).fetchone()


# ---------------------------------------------------------------------------
# F1.1 add-crm-contact
# ---------------------------------------------------------------------------

def add_crm_contact(conn, args):
    """Add a new CRM contact (person).

    Required: --name
    Optional: --email, --phone, --mobile, --job-title, --crm-company-id,
              --lifecycle, --assigned-to, --notes
    Blocks on a case-insensitive duplicate email within the company.
    """
    if not args.name:
        err("--name is required")

    lifecycle = args.lifecycle or "lead"
    if lifecycle not in VALID_CONTACT_LIFECYCLES:
        err(f"--lifecycle must be one of {VALID_CONTACT_LIFECYCLES}")

    if args.email and not _EMAIL_RE.match(args.email):
        err(f"Invalid email format for --email: '{args.email}'")

    company_id = _resolve_company_id(conn, args)

    if args.email and _contact_email_taken(conn, company_id, args.email):
        err(f"A contact with email '{args.email}' already exists.",
            suggestion="Use 'merge crm contacts' to consolidate duplicates.")

    crm_company_id = getattr(args, "crm_company_id", None)
    if crm_company_id:
        _validate_crm_company_exists(conn, crm_company_id, company_id)

    contact_id = str(uuid.uuid4())
    sql, _ = insert_row("crm_contact", {
        "id": P(), "name": P(), "email": P(), "phone": P(), "mobile": P(),
        "job_title": P(), "linkedin_url": P(), "lifecycle": P(),
        "crm_company_id": P(), "assigned_to_user_id": P(), "notes": P(),
        "company_id": P(),
    })
    conn.execute(sql, (
        contact_id, args.name, args.email, args.phone,
        getattr(args, "mobile", None), getattr(args, "job_title", None),
        getattr(args, "linkedin_url", None), lifecycle, crm_company_id,
        args.assigned_to, args.notes, company_id,
    ))

    audit(conn, "erpclaw-crm", "add-crm-contact", "crm_contact", contact_id,
          new_values={"name": args.name, "email": args.email},
          description=f"Created contact: {args.name}")
    conn.commit()

    ok({
        "crm_contact": {
            "id": contact_id, "name": args.name, "email": args.email,
            "lifecycle": lifecycle, "crm_company_id": crm_company_id,
        },
        "message": f"Contact '{args.name}' created",
    })


# ---------------------------------------------------------------------------
# F1.2 update-crm-contact
# ---------------------------------------------------------------------------

def update_crm_contact(conn, args):
    """Update an existing CRM contact.

    Required: --crm-contact-id
    Optional: --name, --email, --phone, --mobile, --job-title,
              --crm-company-id, --lifecycle, --assigned-to, --notes
    """
    contact_id = getattr(args, "crm_contact_id", None)
    if not contact_id:
        err("--crm-contact-id is required")

    contact = _validate_crm_contact_exists(conn, contact_id)
    company_id = contact["company_id"]
    old_values = row_to_dict(contact)

    if args.lifecycle and args.lifecycle not in VALID_CONTACT_LIFECYCLES:
        err(f"--lifecycle must be one of {VALID_CONTACT_LIFECYCLES}")
    if args.email:
        if not _EMAIL_RE.match(args.email):
            err(f"Invalid email format for --email: '{args.email}'")
        if _contact_email_taken(conn, company_id, args.email, exclude_id=contact_id):
            err(f"A contact with email '{args.email}' already exists.")

    crm_company_id = getattr(args, "crm_company_id", None)
    if crm_company_id:
        _validate_crm_company_exists(conn, crm_company_id, company_id)

    field_map = {
        "name": args.name,
        "email": args.email,
        "phone": args.phone,
        "mobile": getattr(args, "mobile", None),
        "job_title": getattr(args, "job_title", None),
        "linkedin_url": getattr(args, "linkedin_url", None),
        "lifecycle": args.lifecycle,
        "crm_company_id": crm_company_id,
        "assigned_to_user_id": args.assigned_to,
        "notes": args.notes,
    }
    data = {k: v for k, v in field_map.items() if v is not None}
    if not data:
        err("No fields to update. Provide at least one optional flag.")
    data["updated_at"] = now()

    sql, params = dynamic_update("crm_contact", data, {"id": contact_id})
    conn.execute(sql, params)

    audit(conn, "erpclaw-crm", "update-crm-contact", "crm_contact", contact_id,
          old_values=old_values, description="Updated contact")
    conn.commit()

    q = Q.from_(_t_crm_contact).select(_t_crm_contact.star).where(_t_crm_contact.id == P())
    updated = conn.execute(q.get_sql(), (contact_id,)).fetchone()
    ok({"crm_contact": row_to_dict(updated), "message": "Contact updated"})


# ---------------------------------------------------------------------------
# F1.3 get-crm-contact
# ---------------------------------------------------------------------------

def get_crm_contact(conn, args):
    """Get a CRM contact with its company associations.

    Required: --crm-contact-id
    """
    contact_id = getattr(args, "crm_contact_id", None)
    if not contact_id:
        err("--crm-contact-id is required")

    contact = _validate_crm_contact_exists(conn, contact_id)
    contact_dict = row_to_dict(contact)

    cr = _t_crm_contact_role
    co = _t_crm_company
    q = (Q.from_(cr).join(co).on(cr.crm_company_id == co.id)
         .select(cr.star, co.name.as_("company_name"), co.domain.as_("company_domain"))
         .where(cr.crm_contact_id == P()))
    roles = conn.execute(q.get_sql(), (contact_id,)).fetchall()
    contact_dict["roles"] = [row_to_dict(r) for r in roles]

    ok({"crm_contact": contact_dict})


# ---------------------------------------------------------------------------
# F1.4 list-crm-contacts
# ---------------------------------------------------------------------------

def list_crm_contacts(conn, args):
    """List CRM contacts with optional filters.

    Optional: --crm-company-id, --lifecycle, --search, --saved-view-id,
              --limit, --offset
    --saved-view-id replays a saved view's filter-JSON (Wave 1B F4).
    Soft-deleted (lifecycle='other' via merge) contacts are excluded unless
    --lifecycle other is explicitly requested.
    """
    company_id = _resolve_company_id(conn, args)
    clauses = ["crm_contact.company_id = ?"]
    params = [company_id]

    crm_company_id = getattr(args, "crm_company_id", None)
    if crm_company_id:
        clauses.append("crm_company_id = ?")
        params.append(crm_company_id)
    if args.lifecycle:
        clauses.append("lifecycle = ?")
        params.append(args.lifecycle)
    if args.search:
        clauses.append("(name LIKE ? OR email LIKE ? OR job_title LIKE ?)")
        params.extend([f"%{args.search}%"] * 3)

    _exec_list_raw(conn, args, "crm_contact", "crm_contact", "crm_contact.*",
                   clauses, params, "crm_contacts")


# ---------------------------------------------------------------------------
# F1.5 remove-crm-contact (soft delete)
# ---------------------------------------------------------------------------

def remove_crm_contact(conn, args):
    """Soft-delete a contact (lifecycle='other'); cascades to crm_contact_role.

    Required: --crm-contact-id
    """
    contact_id = getattr(args, "crm_contact_id", None)
    if not contact_id:
        err("--crm-contact-id is required")

    contact = _validate_crm_contact_exists(conn, contact_id)

    try:
        sql, params = dynamic_update(
            "crm_contact", {"lifecycle": "other", "updated_at": now()},
            {"id": contact_id})
        conn.execute(sql, params)
        conn.execute("DELETE FROM crm_contact_role WHERE crm_contact_id = ?", (contact_id,))
        audit(conn, "erpclaw-crm", "remove-crm-contact", "crm_contact", contact_id,
              old_values=row_to_dict(contact), description="Soft-deleted contact")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    ok({"crm_contact_id": contact_id, "message": "Contact removed (soft delete)"})


# ---------------------------------------------------------------------------
# F1.6 add-crm-company
# ---------------------------------------------------------------------------

def add_crm_company(conn, args):
    """Add a new CRM company (organization).

    Required: --name
    Optional: --domain, --industry, --revenue, --linked-customer-id,
              --lifecycle, --assigned-to, --notes
    Blocks on a case-insensitive duplicate domain within the company.
    """
    if not args.name:
        err("--name is required")

    lifecycle = args.lifecycle or "prospect"
    if lifecycle not in VALID_COMPANY_LIFECYCLES:
        err(f"--lifecycle must be one of {VALID_COMPANY_LIFECYCLES}")

    company_id = _resolve_company_id(conn, args)

    domain = getattr(args, "domain", None)
    if domain and _company_domain_taken(conn, company_id, domain):
        err(f"A company with domain '{domain}' already exists.")

    revenue = getattr(args, "revenue", None)
    if revenue is not None:
        revenue = str(round_currency(to_decimal(revenue)))

    linked_customer_id = getattr(args, "linked_customer_id", None)
    if linked_customer_id:
        _validate_customer_exists(conn, linked_customer_id)

    crm_company_id = str(uuid.uuid4())
    sql, _ = insert_row("crm_company", {
        "id": P(), "name": P(), "domain": P(), "industry": P(),
        "annual_revenue": P(), "linkedin_url": P(), "lifecycle": P(),
        "linked_customer_id": P(), "assigned_to_user_id": P(), "notes": P(),
        "company_id": P(),
    })
    conn.execute(sql, (
        crm_company_id, args.name, domain, args.industry, revenue,
        getattr(args, "linkedin_url", None), lifecycle, linked_customer_id,
        args.assigned_to, args.notes, company_id,
    ))

    audit(conn, "erpclaw-crm", "add-crm-company", "crm_company", crm_company_id,
          new_values={"name": args.name, "domain": domain},
          description=f"Created company: {args.name}")
    conn.commit()

    ok({
        "crm_company": {
            "id": crm_company_id, "name": args.name, "domain": domain,
            "lifecycle": lifecycle, "annual_revenue": revenue,
        },
        "message": f"Company '{args.name}' created",
    })


# ---------------------------------------------------------------------------
# F1.7 update-crm-company
# ---------------------------------------------------------------------------

def update_crm_company(conn, args):
    """Update an existing CRM company.

    Required: --crm-company-id
    Optional: --name, --domain, --industry, --revenue, --linked-customer-id,
              --lifecycle, --assigned-to, --notes
    """
    crm_company_id = getattr(args, "crm_company_id", None)
    if not crm_company_id:
        err("--crm-company-id is required")

    company = _validate_crm_company_exists(conn, crm_company_id)
    company_id = company["company_id"]
    old_values = row_to_dict(company)

    if args.lifecycle and args.lifecycle not in VALID_COMPANY_LIFECYCLES:
        err(f"--lifecycle must be one of {VALID_COMPANY_LIFECYCLES}")

    domain = getattr(args, "domain", None)
    if domain and _company_domain_taken(conn, company_id, domain, exclude_id=crm_company_id):
        err(f"A company with domain '{domain}' already exists.")

    linked_customer_id = getattr(args, "linked_customer_id", None)
    if linked_customer_id:
        _validate_customer_exists(conn, linked_customer_id)

    revenue = getattr(args, "revenue", None)
    if revenue is not None:
        revenue = str(round_currency(to_decimal(revenue)))

    field_map = {
        "name": args.name,
        "domain": domain,
        "industry": args.industry,
        "annual_revenue": revenue,
        "linkedin_url": getattr(args, "linkedin_url", None),
        "lifecycle": args.lifecycle,
        "linked_customer_id": linked_customer_id,
        "assigned_to_user_id": args.assigned_to,
        "notes": args.notes,
    }
    data = {k: v for k, v in field_map.items() if v is not None}
    if not data:
        err("No fields to update. Provide at least one optional flag.")
    data["updated_at"] = now()

    sql, params = dynamic_update("crm_company", data, {"id": crm_company_id})
    conn.execute(sql, params)

    audit(conn, "erpclaw-crm", "update-crm-company", "crm_company", crm_company_id,
          old_values=old_values, description="Updated company")
    conn.commit()

    q = Q.from_(_t_crm_company).select(_t_crm_company.star).where(_t_crm_company.id == P())
    updated = conn.execute(q.get_sql(), (crm_company_id,)).fetchone()
    ok({"crm_company": row_to_dict(updated), "message": "Company updated"})


# ---------------------------------------------------------------------------
# F1.8 get-crm-company
# ---------------------------------------------------------------------------

def get_crm_company(conn, args):
    """Get a CRM company with its associated contacts.

    Required: --crm-company-id
    """
    crm_company_id = getattr(args, "crm_company_id", None)
    if not crm_company_id:
        err("--crm-company-id is required")

    company = _validate_crm_company_exists(conn, crm_company_id)
    company_dict = row_to_dict(company)

    cr = _t_crm_contact_role
    ct = _t_crm_contact
    q = (Q.from_(cr).join(ct).on(cr.crm_contact_id == ct.id)
         .select(cr.star, ct.name.as_("contact_name"), ct.email.as_("contact_email"))
         .where(cr.crm_company_id == P()))
    roles = conn.execute(q.get_sql(), (crm_company_id,)).fetchall()
    company_dict["contacts"] = [row_to_dict(r) for r in roles]

    ok({"crm_company": company_dict})


# ---------------------------------------------------------------------------
# F1.9 list-crm-companies
# ---------------------------------------------------------------------------

def list_crm_companies(conn, args):
    """List CRM companies with optional filters.

    Optional: --lifecycle, --search, --saved-view-id, --limit, --offset
    --saved-view-id replays a saved view's filter-JSON (Wave 1B F4).
    """
    company_id = _resolve_company_id(conn, args)
    clauses = ["crm_company.company_id = ?"]
    params = [company_id]

    if args.lifecycle:
        clauses.append("lifecycle = ?")
        params.append(args.lifecycle)
    if args.search:
        clauses.append("(name LIKE ? OR domain LIKE ? OR industry LIKE ?)")
        params.extend([f"%{args.search}%"] * 3)

    _exec_list_raw(conn, args, "crm_company", "crm_company", "crm_company.*",
                   clauses, params, "crm_companies")


# ---------------------------------------------------------------------------
# F1.10 link-contact-to-company
# ---------------------------------------------------------------------------

def link_contact_to_company(conn, args):
    """Associate a contact with a company (crm_contact_role row).

    Required: --crm-contact-id, --crm-company-id
    Optional: --role-title, --is-primary
    """
    contact_id = getattr(args, "crm_contact_id", None)
    crm_company_id = getattr(args, "crm_company_id", None)
    if not contact_id:
        err("--crm-contact-id is required")
    if not crm_company_id:
        err("--crm-company-id is required")

    contact = _validate_crm_contact_exists(conn, contact_id)
    company_id = contact["company_id"]
    _validate_crm_company_exists(conn, crm_company_id, company_id)

    existing = conn.execute(
        "SELECT id FROM crm_contact_role WHERE crm_contact_id = ? AND crm_company_id = ?",
        (contact_id, crm_company_id)).fetchone()
    if existing:
        err("This contact is already linked to this company.",
            suggestion="Use 'update' to change the role title.")

    is_primary = 1 if getattr(args, "is_primary", False) else 0
    role_id = str(uuid.uuid4())
    sql, _ = insert_row("crm_contact_role", {
        "id": P(), "crm_contact_id": P(), "crm_company_id": P(),
        "role_title": P(), "is_primary": P(), "company_id": P(),
    })
    conn.execute(sql, (
        role_id, contact_id, crm_company_id,
        getattr(args, "role_title", None), is_primary, company_id,
    ))

    audit(conn, "erpclaw-crm", "link-contact-to-company", "crm_contact_role", role_id,
          new_values={"contact": contact_id, "company": crm_company_id},
          description="Linked contact to company")
    conn.commit()

    ok({
        "crm_contact_role": {
            "id": role_id, "crm_contact_id": contact_id,
            "crm_company_id": crm_company_id, "is_primary": bool(is_primary),
        },
        "message": "Contact linked to company",
    })


# ---------------------------------------------------------------------------
# F1.11 merge-crm-contacts (single transaction)
# ---------------------------------------------------------------------------

def merge_crm_contacts(conn, args):
    """Merge a duplicate contact into a primary (single transaction).

    Required: --primary-contact-id, --duplicate-contact-id
    Copies non-null fields from duplicate onto primary where primary is blank,
    reassigns all FK references (crm_contact_role + foundation lead /
    opportunity / crm_activity.crm_contact_id) to the primary, then soft-deletes
    the duplicate (lifecycle='other'). All in one BEGIN...COMMIT; any failure
    rolls back fully.
    """
    primary_id = getattr(args, "primary_contact_id", None)
    duplicate_id = getattr(args, "duplicate_contact_id", None)
    if not primary_id:
        err("--primary-contact-id is required")
    if not duplicate_id:
        err("--duplicate-contact-id is required")
    if primary_id == duplicate_id:
        err("Primary and duplicate must be different contacts.")

    primary = _validate_crm_contact_exists(conn, primary_id)
    duplicate = _validate_crm_contact_exists(conn, duplicate_id)
    if primary["company_id"] != duplicate["company_id"]:
        err("Cannot merge contacts from different companies.")

    # Fill blank primary fields from the duplicate (non-null wins where primary is empty).
    fill_fields = ("email", "phone", "mobile", "job_title", "linkedin_url",
                   "crm_company_id", "assigned_to_user_id", "notes",
                   "address_line1", "address_line2", "city", "state",
                   "postal_code", "country")
    fill = {}
    for f in fill_fields:
        if (primary[f] is None or primary[f] == "") and duplicate[f] is not None:
            # email backfill must not collide with another contact's email
            if f == "email" and _contact_email_taken(
                    conn, primary["company_id"], duplicate[f], exclude_id=primary_id):
                continue
            fill[f] = duplicate[f]

    try:
        if fill:
            fill["updated_at"] = now()
            sql, params = dynamic_update("crm_contact", fill, {"id": primary_id})
            conn.execute(sql, params)

        # Reassign crm_contact_role rows (avoid duplicate-link collisions).
        dup_roles = conn.execute(
            "SELECT crm_company_id FROM crm_contact_role WHERE crm_contact_id = ?",
            (duplicate_id,)).fetchall()
        for r in dup_roles:
            clash = conn.execute(
                "SELECT id FROM crm_contact_role WHERE crm_contact_id = ? AND crm_company_id = ?",
                (primary_id, r["crm_company_id"])).fetchone()
            if clash:
                conn.execute(
                    "DELETE FROM crm_contact_role WHERE crm_contact_id = ? AND crm_company_id = ?",
                    (duplicate_id, r["crm_company_id"]))
            else:
                conn.execute(
                    "UPDATE crm_contact_role SET crm_contact_id = ? WHERE crm_contact_id = ? AND crm_company_id = ?",
                    (primary_id, duplicate_id, r["crm_company_id"]))

        # Reassign the foundation FK references (lead / opportunity / crm_activity).
        # lead + opportunity are erpclaw-crm-owned (this module already writes them
        # via the same PyPika builder, e.g. convert-lead-to-opportunity); crm_activity
        # is co-owned by erpclaw-crm (add-activity inserts it). Growth is the sole
        # writer of the crm_contact_id FK column per ADR-0023. Built via dynamic_update
        # (parameterized, module idiom) — never raw/f-string SQL.
        reassigned = {}
        for table in ("lead", "opportunity", "crm_activity"):
            sql, params = dynamic_update(
                table, {"crm_contact_id": primary_id},
                {"crm_contact_id": duplicate_id})
            reassigned[table] = conn.execute(sql, params).rowcount

        # Soft-delete the duplicate.
        sql, params = dynamic_update(
            "crm_contact", {"lifecycle": "other", "updated_at": now()},
            {"id": duplicate_id})
        conn.execute(sql, params)

        audit(conn, "erpclaw-crm", "merge-crm-contacts", "crm_contact", primary_id,
              new_values={"merged_from": duplicate_id, "reassigned": reassigned},
              description=f"Merged contact {duplicate_id} into {primary_id}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    ok({
        "primary_contact_id": primary_id,
        "merged_contact_id": duplicate_id,
        "fields_filled": [k for k in fill if k != "updated_at"],
        "fk_reassigned": reassigned,
        "message": f"Merged contact into {primary['name']}",
    })


# ---------------------------------------------------------------------------
# F1.12 promote-contact-to-customer (cross-skill via call_skill_action)
# ---------------------------------------------------------------------------

def promote_contact_to_customer(conn, args):
    """Promote a contact to a real foundation customer (single transaction).

    Required: --crm-contact-id
    Creates a `customer` row in erpclaw-selling via
    cross_skill.call_skill_action (Article 5 — NOT a raw subprocess), sets the
    contact lifecycle to 'customer', and back-links the contact's company
    (crm_company.linked_customer_id) to the new customer. Any cross-skill
    failure rolls back the entire growth-side transaction.
    """
    from erpclaw_lib.cross_skill import call_skill_action, CrossSkillError

    contact_id = getattr(args, "crm_contact_id", None)
    if not contact_id:
        err("--crm-contact-id is required")

    contact = _validate_crm_contact_exists(conn, contact_id)
    company_id = contact["company_id"]

    if contact["lifecycle"] == "customer":
        err("This contact has already been promoted to a customer.")

    # Resolve the linked CRM company (if any) for the back-reference.
    crm_company_id = contact["crm_company_id"]

    # 1. Create the foundation customer via the owning skill (Article 5).
    cust_args = {
        "--name": contact["name"],
        "--company-id": company_id,
        "--customer-type": "individual",
    }
    if contact["email"]:
        cust_args["--email"] = contact["email"]
    if contact["phone"]:
        cust_args["--phone"] = contact["phone"]

    db_path = getattr(args, "db_path", None)
    try:
        # Target the top-level "erpclaw" router, NOT the "erpclaw-selling" sub-skill:
        # erpclaw-selling is a foundation sub-skill (not a separately-installed top-level
        # skill), so resolve_skill_script("erpclaw-selling") returns None on every install
        # ("not installed"). The erpclaw router dispatches add-customer -> erpclaw-selling
        # internally. (QA box-validation bug, Wave 1B F1 — pytest mocked the call + missed it.)
        resp = call_skill_action(
            "erpclaw", "add-customer", cust_args, db_path=db_path)
    except CrossSkillError as e:
        # Nothing growth-side has been written yet; surface the failure cleanly.
        err(f"Could not create the customer: {e}")

    customer_id = resp.get("customer_id") or (resp.get("customer") or {}).get("id")
    if not customer_id:
        err("Customer creation returned no id; aborting promotion.")

    # 2. Growth-side updates in one transaction; roll back fully on any failure.
    try:
        sql, params = dynamic_update(
            "crm_contact", {"lifecycle": "customer", "updated_at": now()},
            {"id": contact_id})
        conn.execute(sql, params)

        if crm_company_id:
            sql, params = dynamic_update(
                "crm_company",
                {"linked_customer_id": customer_id, "lifecycle": "customer",
                 "updated_at": now()},
                {"id": crm_company_id})
            conn.execute(sql, params)

        audit(conn, "erpclaw-crm", "promote-contact-to-customer", "crm_contact",
              contact_id, new_values={"customer_id": customer_id},
              description=f"Promoted contact to customer {customer_id}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    ok({
        "crm_contact_id": contact_id,
        "customer_id": customer_id,
        "crm_company_id": crm_company_id,
        "linked_customer_id": customer_id if crm_company_id else None,
        "message": f"Contact '{contact['name']}' promoted to customer",
    })


# ===========================================================================
# Wave 1B F2 — Tasks (first-class entity)
# crm_task / crm_task_link (growth-owned). crm_activity is NOT replaced —
# legacy activity_type='task' rows stay valid. A task links to any CRM entity
# (lead/opportunity/customer/crm_contact/crm_company) via crm_task_link, with a
# runtime existence check on every link target.
# ===========================================================================

def _validate_crm_task_exists(conn, task_id, company_id=None):
    t = _t_crm_task
    q = Q.from_(t).select(t.star).where(t.id == P())
    params = [task_id]
    if company_id is not None:
        q = q.where(t.company_id == P())
        params.append(company_id)
    row = conn.execute(q.get_sql(), params).fetchone()
    if not row:
        err(f"Task {task_id} not found",
            suggestion="Use 'list crm tasks' to see available tasks.")
    return row


def _validate_link_entity_exists(conn, entity_type, entity_id, company_id):
    """Runtime FK-existence check for a task link target.

    Validates the entity_type is in the whitelist and that a row with
    entity_id exists in the resolved table, scoped to the same company. All
    five target tables (lead/opportunity/customer/crm_contact/crm_company)
    carry a company_id column, so the scope filter is uniform.
    """
    if entity_type not in VALID_TASK_LINK_TYPES:
        err(f"--entity-type must be one of {VALID_TASK_LINK_TYPES}")
    if not entity_id:
        err("A link target id is required")
    table = _TASK_LINK_TABLE[entity_type]
    et = Table(table)
    row = conn.execute(
        Q.from_(et).select(et.id).where((et.id == P()) & (et.company_id == P())).get_sql(),
        (entity_id, company_id)).fetchone()
    if not row:
        err(f"{entity_type} {entity_id} not found",
            suggestion=f"Use 'list {entity_type}s' to see available records.")
    return row


def _parse_link_to(value):
    """Parse a --link-to "<entity_type>:<entity_id>" token into a tuple.

    Returns (entity_type, entity_id). err()s on a malformed token.
    """
    if ":" not in value:
        err(f"--link-to must be '<entity_type>:<entity_id>', got: '{value}'")
    entity_type, _, entity_id = value.partition(":")
    entity_type = entity_type.strip()
    entity_id = entity_id.strip()
    if not entity_type or not entity_id:
        err(f"--link-to must be '<entity_type>:<entity_id>', got: '{value}'")
    return entity_type, entity_id


def _insert_task_link(conn, task_id, entity_type, entity_id, company_id):
    """Insert a crm_task_link row (caller validated existence). Idempotent on the
    (task, type, id) unique key — returns the link id, or None if it already exists."""
    existing = conn.execute(
        "SELECT id FROM crm_task_link WHERE crm_task_id = ? AND linked_entity_type = ? "
        "AND linked_entity_id = ?",
        (task_id, entity_type, entity_id)).fetchone()
    if existing:
        return None
    link_id = str(uuid.uuid4())
    sql, _ = insert_row("crm_task_link", {
        "id": P(), "crm_task_id": P(), "linked_entity_type": P(),
        "linked_entity_id": P(), "company_id": P(),
    })
    conn.execute(sql, (link_id, task_id, entity_type, entity_id, company_id))
    return link_id


def _refresh_linked_count(conn, task_id):
    """Recompute crm_task.linked_count from crm_task_link (denorm)."""
    n = conn.execute(
        "SELECT COUNT(*) AS c FROM crm_task_link WHERE crm_task_id = ?",
        (task_id,)).fetchone()["c"]
    sql, params = dynamic_update("crm_task", {"linked_count": n}, {"id": task_id})
    conn.execute(sql, params)
    return n


# ---------------------------------------------------------------------------
# F2.1 add-crm-task
# ---------------------------------------------------------------------------

def add_crm_task(conn, args):
    """Add a new CRM task (single transaction including any links).

    Required: --subject
    Optional: --description, --priority, --due-date, --assigned-to,
              --created-by, --link-to "<type>:<id>" (repeatable)
    --due-date may be in the past (backfill); an audit note flags create-as-overdue.
    Every --link-to target is existence-checked at runtime; a bad target rolls
    back the whole create.
    """
    if not args.subject:
        err("--subject is required")

    priority = args.priority or "medium"
    if priority not in VALID_TASK_PRIORITIES:
        err(f"--priority must be one of {VALID_TASK_PRIORITIES}")

    company_id = _resolve_company_id(conn, args)

    # Parse + validate all link targets BEFORE any write (atomic create).
    link_tokens = getattr(args, "link_to", None) or []
    parsed_links = []
    for token in link_tokens:
        entity_type, entity_id = _parse_link_to(token)
        _validate_link_entity_exists(conn, entity_type, entity_id, company_id)
        parsed_links.append((entity_type, entity_id))

    # Flag a past due_date for audit (allowed for backfill).
    overdue_on_create = False
    if args.due_date:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        overdue_on_create = args.due_date < today

    task_id = str(uuid.uuid4())
    try:
        sql, _ = insert_row("crm_task", {
            "id": P(), "subject": P(), "description": P(), "priority": P(),
            "due_date": P(), "assigned_to_user_id": P(), "created_by_user_id": P(),
            "company_id": P(),
        })
        conn.execute(sql, (
            task_id, args.subject, args.description, priority, args.due_date,
            args.assigned_to, getattr(args, "created_by", None), company_id,
        ))

        linked = []
        for entity_type, entity_id in parsed_links:
            if _insert_task_link(conn, task_id, entity_type, entity_id, company_id):
                linked.append({"entity_type": entity_type, "entity_id": entity_id})
        if linked:
            _refresh_linked_count(conn, task_id)

        audit(conn, "erpclaw-crm", "add-crm-task", "crm_task", task_id,
              new_values={"subject": args.subject, "priority": priority,
                          "due_date": args.due_date, "links": linked,
                          "overdue_on_create": overdue_on_create},
              description=(f"Created task: {args.subject}"
                           + (" (created overdue)" if overdue_on_create else "")))
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    ok({
        "crm_task": {
            "id": task_id, "subject": args.subject, "status": "open",
            "priority": priority, "due_date": args.due_date,
            "linked_count": len(linked),
        },
        "links": linked,
        "overdue_on_create": overdue_on_create,
        "message": f"Task '{args.subject}' created",
    })


# ---------------------------------------------------------------------------
# F2.2 update-crm-task
# ---------------------------------------------------------------------------

def update_crm_task(conn, args):
    """Update an existing CRM task.

    Required: --crm-task-id
    Optional: --subject, --description, --priority, --due-date, --assigned-to
    Status is NOT set here — use complete-crm-task / cancel-crm-task for the
    terminal transitions. Terminal tasks (done/cancelled) are frozen.
    """
    task_id = getattr(args, "crm_task_id", None)
    if not task_id:
        err("--crm-task-id is required")

    task = _validate_crm_task_exists(conn, task_id)
    old_values = row_to_dict(task)

    if task["status"] in ("done", "cancelled"):
        err(f"Task is {task['status']}. Terminal tasks cannot be updated.")

    if args.priority and args.priority not in VALID_TASK_PRIORITIES:
        err(f"--priority must be one of {VALID_TASK_PRIORITIES}")

    field_map = {
        "subject": args.subject,
        "description": args.description,
        "priority": args.priority,
        "due_date": args.due_date,
        "assigned_to_user_id": args.assigned_to,
    }
    data = {k: v for k, v in field_map.items() if v is not None}
    if not data:
        err("No fields to update. Provide at least one optional flag.")
    data["updated_at"] = now()

    sql, params = dynamic_update("crm_task", data, {"id": task_id})
    conn.execute(sql, params)

    audit(conn, "erpclaw-crm", "update-crm-task", "crm_task", task_id,
          old_values=old_values, description="Updated task")
    conn.commit()

    q = Q.from_(_t_crm_task).select(_t_crm_task.star).where(_t_crm_task.id == P())
    updated = conn.execute(q.get_sql(), (task_id,)).fetchone()
    ok({"crm_task": row_to_dict(updated), "message": "Task updated"})


# ---------------------------------------------------------------------------
# F2.3 get-crm-task
# ---------------------------------------------------------------------------

def get_crm_task(conn, args):
    """Get a CRM task with its linked entities.

    Required: --crm-task-id
    """
    task_id = getattr(args, "crm_task_id", None)
    if not task_id:
        err("--crm-task-id is required")

    task = _validate_crm_task_exists(conn, task_id)
    task_dict = row_to_dict(task)

    tl = _t_crm_task_link
    q = (Q.from_(tl).select(tl.star)
         .where(tl.crm_task_id == P())
         .orderby(tl.created_at, order=Order.desc))
    links = conn.execute(q.get_sql(), (task_id,)).fetchall()
    task_dict["links"] = [row_to_dict(r) for r in links]

    ok({"crm_task": task_dict})


# ---------------------------------------------------------------------------
# F2.4 list-crm-tasks
# ---------------------------------------------------------------------------

def list_crm_tasks(conn, args):
    """List CRM tasks with optional filters.

    Optional: --status, --priority, --assigned-to, --linked-to "<type>:<id>",
              --overdue, --due-within-days N, --limit, --offset
    """
    t = _t_crm_task
    q = Q.from_(t).select(t.star)
    q_cnt = Q.from_(t).select(fn.Count("*").as_("cnt"))
    params = []

    company_id = _resolve_company_id(conn, args)
    q = q.where(t.company_id == P())
    q_cnt = q_cnt.where(t.company_id == P())
    params.append(company_id)

    if args.status:
        if args.status not in VALID_TASK_STATUSES:
            err(f"--status must be one of {VALID_TASK_STATUSES}")
        q = q.where(t.status == P())
        q_cnt = q_cnt.where(t.status == P())
        params.append(args.status)
    if args.priority:
        q = q.where(t.priority == P())
        q_cnt = q_cnt.where(t.priority == P())
        params.append(args.priority)
    if args.assigned_to:
        q = q.where(t.assigned_to_user_id == P())
        q_cnt = q_cnt.where(t.assigned_to_user_id == P())
        params.append(args.assigned_to)

    # --linked-to "<type>:<id>": restrict to tasks attached to that entity.
    linked_to = getattr(args, "linked_to", None)
    if linked_to:
        entity_type, entity_id = _parse_link_to(linked_to)
        if entity_type not in VALID_TASK_LINK_TYPES:
            err(f"--linked-to type must be one of {VALID_TASK_LINK_TYPES}")
        sub = (Q.from_(_t_crm_task_link).select(_t_crm_task_link.crm_task_id)
               .where((_t_crm_task_link.linked_entity_type == P())
                      & (_t_crm_task_link.linked_entity_id == P())))
        q = q.where(t.id.isin(sub))
        q_cnt = q_cnt.where(t.id.isin(sub))
        params.extend([entity_type, entity_id])

    # --overdue: due_date strictly before today and not terminal.
    if getattr(args, "overdue", False):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        crit = ((t.due_date.notnull()) & (t.due_date < P())
                & (t.status.notin([ValueWrapper("done"), ValueWrapper("cancelled")])))
        q = q.where(crit)
        q_cnt = q_cnt.where(crit)
        params.append(today)

    # --due-within-days N: due_date between today and today+N inclusive.
    dw = getattr(args, "due_within_days", None)
    if dw is not None:
        from datetime import timedelta
        today_dt = datetime.now(timezone.utc).date()
        upper = (today_dt + timedelta(days=int(dw))).strftime("%Y-%m-%d")
        lower = today_dt.strftime("%Y-%m-%d")
        crit = ((t.due_date.notnull()) & (t.due_date >= P()) & (t.due_date <= P()))
        q = q.where(crit)
        q_cnt = q_cnt.where(crit)
        params.extend([lower, upper])

    limit = int(args.limit or 20)
    offset = int(args.offset or 0)
    rows = conn.execute(
        q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P()).get_sql(),
        params + [limit, offset]).fetchall()
    total = conn.execute(q_cnt.get_sql(), params).fetchone()["cnt"]

    ok({
        "crm_tasks": [row_to_dict(r) for r in rows],
        "total": total, "limit": limit, "offset": offset,
        "has_more": offset + limit < total,
    })


# ---------------------------------------------------------------------------
# F2.5 complete-crm-task
# ---------------------------------------------------------------------------

def complete_crm_task(conn, args):
    """Mark a task done (terminal). Idempotency: rejects if already done.

    Required: --crm-task-id
    Optional: --notes
    """
    task_id = getattr(args, "crm_task_id", None)
    if not task_id:
        err("--crm-task-id is required")

    task = _validate_crm_task_exists(conn, task_id)

    if task["status"] == "done":
        err("Task is already done.")
    if task["status"] == "cancelled":
        err("Task is cancelled and cannot be completed.")

    sql, params = dynamic_update("crm_task", {
        "status": "done",
        "completed_at": now(),
        "updated_at": now(),
    }, {"id": task_id})
    conn.execute(sql, params)

    audit(conn, "erpclaw-crm", "complete-crm-task", "crm_task", task_id,
          old_values={"status": task["status"]},
          new_values={"status": "done", "notes": getattr(args, "notes", None)},
          description="Task completed")
    conn.commit()

    ok({
        "crm_task": {"id": task_id, "subject": task["subject"], "status": "done"},
        "message": f"Task '{task['subject']}' completed",
    })


# ---------------------------------------------------------------------------
# F2.6 cancel-crm-task
# ---------------------------------------------------------------------------

def cancel_crm_task(conn, args):
    """Cancel a task (terminal). Rejects if already terminal.

    Required: --crm-task-id
    Optional: --reason
    """
    task_id = getattr(args, "crm_task_id", None)
    if not task_id:
        err("--crm-task-id is required")

    task = _validate_crm_task_exists(conn, task_id)

    if task["status"] in ("done", "cancelled"):
        err(f"Task is already {task['status']}.")

    sql, params = dynamic_update("crm_task", {
        "status": "cancelled",
        "cancel_reason": getattr(args, "reason", None),
        "updated_at": now(),
    }, {"id": task_id})
    conn.execute(sql, params)

    audit(conn, "erpclaw-crm", "cancel-crm-task", "crm_task", task_id,
          old_values={"status": task["status"]},
          new_values={"status": "cancelled", "reason": getattr(args, "reason", None)},
          description="Task cancelled")
    conn.commit()

    ok({
        "crm_task": {"id": task_id, "subject": task["subject"], "status": "cancelled"},
        "message": f"Task '{task['subject']}' cancelled",
    })


# ---------------------------------------------------------------------------
# F2.7 link-task-to-entity
# ---------------------------------------------------------------------------

def link_task_to_entity(conn, args):
    """Attach a task to a CRM entity (crm_task_link row).

    Required: --task, --entity-type, --entity-id
    Validates the target entity exists at runtime. Re-linking an existing
    (task, type, id) tuple is rejected (already linked).
    """
    task_id = getattr(args, "task", None)
    entity_type = getattr(args, "entity_type", None)
    entity_id = getattr(args, "entity_id", None)
    if not task_id:
        err("--task is required")
    if not entity_type:
        err("--entity-type is required")
    if not entity_id:
        err("--entity-id is required")

    task = _validate_crm_task_exists(conn, task_id)
    company_id = task["company_id"]
    _validate_link_entity_exists(conn, entity_type, entity_id, company_id)

    link_id = _insert_task_link(conn, task_id, entity_type, entity_id, company_id)
    if link_id is None:
        err("This task is already linked to that entity.")
    _refresh_linked_count(conn, task_id)

    audit(conn, "erpclaw-crm", "link-task-to-entity", "crm_task_link", link_id,
          new_values={"task": task_id, "entity_type": entity_type, "entity_id": entity_id},
          description="Linked task to entity")
    conn.commit()

    ok({
        "crm_task_link": {
            "id": link_id, "crm_task_id": task_id,
            "linked_entity_type": entity_type, "linked_entity_id": entity_id,
        },
        "message": f"Task linked to {entity_type}",
    })


# ---------------------------------------------------------------------------
# F2.8 unlink-task-from-entity
# ---------------------------------------------------------------------------

def unlink_task_from_entity(conn, args):
    """Remove a task<->entity link (crm_task_link row).

    Required: --task, --entity-type, --entity-id
    Rejects if no such link exists (no silent no-op).
    """
    task_id = getattr(args, "task", None)
    entity_type = getattr(args, "entity_type", None)
    entity_id = getattr(args, "entity_id", None)
    if not task_id:
        err("--task is required")
    if not entity_type:
        err("--entity-type is required")
    if not entity_id:
        err("--entity-id is required")

    task = _validate_crm_task_exists(conn, task_id)

    existing = conn.execute(
        "SELECT id FROM crm_task_link WHERE crm_task_id = ? AND linked_entity_type = ? "
        "AND linked_entity_id = ?",
        (task_id, entity_type, entity_id)).fetchone()
    if not existing:
        err(f"Task is not linked to {entity_type} {entity_id}.")

    conn.execute(
        "DELETE FROM crm_task_link WHERE crm_task_id = ? AND linked_entity_type = ? "
        "AND linked_entity_id = ?",
        (task_id, entity_type, entity_id))
    _refresh_linked_count(conn, task_id)

    audit(conn, "erpclaw-crm", "unlink-task-from-entity", "crm_task_link", existing["id"],
          old_values={"task": task_id, "entity_type": entity_type, "entity_id": entity_id},
          description="Unlinked task from entity")
    conn.commit()

    ok({
        "crm_task_id": task_id, "entity_type": entity_type, "entity_id": entity_id,
        "message": f"Task unlinked from {entity_type}",
    })


# ===========================================================================
# Wave 1B F3 — Pipeline stages (customizable)
# crm_pipeline / crm_pipeline_stage (growth-owned). Foundation opportunity
# carries a nullable opaque FK column pipeline_stage_id -> crm_pipeline_stage
# (ADR-0023; growth is the SOLE writer of that column). The hardcoded
# opportunity.stage CHECK was dropped in foundation migration 024; VALID_OPP_STAGES
# stays as the app-side text-path enforcement on the legacy `stage` column.
# Pipelines are catalog rows (no company_id) — shared across the install.
# ===========================================================================

def _validate_pipeline_exists(conn, pipeline_id):
    t = _t_crm_pipeline
    q = Q.from_(t).select(t.star).where(t.id == P())
    row = conn.execute(q.get_sql(), (pipeline_id,)).fetchone()
    if not row:
        err(f"Pipeline {pipeline_id} not found",
            suggestion="Use 'list crm pipelines' to see available pipelines.")
    return row


def _validate_pipeline_stage_exists(conn, stage_id):
    t = _t_crm_pipeline_stage
    q = Q.from_(t).select(t.star).where(t.id == P())
    row = conn.execute(q.get_sql(), (stage_id,)).fetchone()
    if not row:
        err(f"Pipeline stage {stage_id} not found",
            suggestion="Use 'list crm pipeline stages' to see available stages.")
    return row


def _pipeline_tables_present(conn):
    """True when the growth-owned pipeline catalog tables exist (F3 installed).

    The dual-write paths must no-op cleanly when growth is only partially installed
    (e.g. a foundation-only DB or a test fixture that skips create_crmadv_tables),
    keeping backward-compat: the legacy `stage` text write stays authoritative.
    """
    return table_exists(conn, "crm_pipeline_stage")


def _default_pipeline_id(conn):
    """Return the default pipeline id, or None if none is flagged default / absent."""
    if not _pipeline_tables_present(conn):
        return None
    row = conn.execute(
        "SELECT id FROM crm_pipeline WHERE is_default = 1 ORDER BY created_at LIMIT 1"
    ).fetchone()
    return row["id"] if row else None


def _terminal_stage_id(conn, pipeline_id, won=True):
    """Return the id of the pipeline's terminal won/lost stage, or None."""
    col = "is_terminal_won" if won else "is_terminal_lost"
    row = conn.execute(
        f"SELECT id FROM crm_pipeline_stage WHERE crm_pipeline_id = ? AND {col} = 1 "
        "ORDER BY stage_order LIMIT 1", (pipeline_id,)).fetchone()
    return row["id"] if row else None


def _resolve_stage_id_for_opportunity(conn, opp, stage_name):
    """Resolve the legacy `stage` text name to a pipeline_stage_id for dual-write.

    Uses the opportunity's current pipeline (the pipeline of its existing
    pipeline_stage_id) if set, else the default pipeline. Returns the matching
    stage id, or None if no pipeline/stage matches (text-only path, FK stays NULL).
    Never raises — the legacy text write is always authoritative; the FK mirror is
    best-effort so backward-compat (zero-pipeline / foundation-only installs) keeps working.
    """
    if not _pipeline_tables_present(conn):
        return None
    pipeline_id = None
    existing_stage = opp["pipeline_stage_id"] if "pipeline_stage_id" in opp.keys() else None
    if existing_stage:
        row = conn.execute(
            "SELECT crm_pipeline_id FROM crm_pipeline_stage WHERE id = ?",
            (existing_stage,)).fetchone()
        if row:
            pipeline_id = row["crm_pipeline_id"]
    if not pipeline_id:
        pipeline_id = _default_pipeline_id(conn)
    if not pipeline_id:
        return None
    row = conn.execute(
        "SELECT id FROM crm_pipeline_stage WHERE crm_pipeline_id = ? AND name = ?",
        (pipeline_id, stage_name)).fetchone()
    return row["id"] if row else None


# ---------------------------------------------------------------------------
# F3.1 add-crm-pipeline
# ---------------------------------------------------------------------------

def add_crm_pipeline(conn, args):
    """Add a new (customizable) pipeline.

    Required: --name
    Optional: --description, --set-as-default
    Pipelines are catalog rows shared across the install (no company scope).
    --set-as-default clears the default flag on every other pipeline first.
    """
    if not args.name:
        err("--name is required")

    existing = conn.execute(
        "SELECT id FROM crm_pipeline WHERE lower(name) = lower(?)", (args.name,)).fetchone()
    if existing:
        err(f"A pipeline named '{args.name}' already exists.")

    set_default = 1 if getattr(args, "set_as_default", False) else 0
    pipeline_id = str(uuid.uuid4())
    try:
        if set_default:
            conn.execute("UPDATE crm_pipeline SET is_default = 0, updated_at = "
                         + now().get_sql())
        sql, _ = insert_row("crm_pipeline", {
            "id": P(), "name": P(), "description": P(), "is_default": P(),
        })
        conn.execute(sql, (pipeline_id, args.name, args.description, set_default))

        audit(conn, "erpclaw-crm", "add-crm-pipeline", "crm_pipeline", pipeline_id,
              new_values={"name": args.name, "is_default": bool(set_default)},
              description=f"Created pipeline: {args.name}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    ok({
        "crm_pipeline": {
            "id": pipeline_id, "name": args.name,
            "description": args.description, "is_default": bool(set_default),
        },
        "message": f"Pipeline '{args.name}' created",
    })


# ---------------------------------------------------------------------------
# F3.2 add-crm-pipeline-stage
# ---------------------------------------------------------------------------

def add_crm_pipeline_stage(conn, args):
    """Add a stage to a pipeline.

    Required: --pipeline, --name
    Optional: --order (defaults to last+1), --terminal won|lost,
              --probability, --shift-existing
    Blocks on a stage_order collision unless --shift-existing is set (then every
    existing stage at >= the requested order shifts up by 1). Enforces terminal
    uniqueness: at most one is_terminal_won and one is_terminal_lost per pipeline.
    """
    pipeline_id = getattr(args, "pipeline", None)
    if not pipeline_id:
        err("--pipeline is required")
    if not args.name:
        err("--name is required")

    _validate_pipeline_exists(conn, pipeline_id)

    # Name uniqueness within the pipeline (case-insensitive).
    dup_name = conn.execute(
        "SELECT id FROM crm_pipeline_stage WHERE crm_pipeline_id = ? AND lower(name) = lower(?)",
        (pipeline_id, args.name)).fetchone()
    if dup_name:
        err(f"Stage '{args.name}' already exists in this pipeline.")

    # Resolve terminal flags + uniqueness.
    terminal = getattr(args, "terminal", None)
    won = lost = 0
    if terminal:
        if terminal not in ("won", "lost"):
            err("--terminal must be 'won' or 'lost'")
        col = "is_terminal_won" if terminal == "won" else "is_terminal_lost"
        clash = conn.execute(
            f"SELECT id FROM crm_pipeline_stage WHERE crm_pipeline_id = ? AND {col} = 1",
            (pipeline_id,)).fetchone()
        if clash:
            err(f"This pipeline already has a terminal-{terminal} stage. "
                "Each pipeline allows exactly one won and one lost terminal stage.")
        won = 1 if terminal == "won" else 0
        lost = 1 if terminal == "lost" else 0

    probability = "0"
    if getattr(args, "probability", None) is not None:
        probability = str(to_decimal(args.probability))

    # Resolve stage_order: explicit, or last+1.
    if getattr(args, "order", None) is not None:
        try:
            order_no = int(args.order)
        except (TypeError, ValueError):
            err("--order must be an integer")
        collide = conn.execute(
            "SELECT id FROM crm_pipeline_stage WHERE crm_pipeline_id = ? AND stage_order = ?",
            (pipeline_id, order_no)).fetchone()
        if collide and not getattr(args, "shift_existing", False):
            err(f"stage_order {order_no} is already used in this pipeline. "
                "Pass --shift-existing to renumber, or pick a free order.")
    else:
        row = conn.execute(
            "SELECT COALESCE(MAX(stage_order), 0) AS m FROM crm_pipeline_stage "
            "WHERE crm_pipeline_id = ?", (pipeline_id,)).fetchone()
        order_no = row["m"] + 1
        collide = None

    stage_id = str(uuid.uuid4())
    try:
        if collide and getattr(args, "shift_existing", False):
            # Shift every stage at >= order_no up by one, highest-first to avoid
            # transient UNIQUE(stage_order) collisions during the renumber.
            to_shift = conn.execute(
                "SELECT id, stage_order FROM crm_pipeline_stage "
                "WHERE crm_pipeline_id = ? AND stage_order >= ? ORDER BY stage_order DESC",
                (pipeline_id, order_no)).fetchall()
            for r in to_shift:
                conn.execute(
                    "UPDATE crm_pipeline_stage SET stage_order = stage_order + 1, "
                    "updated_at = " + now().get_sql() + " WHERE id = ?", (r["id"],))

        sql, _ = insert_row("crm_pipeline_stage", {
            "id": P(), "crm_pipeline_id": P(), "stage_order": P(), "name": P(),
            "is_terminal_won": P(), "is_terminal_lost": P(), "default_probability": P(),
        })
        conn.execute(sql, (stage_id, pipeline_id, order_no, args.name, won, lost, probability))

        audit(conn, "erpclaw-crm", "add-crm-pipeline-stage", "crm_pipeline_stage", stage_id,
              new_values={"pipeline": pipeline_id, "name": args.name, "order": order_no,
                          "terminal": terminal},
              description=f"Added stage '{args.name}' to pipeline")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    ok({
        "crm_pipeline_stage": {
            "id": stage_id, "crm_pipeline_id": pipeline_id, "stage_order": order_no,
            "name": args.name, "is_terminal_won": bool(won), "is_terminal_lost": bool(lost),
            "default_probability": probability,
        },
        "message": f"Stage '{args.name}' added",
    })


# ---------------------------------------------------------------------------
# F3.3 update-crm-pipeline-stage
# ---------------------------------------------------------------------------

def update_crm_pipeline_stage(conn, args):
    """Update a pipeline stage.

    Required: --id
    Optional: --name, --order (collision-checked), --probability,
              --terminal won|lost (uniqueness-checked), --is-active 0|1
    """
    stage_id = getattr(args, "id", None)
    if not stage_id:
        err("--id is required")

    stage = _validate_pipeline_stage_exists(conn, stage_id)
    pipeline_id = stage["crm_pipeline_id"]
    old_values = row_to_dict(stage)

    data = {}

    if args.name is not None:
        dup = conn.execute(
            "SELECT id FROM crm_pipeline_stage WHERE crm_pipeline_id = ? "
            "AND lower(name) = lower(?) AND id != ?",
            (pipeline_id, args.name, stage_id)).fetchone()
        if dup:
            err(f"Stage '{args.name}' already exists in this pipeline.")
        data["name"] = args.name

    if getattr(args, "order", None) is not None:
        try:
            order_no = int(args.order)
        except (TypeError, ValueError):
            err("--order must be an integer")
        collide = conn.execute(
            "SELECT id FROM crm_pipeline_stage WHERE crm_pipeline_id = ? "
            "AND stage_order = ? AND id != ?",
            (pipeline_id, order_no, stage_id)).fetchone()
        if collide:
            err(f"stage_order {order_no} is already used in this pipeline.")
        data["stage_order"] = order_no

    if getattr(args, "probability", None) is not None:
        data["default_probability"] = str(to_decimal(args.probability))

    terminal = getattr(args, "terminal", None)
    if terminal is not None:
        if terminal not in ("won", "lost", "none"):
            err("--terminal must be 'won', 'lost', or 'none'")
        if terminal == "none":
            data["is_terminal_won"] = 0
            data["is_terminal_lost"] = 0
        else:
            col = "is_terminal_won" if terminal == "won" else "is_terminal_lost"
            clash = conn.execute(
                f"SELECT id FROM crm_pipeline_stage WHERE crm_pipeline_id = ? "
                f"AND {col} = 1 AND id != ?", (pipeline_id, stage_id)).fetchone()
            if clash:
                err(f"This pipeline already has a terminal-{terminal} stage.")
            data["is_terminal_won"] = 1 if terminal == "won" else 0
            data["is_terminal_lost"] = 1 if terminal == "lost" else 0

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        if str(is_active) not in ("0", "1"):
            err("--is-active must be 0 or 1")
        data["is_active"] = int(is_active)

    if not data:
        err("No fields to update. Provide at least one optional flag.")
    data["updated_at"] = now()

    sql, params = dynamic_update("crm_pipeline_stage", data, {"id": stage_id})
    conn.execute(sql, params)

    audit(conn, "erpclaw-crm", "update-crm-pipeline-stage", "crm_pipeline_stage", stage_id,
          old_values=old_values, description="Updated pipeline stage")
    conn.commit()

    q = Q.from_(_t_crm_pipeline_stage).select(_t_crm_pipeline_stage.star).where(
        _t_crm_pipeline_stage.id == P())
    updated = conn.execute(q.get_sql(), (stage_id,)).fetchone()
    ok({"crm_pipeline_stage": row_to_dict(updated), "message": "Pipeline stage updated"})


# ---------------------------------------------------------------------------
# F3.4 list-crm-pipelines
# ---------------------------------------------------------------------------

def list_crm_pipelines(conn, args):
    """List pipelines (catalog rows) with a stage count each.

    Optional: --limit, --offset
    """
    limit = int(args.limit or 20)
    offset = int(args.offset or 0)
    # Correlated stage-count subquery — kept as raw parameterized SQL (PyPika
    # subquery-in-select is awkward; module precedent: pipeline-report below).
    rows = conn.execute(
        "SELECT p.*, "
        "(SELECT COUNT(*) FROM crm_pipeline_stage s WHERE s.crm_pipeline_id = p.id) AS stage_count "
        "FROM crm_pipeline p ORDER BY p.is_default DESC, p.created_at LIMIT ? OFFSET ?",
        (limit, offset)).fetchall()
    total = conn.execute("SELECT COUNT(*) AS c FROM crm_pipeline").fetchone()["c"]

    ok({
        "crm_pipelines": [row_to_dict(r) for r in rows],
        "total": total, "limit": limit, "offset": offset,
        "has_more": offset + limit < total,
    })


# ---------------------------------------------------------------------------
# F3.5 list-crm-pipeline-stages
# ---------------------------------------------------------------------------

def list_crm_pipeline_stages(conn, args):
    """List stages, ordered by stage_order, optionally for one pipeline.

    Optional: --pipeline
    """
    t = _t_crm_pipeline_stage
    q = Q.from_(t).select(t.star)
    params = []
    pipeline_id = getattr(args, "pipeline", None)
    if pipeline_id:
        _validate_pipeline_exists(conn, pipeline_id)
        q = q.where(t.crm_pipeline_id == P())
        params.append(pipeline_id)
    q = q.orderby(t.crm_pipeline_id).orderby(t.stage_order)
    rows = conn.execute(q.get_sql(), params).fetchall()

    ok({
        "crm_pipeline_stages": [row_to_dict(r) for r in rows],
        "total": len(rows),
    })


# ---------------------------------------------------------------------------
# F3.6 set-opportunity-pipeline-stage
# ---------------------------------------------------------------------------

def set_opportunity_pipeline_stage(conn, args):
    """Move an opportunity to a stage (by pipeline_stage_id).

    Required: --opportunity, --stage  (--stage is a crm_pipeline_stage id)
    Cross-pipeline transitions are blocked: if the opportunity already sits in a
    pipeline, the target stage must belong to that same pipeline. Dual-writes the
    legacy `stage` text column (the target stage's name) so reports + the text path
    stay consistent. Terminal target stages set probability accordingly.
    """
    opp_id = getattr(args, "opportunity", None)
    stage_id = getattr(args, "stage", None)
    if not opp_id:
        err("--opportunity is required")
    if not stage_id:
        err("--stage is required (a pipeline stage id)")

    opp = _validate_opportunity_exists(conn, opp_id)
    target = _validate_pipeline_stage_exists(conn, stage_id)
    target_pipeline = target["crm_pipeline_id"]

    # Cross-pipeline guard: if the opportunity is already in a pipeline, the target
    # stage must be in that same pipeline.
    current_stage_id = opp["pipeline_stage_id"] if "pipeline_stage_id" in opp.keys() else None
    if current_stage_id:
        cur = conn.execute(
            "SELECT crm_pipeline_id FROM crm_pipeline_stage WHERE id = ?",
            (current_stage_id,)).fetchone()
        if cur and cur["crm_pipeline_id"] != target_pipeline:
            err("Cannot move an opportunity across pipelines. The target stage "
                "belongs to a different pipeline than the opportunity's current one.",
                suggestion="Stages must stay within one pipeline; pick a stage in the same pipeline.")

    # Dual-write: legacy stage text (the stage name) + pipeline_stage_id FK.
    new_stage_name = target["name"]
    new_prob = target["default_probability"]
    data = {
        "stage": new_stage_name,
        "pipeline_stage_id": stage_id,
        "probability": new_prob,
        "weighted_revenue": _calc_weighted_revenue(opp["expected_revenue"], new_prob),
        "updated_at": now(),
    }
    # Terminal lost zeroes weighted revenue (mirror mark-opportunity-lost semantics).
    if target["is_terminal_lost"]:
        data["weighted_revenue"] = "0"
        data["probability"] = "0"
    elif target["is_terminal_won"]:
        data["probability"] = "100"
        data["weighted_revenue"] = opp["expected_revenue"]

    sql, params = dynamic_update("opportunity", data, {"id": opp_id})
    conn.execute(sql, params)

    audit(conn, "erpclaw-crm", "set-opportunity-pipeline-stage", "opportunity", opp_id,
          old_values={"stage": opp["stage"], "pipeline_stage_id": current_stage_id},
          new_values={"stage": new_stage_name, "pipeline_stage_id": stage_id},
          description=f"Moved opportunity to stage '{new_stage_name}'")
    conn.commit()

    ok({
        "opportunity": {
            "id": opp_id, "naming_series": opp["naming_series"],
            "stage": new_stage_name, "pipeline_stage_id": stage_id,
            "crm_pipeline_id": target_pipeline,
        },
        "message": f"Opportunity {opp['naming_series']} moved to '{new_stage_name}'",
    })


# ===========================================================================
# Wave 1B F4 — Saved views: actions
# ===========================================================================

def _validate_saved_view_exists(conn, view_id):
    row = conn.execute(
        "SELECT * FROM crm_saved_view WHERE id = ?", (view_id,)).fetchone()
    if not row:
        err(f"Saved view {view_id} not found")
    return row


def _validate_filter_json_arg(conn, entity_type, filter_json, flag_name):
    """Parse + validate a filter-JSON string at SAVE time. Returns the canonical
    JSON string to store, or None if not supplied. Rejects bad field/op/shape."""
    if filter_json is None:
        return None
    tree = _parse_json_arg(filter_json, flag_name)
    if tree is None:
        return None
    allowed = allowed_columns_for(conn, entity_type)
    udf_cols = _udf_field_names(conn, entity_type)
    try:
        validate_filter_tree(tree, allowed, udf_cols)
    except FilterValidationError as e:
        err(str(e))
    return json.dumps(tree)


def _owner_match_or_err(view, owner_user_id, verb):
    """Owner-only guard for update/delete. A view with an owner_user_id may only
    be mutated by that owner; the caller must pass --owner-user-id to prove it.
    A view with a NULL owner (system/shared-by-default) is mutable by anyone."""
    stored = view["owner_user_id"]
    if stored is None:
        return
    if not owner_user_id or owner_user_id != stored:
        err(f"Only the owner may {verb} this saved view "
            f"(pass --owner-user-id matching the view's owner).")


def add_crm_saved_view(conn, args):
    """Add a saved view (a persisted filter over one CRM entity).

    Required: --name, --entity-type, --filter-json
    Optional: --sort-json, --group-by-json, --column-order-json, --is-shared,
              --owner-user-id
    The filter-JSON is validated at save against the entity's column whitelist
    (native columns + UDF field names) + the operator whitelist; an unknown
    field/operator or over-deep nesting is rejected here.
    """
    if not args.name:
        err("--name is required")
    entity_type = getattr(args, "entity_type", None)
    if not entity_type:
        err("--entity-type is required")
    if entity_type not in VALID_SAVED_VIEW_ENTITIES:
        err(f"--entity-type must be one of {VALID_SAVED_VIEW_ENTITIES}")

    company_id = _resolve_company_id(conn, args)
    owner_user_id = getattr(args, "owner_user_id", None)

    filter_arg = getattr(args, "filter_json", None)
    if filter_arg is None:
        err("--filter-json is required")
    filter_str = _validate_filter_json_arg(conn, entity_type, filter_arg, "filter-json")
    # Optional JSON blobs (sort/group/column-order) are stored opaquely but must
    # at least parse as JSON (no structural validation in v1 beyond well-formed).
    sort_str = _store_opaque_json(getattr(args, "sort_json", None), "sort-json")
    group_str = _store_opaque_json(getattr(args, "group_by_json", None), "group-by-json")
    col_str = _store_opaque_json(getattr(args, "column_order_json", None), "column-order-json")

    is_shared = 1 if getattr(args, "is_shared", False) else 0

    # Name uniqueness per (company, owner) — matches uq_crm_saved_view_name.
    dup = conn.execute(
        "SELECT id FROM crm_saved_view WHERE company_id = ? "
        "AND ((owner_user_id IS NULL AND ? IS NULL) OR owner_user_id = ?) "
        "AND lower(name) = lower(?)",
        (company_id, owner_user_id, owner_user_id, args.name)).fetchone()
    if dup:
        err(f"A saved view named '{args.name}' already exists for this owner.")

    view_id = str(uuid.uuid4())
    try:
        sql, _ = insert_row("crm_saved_view", {
            "id": P(), "name": P(), "entity_type": P(), "owner_user_id": P(),
            "is_shared": P(), "filter_json": P(), "sort_json": P(),
            "group_by_json": P(), "column_order_json": P(), "company_id": P(),
        })
        conn.execute(sql, (view_id, args.name, entity_type, owner_user_id,
                           is_shared, filter_str, sort_str, group_str, col_str,
                           company_id))
        audit(conn, "erpclaw-crm", "add-crm-saved-view", "crm_saved_view", view_id,
              new_values={"name": args.name, "entity_type": entity_type,
                          "is_shared": bool(is_shared)},
              description=f"Created saved view: {args.name}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    ok({
        "crm_saved_view": {
            "id": view_id, "name": args.name, "entity_type": entity_type,
            "owner_user_id": owner_user_id, "is_shared": bool(is_shared),
            "filter_json": filter_str, "sort_json": sort_str,
            "group_by_json": group_str, "column_order_json": col_str,
        },
        "message": f"Saved view '{args.name}' created",
    })


def _store_opaque_json(value, flag_name):
    """Well-formedness check for an opaque JSON blob (sort/group/column-order).
    Returns the canonical string or None. No DSL validation (those are renderer
    hints, not filters that hit SQL)."""
    if value is None:
        return None
    parsed = _parse_json_arg(value, flag_name)
    return json.dumps(parsed)


def update_crm_saved_view(conn, args):
    """Update a saved view (owner-only when the view has an owner).

    Required: --id
    Optional: --name, --filter-json, --sort-json, --group-by-json,
              --column-order-json, --is-shared, --owner-user-id (proof of owner)
    --entity-type is immutable (a view's column whitelist is bound to its entity).
    """
    view_id = getattr(args, "id", None)
    if not view_id:
        err("--id is required")

    view = _validate_saved_view_exists(conn, view_id)
    old_values = row_to_dict(view)
    _owner_match_or_err(view, getattr(args, "owner_user_id", None), "update")
    entity_type = view["entity_type"]

    data = {}
    if args.name is not None:
        company_id = view["company_id"]
        owner = view["owner_user_id"]
        dup = conn.execute(
            "SELECT id FROM crm_saved_view WHERE company_id = ? "
            "AND ((owner_user_id IS NULL AND ? IS NULL) OR owner_user_id = ?) "
            "AND lower(name) = lower(?) AND id != ?",
            (company_id, owner, owner, args.name, view_id)).fetchone()
        if dup:
            err(f"A saved view named '{args.name}' already exists for this owner.")
        data["name"] = args.name

    if getattr(args, "filter_json", None) is not None:
        data["filter_json"] = _validate_filter_json_arg(
            conn, entity_type, args.filter_json, "filter-json")
    if getattr(args, "sort_json", None) is not None:
        data["sort_json"] = _store_opaque_json(args.sort_json, "sort-json")
    if getattr(args, "group_by_json", None) is not None:
        data["group_by_json"] = _store_opaque_json(args.group_by_json, "group-by-json")
    if getattr(args, "column_order_json", None) is not None:
        data["column_order_json"] = _store_opaque_json(args.column_order_json, "column-order-json")

    # --is-shared is a store_true flag; only apply when explicitly given. We use a
    # tri-state arg (--is-shared / --not-shared) is overkill; treat presence of the
    # flag as set-shared, and require --not-shared to clear (parsed in main()).
    if getattr(args, "set_shared", None) is not None:
        data["is_shared"] = 1 if args.set_shared else 0

    if not data:
        err("No fields to update. Provide at least one optional flag.")
    data["updated_at"] = now()

    sql, params = dynamic_update("crm_saved_view", data, {"id": view_id})
    conn.execute(sql, params)
    audit(conn, "erpclaw-crm", "update-crm-saved-view", "crm_saved_view", view_id,
          old_values=old_values, description="Updated saved view")
    conn.commit()

    updated = conn.execute("SELECT * FROM crm_saved_view WHERE id = ?", (view_id,)).fetchone()
    ok({"crm_saved_view": row_to_dict(updated), "message": "Saved view updated"})


def get_crm_saved_view(conn, args):
    """Get a saved view by id.

    Required: --id
    """
    view_id = getattr(args, "id", None)
    if not view_id:
        err("--id is required")
    view = _validate_saved_view_exists(conn, view_id)
    ok({"crm_saved_view": row_to_dict(view)})


def list_crm_saved_views(conn, args):
    """List saved views in the company.

    Optional: --entity-type, --owner-user-id, --shared-only, --limit, --offset
    Default returns views owned by --owner-user-id (when given) plus every shared
    view; with no owner filter it returns all of the company's views.
    """
    company_id = _resolve_company_id(conn, args)
    clauses = ["company_id = ?"]
    params = [company_id]

    entity_type = getattr(args, "entity_type", None)
    if entity_type:
        if entity_type not in VALID_SAVED_VIEW_ENTITIES:
            err(f"--entity-type must be one of {VALID_SAVED_VIEW_ENTITIES}")
        clauses.append("entity_type = ?")
        params.append(entity_type)

    if getattr(args, "shared_only", False):
        clauses.append("is_shared = 1")
    else:
        owner = getattr(args, "owner_user_id", None)
        if owner:
            # Owner's own views OR any shared view.
            clauses.append("(owner_user_id = ? OR is_shared = 1)")
            params.append(owner)

    limit = int(args.limit or 20)
    offset = int(args.offset or 0)
    where = " AND ".join(clauses)
    rows = conn.execute(
        f"SELECT * FROM crm_saved_view WHERE {where} "
        f"ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [limit, offset]).fetchall()
    total = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM crm_saved_view WHERE {where}",
        params).fetchone()["cnt"]
    ok({
        "crm_saved_views": [row_to_dict(r) for r in rows],
        "total": total, "limit": limit, "offset": offset,
        "has_more": offset + limit < total,
    })


def delete_crm_saved_view(conn, args):
    """Hard-delete a saved view (owner-only when the view has an owner).

    Required: --id
    Optional: --owner-user-id (proof of owner)
    """
    view_id = getattr(args, "id", None)
    if not view_id:
        err("--id is required")
    view = _validate_saved_view_exists(conn, view_id)
    _owner_match_or_err(view, getattr(args, "owner_user_id", None), "delete")
    old_values = row_to_dict(view)
    try:
        conn.execute("DELETE FROM crm_saved_view WHERE id = ?", (view_id,))
        audit(conn, "erpclaw-crm", "delete-crm-saved-view", "crm_saved_view", view_id,
              old_values=old_values, description=f"Deleted saved view: {view['name']}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    ok({"crm_saved_view_id": view_id, "message": "Saved view deleted"})


# Map an entity_type to the in-module list-* handler that already applies a saved
# view's filter (the 4 native CRM entities). `customer` is intentionally absent —
# it is foundation-owned (Option A, DECISION #1): apply-saved-view routes it
# through list-customers + a Python post-filter instead.
_SAVED_VIEW_LIST_DISPATCH = {
    "lead": "list_leads",
    "opportunity": "list_opportunities",
    "crm_contact": "list_crm_contacts",
    "crm_company": "list_crm_companies",
}


def _python_eval_filter(tree, row, _depth=1):
    """Evaluate a validated filter tree against a single dict row (Python side).

    Used only for the customer entity (Option A post-filter over list-customers
    output). Operators mirror the SQL builder; string comparison matches SQLite's
    TEXT-affinity comparisons used everywhere else in the module.
    """
    if "conditions" in tree or "logic" in tree:
        logic = tree.get("logic", "AND")
        results = [_python_eval_filter(c, row, _depth + 1) for c in tree["conditions"]]
        return all(results) if logic == "AND" else any(results)
    field = tree["field"]
    op = tree["op"]
    value = tree.get("value")
    cell = row.get(field)
    if op == "eq":
        return _cmp_str(cell) == _cmp_str(value)
    if op == "neq":
        return _cmp_str(cell) != _cmp_str(value)
    if op == "contains":
        return cell is not None and str(value) in str(cell)
    if op == "gt":
        return cell is not None and _cmp_str(cell) > _cmp_str(value)
    if op == "lt":
        return cell is not None and _cmp_str(cell) < _cmp_str(value)
    if op == "in":
        return _cmp_str(cell) in {_cmp_str(v) for v in value}
    if op == "between":
        return cell is not None and _cmp_str(value[0]) <= _cmp_str(cell) <= _cmp_str(value[1])
    return False


def _cmp_str(v):
    return "" if v is None else str(v)


def apply_saved_view(conn, args):
    """Apply a saved view: return the rows it selects.

    Required: --view (the saved-view id; --saved-view-id is also accepted)
    Optional: --limit, --offset
    The 4 native CRM entities (lead / opportunity / crm_contact / crm_company)
    dispatch in-process to their list-* handler with the view's filter applied as
    a parameterized SQL WHERE. The `customer` entity is foundation-owned, so it
    routes through list-customers and post-filters the result in Python
    (Option A, DECISION #1 — growth never edits the foundation list-customers).
    crm_task has no list-* flag wiring in v1 and is rejected with guidance.
    """
    view_id = getattr(args, "view", None) or getattr(args, "saved_view_id", None)
    if not view_id:
        err("--view is required (the saved-view id)")
    view = _validate_saved_view_exists(conn, view_id)
    entity_type = view["entity_type"]

    if entity_type in _SAVED_VIEW_LIST_DISPATCH:
        # Re-dispatch to the entity's own list-* handler with the view applied.
        handler = globals()[_SAVED_VIEW_LIST_DISPATCH[entity_type]]
        args.saved_view_id = view_id
        handler(conn, args)
        return

    if entity_type == "customer":
        _apply_saved_view_customer(conn, args, view)
        return

    err(f"apply-saved-view does not yet support entity_type '{entity_type}'. "
        f"Supported: lead, opportunity, customer, crm_contact, crm_company.")


def _apply_saved_view_customer(conn, args, view):
    """Option A: call foundation list-customers, post-filter in Python.

    Growth does NOT add --saved-view-id to the foundation-owned list-customers;
    it calls the existing action and applies the view's filter to the returned
    rows. UDF conditions read custom_field_value (a READ — allowed cross-module).
    """
    from erpclaw_lib.dependencies import check_subprocess_target, resolve_skill_script
    dep_err = check_subprocess_target(conn, "erpclaw", "customer")
    if dep_err:
        err(dep_err["error"])
    selling_script = resolve_skill_script("erpclaw")

    company_id = _resolve_company_id(conn, args)
    cmd = ["python3", selling_script, "--action", "list-customers",
           "--company-id", company_id, "--limit", "10000", "--offset", "0"]
    if getattr(args, "db_path", None):
        cmd += ["--db-path", args.db_path]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        err(f"list-customers failed: {proc.stderr.strip() or proc.stdout.strip()}")
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        err("list-customers returned non-JSON output")
    rows = payload.get("customers", payload.get("data", []))

    tree = None
    if view["filter_json"]:
        tree = json.loads(view["filter_json"])
        # Defence in depth: re-validate at apply time (a UDF may have been dropped).
        allowed = allowed_columns_for(conn, "customer")
        udf_cols = _udf_field_names(conn, "customer")
        try:
            validate_filter_tree(tree, allowed, udf_cols)
        except FilterValidationError as e:
            err(str(e))
        # Hydrate UDF values onto each row so the Python evaluator can see them.
        if udf_cols:
            for r in rows:
                _hydrate_udf_values(conn, "customer", r, udf_cols)

    if tree is not None:
        rows = [r for r in rows if _python_eval_filter(tree, r)]

    limit = int(args.limit or 20)
    offset = int(args.offset or 0)
    total = len(rows)
    page = rows[offset:offset + limit]
    ok({
        "entity_type": "customer",
        "customers": page,
        "total": total, "limit": limit, "offset": offset,
        "has_more": offset + limit < total,
    })


def _hydrate_udf_values(conn, table_name, row, udf_cols):
    """Read custom_field_value rows for one doc and merge them into the dict row."""
    doc_id = row.get("id")
    if not doc_id:
        return
    cfvs = conn.execute(
        "SELECT field_name, value FROM custom_field_value "
        "WHERE table_name = ? AND doc_id = ?", (table_name, doc_id)).fetchall()
    for cf in cfvs:
        if cf["field_name"] in udf_cols:
            row[cf["field_name"]] = cf["value"]


# ---------------------------------------------------------------------------
# F5 global-crm-search (Wave 1B)
# ---------------------------------------------------------------------------

# Per-entity search spec. Each entry declares the (possibly absent) table, the
# name columns to match against (in priority order — first is the primary
# display name), the column used to build the result snippet, and the column
# that drives the display_name. `customer` is foundation-owned (a READ here, no
# cross-module write); the rest are growth-owned. crm_task is intentionally
# absent from the V1 default — it is searchable only when explicitly requested
# and skips gracefully if F2's table is not present (table_exists guard).
_SEARCH_ENTITIES = {
    "lead":         {"table": "lead",         "name_cols": ["lead_name", "company_name", "email"], "display": "lead_name",        "snippet": "company_name"},
    "opportunity":  {"table": "opportunity",  "name_cols": ["opportunity_name", "source"],         "display": "opportunity_name", "snippet": "source"},
    "customer":     {"table": "customer",     "name_cols": ["name", "email", "phone"],             "display": "name",             "snippet": "email"},
    "crm_contact":  {"table": "crm_contact",  "name_cols": ["name", "email", "job_title"],         "display": "name",             "snippet": "job_title"},
    "crm_company":  {"table": "crm_company",  "name_cols": ["name", "domain", "industry"],         "display": "name",             "snippet": "domain"},
}

# V1 default fan-out set (order is the stable secondary tiebreak when match_rank
# and updated_at are equal — keeps results deterministic across backends).
_DEFAULT_SEARCH_ENTITY_TYPES = ["lead", "opportunity", "customer", "crm_contact", "crm_company"]

# Hard cap on total results merged across all entity types.
_GLOBAL_SEARCH_HARD_CAP = 200


def _search_one_entity(conn, entity_type, spec, company_id, query):
    """Run the 3-pass ranked LIKE search over one entity's name columns.

    Returns a dict id -> row payload (best/lowest match_rank wins per id).
    All values are ?-bound; lower() is applied to both sides so prefix/contains
    matching is case-insensitive on SQLite AND PostgreSQL (PG LIKE is
    case-sensitive otherwise). No SQLite-only constructs.
    """
    table = spec["table"]
    name_cols = spec["name_cols"]
    display_col = spec["display"]
    snippet_col = spec["snippet"]

    # Pull the candidate row (cols we need for the payload) once per matched id.
    # SELECT list is fixed (id + the display/snippet/updated_at cols), the WHERE
    # is the ranked LIKE. We probe rank 1 -> 2 -> 3, taking the first (lowest)
    # rank an id appears at.
    select_cols = f"id, {display_col} AS _display, {snippet_col} AS _snippet, updated_at"
    found = {}

    # rank 1: exact match (case-insensitive) on any name column
    # rank 2: prefix match  q%
    # rank 3: contains match %q%
    passes = [
        (1, [(f"lower({c}) = lower(?)", query) for c in name_cols]),
        (2, [(f"lower({c}) LIKE lower(?)", query + "%") for c in name_cols]),
        (3, [(f"lower({c}) LIKE lower(?)", "%" + query + "%") for c in name_cols]),
    ]
    for rank, col_preds in passes:
        or_sql = " OR ".join(pred for pred, _ in col_preds)
        sql = (f"SELECT {select_cols} FROM {table} "
               f"WHERE company_id = ? AND ({or_sql})")
        params = [company_id] + [val for _, val in col_preds]
        for r in conn.execute(sql, params).fetchall():
            rid = r["id"]
            if rid in found:
                continue  # already matched at a lower (better) rank
            found[rid] = {
                "entity_type": entity_type,
                "id": rid,
                "display_name": r["_display"],
                "snippet": r["_snippet"],
                "updated_at": r["updated_at"],
                "match_rank": rank,
            }
    return found


def global_crm_search(conn, args):
    """Fan out a single query across CRM entities, merge + rank the hits.

    Required: --query (min 2 chars)
    Optional: --limit (default 50, hard-capped at 200 across all entities),
              --entity-types "lead,opportunity,customer,crm_contact,crm_company"
                (CSV; defaults to the V1 set). Unknown or absent-table entity
                types are skipped gracefully (no crash).

    Returns a flat, deterministically-ordered list:
      [{entity_type, id, display_name, snippet, updated_at, match_rank}]
    sorted by match_rank asc, then updated_at desc, then entity_type order,
    then id (stable final tiebreak).
    """
    company_id = _resolve_company_id(conn, args)

    query = (args.query or "").strip()
    if len(query) < 2:
        err("--query must be at least 2 characters")

    # Resolve the requested entity-type set (CSV) against the known set; unknown
    # types are dropped silently per the contract (graceful skip, not a crash).
    requested = getattr(args, "entity_types", None)
    if requested:
        wanted = [t.strip() for t in requested.split(",") if t.strip()]
    else:
        wanted = list(_DEFAULT_SEARCH_ENTITY_TYPES)

    # Order index for the entity-type tiebreak (use the requested order when the
    # caller supplied one, else the default order).
    order_index = {t: i for i, t in enumerate(wanted)}

    limit = int(args.limit or 50)
    if limit < 1:
        limit = 1
    if limit > _GLOBAL_SEARCH_HARD_CAP:
        limit = _GLOBAL_SEARCH_HARD_CAP

    skipped = []
    results = []
    for entity_type in wanted:
        spec = _SEARCH_ENTITIES.get(entity_type)
        if spec is None:
            skipped.append(entity_type)            # unknown entity type
            continue
        if not table_exists(conn, spec["table"]):
            skipped.append(entity_type)            # table absent (addon not installed)
            continue
        results.extend(_search_one_entity(conn, entity_type, spec, company_id, query).values())

    # Merge ordering: match_rank asc, updated_at desc, entity-type order, id.
    results.sort(key=lambda r: (
        r["match_rank"],
        _desc_key(r["updated_at"]),
        order_index.get(r["entity_type"], len(order_index)),
        r["id"],
    ))

    payload = {
        "query": query,
        "results": results[:limit],
        "total": len(results),
        "returned": min(len(results), limit),
        "limit": limit,
    }
    if skipped:
        payload["skipped_entity_types"] = skipped
    ok(payload)


class _desc_key:
    """Sort helper: wrap a value so ascending sort yields descending order.

    Used for the updated_at tiebreak (most-recent first) inside an otherwise
    ascending composite sort key. None sorts last (treated as the empty string).
    """
    __slots__ = ("v",)

    def __init__(self, v):
        self.v = v if v is not None else ""

    def __lt__(self, other):
        return self.v > other.v

    def __eq__(self, other):
        return self.v == other.v


# ===========================================================================
# Wave 1B F6 — CSV Import / Export (8 actions)
# ===========================================================================
#
# Import: validate against csv_import.SCHEMAS, then bulk_insert with a
# company-scoped dup_check and the user-chosen --on-duplicate mode (REQUIRED,
# no default — forces explicit intent). Export: SELECT the entity's curated
# columns (company-scoped, with simple --status/--stage/--lifecycle filters),
# optionally append cf_<name> UDF columns, write via csv_export.write_csv_rows
# (overwrite, never append). All paths are realpath-resolved + .csv-gated.

_VALID_ON_DUPLICATE = ("skip", "update", "fail")


def _csv_in_path(path):
    """Resolve + path-safety-gate an INPUT csv path (realpath + .csv + isfile)."""
    if not path:
        err("--file is required")
    real = os.path.realpath(path)
    if not real.lower().endswith(".csv"):
        err("--file must point to a .csv file")
    if not os.path.isfile(real):
        err(f"File not found: {path}")
    return real


def _require_on_duplicate(args):
    """--on-duplicate is REQUIRED for every import-* action (no default)."""
    mode = getattr(args, "on_duplicate", None)
    if not mode:
        err("--on-duplicate is required. Choose one of: skip | update | fail.",
            suggestion="skip leaves existing rows; update overwrites them; "
                       "fail aborts on the first duplicate.")
    if mode not in _VALID_ON_DUPLICATE:
        err(f"--on-duplicate must be one of {_VALID_ON_DUPLICATE}")
    return mode


def _run_import(conn, args, entity_type, table, dup_check, prep_row,
                update_columns):
    """Shared import driver: validate CSV, parse rows, bulk_insert in one txn.

    `dup_check(conn, row, company_id)` -> existing id | None (company-scoped).
    `prep_row(conn, row, company_id)` augments a parsed row in place (naming
    series, company_id, derived columns). `update_columns` restricts what an
    'update' mode writes. Returns nothing; emits the ok() payload.
    """
    from erpclaw_lib.csv_import import validate_csv, parse_csv_rows, bulk_insert

    mode = _require_on_duplicate(args)
    real = _csv_in_path(getattr(args, "file", None))
    company_id = _resolve_company_id(conn, args)

    errors = validate_csv(real, entity_type)
    if errors:
        err(f"CSV validation failed: {'; '.join(errors)}")

    rows = parse_csv_rows(real, entity_type)
    if not rows:
        err("CSV file is empty")

    # Augment every row before any write (naming series / company_id / derived).
    for row in rows:
        prep_row(conn, row, company_id)

    insert_columns = sorted({c for row in rows for c in row.keys()})

    # Restrict the update set to columns actually present in the parsed rows
    # (a sparse CSV may omit some); bulk_insert requires every update column be
    # in `columns`, and a column not in the file can never carry a new value.
    present_update_columns = [c for c in update_columns if c in insert_columns]

    def _dup(_conn, row):
        return dup_check(_conn, row, company_id)

    try:
        result = bulk_insert(
            conn, table, insert_columns, rows,
            on_duplicate_mode=mode, dup_check=_dup,
            update_columns=present_update_columns)
        audit(conn, "erpclaw-crm", f"import-{entity_type}", table, "bulk",
              new_values={"on_duplicate": mode, **result},
              description=f"Imported {entity_type} from CSV ({mode})")
        conn.commit()
    except ValueError as e:
        # on_duplicate=fail raises here; roll back the whole import.
        conn.rollback()
        err(str(e))
    except Exception:
        conn.rollback()
        raise

    ok({
        "entity_type": entity_type,
        "on_duplicate": mode,
        "imported": result["inserted"],
        "updated": result["updated"],
        "skipped": result["skipped"],
        "total_rows": len(rows),
        "message": (f"Imported {result['inserted']} {entity_type} row(s); "
                    f"updated {result['updated']}, skipped {result['skipped']}."),
    })


# ----- per-entity dup_check + prep_row --------------------------------------

def _dup_lead(conn, row, company_id):
    """A lead is a duplicate when its email (case-insensitive) already exists
    in the company. Leads without an email are never treated as duplicates."""
    email = (row.get("email") or "").strip()
    if not email:
        return None
    r = conn.execute(
        "SELECT id FROM lead WHERE company_id = ? AND lower(email) = lower(?)",
        (company_id, email)).fetchone()
    return r["id"] if r else None


def _prep_lead(conn, row, company_id):
    if row.get("source") and row["source"] not in VALID_LEAD_SOURCES:
        err(f"Invalid source '{row['source']}' (one of {VALID_LEAD_SOURCES})")
    if row.get("status") and row["status"] not in VALID_LEAD_STATUSES:
        err(f"Invalid status '{row['status']}' (one of {VALID_LEAD_STATUSES})")
    if row.get("email") and not _EMAIL_RE.match(row["email"]):
        err(f"Invalid email format: '{row['email']}'")
    row["naming_series"] = get_next_name(conn, "lead")
    row["company_id"] = company_id


def _dup_opportunity(conn, row, company_id):
    """Opportunities have no natural unique key (name is not unique); treat each
    imported row as new. Returning None means every row inserts."""
    return None


def _prep_opportunity(conn, row, company_id):
    if row.get("opportunity_type") and row["opportunity_type"] not in VALID_OPP_TYPES:
        err(f"Invalid opportunity_type '{row['opportunity_type']}' (one of {VALID_OPP_TYPES})")
    if row.get("stage") and row["stage"] not in VALID_OPP_STAGES:
        err(f"Invalid stage '{row['stage']}' (one of {VALID_OPP_STAGES})")
    exp = row.get("expected_revenue") or "0"
    prob = row.get("probability") or "0"
    row["expected_revenue"] = str(round_currency(to_decimal(exp)))
    row["weighted_revenue"] = _calc_weighted_revenue(exp, prob)
    row["naming_series"] = get_next_name(conn, "opportunity")
    row["company_id"] = company_id


def _dup_crm_contact(conn, row, company_id):
    email = (row.get("email") or "").strip()
    if not email:
        return None
    r = conn.execute(
        "SELECT id FROM crm_contact WHERE company_id = ? AND lower(email) = lower(?)",
        (company_id, email)).fetchone()
    return r["id"] if r else None


def _prep_crm_contact(conn, row, company_id):
    if row.get("lifecycle") and row["lifecycle"] not in VALID_CONTACT_LIFECYCLES:
        err(f"Invalid lifecycle '{row['lifecycle']}' (one of {VALID_CONTACT_LIFECYCLES})")
    if row.get("email") and not _EMAIL_RE.match(row["email"]):
        err(f"Invalid email format: '{row['email']}'")
    row["company_id"] = company_id


def _dup_crm_company(conn, row, company_id):
    domain = (row.get("domain") or "").strip()
    if not domain:
        return None
    r = conn.execute(
        "SELECT id FROM crm_company WHERE company_id = ? AND lower(domain) = lower(?)",
        (company_id, domain)).fetchone()
    return r["id"] if r else None


def _prep_crm_company(conn, row, company_id):
    if row.get("lifecycle") and row["lifecycle"] not in VALID_COMPANY_LIFECYCLES:
        err(f"Invalid lifecycle '{row['lifecycle']}' (one of {VALID_COMPANY_LIFECYCLES})")
    if row.get("annual_revenue"):
        row["annual_revenue"] = str(round_currency(to_decimal(row["annual_revenue"])))
    row["company_id"] = company_id


def import_leads(conn, args):
    """Import leads from a CSV file.

    Required: --file, --on-duplicate {skip|update|fail}
    Dedup key: email (case-insensitive, per company). CSV columns:
    lead_name (required), company_name, email, phone, source, territory,
    industry, status, notes.
    """
    _run_import(conn, args, "lead", "lead", _dup_lead, _prep_lead,
                update_columns=["lead_name", "company_name", "email", "phone",
                                "source", "territory", "industry", "status",
                                "assigned_to", "notes"])


def import_opportunities(conn, args):
    """Import opportunities from a CSV file.

    Required: --file, --on-duplicate {skip|update|fail}
    Opportunities have no natural unique key, so every row is inserted (the
    duplicate modes still apply if a future key is added). CSV columns:
    opportunity_name (required), opportunity_type, expected_revenue,
    probability, source, expected_closing_date, stage, notes.
    """
    _run_import(conn, args, "opportunity", "opportunity",
                _dup_opportunity, _prep_opportunity,
                update_columns=["opportunity_name", "opportunity_type",
                                "expected_revenue", "probability",
                                "weighted_revenue", "source",
                                "expected_closing_date", "stage",
                                "assigned_to", "notes"])


def import_crm_contacts(conn, args):
    """Import CRM contacts from a CSV file.

    Required: --file, --on-duplicate {skip|update|fail}
    Dedup key: email (case-insensitive, per company). CSV columns:
    name (required), email, phone, mobile, job_title, linkedin_url,
    lifecycle, assigned_to_user_id, notes.
    """
    _run_import(conn, args, "crm_contact", "crm_contact",
                _dup_crm_contact, _prep_crm_contact,
                update_columns=["name", "email", "phone", "mobile",
                                "job_title", "linkedin_url", "lifecycle",
                                "assigned_to_user_id", "notes"])


def import_crm_companies(conn, args):
    """Import CRM companies from a CSV file.

    Required: --file, --on-duplicate {skip|update|fail}
    Dedup key: domain (case-insensitive, per company). CSV columns:
    name (required), domain, industry, employee_count, annual_revenue,
    linkedin_url, lifecycle, linked_customer_id, notes.
    """
    _run_import(conn, args, "crm_company", "crm_company",
                _dup_crm_company, _prep_crm_company,
                update_columns=["name", "domain", "industry", "employee_count",
                                "annual_revenue", "linkedin_url", "lifecycle",
                                "linked_customer_id", "assigned_to_user_id",
                                "notes"])


# ----- export ---------------------------------------------------------------

# Simple pre-F4 filters per entity: the --flag name -> the column it filters.
_EXPORT_FILTERS = {
    "lead": {"status": "status"},
    "opportunity": {"stage": "stage", "status": "stage"},  # alias status->stage
    "crm_contact": {"lifecycle": "lifecycle"},
    "crm_company": {"lifecycle": "lifecycle"},
}


def _run_export(conn, args, entity_type, table):
    """Shared export driver: SELECT curated columns (company-scoped + simple
    filters), optionally append cf_<name> UDF columns, write overwriting CSV."""
    from erpclaw_lib.csv_export import (
        SCHEMAS as EXPORT_SCHEMAS, validate_export_request, write_csv_rows,
        udf_column_name)

    output = getattr(args, "output", None)
    real, errors = validate_export_request(entity_type, output)
    if errors:
        err(f"Export request invalid: {'; '.join(errors)}")

    company_id = _resolve_company_id(conn, args)
    base_columns = EXPORT_SCHEMAS[entity_type]

    clauses = ["company_id = ?"]
    params = [company_id]
    for flag, column in _EXPORT_FILTERS.get(entity_type, {}).items():
        val = getattr(args, flag, None)
        if val:
            clauses.append(f"{column} = ?")
            params.append(val)

    col_sql = ", ".join(base_columns)
    sql = (f"SELECT {col_sql} FROM {table} "
           f"WHERE {' AND '.join(clauses)} ORDER BY created_at DESC")
    db_rows = [row_to_dict(r) for r in conn.execute(sql, params).fetchall()]

    # UDF columns (M1): appended as cf_<name> only when --include-udfs AND at
    # least one custom field is registered for the table AND a value exists.
    udf_columns = []
    if getattr(args, "include_udfs", False) and db_rows:
        try:
            from erpclaw_lib.custom_fields import (
                get_custom_fields, fetch_custom_field_values)
        except ImportError:
            get_custom_fields = None
        if get_custom_fields is not None:
            field_names = sorted(
                f["field_name"] for f in get_custom_fields(conn, table))
            present = set()
            for row in db_rows:
                values = fetch_custom_field_values(conn, table, row["id"])
                for fn_name, val in values.items():
                    if fn_name in field_names and val is not None:
                        row[udf_column_name(fn_name)] = val
                        present.add(fn_name)
            udf_columns = [udf_column_name(f) for f in field_names if f in present]

    written = write_csv_rows(real, base_columns, db_rows, udf_columns=udf_columns)

    ok({
        "entity_type": entity_type,
        "output": real,
        "exported": written,
        "udf_columns": udf_columns,
        "message": f"Exported {written} {entity_type} row(s) to {real}.",
    })


def export_leads(conn, args):
    """Export leads to a CSV file.

    Required: --output (.csv)
    Optional: --status, --include-udfs
    """
    _run_export(conn, args, "lead", "lead")


def export_opportunities(conn, args):
    """Export opportunities to a CSV file.

    Required: --output (.csv)
    Optional: --stage (or --status alias), --include-udfs
    """
    _run_export(conn, args, "opportunity", "opportunity")


def export_crm_contacts(conn, args):
    """Export CRM contacts to a CSV file.

    Required: --output (.csv)
    Optional: --lifecycle, --include-udfs
    """
    _run_export(conn, args, "crm_contact", "crm_contact")


def export_crm_companies(conn, args):
    """Export CRM companies to a CSV file.

    Required: --output (.csv)
    Optional: --lifecycle, --include-udfs
    """
    _run_export(conn, args, "crm_company", "crm_company")


# ---------------------------------------------------------------------------
# ACTIONS registry
# ---------------------------------------------------------------------------

ACTIONS = {
    "add-lead": add_lead,
    "update-lead": update_lead,
    "get-lead": get_lead,
    "list-leads": list_leads,
    "convert-lead-to-opportunity": convert_lead_to_opportunity,
    "add-opportunity": add_opportunity,
    "update-opportunity": update_opportunity,
    "get-opportunity": get_opportunity,
    "list-opportunities": list_opportunities,
    "convert-opportunity-to-quotation": convert_opportunity_to_quotation,
    "mark-opportunity-won": mark_opportunity_won,
    "mark-opportunity-lost": mark_opportunity_lost,
    "add-campaign": add_campaign,
    "list-campaigns": list_campaigns,
    "add-activity": add_activity,
    "list-activities": list_activities,
    "pipeline-report": pipeline_report,
    "status": status_action,
    # Wave 1B F1 — Contact + Company model
    "add-crm-contact": add_crm_contact,
    "update-crm-contact": update_crm_contact,
    "get-crm-contact": get_crm_contact,
    "list-crm-contacts": list_crm_contacts,
    "remove-crm-contact": remove_crm_contact,
    "add-crm-company": add_crm_company,
    "update-crm-company": update_crm_company,
    "get-crm-company": get_crm_company,
    "list-crm-companies": list_crm_companies,
    "link-contact-to-company": link_contact_to_company,
    "merge-crm-contacts": merge_crm_contacts,
    "promote-contact-to-customer": promote_contact_to_customer,
    # Wave 1B F2 — Tasks (first-class entity)
    "add-crm-task": add_crm_task,
    "update-crm-task": update_crm_task,
    "get-crm-task": get_crm_task,
    "list-crm-tasks": list_crm_tasks,
    "complete-crm-task": complete_crm_task,
    "cancel-crm-task": cancel_crm_task,
    "link-task-to-entity": link_task_to_entity,
    "unlink-task-from-entity": unlink_task_from_entity,
    # Wave 1B F3 — Pipeline stages (customizable)
    "add-crm-pipeline": add_crm_pipeline,
    "add-crm-pipeline-stage": add_crm_pipeline_stage,
    "update-crm-pipeline-stage": update_crm_pipeline_stage,
    "list-crm-pipelines": list_crm_pipelines,
    "list-crm-pipeline-stages": list_crm_pipeline_stages,
    "set-opportunity-pipeline-stage": set_opportunity_pipeline_stage,
    # Wave 1B F4 — Saved views (filter-JSON DSL + persistence)
    "add-crm-saved-view": add_crm_saved_view,
    "update-crm-saved-view": update_crm_saved_view,
    "get-crm-saved-view": get_crm_saved_view,
    "list-crm-saved-views": list_crm_saved_views,
    "delete-crm-saved-view": delete_crm_saved_view,
    "apply-saved-view": apply_saved_view,
    # Wave 1B F5 — Global search (cross-entity wrapper-merge, no FTS5)
    "global-crm-search": global_crm_search,
    # Wave 1B F6 — CSV Import / Export (8 actions)
    "import-leads": import_leads,
    "import-opportunities": import_opportunities,
    "import-crm-contacts": import_crm_contacts,
    "import-crm-companies": import_crm_companies,
    "export-leads": export_leads,
    "export-opportunities": export_opportunities,
    "export-crm-contacts": export_crm_contacts,
    "export-crm-companies": export_crm_companies,
}


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def main():
    parser = SafeArgumentParser(description="ERPClaw CRM Skill")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--company-id")

    # Entity IDs
    parser.add_argument("--lead-id")
    parser.add_argument("--opportunity-id")
    parser.add_argument("--campaign-id")
    parser.add_argument("--activity-id")
    parser.add_argument("--customer-id")

    # Lead fields
    parser.add_argument("--lead-name")
    parser.add_argument("--company-name")
    parser.add_argument("--email")
    parser.add_argument("--phone")
    parser.add_argument("--source")
    parser.add_argument("--territory")
    parser.add_argument("--industry")
    parser.add_argument("--assigned-to")
    parser.add_argument("--notes")

    # Opportunity fields
    parser.add_argument("--opportunity-name")
    parser.add_argument("--opportunity-type")
    parser.add_argument("--expected-closing-date")
    parser.add_argument("--probability")
    parser.add_argument("--expected-revenue")
    parser.add_argument("--stage")
    parser.add_argument("--lost-reason")
    parser.add_argument("--next-follow-up-date")

    # Campaign fields
    parser.add_argument("--name")
    parser.add_argument("--campaign-type")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--budget")
    parser.add_argument("--actual-spend")
    parser.add_argument("--description")

    # Activity fields
    parser.add_argument("--activity-type")
    parser.add_argument("--subject")
    parser.add_argument("--activity-date")
    parser.add_argument("--created-by")
    parser.add_argument("--next-action-date")

    # Cross-skill
    parser.add_argument("--items")  # JSON array for quotation conversion

    # Filters
    parser.add_argument("--status")
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--limit", default="20")
    parser.add_argument("--offset", default="0")
    parser.add_argument("--search")

    # Wave 1B F1 — Contact + Company model
    parser.add_argument("--crm-contact-id")
    parser.add_argument("--crm-company-id")
    parser.add_argument("--mobile")
    parser.add_argument("--job-title")
    parser.add_argument("--linkedin-url")
    parser.add_argument("--lifecycle")
    parser.add_argument("--domain")
    parser.add_argument("--revenue")
    parser.add_argument("--linked-customer-id")
    parser.add_argument("--role-title")
    parser.add_argument("--is-primary", action="store_true")
    parser.add_argument("--primary-contact-id")
    parser.add_argument("--duplicate-contact-id")

    # Wave 1B F2 — Tasks (first-class entity)
    parser.add_argument("--crm-task-id")
    parser.add_argument("--priority")
    parser.add_argument("--due-date")
    parser.add_argument("--link-to", action="append")  # repeatable "<type>:<id>"
    parser.add_argument("--linked-to")                 # filter "<type>:<id>"
    parser.add_argument("--overdue", action="store_true")
    parser.add_argument("--due-within-days")
    parser.add_argument("--task")
    parser.add_argument("--entity-type")
    parser.add_argument("--entity-id")
    parser.add_argument("--reason")

    # Wave 1B F3 — Pipeline stages (customizable). Note: --probability already
    # exists (Opportunity fields) and is reused for stage default probability;
    # --stage already exists and carries the crm_pipeline_stage id for
    # set-opportunity-pipeline-stage (spec: '--stage S').
    parser.add_argument("--id")                 # crm_pipeline_stage id (update)
    parser.add_argument("--pipeline")           # crm_pipeline id
    parser.add_argument("--order")              # stage_order
    parser.add_argument("--terminal")           # won|lost|none
    parser.add_argument("--set-as-default", action="store_true")
    parser.add_argument("--shift-existing", action="store_true")
    parser.add_argument("--is-active")
    parser.add_argument("--opportunity")        # opportunity id (set-opportunity-pipeline-stage)

    # Wave 1B F4 — Saved views. --id (reused, crm_saved_view id for get/update/
    # delete), --name, and --entity-type are reused from earlier blocks. --is-shared
    # below maps to is_shared_flag; --not-shared lets update clear the shared flag.
    parser.add_argument("--owner-user-id")
    parser.add_argument("--filter-json")
    parser.add_argument("--sort-json")
    parser.add_argument("--group-by-json")
    parser.add_argument("--column-order-json")
    parser.add_argument("--is-shared", action="store_true", dest="is_shared_flag")
    parser.add_argument("--not-shared", action="store_true")
    parser.add_argument("--shared-only", action="store_true")
    parser.add_argument("--view")               # saved-view id for apply-saved-view
    parser.add_argument("--saved-view-id")      # flag on list-leads/opportunities/contacts/companies

    # Wave 1B F5 — Global search. --query is the cross-entity search term (min 2
    # chars); --entity-types is an optional CSV restricting the fan-out set.
    # --limit (default "20" above) is reused; global-crm-search caps it at 200.
    parser.add_argument("--query")
    parser.add_argument("--entity-types")

    # Wave 1B F6 — CSV Import / Export. --file is the import source; --output the
    # export destination (both .csv, realpath-gated). --on-duplicate is REQUIRED
    # for every import-* (no default — explicit intent). --status/--stage/
    # --lifecycle (defined above) double as the simple pre-F4 export filters;
    # --include-udfs appends cf_<name> columns to an export.
    parser.add_argument("--file")
    parser.add_argument("--output")
    parser.add_argument("--on-duplicate", choices=["skip", "update", "fail"])
    parser.add_argument("--include-udfs", action="store_true")

    args, unknown = parser.parse_known_args()
    check_unknown_args(parser, unknown)
    check_input_lengths(args)

    # F4: reconcile the tri-state shared flag for update-crm-saved-view, and map
    # the create-path --is-shared. is_shared (bool) is what add-crm-saved-view
    # reads; set_shared (bool|None) is what update reads.
    args.is_shared = bool(getattr(args, "is_shared_flag", False))
    if getattr(args, "not_shared", False):
        args.set_shared = False
    elif getattr(args, "is_shared_flag", False):
        args.set_shared = True
    else:
        args.set_shared = None

    db_path = args.db_path or DEFAULT_DB_PATH
    ensure_db_exists(db_path)
    conn = get_connection(db_path)

    # Dependency check
    _dep = check_required_tables(conn, REQUIRED_TABLES)
    if _dep:
        _dep["suggestion"] = "clawhub install " + " ".join(_dep.get("missing_skills", []))
        print(json.dumps(_dep, indent=2))
        conn.close()
        sys.exit(1)

    try:
        ACTIONS[args.action](conn, args)
    except Exception as e:
        conn.rollback()
        sys.stderr.write(f"[erpclaw-crm] {e}\n")
        err("An unexpected error occurred")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
