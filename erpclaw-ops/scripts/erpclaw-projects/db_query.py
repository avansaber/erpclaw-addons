#!/usr/bin/env python3
"""ERPClaw Projects Skill -- db_query.py

Projects, tasks, milestones, timesheets, and project reports.
All 19 actions are routed through this single entry point.

Usage: python3 db_query.py --action <action-name> [--flags ...]
Output: JSON to stdout, exit 0 on success, exit 1 on error.
"""
import argparse
import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

# Add shared lib to path
try:
    import importlib.util
    if importlib.util.find_spec("erpclaw_lib") is None:
        sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.db import get_connection
    from erpclaw_lib.decimal_utils import to_decimal, round_currency
    from erpclaw_lib.naming import get_next_name
    from erpclaw_lib.validation import check_input_lengths
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.dependencies import check_required_tables
    from erpclaw_lib.cross_skill import create_invoice, submit_invoice, CrossSkillError
    from erpclaw_lib.query_helpers import resolve_company_id, resolve_scope_company
    from erpclaw_lib.query import (
        Q, P, Table, Field, fn, Case, Order,
        DecimalSum, insert_row, update_row, date_add_days, now as sql_now,
    )
    from erpclaw_lib.vendor.pypika.terms import ValueWrapper
    from erpclaw_lib.args import SafeArgumentParser, check_unknown_args
except ImportError:
    import json as _json
    print(_json.dumps({"status": "error", "error": "ERPClaw foundation not installed. Install erpclaw first: clawhub install erpclaw", "suggestion": "clawhub install erpclaw"}))
    sys.exit(1)

# The read-only message helper is newer than some installed foundations; an
# older lib keeps the generic message instead of failing the import.
try:
    from erpclaw_lib.db import unexpected_error_message
except ImportError:
    def unexpected_error_message(exc):
        return "An unexpected error occurred"

REQUIRED_TABLES = ["company"]

VALID_PROJECT_TYPES = ("internal", "external", "service", "product")
VALID_PROJECT_STATUSES = ("open", "in_progress", "completed", "cancelled", "on_hold")
VALID_PROJECT_PRIORITIES = ("low", "medium", "high", "critical")
VALID_BILLING_TYPES = ("fixed_price", "time_and_material", "non_billable")
VALID_TASK_STATUSES = ("open", "in_progress", "completed", "cancelled", "blocked")
VALID_TASK_PRIORITIES = ("low", "medium", "high", "critical")
VALID_MILESTONE_STATUSES = ("pending", "completed", "missed")
VALID_TIMESHEET_STATUSES = ("draft", "submitted", "billed", "cancelled")
VALID_ACTIVITY_TYPES = ("development", "design", "consulting", "support", "admin")

# ── PyPika table aliases ──
_t_company = Table("company")
_t_project = Table("project")
_t_task = Table("task")
_t_milestone = Table("milestone")
_t_timesheet = Table("timesheet")
_t_ts_detail = Table("timesheet_detail")
_t_employee = Table("employee")
_t_customer = Table("customer")
_t_cost_center = Table("cost_center")


def _parse_json_arg(value, name):
    if value is None:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        err(f"Invalid JSON for --{name}: {value}")


def _validate_company_exists(conn, company_id: str):
    """Validate that a company exists and return the row, or error."""
    q = Q.from_(_t_company).select(_t_company.id).where(_t_company.id == P())
    company = conn.execute(q.get_sql(), (company_id,)).fetchone()
    if not company:
        err(f"Company {company_id} not found")
    return company


def _validate_project_exists(conn, project_id: str):
    """Validate that a project exists and return the row, or error."""
    q = Q.from_(_t_project).select(_t_project.star).where(_t_project.id == P())
    project = conn.execute(q.get_sql(), (project_id,)).fetchone()
    if not project:
        err(f"Project {project_id} not found",
             suggestion="Use 'list projects' to see available projects.")
    return project


def _validate_task_exists(conn, task_id: str):
    """Validate that a task exists and return the row, or error."""
    q = Q.from_(_t_task).select(_t_task.star).where(_t_task.id == P())
    task = conn.execute(q.get_sql(), (task_id,)).fetchone()
    if not task:
        err(f"Task {task_id} not found")
    return task


def _validate_milestone_exists(conn, milestone_id: str):
    """Validate that a milestone exists and return the row, or error."""
    q = Q.from_(_t_milestone).select(_t_milestone.star).where(_t_milestone.id == P())
    milestone = conn.execute(q.get_sql(), (milestone_id,)).fetchone()
    if not milestone:
        err(f"Milestone {milestone_id} not found")
    return milestone


def _validate_timesheet_exists(conn, timesheet_id: str):
    """Validate that a timesheet exists and return the row, or error."""
    q = Q.from_(_t_timesheet).select(_t_timesheet.star).where(_t_timesheet.id == P())
    ts = conn.execute(q.get_sql(), (timesheet_id,)).fetchone()
    if not ts:
        err(f"Timesheet {timesheet_id} not found")
    return ts


def _validate_employee_exists(conn, employee_id: str):
    """Validate that an employee exists and return the row, or error."""
    q = Q.from_(_t_employee).select(_t_employee.star).where(_t_employee.id == P())
    emp = conn.execute(q.get_sql(), (employee_id,)).fetchone()
    if not emp:
        err(f"Employee {employee_id} not found")
    return emp


def _validate_customer_exists(conn, customer_id: str):
    """Validate that a customer exists and return the row, or error."""
    q = Q.from_(_t_customer).select(_t_customer.star).where(_t_customer.id == P())
    cust = conn.execute(q.get_sql(), (customer_id,)).fetchone()
    if not cust:
        err(f"Customer {customer_id} not found")
    return cust


# ---------------------------------------------------------------------------
# 1. add-project
# ---------------------------------------------------------------------------

def add_project(conn, args):
    """Create a new project.

    Required: --company-id, --name
    Optional: --customer-id, --project-type, --status, --priority,
              --start-date, --end-date, --estimated-cost, --billing-type,
              --cost-center-id, --description
    """
    if not args.company_id:
        err("--company-id is required")
    if not args.name:
        err("--name is required")

    _validate_company_exists(conn, args.company_id)

    if args.customer_id:
        _validate_customer_exists(conn, args.customer_id)

    project_type = args.project_type or "internal"
    if project_type not in VALID_PROJECT_TYPES:
        err(f"Invalid --project-type: {project_type}. Must be one of {VALID_PROJECT_TYPES}")

    status = args.status or "open"
    if status not in VALID_PROJECT_STATUSES:
        err(f"Invalid --status: {status}. Must be one of {VALID_PROJECT_STATUSES}")

    priority = args.priority or "medium"
    if priority not in VALID_PROJECT_PRIORITIES:
        err(f"Invalid --priority: {priority}. Must be one of {VALID_PROJECT_PRIORITIES}")

    billing_type = args.billing_type or "non_billable"
    if billing_type not in VALID_BILLING_TYPES:
        err(f"Invalid --billing-type: {billing_type}. Must be one of {VALID_BILLING_TYPES}")

    estimated_cost = "0"
    if args.estimated_cost:
        estimated_cost = str(round_currency(to_decimal(args.estimated_cost)))
        if to_decimal(estimated_cost) < 0:
            err("--estimated-cost must be >= 0")

    if args.cost_center_id:
        cc_q = (Q.from_(_t_cost_center).select(_t_cost_center.id)
                .where((_t_cost_center.id == P()) | (_t_cost_center.name == P())))
        cc = conn.execute(cc_q.get_sql(), (args.cost_center_id, args.cost_center_id)).fetchone()
        if not cc:
            err(f"Cost center {args.cost_center_id} not found")
        args.cost_center_id = cc["id"]

    project_id = str(uuid.uuid4())
    naming = get_next_name(conn, "project", company_id=args.company_id)

    ins_sql, _ = insert_row("project", {
        "id": P(), "naming_series": P(), "project_name": P(),
        "customer_id": P(), "project_type": P(), "status": P(),
        "priority": P(), "start_date": P(), "end_date": P(),
        "estimated_cost": P(), "actual_cost": P(), "billing_type": P(),
        "total_billed": P(), "profit_margin": P(), "percent_complete": P(),
        "cost_center_id": P(), "company_id": P(),
    })
    conn.execute(ins_sql, (
        project_id, naming, args.name, args.customer_id, project_type,
        status, priority, args.start_date, args.end_date, estimated_cost,
        "0", billing_type, "0", "0", "0", args.cost_center_id, args.company_id,
    ))
    audit(conn, "erpclaw-projects", "add-project", "project", project_id,
           new_values={"project_name": args.name, "company_id": args.company_id})
    conn.commit()

    q = Q.from_(_t_project).select(_t_project.star).where(_t_project.id == P())
    project = conn.execute(q.get_sql(), (project_id,)).fetchone()
    ok({"project": row_to_dict(project)})


# ---------------------------------------------------------------------------
# 2. update-project
# ---------------------------------------------------------------------------

def update_project(conn, args):
    """Update project fields.

    Required: --project-id
    Optional: --name, --customer-id, --project-type, --status, --priority,
              --start-date, --end-date, --estimated-cost, --billing-type,
              --cost-center-id, --percent-complete, --actual-cost, --total-billed,
              --description
    """
    if not args.project_id:
        err("--project-id is required")

    project = _validate_project_exists(conn, args.project_id)
    old_values = row_to_dict(project)

    updates = []
    params = []

    if args.name is not None:
        updates.append("project_name = ?")
        params.append(args.name)

    if args.customer_id is not None:
        if args.customer_id:
            _validate_customer_exists(conn, args.customer_id)
        updates.append("customer_id = ?")
        params.append(args.customer_id if args.customer_id else None)

    if args.project_type is not None:
        if args.project_type not in VALID_PROJECT_TYPES:
            err(f"Invalid --project-type: {args.project_type}. Must be one of {VALID_PROJECT_TYPES}")
        updates.append("project_type = ?")
        params.append(args.project_type)

    if args.status is not None:
        if args.status not in VALID_PROJECT_STATUSES:
            err(f"Invalid --status: {args.status}. Must be one of {VALID_PROJECT_STATUSES}")
        updates.append("status = ?")
        params.append(args.status)

    if args.priority is not None:
        if args.priority not in VALID_PROJECT_PRIORITIES:
            err(f"Invalid --priority: {args.priority}. Must be one of {VALID_PROJECT_PRIORITIES}")
        updates.append("priority = ?")
        params.append(args.priority)

    if args.start_date is not None:
        updates.append("start_date = ?")
        params.append(args.start_date)

    if args.end_date is not None:
        updates.append("end_date = ?")
        params.append(args.end_date)

    if args.estimated_cost is not None:
        val = str(round_currency(to_decimal(args.estimated_cost)))
        if to_decimal(val) < 0:
            err("--estimated-cost must be >= 0")
        updates.append("estimated_cost = ?")
        params.append(val)

    if args.actual_cost is not None:
        val = str(round_currency(to_decimal(args.actual_cost)))
        if to_decimal(val) < 0:
            err("--actual-cost must be >= 0")
        updates.append("actual_cost = ?")
        params.append(val)

    if args.total_billed is not None:
        val = str(round_currency(to_decimal(args.total_billed)))
        if to_decimal(val) < 0:
            err("--total-billed must be >= 0")
        updates.append("total_billed = ?")
        params.append(val)

    if args.billing_type is not None:
        if args.billing_type not in VALID_BILLING_TYPES:
            err(f"Invalid --billing-type: {args.billing_type}. Must be one of {VALID_BILLING_TYPES}")
        updates.append("billing_type = ?")
        params.append(args.billing_type)

    if args.cost_center_id is not None:
        if args.cost_center_id:
            cc_q = (Q.from_(_t_cost_center).select(_t_cost_center.id)
                    .where((_t_cost_center.id == P()) | (_t_cost_center.name == P())))
            cc = conn.execute(cc_q.get_sql(), (args.cost_center_id, args.cost_center_id)).fetchone()
            if not cc:
                err(f"Cost center {args.cost_center_id} not found")
            args.cost_center_id = cc["id"]
        updates.append("cost_center_id = ?")
        params.append(args.cost_center_id if args.cost_center_id else None)

    if args.percent_complete is not None:
        val = to_decimal(args.percent_complete)
        if val < 0 or val > Decimal("100"):
            err("--percent-complete must be between 0 and 100")
        updates.append("percent_complete = ?")
        params.append(str(round_currency(val)))

    if not updates:
        err("No fields to update")

    # Recalculate profit_margin if actual_cost or total_billed changed
    # We always recalculate from the resulting values
    updates.append(f"updated_at = {sql_now()}")

    sql = f"UPDATE project SET {', '.join(updates)} WHERE id = ?"
    params.append(args.project_id)
    conn.execute(sql, params)

    # Recalculate profit_margin from current values
    refetch_q = Q.from_(_t_project).select(_t_project.star).where(_t_project.id == P())
    updated = conn.execute(refetch_q.get_sql(), (args.project_id,)).fetchone()
    total_billed = to_decimal(updated["total_billed"])
    actual_cost = to_decimal(updated["actual_cost"])
    if total_billed > 0:
        profit_margin = round_currency(
            ((total_billed - actual_cost) / total_billed) * Decimal("100")
        )
    else:
        profit_margin = Decimal("0")

    margin_sql = update_row("project", {"profit_margin": P()}, {"id": P()})
    conn.execute(margin_sql, (str(profit_margin), args.project_id))

    audit(conn, "erpclaw-projects", "update-project", "project", args.project_id,
           old_values=old_values, description="Project updated")
    conn.commit()

    project = conn.execute(refetch_q.get_sql(), (args.project_id,)).fetchone()
    ok({"project": row_to_dict(project)})


# ---------------------------------------------------------------------------
# 3. get-project
# ---------------------------------------------------------------------------

def get_project(conn, args):
    """Return project with tasks, milestones, and timesheet summary.

    Required: --project-id
    """
    if not args.project_id:
        err("--project-id is required")

    project = _validate_project_exists(conn, args.project_id)
    project_dict = row_to_dict(project)

    # Fetch tasks
    tasks_q = (Q.from_(_t_task).select(_t_task.star)
               .where(_t_task.project_id == P())
               .orderby(_t_task.start_date)
               .orderby(_t_task.task_name))
    tasks = conn.execute(tasks_q.get_sql(), (args.project_id,)).fetchall()
    project_dict["tasks"] = [row_to_dict(t) for t in tasks]

    # Fetch milestones
    ms_q = (Q.from_(_t_milestone).select(_t_milestone.star)
            .where(_t_milestone.project_id == P())
            .orderby(_t_milestone.target_date))
    milestones = conn.execute(ms_q.get_sql(), (args.project_id,)).fetchall()
    project_dict["milestones"] = [row_to_dict(m) for m in milestones]

    # Timesheet summary: aggregate from timesheet_detail for this project.
    # Lines are read as TEXT and summed in Python with Decimal: doing
    # arithmetic in SQL on hours/billing_rate (`+ 0`) coerces to binary
    # float on SQLite and is refused on PostgreSQL.
    td = Table("timesheet_detail")
    ts = Table("timesheet")
    ts_lines_q = (Q.from_(td)
                  .join(ts).on(td.timesheet_id == ts.id)
                  .select(
                      td.timesheet_id,
                      td.hours,
                      td.billing_rate,
                      td.billable,
                  )
                  .where(td.project_id == P())
                  .where(ts.status.isin(["submitted", "billed"])))
    ts_lines = conn.execute(ts_lines_q.get_sql(), (args.project_id,)).fetchall()
    total_hours_sum = Decimal("0")
    billable_hours_sum = Decimal("0")
    billable_amount_sum = Decimal("0")
    timesheet_ids = set()
    for line in ts_lines:
        hours = to_decimal(line["hours"])
        rate = to_decimal(line["billing_rate"])
        total_hours_sum += hours
        timesheet_ids.add(line["timesheet_id"])
        if int(line["billable"]) == 1:
            billable_hours_sum += hours
            billable_amount_sum += hours * rate
    project_dict["timesheet_summary"] = {
        "total_hours": str(round_currency(total_hours_sum)),
        "billable_hours": str(round_currency(billable_hours_sum)),
        "billable_amount": str(round_currency(billable_amount_sum)),
        "timesheet_count": len(timesheet_ids),
    }

    ok({"project": project_dict})


# ---------------------------------------------------------------------------
# 4. list-projects
# ---------------------------------------------------------------------------

def list_projects(conn, args):
    """List projects with optional filters.

    Optional: --company-id, --status, --from-date, --to-date, --search,
              --customer-id, --limit, --offset
    """
    p = _t_project
    params = []

    base_q = Q.from_(p)

    if args.company_id:
        base_q = base_q.where(p.company_id == P())
        params.append(args.company_id)

    if args.status:
        if args.status not in VALID_PROJECT_STATUSES:
            err(f"Invalid --status: {args.status}. Must be one of {VALID_PROJECT_STATUSES}")
        base_q = base_q.where(p.status == P())
        params.append(args.status)

    if args.customer_id:
        base_q = base_q.where(p.customer_id == P())
        params.append(args.customer_id)

    if args.from_date:
        base_q = base_q.where(p.start_date >= P())
        params.append(args.from_date)

    if args.to_date:
        base_q = base_q.where((p.end_date <= P()) | (p.end_date.isnull()))
        params.append(args.to_date)

    if args.search:
        base_q = base_q.where(p.project_name.like(P()))
        params.append(f"%{args.search}%")

    limit = int(args.limit or "20")
    offset = int(args.offset or "0")

    # Count
    count_q = base_q.select(fn.Count("*", alias="cnt"))
    count_row = conn.execute(count_q.get_sql(), params).fetchone()
    total = count_row["cnt"]

    # Fetch
    fetch_q = (base_q.select(p.star)
               .orderby(p.created_at, order=Order.desc)
               .limit(P()).offset(P()))
    rows = conn.execute(fetch_q.get_sql(), params + [limit, offset]).fetchall()

    ok({
        "projects": [row_to_dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + limit < total,
    })


# ---------------------------------------------------------------------------
# 5. add-task
# ---------------------------------------------------------------------------

def add_task(conn, args):
    """Create a task under a project.

    Required: --project-id, --name
    Optional: --assigned-to, --priority, --status, --start-date, --end-date,
              --estimated-hours, --depends-on (JSON array of task IDs),
              --parent-task-id, --description
    """
    if not args.project_id:
        err("--project-id is required")
    if not args.name:
        err("--name is required")

    project = _validate_project_exists(conn, args.project_id)

    status = args.status or "open"
    if status not in VALID_TASK_STATUSES:
        err(f"Invalid --status: {status}. Must be one of {VALID_TASK_STATUSES}")

    priority = args.priority or "medium"
    if priority not in VALID_TASK_PRIORITIES:
        err(f"Invalid --priority: {priority}. Must be one of {VALID_TASK_PRIORITIES}")

    estimated_hours = "0"
    if args.estimated_hours:
        estimated_hours = str(round_currency(to_decimal(args.estimated_hours)))
        if to_decimal(estimated_hours) < 0:
            err("--estimated-hours must be >= 0")

    # Validate parent task if provided
    if args.parent_task_id:
        parent = _validate_task_exists(conn, args.parent_task_id)
        if parent["project_id"] != args.project_id:
            err(f"Parent task {args.parent_task_id} belongs to a different project")

    # Validate depends_on task IDs
    depends_on = None
    dep_q = Q.from_(_t_task).select(_t_task.id, _t_task.project_id).where(_t_task.id == P())
    if args.depends_on:
        dep_ids = _parse_json_arg(args.depends_on, "depends-on")
        if not isinstance(dep_ids, list):
            err("--depends-on must be a JSON array of task IDs")
        for dep_id in dep_ids:
            dep_task = conn.execute(dep_q.get_sql(), (dep_id,)).fetchone()
            if not dep_task:
                err(f"Dependency task {dep_id} not found")
            if dep_task["project_id"] != args.project_id:
                err(f"Dependency task {dep_id} belongs to a different project")
        depends_on = json.dumps(dep_ids)

    task_id = str(uuid.uuid4())
    # Use project's company_id for naming
    naming = get_next_name(conn, "task", company_id=project["company_id"])

    ins_sql, _ = insert_row("task", {
        "id": P(), "naming_series": P(), "project_id": P(), "task_name": P(),
        "parent_task_id": P(), "assigned_to": P(), "status": P(), "priority": P(),
        "start_date": P(), "end_date": P(), "estimated_hours": P(),
        "actual_hours": P(), "depends_on": P(), "description": P(),
    })
    conn.execute(ins_sql, (
        task_id, naming, args.project_id, args.name, args.parent_task_id,
        args.assigned_to, status, priority, args.start_date, args.end_date,
        estimated_hours, "0", depends_on, args.description,
    ))
    audit(conn, "erpclaw-projects", "add-task", "task", task_id,
           new_values={"task_name": args.name, "project_id": args.project_id})
    conn.commit()

    refetch_q = Q.from_(_t_task).select(_t_task.star).where(_t_task.id == P())
    task = conn.execute(refetch_q.get_sql(), (task_id,)).fetchone()
    ok({"task": row_to_dict(task)})


# ---------------------------------------------------------------------------
# 6. update-task
# ---------------------------------------------------------------------------

def update_task(conn, args):
    """Update task fields.

    Required: --task-id
    Optional: --name, --assigned-to, --priority, --status, --start-date,
              --end-date, --estimated-hours, --actual-hours, --depends-on,
              --description
    """
    if not args.task_id:
        err("--task-id is required")

    task = _validate_task_exists(conn, args.task_id)
    old_values = row_to_dict(task)

    updates = []
    params = []

    if args.name is not None:
        updates.append("task_name = ?")
        params.append(args.name)

    if args.assigned_to is not None:
        updates.append("assigned_to = ?")
        params.append(args.assigned_to if args.assigned_to else None)

    if args.priority is not None:
        if args.priority not in VALID_TASK_PRIORITIES:
            err(f"Invalid --priority: {args.priority}. Must be one of {VALID_TASK_PRIORITIES}")
        updates.append("priority = ?")
        params.append(args.priority)

    if args.status is not None:
        if args.status not in VALID_TASK_STATUSES:
            err(f"Invalid --status: {args.status}. Must be one of {VALID_TASK_STATUSES}")
        updates.append("status = ?")
        params.append(args.status)

    if args.start_date is not None:
        updates.append("start_date = ?")
        params.append(args.start_date)

    if args.end_date is not None:
        updates.append("end_date = ?")
        params.append(args.end_date)

    if args.estimated_hours is not None:
        val = str(round_currency(to_decimal(args.estimated_hours)))
        if to_decimal(val) < 0:
            err("--estimated-hours must be >= 0")
        updates.append("estimated_hours = ?")
        params.append(val)

    if args.actual_hours is not None:
        val = str(round_currency(to_decimal(args.actual_hours)))
        if to_decimal(val) < 0:
            err("--actual-hours must be >= 0")
        updates.append("actual_hours = ?")
        params.append(val)

    if args.depends_on is not None:
        if args.depends_on == "":
            updates.append("depends_on = ?")
            params.append(None)
        else:
            dep_ids = _parse_json_arg(args.depends_on, "depends-on")
            if not isinstance(dep_ids, list):
                err("--depends-on must be a JSON array of task IDs")
            project_id = task["project_id"]
            dep_q = Q.from_(_t_task).select(_t_task.id, _t_task.project_id).where(_t_task.id == P())
            for dep_id in dep_ids:
                dep_task = conn.execute(dep_q.get_sql(), (dep_id,)).fetchone()
                if not dep_task:
                    err(f"Dependency task {dep_id} not found")
                if dep_task["project_id"] != project_id:
                    err(f"Dependency task {dep_id} belongs to a different project")
            updates.append("depends_on = ?")
            params.append(json.dumps(dep_ids))

    if args.description is not None:
        updates.append("description = ?")
        params.append(args.description if args.description else None)

    if not updates:
        err("No fields to update")

    updates.append(f"updated_at = {sql_now()}")

    sql = f"UPDATE task SET {', '.join(updates)} WHERE id = ?"
    params.append(args.task_id)
    conn.execute(sql, params)

    audit(conn, "erpclaw-projects", "update-task", "task", args.task_id,
           old_values=old_values, description="Task updated")
    conn.commit()

    refetch_q = Q.from_(_t_task).select(_t_task.star).where(_t_task.id == P())
    task = conn.execute(refetch_q.get_sql(), (args.task_id,)).fetchone()
    ok({"task": row_to_dict(task)})


# ---------------------------------------------------------------------------
# 7. list-tasks
# ---------------------------------------------------------------------------

def list_tasks(conn, args):
    """List tasks for a project with optional filters.

    Required: --project-id
    Optional: --status, --assigned-to, --priority, --limit, --offset
    """
    if not args.project_id:
        err("--project-id is required")

    _validate_project_exists(conn, args.project_id)

    t = _t_task
    params = [args.project_id]
    base_q = Q.from_(t).where(t.project_id == P())

    if args.status:
        if args.status not in VALID_TASK_STATUSES:
            err(f"Invalid --status: {args.status}. Must be one of {VALID_TASK_STATUSES}")
        base_q = base_q.where(t.status == P())
        params.append(args.status)

    if args.assigned_to:
        base_q = base_q.where(t.assigned_to == P())
        params.append(args.assigned_to)

    if args.priority:
        if args.priority not in VALID_TASK_PRIORITIES:
            err(f"Invalid --priority: {args.priority}. Must be one of {VALID_TASK_PRIORITIES}")
        base_q = base_q.where(t.priority == P())
        params.append(args.priority)

    limit = int(args.limit or "20")
    offset = int(args.offset or "0")

    count_q = base_q.select(fn.Count("*", alias="cnt"))
    count_row = conn.execute(count_q.get_sql(), params).fetchone()
    total = count_row["cnt"]

    fetch_q = (base_q.select(t.star)
               .orderby(t.start_date).orderby(t.task_name)
               .limit(P()).offset(P()))
    rows = conn.execute(fetch_q.get_sql(), params + [limit, offset]).fetchall()

    ok({
        "tasks": [row_to_dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + limit < total,
    })


# ---------------------------------------------------------------------------
# 8. add-milestone
# ---------------------------------------------------------------------------

def add_milestone(conn, args):
    """Create a milestone for a project.

    Required: --project-id, --name, --target-date
    Optional: --description
    """
    if not args.project_id:
        err("--project-id is required")
    if not args.name:
        err("--name is required")
    if not args.target_date:
        err("--target-date is required")

    _validate_project_exists(conn, args.project_id)

    milestone_id = str(uuid.uuid4())

    ins_sql, _ = insert_row("milestone", {
        "id": P(), "project_id": P(), "milestone_name": P(),
        "target_date": P(), "status": P(), "description": P(),
    })
    conn.execute(ins_sql, (
        milestone_id, args.project_id, args.name, args.target_date,
        "pending", args.description,
    ))
    audit(conn, "erpclaw-projects", "add-milestone", "milestone", milestone_id,
           new_values={"milestone_name": args.name, "project_id": args.project_id})
    conn.commit()

    refetch_q = Q.from_(_t_milestone).select(_t_milestone.star).where(_t_milestone.id == P())
    milestone = conn.execute(refetch_q.get_sql(), (milestone_id,)).fetchone()
    ok({"milestone": row_to_dict(milestone)})


# ---------------------------------------------------------------------------
# 9. update-milestone
# ---------------------------------------------------------------------------

def update_milestone(conn, args):
    """Update milestone fields.

    Required: --milestone-id
    Optional: --name, --target-date, --completion-date, --status, --description
    """
    if not args.milestone_id:
        err("--milestone-id is required")

    milestone = _validate_milestone_exists(conn, args.milestone_id)
    old_values = row_to_dict(milestone)

    updates = []
    params = []

    if args.name is not None:
        updates.append("milestone_name = ?")
        params.append(args.name)

    if args.target_date is not None:
        updates.append("target_date = ?")
        params.append(args.target_date)

    if args.completion_date is not None:
        updates.append("completion_date = ?")
        params.append(args.completion_date if args.completion_date else None)

    if args.status is not None:
        if args.status not in VALID_MILESTONE_STATUSES:
            err(f"Invalid --status: {args.status}. Must be one of {VALID_MILESTONE_STATUSES}")
        updates.append("status = ?")
        params.append(args.status)

    if args.description is not None:
        updates.append("description = ?")
        params.append(args.description if args.description else None)

    if not updates:
        err("No fields to update")

    updates.append(f"updated_at = {sql_now()}")

    sql = f"UPDATE milestone SET {', '.join(updates)} WHERE id = ?"
    params.append(args.milestone_id)
    conn.execute(sql, params)

    audit(conn, "erpclaw-projects", "update-milestone", "milestone", args.milestone_id,
           old_values=old_values, description="Milestone updated")
    conn.commit()

    refetch_q = Q.from_(_t_milestone).select(_t_milestone.star).where(_t_milestone.id == P())
    milestone = conn.execute(refetch_q.get_sql(), (args.milestone_id,)).fetchone()
    ok({"milestone": row_to_dict(milestone)})


# ---------------------------------------------------------------------------
# 10. add-timesheet
# ---------------------------------------------------------------------------

def add_timesheet(conn, args):
    """Create a draft timesheet with line items.

    Required: --company-id, --employee-id, --start-date, --end-date,
              --items (JSON array of detail rows)
    Each item: {project_id, task_id?, activity_type?, hours, billing_rate?,
                billable?, description?, date}
    """
    if not args.company_id:
        err("--company-id is required")
    if not args.employee_id:
        err("--employee-id is required")
    if not args.start_date:
        err("--start-date is required")
    if not args.end_date:
        err("--end-date is required")
    if not args.items:
        err("--items is required (JSON array of timesheet detail rows)")

    _validate_company_exists(conn, args.company_id)
    _validate_employee_exists(conn, args.employee_id)

    items = _parse_json_arg(args.items, "items")
    if not items or not isinstance(items, list):
        err("--items must be a non-empty JSON array")

    timesheet_id = str(uuid.uuid4())
    naming = get_next_name(conn, "timesheet", company_id=args.company_id)

    total_hours = Decimal("0")
    total_billable_hours = Decimal("0")
    total_billable_amount = Decimal("0")
    total_cost = Decimal("0")
    detail_rows = []

    for i, item in enumerate(items):
        # Validate project_id
        proj_id = item.get("project_id")
        if not proj_id:
            err(f"Item {i}: project_id is required")
        _validate_project_exists(conn, proj_id)

        # Validate task_id if provided
        task_id = item.get("task_id")
        if task_id:
            task = _validate_task_exists(conn, task_id)
            if task["project_id"] != proj_id:
                err(f"Item {i}: task {task_id} does not belong to project {proj_id}")

        # Validate activity_type
        activity_type = item.get("activity_type")
        if activity_type and activity_type not in VALID_ACTIVITY_TYPES:
            err(f"Item {i}: invalid activity_type '{activity_type}'. Must be one of {VALID_ACTIVITY_TYPES}")

        # Parse hours
        hours = to_decimal(item.get("hours", "0"))
        if hours <= 0:
            err(f"Item {i}: hours must be > 0")

        # Parse billing rate and billable flag
        billing_rate = to_decimal(item.get("billing_rate", "0"))
        billable = int(item.get("billable", 1))
        if billable not in (0, 1):
            err(f"Item {i}: billable must be 0 or 1")

        # Validate date
        item_date = item.get("date")
        if not item_date:
            err(f"Item {i}: date is required")

        detail_id = str(uuid.uuid4())
        detail_rows.append({
            "id": detail_id,
            "timesheet_id": timesheet_id,
            "project_id": proj_id,
            "task_id": task_id,
            "activity_type": activity_type,
            "hours": str(round_currency(hours)),
            "billing_rate": str(round_currency(billing_rate)),
            "billable": billable,
            "description": item.get("description"),
            "date": item_date,
        })

        total_hours += hours
        if billable:
            total_billable_hours += hours
            total_billable_amount += round_currency(hours * billing_rate)
        # Cost = hours * billing_rate (or could be costing rate; for simplicity cost = billable amount)
        total_cost += round_currency(hours * billing_rate)

    total_hours = round_currency(total_hours)
    total_billable_hours = round_currency(total_billable_hours)
    total_billable_amount = round_currency(total_billable_amount)
    total_cost = round_currency(total_cost)

    # Insert timesheet header
    hdr_sql, _ = insert_row("timesheet", {
        "id": P(), "naming_series": P(), "employee_id": P(),
        "start_date": P(), "end_date": P(), "total_hours": P(),
        "total_billable_hours": P(), "total_billed_hours": P(),
        "total_cost": P(), "total_billable_amount": P(),
        "status": P(), "company_id": P(),
    })
    conn.execute(hdr_sql, (
        timesheet_id, naming, args.employee_id, args.start_date,
        args.end_date, str(total_hours), str(total_billable_hours), "0",
        str(total_cost), str(total_billable_amount), "draft", args.company_id,
    ))

    # Insert detail rows
    dtl_sql, _ = insert_row("timesheet_detail", {
        "id": P(), "timesheet_id": P(), "project_id": P(), "task_id": P(),
        "activity_type": P(), "hours": P(), "billing_rate": P(),
        "billable": P(), "description": P(), "date": P(),
    })
    for d in detail_rows:
        conn.execute(dtl_sql, (
            d["id"], d["timesheet_id"], d["project_id"], d["task_id"],
            d["activity_type"], d["hours"], d["billing_rate"], d["billable"],
            d["description"], d["date"],
        ))

    audit(conn, "erpclaw-projects", "add-timesheet", "timesheet", timesheet_id,
           new_values={"employee_id": args.employee_id,
                       "total_hours": str(total_hours),
                       "items_count": len(detail_rows)})
    conn.commit()

    ts_q = Q.from_(_t_timesheet).select(_t_timesheet.star).where(_t_timesheet.id == P())
    ts = conn.execute(ts_q.get_sql(), (timesheet_id,)).fetchone()
    dtl_q = (Q.from_(_t_ts_detail).select(_t_ts_detail.star)
             .where(_t_ts_detail.timesheet_id == P())
             .orderby(_t_ts_detail.date))
    details = conn.execute(dtl_q.get_sql(), (timesheet_id,)).fetchall()

    result = row_to_dict(ts)
    result["items"] = [row_to_dict(d) for d in details]
    ok({"timesheet": result})


# ---------------------------------------------------------------------------
# 11. get-timesheet
# ---------------------------------------------------------------------------

def get_timesheet(conn, args):
    """Return timesheet with detail rows.

    Required: --timesheet-id
    """
    if not args.timesheet_id:
        err("--timesheet-id is required")

    ts = _validate_timesheet_exists(conn, args.timesheet_id)
    result = row_to_dict(ts)

    dtl_q = (Q.from_(_t_ts_detail).select(_t_ts_detail.star)
             .where(_t_ts_detail.timesheet_id == P())
             .orderby(_t_ts_detail.date))
    details = conn.execute(dtl_q.get_sql(), (args.timesheet_id,)).fetchall()
    result["items"] = [row_to_dict(d) for d in details]

    # Include employee name
    emp_q = (Q.from_(_t_employee)
             .select(_t_employee.full_name.as_("employee_name"))
             .where(_t_employee.id == P()))
    emp = conn.execute(emp_q.get_sql(), (ts["employee_id"],)).fetchone()
    if emp:
        result["employee_name"] = emp["employee_name"]

    ok({"timesheet": result})


# ---------------------------------------------------------------------------
# 12. list-timesheets
# ---------------------------------------------------------------------------

def list_timesheets(conn, args):
    """List timesheets with optional filters.

    Optional: --company-id, --employee-id, --project-id, --status,
              --from-date, --to-date, --limit, --offset
    """
    ts = Table("timesheet")
    td = Table("timesheet_detail")
    params = []

    base_q = Q.from_(ts)

    if args.company_id:
        base_q = base_q.where(ts.company_id == P())
        params.append(args.company_id)

    if args.employee_id:
        base_q = base_q.where(ts.employee_id == P())
        params.append(args.employee_id)

    if args.status:
        if args.status not in VALID_TIMESHEET_STATUSES:
            err(f"Invalid --status: {args.status}. Must be one of {VALID_TIMESHEET_STATUSES}")
        base_q = base_q.where(ts.status == P())
        params.append(args.status)

    if args.from_date:
        base_q = base_q.where(ts.start_date >= P())
        params.append(args.from_date)

    if args.to_date:
        base_q = base_q.where(ts.end_date <= P())
        params.append(args.to_date)

    # If --project-id filter, join to detail to find timesheets for that project
    use_distinct = False
    if args.project_id:
        base_q = base_q.join(td).on(ts.id == td.timesheet_id)
        base_q = base_q.where(td.project_id == P())
        params.append(args.project_id)
        use_distinct = True

    limit = int(args.limit or "20")
    offset = int(args.offset or "0")

    # Use DISTINCT when joining to detail
    if use_distinct:
        count_q = base_q.select(fn.Count(ts.id, alias="cnt").distinct())
    else:
        count_q = base_q.select(fn.Count(ts.id, alias="cnt"))
    count_row = conn.execute(count_q.get_sql(), params).fetchone()
    total = count_row["cnt"]

    # For DISTINCT ts.* with JOIN, use raw SQL fragment since PyPika
    # DISTINCT applies to the whole SELECT, not individual columns for ts.*
    if use_distinct:
        # Build WHERE from base_q but use raw for DISTINCT ts.*
        conditions = []
        raw_params = []
        if args.company_id:
            conditions.append("ts.company_id = ?")
            raw_params.append(args.company_id)
        if args.employee_id:
            conditions.append("ts.employee_id = ?")
            raw_params.append(args.employee_id)
        if args.status:
            conditions.append("ts.status = ?")
            raw_params.append(args.status)
        if args.from_date:
            conditions.append("ts.start_date >= ?")
            raw_params.append(args.from_date)
        if args.to_date:
            conditions.append("ts.end_date <= ?")
            raw_params.append(args.to_date)
        conditions.append("td.project_id = ?")
        raw_params.append(args.project_id)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = conn.execute(
            f"SELECT DISTINCT ts.* FROM timesheet ts "
            f"JOIN timesheet_detail td ON ts.id = td.timesheet_id "
            f"{where} ORDER BY ts.created_at DESC LIMIT ? OFFSET ?",
            raw_params + [limit, offset],
        ).fetchall()
    else:
        fetch_q = (base_q.select(ts.star)
                   .orderby(ts.created_at, order=Order.desc)
                   .limit(P()).offset(P()))
        rows = conn.execute(fetch_q.get_sql(), params + [limit, offset]).fetchall()

    ok({
        "timesheets": [row_to_dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + limit < total,
    })


# ---------------------------------------------------------------------------
# 13. submit-timesheet
# ---------------------------------------------------------------------------

def submit_timesheet(conn, args):
    """Move timesheet from draft to submitted.

    Required: --timesheet-id
    Validates all detail rows have valid project_ids and hours > 0.
    Updates task actual_hours by adding timesheet hours.
    """
    if not args.timesheet_id:
        err("--timesheet-id is required")

    ts = _validate_timesheet_exists(conn, args.timesheet_id)
    if ts["status"] != "draft":
        err(f"Timesheet is '{ts['status']}', can only submit from 'draft'")

    # Validate detail rows
    dtl_q = Q.from_(_t_ts_detail).select(_t_ts_detail.star).where(_t_ts_detail.timesheet_id == P())
    details = conn.execute(dtl_q.get_sql(), (args.timesheet_id,)).fetchall()
    if not details:
        err("Timesheet has no detail rows")

    proj_q = Q.from_(_t_project).select(_t_project.id).where(_t_project.id == P())
    for d in details:
        d_dict = row_to_dict(d)
        # Validate project exists
        proj = conn.execute(proj_q.get_sql(), (d_dict["project_id"],)).fetchone()
        if not proj:
            err(f"Detail row {d_dict['id']}: project {d_dict['project_id']} not found")

        hours = to_decimal(d_dict["hours"])
        if hours <= 0:
            err(f"Detail row {d_dict['id']}: hours must be > 0, got {hours}")

    # Update status; the timestamp is rendered per dialect at call time.
    conn.execute(
        update_row("timesheet",
                   data={"status": P(), "updated_at": sql_now()},
                   where={"id": P()}),
        ("submitted", args.timesheet_id),
    )

    # Update task actual_hours for each detail row that references a task
    task_hrs_q = Q.from_(_t_task).select(_t_task.actual_hours).where(_t_task.id == P())
    for d in details:
        d_dict = row_to_dict(d)
        task_id = d_dict.get("task_id")
        if task_id:
            hours = to_decimal(d_dict["hours"])
            task = conn.execute(task_hrs_q.get_sql(), (task_id,)).fetchone()
            if task:
                new_hours = round_currency(to_decimal(task["actual_hours"]) + hours)
                conn.execute(
                    update_row("task",
                               data={"actual_hours": P(), "updated_at": sql_now()},
                               where={"id": P()}),
                    (str(new_hours), task_id),
                )

    audit(conn, "erpclaw-projects", "submit-timesheet", "timesheet", args.timesheet_id,
           old_values={"status": "draft"}, new_values={"status": "submitted"},
           description="Timesheet submitted")
    conn.commit()

    ts_refetch = Q.from_(_t_timesheet).select(_t_timesheet.star).where(_t_timesheet.id == P())
    ts = conn.execute(ts_refetch.get_sql(), (args.timesheet_id,)).fetchone()
    ok({"timesheet": row_to_dict(ts)})


# ---------------------------------------------------------------------------
# 14. bill-timesheet
# ---------------------------------------------------------------------------

def bill_timesheet(conn, args):
    """Move timesheet from submitted to billed.

    Required: --timesheet-id
    Marks total_billed_hours = total_billable_hours.
    Updates project total_billed and actual_cost.
    """
    if not args.timesheet_id:
        err("--timesheet-id is required")

    ts = _validate_timesheet_exists(conn, args.timesheet_id)
    if ts["status"] != "submitted":
        err(f"Timesheet is '{ts['status']}', can only bill from 'submitted'")

    total_billable_hours = to_decimal(ts["total_billable_hours"])
    total_billable_amount = to_decimal(ts["total_billable_amount"])
    total_cost = to_decimal(ts["total_cost"])

    # Mark as billed, set total_billed_hours.
    conn.execute(
        update_row("timesheet",
                   data={"status": P(), "total_billed_hours": P(), "updated_at": sql_now()},
                   where={"id": P()}),
        ("billed", str(total_billable_hours), args.timesheet_id),
    )

    # Aggregate amounts per project from detail rows and update each project
    dtl_q = Q.from_(_t_ts_detail).select(_t_ts_detail.star).where(_t_ts_detail.timesheet_id == P())
    details = conn.execute(dtl_q.get_sql(), (args.timesheet_id,)).fetchall()

    project_costs = {}  # project_id -> {cost, billed}
    for d in details:
        d_dict = row_to_dict(d)
        proj_id = d_dict["project_id"]
        hours = to_decimal(d_dict["hours"])
        rate = to_decimal(d_dict["billing_rate"])
        billable = int(d_dict["billable"])

        if proj_id not in project_costs:
            project_costs[proj_id] = {"cost": Decimal("0"), "billed": Decimal("0")}

        line_amount = round_currency(hours * rate)
        project_costs[proj_id]["cost"] += line_amount
        if billable:
            project_costs[proj_id]["billed"] += line_amount

    proj_cost_q = (Q.from_(_t_project)
                   .select(_t_project.actual_cost, _t_project.total_billed)
                   .where(_t_project.id == P()))
    for proj_id, amounts in project_costs.items():
        project = conn.execute(proj_cost_q.get_sql(), (proj_id,)).fetchone()
        if project:
            new_actual = round_currency(
                to_decimal(project["actual_cost"]) + amounts["cost"]
            )
            new_billed = round_currency(
                to_decimal(project["total_billed"]) + amounts["billed"]
            )
            # Recalculate profit margin
            if new_billed > 0:
                profit_margin = round_currency(
                    ((new_billed - new_actual) / new_billed) * Decimal("100")
                )
            else:
                profit_margin = Decimal("0")

            conn.execute(
                update_row("project",
                           data={"actual_cost": P(), "total_billed": P(),
                                 "profit_margin": P(), "updated_at": sql_now()},
                           where={"id": P()}),
                (str(new_actual), str(new_billed), str(profit_margin), proj_id),
            )

    audit(conn, "erpclaw-projects", "bill-timesheet", "timesheet", args.timesheet_id,
           old_values={"status": "submitted"},
           new_values={"status": "billed",
                       "total_billed_hours": str(total_billable_hours)},
           description="Timesheet billed")
    conn.commit()

    ts_refetch = Q.from_(_t_timesheet).select(_t_timesheet.star).where(_t_timesheet.id == P())
    ts = conn.execute(ts_refetch.get_sql(), (args.timesheet_id,)).fetchone()
    ok({"timesheet": row_to_dict(ts)})


# ---------------------------------------------------------------------------
# 14b. create-billing-from-timesheets (T&M billing pipeline)
# ---------------------------------------------------------------------------

def create_billing_from_timesheets(conn, args):
    """Create a Sales Invoice from unbilled timesheets for a T&M project.

    Required: --project-id, --company-id
    Optional: --customer-id, --from-date, --to-date

    If --customer-id is not provided, it is resolved from the project record.

    Gathers all submitted (unbilled) timesheet detail rows for the project,
    groups them by employee, creates a Sales Invoice via erpclaw-selling
    cross_skill, then marks the timesheets as billed with the invoice reference.
    """
    if not args.project_id:
        err("--project-id is required")
    if not args.company_id:
        err("--company-id is required")

    _validate_company_exists(conn, args.company_id)
    project = _validate_project_exists(conn, args.project_id)
    p = row_to_dict(project)

    # Resolve customer_id: explicit arg > project record > error
    customer_id = args.customer_id
    if not customer_id:
        customer_id = p.get("customer_id")
    if not customer_id:
        err("No customer found. Provide --customer-id or assign a customer to the project.")
    _validate_customer_exists(conn, customer_id)

    # Verify project is T&M (time_and_material)
    if p["billing_type"] != "time_and_material":
        err(f"Project billing type is '{p['billing_type']}', "
            "create-billing-from-timesheets only applies to 'time_and_material' projects")

    # Query unbilled timesheet details for this project
    # Unbilled = parent timesheet status is 'submitted' (not yet 'billed')
    td = Table("timesheet_detail")
    ts = Table("timesheet")
    e = Table("employee")

    params = [args.project_id, args.company_id]

    base_q = (Q.from_(td)
              .join(ts).on(td.timesheet_id == ts.id)
              .left_join(e).on(ts.employee_id == e.id)
              .where(td.project_id == P())
              .where(ts.company_id == P())
              .where(ts.status == ValueWrapper("submitted"))
              .where(td.billable == 1))

    if args.from_date:
        base_q = base_q.where(td.date >= P())
        params.append(args.from_date)

    if args.to_date:
        base_q = base_q.where(td.date <= P())
        params.append(args.to_date)

    detail_q = (base_q
                .select(
                    td.id, td.timesheet_id, td.project_id, td.task_id,
                    td.activity_type, td.hours, td.billing_rate,
                    td.description, td.date,
                    ts.employee_id,
                    e.full_name.as_("employee_name"),
                )
                .orderby(ts.employee_id).orderby(td.date))

    rows = conn.execute(detail_q.get_sql(), params).fetchall()

    if not rows:
        err("No unbilled billable timesheet entries found for this project",
            suggestion="Ensure timesheets are submitted (not draft) and have billable=1 detail rows.")

    # Group by employee to create invoice line items
    employee_groups = {}  # employee_id -> {name, hours, amount, detail_ids, timesheet_ids}
    all_timesheet_ids = set()

    for row in rows:
        r = row_to_dict(row)
        emp_id = r["employee_id"]
        emp_name = r["employee_name"] or emp_id
        hours = to_decimal(r["hours"])
        rate = to_decimal(r["billing_rate"])
        line_amount = round_currency(hours * rate)

        if emp_id not in employee_groups:
            employee_groups[emp_id] = {
                "employee_name": emp_name,
                "total_hours": Decimal("0"),
                "total_amount": Decimal("0"),
                "detail_ids": [],
                "timesheet_ids": set(),
                "activities": [],
            }

        grp = employee_groups[emp_id]
        grp["total_hours"] += hours
        grp["total_amount"] += line_amount
        grp["detail_ids"].append(r["id"])
        grp["timesheet_ids"].add(r["timesheet_id"])
        all_timesheet_ids.add(r["timesheet_id"])

        # Collect activity info for description
        activity = r["activity_type"] or "general"
        if activity not in grp["activities"]:
            grp["activities"].append(activity)

    # Build invoice items — one line per employee
    invoice_items = []
    grand_total = Decimal("0")

    for emp_id, grp in employee_groups.items():
        total_hours = round_currency(grp["total_hours"])
        total_amount = round_currency(grp["total_amount"])
        grand_total += total_amount

        # Calculate effective rate (weighted average)
        effective_rate = Decimal("0")
        if total_hours > 0:
            effective_rate = round_currency(total_amount / total_hours)

        activities_str = ", ".join(grp["activities"])
        description = (
            f"T&M: {grp['employee_name']} — "
            f"{total_hours}h ({activities_str}) — "
            f"Project: {p['project_name']}"
        )

        invoice_items.append({
            "description": description,
            "qty": str(total_hours),
            "rate": str(effective_rate),
        })

    if grand_total <= 0:
        err("Total billable amount is zero — nothing to invoice")

    # Create the Sales Invoice via cross_skill
    posting_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    db_path = getattr(args, "db_path", None)   # forwarded only when the caller gave --db-path

    try:
        inv_result = create_invoice(
            customer_id=customer_id,
            items=invoice_items,
            company_id=args.company_id,
            posting_date=posting_date,
            db_path=db_path,
        )
    except CrossSkillError as e:
        err(f"Failed to create invoice: {e}",
            suggestion="Ensure erpclaw-selling is installed and the customer exists.")

    invoice_id = inv_result.get("sales_invoice_id")
    invoice_name = inv_result.get("naming_series", "")
    if not invoice_id:
        err("Invoice was created but no ID returned — unexpected response from erpclaw-selling")

    # Mark all affected timesheets as billed with invoice reference
    for ts_id in all_timesheet_ids:
        conn.execute(
            update_row("timesheet",
                       data={"status": P(),
                             "total_billed_hours": Field("total_billable_hours"),
                             "sales_invoice_id": P(), "updated_at": sql_now()},
                       where={"id": P()}),
            ("billed", invoice_id, ts_id),
        )

    # Update project actual_cost and total_billed
    proj_q = (Q.from_(_t_project)
              .select(_t_project.actual_cost, _t_project.total_billed)
              .where(_t_project.id == P()))
    proj_row = conn.execute(proj_q.get_sql(), (args.project_id,)).fetchone()
    if proj_row:
        new_actual = round_currency(to_decimal(proj_row["actual_cost"]) + grand_total)
        new_billed = round_currency(to_decimal(proj_row["total_billed"]) + grand_total)
        profit_margin = Decimal("0")
        if new_billed > 0:
            profit_margin = round_currency(
                ((new_billed - new_actual) / new_billed) * Decimal("100")
            )
        conn.execute(
            update_row("project",
                       data={"actual_cost": P(), "total_billed": P(),
                             "profit_margin": P(), "updated_at": sql_now()},
                       where={"id": P()}),
            (str(new_actual), str(new_billed), str(profit_margin), args.project_id),
        )

    audit(conn, "erpclaw-projects", "create-billing-from-timesheets",
          "project", args.project_id,
          new_values={
              "invoice_id": invoice_id,
              "invoice_name": invoice_name,
              "total_amount": str(grand_total),
              "timesheets_billed": len(all_timesheet_ids),
              "employees": len(employee_groups),
          },
          description=f"T&M billing: created invoice {invoice_name} for {grand_total}")
    conn.commit()

    # Build response
    employee_summary = []
    for emp_id, grp in employee_groups.items():
        employee_summary.append({
            "employee_id": emp_id,
            "employee_name": grp["employee_name"],
            "hours": str(round_currency(grp["total_hours"])),
            "amount": str(round_currency(grp["total_amount"])),
            "timesheet_count": len(grp["timesheet_ids"]),
        })

    ok({
        "invoice_id": invoice_id,
        "invoice_name": invoice_name,
        "project_id": args.project_id,
        "project_name": p["project_name"],
        "customer_id": customer_id,
        "total_amount": str(grand_total),
        "timesheets_billed": len(all_timesheet_ids),
        "employees": employee_summary,
        "from_date": args.from_date,
        "to_date": args.to_date,
    })


# ---------------------------------------------------------------------------
# 15. project-profitability
# ---------------------------------------------------------------------------

def project_profitability(conn, args):
    """Project profitability report.

    Required: --project-id
    Returns estimated_cost, actual_cost, total_billed, profit, margin %,
    and breakdown by employee from timesheets.
    """
    if not args.project_id:
        err("--project-id is required")

    project = _validate_project_exists(conn, args.project_id)
    p = row_to_dict(project)

    estimated_cost = to_decimal(p["estimated_cost"])
    actual_cost = to_decimal(p["actual_cost"])
    total_billed = to_decimal(p["total_billed"])
    profit = round_currency(total_billed - actual_cost)
    margin = Decimal("0")
    if total_billed > 0:
        margin = round_currency((profit / total_billed) * Decimal("100"))

    # Employee breakdown from submitted/billed timesheets. Lines are read
    # as TEXT and aggregated in Python with Decimal (see get-project): SQL
    # arithmetic on hours/billing_rate is inexact on SQLite and refused on
    # PostgreSQL, and grouping only by employee id while selecting the name
    # is refused on PostgreSQL too.
    td = Table("timesheet_detail")
    ts = Table("timesheet")
    e = Table("employee")
    emp_q = (Q.from_(td)
             .join(ts).on(td.timesheet_id == ts.id)
             .left_join(e).on(ts.employee_id == e.id)
             .select(
                 ts.employee_id,
                 e.full_name.as_("employee_name"),
                 td.hours,
                 td.billing_rate,
                 td.billable,
             )
             .where(td.project_id == P())
             .where(ts.status.isin(["submitted", "billed"])))
    detail_rows = conn.execute(emp_q.get_sql(), (args.project_id,)).fetchall()

    by_employee = {}
    for line in detail_rows:
        eid = line["employee_id"]
        entry = by_employee.setdefault(eid, {
            "employee_id": eid,
            "employee_name": line["employee_name"],
            "total_hours": Decimal("0"),
            "billable_hours": Decimal("0"),
            "total_cost": Decimal("0"),
            "billable_amount": Decimal("0"),
        })
        hours = to_decimal(line["hours"])
        rate = to_decimal(line["billing_rate"])
        entry["total_hours"] += hours
        entry["total_cost"] += hours * rate
        if int(line["billable"]) == 1:
            entry["billable_hours"] += hours
            entry["billable_amount"] += hours * rate

    employees = []
    for entry in sorted(by_employee.values(),
                        key=lambda d: (-d["total_hours"], d["employee_id"])):
        employees.append({
            "employee_id": entry["employee_id"],
            "employee_name": entry["employee_name"],
            "total_hours": str(round_currency(entry["total_hours"])),
            "billable_hours": str(round_currency(entry["billable_hours"])),
            "total_cost": str(round_currency(entry["total_cost"])),
            "billable_amount": str(round_currency(entry["billable_amount"])),
        })

    # Cost variance
    cost_variance = round_currency(estimated_cost - actual_cost)

    ok({
        "project_id": args.project_id,
        "project_name": p["project_name"],
        "estimated_cost": str(estimated_cost),
        "actual_cost": str(actual_cost),
        "total_billed": str(total_billed),
        "profit": str(profit),
        "margin_percent": str(margin),
        "cost_variance": str(cost_variance),
        "employees": employees,
    })


# ---------------------------------------------------------------------------
# 16. gantt-data
# ---------------------------------------------------------------------------

def gantt_data(conn, args):
    """Return tasks with dependencies for Gantt chart rendering.

    Required: --project-id
    Returns list of tasks with id, name, start_date, end_date, depends_on,
    status, assigned_to.
    """
    if not args.project_id:
        err("--project-id is required")

    project = _validate_project_exists(conn, args.project_id)

    tk = _t_task
    gantt_q = (Q.from_(tk)
               .select(tk.id, tk.task_name, tk.start_date, tk.end_date,
                       tk.depends_on, tk.status, tk.assigned_to, tk.priority,
                       tk.estimated_hours, tk.actual_hours, tk.parent_task_id)
               .where(tk.project_id == P())
               .orderby(tk.start_date).orderby(tk.task_name))
    tasks = conn.execute(gantt_q.get_sql(), (args.project_id,)).fetchall()

    gantt_items = []
    for t in tasks:
        td = row_to_dict(t)
        depends = None
        if td["depends_on"]:
            try:
                depends = json.loads(td["depends_on"])
            except (json.JSONDecodeError, TypeError):
                depends = None

        gantt_items.append({
            "id": td["id"],
            "name": td["task_name"],
            "start_date": td["start_date"],
            "end_date": td["end_date"],
            "depends_on": depends,
            "status": td["status"],
            "assigned_to": td["assigned_to"],
            "priority": td["priority"],
            "estimated_hours": td["estimated_hours"],
            "actual_hours": td["actual_hours"],
            "parent_task_id": td["parent_task_id"],
        })

    # Include milestones as markers
    ms = _t_milestone
    ms_q = (Q.from_(ms)
            .select(ms.id, ms.milestone_name, ms.target_date,
                    ms.completion_date, ms.status)
            .where(ms.project_id == P())
            .orderby(ms.target_date))
    milestones = conn.execute(ms_q.get_sql(), (args.project_id,)).fetchall()

    milestone_items = []
    for m in milestones:
        md = row_to_dict(m)
        milestone_items.append({
            "id": md["id"],
            "name": md["milestone_name"],
            "target_date": md["target_date"],
            "completion_date": md["completion_date"],
            "status": md["status"],
        })

    ok({
        "project_id": args.project_id,
        "project_name": row_to_dict(project)["project_name"],
        "start_date": row_to_dict(project)["start_date"],
        "end_date": row_to_dict(project)["end_date"],
        "tasks": gantt_items,
        "milestones": milestone_items,
    })


# ---------------------------------------------------------------------------
# 17. resource-utilization
# ---------------------------------------------------------------------------

def resource_utilization(conn, args):
    """Resource utilization report — hours by employee across projects.

    Required: --company-id
    Optional: --from-date, --to-date
    Returns total hours, billable hours, utilization % per employee.
    """
    if not args.company_id:
        err("--company-id is required")

    _validate_company_exists(conn, args.company_id)

    td = Table("timesheet_detail")
    ts = Table("timesheet")
    e = Table("employee")
    params = [args.company_id]

    # Lines are read as TEXT and aggregated in Python with Decimal (see
    # get-project): SQL arithmetic on hours/billing_rate is inexact on
    # SQLite and refused on PostgreSQL, text ordering misranks employees,
    # and grouping only by employee id while selecting the name is refused
    # on PostgreSQL too.
    lines_q = (Q.from_(td)
               .join(ts).on(td.timesheet_id == ts.id)
               .left_join(e).on(ts.employee_id == e.id)
               .select(
                   ts.employee_id,
                   e.full_name.as_("employee_name"),
                   td.project_id,
                   td.hours,
                   td.billing_rate,
                   td.billable,
               )
               .where(ts.company_id == P())
               .where(ts.status.isin(["submitted", "billed"])))

    if args.from_date:
        lines_q = lines_q.where(td.date >= P())
        params.append(args.from_date)

    if args.to_date:
        lines_q = lines_q.where(td.date <= P())
        params.append(args.to_date)

    detail_rows = conn.execute(lines_q.get_sql(), params).fetchall()

    by_employee = {}
    for line in detail_rows:
        eid = line["employee_id"]
        entry = by_employee.setdefault(eid, {
            "employee_id": eid,
            "employee_name": line["employee_name"],
            "total_hours": Decimal("0"),
            "billable_hours": Decimal("0"),
            "non_billable_hours": Decimal("0"),
            "project_ids": set(),
            "billable_amount": Decimal("0"),
        })
        hours = to_decimal(line["hours"])
        rate = to_decimal(line["billing_rate"])
        entry["total_hours"] += hours
        entry["project_ids"].add(line["project_id"])
        if int(line["billable"]) == 1:
            entry["billable_hours"] += hours
            entry["billable_amount"] += hours * rate
        else:
            entry["non_billable_hours"] += hours

    employees = []
    for entry in sorted(by_employee.values(),
                        key=lambda d: (-d["total_hours"], d["employee_id"])):
        total_h = entry["total_hours"]
        billable_h = entry["billable_hours"]
        utilization = Decimal("0")
        if total_h > 0:
            utilization = round_currency((billable_h / total_h) * Decimal("100"))

        employees.append({
            "employee_id": entry["employee_id"],
            "employee_name": entry["employee_name"],
            "total_hours": str(round_currency(total_h)),
            "billable_hours": str(round_currency(billable_h)),
            "non_billable_hours": str(round_currency(entry["non_billable_hours"])),
            "utilization_percent": str(utilization),
            "project_count": len(entry["project_ids"]),
            "billable_amount": str(round_currency(entry["billable_amount"])),
        })

    # Summary totals
    total_hours_all = sum(to_decimal(e["total_hours"]) for e in employees)
    total_billable_all = sum(to_decimal(e["billable_hours"]) for e in employees)
    overall_utilization = Decimal("0")
    if total_hours_all > 0:
        overall_utilization = round_currency(
            (total_billable_all / total_hours_all) * Decimal("100")
        )

    ok({
        "company_id": args.company_id,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "employees": employees,
        "summary": {
            "total_employees": len(employees),
            "total_hours": str(round_currency(total_hours_all)),
            "total_billable_hours": str(round_currency(total_billable_all)),
            "overall_utilization_percent": str(overall_utilization),
        },
    })


# ---------------------------------------------------------------------------
# 18. status
# ---------------------------------------------------------------------------

def status_action(conn, args):
    """Dashboard status for a company.

    Required: --company-id
    Returns active projects count, overdue tasks, upcoming milestones,
    recent timesheets.
    """
    company_id = resolve_scope_company(conn, args.company_id, getattr(args, "company_name", None))

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Active projects (open or in_progress)
    p = _t_project
    active_q = (Q.from_(p).select(fn.Count("*", alias="cnt"))
                .where(p.company_id == P())
                .where(p.status.isin(["open", "in_progress"])))
    active_projects = conn.execute(active_q.get_sql(), (company_id,)).fetchone()

    # Total projects by status
    status_q = (Q.from_(p).select(p.status, fn.Count("*", alias="cnt"))
                .where(p.company_id == P())
                .groupby(p.status))
    project_by_status = conn.execute(status_q.get_sql(), (company_id,)).fetchall()
    status_counts = {r["status"]: r["cnt"] for r in project_by_status}

    # Overdue tasks: open/in_progress tasks with end_date < today
    tk = _t_task
    overdue_q = (Q.from_(tk)
                 .join(p).on(tk.project_id == p.id)
                 .select(tk.id, tk.task_name, tk.end_date, tk.assigned_to,
                         tk.status, tk.project_id, p.project_name)
                 .where(p.company_id == P())
                 .where(tk.status.isin(["open", "in_progress"]))
                 .where(tk.end_date.isnotnull())
                 .where(tk.end_date < P())
                 .orderby(tk.end_date)
                 .limit(20))
    overdue_tasks = conn.execute(overdue_q.get_sql(), (company_id, today)).fetchall()

    # Upcoming milestones: pending milestones within next 30 days. The window
    # end is rendered per dialect at call time.
    window_end = str(date_add_days("?", "30", "+"))
    upcoming_milestones = conn.execute(
        f"""SELECT m.id, m.milestone_name, m.target_date, m.status,
                  m.project_id, p.project_name
           FROM milestone m
           JOIN project p ON m.project_id = p.id
           WHERE p.company_id = ?
             AND m.status = 'pending'
             AND m.target_date >= ?
             AND m.target_date <= {window_end}
           ORDER BY m.target_date
           LIMIT 20""",
        (company_id, today, today),
    ).fetchall()

    # Missed milestones: pending milestones past target date
    ms = _t_milestone
    missed_q = (Q.from_(ms)
                .join(p).on(ms.project_id == p.id)
                .select(ms.id, ms.milestone_name, ms.target_date,
                        ms.project_id, p.project_name)
                .where(p.company_id == P())
                .where(ms.status == ValueWrapper("pending"))
                .where(ms.target_date < P())
                .orderby(ms.target_date)
                .limit(10))
    missed_milestones = conn.execute(missed_q.get_sql(), (company_id, today)).fetchall()

    # Recent timesheets (last 10)
    ts_t = _t_timesheet
    e = _t_employee
    recent_q = (Q.from_(ts_t)
                .left_join(e).on(ts_t.employee_id == e.id)
                .select(ts_t.id, ts_t.naming_series, ts_t.employee_id,
                        e.full_name.as_("employee_name"),
                        ts_t.start_date, ts_t.end_date, ts_t.total_hours,
                        ts_t.total_billable_amount, ts_t.status)
                .where(ts_t.company_id == P())
                .orderby(ts_t.created_at, order=Order.desc)
                .limit(10))
    recent_timesheets = conn.execute(recent_q.get_sql(), (company_id,)).fetchall()

    # Hours summary this month, aggregated in Python with Decimal (see
    # get-project): SQL arithmetic on hours is inexact on SQLite and
    # refused on PostgreSQL.
    month_start = datetime.now(timezone.utc).strftime("%Y-%m-01")
    td = _t_ts_detail
    hrs_q = (Q.from_(td)
             .join(ts_t).on(td.timesheet_id == ts_t.id)
             .select(td.hours, td.billable)
             .where(ts_t.company_id == P())
             .where(ts_t.status.isin(["submitted", "billed"]))
             .where(td.date >= P()))
    month_lines = conn.execute(hrs_q.get_sql(), (company_id, month_start)).fetchall()
    month_total = Decimal("0")
    month_billable = Decimal("0")
    for line in month_lines:
        hours = to_decimal(line["hours"])
        month_total += hours
        if int(line["billable"]) == 1:
            month_billable += hours
    hours_this_month = {"total_hours": month_total, "billable_hours": month_billable}

    ok({
        "company_id": company_id,
        "date": today,
        "active_projects": active_projects["cnt"],
        "projects_by_status": status_counts,
        "overdue_tasks": [row_to_dict(r) for r in overdue_tasks],
        "overdue_tasks_count": len(overdue_tasks),
        "upcoming_milestones": [row_to_dict(r) for r in upcoming_milestones],
        "missed_milestones": [row_to_dict(r) for r in missed_milestones],
        "recent_timesheets": [row_to_dict(r) for r in recent_timesheets],
        "hours_this_month": {
            "total": str(round_currency(to_decimal(hours_this_month["total_hours"]))),
            "billable": str(round_currency(to_decimal(hours_this_month["billable_hours"]))),
        },
    })


def _resolve_company_flag(conn, args):
    """Resolve --company (name or id) into args.company_id.

    main() calls this once before dispatch. An id passed through --company
    keeps working: a bound read checks whether the value is exactly an
    existing company.id first, and only otherwise resolves it as an exact
    company name (a miss refuses).
    """
    if getattr(args, "company_name", None) and not getattr(args, "company_id", None):
        probe = (Q.from_(_t_company).select(_t_company.id).where(_t_company.id == P()))
        if conn.execute(probe.get_sql(), (args.company_name,)).fetchone():
            args.company_id = args.company_name
            return
        args.company_id = resolve_company_id(conn, None, args.company_name)


# ---------------------------------------------------------------------------
# Action dispatch
# ---------------------------------------------------------------------------

ACTIONS = {
    "add-project": add_project,
    "update-project": update_project,
    "get-project": get_project,
    "list-projects": list_projects,
    "add-task": add_task,
    "update-task": update_task,
    "list-tasks": list_tasks,
    "add-milestone": add_milestone,
    "update-milestone": update_milestone,
    "add-timesheet": add_timesheet,
    "get-timesheet": get_timesheet,
    "list-timesheets": list_timesheets,
    "submit-timesheet": submit_timesheet,
    "bill-timesheet": bill_timesheet,
    "create-billing-from-timesheets": create_billing_from_timesheets,
    "project-profitability": project_profitability,
    "gantt-data": gantt_data,
    "resource-utilization": resource_utilization,
    "status": status_action,
}


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def main():
    parser = SafeArgumentParser(description="ERPClaw Projects Skill")
    parser.add_argument("--action", required=True, choices=sorted(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # Entity IDs
    parser.add_argument("--company-id")
    parser.add_argument("--company", dest="company_name", default=None)
    parser.add_argument("--project-id")
    parser.add_argument("--task-id")
    parser.add_argument("--milestone-id")
    parser.add_argument("--timesheet-id")
    parser.add_argument("--employee-id")
    parser.add_argument("--customer-id")
    parser.add_argument("--cost-center-id")

    # Common fields
    parser.add_argument("--name")
    parser.add_argument("--description")

    # Project fields
    parser.add_argument("--project-type")
    parser.add_argument("--billing-type")
    parser.add_argument("--estimated-cost")
    parser.add_argument("--actual-cost")
    parser.add_argument("--total-billed")
    parser.add_argument("--percent-complete")

    # Task fields
    parser.add_argument("--assigned-to")
    parser.add_argument("--estimated-hours")
    parser.add_argument("--actual-hours")
    parser.add_argument("--depends-on")
    parser.add_argument("--parent-task-id")

    # Milestone fields
    parser.add_argument("--target-date")
    parser.add_argument("--completion-date")

    # Timesheet fields
    parser.add_argument("--items")  # JSON array

    # Dates
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")

    # Filters
    parser.add_argument("--status")
    parser.add_argument("--priority")
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--search")
    parser.add_argument("--limit", default="20")
    parser.add_argument("--offset", default="0")

    args, unknown = parser.parse_known_args()
    check_unknown_args(parser, unknown)
    check_input_lengths(args)

    db_path = getattr(args, "db_path", None)   # None unless --db-path was given
    conn = get_connection(db_path)

    # Dependency check
    _dep = check_required_tables(conn, REQUIRED_TABLES)
    if _dep:
        _dep["suggestion"] = "clawhub install " + " ".join(_dep.get("missing_skills", []))
        print(json.dumps(_dep, indent=2))
        conn.close()
        sys.exit(1)

    _resolve_company_flag(conn, args)

    try:
        ACTIONS[args.action](conn, args)
    except Exception as e:
        conn.rollback()
        sys.stderr.write(f"[erpclaw-projects] {e}\n")
        err(unexpected_error_message(e))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
