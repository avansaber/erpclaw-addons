"""ERPClaw Documents -- Templates domain module.

Document template CRUD, generation from templates.
6 actions exported via ACTIONS dict.
"""
import os
import re
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, insert_row, update_row

SKILL = "erpclaw-documents"

VALID_TEMPLATE_TYPES = (
    "general", "contract", "invoice", "letter", "report", "certificate", "other",
)
VALID_TEMPLATE_FORMATS = ("text", "markdown", "html")
VALID_TEMPLATE_ENGINES = ("legacy_replace", "jinja2")


# ---------------------------------------------------------------------------
# Render engines
# ---------------------------------------------------------------------------
def _render_legacy_replace(content, data):
    """The original naive merge engine: substitute every ``{{key}}`` with its
    string value. Preserved verbatim for backward-compatibility — a template
    stored with engine='legacy_replace' must render byte-identically to how it
    rendered before the S8 engine columns were added. Unmatched placeholders are
    left in place, exactly as before."""
    for key, value in data.items():
        content = content.replace("{{" + key + "}}", str(value))
    return content


def _render_jinja2(content, data, fmt):
    """Render ``content`` via a SANDBOXED Jinja2 environment.

    autoescape is enabled only for ``html`` output (XSS hardening for
    user-authored HTML templates); text/markdown render raw. The sandbox blocks
    access to ``__class__``/``import`` and other unsafe attribute traversal.
    Raises ``jinja2.TemplateError`` (or a sandbox SecurityError) on bad
    templates; callers convert that to an err() response.
    """
    from jinja2.sandbox import SandboxedEnvironment
    env = SandboxedEnvironment(autoescape=(fmt == "html"))
    template = env.from_string(content)
    # Pass the merge map positionally so arbitrary (non-identifier) JSON keys
    # are accepted; Jinja2's render does dict(*args, **kwargs) internally.
    return template.render(data)


def _row_get(row, key, default=None):
    """Read a column from a sqlite3.Row/dict, tolerating its absence.

    Defensive for a DB that predates the S8 engine columns (migration 005 not
    yet applied): such a row has no 'engine'/'format' key, so we fall back to the
    backward-compatible defaults rather than raising."""
    try:
        val = row[key]
    except (IndexError, KeyError):
        return default
    return default if val is None else val


def _parse_merge_data(merge_data):
    """Parse the --merge-data JSON object into a dict. err() on bad input."""
    if not merge_data:
        return {}
    import json
    try:
        data = json.loads(merge_data)
    except (json.JSONDecodeError, TypeError):
        err("--merge-data must be valid JSON")
    if not isinstance(data, dict):
        err("--merge-data must be a JSON object (key/value map)")
    return data


# ---------------------------------------------------------------------------
# add-template
# ---------------------------------------------------------------------------
def add_template(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "name", None):
        err("--name is required")
    if not getattr(args, "content", None):
        err("--content is required")

    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    template_type = getattr(args, "template_type", None) or "general"
    if template_type not in VALID_TEMPLATE_TYPES:
        err(f"Invalid template-type: {template_type}")

    fmt = getattr(args, "format", None) or "text"
    if fmt not in VALID_TEMPLATE_FORMATS:
        err(f"Invalid format: {fmt} (expected one of {', '.join(VALID_TEMPLATE_FORMATS)})")
    engine = getattr(args, "engine", None) or "legacy_replace"
    if engine not in VALID_TEMPLATE_ENGINES:
        err(f"Invalid engine: {engine} (expected one of {', '.join(VALID_TEMPLATE_ENGINES)})")

    tpl_id = str(uuid.uuid4())
    ns = get_next_name(conn, "document_template", company_id=args.company_id)

    conn.execute(
        """INSERT INTO document_template
           (id, naming_series, name, template_type, content, format, engine,
            merge_fields, description, is_active, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,1,?)""",
        (
            tpl_id, ns, args.name, template_type,
            args.content, fmt, engine,
            getattr(args, "merge_fields", None),
            getattr(args, "description", None),
            args.company_id,
        ),
    )
    audit(conn, SKILL, "document-add-template", "document_template", tpl_id,
          new_values={"name": args.name, "naming_series": ns,
                      "format": fmt, "engine": engine})
    conn.commit()
    ok({"template_id": tpl_id, "naming_series": ns, "template_type": template_type,
        "format": fmt, "engine": engine})


# ---------------------------------------------------------------------------
# update-template
# ---------------------------------------------------------------------------
def update_template(conn, args):
    tpl_id = getattr(args, "template_id", None)
    if not tpl_id:
        err("--template-id is required")

    row = conn.execute(Q.from_(Table("document_template")).select(Table("document_template").star).where(Field("id") == P()).get_sql(), (tpl_id,)).fetchone()
    if not row:
        err(f"Template {tpl_id} not found")

    updates, params, changed = [], [], []

    for field, attr in [
        ("name", "name"),
        ("content", "content"),
        ("merge_fields", "merge_fields"),
        ("description", "description"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            updates.append(f"{field} = ?")
            params.append(val)
            changed.append(field)

    tt = getattr(args, "template_type", None)
    if tt is not None:
        if tt not in VALID_TEMPLATE_TYPES:
            err(f"Invalid template-type: {tt}")
        updates.append("template_type = ?")
        params.append(tt)
        changed.append("template_type")

    fmt = getattr(args, "format", None)
    if fmt is not None:
        if fmt not in VALID_TEMPLATE_FORMATS:
            err(f"Invalid format: {fmt} (expected one of {', '.join(VALID_TEMPLATE_FORMATS)})")
        updates.append("format = ?")
        params.append(fmt)
        changed.append("format")

    engine = getattr(args, "engine", None)
    if engine is not None:
        if engine not in VALID_TEMPLATE_ENGINES:
            err(f"Invalid engine: {engine} (expected one of {', '.join(VALID_TEMPLATE_ENGINES)})")
        updates.append("engine = ?")
        params.append(engine)
        changed.append("engine")

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        updates.append("is_active = ?")
        params.append(int(is_active))
        changed.append("is_active")

    if not changed:
        err("No fields to update")

    from datetime import datetime, timezone
    updates.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
    params.append(tpl_id)
    conn.execute(
        f"UPDATE document_template SET {', '.join(updates)} WHERE id = ?", params
    )
    audit(conn, SKILL, "document-update-template", "document_template", tpl_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"template_id": tpl_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# get-template
# ---------------------------------------------------------------------------
def get_template(conn, args):
    tpl_id = getattr(args, "template_id", None)
    if not tpl_id:
        err("--template-id is required")

    row = conn.execute(Q.from_(Table("document_template")).select(Table("document_template").star).where(Field("id") == P()).get_sql(), (tpl_id,)).fetchone()
    if not row:
        err(f"Template {tpl_id} not found")

    data = row_to_dict(row)
    ok(data)


# ---------------------------------------------------------------------------
# list-templates
# ---------------------------------------------------------------------------
def list_templates(conn, args):
    conditions, params = [], []
    company_id = getattr(args, "company_id", None)
    if company_id:
        conditions.append("company_id = ?")
        params.append(company_id)
    template_type = getattr(args, "template_type", None)
    if template_type:
        conditions.append("template_type = ?")
        params.append(template_type)
    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        conditions.append("is_active = ?")
        params.append(int(is_active))
    search = getattr(args, "search", None)
    if search:
        conditions.append("(LOWER(name) LIKE LOWER(?) OR LOWER(description) LIKE LOWER(?))")
        params.extend([f"%{search}%", f"%{search}%"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    total = conn.execute(
        f"SELECT COUNT(*) as cnt FROM document_template {where}", params
    ).fetchone()["cnt"]

    rows = conn.execute(
        f"SELECT * FROM document_template {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    templates = [row_to_dict(r) for r in rows]
    ok({"templates": templates, "total_count": total, "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# generate-from-template
# ---------------------------------------------------------------------------
def generate_from_template(conn, args):
    tpl_id = getattr(args, "template_id", None)
    if not tpl_id:
        err("--template-id is required")
    if not getattr(args, "title", None):
        err("--title is required for the generated document")
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    row = conn.execute(Q.from_(Table("document_template")).select(Table("document_template").star).where(Field("id") == P()).get_sql(), (tpl_id,)).fetchone()
    if not row:
        err(f"Template {tpl_id} not found")
    if not row["is_active"]:
        err(f"Template {tpl_id} is inactive")

    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    # Perform merge field substitution. Branch on the template's engine:
    # legacy_replace preserves the original str.replace behavior EXACTLY; jinja2
    # opts into the sandboxed Jinja2 render. Existing templates default to
    # legacy_replace (set by the schema), so no behavior change for them.
    content = row["content"]
    engine = (_row_get(row, "engine") or "legacy_replace")
    fmt = (_row_get(row, "format") or "text")
    merge_data = getattr(args, "merge_data", None)
    if merge_data:
        data = _parse_merge_data(merge_data)
        if engine == "jinja2":
            try:
                content = _render_jinja2(content, data, fmt)
            except Exception as e:  # noqa: BLE001 — surface as a clean err()
                err(f"Template render failed: {e}")
        else:
            content = _render_legacy_replace(content, data)

    # Determine document type from template type
    tpl_type = row["template_type"]
    doc_type = tpl_type if tpl_type in (
        "general", "contract", "invoice", "report", "certificate", "other"
    ) else "general"

    # Create document
    doc_id = str(uuid.uuid4())
    ns = get_next_name(conn, "document", company_id=args.company_id)

    conn.execute(
        """INSERT INTO document
           (id, naming_series, title, document_type, content, current_version,
            owner, is_archived, status, company_id)
           VALUES (?,?,?,?,?,?,?,0,?,?)""",
        (
            doc_id, ns, args.title, doc_type, content, "1",
            getattr(args, "owner", None),
            "draft",
            args.company_id,
        ),
    )

    # Create initial version
    version_id = str(uuid.uuid4())
    sql, _ = insert_row("document_version", {"id": P(), "document_id": P(), "version_number": P(), "content": P(), "change_notes": P(), "created_by": P()})
    conn.execute(sql,
        (
            version_id, doc_id, "1", content,
            f"Generated from template {row['name']}",
            getattr(args, "owner", None),
        ),
    )

    audit(conn, SKILL, "document-generate-from-template", "document", doc_id,
          new_values={"template_id": tpl_id, "template_name": row["name"],
                      "naming_series": ns})
    conn.commit()
    ok({
        "document_id": doc_id, "naming_series": ns, "doc_status": "draft",
        "template_id": tpl_id, "template_name": row["name"],
    })


# ---------------------------------------------------------------------------
# render-template
# ---------------------------------------------------------------------------
def render_template(conn, args):
    """Render a template's body via SANDBOXED Jinja2 and return the string.

    Pure render: validates the template exists, parses --merge-data, picks the
    output format (--format override, else the template's stored format), and
    renders with jinja2.sandbox.SandboxedEnvironment (autoescape ON for html).
    Does NOT create a document or emit a PDF — PDF is a separate later chunk.
    """
    tpl_id = getattr(args, "template_id", None)
    if not tpl_id:
        err("--template-id is required")

    row = conn.execute(
        Q.from_(Table("document_template")).select(Table("document_template").star)
        .where(Field("id") == P()).get_sql(),
        (tpl_id,),
    ).fetchone()
    if not row:
        err(f"Template {tpl_id} not found")

    fmt = getattr(args, "format", None) or _row_get(row, "format") or "text"
    if fmt not in VALID_TEMPLATE_FORMATS:
        err(f"Invalid format: {fmt} (expected one of {', '.join(VALID_TEMPLATE_FORMATS)})")

    data = _parse_merge_data(getattr(args, "merge_data", None))

    try:
        rendered = _render_jinja2(row["content"], data, fmt)
    except Exception as e:  # noqa: BLE001 — surface as a clean err()
        err(f"Template render failed: {e}")

    ok({
        "template_id": tpl_id,
        "template_name": row["name"],
        "format": fmt,
        "engine": "jinja2",
        "rendered": rendered,
    })


# ---------------------------------------------------------------------------
# status (module status)
# ---------------------------------------------------------------------------
def module_status(conn, args):
    ok({
        "skill": SKILL,
        "version": "1.1.0",
        "actions_available": 32,
        "domains": ["documents", "templates", "pdf", "print", "wrappers"],
        "tables": [
            "document", "document_version", "document_tag",
            "document_link", "document_template",
        ],
    })


# ---------------------------------------------------------------------------
# ACTIONS registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "document-add-template": add_template,
    "document-update-template": update_template,
    "document-get-template": get_template,
    "document-list-templates": list_templates,
    "document-generate-from-template": generate_from_template,
    "document-render-template": render_template,
    "status": module_status,
}
