"""ERPClaw Documents -- Templates domain module.

Document template CRUD, generation from templates.
6 actions exported via ACTIONS dict.
"""
import os
import re
import sys
import uuid

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit

SKILL = "erpclaw-documents"

VALID_TEMPLATE_TYPES = (
    "general", "contract", "invoice", "letter", "report", "certificate", "other",
)


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

    if not conn.execute(
        "SELECT id FROM company WHERE id = ?", (args.company_id,)
    ).fetchone():
        err(f"Company {args.company_id} not found")

    template_type = getattr(args, "template_type", None) or "general"
    if template_type not in VALID_TEMPLATE_TYPES:
        err(f"Invalid template-type: {template_type}")

    tpl_id = str(uuid.uuid4())
    ns = get_next_name(conn, "document_template", company_id=args.company_id)

    conn.execute(
        """INSERT INTO document_template
           (id, naming_series, name, template_type, content, merge_fields,
            description, is_active, company_id)
           VALUES (?,?,?,?,?,?,?,1,?)""",
        (
            tpl_id, ns, args.name, template_type,
            args.content,
            getattr(args, "merge_fields", None),
            getattr(args, "description", None),
            args.company_id,
        ),
    )
    audit(conn, SKILL, "document-add-template", "document_template", tpl_id,
          new_values={"name": args.name, "naming_series": ns})
    conn.commit()
    ok({"template_id": tpl_id, "naming_series": ns, "template_type": template_type})


# ---------------------------------------------------------------------------
# update-template
# ---------------------------------------------------------------------------
def update_template(conn, args):
    tpl_id = getattr(args, "template_id", None)
    if not tpl_id:
        err("--template-id is required")

    row = conn.execute(
        "SELECT * FROM document_template WHERE id = ?", (tpl_id,)
    ).fetchone()
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

    is_active = getattr(args, "is_active", None)
    if is_active is not None:
        updates.append("is_active = ?")
        params.append(int(is_active))
        changed.append("is_active")

    if not changed:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
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

    row = conn.execute(
        "SELECT * FROM document_template WHERE id = ?", (tpl_id,)
    ).fetchone()
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
        conditions.append("(name LIKE ? OR description LIKE ?)")
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

    row = conn.execute(
        "SELECT * FROM document_template WHERE id = ?", (tpl_id,)
    ).fetchone()
    if not row:
        err(f"Template {tpl_id} not found")
    if not row["is_active"]:
        err(f"Template {tpl_id} is inactive")

    if not conn.execute(
        "SELECT id FROM company WHERE id = ?", (args.company_id,)
    ).fetchone():
        err(f"Company {args.company_id} not found")

    # Perform merge field substitution
    content = row["content"]
    merge_data = getattr(args, "merge_data", None)
    if merge_data:
        import json
        try:
            fields = json.loads(merge_data)
        except (json.JSONDecodeError, TypeError):
            err("--merge-data must be valid JSON")
        for key, value in fields.items():
            content = content.replace("{{" + key + "}}", str(value))

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
    conn.execute(
        """INSERT INTO document_version
           (id, document_id, version_number, content, change_notes, created_by)
           VALUES (?,?,?,?,?,?)""",
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
# status (module status)
# ---------------------------------------------------------------------------
def module_status(conn, args):
    ok({
        "skill": SKILL,
        "version": "1.0.0",
        "actions_available": 25,
        "domains": ["documents", "templates"],
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
    "status": module_status,
}
