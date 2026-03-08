"""ERPClaw Documents -- Documents domain module.

Document CRUD, versioning, tagging, linking, status transitions, search, retention.
19 actions exported via ACTIONS dict.
"""
import os
import sys
import uuid

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err, row_to_dict
from erpclaw_lib.audit import audit

SKILL = "erpclaw-documents"

VALID_DOC_TYPES = (
    "general", "contract", "policy", "report", "invoice",
    "receipt", "certificate", "specification", "manual", "other",
)
VALID_DOC_STATUSES = ("draft", "review", "approved", "published", "archived", "on_hold")
VALID_LINK_TYPES = ("attachment", "reference", "supporting", "supersedes")


# ---------------------------------------------------------------------------
# add-document
# ---------------------------------------------------------------------------
def add_document(conn, args):
    if not getattr(args, "company_id", None):
        err("--company-id is required")
    if not getattr(args, "title", None):
        err("--title is required")

    if not conn.execute(
        "SELECT id FROM company WHERE id = ?", (args.company_id,)
    ).fetchone():
        err(f"Company {args.company_id} not found")

    doc_type = getattr(args, "document_type", None) or "general"
    if doc_type not in VALID_DOC_TYPES:
        err(f"Invalid document-type: {doc_type}")

    doc_id = str(uuid.uuid4())
    ns = get_next_name(conn, "document", company_id=args.company_id)

    file_size = getattr(args, "file_size", None)
    if file_size is not None:
        file_size = int(file_size)

    conn.execute(
        """INSERT INTO document
           (id, naming_series, title, document_type, file_name, file_path,
            file_size, mime_type, content, current_version, tags,
            linked_entity_type, linked_entity_id, owner, retention_date,
            is_archived, status, company_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)""",
        (
            doc_id, ns, args.title, doc_type,
            getattr(args, "file_name", None),
            getattr(args, "file_path", None),
            file_size,
            getattr(args, "mime_type", None),
            getattr(args, "content", None),
            "1",
            getattr(args, "tags", None),
            getattr(args, "linked_entity_type", None),
            getattr(args, "linked_entity_id", None),
            getattr(args, "owner", None),
            getattr(args, "retention_date", None),
            "draft",
            args.company_id,
        ),
    )

    # Create initial version record
    version_id = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO document_version
           (id, document_id, version_number, file_name, file_path, content,
            change_notes, created_by)
           VALUES (?,?,?,?,?,?,?,?)""",
        (
            version_id, doc_id, "1",
            getattr(args, "file_name", None),
            getattr(args, "file_path", None),
            getattr(args, "content", None),
            "Initial version",
            getattr(args, "owner", None),
        ),
    )

    # Auto-create tags if provided
    tags_str = getattr(args, "tags", None)
    if tags_str:
        for tag in [t.strip() for t in tags_str.split(",") if t.strip()]:
            tag_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO document_tag (id, document_id, tag) VALUES (?,?,?)",
                (tag_id, doc_id, tag),
            )

    audit(conn, SKILL, "document-add-document", "document", doc_id,
          new_values={"title": args.title, "naming_series": ns})
    conn.commit()
    ok({"document_id": doc_id, "naming_series": ns, "doc_status": "draft"})


# ---------------------------------------------------------------------------
# update-document
# ---------------------------------------------------------------------------
def update_document(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")
    row = conn.execute(
        "SELECT * FROM document WHERE id = ?", (doc_id,)
    ).fetchone()
    if not row:
        err(f"Document {doc_id} not found")

    updates, params, changed = [], [], []

    for field, attr in [
        ("title", "title"),
        ("file_name", "file_name"),
        ("file_path", "file_path"),
        ("mime_type", "mime_type"),
        ("content", "content"),
        ("owner", "owner"),
        ("tags", "tags"),
    ]:
        val = getattr(args, attr, None)
        if val is not None:
            updates.append(f"{field} = ?")
            params.append(val)
            changed.append(field)

    dt = getattr(args, "document_type", None)
    if dt is not None:
        if dt not in VALID_DOC_TYPES:
            err(f"Invalid document-type: {dt}")
        updates.append("document_type = ?")
        params.append(dt)
        changed.append("document_type")

    fs = getattr(args, "file_size", None)
    if fs is not None:
        updates.append("file_size = ?")
        params.append(int(fs))
        changed.append("file_size")

    if not changed:
        err("No fields to update")

    updates.append("updated_at = datetime('now')")
    params.append(doc_id)
    conn.execute(
        f"UPDATE document SET {', '.join(updates)} WHERE id = ?", params
    )
    audit(conn, SKILL, "document-update-document", "document", doc_id,
          new_values={"updated_fields": changed})
    conn.commit()
    ok({"document_id": doc_id, "updated_fields": changed})


# ---------------------------------------------------------------------------
# get-document
# ---------------------------------------------------------------------------
def get_document(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")
    row = conn.execute(
        "SELECT * FROM document WHERE id = ?", (doc_id,)
    ).fetchone()
    if not row:
        err(f"Document {doc_id} not found")

    data = row_to_dict(row)
    data["doc_status"] = data.pop("status", "draft")

    # Get version count
    ver_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM document_version WHERE document_id = ?",
        (doc_id,),
    ).fetchone()
    data["version_count"] = ver_count["cnt"] if ver_count else 0

    # Get tags
    tag_rows = conn.execute(
        "SELECT tag FROM document_tag WHERE document_id = ?", (doc_id,)
    ).fetchall()
    data["tag_list"] = [r["tag"] for r in tag_rows]

    # Get links
    link_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM document_link WHERE document_id = ?",
        (doc_id,),
    ).fetchone()
    data["link_count"] = link_count["cnt"] if link_count else 0

    ok(data)


# ---------------------------------------------------------------------------
# list-documents
# ---------------------------------------------------------------------------
def list_documents(conn, args):
    conditions, params = [], []
    company_id = getattr(args, "company_id", None)
    if company_id:
        conditions.append("company_id = ?")
        params.append(company_id)
    status = getattr(args, "status", None)
    if status:
        conditions.append("status = ?")
        params.append(status)
    doc_type = getattr(args, "document_type", None)
    if doc_type:
        conditions.append("document_type = ?")
        params.append(doc_type)
    owner = getattr(args, "owner", None)
    if owner:
        conditions.append("owner = ?")
        params.append(owner)
    search = getattr(args, "search", None)
    if search:
        conditions.append("(title LIKE ? OR content LIKE ? OR tags LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    total = conn.execute(
        f"SELECT COUNT(*) as cnt FROM document {where}", params
    ).fetchone()["cnt"]

    rows = conn.execute(
        f"SELECT * FROM document {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    documents = []
    for r in rows:
        d = row_to_dict(r)
        d["doc_status"] = d.pop("status", "draft")
        documents.append(d)

    ok({"documents": documents, "total_count": total, "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# add-document-version
# ---------------------------------------------------------------------------
def add_document_version(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")

    row = conn.execute(
        "SELECT * FROM document WHERE id = ?", (doc_id,)
    ).fetchone()
    if not row:
        err(f"Document {doc_id} not found")

    # Calculate next version number
    current = row["current_version"]
    try:
        next_ver = str(int(current) + 1)
    except ValueError:
        next_ver = current + ".1"

    version_number = getattr(args, "version_number", None) or next_ver

    version_id = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO document_version
           (id, document_id, version_number, file_name, file_path, content,
            change_notes, created_by)
           VALUES (?,?,?,?,?,?,?,?)""",
        (
            version_id, doc_id, version_number,
            getattr(args, "file_name", None),
            getattr(args, "file_path", None),
            getattr(args, "content", None),
            getattr(args, "change_notes", None),
            getattr(args, "created_by", None),
        ),
    )

    # Update document's current version
    conn.execute(
        "UPDATE document SET current_version = ?, updated_at = datetime('now') WHERE id = ?",
        (version_number, doc_id),
    )

    audit(conn, SKILL, "document-add-document-version", "document_version", version_id,
          new_values={"document_id": doc_id, "version_number": version_number})
    conn.commit()
    ok({"version_id": version_id, "document_id": doc_id, "version_number": version_number})


# ---------------------------------------------------------------------------
# list-document-versions
# ---------------------------------------------------------------------------
def list_document_versions(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")

    if not conn.execute(
        "SELECT id FROM document WHERE id = ?", (doc_id,)
    ).fetchone():
        err(f"Document {doc_id} not found")

    rows = conn.execute(
        "SELECT * FROM document_version WHERE document_id = ? ORDER BY created_at DESC",
        (doc_id,),
    ).fetchall()

    versions = [row_to_dict(r) for r in rows]
    ok({"versions": versions, "count": len(versions), "document_id": doc_id})


# ---------------------------------------------------------------------------
# add-document-tag
# ---------------------------------------------------------------------------
def add_document_tag(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")
    tag = getattr(args, "tag", None)
    if not tag:
        err("--tag is required")

    if not conn.execute(
        "SELECT id FROM document WHERE id = ?", (doc_id,)
    ).fetchone():
        err(f"Document {doc_id} not found")

    # Check for duplicate tag
    existing = conn.execute(
        "SELECT id FROM document_tag WHERE document_id = ? AND tag = ?",
        (doc_id, tag),
    ).fetchone()
    if existing:
        err(f"Tag '{tag}' already exists on document {doc_id}")

    tag_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO document_tag (id, document_id, tag) VALUES (?,?,?)",
        (tag_id, doc_id, tag),
    )
    audit(conn, SKILL, "document-add-document-tag", "document_tag", tag_id,
          new_values={"document_id": doc_id, "tag": tag})
    conn.commit()
    ok({"tag_id": tag_id, "document_id": doc_id, "tag": tag})


# ---------------------------------------------------------------------------
# remove-document-tag
# ---------------------------------------------------------------------------
def remove_document_tag(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")
    tag = getattr(args, "tag", None)
    if not tag:
        err("--tag is required")

    row = conn.execute(
        "SELECT id FROM document_tag WHERE document_id = ? AND tag = ?",
        (doc_id, tag),
    ).fetchone()
    if not row:
        err(f"Tag '{tag}' not found on document {doc_id}")

    conn.execute(
        "DELETE FROM document_tag WHERE document_id = ? AND tag = ?",
        (doc_id, tag),
    )
    audit(conn, SKILL, "document-remove-document-tag", "document_tag", row["id"],
          new_values={"document_id": doc_id, "tag": tag, "action": "removed"})
    conn.commit()
    ok({"document_id": doc_id, "tag": tag, "removed": True})


# ---------------------------------------------------------------------------
# list-document-tags
# ---------------------------------------------------------------------------
def list_document_tags(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")

    if not conn.execute(
        "SELECT id FROM document WHERE id = ?", (doc_id,)
    ).fetchone():
        err(f"Document {doc_id} not found")

    rows = conn.execute(
        "SELECT * FROM document_tag WHERE document_id = ? ORDER BY tag",
        (doc_id,),
    ).fetchall()

    tags = [row_to_dict(r) for r in rows]
    ok({"tags": tags, "count": len(tags), "document_id": doc_id})


# ---------------------------------------------------------------------------
# link-document
# ---------------------------------------------------------------------------
def link_document(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")
    if not getattr(args, "linked_entity_type", None):
        err("--linked-entity-type is required")
    if not getattr(args, "linked_entity_id", None):
        err("--linked-entity-id is required")
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    if not conn.execute(
        "SELECT id FROM document WHERE id = ?", (doc_id,)
    ).fetchone():
        err(f"Document {doc_id} not found")

    if not conn.execute(
        "SELECT id FROM company WHERE id = ?", (args.company_id,)
    ).fetchone():
        err(f"Company {args.company_id} not found")

    link_type = getattr(args, "link_type", None) or "attachment"
    if link_type not in VALID_LINK_TYPES:
        err(f"Invalid link-type: {link_type}")

    link_id = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO document_link
           (id, document_id, linked_entity_type, linked_entity_id,
            link_type, notes, company_id)
           VALUES (?,?,?,?,?,?,?)""",
        (
            link_id, doc_id,
            args.linked_entity_type,
            args.linked_entity_id,
            link_type,
            getattr(args, "notes", None),
            args.company_id,
        ),
    )
    audit(conn, SKILL, "document-link-document", "document_link", link_id,
          new_values={"document_id": doc_id, "entity_type": args.linked_entity_type,
                      "entity_id": args.linked_entity_id})
    conn.commit()
    ok({
        "link_id": link_id, "document_id": doc_id,
        "linked_entity_type": args.linked_entity_type,
        "linked_entity_id": args.linked_entity_id,
        "link_type": link_type,
    })


# ---------------------------------------------------------------------------
# unlink-document
# ---------------------------------------------------------------------------
def unlink_document(conn, args):
    link_id = getattr(args, "link_id", None)
    if not link_id:
        err("--link-id is required")

    row = conn.execute(
        "SELECT * FROM document_link WHERE id = ?", (link_id,)
    ).fetchone()
    if not row:
        err(f"Document link {link_id} not found")

    conn.execute("DELETE FROM document_link WHERE id = ?", (link_id,))
    audit(conn, SKILL, "document-unlink-document", "document_link", link_id,
          new_values={"removed": True})
    conn.commit()
    ok({"link_id": link_id, "removed": True})


# ---------------------------------------------------------------------------
# list-document-links
# ---------------------------------------------------------------------------
def list_document_links(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")

    if not conn.execute(
        "SELECT id FROM document WHERE id = ?", (doc_id,)
    ).fetchone():
        err(f"Document {doc_id} not found")

    rows = conn.execute(
        "SELECT * FROM document_link WHERE document_id = ? ORDER BY created_at DESC",
        (doc_id,),
    ).fetchall()

    links = [row_to_dict(r) for r in rows]
    ok({"links": links, "count": len(links), "document_id": doc_id})


# ---------------------------------------------------------------------------
# list-linked-documents
# ---------------------------------------------------------------------------
def list_linked_documents(conn, args):
    entity_type = getattr(args, "linked_entity_type", None)
    entity_id = getattr(args, "linked_entity_id", None)
    if not entity_type:
        err("--linked-entity-type is required")
    if not entity_id:
        err("--linked-entity-id is required")

    rows = conn.execute(
        """SELECT dl.*, d.title, d.document_type, d.status as doc_status,
                  d.naming_series, d.current_version
           FROM document_link dl
           JOIN document d ON d.id = dl.document_id
           WHERE dl.linked_entity_type = ? AND dl.linked_entity_id = ?
           ORDER BY dl.created_at DESC""",
        (entity_type, entity_id),
    ).fetchall()

    documents = [row_to_dict(r) for r in rows]
    ok({"documents": documents, "count": len(documents),
        "entity_type": entity_type, "entity_id": entity_id})


# ---------------------------------------------------------------------------
# submit-for-review
# ---------------------------------------------------------------------------
def submit_for_review(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")

    row = conn.execute(
        "SELECT status FROM document WHERE id = ?", (doc_id,)
    ).fetchone()
    if not row:
        err(f"Document {doc_id} not found")
    if row["status"] != "draft":
        err(f"Cannot submit for review: document is '{row['status']}'. Must be draft")

    conn.execute(
        "UPDATE document SET status = 'review', updated_at = datetime('now') WHERE id = ?",
        (doc_id,),
    )
    audit(conn, SKILL, "document-submit-for-review", "document", doc_id,
          new_values={"doc_status": "review"})
    conn.commit()
    ok({"document_id": doc_id, "doc_status": "review"})


# ---------------------------------------------------------------------------
# approve-document
# ---------------------------------------------------------------------------
def approve_document(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")

    row = conn.execute(
        "SELECT status FROM document WHERE id = ?", (doc_id,)
    ).fetchone()
    if not row:
        err(f"Document {doc_id} not found")
    if row["status"] != "review":
        err(f"Cannot approve: document is '{row['status']}'. Must be in review")

    conn.execute(
        "UPDATE document SET status = 'approved', updated_at = datetime('now') WHERE id = ?",
        (doc_id,),
    )
    audit(conn, SKILL, "document-approve-document", "document", doc_id,
          new_values={"doc_status": "approved"})
    conn.commit()
    ok({"document_id": doc_id, "doc_status": "approved"})


# ---------------------------------------------------------------------------
# archive-document
# ---------------------------------------------------------------------------
def archive_document(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")

    row = conn.execute(
        "SELECT status FROM document WHERE id = ?", (doc_id,)
    ).fetchone()
    if not row:
        err(f"Document {doc_id} not found")
    if row["status"] == "archived":
        err("Document is already archived")

    conn.execute(
        "UPDATE document SET status = 'archived', is_archived = 1, updated_at = datetime('now') WHERE id = ?",
        (doc_id,),
    )
    audit(conn, SKILL, "document-archive-document", "document", doc_id,
          new_values={"doc_status": "archived", "is_archived": 1})
    conn.commit()
    ok({"document_id": doc_id, "doc_status": "archived", "is_archived": 1})


# ---------------------------------------------------------------------------
# search-documents
# ---------------------------------------------------------------------------
def search_documents(conn, args):
    query = getattr(args, "search", None)
    if not query:
        err("--search is required")

    conditions = ["(d.title LIKE ? OR d.content LIKE ?)"]
    params = [f"%{query}%", f"%{query}%"]

    # Also search in tags
    company_id = getattr(args, "company_id", None)
    if company_id:
        conditions.append("d.company_id = ?")
        params.append(company_id)

    doc_type = getattr(args, "document_type", None)
    if doc_type:
        conditions.append("d.document_type = ?")
        params.append(doc_type)

    where = f"WHERE {' AND '.join(conditions)}"
    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    rows = conn.execute(
        f"""SELECT DISTINCT d.*
            FROM document d
            LEFT JOIN document_tag dt ON dt.document_id = d.id
            {where} OR dt.tag LIKE ?
            ORDER BY d.updated_at DESC LIMIT ? OFFSET ?""",
        params + [f"%{query}%", limit, offset],
    ).fetchall()

    total = conn.execute(
        f"""SELECT COUNT(DISTINCT d.id) as cnt
            FROM document d
            LEFT JOIN document_tag dt ON dt.document_id = d.id
            {where} OR dt.tag LIKE ?""",
        params + [f"%{query}%"],
    ).fetchone()["cnt"]

    documents = []
    for r in rows:
        d = row_to_dict(r)
        d["doc_status"] = d.pop("status", "draft")
        documents.append(d)

    ok({"documents": documents, "total_count": total, "search": query,
        "limit": limit, "offset": offset})


# ---------------------------------------------------------------------------
# set-retention
# ---------------------------------------------------------------------------
def set_retention(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")
    retention_date = getattr(args, "retention_date", None)
    if not retention_date:
        err("--retention-date is required")

    row = conn.execute(
        "SELECT id FROM document WHERE id = ?", (doc_id,)
    ).fetchone()
    if not row:
        err(f"Document {doc_id} not found")

    conn.execute(
        "UPDATE document SET retention_date = ?, updated_at = datetime('now') WHERE id = ?",
        (retention_date, doc_id),
    )
    audit(conn, SKILL, "document-set-retention", "document", doc_id,
          new_values={"retention_date": retention_date})
    conn.commit()
    ok({"document_id": doc_id, "retention_date": retention_date})


# ---------------------------------------------------------------------------
# hold-document
# ---------------------------------------------------------------------------
def hold_document(conn, args):
    doc_id = getattr(args, "document_id", None)
    if not doc_id:
        err("--document-id is required")

    row = conn.execute(
        "SELECT status FROM document WHERE id = ?", (doc_id,)
    ).fetchone()
    if not row:
        err(f"Document {doc_id} not found")
    if row["status"] in ("archived", "on_hold"):
        err(f"Cannot hold: document is '{row['status']}'")

    conn.execute(
        "UPDATE document SET status = 'on_hold', updated_at = datetime('now') WHERE id = ?",
        (doc_id,),
    )
    audit(conn, SKILL, "document-hold-document", "document", doc_id,
          new_values={"doc_status": "on_hold"})
    conn.commit()
    ok({"document_id": doc_id, "doc_status": "on_hold"})


# ---------------------------------------------------------------------------
# ACTIONS registry
# ---------------------------------------------------------------------------
ACTIONS = {
    "document-add-document": add_document,
    "document-update-document": update_document,
    "document-get-document": get_document,
    "document-list-documents": list_documents,
    "document-add-document-version": add_document_version,
    "document-list-document-versions": list_document_versions,
    "document-add-document-tag": add_document_tag,
    "document-remove-document-tag": remove_document_tag,
    "document-list-document-tags": list_document_tags,
    "document-link-document": link_document,
    "document-unlink-document": unlink_document,
    "document-list-document-links": list_document_links,
    "document-list-linked-documents": list_linked_documents,
    "document-submit-for-review": submit_for_review,
    "document-approve-document": approve_document,
    "document-archive-document": archive_document,
    "document-search-documents": search_documents,
    "document-set-retention": set_retention,
    "document-hold-document": hold_document,
}
