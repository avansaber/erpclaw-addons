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
from erpclaw_lib.query import Q, P, Table, Field, fn, Order, LiteralValue, insert_row, update_row, dynamic_update

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

    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    doc_type = getattr(args, "document_type", None) or "general"
    if doc_type not in VALID_DOC_TYPES:
        err(f"Invalid document-type: {doc_type}")

    doc_id = str(uuid.uuid4())
    ns = get_next_name(conn, "document", company_id=args.company_id)

    file_size = getattr(args, "file_size", None)
    if file_size is not None:
        file_size = int(file_size)

    sql, _ = insert_row("document", {
        "id": P(), "naming_series": P(), "title": P(), "document_type": P(),
        "file_name": P(), "file_path": P(), "file_size": P(), "mime_type": P(),
        "content": P(), "current_version": P(), "tags": P(),
        "linked_entity_type": P(), "linked_entity_id": P(), "owner": P(),
        "retention_date": P(), "is_archived": P(), "status": P(), "company_id": P(),
    })
    conn.execute(sql, (
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
        0,
        "draft",
        args.company_id,
    ))

    # Create initial version record
    version_id = str(uuid.uuid4())
    sql, _ = insert_row("document_version", {"id": P(), "document_id": P(), "version_number": P(), "file_name": P(), "file_path": P(), "content": P(), "change_notes": P(), "created_by": P()})
    conn.execute(sql,
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
            sql, _ = insert_row("document_tag", {"id": P(), "document_id": P(), "tag": P()})
            conn.execute(sql,
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
    row = conn.execute(Q.from_(Table("document")).select(Table("document").star).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone()
    if not row:
        err(f"Document {doc_id} not found")

    data, changed = {}, []

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
            data[field] = val
            changed.append(field)

    dt = getattr(args, "document_type", None)
    if dt is not None:
        if dt not in VALID_DOC_TYPES:
            err(f"Invalid document-type: {dt}")
        data["document_type"] = dt
        changed.append("document_type")

    fs = getattr(args, "file_size", None)
    if fs is not None:
        data["file_size"] = int(fs)
        changed.append("file_size")

    if not changed:
        err("No fields to update")

    data["updated_at"] = LiteralValue("datetime('now')")
    sql, params = dynamic_update("document", data, {"id": doc_id})
    conn.execute(sql, params)
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
    row = conn.execute(Q.from_(Table("document")).select(Table("document").star).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone()
    if not row:
        err(f"Document {doc_id} not found")

    data = row_to_dict(row)
    data["doc_status"] = data.pop("status", "draft")

    # Get version count
    dv = Table("document_version")
    ver_count = conn.execute(
        Q.from_(dv).select(fn.Count("*").as_("cnt")).where(dv.document_id == P()).get_sql(),
        (doc_id,),
    ).fetchone()
    data["version_count"] = ver_count["cnt"] if ver_count else 0

    # Get tags
    tag_rows = conn.execute(Q.from_(Table("document_tag")).select(Field('tag')).where(Field("document_id") == P()).get_sql(), (doc_id,)).fetchall()
    data["tag_list"] = [r["tag"] for r in tag_rows]

    # Get links
    dl = Table("document_link")
    link_count = conn.execute(
        Q.from_(dl).select(fn.Count("*").as_("cnt")).where(dl.document_id == P()).get_sql(),
        (doc_id,),
    ).fetchone()
    data["link_count"] = link_count["cnt"] if link_count else 0

    ok(data)


# ---------------------------------------------------------------------------
# list-documents
# ---------------------------------------------------------------------------
def list_documents(conn, args):
    t = Table("document")
    params = []

    q = Q.from_(t).select(t.star)
    cq = Q.from_(t).select(fn.Count("*").as_("cnt"))

    company_id = getattr(args, "company_id", None)
    if company_id:
        q = q.where(t.company_id == P())
        cq = cq.where(t.company_id == P())
        params.append(company_id)
    status = getattr(args, "status", None)
    if status:
        q = q.where(t.status == P())
        cq = cq.where(t.status == P())
        params.append(status)
    doc_type = getattr(args, "document_type", None)
    if doc_type:
        q = q.where(t.document_type == P())
        cq = cq.where(t.document_type == P())
        params.append(doc_type)
    owner = getattr(args, "owner", None)
    if owner:
        q = q.where(t.owner == P())
        cq = cq.where(t.owner == P())
        params.append(owner)
    search = getattr(args, "search", None)
    if search:
        search_crit = (t.title.like(P())) | (t.content.like(P())) | (t.tags.like(P()))
        q = q.where(search_crit)
        cq = cq.where(search_crit)
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    total = conn.execute(cq.get_sql(), params).fetchone()["cnt"]

    q = q.orderby(t.created_at, order=Order.desc).limit(P()).offset(P())
    rows = conn.execute(q.get_sql(), params + [limit, offset]).fetchall()

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

    row = conn.execute(Q.from_(Table("document")).select(Table("document").star).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone()
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
    sql, _ = insert_row("document_version", {"id": P(), "document_id": P(), "version_number": P(), "file_name": P(), "file_path": P(), "content": P(), "change_notes": P(), "created_by": P()})
    conn.execute(sql,
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
    sql = update_row("document",
        data={"current_version": P(), "updated_at": LiteralValue("datetime('now')")},
        where={"id": P()})
    conn.execute(sql, (version_number, doc_id))

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

    if not conn.execute(Q.from_(Table("document")).select(Field('id')).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone():
        err(f"Document {doc_id} not found")

    rows = conn.execute(Q.from_(Table("document_version")).select(Table("document_version").star).where(Field("document_id") == P()).orderby(Field("created_at"), order=Order.desc).get_sql(), (doc_id,)).fetchall()

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

    if not conn.execute(Q.from_(Table("document")).select(Field('id')).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone():
        err(f"Document {doc_id} not found")

    # Check for duplicate tag
    dt = Table("document_tag")
    existing = conn.execute(
        Q.from_(dt).select(dt.id).where(dt.document_id == P()).where(dt.tag == P()).get_sql(),
        (doc_id, tag),
    ).fetchone()
    if existing:
        err(f"Tag '{tag}' already exists on document {doc_id}")

    tag_id = str(uuid.uuid4())
    sql, _ = insert_row("document_tag", {"id": P(), "document_id": P(), "tag": P()})
    conn.execute(sql,
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

    dt = Table("document_tag")
    row = conn.execute(
        Q.from_(dt).select(dt.id).where(dt.document_id == P()).where(dt.tag == P()).get_sql(),
        (doc_id, tag),
    ).fetchone()
    if not row:
        err(f"Tag '{tag}' not found on document {doc_id}")

    conn.execute(
        Q.from_(dt).delete().where(dt.document_id == P()).where(dt.tag == P()).get_sql(),
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

    if not conn.execute(Q.from_(Table("document")).select(Field('id')).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone():
        err(f"Document {doc_id} not found")

    rows = conn.execute(Q.from_(Table("document_tag")).select(Table("document_tag").star).where(Field("document_id") == P()).orderby(Field("tag")).get_sql(), (doc_id,)).fetchall()

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

    if not conn.execute(Q.from_(Table("document")).select(Field('id')).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone():
        err(f"Document {doc_id} not found")

    if not conn.execute(Q.from_(Table("company")).select(Field('id')).where(Field("id") == P()).get_sql(), (args.company_id,)).fetchone():
        err(f"Company {args.company_id} not found")

    link_type = getattr(args, "link_type", None) or "attachment"
    if link_type not in VALID_LINK_TYPES:
        err(f"Invalid link-type: {link_type}")

    link_id = str(uuid.uuid4())
    sql, _ = insert_row("document_link", {"id": P(), "document_id": P(), "linked_entity_type": P(), "linked_entity_id": P(), "link_type": P(), "notes": P(), "company_id": P()})
    conn.execute(sql,
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

    row = conn.execute(Q.from_(Table("document_link")).select(Table("document_link").star).where(Field("id") == P()).get_sql(), (link_id,)).fetchone()
    if not row:
        err(f"Document link {link_id} not found")

    conn.execute(Q.from_(Table("document_link")).delete().where(Field("id") == P()).get_sql(), (link_id,))
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

    if not conn.execute(Q.from_(Table("document")).select(Field('id')).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone():
        err(f"Document {doc_id} not found")

    rows = conn.execute(Q.from_(Table("document_link")).select(Table("document_link").star).where(Field("document_id") == P()).orderby(Field("created_at"), order=Order.desc).get_sql(), (doc_id,)).fetchall()

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

    dl = Table("document_link")
    d = Table("document")
    q = (Q.from_(dl)
         .join(d).on(d.id == dl.document_id)
         .select(dl.star, d.title, d.document_type, d.status.as_("doc_status"),
                 d.naming_series, d.current_version)
         .where(dl.linked_entity_type == P())
         .where(dl.linked_entity_id == P())
         .orderby(dl.created_at, order=Order.desc))
    rows = conn.execute(q.get_sql(), (entity_type, entity_id)).fetchall()

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

    row = conn.execute(Q.from_(Table("document")).select(Field('status')).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone()
    if not row:
        err(f"Document {doc_id} not found")
    if row["status"] != "draft":
        err(f"Cannot submit for review: document is '{row['status']}'. Must be draft")

    sql = update_row("document",
        data={"status": P(), "updated_at": LiteralValue("datetime('now')")},
        where={"id": P()})
    conn.execute(sql, ("review", doc_id))
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

    row = conn.execute(Q.from_(Table("document")).select(Field('status')).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone()
    if not row:
        err(f"Document {doc_id} not found")
    if row["status"] != "review":
        err(f"Cannot approve: document is '{row['status']}'. Must be in review")

    sql = update_row("document",
        data={"status": P(), "updated_at": LiteralValue("datetime('now')")},
        where={"id": P()})
    conn.execute(sql, ("approved", doc_id))
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

    row = conn.execute(Q.from_(Table("document")).select(Field('status')).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone()
    if not row:
        err(f"Document {doc_id} not found")
    if row["status"] == "archived":
        err("Document is already archived")

    sql = update_row("document",
        data={"status": P(), "is_archived": P(), "updated_at": LiteralValue("datetime('now')")},
        where={"id": P()})
    conn.execute(sql, ("archived", 1, doc_id))
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

    d = Table("document")
    dt_t = Table("document_tag")

    # Build base content/title match criterion
    content_crit = (d.title.like(P())) | (d.content.like(P()))
    params = [f"%{query}%", f"%{query}%"]

    # Add optional filters (AND-ed with content match)
    company_id = getattr(args, "company_id", None)
    if company_id:
        content_crit = content_crit & (d.company_id == P())
        params.append(company_id)

    doc_type = getattr(args, "document_type", None)
    if doc_type:
        content_crit = content_crit & (d.document_type == P())
        params.append(doc_type)

    # Final criterion: content match OR tag match
    tag_crit = dt_t.tag.like(P())
    full_crit = content_crit | tag_crit

    limit = getattr(args, "limit", 50) or 50
    offset = getattr(args, "offset", 0) or 0

    base_q = (Q.from_(d)
              .left_join(dt_t).on(dt_t.document_id == d.id)
              .where(full_crit))

    q = (base_q.select(d.star).distinct()
         .orderby(d.updated_at, order=Order.desc)
         .limit(P()).offset(P()))
    rows = conn.execute(q.get_sql(), params + [f"%{query}%", limit, offset]).fetchall()

    cq = base_q.select(fn.Count(d.id).distinct().as_("cnt"))
    total = conn.execute(cq.get_sql(), params + [f"%{query}%"]).fetchone()["cnt"]

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

    row = conn.execute(Q.from_(Table("document")).select(Field('id')).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone()
    if not row:
        err(f"Document {doc_id} not found")

    sql = update_row("document",
        data={"retention_date": P(), "updated_at": LiteralValue("datetime('now')")},
        where={"id": P()})
    conn.execute(sql, (retention_date, doc_id))
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

    row = conn.execute(Q.from_(Table("document")).select(Field('status')).where(Field("id") == P()).get_sql(), (doc_id,)).fetchone()
    if not row:
        err(f"Document {doc_id} not found")
    if row["status"] in ("archived", "on_hold"):
        err(f"Cannot hold: document is '{row['status']}'")

    sql = update_row("document",
        data={"status": P(), "updated_at": LiteralValue("datetime('now')")},
        where={"id": P()})
    conn.execute(sql, ("on_hold", doc_id))
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
