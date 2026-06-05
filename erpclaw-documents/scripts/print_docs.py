"""ERPClaw Documents -- Print / composite layer (S8 chunk 3).

``document-print-document`` is the keystone that ties the S8 render pipeline
together into one user-facing action:

    render (sandboxed Jinja2)  ->  format -> HTML  ->  render-pdf (WeasyPrint)
                                                   ->  persist a ``document`` row
                                                       carrying its ``pdf_path``

It reuses the two render seams already shipped, so this layer adds NO new
import-time dependency and CI patches the same single PDF seam it always has:

  - ``templates._render_jinja2``   (chunk 1 -- sandboxed; the SSTI boundary)
  - ``pdf._render_html_to_pdf``    (chunk 2 -- lazy WeasyPrint, mockable)

The only addition is Markdown -> HTML for ``format='markdown'`` templates, via a
lazy ``markdown-it-py`` import behind its own ``_markdown_to_html`` seam (clear,
actionable error when the library is absent -- same contract as the WeasyPrint
seam).

The print-invoice / print-purchase-order / print-packing-slip convenience
wrappers (which build merge data from a submitted parent document) call into
``print_document`` and arrive in a later chunk.

Imported by db_query.py (unified router) as PRINT_ACTIONS.
"""
import html as _html
import os
import sys
import uuid

sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
from erpclaw_lib.naming import get_next_name
from erpclaw_lib.response import ok, err
from erpclaw_lib.audit import audit
from erpclaw_lib.query import Q, P, Table, Field

# Reuse the render seams from the sibling domain modules. Importing the modules
# (not just the functions) keeps the test-time monkeypatch seam intact: tests
# patch ``pdf._render_html_to_pdf`` and this layer sees the patched version.
import pdf as _pdf
from templates import _render_jinja2, _parse_merge_data, _row_get, VALID_TEMPLATE_FORMATS

SKILL = "erpclaw-documents"


# ---------------------------------------------------------------------------
# Markdown -> HTML (lazy seam, mirrors pdf._render_html_to_pdf's contract)
# ---------------------------------------------------------------------------
def _markdown_to_html(md_text):
    """Convert a Markdown string to HTML via markdown-it-py.

    LAZY import (not at module top) so this module loads even when the library
    is absent -- only ``format='markdown'`` print jobs need it. ``html=False``
    (markdown-it default) means any raw HTML embedded in the Markdown source is
    escaped, not passed through, which keeps the PDF render side XSS-safe.
    Returns ``(html: str | None, error: str)`` -- error is empty on success."""
    try:
        from markdown_it import MarkdownIt
    except ImportError:
        return None, "markdown-it-py not installed; pip install markdown-it-py"
    return MarkdownIt().render(md_text), ""


def _wrap_html(body, fmt):
    """Build a complete HTML document WeasyPrint can render.

    - ``html``: the rendered body is already (partial or full) HTML; if it
      already declares ``<html>`` it is passed through untouched, otherwise it
      is wrapped in a minimal skeleton.
    - ``markdown``: the body is the markdown-it HTML output -> wrap in skeleton.
    - ``text``: escape and wrap in ``<pre>`` so whitespace/newlines survive the
      PDF render.
    """
    if fmt == "text":
        inner = "<pre>" + _html.escape(body) + "</pre>"
    else:
        inner = body
    if "<html" in inner.lower():
        return inner
    return (
        "<!DOCTYPE html>\n<html><head><meta charset=\"utf-8\"></head>"
        "<body>" + inner + "</body></html>"
    )


# ---------------------------------------------------------------------------
# print-document
# ---------------------------------------------------------------------------
def print_document(conn, args):
    """Render a template to a PDF and persist a ``document`` row pointing at it.

    Composite of the S8 pipeline: validates the template + company, renders the
    body through the sandboxed Jinja2 engine, converts the result to HTML
    according to the template's format (``--format`` overrides), writes a PDF via
    the WeasyPrint seam, then inserts a ``document`` (+ initial version) whose
    ``pdf_path`` records where the PDF landed. Output document starts in
    ``draft`` -- identical lifecycle to generate-from-template.
    """
    tpl_id = getattr(args, "template_id", None)
    if not tpl_id:
        err("--template-id is required")
    if not getattr(args, "title", None):
        err("--title is required for the generated document")
    if not getattr(args, "company_id", None):
        err("--company-id is required")

    row = conn.execute(
        Q.from_(Table("document_template")).select(Table("document_template").star)
        .where(Field("id") == P()).get_sql(),
        (tpl_id,),
    ).fetchone()
    if not row:
        err(f"Template {tpl_id} not found")
    if not row["is_active"]:
        err(f"Template {tpl_id} is inactive")

    if not conn.execute(
        Q.from_(Table("company")).select(Field("id")).where(Field("id") == P()).get_sql(),
        (args.company_id,),
    ).fetchone():
        err(f"Company {args.company_id} not found")

    fmt = getattr(args, "format", None) or _row_get(row, "format") or "text"
    if fmt not in VALID_TEMPLATE_FORMATS:
        err(f"Invalid format: {fmt} (expected one of {', '.join(VALID_TEMPLATE_FORMATS)})")

    data = _parse_merge_data(getattr(args, "merge_data", None))

    # 1. Render the body through the sandboxed Jinja2 engine (SSTI boundary).
    try:
        rendered = _render_jinja2(row["content"], data, fmt)
    except Exception as e:  # noqa: BLE001 -- surface as a clean err()
        err(f"Template render failed: {e}")

    # 2. Body -> complete HTML, converting Markdown when needed.
    if fmt == "markdown":
        body_html, md_err = _markdown_to_html(rendered)
        if body_html is None:
            err(md_err)
        page_html = _wrap_html(body_html, fmt)
    else:
        page_html = _wrap_html(rendered, fmt)

    # Oversize guard, identical contract to render-pdf (default 5 MB).
    max_bytes = getattr(args, "max_html_bytes", None) or _pdf._DEFAULT_MAX_HTML_BYTES
    html_bytes = len(page_html.encode("utf-8"))
    if html_bytes > max_bytes:
        err(f"Rendered HTML too large: {html_bytes} bytes exceeds limit of {max_bytes} bytes")

    # 3. Resolve the output path and render the PDF (lazy WeasyPrint seam).
    output_path = getattr(args, "output_path", None)
    if output_path:
        output_path = os.path.expanduser(output_path)
    else:
        output_path = os.path.join(_pdf._pdf_storage_root(), f"print-{uuid.uuid4()}.pdf")
    parent = os.path.dirname(output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    pdf_ok, info = _pdf._render_html_to_pdf(page_html, output_path)
    if not pdf_ok:
        err(info)

    # 4. Persist the document row (+ initial version) with its pdf_path. Single
    #    transaction: both inserts commit together or roll back together.
    tpl_type = row["template_type"]
    doc_type = tpl_type if tpl_type in (
        "general", "contract", "invoice", "report", "certificate", "other"
    ) else "general"

    doc_id = str(uuid.uuid4())
    ns = get_next_name(conn, "document", company_id=args.company_id)
    conn.execute(
        """INSERT INTO document
           (id, naming_series, title, document_type, content, pdf_path,
            current_version, owner, is_archived, status, company_id)
           VALUES (?,?,?,?,?,?,?,?,0,?,?)""",
        (
            doc_id, ns, args.title, doc_type, rendered, output_path, "1",
            getattr(args, "owner", None), "draft", args.company_id,
        ),
    )
    version_id = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO document_version
           (id, document_id, version_number, content, change_notes, created_by)
           VALUES (?,?,?,?,?,?)""",
        (
            version_id, doc_id, "1", rendered,
            f"Printed from template {row['name']}",
            getattr(args, "owner", None),
        ),
    )
    audit(conn, SKILL, "document-print-document", "document", doc_id,
          new_values={"template_id": tpl_id, "template_name": row["name"],
                      "naming_series": ns, "pdf_path": output_path, "format": fmt})
    conn.commit()
    ok({
        "document_id": doc_id,
        "naming_series": ns,
        "doc_status": "draft",
        "template_id": tpl_id,
        "template_name": row["name"],
        "format": fmt,
        "pdf_path": output_path,
    })


# ---------------------------------------------------------------------------
# ACTIONS registry
# ---------------------------------------------------------------------------
PRINT_ACTIONS = {
    "document-print-document": print_document,
}
