"""ERPClaw Documents -- PDF rendering (S8 chunk 2).

`document-render-pdf`: takes an HTML string (or a file) and writes a PDF to
disk via WeasyPrint, returning the output path. Pure render -- no DB writes
(wiring the result onto ``document.pdf_path`` is a later S8 chunk).

Two deliberate design choices mirror the M8-A email substrate
(``erpclaw-alerts/scripts/email_sender.py``):

  1. **Lazy import.** WeasyPrint is imported INSIDE the seam, never at module
     top, so this module loads even when WeasyPrint is absent. If it is not
     installed the action returns a clear, actionable error.
  2. **Single mockable seam.** ``_render_html_to_pdf`` is the one function
     tests patch (no real WeasyPrint in CI), exactly as
     ``_send_via_provider`` is for email.

Imported by db_query.py (unified router) as PDF_ACTIONS.
"""
import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.response import ok, err

SKILL = "erpclaw-documents"

# Reject oversize HTML before handing it to the renderer (S8 plan: default
# 5 MB, configurable per-call via --max-html-bytes).
_DEFAULT_MAX_HTML_BYTES = 5 * 1024 * 1024


def _pdf_storage_root():
    """Resolve the configurable PDF storage root.

    Mirrors the ``ERPCLAW_DB_PATH`` env-var convention erpclaw-setup uses for
    the database location: an operator sets ``ERPCLAW_PDF_STORAGE_ROOT`` at
    setup time; otherwise we default to a sensible dir under the OpenClaw data
    home. The DB stores only the path (Q6: filesystem, not BLOB-in-SQLite)."""
    return os.environ.get("ERPCLAW_PDF_STORAGE_ROOT") or os.path.expanduser(
        "~/.openclaw/erpclaw/documents/pdf"
    )


def _render_html_to_pdf(html, output_path):
    """The single seam tests patch (no real WeasyPrint in CI).

    LAZY-imports WeasyPrint here (not at module top) so the module loads even
    when the dependency is absent. Writes the rendered PDF to ``output_path``.
    Returns ``(success: bool, message: str)`` -- message carries the error
    string on failure and is empty on success. Mirrors
    ``email_sender._send_via_provider``."""
    try:
        from weasyprint import HTML
    except ImportError:
        return False, "WeasyPrint not installed; pip install weasyprint"
    try:
        HTML(string=html).write_pdf(output_path)
        return True, ""
    except Exception as e:  # noqa: BLE001 -- surface the render error to caller
        return False, str(e)


# ---------------------------------------------------------------------------
# render-pdf
# ---------------------------------------------------------------------------
def render_pdf(conn, args):
    """Render HTML to a PDF file on disk and return its path.

    Accepts the HTML inline via ``--html`` or from a file via
    ``--html-from-file``. Writes under the configurable storage root unless
    ``--output-path`` overrides the destination. Does NOT touch the database."""
    html = getattr(args, "html", None)
    html_from_file = getattr(args, "html_from_file", None)
    if html and html_from_file:
        err("Provide either --html or --html-from-file, not both")
    if not html and not html_from_file:
        err("Provide --html or --html-from-file")

    if html_from_file:
        src = os.path.expanduser(html_from_file)
        if not os.path.isfile(src):
            err(f"--html-from-file not found: {html_from_file}")
        try:
            with open(src, "r", encoding="utf-8") as fh:
                html = fh.read()
        except OSError as e:
            err(f"Could not read --html-from-file: {e}")

    # Oversize guard (reject HTML > limit; default 5 MB, configurable).
    max_bytes = getattr(args, "max_html_bytes", None) or _DEFAULT_MAX_HTML_BYTES
    html_bytes = len(html.encode("utf-8"))
    if html_bytes > max_bytes:
        err(f"HTML too large: {html_bytes} bytes exceeds limit of {max_bytes} bytes")

    output_path = getattr(args, "output_path", None)
    if output_path:
        output_path = os.path.expanduser(output_path)
    else:
        output_path = os.path.join(_pdf_storage_root(), f"render-{uuid.uuid4()}.pdf")

    parent = os.path.dirname(output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    rendered, info = _render_html_to_pdf(html, output_path)
    if not rendered:
        err(info)

    ok({
        "result": "rendered",
        "output_path": output_path,
        "bytes_in": html_bytes,
    })


# ---------------------------------------------------------------------------
# ACTIONS registry
# ---------------------------------------------------------------------------
PDF_ACTIONS = {
    "document-render-pdf": render_pdf,
}
