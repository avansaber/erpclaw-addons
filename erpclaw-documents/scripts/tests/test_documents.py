"""L1 tests for ERPClaw Documents module (25 actions, 5 tables).

Actions tested:
  Documents:   document-add-document, document-update-document, document-get-document,
               document-list-documents, document-search-documents
  Versions:    document-add-document-version, document-list-document-versions
  Tags:        document-add-document-tag, document-remove-document-tag, document-list-document-tags
  Links:       document-link-document, document-unlink-document, document-list-document-links,
               document-list-linked-documents
  Workflow:    document-submit-for-review, document-approve-document, document-archive-document,
               document-hold-document
  Retention:   document-set-retention
  Templates:   document-add-template, document-update-template, document-get-template,
               document-list-templates, document-generate-from-template
  Status:      status
"""
import json
import os
import uuid

import pytest
from documents_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_company, seed_naming_series, seed_document, seed_template,
    seed_customer, seed_supplier, seed_item,
    seed_sales_invoice, seed_purchase_order,
    seed_delivery_note_and_packing_slip,
)

mod = load_db_query()


# =============================================================================
# Documents CRUD
# =============================================================================

class TestAddDocument:
    def test_basic_create(self, conn, env):
        result = call_action(mod.document_add_document, conn, ns(
            company_id=env["company_id"],
            title="Expense Policy 2026",
            document_type="policy",
            content="All expenses must be approved.",
            owner="admin",
        ))
        assert is_ok(result), result
        assert result["doc_status"] == "draft"
        assert "document_id" in result
        assert "naming_series" in result

    def test_with_tags(self, conn, env):
        result = call_action(mod.document_add_document, conn, ns(
            company_id=env["company_id"],
            title="Tagged Doc",
            tags="finance,policy,2026",
        ))
        assert is_ok(result), result
        # Verify tags were created
        doc = call_action(mod.document_get_document, conn, ns(
            document_id=result["document_id"],
        ))
        assert len(doc["tag_list"]) == 3

    def test_missing_title_fails(self, conn, env):
        result = call_action(mod.document_add_document, conn, ns(
            company_id=env["company_id"],
            title=None,
        ))
        assert is_error(result)

    def test_missing_company_fails(self, conn, env):
        result = call_action(mod.document_add_document, conn, ns(
            company_id=None,
            title="Orphan Doc",
        ))
        assert is_error(result)

    def test_invalid_doc_type_fails(self, conn, env):
        result = call_action(mod.document_add_document, conn, ns(
            company_id=env["company_id"],
            title="Bad Type",
            document_type="hologram",
        ))
        assert is_error(result)


class TestUpdateDocument:
    def test_update_title(self, conn, env):
        result = call_action(mod.document_update_document, conn, ns(
            document_id=env["document_id"],
            title="Updated Title",
        ))
        assert is_ok(result), result
        assert "title" in result["updated_fields"]

    def test_update_multiple_fields(self, conn, env):
        result = call_action(mod.document_update_document, conn, ns(
            document_id=env["document_id"],
            title="New Title",
            content="New content",
            owner="new_owner",
        ))
        assert is_ok(result), result
        assert len(result["updated_fields"]) == 3

    def test_no_fields_fails(self, conn, env):
        result = call_action(mod.document_update_document, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_error(result)

    def test_missing_id_fails(self, conn, env):
        result = call_action(mod.document_update_document, conn, ns(
            document_id=None,
            title="X",
        ))
        assert is_error(result)


class TestGetDocument:
    def test_get_existing(self, conn, env):
        result = call_action(mod.document_get_document, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_ok(result), result
        assert result["doc_status"] == "draft"
        assert "version_count" in result
        assert result["version_count"] >= 1
        assert "tag_list" in result
        assert "link_count" in result

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.document_get_document, conn, ns(
            document_id="nonexistent-id",
        ))
        assert is_error(result)


class TestListDocuments:
    def test_list_by_company(self, conn, env):
        result = call_action(mod.document_list_documents, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_list_filter_by_status(self, conn, env):
        result = call_action(mod.document_list_documents, conn, ns(
            company_id=env["company_id"],
            status="draft",
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_list_filter_by_type(self, conn, env):
        result = call_action(mod.document_list_documents, conn, ns(
            company_id=env["company_id"],
            document_type="general",
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1


# =============================================================================
# Versions
# =============================================================================

class TestAddDocumentVersion:
    def test_add_version(self, conn, env):
        result = call_action(mod.document_add_document_version, conn, ns(
            document_id=env["document_id"],
            content="Updated content for v2",
            change_notes="Major revision",
            created_by="admin",
        ))
        assert is_ok(result), result
        assert result["version_number"] == "2"
        assert result["document_id"] == env["document_id"]

    def test_custom_version_number(self, conn, env):
        result = call_action(mod.document_add_document_version, conn, ns(
            document_id=env["document_id"],
            version_number="1.1",
            content="Minor update",
        ))
        assert is_ok(result), result
        assert result["version_number"] == "1.1"

    def test_missing_doc_id_fails(self, conn, env):
        result = call_action(mod.document_add_document_version, conn, ns(
            document_id=None,
            content="X",
        ))
        assert is_error(result)


class TestListDocumentVersions:
    def test_list_versions(self, conn, env):
        result = call_action(mod.document_list_document_versions, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_ok(result), result
        assert result["count"] >= 1


# =============================================================================
# Tags
# =============================================================================

class TestAddDocumentTag:
    def test_add_tag(self, conn, env):
        result = call_action(mod.document_add_document_tag, conn, ns(
            document_id=env["document_id"],
            tag="important",
        ))
        assert is_ok(result), result
        assert result["tag"] == "important"

    def test_duplicate_tag_fails(self, conn, env):
        call_action(mod.document_add_document_tag, conn, ns(
            document_id=env["document_id"],
            tag="unique-tag",
        ))
        result = call_action(mod.document_add_document_tag, conn, ns(
            document_id=env["document_id"],
            tag="unique-tag",
        ))
        assert is_error(result)

    def test_missing_tag_fails(self, conn, env):
        result = call_action(mod.document_add_document_tag, conn, ns(
            document_id=env["document_id"],
            tag=None,
        ))
        assert is_error(result)


class TestRemoveDocumentTag:
    def test_remove_tag(self, conn, env):
        call_action(mod.document_add_document_tag, conn, ns(
            document_id=env["document_id"],
            tag="removable",
        ))
        result = call_action(mod.document_remove_document_tag, conn, ns(
            document_id=env["document_id"],
            tag="removable",
        ))
        assert is_ok(result), result
        assert result["removed"] is True

    def test_remove_nonexistent_fails(self, conn, env):
        result = call_action(mod.document_remove_document_tag, conn, ns(
            document_id=env["document_id"],
            tag="never-added",
        ))
        assert is_error(result)


class TestListDocumentTags:
    def test_list_tags(self, conn, env):
        call_action(mod.document_add_document_tag, conn, ns(
            document_id=env["document_id"],
            tag="alpha",
        ))
        call_action(mod.document_add_document_tag, conn, ns(
            document_id=env["document_id"],
            tag="beta",
        ))
        result = call_action(mod.document_list_document_tags, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_ok(result), result
        assert result["count"] == 2


# =============================================================================
# Links
# =============================================================================

class TestLinkDocument:
    def test_link_basic(self, conn, env):
        result = call_action(mod.document_link_document, conn, ns(
            document_id=env["document_id"],
            linked_entity_type="sales_order",
            linked_entity_id="so-12345",
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["link_type"] == "attachment"
        assert "link_id" in result

    def test_link_with_type(self, conn, env):
        result = call_action(mod.document_link_document, conn, ns(
            document_id=env["document_id"],
            linked_entity_type="invoice",
            linked_entity_id="inv-001",
            company_id=env["company_id"],
            link_type="reference",
        ))
        assert is_ok(result), result
        assert result["link_type"] == "reference"

    def test_link_missing_entity_type_fails(self, conn, env):
        result = call_action(mod.document_link_document, conn, ns(
            document_id=env["document_id"],
            linked_entity_type=None,
            linked_entity_id="x",
            company_id=env["company_id"],
        ))
        assert is_error(result)

    def test_invalid_link_type_fails(self, conn, env):
        result = call_action(mod.document_link_document, conn, ns(
            document_id=env["document_id"],
            linked_entity_type="order",
            linked_entity_id="x",
            company_id=env["company_id"],
            link_type="magical",
        ))
        assert is_error(result)


class TestUnlinkDocument:
    def _link(self, conn, env):
        result = call_action(mod.document_link_document, conn, ns(
            document_id=env["document_id"],
            linked_entity_type="order",
            linked_entity_id="ord-001",
            company_id=env["company_id"],
        ))
        return result["link_id"]

    def test_unlink(self, conn, env):
        link_id = self._link(conn, env)
        result = call_action(mod.document_unlink_document, conn, ns(
            link_id=link_id,
        ))
        assert is_ok(result), result
        assert result["removed"] is True

    def test_unlink_nonexistent_fails(self, conn, env):
        result = call_action(mod.document_unlink_document, conn, ns(
            link_id="nonexistent-id",
        ))
        assert is_error(result)


class TestListDocumentLinks:
    def test_list_links(self, conn, env):
        call_action(mod.document_link_document, conn, ns(
            document_id=env["document_id"],
            linked_entity_type="order",
            linked_entity_id="ord-001",
            company_id=env["company_id"],
        ))
        result = call_action(mod.document_list_document_links, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_ok(result), result
        assert result["count"] == 1


class TestListLinkedDocuments:
    def test_list_linked_documents(self, conn, env):
        call_action(mod.document_link_document, conn, ns(
            document_id=env["document_id"],
            linked_entity_type="purchase_order",
            linked_entity_id="po-999",
            company_id=env["company_id"],
        ))
        result = call_action(mod.document_list_linked_documents, conn, ns(
            linked_entity_type="purchase_order",
            linked_entity_id="po-999",
        ))
        assert is_ok(result), result
        assert result["count"] == 1
        assert result["documents"][0]["title"] is not None


# =============================================================================
# Document Workflow
# =============================================================================

class TestSubmitForReview:
    def test_submit_draft(self, conn, env):
        result = call_action(mod.document_submit_for_review, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_ok(result), result
        assert result["doc_status"] == "review"

    def test_submit_non_draft_fails(self, conn, env):
        # Move to review first
        call_action(mod.document_submit_for_review, conn, ns(
            document_id=env["document_id"],
        ))
        # Try again -- already in review
        result = call_action(mod.document_submit_for_review, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_error(result)


class TestApproveDocument:
    def test_approve_from_review(self, conn, env):
        call_action(mod.document_submit_for_review, conn, ns(
            document_id=env["document_id"],
        ))
        result = call_action(mod.document_approve_document, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_ok(result), result
        assert result["doc_status"] == "approved"

    def test_approve_from_draft_fails(self, conn, env):
        result = call_action(mod.document_approve_document, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_error(result)


class TestArchiveDocument:
    def test_archive(self, conn, env):
        result = call_action(mod.document_archive_document, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_ok(result), result
        assert result["doc_status"] == "archived"
        assert result["is_archived"] == 1

    def test_archive_already_archived_fails(self, conn, env):
        call_action(mod.document_archive_document, conn, ns(
            document_id=env["document_id"],
        ))
        result = call_action(mod.document_archive_document, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_error(result)


class TestHoldDocument:
    def test_hold_draft(self, conn, env):
        result = call_action(mod.document_hold_document, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_ok(result), result
        assert result["doc_status"] == "on_hold"

    def test_hold_archived_fails(self, conn, env):
        call_action(mod.document_archive_document, conn, ns(
            document_id=env["document_id"],
        ))
        result = call_action(mod.document_hold_document, conn, ns(
            document_id=env["document_id"],
        ))
        assert is_error(result)


# =============================================================================
# Retention
# =============================================================================

class TestSetRetention:
    def test_set_retention(self, conn, env):
        result = call_action(mod.document_set_retention, conn, ns(
            document_id=env["document_id"],
            retention_date="2030-12-31",
        ))
        assert is_ok(result), result
        assert result["retention_date"] == "2030-12-31"

    def test_missing_date_fails(self, conn, env):
        result = call_action(mod.document_set_retention, conn, ns(
            document_id=env["document_id"],
            retention_date=None,
        ))
        assert is_error(result)


# =============================================================================
# Search
# =============================================================================

class TestSearchDocuments:
    def test_search_by_title(self, conn, env):
        result = call_action(mod.document_search_documents, conn, ns(
            search="Test Document",
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1

    def test_search_missing_query_fails(self, conn, env):
        result = call_action(mod.document_search_documents, conn, ns(
            search=None,
        ))
        assert is_error(result)


# =============================================================================
# Templates
# =============================================================================

class TestAddTemplate:
    def test_basic_create(self, conn, env):
        result = call_action(mod.document_add_template, conn, ns(
            company_id=env["company_id"],
            name="Invoice Template",
            content="Dear {{name}}, total: {{amount}}",
            template_type="invoice",
            merge_fields="name,amount",
        ))
        assert is_ok(result), result
        assert result["template_type"] == "invoice"
        assert "template_id" in result
        assert "naming_series" in result

    def test_missing_name_fails(self, conn, env):
        result = call_action(mod.document_add_template, conn, ns(
            company_id=env["company_id"],
            name=None,
            content="X",
        ))
        assert is_error(result)

    def test_missing_content_fails(self, conn, env):
        result = call_action(mod.document_add_template, conn, ns(
            company_id=env["company_id"],
            name="Empty Template",
            content=None,
        ))
        assert is_error(result)

    def test_invalid_template_type_fails(self, conn, env):
        result = call_action(mod.document_add_template, conn, ns(
            company_id=env["company_id"],
            name="Bad Type",
            content="X",
            template_type="hologram",
        ))
        assert is_error(result)


class TestUpdateTemplate:
    def test_update_name(self, conn, env):
        result = call_action(mod.document_update_template, conn, ns(
            template_id=env["template_id"],
            name="Renamed Template",
        ))
        assert is_ok(result), result
        assert "name" in result["updated_fields"]

    def test_no_fields_fails(self, conn, env):
        result = call_action(mod.document_update_template, conn, ns(
            template_id=env["template_id"],
        ))
        assert is_error(result)


class TestGetTemplate:
    def test_get_existing(self, conn, env):
        result = call_action(mod.document_get_template, conn, ns(
            template_id=env["template_id"],
        ))
        assert is_ok(result), result
        assert result["name"] == "Test Template"
        assert result["is_active"] == 1

    def test_get_nonexistent_fails(self, conn, env):
        result = call_action(mod.document_get_template, conn, ns(
            template_id="nonexistent-id",
        ))
        assert is_error(result)


class TestListTemplates:
    def test_list_by_company(self, conn, env):
        result = call_action(mod.document_list_templates, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["total_count"] >= 1


class TestGenerateFromTemplate:
    def test_generate_basic(self, conn, env):
        result = call_action(mod.document_generate_from_template, conn, ns(
            template_id=env["template_id"],
            title="Generated Contract",
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        assert result["doc_status"] == "draft"
        assert result["template_id"] == env["template_id"]
        assert "document_id" in result

    def test_generate_with_merge_data(self, conn, env):
        import json as _json
        merge = _json.dumps({"name": "Acme Corp", "order_id": "ORD-001"})
        result = call_action(mod.document_generate_from_template, conn, ns(
            template_id=env["template_id"],
            title="Merged Document",
            company_id=env["company_id"],
            merge_data=merge,
        ))
        assert is_ok(result), result
        # Verify merge happened in the document content
        doc = call_action(mod.document_get_document, conn, ns(
            document_id=result["document_id"],
        ))
        assert "Acme Corp" in doc["content"]
        assert "ORD-001" in doc["content"]

    def test_generate_missing_title_fails(self, conn, env):
        result = call_action(mod.document_generate_from_template, conn, ns(
            template_id=env["template_id"],
            title=None,
            company_id=env["company_id"],
        ))
        assert is_error(result)


# =============================================================================
# render-template (S8 — Jinja2 engine)
# =============================================================================

from documents_helpers import seed_template


class TestRenderTemplate:
    def test_jinja2_merge_vars(self, conn, env):
        """Jinja2 substitutes {{ var }} merge variables."""
        import json as _json
        tpl = seed_template(conn, env["company_id"], name="J2 Vars",
                            content="Hi {{ name }}, order {{ order_id }} ready.",
                            fmt="text", engine="jinja2")
        result = call_action(mod.document_render_template, conn, ns(
            template_id=tpl,
            merge_data=_json.dumps({"name": "Acme", "order_id": "ORD-7"}),
        ))
        assert is_ok(result), result
        assert result["rendered"] == "Hi Acme, order ORD-7 ready."
        assert result["engine"] == "jinja2"

    def test_jinja2_conditional_and_loop(self, conn, env):
        """Jinja2 control flow (if + for) renders correctly."""
        import json as _json
        body = (
            "{% if vip %}VALUED {% endif %}Customer. Items:"
            "{% for it in items %} {{ it }}{% endfor %}."
        )
        tpl = seed_template(conn, env["company_id"], name="J2 Flow",
                            content=body, fmt="text", engine="jinja2")
        result = call_action(mod.document_render_template, conn, ns(
            template_id=tpl,
            merge_data=_json.dumps({"vip": True, "items": ["A", "B", "C"]}),
        ))
        assert is_ok(result), result
        assert result["rendered"] == "VALUED Customer. Items: A B C."

    def test_html_autoescape_on(self, conn, env):
        """format=html → autoescape ON: HTML-unsafe merge data is escaped."""
        import json as _json
        tpl = seed_template(conn, env["company_id"], name="J2 HTML",
                            content="<p>{{ note }}</p>", fmt="html", engine="jinja2")
        result = call_action(mod.document_render_template, conn, ns(
            template_id=tpl,
            merge_data=_json.dumps({"note": "<script>alert(1)</script>"}),
        ))
        assert is_ok(result), result
        assert "<script>" not in result["rendered"]
        assert "&lt;script&gt;" in result["rendered"]

    def test_text_no_autoescape(self, conn, env):
        """format=text → autoescape OFF: raw passthrough (no HTML entities)."""
        import json as _json
        tpl = seed_template(conn, env["company_id"], name="J2 Text",
                            content="{{ note }}", fmt="text", engine="jinja2")
        result = call_action(mod.document_render_template, conn, ns(
            template_id=tpl,
            merge_data=_json.dumps({"note": "<b>x</b>"}),
        ))
        assert is_ok(result), result
        assert result["rendered"] == "<b>x</b>"

    def test_format_override(self, conn, env):
        """--format overrides the template's stored format (text→html escapes)."""
        import json as _json
        tpl = seed_template(conn, env["company_id"], name="J2 Override",
                            content="{{ note }}", fmt="text", engine="jinja2")
        result = call_action(mod.document_render_template, conn, ns(
            template_id=tpl,
            merge_data=_json.dumps({"note": "<i>y</i>"}),
            format="html",
        ))
        assert is_ok(result), result
        assert result["format"] == "html"
        assert "&lt;i&gt;" in result["rendered"]

    def test_missing_template_fails(self, conn, env):
        result = call_action(mod.document_render_template, conn, ns(
            template_id="nonexistent-id",
        ))
        assert is_error(result)

    def test_missing_template_id_fails(self, conn, env):
        result = call_action(mod.document_render_template, conn, ns(
            template_id=None,
        ))
        assert is_error(result)

    def test_invalid_format_fails(self, conn, env):
        tpl = seed_template(conn, env["company_id"], engine="jinja2")
        result = call_action(mod.document_render_template, conn, ns(
            template_id=tpl,
            format="pdf",
        ))
        assert is_error(result)

    def test_bad_merge_data_fails(self, conn, env):
        tpl = seed_template(conn, env["company_id"], engine="jinja2")
        result = call_action(mod.document_render_template, conn, ns(
            template_id=tpl,
            merge_data="{not valid json",
        ))
        assert is_error(result)

    # -- Sandbox security boundary -------------------------------------------
    # render-template executes user-authored template bodies server-side, so
    # the SandboxedEnvironment is the entire security boundary. These payloads
    # are the canonical Jinja2 SSTI -> RCE gadget chains (reach __subclasses__
    # / __globals__ / mro() and you can import os and run commands). The
    # sandbox must refuse every one of them. If anyone swaps SandboxedEnvironment
    # for a plain jinja2.Environment, these chains render successfully and
    # is_error flips to is_ok -- this test fails loudly, by design.
    SSTI_PAYLOADS = [
        "{{ ''.__class__.__mro__[1].__subclasses__() }}",
        "{{ ''.__class__.__base__.__subclasses__() }}",
        "{{ ''.__class__.mro()[1].__subclasses__() }}",
        "{{ self.__init__.__globals__ }}",
        "{{ cycler.__init__.__globals__ }}",
        "{{ ''.__class__.__init__.__globals__['os'].popen('id').read() }}",
        "{{ {}.__class__.__base__.__subclasses__() }}",
    ]

    @pytest.mark.parametrize("payload", SSTI_PAYLOADS)
    def test_sandbox_escape_blocked(self, conn, env, payload):
        """Each SSTI gadget chain must be refused by the sandbox and surfaced
        as a clean err(), never rendered into a successful response."""
        tpl = seed_template(conn, env["company_id"], name="Escape",
                            content=payload, fmt="text", engine="jinja2")
        result = call_action(mod.document_render_template, conn, ns(
            template_id=tpl,
        ))
        assert is_error(result), (
            f"sandbox failed to block SSTI payload {payload!r}: {result!r}"
        )
        # Belt-and-suspenders: even the error message must not have leaked
        # the subclass/globals listing the gadget was reaching for.
        import json as _json
        blob = _json.dumps(result)
        assert "<class 'subprocess.Popen'>" not in blob
        assert "os.system" not in blob

    def test_sandbox_escape_blocked_in_generate(self, conn, env):
        """The same boundary holds on the other render entry point:
        generate-from-template with a jinja2 engine must also refuse escapes."""
        tpl = seed_template(conn, env["company_id"], name="EscapeGen",
                            content="{{ ''.__class__.__mro__[1].__subclasses__() }}",
                            fmt="text", engine="jinja2")
        result = call_action(mod.document_generate_from_template, conn, ns(
            template_id=tpl,
            title="Escape Attempt",
            company_id=env["company_id"],
            merge_data="{}",
        ))
        assert is_error(result), result


# =============================================================================
# render-pdf (S8 chunk 2 — HTML -> PDF via WeasyPrint, lazy + mockable seam)
# =============================================================================

import builtins
import pdf as pdf_mod
from unittest.mock import patch


class TestRenderPdf:
    """render-pdf is a pure render: HTML in, PDF file out. WeasyPrint is a
    lazy import behind a single mockable seam (_render_html_to_pdf), so CI
    never needs WeasyPrint installed — every happy-path test patches the seam,
    exactly like the M8-A send-email tests patch _send_via_provider."""

    def _fake_seam(self, recorder, *, write=True):
        """Build a seam stand-in that records its args and writes a stub PDF."""
        def seam(html, output_path):
            recorder["html"] = html
            recorder["output_path"] = output_path
            if write:
                with open(output_path, "wb") as fh:
                    fh.write(b"%PDF-1.7 stub")
            return True, ""
        return seam

    def test_happy_passes_html_to_seam_and_writes_output(self, conn, tmp_path):
        """render-pdf hands the exact HTML to the seam and returns a path that
        was actually written."""
        rec = {}
        out = str(tmp_path / "invoice.pdf")
        with patch.object(pdf_mod, "_render_html_to_pdf", self._fake_seam(rec)):
            result = call_action(mod.document_render_pdf, conn, ns(
                html="<h1>Invoice INV-1</h1>", output_path=out,
            ))
        assert is_ok(result), result
        assert result["output_path"] == out
        assert rec["html"] == "<h1>Invoice INV-1</h1>"
        assert rec["output_path"] == out
        assert os.path.isfile(out)
        assert result["bytes_in"] == len("<h1>Invoice INV-1</h1>".encode("utf-8"))

    def test_default_storage_root_used_when_no_output_path(self, conn, tmp_path):
        """With no --output-path the PDF lands under ERPCLAW_PDF_STORAGE_ROOT."""
        rec = {}
        root = str(tmp_path / "pdf-root")
        os.environ["ERPCLAW_PDF_STORAGE_ROOT"] = root
        try:
            with patch.object(pdf_mod, "_render_html_to_pdf", self._fake_seam(rec)):
                result = call_action(mod.document_render_pdf, conn, ns(
                    html="<p>hi</p>",
                ))
        finally:
            os.environ.pop("ERPCLAW_PDF_STORAGE_ROOT", None)
        assert is_ok(result), result
        assert result["output_path"].startswith(root + os.sep)
        assert result["output_path"].endswith(".pdf")
        assert os.path.isfile(result["output_path"])

    def test_html_from_file_is_read(self, conn, tmp_path):
        """--html-from-file loads the HTML off disk and feeds it to the seam."""
        rec = {}
        src = tmp_path / "body.html"
        src.write_text("<p>From file</p>", encoding="utf-8")
        out = str(tmp_path / "fromfile.pdf")
        with patch.object(pdf_mod, "_render_html_to_pdf", self._fake_seam(rec)):
            result = call_action(mod.document_render_pdf, conn, ns(
                html_from_file=str(src), output_path=out,
            ))
        assert is_ok(result), result
        assert rec["html"] == "<p>From file</p>"

    def test_missing_both_html_args_fails(self, conn):
        result = call_action(mod.document_render_pdf, conn, ns())
        assert is_error(result)

    def test_both_html_args_fails(self, conn, tmp_path):
        src = tmp_path / "b.html"
        src.write_text("<p>x</p>", encoding="utf-8")
        result = call_action(mod.document_render_pdf, conn, ns(
            html="<p>inline</p>", html_from_file=str(src),
        ))
        assert is_error(result)

    def test_html_from_file_not_found_fails(self, conn, tmp_path):
        result = call_action(mod.document_render_pdf, conn, ns(
            html_from_file=str(tmp_path / "does-not-exist.html"),
        ))
        assert is_error(result)

    def test_oversize_html_rejected(self, conn, tmp_path):
        """HTML over --max-html-bytes is rejected before reaching the seam."""
        rec = {}
        with patch.object(pdf_mod, "_render_html_to_pdf", self._fake_seam(rec)):
            result = call_action(mod.document_render_pdf, conn, ns(
                html="x" * 100, max_html_bytes=10,
                output_path=str(tmp_path / "big.pdf"),
            ))
        assert is_error(result)
        assert rec == {}  # seam never invoked

    def test_seam_failure_surfaces_as_error(self, conn, tmp_path):
        """A seam that reports failure is surfaced as a clean err()."""
        def failing_seam(html, output_path):
            return False, "boom: bad css"
        with patch.object(pdf_mod, "_render_html_to_pdf", failing_seam):
            result = call_action(mod.document_render_pdf, conn, ns(
                html="<p>x</p>", output_path=str(tmp_path / "x.pdf"),
            ))
        assert is_error(result)

    # -- WeasyPrint-absent path (the lazy import) ----------------------------
    def test_seam_returns_clear_error_when_weasyprint_absent(self, tmp_path):
        """When WeasyPrint is not installed, the seam returns a clear,
        actionable error rather than raising — proving the lazy import. Forced
        deterministically so the result is identical whether or not WeasyPrint
        happens to be installed in this environment."""
        real_import = builtins.__import__

        def no_weasyprint(name, *a, **k):
            if name == "weasyprint" or name.startswith("weasyprint."):
                raise ImportError("No module named 'weasyprint'")
            return real_import(name, *a, **k)

        with patch.object(builtins, "__import__", side_effect=no_weasyprint):
            ok_flag, msg = pdf_mod._render_html_to_pdf("<p>x</p>", str(tmp_path / "x.pdf"))
        assert ok_flag is False
        assert "WeasyPrint not installed" in msg
        assert "pip install weasyprint" in msg

    def test_render_pdf_end_to_end_when_weasyprint_absent(self, conn, tmp_path):
        """End-to-end: with WeasyPrint absent, render-pdf returns the clear
        installation error (the real lazy-import seam, no patching of it)."""
        real_import = builtins.__import__

        def no_weasyprint(name, *a, **k):
            if name == "weasyprint" or name.startswith("weasyprint."):
                raise ImportError("No module named 'weasyprint'")
            return real_import(name, *a, **k)

        with patch.object(builtins, "__import__", side_effect=no_weasyprint):
            result = call_action(mod.document_render_pdf, conn, ns(
                html="<p>x</p>", output_path=str(tmp_path / "x.pdf"),
            ))
        assert is_error(result)
        blob = json.dumps(result)
        assert "WeasyPrint not installed" in blob


class TestEngineBranchBackwardCompat:
    """generate-from-template must branch on document_template.engine, and the
    legacy_replace path must render byte-identically to the pre-S8 behavior."""

    def test_legacy_replace_unchanged(self, conn, env):
        """A legacy_replace template renders exactly as the old str.replace loop:
        provided vars substituted, UNMATCHED {{placeholders}} left in place."""
        import json as _json
        tpl = seed_template(conn, env["company_id"], name="Legacy",
                            content="Hello {{name}}, order {{order_id}} ready.",
                            fmt="text", engine="legacy_replace")
        result = call_action(mod.document_generate_from_template, conn, ns(
            template_id=tpl,
            title="Legacy Doc",
            company_id=env["company_id"],
            merge_data=_json.dumps({"name": "Acme"}),  # order_id intentionally omitted
        ))
        assert is_ok(result), result
        doc = call_action(mod.document_get_document, conn, ns(
            document_id=result["document_id"],
        ))
        # str.replace semantics: name swapped, order_id placeholder untouched.
        assert doc["content"] == "Hello Acme, order {{order_id}} ready."

    def test_default_template_is_legacy(self, conn, env):
        """The default seeded template (no engine specified) is legacy_replace,
        so existing installs keep the old behavior with no opt-in."""
        result = call_action(mod.document_get_template, conn, ns(
            template_id=env["template_id"],
        ))
        assert is_ok(result), result
        assert result["engine"] == "legacy_replace"
        assert result["format"] == "text"

    def test_jinja2_branch_in_generate(self, conn, env):
        """An engine='jinja2' template routes generate-from-template through the
        Jinja2 render (control flow evaluated, not left literal)."""
        import json as _json
        tpl = seed_template(conn, env["company_id"], name="J2 Gen",
                            content="{% if ok %}YES{% else %}NO{% endif %} {{ who }}",
                            fmt="text", engine="jinja2")
        result = call_action(mod.document_generate_from_template, conn, ns(
            template_id=tpl,
            title="J2 Doc",
            company_id=env["company_id"],
            merge_data=_json.dumps({"ok": True, "who": "Acme"}),
        ))
        assert is_ok(result), result
        doc = call_action(mod.document_get_document, conn, ns(
            document_id=result["document_id"],
        ))
        assert doc["content"] == "YES Acme"


class TestAddTemplateEngine:
    def test_add_jinja2_template(self, conn, env):
        result = call_action(mod.document_add_template, conn, ns(
            company_id=env["company_id"],
            name="Opt-in J2",
            content="Hi {{ name }}",
            format="html",
            engine="jinja2",
        ))
        assert is_ok(result), result
        assert result["engine"] == "jinja2"
        assert result["format"] == "html"

    def test_add_invalid_engine_fails(self, conn, env):
        result = call_action(mod.document_add_template, conn, ns(
            company_id=env["company_id"],
            name="Bad Engine",
            content="x",
            engine="mustache",
        ))
        assert is_error(result)

    def test_add_invalid_format_fails(self, conn, env):
        result = call_action(mod.document_add_template, conn, ns(
            company_id=env["company_id"],
            name="Bad Format",
            content="x",
            format="xml",
        ))
        assert is_error(result)


# =============================================================================
# print-document (S8 chunk 3 — composite: render-template → HTML → render-pdf
# → persist a document row carrying its pdf_path)
# =============================================================================

import print_docs as print_mod
import print_wrappers as wrap_mod


class TestPrintDocument:
    """print-document ties the whole S8 pipeline together. The WeasyPrint seam
    is patched (same single seam as render-pdf), so these tests assert (a) the
    EXACT HTML handed to the renderer — proving the Jinja2 render + format→HTML
    conversion + autoescape — and (b) that a draft document row is persisted
    with its pdf_path pointing at the written PDF."""

    def _fake_seam(self, recorder, *, write=True):
        """Seam stand-in: records the HTML + path and writes a stub PDF."""
        def seam(html, output_path):
            recorder["html"] = html
            recorder["output_path"] = output_path
            if write:
                with open(output_path, "wb") as fh:
                    fh.write(b"%PDF-1.7 stub")
            return True, ""
        return seam

    def test_text_template_writes_pdf_and_document(self, conn, env, tmp_path):
        import json as _json
        tpl = seed_template(conn, env["company_id"], name="Print Text",
                            content="Invoice for {{ customer }}: {{ amount }}",
                            fmt="text", engine="jinja2")
        out = str(tmp_path / "doc.pdf")
        rec = {}
        with patch.object(print_mod._pdf, "_render_html_to_pdf", self._fake_seam(rec)):
            result = call_action(mod.document_print_document, conn, ns(
                template_id=tpl, title="INV-1 PDF", company_id=env["company_id"],
                merge_data=_json.dumps({"customer": "Acme", "amount": "100.00"}),
                output_path=out,
            ))
        assert is_ok(result), result
        assert result["doc_status"] == "draft"
        assert result["pdf_path"] == out
        assert os.path.isfile(out)
        # text format is escaped + wrapped in <pre> for the PDF render.
        assert "<pre>" in rec["html"]
        assert "Invoice for Acme: 100.00" in rec["html"]
        # The persisted document row carries content + pdf_path.
        doc = call_action(mod.document_get_document, conn, ns(
            document_id=result["document_id"],
        ))
        assert doc["content"] == "Invoice for Acme: 100.00"
        assert doc["pdf_path"] == out

    def test_html_template_autoescapes_merge_data(self, conn, env, tmp_path):
        """format=html → Jinja2 autoescape ON: unsafe merge data is escaped in
        the HTML that reaches the PDF renderer."""
        import json as _json
        tpl = seed_template(conn, env["company_id"], name="Print HTML",
                            content="<h1>{{ title }}</h1><p>{{ note }}</p>",
                            fmt="html", engine="jinja2")
        rec = {}
        with patch.object(print_mod._pdf, "_render_html_to_pdf", self._fake_seam(rec)):
            result = call_action(mod.document_print_document, conn, ns(
                template_id=tpl, title="HTML Doc", company_id=env["company_id"],
                merge_data=_json.dumps({"title": "Receipt",
                                        "note": "<script>alert(1)</script>"}),
                output_path=str(tmp_path / "h.pdf"),
            ))
        assert is_ok(result), result
        assert "<h1>Receipt</h1>" in rec["html"]
        assert "<script>" not in rec["html"]
        assert "&lt;script&gt;" in rec["html"]

    def test_markdown_converted_to_html(self, conn, env, tmp_path):
        """format=markdown → markdown-it converts the rendered body to HTML
        before the PDF render."""
        import json as _json
        tpl = seed_template(conn, env["company_id"], name="Print MD",
                            content="# {{ heading }}\n\nHello **{{ who }}**.",
                            fmt="markdown", engine="jinja2")
        rec = {}
        with patch.object(print_mod._pdf, "_render_html_to_pdf", self._fake_seam(rec)):
            result = call_action(mod.document_print_document, conn, ns(
                template_id=tpl, title="MD Doc", company_id=env["company_id"],
                merge_data=_json.dumps({"heading": "Statement", "who": "Acme"}),
                output_path=str(tmp_path / "m.pdf"),
            ))
        assert is_ok(result), result
        assert "<h1>Statement</h1>" in rec["html"]
        assert "<strong>Acme</strong>" in rec["html"]

    def test_default_storage_root_used_when_no_output_path(self, conn, env, tmp_path):
        root = str(tmp_path / "pdf-root")
        os.environ["ERPCLAW_PDF_STORAGE_ROOT"] = root
        rec = {}
        try:
            tpl = seed_template(conn, env["company_id"], name="Print Default",
                                content="x", fmt="text", engine="jinja2")
            with patch.object(print_mod._pdf, "_render_html_to_pdf", self._fake_seam(rec)):
                result = call_action(mod.document_print_document, conn, ns(
                    template_id=tpl, title="Default Doc", company_id=env["company_id"],
                ))
        finally:
            os.environ.pop("ERPCLAW_PDF_STORAGE_ROOT", None)
        assert is_ok(result), result
        assert result["pdf_path"].startswith(root + os.sep)
        assert os.path.isfile(result["pdf_path"])

    def test_format_override(self, conn, env, tmp_path):
        """--format overrides the template's stored format (text→html escapes)."""
        import json as _json
        tpl = seed_template(conn, env["company_id"], name="Print Override",
                            content="{{ note }}", fmt="text", engine="jinja2")
        rec = {}
        with patch.object(print_mod._pdf, "_render_html_to_pdf", self._fake_seam(rec)):
            result = call_action(mod.document_print_document, conn, ns(
                template_id=tpl, title="Override Doc", company_id=env["company_id"],
                merge_data=_json.dumps({"note": "<i>y</i>"}), format="html",
                output_path=str(tmp_path / "o.pdf"),
            ))
        assert is_ok(result), result
        assert result["format"] == "html"
        # html path: no <pre> wrapping, autoescaped merge data.
        assert "<pre>" not in rec["html"]
        assert "&lt;i&gt;" in rec["html"]

    def test_missing_template_id_fails(self, conn, env):
        result = call_action(mod.document_print_document, conn, ns(
            template_id=None, title="X", company_id=env["company_id"],
        ))
        assert is_error(result)

    def test_missing_title_fails(self, conn, env):
        tpl = seed_template(conn, env["company_id"], engine="jinja2")
        result = call_action(mod.document_print_document, conn, ns(
            template_id=tpl, title=None, company_id=env["company_id"],
        ))
        assert is_error(result)

    def test_missing_company_fails(self, conn, env):
        tpl = seed_template(conn, env["company_id"], engine="jinja2")
        result = call_action(mod.document_print_document, conn, ns(
            template_id=tpl, title="X", company_id=None,
        ))
        assert is_error(result)

    def test_nonexistent_template_fails(self, conn, env):
        result = call_action(mod.document_print_document, conn, ns(
            template_id="nonexistent-id", title="X", company_id=env["company_id"],
        ))
        assert is_error(result)

    def test_inactive_template_fails(self, conn, env):
        tpl = seed_template(conn, env["company_id"], name="Inactive",
                            content="x", fmt="text", engine="jinja2")
        call_action(mod.document_update_template, conn, ns(
            template_id=tpl, is_active="0",
        ))
        result = call_action(mod.document_print_document, conn, ns(
            template_id=tpl, title="X", company_id=env["company_id"],
        ))
        assert is_error(result)

    def test_seam_failure_surfaces_as_error_no_document_written(self, conn, env, tmp_path):
        """A PDF render failure is a clean err() and leaves NO document row."""
        def failing_seam(html, output_path):
            return False, "boom: bad css"
        tpl = seed_template(conn, env["company_id"], name="Fail",
                            content="x", fmt="text", engine="jinja2")
        before = call_action(mod.document_list_documents, conn, ns(
            company_id=env["company_id"],
        ))["total_count"]
        with patch.object(print_mod._pdf, "_render_html_to_pdf", failing_seam):
            result = call_action(mod.document_print_document, conn, ns(
                template_id=tpl, title="Fail Doc", company_id=env["company_id"],
                output_path=str(tmp_path / "f.pdf"),
            ))
        assert is_error(result)
        after = call_action(mod.document_list_documents, conn, ns(
            company_id=env["company_id"],
        ))["total_count"]
        assert after == before  # PDF failed before any DB write

    def test_oversize_rejected_before_seam(self, conn, env, tmp_path):
        rec = {}
        tpl = seed_template(conn, env["company_id"], name="Big",
                            content="{{ blob }}", fmt="text", engine="jinja2")
        import json as _json
        with patch.object(print_mod._pdf, "_render_html_to_pdf", self._fake_seam(rec)):
            result = call_action(mod.document_print_document, conn, ns(
                template_id=tpl, title="Big Doc", company_id=env["company_id"],
                merge_data=_json.dumps({"blob": "x" * 200}), max_html_bytes=10,
                output_path=str(tmp_path / "big.pdf"),
            ))
        assert is_error(result)
        assert rec == {}  # seam never invoked

    def test_sandbox_escape_blocked_in_print(self, conn, env, tmp_path):
        """The SSTI boundary holds on this third render entry point too: a
        gadget-chain template must err, never render into a PDF/document."""
        tpl = seed_template(conn, env["company_id"], name="EscapePrint",
                            content="{{ ''.__class__.__mro__[1].__subclasses__() }}",
                            fmt="text", engine="jinja2")
        rec = {}
        with patch.object(print_mod._pdf, "_render_html_to_pdf", self._fake_seam(rec)):
            result = call_action(mod.document_print_document, conn, ns(
                template_id=tpl, title="Escape", company_id=env["company_id"],
                output_path=str(tmp_path / "e.pdf"),
            ))
        assert is_error(result), result
        assert rec == {}  # render blocked before the PDF seam


# =============================================================================
# print-invoice / print-purchase-order / print-packing-slip (S8 chunk 4)
# =============================================================================

class TestPrintWrappers:
    """Wrappers build merge data from submitted parent docs and delegate to
    print_document. The WeasyPrint seam is patched on print_docs._pdf (same
    seam as chunks 2 and 3). Each test checks (a) the wrapper rejects draft
    parents and (b) the happy path produces a document row with a pdf_path."""

    def _fake_seam(self, recorder, *, write=True):
        def seam(html, output_path):
            recorder["html"] = html
            recorder["output_path"] = output_path
            if write:
                with open(output_path, "wb") as fh:
                    fh.write(b"%PDF-1.7 stub")
            return True, ""
        return seam

    # ------------------------------------------------------------------ invoice
    def test_print_invoice_happy(self, conn, env, tmp_path):
        customer_id = seed_customer(conn, env["company_id"])
        item_id = seed_item(conn)
        inv_id = seed_sales_invoice(conn, env["company_id"], customer_id,
                                    status="submitted", items=[item_id])
        rec = {}
        with patch.object(print_mod._pdf, "_render_html_to_pdf", self._fake_seam(rec)):
            result = call_action(mod.document_print_invoice, conn, ns(
                invoice_id=inv_id, company_id=env["company_id"],
                output_path=str(tmp_path / "inv.pdf"),
            ))
        assert is_ok(result), result
        assert os.path.isfile(result["pdf_path"])
        assert "1,000.00" in rec["html"] or "1000" in rec["html"]

    def test_print_invoice_draft_fails(self, conn, env):
        customer_id = seed_customer(conn, env["company_id"])
        inv_id = seed_sales_invoice(conn, env["company_id"], customer_id, status="draft")
        result = call_action(mod.document_print_invoice, conn, ns(
            invoice_id=inv_id, company_id=env["company_id"],
        ))
        assert is_error(result)
        assert "draft" in result.get("message", "").lower()

    def test_print_invoice_missing_id_fails(self, conn, env):
        result = call_action(mod.document_print_invoice, conn, ns(
            invoice_id=None, company_id=env["company_id"],
        ))
        assert is_error(result)

    def test_print_invoice_nonexistent_fails(self, conn, env):
        result = call_action(mod.document_print_invoice, conn, ns(
            invoice_id="nonexistent-inv", company_id=env["company_id"],
        ))
        assert is_error(result)

    # ---------------------------------------------------------- purchase order
    def test_print_purchase_order_happy(self, conn, env, tmp_path):
        supplier_id = seed_supplier(conn, env["company_id"])
        item_id = seed_item(conn)
        po_id = seed_purchase_order(conn, env["company_id"], supplier_id,
                                    status="confirmed", items=[item_id])
        rec = {}
        with patch.object(print_mod._pdf, "_render_html_to_pdf", self._fake_seam(rec)):
            result = call_action(mod.document_print_purchase_order, conn, ns(
                po_id=po_id, company_id=env["company_id"],
                output_path=str(tmp_path / "po.pdf"),
            ))
        assert is_ok(result), result
        assert os.path.isfile(result["pdf_path"])

    def test_print_purchase_order_draft_fails(self, conn, env):
        supplier_id = seed_supplier(conn, env["company_id"])
        po_id = seed_purchase_order(conn, env["company_id"], supplier_id, status="draft")
        result = call_action(mod.document_print_purchase_order, conn, ns(
            po_id=po_id, company_id=env["company_id"],
        ))
        assert is_error(result)

    def test_print_purchase_order_missing_id_fails(self, conn, env):
        result = call_action(mod.document_print_purchase_order, conn, ns(
            po_id=None, company_id=env["company_id"],
        ))
        assert is_error(result)

    # ----------------------------------------------------------- packing slip
    def test_print_packing_slip_happy(self, conn, env, tmp_path):
        customer_id = seed_customer(conn, env["company_id"])
        item_id = seed_item(conn)
        _, slip_id = seed_delivery_note_and_packing_slip(
            conn, env["company_id"], customer_id, item_id
        )
        rec = {}
        with patch.object(print_mod._pdf, "_render_html_to_pdf", self._fake_seam(rec)):
            result = call_action(mod.document_print_packing_slip, conn, ns(
                slip_id=slip_id, company_id=env["company_id"],
                output_path=str(tmp_path / "slip.pdf"),
            ))
        assert is_ok(result), result
        assert os.path.isfile(result["pdf_path"])

    def test_print_packing_slip_draft_dn_fails(self, conn, env):
        customer_id = seed_customer(conn, env["company_id"])
        item_id = seed_item(conn)
        dn_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO delivery_note(id, customer_id, posting_date, status, company_id) "
            "VALUES(?,?,?,?,?)",
            (dn_id, customer_id, "2026-06-01", "draft", env["company_id"]),
        )
        dni_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO delivery_note_item(id, delivery_note_id, item_id, quantity, uom) "
            "VALUES(?,?,?,?,?)",
            (dni_id, dn_id, item_id, "1", "ea"),
        )
        slip_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO packing_slip(id, delivery_note_id, posting_date, company_id) VALUES(?,?,?,?)",
            (slip_id, dn_id, "2026-06-01", env["company_id"]),
        )
        conn.execute(
            "INSERT INTO packing_slip_item(id, packing_slip_id, item_id, delivery_note_item_id, qty_packed) "
            "VALUES(?,?,?,?,?)",
            (str(uuid.uuid4()), slip_id, item_id, dni_id, "1"),
        )
        conn.commit()
        result = call_action(mod.document_print_packing_slip, conn, ns(
            slip_id=slip_id, company_id=env["company_id"],
        ))
        assert is_error(result)

    def test_print_packing_slip_missing_id_fails(self, conn, env):
        result = call_action(mod.document_print_packing_slip, conn, ns(
            slip_id=None, company_id=env["company_id"],
        ))
        assert is_error(result)

    # --------------------------------------------------------------- seed-defaults
    def test_seed_defaults_creates_three_templates(self, conn, env):
        result = call_action(mod.document_seed_defaults, conn, ns(
            company_id=env["company_id"],
        ))
        assert is_ok(result), result
        seeded = result["seeded"]
        assert len(seeded) == 3
        names = {s["name"] for s in seeded}
        assert "Default Invoice" in names
        assert "Default Purchase Order" in names
        assert "Default Packing Slip" in names
        assert all(s["action"] == "seeded" for s in seeded)

    def test_seed_defaults_idempotent(self, conn, env):
        call_action(mod.document_seed_defaults, conn, ns(company_id=env["company_id"]))
        result = call_action(mod.document_seed_defaults, conn, ns(company_id=env["company_id"]))
        assert is_ok(result), result
        assert all(s["action"] == "skipped" for s in result["seeded"])

    def test_seed_defaults_missing_company_fails(self, conn, env):
        result = call_action(mod.document_seed_defaults, conn, ns(company_id=None))
        assert is_error(result)


# =============================================================================
# Status
# =============================================================================

class TestStatus:
    def test_status(self, conn, env):
        result = call_action(mod.status, conn, ns())
        assert is_ok(result), result
        assert result["skill"] == "erpclaw-documents"
        assert result["actions_available"] == 32
        assert "documents" in result["domains"]
        assert "templates" in result["domains"]
        assert "pdf" in result["domains"]
        assert "print" in result["domains"]
