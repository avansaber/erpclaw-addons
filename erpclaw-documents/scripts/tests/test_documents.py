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
import pytest
from documents_helpers import (
    call_action, ns, is_error, is_ok, load_db_query,
    seed_company, seed_naming_series, seed_document, seed_template,
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
# Status
# =============================================================================

class TestStatus:
    def test_status(self, conn, env):
        result = call_action(mod.status, conn, ns())
        assert is_ok(result), result
        assert result["skill"] == "erpclaw-documents"
        assert result["actions_available"] == 25
        assert "documents" in result["domains"]
        assert "templates" in result["domains"]
