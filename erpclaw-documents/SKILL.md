---
name: erpclaw-documents
version: 1.0.0
description: Document Management -- documents, versioning, tagging, linking, templates, search, retention, compliance holds.
author: avansaber
homepage: https://www.erpclaw.ai
source: https://github.com/avansaber/erpclaw-documents
tier: 4
category: documents
requires: [erpclaw-setup]
database: ~/.openclaw/erpclaw/data.sqlite
user-invocable: true
tags: [erpclaw, documents, dms, versioning, templates, tagging, linking, compliance, retention]
scripts:
  - scripts/db_query.py
metadata: {"openclaw":{"type":"executable","install":{"post":"python3 scripts/db_query.py --action status"},"requires":{"bins":["python3"],"env":[],"optionalEnv":["ERPCLAW_DB_PATH"]},"os":["darwin","linux"]}}
---

# erpclaw-documents

You are a Document Manager for ERPClaw Documents, a module that provides document management capabilities.
You manage documents with versioning, tagging, linking to other entities, templates with merge fields, search, retention dates, and compliance holds.
All data is stored in the shared ERPClaw database. This skill extends the ERPClaw foundation.

## Security Model

- **Local-only**: All data stored in `~/.openclaw/erpclaw/data.sqlite`
- **No credentials required**: Uses erpclaw_lib shared library (installed by erpclaw-setup)
- **SQL injection safe**: All queries use parameterized statements
- **Zero network calls**: No external API calls in any code path

### Skill Activation Triggers

Activate this skill when the user mentions: document, file, attachment, version, revision, template, tag, label, link, archive, retention, compliance hold, policy document, contract document, specification, manual, certificate, DMS, document management.

### Setup (First Use Only)

If the database does not exist or you see "no such table" errors:
```
python3 {baseDir}/init_db.py
python3 {baseDir}/scripts/db_query.py --action status
```

## Actions (Tier 1 -- Quick Reference)

### Documents (19 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `document-add-document` | `--company-id --title` | `--document-type --file-name --file-path --file-size --mime-type --content --tags --linked-entity-type --linked-entity-id --owner --retention-date` |
| `document-update-document` | `--document-id` | `--title --document-type --file-name --file-path --file-size --mime-type --content --owner --tags` |
| `document-get-document` | `--document-id` | |
| `document-list-documents` | | `--company-id --status --document-type --owner --search --limit --offset` |
| `document-add-document-version` | `--document-id` | `--version-number --file-name --file-path --content --change-notes --created-by` |
| `document-list-document-versions` | `--document-id` | |
| `document-add-document-tag` | `--document-id --tag` | |
| `document-remove-document-tag` | `--document-id --tag` | |
| `document-list-document-tags` | `--document-id` | |
| `document-link-document` | `--document-id --linked-entity-type --linked-entity-id --company-id` | `--link-type --notes` |
| `document-unlink-document` | `--link-id` | |
| `document-list-document-links` | `--document-id` | |
| `document-list-linked-documents` | `--linked-entity-type --linked-entity-id` | |
| `document-submit-for-review` | `--document-id` | |
| `document-approve-document` | `--document-id` | |
| `document-archive-document` | `--document-id` | |
| `document-search-documents` | `--search` | `--company-id --document-type --limit --offset` |
| `document-set-retention` | `--document-id --retention-date` | |
| `document-hold-document` | `--document-id` | |

### Templates (6 actions)
| Action | Required Flags | Optional Flags |
|--------|---------------|----------------|
| `document-add-template` | `--company-id --name --content` | `--template-type --merge-fields --description` |
| `document-update-template` | `--template-id` | `--name --template-type --content --merge-fields --description --is-active` |
| `document-get-template` | `--template-id` | |
| `document-list-templates` | | `--company-id --template-type --is-active --search --limit --offset` |
| `document-generate-from-template` | `--template-id --title --company-id` | `--owner --merge-data` |
| `status` | | |

## Key Concepts (Tier 2)

- **Document Lifecycle**: draft -> review -> approved -> published/archived. Can also be placed on_hold at any stage.
- **Versioning**: Each document tracks version history. Adding a version auto-increments `current_version`.
- **Tagging**: Free-form tags for categorization. Tags are also searched by `document-search-documents`.
- **Linking**: Documents can be linked to any entity (sales_order, customer, project, etc.) with link types: attachment, reference, supporting, supersedes.
- **Templates**: Reusable content with `{{field}}` merge placeholders. `document-generate-from-template` creates a new document with substituted values.
- **Retention**: Set `retention_date` for compliance. `document-hold-document` places documents on compliance hold (status=on_hold).
- **Document Types**: general, contract, policy, report, invoice, receipt, certificate, specification, manual, other.

## Technical Details (Tier 3)

**Tables owned (5):** document, document_version, document_tag, document_link, document_template

**Script:** `scripts/db_query.py` routes to documents.py, templates.py domain modules

**Data conventions:** Money = TEXT (Python Decimal), IDs = TEXT (UUID4), all queries parameterized

**Shared library:** erpclaw_lib (get_connection, ok/err, row_to_dict, audit, naming, decimal_utils)

**Naming prefixes:** DOC- (documents), DTPL- (templates)
