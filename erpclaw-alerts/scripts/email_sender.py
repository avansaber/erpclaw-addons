"""ERPClaw Alerts -- email substrate (M8 phase A).

Email sender + queue: account config, templates, outbox enqueue, and the
cron-callable queue worker. SMTP-primary (N1); SES/Mailgun providers stub to the
same _send_via_provider seam so tests can patch one place. The SMTP password is
NOT stored in the DB — it lives in the encrypted credentials store keyed
'email_account:<id>'. email_log is append-only.

Imported by db_query.py (unified router) as EMAIL_ACTIONS.
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone, timedelta

try:
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.db import DEFAULT_DB_PATH
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.query import Q, P, Table, fn, insert_row, update_row
    from erpclaw_lib.vendor.pypika.terms import LiteralValue
    from erpclaw_lib import credentials as creds
except ImportError:
    DEFAULT_DB_PATH = "~/.openclaw/erpclaw/data.sqlite"

_now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

VALID_PROVIDERS = ("smtp", "ses", "mailgun")
# Exponential backoff between send attempts (seconds): 1m, 5m, 30m, 2h, 8h.
# After this many failed attempts the row is marked 'failed'.
_BACKOFF_SECONDS = [60, 300, 1800, 7200, 28800]
_MAX_ATTEMPTS = len(_BACKOFF_SECONDS)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _validate_company(conn, company_id):
    if not company_id:
        err("--company-id is required")
    if not conn.execute("SELECT 1 FROM company WHERE id = ?", (company_id,)).fetchone():
        err(f"Company {company_id} not found")


def _cred_key(account_id):
    return f"email_account:{account_id}"


def _render(text, merge_vars):
    """Minimal {{key}} merge-var substitution (Jinja2 arrives with S8/PDF)."""
    if not text or not merge_vars:
        return text or ""
    out = text
    for k, v in merge_vars.items():
        out = out.replace("{{" + str(k) + "}}", str(v))
        out = out.replace("{{ " + str(k) + " }}", str(v))
    return out


def _default_account(conn, company_id, from_account_id=None):
    """Resolve the email_account to send from: explicit --from-account, else the
    company default, else err."""
    t = Table("email_account")
    if from_account_id:
        row = conn.execute(
            "SELECT * FROM email_account WHERE id = ? AND is_active = 1", (from_account_id,)).fetchone()
        if not row:
            err(f"Email account {from_account_id} not found or inactive")
        return row_to_dict(row)
    row = conn.execute(
        "SELECT * FROM email_account WHERE company_id = ? AND is_default = 1 AND is_active = 1 LIMIT 1",
        (company_id,)).fetchone() if company_id else None
    if not row:
        row = conn.execute(
            "SELECT * FROM email_account WHERE is_default = 1 AND is_active = 1 LIMIT 1").fetchone()
    if not row:
        err("No default email account configured; pass --from-account or set one with set-default-email-account")
    return row_to_dict(row)


def _send_via_provider(account, password, to_address, subject, body_text, body_html):
    """Send one message. Returns (ok: bool, message_id_or_error: str).

    The single seam tests patch (no real SMTP/SES in CI). Real SMTP path uses
    smtplib; SES/Mailgun are not wired in phase A (return a clear error).
    """
    provider = account.get("provider", "smtp")
    if provider != "smtp":
        return False, f"provider '{provider}' not yet implemented (phase A is SMTP-only)"
    cfg = json.loads(account.get("config_json") or "{}")
    host = cfg.get("host")
    if not host:
        return False, "SMTP config missing 'host' in config_json"
    port = int(cfg.get("port", 587))
    use_tls = bool(cfg.get("use_tls", True))
    username = cfg.get("username")
    import smtplib
    from email.message import EmailMessage
    msg = EmailMessage()
    msg["From"] = account["from_address"]
    msg["To"] = to_address
    msg["Subject"] = subject
    if account.get("reply_to_address"):
        msg["Reply-To"] = account["reply_to_address"]
    msg.set_content(body_text or " ")
    if body_html:
        msg.add_alternative(body_html, subtype="html")
    try:
        with smtplib.SMTP(host, port, timeout=30) as server:
            if use_tls:
                server.starttls()
            if username and password:
                server.login(username, password)
            server.send_message(msg)
        return True, msg.get("Message-ID") or f"smtp-{uuid.uuid4()}"
    except Exception as e:  # noqa: BLE001 — surface the provider error to the row
        return False, str(e)


def _log_event(conn, outbox_id, event_type, payload=None):
    conn.execute(
        "INSERT INTO email_log (id, email_outbox_id, event_type, event_at, payload_json) "
        "VALUES (?, ?, ?, ?, ?)",
        (str(uuid.uuid4()), outbox_id, event_type, _now_iso(),
         json.dumps(payload) if payload else None))


# ---------------------------------------------------------------------------
# Account actions
# ---------------------------------------------------------------------------
def add_email_account(conn, args):
    """Register an SMTP/SES/Mailgun sending account. SMTP password (--smtp-password)
    is stored encrypted, NOT in the DB."""
    _validate_company(conn, getattr(args, "company_id", None))
    name = getattr(args, "name", None)
    from_address = getattr(args, "from_address", None) or getattr(args, "from_", None)
    if not name or not from_address:
        err("--name and --from-address are required")
    provider = (getattr(args, "provider", None) or "smtp").lower()
    if provider not in VALID_PROVIDERS:
        err(f"--provider must be one of: {', '.join(VALID_PROVIDERS)}")
    config_json = getattr(args, "config_json", None) or "{}"
    try:
        json.loads(config_json)
    except (json.JSONDecodeError, TypeError):
        err("--config-json must be valid JSON")
    acct_id = str(uuid.uuid4())
    is_default = 1 if getattr(args, "is_default", False) else 0
    if is_default:
        conn.execute("UPDATE email_account SET is_default = 0 WHERE company_id = ?",
                     (args.company_id,))
    sql, _ = insert_row("email_account", {
        "id": P(), "name": P(), "provider": P(), "from_address": P(),
        "reply_to_address": P(), "is_default": P(), "is_active": P(),
        "config_json": P(), "company_id": P(),
    })
    conn.execute(sql, (acct_id, name, provider, from_address,
                       getattr(args, "reply_to", None), is_default, 1,
                       config_json, args.company_id))
    pw = getattr(args, "smtp_password", None)
    if pw:
        creds.set_credential(_cred_key(acct_id), pw)
    audit(conn, "erpclaw-alerts", "add-email-account", "email_account", acct_id,
          new_values={"name": name, "provider": provider})
    conn.commit()
    ok({"result": "registered", "email_account_id": acct_id, "provider": provider,
        "is_default": bool(is_default), "credential_stored": bool(pw)})


def set_default_email_account(conn, args):
    """Mark an account as the company default (unsets the others)."""
    acct_id = getattr(args, "account_id", None)
    if not acct_id:
        err("--account-id is required")
    row = conn.execute("SELECT company_id FROM email_account WHERE id = ?", (acct_id,)).fetchone()
    if not row:
        err(f"Email account {acct_id} not found")
    conn.execute("UPDATE email_account SET is_default = 0 WHERE company_id = ?", (row["company_id"],))
    conn.execute("UPDATE email_account SET is_default = 1, updated_at = ? WHERE id = ?",
                 (_now_iso(), acct_id))
    conn.commit()
    ok({"result": "default_set", "email_account_id": acct_id})


def test_email_account(conn, args):
    """Send a probe email and record the health result on the account."""
    acct_id = getattr(args, "account_id", None)
    to = getattr(args, "to", None)
    if not acct_id or not to:
        err("--account-id and --to are required")
    row = conn.execute("SELECT * FROM email_account WHERE id = ?", (acct_id,)).fetchone()
    if not row:
        err(f"Email account {acct_id} not found")
    account = row_to_dict(row)
    pw = creds.get_credential(_cred_key(acct_id))
    sent, info = _send_via_provider(account, pw, to,
                                    "ERPClaw email account test",
                                    "This is a test message from ERPClaw.", "")
    status = "ok" if sent else "error"
    conn.execute("UPDATE email_account SET last_health_check_at = ?, last_health_status = ?, "
                 "updated_at = ? WHERE id = ?", (_now_iso(), status, _now_iso(), acct_id))
    conn.commit()
    if not sent:
        err(f"Test send failed: {info}")
    ok({"result": "ok", "email_account_id": acct_id, "provider_message_id": info})


# ---------------------------------------------------------------------------
# Template actions
# ---------------------------------------------------------------------------
def add_email_template(conn, args):
    """Create a reusable email template with {{merge-var}} placeholders."""
    name = getattr(args, "name", None)
    if not name:
        err("--name is required")
    tid = str(uuid.uuid4())
    sql, _ = insert_row("email_template", {
        "id": P(), "name": P(), "subject": P(), "body_html": P(),
        "body_text": P(), "merge_field_list_json": P(), "language": P(),
        "is_active": P(), "company_id": P(),
    })
    conn.execute(sql, (
        tid, name, getattr(args, "subject", None) or "",
        getattr(args, "body_html", None) or "", getattr(args, "body_text", None) or "",
        getattr(args, "merge_field_list_json", None) or "[]",
        getattr(args, "language", None) or "en", 1,
        getattr(args, "company_id", None)))
    audit(conn, "erpclaw-alerts", "add-email-template", "email_template", tid,
          new_values={"name": name})
    conn.commit()
    ok({"result": "created", "email_template_id": tid, "name": name})


def list_email_templates(conn, args):
    t = Table("email_template")
    q = Q.from_(t).select(t.star).where(t.is_active == 1)
    params = ()
    if getattr(args, "company_id", None):
        q = q.where((t.company_id == P()) | (t.company_id.isnull()))
        params = (args.company_id,)
    rows = conn.execute(q.orderby(t.name).get_sql(), params).fetchall()
    ok({"email_templates": [row_to_dict(r) for r in rows], "count": len(rows)})


# ---------------------------------------------------------------------------
# Send + queue
# ---------------------------------------------------------------------------
def send_email(conn, args):
    """Enqueue one email to the outbox (sent later by process-email-queue).

    Either --template-id OR inline (--subject + --body-text/--body-html), not both."""
    to = getattr(args, "to", None)
    if not to:
        err("--to is required")
    template_id = getattr(args, "template_id", None)
    subject = getattr(args, "subject", None)
    body_text = getattr(args, "body_text", None)
    body_html = getattr(args, "body_html", None)
    if template_id and (subject or body_text or body_html):
        err("Provide either --template-id OR inline --subject/--body-*, not both")
    if not template_id and not (subject or body_text or body_html):
        err("Provide --template-id or an inline --subject/--body-text")

    merge_vars = {}
    mv = getattr(args, "merge_vars", None)
    if mv:
        try:
            merge_vars = json.loads(mv)
        except (json.JSONDecodeError, TypeError):
            err("--merge-vars must be a JSON object")

    company_id = getattr(args, "company_id", None)
    account = _default_account(conn, company_id, getattr(args, "from_account", None))

    if template_id:
        tpl = conn.execute("SELECT * FROM email_template WHERE id = ? AND is_active = 1",
                           (template_id,)).fetchone()
        if not tpl:
            err(f"Email template {template_id} not found or inactive")
        tpl = row_to_dict(tpl)
        subject = _render(tpl["subject"], merge_vars)
        body_text = _render(tpl["body_text"], merge_vars)
        body_html = _render(tpl["body_html"], merge_vars)
    else:
        subject = _render(subject or "", merge_vars)
        body_text = _render(body_text or "", merge_vars)
        body_html = _render(body_html or "", merge_vars)

    outbox_id = str(uuid.uuid4())
    sql, _ = insert_row("email_outbox", {
        "id": P(), "to_address": P(), "from_account_id": P(), "subject": P(),
        "body_html": P(), "body_text": P(), "template_id": P(), "merge_vars_json": P(),
        "status": P(), "next_attempt_at": P(), "company_id": P(),
    })
    conn.execute(sql, (outbox_id, to, account["id"], subject, body_html, body_text,
                       template_id, json.dumps(merge_vars), "queued", _now_iso(), company_id))
    _log_event(conn, outbox_id, "queued")
    conn.commit()
    ok({"result": "queued", "email_outbox_id": outbox_id, "to": to,
        "from_account_id": account["id"]})


def process_email_queue(conn, args):
    """Cron worker: send queued/retry emails whose next_attempt_at has passed.
    Exponential backoff on failure; marks 'failed' after max attempts. Writes an
    append-only email_log event per outcome."""
    limit = int(getattr(args, "limit", None) or 25)
    now = _now_iso()
    t = Table("email_outbox")
    q = (Q.from_(t).select(t.star)
         .where(t.status.isin(["queued", "retry"]))
         .where((t.next_attempt_at.isnull()) | (t.next_attempt_at <= P()))
         .orderby(t.created_at).limit(limit))
    params = [now]
    if getattr(args, "from_account", None):
        q = q.where(t.from_account_id == P())
        params.append(args.from_account)
    rows = [row_to_dict(r) for r in conn.execute(q.get_sql(), tuple(params)).fetchall()]

    sent_n = failed_n = retry_n = 0
    for ob in rows:
        conn.execute("UPDATE email_outbox SET status = 'sending', updated_at = ? WHERE id = ?",
                     (_now_iso(), ob["id"]))
        acct = conn.execute("SELECT * FROM email_account WHERE id = ?",
                            (ob["from_account_id"],)).fetchone()
        if not acct:
            conn.execute("UPDATE email_outbox SET status='failed', error_message=?, updated_at=? WHERE id=?",
                         ("from-account missing", _now_iso(), ob["id"]))
            _log_event(conn, ob["id"], "failed", {"error": "from-account missing"})
            failed_n += 1
            continue
        acct = row_to_dict(acct)
        pw = creds.get_credential(_cred_key(acct["id"]))
        ok_sent, info = _send_via_provider(acct, pw, ob["to_address"], ob["subject"],
                                           ob["body_text"], ob["body_html"])
        attempt = int(ob["attempt_count"]) + 1
        if ok_sent:
            conn.execute("UPDATE email_outbox SET status='sent', attempt_count=?, sent_at=?, "
                         "provider_message_id=?, error_message=NULL, updated_at=? WHERE id=?",
                         (attempt, _now_iso(), info, _now_iso(), ob["id"]))
            _log_event(conn, ob["id"], "sent", {"provider_message_id": info})
            sent_n += 1
        elif attempt >= _MAX_ATTEMPTS:
            conn.execute("UPDATE email_outbox SET status='failed', attempt_count=?, error_message=?, "
                         "updated_at=? WHERE id=?", (attempt, info, _now_iso(), ob["id"]))
            _log_event(conn, ob["id"], "failed", {"error": info, "attempts": attempt})
            failed_n += 1
        else:
            delay = _BACKOFF_SECONDS[attempt - 1]
            next_at = (datetime.now(timezone.utc) + timedelta(seconds=delay)).strftime("%Y-%m-%dT%H:%M:%SZ")
            conn.execute("UPDATE email_outbox SET status='retry', attempt_count=?, next_attempt_at=?, "
                         "error_message=?, updated_at=? WHERE id=?",
                         (attempt, next_at, info, _now_iso(), ob["id"]))
            _log_event(conn, ob["id"], "retry", {"error": info, "next_attempt_at": next_at})
            retry_n += 1
    conn.commit()
    ok({"processed": len(rows), "sent": sent_n, "failed": failed_n, "retry": retry_n})


EMAIL_ACTIONS = {
    "add-email-account": add_email_account,
    "set-default-email-account": set_default_email_account,
    "test-email-account": test_email_account,
    "add-email-template": add_email_template,
    "list-email-templates": list_email_templates,
    "send-email": send_email,
    "process-email-queue": process_email_queue,
}
