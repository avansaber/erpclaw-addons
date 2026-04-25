"""shopify-connect: pair a Shopify App Store install with this ERPClaw.

Flow:
  1. Run the v1.1 schema migration if needed (lazy upgrade for existing
     v1.0.0 installs that never re-ran init_db.py).
  2. HTTPS GET to the Worker /pair/:code to retrieve OAuth token +
     per-shop HMAC secret.
  3. Resolve --company-id: auto-use the single company if the user did
     not pass one, otherwise fail with a clear list.
  4. Call accounts._add_account_core() to create the 14 GL accounts and
     insert the shopify_account row with pairing_method='oauth' and the
     encrypted HMAC secret.
  5. Detect whether ERPClaw has a long-lived process running; set
     status_mode accordingly so the embedded UI renders the right
     badge (active / scheduled / on-demand).

Usage:
  --action shopify-connect --pairing-code ABC-XYZ
                            [--company-id <id>]
                            [--erpclaw-url https://my.public.url]
                            [--worker-url https://shopify-staging.erpclaw.ai]
"""
import json
import os
import socket
import ssl
import sys
import urllib.error
import urllib.request

# Make erpclaw_lib importable the same way as every other action.
LIB_PATH = os.path.expanduser("~/.openclaw/erpclaw/lib")
if LIB_PATH not in sys.path:
    sys.path.insert(0, LIB_PATH)

from erpclaw_lib.response import err, ok

# Local module imports (scripts/ is on sys.path when invoked by db_query.py).
from accounts import _add_account_core
from shopify_helpers import encrypt_token

# Module-level so tests can monkeypatch.
DEFAULT_WORKER_URL = "https://shopify.erpclaw.ai"
PAIR_TIMEOUT_SECONDS = 10


def _fetch_pair(worker_url, code):
    """HTTPS GET /pair/:code on the Worker. Returns parsed JSON or raises.

    Exposed as a module-level function so tests can monkeypatch it without
    touching urllib internals.
    """
    url = f"{worker_url.rstrip('/')}/pair/{code}"
    req = urllib.request.Request(url, method="GET", headers={
        "accept": "application/json",
        "user-agent": "erpclaw-shopify-connect/1.1",
    })
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, timeout=PAIR_TIMEOUT_SECONDS, context=ctx) as resp:
        body = resp.read().decode("utf-8")
        return resp.status, json.loads(body)


def _detect_long_lived_process():
    """Return True if a long-lived ERPClaw process is already running.

    Heuristic (best-effort, cross-platform):
      - webclaw listening on :8000
      - telegram or scheduler pidfile exists under ~/.openclaw/erpclaw/

    If nothing is detected we pick status_mode='on-demand' by default.
    """
    if _port_listening(8000):
        return True
    home = os.path.expanduser("~/.openclaw/erpclaw")
    for pidfile in ("telegram.pid", "scheduler.pid", "webclaw.pid"):
        path = os.path.join(home, pidfile)
        if os.path.exists(path):
            return True
    return False


def _port_listening(port, host="127.0.0.1"):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.5)
    try:
        result = s.connect_ex((host, port))
    except OSError:
        return False
    finally:
        s.close()
    return result == 0


def _resolve_company(conn, supplied_id):
    """Return (company_id, companies_row) or call err() on ambiguity."""
    if supplied_id:
        row = conn.execute(
            "SELECT id, name FROM company WHERE id = ?",
            (supplied_id,),
        ).fetchone()
        if not row:
            err(f"--company-id {supplied_id} not found",
                suggestion="list companies with 'erpclaw list-companies'")
        return row[0], [row]
    rows = conn.execute("SELECT id, name FROM company ORDER BY created_at").fetchall()
    if not rows:
        err("no company found in this ERPClaw install",
            suggestion="add a company before running shopify-connect")
    if len(rows) > 1:
        choices = ", ".join(f"{r['name']} ({r['id']})" for r in rows)
        err(
            f"{len(rows)} companies exist; specify one with --company-id",
            suggestion=f"companies: {choices}",
        )
    return rows[0][0], rows


def _lazy_migrate(conn):
    """Ensure v1.1 columns exist on shopify_account, without needing a full
    init_db.py rerun. Imports the migration helper directly."""
    # Load init_db.py dynamically; it lives one level up from scripts/.
    import importlib.util
    init_path = os.path.join(os.path.dirname(__file__), "..", "init_db.py")
    init_path = os.path.abspath(init_path)
    spec = importlib.util.spec_from_file_location("_erpclaw_shopify_init_db", init_path)
    if spec is None or spec.loader is None:
        return 0
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    added = mod.apply_shopify_account_migrations_v1_1(conn)
    if added:
        conn.commit()
    return added


# ---------------------------------------------------------------------------
# Action entrypoint
# ---------------------------------------------------------------------------

def shopify_connect(conn, args):
    pairing_code = getattr(args, "pairing_code", None)
    if not pairing_code:
        err("--pairing-code is required",
            suggestion="get the code from your Shopify admin -> ERPClaw app")

    worker_url = getattr(args, "worker_url", None) or DEFAULT_WORKER_URL
    erpclaw_url_override = getattr(args, "erpclaw_url", None)

    # Lazy migrate first so _add_account_core can set the v1.1 columns.
    _lazy_migrate(conn)

    # Resolve which company this shop attaches to.
    company_id, _ = _resolve_company(conn, getattr(args, "company_id", None))

    # Note: we cannot check `shopify_account` for an existing row yet
    # because we don't know the shop_domain until the Worker hands it
    # back via /pair/:code. We DO know we'll burn this pairing code on
    # the call, so any failure after this point requires a fresh code.

    # Fetch the pair payload from the Worker.
    try:
        status, body = _fetch_pair(worker_url, pairing_code)
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read().decode("utf-8"))
        except Exception:
            detail = {"code": "UNKNOWN", "detail": str(exc)}
        code = detail.get("code", "UNKNOWN")
        if code == "ERR_PAIRING_NOT_FOUND":
            err("pairing code not found or expired",
                suggestion="reinstall the ERPClaw app from the Shopify App Store to generate a new code")
        if code == "ERR_PAIRING_ALREADY_CONSUMED":
            err("this pairing code was already used",
                suggestion="if you did not just pair, disconnect and reinstall immediately")
        err(f"worker returned {exc.code} {code}",
            suggestion=detail.get("detail"))
    except urllib.error.URLError as exc:
        err(f"could not reach {worker_url}: {exc.reason}",
            suggestion=f"verify your internet connection and {worker_url}/healthz")
    except Exception as exc:  # noqa: BLE001 - last-resort surface
        err(f"unexpected error fetching pair: {exc!r}")

    if status != 200 or not body.get("ok"):
        err(f"unexpected pair response: status={status} body={body}")

    data = body["data"]
    shop_domain = data["shop"]
    access_token = data["access_token"]
    hmac_secret = data["hmac_secret"]
    scopes = data.get("scopes", [])

    # Detect long-lived process -> status_mode.
    if _detect_long_lived_process():
        status_mode = "active"
    else:
        # Caller can flip to 'scheduled' later via shopify-install-daemon.
        status_mode = "on-demand"

    shop_name = shop_domain.split(".", 1)[0]

    # If a stale row exists for this shop (e.g., a prior install whose
    # token died), upsert: update the access_token + hmac_secret + scopes
    # in place rather than refusing. This means a fresh pairing code can
    # always heal a dead-token install without forcing the merchant to
    # run shopify-disconnect (which itself may fail if the dead token
    # can't authenticate the OAuth revoke call).
    existing = conn.execute(
        "SELECT id FROM shopify_account WHERE shop_domain = ?",
        (shop_domain,),
    ).fetchone()
    if existing:
        conn.execute(
            "UPDATE shopify_account SET "
            "access_token_enc = ?, hmac_secret_enc = ?, "
            "pairing_method = 'oauth', status = 'active', "
            "status_mode = ?, updated_at = datetime('now') "
            "WHERE id = ?",
            (
                encrypt_token(access_token),
                encrypt_token(hmac_secret),
                status_mode,
                existing["id"],
            ),
        )
        conn.commit()
        ok({
            "id": existing["id"],
            "shop_domain": shop_domain,
            "shop_name": shop_name,
            "pairing_method": "oauth",
            "status_mode": status_mode,
            "scopes": scopes,
            "gl_accounts_created": 0,
            "message": (
                "re-paired existing shop: refreshed access token and "
                "HMAC secret. GL accounts and prior sync history were "
                "preserved."
            ),
            "status": "ok",
        })
        return

    result = _add_account_core(
        conn,
        company_id=company_id,
        shop_domain=shop_domain,
        shop_name=shop_name,
        encrypted_token=encrypt_token(access_token),
        pairing_method="oauth",
        hmac_secret_enc=encrypt_token(hmac_secret),
        status_mode=status_mode,
        erpclaw_url_override=erpclaw_url_override,
    )

    ok({
        "id": result["id"],
        "shop_domain": shop_domain,
        "shop_name": shop_name,
        "pairing_method": "oauth",
        "status_mode": status_mode,
        "scopes": scopes,
        "gl_accounts_created": result["gl_accounts_created"],
        "message": (
            "paired. Next: open the Shopify admin app page to see your status card."
            if status_mode == "active"
            else "paired in on-demand mode. Run 'shopify-install-daemon' to enable 15-min background push."
        ),
    })


CONNECT_ACTIONS = {
    "shopify-connect": shopify_connect,
}
