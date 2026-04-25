"""Cross-platform OS scheduler for the 15-minute status-push cycle.

macOS  -> launchd LaunchAgent at ~/Library/LaunchAgents/com.avansaber.erpclaw.shopify-push.plist
Linux  -> systemd user timer + service under ~/.config/systemd/user/
          (falls back to crontab when systemd --user is unavailable)

Exposes two actions:
  shopify-install-daemon    creates the scheduler entry (idempotent)
  shopify-uninstall-daemon  removes it (idempotent; no-op if nothing installed)

No Windows support in v1 (per Decisions Log; WSL2 users follow the Linux
path).
"""
import os
import platform
import shutil
import subprocess
import sys

LIB_PATH = os.path.expanduser("~/.openclaw/erpclaw/lib")
if LIB_PATH not in sys.path:
    sys.path.insert(0, LIB_PATH)

from erpclaw_lib.response import err, ok


HOME = os.path.expanduser("~")

LAUNCHD_LABEL = "com.avansaber.erpclaw.shopify-push"
LAUNCHD_PLIST = os.path.join(HOME, "Library/LaunchAgents", f"{LAUNCHD_LABEL}.plist")

SYSTEMD_UNIT_DIR = os.path.join(HOME, ".config/systemd/user")
SYSTEMD_SERVICE = os.path.join(SYSTEMD_UNIT_DIR, "erpclaw-shopify-push.service")
SYSTEMD_TIMER = os.path.join(SYSTEMD_UNIT_DIR, "erpclaw-shopify-push.timer")

CRON_MARKER = "# erpclaw-shopify-push (managed)"


# ---------------------------------------------------------------------------
# Template loader
# ---------------------------------------------------------------------------

def _templates_dir():
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, "..", "templates"))


def _render(template_name, substitutions):
    path = os.path.join(_templates_dir(), template_name)
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    for key, value in substitutions.items():
        text = text.replace(f"{{{{{key}}}}}", str(value))
    return text


def _db_query_path():
    """Absolute path to this module's db_query.py (invoked by the scheduler)."""
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, "db_query.py"))


def _python_executable():
    return sys.executable or "/usr/bin/env python3"


def _substitutions():
    return {
        "PYTHON": _python_executable(),
        "DB_QUERY": _db_query_path(),
        "LABEL": LAUNCHD_LABEL,
        "LOG_PATH": os.path.join(HOME, ".openclaw/erpclaw/logs/shopify_push.log"),
    }


# ---------------------------------------------------------------------------
# macOS launchd
# ---------------------------------------------------------------------------

def _install_launchd():
    os.makedirs(os.path.dirname(LAUNCHD_PLIST), exist_ok=True)
    os.makedirs(os.path.dirname(_substitutions()["LOG_PATH"]), exist_ok=True)
    text = _render("launchd.plist.template", _substitutions())
    with open(LAUNCHD_PLIST, "w", encoding="utf-8") as f:
        f.write(text)
    # Best-effort load. launchctl fails if already loaded; `bootout` + `bootstrap`
    # would be cleaner but requires GUI_UID which complicates portability.
    try:
        subprocess.run(
            ["launchctl", "unload", LAUNCHD_PLIST],
            capture_output=True, check=False,
        )
    except FileNotFoundError:
        return {"installed": False, "reason": "launchctl not available"}
    result = subprocess.run(
        ["launchctl", "load", LAUNCHD_PLIST],
        capture_output=True, text=True, check=False,
    )
    return {
        "mechanism": "launchd",
        "path": LAUNCHD_PLIST,
        "loaded": result.returncode == 0,
        "stderr": (result.stderr or "").strip() or None,
    }


def _uninstall_launchd():
    if not os.path.exists(LAUNCHD_PLIST):
        return {"mechanism": "launchd", "uninstalled": False, "reason": "not installed"}
    subprocess.run(
        ["launchctl", "unload", LAUNCHD_PLIST],
        capture_output=True, check=False,
    )
    os.remove(LAUNCHD_PLIST)
    return {"mechanism": "launchd", "uninstalled": True, "path": LAUNCHD_PLIST}


# ---------------------------------------------------------------------------
# Linux systemd user units
# ---------------------------------------------------------------------------

def _systemd_available():
    if not shutil.which("systemctl"):
        return False
    # Check for user session
    result = subprocess.run(
        ["systemctl", "--user", "is-active", "default.target"],
        capture_output=True, check=False,
    )
    return result.returncode == 0 or "inactive" in (result.stdout + result.stderr).decode(errors="ignore")


def _install_systemd():
    os.makedirs(SYSTEMD_UNIT_DIR, exist_ok=True)
    subs = _substitutions()
    os.makedirs(os.path.dirname(subs["LOG_PATH"]), exist_ok=True)
    with open(SYSTEMD_SERVICE, "w", encoding="utf-8") as f:
        f.write(_render("systemd.service.template", subs))
    with open(SYSTEMD_TIMER, "w", encoding="utf-8") as f:
        f.write(_render("systemd.timer.template", subs))
    result = subprocess.run(
        ["systemctl", "--user", "daemon-reload"],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        return {"mechanism": "systemd", "loaded": False, "stderr": result.stderr.strip()}
    subprocess.run(
        ["systemctl", "--user", "enable", "--now", "erpclaw-shopify-push.timer"],
        capture_output=True, check=False,
    )
    return {
        "mechanism": "systemd",
        "service": SYSTEMD_SERVICE,
        "timer": SYSTEMD_TIMER,
        "loaded": True,
    }


def _uninstall_systemd():
    if not (os.path.exists(SYSTEMD_SERVICE) or os.path.exists(SYSTEMD_TIMER)):
        return {"mechanism": "systemd", "uninstalled": False, "reason": "not installed"}
    subprocess.run(
        ["systemctl", "--user", "disable", "--now", "erpclaw-shopify-push.timer"],
        capture_output=True, check=False,
    )
    for path in (SYSTEMD_TIMER, SYSTEMD_SERVICE):
        if os.path.exists(path):
            os.remove(path)
    subprocess.run(
        ["systemctl", "--user", "daemon-reload"],
        capture_output=True, check=False,
    )
    return {"mechanism": "systemd", "uninstalled": True}


# ---------------------------------------------------------------------------
# Linux cron fallback
# ---------------------------------------------------------------------------

def _install_cron():
    subs = _substitutions()
    os.makedirs(os.path.dirname(subs["LOG_PATH"]), exist_ok=True)
    line = (
        f"*/15 * * * * {subs['PYTHON']} {subs['DB_QUERY']} --action shopify-push-status "
        f">> {subs['LOG_PATH']} 2>&1 {CRON_MARKER}"
    )
    existing = subprocess.run(
        ["crontab", "-l"],
        capture_output=True, text=True, check=False,
    )
    current = existing.stdout if existing.returncode == 0 else ""
    # Strip any previous managed line.
    lines = [l for l in current.splitlines() if CRON_MARKER not in l]
    lines.append(line)
    new_cron = "\n".join(lines) + "\n"
    install = subprocess.run(
        ["crontab", "-"],
        input=new_cron, text=True, capture_output=True, check=False,
    )
    return {
        "mechanism": "cron",
        "installed": install.returncode == 0,
        "entry": line,
        "stderr": install.stderr.strip() or None,
    }


def _uninstall_cron():
    existing = subprocess.run(
        ["crontab", "-l"],
        capture_output=True, text=True, check=False,
    )
    if existing.returncode != 0:
        return {"mechanism": "cron", "uninstalled": False, "reason": "no crontab"}
    lines = [l for l in existing.stdout.splitlines() if CRON_MARKER not in l]
    new_cron = "\n".join(lines) + "\n" if lines else ""
    subprocess.run(
        ["crontab", "-"],
        input=new_cron, text=True, capture_output=True, check=False,
    )
    return {"mechanism": "cron", "uninstalled": True}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def install_daemon():
    system = platform.system()
    if system == "Darwin":
        return _install_launchd()
    if system == "Linux":
        if _systemd_available():
            return _install_systemd()
        return _install_cron()
    return {"installed": False, "reason": f"unsupported OS: {system}"}


def uninstall_daemon():
    system = platform.system()
    results = []
    if system == "Darwin":
        results.append(_uninstall_launchd())
    elif system == "Linux":
        # Try both systemd and cron so we clean up whichever was used.
        results.append(_uninstall_systemd())
        results.append(_uninstall_cron())
    else:
        return {"uninstalled": False, "reason": f"unsupported OS: {system}"}
    return {"uninstalled": any(r.get("uninstalled") for r in results), "details": results}


# ---------------------------------------------------------------------------
# Action wrappers
# ---------------------------------------------------------------------------

def shopify_install_daemon(conn, args):  # noqa: ARG001 conn unused but required by router
    result = install_daemon()
    if result.get("installed") is False and "reason" in result:
        err(f"install failed: {result['reason']}")
    ok(result)


def shopify_uninstall_daemon(conn, args):  # noqa: ARG001
    ok(uninstall_daemon())


DAEMON_ACTIONS = {
    "shopify-install-daemon": shopify_install_daemon,
    "shopify-uninstall-daemon": shopify_uninstall_daemon,
}
