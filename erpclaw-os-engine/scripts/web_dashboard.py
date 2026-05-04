#!/usr/bin/env python3
"""ERPClaw OS Engine — web_dashboard.py

Implements the os-setup-web-dashboard action: clones erpclaw-web from
GitHub, runs npm install + build, sets up Python venv for the API,
runs deploy/setup.sh, optionally configures domain + SSL via certbot.

Moved from foundation `scripts/erpclaw-meta/db_query.py` on 2026-05-04
as part of the erpclaw-os-engine addon split. The function is sudo +
network heavy (it provisions nginx + certbot + reaches GitHub), which
is why it lives in the optional addon rather than the foundation
skill — keeps foundation scan-clean.
"""
import os
import shutil
import subprocess
import sys

# erpclaw_lib was already wired into sys.path by db_query.py self-check
from erpclaw_lib.response import ok, err


# Directory where erpclaw-web gets cloned on the server
_WEB_SKILLS_DIR = os.path.expanduser("~/clawd/skills")
_WEB_DIR = os.path.join(_WEB_SKILLS_DIR, "erpclaw-web")
_WEB_PACKAGE_JSON = os.path.join(_WEB_DIR, "package.json")
_WEB_REPO_URL = "https://github.com/avansaber/erpclaw-web.git"


def _run_cmd(cmd, cwd=None, timeout=300):
    """Run a shell command, return (success, stdout, stderr).

    Uses subprocess.run with capture. Never raises -- returns status.
    """
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.returncode == 0, result.stdout.strip(), result.stderr.strip()
    except FileNotFoundError:
        return False, "", f"Command not found: {cmd[0]}"
    except subprocess.TimeoutExpired:
        return False, "", f"Command timed out after {timeout}s: {' '.join(cmd)}"
    except Exception as e:
        return False, "", str(e)


def _check_binary(name):
    """Return True if a binary is on PATH."""
    return shutil.which(name) is not None


def handle_setup_web_dashboard(args):
    """Set up the ERPClaw Web dashboard -- clone, build, configure, deploy.

    Handles: git clone, npm install, npm build, Python venv, deploy/setup.sh,
    optional domain + SSL configuration via certbot.

    Note: this action requires sudo for nginx config + certbot SSL issuance.
    Run on a machine where you control DNS (so the cert flow can complete).
    Calling without --domain skips the SSL setup.
    """
    domain = getattr(args, "domain", None)
    ssl = getattr(args, "ssl", None)
    skip_build = getattr(args, "skip_build", False)

    # Default: SSL is on when a domain is provided
    if ssl is None:
        ssl = domain is not None

    # Pre-flight checks
    if not _check_binary("node") or not _check_binary("npm"):
        err("Node.js is required for the web dashboard. Install with: sudo apt install nodejs npm")
    if not _check_binary("nginx"):
        err("nginx is required. Install with: sudo apt install nginx")
    if not _check_binary("python3"):
        err("python3 is required but not found on PATH.")
    if not _check_binary("git"):
        err("git is required but not found on PATH.")

    steps_completed = []
    already_installed = os.path.isfile(_WEB_PACKAGE_JSON)

    # Step 1: Clone or detect existing
    if already_installed:
        steps_completed.append("erpclaw-web already installed, skipping clone")
    else:
        success, stdout, stderr = _run_cmd(
            ["git", "clone", "--depth", "1", _WEB_REPO_URL, _WEB_DIR],
            timeout=120,
        )
        if not success:
            err(
                "Could not download ERPClaw Web from GitHub. "
                "Check internet connection."
                + (f" Detail: {stderr}" if stderr else "")
            )
        steps_completed.append("Cloned erpclaw-web from GitHub")

    # Step 2: npm install + build
    if skip_build:
        steps_completed.append("Skipped npm install + build (--skip-build)")
    else:
        success, stdout, stderr = _run_cmd(["npm", "install"], cwd=_WEB_DIR, timeout=300)
        if not success:
            err(f"npm install failed. {stderr}")
        steps_completed.append("npm install completed")

        success, stdout, stderr = _run_cmd(["npm", "run", "build"], cwd=_WEB_DIR, timeout=300)
        if not success:
            err(f"npm run build failed. {stderr}")
        steps_completed.append("Frontend built (npm run build)")

    # Step 3: Python venv + API dependencies
    api_dir = os.path.join(_WEB_DIR, "api")
    venv_dir = os.path.join(api_dir, ".venv")
    requirements_file = os.path.join(api_dir, "requirements.txt")

    if os.path.isdir(api_dir) and os.path.isfile(requirements_file):
        if skip_build and os.path.isdir(venv_dir):
            steps_completed.append("Skipped Python venv setup (--skip-build, venv exists)")
        else:
            success, stdout, stderr = _run_cmd(
                ["python3", "-m", "venv", venv_dir], cwd=api_dir, timeout=60
            )
            if not success:
                err(f"Failed to create Python venv: {stderr}")

            pip_path = os.path.join(venv_dir, "bin", "pip")
            success, stdout, stderr = _run_cmd(
                [pip_path, "install", "-r", requirements_file],
                cwd=api_dir,
                timeout=300,
            )
            if not success:
                err(f"pip install failed: {stderr}")
            steps_completed.append("Python venv created and API dependencies installed")
    else:
        steps_completed.append("No api/ directory or requirements.txt found, skipped venv")

    # Step 4: Run deploy/setup.sh
    setup_script = os.path.join(_WEB_DIR, "deploy", "setup.sh")
    if os.path.isfile(setup_script):
        success, stdout, stderr = _run_cmd(
            ["bash", setup_script], cwd=_WEB_DIR, timeout=120
        )
        if not success:
            err(f"deploy/setup.sh failed. stdout: {stdout} stderr: {stderr}")
        steps_completed.append("deploy/setup.sh completed (nginx + systemd configured)")
    else:
        steps_completed.append("deploy/setup.sh not found, skipping nginx/systemd setup")

    # Step 5: Domain configuration (uses sudo)
    if domain:
        nginx_conf = "/etc/nginx/sites-available/erpclaw-web"
        if os.path.isfile(nginx_conf):
            success, stdout, stderr = _run_cmd(
                ["sudo", "sed", "-i", f"s/server_name .*/server_name {domain};/", nginx_conf],
                timeout=10,
            )
            if success:
                _run_cmd(["sudo", "nginx", "-t"], timeout=10)
                _run_cmd(["sudo", "systemctl", "reload", "nginx"], timeout=10)
                steps_completed.append(f"nginx configured for domain: {domain}")
            else:
                steps_completed.append(f"WARNING: Could not update nginx config for {domain}")
        else:
            steps_completed.append(
                f"nginx config not found at {nginx_conf}, domain not configured"
            )

        # Step 6: SSL via certbot
        if ssl:
            if not _check_binary("certbot"):
                steps_completed.append(
                    "WARNING: certbot not found. Install with: "
                    "sudo apt install certbot python3-certbot-nginx"
                )
            else:
                success, stdout, stderr = _run_cmd(
                    [
                        "sudo", "certbot", "--nginx",
                        "-d", domain,
                        "--non-interactive",
                        "--agree-tos",
                        "--email", "admin@erpclaw.ai",
                    ],
                    timeout=120,
                )
                if success:
                    steps_completed.append(f"SSL certificate issued for {domain}")
                else:
                    steps_completed.append(
                        f"WARNING: certbot failed -- {stderr}. "
                        "You may need to set up DNS first."
                    )
    else:
        steps_completed.append("No --domain provided, using server IP")

    # Build result URL
    if domain:
        protocol = "https" if ssl else "http"
        url = f"{protocol}://{domain}"
    else:
        success, public_ip, _ = _run_cmd(
            ["curl", "-s", "--max-time", "5", "https://checkip.amazonaws.com"],
            timeout=10,
        )
        if success and public_ip:
            url = f"http://{public_ip.strip()}"
        else:
            url = "http://<server-ip>"

    ok({
        "message": "ERPClaw Web dashboard setup complete",
        "url": url,
        "setup_url": f"{url}/setup",
        "steps_completed": steps_completed,
        "next_steps": [
            f"Visit {url}/setup to create your admin account",
            "Connect to your ERPClaw database (auto-detected if on same server)",
            "Start managing your ERP from the web dashboard",
        ],
    })
