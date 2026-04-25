"""Unit tests for shopify-install-daemon / shopify-uninstall-daemon.

Patches platform.system() and subprocess.run so tests are deterministic
regardless of the host OS. Verifies:
  - macOS path writes a plist and calls launchctl load.
  - Linux systemd path writes service + timer and runs systemctl --user.
  - Linux cron fallback path shells crontab -l / crontab -.
  - Uninstall removes files and stops the scheduler.
  - Unsupported OS (Windows) is rejected.
  - Template rendering substitutes {{PYTHON}}, {{DB_QUERY}}, etc.
"""
import importlib
import os
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def daemon_module():
    import daemon as _d  # type: ignore[import-not-found]
    importlib.reload(_d)
    return _d


def _fake_run(returncode=0, stdout="", stderr=""):
    result = MagicMock()
    result.returncode = returncode
    result.stdout = stdout
    result.stderr = stderr
    return result


# ---------------------------------------------------------------------------
# Template rendering
# ---------------------------------------------------------------------------

def test_render_launchd_plist(daemon_module, tmp_path):
    rendered = daemon_module._render(
        "launchd.plist.template",
        {"PYTHON": "/usr/bin/python3", "DB_QUERY": "/tmp/db_query.py", "LABEL": "com.example.test", "LOG_PATH": "/tmp/x.log"},
    )
    assert "com.example.test" in rendered
    assert "/usr/bin/python3" in rendered
    assert "/tmp/db_query.py" in rendered
    assert "{{PYTHON}}" not in rendered


def test_render_systemd_service(daemon_module):
    rendered = daemon_module._render(
        "systemd.service.template",
        {"PYTHON": "/usr/bin/python3", "DB_QUERY": "/tmp/db.py", "LABEL": "x", "LOG_PATH": "/tmp/y"},
    )
    assert "ExecStart=/usr/bin/python3 /tmp/db.py --action shopify-push-status" in rendered


def test_render_systemd_timer(daemon_module):
    rendered = daemon_module._render("systemd.timer.template", {"PYTHON": "x", "DB_QUERY": "y", "LABEL": "z", "LOG_PATH": "w"})
    assert "OnUnitActiveSec=15min" in rendered


# ---------------------------------------------------------------------------
# macOS path
# ---------------------------------------------------------------------------

def test_install_launchd_writes_plist_and_loads(daemon_module, tmp_path, monkeypatch):
    fake_home = tmp_path
    monkeypatch.setattr(daemon_module, "HOME", str(fake_home))
    launchd_plist = os.path.join(fake_home, "Library/LaunchAgents", "com.avansaber.erpclaw.shopify-push.plist")
    monkeypatch.setattr(daemon_module, "LAUNCHD_PLIST", launchd_plist)

    run_calls = []

    def fake_run(cmd, **kw):
        run_calls.append(cmd)
        return _fake_run(returncode=0)

    with patch.object(daemon_module.subprocess, "run", side_effect=fake_run):
        with patch.object(daemon_module.platform, "system", return_value="Darwin"):
            result = daemon_module.install_daemon()

    assert result["mechanism"] == "launchd"
    assert result["loaded"] is True
    assert os.path.exists(launchd_plist)
    # launchctl unload + load
    assert any("unload" in c for c in run_calls)
    assert any("load" in c for c in run_calls)


def test_uninstall_launchd_removes_plist(daemon_module, tmp_path, monkeypatch):
    fake_home = tmp_path
    monkeypatch.setattr(daemon_module, "HOME", str(fake_home))
    plist_dir = os.path.join(fake_home, "Library/LaunchAgents")
    os.makedirs(plist_dir)
    plist_path = os.path.join(plist_dir, "com.avansaber.erpclaw.shopify-push.plist")
    monkeypatch.setattr(daemon_module, "LAUNCHD_PLIST", plist_path)
    with open(plist_path, "w") as f:
        f.write("<plist/>")

    with patch.object(daemon_module.subprocess, "run", return_value=_fake_run()):
        with patch.object(daemon_module.platform, "system", return_value="Darwin"):
            result = daemon_module.uninstall_daemon()

    assert result["uninstalled"] is True
    assert not os.path.exists(plist_path)


def test_uninstall_launchd_is_noop_when_not_installed(daemon_module, tmp_path, monkeypatch):
    fake_home = tmp_path
    monkeypatch.setattr(daemon_module, "HOME", str(fake_home))
    monkeypatch.setattr(
        daemon_module, "LAUNCHD_PLIST",
        os.path.join(fake_home, "Library/LaunchAgents", "notthere.plist"),
    )
    with patch.object(daemon_module.platform, "system", return_value="Darwin"):
        result = daemon_module.uninstall_daemon()
    # details list, first entry is launchd
    assert result["uninstalled"] is False


# ---------------------------------------------------------------------------
# Linux systemd path
# ---------------------------------------------------------------------------

def test_install_systemd_writes_service_and_timer(daemon_module, tmp_path, monkeypatch):
    fake_home = tmp_path
    monkeypatch.setattr(daemon_module, "HOME", str(fake_home))
    unit_dir = os.path.join(fake_home, ".config/systemd/user")
    service_path = os.path.join(unit_dir, "erpclaw-shopify-push.service")
    timer_path = os.path.join(unit_dir, "erpclaw-shopify-push.timer")
    monkeypatch.setattr(daemon_module, "SYSTEMD_UNIT_DIR", unit_dir)
    monkeypatch.setattr(daemon_module, "SYSTEMD_SERVICE", service_path)
    monkeypatch.setattr(daemon_module, "SYSTEMD_TIMER", timer_path)

    with patch.object(daemon_module, "_systemd_available", return_value=True):
        with patch.object(daemon_module.subprocess, "run", return_value=_fake_run()):
            with patch.object(daemon_module.platform, "system", return_value="Linux"):
                result = daemon_module.install_daemon()

    assert result["mechanism"] == "systemd"
    assert result["loaded"] is True
    assert os.path.exists(service_path)
    assert os.path.exists(timer_path)


# ---------------------------------------------------------------------------
# Linux cron fallback
# ---------------------------------------------------------------------------

def test_install_cron_when_no_systemd(daemon_module, tmp_path, monkeypatch):
    fake_home = tmp_path
    monkeypatch.setattr(daemon_module, "HOME", str(fake_home))
    captured_inputs = []

    def fake_run(cmd, **kw):
        captured_inputs.append((cmd, kw.get("input")))
        if cmd[:2] == ["crontab", "-l"]:
            return _fake_run(returncode=1, stdout="")
        return _fake_run(returncode=0)

    with patch.object(daemon_module, "_systemd_available", return_value=False):
        with patch.object(daemon_module.subprocess, "run", side_effect=fake_run):
            with patch.object(daemon_module.platform, "system", return_value="Linux"):
                result = daemon_module.install_daemon()

    assert result["mechanism"] == "cron"
    assert result["installed"] is True
    # Cron payload includes the marker + every-15-min schedule.
    cron_input = next((inp for _, inp in captured_inputs if inp and "erpclaw-shopify-push" in inp), None)
    assert cron_input is not None
    assert "*/15 * * * *" in cron_input


# ---------------------------------------------------------------------------
# Unsupported platform
# ---------------------------------------------------------------------------

def test_unsupported_os_rejected(daemon_module):
    with patch.object(daemon_module.platform, "system", return_value="Windows"):
        result = daemon_module.install_daemon()
    assert result["installed"] is False
    assert "unsupported OS" in result["reason"]
