"""The stripe addon refuses clearly when the stripe package is absent.

Importing stripe_helpers must never install anything and must always succeed.
The refusal happens at the point of use, in require_stripe().
"""
import os
import sys

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import stripe_helpers


def test_accessor_and_constants_are_reachable_with_the_package_blocked(monkeypatch):
    """The module object stays usable while stripe is unimportable.

    Deliberately NOT named "the module imports with the package blocked":
    stripe_helpers is already in sys.modules by the time this runs, so this
    asserts reachability, not import-time behaviour. The import-time claim is
    asserted against the source text in
    test_helpers_module_never_shells_out below, which is the only form of it
    that can fail on the unfixed tree. A reload-based version would pass on
    the defect on any machine where stripe happens to be installed, because
    the auto-install branch would call pip, pip would report the requirement
    already satisfied, and the reload would succeed.
    """
    monkeypatch.setitem(sys.modules, "stripe", None)
    assert stripe_helpers.SKILL == "erpclaw-integrations-stripe"
    assert callable(stripe_helpers.require_stripe)


def test_require_stripe_names_the_package_and_the_install_command(monkeypatch):
    monkeypatch.setitem(sys.modules, "stripe", None)
    with pytest.raises(stripe_helpers.MissingDependencyError) as excinfo:
        stripe_helpers.require_stripe()
    message = str(excinfo.value)
    assert "stripe" in message
    assert "python3 -m pip install stripe" in message


def test_missing_dependency_error_is_an_import_error():
    assert issubclass(stripe_helpers.MissingDependencyError, ImportError)


def test_require_stripe_returns_the_module_when_it_is_present():
    pytest.importorskip("stripe")
    assert stripe_helpers.require_stripe().__name__ == "stripe"


def test_helpers_module_never_shells_out():
    with open(stripe_helpers.__file__, encoding="utf-8") as fh:
        source = fh.read()
    assert "import subprocess" not in source
    assert "subprocess." not in source
