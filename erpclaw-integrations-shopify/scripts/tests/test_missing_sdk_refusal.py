"""The shopify addon refuses clearly when the requests package is absent.

Importing shopify_helpers must never install anything and must always succeed.
The refusal happens at the point of use, in require_requests().
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

import shopify_helpers


def test_accessor_and_constants_are_reachable_with_the_package_blocked(monkeypatch):
    """The module object stays usable while requests is unimportable.

    Deliberately NOT named "the module imports with the package blocked":
    shopify_helpers is already in sys.modules by the time this runs, so this
    asserts reachability, not import-time behaviour. The import-time claim is
    asserted against the source text in
    test_helpers_module_never_shells_out below, which is the only form of it
    that can fail on the unfixed tree.
    """
    monkeypatch.setitem(sys.modules, "requests", None)
    assert shopify_helpers.SKILL == "erpclaw-integrations-shopify"
    assert callable(shopify_helpers.require_requests)


def test_require_requests_names_the_package_and_the_install_command(monkeypatch):
    monkeypatch.setitem(sys.modules, "requests", None)
    with pytest.raises(shopify_helpers.MissingDependencyError) as excinfo:
        shopify_helpers.require_requests()
    message = str(excinfo.value)
    assert "requests" in message
    assert "python3 -m pip install requests" in message


def test_missing_dependency_error_is_an_import_error():
    assert issubclass(shopify_helpers.MissingDependencyError, ImportError)


def test_require_requests_returns_the_module_when_it_is_present():
    pytest.importorskip("requests")
    assert shopify_helpers.require_requests().__name__ == "requests"


def test_helpers_module_never_shells_out():
    with open(shopify_helpers.__file__, encoding="utf-8") as fh:
        source = fh.read()
    assert "import subprocess" not in source
    assert "subprocess." not in source
