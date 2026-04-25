"""L0 constitutional test: SKILL.md must document every registered action.

An undocumented action is an invisible action -- the router accepts it
but no operator or AI agent knows it exists. This test guards against
that class of regression by:

  1. Enumerating every action key from the merged ACTIONS dict in
     db_query.py (the single source of truth for what's callable).
  2. Parsing SKILL.md and collecting every backticked action identifier.
  3. Asserting the router set is a subset of the documented set.

It also guards the reverse: if SKILL.md claims an action that was
renamed or removed, the test flags it so docs don't rot.

Secondary guard: action-handler return values must never leak
`hmac_secret_enc` (per-shop HMAC secret) in plaintext JSON output.
This is enforced by grep, not runtime probing, because the value may
never reach a code path a generic test exercises.
"""
import os
import re
import sys

import pytest


HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.abspath(os.path.join(HERE, ".."))
SKILL_PATH = os.path.abspath(os.path.join(SCRIPTS_DIR, "..", "SKILL.md"))

# Actions that appear in SKILL.md tables but aren't real router keys
# (meta/reserved router overrides). Keep this list small and explicit.
META_ACTIONS = {"status"}


@pytest.fixture(scope="module")
def router_actions():
    sys.path.insert(0, os.path.expanduser("~/.openclaw/erpclaw/lib"))
    sys.path.insert(0, SCRIPTS_DIR)
    from accounts import ACTIONS as A
    from sync import ACTIONS as S
    from mapping import ACTIONS as M
    from gl_rules import ACTIONS as GR
    from gl_posting import ACTIONS as GP
    from reconciliation import ACTIONS as R
    from browse import ACTIONS as B
    from reports import ACTIONS as RP
    from connect import CONNECT_ACTIONS as C
    from disconnect import DISCONNECT_ACTIONS as D
    from status_push import STATUS_PUSH_ACTIONS as SP
    from dispatcher import DISPATCHER_ACTIONS as DP
    from daemon import DAEMON_ACTIONS as DM
    from gdpr import GDPR_ACTIONS as G
    from flush import FLUSH_ACTIONS as F

    keys = set()
    for d in (A, S, M, GR, GP, R, B, RP, C, D, SP, DP, DM, G, F):
        keys.update(d.keys())
    return keys


@pytest.fixture(scope="module")
def skill_actions():
    with open(SKILL_PATH) as fh:
        body = fh.read()
    # Match any backticked token that starts with "shopify-"
    return set(re.findall(r"`(shopify-[a-z0-9-]+)`", body))


def test_every_router_action_documented_in_skill_md(router_actions, skill_actions):
    missing = (router_actions - skill_actions) - META_ACTIONS
    assert not missing, (
        f"These actions are registered in the router but not documented in "
        f"SKILL.md: {sorted(missing)}. Add a row to the appropriate domain "
        f"table in SKILL.md."
    )


def test_skill_md_does_not_reference_removed_actions(router_actions, skill_actions):
    phantom = skill_actions - router_actions
    assert not phantom, (
        f"SKILL.md references actions that are NOT registered in the router: "
        f"{sorted(phantom)}. Either add the handler or remove the doc row."
    )


def test_no_handler_source_leaks_hmac_secret_enc():
    """hmac_secret_enc is the per-shop signing secret. It must never be
    returned in any JSON response body. This catches accidental leaks
    from ``SELECT *`` patterns or response builders that echo rows
    verbatim."""
    offenders = []
    for fname in os.listdir(SCRIPTS_DIR):
        if not fname.endswith(".py"):
            continue
        if fname in {"db_query.py", "status_push.py"}:
            # status_push.py legitimately reads the secret to sign
            # outbound requests; db_query.py does not emit it.
            continue
        path = os.path.join(SCRIPTS_DIR, fname)
        with open(path) as fh:
            for lineno, line in enumerate(fh, start=1):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                if "hmac_secret_enc" in stripped and "return" in stripped:
                    offenders.append(f"{fname}:{lineno}: {stripped}")
    assert not offenders, (
        "hmac_secret_enc must not appear in a return statement outside "
        "status_push.py. Offenders:\n" + "\n".join(offenders)
    )
