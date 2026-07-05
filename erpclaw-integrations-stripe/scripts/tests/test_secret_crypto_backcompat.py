"""M31 H6 proof: the AES-256-GCM key cipher reads legacy XOR-format values.

Before M31, restricted_key_enc / webhook_secret_enc were encrypted with a
home-directory-salted XOR cipher (bare base64, no prefix). M31 H6 switched
encrypt_key/decrypt_key to AES-256-GCM (enc:v2:...). Existing installs still
hold XOR values, so decrypt_key MUST read them back. These tests prove:

  1. A value stored by the OLD XOR cipher decrypts to identical plaintext
     under the NEW decrypt_key (back-compat, the ECRYPT01 precedent).
  2. New encrypt_key writes are AES-256-GCM (enc:v2:) and round-trip.
  3. Empty passes through unchanged.
"""
import base64
import hashlib
import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_TESTS_DIR)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from stripe_helpers import encrypt_key, decrypt_key


def _legacy_xor_encrypt(plaintext):
    """Reproduce the pre-M31 XOR-salt encrypt_key byte-for-byte."""
    if not plaintext:
        return ""
    salt = hashlib.sha256(os.path.expanduser("~").encode()).digest()
    enc = bytes(b ^ salt[i % len(salt)] for i, b in enumerate(plaintext.encode()))
    return base64.b64encode(enc).decode()


def test_legacy_xor_value_reads_back_identically():
    """A key stored by the old XOR cipher decrypts to the same plaintext."""
    for plaintext in [
        "rk_test_LEGACY_abc123",
        "rk_live_LEGACY_restricted_key_example_with_length",  # underscores break the alnum run: scanner-safe by construction (QA RED fix)
        "whsec_legacy_webhook_secret",
    ]:
        legacy_ct = _legacy_xor_encrypt(plaintext)
        assert not legacy_ct.startswith("enc:")  # bare base64, no prefix
        assert decrypt_key(legacy_ct) == plaintext


def test_new_writes_are_gcm_and_round_trip():
    """New encrypt_key output is enc:v2: and decrypts back to plaintext."""
    plaintext = "rk_test_NEW_secret_456"
    ct = encrypt_key(plaintext)
    assert ct.startswith("enc:v2:")
    assert decrypt_key(ct) == plaintext


def test_empty_passthrough():
    assert encrypt_key("") == ""
    assert decrypt_key("") == ""
