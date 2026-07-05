"""M31 H6 proof: the AES-256-GCM token cipher reads legacy XOR-format values.

Before M31, access_token_enc / hmac_secret_enc were encrypted with a
home-directory-salted XOR cipher (bare base64, no prefix). M31 H6 switched
encrypt_token/decrypt_token to AES-256-GCM (enc:v2:...). Existing installs
still hold XOR values, so decrypt_token MUST read them back. These tests prove:

  1. A value stored by the OLD XOR cipher decrypts to identical plaintext
     under the NEW decrypt_token (back-compat, the ECRYPT01 precedent).
  2. New encrypt_token writes are AES-256-GCM (enc:v2:) and round-trip.
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

from shopify_helpers import encrypt_token, decrypt_token


def _legacy_xor_encrypt(plaintext):
    """Reproduce the pre-M31 XOR-salt encrypt_token byte-for-byte."""
    if not plaintext:
        return ""
    salt = hashlib.sha256(os.path.expanduser("~").encode()).digest()
    enc = bytes(b ^ salt[i % len(salt)] for i, b in enumerate(plaintext.encode()))
    return base64.b64encode(enc).decode()


def test_legacy_xor_value_reads_back_identically():
    """A token stored by the old XOR cipher decrypts to the same plaintext."""
    for plaintext in [
        "shpat_LEGACY_access_token_0123456789abcdef",
        "a" * 64,  # hmac secret shape
        "shpss_legacy_shared_secret",
    ]:
        legacy_ct = _legacy_xor_encrypt(plaintext)
        assert not legacy_ct.startswith("enc:")  # bare base64, no prefix
        assert decrypt_token(legacy_ct) == plaintext


def test_new_writes_are_gcm_and_round_trip():
    """New encrypt_token output is enc:v2: and decrypts back to plaintext."""
    plaintext = "shpat_NEW_access_token_fedcba9876543210"
    ct = encrypt_token(plaintext)
    assert ct.startswith("enc:v2:")
    assert decrypt_token(ct) == plaintext


def test_empty_passthrough():
    assert encrypt_token("") == ""
    assert decrypt_token("") == ""
