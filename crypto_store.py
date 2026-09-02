"""
AES-256-GCM envelope encryption for Klaviyo private API keys.

One 256-bit master key lives in the environment (KLAVIYO_MASTER_KEY, base64).
Every stored API key gets a fresh random 96-bit nonce. Ciphertext and nonce go
in Postgres; the master key never does.

Nothing in this module logs, prints, or repr()s a plaintext key.
"""

import base64
import os
import secrets
from typing import Optional, Tuple

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

MASTER_KEY_ENV = "KLAVIYO_MASTER_KEY"
KEY_BYTES = 32   # AES-256
NONCE_BYTES = 12  # 96-bit nonce, the GCM standard

# Bound to the ciphertext by GCM, so a row cannot be replayed as another kind
# of secret or moved to a different column.
AAD = b"klaviyo_client_api_key:v1"


class MasterKeyError(RuntimeError):
    """The master key is missing, malformed, or the wrong length."""


class DecryptionError(RuntimeError):
    """Ciphertext failed authentication - wrong master key or tampering."""


def generate_master_key() -> str:
    """A new base64-encoded 256-bit master key, for `python3 -c` at setup."""
    return base64.b64encode(secrets.token_bytes(KEY_BYTES)).decode("ascii")


def load_master_key(value: Optional[str] = None) -> bytes:
    """
    Validate and decode the master key.

    Fails loudly and early - before any client credential is touched - and the
    error text never contains the key or any part of it.
    """
    raw = value if value is not None else os.getenv(MASTER_KEY_ENV, "")
    raw = (raw or "").strip()

    if not raw:
        raise MasterKeyError(
            "{} is not set. Generate one with:\n"
            "  python3 -c \"import crypto_store; print(crypto_store.generate_master_key())\""
            .format(MASTER_KEY_ENV)
        )

    try:
        # validate=False would silently accept junk; be strict instead.
        key = base64.b64decode(raw, validate=True)
    except (ValueError, base64.binascii.Error):
        raise MasterKeyError(
            "{} is not valid base64. Expected base64 of {} random bytes."
            .format(MASTER_KEY_ENV, KEY_BYTES)
        )

    if len(key) != KEY_BYTES:
        raise MasterKeyError(
            "{} decodes to {} bytes, expected {}. Generate a new one."
            .format(MASTER_KEY_ENV, len(key), KEY_BYTES)
        )

    return key


def encrypt_api_key(plaintext: str, master_key: bytes) -> Tuple[bytes, bytes]:
    """
    Encrypt one API key. Returns (ciphertext_with_tag, nonce).

    A fresh nonce per call means encrypting the same key twice produces
    different ciphertext, so rows leak nothing by comparison.
    """
    if not plaintext:
        raise ValueError("Refusing to encrypt an empty API key")

    nonce = secrets.token_bytes(NONCE_BYTES)
    ciphertext = AESGCM(master_key).encrypt(
        nonce, plaintext.encode("utf-8"), AAD
    )
    return ciphertext, nonce


def decrypt_api_key(ciphertext: bytes, nonce: bytes, master_key: bytes) -> str:
    """
    Decrypt one API key.

    GCM authenticates before returning anything, so a wrong master key or a
    modified row raises instead of yielding garbage.
    """
    try:
        plaintext = AESGCM(master_key).decrypt(bytes(nonce), bytes(ciphertext), AAD)
    except InvalidTag:
        raise DecryptionError(
            "API key failed authentication - wrong KLAVIYO_MASTER_KEY, "
            "or the stored row was modified"
        )
    except ValueError as exc:
        raise DecryptionError("API key could not be decrypted ({})".format(exc))

    return plaintext.decode("utf-8")
