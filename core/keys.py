# core/keys.py
#
# Key hashing for stored credentials.
#
# API keys and external-application keys must NOT be stored in plaintext: a DB
# leak would then hand an attacker working credentials. Instead we store a
# SHA-256 hash of the key and compare hashes on each request. The plaintext key
# is shown to the caller exactly ONCE at creation and never persisted.
#
# SHA-256 (not bcrypt/argon2) is appropriate here because these keys are
# high-entropy random tokens (secrets.token_hex(32) = 256 bits), so they are not
# brute-forceable; a fast hash is fine and keeps auth cheap. For human-chosen
# passwords a slow KDF would be required — but these are not passwords.

import hashlib


def hash_key(key: str) -> str:
    """Return the hex SHA-256 of a key, for storage/lookup."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()
