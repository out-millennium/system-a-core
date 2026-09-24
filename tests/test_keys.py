# Tests for key hashing (stored credentials are hashed, not plaintext).

from core.keys import hash_key


def test_hash_is_sha256_hex():
    h = hash_key("ext_deadbeef")
    assert isinstance(h, str)
    assert len(h) == 64  # SHA-256 hex
    assert all(c in "0123456789abcdef" for c in h)


def test_hash_is_deterministic():
    assert hash_key("k1") == hash_key("k1")


def test_hash_differs_for_different_keys():
    assert hash_key("k1") != hash_key("k2")


def test_hash_is_not_the_plaintext():
    # The stored value must never equal the original key.
    key = "ext_0123456789abcdef"
    assert hash_key(key) != key
