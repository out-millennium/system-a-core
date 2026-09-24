# Tests for withdrawal-challenge code hashing + model validation (no DB).

import sys
import types

# These tests are pure (no DB). core.challenges imports core.db at module load,
# so we only need SOMETHING importable under that name. IMPORTANT: don't clobber
# a real core.db when one is importable — otherwise DB-backed tests collected in
# the same run (e.g. test_ledger_invariants) would receive this fake module and
# skip. Only install the stub as a fallback when the real module can't load.
if "core.db" not in sys.modules:
    try:
        import core.db  # noqa: F401  (real module available — use it)
    except Exception:
        sys.modules["core.db"] = types.ModuleType("core.db")

import pytest  # noqa: E402
from pydantic import ValidationError  # noqa: E402

from core import challenges  # noqa: E402
from core.models import (  # noqa: E402
    WithdrawalChallengeRequest,
    WithdrawalChallengeVerify,
)


def test_code_is_six_digits():
    for _ in range(50):
        code = challenges._gen_code()
        assert len(code) == 6 and code.isdigit()


def test_hash_is_stable_and_salted():
    assert challenges._hash_code("123456") == challenges._hash_code("123456")
    assert challenges._hash_code("123456") != challenges._hash_code("654321")
    # Hash must not equal the plaintext.
    assert challenges._hash_code("123456") != "123456"


def test_request_model_validation():
    WithdrawalChallengeRequest(account="alice", external_ref="ord-1", amount=10)
    # amount optional
    WithdrawalChallengeRequest(account="alice", external_ref="ord-1")
    with pytest.raises(ValidationError):
        WithdrawalChallengeRequest(account="bad name", external_ref="ord-1")
    with pytest.raises(ValidationError):
        WithdrawalChallengeRequest(account="alice", external_ref="", amount=1)


def test_verify_model_validation():
    WithdrawalChallengeVerify(account="alice", external_ref="ord-1", code="123456")
    with pytest.raises(ValidationError):
        WithdrawalChallengeVerify(account="alice", external_ref="ord-1", code="1")  # too short
