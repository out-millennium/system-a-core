# system-a-core/tests/test_models.py
#
# Validation tests for the Core request models. These exercise the pydantic
# constraints only (account-name pattern, positive/bounded amounts, operation
# id length) — no FastAPI app, no database — so they run with a plain `pytest`.

import pytest
from pydantic import ValidationError

from core.models import AccountCreate, Transfer, Burn, SystemBurn, InitCredit


# ---- account name ---------------------------------------------------------

@pytest.mark.parametrize("name", ["alice", "a.b_c-1", "USER_01", "x" * 64])
def test_valid_account_names(name):
    assert AccountCreate(name=name).name == name


@pytest.mark.parametrize(
    "name",
    [
        "",              # empty
        "x" * 65,        # too long
        "has space",     # space not allowed
        "bad/slash",     # slash not allowed
        "emoji😀",       # non-ascii
        "a@b",           # @ not allowed
    ],
)
def test_invalid_account_names(name):
    with pytest.raises(ValidationError):
        AccountCreate(name=name)


def test_account_name_is_stripped():
    assert AccountCreate(name="  alice  ").name == "alice"


# ---- amounts --------------------------------------------------------------

def test_transfer_requires_positive_amount():
    with pytest.raises(ValidationError):
        Transfer(from_account="a", to_account="b", amount=0)
    with pytest.raises(ValidationError):
        Transfer(from_account="a", to_account="b", amount=-5)


def test_transfer_amount_upper_bound():
    # 10**38 - 1 is the max; one above must fail (matches NUMERIC(38,0)).
    Transfer(from_account="a", to_account="b", amount=10**38 - 1)
    with pytest.raises(ValidationError):
        Transfer(from_account="a", to_account="b", amount=10**38)


def test_burn_positive_amount():
    Burn(from_account="a", amount=1)
    with pytest.raises(ValidationError):
        Burn(from_account="a", amount=0)


def test_init_credit_positive_amount():
    InitCredit(to_account="a", amount=100)
    with pytest.raises(ValidationError):
        InitCredit(to_account="a", amount=0)


def test_init_credit_has_client_operation_id():
    # init_credit is now idempotent: the model carries an auto-generated
    # client_operation_id (like SystemBurn/Transfer) so retries dedupe.
    c = InitCredit(to_account="alice", amount=10)
    assert isinstance(c.client_operation_id, str) and len(c.client_operation_id) > 0
    # An explicit id is preserved.
    c2 = InitCredit(to_account="alice", amount=10, client_operation_id="op-123")
    assert c2.client_operation_id == "op-123"


def test_system_burn_validation():
    # Valid: positive, bounded amount + auto-generated client_operation_id.
    s = SystemBurn(from_account="alice", amount=50)
    assert s.from_account == "alice"
    assert isinstance(s.client_operation_id, str) and len(s.client_operation_id) > 0

    # Positive amount required.
    with pytest.raises(ValidationError):
        SystemBurn(from_account="alice", amount=0)
    with pytest.raises(ValidationError):
        SystemBurn(from_account="alice", amount=-1)

    # Upper bound matches NUMERIC(38,0).
    SystemBurn(from_account="alice", amount=10**38 - 1)
    with pytest.raises(ValidationError):
        SystemBurn(from_account="alice", amount=10**38)

    # Account-name pattern enforced.
    with pytest.raises(ValidationError):
        SystemBurn(from_account="bad name", amount=1)


# ---- client operation id --------------------------------------------------

def test_transfer_generates_client_operation_id_by_default():
    t = Transfer(from_account="a", to_account="b", amount=1)
    assert isinstance(t.client_operation_id, str) and len(t.client_operation_id) > 0


def test_client_operation_id_length_bound():
    with pytest.raises(ValidationError):
        Transfer(
            from_account="a",
            to_account="b",
            amount=1,
            client_operation_id="x" * 129,
        )
