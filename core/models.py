# core/models.py

from pydantic import BaseModel, constr, conint, Field
import uuid


AccountName = constr(
    strip_whitespace=True,
    min_length=1,
    max_length=64,
    pattern=r"^[a-zA-Z0-9_\-\.]+$"
)

ClientOperationId = constr(
    strip_whitespace=True,
    min_length=1,
    max_length=128
)


class InitCredit(BaseModel):
    to_account: AccountName
    amount: conint(gt=0, le=10**38 - 1)
    # Idempotency: a retried credit with the same id is recorded once (mirrors
    # Transfer/Burn/SystemBurn). Auto-generated when the caller omits it.
    client_operation_id: ClientOperationId = Field(default_factory=lambda: str(uuid.uuid4()))


class Transfer(BaseModel):
    from_account: AccountName
    to_account: AccountName
    amount: conint(gt=0, le=10**38 - 1)
    client_operation_id: ClientOperationId = Field(default_factory=lambda: str(uuid.uuid4()))


class Burn(BaseModel):
    from_account: AccountName
    amount: conint(gt=0, le=10**38 - 1)
    client_operation_id: ClientOperationId = Field(default_factory=lambda: str(uuid.uuid4()))


class SystemBurn(BaseModel):
    # System-initiated debit (admin-key only). Same shape as Burn.
    from_account: AccountName
    amount: conint(gt=0, le=10**38 - 1)
    client_operation_id: ClientOperationId = Field(default_factory=lambda: str(uuid.uuid4()))


class AccountCreate(BaseModel):
    name: AccountName


class RevokeKey(BaseModel):
    api_key: str


class ExternalAppCreate(BaseModel):
    # Human-readable name of the recognized external application (Decl. 04).
    name: constr(strip_whitespace=True, min_length=1, max_length=64)
    # Comma-separated subset of: credit, burn, balance. Defaults to all three.
    scopes: constr(strip_whitespace=True, min_length=1, max_length=128) = "credit,burn,balance"


class ExternalAppRevoke(BaseModel):
    name: constr(strip_whitespace=True, min_length=1, max_length=64)


class WithdrawalChallengeRequest(BaseModel):
    # Requested by a recognized external application (scope 'burn') before a
    # withdrawal. `external_ref` is the app's own order id.
    account: AccountName
    external_ref: constr(strip_whitespace=True, min_length=1, max_length=128)
    amount: conint(gt=0, le=10**38 - 1) | None = None


class WithdrawalChallengeVerify(BaseModel):
    account: AccountName
    external_ref: constr(strip_whitespace=True, min_length=1, max_length=128)
    code: constr(strip_whitespace=True, min_length=4, max_length=12)
    # Optional binding: when provided, verify() requires it to MATCH the amount
    # the challenge was created for. This stops a confirmation approved for one
    # amount from being used to authorise a different-amount withdrawal.
    amount: conint(gt=0, le=10**38 - 1) | None = None


class WithdrawalChallengeOwnerAction(BaseModel):
    # Owner action proxied by System A (admin key), acting on the owner's account.
    account: AccountName
