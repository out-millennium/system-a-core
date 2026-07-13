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


class Transfer(BaseModel):
    from_account: AccountName
    to_account: AccountName
    amount: conint(gt=0, le=10**38 - 1)
    client_operation_id: ClientOperationId = Field(default_factory=lambda: str(uuid.uuid4()))


class Burn(BaseModel):
    from_account: AccountName
    amount: conint(gt=0, le=10**38 - 1)
    client_operation_id: ClientOperationId = Field(default_factory=lambda: str(uuid.uuid4()))


class AccountCreate(BaseModel):
    name: AccountName


class RevokeKey(BaseModel):
    api_key: str
