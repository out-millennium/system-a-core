# core/models.py
from pydantic import BaseModel

class InitCredit(BaseModel):
    to_account: str
    amount: int

class Transfer(BaseModel):
    from_account: str
    to_account: str
    amount: int

class Burn(BaseModel):
    from_account: str
    amount: int
