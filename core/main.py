# core/main.py

from fastapi import FastAPI, HTTPException
from core.models import InitCredit, Transfer, Burn
from core import ledger
from .db import get_balance, init_db, get_ledger_entries

app = FastAPI(title="System A Core")


@app.on_event("startup")
def startup():
    init_db()


@app.post("/init_credit")
def init_credit(data: InitCredit):
    ledger.init_credit(data.to_account, data.amount)
    return {"status": "ok"}


@app.post("/transfer")
def transfer(data: Transfer):
    ledger.transfer(data.from_account, data.to_account, data.amount)
    return {"status": "ok"}


@app.post("/burn")
def burn(data: Burn):
    ledger.burn(data.from_account, data.amount)
    return {"status": "ok"}


@app.get("/balance/{account_id}")
def read_balance(account_id: str):
    balance = get_balance(account_id)

    if balance is None:
        raise HTTPException(status_code=404, detail="Account not found")

    return {
        "account_id": account_id,
        "balance": balance
    }


@app.get("/ledger")
def read_ledger(limit: int = 50, offset: int = 0):
    try:
        return get_ledger_entries(limit=limit, offset=offset)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/ledger/{account_id}")
def read_account_ledger(account_id: str, limit: int = 50, offset: int = 0):
    try:
        return get_ledger_entries(limit=limit, offset=offset, account_id=account_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
