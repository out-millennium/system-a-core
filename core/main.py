# core/main.py
from fastapi import FastAPI
from models import InitCredit, Transfer, Burn
import ledger

app = FastAPI(title="System A Core")

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
