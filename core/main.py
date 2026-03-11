# core/main.py

from fastapi import FastAPI, HTTPException, Header, Depends
import os
import secrets
import logging
import uuid

from core.models import InitCredit, Transfer, Burn, AccountCreate
from core import ledger
from . import db
from .db import init_db, get_ledger, get_account_ledger
from .ledger import get_balance


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("system_a")

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")

app = FastAPI(title="System A Core")


@app.on_event("startup")
def startup():
    init_db()


def verify_admin(x_admin_key: str = Header(...)):

    if ADMIN_API_KEY is None:
        raise HTTPException(status_code=500, detail="Admin key not configured")

    if x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=403, detail="Admin key required")


def verify_api_key(x_api_key: str = Header(...)):

    with db.get_cursor() as cur:
        account = db.get_account_by_api_key(cur, x_api_key)

    if account is None:
        raise HTTPException(status_code=403, detail="Invalid API key")

    return account


@app.post("/v1/account")
def create_account(data: AccountCreate):

    api_key = secrets.token_hex(32)

    ledger.create_account(data.name)

    with db.get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO api_keys (key, account_name)
            VALUES (%s,%s)
            """,
            (api_key, data.name)
        )

    return {
        "account": data.name,
        "api_key": api_key
    }


@app.post("/v1/init_credit")
def init_credit(data: InitCredit, _: str = Depends(verify_admin)):

    logger.info("init_credit %s amount=%s", data.to_account, data.amount)

    ledger.init_credit(data.to_account, data.amount)

    return {"status": "ok"}


@app.post("/v1/transfer")
def transfer(data: Transfer, account: str = Depends(verify_api_key)):

    if data.from_account != account:
        raise HTTPException(status_code=403, detail="Not owner of account")

    client_id = data.client_operation_id or str(uuid.uuid4())

    ledger.transfer(
        data.from_account,
        data.to_account,
        data.amount,
        client_operation_id=client_id
    )

    return {"status": "ok"}


@app.post("/v1/burn")
def burn(data: Burn, account: str = Depends(verify_api_key)):

    if data.from_account != account:
        raise HTTPException(status_code=403, detail="Not owner of account")

    ledger.burn(
        data.from_account,
        data.amount,
        client_operation_id=data.client_operation_id
    )

    return {"status": "ok"}


@app.get("/v1/balance/{account_id}")
def read_balance(account_id: str):

    balance = get_balance(account_id)

    return {
        "account_id": account_id,
        "balance": balance
    }


@app.get("/v1/ledger")
def read_ledger(limit: int = 100, offset: int = 0, _: str = Depends(verify_admin)):

    rows = get_ledger(limit, offset)

    result = []

    for r in rows:
        result.append({
            "operation_id": r[0],
            "client_operation_id": r[1],
            "operation_type": r[2],
            "from_account": r[3],
            "to_account": r[4],
            "amount": r[5],
            "timestamp": r[6]
        })

    return result


@app.get("/v1/ledger/{account_id}")
def read_account_ledger(account_id: str, limit: int = 100, offset: int = 0):

    rows = get_account_ledger(account_id, limit, offset)

    result = []

    for r in rows:
        result.append({
            "operation_id": r[0],
            "client_operation_id": r[1],
            "operation_type": r[2],
            "from_account": r[3],
            "to_account": r[4],
            "amount": r[5],
            "timestamp": r[6]
        })

    return result


@app.get("/v1/operation/{operation_id}")
def read_operation(operation_id: str):

    row = db.get_operation(operation_id)

    if row is None:
        raise HTTPException(status_code=404, detail="Operation not found")

    return {
        "operation_id": row[0],
        "client_operation_id": row[1],
        "operation_type": row[2],
        "from_account": row[3],
        "to_account": row[4],
        "amount": row[5],
        "timestamp": row[6]
    }


@app.get("/health")
def health():
    return {"status": "ok"}
