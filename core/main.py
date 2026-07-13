# core/main.py

from fastapi import FastAPI, HTTPException, Header, Depends, Query, Request
import os
import secrets
import logging
import json

from core.models import InitCredit, Transfer, Burn, AccountCreate, RevokeKey
from core import ledger
from . import db
from .db import init_db, get_ledger, get_account_ledger
from .ledger import get_balance
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from datetime import datetime, timezone


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("system_a")

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")

CORE_VERSION = "0.1.0"

app = FastAPI(title="System A Core")

from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.on_event("startup")
def startup():
    init_db()


def audit(event: str, **kwargs):
    logger.info(json.dumps({
        "event": event,
        "ts": datetime.now(timezone.utc).isoformat(),
        **kwargs
    }))


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

    try:
        ledger.create_account(data.name, api_key)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "account": data.name,
        "api_key": api_key
    }


@app.delete("/v1/account/api_key")
def revoke_key(data: RevokeKey, _: str = Depends(verify_admin)):
    try:
        ledger.revoke_api_key(data.api_key)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"status": "ok"}


@app.post("/v1/init_credit")
def init_credit(data: InitCredit, _: str = Depends(verify_admin)):

    audit("init_credit", account=data.to_account, amount=str(data.amount))

    try:
        ledger.init_credit(data.to_account, data.amount)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {"status": "ok"}


@app.post("/v1/transfer")
@limiter.limit("30/minute")
def transfer(request: Request, data: Transfer, account: str = Depends(verify_api_key), _: str = Depends(verify_admin)):

    if data.from_account != account:
        raise HTTPException(status_code=403, detail="Not owner of account")

    try:
        ledger.transfer(
            data.from_account,
            data.to_account,
            data.amount,
            client_operation_id=data.client_operation_id
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    audit("transfer", from_account=data.from_account, to_account=data.to_account, amount=str(data.amount), op_id=str(data.client_operation_id))
    return {"status": "ok"}


@app.post("/v1/burn")
@limiter.limit("30/minute")
def burn(request: Request, data: Burn, account: str = Depends(verify_api_key), _: str = Depends(verify_admin)):

    if data.from_account != account:
        raise HTTPException(status_code=403, detail="Not owner of account")

    try:
        ledger.burn(
            data.from_account,
            data.amount,
            client_operation_id=data.client_operation_id
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    audit("burn", from_account=data.from_account, amount=str(data.amount), op_id=str(data.client_operation_id))
    return {"status": "ok"}


@app.get("/v1/balance/{account_id}")
def read_balance(account_id: str, _: str = Depends(verify_admin)):

    try:
        balance = get_balance(account_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return {
        "account_id": account_id,
        "balance": balance
    }


@app.get("/v1/ledger")
def read_ledger(limit: int = Query(default=100, ge=1, le=1000), offset: int = 0, _: str = Depends(verify_admin)):

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
def read_account_ledger(account_id: str, limit: int = Query(default=100, ge=1, le=1000), offset: int = 0, account: str = Depends(verify_api_key), _: str = Depends(verify_admin)):

    if account_id != account:
        raise HTTPException(status_code=403, detail="Not owner of account")

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

@app.get("/v1/version")
def version():
    return {
        "service": "core",
        "version": CORE_VERSION,
    }
