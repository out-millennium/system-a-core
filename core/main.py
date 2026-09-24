# core/main.py

from fastapi import FastAPI, HTTPException, Header, Depends, Query, Request
from contextlib import asynccontextmanager
import os
import secrets
import logging
import json
import uuid

from core.models import (
    InitCredit, Transfer, Burn, SystemBurn, AccountCreate, RevokeKey,
    ExternalAppCreate, ExternalAppRevoke,
    WithdrawalChallengeRequest, WithdrawalChallengeVerify,
    WithdrawalChallengeOwnerAction,
)
from core import ledger
from core import challenges
from . import db
from .db import init_db, get_ledger, get_account_ledger, verify_ledger_chain
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

# Support KEY ROTATION: in addition to the single ADMIN_API_KEY, accept a
# comma/whitespace-separated list in ADMIN_API_KEYS. To rotate without downtime:
# add the new key to the list, deploy, switch callers to it, then drop the old
# one. All are compared in constant time.
def _load_admin_keys() -> list[str]:
    keys: list[str] = []
    if ADMIN_API_KEY:
        keys.append(ADMIN_API_KEY)
    extra = os.getenv("ADMIN_API_KEYS", "")
    for k in extra.replace(",", " ").split():
        k = k.strip()
        if k and k not in keys:
            keys.append(k)
    return keys


ADMIN_API_KEYS = _load_admin_keys()


def _is_admin_key(candidate: str) -> bool:
    """Constant-time check of a presented key against every accepted admin key.
    Iterates ALL keys (no early return) so timing doesn't reveal which matched."""
    matched = False
    for k in ADMIN_API_KEYS:
        if secrets.compare_digest(candidate, k):
            matched = True
    return matched


CORE_VERSION = "0.1.0"
CORE_ENV = os.getenv("CORE_ENV", "development").lower()


# Lifespan replaces the deprecated @app.on_event("startup"): initialise the DB
# schema once when the app boots.
@asynccontextmanager
async def lifespan(app: FastAPI):
    if CORE_ENV == "production" and not CORE_HMAC_SECRET:
        raise RuntimeError("CORE_HMAC_SECRET is required when CORE_ENV=production")
    init_db()
    yield
    # Clean shutdown: close the DB connection pool if one is in use.
    db.close_pool()


app = FastAPI(title="System A Core", lifespan=lifespan)

from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)


def client_ip_key(request: Request) -> str:
    """Rate-limit key that honours the real client IP behind nginx.

    slowapi's default get_remote_address returns the *peer* address, which
    behind a reverse proxy is nginx itself — so every client would share one
    bucket. nginx sets X-Forwarded-For (client, proxy1, ...); we take the first
    hop. Falls back to the peer address when the header is absent (direct hit).
    """
    xff = request.headers.get("x-forwarded-for")
    if xff:
        first = xff.split(",")[0].strip()
        if first:
            return first
    return get_remote_address(request)


limiter = Limiter(key_func=client_ip_key)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# Request-ID correlation: reuse the id nginx assigns (X-Request-ID) or mint one,
# attach it to the response, and include it in structured logs via audit().
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    rid = request.headers.get("x-request-id") or uuid.uuid4().hex
    request.state.request_id = rid
    response = await call_next(request)
    response.headers["X-Request-ID"] = rid
    return response


def audit(event: str, **kwargs):
    logger.info(json.dumps({
        "event": event,
        "ts": datetime.now(timezone.utc).isoformat(),
        **kwargs
    }))


def verify_admin(x_admin_key: str = Header(...)):

    if not ADMIN_API_KEYS:
        raise HTTPException(status_code=500, detail="Admin key not configured")

    # Constant-time compare against every accepted key (supports rotation).
    if not _is_admin_key(x_admin_key):
        raise HTTPException(status_code=403, detail="Admin key required")


# --- Optional HMAC request signing (anti-replay) -----------------------------
# Defense-in-depth beyond the bearer key: when CORE_HMAC_SECRET is set, callers
# must sign privileged requests. This protects against replay if a TLS terminator
# is compromised or a request is captured. It is OPT-IN: with no secret set it is
# a complete no-op, so existing external apps keep working unchanged.
#
# Signature scheme (caller side):
#   ts   = current unix seconds (sent as X-Timestamp)
#   msg  = f"{ts}.{raw_request_body}"
#   sig  = hex(HMAC_SHA256(CORE_HMAC_SECRET, msg))   (sent as X-Signature)
# The server recomputes and compares in constant time, and rejects timestamps
# outside a small window (replay protection).
CORE_HMAC_SECRET = os.getenv("CORE_HMAC_SECRET")
HMAC_MAX_SKEW_SECONDS = int(os.getenv("CORE_HMAC_MAX_SKEW", "300"))


async def verify_signature(request: Request):
    """Verify X-Signature/X-Timestamp when CORE_HMAC_SECRET is configured.

    No-op if the secret is unset. Uses the raw body so the signature covers the
    exact bytes. Constant-time comparison; bounded timestamp skew for replay
    resistance.
    """
    if not CORE_HMAC_SECRET:
        return  # signing disabled -> no-op (backward compatible)

    import hmac as _hmac
    import hashlib as _hashlib
    import time as _time

    ts = request.headers.get("x-timestamp")
    sig = request.headers.get("x-signature")
    if not ts or not sig:
        raise HTTPException(status_code=401, detail="Signature required")
    try:
        skew = abs(_time.time() - float(ts))
    except ValueError:
        raise HTTPException(status_code=401, detail="Bad timestamp")
    if skew > HMAC_MAX_SKEW_SECONDS:
        raise HTTPException(status_code=401, detail="Timestamp outside window")

    body = await request.body()
    msg = f"{ts}.".encode() + body
    expected = _hmac.new(
        CORE_HMAC_SECRET.encode(), msg, _hashlib.sha256
    ).hexdigest()
    if not secrets.compare_digest(sig, expected):
        raise HTTPException(status_code=401, detail="Invalid signature")


def require_scope(scope: str):
    """
    Dependency factory: authorize a request that presents EITHER the master
    ADMIN_API_KEY (full access — used by System A's own frontend) OR a recognized
    external-application key (System A Declaration 04) whose scope set includes
    `scope`. External keys are issued and revoked independently of the master
    key, so recognizing / withdrawing an external application never touches it.

    Header used: x-admin-key (same header, any of the accepted keys).
    Returns a small dict describing the caller (for auditing).
    """

    def _dep(x_admin_key: str = Header(...)):
        if not ADMIN_API_KEYS:
            raise HTTPException(status_code=500, detail="Admin key not configured")

        # 1) Master key(s) — full access (constant-time compare; supports rotation).
        if _is_admin_key(x_admin_key):
            return {"caller": "system", "name": "system", "scopes": ["*"]}

        # 2) Recognized external application — scoped access.
        with db.get_cursor() as cur:
            app_row = db.get_external_app_by_key(cur, x_admin_key)
        if app_row and scope in app_row["scopes"]:
            return {"caller": "external", "name": app_row["name"], "scopes": app_row["scopes"]}

        raise HTTPException(status_code=403, detail="Admin key required")

    return _dep


def verify_api_key(x_api_key: str = Header(...)):

    with db.get_cursor() as cur:
        account = db.get_account_by_api_key(cur, x_api_key)

    if account is None:
        raise HTTPException(status_code=403, detail="Invalid API key")

    return account


@app.post("/account")
@limiter.limit("10/minute")
def create_account(request: Request, data: AccountCreate):

    api_key = secrets.token_hex(32)

    try:
        ledger.create_account(data.name, api_key)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "account": data.name,
        "api_key": api_key
    }


@app.delete("/account/api_key")
def revoke_key(data: RevokeKey, _: str = Depends(verify_admin)):
    try:
        ledger.revoke_api_key(data.api_key)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"status": "ok"}


@app.post("/init_credit")
@limiter.limit("30/minute")
def init_credit(request: Request, data: InitCredit, caller: dict = Depends(require_scope("credit")), _sig: None = Depends(verify_signature)):

    audit("init_credit", account=data.to_account, amount=str(data.amount), op_id=str(data.client_operation_id), by=caller["name"])

    try:
        ledger.init_credit(data.to_account, data.amount, client_operation_id=data.client_operation_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {"status": "ok"}


@app.post("/transfer")
@limiter.limit("30/minute")
def transfer(request: Request, data: Transfer, account: str = Depends(verify_api_key), _: str = Depends(verify_admin), _sig: None = Depends(verify_signature)):

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


@app.post("/burn")
@limiter.limit("30/minute")
def burn(request: Request, data: Burn, account: str = Depends(verify_api_key), _: str = Depends(verify_admin), _sig: None = Depends(verify_signature)):

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


@app.post("/system_burn")
@limiter.limit("30/minute")
def system_burn(request: Request, data: SystemBurn, caller: dict = Depends(require_scope("burn")), _sig: None = Depends(verify_signature)):
    """
    System-initiated debit of an account. Requires the master admin key OR a
    recognized external-application key with the 'burn' scope (no account
    api_key), so a recognized external application can debit any account — the
    mirror of /init_credit. Recorded in the ledger as operation_type=
    'system_burn'; balance guards and idempotency (client_operation_id) apply.
    """

    try:
        ledger.system_burn(
            data.from_account,
            data.amount,
            client_operation_id=data.client_operation_id
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    audit("system_burn", from_account=data.from_account, amount=str(data.amount), op_id=str(data.client_operation_id), by=caller["name"])
    return {"status": "ok"}


@app.get("/balance/{account_id}")
def read_balance(account_id: str, _: dict = Depends(require_scope("balance"))):

    try:
        balance = get_balance(account_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return {
        "account_id": account_id,
        "balance": balance
    }


@app.get("/ledger")
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


# Declared BEFORE /ledger/{account_id} so "verify" is not parsed as an account.
@app.get("/ledger/verify")
def verify_ledger(_: str = Depends(verify_admin)):
    """Verify the append-only ledger hash chain (tamper-evidence). Admin only."""
    return verify_ledger_chain()


@app.get("/ledger/{account_id}")
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


@app.get("/operation/{operation_id}")
def read_operation(operation_id: str, _: dict = Depends(require_scope("balance"))):
    # Ledger operation details (accounts + amount) are not public: require the
    # same read authorization as /balance (master key or a balance-scoped app).

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


# --- External application management (System A Declaration 04) --------------
# These endpoints issue / list / revoke recognized external-application keys.
# They are protected by the MASTER admin key only: only the System A creator
# grants or withdraws recognition. An external key can never manage other keys.

VALID_SCOPES = {"credit", "burn", "balance"}


@app.post("/external_apps")
def create_external_app(data: ExternalAppCreate, _: str = Depends(verify_admin)):
    scopes = [s.strip() for s in data.scopes.split(",") if s.strip()]
    if not scopes or any(s not in VALID_SCOPES for s in scopes):
        raise HTTPException(
            status_code=400,
            detail=f"scopes must be a comma-separated subset of {sorted(VALID_SCOPES)}",
        )
    key = "ext_" + secrets.token_hex(32)
    normalized = ",".join(scopes)
    with db.get_cursor() as cur:
        db.create_external_app(cur, key, data.name, normalized)
    audit("external_app_created", name=data.name, scopes=normalized)
    # The key is returned ONCE; store it now (it cannot be retrieved later).
    return {"name": data.name, "scopes": scopes, "key": key}


@app.get("/external_apps")
def list_external_apps(_: str = Depends(verify_admin)):
    with db.get_cursor() as cur:
        rows = db.list_external_apps(cur)
    return [
        {
            "name": r[0],
            "scopes": [s.strip() for s in (r[1] or "").split(",") if s.strip()],
            "active": r[2],
            "created_at": r[3],
            "revoked_at": r[4],
        }
        for r in rows
    ]


@app.delete("/external_apps")
def revoke_external_app(data: ExternalAppRevoke, _: str = Depends(verify_admin)):
    with db.get_cursor() as cur:
        n = db.revoke_external_app(cur, data.name)
    if n == 0:
        raise HTTPException(status_code=404, detail="no active app with that name")
    audit("external_app_revoked", name=data.name, keys=n)
    return {"status": "ok", "revoked_keys": n}


# --- Withdrawal confirmation challenges (second-channel confirm) ------------
# A recognized external app (scope 'burn') asks for a challenge before a
# withdrawal; the account owner approves it in the System A dashboard (which
# proxies with the admin key) and reveals a 6-digit code; the owner types the
# code back into the external app, which verifies it here.

@app.post("/withdrawal_challenge")
@limiter.limit("30/minute")
def withdrawal_challenge(request: Request, data: WithdrawalChallengeRequest,
                         caller: dict = Depends(require_scope("burn")),
                         _sig: None = Depends(verify_signature)):
    res = challenges.request_challenge(
        data.account, data.external_ref, caller["name"], data.amount
    )
    audit("withdrawal_challenge_requested", account=data.account,
          external_ref=data.external_ref, by=caller["name"])
    return res


@app.post("/withdrawal_challenge/verify")
@limiter.limit("20/minute")
def withdrawal_challenge_verify(request: Request, data: WithdrawalChallengeVerify,
                               caller: dict = Depends(require_scope("burn")),
                               _sig: None = Depends(verify_signature)):
    result = challenges.verify(data.account, data.external_ref, data.code, data.amount)
    audit("withdrawal_challenge_verify", account=data.account,
          external_ref=data.external_ref, amount=data.amount, result=result, by=caller["name"])
    # Always 200 with a result field; the external app decides. (Avoids leaking
    # timing/existence via status codes.)
    return {"result": result}


@app.get("/withdrawal_challenges/{account_id}")
def withdrawal_challenges_list(account_id: str, _: str = Depends(verify_admin)):
    # Owner-facing (proxied by System A with the admin key): list pending.
    return {"challenges": challenges.list_pending(account_id)}


@app.get("/withdrawal_challenges/{account_id}/expired")
def withdrawal_challenges_expired(account_id: str, _: str = Depends(verify_admin)):
    # Owner-facing: challenges that expired unapproved while the owner was away
    # (returned once each). Used for the dashboard "expired while away" notice.
    return {"expired": challenges.expired_unnotified(account_id)}


@app.post("/withdrawal_challenge/{challenge_id}/approve")
def withdrawal_challenge_approve(challenge_id: int, data: WithdrawalChallengeOwnerAction,
                                 _: str = Depends(verify_admin)):
    res = challenges.approve(data.account, challenge_id)
    if res is None:
        raise HTTPException(status_code=404, detail="challenge not found or not pending")
    audit("withdrawal_challenge_approved", account=data.account, challenge_id=challenge_id)
    return res


@app.post("/withdrawal_challenge/{challenge_id}/deny")
def withdrawal_challenge_deny(challenge_id: int, data: WithdrawalChallengeOwnerAction,
                             _: str = Depends(verify_admin)):
    ok = challenges.deny(data.account, challenge_id)
    if not ok:
        raise HTTPException(status_code=404, detail="challenge not found")
    audit("withdrawal_challenge_denied", account=data.account, challenge_id=challenge_id)
    return {"status": "ok"}


@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/version")
def version():
    return {
        "service": "core",
        "version": CORE_VERSION,
    }
