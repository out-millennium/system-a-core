# Ledger invariant tests (require a real Postgres). These are the money-critical
# guarantees: no double-spend, idempotency by client_operation_id, concurrency
# safety under the advisory lock, and the materialized balance matching the
# authoritative ledger sum + an intact hash chain.
#
# Skipped automatically when no database is reachable, so the DB-free suite
# still runs anywhere. To run these: start Postgres and set DB_* env (the
# healthcheck / CI provisions one).

import os
import uuid
import concurrent.futures

import pytest

# Only attempt DB tests when a password is configured (our db.py requires it).
if not os.getenv("DB_PASSWORD"):
    pytest.skip("no DB configured (set DB_PASSWORD)", allow_module_level=True)

try:
    from core import db, ledger
    db.init_db()
    _DB_OK = True
except Exception as e:  # pragma: no cover - environment dependent
    _DB_OK = False
    _DB_ERR = e

pytestmark = pytest.mark.skipif(not _DB_OK, reason="database not reachable")


def _acct():
    name = "t_" + uuid.uuid4().hex[:10]
    ledger.create_account(name, "k_" + uuid.uuid4().hex)
    return name


def test_credit_then_balance():
    a = _acct()
    ledger.init_credit(a, 500)
    assert ledger.get_balance(a) == 500


def test_transfer_moves_exactly():
    a, b = _acct(), _acct()
    ledger.init_credit(a, 1000)
    ledger.transfer(a, b, 250)
    assert ledger.get_balance(a) == 750
    assert ledger.get_balance(b) == 250


def test_insufficient_funds_blocked():
    a, b = _acct(), _acct()
    ledger.init_credit(a, 100)
    with pytest.raises(ValueError):
        ledger.transfer(a, b, 101)
    assert ledger.get_balance(a) == 100
    assert ledger.get_balance(b) == 0


def test_idempotent_client_operation_id():
    a, b = _acct(), _acct()
    ledger.init_credit(a, 1000)
    cop = "cop_" + uuid.uuid4().hex
    # Same client_operation_id applied twice must record ONCE.
    ledger.transfer(a, b, 100, client_operation_id=cop)
    ledger.transfer(a, b, 100, client_operation_id=cop)
    assert ledger.get_balance(b) == 100  # not 200
    assert ledger.get_balance(a) == 900


def test_no_double_spend_under_concurrency():
    # Fund an account with exactly enough for ONE of two concurrent transfers.
    a, b = _acct(), _acct()
    ledger.init_credit(a, 100)

    def do():
        try:
            ledger.transfer(a, b, 100, client_operation_id="cop_" + uuid.uuid4().hex)
            return "ok"
        except ValueError:
            return "rejected"

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(lambda _: do(), range(8)))

    # Exactly one transfer may succeed; the rest must be rejected. The balance
    # can never go negative (advisory lock + fresh balance read prevent it).
    assert results.count("ok") == 1, results
    assert ledger.get_balance(a) == 0
    assert ledger.get_balance(b) == 100


def test_materialized_balance_matches_ledger():
    a, b = _acct(), _acct()
    ledger.init_credit(a, 777)
    ledger.transfer(a, b, 333)
    ledger.burn(b, 33)
    with db.get_cursor() as cur:
        auth_a = ledger._balance_from_ledger(cur, a)
        auth_b = ledger._balance_from_ledger(cur, b)
    assert ledger.get_balance(a) == auth_a == 444
    assert ledger.get_balance(b) == auth_b == 300


def test_hash_chain_verifies():
    a = _acct()
    ledger.init_credit(a, 10)
    res = db.verify_ledger_chain()
    assert res["ok"] is True, res


def test_withdrawal_challenge_amount_binding():
    """A confirmation approved for one amount cannot authorise a different one."""
    from core import challenges
    import uuid as _uuid
    acc = "wb_" + _uuid.uuid4().hex[:8]
    ext = "ord_" + _uuid.uuid4().hex[:8]
    challenges.request_challenge(acc, ext, "Meridian", 1000)
    pend = challenges.list_pending(acc)
    appr = challenges.approve(acc, pend[0]["id"])
    code = appr["code"]
    # Wrong amount is rejected without consuming the challenge...
    assert challenges.verify(acc, ext, code, amount=999) == "amount_mismatch"
    # ...and the exact amount confirms.
    assert challenges.verify(acc, ext, code, amount=1000) == "confirmed"
