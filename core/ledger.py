# core/ledger.py

import uuid

import psycopg

from .db import get_cursor
from .keys import hash_key


def _generate_operation_id():
    return str(uuid.uuid4())


def _generate_client_operation_id(client_operation_id):
    if client_operation_id is None:
        return str(uuid.uuid4())
    return client_operation_id


def _lock_account(cur, account: str):
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtext(%s))",
        (account,)
    )


def create_account(name: str, api_key: str):

    with get_cursor() as cur:

        cur.execute(
            """
            INSERT INTO accounts (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING RETURNING name
            """,
            (name,)
        )

        result = cur.fetchone()

        if result is None:
            raise ValueError("account already exists")

        # Store ONLY the hash. `key` is the PRIMARY KEY, so we put the hash there
        # too (never the plaintext); the plaintext is returned to the caller once.
        kh = hash_key(api_key)
        cur.execute(
            """
            INSERT INTO api_keys (key, key_hash, account_name)
            VALUES (%s, %s, %s)
            """,
            (kh, kh, name)
        )


def _balance_from_ledger(cur, account: str):
    """Authoritative O(n) balance straight from the append-only ledger.
    Used as a fallback and by the reconciliation self-check."""
    cur.execute(
        """
        SELECT
            COALESCE(SUM(
                CASE
                    WHEN to_account=%s THEN amount
                    WHEN from_account=%s THEN -amount
                    ELSE 0
                END
            ),0)
        FROM ledger
        """,
        (account, account)
    )
    return cur.fetchone()[0]


def get_balance(account: str):

    with get_cursor() as cur:

        cur.execute(
            "SELECT 1 FROM accounts WHERE name=%s",
            (account,)
        )

        if cur.fetchone() is None:
            raise ValueError("account does not exist")

        # O(1) read from the materialized balances table (kept in sync by a DB
        # trigger). Fall back to summing the ledger if a row is somehow missing.
        cur.execute("SELECT balance FROM balances WHERE account=%s", (account,))
        row = cur.fetchone()
        if row is not None:
            return row[0]
        return _balance_from_ledger(cur, account)


def operation_exists(cur, client_operation_id):

    if client_operation_id is None:
        return False

    cur.execute(
        """
        SELECT 1 FROM ledger
        WHERE client_operation_id=%s
        """,
        (client_operation_id,)
    )

    return cur.fetchone() is not None


def account_exists(cur, account: str):

    cur.execute(
        "SELECT 1 FROM accounts WHERE name=%s",
        (account,)
    )

    return cur.fetchone() is not None


def init_credit(to_account: str, amount: int, client_operation_id: str | None = None):

    operation_id = _generate_operation_id()
    client_operation_id = _generate_client_operation_id(client_operation_id)

    if amount <= 0:
        raise ValueError("amount must be positive")

    with get_cursor() as cur:

        # Idempotent race: another request with the same client_operation_id
        # already recorded this credit (mirrors transfer()/system_burn()). This
        # closes a double-emission window when the same credit is retried.
        if operation_exists(cur, client_operation_id):
            return

        if not account_exists(cur, to_account):
            raise ValueError("account does not exist")

        _lock_account(cur, to_account)

        cur.execute(
            """
            INSERT INTO ledger(
                operation_id,
                client_operation_id,
                operation_type,
                to_account,
                amount
            )
            VALUES (%s,%s,'init_credit',%s,%s)
            """,
            (
                operation_id,
                client_operation_id,
                to_account,
                amount
            )
        )


def transfer(
    from_account: str,
    to_account: str,
    amount: int,
    client_operation_id: str | None = None
):

    operation_id = _generate_operation_id()
    client_operation_id = _generate_client_operation_id(client_operation_id)

    if amount <= 0:
        raise ValueError("amount must be positive")

    if from_account == to_account:
        raise ValueError("cannot transfer to same account")

    with get_cursor() as cur:

        if operation_exists(cur, client_operation_id):
            return

        if not account_exists(cur, from_account):
            raise ValueError("from_account does not exist")

        if not account_exists(cur, to_account):
            raise ValueError("to_account does not exist")

        accounts = sorted([from_account, to_account])

        for acc in accounts:
            _lock_account(cur, acc)

        # Fast balance read from the materialized table (kept in sync by the
        # AFTER INSERT trigger) under the advisory lock we already hold; fall
        # back to summing the ledger if the row is missing.
        cur.execute("SELECT balance FROM balances WHERE account=%s", (from_account,))
        _brow = cur.fetchone()
        balance = _brow[0] if _brow is not None else _balance_from_ledger(cur, from_account)

        if balance < amount:
            raise ValueError("insufficient funds")

        try:
            cur.execute(
                """
                INSERT INTO ledger(
                    operation_id,
                    client_operation_id,
                    operation_type,
                    from_account,
                    to_account,
                    amount
                )
                VALUES (%s,%s,'transfer',%s,%s,%s)
                """,
                (
                    operation_id,
                    client_operation_id,
                    from_account,
                    to_account,
                    amount
                )
            )
        except psycopg.errors.UniqueViolation:
            # A concurrent request with the same client_operation_id won the
            # race after our pre-check. The UNIQUE constraint guarantees the
            # operation is recorded exactly once, so this is an idempotent hit,
            # not an error.
            return


def burn(
    from_account: str,
    amount: int,
    client_operation_id: str | None = None
):

    operation_id = _generate_operation_id()
    client_operation_id = _generate_client_operation_id(client_operation_id)

    if amount <= 0:
        raise ValueError("amount must be positive")

    with get_cursor() as cur:

        if operation_exists(cur, client_operation_id):
            return

        if not account_exists(cur, from_account):
            raise ValueError("account does not exist")

        _lock_account(cur, from_account)

        # Fast balance read from the materialized table (kept in sync by the
        # AFTER INSERT trigger) under the advisory lock we already hold; fall
        # back to summing the ledger if the row is missing.
        cur.execute("SELECT balance FROM balances WHERE account=%s", (from_account,))
        _brow = cur.fetchone()
        balance = _brow[0] if _brow is not None else _balance_from_ledger(cur, from_account)

        if balance < amount:
            raise ValueError("insufficient funds")

        try:
            cur.execute(
                """
                INSERT INTO ledger(
                    operation_id,
                    client_operation_id,
                    operation_type,
                    from_account,
                    amount
                )
                VALUES (%s,%s,'burn',%s,%s)
                """,
                (
                    operation_id,
                    client_operation_id,
                    from_account,
                    amount
                )
            )
        except psycopg.errors.UniqueViolation:
            # Idempotent race: another request with the same client_operation_id
            # already recorded this burn (see transfer() for details).
            return


def system_burn(
    from_account: str,
    amount: int,
    client_operation_id: str | None = None
):
    """
    System-initiated debit: identical mechanics to burn() (removes funds from an
    account, reducing its balance), but recorded as operation_type='system_burn'.

    Unlike burn()/transfer(), the calling endpoint authorises this with the
    ADMIN key ONLY (no account api_key), so an external microservice acting on
    behalf of the system can debit any account — like init_credit() but in the
    opposite direction. The ledger stays append-only and balances are still
    guarded (positive amount, sufficient funds, advisory lock, idempotency).
    """

    operation_id = _generate_operation_id()
    client_operation_id = _generate_client_operation_id(client_operation_id)

    if amount <= 0:
        raise ValueError("amount must be positive")

    with get_cursor() as cur:

        if operation_exists(cur, client_operation_id):
            return

        if not account_exists(cur, from_account):
            raise ValueError("account does not exist")

        _lock_account(cur, from_account)

        # Fast balance read from the materialized table (kept in sync by the
        # AFTER INSERT trigger) under the advisory lock we already hold; fall
        # back to summing the ledger if the row is missing.
        cur.execute("SELECT balance FROM balances WHERE account=%s", (from_account,))
        _brow = cur.fetchone()
        balance = _brow[0] if _brow is not None else _balance_from_ledger(cur, from_account)

        if balance < amount:
            raise ValueError("insufficient funds")

        try:
            cur.execute(
                """
                INSERT INTO ledger(
                    operation_id,
                    client_operation_id,
                    operation_type,
                    from_account,
                    amount
                )
                VALUES (%s,%s,'system_burn',%s,%s)
                """,
                (
                    operation_id,
                    client_operation_id,
                    from_account,
                    amount
                )
            )
        except psycopg.errors.UniqueViolation:
            # Idempotent race: another request with the same client_operation_id
            # already recorded this system_burn (see transfer() for details).
            return


def revoke_api_key(api_key: str):
    with get_cursor() as cur:
        # Delete by hash (new) or plaintext (legacy).
        cur.execute(
            "DELETE FROM api_keys WHERE key_hash=%s OR key=%s",
            (hash_key(api_key), api_key),
        )
        if cur.rowcount == 0:
            raise ValueError("api key not found")
