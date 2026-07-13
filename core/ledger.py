# core/ledger.py

import uuid

from .db import get_cursor


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

        cur.execute(
            """
            INSERT INTO api_keys (key, account_name)
            VALUES (%s, %s)
            """,
            (api_key, name)
        )


def get_balance(account: str):

    with get_cursor() as cur:

        cur.execute(
            "SELECT 1 FROM accounts WHERE name=%s",
            (account,)
        )

        if cur.fetchone() is None:
            raise ValueError("account does not exist")

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


def init_credit(to_account: str, amount: int):

    operation_id = _generate_operation_id()

    if amount <= 0:
        raise ValueError("amount must be positive")

    with get_cursor() as cur:

        if not account_exists(cur, to_account):
            raise ValueError("account does not exist")

        _lock_account(cur, to_account)

        cur.execute(
            """
            INSERT INTO ledger(
                operation_id,
                operation_type,
                to_account,
                amount
            )
            VALUES (%s,'init_credit',%s,%s)
            """,
            (
                operation_id,
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
            (from_account, from_account)
        )

        balance = cur.fetchone()[0]

        if balance < amount:
            raise ValueError("insufficient funds")

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
            (from_account, from_account)
        )

        balance = cur.fetchone()[0]

        if balance < amount:
            raise ValueError("insufficient funds")

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


def revoke_api_key(api_key: str):
    with get_cursor() as cur:
        cur.execute("DELETE FROM api_keys WHERE key=%s", (api_key,))
        if cur.rowcount == 0:
            raise ValueError("api key not found")
