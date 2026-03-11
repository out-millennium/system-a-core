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


def create_account(name: str):

    with get_cursor() as cur:

        cur.execute(
            """
            INSERT INTO accounts (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING
            """,
            (name,)
        )


def get_balance(account: str):

    with get_cursor() as cur:

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


def init_credit(to_account: str, amount: int):

    operation_id = _generate_operation_id()

    if amount <= 0:
        raise ValueError("amount must be positive")

    with get_cursor() as cur:

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
