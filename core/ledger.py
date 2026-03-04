# core/ledger.py

from . import db


def create_account(account_id: str) -> None:
    db.create_account(account_id)


def get_balance(account_id: str):
    balance = db.get_balance(account_id)
    return balance if balance is not None else 0


def init_credit(to_account: str, amount: int) -> None:
    if amount <= 0:
        raise ValueError("amount must be positive")

    db.create_account(to_account)

    with db.get_cursor() as cur:
        cur.execute(
            "UPDATE accounts SET balance = balance + %s WHERE name = %s;",
            (amount, to_account)
        )
        cur.execute(
            """
            INSERT INTO ledger (from_account, to_account, amount)
            VALUES (%s, %s, %s);
            """,
            (None, to_account, amount)
        )


def transfer(from_account: str, to_account: str, amount: int) -> None:
    db.transfer(from_account, to_account, amount)


def burn(from_account: str, amount: int) -> None:
    if amount <= 0:
        raise ValueError("amount must be positive")

    with db.get_cursor() as cur:
        cur.execute(
            "SELECT balance FROM accounts WHERE name = %s FOR UPDATE;",
            (from_account,)
        )
        row = cur.fetchone()

        if row is None:
            raise ValueError("Source account does not exist")

        if row[0] < amount:
            raise ValueError("insufficient balance")

        cur.execute(
            "UPDATE accounts SET balance = balance - %s WHERE name = %s;",
            (amount, from_account)
        )

        cur.execute(
            """
            INSERT INTO ledger (from_account, to_account, amount)
            VALUES (%s, %s, %s);
            """,
            (from_account, None, amount)
        )
