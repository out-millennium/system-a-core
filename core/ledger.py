# core/ledger.py

from . import db


def get_balance(account_id: str) -> int:
    balance = db.get_balance(account_id)
    return balance if balance is not None else 0


def set_balance(account_id: str, balance: int) -> None:
    db.set_balance(account_id, balance)


def log(operation: str, from_acc: str | None, to_acc: str | None, amount: int) -> None:
    db.log(operation, from_acc, to_acc, amount)


def init_credit(to_account: str, amount: int) -> None:
    if amount <= 0:
        raise ValueError("amount must be positive")

    balance = get_balance(to_account)
    set_balance(to_account, balance + amount)
    log("init_credit", None, to_account, amount)
    db.commit()


def transfer(from_account: str, to_account: str, amount: int) -> None:
    if amount <= 0:
        raise ValueError("amount must be positive")

    from_balance = get_balance(from_account)

    if from_balance < amount:
        raise ValueError("insufficient balance")

    to_balance = get_balance(to_account)

    set_balance(from_account, from_balance - amount)
    set_balance(to_account, to_balance + amount)

    log("transfer", from_account, to_account, amount)
    db.commit()


def burn(from_account: str, amount: int) -> None:
    if amount <= 0:
        raise ValueError("amount must be positive")

    balance = get_balance(from_account)

    if balance < amount:
        raise ValueError("insufficient balance")

    set_balance(from_account, balance - amount)
    log("burn", from_account, None, amount)
    db.commit()
