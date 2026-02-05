# core/ledger.py
from db import cursor, conn

def get_balance(account_id):
    cursor.execute("SELECT balance FROM accounts WHERE account_id = ?", (account_id,))
    row = cursor.fetchone()
    return row[0] if row else 0

def set_balance(account_id, balance):
    cursor.execute("""
    INSERT INTO accounts (account_id, balance)
    VALUES (?, ?)
    ON CONFLICT(account_id) DO UPDATE SET balance = excluded.balance
    """, (account_id, balance))

def log(operation, from_acc, to_acc, amount):
    cursor.execute("""
    INSERT INTO ledger (operation, from_account, to_account, amount)
    VALUES (?, ?, ?, ?)
    """, (operation, from_acc, to_acc, amount))

def init_credit(to_account, amount):
    if amount <= 0:
        raise ValueError("amount must be positive")
    balance = get_balance(to_account)
    set_balance(to_account, balance + amount)
    log("init_credit", None, to_account, amount)
    conn.commit()

def transfer(from_account, to_account, amount):
    if amount <= 0:
        raise ValueError("amount must be positive")
    if get_balance(from_account) < amount:
        raise ValueError("insufficient balance")
    set_balance(from_account, get_balance(from_account) - amount)
    set_balance(to_account, get_balance(to_account) + amount)
    log("transfer", from_account, to_account, amount)
    conn.commit()

def burn(from_account, amount):
    if amount <= 0:
        raise ValueError("amount must be positive")
    if get_balance(from_account) < amount:
        raise ValueError("insufficient balance")
    set_balance(from_account, get_balance(from_account) - amount)
    log("burn", from_account, None, amount)
    conn.commit()
