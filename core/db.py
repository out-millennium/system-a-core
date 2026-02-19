# core/db.py

import sqlite3

conn = None
cursor = None


def init_db():
    global conn, cursor

    conn = sqlite3.connect("database.db", check_same_thread=False)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS accounts (
        account_id TEXT PRIMARY KEY,
        balance INTEGER NOT NULL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        operation TEXT NOT NULL,
        from_account TEXT,
        to_account TEXT,
        amount INTEGER NOT NULL
    )
    """)

    conn.commit()


def get_balance(account_id: str):
    cursor.execute(
        "SELECT balance FROM accounts WHERE account_id = ?",
        (account_id,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def set_balance(account_id: str, balance: int):
    cursor.execute("""
    INSERT INTO accounts (account_id, balance)
    VALUES (?, ?)
    ON CONFLICT(account_id) DO UPDATE SET balance = excluded.balance
    """, (account_id, balance))


def log(operation: str, from_acc: str | None, to_acc: str | None, amount: int):
    cursor.execute("""
    INSERT INTO ledger (operation, from_account, to_account, amount)
    VALUES (?, ?, ?, ?)
    """, (operation, from_acc, to_acc, amount))


def commit():
    conn.commit()
