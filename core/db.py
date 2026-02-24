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


def get_ledger_entries(limit: int = 50, offset: int = 0, account_id: str | None = None):
    if limit < 1 or limit > 500:
        raise ValueError("limit must be between 1 and 500")

    if offset < 0:
        raise ValueError("offset must be >= 0")

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    if account_id:
        cursor.execute("""
            SELECT id, operation, from_account, to_account, amount
            FROM ledger
            WHERE from_account = ? OR to_account = ?
            ORDER BY id ASC
            LIMIT ? OFFSET ?
        """, (account_id, account_id, limit, offset))
    else:
        cursor.execute("""
            SELECT id, operation, from_account, to_account, amount
            FROM ledger
            ORDER BY id ASC
            LIMIT ? OFFSET ?
        """, (limit, offset))

    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "id": row[0],
            "operation": row[1],
            "from_account": row[2],
            "to_account": row[3],
            "amount": row[4],
        }
        for row in rows
    ]
