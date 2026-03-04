# core/db.py

import os
import psycopg
from contextlib import contextmanager


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "system_a_core")
DB_USER = os.getenv("DB_USER", "system_a")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not DB_PASSWORD:
    raise RuntimeError("DB_PASSWORD environment variable not set")


def get_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


@contextmanager
def get_cursor():
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                yield cur
    finally:
        conn.close()


def init_db():
    with get_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                balance NUMERIC NOT NULL DEFAULT 0 CHECK (balance >= 0)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS ledger (
                id SERIAL PRIMARY KEY,
                from_account TEXT,
                to_account TEXT,
                amount NUMERIC(38,0) NOT NULL CHECK (amount > 0),
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CHECK (from_account IS NOT NULL OR to_account IS NOT NULL)
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ledger_from_account
            ON ledger (from_account);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ledger_to_account
            ON ledger (to_account);
        """)

        cur.execute("""
            CREATE OR REPLACE FUNCTION prevent_ledger_update()
            RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'Ledger is immutable. UPDATE is forbidden.';
            END;
            $$ LANGUAGE plpgsql;
        """)

        cur.execute("""
            DROP TRIGGER IF EXISTS ledger_no_update ON ledger;
            CREATE TRIGGER ledger_no_update
            BEFORE UPDATE ON ledger
            FOR EACH ROW
            EXECUTE FUNCTION prevent_ledger_update();
        """)

        cur.execute("""
            CREATE OR REPLACE FUNCTION prevent_ledger_delete()
            RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'Ledger is immutable. DELETE is forbidden.';
            END;
            $$ LANGUAGE plpgsql;
        """)

        cur.execute("""
            DROP TRIGGER IF EXISTS ledger_no_delete ON ledger;
            CREATE TRIGGER ledger_no_delete
            BEFORE DELETE ON ledger
            FOR EACH ROW
            EXECUTE FUNCTION prevent_ledger_delete();
        """)


def create_account(name):
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING;
            """,
            (name,)
        )


def transfer(from_account, to_account, amount):
    if amount <= 0:
        raise ValueError("Amount must be positive")

    if from_account == to_account:
        raise ValueError("Cannot transfer to the same account")

    ordered = sorted([from_account, to_account])

    with get_cursor() as cur:
        cur.execute(
            "SELECT name, balance FROM accounts WHERE name = %s FOR UPDATE;",
            (ordered[0],)
        )
        first = cur.fetchone()

        cur.execute(
            "SELECT name, balance FROM accounts WHERE name = %s FOR UPDATE;",
            (ordered[1],)
        )
        second = cur.fetchone()

        if first is None or second is None:
            raise ValueError("One or both accounts do not exist")

        balances = {
            first[0]: first[1],
            second[0]: second[1],
        }

        if balances[from_account] < amount:
            raise ValueError("Insufficient funds")

        cur.execute(
            "UPDATE accounts SET balance = balance - %s WHERE name = %s;",
            (amount, from_account)
        )

        cur.execute(
            "UPDATE accounts SET balance = balance + %s WHERE name = %s;",
            (amount, to_account)
        )

        cur.execute(
            """
            INSERT INTO ledger (from_account, to_account, amount)
            VALUES (%s, %s, %s);
            """,
            (from_account, to_account, amount)
        )


def get_balance(name):
    with get_cursor() as cur:
        cur.execute(
            "SELECT balance FROM accounts WHERE name = %s;",
            (name,)
        )
        row = cur.fetchone()
        return row[0] if row else None


def get_ledger():
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id, from_account, to_account, amount, timestamp
            FROM ledger
            ORDER BY id;
            """
        )
        return cur.fetchall()


def get_ledger_entries(limit: int = 50, offset: int = 0, account_id: str | None = None):
    if limit < 0 or offset < 0:
        raise ValueError("limit and offset must be non-negative")

    with get_cursor() as cur:
        if account_id:
            cur.execute(
                """
                SELECT id, from_account, to_account, amount, timestamp
                FROM ledger
                WHERE from_account = %s OR to_account = %s
                ORDER BY id
                LIMIT %s OFFSET %s;
                """,
                (account_id, account_id, limit, offset)
            )
        else:
            cur.execute(
                """
                SELECT id, from_account, to_account, amount, timestamp
                FROM ledger
                ORDER BY id
                LIMIT %s OFFSET %s;
                """,
                (limit, offset)
            )

        return cur.fetchall()
