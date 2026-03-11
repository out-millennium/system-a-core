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
            name TEXT UNIQUE NOT NULL
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS api_keys (
            key TEXT PRIMARY KEY,
            account_name TEXT REFERENCES accounts(name),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS ledger (
            id SERIAL PRIMARY KEY,

            operation_id TEXT UNIQUE NOT NULL,
            client_operation_id TEXT UNIQUE,

            operation_type TEXT NOT NULL,

            from_account TEXT,
            to_account TEXT,

            amount NUMERIC(38,0) NOT NULL CHECK (amount > 0),

            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ledger_from_account
        ON ledger(from_account);
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ledger_to_account
        ON ledger(to_account);
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ledger_timestamp
        ON ledger(timestamp);
        """)


def get_account_by_api_key(cur, api_key):

    cur.execute(
        "SELECT account_name FROM api_keys WHERE key=%s",
        (api_key,)
    )

    row = cur.fetchone()

    if row:
        return row[0]

    return None


def get_ledger(limit: int = 100, offset: int = 0):

    with get_cursor() as cur:

        cur.execute(
            """
            SELECT
                operation_id,
                client_operation_id,
                operation_type,
                from_account,
                to_account,
                amount,
                timestamp
            FROM ledger
            ORDER BY id
            LIMIT %s OFFSET %s
            """,
            (limit, offset)
        )

        return cur.fetchall()


def get_account_ledger(account: str, limit: int = 100, offset: int = 0):

    with get_cursor() as cur:

        cur.execute(
            """
            SELECT
                operation_id,
                client_operation_id,
                operation_type,
                from_account,
                to_account,
                amount,
                timestamp
            FROM ledger
            WHERE from_account=%s OR to_account=%s
            ORDER BY id
            LIMIT %s OFFSET %s
            """,
            (account, account, limit, offset)
        )

        return cur.fetchall()


def get_operation(operation_id: str):

    with get_cursor() as cur:

        cur.execute(
            """
            SELECT
                operation_id,
                client_operation_id,
                operation_type,
                from_account,
                to_account,
                amount,
                timestamp
            FROM ledger
            WHERE operation_id=%s
            """,
            (operation_id,)
        )

        return cur.fetchone()
