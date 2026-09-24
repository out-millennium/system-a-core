# core/db.py

import os
import psycopg
from pathlib import Path
from contextlib import contextmanager
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "system_a_core")
DB_USER = os.getenv("DB_USER", "system_a")
DB_PASSWORD = os.getenv("DB_PASSWORD")


if not DB_PASSWORD:
    raise RuntimeError("DB_PASSWORD environment variable not set")


_CONNINFO = (
    f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
    f"user={DB_USER} password={DB_PASSWORD}"
)

# Connection pool (psycopg_pool). Reuses connections instead of opening one per
# request, which removes the per-call TCP+auth latency and bounds the number of
# server connections. Pool size is configurable via env. If psycopg_pool is not
# installed (e.g. minimal test env), we fall back to direct connections so the
# code keeps working everywhere.
_pool = None
try:
    from psycopg_pool import ConnectionPool  # type: ignore

    _POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
    _POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))
    _pool = ConnectionPool(
        conninfo=_CONNINFO,
        min_size=_POOL_MIN,
        max_size=_POOL_MAX,
        open=False,  # opened lazily on first use
        kwargs={"autocommit": False},
    )
except Exception:
    _pool = None


def get_connection():
    """Direct connection (fallback path / callers that manage their own conn)."""
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


@contextmanager
def get_cursor():
    """Yield a cursor inside a transaction. Uses the pool when available and
    returns the connection to the pool afterwards; otherwise opens/closes a
    direct connection. Same API as before (transaction commits on clean exit,
    rolls back on exception via `with conn`)."""
    if _pool is not None:
        if not getattr(_pool, "_opened", False):
            try:
                # Bounded wait so a DB-free environment fails fast instead of the
                # pool retrying for tens of seconds.
                _pool.open(wait=True, timeout=float(os.getenv("DB_POOL_OPEN_TIMEOUT", "3")))
                _pool._opened = True  # type: ignore[attr-defined]
            except Exception:
                pass
        with _pool.connection() as conn:  # returns to pool automatically
            with conn.cursor() as cur:
                yield cur
        return

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
        # Store a SHA-256 hash of the key (plaintext is never needed after
        # creation). Added non-destructively so existing rows keep working.
        cur.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS key_hash TEXT;")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);")

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

        # Tamper-evident hash chain over the ledger. Each new entry stores the
        # hash of the previous entry (prev_hash) and its own hash (entry_hash =
        # SHA-256 over the canonical fields + prev_hash). Any retroactive edit
        # breaks the chain from that point on, so the whole history is externally
        # verifiable (see verify_ledger_chain). Nullable + additive: pre-existing
        # rows keep NULL and the chain simply starts at the first hashed entry.
        cur.execute("ALTER TABLE ledger ADD COLUMN IF NOT EXISTS prev_hash TEXT;")
        cur.execute("ALTER TABLE ledger ADD COLUMN IF NOT EXISTS entry_hash TEXT;")

        # pgcrypto gives us digest() for SHA-256 inside the trigger.
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

        # BEFORE INSERT trigger that links every new row to the previous one.
        # Implemented in the DB so NO code path (any of init_credit/transfer/
        # burn/system_burn, or a manual insert) can bypass the chain. The lock
        # on the tip serialises concurrent inserts so the chain is linear.
        cur.execute("""
        CREATE OR REPLACE FUNCTION ledger_hash_chain() RETURNS trigger AS $$
        DECLARE
            prev TEXT;
        BEGIN
            -- Most recent already-hashed entry becomes our prev_hash. A plain
            -- advisory lock serialises the tip so two concurrent inserts can't
            -- both read the same prev.
            PERFORM pg_advisory_xact_lock(hashtext('ledger_chain_tip'));
            SELECT entry_hash INTO prev
            FROM ledger
            WHERE entry_hash IS NOT NULL
            ORDER BY id DESC
            LIMIT 1;

            NEW.prev_hash := prev;  -- NULL for the very first hashed entry
            NEW.entry_hash := encode(
                digest(
                    coalesce(prev, '') || '|' ||
                    NEW.operation_id || '|' ||
                    coalesce(NEW.client_operation_id, '') || '|' ||
                    NEW.operation_type || '|' ||
                    coalesce(NEW.from_account, '') || '|' ||
                    coalesce(NEW.to_account, '') || '|' ||
                    NEW.amount::text,
                    'sha256'
                ),
                'hex'
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """)
        cur.execute("DROP TRIGGER IF EXISTS trg_ledger_hash_chain ON ledger;")
        cur.execute("""
        CREATE TRIGGER trg_ledger_hash_chain
        BEFORE INSERT ON ledger
        FOR EACH ROW EXECUTE FUNCTION ledger_hash_chain();
        """)

        # ---- Materialized balances (O(1) reads, short lock) ----
        # A running balance per account, maintained by an AFTER INSERT trigger so
        # it always matches the append-only ledger. get_balance() reads this in
        # O(1) instead of SUM()-ing the whole ledger each time. Correctness is
        # preserved because the same advisory lock that guards a transfer also
        # serialises the balance update within the same transaction.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS balances (
            account TEXT PRIMARY KEY REFERENCES accounts(name),
            balance NUMERIC(38,0) NOT NULL DEFAULT 0
        );
        """)

        cur.execute("""
        CREATE OR REPLACE FUNCTION ledger_apply_balance() RETURNS trigger AS $$
        BEGIN
            IF NEW.to_account IS NOT NULL THEN
                INSERT INTO balances(account, balance)
                VALUES (NEW.to_account, NEW.amount)
                ON CONFLICT (account)
                DO UPDATE SET balance = balances.balance + NEW.amount;
            END IF;
            IF NEW.from_account IS NOT NULL THEN
                INSERT INTO balances(account, balance)
                VALUES (NEW.from_account, -NEW.amount)
                ON CONFLICT (account)
                DO UPDATE SET balance = balances.balance - NEW.amount;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """)
        cur.execute("DROP TRIGGER IF EXISTS trg_ledger_apply_balance ON ledger;")
        cur.execute("""
        CREATE TRIGGER trg_ledger_apply_balance
        AFTER INSERT ON ledger
        FOR EACH ROW EXECUTE FUNCTION ledger_apply_balance();
        """)

        # Backfill balances from the ledger for any account missing a row (first
        # migration, or accounts that predate this table). Idempotent: recomputes
        # from the authoritative ledger and upserts.
        cur.execute("""
        INSERT INTO balances(account, balance)
        SELECT a.name,
               COALESCE(SUM(
                   CASE
                       WHEN l.to_account = a.name THEN l.amount
                       WHEN l.from_account = a.name THEN -l.amount
                       ELSE 0
                   END
               ), 0)
        FROM accounts a
        LEFT JOIN ledger l
          ON l.to_account = a.name OR l.from_account = a.name
        GROUP BY a.name
        ON CONFLICT (account) DO UPDATE SET balance = EXCLUDED.balance;
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

        # Recognized EXTERNAL APPLICATIONS (System A Declaration 04). Each has
        # its own secret key and a limited scope set, issued and revoked
        # independently of the master ADMIN_API_KEY.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS external_apps (
            id SERIAL PRIMARY KEY,
            key TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            scopes TEXT NOT NULL DEFAULT 'credit,burn,balance',
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            revoked_at TIMESTAMPTZ
        );
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_external_apps_key
        ON external_apps(key);
        """)
        # Store a SHA-256 hash of the key; plaintext is never needed after
        # creation. Added non-destructively so existing rows keep working.
        cur.execute("ALTER TABLE external_apps ADD COLUMN IF NOT EXISTS key_hash TEXT;")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_external_apps_hash ON external_apps(key_hash);")

        # Withdrawal confirmation challenges (second-channel confirmation for
        # external-app withdrawals). A recognized external app requests a
        # challenge for an account; the account owner sees it in the System A
        # dashboard and reveals a 6-digit code, which they type back into the
        # external app. Only the code HASH is stored. One active challenge per
        # (account, external_ref); short-lived; limited attempts.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS withdrawal_challenges (
            id           SERIAL PRIMARY KEY,
            account_name TEXT NOT NULL,
            external_ref TEXT NOT NULL,          -- the external app's order id
            app_name     TEXT,                   -- which external app requested
            code_hash    TEXT NOT NULL,          -- SHA-256 of the 6-digit code
            amount       NUMERIC(38,0),          -- units the app intends to burn
            status       TEXT NOT NULL DEFAULT 'pending',  -- pending|approved|confirmed|denied|expired
            attempts     INTEGER NOT NULL DEFAULT 0,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at   TIMESTAMPTZ NOT NULL,
            UNIQUE (account_name, external_ref)
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_wc_account ON withdrawal_challenges(account_name);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_wc_ref ON withdrawal_challenges(external_ref);")
        # Marks when the owner has been shown the "expired while you were away"
        # notice, so each expired request is reported to them exactly once.
        cur.execute("ALTER TABLE withdrawal_challenges ADD COLUMN IF NOT EXISTS notified_at TIMESTAMPTZ;")


def get_external_app_by_key(cur, key):
    """Return (name, scopes_list) for an ACTIVE external app, or None.

    Matches on the SHA-256 hash first; falls back to a legacy plaintext match so
    keys issued before hashing keep working until rotated."""
    from .keys import hash_key

    cur.execute(
        "SELECT name, scopes FROM external_apps "
        "WHERE (key_hash=%s OR key=%s) AND active=TRUE",
        (hash_key(key), key),
    )
    row = cur.fetchone()
    if not row:
        return None
    name, scopes = row
    scope_list = [s.strip() for s in (scopes or "").split(",") if s.strip()]
    return {"name": name, "scopes": scope_list}


def create_external_app(cur, key, name, scopes):
    # Persist ONLY the hash. `key` is NOT NULL/UNIQUE, so we also store the hash
    # there (never the plaintext) to satisfy the constraint; the plaintext key is
    # returned to the caller once and never saved.
    from .keys import hash_key

    kh = hash_key(key)
    cur.execute(
        "INSERT INTO external_apps (key, key_hash, name, scopes) VALUES (%s, %s, %s, %s)",
        (kh, kh, name, scopes),
    )


def list_external_apps(cur):
    cur.execute(
        """
        SELECT name, scopes, active, created_at, revoked_at
        FROM external_apps
        ORDER BY created_at DESC
        """
    )
    return cur.fetchall()


def revoke_external_app(cur, name):
    """Deactivate every key issued under `name`. Returns rows affected."""
    cur.execute(
        "UPDATE external_apps SET active=FALSE, revoked_at=NOW() "
        "WHERE name=%s AND active=TRUE",
        (name,),
    )
    return cur.rowcount


def get_account_by_api_key(cur, api_key):
    # Match on the SHA-256 hash first; fall back to legacy plaintext so keys
    # issued before hashing keep working until rotated.
    from .keys import hash_key

    cur.execute(
        "SELECT account_name FROM api_keys WHERE key_hash=%s OR key=%s",
        (hash_key(api_key), api_key),
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


def verify_ledger_chain():
    """Verify the tamper-evident hash chain over the ledger.

    Recomputes each entry's hash from its fields + the stored prev_hash and
    checks (a) the recomputed hash matches the stored entry_hash and (b) each
    row's prev_hash equals the previous hashed row's entry_hash. Returns a dict:
        { ok: bool, checked: int, first_bad_id: int | None, reason: str | None }
    Rows with NULL entry_hash (created before the chain existed) are skipped;
    the chain is validated from the first hashed row onward.
    """
    import hashlib

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id, operation_id, client_operation_id, operation_type,
                   from_account, to_account, amount, prev_hash, entry_hash
            FROM ledger
            WHERE entry_hash IS NOT NULL
            ORDER BY id ASC
            """
        )
        rows = cur.fetchall()

    prev = None
    checked = 0
    for r in rows:
        (rid, op_id, cop_id, op_type, from_acc, to_acc, amount,
         prev_hash, entry_hash) = r

        # Chain link: this row's prev_hash must equal the previous entry_hash.
        if prev_hash != prev:
            return {"ok": False, "checked": checked, "first_bad_id": rid,
                    "reason": "prev_hash mismatch (broken link)"}

        payload = "|".join([
            prev_hash or "",
            op_id,
            cop_id or "",
            op_type,
            from_acc or "",
            to_acc or "",
            str(amount),
        ])
        recomputed = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        if recomputed != entry_hash:
            return {"ok": False, "checked": checked, "first_bad_id": rid,
                    "reason": "entry_hash mismatch (row altered)"}

        prev = entry_hash
        checked += 1

    return {"ok": True, "checked": checked, "first_bad_id": None, "reason": None}


def close_pool():
    """Close the connection pool and stop its background workers. Call on app
    shutdown or at the end of a test session so the process can exit cleanly."""
    global _pool
    try:
        if _pool is not None:
            _pool.close()
    except Exception:
        pass
