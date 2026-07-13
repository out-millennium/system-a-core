"""initial

Revision ID: 2e5d5344d5b5
Revises: 
Create Date: 2026-05-16 09:57:30.732784

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2e5d5344d5b5'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS api_keys (
            key TEXT PRIMARY KEY,
            account_name TEXT REFERENCES accounts(name),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    op.execute("""
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
    op.execute("CREATE INDEX IF NOT EXISTS idx_ledger_from_account ON ledger(from_account);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ledger_to_account ON ledger(to_account);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ledger_timestamp ON ledger(timestamp);")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ledger;")
    op.execute("DROP TABLE IF EXISTS api_keys;")
    op.execute("DROP TABLE IF EXISTS accounts;")
