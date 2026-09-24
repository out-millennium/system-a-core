"""external apps (recognized external applications, Decl. 04)

Revision ID: 3a1f8c2b7e10
Revises: 2e5d5344d5b5
Create Date: 2026-08-04 16:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "3a1f8c2b7e10"
down_revision: Union[str, Sequence[str], None] = "2e5d5344d5b5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS external_apps (
            id SERIAL PRIMARY KEY,
            key TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            scopes TEXT NOT NULL DEFAULT 'credit,burn,balance',
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            revoked_at TIMESTAMPTZ
        );
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_external_apps_key ON external_apps(key);"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS external_apps;")
