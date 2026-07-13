import os
from logging.config import fileConfig
from sqlalchemy import create_engine
from alembic import context

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None

def run_migrations_online() -> None:
    url = (
        f"postgresql+psycopg://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ.get('DB_HOST', 'localhost')}:{os.environ.get('DB_PORT', '5432')}"
        f"/{os.environ['DB_NAME']}"
    )

    connectable = create_engine(url)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()

run_migrations_online()