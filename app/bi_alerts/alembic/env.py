from __future__ import annotations

from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

from bi_configs.utils import ROOT_CERTIFICATES_FILENAME
from bi_alerts.settings import from_granular_settings

settings = from_granular_settings()

from sqlalchemy.engine.url import URL
config.set_main_option(
    'sqlalchemy.url',
    str(URL(
        drivername='postgres',
        username=settings.SQLA_DB_CFG_MASTER['user'],
        password=settings.SQLA_DB_CFG_MASTER['password'],
        host=settings.SQLA_DB_CFG_MASTER['host'],
        port=settings.SQLA_DB_CFG_MASTER['port'],
        database=settings.SQLA_DB_CFG_MASTER['database'],
        query={
            'sslmode': settings.SQLA_DB_CFG_MASTER['sslmode'],  # 'verify-full',
            'sslrootcert': ROOT_CERTIFICATES_FILENAME,
            'target_session_attrs': 'read-write',
        }
    ))
)

from bi_alerts.models import db

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
from bi_alerts.models import Base
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix='sqlalchemy.',
        poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
