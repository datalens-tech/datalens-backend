import time

from sqlalchemy import create_engine, exc as sa_exc
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from bi_alerts.settings import from_granular_settings


def get_pg_engine_from_config() -> Engine:
    settings = from_granular_settings()
    engine = create_engine(URL(
        drivername='postgresql',
        username=settings.SQLA_DB_CFG_MASTER['user'],
        password=settings.SQLA_DB_CFG_MASTER['password'],
        host=settings.SQLA_DB_CFG_MASTER['host'],
        port=settings.SQLA_DB_CFG_MASTER['port'],
        database=settings.SQLA_DB_CFG_MASTER['database'],
    ), pool_pre_ping=True)
    return engine


def ensure_db_is_up(timeout: int = 60):
    engine = get_pg_engine_from_config()
    tick = 10
    started = time.monotonic()
    while True:
        try:
            engine.execute('select 1')
        except sa_exc.OperationalError:
            if time.monotonic() - started >= timeout:
                raise
            time.sleep(tick)
        else:
            break
