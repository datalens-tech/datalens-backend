from __future__ import annotations

import pytest
import sqlalchemy as sa

from dl_sqlalchemy_ydb.dialect import register_dialect


def pytest_configure(config):  # noqa
    register_dialect()


@pytest.fixture(scope="session")
def engine_url():
    return "ydb://"


@pytest.fixture(scope="session")
def sa_engine(engine_url):
    return sa.create_engine(engine_url)


@pytest.fixture(scope="session")
def connection(sa_engine):
    with sa_engine.connect() as conn:
        yield conn
