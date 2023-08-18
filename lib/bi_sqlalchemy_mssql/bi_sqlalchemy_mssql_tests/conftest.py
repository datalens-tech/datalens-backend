from __future__ import annotations

import pytest
import sqlalchemy as sa

from bi_sqlalchemy_mssql.base import register_dialect


def pytest_configure(config):  # noqa
    register_dialect()


@pytest.fixture(scope='session')
def engine_url():
    return 'bi_mssql://'


@pytest.fixture(scope="session")
def sa_engine(engine_url):
    return sa.create_engine(engine_url)
