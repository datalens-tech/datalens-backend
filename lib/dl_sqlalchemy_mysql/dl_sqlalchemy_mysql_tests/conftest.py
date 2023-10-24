from __future__ import annotations

import pytest
import sqlalchemy as sa

from dl_sqlalchemy_mysql.base import register_dialect


def pytest_configure(config):  # noqa
    register_dialect()


@pytest.fixture(scope="session")
def engine_url():
    return "dl_mysql://"


@pytest.fixture(scope="session")
def sa_engine(engine_url):
    return sa.create_engine(engine_url)
