import pytest
import sqlalchemy as sa

from dl_sqlalchemy_starrocks.base import register_dialect


def pytest_configure(config):
    register_dialect()


@pytest.fixture(scope="session")
def engine_url():
    return "bi_starrocks://"


@pytest.fixture(scope="session")
def sa_engine(engine_url):
    return sa.create_engine(engine_url)
