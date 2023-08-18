from __future__ import annotations

import pytest  # type: ignore
import sqlalchemy as sa

from bi_sqlalchemy_promql.base import register_dialect


def pytest_configure(config):  # noqa
    register_dialect()


@pytest.fixture(scope='session')
def engine_url():
    return 'bi_promql://user:pass@prometheus-host.yandex.net:2135?protocol=https'


@pytest.fixture(scope='session')
def sa_engine(engine_url):
    return sa.create_engine(engine_url)
