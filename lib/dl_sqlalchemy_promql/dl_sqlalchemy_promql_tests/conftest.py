from __future__ import annotations

import pytest  # type: ignore
import sqlalchemy as sa

from dl_sqlalchemy_promql.base import register_dialect


def pytest_configure(config):  # noqa
    register_dialect()


@pytest.fixture(scope="session")
def engine_url():
    return "bi_promql://user:pass@prometheus-host.domain.net:2135?protocol=https"


@pytest.fixture(scope="session")
def sa_engine(engine_url):
    return sa.create_engine(engine_url)
