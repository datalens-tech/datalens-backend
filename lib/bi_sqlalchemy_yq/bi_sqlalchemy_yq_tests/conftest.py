from __future__ import annotations

import os

import pytest
import sqlalchemy as sa

from bi_sqlalchemy_yq.base import register_dialect


def pytest_configure(config):  # noqa
    register_dialect()


@pytest.fixture(scope="session")
def engine_url():
    return (
        os.environ.get("BISAYQ_ENGINE_URL")
        or "bi_yq://:fake_token@grpc.yandex-query.cloud-preprod.yandex.net:2135//root/default?cloud_id=fake_cloud_id&folder_id=fake_folder_id"
    )


@pytest.fixture(scope="session")
def sa_engine(engine_url):
    return sa.create_engine(engine_url)
