from __future__ import annotations

import aiohttp.pytest_plugin
import aiohttp.test_utils
import pytest

from bi_alerts.app import create_app
from bi_alerts.utils.solomon import SolomonClient

from bi_alerts_tests.utils import get_pg_engine_from_config, ensure_db_is_up


try:
    del aiohttp.pytest_plugin.loop
except AttributeError:
    pass


@pytest.fixture(autouse=True, scope='session')
def wait_for_db():
    ensure_db_is_up()


@pytest.fixture
def web_app(loop, aiohttp_client):
    return loop.run_until_complete(
        aiohttp_client(
            create_app(
                crypto_config={
                    'DL_CRY_ACTUAL_KEY_ID': 'test_key',
                    'DL_CRY_KEY_VAL_ID_test_key': '0DONKF1G2lkLG_8-2ruzu52YeeDpWAViZaDUxarl1B0=',
                },
            )
        )
    )


@pytest.fixture
async def db_models():
    from bi_alerts.models import Base

    engine = get_pg_engine_from_config()
    Base.metadata.drop_all(engine)  # to ensure an empty db
    Base.metadata.create_all(engine)

    yield
    Base.metadata.drop_all(engine)
    engine.execute('DROP TABLE IF EXISTS alembic_version')


@pytest.fixture
def mock_solomon_api(monkeypatch):
    async def mock_pass(*args, **kwargs):
        pass

    async def mock_solomon_get_alert(*args, **kwargs):
        return {'version': 1}

    monkeypatch.setattr(SolomonClient, 'create_channel_if_not_exists', mock_pass)
    monkeypatch.setattr(SolomonClient, 'create_alert', mock_pass)
    monkeypatch.setattr(SolomonClient, 'delete_alert', mock_pass)
    monkeypatch.setattr(SolomonClient, 'update_alert', mock_pass)
    monkeypatch.setattr(SolomonClient, 'get_alert', mock_solomon_get_alert)


@pytest.fixture
def mock_blackbox_api(monkeypatch):
    async def mock_blackbox_oauth_check(*args, **kwargs):
        return {
            'username': 'username',
            'user_id': '1231231231',
        }

    monkeypatch.setattr('bi_alerts.utils.blackbox.authenticate_async', mock_blackbox_oauth_check)
