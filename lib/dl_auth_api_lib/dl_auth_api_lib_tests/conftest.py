import asyncio
import logging
import os

import aiohttp.pytest_plugin
import pytest

from dl_api_commons.aio.typing import AIOHTTPMiddleware
from dl_api_commons.base_models import (
    NoAuthData,
    TenantCommon,
)
from dl_api_commons.client.common import DLCommonAPIClient
from dl_auth_api_lib.app import OAuthApiAppFactory
from dl_auth_api_lib.oauth.yandex import YandexOAuthClient
from dl_auth_api_lib.settings import AuthAPISettings
from dl_testing.utils import get_default_aiohttp_session


LOGGER = logging.getLogger(__name__)


pytest_plugins = ("aiohttp.pytest_plugin",)

try:
    del aiohttp.pytest_plugin.loop
except AttributeError:
    pass


@pytest.fixture(autouse=True)
def loop(event_loop):
    """
    Preventing creation of new loop by `aiohttp.pytest_plugin` loop fixture in favor of pytest-asyncio one
    And set loop pytest-asyncio created loop as default for thread
    """
    asyncio.set_event_loop(event_loop)
    return event_loop


@pytest.fixture(scope="function")
def config_path() -> str:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dir_path, "config.yaml")


@pytest.fixture(scope="function")
def oauth_app_settings(monkeypatch, config_path):
    monkeypatch.setenv("CONFIG_PATH", config_path)
    monkeypatch.setenv("AUTH_CLIENTS__APP_METRICA__CLIENT_SECRET", "123pass")
    monkeypatch.setenv("AUTH_CLIENTS__YA_CLIENT__CLIENT_SECRET", "pass1234")
    settings = AuthAPISettings(
        auth_clients=dict(
            metrica=YandexOAuthClient(
                conn_type="metrica",
                client_id="metrica",
                client_secret="pass123",
                redirect_uri="localhost",
                scope="read",
            ),
            app_metrica=dict(
                auth_type="yandex",
                conn_type="app_metrica",
                client_id="app_metrica",
                redirect_uri="localhost",
            ),
            custom_conn=dict(
                auth_type="yandex",
                conn_type="custom_conn",
                client_id="custom_conn",
                client_secret="pass321",
                redirect_uri="localhost",
                auth_url="https://oauth.yandex.com/authorize?",
                token_url="https://oauth.yandex.com/token",
            ),
            gsheets=dict(
                auth_type="google",
                conn_type="gsheets",
                client_id="gsheets_id",
                client_secret="gsheets_pass",
                redirect_uri="localhost",
                scope="https://www.googleapis.com/auth/spreadsheets.readonly",
            ),
        )
    )
    yield settings


class TestingOAuthApiAppFactory(OAuthApiAppFactory[AuthAPISettings]):
    def get_auth_middlewares(self) -> list[AIOHTTPMiddleware]:
        return []


@pytest.fixture(scope="function")
def oauth_app(loop, aiohttp_client, oauth_app_settings):
    app_factory = TestingOAuthApiAppFactory(settings=oauth_app_settings)
    app = app_factory.create_app()
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope="function")
async def oauth_app_client(oauth_app) -> DLCommonAPIClient:
    async with get_default_aiohttp_session() as session:
        yield DLCommonAPIClient(
            base_url=f"http://{oauth_app.host}:{oauth_app.port}",
            tenant=TenantCommon(),
            auth_data=NoAuthData(),
            session=session,
        )
