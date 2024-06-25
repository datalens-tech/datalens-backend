from __future__ import annotations

import asyncio
from typing import (
    Generator,
    TypeVar,
)

import pytest

from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.database import (
    CoreDbConfig,
    Db,
)
from dl_core_testing.engine_wrapper import TestingEngineWrapper
from dl_core_testing.testcases.connection import BaseConnectionTestClass
import dl_sqlalchemy_metrica_api

from dl_connector_metrica.core.constants import (
    CONNECTION_TYPE_APPMETRICA_API,
    CONNECTION_TYPE_METRICA_API,
)
from dl_connector_metrica.core.testing.connection import (
    make_saved_appmetrica_api_connection,
    make_saved_metrika_api_connection,
)
from dl_connector_metrica.core.us_connection import (
    AppMetricaApiConnection,
    MetrikaApiConnection,
)
import dl_connector_metrica_tests.ext.config as test_config


_CONN_TV = TypeVar("_CONN_TV", MetrikaApiConnection, AppMetricaApiConnection)


class MetricaTestSetup(BaseConnectionTestClass[_CONN_TV]):
    @pytest.fixture(scope="function", autouse=True)
    # FIXME: This fixture is a temporary solution for failing core tests when they are run together with api tests
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
        # Attempt to cover an old version of pytest-asyncio:
        # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
        asyncio.set_event_loop_policy(None)

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return ""

    @pytest.fixture(scope="class")
    def db(self, db_config: CoreDbConfig) -> Db:
        engine_wrapper = TestingEngineWrapper(config=db_config.engine_config)
        return Db(config=db_config, engine_wrapper=engine_wrapper)

    @pytest.fixture(scope="function", autouse=True)
    def shrink_metrika_default_date_period(self, monkeypatch):
        """
        To reduce load for Metrika API and tests run time.
        """
        monkeypatch.setattr(dl_sqlalchemy_metrica_api.base, "DEFAULT_DATE_PERIOD", 3)


class BaseMetricaTestClass(MetricaTestSetup[MetrikaApiConnection]):
    conn_type = CONNECTION_TYPE_METRICA_API
    core_test_config = test_config.CORE_TEST_CONFIG

    @pytest.fixture(scope="function")
    def connection_creation_params(self, metrica_token: str) -> dict:
        return dict(
            counter_id=test_config.METRIKA_SAMPLE_COUNTER_ID,
            token=metrica_token,
        )

    @pytest.fixture(scope="function")
    def saved_connection(
        self,
        sync_us_manager: SyncUSManager,
        connection_creation_params: dict,
    ) -> MetrikaApiConnection:
        return make_saved_metrika_api_connection(sync_usm=sync_us_manager, **connection_creation_params)


class BaseAppMetricaTestClass(MetricaTestSetup[AppMetricaApiConnection]):
    conn_type = CONNECTION_TYPE_APPMETRICA_API
    core_test_config = test_config.CORE_TEST_CONFIG

    @pytest.fixture(scope="function")
    def connection_creation_params(self, metrica_token: str) -> dict:
        return dict(
            counter_id=test_config.APPMETRICA_SAMPLE_COUNTER_ID,
            token=metrica_token,
        )

    @pytest.fixture(scope="function")
    def saved_connection(
        self,
        sync_us_manager: SyncUSManager,
        connection_creation_params: dict,
    ) -> AppMetricaApiConnection:
        return make_saved_appmetrica_api_connection(sync_usm=sync_us_manager, **connection_creation_params)
