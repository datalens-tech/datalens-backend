from __future__ import annotations

from typing import TypeVar

import pytest
import sqlalchemy_metrika_api

from bi_constants.enums import ConnectionType

from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_connector_metrica.core.constants import CONNECTION_TYPE_METRICA_API, CONNECTION_TYPE_APPMETRICA_API
from bi_connector_metrica.core.us_connection import MetrikaApiConnection, AppMetricaApiConnection
from bi_connector_metrica.core.testing.connection import (
    make_saved_metrika_api_connection, make_saved_appmetrica_api_connection
)
from bi_core_testing.testcases.connection import BaseConnectionTestClass
from bi_core_testing.database import Db, CoreDbConfig

import bi_connector_metrica_tests.ext.config as common_test_config
import bi_connector_metrica_tests.ext.core.config as test_config


_CONN_TV = TypeVar('_CONN_TV', MetrikaApiConnection, AppMetricaApiConnection)


class MetricaTestSetup(BaseConnectionTestClass[_CONN_TV]):
    """
    Metrica has a completely external DB, so we need to skip
    DB initialization steps
    """

    @pytest.fixture(scope='class')
    def db_url(self) -> str:
        return ''

    @pytest.fixture(scope='class')
    def db(self, db_config: CoreDbConfig) -> Db:
        pass

    @pytest.fixture(scope='function', autouse=True)
    def shrink_metrika_default_date_period(self, monkeypatch):
        """
        To reduce load to Metrika API and tests run time.
        """
        monkeypatch.setattr(sqlalchemy_metrika_api.base, 'DEFAULT_DATE_PERIOD', 3)


class BaseMetricaTestClass(MetricaTestSetup[MetrikaApiConnection]):
    conn_type = CONNECTION_TYPE_METRICA_API
    core_test_config = common_test_config.CORE_TEST_CONFIG

    @pytest.fixture(scope='function')
    def connection_creation_params(self, metrica_token: str) -> dict:
        return dict(
            counter_id=test_config.METRIKA_SAMPLE_COUNTER_ID,
            token=metrica_token,
        )

    @pytest.fixture(scope='function')
    def saved_connection(
            self, sync_us_manager: SyncUSManager, connection_creation_params: dict,
    ) -> MetrikaApiConnection:
        return make_saved_metrika_api_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params
        )


class BaseAppMetricaTestClass(MetricaTestSetup[AppMetricaApiConnection]):
    conn_type = CONNECTION_TYPE_APPMETRICA_API
    core_test_config = common_test_config.CORE_TEST_CONFIG

    @pytest.fixture(scope='function')
    def connection_creation_params(self, metrica_token: str) -> dict:
        return dict(
            counter_id=test_config.APPMETRICA_SAMPLE_COUNTER_ID,
            token=metrica_token,
        )

    @pytest.fixture(scope='function')
    def saved_connection(
            self, sync_us_manager: SyncUSManager, connection_creation_params: dict,
    ) -> AppMetricaApiConnection:
        return make_saved_appmetrica_api_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params
        )
