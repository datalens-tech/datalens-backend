from __future__ import annotations

from typing import Generic, TypeVar

import pytest

from bi_core_testing.testcases.connection import BaseConnectionTestClass
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.us_connection import ConnectionCHFilteredSubselectByPuidBase

import bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config as test_config


_CONN_TV = TypeVar('_CONN_TV', bound=ConnectionCHFilteredSubselectByPuidBase)


class BaseClickhouseFilteredSubselectByPuidTestClass(BaseConnectionTestClass, Generic[_CONN_TV]):
    core_test_config = test_config.CORE_TEST_CONFIG
    inst_specific_sr_factory = test_config.YC_SR_FACTORY

    @pytest.fixture(scope='class')
    def db_url(self) -> str:
        return test_config.DB_CORE_URL

    @pytest.fixture(scope='function')
    def connection_creation_params(self, ext_test_blackbox_user_oauth: str) -> dict:
        return dict(
            endpoint=test_config.CoreConnectionSettings.ENDPOINT,
            cluster_name=test_config.CoreConnectionSettings.CLUSTER_NAME,
            max_execution_time=test_config.CoreConnectionSettings.MAX_EXECUTION_TIME,
            token=ext_test_blackbox_user_oauth,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )


class ClickhouseFilteredSubselectByPuidTestClassWithWrongAuth(
        BaseClickhouseFilteredSubselectByPuidTestClass[_CONN_TV], Generic[_CONN_TV]
):
    @pytest.fixture(scope='function')
    def connection_creation_params(self) -> dict:
        return dict(
            endpoint=test_config.CoreConnectionSettings.ENDPOINT,
            cluster_name=test_config.CoreConnectionSettings.CLUSTER_NAME,
            max_execution_time=test_config.CoreConnectionSettings.MAX_EXECUTION_TIME,
            token='invalid',
        )
