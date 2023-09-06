from __future__ import annotations

import pytest

from bi_constants.enums import CreateDSFrom, ConnectionType

from bi_api_client.dsmaker.primitives import Dataset

from bi_api_lib_testing.data_api_base import DataApiTestBase
from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration

from bi_legacy_test_bundle_tests.api_lib.config import BI_TEST_CONFIG, DB_PARAMS


class DefaultBiApiTestBase(DataApiTestBase):
    conn_type = ConnectionType.clickhouse

    @pytest.fixture(scope='class')
    def bi_test_config(self) -> BiApiTestEnvironmentConfiguration:
        return BI_TEST_CONFIG

    @pytest.fixture(scope='function')
    def environment_readiness(self, enable_all_connectors, initdb_ready) -> None:
        pass

    @pytest.fixture(scope='class')
    def connection_params(self) -> dict:
        host_port, password = DB_PARAMS['clickhouse']
        return dict(
            host=host_port.split(':')[0],
            port=int(host_port.split(':')[1]),
            username='test_user',
            password=password,
        )

    @pytest.fixture(scope='session')
    def dataset_params(self) -> dict:
        return dict(
            source_type=CreateDSFrom.CH_TABLE.name,
            parameters=dict(
                db_name='test_data',
                table_name='sample_superstore',
            ),
        )

    @pytest.fixture(scope='function')
    def dataset_id(self, saved_dataset: Dataset) -> str:
        return saved_dataset.id
