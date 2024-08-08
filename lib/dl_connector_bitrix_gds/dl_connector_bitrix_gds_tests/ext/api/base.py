import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import (
    DataApiTestParams,
    StandardizedDataApiTestBase,
)
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_core_testing.database import (
    CoreDbConfig,
    Db,
)
from dl_core_testing.engine_wrapper import TestingEngineWrapper

from dl_connector_bitrix_gds.core.constants import (
    CONNECTION_TYPE_BITRIX24,
    SOURCE_TYPE_BITRIX_GDS,
)
from dl_connector_bitrix_gds_tests.ext.config import (
    API_TEST_CONFIG,
    BITRIX_PORTALS,
    DB_NAME,
    SMART_TABLE_NAME,
    TABLE_NAME,
    UF_DATE_TABLE_NAME,
)


class BitrixConnectionTestBase(ConnectionTestBase):
    conn_type = CONNECTION_TYPE_BITRIX24

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return ""

    @pytest.fixture(scope="class")
    def db(self, db_config: CoreDbConfig) -> Db:
        engine_wrapper = TestingEngineWrapper(config=db_config.engine_config)
        return Db(config=db_config, engine_wrapper=engine_wrapper)

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self, bitrix_datalens_token: str) -> dict:
        return dict(
            portal=BITRIX_PORTALS["datalens"],
            token=bitrix_datalens_token,
        )


class BitrixInvalidConnectionTestBase(BitrixConnectionTestBase):
    @pytest.fixture(scope="class")
    def connection_params(self, bitrix_datalens_token: str) -> dict:
        return dict(
            portal=BITRIX_PORTALS["invalid"],
            token=bitrix_datalens_token,
        )


class BitrixDatasetTestBase(BitrixConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self) -> dict:
        return dict(
            source_type=SOURCE_TYPE_BITRIX_GDS.name,
            title=TABLE_NAME,
            parameters=dict(
                db_name=DB_NAME,
                table_name=TABLE_NAME,
            ),
        )


class BitrixSmartTablesDatasetTestBase(BitrixConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self) -> dict:
        return dict(
            source_type=SOURCE_TYPE_BITRIX_GDS.name,
            title=SMART_TABLE_NAME,
            parameters=dict(
                db_name=DB_NAME,
                table_name=SMART_TABLE_NAME,
            ),
        )


class BitrixUfDateTablesDatasetTestBase(BitrixConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self) -> dict:
        return dict(
            source_type=SOURCE_TYPE_BITRIX_GDS.name,
            title=UF_DATE_TABLE_NAME,
            parameters=dict(
                db_name=DB_NAME,
                table_name=UF_DATE_TABLE_NAME,
            ),
        )


class BitrixDataApiTestBase(BitrixDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_enabled = False

    @pytest.fixture(scope="class")
    def data_api_test_params(self) -> DataApiTestParams:
        return DataApiTestParams(
            two_dims=("ASSIGNED_BY_NAME", "ID"),
            summable_field="ID",
            range_field="ID",
            distinct_field="ASSIGNED_BY_NAME",
            date_field="DATE_CREATE",
        )


class BitrixSmartTablesDataApiTestBase(BitrixSmartTablesDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_enabled = False


class BitrixUfDateTablesDataApiTestBase(BitrixUfDateTablesDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_enabled = False
