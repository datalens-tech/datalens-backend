import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import (
    DataApiTestParams,
    StandardizedDataApiTestBase,
)
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_constants.enums import RawSQLLevel
from dl_core_testing.database import (
    C,
    Db,
    DbTable,
    make_table,
)

from dl_connector_ydb.core.ydb.constants import (
    CONNECTION_TYPE_YDB,
    SOURCE_TYPE_YDB_TABLE,
)
from dl_connector_ydb_tests.db.config import (
    API_TEST_CONFIG,
    CONNECTION_PARAMS,
    DB_CORE_URL,
    TABLE_DATA,
    TABLE_NAME,
    TABLE_SCHEMA,
)


class YDBConnectionTestBase(ConnectionTestBase):
    bi_compeng_pg_on = False
    conn_type = CONNECTION_TYPE_YDB

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return DB_CORE_URL

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return CONNECTION_PARAMS

    @pytest.fixture(scope="class")
    def sample_table(self, db: Db) -> DbTable:
        db_table = make_table(
            db=db,
            name=TABLE_NAME,
            columns=[C(name=name, user_type=user_type, sa_type=sa_type) for name, user_type, sa_type in TABLE_SCHEMA],
            data=[],  # to avoid producing a sample data
            create_in_db=False,
        )
        db.create_table(db_table.table)
        db.insert_into_table(db_table.table, TABLE_DATA)
        return db_table


class YDBDashSQLConnectionTest(YDBConnectionTestBase):
    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return CONNECTION_PARAMS | dict(raw_sql_level=RawSQLLevel.dashsql.value)


class YDBDatasetTestBase(YDBConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table: DbTable) -> dict:
        return dict(
            source_type=SOURCE_TYPE_YDB_TABLE.name,
            parameters=dict(
                table_name=sample_table.name,
            ),
        )


class YDBDataApiTestBase(YDBDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_on = False

    @pytest.fixture(scope="class")
    def data_api_test_params(self) -> DataApiTestParams:
        return DataApiTestParams(
            two_dims=("some_string", "some_int32"),
            summable_field="some_int32",
            range_field="some_int64",
            distinct_field="id",
            date_field="some_date",
        )
