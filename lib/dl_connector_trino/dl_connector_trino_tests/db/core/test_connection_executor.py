from typing import (
    Optional,
    Sequence,
)

import pytest
import sqlalchemy as sa
import trino.sqlalchemy.datatype as trino_types

from dl_constants.enums import UserDataType
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.connection_models.common_models import (
    DBIdent,
    TableIdent,
)
import dl_core.exc as core_exc
from dl_core_testing.testcases.connection_executor import (
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)

from dl_connector_trino.core.us_connection import ConnectionTrino
import dl_connector_trino_tests.db.config as test_config
from dl_connector_trino_tests.db.core.base import BaseTrinoTestClass


class TrinoSyncConnectionExecutorBase(
    BaseTrinoTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionTrino],
):
    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return None

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_URL_MEMORY_CATALOG

    def check_db_version(self, db_version: Optional[str]) -> None:
        assert db_version is not None
        assert db_version.startswith("0.") or int(db_version) >= 300


class TestTrinoSyncConnectionExecutor(
    TrinoSyncConnectionExecutorBase,
    DefaultSyncConnectionExecutorTestSuite[ConnectionTrino],
):
    @pytest.mark.parametrize(
        "layer, exception_params_key",
        [
            ("table_name", "table_definition"),
            ("schema_name", "schema_definition"),
            ("db_name", "catalog_definition"),
        ],
    )
    def test_error_on_select_from_nonexistent_source(
        self,
        layer: str,
        exception_params_key: str,
        sync_connection_executor: SyncConnExecutorBase,
        existing_table_ident: TableIdent,
    ) -> None:
        nonexistent_table_features = dict(
            db_name=existing_table_ident.db_name,
            schema_name=existing_table_ident.schema_name,
            table_name=existing_table_ident.table_name,
        )
        nonexistent_table_features[layer] = "nonexistent_" + nonexistent_table_features[layer]
        nonexistent_table_ident = TableIdent(**nonexistent_table_features)

        nonexistent_table = sa.Table(
            nonexistent_table_ident.table_name,
            sa.MetaData(),
            sa.Column("some_column", sa.String),
            schema=nonexistent_table_ident.schema_name,
        )
        sa_query = nonexistent_table.select()
        conn_executor_query = ConnExecutorQuery(sa_query, db_name=nonexistent_table_ident.db_name)
        with pytest.raises(core_exc.SourceDoesNotExist) as exc_info:
            sync_connection_executor.execute(conn_executor_query)

        exc_instance = exc_info.value
        assert exception_params_key in exc_instance.params

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        return {
            "types_trino_number": [
                self.CD(sa.BOOLEAN(), UserDataType.boolean),
                self.CD(sa.SMALLINT(), UserDataType.integer),
                self.CD(sa.INTEGER(), UserDataType.integer),
                self.CD(sa.BIGINT(), UserDataType.integer),
                self.CD(sa.REAL(), UserDataType.float),
                self.CD(trino_types.DOUBLE(), UserDataType.float),
                self.CD(sa.DECIMAL(), UserDataType.float),
            ],
            "types_trino_string": [
                self.CD(sa.VARCHAR(), UserDataType.string),
                self.CD(sa.CHAR(), UserDataType.string),
                self.CD(sa.VARBINARY(), UserDataType.string),
                self.CD(trino_types.JSON(), UserDataType.string),
            ],
            "types_trino_date": [
                self.CD(sa.DATE(), UserDataType.date),
                self.CD(trino_types.TIME(), UserDataType.datetimetz),
                self.CD(trino_types.TIMESTAMP(), UserDataType.genericdatetime),
            ],
            "types_trino_array": [
                self.CD(sa.ARRAY(sa.BIGINT()), UserDataType.array_int),
                self.CD(sa.ARRAY(trino_types.DOUBLE()), UserDataType.array_float),
                self.CD(sa.ARRAY(sa.VARCHAR()), UserDataType.array_str),
            ],
        }
