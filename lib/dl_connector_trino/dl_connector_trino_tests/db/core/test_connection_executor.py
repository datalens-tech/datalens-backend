from typing import Optional

import pytest
import sqlalchemy as sa

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
from dl_connector_trino_tests.db.core.base import BaseTrinoTestClass
import dl_connector_trino_tests.db.config as test_config


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
