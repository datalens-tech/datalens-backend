from collections.abc import Sequence
import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import CompileError
from sqlalchemy.sql import compiler
import trino.sqlalchemy.datatype as tsa

from dl_constants.enums import UserDataType
from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.connection_executors.sync_base import (
    SyncConnExecutorBase,
    SyncExecutionResult,
)
from dl_core.connection_models.common_models import (
    DBIdent,
    TableIdent,
)
import dl_core.exc as core_exc
from dl_core_testing.testcases.connection_executor import (
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)

from dl_connector_trino.core.adapters import (
    CustomTrinoDialect,
    TrinoDefaultAdapter,
)
from dl_connector_trino.core.error_transformer import ExpressionNotAggregateError
from dl_connector_trino.core.us_connection import ConnectionTrino
from dl_connector_trino_tests.db.core.base import BaseTrinoTestClass


class TrinoSyncConnectionExecutorBase(
    BaseTrinoTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionTrino],
):
    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return None

    def check_db_version(self, db_version: str | None) -> None:
        assert db_version is not None
        assert db_version.startswith("0.") or int(db_version) >= 300


class TestTrinoSyncConnectionExecutor(
    TrinoSyncConnectionExecutorBase,
    DefaultSyncConnectionExecutorTestSuite[ConnectionTrino],
):
    @pytest.mark.parametrize(
        "layer, expected_error_name",
        [
            ("table_name", "TABLE_NOT_FOUND"),
            ("schema_name", "SCHEMA_NOT_FOUND"),
            ("db_name", "CATALOG_NOT_FOUND"),
        ],
    )
    def test_error_on_select_from_nonexistent_source(
        self,
        layer: str,
        expected_error_name: str,
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
        assert exc_instance.details["error_name"] == expected_error_name

    def test_error_on_select_from_nonexistent_column(
        self,
        sync_connection_executor: SyncConnExecutorBase,
        existing_table_ident: TableIdent,
    ) -> None:
        table_ident = existing_table_ident

        table = sa.Table(
            table_ident.table_name,
            sa.MetaData(),
            sa.Column("nonexistent_column", sa.String),
            schema=table_ident.schema_name,
        )
        conn_executor_query = ConnExecutorQuery(table.select(), db_name=table_ident.db_name)
        with pytest.raises(core_exc.ColumnDoesNotExist) as exc_info:
            sync_connection_executor.execute(conn_executor_query)

        exc_instance = exc_info.value
        assert exc_instance.details["error_name"] == "COLUMN_NOT_FOUND"

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        return {
            "types_trino_number": [
                self.CD(sa.BOOLEAN(), UserDataType.boolean),
                self.CD(sa.SMALLINT(), UserDataType.integer),
                self.CD(sa.INTEGER(), UserDataType.integer),
                self.CD(sa.BIGINT(), UserDataType.integer),
                self.CD(sa.REAL(), UserDataType.float),
                self.CD(tsa.DOUBLE(), UserDataType.float),
                self.CD(sa.DECIMAL(), UserDataType.float),
            ],
            "types_trino_string": [
                self.CD(sa.VARCHAR(), UserDataType.string),
                self.CD(sa.CHAR(), UserDataType.string),
                self.CD(sa.VARBINARY(), UserDataType.string),
                self.CD(tsa.JSON(), UserDataType.string),
            ],
            "types_trino_date": [
                self.CD(sa.DATE(), UserDataType.date),
                self.CD(tsa.TIMESTAMP(), UserDataType.genericdatetime),
            ],
            "types_trino_array": [
                self.CD(sa.ARRAY(sa.BIGINT()), UserDataType.array_int),
                self.CD(sa.ARRAY(tsa.DOUBLE()), UserDataType.array_float),
                self.CD(sa.ARRAY(sa.VARCHAR()), UserDataType.array_str),
            ],
        }

    def make_select_with_duplicate_parametrized_expression(
        self,
        sync_connection_executor: SyncConnExecutorBase,
        existing_table_ident: TableIdent,
        add_complex_types: bool = False,
    ) -> SyncExecutionResult:
        table = sa.Table(
            existing_table_ident.table_name,
            sa.MetaData(),
            sa.Column("sales", sa.Integer),
            sa.Column("order_date", sa.Date),
            schema=existing_table_ident.schema_name,
        )

        expr1 = (table.c.sales + 1).label("res_1")
        expr2 = (table.c.sales + 1).label("res_2")

        sa_query = (
            sa.select(
                expr1,
                expr2,
            )
            .select_from(table)
            .group_by(sa.text("1"))
        )
        if add_complex_types:
            sa_query = sa_query.where(
                table.c.order_date >= datetime.date(1900, 1, 1),
            )

        conn_executor_query = ConnExecutorQuery(sa_query, db_name=existing_table_ident.db_name)
        result = sync_connection_executor.execute(conn_executor_query)
        return result

    def test_duplicate_parametrized_expression_in_select_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sync_connection_executor: SyncConnExecutorBase,
        existing_table_ident: TableIdent,
    ) -> None:
        # Reproduce adapter behavior before the query compilation fix
        monkeypatch.setattr(
            TrinoDefaultAdapter,
            "execute_by_steps",
            BaseClassicAdapter.execute_by_steps,
        )
        with pytest.raises(ExpressionNotAggregateError):
            self.make_select_with_duplicate_parametrized_expression(
                sync_connection_executor=sync_connection_executor,
                existing_table_ident=existing_table_ident,
            )

    def test_duplicate_parametrized_expression_in_select(
        self,
        sync_connection_executor: SyncConnExecutorBase,
        existing_table_ident: TableIdent,
    ) -> None:
        self.make_select_with_duplicate_parametrized_expression(
            sync_connection_executor=sync_connection_executor,
            existing_table_ident=existing_table_ident,
        )

    def test_duplicate_parametrized_expression_in_select_with_complex_types_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sync_connection_executor: SyncConnExecutorBase,
        existing_table_ident: TableIdent,
    ) -> None:
        # Reproduce SQLCompiler behavior before defining the custom rendering for complex types
        monkeypatch.setattr(
            CustomTrinoDialect,
            "statement_compiler",
            compiler.SQLCompiler,
        )
        with pytest.raises(CompileError):
            self.make_select_with_duplicate_parametrized_expression(
                sync_connection_executor=sync_connection_executor,
                existing_table_ident=existing_table_ident,
                add_complex_types=True,
            )

    def test_duplicate_parametrized_expression_in_select_with_complex_types(
        self,
        sync_connection_executor: SyncConnExecutorBase,
        existing_table_ident: TableIdent,
    ) -> None:
        self.make_select_with_duplicate_parametrized_expression(
            sync_connection_executor=sync_connection_executor,
            existing_table_ident=existing_table_ident,
            add_complex_types=True,
        )
