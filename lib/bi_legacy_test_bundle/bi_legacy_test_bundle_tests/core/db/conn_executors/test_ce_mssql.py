from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mssql as ms_types

from bi_legacy_test_bundle_tests.core.db.conn_executors.test_base import BaseSchemaSupportedExecutorSet
from dl_constants.enums import UserDataType
from dl_core import exc
from dl_core.connection_executors.async_base import ConnExecutorQuery
from dl_core.connection_models import (
    ConnDTO,
    TableIdent,
)

from bi_connector_mssql.core.connection_executors import MSSQLConnExecutor
from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL
from bi_connector_mssql.core.dto import MSSQLConnDTO


# noinspection PyMethodMayBeStatic


class TestMSSQLExecutor(BaseSchemaSupportedExecutorSet):
    executor_cls = MSSQLConnExecutor

    @pytest.fixture()
    def db(self, mssql_db):
        return mssql_db

    @pytest.fixture()
    def conn_dto(self, db) -> ConnDTO:
        return MSSQLConnDTO(
            conn_id=None,
            multihosts=db.get_conn_hosts(),
            **db.get_conn_credentials(full=True),
        )

    @pytest.fixture()
    def all_supported_types_test_case(self, db):
        columns_data = [
            self.CD("my_tinyint", ms_types.TINYINT(), UserDataType.integer),
            self.CD("my_smallint", ms_types.SMALLINT(), UserDataType.integer),
            self.CD("my_integer", ms_types.INTEGER(), UserDataType.integer),
            self.CD("my_bigint", ms_types.BIGINT(), UserDataType.integer),
            self.CD("my_float", ms_types.FLOAT(), UserDataType.float),
            self.CD("my_real", ms_types.REAL(), UserDataType.float),
            self.CD("my_numeric", ms_types.NUMERIC(), UserDataType.float),
            self.CD("my_decimal", ms_types.DECIMAL(), UserDataType.float),
            self.CD("my_bit", ms_types.BIT(), UserDataType.boolean),
            self.CD("my_char", ms_types.CHAR(), UserDataType.string),
            self.CD("my_varchar", ms_types.VARCHAR(100), UserDataType.string),
            self.CD("my_text", ms_types.TEXT(), UserDataType.string),
            self.CD("my_nchar", ms_types.NCHAR(), UserDataType.string),
            self.CD("my_nvarchar", ms_types.NVARCHAR(100), UserDataType.string),
            self.CD("my_ntext", ms_types.NTEXT(), UserDataType.string),
            self.CD("my_date", ms_types.DATE(), UserDataType.date),
            self.CD("my_datetime", ms_types.DATETIME(), UserDataType.genericdatetime),
            self.CD("my_datetime2", ms_types.DATETIME2(), UserDataType.genericdatetime),
            self.CD("my_smalldatetime", ms_types.SMALLDATETIME(), UserDataType.genericdatetime),
            self.CD("my_datetimeoffset", ms_types.DATETIMEOFFSET(), UserDataType.genericdatetime),
        ]

        table = db.table_from_columns(cd.to_sa_col() for cd in columns_data)
        db.create_table(table)

        yield self.TypeDiscoveryTestCase(
            table_ident=TableIdent(
                db_name=db.name,
                schema_name=table.schema,
                table_name=table.name,
            ),
            expected_schema_info=self.column_data_to_schema_info(columns_data, CONNECTION_TYPE_MSSQL),
        )

        db.drop_table(table)

    @pytest.mark.asyncio
    async def test_execution_errors(self, executor):
        with pytest.raises(exc.DataParseError):
            await executor.execute(
                ConnExecutorQuery(
                    query=sa.select([sa.cast("dewnm", sa.Date)]),
                    user_types=[UserDataType.date],
                    debug_compiled_query="123",
                ),
            )
