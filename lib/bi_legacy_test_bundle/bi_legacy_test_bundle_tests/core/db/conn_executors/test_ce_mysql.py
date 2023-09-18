from __future__ import annotations

import pytest
from sqlalchemy.dialects import mysql as my_types

from bi_legacy_test_bundle_tests.core.common_ce import (
    BaseConnExecutorSet,
    SelectDataTestSet,
)
from dl_constants.enums import BIType
from dl_core.connection_executors.common_base import (
    ConnExecutorQuery,
    ExecutionMode,
)
from dl_core.connection_models import (
    ConnDTO,
    TableIdent,
)
from dl_core_testing.database import (
    C,
    Db,
    make_table,
)

from bi_connector_mysql.core.connection_executors import MySQLConnExecutor
from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from bi_connector_mysql.core.dto import MySQLConnDTO


class TestMySQLSyncAdapterExecutor(BaseConnExecutorSet):
    executor_cls = MySQLConnExecutor

    @pytest.fixture()
    def db(self, mysql_db):
        return mysql_db

    @pytest.fixture()
    def conn_dto(self, db: Db) -> ConnDTO:
        return MySQLConnDTO(conn_id=None, multihosts=db.get_conn_hosts(), **db.get_conn_credentials(full=True))

    @pytest.fixture()
    def select_data_test_set(self, db) -> SelectDataTestSet:
        tbl = make_table(
            db,
            rows=10,
            columns=[
                C("str_val", BIType.string, sa_type=my_types.VARCHAR(256), vg=lambda rn, **kwargs: str(rn)),
            ],
        )
        yield SelectDataTestSet(
            # table=tbl,
            query=ConnExecutorQuery(
                query=f"SELECT * FROM {db.quote(tbl.name)} ORDER BY str_val",
                chunk_size=6,
            ),
            expected_data=[(str(i),) for i in range(10)],
        )
        db.drop_table(tbl.table)

    @pytest.fixture()
    def all_supported_types_test_case(self, db):
        columns_data = [
            self.CD("my_tinyint", my_types.TINYINT(), BIType.integer),
            self.CD("my_smallint", my_types.SMALLINT(), BIType.integer),
            self.CD("my_mediumint", my_types.MEDIUMINT(), BIType.integer),
            self.CD("my_integer", my_types.INTEGER(), BIType.integer),
            self.CD("my_bigint", my_types.BIGINT(), BIType.integer),
            self.CD("my_float", my_types.FLOAT(), BIType.float),
            self.CD("my_double", my_types.DOUBLE(), BIType.float),
            self.CD("my_numeric", my_types.NUMERIC(), BIType.float, nt_name_str="decimal"),
            self.CD("my_decimal", my_types.DECIMAL(), BIType.float),
            self.CD("my_bit", my_types.BIT(1), BIType.boolean),
            self.CD("my_tinyblob", my_types.TINYBLOB(), BIType.string),
            self.CD("my_blob", my_types.BLOB(), BIType.string),
            self.CD("my_binary", my_types.BINARY(), BIType.string),
            self.CD("my_varbinary", my_types.VARBINARY(100), BIType.string),
            self.CD("my_char", my_types.CHAR(), BIType.string),
            self.CD("my_varchar", my_types.VARCHAR(100), BIType.string),
            self.CD("my_tinytext", my_types.TINYTEXT(), BIType.string),
            self.CD("my_text", my_types.TEXT(), BIType.string),
            self.CD("my_date", my_types.DATE(), BIType.date),
            self.CD("my_timestamp", my_types.TIMESTAMP(), BIType.genericdatetime),
            self.CD("my_datetime", my_types.DATETIME(), BIType.genericdatetime),
            self.CD("my_enum", my_types.ENUM("a", "b", "c", name="some_enum"), BIType.string),
        ]

        table = db.table_from_columns(cd.to_sa_col() for cd in columns_data)
        db.create_table(table)

        yield self.TypeDiscoveryTestCase(
            table_ident=TableIdent(
                db_name=db.name,
                schema_name=table.schema,
                table_name=table.name,
            ),
            expected_schema_info=self.column_data_to_schema_info(columns_data, CONNECTION_TYPE_MYSQL),
        )
        db.drop_table(table)

    @pytest.mark.asyncio
    async def test_get_db_version(self, executor):
        if executor._exec_mode == ExecutionMode.RQE:
            pytest.skip("Hangs forever in RQE on fetch_many()")
        return await super().test_get_db_version(executor)

    def test_get_db_version_sync(self, sync_exec_wrapper):
        if sync_exec_wrapper._async_conn_executor._exec_mode == ExecutionMode.RQE:
            pytest.skip("Hangs forever in RQE on fetch_many()")

        super().test_get_db_version_sync(sync_exec_wrapper)
