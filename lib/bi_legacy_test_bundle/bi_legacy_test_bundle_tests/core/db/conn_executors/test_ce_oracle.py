from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.oracle import base as or_types  # not all data types are imported in init in older SA versions

from bi_legacy_test_bundle_tests.core.common_ce import (
    BaseConnExecutorSet,
    ErrorTestSet,
    SelectDataTestSet,
)
from dl_constants.enums import BIType
from dl_core import exc
from dl_core.connection_executors import ConnExecutorQuery
from dl_core.connection_models import (
    ConnDTO,
    TableIdent,
)
from dl_core_testing.database import (
    C,
    make_table,
)

from bi_connector_oracle.core.connection_executors import OracleDefaultConnExecutor
from bi_connector_oracle.core.constants import (
    CONNECTION_TYPE_ORACLE,
    OracleDbNameType,
)
from bi_connector_oracle.core.dto import OracleConnDTO

# noinspection PyMethodMayBeStatic


class TestOracleExecutor(BaseConnExecutorSet):
    executor_cls = OracleDefaultConnExecutor

    @pytest.fixture()
    def db(self, oracle_db):
        return oracle_db

    @pytest.fixture()
    def conn_dto(self, db) -> ConnDTO:
        return OracleConnDTO(
            conn_id=None,
            multihosts=db.get_conn_hosts(),
            **db.get_conn_credentials(full=True),
            db_name_type=OracleDbNameType.service_name,
        )

    @pytest.fixture()
    def select_data_test_set(self, db) -> SelectDataTestSet:
        tbl = make_table(
            db,
            rows=10,
            columns=[
                C("str_val", BIType.string, sa_type=or_types.NVARCHAR2(255), vg=lambda rn, **kwargs: str(rn)),
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
            self.CD("my_integer", or_types.NUMBER(), BIType.integer, nt_name_str="integer"),
            self.CD("my_number", or_types.NUMBER(10, 5), BIType.float),
            self.CD("my_float", or_types.BINARY_FLOAT(), BIType.float),
            self.CD("my_double", or_types.BINARY_DOUBLE(), BIType.float),
            self.CD("my_char", or_types.CHAR(), BIType.string),
            self.CD("my_nchar", sa.NCHAR(), BIType.string),
            self.CD("my_varchar", or_types.VARCHAR2(100), BIType.string, nt_name_str="varchar"),
            self.CD("my_nvarchar", or_types.NVARCHAR2(100), BIType.string),
            self.CD("my_date", or_types.DATE(), BIType.genericdatetime),
            self.CD("my_timestamp", or_types.TIMESTAMP(), BIType.genericdatetime),
        ]

        table = db.table_from_columns(cd.to_sa_col() for cd in columns_data)
        db.create_table(table)

        yield self.TypeDiscoveryTestCase(
            table_ident=TableIdent(
                db_name=db.name,
                schema_name=table.schema,
                table_name=table.name,
            ),
            expected_schema_info=self.column_data_to_schema_info(columns_data, CONNECTION_TYPE_ORACLE),
        )

        db.drop_table(table)

    @pytest.fixture()
    def error_test_set(self, request) -> ErrorTestSet:
        return ErrorTestSet(
            query=ConnExecutorQuery("SELECT * FROM table_1234123_not_existing"),
            expected_err_cls=exc.DatabaseQueryError,
        )
