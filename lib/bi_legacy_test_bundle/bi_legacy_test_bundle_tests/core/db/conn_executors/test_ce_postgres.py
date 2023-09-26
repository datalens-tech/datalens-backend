from __future__ import annotations

import contextlib
from typing import Generator
import uuid

import pytest
import shortuuid
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg_types

from bi_legacy_test_bundle_tests.core.db.conn_executors.test_base import (
    BaseConnExecutorSet,
    BaseSchemaSupportedExecutorSet,
)
from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from dl_connector_postgresql.core.postgresql.dto import PostgresConnDTO
from dl_connector_postgresql.core.postgresql_base.connection_executors import PostgresConnExecutor
from dl_constants.enums import UserDataType
from dl_core.connection_models import (
    ConnDTO,
    SATextTableDefinition,
    TableIdent,
)
from dl_core.db import (
    IndexInfo,
    SchemaInfo,
)


# noinspection PyMethodMayBeStatic
class TestPostgresExecutor(BaseSchemaSupportedExecutorSet):
    executor_cls = PostgresConnExecutor

    @pytest.fixture()
    def db(self, postgres_db):
        return postgres_db

    @pytest.fixture()
    def conn_dto(self, db) -> ConnDTO:
        return PostgresConnDTO(
            conn_id=None,
            host=db.url.host,
            port=db.url.port,
            db_name=db.url.database,
            username=db.url.username,
            password=db.url.password,
            multihosts=db.get_conn_hosts(),
        )

    @pytest.fixture()
    def all_supported_types_test_case(self, postgres_db):
        db = postgres_db
        enum_type = pg_types.ENUM("a", "b", "c", name=f"some_enum_{uuid.uuid4().hex}", create_type=False)
        enum_type.create(bind=db.get_current_connection())

        columns_data = [
            self.CD("my_smallint", pg_types.SMALLINT(), UserDataType.integer),
            self.CD("my_integer", pg_types.INTEGER(), UserDataType.integer),
            self.CD("my_bigint", pg_types.BIGINT(), UserDataType.integer),
            self.CD("my_real", pg_types.REAL(), UserDataType.float),
            self.CD("my_double", pg_types.DOUBLE_PRECISION(), UserDataType.float),
            self.CD("my_numeric", pg_types.NUMERIC(), UserDataType.float),
            self.CD("my_boolean", pg_types.BOOLEAN(), UserDataType.boolean),
            self.CD("my_char", pg_types.CHAR(), UserDataType.string),
            self.CD("my_varchar", pg_types.VARCHAR(100), UserDataType.string),
            self.CD("my_text", pg_types.TEXT(), UserDataType.string),
            self.CD("my_date", pg_types.DATE(), UserDataType.date),
            self.CD("my_timestamp_notz", pg_types.TIMESTAMP(timezone=False), UserDataType.genericdatetime),
            self.CD("my_timestamp_wtz", pg_types.TIMESTAMP(timezone=True), UserDataType.genericdatetime),
            self.CD("my_enum", enum_type, UserDataType.string),
        ]

        table = db.table_from_columns(cd.to_sa_col() for cd in columns_data)
        db.create_table(table)

        yield self.TypeDiscoveryTestCase(
            table_ident=TableIdent(
                db_name=db.name,
                schema_name=table.schema,
                table_name=table.name,
            ),
            expected_schema_info=self.column_data_to_schema_info(columns_data, CONNECTION_TYPE_POSTGRES),
        )

        db.drop_table(table)
        enum_type.drop(bind=db.get_current_connection())

    @contextlib.contextmanager
    def _get_table_names_test_case(self, db) -> Generator[BaseConnExecutorSet.ListTableTestCase, None, None]:
        """Override fixture to take in account all-schemas listing"""
        with super()._get_table_names_test_case(db=db) as test_case:
            # To match the live behavior, requirest tables list for all schemas.
            test_case = test_case.clone(
                target_schema_ident=test_case.target_schema_ident.clone(schema_name=None),
                full_match_required=False,
            )
            yield test_case

    @pytest.fixture(params=["no_idx", "one_columns_sorting", "two_columns_sorting"])
    def index_test_case(self, postgres_db, request):
        table_name = f"idx_test_{shortuuid.uuid()}"

        map_case_name_ddl_idx = dict(
            no_idx=(
                [
                    """CREATE TABLE "{table_name}" (num_1 integer, num_2 integer, txt text)""",
                ],
                [],
            ),
            one_columns_sorting=(
                [
                    """CREATE TABLE "{table_name}" (num_1 integer, num_2 integer, txt text)""",
                    """CREATE INDEX ON "{table_name}" (num_1)""",
                ],
                [
                    IndexInfo(
                        columns=("num_1",),
                        kind=None,
                    )
                ],
            ),
            two_columns_sorting=(
                [
                    """CREATE TABLE "{table_name}" (num_1 integer, num_2 integer, txt text)""",
                    """CREATE INDEX ON "{table_name}" (num_1, num_2)""",
                ],
                [
                    IndexInfo(
                        columns=("num_1", "num_2"),
                        kind=None,
                    )
                ],
            ),
        )

        ddl_list, expected_indexes = map_case_name_ddl_idx[request.param]

        try:
            for ddl in ddl_list:
                postgres_db.execute(ddl.format(table_name=table_name))

            yield self.TypeDiscoveryTestCase(
                table_ident=TableIdent(
                    db_name=None,
                    schema_name=None,
                    table_name=table_name,
                ),
                expected_schema_info=SchemaInfo(schema=[], indexes=frozenset(expected_indexes)),
            )
        finally:
            postgres_db.execute(f"""DROP TABLE IF EXISTS "{table_name}" """)

    subselect_test_query = "select 1 as num"

    def test_get_subselect_table_schema_info(self, sync_exec_wrapper):
        query = self.subselect_test_query
        query = "({}) as source".format(query)
        subselect = sa.sql.elements.TextClause(query)
        schema_info = sync_exec_wrapper.get_table_schema_info(SATextTableDefinition(text=subselect))
        assert schema_info
        assert schema_info.schema
        # Additionally, re-check the structure:
        assert schema_info.schema[0].native_type.nullable is not None
