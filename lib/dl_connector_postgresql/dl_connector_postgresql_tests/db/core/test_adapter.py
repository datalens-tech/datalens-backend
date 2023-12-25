import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_core.connection_models.common_models import SchemaIdent
from dl_core_testing.testcases.adapter import BaseAsyncAdapterTestClass
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_postgresql.core.postgresql_base.async_adapters_postgres import AsyncPostgresAdapter
from dl_connector_postgresql.core.postgresql_base.target_dto import PostgresConnTargetDTO
from dl_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass


class TestAsyncPostgreSQLAdapter(
    BasePostgreSQLTestClass,
    BaseAsyncAdapterTestClass[PostgresConnTargetDTO],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            BaseAsyncAdapterTestClass.test_default_pass_db_query_to_user: "Not relevant",
        },
    )

    ASYNC_ADAPTER_CLS = AsyncPostgresAdapter

    async def test_tables_list(self, conn_bi_context: RequestContextInfo, target_conn_dto: PostgresConnTargetDTO):
        tables = await self._make_dba(target_conn_dto, conn_bi_context).get_tables(
            SchemaIdent(db_name="test_data", schema_name=None)
        )

        assert [f"{t.schema_name}.{t.table_name}" for t in tables] == [
            "test_data.sample",
            "test_data_partitions.sample_partition",
        ]

    @pytest.mark.parametrize(
        "schema, expected_tables",
        [
            ("test_data", ["sample"]),
            ("test_data_partitions", ["sample_partition"]),
        ],
    )
    async def test_tables_list_schema(
        self, conn_bi_context: RequestContextInfo, target_conn_dto: PostgresConnTargetDTO, schema, expected_tables
    ):
        tables = await self._make_dba(target_conn_dto, conn_bi_context).get_tables(
            SchemaIdent(db_name="test_data", schema_name=schema)
        )

        assert [f"{t.schema_name}.{t.table_name}" for t in tables] == [f"{schema}.{t}" for t in expected_tables]
