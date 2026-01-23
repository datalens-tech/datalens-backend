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

    @pytest.mark.asyncio
    async def test_tables_list(self, conn_bi_context: RequestContextInfo, target_conn_dto: PostgresConnTargetDTO):
        tables = await self._make_dba(target_conn_dto, conn_bi_context).get_tables(
            SchemaIdent(db_name="test_data", schema_name=None)
        )

        assert [
            f"{t.schema_name}.{t.table_name}"
            for t in tables
            if t.schema_name != "public"  # skip public schema because it contains garbage tables from other tests
            # TODO: clean up tables created in other tests
        ] == [
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
    @pytest.mark.asyncio
    async def test_tables_list_schema(
        self, conn_bi_context: RequestContextInfo, target_conn_dto: PostgresConnTargetDTO, schema, expected_tables
    ):
        tables = await self._make_dba(target_conn_dto, conn_bi_context).get_tables(
            SchemaIdent(db_name="test_data", schema_name=schema)
        )

        assert [f"{t.schema_name}.{t.table_name}" for t in tables] == [f"{schema}.{t}" for t in expected_tables]

    @pytest.mark.asyncio
    async def test_list_system_schemas(
        self, conn_bi_context: RequestContextInfo, target_conn_dto: PostgresConnTargetDTO
    ):
        """Test that tables from schemas starting with 'pg' but not 'pg_' are included in the table list."""
        dba = self._make_dba(target_conn_dto, conn_bi_context)
        tables = await dba.get_tables(SchemaIdent(db_name="test_data", schema_name=None))
        table_schemas = {t.schema_name for t in tables}

        assert "pgmyschema" in table_schemas, (
            f"Schema 'pgmyschema' should be included in table list. " f"Found schemas: {sorted(table_schemas)}"
        )

        pgmyschema_tables = [f"{t.schema_name}.{t.table_name}" for t in tables if t.schema_name == "pgmyschema"]
        assert "pgmyschema.test_table" in pgmyschema_tables, (
            f"Table 'pgmyschema.test_table' should be in the results. " f"Found: {pgmyschema_tables}"
        )

        pg_underscore_schemas = [s for s in table_schemas if s.startswith("pg_")]
        assert len(pg_underscore_schemas) == 0, (
            f"System schemas starting with 'pg_' should be filtered out. " f"Found: {pg_underscore_schemas}"
        )
