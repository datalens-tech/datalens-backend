import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_core.connection_models.common_models import SchemaIdent
from dl_core_testing.testcases.adapter import BaseAsyncAdapterTestClass
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_greenplum.core.adapters import AsyncGreenplumAdapter
from dl_connector_greenplum_tests.db.core.base import (
    GP6TestClass,
    GP7TestClass,
)
from dl_connector_postgresql.core.postgresql_base.target_dto import PostgresConnTargetDTO


class TestGP6AsyncAdapter(
    GP6TestClass,
    BaseAsyncAdapterTestClass[PostgresConnTargetDTO],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            BaseAsyncAdapterTestClass.test_default_pass_db_query_to_user: "Not relevant",
        },
    )

    ASYNC_ADAPTER_CLS = AsyncGreenplumAdapter

    @pytest.mark.asyncio
    async def test_tables_list(self, conn_bi_context: RequestContextInfo, target_conn_dto: PostgresConnTargetDTO):
        tables = await self._make_dba(target_conn_dto, conn_bi_context).get_tables(
            SchemaIdent(db_name="test_data", schema_name=None)
        )

        # Filter out public schema and check that test_data.sample is present
        non_public_tables = [f"{t.schema_name}.{t.table_name}" for t in tables if t.schema_name != "public"]
        assert "test_data.sample" in non_public_tables

    @pytest.mark.asyncio
    async def test_tables_list_schema(
        self, conn_bi_context: RequestContextInfo, target_conn_dto: PostgresConnTargetDTO
    ):
        tables = await self._make_dba(target_conn_dto, conn_bi_context).get_tables(
            SchemaIdent(db_name="test_data", schema_name="test_data")
        )

        assert [f"{t.schema_name}.{t.table_name}" for t in tables] == ["test_data.sample"]

    @pytest.mark.asyncio
    async def test_list_system_schemas(
        self, conn_bi_context: RequestContextInfo, target_conn_dto: PostgresConnTargetDTO
    ):
        """Test that system schemas (pg_*, gp_*) are filtered out from the table list."""
        dba = self._make_dba(target_conn_dto, conn_bi_context)
        tables = await dba.get_tables(SchemaIdent(db_name="test_data", schema_name=None))
        table_schemas = {t.schema_name for t in tables}

        # Test that both pg_ and gp_ system schemas are filtered out
        pg_underscore_schemas = [s for s in table_schemas if s.startswith("pg_")]
        assert len(pg_underscore_schemas) == 0, (
            f"System schemas starting with 'pg_' should be filtered out. " f"Found: {pg_underscore_schemas}"
        )

        gp_underscore_schemas = [s for s in table_schemas if s.startswith("gp_")]
        assert len(gp_underscore_schemas) == 0, (
            f"System schemas starting with 'gp_' should be filtered out. " f"Found: {gp_underscore_schemas}"
        )


class TestGP7AsyncAdapter(
    GP7TestClass,
    BaseAsyncAdapterTestClass[PostgresConnTargetDTO],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            BaseAsyncAdapterTestClass.test_default_pass_db_query_to_user: "Not relevant",
        },
    )

    ASYNC_ADAPTER_CLS = AsyncGreenplumAdapter

    @pytest.mark.asyncio
    async def test_tables_list(self, conn_bi_context: RequestContextInfo, target_conn_dto: PostgresConnTargetDTO):
        tables = await self._make_dba(target_conn_dto, conn_bi_context).get_tables(
            SchemaIdent(db_name="test_data", schema_name=None)
        )

        # Filter out public schema and check that test_data.sample is present
        non_public_tables = [f"{t.schema_name}.{t.table_name}" for t in tables if t.schema_name != "public"]
        assert "test_data.sample" in non_public_tables

    @pytest.mark.asyncio
    async def test_tables_list_schema(
        self, conn_bi_context: RequestContextInfo, target_conn_dto: PostgresConnTargetDTO
    ):
        tables = await self._make_dba(target_conn_dto, conn_bi_context).get_tables(
            SchemaIdent(db_name="test_data", schema_name="test_data")
        )

        assert [f"{t.schema_name}.{t.table_name}" for t in tables] == ["test_data.sample"]

    @pytest.mark.asyncio
    async def test_list_system_schemas(
        self, conn_bi_context: RequestContextInfo, target_conn_dto: PostgresConnTargetDTO
    ):
        """Test that system schemas (pg_*, gp_*) are filtered out from the table list."""
        dba = self._make_dba(target_conn_dto, conn_bi_context)
        tables = await dba.get_tables(SchemaIdent(db_name="test_data", schema_name=None))
        table_schemas = {t.schema_name for t in tables}

        # Test that both pg_ and gp_ system schemas are filtered out
        pg_underscore_schemas = [s for s in table_schemas if s.startswith("pg_")]
        assert len(pg_underscore_schemas) == 0, (
            f"System schemas starting with 'pg_' should be filtered out. " f"Found: {pg_underscore_schemas}"
        )

        gp_underscore_schemas = [s for s in table_schemas if s.startswith("gp_")]
        assert len(gp_underscore_schemas) == 0, (
            f"System schemas starting with 'gp_' should be filtered out. " f"Found: {gp_underscore_schemas}"
        )
