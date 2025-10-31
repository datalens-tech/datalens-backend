from contextlib import asynccontextmanager
from typing import AsyncIterator

import asyncpg
import sqlalchemy as sa

from dl_core.connection_executors.models.db_adapter_data import RawSchemaInfo
from dl_core.connection_models import TableDefinition
from dl_core.connection_models.common_models import DBIdent

from dl_connector_postgresql.core.postgresql_base.adapters_base_postgres import PostgresQueryConstructorMixin
from dl_connector_postgresql.core.postgresql_base.adapters_postgres import PostgresAdapter
from dl_connector_postgresql.core.postgresql_base.async_adapters_postgres import AsyncPostgresAdapter


class GreenplumQueryConstructorMixin(PostgresQueryConstructorMixin):
    def get_list_all_tables_query(
        self, search_text: str | None = None, limit: int | None = None, offset: int | None = None
    ) -> sa.sql.elements.TextClause:
        sql_parts = [
            """
        SELECT
            pg_namespace.nspname as schema,
            pg_class.relname as name
        FROM
            pg_class
            JOIN pg_namespace
            ON pg_namespace.oid = pg_class.relnamespace
            LEFT JOIN pg_partitions
            ON pg_partitions.partitiontablename = pg_class.relname
        WHERE
            pg_namespace.nspname not like 'pg_%'
            AND pg_namespace.nspname not like 'gp_%'
            AND pg_namespace.nspname != 'session_state'
            AND pg_namespace.nspname != 'information_schema'
            AND pg_class.relkind in ('m', 'p', 'r', 'v')
            AND pg_partitions.tablename is NULL
        """
        ]

        if search_text:
            sql_parts.append("AND (pg_namespace.nspname || '.' || pg_class.relname) ILIKE :search_text")

        sql_parts.append("ORDER BY schema, name")
        sql_parts.extend(self._get_pagination_sql_parts(limit, offset))
        sql = " ".join(sql_parts)
        query = sa.text(sql)
        params = self._compile_pagination_params(search_text, limit, offset)
        query = query.bindparams(*params)

        return query

    def get_list_schema_names_query(
        self, search_text: str | None = None, limit: int | None = None, offset: int | None = None
    ) -> sa.sql.elements.TextClause:
        sql_parts = [
            """
        SELECT nspname FROM pg_namespace
        WHERE nspname NOT LIKE 'pg_%'
        AND nspname NOT LIKE 'gp_%'
        AND nspname != 'session_state'
        AND nspname != 'information_schema'
        """
        ]

        if search_text:
            sql_parts.append("AND nspname ILIKE :search_text")

        sql_parts.append("ORDER BY nspname")
        sql_parts.extend(self._get_pagination_sql_parts(limit, offset))
        sql = " ".join(sql_parts)
        query = sa.text(sql)
        params = self._compile_pagination_params(search_text, limit, offset)
        query = query.bindparams(*params)

        return query

    def get_list_table_and_view_names_query(
        self, schema_name: str, search_text: str | None = None, limit: int | None = None, offset: int | None = None
    ) -> sa.sql.elements.TextClause:
        sql_parts = [
            """
        SELECT c.relname
        FROM
            pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_partitions p ON p.partitiontablename = c.relname
        WHERE
            n.nspname = :schema
            AND c.relkind in ('r', 'p', 'v', 'm')
            AND p.tablename is NULL
        """
        ]

        if search_text:
            sql_parts.append("AND c.relname ILIKE :search_text")

        sql_parts.append("ORDER BY c.relname")
        sql_parts.extend(self._get_pagination_sql_parts(limit, offset))
        sql = " ".join(sql_parts)
        query = sa.text(sql)

        params = [
            sa.bindparam("schema", schema_name, type_=sa.String),
            *self._compile_pagination_params(search_text, limit, offset),
        ]
        query = query.bindparams(*params)

        return query


class GreenplumAdapter(GreenplumQueryConstructorMixin, PostgresAdapter):
    def _get_schema_names(self, db_ident: DBIdent) -> list[str]:
        db_engine = self.get_db_engine(db_ident.db_name)
        schema_query = self.get_list_schema_names_query()
        table_list = [table_name for table_name, in db_engine.execute(schema_query)]
        return table_list


class AsyncGreenplumAdapter(GreenplumQueryConstructorMixin, AsyncPostgresAdapter):
    async def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        raise NotImplementedError()

    @asynccontextmanager
    async def _query_preparation_context(self, connection: asyncpg.Connection) -> AsyncIterator[None]:
        async with super()._query_preparation_context(connection):
            # enable gp_recursive_cte during query execution if it is disabled

            db_version = await connection.fetchval("SELECT version()")
            if "greenplum" not in db_version.lower():
                # Doing nothing to keep compatibility with postgres
                # For tests and existing connections to PG via GP connector
                yield
                return

            gp_recursive_cte_initial = await connection.fetchval("SHOW gp_recursive_cte")
            if gp_recursive_cte_initial == "on":
                yield
                return

            await connection.execute("SET gp_recursive_cte = on")
            try:
                yield
            finally:
                await connection.execute("SET gp_recursive_cte = off")
