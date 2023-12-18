from contextlib import asynccontextmanager
from typing import (
    AsyncIterator,
    List,
)

import asyncpg
import sqlalchemy as sa

from dl_core.connection_executors.models.db_adapter_data import RawSchemaInfo
from dl_core.connection_models import TableDefinition
from dl_core.connection_models.common_models import DBIdent

from dl_connector_postgresql.core.postgresql_base.adapters_postgres import PostgresAdapter
from dl_connector_postgresql.core.postgresql_base.async_adapters_postgres import AsyncPostgresAdapter


GP_LIST_SOURCES_ALL_SCHEMAS_SQL = """
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
ORDER BY schema, name;
"""


GP_LIST_SCHEMA_NAMES = """
SELECT nspname FROM pg_namespace
WHERE nspname NOT LIKE 'pg_%'
AND nspname NOT LIKE 'gp_%'
AND nspname != 'session_state'
AND nspname != 'information_schema'
ORDER BY nspname
"""


GP_LIST_TABLE_NAMES = """
SELECT c.relname 
FROM 
    pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_partitions p ON p.partitiontablename = c.relname
WHERE 
    n.nspname = :schema 
    AND c.relkind in ('r', 'p')
    AND p.tablename is NULL
"""


class GreenplumAdapter(PostgresAdapter):
    _LIST_ALL_TABLES_QUERY = GP_LIST_SOURCES_ALL_SCHEMAS_SQL
    _LIST_TABLE_NAMES_QUERY = GP_LIST_TABLE_NAMES
    _LIST_SCHEMA_NAMES_QUERY = GP_LIST_SCHEMA_NAMES

    def _get_schema_names(self, db_ident: DBIdent) -> List[str]:
        db_engine = self.get_db_engine(db_ident.db_name)
        table_list = [table_name for table_name, in db_engine.execute(sa.text(self._LIST_SCHEMA_NAMES_QUERY))]
        return table_list


class AsyncGreenplumAdapter(AsyncPostgresAdapter):
    _LIST_ALL_TABLES_QUERY = GP_LIST_SOURCES_ALL_SCHEMAS_SQL
    _LIST_SCHEMA_NAMES_QUERY = GP_LIST_SCHEMA_NAMES
    _LIST_TABLE_NAMES_QUERY = GP_LIST_TABLE_NAMES

    async def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        raise NotImplementedError()

    @asynccontextmanager
    async def _query_preparation_context(self, connection: asyncpg.Connection) -> AsyncIterator[None]:
        async with super()._query_preparation_context(connection):
            # enable gp_recursive_cte during query execution if it is disabled

            db_version = await connection.fetchval("SELECT version()")
            if "greenplum" not in db_version.lower():
                # to keep compatibility with postgres
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
