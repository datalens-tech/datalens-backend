from __future__ import annotations

from typing import Any, Callable, Optional, Type, TYPE_CHECKING

from bi_constants.enums import CreateDSFrom
from bi_core.connection_models import TableDefinition, TableIdent
from bi_core.data_source.sql import (
    BaseSQLDataSource,
    SQLDataSource,
    TableSQLDataSourceMixin,
    require_table_name, SubselectDataSource,
)
from bi_core.db import SchemaInfo
from bi_core.utils import sa_plain_text

from bi_connector_snowflake.core.constants import (
    CONNECTION_TYPE_SNOWFLAKE,
    SOURCE_TYPE_SNOWFLAKE_TABLE, SOURCE_TYPE_SNOWFLAKE_SUBSELECT,
)
from bi_connector_snowflake.core.data_source_spec import SnowFlakeTableDataSourceSpec, SnowFlakeSubselectDataSourceSpec
from bi_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake

if TYPE_CHECKING:
    from bi_core.connection_executors.sync_base import SyncConnExecutorBase


class SnowFlakeDataSourceMixin(BaseSQLDataSource):
    conn_type = CONNECTION_TYPE_SNOWFLAKE

    @classmethod
    def get_connection_cls(cls) -> Type[ConnectionSQLSnowFlake]:
        return ConnectionSQLSnowFlake

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in (SOURCE_TYPE_SNOWFLAKE_TABLE, SOURCE_TYPE_SNOWFLAKE_SUBSELECT)


class SnowFlakeTableDataSource(SnowFlakeDataSourceMixin, TableSQLDataSourceMixin, SQLDataSource):
    @property
    def spec(self) -> SnowFlakeTableDataSourceSpec:
        assert isinstance(self._spec, SnowFlakeTableDataSourceSpec)
        return self._spec

    @property
    def connection(self) -> ConnectionSQLSnowFlake:
        connection = super().connection
        assert isinstance(connection, ConnectionSQLSnowFlake)
        return connection

    @property
    def db_name(self) -> Optional[str]:
        return self.connection.db_name

    @property
    def schema_name(self) -> Optional[str]:
        return self.connection.schema_name

    def get_parameters(self) -> dict:
        return dict(
            super().get_parameters(),
            table_name=self.spec.table_name,
        )

    def get_table_definition(self) -> TableDefinition:
        assert self.table_name is not None
        return TableIdent(
            db_name=self.db_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
        )

    def get_schema_info(
            self, conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> SchemaInfo:
        return super(SnowFlakeTableDataSource, self).get_schema_info(conn_executor_factory=conn_executor_factory)

    @require_table_name
    def get_sql_source(self, alias: str = None) -> Any:
        q = self.quote
        alias_str = "" if alias is None else f" AS {q(alias)}"
        return sa_plain_text(f"{q(self.db_name)}.{q(self.schema_name)}.{q(self.table_name)}{alias_str}")


class SnowFlakeSubselectDataSource(SnowFlakeDataSourceMixin, SubselectDataSource):
    @property
    def spec(self) -> SnowFlakeSubselectDataSourceSpec:
        assert isinstance(self._spec, SnowFlakeSubselectDataSourceSpec)
        return self._spec

    @property
    def db_version(self) -> Optional[str]:
        return None
