from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Callable,
    Optional,
    Type,
)

from sqlalchemy.sql.elements import ClauseElement

from dl_constants.enums import DataSourceType
from dl_core.connection_models import (
    TableDefinition,
    TableIdent,
)
from dl_core.data_source.sql import (
    BaseSQLDataSource,
    SQLDataSource,
    SubselectDataSource,
    TableSQLDataSourceMixin,
    require_table_name,
)
from dl_core.db import SchemaInfo
from dl_core.utils import sa_plain_text

from dl_connector_snowflake.core.constants import (
    CONNECTION_TYPE_SNOWFLAKE,
    SOURCE_TYPE_SNOWFLAKE_SUBSELECT,
    SOURCE_TYPE_SNOWFLAKE_TABLE,
)
from dl_connector_snowflake.core.data_source_spec import (
    SnowFlakeSubselectDataSourceSpec,
    SnowFlakeTableDataSourceSpec,
)
from dl_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake


if TYPE_CHECKING:
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase


class SnowFlakeDataSourceMixin(BaseSQLDataSource):
    conn_type = CONNECTION_TYPE_SNOWFLAKE

    @classmethod
    def get_connection_cls(cls) -> Type[ConnectionSQLSnowFlake]:
        return ConnectionSQLSnowFlake

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
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
        self,
        conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> SchemaInfo:
        return super(SnowFlakeTableDataSource, self).get_schema_info(conn_executor_factory=conn_executor_factory)

    @require_table_name
    def get_sql_source(self, alias: Optional[str] = None) -> ClauseElement:
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
