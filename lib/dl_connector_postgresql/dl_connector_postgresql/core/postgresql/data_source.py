from __future__ import annotations

from typing import (
    ClassVar,
    FrozenSet,
)

from dl_constants.enums import (
    DataSourceType,
    JoinType,
)
from dl_core.data_source.sql import (
    BaseSQLDataSource,
    StandardSchemaSQLDataSource,
    SubselectDataSource,
)

from dl_connector_postgresql.core.postgresql.constants import (
    CONNECTION_TYPE_POSTGRES,
    SOURCE_TYPE_PG_SUBSELECT,
    SOURCE_TYPE_PG_TABLE,
)


class PostgreSQLDataSourceMixin(BaseSQLDataSource):
    supported_join_types: ClassVar[FrozenSet[JoinType]] = frozenset(
        {
            JoinType.inner,
            JoinType.left,
            JoinType.full,
            JoinType.right,
        }
    )

    conn_type = CONNECTION_TYPE_POSTGRES

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type in (SOURCE_TYPE_PG_TABLE, SOURCE_TYPE_PG_SUBSELECT)


class PostgreSQLDataSource(PostgreSQLDataSourceMixin, StandardSchemaSQLDataSource):
    """PG table"""


class PostgreSQLSubselectDataSource(PostgreSQLDataSourceMixin, SubselectDataSource):
    """PG subselect"""
