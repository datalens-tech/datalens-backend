from __future__ import annotations

import logging
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
    StandardSQLDataSource,
    SubselectDataSource,
)

from dl_connector_mysql.core.constants import (
    CONNECTION_TYPE_MYSQL,
    SOURCE_TYPE_MYSQL_SUBSELECT,
    SOURCE_TYPE_MYSQL_TABLE,
)
from dl_connector_mysql.core.query_compiler import MySQLQueryCompiler


LOGGER = logging.getLogger(__name__)


class MySQLDataSourceMixin(BaseSQLDataSource):
    supported_join_types: ClassVar[FrozenSet[JoinType]] = frozenset(
        {
            JoinType.inner,
            JoinType.left,
            JoinType.right,
        }
    )

    conn_type = CONNECTION_TYPE_MYSQL
    compiler_cls = MySQLQueryCompiler

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type in (SOURCE_TYPE_MYSQL_TABLE, SOURCE_TYPE_MYSQL_SUBSELECT)


class MySQLDataSource(MySQLDataSourceMixin, StandardSQLDataSource):
    """MySQL table"""


class MySQLSubselectDataSource(MySQLDataSourceMixin, SubselectDataSource):
    """MySQL table from a subquery"""
