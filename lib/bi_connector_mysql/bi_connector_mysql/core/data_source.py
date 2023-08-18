from __future__ import annotations

import logging
from typing import ClassVar, FrozenSet

from bi_constants.enums import CreateDSFrom, JoinType

from bi_core.data_source.sql import StandardSQLDataSource, SubselectDataSource, BaseSQLDataSource

from bi_connector_mysql.core.constants import (
    CONNECTION_TYPE_MYSQL, SOURCE_TYPE_MYSQL_TABLE, SOURCE_TYPE_MYSQL_SUBSELECT,
)
from bi_connector_mysql.core.query_compiler import MySQLQueryCompiler

LOGGER = logging.getLogger(__name__)


class MySQLDataSourceMixin(BaseSQLDataSource):
    supported_join_types: ClassVar[FrozenSet[JoinType]] = frozenset({
        JoinType.inner,
        JoinType.left,
        JoinType.right,
    })

    conn_type = CONNECTION_TYPE_MYSQL

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in (SOURCE_TYPE_MYSQL_TABLE, SOURCE_TYPE_MYSQL_SUBSELECT)


class MySQLDataSource(MySQLDataSourceMixin, StandardSQLDataSource):
    """ MySQL table """
    compiler_cls = MySQLQueryCompiler


class MySQLSubselectDataSource(MySQLDataSourceMixin, SubselectDataSource):
    """ MySQL table from a subquery """
