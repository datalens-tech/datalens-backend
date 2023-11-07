from __future__ import annotations

import logging

from dl_constants.enums import DataSourceType
from dl_core.data_source.sql import (
    BaseSQLDataSource,
    StandardSchemaSQLDataSource,
    SubselectDataSource,
)

from dl_connector_mssql.core.constants import (
    CONNECTION_TYPE_MSSQL,
    SOURCE_TYPE_MSSQL_SUBSELECT,
    SOURCE_TYPE_MSSQL_TABLE,
)
from dl_connector_mssql.core.query_compiler import MSSQLQueryCompiler


LOGGER = logging.getLogger(__name__)


class MSSQLDataSourceMixin(BaseSQLDataSource):
    compiler_cls = MSSQLQueryCompiler

    conn_type = CONNECTION_TYPE_MSSQL

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type in (SOURCE_TYPE_MSSQL_TABLE, SOURCE_TYPE_MSSQL_SUBSELECT)


class MSSQLDataSource(MSSQLDataSourceMixin, StandardSchemaSQLDataSource):  # type: ignore  # TODO: fix
    """MSSQL table"""


class MSSQLSubselectDataSource(MSSQLDataSourceMixin, SubselectDataSource):  # type: ignore  # TODO: fix
    """MSSQL table"""
