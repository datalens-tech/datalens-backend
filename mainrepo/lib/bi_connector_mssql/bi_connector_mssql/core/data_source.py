from __future__ import annotations

import logging

from bi_constants.enums import CreateDSFrom

from bi_core.data_source.sql import StandardSchemaSQLDataSource, SubselectDataSource, BaseSQLDataSource

from bi_connector_mssql.core.constants import (
    CONNECTION_TYPE_MSSQL, SOURCE_TYPE_MSSQL_TABLE, SOURCE_TYPE_MSSQL_SUBSELECT
)
from bi_connector_mssql.core.query_compiler import MSSQLQueryCompiler

LOGGER = logging.getLogger(__name__)


class MSSQLDataSourceMixin(BaseSQLDataSource):
    compiler_cls = MSSQLQueryCompiler

    conn_type = CONNECTION_TYPE_MSSQL

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in (SOURCE_TYPE_MSSQL_TABLE, SOURCE_TYPE_MSSQL_SUBSELECT)


class MSSQLDataSource(MSSQLDataSourceMixin, StandardSchemaSQLDataSource):  # type: ignore  # TODO: fix
    """ MSSQL table """


class MSSQLSubselectDataSource(MSSQLDataSourceMixin, SubselectDataSource):  # type: ignore  # TODO: fix
    """ MSSQL table """
