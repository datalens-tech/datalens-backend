from __future__ import annotations

from dl_connector_greenplum.core.constants import (
    CONNECTION_TYPE_GREENPLUM,
    SOURCE_TYPE_GP_SUBSELECT,
    SOURCE_TYPE_GP_TABLE,
)
from dl_connector_postgresql.core.postgresql_base.query_compiler import PostgreSQLQueryCompiler
from dl_constants.enums import DataSourceType
from dl_core.data_source.sql import (
    BaseSQLDataSource,
    StandardSchemaSQLDataSource,
    SubselectDataSource,
)


class GreenplumDataSourceMixin(BaseSQLDataSource):
    compiler_cls = PostgreSQLQueryCompiler

    conn_type = CONNECTION_TYPE_GREENPLUM

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type in (SOURCE_TYPE_GP_TABLE, SOURCE_TYPE_GP_SUBSELECT)


class GreenplumTableDataSource(GreenplumDataSourceMixin, StandardSchemaSQLDataSource):
    """Greenplum table"""


class GreenplumSubselectDataSource(GreenplumDataSourceMixin, SubselectDataSource):
    """Greenplum subselect"""
