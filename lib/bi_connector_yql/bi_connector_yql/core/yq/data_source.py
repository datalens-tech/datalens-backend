from __future__ import annotations

from dl_constants.enums import DataSourceType
from dl_core.data_source.sql import (
    StandardSQLDataSource,
    SubselectDataSource,
)

from bi_connector_yql.core.yq.constants import (
    CONNECTION_TYPE_YQ,
    SOURCE_TYPE_YQ_SUBSELECT,
    SOURCE_TYPE_YQ_TABLE,
)
from bi_connector_yql.core.yql_base.data_source import YQLDataSourceMixin


class YQDataSourceMixin(YQLDataSourceMixin):
    conn_type = CONNECTION_TYPE_YQ

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type in (SOURCE_TYPE_YQ_TABLE, SOURCE_TYPE_YQ_SUBSELECT)


class YQTableDataSource(YQDataSourceMixin, StandardSQLDataSource):
    """YQ table"""


class YQSubselectDataSource(YQDataSourceMixin, SubselectDataSource):
    """YQ subselect"""
