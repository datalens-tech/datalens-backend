from __future__ import annotations

from dl_constants.enums import DataSourceType

from dl_connector_clickhouse.core.clickhouse.constants import (
    SOURCE_TYPE_CH_SUBSELECT,
    SOURCE_TYPE_CH_TABLE,
)
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse.core.clickhouse_base.data_source import (
    ActualClickHouseBaseMixin,
    ClickHouseDataSourceBase,
    CommonClickHouseSubselectDataSource,
)


class ClickHouseDataSource(ClickHouseDataSourceBase):
    conn_type = CONNECTION_TYPE_CLICKHOUSE

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type in {
            SOURCE_TYPE_CH_TABLE,
            SOURCE_TYPE_CH_SUBSELECT,
        }


class ClickHouseSubselectDataSource(ActualClickHouseBaseMixin, CommonClickHouseSubselectDataSource):
    """Clickhouse subselect"""

    conn_type = CONNECTION_TYPE_CLICKHOUSE

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type in {
            SOURCE_TYPE_CH_TABLE,
            SOURCE_TYPE_CH_SUBSELECT,
        }
