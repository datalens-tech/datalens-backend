from __future__ import annotations

from bi_constants.enums import CreateDSFrom

from bi_connector_clickhouse.core.clickhouse.constants import (
    CONNECTION_TYPE_CLICKHOUSE,
    SOURCE_TYPE_CH_SUBSELECT,
    SOURCE_TYPE_CH_TABLE,
)
from bi_connector_clickhouse.core.clickhouse_base.data_source import (
    ActualClickHouseBaseMixin,
    ClickHouseDataSourceBase,
    CommonClickHouseSubselectDataSource,
)


class ClickHouseDataSource(ClickHouseDataSourceBase):
    conn_type = CONNECTION_TYPE_CLICKHOUSE

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_CH_TABLE,
            SOURCE_TYPE_CH_SUBSELECT,
        }


class ClickHouseSubselectDataSource(ActualClickHouseBaseMixin, CommonClickHouseSubselectDataSource):  # type: ignore  # TODO: fix
    """Clickhouse subselect"""

    conn_type = CONNECTION_TYPE_CLICKHOUSE

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_CH_TABLE,
            SOURCE_TYPE_CH_SUBSELECT,
        }
