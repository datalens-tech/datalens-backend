from __future__ import annotations

import logging
from typing import ClassVar

from dl_constants.enums import (
    DataSourceType,
    JoinType,
)
from dl_core.data_source.sql import (
    BaseSQLDataSource,
    StandardSQLDataSource,
    SubselectDataSource,
)

from dl_connector_starrocks.core.constants import (
    CONNECTION_TYPE_STARROCKS,
    SOURCE_TYPE_STARROCKS_SUBSELECT,
    SOURCE_TYPE_STARROCKS_TABLE,
)


LOGGER = logging.getLogger(__name__)


class StarRocksDataSourceMixin(BaseSQLDataSource):
    supported_join_types: ClassVar[frozenset[JoinType]] = frozenset(
        {
            JoinType.inner,
            JoinType.left,
            JoinType.right,
        }
    )

    conn_type = CONNECTION_TYPE_STARROCKS

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type in (SOURCE_TYPE_STARROCKS_TABLE, SOURCE_TYPE_STARROCKS_SUBSELECT)


class StarRocksDataSource(StarRocksDataSourceMixin, StandardSQLDataSource):
    """StarRocks table"""


class StarRocksSubselectDataSource(StarRocksDataSourceMixin, SubselectDataSource):
    """StarRocks table from a subquery"""
