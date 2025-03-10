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

from dl_connector_trino.core.constants import (
    CONNECTION_TYPE_TRINO,
    SOURCE_TYPE_TRINO_SUBSELECT,
    SOURCE_TYPE_TRINO_TABLE,
)


LOGGER = logging.getLogger(__name__)


class TrinoDataSourceMixin(BaseSQLDataSource):
    supported_join_types: ClassVar[FrozenSet[JoinType]] = frozenset(
        {
            JoinType.inner,
            JoinType.left,
            JoinType.right,
            JoinType.full,
        }
    )

    conn_type = CONNECTION_TYPE_TRINO

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type in (SOURCE_TYPE_TRINO_TABLE, SOURCE_TYPE_TRINO_SUBSELECT)


class TrinoDataSource(TrinoDataSourceMixin, StandardSQLDataSource):
    """Trino table"""


class TrinoSubselectDataSource(TrinoDataSourceMixin, SubselectDataSource):
    """Trino table from a subquery"""
