from __future__ import annotations

import logging
from typing import ClassVar, FrozenSet

from bi_constants.enums import JoinType, ConnectionType

from bi_core.connectors.clickhouse_base.data_source import ClickHouseTemplatedSubselectDataSource


LOGGER = logging.getLogger(__name__)


class UsageTrackingYaTeamDataSource(ClickHouseTemplatedSubselectDataSource):
    """
    Clickhouse datasource with data filtration by current user id.
    """
    supported_join_types: ClassVar[FrozenSet[JoinType]] = frozenset()

    conn_type = ConnectionType.usage_tracking_ya_team
