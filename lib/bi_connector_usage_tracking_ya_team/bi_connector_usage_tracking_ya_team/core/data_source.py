from __future__ import annotations

import logging
from typing import ClassVar, FrozenSet

from bi_constants.enums import JoinType

from bi_connector_usage_tracking_ya_team.core.constants import (
    CONNECTION_TYPE_USAGE_TRACKING_YA_TEAM,
)
from bi_core.connectors.clickhouse_base.data_source import ClickHouseTemplatedSubselectDataSource


LOGGER = logging.getLogger(__name__)


class UsageTrackingYaTeamDataSource(ClickHouseTemplatedSubselectDataSource):
    """
    Clickhouse datasource with data filtration by current user id.
    """
    supported_join_types: ClassVar[FrozenSet[JoinType]] = frozenset()

    conn_type = CONNECTION_TYPE_USAGE_TRACKING_YA_TEAM
