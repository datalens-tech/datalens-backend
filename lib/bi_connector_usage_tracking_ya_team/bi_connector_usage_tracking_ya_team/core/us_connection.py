from __future__ import annotations

from typing import ClassVar

from bi_configs.connectors_settings import UsageTrackingYaTeamConnectionSettings

from bi_core.us_connection_base import HiddenDatabaseNameMixin

from bi_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions
from bi_connector_clickhouse.core.clickhouse_base.us_connection import SubselectParameter, SubselectParameterType
from bi_connector_bundle_ch_filtered.base.core.us_connection import ConnectionCHFilteredHardcodedDataBase
from bi_connector_usage_tracking_ya_team.core.constants import (
    SOURCE_TYPE_CH_USAGE_TRACKING_YA_TEAM_TABLE,
)


class UsageTrackingYaTeamConnection(
        HiddenDatabaseNameMixin,
        ConnectionCHFilteredHardcodedDataBase[UsageTrackingYaTeamConnectionSettings]
):
    source_type = SOURCE_TYPE_CH_USAGE_TRACKING_YA_TEAM_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_USAGE_TRACKING_YA_TEAM_TABLE,))
    is_always_internal_source = True
    allow_cache: ClassVar[bool] = True
    settings_type = UsageTrackingYaTeamConnectionSettings

    @property
    def is_subselect_allowed(self) -> bool:
        return True

    def get_conn_options(self) -> CHConnectOptions:
        return super().get_conn_options().clone(
            max_execution_time=self._connector_settings.MAX_EXECUTION_TIME,
        )

    @property
    def subselect_parameters(self) -> list[SubselectParameter]:
        user_id = self.us_manager.get_services_registry().rci.user_id
        assert user_id is not None
        return [
            SubselectParameter(
                name='user_id',
                ss_type=SubselectParameterType.single_value,
                values=user_id,
            )
        ]
