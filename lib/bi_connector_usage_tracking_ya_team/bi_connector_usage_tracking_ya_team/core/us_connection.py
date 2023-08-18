from __future__ import annotations

from typing import ClassVar, TYPE_CHECKING

from bi_constants.enums import CreateDSFrom

from bi_core.connectors.clickhouse_base.conn_options import CHConnectOptions
from bi_core.connectors.clickhouse_base.us_connection import (
    ConnectionCHFilteredHardcodedDataBase, SubselectParameter, SubselectParameterType
)
from bi_core.us_connection_base import HiddenDatabaseNameMixin

if TYPE_CHECKING:
    from bi_configs.connectors_settings import UsageTrackingYaTeamConnectionSettings


class UsageTrackingYaTeamConnection(HiddenDatabaseNameMixin, ConnectionCHFilteredHardcodedDataBase):
    source_type = CreateDSFrom.CH_USAGE_TRACKING_YA_TEAM_TABLE
    allowed_source_types = frozenset((CreateDSFrom.CH_USAGE_TRACKING_YA_TEAM_TABLE,))
    is_always_internal_source = True
    allow_cache: ClassVar[bool] = True

    @property
    def _connector_settings(self) -> UsageTrackingYaTeamConnectionSettings:
        settings = self._all_connectors_settings.USAGE_TRACKING_YA_TEAM
        assert settings is not None
        return settings

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
