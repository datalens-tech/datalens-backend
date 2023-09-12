from __future__ import annotations

from typing import ClassVar, Optional

from bi_configs.connectors_settings import UsageTrackingConnectionSettings

from bi_core.us_connection_base import HiddenDatabaseNameMixin

from bi_core.connectors.clickhouse_base.us_connection import SubselectParameter, SubselectParameterType
from bi_connector_bundle_ch_filtered.base.core.us_connection import ConnectionCHFilteredHardcodedDataBase
from bi_connector_bundle_ch_filtered.usage_tracking.core.constants import (
    SOURCE_TYPE_CH_USAGE_TRACKING_TABLE,
)


class UsageTrackingConnection(
        HiddenDatabaseNameMixin, ConnectionCHFilteredHardcodedDataBase[UsageTrackingConnectionSettings]
):
    source_type = SOURCE_TYPE_CH_USAGE_TRACKING_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_USAGE_TRACKING_TABLE,))
    is_always_internal_source = True
    allow_cache: ClassVar[bool] = True
    settings_type = UsageTrackingConnectionSettings

    tenant_id: Optional[str] = None

    @property
    def required_iam_role(self) -> str:
        return self._connector_settings.REQUIRED_IAM_ROLE

    @property
    def is_subselect_allowed(self) -> bool:
        return True

    @property
    def subselect_parameters(self) -> list[SubselectParameter]:
        assert self.tenant_id is not None
        return [
            SubselectParameter(
                name='tenant_id',
                ss_type=SubselectParameterType.single_value,
                values=self.tenant_id,
            )
        ]
