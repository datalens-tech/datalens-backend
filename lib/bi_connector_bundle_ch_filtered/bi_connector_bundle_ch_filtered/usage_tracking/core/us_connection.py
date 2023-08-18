from __future__ import annotations

from typing import ClassVar, Optional, TYPE_CHECKING

from bi_constants.enums import CreateDSFrom

from bi_core.connectors.clickhouse_base.us_connection import (
    ConnectionCHFilteredHardcodedDataBase, SubselectParameter, SubselectParameterType
)
from bi_core.us_connection_base import HiddenDatabaseNameMixin

if TYPE_CHECKING:
    from bi_configs.connectors_settings import UsageTrackingConnectionSettings


class UsageTrackingConnection(HiddenDatabaseNameMixin, ConnectionCHFilteredHardcodedDataBase):
    source_type = CreateDSFrom.CH_USAGE_TRACKING_TABLE
    allowed_source_types = frozenset((CreateDSFrom.CH_USAGE_TRACKING_TABLE,))
    is_always_internal_source = True
    allow_cache: ClassVar[bool] = True

    tenant_id: Optional[str] = None

    @property
    def _connector_settings(self) -> UsageTrackingConnectionSettings:
        settings = self._all_connectors_settings.USAGE_TRACKING
        assert settings is not None
        return settings

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
