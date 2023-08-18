from __future__ import annotations

from typing import ClassVar, Optional, TYPE_CHECKING

from bi_constants.enums import CreateDSFrom

from bi_core.connectors.clickhouse_base.us_connection import (
    ConnectionCHFilteredHardcodedDataBase, SubselectParameter, SubselectParameterType
)

if TYPE_CHECKING:
    from bi_configs.connectors_settings import BillingConnectorSettings


class BillingAnalyticsCHConnection(ConnectionCHFilteredHardcodedDataBase):
    source_type = CreateDSFrom.CH_BILLING_ANALYTICS_TABLE
    allowed_source_types = frozenset((CreateDSFrom.CH_BILLING_ANALYTICS_TABLE,))
    is_always_internal_source = True
    allow_cache: ClassVar[bool] = True

    billing_accounts: Optional[list[str]] = None

    @property
    def _connector_settings(self) -> BillingConnectorSettings:
        settings = self._all_connectors_settings.CH_BILLING_ANALYTICS
        assert settings is not None
        return settings

    @property
    def is_subselect_allowed(self) -> bool:
        return True

    @property
    def subselect_parameters(self) -> list[SubselectParameter]:
        assert self.billing_accounts is not None
        return [
            SubselectParameter(
                name='billing_account_id_list',
                ss_type=SubselectParameterType.list_of_values,
                values=self.billing_accounts,
            )
        ]
