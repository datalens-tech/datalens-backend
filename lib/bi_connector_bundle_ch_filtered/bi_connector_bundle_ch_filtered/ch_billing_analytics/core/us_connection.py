from __future__ import annotations

from typing import ClassVar, Optional

from bi_configs.connectors_settings import BillingConnectorSettings

from bi_connector_clickhouse.core.clickhouse_base.us_connection import SubselectParameter, SubselectParameterType
from bi_connector_bundle_ch_filtered.base.core.us_connection import ConnectionCHFilteredHardcodedDataBase

from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.constants import (
    SOURCE_TYPE_CH_BILLING_ANALYTICS_TABLE,
)


class BillingAnalyticsCHConnection(ConnectionCHFilteredHardcodedDataBase[BillingConnectorSettings]):
    source_type = SOURCE_TYPE_CH_BILLING_ANALYTICS_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_BILLING_ANALYTICS_TABLE,))
    is_always_internal_source = True
    allow_cache: ClassVar[bool] = True
    settings_type = BillingConnectorSettings

    billing_accounts: Optional[list[str]] = None

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
