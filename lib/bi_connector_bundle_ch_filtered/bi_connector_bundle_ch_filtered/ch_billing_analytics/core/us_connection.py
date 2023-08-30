from __future__ import annotations

from typing import ClassVar, Optional

from bi_configs.connectors_settings import BillingConnectorSettings
from bi_constants.enums import CreateDSFrom

from bi_core.connectors.clickhouse_base.us_connection import (
    ConnectionCHFilteredHardcodedDataBase, SubselectParameter, SubselectParameterType
)


class BillingAnalyticsCHConnection(ConnectionCHFilteredHardcodedDataBase[BillingConnectorSettings]):
    source_type = CreateDSFrom.CH_BILLING_ANALYTICS_TABLE
    allowed_source_types = frozenset((CreateDSFrom.CH_BILLING_ANALYTICS_TABLE,))
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
