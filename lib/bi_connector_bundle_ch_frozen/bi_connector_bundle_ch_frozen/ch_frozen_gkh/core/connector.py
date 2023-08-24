from __future__ import annotations

from bi_configs.connectors_settings import CHFrozenGKHConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.connector import (
    CHFrozenBaseCoreConnectionDefinition,
    CHFrozenBaseCoreSourceDefinition,
    CHFrozenCoreConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.core.constants import CONNECTION_TYPE_CH_FROZEN_GKH
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.core.data_source import ClickHouseFrozenGKHDataSource


class CHFrozenGKHCoreConnectionDefinition(CHFrozenBaseCoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_FROZEN_GKH
    settings_class = CHFrozenGKHConnectorSettings


class CHFrozenGKHCoreSourceDefinition(CHFrozenBaseCoreSourceDefinition):
    source_cls = ClickHouseFrozenGKHDataSource


class CHFrozenGKHCoreConnector(CHFrozenCoreConnector):
    connection_definitions = (
        CHFrozenGKHCoreConnectionDefinition,
    )
    source_definitions = (
        CHFrozenGKHCoreSourceDefinition,
    )
