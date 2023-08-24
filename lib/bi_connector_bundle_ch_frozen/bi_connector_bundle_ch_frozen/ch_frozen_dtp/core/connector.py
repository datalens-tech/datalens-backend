from __future__ import annotations

from bi_configs.connectors_settings import CHFrozenDTPConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.connector import (
    CHFrozenBaseCoreConnectionDefinition,
    CHFrozenBaseCoreSourceDefinition,
    CHFrozenCoreConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.core.constants import CONNECTION_TYPE_CH_FROZEN_DTP
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.core.data_source import ClickHouseFrozenDTPDataSource


class CHFrozenDTPCoreConnectionDefinition(CHFrozenBaseCoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_FROZEN_DTP
    settings_class = CHFrozenDTPConnectorSettings


class CHFrozenDTPCoreSourceDefinition(CHFrozenBaseCoreSourceDefinition):
    source_cls = ClickHouseFrozenDTPDataSource


class CHFrozenDTPCoreConnector(CHFrozenCoreConnector):
    connection_definitions = (
        CHFrozenDTPCoreConnectionDefinition,
    )
    source_definitions = (
        CHFrozenDTPCoreSourceDefinition,
    )
