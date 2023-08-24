from __future__ import annotations

from bi_configs.connectors_settings import CHFrozenTransparencyConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.connector import (
    CHFrozenBaseCoreConnectionDefinition,
    CHFrozenBaseCoreSourceDefinition,
    CHFrozenCoreConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.core.constants import CONNECTION_TYPE_CH_FROZEN_TRANSPARENCY
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.core.data_source import ClickHouseFrozenTransparencyDataSource


class CHFrozenTransparencyCoreConnectionDefinition(CHFrozenBaseCoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_FROZEN_TRANSPARENCY
    settings_class = CHFrozenTransparencyConnectorSettings


class CHFrozenTransparencyCoreSourceDefinition(CHFrozenBaseCoreSourceDefinition):
    source_cls = ClickHouseFrozenTransparencyDataSource


class CHFrozenTransparencyCoreConnector(CHFrozenCoreConnector):
    connection_definitions = (
        CHFrozenTransparencyCoreConnectionDefinition,
    )
    source_definitions = (
        CHFrozenTransparencyCoreSourceDefinition,
    )
